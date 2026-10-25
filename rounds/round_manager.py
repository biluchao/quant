"""
rounds/round_manager.py — 轮次生命周期管理 (重铸标准版)

核心设计原则：
1. 事务强一致：开仓必须伴随止损与落库。挂单失败立刻强制回滚平仓。
2. 绝对隔离：下单异常（如断网、拒单）不得穿透导致主程序崩溃。
3. 强依赖：所有组件由外部注入，绝不内部自造兜底。
4. 幂等平仓：同一轮次多次触发平仓，只结算一次。
"""

import logging
import threading
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable, Dict, List, Optional

from core.gateway import (
    Order, OrderResult, OrderSide, OrderStatus, OrderType, WorkingType
)
from rounds.position_round import (
    PositionRound, RoundOrder, RoundRecord, PositionRoundError
)
from system.order_idempotent import OrderKind

logger = logging.getLogger(__name__)

class RoundManagerError(Exception):
    pass

class RoundManager:
    def __init__(
        self,
        risk_policy: Any,
        persister: Any,
        clock: Any,
        order_gateway: Any,
        id_generator: Any,
        symbol: str,
        config: dict,
        on_round_close: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        # 强依赖注入检查
        if not all([risk_policy, persister, clock, order_gateway, id_generator, symbol]):
            raise ValueError("RoundManager 缺少必要的核心依赖，拒绝启动")

        self.risk = risk_policy
        self.persister = persister
        self.clock = clock
        self.gateway = order_gateway
        self.id_gen = id_generator
        self.symbol = symbol.upper()
        self.config = config
        self.on_round_close = on_round_close

        # 从配置获取周期
        interval_str = config.get("data", {}).get("kline_interval", "3m")
        self.kline_interval_ms = {"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000}.get(interval_str, 180000)

        self._lock = threading.RLock()
        self._active_rounds: Dict[str, PositionRound] = {}
        self._closed_rounds: List[RoundRecord] = []
        self._stop_orders: Dict[str, str] = {}  # round_id -> stop_client_order_id
        self._round_seq = 0

    # ========================================================================
    # 开仓流程 (事务一致性)
    # ========================================================================

    def open_round(
        self, direction: int, entry_hint: Optional[Decimal], stop_hint: Optional[Decimal], signal_id: str, bar_ts: int, meta: dict = None
    ) -> Optional[Dict[str, Any]]:
        """执行开仓逻辑"""
        with self._lock:
            if self._active_rounds:
                logger.warning("已有活跃轮次，拒绝开新仓。")
                return None

            if not stop_hint:
                logger.error("开仓缺少止损提示，拒绝下单。")
                return None
            
            self._round_seq += 1
            round_seq = self._round_seq

        # 1. 计算目标数量与风控审批
        try:
            entry_price = entry_hint if entry_hint else Decimal("0")
            if entry_price <= 0:
                # 如果没给入场价，拒绝开仓
                return None
                
            risk_per_unit = abs(entry_price - stop_hint)
            target_qty = self._calc_target_qty(risk_per_unit)
            if target_qty <= 0:
                return None

            # 简单构造一个 Order 给风控审
            client_id = self.id_gen.generate(OrderKind.ENTRY, round_seq, 0, 0)
            temp_order = Order(
                client_order_id=client_id, symbol=self.symbol, side=OrderSide.BUY if direction == 1 else OrderSide.SELL,
                type=OrderType.MARKET, qty=target_qty, metadata={"expected_price": entry_price, "stop_price_hint": stop_hint}
            )
            
            risk_result = self.risk.approve_order(temp_order)
            if not risk_result.is_approved():
                logger.info(f"风控拒绝开仓: {risk_result.reason}")
                return None
                
            final_qty = risk_result.effective_qty(target_qty)
            if final_qty != target_qty:
                temp_order = Order(
                    client_order_id=client_id, symbol=self.symbol, side=temp_order.side,
                    type=OrderType.MARKET, qty=final_qty, metadata=temp_order.metadata
                )
        except Exception as e:
            logger.error(f"开仓前置处理失败: {e}")
            return None

        # 2. 下单
        try:
            res = self.gateway.place_order(temp_order)
            if res.status not in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
                raise RoundManagerError(f"开仓未成交: {res.status}")
        except Exception as e:
            logger.error(f"开仓下单失败: {e}")
            return None

        filled_qty = res.executed_qty
        filled_price = res.avg_price or entry_price

        # 3. 建立账本
        try:
            round_order = RoundOrder(order_id=res.client_order_id, price=filled_price, qty=filled_qty, added_ts=bar_ts)
            round_id = f"r{round_seq:04d}-{res.client_order_id[-4:]}"
            
            position_round = PositionRound(
                round_id=round_id, symbol=self.symbol, direction=direction, initial_order=round_order,
                initial_stop=stop_hint, opened_bar_ts=bar_ts, signal_id=signal_id, entry_meta=meta,
                kline_interval_ms=self.kline_interval_ms
            )
        except Exception as e:
            logger.critical(f"账本建立失败，强制平仓回滚: {e}")
            self._emergency_close(direction, filled_qty)
            return None

        # 4. 挂服务器端止损
        try:
            stop_id = self._place_stop_order(position_round, round_seq, 0)
            if not stop_id:
                raise RoundManagerError("止损单挂单失败")
        except Exception as e:
            logger.critical(f"止损挂单失败，强制平仓回滚: {e}")
            self._emergency_close(direction, filled_qty)
            return None

        # 5. 落库并注册
        try:
            self.persister.insert_round_open(position_round.to_dict())
        except Exception as e:
            logger.critical(f"持久化失败，强制回滚: {e}")
            try: self.gateway.cancel_order(stop_id)
            except: pass
            self._emergency_close(direction, filled_qty)
            return None

        with self._lock:
            self._active_rounds[round_id] = position_round
            self._stop_orders[round_id] = stop_id

        logger.info(f"✅ 轮次开仓成功: {round_id} | 方向: {direction} | 均价: {filled_price} | 止损: {stop_hint}")
        return self._get_active_snapshot()

    def _emergency_close(self, direction: int, qty: Decimal) -> None:
        """开仓失败时的保命回滚"""
        side = OrderSide.SELL if direction == 1 else OrderSide.BUY
        try:
            cid = self.id_gen.generate(OrderKind.CLOSE_ALL, 0, 0, 0)
            self.gateway.place_order(Order(
                client_order_id=cid, symbol=self.symbol, side=side,
                type=OrderType.MARKET, qty=qty, reduce_only=True
            ))
            logger.info("回滚平仓指令发送成功。")
        except Exception as e:
            logger.critical(f"致命：回滚平仓失败，可能裸奔！{e}")

    # ========================================================================
    # 平仓流程
    # ========================================================================

    def close_round(self, exit_reason: str, exit_price_hint: Optional[Decimal] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._active_rounds:
                return None
            # 取第一个活跃轮次（Step1 单轮限制）
            round_id, pr = next(iter(self._active_rounds.items()))

        if not pr.is_open():
            return pr.get_record().to_dict() if pr.get_record() else None

        # 1. 撤止损（失败不阻断平仓）
        with self._lock:
            stop_id = self._stop_orders.get(round_id)
        if stop_id:
            try:
                self.gateway.cancel_order(stop_id)
            except Exception as e:
                logger.warning(f"平仓前撤止损失败 (忽略): {e}")

        # 2. 下平仓单
        side = OrderSide.SELL if pr.direction == 1 else OrderSide.BUY
        cid = self.id_gen.generate(OrderKind.EXIT, 0, 0, 0)
        
        try:
            res = self.gateway.place_order(Order(
                client_order_id=cid, symbol=self.symbol, side=side,
                type=OrderType.MARKET, qty=pr.total_qty, reduce_only=True
            ))
            if res.status not in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
                raise RoundManagerError(f"平仓未成交: {res.status}")
        except Exception as e:
            logger.error(f"平仓下单失败，轮次保持活跃等待重试: {e}")
            return None

        # 3. 结算
        exit_price = res.avg_price or (exit_price_hint if exit_price_hint else pr.current_stop)
        exit_ts = res.transact_time or self.clock.now_ms()

        record = pr.close(exit_price, exit_ts, exit_reason)

        # 4. 持久化与清理
        try:
            self.persister.update_round_close(
                round_id=record.round_id, exit_price=str(record.exit_price), exit_ts=record.closed_ts,
                pnl=str(record.realized_pnl), r_multiple=record.r_multiple, exit_reason=record.exit_reason,
                peak_unrealized=str(record.peak_unrealized)
            )
        except Exception as e:
            logger.error(f"平仓持久化失败: {e}")

        with self._lock:
            self._active_rounds.pop(round_id, None)
            self._stop_orders.pop(round_id, None)
            self._closed_rounds.append(record)

        # 5. 触发回调
        if self.on_round_close:
            try:
                self.on_round_close(record.to_dict())
            except Exception as e:
                logger.error(f"on_round_close 回调异常: {e}")

        logger.info(f"✅ 轮次平仓成功: {round_id} | 原因: {exit_reason} | PnL: {record.realized_pnl}")
        return record.to_dict()

    # ========================================================================
    # 辅助工具
    # ========================================================================

    def _calc_target_qty(self, risk_per_unit: Decimal) -> Decimal:
        """根据风控资金管理规则计算目标仓位"""
        # 假设单笔允许亏损 0.7%
        risk_pct = Decimal(str(self.config.get("risk", {}).get("per_trade_risk", "0.007")))
        equity = self.risk.equity
        
        max_loss_usd = equity * risk_pct
        if risk_per_unit <= 0:
            return Decimal("0")
            
        qty = max_loss_usd / risk_per_unit
        
        # 简单向下取整到 0.001（实际应调用 precision 处理）
        step = Decimal("0.001")
        return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _place_stop_order(self, pr: PositionRound, seq: int, stop_seq: int) -> Optional[str]:
        try:
            cid = self.id_gen.generate(OrderKind.STOP, seq, stop_seq, 0)
            side = OrderSide.SELL if pr.direction == 1 else OrderSide.BUY
            res = self.gateway.place_order(Order(
                client_order_id=cid, symbol=self.symbol, side=side, type=OrderType.STOP_MARKET,
                qty=pr.total_qty, stop_price=pr.current_stop, reduce_only=True, working_type=WorkingType.MARK_PRICE
            ))
            return cid if res.status == OrderStatus.NEW else None
        except Exception:
            return None

    def get_active_round(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self._active_rounds: return None
            pr = next(iter(self._active_rounds.values()))
            return self._get_active_snapshot(pr)

    def _get_active_snapshot(self, pr: PositionRound = None) -> Dict[str, Any]:
        if not pr:
            return None
        return {
            "round_id": pr.round_id, "symbol": pr.symbol, "direction": pr.direction,
            "entry_price": str(pr.avg_entry), "current_stop": str(pr.current_stop),
            "total_qty": str(pr.total_qty), "peak_unrealized": str(pr.peak_unrealized),
            "opened_ts": pr.opened_bar_ts, "signal_id": pr.signal_id
        }
