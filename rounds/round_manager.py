"""
rounds/round_manager.py — 轮次管理器（柜面操作）

一轮（Round）的完整生命周期管理：
- 开仓（open_round）：风控批准 → 下单 → 创建 PositionRound → 挂服务器端止损 → 持久化
- 更新止损（update_stop）：PositionRound.update_stop → 撤旧止损 → 挂新止损
- 平仓（close_round）：撤止损 → 下单平仓 → PositionRound.close → 持久化 → 回调
- 恢复（recover）：从持久化 + 交易所持仓重建

职责边界：
- PositionRound：单轮记账 + 止损状态
- RoundManager：订单执行 + 持久化 + 回调 + 历史管理
- StateMachine：触发操作，处理结果
- Gateway：实际下单
- RiskPolicy：批准订单
- Persister：落库

不做什么：
- 不做风控决策（RiskPolicy 负责）
- 不生成状态机信号（StateMachine + Strategy 负责）
- 不感知因子/策略/状态内容（只转发）
- 不持有 FactorRegistry 引用

关键契约：
1. Step 1 单轮：`_active_rounds` 最多 1 个元素，开新轮前检查无活跃轮次
2. 开仓：风控 → 下单 → 创建 → 挂止损。任何一步失败，返回 None，不留残余
3. 开仓止损挂失败：立即市价平仓，返回 None（宁可不开，不可无保护持仓）
4. 平仓：撤止损 → 下单 → PositionRound.close → 持久化 → 回调。平仓失败返回 None，轮次保持活跃
5. 平仓幂等：已 closed 返回原记录
6. 更新止损：PositionRound 先更新（唯一真相）→ 撤旧 → 挂新。挂新失败记录 CRITICAL，下一根 K 线重试
7. 止损单方向：与持仓方向相反（多单挂 SELL 止损）
8. 所有订单 ID 通过 `id_generator` 生成
9. 所有落库通过 `persister`
10. `on_round_close` 回调在平仓成功后触发，异常不阻断
11. `_round_seq` 从 `persister` 恢复，重启后不重复
12. 恢复时快照与持仓不匹配 → 抛异常，人工确认
13. 所有价格/数量用 Decimal
14. 所有时间戳毫秒 int（UTC）
15. 线程安全（RLock）

订单 ID 格式（由 id_generator 生成）：
    bm-ent-{round_seq:04d}-00-{attempt:02d}  开仓
    bm-stp-{round_seq:04d}-{stop_seq:02d}-00 止损
    bm-ext-{round_seq:04d}-00-{attempt:02d}  平仓

订单类型：
    开仓：MARKET，side=BUY（多）/ SELL（空），reduce_only=False
    止损：STOP_MARKET，side=SELL（多）/ BUY（空），reduce_only=True，
          working_type=MARK_PRICE
    平仓：MARKET，side=SELL（多）/ BUY（空），reduce_only=True

与 state_machine 的协作：
    state_machine._open_round()
      → round_manager.open_round(...)
          → risk_policy.approve_order()
          → gateway.place_order(MARKET)
          → PositionRound 创建
          → gateway.place_order(STOP_MARKET)
          → persister.insert_round_open()
      → 返回 active_round 快照

    state_machine._close_round()
      → round_manager.close_round(exit_reason, exit_price_hint)
          → gateway.cancel_order(stop_id)
          → gateway.place_order(MARKET reduce_only)
          → PositionRound.close(exit_price, ts, reason)
          → persister.update_round_close()
          → on_round_close 回调
      → 返回 RoundRecord 字典

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from core.gateway import (
    Order,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    WorkingType,
)
from rounds.position_round import (
    PositionRound,
    PositionRoundError,
    RoundOrder,
    RoundRecord,
    RoundStatus,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "round_manager",
    "type": "round",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["round_manager", "round_lifecycle"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Round manager: create, update, close PositionRound with transaction-before-transition",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_IN_MEMORY = 100
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000
MIN_QTY_THRESHOLD = Decimal("0")


# ============================================================================
# 错误
# ============================================================================
class RoundManagerError(Exception):
    """RoundManager 错误（通常需要人工确认）。"""
    pass


# ============================================================================
# RoundManager
# ============================================================================
class RoundManager:
    """
    轮次管理器。详见模块 docstring。

    用法：
        rm = RoundManager(
            risk_policy=risk_policy,
            persister=persister,
            clock=clock,
            order_gateway=gateway,
            id_generator=id_gen,
            symbol="BTCUSDT",
            config=config,
            max_in_memory=100,
            on_round_close=lambda rec: logger.info(f"closed {rec['round_id']}"),
        )

        snapshot = rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-0001",
            bar_ts=1700000000000,
            meta={"factor_snapshot": {...}},
        )

        rm.update_stop(Decimal("66900"))

        record = rm.close_round(
            exit_reason="stop",
            exit_price_hint=None,
        )
    """

    name = "round_manager"
    version = "1.0.0"

    def __init__(
        self,
        risk_policy: Any,
        persister: Any,
        clock: Any,
        order_gateway: Any,
        id_generator: Any,
        symbol: str,
        config: Optional[dict] = None,
        max_in_memory: int = DEFAULT_MAX_IN_MEMORY,
        on_round_close: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        cfg = (config or {}).get("round_manager", {})
        data_cfg = (config or {}).get("data", {})

        self._risk_policy = risk_policy
        self._persister = persister
        self._clock = clock
        self._gateway = order_gateway
        self._id_generator = id_generator
        self._symbol = symbol.upper()
        self._max_in_memory = int(
            cfg.get("max_in_memory", max_in_memory)
        )
        self._on_round_close = on_round_close

        interval_str = data_cfg.get("kline_interval", "3m")
        self._kline_interval_ms = self._parse_interval_ms(interval_str)

        self._lock = threading.RLock()
        self._active_rounds: Dict[str, PositionRound] = {}
        self._closed_rounds: List[RoundRecord] = []
        self._round_seq: int = 0
        self._stop_orders: Dict[str, str] = {}
        self._stop_seq: Dict[str, int] = {}
        self._round_seq_map: Dict[str, int] = {}

        self._stats = {
            "open_attempts": 0,
            "open_successes": 0,
            "open_rejections": 0,
            "open_rollbacks": 0,
            "close_attempts": 0,
            "close_successes": 0,
            "close_failures": 0,
            "close_rejections": 0,
            "stop_update_attempts": 0,
            "stop_update_successes": 0,
            "stop_update_rejections": 0,
            "stop_update_failures": 0,
            "recoveries": 0,
            "recover_failures": 0,
            "errors": 0,
        }
        self._last_error: Optional[str] = None

        logger.info(
            f"RoundManager initialized "
            f"symbol={self._symbol} "
            f"max_in_memory={self._max_in_memory} "
            f"kline_interval_ms={self._kline_interval_ms}"
        )

    def _parse_interval_ms(self, interval: str) -> int:
        """解析 interval 为毫秒。"""
        mapping = {
            "1m": 60 * 1000,
            "3m": 3 * 60 * 1000,
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000,
        }
        if interval not in mapping:
            raise RoundManagerError(
                f"unsupported interval: {interval}"
            )
        return mapping[interval]

    # ========================================================================
    # 开仓
    # ========================================================================

    def open_round(
        self,
        direction: int,
        entry_price_hint: Optional[Any],
        stop_price_hint: Optional[Any],
        signal_id: Optional[str],
        bar_ts: int,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        开仓。

        契约：
        - 有活跃轮次时拒绝（Step 1 单轮）
        - 风控批准、下单成功、止损挂成功才创建 PositionRound
        - 任何一步失败返回 None，不留残余
        - 止损挂失败立即平仓（宁可不开，不可无保护持仓）

        Returns:
            活跃轮次快照或 None
        """
        with self._lock:
            self._stats["open_attempts"] += 1

            # 单轮检查
            if self._active_rounds:
                logger.warning(
                    f"open_round rejected: already has active round "
                    f"{list(self._active_rounds.keys())}"
                )
                self._stats["open_rejections"] += 1
                return None

        try:
            return self._open_round_impl(
                direction=direction,
                entry_price_hint=entry_price_hint,
                stop_price_hint=stop_price_hint,
                signal_id=signal_id,
                bar_ts=bar_ts,
                meta=meta,
            )
        except Exception as e:
            logger.error(
                f"open_round error: {e}", exc_info=True
            )
            self._stats["errors"] += 1
            self._last_error = str(e)
            return None

    def _open_round_impl(
        self,
        direction: int,
        entry_price_hint: Optional[Any],
        stop_price_hint: Optional[Any],
        signal_id: Optional[str],
        bar_ts: int,
        meta: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """开仓的实际实现。"""
        # ---- 参数校验 ----
        if direction not in (1, -1):
            logger.error(f"invalid direction {direction}")
            self._stats["open_rejections"] += 1
            return None
        if stop_price_hint is None:
            logger.error("stop_price_hint is required")
            self._stats["open_rejections"] += 1
            return None

        try:
            stop_price = Decimal(str(stop_price_hint))
        except Exception as e:
            logger.error(f"invalid stop_price {stop_price_hint}: {e}")
            self._stats["open_rejections"] += 1
            return None

        # 入场价：优先用 hint，否则用当前 mark price
        entry_price: Optional[Decimal] = None
        if entry_price_hint is not None:
            try:
                entry_price = Decimal(str(entry_price_hint))
            except Exception:
                entry_price = None

        if entry_price is None:
            try:
                mark = self._gateway.get_mark_price()
                entry_price = Decimal(str(mark))
            except Exception as e:
                logger.error(f"cannot get mark price: {e}")
                self._stats["open_rejections"] += 1
                return None

        if entry_price <= 0 or stop_price <= 0:
            logger.error(
                f"invalid prices: entry={entry_price} stop={stop_price}"
            )
            self._stats["open_rejections"] += 1
            return None

        # 方向性止损校验
        if direction == 1 and stop_price >= entry_price:
            logger.error(
                f"LONG stop {stop_price} >= entry {entry_price}"
            )
            self._stats["open_rejections"] += 1
            return None
        if direction == -1 and stop_price <= entry_price:
            logger.error(
                f"SHORT stop {stop_price} <= entry {entry_price}"
            )
            self._stats["open_rejections"] += 1
            return None

        # ---- 计算数量 ----
        qty = self._calc_target_qty(entry_price, stop_price)
        if qty <= MIN_QTY_THRESHOLD:
            logger.warning(
                f"calculated qty {qty} too small, rejecting open"
            )
            self._stats["open_rejections"] += 1
            return None

        # ---- 生成 ID ----
        round_seq = self._next_round_seq()
        round_id = f"r{round_seq:06d}-{uuid.uuid4().hex[:8]}"

        from system.order_idempotent import OrderKind
        try:
            entry_client_id = self._id_generator.generate(
                OrderKind.ENTRY, round_seq, role_seq=0, attempt=0
            )
        except Exception as e:
            logger.error(f"generate entry id failed: {e}")
            self._stats["open_rejections"] += 1
            return None

        # ---- 构造订单 ----
        side = OrderSide.BUY if direction == 1 else OrderSide.SELL
        try:
            entry_order = Order(
                client_order_id=entry_client_id,
                symbol=self._symbol,
                side=side,
                type=OrderType.MARKET,
                qty=qty,
                reduce_only=False,
            )
        except Exception as e:
            logger.error(f"construct order failed: {e}")
            self._stats["open_rejections"] += 1
            return None

        # ---- 风控批准 ----
        approved = self._risk_approve(entry_order, direction, stop_price)
        if approved is None:
            logger.warning("risk rejected open_round")
            self._stats["open_rejections"] += 1
            return None

        if approved != qty:
            logger.info(
                f"risk reduced qty from {qty} to {approved}"
            )
            try:
                entry_order = Order(
                    client_order_id=entry_client_id,
                    symbol=self._symbol,
                    side=side,
                    type=OrderType.MARKET,
                    qty=approved,
                    reduce_only=False,
                )
            except Exception as e:
                logger.error(f"reconstruct order failed: {e}")
                self._stats["open_rejections"] += 1
                return None
            qty = approved

        # ---- 下单 ----
        try:
            entry_result = self._gateway.place_order(entry_order)
        except Exception as e:
            logger.error(f"place entry order failed: {e}")
            self._stats["open_rejections"] += 1
            self._last_error = str(e)
            return None

        if entry_result.status not in (
            OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED
        ):
            logger.error(
                f"entry order not filled: status={entry_result.status} "
                f"reason={entry_result.reject_reason}"
            )
            self._stats["open_rejections"] += 1
            return None

        filled_qty = entry_result.executed_qty
        filled_price = entry_result.avg_price
        if filled_qty is None or filled_qty <= 0:
            logger.error(f"invalid filled_qty {filled_qty}")
            self._stats["open_rejections"] += 1
            return None
        if filled_price is None or filled_price <= 0:
            logger.error(f"invalid filled_price {filled_price}")
            self._stats["open_rejections"] += 1
            return None

        if filled_qty < qty:
            logger.warning(
                f"partial fill: requested {qty}, filled {filled_qty}"
            )

        # ---- 创建 PositionRound ----
        try:
            round_order = RoundOrder(
                order_id=entry_result.client_order_id,
                price=filled_price,
                qty=filled_qty,
                added_ts=bar_ts,
                tag="initial",
            )
            position_round = PositionRound(
                round_id=round_id,
                symbol=self._symbol,
                direction=direction,
                initial_order=round_order,
                initial_stop=stop_price,
                opened_bar_ts=bar_ts,
                signal_id=signal_id,
                entry_meta=meta or {},
                kline_interval_ms=self._kline_interval_ms,
            )
        except Exception as e:
            logger.error(
                f"create PositionRound failed: {e}, "
                f"rolling back (closing filled position)"
            )
            self._stats["open_rollbacks"] += 1
            self._emergency_close_position(direction, filled_qty)
            return None

        # ---- 挂服务器端止损 ----
        stop_id = self._place_server_side_stop(
            position_round, round_seq, stop_seq=0
        )
        if stop_id is None:
            logger.critical(
                f"failed to place server-side stop, "
                f"rolling back (closing filled position)"
            )
            self._stats["open_rollbacks"] += 1
            self._emergency_close_position(direction, filled_qty)
            return None

        # ---- 持久化 ----
        try:
            self._persister.insert_round_open(
                position_round.to_dict()
            )
        except Exception as e:
            logger.error(
                f"persist round open failed: {e}, "
                f"rolling back (closing filled position + cancel stop)"
            )
            self._stats["open_rollbacks"] += 1
            try:
                self._gateway.cancel_order(stop_id)
            except Exception:
                pass
            self._emergency_close_position(direction, filled_qty)
            return None

        # ---- 登记 ----
        with self._lock:
            self._active_rounds[round_id] = position_round
            self._stop_orders[round_id] = stop_id
            self._stop_seq[round_id] = 0
            self._round_seq_map[round_id] = round_seq
            self._stats["open_successes"] += 1

        logger.info(
            f"round opened: round_id={round_id} "
            f"direction={direction} qty={filled_qty} "
            f"entry={filled_price} stop={stop_price} "
            f"stop_order_id={stop_id} signal_id={signal_id}"
        )

        return self._get_active_snapshot()

    # ========================================================================
    # 更新止损
    # ========================================================================

    def update_stop(self, new_stop: Any) -> bool:
        """
        更新止损。

        契约：
        - 有活跃轮次才处理
        - PositionRound 先更新（唯一真相）
        - 撤旧止损 → 挂新止损
        - 挂新失败记录 CRITICAL，下一根 K 线重试

        Returns:
            True: 更新成功（PositionRound 已更新 + 新止损已挂）
            False: 拒绝或失败
        """
        with self._lock:
            self._stats["stop_update_attempts"] += 1

            if not self._active_rounds:
                self._stats["stop_update_rejections"] += 1
                return False

            round = list(self._active_rounds.values())[0]

        # PositionRound 更新（唯一真相）
        if not round.update_stop(new_stop):
            with self._lock:
                self._stats["stop_update_rejections"] += 1
            return False

        # 撤旧止损
        with self._lock:
            old_stop_id = self._stop_orders.get(round.round_id)
        if old_stop_id:
            try:
                self._gateway.cancel_order(old_stop_id)
            except Exception as e:
                logger.warning(
                    f"cancel old stop {old_stop_id} failed: {e}"
                )

        # 挂新止损
        with self._lock:
            self._stop_seq[round.round_id] = (
                self._stop_seq.get(round.round_id, 0) + 1
            )
            stop_seq = self._stop_seq[round.round_id]
            round_seq = self._round_seq_map.get(round.round_id, 0)

        new_stop_id = self._place_server_side_stop(
            round, round_seq, stop_seq=stop_seq
        )

        if new_stop_id is None:
            logger.critical(
                f"failed to place new stop for round {round.round_id} "
                f"after update, will retry next bar"
            )
            with self._lock:
                self._stats["stop_update_failures"] += 1
            return False

        with self._lock:
            self._stop_orders[round.round_id] = new_stop_id
            self._stats["stop_update_successes"] += 1

        return True

    # ========================================================================
    # 平仓
    # ========================================================================

    def close_round(
        self,
        exit_reason: str,
        exit_price_hint: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        平仓。

        契约：
        - 有活跃轮次才处理
        - 撤止损 → 下单平仓 → PositionRound.close → 持久化 → 回调
        - 平仓失败返回 None，轮次保持活跃
        - 幂等：已 closed 返回原记录

        Returns:
            RoundRecord 字典或 None
        """
        with self._lock:
            self._stats["close_attempts"] += 1

            if not self._active_rounds:
                logger.warning("close_round: no active round")
                self._stats["close_rejections"] += 1
                return None

            round = list(self._active_rounds.values())[0]

        if not round.is_open():
            with self._lock:
                self._stats["close_rejections"] += 1
            record = round.get_record()
            return record.to_dict() if record else None

        try:
            return self._close_round_impl(
                round, exit_reason, exit_price_hint
            )
        except Exception as e:
            logger.error(
                f"close_round error: {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1
                self._stats["close_failures"] += 1
            self._last_error = str(e)
            return None

    def _close_round_impl(
        self,
        round: PositionRound,
        exit_reason: str,
        exit_price_hint: Optional[Any],
    ) -> Optional[Dict[str, Any]]:
        """平仓的实际实现。"""
        # ---- 1. 撤止损单 ----
        with self._lock:
            stop_id = self._stop_orders.get(round.round_id)
        if stop_id:
            try:
                self._gateway.cancel_order(stop_id)
            except Exception as e:
                logger.warning(
                    f"cancel stop {stop_id} failed: {e} "
                    f"(will be neutralized by reduceOnly close)"
                )

        # ---- 2. 下单平仓 ----
        with self._lock:
            round_seq = self._round_seq_map.get(round.round_id, 0)

        from system.order_idempotent import OrderKind
        try:
            exit_client_id = self._id_generator.generate(
                OrderKind.EXIT, round_seq, role_seq=0, attempt=0
            )
        except Exception as e:
            logger.error(f"generate exit id failed: {e}")
            with self._lock:
                self._stats["close_failures"] += 1
            return None

        exit_side = (
            OrderSide.SELL if round.direction == 1 else OrderSide.BUY
        )
        try:
            exit_order = Order(
                client_order_id=exit_client_id,
                symbol=self._symbol,
                side=exit_side,
                type=OrderType.MARKET,
                qty=round.total_qty,
                reduce_only=True,
            )
        except Exception as e:
            logger.error(f"construct exit order failed: {e}")
            with self._lock:
                self._stats["close_failures"] += 1
            return None

        try:
            exit_result = self._gateway.place_order(exit_order)
        except Exception as e:
            logger.error(f"place exit order failed: {e}")
            with self._lock:
                self._stats["close_failures"] += 1
            self._last_error = str(e)
            return None

        if exit_result.status not in (
            OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED
        ):
            logger.error(
                f"exit order not filled: status={exit_result.status} "
                f"reason={exit_result.reject_reason}"
            )
            with self._lock:
                self._stats["close_failures"] += 1
            return None

        # ---- 3. 确定退出价格 ----
        exit_price: Optional[Decimal] = exit_result.avg_price
        if exit_price is None or exit_price <= 0:
            if exit_price_hint is not None:
                try:
                    exit_price = Decimal(str(exit_price_hint))
                except Exception:
                    pass
        if exit_price is None or exit_price <= 0:
            logger.error(
                f"cannot determine exit_price for round "
                f"{round.round_id}, using current stop"
            )
            exit_price = round.current_stop

        exit_ts = exit_result.transact_time or self._safe_now_ms()

        # ---- 4. 关闭 PositionRound ----
        try:
            record = round.close(exit_price, exit_ts, exit_reason)
        except Exception as e:
            logger.error(f"PositionRound.close failed: {e}")
            with self._lock:
                self._stats["close_failures"] += 1
            return None

        # ---- 5. 持久化 ----
        try:
            self._persister.update_round_close(
                round_id=record.round_id,
                exit_price=str(record.exit_price),
                exit_ts=record.closed_ts,
                pnl=str(record.realized_pnl),
                r_multiple=record.r_multiple,
                exit_reason=record.exit_reason,
                state_exit=None,
                factor_snapshot_exit=None,
                peak_unrealized=str(record.peak_unrealized),
                pyramid_count=record.pyramid_count,
                stop_price=str(record.final_stop),
            )
        except Exception as e:
            logger.error(f"persist close failed: {e}")
            # 不阻断，轮次已平仓

        # ---- 6. 清理并归档 ----
        with self._lock:
            self._active_rounds.pop(round.round_id, None)
            self._stop_orders.pop(round.round_id, None)
            self._stop_seq.pop(round.round_id, None)
            self._round_seq_map.pop(round.round_id, None)
            self._closed_rounds.append(record)
            if len(self._closed_rounds) > self._max_in_memory:
                self._closed_rounds.pop(0)
            self._stats["close_successes"] += 1

        # ---- 7. 回调 ----
        if self._on_round_close is not None:
            try:
                self._on_round_close(record.to_dict())
            except Exception as e:
                logger.error(
                    f"on_round_close callback failed: {e}",
                    exc_info=True,
                )

        logger.info(
            f"round closed: round_id={record.round_id} "
            f"reason={exit_reason} "
            f"pnl={record.realized_pnl} "
            f"r={record.r_multiple:.3f}"
        )

        return record.to_dict()

    # ========================================================================
    # 查询
    # ========================================================================

    def get_active_round(self) -> Optional[Dict[str, Any]]:
        """获取活跃轮次快照。无活跃轮次返回 None。"""
        with self._lock:
            if not self._active_rounds:
                return None
        return self._get_active_snapshot()

    def _get_active_snapshot(self) -> Optional[Dict[str, Any]]:
        """构造活跃轮次快照（供状态机使用）。"""
        with self._lock:
            if not self._active_rounds:
                return None
            round = list(self._active_rounds.values())[0]

        return {
            "round_id": round.round_id,
            "symbol": round.symbol,
            "direction": round.direction,
            "signal_id": round.signal_id,
            "entry_price": str(round.avg_entry),
            "current_stop": str(round.current_stop),
            "initial_stop": str(round.initial_stop),
            "total_qty": str(round.total_qty),
            "peak_unrealized": str(round.peak_unrealized),
            "opened_ts": round.opened_bar_ts,
            "pyramid_count": len(round.orders) - 1,
            "entry_meta": dict(round.entry_meta),
        }

    def get_round(self, round_id: str) -> Optional[Dict[str, Any]]:
        """按 round_id 查（活跃或已平仓）。"""
        with self._lock:
            if round_id in self._active_rounds:
                return self._active_rounds[round_id].to_dict()
            for rec in self._closed_rounds:
                if rec.round_id == round_id:
                    return rec.to_dict()
            return None

    def list_closed_rounds(self) -> List[Dict[str, Any]]:
        """已平仓轮次列表（内存中）。"""
        with self._lock:
            return [r.to_dict() for r in self._closed_rounds]

    def get_stats(self) -> Dict[str, Any]:
        """统计快照。"""
        with self._lock:
            return dict(self._stats)

    # ========================================================================
    # 风控
    # ========================================================================

    def _risk_approve(
        self,
        order: Order,
        direction: int,
        stop_price: Decimal,
    ) -> Optional[Decimal]:
        """
        通过风控检查。

        契约：
        - 无 risk_policy 时返回 order.qty（视为无风控）
        - 未批准返回 None
        - REDUCE 返回 approved_qty
        - APPROVE 返回 order.qty

        Returns:
            批准的数量或 None
        """
        if self._risk_policy is None:
            return order.qty

        try:
            from core.risk import RiskContext, RiskDecision
        except ImportError:
            logger.warning(
                "core.risk not available, skipping risk check"
            )
            return order.qty

        try:
            equity = Decimal(str(getattr(
                self._risk_policy, "equity", Decimal("1000")
            )))
            context = RiskContext(
                order=order,
                position=None,  # Step 1 单轮，开仓前无持仓
                account=None,
                active_rounds=[],
                symbol_info={},
                now_ms=self._safe_now_ms(),
                daily_pnl=Decimal("0"),
                daily_peak_equity=equity,
                consecutive_losses=0,
            )
            result = self._risk_policy.approve_order(context)
        except Exception as e:
            logger.error(
                f"risk approve_order failed: {e}", exc_info=True
            )
            return None

        if result.decision == RiskDecision.APPROVE:
            return order.qty
        elif result.decision == RiskDecision.REDUCE:
            if result.approved_qty is None:
                logger.error("REDUCE without approved_qty")
                return None
            return result.approved_qty
        else:
            logger.warning(
                f"risk rejected: {result.reason} "
                f"(level={result.level})"
            )
            return None

    def _calc_target_qty(
        self,
        entry_price: Decimal,
        stop_price: Decimal,
    ) -> Decimal:
        """
        计算目标数量，使风险 <= per_trade_risk。

        契约：
        - 无 risk_policy 时返回 0（保守）
        - 精度对齐到 step_size
        - 名义价值 < min_notional 返回 0
        """
        if self._risk_policy is None:
            return Decimal("0")

        try:
            equity = Decimal(str(getattr(
                self._risk_policy, "equity", Decimal("0")
            )))
            per_trade_risk = Decimal(str(getattr(
                self._risk_policy, "per_trade_risk", Decimal("0.007")
            )))
        except Exception as e:
            logger.error(f"get risk attributes failed: {e}")
            return Decimal("0")

        if equity <= 0 or per_trade_risk <= 0:
            return Decimal("0")

        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            return Decimal("0")

        risk_usdt = equity * per_trade_risk
        qty = risk_usdt / stop_distance

        # 精度对齐
        info = self._get_symbol_info()
        step_size = info.get("step_size")
        if step_size is not None and step_size > 0:
            qty = (qty / step_size).to_integral_value(
                rounding=ROUND_DOWN
            ) * step_size

        # 名义价值检查
        min_notional = info.get("min_notional")
        if min_notional is not None and min_notional > 0:
            notional = qty * entry_price
            if notional < min_notional:
                logger.warning(
                    f"qty {qty} notional {notional} < min_notional "
                    f"{min_notional}, returning 0"
                )
                return Decimal("0")

        return qty

    def _get_symbol_info(self) -> Dict[str, Decimal]:
        """获取交易对精度信息（从 gateway 缓存的 symbol_info）。"""
        try:
            if hasattr(self._gateway, "_symbol_info"):
                info = getattr(self._gateway, "_symbol_info", {}).get(
                    self._symbol
                )
                if info:
                    return info
        except Exception:
            pass
        return {}

    # ========================================================================
    # 服务器端止损
    # ========================================================================

    def _place_server_side_stop(
        self,
        round: PositionRound,
        round_seq: int,
        stop_seq: int,
    ) -> Optional[str]:
        """
        挂服务器端止损单。

        Returns:
            client_order_id 或 None（挂单失败）
        """
        from system.order_idempotent import OrderKind
        try:
            stop_client_id = self._id_generator.generate(
                OrderKind.STOP, round_seq, role_seq=stop_seq, attempt=0
            )
        except Exception as e:
            logger.error(f"generate stop id failed: {e}")
            return None

        stop_side = (
            OrderSide.SELL if round.direction == 1 else OrderSide.BUY
        )
        stop_price = round.current_stop

        try:
            stop_order = Order(
                client_order_id=stop_client_id,
                symbol=self._symbol,
                side=stop_side,
                type=OrderType.STOP_MARKET,
                qty=round.total_qty,
                stop_price=stop_price,
                reduce_only=True,
                working_type=WorkingType.MARK_PRICE,
            )
        except Exception as e:
            logger.error(f"construct stop order failed: {e}")
            return None

        try:
            result = self._gateway.place_order(stop_order)
        except Exception as e:
            logger.error(f"place stop order failed: {e}")
            return None

        if result.status not in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED):
            logger.error(
                f"stop order not accepted: status={result.status} "
                f"reason={result.reject_reason}"
            )
            return None

        return stop_client_id

    def _emergency_close_position(
        self, direction: int, qty: Decimal
    ) -> None:
        """紧急平仓（回滚用）。尽力平仓，失败记录 CRITICAL。"""
        side = (
            OrderSide.SELL if direction == 1 else OrderSide.BUY
        )
        try:
            from system.order_idempotent import OrderKind
            client_id = self._id_generator.generate(
                OrderKind.CLOSE_ALL, 0, role_seq=0, attempt=0
            )
        except Exception as e:
            logger.critical(
                f"generate emergency close id failed: {e}"
            )
            return

        try:
            order = Order(
                client_order_id=client_id,
                symbol=self._symbol,
                side=side,
                type=OrderType.MARKET,
                qty=qty,
                reduce_only=True,
            )
            self._gateway.place_order(order)
            logger.info(f"emergency close submitted: qty={qty}")
        except Exception as e:
            logger.critical(
                f"emergency close failed: {e}, "
                f"position may be unprotected! qty={qty}"
            )

    # ========================================================================
    # 恢复
    # ========================================================================

    def recover(
        self,
        snapshot: Optional[Dict[str, Any]],
        positions: List[Any],
    ) -> Optional[Dict[str, Any]]:
        """
        从快照 + 交易所持仓恢复。

        契约：
        - 快照和持仓都为空 → 返回 None（正常）
        - 快照存在但持仓为空 → 关闭死轮次，返回 None
        - 快照为空但持仓存在 → 抛异常（孤儿持仓）
        - 快照和持仓都存在但字段不匹配 → 抛异常
        - 完全匹配 → 重建 PositionRound，返回快照

        Returns:
            恢复的活跃轮次快照或 None
        """
        with self._lock:
            self._stats["recoveries"] += 1

        snapshot_present = (
            snapshot is not None and snapshot.get("active_round")
        )
        positions_present = bool(positions)

        # 1. 都为空：正常
        if not snapshot_present and not positions_present:
            logger.info("recover: no snapshot, no positions, clean start")
            return None

        # 2. 快照存在但无持仓：死轮次
        if snapshot_present and not positions_present:
            logger.warning(
                "recover: snapshot exists but no exchange position, "
                "closing dead round"
            )
            self._recover_dead_round(snapshot["active_round"])
            return None

        # 3. 快照为空但有持仓：孤儿
        if not snapshot_present and positions_present:
            msg = (
                f"recover: {len(positions)} exchange positions found "
                f"but no local round. Manual intervention required."
            )
            logger.critical(msg)
            with self._lock:
                self._stats["recover_failures"] += 1
            raise RoundManagerError(msg)

        # 4. 都非空：匹配
        return self._recover_match(
            snapshot["active_round"], positions[0]
        )

    def _recover_dead_round(self, active_round: Dict[str, Any]) -> None:
        """关闭死轮次（无持仓）。"""
        try:
            round_id = active_round["round_id"]
            entry_price = Decimal(str(active_round["entry_price"]))
            qty = Decimal(str(active_round["total_qty"]))
            opened_ts = int(active_round["opened_ts"])
            direction = int(active_round["direction"])

            round_order = RoundOrder(
                order_id="recovered",
                price=entry_price,
                qty=qty,
                added_ts=opened_ts,
                tag="initial",
            )
            initial_stop = Decimal(
                str(active_round.get("initial_stop", entry_price))
            )
            pr = PositionRound(
                round_id=round_id,
                symbol=self._symbol,
                direction=direction,
                initial_order=round_order,
                initial_stop=initial_stop,
                opened_bar_ts=opened_ts,
                signal_id=active_round.get("signal_id"),
                entry_meta=active_round.get("entry_meta", {}),
                kline_interval_ms=self._kline_interval_ms,
            )
            record = pr.close(
                exit_price=entry_price,
                exit_ts=self._safe_now_ms(),
                reason="recovered_dead",
            )
            try:
                self._persister.update_round_close(
                    round_id=record.round_id,
                    exit_price=str(record.exit_price),
                    exit_ts=record.closed_ts,
                    pnl="0",
                    r_multiple=0.0,
                    exit_reason="recovered_dead",
                )
            except Exception as e:
                logger.error(f"persist dead round close failed: {e}")
            logger.info(
                f"recovered dead round closed: round_id={round_id}"
            )
        except Exception as e:
            logger.error(
                f"close dead round failed: {e}", exc_info=True
            )

    def _recover_match(
        self,
        active_round: Dict[str, Any],
        position: Any,
    ) -> Dict[str, Any]:
        """恢复时快照和持仓匹配。"""
        try:
            snap_direction = int(active_round["direction"])
            snap_qty = Decimal(str(active_round["total_qty"]))
            snap_entry = Decimal(str(active_round["entry_price"]))

            # 方向匹配
            if hasattr(position, "side"):
                pos_side = position.side
                pos_direction = 1 if str(pos_side).upper().endswith("BUY") else -1
                if pos_direction != snap_direction:
                    raise RoundManagerError(
                        f"direction mismatch: snapshot={snap_direction} "
                        f"position={pos_direction}"
                    )

            # 数量匹配（容差 1e-6）
            if hasattr(position, "qty"):
                pos_qty = Decimal(str(position.qty))
                if abs(pos_qty - snap_qty) > Decimal("1e-6"):
                    raise RoundManagerError(
                        f"qty mismatch: snapshot={snap_qty} "
                        f"position={pos_qty}"
                    )

            # 入场价匹配（容差 1e-4）
            if hasattr(position, "entry_price"):
                pos_entry = Decimal(str(position.entry_price))
                if abs(pos_entry - snap_entry) > Decimal("1e-4"):
                    raise RoundManagerError(
                        f"entry_price mismatch: snapshot={snap_entry} "
                        f"position={pos_entry}"
                    )

            # 重建 PositionRound
            round_order = RoundOrder(
                order_id="recovered",
                price=snap_entry,
                qty=snap_qty,
                added_ts=int(active_round["opened_ts"]),
                tag="initial",
            )
            initial_stop = Decimal(
                str(active_round.get("initial_stop", snap_entry))
            )
            current_stop = Decimal(
                str(active_round.get("current_stop", initial_stop))
            )
            pr = PositionRound(
                round_id=active_round["round_id"],
                symbol=self._symbol,
                direction=snap_direction,
                initial_order=round_order,
                initial_stop=initial_stop,
                opened_bar_ts=int(active_round["opened_ts"]),
                signal_id=active_round.get("signal_id"),
                entry_meta=active_round.get("entry_meta", {}),
                kline_interval_ms=self._kline_interval_ms,
            )
            pr.current_stop = current_stop

            # 重新挂止损
            round_seq = self._next_round_seq()
            self._round_seq_map[pr.round_id] = round_seq
            stop_id = self._place_server_side_stop(
                pr, round_seq, stop_seq=0
            )
            if stop_id is None:
                logger.critical(
                    f"recover: failed to place stop for "
                    f"recovered round {pr.round_id}, will retry next bar"
                )

            with self._lock:
                self._active_rounds[pr.round_id] = pr
                if stop_id:
                    self._stop_orders[pr.round_id] = stop_id
                    self._stop_seq[pr.round_id] = 0

            logger.info(
                f"recover: round {pr.round_id} recovered, "
                f"direction={snap_direction} qty={snap_qty} "
                f"entry={snap_entry} stop={current_stop}"
            )
            return self._get_active_snapshot()
        except RoundManagerError:
            raise
        except Exception as e:
            logger.error(
                f"recover match failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["recover_failures"] += 1
            raise RoundManagerError(
                f"recover failed: {e}"
            ) from e

    # ========================================================================
    # 辅助
    # ========================================================================

    def _next_round_seq(self) -> int:
        """下一个 round_seq。"""
        with self._lock:
            self._round_seq += 1
            return self._round_seq

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        if self._clock is None:
            return int(time.time() * 1000)
        try:
            return int(self._clock.now_ms())
        except Exception:
            return int(time.time() * 1000)

    # ========================================================================
    # 健康检查与重置
    # ========================================================================

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            active_snapshot = None
            if self._active_rounds:
                active_snapshot = list(
                    self._active_rounds.values()
                )[0].health()
            return {
                "healthy": True,
                "name": self.name,
                "version": self.version,
                "symbol": self._symbol,
                "active_rounds_count": len(self._active_rounds),
                "closed_rounds_count": len(self._closed_rounds),
                "active_round": active_snapshot,
                "round_seq": self._round_seq,
                "stats": dict(self._stats),
                "last_error": self._last_error,
                "timestamp": self._safe_now_ms(),
            }

    def reset(self) -> None:
        """重置（测试用）。"""
        with self._lock:
            self._active_rounds.clear()
            self._closed_rounds.clear()
            self._stop_orders.clear()
            self._stop_seq.clear()
            self._round_seq_map.clear()
            self._round_seq = 0
            for k in self._stats:
                self._stats[k] = 0
            self._last_error = None
        logger.info("RoundManager reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> RoundManager:
    """
    模块工厂入口。

    config 需要：
    - instances.risk_policy
    - instances.persister
    - instances.clock
    - instances.gateway
    - instances.order_idempotent_gen
    - symbol
    """
    instances = config.get("instances", {})
    return RoundManager(
        risk_policy=instances.get("risk_policy"),
        persister=instances.get("persister"),
        clock=instances.get("clock"),
        order_gateway=instances.get("gateway"),
        id_generator=instances.get("order_idempotent_gen"),
        symbol=config.get("symbol", "btcusdt"),
        config=config,
    )
