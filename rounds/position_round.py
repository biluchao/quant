"""
rounds/position_round.py — 单轮持仓账本 (重铸标准版)

核心设计原则：
1. 账本绝对清晰：强类型 Decimal 运算，杜绝浮点误差。
2. 平仓幂等：多次平仓返回同一记录，绝不重复结算。
3. 风险不变量：initial_risk (R0) 一经确定，终生不变。
4. 严苛校验：止损方向、加仓均价、峰值追踪全部标准化。
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import time

logger = logging.getLogger(__name__)

class RoundStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"

class PositionRoundError(Exception):
    """账本逻辑错误"""
    pass

@dataclass(frozen=True)
class RoundOrder:
    """单笔加仓订单明细"""
    order_id: str
    price: Decimal
    qty: Decimal
    added_ts: int
    tag: str = "initial"

@dataclass(frozen=True)
class RoundRecord:
    """平仓后的最终不可变记录，供归因分析"""
    round_id: str
    symbol: str
    direction: int
    signal_id: Optional[str]
    
    entry_price: Decimal
    exit_price: Decimal
    total_qty: Decimal
    initial_stop: Decimal
    final_stop: Decimal
    
    realized_pnl: Decimal
    r_multiple: float
    peak_close: Decimal
    peak_unrealized: Decimal
    peak_r: float
    
    opened_ts: int
    closed_ts: int
    duration_ms: int
    bars_held: int
    
    exit_reason: str
    pyramid_count: int
    entry_meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "round_id": self.round_id, "symbol": self.symbol, "direction": self.direction,
            "signal_id": self.signal_id, "entry_price": str(self.entry_price),
            "exit_price": str(self.exit_price), "total_qty": str(self.total_qty),
            "initial_stop": str(self.initial_stop), "final_stop": str(self.final_stop),
            "realized_pnl": str(self.realized_pnl), "r_multiple": self.r_multiple,
            "peak_close": str(self.peak_close), "peak_unrealized": str(self.peak_unrealized),
            "peak_r": self.peak_r, "opened_ts": self.opened_ts, "closed_ts": self.closed_ts,
            "duration_ms": self.duration_ms, "bars_held": self.bars_held,
            "exit_reason": self.exit_reason, "pyramid_count": self.pyramid_count,
            "entry_meta": self.entry_meta
        }


class PositionRound:
    """
    单轮持仓管理，从开仓到平仓的完整账本。
    """

    def __init__(
        self,
        round_id: str,
        symbol: str,
        direction: int,
        initial_order: RoundOrder,
        initial_stop: Decimal,
        opened_bar_ts: int,
        signal_id: Optional[str] = None,
        entry_meta: Optional[Dict[str, Any]] = None,
        kline_interval_ms: int = 180000,
    ):
        # 强校验
        if direction not in (1, -1):
            raise PositionRoundError(f"非法方向: {direction}")
        if initial_order.qty <= 0 or initial_order.price <= 0 or initial_stop <= 0:
            raise PositionRoundError("价格、数量、止损必须大于0")
        if direction == 1 and initial_stop >= initial_order.price:
            raise PositionRoundError(f"多单止损 {initial_stop} 必须低于入场 {initial_order.price}")
        if direction == -1 and initial_stop <= initial_order.price:
            raise PositionRoundError(f"空单止损 {initial_stop} 必须高于入场 {initial_order.price}")

        # 基础信息
        self.round_id = round_id
        self.symbol = symbol
        self.direction = direction
        self.signal_id = signal_id
        self.entry_meta = dict(entry_meta or {})
        self.kline_interval_ms = kline_interval_ms
        self.opened_bar_ts = int(opened_bar_ts)

        # 账本字段
        self.orders: List[RoundOrder] = [initial_order]
        self.total_qty = initial_order.qty
        self.avg_entry = initial_order.price
        self.initial_stop = initial_stop
        self.current_stop = initial_stop
        
        # 初始风险 R0 = |入场价 - 止损| * 数量。此生不变。
        self.initial_risk = abs(initial_order.price - initial_stop) * initial_order.qty

        # 追踪字段
        self.peak_close = initial_order.price
        self.peak_unrealized = Decimal("0")

        # 状态控制
        self.status = RoundStatus.OPEN
        self._record: Optional[RoundRecord] = None

        logger.info(f"开仓账本建立: {round_id} | 方向: {direction} | 均价: {self.avg_entry} | R0: {self.initial_risk}")

    # ========================================================================
    # 止损管理
    # ========================================================================

    def update_stop(self, new_stop: Decimal) -> bool:
        """
        更新止损（单向铁律）。
        返回 True 表示成功更新。
        """
        if self.status != RoundStatus.OPEN:
            return False
        if new_stop <= 0:
            return False

        if self.direction == 1:
            # 多单只能上移
            if new_stop > self.current_stop:
                logger.info(f"账本止损上移: {self.current_stop} -> {new_stop}")
                self.current_stop = new_stop
                return True
        else:
            # 空单只能下移
            if new_stop < self.current_stop:
                logger.info(f"账本止损下移: {self.current_stop} -> {new_stop}")
                self.current_stop = new_stop
                return True

        return False

    def check_stop(self, bar: Any) -> bool:
        """硬止损检查（看 high/low）"""
        if self.status != RoundStatus.OPEN:
            return False
        
        if self.direction == 1:
            return Decimal(str(bar.low)) <= self.current_stop
        else:
            return Decimal(str(bar.high)) >= self.current_stop

    # ========================================================================
    # 峰值追踪
    # ========================================================================

    def update_peak(self, current_close: Decimal) -> None:
        """更新峰值浮盈（只追踪正向）"""
        if self.status != RoundStatus.OPEN:
            return

        # 更新峰值价格
        if self.direction == 1:
            if current_close > self.peak_close:
                self.peak_close = current_close
        else:
            if current_close < self.peak_close:
                self.peak_close = current_close

        # 更新峰值浮盈
        unrealized = (current_close - self.avg_entry) * self.direction * self.total_qty
        if unrealized > self.peak_unrealized:
            self.peak_unrealized = unrealized

    # ========================================================================
    # 加仓
    # ========================================================================

    def add_order(self, order: RoundOrder, new_stop: Optional[Decimal] = None) -> bool:
        """
        金字塔加仓。
        注意：initial_risk 绝不变！必须通过上移止损来控制总风险。
        """
        if self.status != RoundStatus.OPEN:
            return False
        if order.qty <= 0 or order.price <= 0:
            return False

        self.orders.append(order)
        self.total_qty += order.qty
        
        # 重新计算加权均价
        total_notional = sum((o.price * o.qty for o in self.orders), Decimal("0"))
        self.avg_entry = total_notional / self.total_qty

        logger.info(f"账本加仓: {order.qty} @ {order.price} | 新均价: {self.avg_entry} | 总数量: {self.total_qty}")

        if new_stop:
            self.update_stop(new_stop)

        return True

    # ========================================================================
    # 平仓（幂等）
    # ========================================================================

    def close(self, exit_price: Decimal, exit_ts: int, reason: str) -> RoundRecord:
        """
        平仓结算（绝对幂等）。
        """
        if self.status == RoundStatus.CLOSED and self._record is not None:
            logger.warning(f"账本 {self.round_id} 已平仓，直接返回原记录。")
            return self._record

        if exit_price <= 0 or exit_ts <= 0:
            raise PositionRoundError("平仓价格或时间戳非法")

        self.status = RoundStatus.CLOSED
        
        # 计算已实现盈亏
        realized_pnl = (exit_price - self.avg_entry) * self.direction * self.total_qty
        
        # 计算时间跨度
        duration_ms = max(0, exit_ts - self.opened_bar_ts)
        bars_held = duration_ms // self.kline_interval_ms

        # 计算 R 倍数
        r_multiple = float(realized_pnl / self.initial_risk) if self.initial_risk > 0 else 0.0
        peak_r = float(self.peak_unrealized / self.initial_risk) if self.initial_risk > 0 else 0.0

        self._record = RoundRecord(
            round_id=self.round_id,
            symbol=self.symbol,
            direction=self.direction,
            signal_id=self.signal_id,
            entry_price=self.avg_entry,
            exit_price=exit_price,
            total_qty=self.total_qty,
            initial_stop=self.initial_stop,
            final_stop=self.current_stop,
            realized_pnl=realized_pnl,
            r_multiple=r_multiple,
            peak_close=self.peak_close,
            peak_unrealized=self.peak_unrealized,
            peak_r=peak_r,
            opened_ts=self.opened_bar_ts,
            closed_ts=int(exit_ts),
            duration_ms=duration_ms,
            bars_held=bars_held,
            exit_reason=reason,
            pyramid_count=len(self.orders) - 1,
            entry_meta=self.entry_meta
        )

        logger.info(f"账本平仓: {self.round_id} | 原因: {reason} | 盈亏: {realized_pnl} | R: {r_multiple:.3f}")
        return self._record

    def is_open(self) -> bool:
        return self.status == RoundStatus.OPEN

    def get_record(self) -> Optional[RoundRecord]:
        return self._record
