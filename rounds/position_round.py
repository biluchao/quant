"""
rounds/position_round.py — 单轮持仓管理（记账 + 止损状态）

一轮（Round）是一笔交易的完整生命周期，从开仓到平仓。

职责（唯一）：
1. 记录一轮的所有字段（方向、入场价、数量、止损、峰值等）
2. 检查止损是否触发（check_stop）
3. 更新止损（update_stop，只朝有利方向）
4. 更新峰值追踪（update_peak）
5. 计算未实现盈亏和 R 倍数（unrealized_pnl / r_multiple）
6. 平仓并返回不可变的 RoundRecord（close，幂等）

不做什么：
- 不创建订单（RoundManager 负责）
- 不执行平仓（RoundManager 负责）
- 不持有 gateway / risk_policy 引用
- 不感知策略、状态机、因子
- 不持久化（RoundManager 负责调 persister）

职责边界：
- PositionRound：单轮记账 + 止损状态
- RoundManager：创建 PositionRound、执行订单、持有 closed_rounds 历史
- state_machine：触发 open/close/update_stop
- persister：落库

关键契约：
1. 止损只能朝有利方向移动（多单只上移，空单只下移）
2. 平仓幂等（重复调用返回相同记录）
3. 所有价格/数量用 Decimal
4. 所有时间戳毫秒 int
5. `initial_risk` 在轮次生命周期内不变（加仓时也不重算，用于约束 Risk_round <= Risk_0）
6. `avg_entry` 在加仓时重算
7. `peak_close` 按趋势方向追踪（多单最高 close，空单最低 close）
8. `peak_unrealized` 只追踪正向浮盈（永远 >= 0）
9. `exit_reason` 用字符串，与上游统一（stop / top_escape / timeout / risk_halt / emergency）
10. `bars_held` 按 `kline_interval_ms` 计算，floor 取整

为什么 initial_risk 不随加仓重算：
- R0 是风控批准这一轮时的风险预算
- 加仓时风险约束是 Risk_round <= Risk_0（数学约束）
- 如果重算 initial_risk，R 倍数会失真，风控失效
- 具体见 PARAM_TABLE.md 和 FACTOR_SPEC.md 的倒金字塔分析

为什么平仓要幂等：
- state_machine 可能在异常路径重复调 close_round
- round_manager 可能在重试时重复平仓
- 平仓不可逆，重复执行会导致 pnl 翻倍
- 幂等设计：第一次执行结算，后续调用返回相同记录

与 state_trend 的协作：
- state_trend._check_stop(bar) 判断止损是否触发（用 bar.low/high）
- state_trend 返回 EXIT(target="idle") transition
- state_machine 处理 EXIT，调 round_manager.close_round()
- round_manager 调 PositionRound.close(exit_price, ...)

与 RoundManager 的协作（Step 3 加仓）：
- round_manager 调 PositionRound.add_order(order, new_stop)
- 内部重算 avg_entry、total_qty，不重算 initial_risk
- add_order 后必须调 update_stop 保证 Risk_round <= Risk_0
- Step 1 不使用 add_order

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import time

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "position_round",
    "type": "round",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["position_round", "round_accounting"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Single round position accounting with directional stop constraint and idempotent close",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000


# ============================================================================
# 错误
# ============================================================================
class PositionRoundError(Exception):
    """PositionRound 错误。"""
    pass


# ============================================================================
# 状态
# ============================================================================
class RoundStatus(str, Enum):
    """轮次状态。"""
    OPEN = "open"
    CLOSED = "closed"


# ============================================================================
# 订单（一轮内的一个订单）
# ============================================================================
@dataclass(frozen=True)
class RoundOrder:
    """
    一轮内的一个订单。

    Step 1：每轮只有一个订单（首仓）。
    Step 3：可能有多笔（金字塔加仓），tag 区分。
    """
    order_id: str        # client_order_id（本地确定性 ID）
    price: Decimal       # 成交价
    qty: Decimal         # 数量
    added_ts: int        # 添加时间（毫秒，bar.ts）
    tag: str = "initial"  # "initial" / "pyramid_1" / "pyramid_2" / "pyramid_3"


# ============================================================================
# 轮次记录（已平仓的不可变记录）
# ============================================================================
@dataclass(frozen=True)
class RoundRecord:
    """
    已平仓轮次的完整记录。

    用于：
    1. 持久化到 persister.rounds 表
    2. 归因分析（analytics/round_attribution）
    3. 参数调整时的反事实分析
    """
    # ---- 标识 ----
    round_id: str
    symbol: str
    direction: int                    # 1=LONG, -1=SHORT
    signal_id: Optional[str]

    # ---- 价格与数量 ----
    entry_price: Decimal              # 加权平均入场价
    exit_price: Decimal
    total_qty: Decimal
    initial_stop: Decimal
    final_stop: Decimal               # 最终止损（可能被移动止损更新过）

    # ---- 盈亏 ----
    realized_pnl: Decimal             # 已实现毛盈亏（不含手续费）
    r_multiple: float                 # realized_pnl / initial_risk
    peak_close: Decimal               # 峰值 close（多单最高，空单最低）
    peak_unrealized: Decimal          # 峰值浮盈（永远 >= 0）
    peak_r: float                     # peak_unrealized / initial_risk

    # ---- 时间 ----
    opened_ts: int                    # bar.ts
    closed_ts: int                    # bar.ts
    duration_ms: int
    bars_held: int                    # duration_ms // kline_interval_ms

    # ---- 元数据 ----
    exit_reason: str                  # "stop" / "top_escape" / "timeout" / "risk_halt" / "emergency"
    pyramid_count: int                # Step 1 恒为 0
    entry_meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """序列化。Decimal → str。"""
        return {
            "round_id": self.round_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "signal_id": self.signal_id,
            "entry_price": str(self.entry_price),
            "exit_price": str(self.exit_price),
            "total_qty": str(self.total_qty),
            "initial_stop": str(self.initial_stop),
            "final_stop": str(self.final_stop),
            "realized_pnl": str(self.realized_pnl),
            "r_multiple": self.r_multiple,
            "peak_close": str(self.peak_close),
            "peak_unrealized": str(self.peak_unrealized),
            "peak_r": self.peak_r,
            "opened_ts": self.opened_ts,
            "closed_ts": self.closed_ts,
            "duration_ms": self.duration_ms,
            "bars_held": self.bars_held,
            "exit_reason": self.exit_reason,
            "pyramid_count": self.pyramid_count,
            "entry_meta": dict(self.entry_meta),
        }


# ============================================================================
# PositionRound
# ============================================================================
class PositionRound:
    """
    单轮持仓管理。详见模块 docstring。

    用法：
        order = RoundOrder(
            order_id="bm-ent-0001-00-00",
            price=Decimal("67000.5"),
            qty=Decimal("0.001"),
            added_ts=1700000000000,
            tag="initial",
        )
        round = PositionRound(
            round_id="abc123",
            symbol="BTCUSDT",
            direction=1,
            initial_order=order,
            initial_stop=Decimal("66800.0"),
            opened_bar_ts=1700000000000,
            signal_id="sig-20240101-0001",
            entry_meta={"factor_snapshot": {...}},
        )

        # 每根 K 线更新
        round.update_peak(bar)
        if round.check_stop(bar):
            record = round.close(bar.close, bar.ts, reason="stop")

        # 移动止损
        if should_update_stop:
            ok = round.update_stop(new_stop)
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
        kline_interval_ms: int = DEFAULT_KLINE_INTERVAL_MS,
    ):
        # ---- 参数校验 ----
        if not round_id:
            raise PositionRoundError("round_id is required")
        if not symbol:
            raise PositionRoundError("symbol is required")
        if direction not in (1, -1):
            raise PositionRoundError(
                f"direction must be 1 or -1, got {direction}"
            )
        if initial_order.qty <= 0:
            raise PositionRoundError(
                f"initial order qty must be > 0, got {initial_order.qty}"
            )
        if initial_order.price <= 0:
            raise PositionRoundError(
                f"initial order price must be > 0, "
                f"got {initial_order.price}"
            )

        try:
            initial_stop_dec = Decimal(str(initial_stop))
        except Exception as e:
            raise PositionRoundError(
                f"invalid initial_stop: {initial_stop}"
            ) from e

        if initial_stop_dec <= 0:
            raise PositionRoundError(
                f"initial_stop must be > 0, got {initial_stop_dec}"
            )

        # 方向性校验：止损位置
        if direction == 1:
            if initial_stop_dec >= initial_order.price:
                raise PositionRoundError(
                    f"LONG initial_stop ({initial_stop_dec}) must be "
                    f"< entry_price ({initial_order.price})"
                )
        else:
            if initial_stop_dec <= initial_order.price:
                raise PositionRoundError(
                    f"SHORT initial_stop ({initial_stop_dec}) must be "
                    f"> entry_price ({initial_order.price})"
                )

        if kline_interval_ms <= 0:
            raise PositionRoundError(
                f"kline_interval_ms must be > 0, "
                f"got {kline_interval_ms}"
            )
        if opened_bar_ts <= 0:
            raise PositionRoundError(
                f"opened_bar_ts must be > 0, got {opened_bar_ts}"
            )

        # ---- 核心字段 ----
        self.round_id = round_id
        self.symbol = symbol
        self.direction = direction
        self.orders: List[RoundOrder] = [initial_order]
        self.total_qty = initial_order.qty
        self.avg_entry = initial_order.price
        self.initial_stop = initial_stop_dec
        self.current_stop = initial_stop_dec
        self.opened_bar_ts = int(opened_bar_ts)
        self.signal_id = signal_id
        self.entry_meta = dict(entry_meta or {})
        self.kline_interval_ms = int(kline_interval_ms)

        # ---- 初始风险（不随加仓变化）----
        self.initial_risk = (
            abs(initial_order.price - initial_stop_dec) * initial_order.qty
        )

        # ---- 追踪 ----
        self.peak_close = initial_order.price
        self.peak_unrealized = Decimal("0")

        # ---- 状态 ----
        self.status = RoundStatus.OPEN
        self._record: Optional[RoundRecord] = None

        # ---- 统计 ----
        self._stats = {
            "stop_updates_accepted": 0,
            "stop_updates_rejected": 0,
            "peak_updates": 0,
        }

        logger.info(
            f"PositionRound opened: "
            f"round_id={round_id} symbol={symbol} "
            f"direction={direction} "
            f"entry={self.avg_entry} qty={self.total_qty} "
            f"initial_stop={self.initial_stop} "
            f"initial_risk={self.initial_risk} "
            f"signal_id={signal_id}"
        )

    # ========================================================================
    # 止损
    # ========================================================================

    def update_stop(self, new_stop: Any) -> bool:
        """
        更新止损。只朝有利方向移动。

        契约：
        - 多单：new_stop > current_stop 才接受（只能上移）
        - 空单：new_stop < current_stop 才接受（只能下移）
        - new_stop <= 0 拒绝
        - 已平仓拒绝
        - 返回是否实际更新

        Returns:
            True: 止损已更新
            False: 拒绝（原因不记录详细，由调用方根据上下文判断）
        """
        if self.status != RoundStatus.OPEN:
            return False

        try:
            new_stop_dec = Decimal(str(new_stop))
        except Exception as e:
            logger.debug(
                f"update_stop rejected: invalid new_stop {new_stop}: {e}"
            )
            self._stats["stop_updates_rejected"] += 1
            return False

        if new_stop_dec <= 0:
            self._stats["stop_updates_rejected"] += 1
            return False

        # 方向性约束
        if self.direction == 1:
            # 多单：只能上移
            if new_stop_dec <= self.current_stop:
                self._stats["stop_updates_rejected"] += 1
                logger.debug(
                    f"update_stop rejected for {self.round_id}: "
                    f"LONG new_stop {new_stop_dec} <= "
                    f"current {self.current_stop}"
                )
                return False
        else:
            # 空单：只能下移
            if new_stop_dec >= self.current_stop:
                self._stats["stop_updates_rejected"] += 1
                logger.debug(
                    f"update_stop rejected for {self.round_id}: "
                    f"SHORT new_stop {new_stop_dec} >= "
                    f"current {self.current_stop}"
                )
                return False

        old_stop = self.current_stop
        self.current_stop = new_stop_dec
        self._stats["stop_updates_accepted"] += 1

        logger.info(
            f"round {self.round_id} stop updated: "
            f"{old_stop} -> {new_stop_dec}"
        )
        return True

    def check_stop(self, bar: Any) -> bool:
        """
        检查止损是否在 bar 内触发。

        契约：
        - 用 bar.low / bar.high（不用 close）
        - 多单：bar.low <= current_stop
        - 空单：bar.high >= current_stop
        - 已平仓返回 False
        - bar 字段缺失返回 False

        Returns:
            True: 止损触发
        """
        if self.status != RoundStatus.OPEN:
            return False

        if self.direction == 1:
            bar_low = getattr(bar, "low", None)
            if bar_low is None:
                return False
            try:
                return Decimal(str(bar_low)) <= self.current_stop
            except Exception:
                return False
        else:
            bar_high = getattr(bar, "high", None)
            if bar_high is None:
                return False
            try:
                return Decimal(str(bar_high)) >= self.current_stop
            except Exception:
                return False

    # ========================================================================
    # 峰值追踪
    # ========================================================================

    def update_peak(self, bar: Any) -> None:
        """
        更新峰值追踪（用 bar.close）。

        契约：
        - 多单：追踪最高 close
        - 空单：追踪最低 close
        - peak_unrealized 只在正向浮盈时更新（永远 >= 0）
        - 已平仓直接返回
        """
        if self.status != RoundStatus.OPEN:
            return

        try:
            close_dec = Decimal(str(bar.close))
        except Exception:
            return

        # 更新 peak_close（按趋势方向）
        if self.direction == 1:
            if close_dec > self.peak_close:
                self.peak_close = close_dec
        else:
            if close_dec < self.peak_close:
                self.peak_close = close_dec

        # 更新 peak_unrealized（只追踪正向）
        unrealized = (close_dec - self.avg_entry) * self.direction * self.total_qty
        if unrealized > self.peak_unrealized:
            self.peak_unrealized = unrealized

        self._stats["peak_updates"] += 1

    # ========================================================================
    # 盈亏
    # ========================================================================

    def unrealized_pnl(self, current_price: Any) -> Decimal:
        """给定当前价格，计算未实现盈亏。"""
        try:
            price_dec = Decimal(str(current_price))
        except Exception:
            return Decimal("0")
        return (
            (price_dec - self.avg_entry)
            * self.direction
            * self.total_qty
        )

    def r_multiple(self, current_price: Any) -> float:
        """给定当前价格，计算 R 倍数。"""
        if self.initial_risk <= 0:
            return 0.0
        return float(self.unrealized_pnl(current_price) / self.initial_risk)

    # ========================================================================
    # 加仓（Step 3）
    # ========================================================================

    def add_order(
        self,
        order: RoundOrder,
        new_stop: Optional[Any] = None,
    ) -> bool:
        """
        添加订单（金字塔加仓）。Step 1 不使用。

        契约：
        - 已平仓拒绝
        - qty <= 0 拒绝
        - 不重算 initial_risk（R0 在轮次生命周期内不变）
        - 重算 avg_entry 和 total_qty
        - 若 new_stop 提供，调 update_stop

        Returns:
            True: 添加成功
            False: 拒绝
        """
        if self.status != RoundStatus.OPEN:
            return False
        if order.qty <= 0 or order.price <= 0:
            return False

        self.orders.append(order)
        self.total_qty += order.qty

        # 重算加权平均入场价
        total_cost = sum(o.price * o.qty for o in self.orders)
        self.avg_entry = total_cost / self.total_qty

        logger.info(
            f"round {self.round_id} order added: "
            f"order_id={order.order_id} price={order.price} "
            f"qty={order.qty} tag={order.tag} "
            f"new_avg_entry={self.avg_entry} "
            f"new_total_qty={self.total_qty} "
            f"initial_risk_unchanged={self.initial_risk}"
        )

        # 更新止损（若有）
        if new_stop is not None:
            self.update_stop(new_stop)

        return True

    # ========================================================================
    # 平仓
    # ========================================================================

    def close(
        self,
        exit_price: Any,
        exit_ts: int,
        reason: str,
    ) -> RoundRecord:
        """
        平仓。幂等。

        契约：
        - 已平仓返回相同记录（幂等）
        - exit_price 必须 > 0
        - 结算 realized_pnl
        - 生成不可变 RoundRecord

        Returns:
            RoundRecord
        """
        # 幂等
        if self.status == RoundStatus.CLOSED and self._record is not None:
            logger.warning(
                f"round {self.round_id} already closed, "
                f"returning existing record"
            )
            return self._record

        try:
            exit_price_dec = Decimal(str(exit_price))
        except Exception as e:
            raise PositionRoundError(
                f"invalid exit_price: {exit_price}"
            ) from e

        if exit_price_dec <= 0:
            raise PositionRoundError(
                f"exit_price must be > 0, got {exit_price_dec}"
            )

        if exit_ts <= 0:
            raise PositionRoundError(
                f"exit_ts must be > 0, got {exit_ts}"
            )

        # 结算
        self.status = RoundStatus.CLOSED
        realized_pnl = (
            (exit_price_dec - self.avg_entry)
            * self.direction
            * self.total_qty
        )

        duration_ms = max(0, int(exit_ts) - self.opened_bar_ts)
        bars_held = duration_ms // self.kline_interval_ms

        r_multiple = (
            float(realized_pnl / self.initial_risk)
            if self.initial_risk > 0 else 0.0
        )
        peak_r = (
            float(self.peak_unrealized / self.initial_risk)
            if self.initial_risk > 0 else 0.0
        )

        self._record = RoundRecord(
            round_id=self.round_id,
            symbol=self.symbol,
            direction=self.direction,
            signal_id=self.signal_id,
            entry_price=self.avg_entry,
            exit_price=exit_price_dec,
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
            entry_meta=dict(self.entry_meta),
        )

        logger.info(
            f"round {self.round_id} closed: "
            f"reason={reason} "
            f"exit_price={exit_price_dec} "
            f"pnl={realized_pnl} "
            f"r_multiple={r_multiple:.3f} "
            f"peak_r={peak_r:.3f} "
            f"bars_held={bars_held}"
        )

        return self._record

    # ========================================================================
    # 状态查询
    # ========================================================================

    def is_open(self) -> bool:
        return self.status == RoundStatus.OPEN

    def is_closed(self) -> bool:
        return self.status == RoundStatus.CLOSED

    def get_record(self) -> Optional[RoundRecord]:
        """获取已平仓记录。未平仓返回 None。"""
        return self._record

    def to_dict(self) -> Dict[str, Any]:
        """
        当前状态快照。用于持久化和查询。

        未平仓：所有字段都是当前值
        已平仓：附带 record 字段
        """
        base = {
            "round_id": self.round_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "status": self.status.value,
            "signal_id": self.signal_id,
            "orders": [
                {
                    "order_id": o.order_id,
                    "price": str(o.price),
                    "qty": str(o.qty),
                    "tag": o.tag,
                    "added_ts": o.added_ts,
                }
                for o in self.orders
            ],
            "total_qty": str(self.total_qty),
            "avg_entry": str(self.avg_entry),
            "initial_stop": str(self.initial_stop),
            "current_stop": str(self.current_stop),
            "initial_risk": str(self.initial_risk),
            "peak_close": str(self.peak_close),
            "peak_unrealized": str(self.peak_unrealized),
            "opened_bar_ts": self.opened_bar_ts,
            "kline_interval_ms": self.kline_interval_ms,
            "entry_meta": dict(self.entry_meta),
            "stats": dict(self._stats),
        }
        if self._record is not None:
            base["record"] = self._record.to_dict()
        return base

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        return {
            "healthy": True,
            "round_id": self.round_id,
            "status": self.status.value,
            "direction": self.direction,
            "avg_entry": str(self.avg_entry),
            "current_stop": str(self.current_stop),
            "initial_risk": str(self.initial_risk),
            "peak_unrealized": str(self.peak_unrealized),
            "orders_count": len(self.orders),
            "bars_since_open_estimate": (
                max(0, int(time.time() * 1000) - self.opened_bar_ts)
                // self.kline_interval_ms
            ),
            "stats": dict(self._stats),
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> None:
    """
    PositionRound 不支持无参数工厂创建。

    它必须从 RoundManager 用完整参数构造（round_id / 订单 / 止损 / 时间戳）。
    本函数仅用于 ModuleRegistry 兼容性，返回 None。
    """
    logger.info(
        "PositionRound is instantiated directly by RoundManager, "
        "not via create()"
    )
    return None
