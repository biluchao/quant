"""
tests/unit/test_round.py — 轮次与止损单元测试（多空对称 + 合约）

范围：
1. PositionRound：构造校验、止损方向性、被动止损触发、峰值追踪、盈亏、平仓幂等
2. RoundManager：开仓流程、风控审批、失败回滚、平仓流程、止损更新
3. 合约相关：side 映射、reduce_only、working_type、精度处理
4. 多空对称：同一参数下，多单和空单的行为严格镜像

设计原则：
- 每个测试独立：用 fixture 隔离，无共享状态
- 断言精确：不写 `assert result`，而是 `assert result.decision == ...`
- 覆盖异常路径：不下单、风控拒、下单失败、止损挂失败
- 多空对称用 parametrize 统一覆盖

Mock 设计：
- FakeGateway：记录调用，可配置成功/失败/成交价
- FakePersister：记录调用，不落盘
- FakeClock：固定时间戳，可控
- FakeIdGenerator：确定性 ID
- FakeRiskPolicy：可配置 APPROVE/REDUCE/REJECT

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

import pytest
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from core.gateway import (
    Order, OrderResult, OrderSide, OrderStatus, OrderType, WorkingType,
)
from core.risk import RiskDecision, RiskLevel, RiskResult
from rounds.position_round import (
    PositionRound, PositionRoundError, RoundOrder, RoundStatus,
)
from rounds.round_manager import RoundManager, RoundManagerError


# ============================================================================
# Fake 组件
# ============================================================================

@dataclass
class FakeBar:
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("1000")
    is_closed: bool = True


class FakeClock:
    def __init__(self, now: int = 1_700_000_000_000):
        self._now = now

    def now_ms(self) -> int:
        return self._now

    def set(self, ts: int) -> None:
        self._now = ts


class FakeIdGenerator:
    def __init__(self):
        self._seq = 0

    def generate(self, kind, round_seq=0, role_seq=0, attempt=0) -> str:
        self._seq += 1
        kind_str = kind.value if hasattr(kind, "value") else str(kind)
        return f"test-{kind_str}-{self._seq:04d}"


class FakePersister:
    def __init__(self):
        self.round_opens: List[Dict[str, Any]] = []
        self.round_closes: List[Dict[str, Any]] = []
        self.transitions: List[Dict[str, Any]] = []

    def insert_round_open(self, data: Dict[str, Any]) -> None:
        self.round_opens.append(data)

    def update_round_close(self, **kwargs: Any) -> None:
        self.round_closes.append(kwargs)

    def write_state_transition(self, **kwargs: Any) -> None:
        self.transitions.append(kwargs)

    def flush(self) -> None:
        pass


class FakeRiskPolicy:
    def __init__(self):
        self.equity = Decimal("10000")
        self.per_trade_risk = Decimal("0.007")
        self._decision = "approve"   # approve / reduce / reject
        self._approved_qty: Optional[Decimal] = None

    def set_approve(self) -> None:
        self._decision = "approve"

    def set_reduce(self, qty: Decimal) -> None:
        self._decision = "reduce"
        self._approved_qty = qty

    def set_reject(self) -> None:
        self._decision = "reject"

    def approve_order(self, context) -> RiskResult:
        if self._decision == "approve":
            return RiskResult(
                decision=RiskDecision.APPROVE,
                approved_qty=None,
                reason="approved",
                level=RiskLevel.NORMAL,
            )
        if self._decision == "reduce":
            return RiskResult(
                decision=RiskDecision.REDUCE,
                approved_qty=self._approved_qty,
                reason="test_reduce",
                level=RiskLevel.NORMAL,
            )
        return RiskResult(
            decision=RiskDecision.REJECT,
            approved_qty=None,
            reason="test_reject",
            level=RiskLevel.HALT,
        )

    def get_risk_level(self) -> RiskLevel:
        return RiskLevel.NORMAL


class FakeGateway:
    exchange_name = "fake"

    def __init__(self):
        self._symbol_info: Dict[str, Dict[str, Decimal]] = {
            "BTCUSDT": {
                "step_size": Decimal("0.001"),
                "tick_size": Decimal("0.1"),
                "min_notional": Decimal("5"),
            }
        }
        self.place_order_calls: List[Order] = []
        self.cancel_order_calls: List[str] = []

        # 成交价（None = 用默认 67000）
        self.next_fill_price: Optional[Decimal] = None
        # 平仓 transact_time
        self.next_transact_time: int = 1_700_000_000_000
        # 失败注入
        self.fail_on_stop: bool = False
        self.fail_on_market: bool = False
        self.fail_on_cancel: bool = False

    def get_mark_price(self) -> Decimal:
        return Decimal("67000")

    def place_order(self, order: Order) -> OrderResult:
        self.place_order_calls.append(order)

        if order.type == OrderType.STOP_MARKET:
            if self.fail_on_stop:
                raise RuntimeError("stop order failed")
            return self._make_result(
                order, OrderStatus.NEW,
                executed_qty=Decimal("0"),
                avg_price=None,
            )

        # MARKET
        if self.fail_on_market:
            raise RuntimeError("market order failed")

        fill_price = self.next_fill_price or Decimal("67000")
        return self._make_result(
            order, OrderStatus.FILLED,
            executed_qty=order.qty,
            avg_price=fill_price,
        )

    def _make_result(
        self,
        order: Order,
        status: OrderStatus,
        executed_qty: Decimal,
        avg_price: Optional[Decimal],
    ) -> OrderResult:
        return OrderResult(
            client_order_id=order.client_order_id,
            exchange_order_id=f"ex-{order.client_order_id}",
            symbol=order.symbol,
            status=status,
            side=order.side,
            type=order.type,
            requested_qty=order.qty,
            executed_qty=executed_qty,
            avg_price=avg_price,
            request_price=order.price,
            stop_price=order.stop_price,
            fills=[],
            total_fee=Decimal("0"),
            fee_asset="USDT",
            transact_time=self.next_transact_time,
            update_time=self.next_transact_time,
        )

    def cancel_order(self, client_order_id: str) -> OrderResult:
        self.cancel_order_calls.append(client_order_id)
        if self.fail_on_cancel:
            raise RuntimeError("cancel failed")
        return OrderResult(
            client_order_id=client_order_id,
            exchange_order_id=f"ex-cancel-{client_order_id}",
            symbol="BTCUSDT",
            status=OrderStatus.CANCELED,
            side=OrderSide.SELL,
            type=OrderType.STOP_MARKET,
            requested_qty=Decimal("0"),
            executed_qty=Decimal("0"),
            avg_price=None,
            request_price=None,
            stop_price=None,
            fills=[],
            total_fee=Decimal("0"),
            fee_asset="USDT",
            transact_time=self.next_transact_time,
            update_time=self.next_transact_time,
        )


# ============================================================================
# 构造辅助
# ============================================================================

def make_round_order(
    price: Decimal = Decimal("67000"),
    qty: Decimal = Decimal("0.001"),
    tag: str = "initial",
    ts: int = 1_700_000_000_000,
) -> RoundOrder:
    return RoundOrder(
        order_id="test-order",
        price=price,
        qty=qty,
        added_ts=ts,
        tag=tag,
    )


def make_long_round(
    entry: Decimal = Decimal("67000"),
    stop: Decimal = Decimal("66800"),
    qty: Decimal = Decimal("0.001"),
    opened_ts: int = 1_700_000_000_000,
    interval_ms: int = 180_000,
) -> PositionRound:
    return PositionRound(
        round_id="test-round-long",
        symbol="BTCUSDT",
        direction=1,
        initial_order=make_round_order(price=entry, qty=qty),
        initial_stop=stop,
        opened_bar_ts=opened_ts,
        kline_interval_ms=interval_ms,
    )


def make_short_round(
    entry: Decimal = Decimal("67000"),
    stop: Decimal = Decimal("67200"),
    qty: Decimal = Decimal("0.001"),
    opened_ts: int = 1_700_000_000_000,
    interval_ms: int = 180_000,
) -> PositionRound:
    return PositionRound(
        round_id="test-round-short",
        symbol="BTCUSDT",
        direction=-1,
        initial_order=make_round_order(price=entry, qty=qty),
        initial_stop=stop,
        opened_bar_ts=opened_ts,
        kline_interval_ms=interval_ms,
    )


def make_bar(
    ts: int = 1_700_000_000_000,
    o: Decimal = Decimal("67000"),
    h: Decimal = Decimal("67100"),
    l: Decimal = Decimal("66900"),
    c: Decimal = Decimal("67050"),
    v: Decimal = Decimal("1000"),
) -> FakeBar:
    return FakeBar(ts=ts, open=o, high=h, low=l, close=c, volume=v)


def make_round_manager(
    gateway: Optional[FakeGateway] = None,
    risk_policy: Optional[FakeRiskPolicy] = None,
    persister: Optional[FakePersister] = None,
    clock: Optional[FakeClock] = None,
    id_gen: Optional[FakeIdGenerator] = None,
    on_round_close=None,
) -> RoundManager:
    config = {
        "symbol": "BTCUSDT",
        "data": {"kline_interval": "3m"},
        "round_manager": {"max_in_memory": 100},
    }
    return RoundManager(
        risk_policy=risk_policy or FakeRiskPolicy(),
        persister=persister or FakePersister(),
        clock=clock or FakeClock(),
        order_gateway=gateway or FakeGateway(),
        id_generator=id_gen or FakeIdGenerator(),
        symbol="BTCUSDT",
        config=config,
        on_round_close=on_round_close,
    )


# ============================================================================
# PositionRound：构造校验
# ============================================================================

class TestPositionRoundInit:

    def test_long_valid(self):
        r = make_long_round()
        assert r.direction == 1
        assert r.initial_stop == Decimal("66800")
        assert r.current_stop == Decimal("66800")
        assert r.initial_risk == Decimal("0.2")   # |67000-66800| * 0.001
        assert r.is_open() is True

    def test_short_valid(self):
        r = make_short_round()
        assert r.direction == -1
        assert r.initial_stop == Decimal("67200")
        assert r.initial_risk == Decimal("0.2")

    def test_invalid_direction_zero(self):
        with pytest.raises(PositionRoundError, match="direction"):
            PositionRound(
                round_id="x", symbol="BTCUSDT", direction=0,
                initial_order=make_round_order(),
                initial_stop=Decimal("66800"),
                opened_bar_ts=1_700_000_000_000,
            )

    def test_invalid_direction_two(self):
        with pytest.raises(PositionRoundError):
            PositionRound(
                round_id="x", symbol="BTCUSDT", direction=2,
                initial_order=make_round_order(),
                initial_stop=Decimal("66800"),
                opened_bar_ts=1_700_000_000_000,
            )

    def test_long_stop_above_entry_rejected(self):
        with pytest.raises(PositionRoundError, match="LONG"):
            PositionRound(
                round_id="x", symbol="BTCUSDT", direction=1,
                initial_order=make_round_order(price=Decimal("67000")),
                initial_stop=Decimal("67200"),   # 高于入场
                opened_bar_ts=1_700_000_000_000,
            )

    def test_short_stop_below_entry_rejected(self):
        with pytest.raises(PositionRoundError, match="SHORT"):
            PositionRound(
                round_id="x", symbol="BTCUSDT", direction=-1,
                initial_order=make_round_order(price=Decimal("67000")),
                initial_stop=Decimal("66800"),   # 低于入场
                opened_bar_ts=1_700_000_000_000,
            )

    def test_zero_qty_rejected(self):
        with pytest.raises(PositionRoundError, match="qty"):
            PositionRound(
                round_id="x", symbol="BTCUSDT", direction=1,
                initial_order=make_round_order(qty=Decimal("0")),
                initial_stop=Decimal("66800"),
                opened_bar_ts=1_700_000_000_000,
            )

    def test_negative_stop_rejected(self):
        with pytest.raises(PositionRoundError, match="initial_stop"):
            make_long_round(stop=Decimal("-1"))


# ============================================================================
# PositionRound：止损方向性（多空核心）
# ============================================================================

class TestPositionRoundStop:

    def test_long_stop_only_up(self):
        r = make_long_round()
        # 上移：接受
        assert r.update_stop(Decimal("66900")) is True
        assert r.current_stop == Decimal("66900")
        # 继续上移：接受
        assert r.update_stop(Decimal("66950")) is True
        # 下移：拒绝
        assert r.update_stop(Decimal("66900")) is False
        assert r.current_stop == Decimal("66950")

    def test_long_stop_reject_same_price(self):
        r = make_long_round()
        # 相同价格：不算上移，拒绝
        assert r.update_stop(Decimal("66800")) is False

    def test_short_stop_only_down(self):
        r = make_short_round()
        # 下移：接受
        assert r.update_stop(Decimal("67100")) is True
        assert r.current_stop == Decimal("67100")
        # 继续下移：接受
        assert r.update_stop(Decimal("67050")) is True
        # 上移：拒绝
        assert r.update_stop(Decimal("67100")) is False
        assert r.current_stop == Decimal("67050")

    def test_short_stop_reject_same_price(self):
        r = make_short_round()
        assert r.update_stop(Decimal("67200")) is False

    def test_reject_zero_stop(self):
        r = make_long_round()
        assert r.update_stop(Decimal("0")) is False

    def test_reject_negative_stop(self):
        r = make_long_round()
        assert r.update_stop(Decimal("-1")) is False

    def test_reject_when_closed(self):
        r = make_long_round()
        r.close(Decimal("67200"), 1_700_000_500_000, "test")
        assert r.update_stop(Decimal("67100")) is False


# ============================================================================
# PositionRound：被动止损触发（多空核心）
# ============================================================================

class TestPositionRoundCheckStop:

    def test_long_triggered_by_low(self):
        r = make_long_round(stop=Decimal("66800"))
        bar = make_bar(l=Decimal("66790"), c=Decimal("67000"))
        assert r.check_stop(bar) is True

    def test_long_not_triggered(self):
        r = make_long_round(stop=Decimal("66800"))
        bar = make_bar(l=Decimal("66810"), c=Decimal("66900"))
        assert r.check_stop(bar) is False

    def test_long_triggered_even_if_close_above(self):
        """插针：low 打穿止损，close 回到止损之上，必须触发。"""
        r = make_long_round(stop=Decimal("66800"))
        bar = make_bar(l=Decimal("66700"), c=Decimal("67000"))
        assert r.check_stop(bar) is True

    def test_short_triggered_by_high(self):
        r = make_short_round(stop=Decimal("67200"))
        bar = make_bar(h=Decimal("67210"), c=Decimal("67100"))
        assert r.check_stop(bar) is True

    def test_short_not_triggered(self):
        r = make_short_round(stop=Decimal("67200"))
        bar = make_bar(h=Decimal("67190"), c=Decimal("67100"))
        assert r.check_stop(bar) is False

    def test_short_triggered_even_if_close_below(self):
        """插针：high 打穿止损，close 回到止损之下，必须触发。"""
        r = make_short_round(stop=Decimal("67200"))
        bar = make_bar(h=Decimal("67300"), c=Decimal("67000"))
        assert r.check_stop(bar) is True

    def test_closed_returns_false(self):
        r = make_long_round()
        r.close(Decimal("67200"), 1_700_000_500_000, "test")
        bar = make_bar(l=Decimal("66000"))
        assert r.check_stop(bar) is False


# ============================================================================
# PositionRound：峰值追踪（多空核心）
# ============================================================================

class TestPositionRoundPeak:

    def test_long_peak_tracks_highest_close(self):
        r = make_long_round()
        r.update_peak(make_bar(c=Decimal("67200")))
        assert r.peak_close == Decimal("67200")
        r.update_peak(make_bar(c=Decimal("67100")))   # 回落，不改
        assert r.peak_close == Decimal("67200")
        r.update_peak(make_bar(c=Decimal("67500")))   # 新高
        assert r.peak_close == Decimal("67500")

    def test_short_peak_tracks_lowest_close(self):
        r = make_short_round()
        r.update_peak(make_bar(c=Decimal("66800")))
        assert r.peak_close == Decimal("66800")
        r.update_peak(make_bar(c=Decimal("66900")))   # 回升，不改
        assert r.peak_close == Decimal("66800")
        r.update_peak(make_bar(c=Decimal("66500")))   # 新低
        assert r.peak_close == Decimal("66500")

    def test_peak_unrealized_only_positive_long(self):
        """多单，亏损时 peak_unrealized 保持 0。"""
        r = make_long_round(entry=Decimal("67000"))
        r.update_peak(make_bar(c=Decimal("66800")))   # 亏 0.2
        assert r.peak_unrealized == Decimal("0")
        r.update_peak(make_bar(c=Decimal("67500")))   # 赚 0.5
        assert r.peak_unrealized == Decimal("0.5")
        r.update_peak(make_bar(c=Decimal("67200")))   # 回落，不改
        assert r.peak_unrealized == Decimal("0.5")

    def test_peak_unrealized_only_positive_short(self):
        r = make_short_round(entry=Decimal("67000"))
        r.update_peak(make_bar(c=Decimal("67200")))   # 亏
        assert r.peak_unrealized == Decimal("0")
        r.update_peak(make_bar(c=Decimal("66500")))   # 赚 0.5
        assert r.peak_unrealized == Decimal("0.5")

    def test_closed_skips_peak(self):
        r = make_long_round()
        r.close(Decimal("67200"), 1_700_000_500_000, "test")
        r.update_peak(make_bar(c=Decimal("68000")))
        assert r.peak_close == Decimal("67000")   # 保持初始


# ============================================================================
# PositionRound：盈亏与 R 倍数（多空核心）
# ============================================================================

class TestPositionRoundPnl:

    def test_long_profit(self):
        r = make_long_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        pnl = r.unrealized_pnl(Decimal("67200"))
        assert pnl == Decimal("0.2")
        assert r.r_multiple(Decimal("67200")) == pytest.approx(1.0)

    def test_long_loss(self):
        r = make_long_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        pnl = r.unrealized_pnl(Decimal("66800"))
        assert pnl == Decimal("-0.2")
        assert r.r_multiple(Decimal("66800")) == pytest.approx(-1.0)

    def test_short_profit(self):
        r = make_short_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        pnl = r.unrealized_pnl(Decimal("66800"))
        assert pnl == Decimal("0.2")
        assert r.r_multiple(Decimal("66800")) == pytest.approx(1.0)

    def test_short_loss(self):
        r = make_short_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        pnl = r.unrealized_pnl(Decimal("67200"))
        assert pnl == Decimal("-0.2")
        assert r.r_multiple(Decimal("67200")) == pytest.approx(-1.0)


# ============================================================================
# PositionRound：平仓
# ============================================================================

class TestPositionRoundClose:

    def test_long_close_profit(self):
        r = make_long_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        record = r.close(Decimal("67200"), 1_700_000_500_000, "test_exit")
        assert record.exit_reason == "test_exit"
        assert record.realized_pnl == Decimal("0.2")
        assert record.r_multiple == pytest.approx(1.0)
        assert r.is_closed() is True

    def test_short_close_profit(self):
        r = make_short_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        record = r.close(Decimal("66800"), 1_700_000_500_000, "test_exit")
        assert record.realized_pnl == Decimal("0.2")

    def test_close_idempotent(self):
        r = make_long_round()
        r1 = r.close(Decimal("67200"), 1_700_000_500_000, "test_exit")
        r2 = r.close(Decimal("67000"), 1_700_000_900_000, "another")
        assert r1 is r2   # 同一对象

    def test_close_invalid_price(self):
        r = make_long_round()
        with pytest.raises(PositionRoundError):
            r.close(Decimal("0"), 1_700_000_500_000, "test_exit")

    def test_bars_held(self):
        r = make_long_round(
            opened_ts=1_700_000_000_000, interval_ms=180_000,
        )
        record = r.close(
            Decimal("67200"), 1_700_000_900_000, "test_exit",
        )
        # 900000 / 180000 = 5
        assert record.bars_held == 5
        assert record.duration_ms == 900_000

    def test_record_fields_complete(self):
        r = make_long_round(
            entry=Decimal("67000"), stop=Decimal("66800"),
            qty=Decimal("0.001"),
        )
        record = r.close(Decimal("67200"), 1_700_000_500_000, "test_exit")
        assert record.round_id == "test-round-long"
        assert record.symbol == "BTCUSDT"
        assert record.direction == 1
        assert record.entry_price == Decimal("67000")
        assert record.initial_stop == Decimal("66800")
        assert record.exit_reason == "test_exit"
        assert record.pyramid_count == 0


# ============================================================================
# 多空对称性
# ============================================================================

class TestPositionRoundSymmetry:

    @pytest.mark.parametrize("direction,favorable_close,mult", [
        (1, Decimal("67200"), 1),
        (-1, Decimal("66800"), 1),
    ])
    def test_favorable_exit_symmetric(
        self, direction, favorable_close, mult
    ):
        """同样幅度 200 的有利波动，多单空单收益相同。"""
        if direction == 1:
            r = make_long_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        else:
            r = make_short_round(
                entry=Decimal("67000"), qty=Decimal("0.001")
            )
        record = r.close(favorable_close, 1_700_000_500_000, "test")
        assert record.realized_pnl == Decimal("0.2") * mult
        assert record.r_multiple == pytest.approx(1.0)

    @pytest.mark.parametrize("direction,stop_distance", [
        (1, Decimal("200")),
        (-1, Decimal("200")),
    ])
    def test_stop_distance_symmetric(self, direction, stop_distance):
        """多空止损与入场距离相同，initial_risk 应相同。"""
        entry = Decimal("67000")
        stop = entry - stop_distance if direction == 1 else entry + stop_distance
        if direction == 1:
            r = make_long_round(entry=entry, stop=stop, qty=Decimal("0.001"))
        else:
            r = make_short_round(entry=entry, stop=stop, qty=Decimal("0.001"))
        assert r.initial_risk == Decimal("0.2")

    def test_adverse_price_symmetric(self):
        """相同幅度的不利波动，多空亏损等额。"""
        long_r = make_long_round(entry=Decimal("67000"), qty=Decimal("0.001"))
        short_r = make_short_round(
            entry=Decimal("67000"), qty=Decimal("0.001")
        )
        long_loss = long_r.unrealized_pnl(Decimal("66800"))
        short_loss = short_r.unrealized_pnl(Decimal("67200"))
        assert long_loss == short_loss == Decimal("-0.2")


# ============================================================================
# 加仓（Step 3 预留接口）
# ============================================================================

class TestPositionRoundAddOrder:

    def test_initial_risk_unchanged(self):
        r = make_long_round(qty=Decimal("0.001"))
        initial_risk_before = r.initial_risk
        r.add_order(
            RoundOrder(
                order_id="add-1",
                price=Decimal("67100"),
                qty=Decimal("0.0008"),
                added_ts=1_700_000_300_000,
                tag="pyramid_1",
            )
        )
        assert r.initial_risk == initial_risk_before

    def test_avg_entry_recomputed(self):
        r = make_long_round(
            entry=Decimal("67000"), qty=Decimal("0.001")
        )
        r.add_order(
            RoundOrder(
                order_id="add-1",
                price=Decimal("67100"),
                qty=Decimal("0.001"),
                added_ts=1_700_000_300_000,
                tag="pyramid_1",
            )
        )
        # (67000*0.001 + 67100*0.001) / 0.002 = 67050
        assert r.avg_entry == Decimal("67050")
        assert r.total_qty == Decimal("0.002")

    def test_add_rejects_closed(self):
        r = make_long_round()
        r.close(Decimal("67200"), 1_700_000_500_000, "test")
        ok = r.add_order(
            RoundOrder(
                order_id="add-1",
                price=Decimal("67100"),
                qty=Decimal("0.0008"),
                added_ts=1_700_000_600_000,
                tag="pyramid_1",
            )
        )
        assert ok is False


# ============================================================================
# RoundManager：开仓
# ============================================================================

class TestRoundManagerOpen:

    def _open_long(self, rm):
        return rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta={"test": True},
        )

    def _open_short(self, rm):
        return rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id="sig-002",
            bar_ts=1_700_000_000_000,
            meta={"test": True},
        )

    def test_open_long_success(self):
        rm = make_round_manager()
        snapshot = self._open_long(rm)
        assert snapshot is not None
        assert snapshot["direction"] == 1
        assert snapshot["round_id"]
        assert rm.get_active_round() is not None

    def test_open_short_success(self):
        rm = make_round_manager()
        snapshot = self._open_short(rm)
        assert snapshot is not None
        assert snapshot["direction"] == -1

    def test_reject_second_active(self):
        rm = make_round_manager()
        assert self._open_long(rm) is not None
        # 第二次开仓应被拒（Step 1 单轮）
        assert self._open_long(rm) is None

    def test_reject_invalid_direction(self):
        rm = make_round_manager()
        result = rm.open_round(
            direction=0,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert result is None

    def test_reject_missing_stop(self):
        rm = make_round_manager()
        result = rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=None,
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert result is None

    def test_reject_long_stop_above_entry(self):
        rm = make_round_manager()
        result = rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),   # 高于入场
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert result is None

    def test_risk_reject(self):
        risk = FakeRiskPolicy()
        risk.set_reject()
        rm = make_round_manager(risk_policy=risk)
        result = self._open_long(rm)
        assert result is None
        assert rm.get_active_round() is None

    def test_risk_reduce_uses_approved_qty(self):
        risk = FakeRiskPolicy()
        risk.set_reduce(Decimal("0.1"))   # 原目标可能 0.35
        gateway = FakeGateway()
        rm = make_round_manager(risk_policy=risk, gateway=gateway)
        result = self._open_long(rm)
        assert result is not None
        # 进场单的 qty 应为 0.1
        market_orders = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.MARKET
        ]
        assert market_orders[0].qty == Decimal("0.1")

    def test_entry_order_fails(self):
        gateway = FakeGateway()
        gateway.fail_on_market = True
        rm = make_round_manager(gateway=gateway)
        result = self._open_long(rm)
        assert result is None
        assert rm.get_active_round() is None

    def test_stop_order_fails_rolls_back(self):
        """止损单挂失败 → 立即平仓回滚 → 返回 None。"""
        gateway = FakeGateway()
        gateway.fail_on_stop = True
        rm = make_round_manager(gateway=gateway)
        result = self._open_long(rm)
        assert result is None
        assert rm.get_active_round() is None

        # 应该有 2 次 MARKET 调用：进场 + 紧急平仓
        market_orders = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.MARKET
        ]
        assert len(market_orders) == 2
        # 紧急平仓单必须是 reduce_only
        emergency_close = market_orders[1]
        assert emergency_close.reduce_only is True
        # 方向应为反向
        assert emergency_close.side == OrderSide.SELL

    def test_persister_called_on_open(self):
        persister = FakePersister()
        rm = make_round_manager(persister=persister)
        self._open_long(rm)
        assert len(persister.round_opens) == 1
        assert persister.round_opens[0]["direction"] == 1


# ============================================================================
# RoundManager：平仓
# ============================================================================

class TestRoundManagerClose:

    def test_close_no_active(self):
        rm = make_round_manager()
        assert rm.close_round(exit_reason="test") is None

    def test_close_long_profit(self):
        gateway = FakeGateway()
        gateway.next_fill_price = Decimal("67200")
        rm = make_round_manager(gateway=gateway)

        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        # 换仓后平仓，用新价格
        gateway.next_fill_price = Decimal("67200")

        record = rm.close_round(exit_reason="test_exit")
        assert record is not None
        assert record["exit_reason"] == "test_exit"
        # 目标 qty = 0.35（10000 * 0.007 / 200）
        # pnl = (67200 - 67000) * 0.35 = 70
        assert Decimal(record["realized_pnl"]) == Decimal("70")
        assert record["r_multiple"] == pytest.approx(1.0)
        assert rm.get_active_round() is None

    def test_close_short_profit(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)

        rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id="sig-002",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        gateway.next_fill_price = Decimal("66800")

        record = rm.close_round(exit_reason="test_exit")
        assert record is not None
        assert Decimal(record["realized_pnl"]) == Decimal("70")

    def test_exit_order_fails_keeps_active(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)

        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        gateway.fail_on_market = True

        record = rm.close_round(exit_reason="test_exit")
        assert record is None
        # 轮次保持活跃（等待重试）
        assert rm.get_active_round() is not None

    def test_close_idempotent(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)

        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        record1 = rm.close_round(exit_reason="test_exit")
        record2 = rm.close_round(exit_reason="test_exit")
        # 第二次调用：无活跃轮次，返回 None（幂等，不重复结算）
        assert record1 is not None
        assert record2 is None

    def test_cancels_stop_before_close(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)

        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        # 找到挂上去的止损单 ID
        stop_orders = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ]
        assert len(stop_orders) == 1
        stop_id = stop_orders[0].client_order_id

        gateway.cancel_order_calls.clear()
        rm.close_round(exit_reason="test_exit")
        assert stop_id in gateway.cancel_order_calls

    def test_on_round_close_callback_fired(self):
        fired = []

        def on_close(record):
            fired.append(record)

        rm = make_round_manager(on_round_close=on_close)

        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        rm.close_round(exit_reason="test_exit")
        assert len(fired) == 1
        assert fired[0]["exit_reason"] == "test_exit"


# ============================================================================
# RoundManager：止损更新
# ============================================================================

class TestRoundManagerUpdateStop:

    def test_no_active(self):
        rm = make_round_manager()
        assert rm.update_stop(Decimal("66900")) is False

    def test_long_move_up(self):
        rm = make_round_manager()
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert rm.update_stop(Decimal("66900")) is True
        snapshot = rm.get_active_round()
        assert snapshot["current_stop"] == "66900"

    def test_long_reject_down(self):
        rm = make_round_manager()
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        # 下移拒绝
        assert rm.update_stop(Decimal("66700")) is False
        snapshot = rm.get_active_round()
        assert snapshot["current_stop"] == "66800"

    def test_short_move_down(self):
        rm = make_round_manager()
        rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id="sig-002",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert rm.update_stop(Decimal("67100")) is True
        snapshot = rm.get_active_round()
        assert snapshot["current_stop"] == "67100"

    def test_short_reject_up(self):
        rm = make_round_manager()
        rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id="sig-002",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        assert rm.update_stop(Decimal("67300")) is False

    def test_update_cancels_old_and_places_new(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id="sig-001",
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        old_stop_orders = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ]
        old_stop_id = old_stop_orders[0].client_order_id
        gateway.cancel_order_calls.clear()
        gateway.place_order_calls.clear()

        assert rm.update_stop(Decimal("66900")) is True

        # 验证：撤旧止损，挂新止损
        assert old_stop_id in gateway.cancel_order_calls
        new_stop_orders = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ]
        assert len(new_stop_orders) == 1
        assert new_stop_orders[0].stop_price == Decimal("66900")


# ============================================================================
# 合约特性
# ============================================================================

class TestContract:

    def test_long_open_side_buy(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        entry_order = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.MARKET
        ][0]
        assert entry_order.side == OrderSide.BUY
        assert entry_order.reduce_only is False

    def test_short_open_side_sell(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        entry_order = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.MARKET
        ][0]
        assert entry_order.side == OrderSide.SELL

    def test_long_close_side_sell(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        gateway.place_order_calls.clear()
        rm.close_round(exit_reason="test")
        close_order = gateway.place_order_calls[0]
        assert close_order.side == OrderSide.SELL

    def test_short_close_side_buy(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        gateway.place_order_calls.clear()
        rm.close_round(exit_reason="test")
        close_order = gateway.place_order_calls[0]
        assert close_order.side == OrderSide.BUY

    def test_close_uses_reduce_only(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        gateway.place_order_calls.clear()
        rm.close_round(exit_reason="test")
        close_order = gateway.place_order_calls[0]
        assert close_order.reduce_only is True

    def test_stop_type_stop_market(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        stop_order = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ][0]
        assert stop_order.type == OrderType.STOP_MARKET
        assert stop_order.reduce_only is True

    def test_stop_working_type_mark_price(self):
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        stop_order = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ][0]
        assert stop_order.working_type == WorkingType.MARK_PRICE

    def test_stop_side_opposite_of_position(self):
        """多单止损 SELL，空单止损 BUY（平仓方向）。"""
        # 多单
        gw1 = FakeGateway()
        rm1 = make_round_manager(gateway=gw1)
        rm1.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        stop_order_long = [
            o for o in gw1.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ][0]
        assert stop_order_long.side == OrderSide.SELL

        # 空单
        gw2 = FakeGateway()
        rm2 = make_round_manager(gateway=gw2)
        rm2.open_round(
            direction=-1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("67200"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        stop_order_short = [
            o for o in gw2.place_order_calls
            if o.type == OrderType.STOP_MARKET
        ][0]
        assert stop_order_short.side == OrderSide.BUY

    def test_qty_rounded_to_step_size(self):
        """目标 qty 会被 round 到 step_size 的整数倍。"""
        gateway = FakeGateway()
        rm = make_round_manager(gateway=gateway)
        rm.open_round(
            direction=1,
            entry_price_hint=Decimal("67000"),
            stop_price_hint=Decimal("66800"),
            signal_id=None,
            bar_ts=1_700_000_000_000,
            meta=None,
        )
        entry_order = [
            o for o in gateway.place_order_calls
            if o.type == OrderType.MARKET
        ][0]
        # 0.35 是 step_size 0.001 的整数倍
        assert entry_order.qty == Decimal("0.35")
        # 且 qty % step_size == 0
        assert entry_order.qty % Decimal("0.001") == 0
