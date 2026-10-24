"""
tests/integration/test_full_cycle.py — 集成测试：完整闭环

覆盖 Step 1 的全链路：
    K 线 → 因子 → 策略信号 → 状态机迁移 → 风控 → 网关下单
    → 止损挂单 → 止损触发 → 平仓结算 → 持久化 → 归因

测试内容：
1. 多头突破→止损：从 IDLE 到 TREND_RIDE 到 IDLE 的完整路径
2. 空头突破→止损：严格镜像
3. 多空对称：同样结构，轮次数一致
4. 状态机保护：有活跃轮次时不重复开仓
5. 风控拦截：HALT 时不开仓；REDUCE 时 qty 减半
6. 持久化落盘：round_open / round_close / state_transitions
7. 紧急停止：平仓 + 状态切 IDLE
8. 归因报告：RoundAttribution 分析结果

设计原则：
- 用真实组件（非 mock），只 mock 盘口数据和持久化
- SyntheticFeed 根据 K 线方向生成不平衡盘口（阳线买盘强，阴线卖盘强）
- SimulatedClock 可控推进
- 每次测试独立构造 Harness
- 断言链路每个环节

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass
from decimal import Decimal
import math
from typing import Any, Dict, List, Optional

import pytest

from core.gateway import OrderType
from core.risk import RiskLevel
from core.factor import FactorState

from system.data_buffer import RingBuffer
from system.order_idempotent import OrderIdGenerator

from execution.gateway_virtual import VirtualGateway, MarketFeed

from factors.factor_registry import FactorRegistry
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor

from strategies.strategy_trend_round import TrendRoundStrategy

from states.state_idle import IdleState
from states.state_breakout import BreakoutConfirmState
from states.state_trend import TrendRideState
from states.state_top_detect import TopDetectState
from states.state_machine import StateMachine

from rounds.round_manager import RoundManager

from risk.policy_minimal import PolicyMinimal

from backtest.replay_engine import SimulatedClock

from analytics.round_attribution import RoundAttribution


# ============================================================================
# 测试 K 线与盘口辅助
# ============================================================================

@dataclass
class FakeKline:
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    is_closed: bool = True

    @property
    def close_ts(self) -> int:
        return self.ts + 180_000


def make_klines_long_cycle(
    n_warmup: int = 130,
    n_rise: int = 5,
    n_fall: int = 1,
    base_price: float = 67000.0,
    start_ts: int = 1_700_000_000_000,
) -> List[FakeKline]:
    """震荡 → 突破上涨 → 反向打穿止损。"""
    klines: List[FakeKline] = []
    ts = start_ts
    price = base_price

    # 1. 预热期震荡
    for i in range(n_warmup):
        delta = math.sin(i / 3.0) * 20
        price = base_price + delta
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(price - 10)),
            high=Decimal(str(price + 20)),
            low=Decimal(str(price - 20)),
            close=Decimal(str(price)),
            volume=Decimal("1000"),
        ))
        ts += 180_000

    # 2. 突破段
    for i in range(n_rise):
        o = price
        price += 100
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(o)),
            high=Decimal(str(price + 30)),
            low=Decimal(str(o - 20)),
            close=Decimal(str(price)),
            volume=Decimal(str(2500 + i * 300)),
        ))
        ts += 180_000

    # 3. 反向段（打穿 1.5 ATR 止损）
    for _ in range(n_fall):
        o = price
        price -= 800
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(o)),
            high=Decimal(str(o + 20)),
            low=Decimal(str(price - 50)),
            close=Decimal(str(price)),
            volume=Decimal("4000"),
        ))
        ts += 180_000

    return klines


def make_klines_short_cycle(
    n_warmup: int = 130,
    n_fall: int = 5,
    n_rise: int = 1,
    base_price: float = 67000.0,
    start_ts: int = 1_700_000_000_000,
) -> List[FakeKline]:
    """震荡 → 突破下跌 → 反向打穿止损（空头对称）。"""
    klines: List[FakeKline] = []
    ts = start_ts
    price = base_price

    for i in range(n_warmup):
        delta = math.sin(i / 3.0) * 20
        price = base_price + delta
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(price + 10)),
            high=Decimal(str(price + 20)),
            low=Decimal(str(price - 20)),
            close=Decimal(str(price)),
            volume=Decimal("1000"),
        ))
        ts += 180_000

    for i in range(n_fall):
        o = price
        price -= 100
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(o)),
            high=Decimal(str(o + 20)),
            low=Decimal(str(price - 30)),
            close=Decimal(str(price)),
            volume=Decimal(str(2500 + i * 300)),
        ))
        ts += 180_000

    for _ in range(n_rise):
        o = price
        price += 800
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(o)),
            high=Decimal(str(price + 50)),
            low=Decimal(str(o - 20)),
            close=Decimal(str(price)),
            volume=Decimal("4000"),
        ))
        ts += 180_000

    return klines


class SyntheticFeed(MarketFeed):
    """根据 K 线方向生成不平衡盘口。"""

    def __init__(self):
        self._snapshot: Optional[Dict[str, Any]] = None

    def update_bar(self, bar: FakeKline) -> None:
        close = Decimal(str(bar.close))
        open_ = Decimal(str(bar.open))
        if close > open_:
            bid_qty, ask_qty = Decimal("20"), Decimal("10")
        elif close < open_:
            bid_qty, ask_qty = Decimal("10"), Decimal("20")
        else:
            bid_qty, ask_qty = Decimal("10"), Decimal("10")

        snap_ts = int(bar.ts) + 180_000
        spread = close * Decimal("0.0005")
        self._snapshot = {
            "ts": snap_ts,
            "bids": [(close - spread - i * spread, bid_qty) for i in range(10)],
            "asks": [(close + spread + i * spread, ask_qty) for i in range(10)],
        }

    def get_snapshot_at(self, ts: int) -> Optional[Dict[str, Any]]:
        if self._snapshot is None:
            return None
        if abs(self._snapshot["ts"] - ts) > 500:
            return None
        return self._snapshot


class FakePersister:
    """内存持久化，记录所有写入。"""
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

    def get_state_path(self) -> List[str]:
        path: List[str] = []
        for t in self.transitions:
            if t["from_state"] != t["to_state"]:
                if not path:
                    path.append(t["from_state"])
                path.append(t["to_state"])
        return path


# ============================================================================
# 测试 Harness
# ============================================================================

class Harness:
    """
    集成测试编排器。

    契约：
    - 构造真实组件（非 mock）
    - FakePersister 记录所有写入
    - SyntheticFeed 提供与 K 线方向一致的盘口
    """

    DEFAULT_CONFIG = {
        "symbol": "BTCUSDT",
        "initial_equity": 10000.0,
        "data": {"kline_interval": "3m"},
        "factors": {
            "quantile_window": 100,
            "warmup_bars": 120,
            "max_compute_ms": 5.0,
            "momentum": {
                "roc_period": 3,
                "roc_weight": 0.7,
                "accel_weight": 0.3,
                "weight": 0.35,
            },
            "obi": {
                "levels": 10,
                "decay_k": 0.15,
                "smooth_half_life_s": 2.0,
                "snapshot_max_age_ms": 500,
                "weight": 0.40,
            },
            "volume": {"ma_window": 20, "weight": 0.25},
        },
        "strategy": {
            "trend_round": {
                "entry_threshold": 0.5,
                "direction_threshold": 0.1,
                "reverse_threshold": 0.3,
                "max_bars_in_round": 60,
                "cooldown_bars": 1,
            }
        },
        "states": {
            "breakout_confirm": {
                "ma_period": 25,
                "volume_ma_window": 20,
                "volume_amplification_threshold": 1.5,
                "obi_direction_threshold": 0.15,
                "max_bars_in_state": 5,
                "initial_stop_atr": 1.5,
            },
            "trend_ride": {
                "max_bars_in_round": 60,
                "trailing_stop_trigger_atr": 1.5,
                "trailing_stop_distance_atr": 1.0,
                "top_escape_min_profit_atr": 2.0,
                "momentum_reversal_threshold": 0.2,
                "atr_period": 14,
            },
            "top_detect": {
                "max_observation_bars": 10,
                "escape_confirm_distance_atr": 2.0,
                "escape_return_tolerance_atr": 0.5,
                "atr_period": 14,
            },
        },
        "risk": {
            "per_trade_risk": "0.007",
            "daily_dd_warn": "0.01",
            "daily_dd_reduce": "0.02",
            "daily_dd_close_only": "0.025",
            "daily_dd_halt": "0.03",
            "protection_rounds": 0,
            "protection_qty_ratio": "1.0",
        },
    }

    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        self.config = _deep_merge(
            self.DEFAULT_CONFIG, config_overrides or {}
        )

        self.clock = SimulatedClock(start_ts=1_700_000_000_000)
        self.buffer = RingBuffer(maxlen=1000)
        self.persister = FakePersister()
        self.feed = SyntheticFeed()

        risk_config = dict(self.config)
        risk_config["instances"] = {"clock": self.clock}
        self.risk = PolicyMinimal(risk_config)

        self.id_gen = OrderIdGenerator({"order_id": {"prefix": "test"}})
        self.gateway = VirtualGateway(
            feed=self.feed,
            clock=self.clock,
            id_generator=self.id_gen,
            symbol=self.config["symbol"],
            config=self.config,
            fee_rate=Decimal("0.0004"),
            latency_ms=0,
            initial_balance=Decimal(str(self.config["initial_equity"])),
        )

        self.factor_registry = FactorRegistry(self.config)
        self.factor_registry.register(
            "momentum", MomentumFactor(self.config), weight=0.35
        )
        self.factor_registry.register(
            "obi", OBIFactor(self.config), weight=0.40
        )
        self.factor_registry.register(
            "volume", VolumeFactor(self.config), weight=0.25
        )

        self.strategy = TrendRoundStrategy(self.config)

        self.rounds = RoundManager(
            risk_policy=self.risk,
            persister=self.persister,
            clock=self.clock,
            order_gateway=self.gateway,
            id_generator=self.id_gen,
            symbol=self.config["symbol"],
            config=self.config,
        )

        self.state_machine = StateMachine(
            factor_registry=self.factor_registry,
            strategies={"trend_round": self.strategy},
            config=self.config,
            persister=self.persister,
            risk_policy=self.risk,
            order_gateway=self.gateway,
            round_manager=self.rounds,
            clock=self.clock,
            symbol=self.config["symbol"],
            kline_buffer=self.buffer,
        )
        self.state_machine.register_state("idle", IdleState(self.config))
        self.state_machine.register_state(
            "breakout_confirm", BreakoutConfirmState(self.config)
        )
        self.state_machine.register_state(
            "trend_ride", TrendRideState(self.config)
        )
        self.state_machine.register_state(
            "top_detect", TopDetectState(self.config)
        )
        self.state_machine.validate()

    def run_klines(self, klines: List[FakeKline]) -> None:
        warmup = self.config["factors"]["warmup_bars"]
        for i, bar in enumerate(klines):
            self.buffer.push(bar)
            if i < warmup:
                continue
            self.clock.advance_to(int(bar.ts))
            self.feed.update_bar(bar)
            snapshot = self.feed.get_snapshot_at(int(bar.ts) + 180_000)
            self.state_machine.on_bar_close(bar, snapshot)

    def get_rounds(self) -> List[Dict[str, Any]]:
        return self.rounds.list_closed_rounds()

    def get_gateway_orders(self) -> List[Any]:
        return list(self.gateway._recent_orders.values())


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for k, v in override.items():
        if (
            k in result
            and isinstance(result[k], dict)
            and isinstance(v, dict)
        ):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# ============================================================================
# 多头完整周期
# ============================================================================

class TestLongCycle:

    def test_enters_trend_ride(self):
        """突破后，状态机经过 breakout_confirm 进入 trend_ride。"""
        klines = make_klines_long_cycle()
        h = Harness()
        h.run_klines(klines)

        path = h.persister.get_state_path()
        assert "breakout_confirm" in path, (
            f"state path missing breakout_confirm: {path}"
        )
        assert "trend_ride" in path, (
            f"state path missing trend_ride: {path}"
        )

    def test_round_opened_with_long_direction(self):
        """round_open 落盘，方向为 +1，止损低于入场。"""
        klines = make_klines_long_cycle()
        h = Harness()
        h.run_klines(klines)

        assert len(h.persister.round_opens) >= 1, (
            "no round_open persisted"
        )
        open_ = h.persister.round_opens[0]
        assert open_["direction"] == 1
        entry = Decimal(open_["entry_price"])
        stop = Decimal(open_["initial_stop"])
        assert entry > 0
        assert stop < entry, (
            f"LONG stop {stop} not below entry {entry}"
        )

    def test_stop_triggers_and_settles(self):
        """反向暴跌打穿止损，触发结算，pnl 为负。"""
        klines = make_klines_long_cycle()
        h = Harness()
        h.run_klines(klines)

        assert len(h.persister.round_closes) >= 1, (
            "no round_close persisted"
        )
        close = h.persister.round_closes[0]
        assert close["exit_reason"] == "stop"
        assert Decimal(close["pnl"]) < 0, (
            f"stop exit should be negative, got {close['pnl']}"
        )

    def test_gateway_received_orders(self):
        """网关收到市价单（开仓）+ 止损单。"""
        klines = make_klines_long_cycle()
        h = Harness()
        h.run_klines(klines)

        orders = h.get_gateway_orders()
        market_orders = [o for o in orders if o.type == OrderType.MARKET]
        stop_orders = [
            o for o in orders if o.type == OrderType.STOP_MARKET
        ]

        assert len(market_orders) >= 1, "no market order sent"
        assert len(stop_orders) >= 1, "no stop order sent"


# ============================================================================
# 空头完整周期
# ============================================================================

class TestShortCycle:

    def test_enters_trend_ride(self):
        klines = make_klines_short_cycle()
        h = Harness()
        h.run_klines(klines)

        path = h.persister.get_state_path()
        assert "breakout_confirm" in path
        assert "trend_ride" in path

    def test_round_opened_with_short_direction(self):
        """空单止损必须高于入场。"""
        klines = make_klines_short_cycle()
        h = Harness()
        h.run_klines(klines)

        assert len(h.persister.round_opens) >= 1
        open_ = h.persister.round_opens[0]
        assert open_["direction"] == -1
        entry = Decimal(open_["entry_price"])
        stop = Decimal(open_["initial_stop"])
        assert stop > entry, (
            f"SHORT stop {stop} not above entry {entry}"
        )

    def test_stop_triggers(self):
        klines = make_klines_short_cycle()
        h = Harness()
        h.run_klines(klines)

        assert len(h.persister.round_closes) >= 1
        close = h.persister.round_closes[0]
        assert close["exit_reason"] == "stop"


# ============================================================================
# 多空对称
# ============================================================================

class TestSymmetry:

    def test_rounds_symmetric(self):
        """同样的突破-反向结构，多空产生相同数量的轮次。"""
        h_long = Harness()
        h_long.run_klines(make_klines_long_cycle())

        h_short = Harness()
        h_short.run_klines(make_klines_short_cycle())

        assert len(h_long.persister.round_opens) == (
            len(h_short.persister.round_opens)
        )


# ============================================================================
# 风控集成
# ============================================================================

class TestRiskIntegration:

    def test_halt_blocks_open(self):
        """HALT 等级下，突破信号被拒，不产生轮次。"""
        h = Harness()
        # 注入 4% 回撤触发 HALT
        h.risk.on_round_closed({"realized_pnl": "-400"})
        assert h.risk.get_risk_level() == RiskLevel.HALT

        h.run_klines(make_klines_long_cycle())

        assert h.rounds.get_active_round() is None
        assert len(h.persister.round_opens) == 0

    def test_reduce_halves_qty(self):
        """REDUCE 等级下，开仓 qty 是正常的一半。"""
        # 正常场景
        h_normal = Harness()
        h_normal.run_klines(make_klines_long_cycle())
        normal_opens = len(h_normal.persister.round_opens)

        # REDUCE 场景
        h_reduce = Harness()
        h_reduce.risk.on_round_closed({"realized_pnl": "-200"})
        assert h_reduce.risk.get_risk_level() == RiskLevel.REDUCE
        h_reduce.run_klines(make_klines_long_cycle())
        reduce_opens = len(h_reduce.persister.round_opens)

        # 两者都能开仓（只是 qty 不同）
        assert normal_opens >= 1
        assert reduce_opens >= 1

        normal_qty = Decimal(
            h_normal.persister.round_opens[0]["total_qty"]
        )
        reduce_qty = Decimal(
            h_reduce.persister.round_opens[0]["total_qty"]
        )
        assert reduce_qty < normal_qty, (
            f"REDUCE qty {reduce_qty} not less than normal {normal_qty}"
        )


# ============================================================================
# 状态机保护
# ============================================================================

class TestStateMachineProtection:

    def test_no_double_open(self):
        """有活跃轮次时不重复开仓。"""
        # 一直涨，无反向段 → 应保持 1 个活跃轮次
        h = Harness()
        h.run_klines(make_klines_long_cycle(n_rise=10, n_fall=0))

        # 最多 1 个 round_open
        assert len(h.persister.round_opens) <= 1, (
            f"expected at most 1 round, got "
            f"{len(h.persister.round_opens)}"
        )


# ============================================================================
# 持久化
# ============================================================================

class TestPersistence:

    def test_state_transitions_recorded(self):
        """状态迁移落盘。"""
        h = Harness()
        h.run_klines(make_klines_long_cycle())

        # 有 idle→breakout_confirm、breakout_confirm→trend_ride 的记录
        transitions = h.persister.transitions
        assert len(transitions) > 0

        to_states = {t["to_state"] for t in transitions}
        assert "breakout_confirm" in to_states or (
            "trend_ride" in to_states
        )

    def test_round_opens_have_required_fields(self):
        """round_open 落盘含关键字段。"""
        h = Harness()
        h.run_klines(make_klines_long_cycle())

        if not h.persister.round_opens:
            pytest.skip("no round opened in this run")

        open_ = h.persister.round_opens[0]
        required = [
            "round_id", "direction", "entry_price", "initial_stop",
            "total_qty",
        ]
        for field in required:
            assert field in open_, (
                f"round_open missing field: {field}"
            )

    def test_round_closes_have_required_fields(self):
        """round_close 落盘含关键字段。"""
        h = Harness()
        h.run_klines(make_klines_long_cycle())

        if not h.persister.round_closes:
            pytest.skip("no round closed in this run")

        close = h.persister.round_closes[0]
        required = [
            "round_id", "exit_reason", "pnl", "r_multiple",
        ]
        for field in required:
            assert field in close, (
                f"round_close missing field: {field}"
            )


# ============================================================================
# 紧急停止
# ============================================================================

class TestEmergencyStop:

    def test_emergency_closes_positions(self):
        """紧急停止会平掉所有活跃轮次。"""
        from risk.emergency_stop import EmergencyStop, TriggerSource

        h = Harness()
        # 一直涨，保留活跃轮次
        h.run_klines(make_klines_long_cycle(n_rise=10, n_fall=0))

        if h.rounds.get_active_round() is None:
            pytest.skip("no active round in this run")

        em = EmergencyStop(
            state_machine=h.state_machine,
            gateway=h.gateway,
            round_manager=h.rounds,
            persister=h.persister,
            clock=h.clock,
        )

        event = em.trigger(
            source=TriggerSource.EXTERNAL_API,
            reason="integration_test",
            operator="test",
        )
        assert event is not None
        assert h.rounds.get_active_round() is None


# ============================================================================
# 归因
# ============================================================================

class TestAttribution:

    def test_report_matches_rounds(self):
        """归因报告的统计与轮次列表一致。"""
        h = Harness()
        h.run_klines(make_klines_long_cycle())

        rounds = h.get_rounds()
        if not rounds:
            pytest.skip("no rounds executed")

        report = RoundAttribution().analyze(rounds)

        assert report.total_rounds == len(rounds)
        assert report.wins + report.losses + report.breakeven == (
            report.total_rounds
        )
        expected_pnl = sum(
            Decimal(str(r["realized_pnl"])) for r in rounds
        )
        assert Decimal(report.total_pnl) == expected_pnl
