"""
tests/unit/test_factor.py — 因子单元测试

范围：
1. 动量因子（MomentumFactor）：ROC/加速度、预热、异常、边界
2. OBI 因子（OBIFactor）：距离加权、时间平滑、stale、交叉盘口
3. 成交量因子（VolumeFactor）：相对成交量、除零保护、自相关陷阱
4. 因子注册表（FactorRegistry）：注册、批量计算、权重归一化、错误隔离
5. 归一化一致性：所有因子输出范围符合声明
6. 预热期行为：WARMUP 状态不污染 _raw_history

设计原则：
- 每个测试独立：用工厂函数构造，无共享状态
- 断言精确：`fv.state == FactorState.WARMUP`，不写 `assert fv`
- 确定性：固定输入 → 固定输出，无时间依赖
- 覆盖边界：历史不足、除零、异常值、快照过期
- 覆盖异常路径：内部异常包装成 ERROR、因子返回非 FactorValue

关键断言：
- 所有因子的输出 `value ∈ normalized_range`
- 预热期 `state == WARMUP`，`_raw_history` 为空
- stale 状态返回上一根值，且 `_prev_smoothed` 不被污染
- 异常输出 `state == ERROR`，包含 error 信息
- 注册表的权重归一化严格：可用因子权重和 == 1.0

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest

from core.factor import (
    DEGRADE_THRESHOLD,
    FactorProvider,
    FactorState,
    FactorValue,
)
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor
from factors.factor_registry import FactorRegistry


# ============================================================================
# Fake 组件
# ============================================================================

@dataclass
class FakeBar:
    """测试用 K 线。"""
    ts: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("1000")
    is_closed: bool = True

    @property
    def close_ts(self) -> int:
        return self.ts + 180_000


def make_bar(
    ts: int,
    close: float,
    volume: float = 1000,
    high: Optional[float] = None,
    low: Optional[float] = None,
) -> FakeBar:
    """构造测试 K 线。"""
    close_d = Decimal(str(close))
    return FakeBar(
        ts=ts,
        open=close_d,
        high=Decimal(str(high)) if high is not None else close_d,
        low=Decimal(str(low)) if low is not None else close_d,
        close=close_d,
        volume=Decimal(str(volume)),
    )


def make_snapshot(
    ts: int,
    bids: Optional[List] = None,
    asks: Optional[List] = None,
) -> Dict[str, Any]:
    """构造盘口快照。"""
    if bids is None:
        bids = [(Decimal("66999"), Decimal("10")) for _ in range(10)]
    if asks is None:
        asks = [(Decimal("67001"), Decimal("10")) for _ in range(10)]
    return {
        "ts": ts,
        "bids": [(Decimal(str(p)), Decimal(str(q))) for p, q in bids],
        "asks": [(Decimal(str(p)), Decimal(str(q))) for p, q in asks],
    }


# ============================================================================
# 动量因子
# ============================================================================

class TestMomentumFactor:

    def _make(self, config: Optional[Dict] = None) -> MomentumFactor:
        cfg = {
            "factors": {
                "quantile_window": 500,
                "warmup_bars": 5,
                "momentum": {
                    "roc_period": 3,
                    "roc_weight": 0.7,
                    "accel_weight": 0.3,
                },
            }
        }
        if config:
            cfg["factors"].update(config)
        return MomentumFactor(cfg)

    def test_init_defaults(self):
        f = self._make()
        assert f.roc_period == 3
        assert f.roc_weight == 0.7
        assert f.accel_weight == 0.3
        assert f.normalized_range == (-1.0, 1.0)

    def test_init_invalid_roc_period(self):
        with pytest.raises(ValueError, match="roc_period"):
            self._make({"momentum": {"roc_period": 0}})

    def test_init_invalid_weights_sum(self):
        """权重和必须为 1。"""
        with pytest.raises(ValueError, match="roc_weight"):
            self._make({"momentum": {"roc_weight": 0.5, "accel_weight": 0.3}})

    def test_init_negative_weight(self):
        with pytest.raises(ValueError):
            self._make({"momentum": {"roc_weight": -0.1, "accel_weight": 1.1}})

    def test_warmup_returns_warmup_state(self):
        """历史不足 roc_period+1 时，返回 WARMUP。"""
        f = self._make()
        for i in range(3):   # 需要 4 根，只给 3 根
            bar = make_bar(ts=i * 180_000, close=67000 + i)
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert fv.state == FactorState.WARMUP
            assert fv.name == "momentum"

    def test_ready_after_enough_bars(self):
        """足够历史后，返回 READY（经过预热切换）。"""
        f = self._make()
        # 推 5 根（warmup_bars=5）
        for i in range(5):
            bar = make_bar(ts=i * 180_000, close=67000 + i * 10)
            f.on_bar_close(bar)
            fv = f.compute(bar)
        # 第 6 根 K 线时，warmup 完成 + 历史足够
        bar = make_bar(ts=5 * 180_000, close=67050)
        f.on_bar_close(bar)
        fv = f.compute(bar)
        assert fv.state == FactorState.READY
        assert fv.value is not None

    def test_value_in_range(self):
        """所有输出必须在 [-1, 1]。"""
        f = self._make()
        for i in range(20):
            bar = make_bar(ts=i * 180_000, close=67000 + i * 20)
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert -1.0 <= fv.value <= 1.0

    def test_roc_calculation(self):
        """ROC(3) = (close[t] - close[t-3]) / close[t-3]。"""
        f = self._make({"quantile_window": 10})   # 小窗口便于观察
        closes = [67000, 67010, 67020, 67030]
        for i, c in enumerate(closes):
            bar = make_bar(ts=i * 180_000, close=c)
            f.on_bar_close(bar)
            f.compute(bar)
        # 第 4 根：ROC = (67030 - 67000) / 67000 = 0.000448
        # 无加速度（第一根有完整数据），raw = 0.7 * roc + 0.3 * 0 = roc
        # 分位数归一化后应该是 0 到 1 的某个位置
        # 这里只验证：_raw_history 有 1 个值
        assert len(f._raw_history) == 1
        raw = f._raw_history[0]
        expected_roc = (67030 - 67000) / 67000
        # 第一根的 accel 是 0，raw = 0.7 * roc
        assert abs(raw - 0.7 * expected_roc) < 1e-6

    def test_no_close_field(self):
        """bar 缺 close 时，返回 ERROR。"""
        f = self._make()
        bar = FakeBar(
            ts=0, open=Decimal("0"), high=Decimal("0"),
            low=Decimal("0"), close=None, volume=Decimal("0"),
        )
        fv = f.compute(bar)
        assert fv.state == FactorState.ERROR
        assert "close" in (fv.error or "").lower()

    def test_deterministic(self):
        """同一输入，两次调用结果一致。"""
        f = self._make()
        bars = [
            make_bar(ts=i * 180_000, close=67000 + i * 10)
            for i in range(10)
        ]
        results = []
        for b in bars:
            f.on_bar_close(b)
            fv = f.compute(b)
            results.append(fv.value)

        # 重建
        f2 = self._make()
        results2 = []
        for b in bars:
            f2.on_bar_close(b)
            fv = f2.compute(b)
            results2.append(fv.value)

        assert results == results2

    def test_reset(self):
        f = self._make()
        for i in range(10):
            bar = make_bar(ts=i * 180_000, close=67000 + i * 10)
            f.on_bar_close(bar)
            f.compute(bar)
        f.reset()
        assert len(f._close_history) == 0
        assert f._prev_roc is None
        assert len(f._raw_history) == 0


# ============================================================================
# OBI 因子
# ============================================================================

class TestOBIFactor:

    def _make(self, config: Optional[Dict] = None) -> OBIFactor:
        cfg = {
            "data": {"kline_interval": "3m"},
            "factors": {
                "quantile_window": 500,
                "warmup_bars": 5,
                "obi": {
                    "levels": 10,
                    "decay_k": 0.15,
                    "smooth_half_life_s": 2.0,
                    "snapshot_max_age_ms": 500,
                },
            },
        }
        if config:
            cfg["factors"].update(config)
        return OBIFactor(cfg)

    def test_init_defaults(self):
        f = self._make()
        assert f.levels == 10
        assert f.decay_k == 0.15
        assert f.normalized_range == (-1.0, 1.0)

    def test_init_invalid_levels(self):
        with pytest.raises(ValueError, match="levels"):
            self._make({"obi": {"levels": 0}})

    def test_init_invalid_decay_k(self):
        with pytest.raises(ValueError, match="decay_k"):
            self._make({"obi": {"decay_k": -0.1}})

    def test_none_snapshot_returns_stale(self):
        """snapshot=None → stale。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        fv = f.compute(bar, snapshot=None)
        assert fv.stale is True
        assert fv.state == FactorState.WARMUP   # 无上一根值

    def test_outdated_snapshot_returns_stale(self):
        """快照过期 → stale。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        snap = make_snapshot(ts=0 - 1000)   # 比 bar 早 1000ms
        fv = f.compute(bar, snapshot=snap)
        assert fv.stale is True

    def test_stale_returns_previous_value(self):
        """已有上一根值时，stale 返回上一根的值。"""
        f = self._make()
        bar1 = make_bar(ts=0, close=67000)
        snap1 = make_snapshot(
            ts=bar1.ts + 100,
            bids=[(Decimal("66999"), Decimal("20")) for _ in range(10)],
            asks=[(Decimal("67001"), Decimal("10")) for _ in range(10)],
        )
        fv1 = f.compute(bar1, snapshot=snap1)
        # 快照无效
        bar2 = make_bar(ts=180_000, close=67100)
        fv2 = f.compute(bar2, snapshot=None)
        assert fv2.stale is True
        assert fv2.value == fv1.value

    def test_crossed_book_rejected(self):
        """交叉盘口（bids[0] >= asks[0]）被拒绝。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        # bids[0] = 67002 >= asks[0] = 67001
        snap = make_snapshot(
            ts=0,
            bids=[(Decimal("67002"), Decimal("10"))],
            asks=[(Decimal("67001"), Decimal("10"))],
        )
        fv = f.compute(bar, snapshot=snap)
        assert fv.stale is True

    def test_empty_book_rejected(self):
        """空盘口被拒绝。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        snap = make_snapshot(ts=0, bids=[], asks=[])
        fv = f.compute(bar, snapshot=snap)
        assert fv.stale is True

    def test_balanced_book_neutral(self):
        """买卖量均衡时，raw ≈ 0。"""
        f = self._make({"quantile_window": 5})
        bar = make_bar(ts=0, close=67000)
        snap = make_snapshot(ts=bar.ts + 100)
        fv = f.compute(bar, snapshot=snap)
        # 第一根：smoothed = raw ≈ 0
        assert abs(fv.raw_value) < 1e-6

    def test_bid_heavy_positive(self):
        """买盘重时，raw > 0。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        snap = make_snapshot(
            ts=bar.ts + 100,
            bids=[(Decimal("66999"), Decimal("100")) for _ in range(10)],
            asks=[(Decimal("67001"), Decimal("10")) for _ in range(10)],
        )
        fv = f.compute(bar, snapshot=snap)
        assert fv.raw_value > 0

    def test_ask_heavy_negative(self):
        """卖盘重时，raw < 0。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        snap = make_snapshot(
            ts=bar.ts + 100,
            bids=[(Decimal("66999"), Decimal("10")) for _ in range(10)],
            asks=[(Decimal("67001"), Decimal("100")) for _ in range(10)],
        )
        fv = f.compute(bar, snapshot=snap)
        assert fv.raw_value < 0

    def test_value_in_range(self):
        """所有输出必须在 [-1, 1]。"""
        f = self._make()
        for i in range(20):
            bar = make_bar(ts=i * 180_000, close=67000 + i * 10)
            snap = make_snapshot(
                ts=bar.ts + 100,
                bids=[(Decimal("66999"), Decimal("20")) for _ in range(10)],
                asks=[(Decimal("67001"), Decimal("10")) for _ in range(10)],
            )
            fv = f.compute(bar, snapshot=snap)
            assert -1.0 <= fv.value <= 1.0

    def test_stale_does_not_pollute_history(self):
        """stale 不污染 _raw_history。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        f.compute(bar, snapshot=None)   # stale
        assert len(f._raw_history) == 0

    def test_stale_does_not_update_prev_smoothed(self):
        """stale 不更新 _prev_smoothed。"""
        f = self._make()
        bar1 = make_bar(ts=0, close=67000)
        snap1 = make_snapshot(ts=100)
        f.compute(bar1, snapshot=snap1)
        prev = f._prev_smoothed
        bar2 = make_bar(ts=180_000, close=67100)
        f.compute(bar2, snapshot=None)
        assert f._prev_smoothed == prev

    def test_reset(self):
        f = self._make()
        bar = make_bar(ts=0, close=67000)
        f.compute(bar, snapshot=make_snapshot(ts=100))
        f.reset()
        assert f._prev_smoothed is None


# ============================================================================
# 成交量因子
# ============================================================================

class TestVolumeFactor:

    def _make(self, config: Optional[Dict] = None) -> VolumeFactor:
        cfg = {
            "factors": {
                "quantile_window": 500,
                "warmup_bars": 5,
                "volume": {"ma_window": 20},
            }
        }
        if config:
            cfg["factors"].update(config)
        return VolumeFactor(cfg)

    def test_init_defaults(self):
        f = self._make()
        assert f.ma_window == 20
        assert f.normalized_range == (0.0, 1.0)

    def test_init_invalid_ma_window(self):
        with pytest.raises(ValueError, match="ma_window"):
            self._make({"volume": {"ma_window": 1}})

    def test_warmup_returns_warmup_state(self):
        """历史不足 ma_window 时，返回 WARMUP。"""
        f = self._make()
        for i in range(10):
            bar = make_bar(ts=i * 180_000, close=67000, volume=1000)
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert fv.state == FactorState.WARMUP

    def test_self_correlation_trap(self):
        """关键测试：不能含当前 K 线自身。"""
        f = self._make({"volume": {"ma_window": 3}})
        # 历史：[100, 100, 100]
        for v in [100, 100, 100]:
            bar = make_bar(ts=0, close=67000, volume=v)
            f.on_bar_close(bar)
            f.compute(bar)
        # 当前：volume = 200
        # 正确：ratio = 200 / ((100+100+100)/3) = 200/100 = 2.0
        # 错误：ratio = 200 / ((100+100+200)/3) = 200/133.3 = 1.5
        bar = make_bar(ts=1000, close=67000, volume=200)
        f.on_bar_close(bar)
        fv = f.compute(bar)
        assert fv.state == FactorState.READY
        assert abs(fv.raw_value - 2.0) < 1e-6

    def test_value_in_range(self):
        """所有输出必须在 [0, 1]。"""
        f = self._make({"volume": {"ma_window": 3}})
        for i in range(20):
            bar = make_bar(
                ts=i * 180_000, close=67000,
                volume=1000 + (i * 100),
            )
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert 0.0 <= fv.value <= 1.0

    def test_zero_mean_returns_neutral(self):
        """历史均值接近 0 时，返回中性值 0.5。"""
        f = self._make({"volume": {"ma_window": 3}})
        for i in range(3):
            bar = make_bar(ts=i * 180_000, close=67000, volume=0)
            f.on_bar_close(bar)
            f.compute(bar)
        bar = make_bar(ts=1000, close=67000, volume=100)
        f.on_bar_close(bar)
        fv = f.compute(bar)
        assert fv.state == FactorState.READY
        assert fv.value == 0.5

    def test_negative_volume_returns_error(self):
        """负成交量 → ERROR。"""
        f = self._make()
        bar = make_bar(ts=0, close=67000, volume=-100)
        fv = f.compute(bar)
        assert fv.state == FactorState.ERROR

    def test_deterministic(self):
        """同输入两次调用结果一致。"""
        f = self._make({"volume": {"ma_window": 3}})
        bars = [
            make_bar(ts=i * 180_000, close=67000, volume=1000 + i * 100)
            for i in range(10)
        ]
        results1 = []
        for b in bars:
            f.on_bar_close(b)
            fv = f.compute(b)
            results1.append(fv.value)

        f2 = self._make({"volume": {"ma_window": 3}})
        results2 = []
        for b in bars:
            f2.on_bar_close(b)
            fv = f2.compute(b)
            results2.append(fv.value)

        assert results1 == results2

    def test_reset(self):
        f = self._make({"volume": {"ma_window": 3}})
        for i in range(5):
            bar = make_bar(ts=i * 180_000, close=67000, volume=1000)
            f.on_bar_close(bar)
            f.compute(bar)
        f.reset()
        assert len(f._volume_history) == 0
        assert f._last_ratio is None


# ============================================================================
# 因子注册表
# ============================================================================

class TestFactorRegistry:

    def _make_registry(self) -> FactorRegistry:
        cfg = {
            "factors": {
                "quantile_window": 500,
                "warmup_bars": 5,
                "max_compute_ms": 5.0,
                "momentum": {
                    "roc_period": 3, "roc_weight": 0.7,
                    "accel_weight": 0.3, "weight": 0.35,
                },
                "obi": {
                    "levels": 10, "decay_k": 0.15,
                    "smooth_half_life_s": 2.0, "weight": 0.40,
                },
                "volume": {"ma_window": 20, "weight": 0.25},
            },
            "data": {"kline_interval": "3m"},
        }
        reg = FactorRegistry(cfg)
        reg.register("momentum", MomentumFactor(cfg), weight=0.35)
        reg.register("obi", OBIFactor(cfg), weight=0.40)
        reg.register("volume", VolumeFactor(cfg), weight=0.25)
        return reg

    def test_register_duplicate_raises(self):
        reg = self._make_registry()
        with pytest.raises(ValueError, match="already registered"):
            reg.register("momentum", MomentumFactor({"factors": {}}))

    def test_register_negative_weight_raises(self):
        reg = FactorRegistry({"factors": {}})
        with pytest.raises(ValueError, match="weight"):
            reg.register("test", MomentumFactor({"factors": {}}), weight=-0.1)

    def test_names_in_order(self):
        reg = self._make_registry()
        assert reg.names() == ["momentum", "obi", "volume"]

    def test_compute_all_returns_all_factors(self):
        reg = self._make_registry()
        bar = make_bar(ts=0, close=67000, volume=1000)
        snap = make_snapshot(ts=100)
        result = reg.process_bar(bar, snap)
        assert set(result.keys()) == {"momentum", "obi", "volume"}
        for name, fv in result.items():
            assert fv.name == name
            assert isinstance(fv, FactorValue)

    def test_error_isolation(self):
        """一个因子崩了，其他因子仍计算。"""
        reg = self._make_registry()
        # 第一根没有 close 的 bar（momentum 会 ERROR，但其他因子能容忍）
        bar = FakeBar(
            ts=0,
            open=Decimal("67000"),
            high=Decimal("67100"),
            low=Decimal("66900"),
            close=None,   # momentum 崩
            volume=Decimal("1000"),
        )
        snap = make_snapshot(ts=100)
        result = reg.process_bar(bar, snap)
        assert result["momentum"].state == FactorState.ERROR
        # 其他因子即使 WARMUP 也正常返回
        assert "obi" in result
        assert "volume" in result

    def test_weights_normalized_all_usable(self):
        """全部可用时，权重归一化后不变。"""
        reg = self._make_registry()
        # 推进足够 K 线让所有因子 READY
        for i in range(30):
            bar = make_bar(
                ts=i * 180_000, close=67000 + i * 10, volume=1000,
            )
            snap = make_snapshot(ts=bar.ts + 100)
            reg.process_bar(bar, snap)
        weights = reg.get_weights()
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)
        assert weights["momentum"] == pytest.approx(0.35, abs=1e-6)
        assert weights["obi"] == pytest.approx(0.40, abs=1e-6)
        assert weights["volume"] == pytest.approx(0.25, abs=1e-6)

    def test_weights_normalized_partial(self):
        """部分因子不可用时，权重按比例归一化。"""
        reg = self._make_registry()
        # 手动设置：obi 为 DEGRADED
        reg._factors["obi"]._state = FactorState.DEGRADED
        reg._factors["obi"]._last_value = None

        weights = reg.get_weights()
        # 只有 momentum 和 volume 可用
        assert "obi" not in weights
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)
        # momentum 0.35 / (0.35+0.25) = 0.583
        assert weights["momentum"] == pytest.approx(0.583, abs=0.01)
        assert weights["volume"] == pytest.approx(0.417, abs=0.01)

    def test_weights_empty_when_none_usable(self):
        """全部不可用时，返回空 dict。"""
        reg = self._make_registry()
        for name in reg.names():
            reg._factors[name]._state = FactorState.DEGRADED
            reg._factors[name]._last_value = None
        weights = reg.get_weights()
        assert weights == {}

    def test_is_ready(self):
        reg = self._make_registry()
        assert reg.is_ready() is False
        for i in range(30):
            bar = make_bar(
                ts=i * 180_000, close=67000 + i * 10, volume=1000,
            )
            snap = make_snapshot(ts=bar.ts + 100)
            reg.process_bar(bar, snap)
        assert reg.is_ready() is True

    def test_get_status(self):
        reg = self._make_registry()
        bar = make_bar(ts=0, close=67000, volume=1000)
        snap = make_snapshot(ts=100)
        reg.process_bar(bar, snap)
        status = reg.get_status()
        assert set(status.keys()) == {"momentum", "obi", "volume"}
        assert all(isinstance(v, str) for v in status.values())

    def test_health(self):
        reg = self._make_registry()
        h = reg.health()
        assert h["factor_count"] == 3
        assert "usable_count" in h
        assert "weights_normalized" in h
        assert "factors" in h


# ============================================================================
# 归一化一致性
# ============================================================================

class TestNormalizationRange:

    def test_momentum_range(self):
        """动量因子输出在 [-1, 1]。"""
        cfg = {"factors": {"quantile_window": 10, "momentum": {}}}
        f = MomentumFactor(cfg)
        for i in range(50):
            bar = make_bar(ts=i * 180_000, close=67000 + (i - 25) * 10)
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert -1.0 <= fv.value <= 1.0

    def test_volume_range(self):
        """成交量因子输出在 [0, 1]。"""
        cfg = {"factors": {"quantile_window": 10, "volume": {"ma_window": 3}}}
        f = VolumeFactor(cfg)
        for i in range(50):
            bar = make_bar(
                ts=i * 180_000, close=67000, volume=1000 + i * 100,
            )
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert 0.0 <= fv.value <= 1.0


# ============================================================================
# 预热期行为
# ============================================================================

class TestWarmupBehavior:

    def test_warmup_does_not_pollute_raw_history(self):
        """预热期（WARMUP 状态）不进 _raw_history。"""
        cfg = {"factors": {"quantile_window": 100, "momentum": {}}}
        f = MomentumFactor(cfg)
        # 前 3 根历史不足，返回 WARMUP
        for i in range(3):
            bar = make_bar(ts=i * 180_000, close=67000 + i * 10)
            f.on_bar_close(bar)
            fv = f.compute(bar)
            assert fv.state == FactorState.WARMUP

        # _raw_history 仍为空
        assert len(f._raw_history) == 0

        # 第 4 根开始有值，进 _raw_history
        bar = make_bar(ts=3 * 180_000, close=67030)
        f.on_bar_close(bar)
        f.compute(bar)
        assert len(f._raw_history) == 1
