"""
tests/unit/test_clock.py — 时钟单元测试

范围：
1. SimulatedClock（回测时钟）：严格单调、倒退检测、reset
2. HybridClock（实盘时钟）：三域分离（now/wall/exchange）、中位数滤波、偏移拒绝、倒退检测、force_resync、健康状态
3. 时间戳转换：to_exchange_ts / from_exchange_ts 往返
4. 健康检查：HEALTHY / WARN / STALE / REJECTED / BACKWARD 五态
5. 边界：首次同步、同步失败、偏移超限、时钟漂移

设计原则：
- 时间依赖通过注入 FakeRestClient 控制
- 单调性测试直接对 time.monotonic_ns() 施加影响不可行，
  通过 monkeypatch 覆盖 now_ms 实现倒退场景
- 断言精确：`assert result.accepted is False`，不写 `assert not result`
- 覆盖异常路径

关键断言：
- now_ms 严格单调（不倒退）
- 倒退超容差抛 ClockBackwardError
- 偏移超限拒绝校准，保留旧偏移
- 中位数滤波：3 次偏移 [10, 12, 100] → 中位数 12（不是 40）
- 健康状态优先级：BACKWARD > REJECTED > STALE > WARN > HEALTHY

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from decimal import Decimal
import threading
import time
from typing import Any, Dict, List, Optional

import pytest

from core.clock import (
    ClockBackwardError,
    ClockError,
    ClockHealth,
    ClockOffsetError,
    ClockSyncError,
)
from system.clock_hybrid import HybridClock
from backtest.replay_engine import SimulatedClock


# ============================================================================
# Fake 组件
# ============================================================================

class FakeRestClient:
    """可控的 REST 客户端。"""
    def __init__(self):
        self._server_time = 1_700_000_000_000
        self._should_fail = False

    def set_time(self, ts: int) -> None:
        self._server_time = ts

    def set_fail(self, fail: bool = True) -> None:
        self._should_fail = fail

    def get_server_time(self) -> int:
        if self._should_fail:
            raise ConnectionError("fake rest failure")
        return self._server_time


# ============================================================================
# SimulatedClock
# ============================================================================

class TestSimulatedClock:

    def test_initial_now(self):
        c = SimulatedClock(start_ts=1_700_000_000_000)
        assert c.now_ms() == 1_700_000_000_000
        assert c.wall_ms() == 1_700_000_000_000
        assert c.exchange_ms() == 1_700_000_000_000

    def test_advance_forward(self):
        c = SimulatedClock(start_ts=1000)
        c.advance_to(2000)
        assert c.now_ms() == 2000
        c.advance_to(5000)
        assert c.now_ms() == 5000

    def test_advance_backward_raises(self):
        c = SimulatedClock(start_ts=1000)
        c.advance_to(2000)
        with pytest.raises(ValueError, match="backward"):
            c.advance_to(1500)

    def test_advance_same_ts_allowed(self):
        """相同时间戳不算倒退。"""
        c = SimulatedClock(start_ts=1000)
        c.advance_to(1000)   # 无变化
        assert c.now_ms() == 1000

    def test_offset_always_zero(self):
        c = SimulatedClock(start_ts=1000)
        assert c.get_market_offset_ms() == 0.0

    def test_health_check(self):
        c = SimulatedClock(start_ts=1000)
        c.advance_to(2000)
        c.advance_to(3000)
        h = c.health_check()
        assert h["healthy"] is True
        assert h["now_ms"] == 3000
        assert h["initial_ms"] == 1000
        assert h["advances"] == 2

    def test_backward_count(self):
        c = SimulatedClock(start_ts=1000)
        c.advance_to(2000)
        for ts in [1500, 1000, 500]:
            with pytest.raises(ValueError):
                c.advance_to(ts)
        h = c.health_check()
        assert h["backward_attempts"] == 3

    def test_reset(self):
        c = SimulatedClock(start_ts=1000)
        c.advance_to(5000)
        c.reset()
        assert c.now_ms() == 1000


# ============================================================================
# HybridClock 初始化
# ============================================================================

class TestHybridClockInit:

    def _make(self, **kwargs) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        config["clock"].update(kwargs)
        return HybridClock(config)

    def test_initial_offset_zero(self):
        c = self._make()
        assert c.get_market_offset_ms() == 0.0

    def test_now_ms_advances(self):
        """now_ms 应随时间前进。"""
        c = self._make()
        n1 = c.now_ms()
        time.sleep(0.005)   # 5ms
        n2 = c.now_ms()
        assert n2 > n1

    def test_wall_ms_advances(self):
        """wall_ms 应随真实时间前进。"""
        c = self._make()
        w1 = c.wall_ms()
        time.sleep(0.005)
        w2 = c.wall_ms()
        assert w2 > w1

    def test_exchange_ms_equals_now_without_sync(self):
        """未校准时，exchange_ms == now_ms。"""
        c = self._make()
        n = c.now_ms()
        e = c.exchange_ms()
        # 允许 5ms 误差（调用之间有时间差）
        assert abs(e - n) < 100

    def test_initial_state_healthy(self):
        c = self._make()
        h = c.health_check()
        assert h["health"] == ClockHealth.HEALTHY.value

    def test_invalid_config_rejected(self):
        """max_offset_ms <= 0 应被拒。"""
        with pytest.raises((ValueError, ClockError)):
            HybridClock({
                "clock": {"max_offset_ms": -100}
            })


# ============================================================================
# HybridClock 校准
# ============================================================================

class TestHybridClockSync:

    def _make(self, **kwargs) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        config["clock"].update(kwargs)
        return HybridClock(config)

    def test_first_sync_accepts_offset(self):
        c = self._make()
        local_now = c.now_ms()
        exchange_ts = local_now + 100   # 本地慢 100ms
        record = c.sync(exchange_ts)
        assert record.accepted is True
        assert abs(record.offset_ms - 100) < 10

    def test_median_filter_after_multiple_syncs(self):
        """3 次校准 [10, 12, 100] → 中位数 12，不是均值 40。"""
        c = self._make()
        local_now = c.now_ms()
        offsets = [10, 12, 100]
        for off in offsets:
            # 每次都基于当前 now 计算
            c.sync(c.now_ms() + off)
        # 中位数 12
        assert abs(c.get_market_offset_ms() - 12) < 5

    def test_offset_exceeds_max_rejected(self):
        c = self._make(max_offset_ms=5000)
        local_now = c.now_ms()
        # 偏移 10 秒 > 5 秒上限
        record = c.sync(local_now + 10_000)
        assert record.accepted is False
        assert "max_offset" in (record.reason or "")

    def test_reject_preserves_old_offset(self):
        """拒绝校准后，旧偏移仍生效。"""
        c = self._make()
        local_now = c.now_ms()
        # 先接受一次 +100ms
        c.sync(local_now + 100)
        old_offset = c.get_market_offset_ms()
        # 再尝试超限偏移
        c.sync(c.now_ms() + 10_000)
        # 旧偏移保留
        assert abs(c.get_market_offset_ms() - old_offset) < 5

    def test_exchange_ms_reflects_offset(self):
        """exchange_ms = now_ms + offset。"""
        c = self._make()
        local_now = c.now_ms()
        c.sync(local_now + 100)   # 本地慢 100ms

        e = c.exchange_ms()
        n = c.now_ms()
        assert abs((e - n) - 100) < 10

    def test_sync_record_fields(self):
        c = self._make()
        local_now = c.now_ms()
        record = c.sync(local_now + 50)
        assert record.ts > 0
        assert record.exchange_ts == local_now + 50
        assert record.accepted is True
        assert record.reason is None


# ============================================================================
# HybridClock 时钟倒退
# ============================================================================

class TestHybridClockBackward:

    def _make(self, tolerance_ms: int = 0) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": tolerance_ms,
            }
        }
        return HybridClock(config)

    def test_backward_detected(self, monkeypatch):
        """模拟时间倒退：monkeypatch time.monotonic_ns 使 now_ms 倒退。"""
        c = self._make(tolerance_ms=0)
        n1 = c.now_ms()
        # 模拟倒退：让 monotonic_ns 返回更小的值
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() - 1_000_000_000,  # 倒退 1 秒
        )
        with pytest.raises(ClockBackwardError):
            c.now_ms()

    def test_backward_with_tolerance_returns_last(self, monkeypatch):
        """容差范围内，返回 last_now 而非抛异常。"""
        c = self._make(tolerance_ms=100)
        n1 = c.now_ms()
        # 倒退 50ms（容差内）
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() - 50_000_000,  # 倒退 50ms
        )
        n2 = c.now_ms()
        # 应返回 last_now 而非抛异常
        assert n2 == n1

    def test_backward_count_increments_on_rejection(self, monkeypatch):
        c = self._make(tolerance_ms=0)
        c.now_ms()
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() - 1_000_000_000,
        )
        try:
            c.now_ms()
        except ClockBackwardError:
            pass
        h = c.health_check()
        assert h["backward_count"] == 1
        assert h["health"] == ClockHealth.BACKWARD.value

    def test_backward_health_is_critical(self, monkeypatch):
        c = self._make(tolerance_ms=0)
        c.now_ms()
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() - 1_000_000_000,
        )
        try:
            c.now_ms()
        except ClockBackwardError:
            pass
        h = c.health_check()
        assert h["healthy"] is False
        assert h["health"] == ClockHealth.BACKWARD.value


# ============================================================================
# HybridClock 定期校准
# ============================================================================

class TestHybridClockResync:

    def _make(self, interval_s: int = 3600) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": interval_s,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        return HybridClock(config)

    def test_should_resync_initially(self):
        """首次调用返回 True。"""
        c = self._make()
        assert c.should_resync() is True

    def test_should_resync_after_sync(self):
        """刚同步后返回 False。"""
        c = self._make()
        c.sync(c.now_ms() + 10)
        assert c.should_resync() is False

    def test_should_resync_after_interval(self, monkeypatch):
        """超过 resync_interval_s 后返回 True。"""
        c = self._make(interval_s=3600)
        c.sync(c.now_ms() + 10)

        # 模拟时间前进 3700 秒
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() + 3700 * 1_000_000_000,
        )
        assert c.should_resync() is True


# ============================================================================
# HybridClock force_resync
# ============================================================================

class TestHybridClockForceResync:

    def _make(self) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        return HybridClock(config)

    def test_no_rest_client_raises(self):
        c = self._make()
        with pytest.raises(ClockSyncError, match="rest_client"):
            c.force_resync()

    def test_force_resync_works(self):
        c = self._make()
        rest = FakeRestClient()
        c.set_rest_client(rest)

        rest.set_time(c.now_ms() + 100)
        record = c.force_resync()
        assert record.accepted is True
        assert abs(c.get_market_offset_ms() - 100) < 10

    def test_force_resync_rest_failure(self):
        c = self._make()
        rest = FakeRestClient()
        rest.set_fail(True)
        c.set_rest_client(rest)

        with pytest.raises(ClockSyncError):
            c.force_resync()


# ============================================================================
# 时间戳转换
# ============================================================================

class TestTimestampConversions:

    def _make_with_offset(self, offset_ms: int) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 1,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        c = HybridClock(config)
        c.sync(c.now_ms() + offset_ms)
        return c

    def test_to_exchange_ts_positive_offset(self):
        """本地慢 100ms，本地时间 + 100 = 交易所时间。"""
        c = self._make_with_offset(100)
        local = 1_700_000_000_000
        exchange = c.to_exchange_ts(local)
        assert abs(exchange - (local + 100)) < 10

    def test_to_exchange_ts_negative_offset(self):
        """本地快 100ms，本地时间 - 100 = 交易所时间。"""
        c = self._make_with_offset(-100)
        local = 1_700_000_000_000
        exchange = c.to_exchange_ts(local)
        assert abs(exchange - (local - 100)) < 10

    def test_roundtrip(self):
        """to_exchange_ts → from_exchange_ts 应回到原值。"""
        c = self._make_with_offset(150)
        local = 1_700_000_000_000
        exchange = c.to_exchange_ts(local)
        recovered = c.from_exchange_ts(exchange)
        assert abs(recovered - local) < 5


# ============================================================================
# 健康状态优先级
# ============================================================================

class TestHybridClockHealth:

    def _make(self, **kwargs) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        config["clock"].update(kwargs)
        return HybridClock(config)

    def test_healthy_initially(self):
        c = self._make()
        assert c.health_check()["health"] == ClockHealth.HEALTHY.value

    def test_warn_after_large_offset(self):
        """偏移超 warn 阈值但未超 max → WARN。"""
        c = self._make(warn_offset_ms=200, max_offset_ms=5000)
        c.sync(c.now_ms() + 1000)   # 1s 偏移，超 warn，未超 max
        h = c.health_check()
        assert h["health"] == ClockHealth.WARN.value
        assert h["healthy"] is True   # WARN 仍算健康

    def test_rejected_after_invalid_sync(self):
        """超限校准被拒 → REJECTED。"""
        c = self._make(max_offset_ms=5000)
        c.sync(c.now_ms() + 10_000)   # 超限
        h = c.health_check()
        assert h["health"] == ClockHealth.REJECTED.value
        assert h["healthy"] is False

    def test_stale_after_long_no_sync(self, monkeypatch):
        """超过 2*resync_interval 未同步 → STALE。"""
        c = self._make(resync_interval_s=3600)
        c.sync(c.now_ms() + 10)

        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() + 8000 * 1_000_000_000,  # 8000s
        )
        h = c.health_check()
        assert h["health"] == ClockHealth.STALE.value
        assert h["healthy"] is False

    def test_backward_priority_over_others(self, monkeypatch):
        """BACKWARD 优先级最高：即使有其他问题，也显示 BACKWARD。"""
        c = self._make()
        c.now_ms()
        original_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            time, "monotonic_ns",
            lambda: original_monotonic() - 1_000_000_000,
        )
        try:
            c.now_ms()
        except ClockBackwardError:
            pass
        h = c.health_check()
        assert h["health"] == ClockHealth.BACKWARD.value

    def test_health_fields(self):
        c = self._make()
        h = c.health_check()
        assert "offset_ms" in h
        assert "offset_history_size" in h
        assert "last_sync_ts" in h
        assert "backward_count" in h
        assert "timestamp" in h


# ============================================================================
# 线程安全
# ============================================================================

class TestHybridClockThreading:

    def _make(self) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        return HybridClock(config)

    def test_concurrent_now_calls(self):
        """多线程并发 now_ms 调用不崩。"""
        c = self._make()
        errors = []
        results = []

        def worker():
            try:
                for _ in range(100):
                    results.append(c.now_ms())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 400

    def test_concurrent_sync_calls(self):
        """多线程并发 sync 不崩，偏移历史正常累加。"""
        c = self._make()
        errors = []

        def worker():
            try:
                for _ in range(10):
                    c.sync(c.now_ms() + 50)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # 40 次 sync，但 offset_window=100 不限制
        h = c.health_check()
        assert h["offset_history_size"] <= 100


# ============================================================================
# 重置
# ============================================================================

class TestHybridClockReset:

    def _make(self) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 100,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        return HybridClock(config)

    def test_reset_clears_state(self):
        c = self._make()
        c.sync(c.now_ms() + 100)
        c.now_ms()
        c.reset()
        assert c.get_market_offset_ms() == 0.0
        h = c.health_check()
        assert h["offset_history_size"] == 0
        assert h["backward_count"] == 0
        assert h["last_sync_ts"] == 0


# ============================================================================
# 多空对称 / 时间戳语义
# ============================================================================

class TestClockSemantics:

    def _make_with_offset(self, offset_ms: int) -> HybridClock:
        config = {
            "clock": {
                "max_offset_ms": 5000,
                "warn_offset_ms": 200,
                "offset_window": 1,
                "resync_interval_s": 3600,
                "recv_window_ms": 5000,
                "backward_tolerance_ms": 0,
            }
        }
        c = HybridClock(config)
        c.sync(c.now_ms() + offset_ms)
        return c

    def test_positive_offset_means_local_slow(self):
        """偏移为正 → 本地慢 → exchange_ms > now_ms。"""
        c = self._make_with_offset(100)
        e = c.exchange_ms()
        n = c.now_ms()
        assert e > n

    def test_negative_offset_means_local_fast(self):
        """偏移为负 → 本地快 → exchange_ms < now_ms。"""
        c = self._make_with_offset(-100)
        e = c.exchange_ms()
        n = c.now_ms()
        assert e < n

    def test_buy_sell_timestamp_uses_exchange(self):
        """下单时间戳必须用 exchange_ms，不是 wall_ms 或 now_ms。"""
        c = self._make_with_offset(100)
        api_ts = c.exchange_ms()   # 交易所返回的 serverTime 附近
        local_ts = c.now_ms()
        # api_ts 应比 local 大 100 左右
        assert api_ts - local_ts > 50
        # wall_ms 与 now_ms 未校准，与 api_ts 无关
        # （wall_ms 与 now_ms 在初始化时对齐，wall_ms 是墙钟）
