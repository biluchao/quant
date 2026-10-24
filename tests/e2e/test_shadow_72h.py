"""
tests/e2e/test_shadow_72h.py — 影子模式 E2E 测试

默认短时长（约 3 秒）用于 CI 回归，验证：
1. ShadowEngine 能从启动到关闭完整跑通
2. 信号跟踪器能收集数据
3. 数据质量能追踪
4. 报告能落盘
5. 外部 stop() 能优雅终止
6. 关闭时无资源泄漏

完整 72 小时验收测试：
- 由 SHADOW_E2E_FULL=1 环境变量保护，CI 不跑
- 验证真实时长、信号数量、错误数
- 运行命令：SHADOW_E2E_FULL=1 pytest tests/e2e/test_shadow_72h.py -m slow -s

设计决策：
1. 用 SimulatedClock 替换 HybridClock：
   - HybridClock 是实时的，会尝试真实网络同步
   - SimulatedClock 是确定的，K 线时间与 now_ms 一致
   - 让整个测试可复现，无外部依赖

2. 用 SimpleFeed 替换 LiveSnapshotFeed：
   - 原 LiveSnapshotFeed 强制要求 kline_authority，但 ShadowEngine 传的是 None
   - SimpleFeed 提供 push/latest/get_snapshot_at，接口兼容

3. 用 FakeKlineStream 同时推送 K 线和快照：
   - 快照方向与 K 线方向一致（阳线买盘强，阴线卖盘强）
   - 确保 OBI 因子有方向，信号能真正产生

4. scope="class" 的 fixture：
   - 一次启动，所有测试复用
   - 节省时间（3 秒 * 1 次，而不是 3 秒 * 8 次）
   - 每个测试检查报告的不同方面

已知限制：
- 风控的 daily_reset / market_shock 用真实时间（内部回退），
  与状态机的 SimulatedClock 时间不一致。但测试期间这两者都不触发，
  不影响 E2E 验证。

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import asyncio
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from backtest.shadow_engine import ShadowEngine


# ============================================================================
# 常量
# ============================================================================
SHORT_DURATION_HOURS = 3 / 3600.0        # 3 秒（CI 默认）
SHORT_KLINE_INTERVAL_MS = 20             # 20ms/根
SHORT_WARMUP_BARS = 30
SHORT_KLINES_COUNT = 200                 # 4 秒的数据量（流比引擎活得久）


# ============================================================================
# Fake 组件
# ============================================================================

class SimpleFeed:
    """替换 LiveSnapshotFeed（接受 kline_authority=None）。"""

    def __init__(self, kline_authority=None):
        self._authority = kline_authority
        self._cache: deque = deque(maxlen=100)

    def push(self, snapshot: Dict[str, Any]) -> None:
        self._cache.append(snapshot)

    def latest(self) -> Optional[Dict[str, Any]]:
        return self._cache[-1] if self._cache else None

    def get_snapshot_at(self, ts: int) -> Optional[Dict[str, Any]]:
        if not self._cache:
            return None
        best = min(self._cache, key=lambda s: abs(s["ts"] - ts))
        if abs(best["ts"] - ts) > 500:
            return None
        return best


class FakeKlineStream:
    """
    同时推送 K 线和对应快照。

    契约：
    - 每条 K 线先推快照，再推 K 线
    - 快照方向与 K 线方向一致（阳线买盘强）
    - 可被 stop() 打断（引擎关闭时调用）
    """

    def __init__(self, klines: List[Any], interval_ms: int = 20):
        self._klines = klines
        self._interval_s = interval_ms / 1000.0
        self._running = False
        self._on_bar = None
        self._on_snapshot = None
        self._bars_sent = 0

    def attach_engine(self, engine: ShadowEngine) -> None:
        self._on_bar = engine._on_bar
        self._on_snapshot = engine._on_snapshot

    def run(self) -> None:
        self._running = True
        for bar in self._klines:
            if not self._running:
                break
            time.sleep(self._interval_s)
            if self._on_snapshot:
                self._on_snapshot(self._make_snapshot(bar))
            if self._on_bar:
                self._on_bar(bar)
                self._bars_sent += 1

    def _make_snapshot(self, bar: Any) -> Dict[str, Any]:
        close = float(bar.close)
        open_ = float(bar.open)
        if close > open_:
            bid_qty, ask_qty = 20.0, 10.0
        elif close < open_:
            bid_qty, ask_qty = 10.0, 20.0
        else:
            bid_qty, ask_qty = 10.0, 10.0
        return {
            "ts": int(bar.ts) + 180_000,
            "bids": [(close * 0.9999, bid_qty)] * 10,
            "asks": [(close * 1.0001, ask_qty)] * 10,
        }

    def stop(self) -> None:
        self._running = False


# ============================================================================
# 测试 K 线生成
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


def make_klines(
    count: int,
    base_price: float = 67000.0,
    start_ts: int = 1_700_000_000_000,
) -> List[FakeKline]:
    """
    生成合成 K 线：震荡 / 上涨 / 震荡 / 下跌交替。

    每个 phase 30 根 K 线：
    - phase 0, 2: 震荡（+50, -30 交替）
    - phase 1: 上涨（+200）
    - phase 3: 下跌（-200）
    """
    klines: List[FakeKline] = []
    price = base_price
    ts = start_ts

    for i in range(count):
        phase = (i // 30) % 4
        if phase in (0, 2):
            delta = 50 if (i % 3 == 0) else -30
        elif phase == 1:
            delta = 200
        else:
            delta = -200

        o = price
        price += delta
        klines.append(FakeKline(
            ts=ts,
            open=Decimal(str(o)),
            high=Decimal(str(max(o, price) + 20)),
            low=Decimal(str(min(o, price) - 20)),
            close=Decimal(str(price)),
            volume=Decimal("1000"),
        ))
        ts += 180_000

    return klines


# ============================================================================
# 测试配置
# ============================================================================

def make_test_config() -> Dict[str, Any]:
    """CI 短时长测试用的最小配置。"""
    return {
        "symbol": "BTCUSDT",
        "initial_equity": 10000.0,
        "data": {"kline_interval": "3m"},
        "factors": {
            "quantile_window": 100,
            "warmup_bars": SHORT_WARMUP_BARS,
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


# ============================================================================
# Fixture
# ============================================================================

@pytest.fixture(scope="class")
def shadow_run(tmp_path_factory, monkeypatch):
    """
    跑一次 ShadowEngine，所有测试共享结果。

    顺序：
    1. monkeypatch LiveSnapshotFeed → SimpleFeed
    2. monkeypatch ShadowEngine._init_clock → SimulatedClock
    3. 构造 ShadowEngine + FakeKlineStream
    4. attach engine 到 stream
    5. asyncio.run(engine.run())
    6. 返回 (engine, report, stream, tmp_path)
    """
    from backtest import shadow_engine as se_mod
    from backtest.replay_engine import SimulatedClock

    # 1. 替换 LiveSnapshotFeed
    monkeypatch.setattr(se_mod, "LiveSnapshotFeed", SimpleFeed)

    # 2. 替换 _init_clock → SimulatedClock
    def _patched_init_clock(self):
        self.clock = SimulatedClock(start_ts=1_700_000_000_000)
        # 同时注入到 config，让风控等组件也用同一个时钟
        if "instances" not in self.config:
            self.config["instances"] = {}
        self.config["instances"]["clock"] = self.clock

    monkeypatch.setattr(ShadowEngine, "_init_clock", _patched_init_clock)

    tmp_path = tmp_path_factory.mktemp("shadow_e2e")
    config = make_test_config()

    klines = make_klines(count=SHORT_KLINES_COUNT)
    kline_stream = FakeKlineStream(klines, interval_ms=SHORT_KLINE_INTERVAL_MS)

    engine = ShadowEngine(
        config=config,
        duration_hours=SHORT_DURATION_HOURS,
        seed=42,
        warmup_bars=SHORT_WARMUP_BARS,
        shadow_db_path=str(tmp_path / "shadow_test.db"),
        report_dir=str(tmp_path / "reports"),
        inject_data_streams=[kline_stream],
    )

    kline_stream.attach_engine(engine)

    report = asyncio.run(engine.run())

    return {
        "engine": engine,
        "report": report,
        "stream": kline_stream,
        "tmp_path": tmp_path,
    }


# ============================================================================
# E2E 测试（CI 默认）
# ============================================================================

class TestShadowEngineE2E:
    """CI 短时长 E2E：验证引擎生命周期完整。"""

    def test_engine_runs_to_completion(self, shadow_run):
        """引擎能完整跑完，返回 final 报告。"""
        report = shadow_run["report"]
        assert report is not None, "engine.run() returned None"
        assert report.kind == "final"

    def test_engine_records_runtime(self, shadow_run):
        """runtime_s 应该 > 0 且 <= duration + 少量开销。"""
        report = shadow_run["report"]
        assert report.runtime_s > 0
        # 短时长 3 秒，加 1 秒开销容忍
        assert report.runtime_s <= SHORT_DURATION_HOURS * 3600 + 1.0

    def test_engine_received_klines(self, shadow_run):
        """流推送的 K 线应被引擎接收。"""
        report = shadow_run["report"]
        assert report.bars_received > 0, (
            f"engine received no bars. stream sent "
            f"{shadow_run['stream']._bars_sent}"
        )

    def test_signal_tracker_collects(self, shadow_run):
        """信号跟踪器收集数据（可能 0 个信号，但结构完整）。"""
        report = shadow_run["report"]
        stats = report.signal_stats
        assert "total_recorded" in stats
        assert "by_type" in stats
        assert "by_direction" in stats
        assert "per_horizon" in stats
        # 记录数 >= 0
        assert stats["total_recorded"] >= 0

    def test_data_quality_tracked(self, shadow_run):
        """数据质量跟踪器至少记录了 kline 计数。"""
        report = shadow_run["report"]
        dq = report.data_quality
        assert "counts" in dq
        assert dq["counts"]["kline"] > 0

    def test_report_saved_to_disk(self, shadow_run):
        """最终报告应保存到磁盘。"""
        tmp_path = shadow_run["tmp_path"]
        report_dir = Path(tmp_path) / "reports"
        assert report_dir.exists(), f"report dir not created: {report_dir}"
        reports = list(report_dir.glob("shadow_final_*.json"))
        assert len(reports) >= 1, (
            f"no final report saved in {report_dir}, "
            f"files: {list(report_dir.iterdir())}"
        )

    def test_report_structure(self, shadow_run):
        """报告 dict 应包含所有关键段。"""
        d = shadow_run["report"].to_dict()
        required = [
            "kind", "ts", "runtime_s", "bars_received",
            "signal_stats", "round_stats", "data_quality",
            "virtual_account", "errors", "warnings",
        ]
        for key in required:
            assert key in d, f"report missing key: {key}"


# ============================================================================
# 外部停止测试
# ============================================================================

class TestShadowEngineExternalStop:
    """外部 stop() 能优雅终止引擎。"""

    def test_external_stop(self, tmp_path, monkeypatch):
        from backtest import shadow_engine as se_mod
        from backtest.replay_engine import SimulatedClock

        monkeypatch.setattr(se_mod, "LiveSnapshotFeed", SimpleFeed)

        def _patched_init_clock(self):
            self.clock = SimulatedClock(start_ts=1_700_000_000_000)
        monkeypatch.setattr(ShadowEngine, "_init_clock", _patched_init_clock)

        config = make_test_config()
        config["factors"]["warmup_bars"] = 5

        klines = make_klines(count=1000)
        kline_stream = FakeKlineStream(klines, interval_ms=50)

        engine = ShadowEngine(
            config=config,
            duration_hours=10.0,  # 10 小时（不会自然到期）
            seed=42,
            warmup_bars=5,
            shadow_db_path=str(tmp_path / "shadow.db"),
            report_dir=str(tmp_path / "reports"),
            inject_data_streams=[kline_stream],
        )
        kline_stream.attach_engine(engine)

        async def _run_and_stop():
            task = asyncio.create_task(engine.run())
            await asyncio.sleep(1.0)
            await engine.stop()
            return await task

        report = asyncio.run(_run_and_stop())

        assert report is not None
        assert report.kind == "final"
        # 1 秒停止，远小于 10 小时
        assert report.runtime_s < 5.0


# ============================================================================
# 优雅关闭测试
# ============================================================================

class TestShadowEngineGracefulShutdown:

    def test_stream_stopped(self, shadow_run):
        """关闭后数据流应停止。"""
        assert shadow_run["stream"]._running is False

    def test_no_active_threads_left(self, shadow_run):
        """关闭后不应有 leftover 的 shadow-* 线程。"""
        # 给线程 0.5 秒退出
        time.sleep(0.5)
        leftover = [
            t for t in threading.enumerate()
            if t.name.startswith("shadow-") and t.is_alive()
        ]
        assert len(leftover) == 0, (
            f"leftover threads: {[t.name for t in leftover]}"
        )


# ============================================================================
# 完整 72 小时验收测试（需要显式开启）
# ============================================================================

@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("SHADOW_E2E_FULL") != "1",
    reason="Full 72h test requires SHADOW_E2E_FULL=1",
)
class TestShadowFull72h:
    """
    完整 72 小时验收测试。

    运行：
        SHADOW_E2E_FULL=1 pytest tests/e2e/test_shadow_72h.py::TestShadowFull72h -m slow -s

    注意：
    - 这个测试真的会跑 72 小时
    - 用 60 秒间隔的 K 线模拟分钟级推送
    - CI 不会跑（需要显式设置环境变量）
    """

    def test_full_72h_acceptance(self, tmp_path_factory, monkeypatch):
        from backtest import shadow_engine as se_mod
        from backtest.replay_engine import SimulatedClock

        monkeypatch.setattr(se_mod, "LiveSnapshotFeed", SimpleFeed)

        def _patched_init_clock(self):
            self.clock = SimulatedClock(start_ts=1_700_000_000_000)
            if "instances" not in self.config:
                self.config["instances"] = {}
            self.config["instances"]["clock"] = self.clock
        monkeypatch.setattr(ShadowEngine, "_init_clock", _patched_init_clock)

        tmp_path = tmp_path_factory.mktemp("shadow_full")

        config = make_test_config()
        config["factors"]["warmup_bars"] = 50

        # 72 小时，60 秒/根 K 线 → 4320 根
        kline_count = 4320
        klines = make_klines(count=kline_count)
        kline_stream = FakeKlineStream(klines, interval_ms=60000)

        engine = ShadowEngine(
            config=config,
            duration_hours=72.0,
            seed=42,
            warmup_bars=50,
            shadow_db_path=str(tmp_path / "shadow.db"),
            report_dir=str(tmp_path / "reports"),
            inject_data_streams=[kline_stream],
        )
        kline_stream.attach_engine(engine)

        report = asyncio.run(engine.run())

        # 验收标准
        expected_s = 72.0 * 3600
        assert report.runtime_s >= expected_s * 0.95, (
            f"runtime {report.runtime_s}s < 95% of {expected_s}s"
        )
        assert report.signal_stats["total_recorded"] > 0, (
            "no signals recorded in 72h"
        )
        assert len(report.errors) == 0, (
            f"errors during 72h: {report.errors[:5]}"
)
