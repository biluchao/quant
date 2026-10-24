"""
backtest/shadow_engine.py — 影子模式引擎（实时数据 + 虚拟订单）

和 replay_engine.py 共享"决策链"，但数据源和执行环境不同。

定位：
- 影子模式 = 实时数据 + 虚拟订单
- 用于验证策略在"未来数据"上的表现
- 采集信号质量数据，供 Step 2 参数调优使用

与 replay_engine.py 的区别：
| 维度 | replay_engine | shadow_engine |
|---|---|---|
| 数据源 | 历史 TickDB | 实时 WebSocket |
| 时钟 | SimulatedClock | HybridClock |
| 执行 | VirtualGateway | VirtualGateway |
| 成交价 | 历史盘口 / 合成 | LiveSnapshotFeed |
| 延迟 | 0ms | 80ms（模拟真实网络） |
| 确定性 | 是（固定 seed） | 否（实时数据） |
| 时长 | 跑完即止 | 至少 N 小时 |
| 核心价值 | 策略表现 | 信号质量 + 与回测对比 |

职责（唯一）：
1. 启动实时数据流
2. 驱动状态机（与实盘同一份代码）
3. 记录所有信号和轮次
4. 生成周期性报告 + 最终报告
5. 优雅关闭

不做什么：
- 不真实下单（虚拟网关）
- 不碰真实账户
- 不做策略调参（只采集数据）
- 不修改任何 config

职责边界：
- ShadowEngine：编排 + 信号跟踪 + 报告
- StateMachine / Strategy / FactorRegistry：与实盘完全相同
- VirtualGateway：虚拟执行
- LiveSnapshotFeed：从 KlineAuthority 取最新快照
- SignalTracker：记录信号 + 前瞻期表现
- DataQualityTracker：记录数据流质量

关键契约（"七必须"）：
1. 必须跑满 N 小时：`asyncio.wait_for(main_loop, timeout=N*3600)`
2. 必须优雅关闭：数据流逐个 stop，定时器逐个 cancel，排空队列
3. 必须确定性可复现（在给定 seed 下）：虚拟网关用固定 seed
4. 必须信号跟踪：所有策略信号进 SignalTracker
5. 必须数据质量跟踪：延迟 / 缺口 / 快照过期
6. 必须内存有界：pending / completed / rounds 全部 deque(maxlen=N)
7. 必须可离线对比：报告落盘到 reports/shadow/

生命周期：
    startup:
    1. 构建所有组件（时钟 / 风控 / 网关 / 因子 / 策略 / 轮次 / 状态机）
    2. 包装策略（注入信号跟踪器）
    3. 启动数据流（KlineStream / DepthStream / AggTradeStream）
    4. 启动心跳协程（每 5 分钟记录 alive）
    5. 启动报告协程（每小时生成报告）
    main loop:
    - 等待 duration_hours 小时
    - 期间由 WS 回调驱动状态机
    shutdown:
    1. 取消心跳 / 报告协程
    2. 停止数据流
    3. 排空队列
    4. 强制平仓
    5. 生成最终报告
    6. 关闭持久化

信号跟踪：
- 信号进入 pending 队列
- 每根 K 线到达时，检查 pending 中是否有信号达到前瞻期
- 达到 24 根 K 线时 finalize，移入 completed
- 统计：按方向 / 按状态 / 按前瞻期

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
import asyncio
import json
import logging
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

from core.strategy import (
    SignalType,
    StrategyContext,
    StrategyProvider,
    StrategySignal,
)

from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.data_buffer import RingBuffer
from system.order_idempotent import OrderIdGenerator, OrderKind

from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream
from data.tick_db import TickDB

from execution.gateway_virtual import VirtualGateway, LiveSnapshotFeed
from execution.fill_model_market import MarketFillModel, FillConfig
from execution.slippage_model import SlippageModel, SlippageConfig

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


logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_DURATION_HOURS = 72
DEFAULT_WARMUP_BARS = 500
DEFAULT_SHADOW_LATENCY_MS = 80
DEFAULT_HEARTBEAT_INTERVAL_S = 300       # 5 分钟
DEFAULT_REPORT_INTERVAL_S = 3600         # 1 小时
MAX_PENDING_SIGNALS = 1000
MAX_COMPLETED_SIGNALS = 10000
MAX_CLOSED_ROUNDS = 500
MAX_ERROR_MESSAGES = 100
HORIZONS_BARS = (3, 6, 12, 24)
DEFAULT_KLINE_INTERVAL_MS = 180_000      # 3 分钟
SHUTDOWN_TIMEOUT_S = 30.0


# ============================================================================
# 信号观察
# ============================================================================
@dataclass
class SignalObservation:
    """单个信号的观察记录。"""
    signal_id: str
    bar_ts: int
    direction: int
    score: float
    reason: str
    signal_type: str              # ENTRY / HOLD / EXIT / NOOP
    entry_price: Decimal
    forward_prices: Dict[int, Decimal] = field(default_factory=dict)
    forward_returns: Dict[int, float] = field(default_factory=dict)
    correct: Dict[int, bool] = field(default_factory=dict)
    finalized: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "bar_ts": self.bar_ts,
            "direction": self.direction,
            "score": self.score,
            "reason": self.reason,
            "signal_type": self.signal_type,
            "entry_price": str(self.entry_price),
            "forward_returns": dict(self.forward_returns),
            "correct": dict(self.correct),
            "finalized": self.finalized,
        }


# ============================================================================
# 信号跟踪器
# ============================================================================
class SignalTracker:
    """
    信号跟踪器。

    契约：
    - 线程安全（RLock）
    - pending / completed 有上限
    - 只记录，不修改信号
    - 信号记录失败不阻断状态机
    """

    def __init__(
        self,
        horizons: Tuple[int, ...] = HORIZONS_BARS,
        kline_interval_ms: int = DEFAULT_KLINE_INTERVAL_MS,
    ):
        self._horizons = tuple(sorted(horizons))
        self._max_horizon = max(self._horizons) if self._horizons else 0
        self._kline_interval_ms = int(kline_interval_ms)

        self._lock = threading.RLock()
        self._pending: Deque[SignalObservation] = deque(
            maxlen=MAX_PENDING_SIGNALS
        )
        self._completed: Deque[SignalObservation] = deque(
            maxlen=MAX_COMPLETED_SIGNALS
        )

        self._stats = {
            "total_recorded": 0,
            "total_finalized": 0,
            "by_type": {},
            "by_direction": {},
            "dropped_pending": 0,
        }

    def record(self, signal: StrategySignal, context: StrategyContext) -> None:
        """记录一个信号。"""
        try:
            bar_ts = int(getattr(context.bar, "ts", 0))
            entry_price = getattr(context.bar, "close", None)
            if bar_ts <= 0 or entry_price is None:
                return

            sig_type = getattr(signal, "type", None)
            type_str = (
                sig_type.value if hasattr(sig_type, "value")
                else str(sig_type) if sig_type else "unknown"
            )

            obs = SignalObservation(
                signal_id=str(getattr(signal, "signal_id", "")),
                bar_ts=bar_ts,
                direction=int(getattr(signal, "direction", 0)),
                score=float(getattr(signal, "score", 0.0)),
                reason=str(getattr(signal, "reason", "")),
                signal_type=type_str,
                entry_price=Decimal(str(entry_price)),
            )

            with self._lock:
                self._pending.append(obs)
                self._stats["total_recorded"] += 1
                self._stats["by_type"][type_str] = (
                    self._stats["by_type"].get(type_str, 0) + 1
                )
                dir_str = "long" if obs.direction == 1 else (
                    "short" if obs.direction == -1 else "flat"
                )
                self._stats["by_direction"][dir_str] = (
                    self._stats["by_direction"].get(dir_str, 0) + 1
                )
        except Exception as e:
            logger.error(f"SignalTracker.record failed: {e}")

    def on_bar_close(self, bar: Any) -> None:
        """每根 K 线收线时更新 pending 信号。"""
        try:
            bar_ts = int(getattr(bar, "ts", 0))
            close = getattr(bar, "close", None)
            if bar_ts <= 0 or close is None:
                return
            close_dec = Decimal(str(close))

            with self._lock:
                still_pending: Deque[SignalObservation] = deque(
                    maxlen=MAX_PENDING_SIGNALS
                )
                for obs in self._pending:
                    bars_elapsed = (bar_ts - obs.bar_ts) // self._kline_interval_ms
                    if bars_elapsed in self._horizons:
                        obs.forward_prices[bars_elapsed] = close_dec
                        if obs.direction != 0 and obs.entry_price > 0:
                            ret = float(
                                (close_dec - obs.entry_price)
                                / obs.entry_price
                                * obs.direction
                            )
                            obs.forward_returns[bars_elapsed] = ret
                            obs.correct[bars_elapsed] = ret > 0

                        if bars_elapsed >= self._max_horizon:
                            obs.finalized = True
                            self._completed.append(obs)
                            self._stats["total_finalized"] += 1
                        else:
                            still_pending.append(obs)
                    else:
                        still_pending.append(obs)

                self._pending = still_pending
        except Exception as e:
            logger.error(f"SignalTracker.on_bar_close failed: {e}")

    def stats(self) -> Dict[str, Any]:
        """信号质量统计。"""
        with self._lock:
            completed = list(self._completed)

        per_horizon: Dict[str, Dict[str, Any]] = {}
        for h in self._horizons:
            correct = [
                o for o in completed if h in o.correct
            ]
            if correct:
                n = len(correct)
                n_correct = sum(1 for o in correct if o.correct[h])
                avg_ret = sum(
                    o.forward_returns.get(h, 0.0) for o in correct
                ) / n
                per_horizon[str(h)] = {
                    "count": n,
                    "correct": n_correct,
                    "correct_rate": n_correct / n,
                    "avg_return": avg_ret,
                }
            else:
                per_horizon[str(h)] = {
                    "count": 0,
                    "correct": 0,
                    "correct_rate": 0.0,
                    "avg_return": 0.0,
                }

        # 按状态分组的准确率
        by_reason: Dict[str, Dict[str, Any]] = {}
        for o in completed:
            reason = o.reason or "unknown"
            bucket = by_reason.setdefault(reason, {
                "count": 0, "correct_6": 0
            })
            bucket["count"] += 1
            if o.correct.get(6):
                bucket["correct_6"] += 1

        for v in by_reason.values():
            v["correct_rate_6"] = (
                v["correct_6"] / v["count"] if v["count"] else 0.0
            )

        with self._lock:
            return {
                "total_recorded": self._stats["total_recorded"],
                "total_finalized": self._stats["total_finalized"],
                "pending": len(self._pending),
                "by_type": dict(self._stats["by_type"]),
                "by_direction": dict(self._stats["by_direction"]),
                "per_horizon": per_horizon,
                "by_reason": by_reason,
            }

    def get_all_completed(self) -> List[Dict[str, Any]]:
        """获取所有已完成的信号（用于落盘）。"""
        with self._lock:
            return [o.to_dict() for o in self._completed]


# ============================================================================
# 数据质量跟踪器
# ============================================================================
class DataQualityTracker:
    """跟踪数据流质量。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._last_ts: Dict[str, float] = {
            "kline": 0.0, "depth": 0.0, "aggtrade": 0.0
        }
        self._counts: Dict[str, int] = {
            "kline": 0, "depth": 0, "aggtrade": 0
        }
        self._gaps: Dict[str, int] = {
            "kline": 0, "depth": 0, "aggtrade": 0
        }
        self._stale_snapshots = 0
        self._synthetic_snapshots = 0

    def on_message(self, stream: str) -> None:
        with self._lock:
            if stream in self._last_ts:
                self._last_ts[stream] = time.time()
                self._counts[stream] += 1

    def on_gap(self, stream: str, count: int = 1) -> None:
        with self._lock:
            if stream in self._gaps:
                self._gaps[stream] += count

    def on_stale_snapshot(self) -> None:
        with self._lock:
            self._stale_snapshots += 1

    def on_synthetic_snapshot(self) -> None:
        with self._lock:
            self._synthetic_snapshots += 1

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            ages = {
                stream: (now - ts) if ts > 0 else None
                for stream, ts in self._last_ts.items()
            }
            return {
                "counts": dict(self._counts),
                "gaps": dict(self._gaps),
                "stale_snapshots": self._stale_snapshots,
                "synthetic_snapshots": self._synthetic_snapshots,
                "last_message_age_s": ages,
            }


# ============================================================================
# 影子报告
# ============================================================================
@dataclass
class ShadowReport:
    """单次报告（周期性 / 最终）。"""
    kind: str                                    # "periodic" | "final"
    ts: int
    runtime_s: float
    bars_received: int
    signal_stats: Dict[str, Any]
    round_stats: Dict[str, Any]
    data_quality: Dict[str, Any]
    virtual_account: Dict[str, Any]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "ts": self.ts,
            "runtime_s": self.runtime_s,
            "bars_received": self.bars_received,
            "signal_stats": self.signal_stats,
            "round_stats": self.round_stats,
            "data_quality": self.data_quality,
            "virtual_account": self.virtual_account,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


# ============================================================================
# 信号跟踪策略包装器
# ============================================================================
class _SignalTrackingStrategy(StrategyProvider):
    """
    包装 StrategyProvider，在返回信号前记录。

    契约：
    - 信号记录失败不影响策略
    - 原样返回原信号（不做任何修改）
    """

    def __init__(self, wrapped: StrategyProvider, tracker: SignalTracker):
        self._wrapped = wrapped
        self._tracker = tracker
        self.name = wrapped.name
        self.version = wrapped.version

    def compute(self, context: StrategyContext) -> StrategySignal:
        signal = self._wrapped.compute(context)
        if signal is not None:
            try:
                self._tracker.record(signal, context)
            except Exception as e:
                logger.error(f"signal tracking failed: {e}")
        return signal

    def on_bar_close(self, bar: Any) -> None:
        return self._wrapped.on_bar_close(bar)

    def on_round_opened(self, round_snapshot: Dict[str, Any]) -> None:
        fn = getattr(self._wrapped, "on_round_opened", None)
        if fn:
            return fn(round_snapshot)

    def on_round_closed(self, round_record: Dict[str, Any]) -> None:
        fn = getattr(self._wrapped, "on_round_closed", None)
        if fn:
            return fn(round_record)

    def health(self) -> Dict[str, Any]:
        try:
            return self._wrapped.health()
        except Exception:
            return {"name": self.name}


# ============================================================================
# ShadowEngine
# ============================================================================
class ShadowEngine:
    """
    影子模式引擎。详见模块 docstring。

    用法：
        engine = ShadowEngine(
            config=config,
            duration_hours=72,
            seed=42,
        )
        report = asyncio.run(engine.run())
        print(report.to_dict())
    """

    name = "shadow_engine"
    version = "1.0.0"

    def __init__(
        self,
        config: Dict[str, Any],
        duration_hours: float = DEFAULT_DURATION_HOURS,
        seed: int = 42,
        warmup_bars: int = DEFAULT_WARMUP_BARS,
        shadow_db_path: str = "data/shadow_quant.db",
        report_dir: str = "reports/shadow",
        inject_data_streams: Optional[List[Any]] = None,
    ):
        if not config:
            raise ValueError("ShadowEngine: config is required")
        if duration_hours <= 0:
            raise ValueError(
                f"duration_hours must be > 0, got {duration_hours}"
            )

        self.config = config
        self.duration_hours = float(duration_hours)
        self.seed = int(seed)
        self.warmup_bars = int(warmup_bars)
        self.shadow_db_path = shadow_db_path
        self.report_dir = report_dir
        self._inject_data_streams = inject_data_streams

        self._symbol = config.get("symbol", "BTCUSDT").upper()
        self._kline_interval_ms = self._parse_interval_ms(
            (config.get("data") or {}).get("kline_interval", "3m")
        )

        # ---- 组件 ----
        self.clock: Optional[HybridClock] = None
        self.buffer: Optional[RingBuffer] = None
        self.tick_db: Optional[TickDB] = None
        self.persister: Optional[StatePersister] = None
        self.risk: Optional[PolicyMinimal] = None
        self.gateway: Optional[VirtualGateway] = None
        self.factor_registry: Optional[FactorRegistry] = None
        self.strategy: Optional[StrategyProvider] = None
        self.rounds: Optional[RoundManager] = None
        self.state_machine: Optional[StateMachine] = None

        # ---- 数据流 ----
        self._kline_stream: Optional[KlineStream] = None
        self._depth_stream: Optional[DepthStream] = None
        self._agg_trade_stream: Optional[AggTradeStream] = None
        self._live_snapshot_feed: Optional[LiveSnapshotFeed] = None

        # ---- 跟踪器 ----
        self.signal_tracker = SignalTracker(
            horizons=HORIZONS_BARS,
            kline_interval_ms=self._kline_interval_ms,
        )
        self.data_quality = DataQualityTracker()

        # ---- 运行状态 ----
        self._shutdown_event = asyncio.Event()
        self._bars_received = 0
        self._start_ts = 0.0
        self._errors: Deque[str] = deque(maxlen=MAX_ERROR_MESSAGES)
        self._warnings: Deque[str] = deque(maxlen=MAX_ERROR_MESSAGES)
        self._lock = threading.RLock()
        self._periodic_report_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    def _parse_interval_ms(self, interval: str) -> int:
        mapping = {
            "1m": 60_000, "3m": 180_000, "5m": 300_000,
            "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
            "4h": 14_400_000, "1d": 86_400_000,
        }
        return mapping.get(interval, DEFAULT_KLINE_INTERVAL_MS)

    # ========================================================================
    # 主入口
    # ========================================================================

    async def run(self) -> ShadowReport:
        """
        执行影子模式。

        契约：
        - 至少跑 duration_hours 小时（除非异常）
        - 优雅关闭（数据流、定时器、队列全部排空）
        - 返回最终报告
        """
        t0 = time.perf_counter()
        self._start_ts = t0

        try:
            self._bootstrap()
        except Exception as e:
            logger.error(
                f"ShadowEngine bootstrap failed: {e}", exc_info=True
            )
            self._errors.append(f"bootstrap failed: {e}")
            return self._generate_final_report(t0)

        logger.info(
            f"ShadowEngine starting: "
            f"symbol={self._symbol} duration={self.duration_hours}h "
            f"seed={self.seed}"
        )

        try:
            self._start_data_streams()
            self._periodic_report_task = asyncio.create_task(
                self._periodic_report_loop()
            )
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop()
            )

            # 等待 duration 或 shutdown
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.duration_hours * 3600,
                )
                logger.info("ShadowEngine shutdown triggered")
            except asyncio.TimeoutError:
                logger.info(
                    f"ShadowEngine reached duration "
                    f"({self.duration_hours}h), stopping"
                )
        except Exception as e:
            logger.error(
                f"ShadowEngine run loop failed: {e}", exc_info=True
            )
            self._errors.append(f"run loop failed: {e}")
        finally:
            await self._shutdown()

        return self._generate_final_report(t0)

    async def stop(self) -> None:
        """外部触发停止。"""
        self._shutdown_event.set()

    # ========================================================================
    # 组件构建
    # ========================================================================

    def _bootstrap(self) -> None:
        self._init_clock()
        self._init_buffer()
        self._init_tick_db()
        self._init_persister()
        self._init_risk()
        self._init_gateway()
        self._init_factors()
        self._init_strategy()
        self._init_rounds()
        self._init_state_machine()
        self._init_data_streams()

    def _init_clock(self) -> None:
        self.clock = HybridClock(self.config)
        try:
            from data.rest_client import RestClient
            rest = RestClient(
                api_key=(self.config.get("binance_api_key") or ""),
                api_secret=(self.config.get("binance_api_secret") or ""),
                clock=self.clock,
                limiter=None,
            )
            self.clock.sync(rest.get_server_time())
        except Exception as e:
            logger.warning(f"clock sync failed: {e}")

    def _init_buffer(self) -> None:
        self.buffer = RingBuffer(maxlen=1000)

    def _init_tick_db(self) -> None:
        try:
            self.tick_db = TickDB(self.shadow_db_path)
        except Exception as e:
            logger.warning(f"TickDB init failed: {e}")
            self.tick_db = None

    def _init_persister(self) -> None:
        self.persister = StatePersister(self.shadow_db_path)
        self.persister.init_schema()

    def _init_risk(self) -> None:
        risk_config = dict(self.config)
        risk_config["initial_equity"] = float(
            self.config.get("initial_equity", 10000)
        )
        self.risk = PolicyMinimal(risk_config)

    def _init_gateway(self) -> None:
        # LiveSnapshotFeed 从 KlineAuthority 取最新快照
        # 但 shadow 模式不启动 KlineAuthority（避免双重数据流），
        # 所以用 depth 流自己缓存最新快照
        self._live_snapshot_feed = LiveSnapshotFeed(
            kline_authority=None  # shadow 模式由 depth 流直接喂
        )

        import numpy as np
        rng = np.random.default_rng(self.seed)
        slippage_config = SlippageConfig(
            base_bps=Decimal("2.0"),
            latency_per_ms_bps=Decimal("0.1"),
            tail_probability=0.05,
            tail_extra_bps=Decimal("20.0"),
        )
        slippage = SlippageModel(config=slippage_config, rng=rng)

        id_gen = OrderIdGenerator(
            {"order_id": {"prefix": "sh"}}
        )

        self.gateway = VirtualGateway(
            feed=self._live_snapshot_feed,
            clock=self.clock,
            id_generator=id_gen,
            symbol=self._symbol,
            config=self.config,
            fee_rate=Decimal("0.0004"),
            latency_ms=DEFAULT_SHADOW_LATENCY_MS,
            initial_balance=Decimal(
                str(self.config.get("initial_equity", 10000))
            ),
        )

    def _init_factors(self) -> None:
        self.factor_registry = FactorRegistry(self.config)
        factors_cfg = self.config.get("factors", {})

        if (factors_cfg.get("momentum") or {}).get("enabled", True):
            self.factor_registry.register(
                "momentum",
                MomentumFactor(self.config),
                weight=float(
                    (factors_cfg.get("momentum") or {}).get("weight", 0.35)
                ),
            )
        if (factors_cfg.get("obi") or {}).get("enabled", True):
            self.factor_registry.register(
                "obi",
                OBIFactor(self.config),
                weight=float(
                    (factors_cfg.get("obi") or {}).get("weight", 0.40)
                ),
            )
        if (factors_cfg.get("volume") or {}).get("enabled", True):
            self.factor_registry.register(
                "volume",
                VolumeFactor(self.config),
                weight=float(
                    (factors_cfg.get("volume") or {}).get("weight", 0.25)
                ),
            )

    def _init_strategy(self) -> None:
        raw_strategy = TrendRoundStrategy(self.config)
        self.strategy = _SignalTrackingStrategy(
            raw_strategy, self.signal_tracker
        )

    def _init_rounds(self) -> None:
        self.rounds = RoundManager(
            risk_policy=self.risk,
            persister=self.persister,
            clock=self.clock,
            order_gateway=self.gateway,
            id_generator=None,
            symbol=self._symbol,
            config=self.config,
            max_in_memory=MAX_CLOSED_ROUNDS,
        )

    def _init_state_machine(self) -> None:
        strategies = {"trend_round": self.strategy}
        self.state_machine = StateMachine(
            factor_registry=self.factor_registry,
            strategies=strategies,
            config=self.config,
            persister=self.persister,
            risk_policy=self.risk,
            order_gateway=self.gateway,
            round_manager=self.rounds,
            clock=self.clock,
            symbol=self._symbol,
            kline_buffer=self.buffer,
        )
        self.state_machine.register_state(
            "idle", IdleState(self.config)
        )
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

    def _init_data_streams(self) -> None:
        """创建数据流。可注入 mock。"""
        if self._inject_data_streams is not None:
            streams = self._inject_data_streams
            if len(streams) >= 1:
                self._kline_stream = streams[0]
            if len(streams) >= 2:
                self._depth_stream = streams[1]
            if len(streams) >= 3:
                self._agg_trade_stream = streams[2]
            return

        # 真实 WS 流
        self._kline_stream = KlineStream(
            symbol=self._symbol.lower(),
            on_bar=self._on_bar,
            on_disconnect=lambda reason: self._on_stream_disconnect(
                "kline", reason
            ),
            config=self.config,
        )
        self._depth_stream = DepthStream(
            symbol=self._symbol.lower(),
            on_snapshot=self._on_snapshot,
            config=self.config,
        )
        self._agg_trade_stream = AggTradeStream(
            symbol=self._symbol.lower(),
            on_trade=self._on_trade,
            config=self.config,
        )

    # ========================================================================
    # 数据流启动 / 停止
    # ========================================================================

    def _start_data_streams(self) -> None:
        def _run_sync(fn):
            try:
                fn()
            except Exception as e:
                logger.error(
                    f"data stream crashed: {e}", exc_info=True
                )
                self._errors.append(f"stream crashed: {e}")

        if self._kline_stream is not None:
            import threading
            t = threading.Thread(
                target=_run_sync,
                args=(self._kline_stream.run,),
                daemon=True,
                name="shadow-kline",
            )
            t.start()
        if self._depth_stream is not None:
            import threading
            t = threading.Thread(
                target=_run_sync,
                args=(self._depth_stream.run,),
                daemon=True,
                name="shadow-depth",
            )
            t.start()
        if self._agg_trade_stream is not None:
            import threading
            t = threading.Thread(
                target=_run_sync,
                args=(self._agg_trade_stream.run,),
                daemon=True,
                name="shadow-aggtrade",
            )
            t.start()
        logger.info("Data streams started")

    async def _shutdown(self) -> None:
        """优雅关闭。"""
        logger.info("ShadowEngine shutting down...")

        # 取消定时任务
        for task in (self._periodic_report_task, self._heartbeat_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        # 停止数据流
        for stream in (
            self._kline_stream,
            self._depth_stream,
            self._agg_trade_stream,
        ):
            if stream is not None and hasattr(stream, "stop"):
                try:
                    stream.stop()
                except Exception as e:
                    logger.warning(f"stream.stop failed: {e}")

        await asyncio.sleep(0.5)

        # 强制平仓
        try:
            if self.rounds is not None:
                last_bar = self.buffer.latest() if self.buffer else None
                last_close = (
                    Decimal(str(last_bar.close)) if last_bar else Decimal("0")
                )
                for rid in list(self.rounds._active_rounds.keys()):
                    try:
                        self.rounds.close_round(
                            exit_reason="shadow_shutdown",
                            exit_price_hint=last_close if last_close > 0 else None,
                        )
                    except Exception as e:
                        logger.warning(f"force close {rid} failed: {e}")
        except Exception as e:
            logger.warning(f"force close failed: {e}")

        # 关闭持久化
        if self.persister:
            try:
                self.persister.flush()
            except Exception:
                pass

        logger.info("ShadowEngine shutdown complete")

    # ========================================================================
    # 数据流回调
    # ========================================================================

    def _on_bar(self, bar: Any) -> None:
        """K 线收线回调（实时）。"""
        if getattr(bar, "is_closed", False) is False:
            return

        try:
            with self._lock:
                self._bars_received += 1
                self.data_quality.on_message("kline")

            # 推入缓冲区
            if self.buffer is not None:
                self.buffer.push(bar)

            # 信号跟踪
            self.signal_tracker.on_bar_close(bar)

            # 预热
            if self._bars_received < self.warmup_bars:
                return

            # 更新 tick_db
            if self.tick_db is not None:
                try:
                    self.tick_db.write_kline(
                        symbol=self._symbol,
                        ts=int(bar.ts),
                        open=str(bar.open),
                        high=str(bar.high),
                        low=str(bar.low),
                        close=str(bar.close),
                        volume=str(bar.volume),
                        is_closed=True,
                    )
                except Exception:
                    pass

            # 推进时钟
            try:
                self.clock.advance_to(int(bar.ts))
            except Exception as e:
                logger.warning(f"clock.advance_to failed: {e}")

            # 获取最新快照
            snapshot = None
            if self._live_snapshot_feed is not None:
                snapshot = self._live_snapshot_feed.latest()

            # 驱动状态机
            if self.state_machine is not None:
                try:
                    self.state_machine.on_bar_close(bar, snapshot)
                except Exception as e:
                    logger.error(
                        f"state machine error: {e}", exc_info=True
                    )
                    self._errors.append(
                        f"state machine ts={bar.ts}: {e}"
                    )
        except Exception as e:
            logger.error(f"_on_bar failed: {e}", exc_info=True)
            self._errors.append(f"_on_bar: {e}")

    def _on_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """深度快照回调。"""
        try:
            self.data_quality.on_message("depth")
            if self._live_snapshot_feed is not None:
                self._live_snapshot_feed.push(snapshot)
        except Exception as e:
            logger.error(f"_on_snapshot failed: {e}")

    def _on_trade(self, trade: Dict[str, Any]) -> None:
        """成交回调。"""
        try:
            self.data_quality.on_message("aggtrade")
        except Exception as e:
            logger.error(f"_on_trade failed: {e}")

    def _on_stream_disconnect(self, stream: str, reason: str) -> None:
        """数据流断开。"""
        logger.warning(
            f"stream disconnected: {stream} reason={reason}"
        )
        self._warnings.append(f"{stream} disconnect: {reason}")
        self.data_quality.on_gap(stream, 1)

    # ========================================================================
    # 周期报告 / 心跳
    # ========================================================================

    async def _periodic_report_loop(self) -> None:
        """每小时生成一次报告。"""
        try:
            while True:
                await asyncio.sleep(DEFAULT_REPORT_INTERVAL_S)
                try:
                    report = self._generate_report(
                        kind="periodic",
                        t0=self._start_ts,
                    )
                    logger.info(
                        f"periodic report: "
                        f"bars={report.bars_received} "
                        f"rounds={report.round_stats.get('total_rounds', 0)} "
                        f"signals={report.signal_stats.get('total_recorded', 0)}"
                    )
                    self._save_report(report)
                except Exception as e:
                    logger.error(
                        f"periodic report failed: {e}",
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            pass

    async def _heartbeat_loop(self) -> None:
        """每 5 分钟记录一次存活。"""
        try:
            while True:
                await asyncio.sleep(DEFAULT_HEARTBEAT_INTERVAL_S)
                try:
                    with self._lock:
                        bars = self._bars_received
                    runtime = time.perf_counter() - self._start_ts
                    logger.info(
                        f"shadow heartbeat: runtime={runtime:.0f}s "
                        f"bars={bars} "
                        f"pending_signals="
                        f"{self.signal_tracker._stats['total_recorded'] - self.signal_tracker._stats['total_finalized']}"
                    )
                except Exception as e:
                    logger.error(f"heartbeat failed: {e}")
        except asyncio.CancelledError:
            pass

    # ========================================================================
    # 报告生成
    # ========================================================================

    def _generate_report(
        self, kind: str, t0: float
    ) -> ShadowReport:
        """生成报告对象。"""
        runtime_s = time.perf_counter() - t0

        # 轮次统计
        round_stats: Dict[str, Any] = {}
        try:
            if self.rounds is not None:
                closed = self.rounds.list_closed_rounds() or []
                round_stats = self._compute_round_stats(closed)
        except Exception as e:
            logger.warning(f"round stats failed: {e}")

        # 虚拟账户
        account: Dict[str, Any] = {}
        try:
            if self.gateway is not None:
                h = self.gateway.health_check()
                account = {
                    "wallet_balance": h.get("wallet_balance"),
                    "realized_pnl": h.get("realized_pnl"),
                    "total_fees": h.get("total_fees"),
                    "positions_count": h.get("positions_count"),
                }
        except Exception as e:
            logger.warning(f"account stats failed: {e}")

        with self._lock:
            bars = self._bars_received

        return ShadowReport(
            kind=kind,
            ts=int(time.time() * 1000),
            runtime_s=runtime_s,
            bars_received=bars,
            signal_stats=self.signal_tracker.stats(),
            round_stats=round_stats,
            data_quality=self.data_quality.stats(),
            virtual_account=account,
            errors=list(self._errors),
            warnings=list(self._warnings),
        )

    def _generate_final_report(self, t0: float) -> ShadowReport:
        report = self._generate_report(kind="final", t0=t0)
        try:
            self._save_report(report)
        except Exception as e:
            logger.error(f"save final report failed: {e}")
        return report

    def _compute_round_stats(
        self, closed: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        if not closed:
            return {
                "total_rounds": 0,
                "wins": 0, "losses": 0, "breakeven": 0,
                "win_rate": 0.0,
                "total_pnl": "0",
                "avg_r": 0.0,
                "profit_factor": 0.0,
            }

        pnls: List[Decimal] = []
        r_vals: List[float] = []
        wins = losses = breakeven = 0

        for r in closed:
            try:
                pnl = Decimal(str(r.get("realized_pnl", "0")))
            except Exception:
                pnl = Decimal("0")
            pnls.append(pnl)
            try:
                r_vals.append(float(r.get("r_multiple", 0.0)))
            except Exception:
                r_vals.append(0.0)
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            else:
                breakeven += 1

        total_pnl = sum(pnls, Decimal("0"))
        gross_profit = sum((p for p in pnls if p > 0), Decimal("0"))
        gross_loss = abs(sum((p for p in pnls if p < 0), Decimal("0")))
        if gross_loss > 0:
            pf = float(gross_profit / gross_loss)
        elif gross_profit > 0:
            pf = 999.0
        else:
            pf = 0.0

        return {
            "total_rounds": len(closed),
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": wins / len(closed) if closed else 0.0,
            "total_pnl": str(total_pnl),
            "avg_r": sum(r_vals) / len(r_vals) if r_vals else 0.0,
            "profit_factor": pf,
        }

    def _save_report(self, report: ShadowReport) -> None:
        """落盘报告到 reports/shadow/。"""
        try:
            import os
            from pathlib import Path

            Path(self.report_dir).mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
            filename = f"{self.report_dir}/shadow_{report.kind}_{ts}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(
                    report.to_dict(), f,
                    ensure_ascii=False, indent=2,
                )
            logger.info(f"report saved: {filename}")
        except Exception as e:
            logger.error(f"_save_report failed: {e}", exc_info=True)


# ============================================================================
# 工厂函数
# ============================================================================
def create(
    config: Dict[str, Any],
    duration_hours: float = DEFAULT_DURATION_HOURS,
    seed: int = 42,
) -> ShadowEngine:
    """工厂函数。"""
    return ShadowEngine(
        config=config,
        duration_hours=duration_hours,
        seed=seed,
    )
