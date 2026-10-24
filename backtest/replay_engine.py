"""
backtest/replay_engine.py — 历史回放引擎（可复用核心）

从 TickDB 加载的历史 K 线驱动状态机完成决策流程，返回结构化结果。

定位：
- 可复用的核心引擎，不是入口脚本
- run_backtest.py 是入口，负责参数解析 + 日志 + 调用本引擎
- shadow_engine.py / 测试脚本 / 参数扫描工具都可以直接用

职责（唯一）：
1. 接收 config + klines + 初始资金 + seed
2. 按顺序回放 K 线，驱动状态机
3. 主循环中处理模拟时钟、因子、信号、状态、轮次
4. 结束强制平仓
5. 返回 ReplayResult

不做什么：
- 不做参数解析（run_backtest.py 负责）
- 不做日志初始化（调用方负责）
- 不做数据库清理（调用方负责）
- 不做报告写盘（调用方负责）
- 不做随机种子设置（调用方负责）
- 不做 K 线加载（调用方提供）

职责边界：
- ReplayEngine：纯回放逻辑
- run_backtest.py：入口 + 参数 + 日志 + 报告
- shadow_engine.py：影子模式（实时数据 + 虚拟订单）
- 测试：直接调用 ReplayEngine.run()

关键契约（"五必须"）：
1. 必须纯回放：不改 config，不读环境变量，不做 I/O（除可选持久化）
2. 必须确定性：同 seed + 同 klines + 同 config → 同结果
3. 必须异常隔离：单根 K 线异常不中断，计入 errors
4. 必须清理：结束时强制平仓，活跃轮次全部结算
5. 必须可复用：多次调用本引擎，每次独立（用不同 config 或 klines）

主循环伪代码：
    for i, bar in enumerate(klines):
        buffer.push(bar)                 # 推入 K 线缓冲
        if i < warmup_bars:              # 预热期跳过
            continue
        clock.advance_to(bar.ts)         # 推进模拟时钟
        snapshot = _make_snapshot(bar)   # 获取/合成盘口
        try:
            state_machine.on_bar_close(bar, snapshot)
        except Exception:
            errors += 1
    _force_close_all(klines[-1])         # 强制平仓
    return _compute_stats()

组件初始化顺序（不可颠倒）：
1. clock    （无依赖）
2. buffer   （无依赖）
3. tick_db  （无依赖）
4. persister（无依赖）
5. risk     （无依赖，需要 initial_equity）
6. gateway  （依赖 clock、feed）
7. factors  （依赖 config）
8. strategy （依赖 config）
9. rounds   （依赖 risk、persister）
10. state   （依赖 factors、strategy、risk、gateway、rounds）

快照合成：
- 优先从 TickDB 加载历史盘口
- 若无历史盘口，用 bar.close 合成（spread = close * 0.05%）
- 合成快照标记 synthetic=True，统计中单独计数

与 run_backtest.py 的关系：
    # run_backtest.py 中
    from backtest.replay_engine import ReplayEngine, ReplayResult
    
    klines = loader.load(...)
    engine = ReplayEngine(
        config=config,
        klines=klines,
        initial_equity=Decimal("10000"),
        seed=42,
    )
    result = engine.run()
    # result.to_dict() → 报告

与 shadow_engine.py 的关系：
- shadow_engine.py 复用相同的组件初始化逻辑，但用 LiveSnapshotFeed + 实时数据
- 两者的“决策链”完全一致

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal
import logging
import time
from typing import Any, Dict, List, Optional

from system.persister import StatePersister
from system.data_buffer import RingBuffer

from data.tick_db import TickDB

from execution.gateway_virtual import VirtualGateway, HistoricalFeed
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
DEFAULT_WARMUP_BARS = 500
DEFAULT_INITIAL_EQUITY = Decimal("10000")
DEFAULT_SEED = 42
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000
PROGRESS_INTERVAL_BARS = 1000
SYNTHETIC_SPREAD_PCT = Decimal("0.0005")
SYNTHETIC_LEVELS = 10
SYNTHETIC_QTY_PER_LEVEL = Decimal("10")


# ============================================================================
# 模拟时钟
# ============================================================================
class SimulatedClock:
    """
    回测时钟。

    契约：
    - 严格单调：advance_to(ts) 要求 ts >= now
    - 从 0 或指定起点开始
    - 不依赖真实时间
    - 接口与 HybridClock 兼容（now_ms / get_market_offset_ms / health_check）
    """

    def __init__(self, start_ts: int = 0):
        self._now = int(start_ts)
        self._initial = int(start_ts)
        self._advances = 0
        self._backward_attempts = 0

    def advance_to(self, ts: int) -> None:
        ts = int(ts)
        if ts < self._now:
            self._backward_attempts += 1
            raise ValueError(
                f"SimulatedClock backward: {ts} < {self._now}"
            )
        self._now = ts
        self._advances += 1

    def now_ms(self) -> int:
        return self._now

    def wall_ms(self) -> int:
        return self._now

    def exchange_ms(self) -> int:
        return self._now

    def get_market_offset_ms(self) -> float:
        return 0.0

    def health_check(self) -> Dict[str, Any]:
        return {
            "healthy": True,
            "now_ms": self._now,
            "initial_ms": self._initial,
            "advances": self._advances,
            "backward_attempts": self._backward_attempts,
        }

    def reset(self) -> None:
        self._now = self._initial
        self._advances = 0
        self._backward_attempts = 0


# ============================================================================
# 回放结果
# ============================================================================
@dataclass
class ReplayResult:
    """
    回放结果。不可变，可序列化。

    所有字段都是"可直接用"的：
    - 数字是原生类型（int / float / str）
    - 列表是 dict 列表
    - 无 Decimal / Enum / 自定义对象
    """
    # ---- 元信息 ----
    total_bars: int = 0
    warmup_bars: int = 0
    processed_bars: int = 0
    duration_ms: float = 0.0
    seed: int = DEFAULT_SEED

    # ---- 轮次统计 ----
    total_rounds: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    win_rate: float = 0.0

    # ---- 盈亏统计 ----
    total_pnl: str = "0"
    avg_r: float = 0.0
    median_r: float = 0.0
    profit_factor: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_estimate: float = 0.0

    # ---- 最终状态 ----
    final_equity: str = "0"
    initial_equity: str = "0"
    final_state: str = "idle"
    forced_close_count: int = 0

    # ---- 错误与告警 ----
    errors: int = 0
    error_messages: List[str] = field(default_factory=list)
    synthetic_snapshots: int = 0
    warnings: List[str] = field(default_factory=list)

    # ---- 原始数据（供进一步分析）----
    rounds: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """序列化为 JSON 兼容字典。"""
        return {
            "meta": {
                "total_bars": self.total_bars,
                "warmup_bars": self.warmup_bars,
                "processed_bars": self.processed_bars,
                "duration_ms": self.duration_ms,
                "seed": self.seed,
                "synthetic_snapshots": self.synthetic_snapshots,
            },
            "rounds_summary": {
                "total_rounds": self.total_rounds,
                "wins": self.wins,
                "losses": self.losses,
                "breakeven": self.breakeven,
                "win_rate": self.win_rate,
                "forced_close_count": self.forced_close_count,
            },
            "pnl_summary": {
                "initial_equity": self.initial_equity,
                "final_equity": self.final_equity,
                "total_pnl": self.total_pnl,
                "avg_r": self.avg_r,
                "median_r": self.median_r,
                "profit_factor": self.profit_factor,
                "max_drawdown_pct": self.max_drawdown_pct,
                "sharpe_estimate": self.sharpe_estimate,
            },
            "final_state": self.final_state,
            "errors": {
                "count": self.errors,
                "messages": list(self.error_messages),
            },
            "warnings": list(self.warnings),
            "rounds": list(self.rounds),
        }


# ============================================================================
# ReplayEngine
# ============================================================================
class ReplayEngine:
    """
    历史回放引擎。详见模块 docstring。

    用法：
        from backtest.replay_engine import ReplayEngine
        from decimal import Decimal

        engine = ReplayEngine(
            config=config,          # 完整配置 dict（base + step1 + symbol）
            klines=klines,          # List[KlineData]，已按时间升序
            initial_equity=Decimal("10000"),
            seed=42,
            warmup_bars=500,
        )
        result = engine.run()
        print(result.to_dict())
    """

    name = "replay_engine"
    version = "1.0.0"

    def __init__(
        self,
        config: Dict[str, Any],
        klines: List[Any],
        initial_equity: Decimal = DEFAULT_INITIAL_EQUITY,
        seed: int = DEFAULT_SEED,
        warmup_bars: int = DEFAULT_WARMUP_BARS,
        replay_db_path: str = "backtest_replay.db",
    ):
        # ---- 参数校验 ----
        if not config:
            raise ValueError("ReplayEngine: config is required")
        if not klines:
            raise ValueError("ReplayEngine: klines must not be empty")
        if len(klines) <= warmup_bars:
            raise ValueError(
                f"ReplayEngine: klines ({len(klines)}) <= "
                f"warmup_bars ({warmup_bars}), cannot replay"
            )
        if warmup_bars < 0:
            raise ValueError(
                f"ReplayEngine: warmup_bars must be >= 0, "
                f"got {warmup_bars}"
            )
        if initial_equity <= 0:
            raise ValueError(
                f"ReplayEngine: initial_equity must be > 0, "
                f"got {initial_equity}"
            )

        self.config = config
        self.klines = list(klines)
        self.initial_equity = Decimal(str(initial_equity))
        self.seed = int(seed)
        self.warmup_bars = int(warmup_bars)
        self.replay_db_path = replay_db_path

        # ---- 组件（延迟构建）----
        self.clock: Optional[SimulatedClock] = None
        self.buffer: Optional[RingBuffer] = None
        self.tick_db: Optional[TickDB] = None
        self.persister: Optional[StatePersister] = None
        self.risk: Optional[PolicyMinimal] = None
        self.gateway: Optional[VirtualGateway] = None
        self.factor_registry: Optional[FactorRegistry] = None
        self.strategy: Optional[TrendRoundStrategy] = None
        self.rounds: Optional[RoundManager] = None
        self.state_machine: Optional[StateMachine] = None

        # ---- 统计 ----
        self._errors: List[str] = []
        self._warnings: List[str] = []
        self._synthetic_snapshots = 0

        # ---- K 线索引（用于快照合成）----
        self._symbol = config.get("symbol", "BTCUSDT").upper()
        self._kline_interval_ms = self._parse_interval_ms(
            (config.get("data") or {}).get("kline_interval", "3m")
        )

    # ========================================================================
    # 主入口
    # ========================================================================

    def run(self) -> ReplayResult:
        """
        执行回放。

        契约：
        - 每次调用独立：多次调 run() 效果相同
        - 不修改 config
        - 不抛异常（内部全 catch，结果记录到 ReplayResult）
        """
        t0 = time.perf_counter()

        # ---- 1. 构建组件 ----
        try:
            self._bootstrap()
        except Exception as e:
            logger.error(
                f"ReplayEngine bootstrap failed: {e}",
                exc_info=True,
            )
            return ReplayResult(
                total_bars=len(self.klines),
                warmup_bars=self.warmup_bars,
                seed=self.seed,
                initial_equity=str(self.initial_equity),
                errors=1,
                error_messages=[f"bootstrap failed: {e}"],
            )

        # ---- 2. 主循环 ----
        self._run_loop()

        # ---- 3. 强制平仓 ----
        forced_close = self._force_close_all()

        # ---- 4. 统计 ----
        result = self._compute_result(
            t0=t0, forced_close=forced_close
        )

        return result

    # ========================================================================
    # 组件构建
    # ========================================================================

    def _bootstrap(self) -> None:
        """按顺序构建所有组件。"""
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

    def _init_clock(self) -> None:
        """从第一根 K 线初始化模拟时钟。"""
        first_ts = int(getattr(self.klines[0], "ts", 0))
        if first_ts <= 0:
            raise ValueError("first kline has invalid ts")
        self.clock = SimulatedClock(start_ts=first_ts)

    def _init_buffer(self) -> None:
        """K 线环形缓冲区。"""
        self.buffer = RingBuffer(maxlen=1000)

    def _init_tick_db(self) -> None:
        """回测数据源（可能有盘口快照）。"""
        try:
            self.tick_db = TickDB(self.replay_db_path)
        except Exception as e:
            logger.warning(
                f"TickDB init failed, falling back to synthetic: {e}"
            )
            self.tick_db = None
            self._warnings.append(f"tick_db init failed: {e}")

    def _init_persister(self) -> None:
        """状态持久化（独立回测数据库）。"""
        self.persister = StatePersister(self.replay_db_path)
        self.persister.init_schema()

    def _init_risk(self) -> None:
        """风控（使用初始资金）。"""
        risk_config = dict(self.config)
        risk_config["initial_equity"] = float(self.initial_equity)
        self.risk = PolicyMinimal(risk_config)

    def _init_gateway(self) -> None:
        """虚拟网关 + 历史数据源。"""
        # 历史数据源
        if self.tick_db is not None:
            feed = HistoricalFeed(
                tick_db=self.tick_db,
                current_ts_provider=lambda: int(self.clock.now_ms()),
            )
        else:
            # 无 TickDB，全部合成
            feed = HistoricalFeed(
                tick_db=None,
                current_ts_provider=lambda: int(self.clock.now_ms()),
            )

        # 成交模型
        fill_config = FillConfig(
            max_level_pct=Decimal("0.3"),
            fee_rate=Decimal("0.0004"),
            max_levels_used=20,
            enable_fee=True,
        )

        # 滑点模型（固定 seed）
        import numpy as np
        rng = np.random.default_rng(self.seed)
        slippage_config = SlippageConfig(
            base_bps=Decimal("2.0"),
            latency_per_ms_bps=Decimal("0.1"),
            tail_probability=0.05,
            tail_extra_bps=Decimal("20.0"),
        )
        self._slippage = SlippageModel(
            config=slippage_config, rng=rng
        )

        # 虚拟网关
        self.gateway = VirtualGateway(
            feed=feed,
            clock=self.clock,
            id_generator=None,  # 回测不需要 ID 生成器（内部生成）
            symbol=self._symbol,
            config=self.config,
            fee_rate=Decimal("0.0004"),
            latency_ms=0,  # 回测无延迟
            initial_balance=self.initial_equity,
        )

    def _init_factors(self) -> None:
        """因子注册表。"""
        self.factor_registry = FactorRegistry(self.config)
        factors_cfg = self.config.get("factors", {})

        # 动量因子
        if (factors_cfg.get("momentum") or {}).get("enabled", True):
            self.factor_registry.register(
                "momentum",
                MomentumFactor(self.config),
                weight=float(
                    (factors_cfg.get("momentum") or {}).get(
                        "weight", 0.35
                    )
                ),
            )

        # OBI 因子
        if (factors_cfg.get("obi") or {}).get("enabled", True):
            self.factor_registry.register(
                "obi",
                OBIFactor(self.config),
                weight=float(
                    (factors_cfg.get("obi") or {}).get("weight", 0.40)
                ),
            )

        # 成交量因子
        if (factors_cfg.get("volume") or {}).get("enabled", True):
            self.factor_registry.register(
                "volume",
                VolumeFactor(self.config),
                weight=float(
                    (factors_cfg.get("volume") or {}).get(
                        "weight", 0.25
                    )
                ),
            )

    def _init_strategy(self) -> None:
        """策略。"""
        self.strategy = TrendRoundStrategy(self.config)

    def _init_rounds(self) -> None:
        """轮次管理器。"""
        self.rounds = RoundManager(
            risk_policy=self.risk,
            persister=self.persister,
            clock=self.clock,
            order_gateway=self.gateway,
            id_generator=None,  # 回测不走 OrderRouter
            symbol=self._symbol,
            config=self.config,
        )

    def _init_state_machine(self) -> None:
        """状态机。"""
        # 策略字典
        strategies = {"trend_round": self.strategy}

        # 状态机
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

        # 注册状态
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

    # ========================================================================
    # 主循环
    # ========================================================================

    def _run_loop(self) -> None:
        """回放主循环。"""
        klines = self.klines
        n = len(klines)
        warmup = self.warmup_bars

        logger.info(
            f"ReplayEngine starting: "
            f"total={n} bars, warmup={warmup}, "
            f"symbol={self._symbol}, seed={self.seed}"
        )

        for i, bar in enumerate(klines):
            try:
                # 推入缓冲区（供状态机读取最近 K 线）
                self.buffer.push(bar)

                # 预热期跳过决策
                if i < warmup:
                    continue

                # 推进时钟
                self.clock.advance_to(int(bar.ts))

                # 获取/合成快照
                snapshot = self._make_snapshot(bar)

                # 驱动状态机
                self.state_machine.on_bar_close(bar, snapshot)

                # 进度报告
                if (i - warmup) % PROGRESS_INTERVAL_BARS == 0:
                    logger.info(
                        f"replay progress: {i}/{n} "
                        f"({(i/warmup if warmup else 0):.1%})"
                    )
            except Exception as e:
                self._errors.append(
                    f"bar#{i} ts={getattr(bar, 'ts', '?')}: {e}"
                )
                logger.error(
                    f"ReplayEngine bar#{i} error: {e}",
                    exc_info=True,
                )
                # 不中断，继续下一根

        logger.info(
            f"ReplayEngine loop done: "
            f"processed={n - warmup}, errors={len(self._errors)}"
        )

    def _make_snapshot(self, bar: Any) -> Optional[Dict[str, Any]]:
        """
        获取盘口快照。

        契约：
        - 优先从 TickDB 查询
        - 无历史盘口时，用 bar.close 合成
        - 合成快照标记 synthetic=True
        """
        # 从 TickDB 查（如果支持）
        if self.tick_db is not None and hasattr(
            self.tick_db, "query_snapshots"
        ):
            try:
                snap = self.tick_db.query_snapshots(
                    symbol=self._symbol,
                    start_ts=int(bar.ts),
                    end_ts=int(bar.ts) + 500,
                )
                if snap:
                    s = snap[0]
                    return {
                        "ts": int(s.get("ts", bar.ts)),
                        "bids": s.get("bids", []),
                        "asks": s.get("asks", []),
                    }
            except Exception as e:
                logger.debug(f"query_snapshots failed: {e}")

        # 合成
        self._synthetic_snapshots += 1
        try:
            close = Decimal(str(bar.close))
        except Exception:
            return None

        if close <= 0:
            return None

        spread = close * SYNTHETIC_SPREAD_PCT
        bids = [
            (close - spread - i * spread, SYNTHETIC_QTY_PER_LEVEL)
            for i in range(SYNTHETIC_LEVELS)
        ]
        asks = [
            (close + spread + i * spread, SYNTHETIC_QTY_PER_LEVEL)
            for i in range(SYNTHETIC_LEVELS)
        ]

        return {
            "ts": int(bar.ts),
            "bids": bids,
            "asks": asks,
            "_synthetic": True,
        }

    # ========================================================================
    # 强制平仓
    # ========================================================================

    def _force_close_all(self) -> int:
        """
        强制平掉所有活跃轮次。

        契约：
        - 用最后一根 K 线的 close 作为退出价
        - 退出原因为 "end_of_backtest"
        - 失败轮次计入 errors
        """
        closed = 0
        try:
            last_bar = self.klines[-1]
            if self.rounds is not None:
                active = getattr(self.rounds, "_active_rounds", None)
                if active:
                    for round_id in list(active.keys()):
                        try:
                            result = self.rounds.close_round(
                                exit_reason="end_of_backtest",
                                exit_price_hint=Decimal(str(last_bar.close)),
                            )
                            if result is not None:
                                closed += 1
                        except Exception as e:
                            self._errors.append(
                                f"force close {round_id} failed: {e}"
                            )
        except Exception as e:
            logger.error(
                f"_force_close_all failed: {e}", exc_info=True
            )

        if closed > 0:
            logger.info(f"force closed {closed} active rounds")

        return closed

    # ========================================================================
    # 统计
    # ========================================================================

    def _compute_result(
        self, t0: float, forced_close: int
    ) -> ReplayResult:
        """汇总回放结果。"""
        duration_ms = (time.perf_counter() - t0) * 1000

        # 轮次
        raw_rounds: List[Dict[str, Any]] = []
        try:
            if self.rounds is not None:
                raw_rounds = self.rounds.list_closed_rounds() or []
        except Exception as e:
            self._errors.append(f"list_closed_rounds failed: {e}")

        # 盈亏
        pnls: List[Decimal] = []
        r_multiples: List[float] = []
        wins = 0
        losses = 0
        breakeven = 0

        for r in raw_rounds:
            try:
                pnl = Decimal(str(r.get("realized_pnl", "0")))
                pnls.append(pnl)
            except Exception:
                pnls.append(Decimal("0"))

            try:
                r = float(r.get("r_multiple", 0.0))
                r_multiples.append(r)
            except Exception:
                r_multiples.append(0.0)

            if pnls[-1] > 0:
                wins += 1
            elif pnls[-1] < 0:
                losses += 1
            else:
                breakeven += 1

        total_pnl = sum(pnls, Decimal("0"))
        win_rate = wins / len(raw_rounds) if raw_rounds else 0.0
        avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0
        median_r = _median(r_multiples)

        # 盈亏比
        gross_profit = sum((p for p in pnls if p > 0), Decimal("0"))
        gross_loss = abs(sum((p for p in pnls if p < 0), Decimal("0")))
        if gross_loss > 0:
            profit_factor = float(gross_profit / gross_loss)
        elif gross_profit > 0:
            profit_factor = 999.0  # 无亏损，避免 inf
        else:
            profit_factor = 0.0

        # 最大回撤（基于 equity 曲线）
        max_dd = _compute_max_drawdown(
            raw_rounds, self.initial_equity
        )

        # 夏普（基于 r_multiples）
        sharpe = _compute_sharpe(r_multiples)

        # 最终权益
        final_equity = self.initial_equity + total_pnl

        # 最终状态
        final_state = "unknown"
        try:
            if self.state_machine is not None:
                final_state = str(self.state_machine.current_state)
        except Exception:
            pass

        return ReplayResult(
            total_bars=len(self.klines),
            warmup_bars=self.warmup_bars,
            processed_bars=len(self.klines) - self.warmup_bars,
            duration_ms=duration_ms,
            seed=self.seed,
            total_rounds=len(raw_rounds),
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            win_rate=win_rate,
            total_pnl=str(total_pnl),
            avg_r=avg_r,
            median_r=median_r,
            profit_factor=profit_factor,
            max_drawdown_pct=max_dd,
            sharpe_estimate=sharpe,
            final_equity=str(final_equity),
            initial_equity=str(self.initial_equity),
            final_state=final_state,
            forced_close_count=forced_close,
            errors=len(self._errors),
            error_messages=self._errors[:50],  # 截断
            synthetic_snapshots=self._synthetic_snapshots,
            warnings=self._warnings[:20],
            rounds=raw_rounds,
        )

    # ========================================================================
    # 辅助
    # ========================================================================

    def _parse_interval_ms(self, interval: str) -> int:
        """解析 K 线周期为毫秒。"""
        mapping = {
            "1m": 60000,
            "3m": 180000,
            "5m": 300000,
            "15m": 900000,
            "30m": 1800000,
            "1h": 3600000,
            "4h": 14400000,
            "1d": 86400000,
        }
        return mapping.get(interval, DEFAULT_KLINE_INTERVAL_MS)


# ============================================================================
# 统计辅助
# ============================================================================

def _median(values: List[float]) -> float:
    """中位数。"""
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return float(s[n // 2])
    return float((s[n // 2 - 1] + s[n // 2]) / 2.0)


def _compute_max_drawdown(
    rounds: List[Dict[str, Any]], initial: Decimal
) -> float:
    """基于 equity 曲线的最大回撤。"""
    if not rounds:
        return 0.0

    sorted_rounds = sorted(
        rounds, key=lambda r: int(r.get("closed_ts", 0))
    )
    equity = initial
    peak = initial
    max_dd = Decimal("0")

    for r in sorted_rounds:
        try:
            pnl = Decimal(str(r.get("realized_pnl", "0")))
        except Exception:
            pnl = Decimal("0")
        equity += pnl
        if equity > peak:
            peak = equity
        if peak > 0:
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd

    return float(max_dd) * 100


def _compute_sharpe(r_multiples: List[float]) -> float:
    """基于 R 倍数序列的夏普估算。"""
    n = len(r_multiples)
    if n < 2:
        return 0.0
    mean = sum(r_multiples) / n
    variance = sum((x - mean) ** 2 for x in r_multiples) / (n - 1)
    import math
    std = math.sqrt(variance)
    if std < 1e-9:
        return 0.0
    return mean / std * math.sqrt(n)
