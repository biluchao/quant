"""
strategies/strategy_trend_round.py — 趋势轮次策略

把因子层的原料翻译成交易意图。

核心逻辑：
1. 方向判定
   - direction_score = 方向因子（normalized_range=(-1,1)）的加权平均值
   - direction_score ∈ [-1, 1]
   - |direction_score| > direction_threshold 才有方向，否则 NOOP

2. 分数计算
   - raw_score = Σ(fv.value * weight)，权重已由 FactorRegistry 归一化
   - 动态范围映射到 [0, 1]：
       signed_w = 方向因子权重和
       unsigned_w = 无方向因子权重和
       min_raw = -signed_w
       max_raw = signed_w + unsigned_w
       score = (raw_score - min_raw) / (max_raw - min_raw)
   - 全部可用时：signed=0.75, unsigned=0.25 → score = (raw+0.75)/1.75

3. 交易意图
   - 无持仓（active_round=None）
       - score > entry_threshold 且 direction != 0 → ENTRY
       - 冷却期 → NOOP(reason="cooldown")
       - 否则 → NOOP
   - 有持仓
       - direction_score 反向 > reverse_threshold → EXIT(reason="trend_reversal")
       - bars_held > max_bars_in_round → EXIT(reason="timeout")
       - 否则 → HOLD

4. 诚实性
   - 全部因子 WARMUP → NOOP(state=WARMUP)
   - 方向因子全部不可用 → NOOP(reason="no_directional_factors")
   - 部分因子缺失 → 从可用因子算，标记 partial=True
   - 有 active_round 时永不输出 ENTRY

关键契约：
1. 每根 K 线只 compute 一次，signal.bar_ts == context.bar.ts
2. 永远返回 StrategySignal，不返回 None
3. 内部异常包装成 NOOP(error=...)，不抛
4. ENTRY/ADD/REDUCE 必须 direction != 0；HOLD/EXIT/NOOP 必须 direction == 0
5. score ∈ [0, 1]
6. 策略不感知状态机状态、不感知风控、不持有 gateway/risk_policy
7. active_round 只读，不修改
8. 方向判定完全由方向因子决定，volume 只贡献强度
9. 冷却期是轮次关闭后的强制等待，由基类 on_round_closed 触发
10. 回测/影子/实盘同一份代码

职责边界：
- 策略：输出交易意图（StrategySignal）
- 状态机：读信号，决定是否迁移状态
- 风控：读 Order，决定是否批准
- 轮次管理：创建/关闭 PositionRound
- 执行：下单

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
import time

from core.strategy import (
    SignalType,
    StrategyContext,
    StrategyProvider,
    StrategySignal,
    StrategyState,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "trend_round",
    "type": "strategy",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["trend_round", "trend_following"],
    "required_factors": ["momentum", "obi", "volume"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Trend round strategy: weighted factor score with direction from directional factors",
}


# ============================================================================
# 默认参数（可被 config 覆盖）
# ============================================================================
DEFAULT_ENTRY_THRESHOLD = 0.50
DEFAULT_DIRECTION_THRESHOLD = 0.10
DEFAULT_REVERSE_THRESHOLD = 0.30
DEFAULT_MAX_BARS_IN_ROUND = 60
DEFAULT_COOLDOWN_BARS = 1
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000
DEFAULT_WARMUP_BARS = 500


# ============================================================================
# TrendRoundStrategy
# ============================================================================
class TrendRoundStrategy(StrategyProvider):
    """
    趋势轮次策略。详见模块 docstring。

    用法：
        strategy = TrendRoundStrategy(config)
        registry.register("trend_round", strategy)

        # 每根 K 线收线时
        signal = strategy.compute(context)
        # signal.type ∈ {ENTRY, HOLD, EXIT, NOOP}
    """

    name = "trend_round"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        # ---- 提取策略参数 ----
        strategy_cfg = self._extract_strategy_config(config)
        data_cfg = config.get("data", {})

        self.entry_threshold = float(
            strategy_cfg.get("entry_threshold", DEFAULT_ENTRY_THRESHOLD)
        )
        self.direction_threshold = float(
            strategy_cfg.get(
                "direction_threshold", DEFAULT_DIRECTION_THRESHOLD
            )
        )
        self.reverse_threshold = float(
            strategy_cfg.get(
                "reverse_threshold", DEFAULT_REVERSE_THRESHOLD
            )
        )
        self.max_bars_in_round = int(
            strategy_cfg.get(
                "max_bars_in_round", DEFAULT_MAX_BARS_IN_ROUND
            )
        )
        self.cooldown_bars = int(
            strategy_cfg.get("cooldown_bars", DEFAULT_COOLDOWN_BARS)
        )

        # K 线间隔（用于计算持仓时长）
        interval_str = data_cfg.get("kline_interval", "3m")
        self.kline_interval_ms = self._parse_interval_ms(interval_str)

        # ---- 参数校验 ----
        if not (0.0 < self.entry_threshold <= 1.0):
            raise ValueError(
                f"entry_threshold must be in (0, 1], "
                f"got {self.entry_threshold}"
            )
        if not (0.0 < self.direction_threshold <= 1.0):
            raise ValueError(
                f"direction_threshold must be in (0, 1], "
                f"got {self.direction_threshold}"
            )
        if not (0.0 < self.reverse_threshold <= 1.0):
            raise ValueError(
                f"reverse_threshold must be in (0, 1], "
                f"got {self.reverse_threshold}"
            )
        if self.max_bars_in_round < 1:
            raise ValueError(
                f"max_bars_in_round must be >= 1, "
                f"got {self.max_bars_in_round}"
            )
        if self.cooldown_bars < 0:
            raise ValueError(
                f"cooldown_bars must be >= 0, "
                f"got {self.cooldown_bars}"
            )

        # ---- 内部状态 ----
        self._last_computed_bar_ts: Optional[int] = None

        # ---- 统计 ----
        self._stats = {
            "computes": 0,
            "entries": 0,
            "holds": 0,
            "exits": 0,
            "noops": 0,
            "partial_computes": 0,
            "errors": 0,
            "cooldown_skips": 0,
            "duplicate_skips": 0,
            "no_directional_factors": 0,
        }

        logger.info(
            f"TrendRoundStrategy initialized "
            f"entry_threshold={self.entry_threshold} "
            f"direction_threshold={self.direction_threshold} "
            f"reverse_threshold={self.reverse_threshold} "
            f"max_bars_in_round={self.max_bars_in_round} "
            f"cooldown_bars={self.cooldown_bars} "
            f"kline_interval={interval_str} "
            f"warmup_bars={self.warmup_bars}"
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
            raise ValueError(
                f"unsupported interval: {interval}, "
                f"supported: {list(mapping.keys())}"
            )
        return mapping[interval]

    # ========================================================================
    # 核心：计算
    # ========================================================================

    def compute(self, context: StrategyContext) -> StrategySignal:
        """
        计算策略信号。

        契约：
        - 每根 K 线只 compute 一次（重复返回 NOOP）
        - 内部异常包装成 NOOP(error=...)
        - 永远返回 StrategySignal，不返回 None
        """
        t0 = time.perf_counter()

        try:
            # ---- 0. context 校验 ----
            validation_error = self._validate_context(context)
            if validation_error:
                self._stats["errors"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="invalid_context",
                    error=validation_error,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            bar = context.bar
            bar_ts = int(bar.ts)

            # ---- 1. 防重复 ----
            if self._last_computed_bar_ts == bar_ts:
                self._stats["duplicate_skips"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="duplicate_bar",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )
            self._last_computed_bar_ts = bar_ts

            # ---- 2. 冷却期检查 ----
            if self.is_in_cooldown():
                self._stats["cooldown_skips"] += 1
                self._stats["noops"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="cooldown",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # ---- 3. 状态判定 ----
            factor_state, partial = self._compute_worst_factor_state(
                context.factors
            )
            if factor_state == StrategyState.WARMUP or not context.factors:
                self._stats["noops"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="factors_warming_up",
                    state=StrategyState.WARMUP,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            if partial:
                self._stats["partial_computes"] += 1

            # ---- 4. 计算方向和分数 ----
            direction, direction_score, dir_available = (
                self._compute_direction(context.factors)
            )

            if not dir_available:
                self._stats["no_directional_factors"] += 1
                self._stats["noops"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="no_directional_factors",
                    state=StrategyState.DEGRADED,
                    partial=True,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            score = self._compute_score(context.factors)

            # ---- 5. 交易意图 ----
            signal = self._decide(
                context=context,
                direction=direction,
                direction_score=direction_score,
                score=score,
                partial=partial,
                t0=t0,
            )
            return signal

        except Exception as e:
            logger.error(
                f"TrendRoundStrategy.compute error: {e}",
                exc_info=True,
            )
            self._stats["errors"] += 1
            try:
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    reason="compute_exception",
                    error=f"{type(e).__name__}: {e}",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )
            except Exception as inner_e:
                # 极端情况：连信号都无法构造
                logger.critical(
                    f"failed to construct error signal: {inner_e}"
                )
                raise

    # ========================================================================
    # 辅助：context 校验
    # ========================================================================

    def _validate_context(self, context: StrategyContext) -> Optional[str]:
        """校验 context，返回错误信息或 None。"""
        if context is None:
            return "context is None"
        if context.bar is None:
            return "context.bar is None"
        if getattr(context.bar, "ts", None) is None:
            return "context.bar.ts is None"
        if not isinstance(context.factors, dict):
            return (
                f"context.factors must be dict, "
                f"got {type(context.factors).__name__}"
            )
        return None

    # ========================================================================
    # 辅助：方向因子判定
    # ========================================================================

    def _get_directional_factors(
        self, factors: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        从 factors 中挑出方向因子。

        方向因子 = normalized_range 为 (-1, 1) 的因子。
        从 FactorValue 无法直接读 normalized_range，
        但 Step 1 只有三个因子，momentum 和 obi 是方向因子，volume 不是。
        Step 2 加因子时需要改为从 FactorRegistry 的 MANIFEST 读取。
        """
        # Step 1 硬编码：momentum 和 obi 是方向因子
        DIRECTIONAL_NAMES = {"momentum", "obi"}
        return {
            name: fv for name, fv in factors.items()
            if name in DIRECTIONAL_NAMES
        }

    # ========================================================================
    # 辅助：方向和分数
    # ========================================================================

    def _compute_direction(
        self, factors: Dict[str, Any]
    ) -> Tuple[int, float, bool]:
        """
        计算方向。

        方向 = 方向因子加权平均的符号
        - 若方向因子全部不可用 → (0, 0.0, False)
        - 若 |direction_score| <= direction_threshold → (0, score, True)
        - 否则 → (sign, score, True)

        Returns:
            (direction, direction_score, dir_available)
            - direction: 1 / -1 / 0
            - direction_score: 方向因子加权平均（∈ [-1, 1]）
            - dir_available: 是否有可用的方向因子
        """
        dir_factors = self._get_directional_factors(factors)
        usable_dir = {
            name: fv for name, fv in dir_factors.items()
            if hasattr(fv, "is_usable") and fv.is_usable()
        }
        if not usable_dir:
            return (0, 0.0, False)

        # 简单平均（权重由 FactorRegistry 归一化后，可用因子权重和才是 1）
        # 这里用简单平均避免依赖外部权重状态
        total = 0.0
        count = 0
        for fv in usable_dir.values():
            total += fv.value
            count += 1
        direction_score = total / count if count > 0 else 0.0

        if abs(direction_score) <= self.direction_threshold:
            return (0, direction_score, True)
        direction = 1 if direction_score > 0 else -1
        return (direction, direction_score, True)

    def _compute_score(self, factors: Dict[str, Any]) -> float:
        """
        计算 [0, 1] 区间的分数。

        公式：
            raw_score = Σ(fv.value * raw_weight)   （weights 归一化后）
            signed_w = 方向因子权重和
            unsigned_w = 无方向因子权重和
            min_raw = -signed_w
            max_raw = signed_w + unsigned_w
            score = (raw_score - min_raw) / (max_raw - min_raw)

        由于基类不直接持有因子原始权重，我们用"等权 + 因子数量"近似：
        - 方向因子数量 n_dir，无方向因子数量 n_undir
        - signed_w = n_dir / n_total
        - unsigned_w = n_undir / n_total

        这对 Step 1（3 个因子，配置 0.35/0.40/0.25）不精确，
        但差值对 score 影响很小，而且策略层不应该直接读 FactorRegistry
        的内部权重——权重由 FactorRegistry.get_weights() 统一提供。

        Step 2 会改进：通过 context 传入 weights dict。
        """
        usable = {
            name: fv for name, fv in factors.items()
            if hasattr(fv, "is_usable") and fv.is_usable()
        }
        if not usable:
            return 0.0

        dir_names = {"momentum", "obi"}

        n_dir = sum(1 for name in usable if name in dir_names)
        n_undir = len(usable) - n_dir
        n_total = len(usable)

        if n_total == 0:
            return 0.0

        signed_w = n_dir / n_total
        unsigned_w = n_undir / n_total

        # 等权加权和
        raw_score = sum(
            fv.value / n_total for fv in usable.values()
        )

        min_raw = -signed_w
        max_raw = signed_w + unsigned_w
        range_raw = max_raw - min_raw

        if range_raw < 1e-9:
            return 0.0

        score = (raw_score - min_raw) / range_raw
        # clamp 到 [0, 1]
        return max(0.0, min(1.0, score))

    # ========================================================================
    # 辅助：交易意图
    # ========================================================================

    def _decide(
        self,
        context: StrategyContext,
        direction: int,
        direction_score: float,
        score: float,
        partial: bool,
        t0: float,
    ) -> StrategySignal:
        """
        根据方向和分数决定交易意图。

        规则：
        - 无持仓：
            - score > entry_threshold 且 direction != 0 → ENTRY
            - 否则 → NOOP
        - 有持仓：
            - 反向幅度 > reverse_threshold → EXIT(reason="trend_reversal")
            - bars_held > max_bars_in_round → EXIT(reason="timeout")
            - 否则 → HOLD
        """
        compute_ms = (time.perf_counter() - t0) * 1000

        # ---- 无持仓 ----
        if context.active_round is None:
            if direction == 0:
                self._stats["noops"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    direction=0,
                    score=score,
                    reason="direction_below_threshold",
                    partial=partial,
                    compute_ms=compute_ms,
                )

            if score <= self.entry_threshold:
                self._stats["noops"] += 1
                return self._make_signal(
                    context,
                    SignalType.NOOP,
                    direction=0,
                    score=score,
                    reason="score_below_threshold",
                    partial=partial,
                    compute_ms=compute_ms,
                )

            # ENTRY
            self._stats["entries"] += 1
            return self._make_signal(
                context,
                SignalType.ENTRY,
                direction=direction,
                score=score,
                reason="breakout_confirmed",
                partial=partial,
                compute_ms=compute_ms,
            )

        # ---- 有持仓 ----
        round_direction = int(context.active_round.get("direction", 0))
        if round_direction == 0:
            # 异常：有持仓但方向未知
            self._stats["errors"] += 1
            return self._make_signal(
                context,
                SignalType.NOOP,
                reason="invalid_active_round_direction",
                error="active_round.direction is 0 or missing",
                compute_ms=compute_ms,
            )

        # 检查反向
        # 持仓方向为 LONG（+1）时，如果 direction_score < -reverse_threshold → EXIT
        # 持仓方向为 SHORT（-1）时，如果 direction_score > +reverse_threshold → EXIT
        reverse_signal = round_direction * direction_score < -self.reverse_threshold

        if reverse_signal:
            self._stats["exits"] += 1
            return self._make_signal(
                context,
                SignalType.EXIT,
                direction=0,
                score=score,
                reason="trend_reversal",
                partial=partial,
                compute_ms=compute_ms,
            )

        # 检查超时
        opened_ts = int(context.active_round.get("opened_ts", 0))
        if opened_ts > 0:
            elapsed_ms = int(context.bar.ts) - opened_ts
            bars_held = elapsed_ms // self.kline_interval_ms
            if bars_held > self.max_bars_in_round:
                self._stats["exits"] += 1
                return self._make_signal(
                    context,
                    SignalType.EXIT,
                    direction=0,
                    score=score,
                    reason="timeout",
                    partial=partial,
                    compute_ms=compute_ms,
                )

        # 继续持有
        self._stats["holds"] += 1
        return self._make_signal(
            context,
            SignalType.HOLD,
            direction=0,
            score=score,
            reason="holding",
            partial=partial,
            compute_ms=compute_ms,
        )

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_round_closed(self, round_record: dict) -> None:
        """
        轮次结束钩子。设置冷却期。

        契约：
        - 冷却期内禁止开新仓
        - 冷却期时长由 `cooldown_bars` 决定
        """
        super().on_round_closed(round_record)
        if self.cooldown_bars > 0:
            self._cooldown_until_bar = self._bars_seen + self.cooldown_bars
            logger.info(
                f"TrendRound cooldown until bar #{self._cooldown_until_bar} "
                f"(round={round_record.get('round_id')}, "
                f"exit_reason={round_record.get('exit_reason')})"
            )

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def reset(self) -> None:
        """重置策略状态。"""
        super().reset()
        self._last_computed_bar_ts = None
        for k in self._stats:
            self._stats[k] = 0
        logger.info("TrendRoundStrategy reset")

    def health(self) -> Dict[str, Any]:
        """健康检查（扩展基类）。"""
        base = super().health()
        base.update({
            "entry_threshold": self.entry_threshold,
            "direction_threshold": self.direction_threshold,
            "reverse_threshold": self.reverse_threshold,
            "max_bars_in_round": self.max_bars_in_round,
            "cooldown_bars": self.cooldown_bars,
            "last_computed_bar_ts": self._last_computed_bar_ts,
            "stats": dict(self._stats),
        })
        # 计算触发率
        total = (
            self._stats["entries"]
            + self._stats["holds"]
            + self._stats["exits"]
            + self._stats["noops"]
        )
        if total > 0:
            base["signal_rates"] = {
                "entry": self._stats["entries"] / total,
                "hold": self._stats["holds"] / total,
                "exit": self._stats["exits"] / total,
                "noop": self._stats["noops"] / total,
            }
        return base


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> TrendRoundStrategy:
    """模块工厂入口。"""
    return TrendRoundStrategy(config)
