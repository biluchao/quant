"""
core/strategy.py — 策略抽象基类与通用数据结构

策略是"市场判断引擎"，不是"下单引擎"。

策略职责：
- 读取因子值、K 线、盘口、活跃轮次
- 计算市场判断（分数、方向、信号类型）
- 输出 StrategySignal

策略不做：
- 不直接下单（那是执行层的事）
- 不决定仓位（那是风控的事）
- 不决定止损价（那是轮次管理的事）
- 不感知状态机当前状态（只输出判断，状态机自己决定怎么用）
- 不做 I/O（网络、磁盘）

关键契约：
1. 策略永远返回 StrategySignal，不返回 None
2. 没有信号时返回 NOOP，有持仓且继续持有返回 HOLD
3. 策略内部异常必须包装成 NOOP(error=...)，不得向上抛
4. 策略只输出 score + direction + type，仓位/价格/止损由风控和执行决定
5. 每根 K 线只 compute 一次，signal.bar_ts == context.bar.ts
6. 策略不持有 gateway / risk_policy 引用

信号类型语义：
- ENTRY: 建议开新轮次，direction != 0
- ADD:   建议给活跃轮次加仓，direction == 活跃轮次方向
- REDUCE:建议减仓（部分平），direction == 活跃轮次方向
- EXIT:  建议平掉活跃轮次，direction == 0
- HOLD:  有持仓，继续持有，direction == 0
- NOOP:  无信号，不做任何事，direction == 0

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4
import logging
import time

logger = logging.getLogger(__name__)


# ============================================================================
# 默认值
# ============================================================================
DEFAULT_ENTRY_THRESHOLD = 0.50
DEFAULT_WARMUP_BARS = 500
DEGRADE_THRESHOLD = 3


# ============================================================================
# 信号类型
# ============================================================================
class SignalType(str, Enum):
    """策略信号类型。"""
    ENTRY = "entry"     # 建议开新轮次
    ADD = "add"         # 建议给活跃轮次加仓
    REDUCE = "reduce"   # 建议减仓
    EXIT = "exit"       # 建议平掉活跃轮次
    HOLD = "hold"       # 有持仓，继续持有
    NOOP = "noop"       # 无信号


# 需要 direction != 0 的信号类型
_DIRECTIONAL_TYPES = {SignalType.ENTRY, SignalType.ADD, SignalType.REDUCE}


# ============================================================================
# 策略状态
# ============================================================================
class StrategyState(str, Enum):
    """策略生命周期状态。"""
    WARMUP = "warmup"       # 预热期
    READY = "ready"         # 正常
    DEGRADED = "degraded"   # 因子降级，部分功能不可用
    ERROR = "error"         # 本次计算失败


# ============================================================================
# 策略输入上下文
# ============================================================================
@dataclass(frozen=True)
class StrategyContext:
    """
    策略计算的输入。由状态机组装，传给策略。

    策略只读，不得修改。
    """
    bar: Any                                # 已收线的 K 线（ts/close/open/high/low/volume）
    snapshot: Optional[dict]                # 盘口快照，可能为 None
    factors: Dict[str, Any]                 # 因子名 -> FactorValue（core.factor.FactorValue）
    recent_bars: List[Any]                  # 最近 N 根 K 线，按时间升序，最后一根是当前 bar
    active_round: Optional[dict]            # 活跃轮次快照，None 表示无持仓
                                            # 字段：round_id / direction / weighted_avg_cost /
                                            #       total_qty / current_stop / pyramid_count /
                                            #       peak_unrealized / opened_ts
    current_state: str                      # 状态机当前状态名（策略只读，不依赖）
    now_ms: int                             # 当前时间（单调时钟）


# ============================================================================
# 策略输出信号
# ============================================================================
@dataclass(frozen=True)
class StrategySignal:
    """
    策略输出信号。不可变。

    状态机只读此结构，不关心策略内部实现。
    """
    # ---- 标识 ----
    signal_id: str                          # 信号唯一 ID（UUID）
    strategy_name: str                      # 策略名
    version: str                            # 策略版本
    bar_ts: int                             # 对应 K 线收线时间戳（毫秒）

    # ---- 判断 ----
    type: SignalType                        # 信号类型
    direction: int                          # 1=LONG, -1=SHORT, 0=无方向
    score: float                            # 0 到 1，方向无关的强度
    reason: str                             # snake_case 原因码

    # ---- 上下文（用于归因和回放）----
    factor_snapshot: Dict[str, Dict[str, Any]]  # 因子名 -> {value, raw_value, state, outlier, stale}
    factor_weights: Dict[str, float]        # 实际使用的因子权重（已归一化）
    context_meta: Dict[str, Any] = field(default_factory=dict)  # 额外上下文，如 active_round_id

    # ---- 状态 ----
    state: StrategyState = StrategyState.READY
    partial: bool = False                   # 是否有因子缺失导致部分计算
    error: Optional[str] = None             # state=ERROR 时的错误信息
    compute_ms: float = 0.0                 # 策略计算耗时

    # ------------------------------------------------------------------------
    # 校验与便捷方法
    # ------------------------------------------------------------------------

    def __post_init__(self):
        """构造后校验一致性。不通过抛 ValueError。"""
        if self.type in _DIRECTIONAL_TYPES:
            if self.direction not in (1, -1):
                raise ValueError(
                    f"signal type={self.type} requires direction ∈ {{1,-1}}, "
                    f"got {self.direction}"
                )
        else:
            if self.direction != 0:
                raise ValueError(
                    f"signal type={self.type} requires direction=0, "
                    f"got {self.direction}"
                )

        if not (0.0 <= self.score <= 1.0):
            raise ValueError(
                f"score must be in [0, 1], got {self.score}"
            )

    def is_actionable(self) -> bool:
        """状态机可用此信号做决策。"""
        return (
            self.state == StrategyState.READY
            and self.type not in (SignalType.NOOP, SignalType.HOLD)
            and not self.partial
            and self.error is None
        )

    def is_noop(self) -> bool:
        return self.type == SignalType.NOOP and self.error is None

    def to_dict(self) -> dict:
        """用于日志和归因。"""
        d = asdict(self)
        d["type"] = self.type.value
        d["state"] = self.state.value
        return d


# ============================================================================
# 策略 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class StrategyManifest:
    """
    策略 MANIFEST 字典解析后的结构。
    """
    name: str
    type: str
    version: str
    step: int
    required_factors: tuple
    capabilities: tuple
    owner: str
    fallback: Optional[str] = None
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyManifest":
        required = ("name", "type", "version", "step", "owner")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")

        if d["type"] != "strategy":
            raise ValueError(
                f"MANIFEST type must be 'strategy', got '{d['type']}'"
            )

        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            required_factors=tuple(d.get("required_factors", [])),
            capabilities=tuple(d.get("capabilities", [])),
            owner=d["owner"],
            fallback=d.get("fallback"),
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 策略抽象基类
# ============================================================================
class StrategyProvider(ABC):
    """
    策略抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.name / self.version
    2. 实现 compute(context) -> StrategySignal

    子类可选覆盖：
    - on_bar_close(bar)：每根 K 线收线后的钩子
    - on_round_opened(round_snapshot)：轮次开始时的钩子
    - on_round_closed(round_record)：轮次结束时的钩子（可用于冷却）
    - health()：自定义健康检查

    子类不应：
    - 持有 gateway / risk_policy / round_manager 引用
    - 在 compute() 中做 I/O
    - 在 compute() 中修改 context
    - 在 compute() 中直接下单
    """

    # 子类必须在 __init__ 中设置
    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        strategy_cfg = self._extract_strategy_config(config)

        self.entry_threshold = float(
            strategy_cfg.get("entry_threshold", DEFAULT_ENTRY_THRESHOLD)
        )
        self.warmup_bars = int(
            strategy_cfg.get("warmup_bars", DEFAULT_WARMUP_BARS)
        )

        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = StrategyState.WARMUP
        self._last_signal: Optional[StrategySignal] = None
        self._cooldown_until_bar: int = 0  # 冷却截止的 bar 计数

    def _extract_strategy_config(self, config: dict) -> dict:
        """从 config 中提取本策略的配置段。"""
        strategy_cfg = config.get("strategy", {})
        if not isinstance(strategy_cfg, dict):
            return {}
        if self.name and self.name in strategy_cfg:
            section = strategy_cfg[self.name]
            if isinstance(section, dict):
                merged = {
                    k: v for k, v in strategy_cfg.items()
                    if not isinstance(v, dict)
                }
                merged.update(section)
                return merged
        return {
            k: v for k, v in strategy_cfg.items()
            if not isinstance(v, dict)
        }

    # ------------------------------------------------------------------------
    # 抽象方法
    # ------------------------------------------------------------------------

    @abstractmethod
    def compute(self, context: StrategyContext) -> StrategySignal:
        """
        计算策略信号。每根 K 线收线后调用一次。

        契约：
        - 永远返回 StrategySignal，不返回 None
        - 无信号时返回 NOOP
        - 有持仓且继续持有返回 HOLD
        - 内部异常包装成 NOOP(error=...)
        - signal.bar_ts == context.bar.ts
        - 禁止 I/O，禁止直接下单
        """
        ...

    # ------------------------------------------------------------------------
    # 生命周期钩子
    # ------------------------------------------------------------------------

    def on_bar_close(self, bar) -> None:
        """
        每根 K 线收线后调用（在 compute 之前）。
        默认实现：递增 bars_seen，预热完成切换 WARMUP→READY。
        """
        self._bars_seen += 1
        if (
            self._state == StrategyState.WARMUP
            and self._bars_seen >= self.warmup_bars
        ):
            self._state = StrategyState.READY
            logger.info(
                f"strategy {self.name} warmup complete after "
                f"{self._bars_seen} bars"
            )

    def on_round_opened(self, round_snapshot: dict) -> None:
        """轮次开始钩子。默认无操作。"""
        pass

    def on_round_closed(self, round_record: dict) -> None:
        """
        轮次结束钩子。默认实现：设置 1 根 K 线的冷却期。
        子类可覆盖。
        """
        self._cooldown_until_bar = self._bars_seen + 1

    def is_in_cooldown(self) -> bool:
        return self._bars_seen < self._cooldown_until_bar

    def health(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "state": self._state.value,
            "bars_seen": self._bars_seen,
            "warmup_progress": min(
                1.0, self._bars_seen / max(self.warmup_bars, 1)
            ),
            "consecutive_errors": self._consecutive_errors,
            "in_cooldown": self.is_in_cooldown(),
            "last_signal_ts": (
                self._last_signal.bar_ts if self._last_signal else None
            ),
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = StrategyState.WARMUP
        self._last_signal = None
        self._cooldown_until_bar = 0
        logger.info(f"strategy {self.name} reset")

    # ------------------------------------------------------------------------
    # 辅助方法：子类在 compute() 中使用
    # ------------------------------------------------------------------------

    def _compute_worst_factor_state(
        self, factors: Dict[str, Any]
    ) -> tuple:
        """
        根据因子状态计算策略状态。

        返回：(策略状态, 是否部分缺失)
        - 所有因子可用 → (READY, False)
        - 部分因子不可用 → (DEGRADED, True)
        - 所有因子不可用 → (DEGRADED, True)
        """
        if not factors:
            return (StrategyState.DEGRADED, True)

        usable = 0
        for fv in factors.values():
            if hasattr(fv, "is_usable") and fv.is_usable():
                usable += 1

        if usable == 0:
            return (StrategyState.DEGRADED, True)
        if usable < len(factors):
            return (StrategyState.DEGRADED, True)
        return (StrategyState.READY, False)

    def _normalize_weights(
        self, factors: Dict[str, Any], raw_weights: Dict[str, float]
    ) -> Dict[str, float]:
        """
        根据可用因子重新归一化权重。

        - 只包含 is_usable() 为 True 的因子
        - 权重之和归一化为 1.0
        - 无可用因子时返回空 dict
        """
        usable = {
            name: w for name, w in raw_weights.items()
            if name in factors
            and hasattr(factors[name], "is_usable")
            and factors[name].is_usable()
        }
        total = sum(usable.values())
        if total <= 1e-9:
            return {}
        return {k: v / total for k, v in usable.items()}

    def _make_signal(
        self,
        context: StrategyContext,
        signal_type: SignalType,
        direction: int = 0,
        score: float = 0.0,
        reason: str = "",
        factor_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
        factor_weights: Optional[Dict[str, float]] = None,
        *,
        state: Optional[StrategyState] = None,
        partial: bool = False,
        error: Optional[str] = None,
        compute_ms: float = 0.0,
    ) -> StrategySignal:
        """
        构造 StrategySignal。子类在 compute() 中调用此方法。

        自动处理：
        - 生成 signal_id
        - 填充 strategy_name / version
        - 填充 bar_ts（== context.bar.ts）
        - 状态追踪（成功/失败计数）
        - 耗时告警
        """
        bar_ts = int(context.bar.ts)

        # 错误路径
        if error is not None:
            signal = StrategySignal(
                signal_id=f"sig-{uuid4().hex[:12]}",
                strategy_name=self.name,
                version=self.version,
                bar_ts=bar_ts,
                type=SignalType.NOOP,
                direction=0,
                score=0.0,
                reason="error",
                factor_snapshot=factor_snapshot or {},
                factor_weights=factor_weights or {},
                state=StrategyState.ERROR,
                partial=True,
                error=error,
                compute_ms=compute_ms,
            )
            self._track_error(error)
            return signal

        # 正常路径
        if state is None:
            state = self._state
            if state == StrategyState.DEGRADED and not partial:
                # 所有因子恢复，自动回 READY
                state = StrategyState.READY

        signal = StrategySignal(
            signal_id=f"sig-{uuid4().hex[:12]}",
            strategy_name=self.name,
            version=self.version,
            bar_ts=bar_ts,
            type=signal_type,
            direction=direction,
            score=float(max(0.0, min(1.0, score))),
            reason=reason,
            factor_snapshot=factor_snapshot or {},
            factor_weights=factor_weights or {},
            context_meta={
                "active_round_id": (
                    context.active_round["round_id"]
                    if context.active_round else None
                ),
            },
            state=state,
            partial=partial,
            compute_ms=compute_ms,
        )
        self._track_success(signal)
        return signal

    def _track_error(self, error: str) -> None:
        self._consecutive_errors += 1
        if self._consecutive_errors >= DEGRADE_THRESHOLD:
            if self._state != StrategyState.DEGRADED:
                logger.warning(
                    f"strategy {self.name} DEGRADED after "
                    f"{self._consecutive_errors} consecutive errors: {error}"
                )
            self._state = StrategyState.DEGRADED

    def _track_success(self, signal: StrategySignal) -> None:
        self._consecutive_errors = 0
        if self._state == StrategyState.DEGRADED and not signal.partial:
            self._state = StrategyState.READY
            logger.info(f"strategy {self.name} recovered from DEGRADED")
        self._last_signal = signal


# ============================================================================
# 模块级工厂校验
# ============================================================================
def validate_strategy_module(module, config: dict) -> StrategyProvider:
    """
    加载并校验一个策略模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 StrategyProvider 实例
    4. 实例的 name / version 与 MANIFEST 一致
    5. MANIFEST.required_factors 全部存在于 config 的因子列表中
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = StrategyManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, StrategyProvider):
        raise TypeError(
            f"{module.__name__}.create() must return StrategyProvider, "
            f"got {type(instance).__name__}"
        )
    if instance.name != manifest.name:
        raise ValueError(
            f"{module.__name__}: instance.name='{instance.name}' "
            f"!= MANIFEST.name='{manifest.name}'"
        )
    if instance.version != manifest.version:
        raise ValueError(
            f"{module.__name__}: instance.version='{instance.version}' "
            f"!= MANIFEST.version='{manifest.version}'"
        )

    # 校验 required_factors 在配置中存在
    factors_cfg = config.get("factors", {})
    for factor_name in manifest.required_factors:
        if factor_name not in factors_cfg:
            raise ValueError(
                f"strategy {manifest.name} requires factor "
                f"'{factor_name}' but it's not in config.factors"
            )

    return instance
