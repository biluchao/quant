"""
core/state.py — 状态抽象基类与通用数据结构

状态机是策略的"心脏"。

状态职责：
- 接收 StateContext（K 线、因子、盘口、活跃轮次、策略信号）
- 判断当前状态是否该转换
- 返回 StateTransition（目标状态、动作、原因）
- 维护状态内部状态（如一致性窗口、CUSUM 累积量）

状态不做：
- 不直接下单（状态机/风控/执行层的事）
- 不修改 context（不可变）
- 不知道策略（只读 context.signal）
- 不知道风控详情（只读 context.risk_level）
- 不知道其他状态（状态之间不直接通信）
- 不做 I/O

关键契约：
1. 状态永远返回 StateTransition，不返回 None
2. 没有转换时返回 NOOP 或 HOLD（target=None, action=NOOP/HOLD）
3. 有转换时 target != None, action ∈ {ENTRY, ADD, REDUCE, EXIT}
4. 状态内部异常必须包装成 StateTransition(action=NOOP, error=...)
5. 状态不持有 Gateway / RiskPolicy / RoundManager / StrategyProvider 引用
6. 状态只修改自身实例状态，不修改 context
7. 状态机的 on_enter/on_bar_close/on_exit 顺序由状态机保证
8. 同一根 K 线只允许一次转换（状态机保证）
9. 状态转换失败（on_enter 抛异常）由状态机回滚
10. 状态机不存轮次 ID，轮次信息通过 context 传递

状态生命周期：
    IDLE ──► BREAKOUT_CONFIRM ──► TREND_RIDE ──► TOP_DETECT ──► IDLE
              (未确认)  │            (持仓)         (逃顶)      
                        ▼
                       IDLE

转换验证：
- target 必须已注册
- (current, target) 必须在转换表中
- 同一根 K 线只允许一次转换
- 强制转换（emergency_stop）绕过转换表

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 状态动作
# ============================================================================
class StateAction(str, Enum):
    """状态转换时执行的动作。"""
    ENTRY = "entry"       # 开新轮次
    ADD = "add"           # 给活跃轮次加仓
    REDUCE = "reduce"     # 减仓
    EXIT = "exit"         # 平掉活跃轮次
    HOLD = "hold"         # 有持仓，继续持有
    NOOP = "noop"         # 无操作


# 需要 direction != 0 的动作
_DIRECTIONAL_ACTIONS = {StateAction.ENTRY, StateAction.ADD, StateAction.REDUCE}


# ============================================================================
# 转换原因（标准化枚举）
# ============================================================================
class TransitionReason(str, Enum):
    """状态转换的原因。snake_case。"""
    # ---- 入场 ----
    BREAKOUT_CONFIRMED = "breakout_confirmed"
    TREND_SCORE_EXCEEDED = "trend_score_exceeded"

    # ---- 持仓期间 ----
    STOP_TRIGGERED = "stop_triggered"
    TOP_DIVERGENCE_DETECTED = "top_divergence_detected"
    TREND_QUALITY_DEGRADED = "trend_quality_degraded"

    # ---- 退出 ----
    TIMEOUT = "timeout"
    MANUAL_EXIT = "manual_exit"
    EMERGENCY_EXIT = "emergency_exit"
    RISK_HALT = "risk_halt"

    # ---- 无操作 ----
    NO_SIGNAL = "no_signal"
    WAITING_CONFIRMATION = "waiting_confirmation"
    COOLDOWN = "cooldown"

    # ---- 异常 ----
    INTERNAL_ERROR = "internal_error"


# ============================================================================
# 状态输入上下文
# ============================================================================
@dataclass(frozen=True)
class StateContext:
    """
    状态计算的输入。由状态机组装，传给状态。

    契约：
    - 不可变，状态不得修改
    - 所有字段由状态机保证填充
    """
    # ---- 当前 K 线 ----
    bar: Any                                # 已收线的 K 线

    # ---- 盘口 ----
    snapshot: Optional[dict]                # 盘口快照，可能为 None

    # ---- 因子值 ----
    # 因子名 -> FactorValue（core.factor.FactorValue）
    factors: Dict[str, Any]

    # ---- 策略信号 ----
    # 策略名 -> StrategySignal（core.strategy.StrategySignal）
    # 状态从 signals["trend_round"] 读取 score/direction
    signals: Dict[str, Any]

    # ---- 活跃轮次 ----
    # 无持仓为 None；有持仓含：
    # round_id / direction / weighted_avg_cost / total_qty / current_stop /
    # pyramid_count / peak_unrealized / opened_ts / opened_bar_count
    active_round: Optional[dict]

    # ---- 风控 ----
    risk_level: str                         # "normal" / "warn" / "reduce" / "close_only" / "halt"
    risk_approved: bool                     # 当前风控是否允许开仓

    # ---- 状态机 ----
    current_state: str                      # 当前状态名
    bars_in_state: int                      # 在当前状态待了多少根 K 线
    enter_reason: str                       # 进入当前状态的原因（TransitionReason 的 value）
    now_ms: int                             # 单调时钟毫秒

    # ---- 数据 ----
    recent_bars: List[Any]                  # 最近 N 根 K 线，按时间升序，最后一根是当前 bar
    bar_phase: str                          # K 线阶段：open / body / tail / close


# ============================================================================
# 状态转换结果
# ============================================================================
@dataclass(frozen=True)
class StateTransition:
    """
    状态转换结果。不可变。

    契约：
    - target=None 时 action 必须是 NOOP 或 HOLD
    - target!=None 时 action 必须是 ENTRY/ADD/REDUCE/EXIT
    - ENTRY/ADD/REDUCE 必须 direction != 0
    - EXIT 必须 direction == 0
    """
    # ---- 状态转换 ----
    target: Optional[str]                   # 目标状态名，None 表示不转换
    action: StateAction
    reason: str                             # TransitionReason 的 value

    # ---- 执行提示（可选）----
    direction: int = 0                      # 1=LONG, -1=SHORT, 0=无方向
    entry_price_hint: Optional[Decimal] = None
    stop_price_hint: Optional[Decimal] = None

    # ---- 状态 ----
    partial: bool = False                   # 是否部分计算（某些因子缺失）
    error: Optional[str] = None             # 内部错误信息
    compute_ms: float = 0.0                 # 计算耗时

    # ---- 上下文（用于归因）----
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """构造时校验一致性。"""
        if self.target is None:
            if self.action not in (StateAction.NOOP, StateAction.HOLD):
                raise ValueError(
                    f"target=None requires action ∈ {{NOOP, HOLD}}, "
                    f"got {self.action}"
                )
            if self.direction != 0:
                raise ValueError(
                    f"target=None requires direction=0, "
                    f"got {self.direction}"
                )
        else:
            if self.action not in (
                StateAction.ENTRY, StateAction.ADD,
                StateAction.REDUCE, StateAction.EXIT,
            ):
                raise ValueError(
                    f"target != None requires action ∈ "
                    f"{{ENTRY, ADD, REDUCE, EXIT}}, got {self.action}"
                )

        # 方向一致性
        if self.action in _DIRECTIONAL_ACTIONS:
            if self.direction not in (1, -1):
                raise ValueError(
                    f"action={self.action} requires direction ∈ {{1,-1}}, "
                    f"got {self.direction}"
                )
        else:
            if self.direction != 0:
                raise ValueError(
                    f"action={self.action} requires direction=0, "
                    f"got {self.direction}"
                )

    def is_noop(self) -> bool:
        return self.target is None and self.action == StateAction.NOOP

    def is_hold(self) -> bool:
        return self.target is None and self.action == StateAction.HOLD

    def is_transition(self) -> bool:
        return self.target is not None

    def to_dict(self) -> dict:
        """用于日志和归因。"""
        return {
            "target": self.target,
            "action": self.action.value,
            "reason": self.reason,
            "direction": self.direction,
            "entry_price_hint": (
                str(self.entry_price_hint)
                if self.entry_price_hint else None
            ),
            "stop_price_hint": (
                str(self.stop_price_hint)
                if self.stop_price_hint else None
            ),
            "partial": self.partial,
            "error": self.error,
            "compute_ms": self.compute_ms,
        }


# ============================================================================
# 状态 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class StateManifest:
    """状态 MANIFEST 字典解析后的结构。"""
    name: str
    type: str
    version: str
    step: int
    capabilities: tuple
    allowed_actions: tuple
    owner: str
    fallback: Optional[str] = None
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "StateManifest":
        required = ("name", "type", "version", "step", "owner")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")
        if d["type"] != "state":
            raise ValueError(
                f"MANIFEST type must be 'state', got '{d['type']}'"
            )
        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            capabilities=tuple(d.get("capabilities", [])),
            allowed_actions=tuple(d.get("allowed_actions", [])),
            owner=d["owner"],
            fallback=d.get("fallback"),
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 状态抽象基类
# ============================================================================
class StateProvider(ABC):
    """
    状态抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.name / self.version
    2. 实现 on_bar_close(context) -> StateTransition
    3. 实现 on_enter(context) / on_exit(context)
    4. 不持有 Gateway / RiskPolicy / RoundManager / StrategyProvider 引用

    子类可选覆盖：
    - health()：自定义健康检查
    - reset()：自定义重置逻辑
    """

    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config

    # ------------------------------------------------------------------------
    # 生命周期钩子
    # ------------------------------------------------------------------------

    @abstractmethod
    def on_enter(self, context: StateContext) -> None:
        """
        进入状态时调用。仅调用一次（每次进入）。

        用途：
        - 初始化状态内部变量（如一致性窗口、CUSUM 累积量）
        - 设置冷却期
        - 记录进入原因（context.enter_reason）

        契约：
        - 不得在此方法中下单
        - 不得修改 context（不可变）
        - 可以修改自身实例状态
        - 抛异常会导致状态机回滚到转换前状态
        """
        ...

    @abstractmethod
    def on_bar_close(self, context: StateContext) -> StateTransition:
        """
        每根 K 线收线时调用。返回状态转换。

        契约：
        - 永远返回 StateTransition
        - 无转换返回 StateTransition(target=None, action=NOOP)
        - 有转换返回 StateTransition(target="...", action=...)
        - 内部异常必须包装成 StateTransition(target=None, action=NOOP, error=...)
        - 禁止 I/O
        - 禁止修改 context
        """
        ...

    @abstractmethod
    def on_exit(self, context: StateContext) -> None:
        """
        退出状态时调用。

        用途：
        - 清理状态内部变量
        - 记录退出统计

        契约：
        - 不得在此方法中下单
        - 不得修改 context
        - 不得否决转换（转换由 on_bar_close 决定）
        """
        ...

    # ------------------------------------------------------------------------
    # 健康检查与重置
    # ------------------------------------------------------------------------

    def health(self) -> Dict[str, Any]:
        """健康检查。监控系统定期调用。"""
        return {
            "name": self.name,
            "version": self.version,
            "type": "state",
        }

    def reset(self) -> None:
        """重置状态内部变量。测试和热重启时使用。"""
        logger.info(f"state {self.name} reset")

    # ------------------------------------------------------------------------
    # 辅助方法：构造 StateTransition
    # ------------------------------------------------------------------------

    def _noop(
        self,
        reason: str = TransitionReason.NO_SIGNAL.value,
        meta: Optional[Dict[str, Any]] = None,
        compute_ms: float = 0.0,
    ) -> StateTransition:
        """构造 NOOP 转换。"""
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=reason,
            compute_ms=compute_ms,
            meta=dict(meta or {}),
        )

    def _hold(
        self,
        reason: str = TransitionReason.NO_SIGNAL.value,
        meta: Optional[Dict[str, Any]] = None,
        compute_ms: float = 0.0,
    ) -> StateTransition:
        """构造 HOLD 转换（有持仓，继续持有）。"""
        return StateTransition(
            target=None,
            action=StateAction.HOLD,
            reason=reason,
            compute_ms=compute_ms,
            meta=dict(meta or {}),
        )

    def _transition(
        self,
        target: str,
        action: StateAction,
        direction: int,
        reason: str,
        *,
        entry_price_hint: Optional[Decimal] = None,
        stop_price_hint: Optional[Decimal] = None,
        partial: bool = False,
        compute_ms: float = 0.0,
        meta: Optional[Dict[str, Any]] = None,
    ) -> StateTransition:
        """构造状态转换。"""
        return StateTransition(
            target=target,
            action=action,
            direction=direction,
            reason=reason,
            entry_price_hint=entry_price_hint,
            stop_price_hint=stop_price_hint,
            partial=partial,
            compute_ms=compute_ms,
            meta=dict(meta or {}),
        )

    def _error(
        self,
        error: str,
        compute_ms: float = 0.0,
    ) -> StateTransition:
        """构造错误转换（内部异常包装）。"""
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=TransitionReason.INTERNAL_ERROR.value,
            error=error,
            partial=True,
            compute_ms=compute_ms,
        )


# ============================================================================
# 状态机转换表校验
# ============================================================================
def validate_transition_table(
    states: Dict[str, StateProvider],
    transition_table: Dict[str, List[str]],
) -> None:
    """
    校验状态机转换表。

    规则：
    1. 所有 states 的 name 在 transition_table 中存在
    2. transition_table 中所有 target 在 states 中存在
    3. 禁止自转换（state 不能转换到自己，同一状态用 NOOP/HOLD 表示）
    4. 初始状态 IDLE 必须在 states 中
    5. 每个非 IDLE 状态必须有返回 IDLE 的路径
    """
    if "idle" not in states:
        raise ValueError("initial state 'idle' not registered")

    for name, state in states.items():
        if name not in transition_table:
            raise ValueError(
                f"state '{name}' not in transition_table"
            )
        for target in transition_table[name]:
            if target not in states:
                raise ValueError(
                    f"transition '{name}' → '{target}': "
                    f"target not registered"
                )
            if target == name:
                raise ValueError(
                    f"self-transition '{name}' → '{name}' not allowed, "
                    f"use NOOP/HOLD instead"
                )

    # 每个非 IDLE 状态必须能回到 IDLE
    for name in states:
        if name == "idle":
            continue
        if "idle" not in transition_table.get(name, []):
            logger.warning(
                f"state '{name}' has no path back to 'idle', "
                f"consider adding a timeout"
            )


# ============================================================================
# 模块级工厂校验
# ============================================================================
def validate_state_module(module, config: dict) -> StateProvider:
    """
    加载并校验一个状态模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 StateProvider 实例
    4. 实例的 name / version 与 MANIFEST 一致
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = StateManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, StateProvider):
        raise TypeError(
            f"{module.__name__}.create() must return StateProvider, "
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

    return instance
