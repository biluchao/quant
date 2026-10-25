"""
core/state.py — 状态机抽象基类与数据结构 (重铸标准版)

核心设计原则：
1. 废除隐式 meta 字典：所有跨状态流转的数据必须显式声明为强类型字段。
2. 强校验 StateTransition：状态迁移必须符合动作逻辑（如开仓必须有方向，平仓必须无方向）。
3. 严格状态注册：拒绝重复注册，拒绝自动装载魔法。
4. 数据流转单向化：上下文由状态机注入，状态只读不写。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import time

logger = logging.getLogger(__name__)

# ============================================================================
# 枚举与常量
# ============================================================================

class StateAction(str, Enum):
    """状态转换执行的动作。"""
    ENTRY = "entry"       # 开新轮次
    ADD = "add"           # 给活跃轮次加仓
    REDUCE = "reduce"     # 减仓
    EXIT = "exit"         # 平掉活跃轮次
    HOLD = "hold"         # 有持仓，继续持有
    NOOP = "noop"         # 无操作

# 需要明确方向的动作
_DIRECTIONAL_ACTIONS = {StateAction.ENTRY, StateAction.ADD, StateAction.REDUCE}

class TransitionReason(str, Enum):
    """状态转换的标准原因枚举。"""
    BREAKOUT_CONFIRMED = "breakout_confirmed"
    TREND_SCORE_EXCEEDED = "trend_score_exceeded"
    STOP_TRIGGERED = "stop_triggered"
    TOP_DIVERGENCE_DETECTED = "top_divergence_detected"
    TREND_QUALITY_DEGRADED = "trend_quality_degraded"
    TIMEOUT = "timeout"
    MANUAL_EXIT = "manual_exit"
    EMERGENCY_EXIT = "emergency_exit"
    RISK_HALT = "risk_halt"
    NO_SIGNAL = "no_signal"
    WAITING_CONFIRMATION = "waiting_confirmation"
    COOLDOWN = "cooldown"
    INTERNAL_ERROR = "internal_error"

# ============================================================================
# 核心数据结构
# ============================================================================

@dataclass(frozen=True)
class StateContext:
    """
    状态机计算的输入。由状态机组装，传给状态。
    契约：必须被完全填充，严禁传入 None 给非 Optional 字段。
    """
    # ---- 当前基础环境 ----
    bar: Any                                # 当前收线的 K 线对象
    snapshot: Optional[dict]                # 盘口快照，可能为 None
    factors: Dict[str, Any]                 # 因子快照 {name: FactorValue}
    signals: Dict[str, Any]                 # 策略信号 {strategy_name: StrategySignal}
    active_round: Optional[dict]            # 活跃轮次快照（包含方向、均价、止损等），无持仓为 None
    
    # ---- 系统状态 ----
    risk_level: str                         # "normal", "warn", "reduce", "close_only", "halt"
    risk_approved: bool                     # 当前风控是否允许开仓
    current_state: str                      # 当前状态名称
    bars_in_state: int                      # 在当前状态待了多少根 K 线
    enter_reason: str                       # 进入当前状态的原因
    now_ms: int                             # 单调时钟毫秒时间戳
    
    # ---- K线上下文 ----
    recent_bars: List[Any]                  # 最近 N 根 K 线
    bar_phase: str                          # K 线阶段 (open/body/tail/close)
    
    # ---- 显式跨状态数据（原 meta 字段的重构） ----
    trend_direction: int = 0                # 趋势方向 (1多 / -1空)
    peak_close: Optional[Decimal] = None    # 逃顶时的峰值价格 (TopDetect 用)
    peak_bar_ts: Optional[int] = None       # 峰值出现的时间
    entry_price: Optional[Decimal] = None   # 原轮次入场价
    signal_id: Optional[str] = None         # 触发信号 ID


@dataclass(frozen=True)
class StateTransition:
    """
    状态转换结果。严格校验字段一致性。
    """
    target: Optional[str]                   # 目标状态名，None 表示不转换
    action: StateAction                     # 执行的动作
    reason: str                             # TransitionReason 的 value 或自定义字符串
    
    # ---- 执行提示 ----
    direction: int = 0                      # 1=LONG, -1=SHORT, 0=无方向
    entry_price_hint: Optional[Decimal] = None
    stop_price_hint: Optional[Decimal] = None

    # ---- 异常与状态 ----
    partial: bool = False                   # 是否部分计算
    error: Optional[str] = None             # 内部错误信息
    compute_ms: float = 0.0                 # 计算耗时

    def __post_init__(self):
        """实例化瞬间进行严苛的逻辑校验。"""
        # 规则 1：如果不转换状态，只能是无操作或继续持有
        if self.target is None:
            if self.action not in (StateAction.NOOP, StateAction.HOLD):
                raise ValueError(f"target=None requires action in (NOOP, HOLD), got {self.action}")
            if self.direction != 0:
                raise ValueError(f"target=None requires direction=0, got {self.direction}")
        
        # 规则 2：如果转换状态，必须是主动操作
        else:
            if self.action not in (StateAction.ENTRY, StateAction.ADD, StateAction.REDUCE, StateAction.EXIT):
                raise ValueError(f"target != None requires action in (ENTRY, ADD, REDUCE, EXIT), got {self.action}")
            
            # 规则 3：开仓/加仓必须带方向
            if self.action in _DIRECTIONAL_ACTIONS:
                if self.direction not in (1, -1):
                    raise ValueError(f"action={self.action} requires direction in (1, -1), got {self.direction}")
            
            # 规则 4：平仓绝不能带方向
            if self.action == StateAction.EXIT:
                if self.direction != 0:
                    raise ValueError(f"action=EXIT requires direction=0, got {self.direction}")


# ============================================================================
# 基础 Abstract Base Class
# ============================================================================

class StateProvider(ABC):
    """
    状态抽象基类。
    子类必须实现 on_enter, on_bar_close, on_exit。
    """
    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def on_enter(self, context: StateContext) -> None:
        """进入状态时调用。用于初始化内部变量。"""
        ...

    @abstractmethod
    def on_bar_close(self, context: StateContext) -> StateTransition:
        """每根 K 线收线时调用。返回状态转换。"""
        ...

    @abstractmethod
    def on_exit(self, context: StateContext) -> None:
        """退出状态时调用。用于清理。"""
        ...

    def health(self) -> dict:
        """健康检查。"""
        return {"name": self.name, "version": self.version}

    def reset(self) -> None:
        """重置状态。"""
        pass

    # ------------------------------------------------------------------------
    # 辅助构造方法（强制子类使用标准 Transition 生成器，避免手写拼错）
    # ------------------------------------------------------------------------
    
    def _noop(self, reason: str = TransitionReason.NO_SIGNAL.value) -> StateTransition:
        return StateTransition(target=None, action=StateAction.NOOP, reason=reason)

    def _hold(self, reason: str = TransitionReason.NO_SIGNAL.value, stop_hint: Optional[Decimal] = None) -> StateTransition:
        return StateTransition(target=None, action=StateAction.HOLD, reason=reason, stop_price_hint=stop_hint)

    def _transition(
        self,
        target: str,
        action: StateAction,
        direction: int,
        reason: str,
        *,
        entry_hint: Optional[Decimal] = None,
        stop_hint: Optional[Decimal] = None,
        partial: bool = False
    ) -> StateTransition:
        return StateTransition(
            target=target,
            action=action,
            direction=direction,
            reason=reason,
            entry_price_hint=entry_hint,
            stop_price_hint=stop_hint,
            partial=partial
        )

    def _error(self, error_msg: str) -> StateTransition:
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=TransitionReason.INTERNAL_ERROR.value,
            error=error_msg,
            partial=True
        )


# ============================================================================
# 模块级校验与路由表验证
# ============================================================================

def validate_transition_table(states: Dict[str, StateProvider], transition_table: Dict[str, List[str]]) -> None:
    """
    校验状态机的转换表（初始化时调用）。
    不符合规范直接抛异常，拒绝启动。
    """
    if "idle" not in states:
        raise ValueError("Missing initial state 'idle'")

    for name, state in states.items():
        if name not in transition_table:
            raise ValueError(f"State '{name}' not found in transition table")
        
        for target in transition_table[name]:
            if target not in states:
                raise ValueError(f"Transition '{name}' -> '{target}': target state not registered")
            if target == name:
                raise ValueError(f"Self-transition '{name}' -> '{name}' not allowed, use NOOP/HOLD instead")
