"""
core/strategy.py — 策略抽象基类与信号结构 (重铸标准版)

核心设计原则：
1. 绝对物理隔离：策略层只能读取上下文，绝不能触碰网关或轮次管理器。
2. 信号强校验：StrategySignal 构造瞬间执行严格的方向与分数逻辑校验。
3. 异常零容忍向上抛：所有内部异常必须被封装为带 error 的 NOOP 信号。
4. 统一上下文：强制注入行情、因子、风控状态和只读持仓快照。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import uuid

logger = logging.getLogger(__name__)

# ============================================================================
# 枚举与常量
# ============================================================================

class StrategyState(str, Enum):
    WARMUP = "warmup"
    READY = "ready"
    DEGRADED = "degraded"
    ERROR = "error"

class SignalType(str, Enum):
    ENTRY = "entry"     # 建议开新轮次
    EXIT = "exit"       # 建议平掉活跃轮次
    HOLD = "hold"       # 有持仓，继续持有
    NOOP = "noop"       # 无操作

# 需要明确方向的动作
_DIRECTIONAL_TYPES = {SignalType.ENTRY}
# 必须是空方向的动作
_NON_DIRECTIONAL_TYPES = {SignalType.EXIT, SignalType.HOLD, SignalType.NOOP}

# ============================================================================
# 数据结构
# ============================================================================

@dataclass(frozen=True)
class StrategyContext:
    """
    策略计算的输入上下文。
    由状态机组装并传入，策略只读，严禁修改。
    """
    # 行情数据
    bar: Any                                # 当前已收线的 K 线
    snapshot: Optional[dict]                # 当前盘口快照
    factors: Dict[str, Any]                 # 因子快照 {name: FactorValue}
    
    # 账户与系统状态
    active_round: Optional[dict]            # 活跃轮次只读快照（无持仓为 None）
    risk_level: str                         # 当前风控等级
    risk_approved: bool                     # 当前是否允许开新仓
    now_ms: int                             # 当前系统时间戳（毫秒）


@dataclass(frozen=True)
class StrategySignal:
    """
    策略输出的强类型信号。
    构造时强制校验逻辑一致性。
    """
    signal_id: str
    strategy_name: str
    bar_ts: int
    
    signal_type: SignalType
    direction: int = 0                      # 1=LONG, -1=SHORT, 0=FLAT
    score: float = 0.0                      # 信号强度 [0.0, 1.0]
    reason: str = ""                        # 产生信号的原因
    
    state: StrategyState = StrategyState.READY
    error: Optional[str] = None
    partial: bool = False                   # 是否因部分因子缺失而勉强计算

    def __post_init__(self):
        """严苛的实例化校验"""
        # 1. 分数区间校验
        if not (0.0 <= self.score <= 1.0):
            raise ValueError(f"信号分数必须位于 [0.0, 1.0]，当前为 {self.score}")

        # 2. 方向与类型的一致性校验
        if self.signal_type in _DIRECTIONAL_TYPES:
            if self.direction not in (1, -1):
                raise ValueError(f"信号类型 {self.signal_type} 必须带有方向 (1/-1)，当前为 {self.direction}")
        elif self.signal_type in _NON_DIRECTIONAL_TYPES:
            if self.direction != 0:
                raise ValueError(f"信号类型 {self.signal_type} 不能带有方向，当前为 {self.direction}")

    def is_actionable(self) -> bool:
        """下游判断该信号是否可直接执行"""
        return (
            self.state == StrategyState.READY
            and self.error is None
            and self.signal_type not in (SignalType.NOOP, SignalType.HOLD)
        )

# ============================================================================
# 策略基类
# ============================================================================

class StrategyProvider(ABC):
    """
    策略抽象基类。
    """
    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        # 策略绝对不能在这里拿 gateway 或 round_manager，只能拿配置
        self.config = config
        strategy_cfg = config.get("strategy", {}).get(self.name, {})
        
        self.warmup_bars = int(strategy_cfg.get("warmup_bars", 100))
        self._bars_seen = 0
        self._state = StrategyState.WARMUP

    # ========================================================================
    # 核心执行入口 (由状态机调用)
    # ========================================================================

    def safe_compute(self, context: StrategyContext) -> StrategySignal:
        """
        安全执行策略计算，拦截一切异常。
        """
        try:
            sig = self._compute_impl(context)
            if sig is None:
                return self._make_noop(context, reason="compute_returned_none")
            return sig
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"
            logger.error(f"策略 {self.name} 发生崩溃: {err_msg}", exc_info=True)
            self._state = StrategyState.ERROR
            
            # 异常时返回一个 ERROR 状态的 NOOP，绝不崩主线程
            return StrategySignal(
                signal_id=f"sig-err-{uuid.uuid4().hex[:8]}",
                strategy_name=self.name,
                bar_ts=getattr(context.bar, "ts", 0),
                signal_type=SignalType.NOOP,
                state=StrategyState.ERROR,
                error=err_msg
            )

    @abstractmethod
    def _compute_impl(self, context: StrategyContext) -> StrategySignal:
        """
        子类实现实际逻辑。
        必须返回 StrategySignal，不允许返回 None（基类已通过 safe_compute 兜底，但契约要求必须返回）。
        """
        ...

    # ========================================================================
    # 状态更新与辅助构造
    # ========================================================================

    def on_bar_close(self, bar: Any) -> None:
        """每根 K 线收线时更新预热状态"""
        self._bars_seen += 1
        if self._state == StrategyState.WARMUP and self._bars_seen >= self.warmup_bars:
            self._state = StrategyState.READY
            logger.info(f"策略 {self.name} 预热完成。")

    def _make_noop(self, context: StrategyContext, reason: str = "no_signal") -> StrategySignal:
        """构造标准 NOOP 信号"""
        return StrategySignal(
            signal_id=f"sig-{uuid.uuid4().hex[:8]}",
            strategy_name=self.name,
            bar_ts=getattr(context.bar, "ts", 0),
            signal_type=SignalType.NOOP,
            score=0.0,
            reason=reason
        )

    def _make_entry(self, context: StrategyContext, direction: int, score: float, reason: str) -> StrategySignal:
        """构造标准 ENTRY 信号"""
        return StrategySignal(
            signal_id=f"sig-{uuid.uuid4().hex[:8]}",
            strategy_name=self.name,
            bar_ts=getattr(context.bar, "ts", 0),
            signal_type=SignalType.ENTRY,
            direction=direction,
            score=score,
            reason=reason
        )

    def health(self) -> dict:
        return {
            "name": self.name,
            "state": self._state.value,
            "bars_seen": self._bars_seen
        }

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "strategy_base",
    "type": "strategy",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["base_strategy"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> StrategyProvider:
    raise NotImplementedError("StrategyProvider 是抽象基类，需实例化具体子类")
