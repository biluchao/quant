"""
states/state_idle.py — 空闲状态（IDLE）

IDLE 是状态机的初始状态，也是所有轮次结束后的归位状态。

职责（唯一）：
- 监听策略信号
- 当信号是"可执行的、方向明确的、风控允许的" ENTRY 时，
  把状态机交给 BREAKOUT_CONFIRM
- 其余一切，返回 NOOP

不做什么：
- 不计算价格（由策略层/BREAKOUT_CONFIRM 计算）
- 不判断信号质量（策略层已经判过）
- 不重复冷却（策略层管冷却）
- 不创建轮次（RoundManager 管）
- 不下单（BREAKOUT_CONFIRM 确认后由状态机触发）

状态机迁移：
    IDLE ──(可执行 ENTRY 信号)──► BREAKOUT_CONFIRM
    IDLE ──(其他一切)──► IDLE (NOOP)

关键契约：
1. IDLE 是全状态机的最快状态：不做计算，只做检查
2. 每个检查失败必须记录 reason（用于归因和监控）
3. 异常包装成 NOOP(error=...) 或抛异常，不返回 None
4. signals 字典可能缺失，兼容多策略场景（Step 2）
5. 风控 HALT / CLOSE_ONLY 时拒绝 ENTRY
6. 有活跃轮次时拒绝进入（防御性，IDLE 不该有轮次）
7. 方向必须 ∈ {1, -1}
8. 信号必须是 ENTRY 类型
9. 信号必须 is_actionable()
10. meta 字段包含 signal_id、trigger_score、factor_status 用于归因

与其他状态的职责边界：
- BREAKOUT_CONFIRM：做 3 根 K 线确认，确认后提交订单 + 迁移到 TREND_RIDE
- TREND_RIDE：管理活跃轮次，检查止损/逃顶
- TOP_DETECT：判断是否逃顶
- IDLE：只在轮次结束/止损/逃顶完成后进入，等待下一个信号

为什么 IDLE 输出 ENTRY 但不下单：
- IDLE → BREAKOUT_CONFIRM 是"开始进入流程"，不是"执行"
- BREAKOUT_CONFIRM 的职责是确认（3 根 K 线验证）
- 如果 IDLE 直接下单，BREAKOUT_CONFIRM 就失去意义
- 状态机处理 StateTransition 时，target=BREAKOUT_CONFIRM 且 action=ENTRY
  意味着"记录进入意图，进入确认阶段"，而不是"立即下单"
- 实际下单发生在 BREAKOUT_CONFIRM 返回 target=TREND_RIDE 且 action=ENTRY 时

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

import logging
from typing import Any, Dict, List, Optional

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)
from core.strategy import SignalType

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "idle",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["idle", "entry_gate"],
    "module_dependencies": [],
    "allowed_actions": ["entry", "noop", "hold"],
    "fallback": None,
    "enabled": True,
    "description": "Idle state: the only gate for new round entry",
}


# ============================================================================
# 常量
# ============================================================================
# Step 1 单策略，硬编码策略名
DEFAULT_STRATEGY_NAME = "trend_round"

# 阻止 ENTRY 的风控等级
BLOCKING_RISK_LEVELS = {"halt", "close_only"}


# ============================================================================
# IdleState
# ============================================================================
class IdleState(StateProvider):
    """
    空闲状态。详见模块 docstring。

    用法：
        state = IdleState(config)
        # 注册到状态机
        state_machine.register_state("idle", state)
    """

    name = "idle"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        state_cfg = config.get("states", {})
        idle_cfg = state_cfg.get("idle", {})

        # 策略名（Step 1 单策略，Step 2 可从配置读）
        self.strategy_name = idle_cfg.get(
            "strategy_name", DEFAULT_STRATEGY_NAME
        )

        # ---- 统计 ----
        self._stats = {
            "enters": 0,
            "bar_closes": 0,
            "transitions": 0,
            "noops": 0,
            "rejections": {
                "no_signal": 0,
                "signal_not_actionable": 0,
                "not_entry_type": 0,
                "invalid_direction": 0,
                "risk_blocked": 0,
                "active_round_present": 0,
                "internal_error": 0,
            },
        }

        logger.info(
            f"IdleState initialized strategy_name={self.strategy_name}"
        )

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """
        进入 IDLE。

        契约：
        - 幂等（多次进入不崩）
        - 记录进入原因（用于监控）
        - 防御性检查：IDLE 不该有活跃轮次
        """
        self._stats["enters"] += 1

        enter_reason = getattr(context, "enter_reason", "unknown")
        logger.info(
            f"IdleState entered reason={enter_reason} "
            f"active_round={getattr(context, 'active_round', None) is not None}"
        )

        # 防御性检查：IDLE 进入时不应该有活跃轮次
        if getattr(context, "active_round", None) is not None:
            active_round = context.active_round
            logger.critical(
                f"IdleState entered with active round! "
                f"round_id={active_round.get('round_id')} "
                f"direction={active_round.get('direction')}. "
                f"This indicates a state machine bug."
            )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """
        每根 K 线收线时决策。

        契约：
        - 永远返回 StateTransition
        - 内部异常包装成 NOOP(error=...)
        - 不做计算，只做检查
        """
        self._stats["bar_closes"] += 1

        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(
                f"IdleState.on_bar_close error: {e}",
                exc_info=True,
            )
            self._stats["rejections"]["internal_error"] += 1
            return self._error(str(e))

    def on_exit(self, context: StateContext) -> None:
        """
        退出 IDLE。

        契约：
        - 只记录日志
        - 不做决策（决策在 on_bar_close 已做）
        """
        logger.info("IdleState exited")

    # ========================================================================
    # 核心：评估
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        """
        评估当前状态，返回转换。

        检查顺序（短路）：
        1. 活跃轮次检查（防御性）
        2. 信号存在性检查
        3. 信号可执行性检查
        4. 信号类型检查
        5. 方向检查
        6. 风控检查
        7. 全部通过 → ENTRY
        """
        # ---- 1. 防御性：IDLE 不该有活跃轮次 ----
        if getattr(context, "active_round", None) is not None:
            logger.critical(
                "IdleState has active_round, this is a bug. "
                "Returning NOOP but this should be investigated."
            )
            self._stats["rejections"]["active_round_present"] += 1
            return self._noop("active_round_present")

        # ---- 2. 获取信号 ----
        signals = getattr(context, "signals", None) or {}
        signal = signals.get(self.strategy_name)

        if signal is None:
            self._stats["rejections"]["no_signal"] += 1
            return self._noop("no_signal")

        # ---- 3. 信号可执行性 ----
        try:
            is_actionable = signal.is_actionable()
        except Exception as e:
            logger.warning(
                f"signal.is_actionable() raised: {e}"
            )
            self._stats["rejections"]["signal_not_actionable"] += 1
            return self._noop("signal_not_actionable")

        if not is_actionable:
            self._stats["rejections"]["signal_not_actionable"] += 1
            reason = getattr(signal, "reason", "unknown")
            return self._noop(f"signal_not_actionable:{reason}")

        # ---- 4. 信号类型 ----
        signal_type = getattr(signal, "type", None)
        if signal_type != SignalType.ENTRY:
            self._stats["rejections"]["not_entry_type"] += 1
            type_value = (
                signal_type.value
                if hasattr(signal_type, "value")
                else str(signal_type)
            )
            return self._noop(f"not_entry_type:{type_value}")

        # ---- 5. 方向 ----
        direction = getattr(signal, "direction", 0)
        if direction not in (1, -1):
            self._stats["rejections"]["invalid_direction"] += 1
            logger.warning(
                f"ENTRY signal with invalid direction={direction}"
            )
            return self._noop(f"invalid_direction:{direction}")

        # ---- 6. 风控 ----
        risk_approved = getattr(context, "risk_approved", False)
        risk_level = getattr(context, "risk_level", "unknown")

        if not risk_approved:
            self._stats["rejections"]["risk_blocked"] += 1
            return self._noop(f"risk_not_approved:{risk_level}")

        if risk_level in BLOCKING_RISK_LEVELS:
            self._stats["rejections"]["risk_blocked"] += 1
            return self._noop(f"risk_level_blocked:{risk_level}")

        # ---- 7. 全部通过 → ENTRY ----
        self._stats["transitions"] += 1

        meta = self._build_meta(signal, context)

        return self._transition(
            target="breakout_confirm",
            action=StateAction.ENTRY,
            direction=direction,
            reason=TransitionReason.BREAKOUT_CONFIRMED.value,
            meta=meta,
        )

    # ========================================================================
    # 辅助
    # ========================================================================

    def _build_meta(
        self, signal: Any, context: StateContext
    ) -> Dict[str, Any]:
        """
        构造 meta 字段（用于落库和归因）。

        包含：
        - signal_id: 信号 ID
        - trigger_score: 信号分数
        - signal_reason: 信号原因
        - risk_level: 当时风控等级
        - bars_in_idle: 在 IDLE 待了多少根 K 线
        - enter_reason: 进入 IDLE 的原因
        """
        meta: Dict[str, Any] = {
            "signal_id": getattr(signal, "signal_id", None),
            "trigger_score": getattr(signal, "score", None),
            "signal_reason": getattr(signal, "reason", None),
            "risk_level": getattr(context, "risk_level", None),
            "bars_in_idle": getattr(context, "bars_in_state", 0),
            "enter_reason": getattr(context, "enter_reason", None),
        }
        # 因子状态快照（用于归因时了解"为什么这个信号被接受"）
        factors = getattr(context, "factors", {})
        if factors:
            meta["factor_status"] = {
                name: getattr(fv, "state", None)
                if not hasattr(getattr(fv, "state", None), "value")
                else fv.state.value
                for name, fv in factors.items()
            }
        return meta

    def _noop(self, reason: str) -> StateTransition:
        """构造 NOOP 转换，统计拒绝原因。"""
        self._stats["noops"] += 1
        return self._noop_transition(reason)

    def _noop_transition(self, reason: str) -> StateTransition:
        """构造 NOOP 转换（不统计，避免重复计数）。"""
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=reason,
            meta={"rejection_reason": reason},
        )

    def _error(self, error: str) -> StateTransition:
        """构造错误转换。"""
        return self._noop_transition(
            TransitionReason.INTERNAL_ERROR.value
        )

    # ========================================================================
    # 健康与重置
    # ========================================================================

    def reset(self) -> None:
        """重置状态。"""
        for k in self._stats:
            if k == "rejections":
                for rk in self._stats["rejections"]:
                    self._stats["rejections"][rk] = 0
            else:
                self._stats[k] = 0
        logger.info("IdleState reset")

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        total = self._stats["bar_closes"]
        rejection_total = sum(
            self._stats["rejections"].values()
        )
        transition_rate = (
            self._stats["transitions"] / total
            if total > 0 else 0.0
        )
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "strategy_name": self.strategy_name,
            "stats": dict(self._stats),
            "summary": {
                "total_bar_closes": total,
                "transitions": self._stats["transitions"],
                "noops": self._stats["noops"],
                "rejection_total": rejection_total,
                "transition_rate": transition_rate,
            },
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> IdleState:
    """模块工厂入口。"""
    return IdleState(config)
