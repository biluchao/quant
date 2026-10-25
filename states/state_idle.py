"""
states/state_idle.py — 空闲状态 (重铸标准版)

核心设计原则：
1. 铁面门卫：只有完全合规的 ENTRY 信号 + 风控放行，才允许流向 BREAKOUT_CONFIRM。
2. 严苛校验：拦截任何方向不明、状态降级或类型错误的信号。
3. 防御性断言：IDLE 状态下绝不允许存在活跃轮次，若有，记录 CRITICAL。

状态机迁移：
    IDLE ──(合法 ENTRY 信号)──► BREAKOUT_CONFIRM
    IDLE ──(其他一切)──► IDLE (NOOP)
"""

import logging
from typing import Any, Dict

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)
from core.strategy import SignalType, StrategyState

logger = logging.getLogger(__name__)

class IdleState(StateProvider):
    """
    空闲状态实现。
    """
    name = "idle"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)
        
        # 提取状态专属参数（如果有的话，目前主要依赖策略默认）
        state_cfg = config.get("states", {}).get(self.name, {})
        self.strategy_name = state_cfg.get("strategy_name", "trend_round")

    # ========================================================================
    # 生命周期与核心逻辑
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """进入 IDLE 状态"""
        logger.info(f"进入 IDLE 状态 (原因: {context.enter_reason})")
        
        # 防御性检查：IDLE 绝不应该带着持仓进来
        if context.active_round is not None:
            logger.critical(
                f"致命错误：IDLE 状态中出现了活跃轮次 {context.active_round.get('round_id')}！"
                "系统逻辑可能存在漏洞。"
            )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """每根 K 线收线时执行门卫逻辑"""
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(f"IDLE 状态执行异常: {e}", exc_info=True)
            return self._error(f"idle_exception: {str(e)}")

    def on_exit(self, context: StateContext) -> None:
        """退出 IDLE 状态"""
        logger.debug("退出 IDLE 状态")

    # ========================================================================
    # 核心评估逻辑
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        # 1. 防御性：不应存在活跃轮次
        if context.active_round is not None:
            return self._noop(reason="unexpected_active_round")

        # 2. 获取当前策略的信号
        signal = context.signals.get(self.strategy_name)
        if signal is None:
            return self._noop(reason="no_signal")

        # 3. 信号可用性与类型校验
        if not signal.is_actionable():
            return self._noop(reason=f"signal_not_actionable:{signal.reason}")
            
        if signal.signal_type != SignalType.ENTRY:
            return self._noop(reason=f"not_entry_type:{signal.signal_type.value}")

        # 4. 方向严格校验
        if signal.direction not in (1, -1):
            logger.warning(f"收到非法的 ENTRY 方向: {signal.direction}")
            return self._noop(reason="invalid_direction")

        # 5. 风控绝对拦截
        if not context.risk_approved:
            return self._noop(reason=f"risk_blocked:{context.risk_level}")

        # 6. 所有条件满足，允许进入 BREAKOUT_CONFIRM
        logger.info(f"信号准入通过，准备进入 BREAKOUT_CONFIRM (方向: {signal.direction}, 分数: {signal.score:.4f})")
        
        # 强类型数据传递：将趋势方向通过 transition 传给下一个状态
        return self._transition(
            target="breakout_confirm",
            action=StateAction.ENTRY,
            direction=signal.direction,
            reason=TransitionReason.BREAKOUT_CONFIRMED.value,
            # 如果系统设计需要把信号ID或分数传下去，可以放在这里，但基类没定义的话就通过 Context 隐式流转
        )


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "idle",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["idle_state"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> IdleState:
    return IdleState(config)
