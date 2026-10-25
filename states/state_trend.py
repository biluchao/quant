"""
states/state_trend.py — 趋势骑行状态 (重铸标准版)

核心设计原则：
1. 单向止损铁律：多单止损只上移，空单止损只下移。
2. 严格对称判断：多空的止盈（逃顶）、止损逻辑完全镜像。
3. 依赖强类型：所有数据从 context.active_round 中严格提取。
4. 极简输出：只输出 EXIT（平仓）或 HOLD（带移动止损提示）。

状态机迁移：
    TREND_RIDE ──(触发止损/超时/逃顶)──► IDLE (action=EXIT)
    TREND_RIDE ──(更新移动止损)──► TREND_RIDE (action=HOLD, stop_price_hint)
    TREND_RIDE ──(继续持有)──► TREND_RIDE (action=HOLD)
"""

import logging
from decimal import Decimal
from typing import Any, Optional

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)

logger = logging.getLogger(__name__)

class TrendRideState(StateProvider):
    """
    趋势持仓管理状态。
    """
    name = "trend_ride"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)
        
        state_cfg = config.get("states", {}).get(self.name, {})
        self.max_bars_in_round = int(state_cfg.get("max_bars_in_round", 60))
        self.trailing_trigger_atr = float(state_cfg.get("trailing_trigger_atr", 1.5))
        self.trailing_distance_atr = float(state_cfg.get("trailing_distance_atr", 1.0))
        self.escape_profit_atr = float(state_cfg.get("top_escape_min_profit_atr", 2.0))
        self.momentum_reverse_thresh = float(state_cfg.get("momentum_reversal_threshold", -0.2))
        self.atr_period = int(state_cfg.get("atr_period", 14))

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        logger.info(
            f"进入 TREND_RIDE: 方向={context.active_round.get('direction')}, "
            f"均价={context.active_round.get('entry_price')}, "
            f"止损={context.active_round.get('current_stop')}"
        )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(f"TREND_RIDE 执行异常: {e}", exc_info=True)
            return self._noop(reason="internal_error")

    def on_exit(self, context: StateContext) -> None:
        logger.info("退出 TREND_RIDE 状态")

    # ========================================================================
    # 核心逻辑
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        # 1. 防御性检查：如果没有活跃轮次，说明出 bug 了，必须立刻回 IDLE
        if context.active_round is None:
            logger.critical("TREND_RIDE 中不存在活跃轮次，强制回 IDLE！")
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason="no_active_round")

        # 2. 强类型提取数据
        round_data = context.active_round
        direction = int(round_data.get("direction", 0))
        current_stop = Decimal(str(round_data.get("current_stop", "0")))
        entry_price = Decimal(str(round_data.get("entry_price", "0")))
        bars_held = int(round_data.get("bars_held", 0))
        
        if direction not in (1, -1) or current_stop <= 0:
            logger.critical("活跃轮次数据损坏，强制平仓！")
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason="corrupted_round_data")

        # 3. 检查硬止损（用 bar 的 high/low 防插针）
        if self._check_hard_stop(context, direction, current_stop):
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason=TransitionReason.STOP_TRIGGERED.value)

        # 4. 检查时间止损
        if bars_held >= self.max_bars_in_round:
            logger.info(f"持仓超时 ({bars_held} 根)，强制平仓。")
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason=TransitionReason.TIMEOUT.value)

        # 5. 计算当前 ATR
        atr = self._calc_atr(context)

        # 6. 检查逃顶（趋势反转/背离）
        if self._check_escape(context, direction, entry_price, atr):
            logger.info("检测到逃顶/底信号，准备离场。")
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason=TransitionReason.TOP_DIVERGENCE_DETECTED.value)

        # 7. 计算移动止损（只上移，不下降）
        new_stop = self._calc_trailing_stop(context, direction, entry_price, current_stop, atr)
        if new_stop is not None:
            logger.info(f"触发移动止损: {current_stop} -> {new_stop}")
            return self._hold(reason="trailing_stop_update", stop_hint=new_stop)

        # 8. 继续持有
        return self._hold(reason="holding")

    # ========================================================================
    # 具体检测逻辑
    # ========================================================================

    def _check_hard_stop(self, context: StateContext, direction: int, stop: Decimal) -> bool:
        """硬止损：多单看 low，空单看 high"""
        bar = context.bar
        if direction == 1:
            return Decimal(str(bar.low)) <= stop
        else:
            return Decimal(str(bar.high)) >= stop

    def _check_escape(self, context: StateContext, direction: int, entry: Decimal, atr: float) -> bool:
        """逃顶/底：需要利润达标，且动量出现强反转"""
        current_close = Decimal(str(context.bar.close))
        profit = (current_close - entry) * direction
        
        if atr <= 0:
            return False

        # 利润必须超过设定的 ATR 倍数
        if profit < Decimal(str(atr * self.escape_profit_atr)):
            return False

        # 动量反转检查
        mom = context.factors.get("momentum")
        if not mom or not mom.is_usable():
            return False
            
        # 比如多单时，动量如果是很弱的负数，说明衰竭
        # mom.value * direction 必须小于反向阈值
        return (mom.value * direction) < self.momentum_reverse_thresh

    def _calc_trailing_stop(self, context: StateContext, direction: int, entry: Decimal, current_stop: Decimal, atr: float) -> Optional[Decimal]:
        """计算移动止损。铁律：多单只上移，空单只下移。"""
        if atr <= 0:
            return None

        current_close = Decimal(str(context.bar.close))
        profit = (current_close - entry) * direction
        
        # 利润没有超过触发线，不移动止损
        if profit < Decimal(str(atr * self.trailing_trigger_atr)):
            return None

        # 计算理论新止损
        trail_distance = Decimal(str(atr * self.trailing_distance_atr))
        if direction == 1:
            target_stop = current_close - trail_distance
            # 只能上移
            if target_stop > current_stop:
                return target_stop
        else:
            target_stop = current_close + trail_distance
            # 只能下移
            if target_stop < current_stop:
                return target_stop
                
        return None

    def _calc_atr(self, context: StateContext) -> float:
        """计算当前 ATR"""
        if len(context.recent_bars) < self.atr_period + 1:
            return 0.0
        tr_values = []
        for i in range(-self.atr_period, 0):
            bar = context.recent_bars[i]
            prev_close = float(context.recent_bars[i-1].close)
            h, l = float(bar.high), float(bar.low)
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            tr_values.append(tr)
        return sum(tr_values) / len(tr_values)


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "trend_ride",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["trend_management", "trailing_stop"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> TrendRideState:
    return TrendRideState(config)
