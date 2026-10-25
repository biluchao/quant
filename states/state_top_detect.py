"""
states/state_top_detect.py — 逃顶观察状态 (重铸标准版)

核心设计原则：
1. 绝不交易：只做观察，只输出 HOLD 或 EXIT，绝不输出 ENTRY。
2. 兜底极值：如果 Context 未传递前高/前低，自动从最近的 K 线中提取。
3. 明确退场：逃顶成功、假逃顶、超时，全部统一退回 IDLE。

状态机迁移：
    TREND_RIDE ──(逃顶信号)──► TOP_DETECT
    TOP_DETECT ──(逃顶成功/假逃顶/超时)──► IDLE (action=EXIT)
    TOP_DETECT ──(观察中)──► TOP_DETECT (action=HOLD)
"""

import logging
from decimal import Decimal
from typing import Optional

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)

logger = logging.getLogger(__name__)

class TopDetectState(StateProvider):
    """
    逃顶后观察状态。
    """
    name = "top_detect"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)
        
        state_cfg = config.get("states", {}).get(self.name, {})
        self.max_observation_bars = int(state_cfg.get("max_observation_bars", 10))
        self.confirm_atr = float(state_cfg.get("escape_confirm_distance_atr", 2.0))
        self.tolerance_atr = float(state_cfg.get("escape_return_tolerance_atr", 0.5))
        self.atr_period = int(state_cfg.get("atr_period", 14))

        # 内部状态
        self._peak_close: Optional[Decimal] = None
        self._trend_direction = 0

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """进入观察期，锁定前高/前低和趋势方向"""
        self._trend_direction = context.trend_direction
        
        # 尝试从上下文中获取峰值
        self._peak_close = context.peak_close
        
        # 兜底：如果 Context 里没有（比如上一步平仓清空了轮次），从最近的 K 线找
        if self._peak_close is None and context.recent_bars:
            if self._trend_direction == 1:
                self._peak_close = max(Decimal(str(b.high)) for b in context.recent_bars[-30:])
            elif self._trend_direction == -1:
                self._peak_close = min(Decimal(str(b.low)) for b in context.recent_bars[-30:])

        logger.info(f"进入 TOP_DETECT: 方向={self._trend_direction}, 基准极值={self._peak_close}")

    def on_bar_close(self, context: StateContext) -> StateTransition:
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(f"TOP_DETECT 执行异常: {e}", exc_info=True)
            return self._transition(target="idle", action=StateAction.EXIT, direction=0, reason="internal_error")

    def on_exit(self, context: StateContext) -> None:
        logger.info("退出 TOP_DETECT")

    # ========================================================================
    # 核心评估逻辑
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        # 1. 数据完整性校验
        if self._trend_direction not in (1, -1) or self._peak_close is None:
            logger.warning("逃顶观察缺少必要数据，直接退出。")
            return self._exit("missing_data")

        # 2. 观察超时
        if context.bars_in_state > self.max_observation_bars:
            logger.info(f"逃顶观察超时 ({context.bars_in_state} 根)，退回 IDLE。")
            return self._exit("observation_timeout")

        # 3. 计算逃离距离
        atr = self._calc_atr(context)
        if atr <= 0:
            return self._exit("atr_unavailable")

        current_close = Decimal(str(context.bar.close))
        
        # escape_direction: 多单逃顶后，价格继续跌才算成功，所以方向相反
        escape_direction = -self._trend_direction
        # 距离 = (当前价 - 极值) * 逃逸方向
        # 若多单，极值 100，现价 90，逃逸方向 -1，则 (90-100)*-1 = 10 (正数，成功逃顶)
        distance = (current_close - self._peak_close) * escape_direction

        # 4. 判定：真逃顶（继续远离极值）
        if distance > Decimal(str(atr * self.confirm_atr)):
            logger.info(f"逃顶确认成功，继续远离极值 {distance} 点。")
            return self._exit("escape_confirmed")

        # 5. 判定：假逃顶（价格又回到了极值附近）
        if distance < -Decimal(str(atr * self.tolerance_atr)):
            logger.warning(f"假逃顶确认，价格重返极值附近 {distance} 点。")
            return self._exit("false_escape")

        # 6. 继续观察
        return self._hold(reason="observing")

    # ========================================================================
    # 辅助工具
    # ========================================================================

    def _exit(self, reason: str) -> StateTransition:
        """结束观察，清理并回 IDLE"""
        return self._transition(
            target="idle",
            action=StateAction.EXIT,
            direction=0,
            reason=reason
        )

    def _calc_atr(self, context: StateContext) -> float:
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

    def reset(self) -> None:
        super().reset()
        self._peak_close = None
        self._trend_direction = 0


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "top_detect",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["top_escape_detection"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> TopDetectState:
    return TopDetectState(config)
