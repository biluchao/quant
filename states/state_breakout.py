"""
states/state_breakout.py — 突破确认状态 (重铸标准版)

核心设计原则：
1. 严格时序验证：三根 K 线依次验证方向、成交量、OBI。
2. 杜绝自欺欺人：任何一个条件不满足，或者中途信号反转，立刻中止回 IDLE。
3. 强依赖：严格依赖 Context 中的强类型字段，不再使用 meta 传递。

状态机迁移：
    BREAKOUT_CONFIRM ──(三根 K 线全部通过)──► TREND_RIDE (action=ENTRY)
    BREAKOUT_CONFIRM ──(任意检查失败/超时)──► IDLE (action=EXIT)
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
from core.strategy import SignalType

logger = logging.getLogger(__name__)

class BreakoutConfirmState(StateProvider):
    """
    突破确认状态实现。
    """
    name = "breakout_confirm"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)
        
        state_cfg = config.get("states", {}).get(self.name, {})
        self.strategy_name = state_cfg.get("strategy_name", "trend_round")
        
        # 技术指标参数
        self.ma_period = int(state_cfg.get("ma_period", 25))
        self.volume_ma_window = int(state_cfg.get("volume_ma_window", 20))
        self.volume_threshold = float(state_cfg.get("volume_threshold", 1.5))
        self.obi_threshold = float(state_cfg.get("obi_threshold", 0.15))
        self.max_bars = int(state_cfg.get("max_bars_in_state", 5))
        self.atr_period = int(state_cfg.get("atr_period", 14))
        self.stop_atr_multiplier = float(state_cfg.get("initial_stop_atr", 1.5))

        # 内部状态：锁存突破方向
        self._direction = 0

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """进入确认状态，锁定当前信号的方向"""
        sig = context.signals.get(self.strategy_name)
        if not sig or sig.signal_type != SignalType.ENTRY:
            logger.error("非法进入 BREAKOUT_CONFIRM：未找到有效的 ENTRY 信号")
            self._direction = 0
            return

        self._direction = sig.direction
        logger.info(f"进入 BREAKOUT_CONFIRM，锁定方向: {self._direction}")

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """处理确认流程"""
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(f"BREAKOUT_CONFIRM 执行异常: {e}", exc_info=True)
            return self._abort("internal_error")

    def on_exit(self, context: StateContext) -> None:
        self._direction = 0

    # ========================================================================
    # 核心逻辑
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        # 1. 防御性检查
        if self._direction == 0:
            return self._abort("direction_lost")
        if context.active_round is not None:
            logger.critical("BREAKOUT_CONFIRM 中不允许存在活跃轮次！")
            return self._abort("unexpected_active_round")
        if not context.risk_approved:
            return self._abort("risk_blocked")

        # 2. 检查信号是否在中途反转
        current_sig = context.signals.get(self.strategy_name)
        if current_sig and current_sig.signal_type == SignalType.ENTRY:
            if current_sig.direction != 0 and current_sig.direction != self._direction:
                logger.warning(f"确认期间信号方向反转: {current_sig.direction} != {self._direction}")
                return self._abort("signal_reversed")

        # 3. 分阶段验证 (利用强类型的 bars_in_state)
        step = context.bars_in_state

        if step == 1:
            # 第一阶段：确认方向未回头（收盘价在均线同侧）
            if not self._check_direction(context):
                return self._abort("direction_reversed")
            logger.info("突破确认 Step 1 通过：方向一致。")
            return self._noop(reason="confirming_volume")

        elif step == 2:
            # 第二阶段：确认成交量放大
            if not self._check_volume(context):
                return self._abort("volume_insufficient")
            logger.info("突破确认 Step 2 通过：成交量放大。")
            return self._noop(reason="confirming_obi")

        elif step == 3:
            # 第三阶段：确认 OBI 同向
            if not self._check_obi(context):
                return self._abort("obi_disagree")
            logger.info("突破确认 Step 3 通过：OBI 同向。准备进入 TREND_RIDE。")
            return self._confirm_entry(context)

        else:
            return self._abort("timeout")

    # ========================================================================
    # 具体指标检查
    # ========================================================================

    def _check_direction(self, context: StateContext) -> bool:
        """检查收盘价是否在均线同侧"""
        if len(context.recent_bars) < self.ma_period:
            return False
            
        closes = [float(b.close) for b in context.recent_bars[-self.ma_period:]]
        ma = sum(closes) / self.ma_period
        current_close = float(context.bar.close)

        if self._direction == 1:
            return current_close > ma
        else:
            return current_close < ma

    def _check_volume(self, context: StateContext) -> bool:
        """检查成交量是否放大到均量的倍数以上"""
        # 需要 window + 1 根数据（包含当前和之前的 window 根）
        if len(context.recent_bars) < self.volume_ma_window + 1:
            return False
            
        # 取当前 K 线之前的 N 根计算均值
        hist_bars = context.recent_bars[-(self.volume_ma_window + 1):-1]
        vol_sum = sum(float(b.volume) for b in hist_bars)
        avg_vol = vol_sum / self.volume_ma_window
        
        if avg_vol <= 0:
            return False
            
        current_vol = float(context.bar.volume)
        return current_vol >= (avg_vol * self.volume_threshold)

    def _check_obi(self, context: StateContext) -> bool:
        """检查盘口失衡是否支持该方向"""
        obi_fv = context.factors.get("obi")
        if not obi_fv or not obi_fv.is_usable():
            return False
            
        val = obi_fv.value
        if self._direction == 1:
            return val > self.obi_threshold
        else:
            return val < -self.obi_threshold

    # ========================================================================
    # 转换与辅助工具
    # ========================================================================

    def _confirm_entry(self, context: StateContext) -> StateTransition:
        """三关全过，计算止损，发出开仓转换"""
        # 计算 ATR 作为止损基准
        atr = self._calc_atr(context)
        entry_price = Decimal(str(context.bar.close))
        
        if atr > 0:
            stop_distance = Decimal(str(atr * self.stop_atr_multiplier))
            stop_price = entry_price - stop_distance if self._direction == 1 else entry_price + stop_distance
        else:
            # 如果算不出 ATR，拒绝开仓以防裸奔
            return self._abort("atr_calculation_failed")

        return self._transition(
            target="trend_ride",
            action=StateAction.ENTRY,
            direction=self._direction,
            reason=TransitionReason.BREAKOUT_CONFIRMED.value,
            entry_hint=entry_price,
            stop_hint=stop_price
        )

    def _abort(self, reason: str) -> StateTransition:
        """中止确认，打回 IDLE"""
        logger.warning(f"突破确认失败，退回 IDLE。原因: {reason}")
        return self._transition(
            target="idle",
            action=StateAction.EXIT,
            direction=0,
            reason=reason
        )

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

    def reset(self) -> None:
        super().reset()
        self._direction = 0


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "breakout_confirm",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["breakout_validation"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> BreakoutConfirmState:
    return BreakoutConfirmState(config)
