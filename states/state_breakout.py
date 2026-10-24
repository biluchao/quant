"""
states/state_breakout.py — 突破确认状态（BREAKOUT_CONFIRM）

从 IDLE 进入，用三根 K 线验证突破是否成立。

状态机迁移：
    IDLE ──(ENTRY 信号)──► BREAKOUT_CONFIRM
    BREAKOUT_CONFIRM ──(三根 K 线确认)──► TREND_RIDE (action=ENTRY)
    BREAKOUT_CONFIRM ──(任意检查失败)──► IDLE (action=EXIT)

三根 K 线的确认逻辑（与 FACTOR_SPEC / PARAM_TABLE 一致）：
- K 线 1（步骤 0）：方向检查
    - 收盘价仍在均线同侧：sign(close - ma25) == entry_direction
- K 线 2（步骤 1）：方向 + 成交量检查
    - 方向仍然有效
    - 成交量 > 1.5 * mean(前 20 根成交量)
- K 线 3（步骤 2）：方向 + OBI 检查
    - 方向仍然有效
    - OBI 同向且 |OBI| > 0.15

任何一步失败 → 中止 → 返回 IDLE。

关键契约：
1. 方向的来源是 on_enter 时保存的 _entry_direction，不是确认期间的新信号
   - 如果确认期间策略反转方向 → 中止（不是跟随新方向）
2. 方向在每根 K 线都检查，任何一根反转则中止
3. 成交量只在 K 线 2 检查（与规格一致）
4. OBI 只在 K 线 3 检查（与规格一致）
5. 超时：bars_in_state > max_bars_in_state 则中止（默认 5 根）
6. 活跃轮次检查：有 active_round 时中止（防御性，BREAKOUT 不该有轮次）
7. 风控检查：risk_level in {halt, close_only} 时中止
8. 重复 K 线防重：_last_bar_ts 记录，重复返回 NOOP
9. 中止返回 target="idle", action=EXIT, direction=0
10. 成功返回 target="trend_ride", action=ENTRY, direction=_entry_direction
11. 中止不触发策略冷却（冷却由策略层的 on_round_closed 设置）
12. 内部异常包装成中止，不抛

与 TREND_RIDE 的职责边界：
- BREAKOUT_CONFIRM：验证方向，计算 stop_price_hint，输出 ENTRY 意图
- TREND_RIDE：接收活跃轮次，管止损/移动止损/逃顶

与 IDLE 的职责边界：
- IDLE：等待信号，只做"开始进入流程"
- BREAKOUT_CONFIRM：验证信号真实性，决定"是否真的开始"
- 实际下单在 BREAKOUT_CONFIRM → TREND_RIDE 时触发

为什么三根 K 线：
- 第 1 根：过滤"假突破"（突破瞬间方向反转）
- 第 2 根：过滤"无量假突破"（放量是趋势的必要条件）
- 第 3 根：过滤"微观不一致"（OBI 反向说明盘口不认可方向）

为什么时长上限 5 根：
- 正常情况下 3 根 K 线（9 分钟）足够确认
- 如果超过 5 根还没确认，说明方向在振荡
- 振荡期间确认 → 进入 TREND_RIDE → 立即被止损
- 中止是一种保护，不是失败

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from decimal import Decimal
from typing import Any, Dict, List, Optional
import logging
import time

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "breakout_confirm",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["breakout_confirm", "entry_validation"],
    "module_dependencies": [],
    "allowed_actions": ["entry", "exit", "noop"],
    "fallback": "idle",
    "enabled": True,
    "description": "Breakout confirm: validate 3-bar breakout before entering TREND_RIDE",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_STRATEGY_NAME = "trend_round"
DEFAULT_MA_PERIOD = 25
DEFAULT_VOLUME_MA_WINDOW = 20
DEFAULT_VOLUME_AMPLIFICATION_THRESHOLD = 1.5
DEFAULT_OBI_DIRECTION_THRESHOLD = 0.15
DEFAULT_MAX_BARS_IN_STATE = 5
DEFAULT_INITIAL_STOP_ATR = 1.5
DEFAULT_ATR_PERIOD = 14
STOP_PRICE_SANITY_PCT = 0.20  # 止损价超出 bar.close ± 20% 视为无效

# 阻止继续确认的风控等级
BLOCKING_RISK_LEVELS = {"halt", "close_only"}

# 状态持续超过此根数视为"确认超时"
DEFAULT_TIMEOUT_BARS = 5


# ============================================================================
# BreakoutConfirmState
# ============================================================================
class BreakoutConfirmState(StateProvider):
    """
    突破确认状态。详见模块 docstring。

    用法：
        state = BreakoutConfirmState(config)
        # 注册到状态机
        state_machine.register_state("breakout_confirm", state)
    """

    name = "breakout_confirm"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        state_cfg = config.get("states", {})
        breakout_cfg = state_cfg.get("breakout_confirm", {})

        # ---- 配置 ----
        self.strategy_name = breakout_cfg.get(
            "strategy_name", DEFAULT_STRATEGY_NAME
        )
        self.ma_period = int(
            breakout_cfg.get("ma_period", DEFAULT_MA_PERIOD)
        )
        self.volume_ma_window = int(
            breakout_cfg.get(
                "volume_ma_window", DEFAULT_VOLUME_MA_WINDOW
            )
        )
        self.volume_amplification_threshold = float(
            breakout_cfg.get(
                "volume_amplification_threshold",
                DEFAULT_VOLUME_AMPLIFICATION_THRESHOLD,
            )
        )
        self.obi_direction_threshold = float(
            breakout_cfg.get(
                "obi_direction_threshold",
                DEFAULT_OBI_DIRECTION_THRESHOLD,
            )
        )
        self.max_bars_in_state = int(
            breakout_cfg.get(
                "max_bars_in_state", DEFAULT_MAX_BARS_IN_STATE
            )
        )
        self.initial_stop_atr = float(
            breakout_cfg.get(
                "initial_stop_atr", DEFAULT_INITIAL_STOP_ATR
            )
        )

        # ---- 校验 ----
        if self.ma_period < 2:
            raise ValueError(
                f"ma_period must be >= 2, got {self.ma_period}"
            )
        if self.volume_ma_window < 2:
            raise ValueError(
                f"volume_ma_window must be >= 2, "
                f"got {self.volume_ma_window}"
            )
        if self.volume_amplification_threshold <= 0:
            raise ValueError(
                f"volume_amplification_threshold must be > 0, "
                f"got {self.volume_amplification_threshold}"
            )
        if not (0.0 < self.obi_direction_threshold <= 1.0):
            raise ValueError(
                f"obi_direction_threshold must be in (0, 1], "
                f"got {self.obi_direction_threshold}"
            )
        if self.max_bars_in_state < 3:
            raise ValueError(
                f"max_bars_in_state must be >= 3 (need 3 bars for confirmation), "
                f"got {self.max_bars_in_state}"
            )
        if self.initial_stop_atr <= 0:
            raise ValueError(
                f"initial_stop_atr must be > 0, "
                f"got {self.initial_stop_atr}"
            )

        # ---- 内部状态 ----
        self._entry_direction: int = 0
        self._entry_signal_id: Optional[str] = None
        self._entry_score: float = 0.0
        self._step: int = 0
        self._last_bar_ts: Optional[int] = None

        # ---- 统计 ----
        self._stats = {
            "enters": 0,
            "bar_closes": 0,
            "confirmed": 0,
            "aborted": 0,
            "noops": 0,
            "abort_reasons": {
                "no_entry_direction": 0,
                "unexpected_active_round": 0,
                "confirmation_timeout": 0,
                "risk_blocked": 0,
                "signal_reversed": 0,
                "direction_reversed": 0,
                "volume_insufficient": 0,
                "obi_disagree": 0,
                "invalid_step": 0,
                "duplicate_bar": 0,
                "internal_error": 0,
            },
        }

        logger.info(
            f"BreakoutConfirmState initialized "
            f"strategy_name={self.strategy_name} "
            f"ma_period={self.ma_period} "
            f"volume_ma_window={self.volume_ma_window} "
            f"volume_threshold={self.volume_amplification_threshold}x "
            f"obi_threshold={self.obi_direction_threshold} "
            f"max_bars_in_state={self.max_bars_in_state} "
            f"initial_stop_atr={self.initial_stop_atr}"
        )

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """
        进入 BREAKOUT_CONFIRM。保存原始信号信息。

        契约：
        - 从 context.signals[strategy_name] 提取 direction / signal_id / score
        - signals 缺失时 _entry_direction = 0，on_bar_close 会检测并中止
        - 重置 _step 和 _last_bar_ts
        """
        self._stats["enters"] += 1
        self._step = 0
        self._last_bar_ts = None

        signals = getattr(context, "signals", None) or {}
        signal = signals.get(self.strategy_name)

        if signal is None:
            logger.critical(
                f"BreakoutConfirmState entered without signal for "
                f"strategy '{self.strategy_name}'. This indicates "
                f"IDLE didn't validate properly."
            )
            self._entry_direction = 0
            self._entry_signal_id = None
            self._entry_score = 0.0
            return

        self._entry_direction = getattr(signal, "direction", 0)
        self._entry_signal_id = getattr(signal, "signal_id", None)
        self._entry_score = float(getattr(signal, "score", 0.0))

        logger.info(
            f"BreakoutConfirmState entered "
            f"direction={self._entry_direction} "
            f"signal_id={self._entry_signal_id} "
            f"score={self._entry_score:.4f}"
        )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """
        每根 K 线收线时执行确认步骤。

        契约：
        - 永远返回 StateTransition
        - 内部异常包装成中止
        - 每根 K 线只处理一次（_last_bar_ts 防重）
        """
        self._stats["bar_closes"] += 1
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(
                f"BreakoutConfirmState.on_bar_close error: {e}",
                exc_info=True,
            )
            self._stats["abort_reasons"]["internal_error"] += 1
            return self._abort("internal_error")

    def on_exit(self, context: StateContext) -> None:
        """退出 BREAKOUT_CONFIRM。清理内部状态。"""
        logger.debug(
            f"BreakoutConfirmState exited "
            f"(step={self._step}, confirmed={self._stats['confirmed']}, "
            f"aborted={self._stats['aborted']})"
        )
        self._step = 0
        self._last_bar_ts = None

    # ========================================================================
    # 核心：评估
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        """核心评估逻辑。"""
        # ---- 1. 防御性检查：无方向 ----
        if self._entry_direction not in (1, -1):
            return self._abort("no_entry_direction")

        # ---- 2. 活跃轮次防御 ----
        if getattr(context, "active_round", None) is not None:
            logger.critical(
                "BreakoutConfirmState has active_round. "
                "This is a state machine bug."
            )
            return self._abort("unexpected_active_round")

        # ---- 3. 超时 ----
        bars_in_state = getattr(context, "bars_in_state", 0)
        if bars_in_state > self.max_bars_in_state:
            logger.warning(
                f"BreakoutConfirmState timeout: "
                f"{bars_in_state} bars > {self.max_bars_in_state}"
            )
            return self._abort("confirmation_timeout")

        # ---- 4. 风控 ----
        risk_level = getattr(context, "risk_level", "unknown")
        if risk_level in BLOCKING_RISK_LEVELS:
            return self._abort(f"risk_blocked:{risk_level}")

        # ---- 5. 信号方向一致性 ----
        signals = getattr(context, "signals", None) or {}
        current_signal = signals.get(self.strategy_name)
        if current_signal is not None:
            current_dir = getattr(current_signal, "direction", 0)
            if current_dir != 0 and current_dir != self._entry_direction:
                logger.warning(
                    f"signal direction reversed during confirmation: "
                    f"entry={self._entry_direction} "
                    f"current={current_dir}"
                )
                return self._abort("signal_reversed")

        # ---- 6. 防重复 ----
        bar_ts = int(context.bar.ts)
        if self._last_bar_ts == bar_ts:
            self._stats["abort_reasons"]["duplicate_bar"] += 1
            return self._noop("duplicate_bar")
        self._last_bar_ts = bar_ts

        # ---- 7. 每根 K 线都检查方向 ----
        if not self._check_direction(context):
            logger.info(
                f"direction reversed at step={self._step}, "
                f"close={context.bar.close} "
                f"direction={self._entry_direction}"
            )
            return self._abort("direction_reversed")

        # ---- 8. 步骤执行 ----
        if self._step == 0:
            # K 线 1：方向已确认 → 进入 K 线 2
            self._step = 1
            logger.info(
                f"breakout confirm step 0 -> 1 "
                f"(direction confirmed, waiting for volume)"
            )
            return self._noop("confirming_volume")

        elif self._step == 1:
            # K 线 2：检查成交量
            if not self._check_volume(context):
                logger.info(
                    f"volume insufficient at step 1"
                )
                return self._abort("volume_insufficient")
            self._step = 2
            logger.info(
                f"breakout confirm step 1 -> 2 "
                f"(volume confirmed, waiting for obi)"
            )
            return self._noop("confirming_obi")

        elif self._step == 2:
            # K 线 3：检查 OBI
            if not self._check_obi(context):
                logger.info(
                    f"obi disagrees at step 2"
                )
                return self._abort("obi_disagree")

            # 三根 K 线全部通过 → 确认
            return self._confirm(context)

        else:
            return self._abort(f"invalid_step:{self._step}")

    # ========================================================================
    # 检查函数
    # ========================================================================

    def _check_direction(self, context: StateContext) -> bool:
        """
        检查方向是否仍有效：收盘价在均线的同侧。

        契约：
        - 数据不足时返回 False（防御性，中止）
        - 多方向：close > ma
        - 空方向：close < ma
        """
        bars = getattr(context, "recent_bars", [])
        if len(bars) < self.ma_period:
            logger.warning(
                f"insufficient bars for MA: {len(bars)} < {self.ma_period}"
            )
            return False

        recent = bars[-self.ma_period:]
        try:
            ma = sum(b.close for b in recent) / Decimal(str(self.ma_period))
        except (TypeError, ArithmeticError) as e:
            logger.warning(f"MA computation error: {e}")
            return False

        close = context.bar.close
        diff = close - ma

        if self._entry_direction == 1:
            return diff > 0
        else:
            return diff < 0

    def _check_volume(self, context: StateContext) -> bool:
        """
        检查成交量是否放大：当前 volume > 1.5 * mean(前 20 根)。

        契约：
        - 数据不足时返回 False
        - 均值接近 0 时返回 False（防御性）
        - 严格大于（不是 >=）
        """
        bars = getattr(context, "recent_bars", [])
        if len(bars) < self.volume_ma_window + 1:
            logger.warning(
                f"insufficient bars for volume MA: "
                f"{len(bars)} < {self.volume_ma_window + 1}"
            )
            return False

        # 前 N 根（不含当前 bar）
        prev_bars = bars[-(self.volume_ma_window + 1):-1]
        try:
            prev_volumes = [float(b.volume) for b in prev_bars]
            avg_volume = sum(prev_volumes) / self.volume_ma_window
        except (TypeError, ValueError) as e:
            logger.warning(f"volume MA computation error: {e}")
            return False

        if avg_volume < 1e-9:
            logger.warning("volume MA near zero")
            return False

        current_volume = float(context.bar.volume)
        threshold = self.volume_amplification_threshold * avg_volume

        passed = current_volume > threshold
        if not passed:
            logger.debug(
                f"volume check: current={current_volume:.4f} "
                f"threshold={threshold:.4f} "
                f"avg={avg_volume:.4f}"
            )
        return passed

    def _check_obi(self, context: StateContext) -> bool:
        """
        检查 OBI 是否同向且强度足够。

        契约：
        - OBI 因子不可用（DEGRADED/WARMUP/STALE）→ False
        - 多方向：OBI > obi_direction_threshold
        - 空方向：OBI < -obi_direction_threshold
        """
        factors = getattr(context, "factors", {}) or {}
        obi_fv = factors.get("obi")

        if obi_fv is None:
            logger.warning("OBI factor not present in context.factors")
            return False

        if not hasattr(obi_fv, "is_usable") or not obi_fv.is_usable():
            logger.info("OBI factor not usable (stale/error/outlier)")
            return False

        value = float(obi_fv.value)
        threshold = self.obi_direction_threshold

        if self._entry_direction == 1:
            passed = value > threshold
        else:
            passed = value < -threshold

        if not passed:
            logger.debug(
                f"obi check: value={value:.4f} "
                f"threshold=±{threshold} "
                f"direction={self._entry_direction}"
            )
        return passed

    # ========================================================================
    # 转换构造
    # ========================================================================

    def _confirm(self, context: StateContext) -> StateTransition:
        """
        三根 K 线全部确认，构造 ENTRY 转换。

        契约：
        - target="trend_ride"
        - action=ENTRY
        - direction=_entry_direction
        - entry_price_hint=bar.close
        - stop_price_hint 基于 ATR
        """
        self._stats["confirmed"] += 1
        self._step = 0  # 重置，虽然即将退出

        entry_price = context.bar.close
        stop_price = self._compute_stop_price(context, entry_price)

        logger.info(
            f"breakout confirmed: direction={self._entry_direction} "
            f"entry_price={entry_price} "
            f"stop_price={stop_price} "
            f"bars_elapsed={getattr(context, 'bars_in_state', 0)}"
        )

        return StateTransition(
            target="trend_ride",
            action=StateAction.ENTRY,
            direction=self._entry_direction,
            reason=TransitionReason.BREAKOUT_CONFIRMED.value,
            entry_price_hint=entry_price,
            stop_price_hint=stop_price,
            meta={
                "signal_id": self._entry_signal_id,
                "entry_score": self._entry_score,
                "bars_elapsed": getattr(context, "bars_in_state", 0),
                "confirm_step": 2,
            },
        )

    def _abort(self, reason: str) -> StateTransition:
        """
        中止确认，返回 IDLE。

        契约：
        - target="idle"
        - action=EXIT
        - direction=0
        - reason 为具体的中止原因
        """
        self._stats["aborted"] += 1
        reason_key = reason.split(":")[0]
        if reason_key in self._stats["abort_reasons"]:
            self._stats["abort_reasons"][reason_key] += 1
        self._step = 0

        logger.info(
            f"breakout confirm aborted: reason={reason} "
            f"entry_direction={self._entry_direction}"
        )

        return StateTransition(
            target="idle",
            action=StateAction.EXIT,
            direction=0,
            reason=reason,
            meta={
                "abort_reason": reason,
                "signal_id": self._entry_signal_id,
                "entry_score": self._entry_score,
            },
        )

    def _noop(self, reason: str) -> StateTransition:
        """NOOP 转换（不统计中止，因为还在确认中）。"""
        self._stats["noops"] += 1
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=reason,
            meta={
                "step": self._step,
                "signal_id": self._entry_signal_id,
            },
        )

    # ========================================================================
    # 止损价计算
    # ========================================================================

    def _compute_stop_price(
        self, context: StateContext, entry_price: Any
    ) -> Optional[Decimal]:
        """
        基于 ATR 计算止损价提示。

        契约：
        - ATR 数据不足 → 返回 None（由状态机使用默认值）
        - 止损价超出 bar.close ± 20% → 返回 None（防御性）
        """
        atr = self._compute_atr(context)
        if atr is None:
            return None

        try:
            entry = Decimal(str(entry_price))
            atr_dec = Decimal(str(atr))
            stop_distance = atr_dec * Decimal(str(self.initial_stop_atr))

            if self._entry_direction == 1:
                stop = entry - stop_distance
            else:
                stop = entry + stop_distance

            # 合理性检查
            if stop <= 0:
                logger.warning(
                    f"computed non-positive stop: {stop}, ignoring hint"
                )
                return None

            deviation = abs(stop - entry) / entry
            if deviation > Decimal(str(STOP_PRICE_SANITY_PCT)):
                logger.warning(
                    f"computed stop deviates {deviation * 100:.1f}% "
                    f"from entry, ignoring hint"
                )
                return None

            return stop
        except (ArithmeticError, ValueError, TypeError) as e:
            logger.warning(f"stop price computation error: {e}")
            return None

    def _compute_atr(
        self, context: StateContext, period: int = DEFAULT_ATR_PERIOD
    ) -> Optional[float]:
        """
        计算 ATR。

        契约：
        - 数据不足 → 返回 None
        """
        bars = getattr(context, "recent_bars", [])
        if len(bars) < period + 1:
            return None

        try:
            trs = []
            start = max(1, len(bars) - period - 1)
            for i in range(start, len(bars)):
                h = float(bars[i].high)
                l = float(bars[i].low)
                prev_c = float(bars[i - 1].close)
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                trs.append(tr)
            if len(trs) < period:
                return None
            return sum(trs[-period:]) / period
        except (TypeError, ValueError, AttributeError) as e:
            logger.warning(f"ATR computation error: {e}")
            return None

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def reset(self) -> None:
        """重置状态。"""
        self._entry_direction = 0
        self._entry_signal_id = None
        self._entry_score = 0.0
        self._step = 0
        self._last_bar_ts = None
        for k in self._stats:
            if k == "abort_reasons":
                for rk in self._stats["abort_reasons"]:
                    self._stats["abort_reasons"][rk] = 0
            else:
                self._stats[k] = 0
        logger.info("BreakoutConfirmState reset")

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        total = self._stats["bar_closes"]
        confirmed = self._stats["confirmed"]
        aborted = self._stats["aborted"]
        confirm_rate = (
            confirmed / total if total > 0 else 0.0
        )
        abort_rate = (
            aborted / total if total > 0 else 0.0
        )
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "strategy_name": self.strategy_name,
            "current_step": self._step,
            "entry_direction": self._entry_direction,
            "stats": dict(self._stats),
            "summary": {
                "bar_closes": total,
                "confirmed": confirmed,
                "aborted": aborted,
                "noops": self._stats["noops"],
                "confirm_rate": confirm_rate,
                "abort_rate": abort_rate,
            },
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> BreakoutConfirmState:
    """模块工厂入口。"""
    return BreakoutConfirmState(config)
