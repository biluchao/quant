"""
states/state_trend.py — 趋势骑行状态（TREND_RIDE）

在活跃轮次中进行持仓管理。

职责（唯一）：
1. 被动止损检查：价格触及 current_stop → EXIT(target="idle")
2. 逃顶检测：动量反转 + 足够盈利 → EXIT(target="top_detect")
3. 移动止损：盈利足够时收紧止损（HOLD + stop_price_hint）
4. 时间止损：持仓超过 max_bars_in_round → EXIT(target="idle")
5. 风控 HALT：立即 EXIT(target="idle", reason="risk_halt")

不做什么：
- 不加仓（Step 3 管）
- 不反向（Step 3 管）
- 不分批止盈（Step 3 管）
- 不直接下单（状态机 / 轮次管理 / 执行层负责）
- 不更新 active_round 的字段（只读）

状态机迁移：
    TREND_RIDE ──(止损触发)──► IDLE (action=EXIT)
    TREND_RIDE ──(逃顶触发)──► TOP_DETECT (action=EXIT)
    TREND_RIDE ──(时间止损)──► IDLE (action=EXIT)
    TREND_RIDE ──(风控 HALT)──► IDLE (action=EXIT)
    TREND_RIDE ──(移动止损)──► TREND_RIDE (action=HOLD, stop_price_hint)
    TREND_RIDE ──(继续持有)──► TREND_RIDE (action=HOLD)

关键契约：
1. 必须有活跃轮次（active_round 不为 None），否则 EXIT(target="idle")
2. 被动止损用 bar.low / bar.high（插针必须触发），不用 bar.close
3. 移动止损只朝有利方向移动（多单只上移，空单只下移）
4. 移动止损是 HOLD + stop_price_hint，由状态机决定是否采纳
5. 检查优先级：被动止损 > 风控 HALT > 逃顶 > 时间止损 > 移动止损
6. 逃顶：动量反转（`momentum.value * direction < -0.2`）+ 盈利 > 2 ATR
7. 时间止损：`(bar.ts - opened_ts) // interval_ms > max_bars_in_round`
8. 内部异常包装成 NOOP(error=...)，不抛
9. 每根 K 线只处理一次（_last_bar_ts 防重）
10. 所有状态用 active_round 的当前快照，不修改 active_round

与 TOP_DETECT 的职责边界：
- TREND_RIDE：持仓管理，检测逃顶信号
- TOP_DETECT：逃顶后的观察阶段，由状态机平仓 + 迁移
- TREND_RIDE 的 EXIT(target="top_detect") 触发状态机平仓 + 迁移
- TOP_DETECT 记录顶部价格（从 meta 读），观察后续

为什么被动止损用 bar.low / bar.high 而不是 bar.close：
- 插针行情下，K 线盘中可能瞬间打穿止损
- 收线时 close 可能回到止损之上，此时止损"没触发"
- 但持仓可能已在盘中被打穿（如果是实盘服务器端止损）
- 用 low/high 判断更保守，对风险更负责
- 代价：可能会有一两根 K 线的"假触发"（收盘价仍正常）
- 但假触发可以承受（少赚一点），漏触发不能承受（可能大亏）

为什么移动止损只能上移（多单）：
- 止损放宽 = 风险放大
- 风控层（RiskPolicy）已经基于 current_stop 批准了本轮
- 状态层放宽止损 = 绕过风控 = 灾难
- 即使行情"看起来会回来"，也不能放宽
- 正确做法：平仓重新进（走完 IDLE → BREAKOUT_CONFIRM → TREND_RIDE）

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
    "name": "trend_ride",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["trend_ride", "position_management", "trailing_stop"],
    "module_dependencies": [],
    "allowed_actions": ["hold", "exit", "noop"],
    "fallback": "idle",
    "enabled": True,
    "description": "Trend ride: manage active round with passive stop, trailing stop, and top-escape detection",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_BARS_IN_ROUND = 60
DEFAULT_TRAILING_STOP_TRIGGER_ATR = 1.5
DEFAULT_TRAILING_STOP_DISTANCE_ATR = 1.0
DEFAULT_TOP_ESCAPE_MIN_PROFIT_ATR = 2.0
DEFAULT_MOMENTUM_REVERSAL_THRESHOLD = 0.2
DEFAULT_ATR_PERIOD = 14
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000
MIN_TAKE_PROFIT = Decimal("0")

# 阻止持仓的风控等级
BLOCKING_RISK_LEVELS = {"halt"}


# ============================================================================
# TrendRideState
# ============================================================================
class TrendRideState(StateProvider):
    """
    趋势骑行状态。详见模块 docstring。

    用法：
        state = TrendRideState(config)
        # 注册到状态机
        state_machine.register_state("trend_ride", state)
    """

    name = "trend_ride"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        state_cfg = config.get("states", {})
        trend_cfg = state_cfg.get("trend_ride", {})

        # ---- 配置 ----
        self.max_bars_in_round = int(
            trend_cfg.get(
                "max_bars_in_round", DEFAULT_MAX_BARS_IN_ROUND
            )
        )
        self.trailing_stop_trigger_atr = float(
            trend_cfg.get(
                "trailing_stop_trigger_atr",
                DEFAULT_TRAILING_STOP_TRIGGER_ATR,
            )
        )
        self.trailing_stop_distance_atr = float(
            trend_cfg.get(
                "trailing_stop_distance_atr",
                DEFAULT_TRAILING_STOP_DISTANCE_ATR,
            )
        )
        self.top_escape_min_profit_atr = float(
            trend_cfg.get(
                "top_escape_min_profit_atr",
                DEFAULT_TOP_ESCAPE_MIN_PROFIT_ATR,
            )
        )
        self.momentum_reversal_threshold = float(
            trend_cfg.get(
                "momentum_reversal_threshold",
                DEFAULT_MOMENTUM_REVERSAL_THRESHOLD,
            )
        )
        self.atr_period = int(
            trend_cfg.get("atr_period", DEFAULT_ATR_PERIOD)
        )
        self.kline_interval_ms = int(
            trend_cfg.get(
                "kline_interval_ms", DEFAULT_KLINE_INTERVAL_MS
            )
        )

        # ---- 参数校验 ----
        if self.max_bars_in_round < 1:
            raise ValueError(
                f"max_bars_in_round must be >= 1, "
                f"got {self.max_bars_in_round}"
            )
        if self.trailing_stop_trigger_atr <= 0:
            raise ValueError(
                f"trailing_stop_trigger_atr must be > 0, "
                f"got {self.trailing_stop_trigger_atr}"
            )
        if self.trailing_stop_distance_atr <= 0:
            raise ValueError(
                f"trailing_stop_distance_atr must be > 0, "
                f"got {self.trailing_stop_distance_atr}"
            )
        if self.top_escape_min_profit_atr < 0:
            raise ValueError(
                f"top_escape_min_profit_atr must be >= 0, "
                f"got {self.top_escape_min_profit_atr}"
            )
        if not (0.0 <= self.momentum_reversal_threshold <= 1.0):
            raise ValueError(
                f"momentum_reversal_threshold must be in [0, 1], "
                f"got {self.momentum_reversal_threshold}"
            )
        if self.atr_period < 2:
            raise ValueError(
                f"atr_period must be >= 2, got {self.atr_period}"
            )
        if self.kline_interval_ms <= 0:
            raise ValueError(
                f"kline_interval_ms must be > 0, "
                f"got {self.kline_interval_ms}"
            )

        # ---- 内部状态 ----
        self._direction: int = 0
        self._last_bar_ts: Optional[int] = None
        self._peak_close: Optional[Decimal] = None
        self._entry_price: Optional[Decimal] = None
        self._entry_signal_id: Optional[str] = None

        # ---- 统计 ----
        self._stats = {
            "enters": 0,
            "bar_closes": 0,
            "holds": 0,
            "exits": 0,
            "noops": 0,
            "trailing_stop_updates": 0,
            "exit_reasons": {
                "stop_triggered": 0,
                "top_escape": 0,
                "timeout": 0,
                "risk_halt": 0,
                "no_active_round": 0,
                "invalid_direction": 0,
                "invalid_stop": 0,
                "duplicate_bar": 0,
                "internal_error": 0,
            },
        }

        logger.info(
            f"TrendRideState initialized "
            f"max_bars_in_round={self.max_bars_in_round} "
            f"trailing_trigger_atr={self.trailing_stop_trigger_atr} "
            f"trailing_distance_atr={self.trailing_stop_distance_atr} "
            f"top_escape_min_profit_atr={self.top_escape_min_profit_atr} "
            f"momentum_reversal_threshold={self.momentum_reversal_threshold} "
            f"atr_period={self.atr_period} "
            f"kline_interval_ms={self.kline_interval_ms}"
        )

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """
        进入 TREND_RIDE。记录入场信息。

        契约：
        - 从 context.active_round 读方向、入场价、signal_id
        - 无 active_round 时打 CRITICAL，_direction=0，on_bar_close 会退出
        - 重置 _last_bar_ts 和 _peak_close
        """
        self._stats["enters"] += 1
        self._last_bar_ts = None
        self._peak_close = None
        self._entry_price = None
        self._entry_signal_id = None

        round = getattr(context, "active_round", None)
        if round is None:
            logger.critical(
                "TrendRideState entered without active_round. "
                "This is a state machine bug."
            )
            self._direction = 0
            return

        self._direction = int(round.get("direction", 0))
        if self._direction not in (1, -1):
            logger.critical(
                f"TrendRideState entered with invalid direction "
                f"{self._direction}. This is a state machine bug."
            )
            return

        entry_price = round.get("entry_price")
        if entry_price is not None:
            try:
                self._entry_price = Decimal(str(entry_price))
            except Exception:
                self._entry_price = None

        self._entry_signal_id = round.get("signal_id")

        logger.info(
            f"TrendRideState entered: "
            f"direction={self._direction} "
            f"entry={self._entry_price} "
            f"current_stop={round.get('current_stop')} "
            f"signal_id={self._entry_signal_id}"
        )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """
        每根 K 线收线时管理持仓。

        契约：
        - 永远返回 StateTransition
        - 内部异常包装成 NOOP(error=...)
        - 检查优先级：止损 > 风控 > 逃顶 > 时间止损 > 移动止损
        """
        self._stats["bar_closes"] += 1
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(
                f"TrendRideState.on_bar_close error: {e}",
                exc_info=True,
            )
            self._stats["exit_reasons"]["internal_error"] += 1
            return self._noop("internal_error")

    def on_exit(self, context: StateContext) -> None:
        """退出 TREND_RIDE。清理内部状态。"""
        logger.debug(
            f"TrendRideState exited "
            f"(direction={self._direction}, "
            f"holds={self._stats['holds']}, "
            f"exits={self._stats['exits']})"
        )
        self._direction = 0
        self._last_bar_ts = None
        self._peak_close = None
        self._entry_price = None
        self._entry_signal_id = None

    # ========================================================================
    # 核心：评估
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        """核心评估逻辑。"""
        # ---- 1. 方向校验 ----
        if self._direction not in (1, -1):
            return self._exit("idle", "invalid_direction")

        # ---- 2. 活跃轮次校验 ----
        round = getattr(context, "active_round", None)
        if round is None:
            return self._exit("idle", "no_active_round")

        # 方向一致性
        round_direction = int(round.get("direction", 0))
        if round_direction != self._direction:
            logger.critical(
                f"direction mismatch: state={self._direction} "
                f"round={round_direction}"
            )
            return self._exit("idle", "invalid_direction")

        # ---- 3. 防重复 ----
        bar_ts = int(context.bar.ts)
        if self._last_bar_ts == bar_ts:
            self._stats["exit_reasons"]["duplicate_bar"] += 1
            return self._noop("duplicate_bar")
        self._last_bar_ts = bar_ts

        # ---- 4. 更新峰值（用于逃顶归因）----
        current_close = context.bar.close
        self._update_peak(current_close, self._direction)

        # ---- 5. 被动止损（最高优先级）----
        if self._check_stop(context, round):
            return self._exit("idle", "stop_triggered")

        # ---- 6. 风控 HALT（次高优先级）----
        risk_level = getattr(context, "risk_level", "unknown")
        if risk_level in BLOCKING_RISK_LEVELS:
            return self._exit("idle", "risk_halt")

        # ---- 7. 逃顶 ----
        if self._check_top_escape(context, round):
            return self._exit("top_detect", "top_escape")

        # ---- 8. 时间止损 ----
        if self._check_timeout(context, round):
            return self._exit("idle", "timeout")

        # ---- 9. 移动止损 ----
        new_stop = self._update_trailing_stop(context, round)
        if new_stop is not None:
            return self._hold_with_new_stop(new_stop)

        # ---- 10. 继续持有 ----
        return self._hold()

    # ========================================================================
    # 检查函数
    # ========================================================================

    def _check_stop(
        self, context: StateContext, round: dict
    ) -> bool:
        """
        被动止损检查。

        契约：
        - 用 bar.low / bar.high（不是 bar.close）
        - 多单：bar.low <= current_stop
        - 空单：bar.high >= current_stop
        - current_stop 缺失时视为触发（保守）
        """
        current_stop_raw = round.get("current_stop")
        if current_stop_raw is None:
            logger.critical(
                "active_round has no current_stop, "
                "treating as stop triggered"
            )
            self._stats["exit_reasons"]["invalid_stop"] += 1
            return True

        try:
            current_stop = Decimal(str(current_stop_raw))
        except Exception:
            logger.critical(
                f"invalid current_stop: {current_stop_raw}"
            )
            return True

        bar = context.bar
        if self._direction == 1:
            bar_low = getattr(bar, "low", None)
            if bar_low is None:
                return False
            return Decimal(str(bar_low)) <= current_stop
        else:
            bar_high = getattr(bar, "high", None)
            if bar_high is None:
                return False
            return Decimal(str(bar_high)) >= current_stop

    def _check_top_escape(
        self, context: StateContext, round: dict
    ) -> bool:
        """
        逃顶检测。

        契约：
        - 需盈利 > top_escape_min_profit_atr * ATR
        - 需动量反转：momentum.value * direction < -threshold
        - 任一条件不满足返回 False
        """
        # ---- 盈利检查 ----
        atr = self._compute_atr(context)
        if atr is None:
            return False

        entry_price = self._entry_price
        if entry_price is None:
            try:
                entry_price = Decimal(
                    str(round.get("entry_price", 0))
                )
            except Exception:
                return False
        if entry_price <= 0:
            return False

        current_close = context.bar.close
        pnl = (current_close - entry_price) * self._direction

        min_profit = (
            Decimal(str(self.top_escape_min_profit_atr))
            * Decimal(str(atr))
        )
        if pnl < min_profit:
            return False

        # ---- 动量反转检查 ----
        factors = getattr(context, "factors", {}) or {}
        momentum = factors.get("momentum")
        if momentum is None:
            return False
        if not hasattr(momentum, "is_usable") or not momentum.is_usable():
            return False

        momentum_score = float(momentum.value) * self._direction
        if momentum_score >= -self.momentum_reversal_threshold:
            return False

        logger.info(
            f"top escape detected: "
            f"direction={self._direction} "
            f"pnl={pnl} min_profit={min_profit} "
            f"momentum_score={momentum_score:.4f} "
            f"peak_close={self._peak_close}"
        )
        return True

    def _check_timeout(
        self, context: StateContext, round: dict
    ) -> bool:
        """
        时间止损检查。

        契约：
        - bars_held > max_bars_in_round 触发
        - opened_ts 缺失时视为 0（不触发）
        """
        opened_ts_raw = round.get("opened_ts")
        if opened_ts_raw is None:
            return False

        try:
            opened_ts = int(opened_ts_raw)
        except (ValueError, TypeError):
            return False

        if opened_ts <= 0:
            return False

        elapsed_ms = int(context.bar.ts) - opened_ts
        if elapsed_ms < 0:
            return False

        bars_held = elapsed_ms // self.kline_interval_ms
        if bars_held > self.max_bars_in_round:
            logger.info(
                f"timeout: bars_held={bars_held} > "
                f"{self.max_bars_in_round}"
            )
            return True

        return False

    def _update_trailing_stop(
        self, context: StateContext, round: dict
    ) -> Optional[Decimal]:
        """
        更新移动止损。

        契约：
        - 盈利 > trailing_stop_trigger_atr * ATR 才更新
        - 新止损 = bar.close - distance（多单）/ bar.close + distance（空单）
        - 多单只上移，空单只下移
        - 新止损必须 > 0
        - 新止损必须严格优于旧止损，否则返回 None
        """
        atr = self._compute_atr(context)
        if atr is None:
            return None

        entry_price = self._entry_price
        if entry_price is None:
            return None

        current_close = context.bar.close
        pnl = (current_close - entry_price) * self._direction

        trigger_profit = (
            Decimal(str(self.trailing_stop_trigger_atr))
            * Decimal(str(atr))
        )
        if pnl < trigger_profit:
            return None

        current_stop_raw = round.get("current_stop")
        if current_stop_raw is None:
            return None
        try:
            current_stop = Decimal(str(current_stop_raw))
        except Exception:
            return None

        distance = (
            Decimal(str(self.trailing_stop_distance_atr))
            * Decimal(str(atr))
        )

        if self._direction == 1:
            new_stop = current_close - distance
        else:
            new_stop = current_close + distance

        # 合理性检查
        if new_stop <= 0:
            return None

        # 方向性检查
        if self._direction == 1 and new_stop <= current_stop:
            return None
        if self._direction == -1 and new_stop >= current_stop:
            return None

        return new_stop

    # ========================================================================
    # 辅助
    # ========================================================================

    def _update_peak(
        self, current_close: Any, direction: int
    ) -> None:
        """更新峰值收盘价（用于逃顶归因）。"""
        try:
            close_dec = Decimal(str(current_close))
        except Exception:
            return

        if self._peak_close is None:
            self._peak_close = close_dec
        elif direction == 1 and close_dec > self._peak_close:
            self._peak_close = close_dec
        elif direction == -1 and close_dec < self._peak_close:
            self._peak_close = close_dec

    def _compute_atr(
        self,
        context: StateContext,
        period: Optional[int] = None,
    ) -> Optional[float]:
        """
        计算 ATR。

        契约：
        - 数据不足返回 None
        - TR = max(high-low, |high-prev_close|, |low-prev_close|)
        """
        p = period or self.atr_period
        bars = getattr(context, "recent_bars", [])
        if len(bars) < p + 1:
            return None

        try:
            trs = []
            start = max(1, len(bars) - p - 1)
            for i in range(start, len(bars)):
                h = float(bars[i].high)
                l = float(bars[i].low)
                prev_c = float(bars[i - 1].close)
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                trs.append(tr)
            if len(trs) < p:
                return None
            return sum(trs[-p:]) / p
        except (TypeError, ValueError, AttributeError) as e:
            logger.warning(f"ATR computation error: {e}")
            return None

    # ========================================================================
    # 转换构造
    # ========================================================================

    def _hold(self) -> StateTransition:
        """HOLD 转换（不更新止损）。"""
        self._stats["holds"] += 1
        return StateTransition(
            target=None,
            action=StateAction.HOLD,
            reason="holding",
            meta={
                "direction": self._direction,
                "peak_close": (
                    str(self._peak_close)
                    if self._peak_close is not None else None
                ),
            },
        )

    def _hold_with_new_stop(
        self, new_stop: Decimal
    ) -> StateTransition:
        """HOLD 转换（更新止损）。"""
        self._stats["holds"] += 1
        self._stats["trailing_stop_updates"] += 1
        return StateTransition(
            target=None,
            action=StateAction.HOLD,
            reason="trailing_stop_update",
            stop_price_hint=new_stop,
            meta={
                "direction": self._direction,
                "trailing_stop": str(new_stop),
                "peak_close": (
                    str(self._peak_close)
                    if self._peak_close is not None else None
                ),
            },
        )

    def _exit(self, target: str, reason: str) -> StateTransition:
        """
        EXIT 转换。

        契约：
        - target="idle"（止损/超时/风控）或 "top_detect"（逃顶）
        - action=EXIT
        - direction=0
        """
        self._stats["exits"] += 1
        reason_key = reason.split(":")[0]
        if reason_key in self._stats["exit_reasons"]:
            self._stats["exit_reasons"][reason_key] += 1

        logger.info(
            f"TrendRideState exit: target={target} "
            f"reason={reason} direction={self._direction} "
            f"peak_close={self._peak_close}"
        )

        return StateTransition(
            target=target,
            action=StateAction.EXIT,
            direction=0,
            reason=reason,
            meta={
                "exit_reason": reason,
                "direction": self._direction,
                "entry_price": (
                    str(self._entry_price)
                    if self._entry_price is not None else None
                ),
                "peak_close": (
                    str(self._peak_close)
                    if self._peak_close is not None else None
                ),
                "signal_id": self._entry_signal_id,
            },
        )

    def _noop(self, reason: str) -> StateTransition:
        """NOOP 转换（不改变状态）。"""
        self._stats["noops"] += 1
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=reason,
            meta={
                "direction": self._direction,
            },
        )

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def reset(self) -> None:
        """重置状态。"""
        self._direction = 0
        self._last_bar_ts = None
        self._peak_close = None
        self._entry_price = None
        self._entry_signal_id = None
        for k in self._stats:
            if k == "exit_reasons":
                for rk in self._stats["exit_reasons"]:
                    self._stats["exit_reasons"][rk] = 0
            else:
                self._stats[k] = 0
        logger.info("TrendRideState reset")

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        total = self._stats["bar_closes"]
        holds = self._stats["holds"]
        exits = self._stats["exits"]
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "direction": self._direction,
            "peak_close": (
                str(self._peak_close)
                if self._peak_close is not None else None
            ),
            "entry_price": (
                str(self._entry_price)
                if self._entry_price is not None else None
            ),
            "signal_id": self._entry_signal_id,
            "stats": dict(self._stats),
            "summary": {
                "bar_closes": total,
                "holds": holds,
                "exits": exits,
                "trailing_stop_updates": self._stats[
                    "trailing_stop_updates"
                ],
                "exit_reasons": dict(self._stats["exit_reasons"]),
            },
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> TrendRideState:
    """模块工厂入口。"""
    return TrendRideState(config)
