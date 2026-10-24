"""
states/state_top_detect.py — 逃顶观察状态（TOP_DETECT）

TREND_RIDE 检测到逃顶后，平仓并迁移到此状态，观察逃顶质量。

职责（唯一）：
1. 记录逃顶时的峰值价格（从 meta 读，或从 recent_bars 推导）
2. 观察后续价格走势，判定逃顶质量：
   - escape_confirmed: 价格继续远离峰值（确认逃顶成功）
   - false_escape: 价格回到峰值附近（假逃顶）
   - observation_timeout: 观察窗口结束，仍未明确
3. 记录观察结果到 _stats 和 meta（供 Step 3 反向策略使用）
4. 无论哪种结果，都 EXIT(target="idle")

不做什么：
- 不开新仓（Step 1 不做反向）
- 不做反向做单（Step 3 的事）
- 不修改 active_round（进入时应已平仓）
- 不触发策略冷却（策略层 on_round_closed 负责）
- 不感知策略/风控/订单（只读 context）

状态机迁移：
    TREND_RIDE ──(逃顶触发)──► TOP_DETECT (action=EXIT)
    TOP_DETECT ──(确认逃顶成功)──► IDLE (action=EXIT, reason="escape_confirmed")
    TOP_DETECT ──(假逃顶)──► IDLE (action=EXIT, reason="false_escape")
    TOP_DETECT ──(观察超时)──► IDLE (action=EXIT, reason="observation_timeout")
    TOP_DETECT ──(风控 HALT)──► IDLE (action=EXIT, reason="risk_halt")
    TOP_DETECT ──(仍在观察)──► TOP_DETECT (action=HOLD)

关键契约：
1. 所有退出都是 EXIT(target="idle")，永不输出 ENTRY/ADD/REDUCE
2. direction 永远为 0（TOP_DETECT 不做方向决策）
3. 进入时无 active_round（TREND_RIDE 已平仓）
4. 进入时有 active_round → CRITICAL 日志 + EXIT(target="idle")
5. 依赖 context.meta 传递：
   - trend_direction: TREND_RIDE 的方向（1=多单逃顶, -1=空单逃底）
   - peak_close: 逃顶时的峰值收盘价
   - peak_bar_ts: 峰值出现的时间
   - entry_price: 原轮次入场价
   - signal_id: 原信号 ID
6. meta 缺失时 fallback：
   - trend_direction: 从 recent_bars 近期趋势推导，默认 +1
   - peak_close: 从 recent_bars 的 max(high) / min(low) 推导
7. 用 bar.close 判断逃顶质量，不用 high/low（观察不是止损）
8. 超时：observation_count > max_observation_bars
9. 每次进入重置内部状态，多次进入完全独立
10. 内部异常包装成 NOOP(error=...)，不抛

为什么 TOP_DETECT 在 Step 1 不做反向：
- 反向做单是 Step 3 的事
- Step 1 的目标是"跑通闭环"，不是"覆盖所有机会"
- 逃顶后的反向做单需要更复杂的逻辑（趋势反转确认、空单资金管理、
  反向单的不对称参数），Step 1 没这些基础设施
- 但 TOP_DETECT 在 Step 1 里仍然有价值：
  * 记录逃顶质量（用于 Step 3 设计反向策略的参数）
  * 记录假逃顶的频率（衡量 TREND_RIDE 逃顶阈值的合理性）
  * 保留状态机结构，Step 3 只需扩展 TOP_DETECT 的逻辑

Step 1 的观察结果如何被 Step 3 使用：
- `state_transitions` 表记录所有 TOP_DETECT 的进入和退出
- `meta` 含 peak_close / final_close / escape_confirmed / observation_bars
- 分析数据可回答：
  * 逃顶成功率高吗？（如果低于 60%，逃顶阈值偏松）
  * 假逃顶频率高吗？（如果高于 30%，逃顶阈值偏紧）
  * 观察窗口够长吗？（如果 40% 都超时，观察窗口偏短）
- Step 3 根据这些数据设计反向做单的参数和触发条件

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
    "name": "top_detect",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["top_detect", "escape_quality_observation"],
    "module_dependencies": [],
    "allowed_actions": ["hold", "exit", "noop"],
    "fallback": "idle",
    "enabled": True,
    "description": "Top detect observation: evaluate escape quality, never trade (Step 1)",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_OBSERVATION_BARS = 10
DEFAULT_ESCAPE_CONFIRM_DISTANCE_ATR = 2.0
DEFAULT_ESCAPE_RETURN_TOLERANCE_ATR = 0.5
DEFAULT_ATR_PERIOD = 14
DEFAULT_TREND_LOOKBACK_BARS = 5

# 阻止继续观察的风控等级
BLOCKING_RISK_LEVELS = {"halt"}


# ============================================================================
# TopDetectState
# ============================================================================
class TopDetectState(StateProvider):
    """
    逃顶观察状态。详见模块 docstring。

    用法：
        state = TopDetectState(config)
        # 注册到状态机
        state_machine.register_state("top_detect", state)
    """

    name = "top_detect"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        state_cfg = config.get("states", {})
        top_cfg = state_cfg.get("top_detect", {})

        # ---- 配置 ----
        self.max_observation_bars = int(
            top_cfg.get(
                "max_observation_bars", DEFAULT_MAX_OBSERVATION_BARS
            )
        )
        self.escape_confirm_distance_atr = float(
            top_cfg.get(
                "escape_confirm_distance_atr",
                DEFAULT_ESCAPE_CONFIRM_DISTANCE_ATR,
            )
        )
        self.escape_return_tolerance_atr = float(
            top_cfg.get(
                "escape_return_tolerance_atr",
                DEFAULT_ESCAPE_RETURN_TOLERANCE_ATR,
            )
        )
        self.atr_period = int(
            top_cfg.get("atr_period", DEFAULT_ATR_PERIOD)
        )

        # ---- 参数校验 ----
        if self.max_observation_bars < 1:
            raise ValueError(
                f"max_observation_bars must be >= 1, "
                f"got {self.max_observation_bars}"
            )
        if self.escape_confirm_distance_atr <= 0:
            raise ValueError(
                f"escape_confirm_distance_atr must be > 0, "
                f"got {self.escape_confirm_distance_atr}"
            )
        if self.escape_return_tolerance_atr < 0:
            raise ValueError(
                f"escape_return_tolerance_atr must be >= 0, "
                f"got {self.escape_return_tolerance_atr}"
            )
        if (
            self.escape_return_tolerance_atr
            >= self.escape_confirm_distance_atr
        ):
            raise ValueError(
                f"escape_return_tolerance_atr "
                f"({self.escape_return_tolerance_atr}) must be < "
                f"escape_confirm_distance_atr "
                f"({self.escape_confirm_distance_atr}), "
                f"otherwise false_escape and escape_confirmed "
                f"would overlap"
            )
        if self.atr_period < 2:
            raise ValueError(
                f"atr_period must be >= 2, got {self.atr_period}"
            )

        # ---- 内部状态 ----
        self._trend_direction: int = 0
        self._escape_direction: int = 0
        self._peak_close: Optional[Decimal] = None
        self._peak_bar_ts: Optional[int] = None
        self._entry_price: Optional[Decimal] = None
        self._entry_signal_id: Optional[str] = None
        self._observation_count: int = 0
        self._last_bar_ts: Optional[int] = None
        self._meta_received: bool = False

        # ---- 统计 ----
        self._stats = {
            "enters": 0,
            "bar_closes": 0,
            "holds": 0,
            "exits": 0,
            "noops": 0,
            "meta_missing": 0,
            "exit_reasons": {
                "escape_confirmed": 0,
                "false_escape": 0,
                "observation_timeout": 0,
                "risk_halt": 0,
                "unexpected_active_round": 0,
                "invalid_direction": 0,
                "duplicate_bar": 0,
                "internal_error": 0,
            },
        }

        logger.info(
            f"TopDetectState initialized "
            f"max_observation_bars={self.max_observation_bars} "
            f"escape_confirm_distance_atr="
            f"{self.escape_confirm_distance_atr} "
            f"escape_return_tolerance_atr="
            f"{self.escape_return_tolerance_atr} "
            f"atr_period={self.atr_period}"
        )

    # ========================================================================
    # 生命周期钩子
    # ========================================================================

    def on_enter(self, context: StateContext) -> None:
        """
        进入 TOP_DETECT。记录峰值信息。

        契约：
        - 优先从 context.meta 读
        - meta 缺失 → fallback 从 recent_bars 推导
        - 完全无法推导 → 打 CRITICAL，_trend_direction=0，on_bar_close 会退出
        - 幂等：多次进入重置状态
        """
        self._stats["enters"] += 1

        # 重置内部状态
        self._observation_count = 0
        self._last_bar_ts = None
        self._peak_close = None
        self._peak_bar_ts = None
        self._entry_price = None
        self._entry_signal_id = None
        self._trend_direction = 0
        self._escape_direction = 0
        self._meta_received = False

        # ---- 1. 尝试从 context.meta 读取 ----
        meta = getattr(context, "meta", None)
        if isinstance(meta, dict) and meta:
            self._meta_received = True
            self._load_from_meta(meta)

        # ---- 2. meta 缺失，fallback ----
        if not self._meta_received:
            self._stats["meta_missing"] += 1
            logger.warning(
                "TopDetectState entered without context.meta, "
                "using fallback derivation from recent_bars"
            )
            self._derive_fallback(context)

        # ---- 3. 校验结果 ----
        if self._trend_direction not in (1, -1):
            logger.critical(
                f"TopDetectState entered with invalid trend_direction="
                f"{self._trend_direction}. This indicates a state machine "
                f"or context bug."
            )
            self._escape_direction = 0
            return

        self._escape_direction = -self._trend_direction

        # ---- 4. 合理性检查：peak 必须存在 ----
        if self._peak_close is None or self._peak_close <= 0:
            logger.critical(
                f"TopDetectState entered with invalid peak_close="
                f"{self._peak_close}. Using current bar.close as fallback."
            )
            try:
                self._peak_close = Decimal(str(context.bar.close))
                self._peak_bar_ts = int(context.bar.ts)
            except Exception as e:
                logger.critical(
                    f"failed to use bar.close as peak: {e}"
                )

        logger.info(
            f"TopDetectState entered: "
            f"trend_direction={self._trend_direction} "
            f"escape_direction={self._escape_direction} "
            f"peak_close={self._peak_close} "
            f"peak_bar_ts={self._peak_bar_ts} "
            f"entry_price={self._entry_price} "
            f"signal_id={self._entry_signal_id} "
            f"meta_received={self._meta_received}"
        )

    def on_bar_close(self, context: StateContext) -> StateTransition:
        """
        每根 K 线收线时观察逃顶质量。

        契约：
        - 永远返回 StateTransition
        - 内部异常包装成 NOOP(error=...)
        - 检查优先级：active_round 异常 > 风控 HALT > 逃顶确认 > 假逃顶 > 超时
        """
        self._stats["bar_closes"] += 1
        try:
            return self._evaluate(context)
        except Exception as e:
            logger.error(
                f"TopDetectState.on_bar_close error: {e}",
                exc_info=True,
            )
            self._stats["exit_reasons"]["internal_error"] += 1
            return self._noop("internal_error")

    def on_exit(self, context: StateContext) -> None:
        """退出 TOP_DETECT。清理内部状态。"""
        logger.debug(
            f"TopDetectState exited "
            f"(observations={self._observation_count}, "
            f"peak_close={self._peak_close}, "
            f"trend_direction={self._trend_direction})"
        )
        self._observation_count = 0
        self._last_bar_ts = None
        self._peak_close = None
        self._peak_bar_ts = None
        self._entry_price = None
        self._entry_signal_id = None
        self._trend_direction = 0
        self._escape_direction = 0
        self._meta_received = False

    # ========================================================================
    # 核心：评估
    # ========================================================================

    def _evaluate(self, context: StateContext) -> StateTransition:
        """核心评估逻辑。"""
        # ---- 1. 方向有效性检查 ----
        if self._escape_direction not in (1, -1):
            return self._exit("invalid_direction")

        # ---- 2. active_round 防御（TOP_DETECT 不该有轮次）----
        if getattr(context, "active_round", None) is not None:
            logger.critical(
                "TopDetectState has active_round. "
                "TREND_RIDE should have closed the round before "
                "transitioning to TOP_DETECT."
            )
            return self._exit("unexpected_active_round")

        # ---- 3. 防重复 ----
        bar_ts = int(context.bar.ts)
        if self._last_bar_ts == bar_ts:
            self._stats["exit_reasons"]["duplicate_bar"] += 1
            return self._noop("duplicate_bar")
        self._last_bar_ts = bar_ts

        # ---- 4. 观察计数 ----
        self._observation_count += 1

        # ---- 5. 风控 HALT ----
        risk_level = getattr(context, "risk_level", "unknown")
        if risk_level in BLOCKING_RISK_LEVELS:
            return self._exit("risk_halt")

        # ---- 6. 逃顶质量判定 ----
        # 需要 ATR
        atr = self._compute_atr(context)
        if atr is not None and self._peak_close is not None:
            current_close = context.bar.close
            escape_distance = self._compute_escape_distance(
                current_close, self._peak_close
            )

            confirm_threshold = (
                self.escape_confirm_distance_atr * atr
            )
            tolerance_threshold = (
                self.escape_return_tolerance_atr * atr
            )

            # 确认成功：继续远离峰值
            if escape_distance > confirm_threshold:
                logger.info(
                    f"escape confirmed: "
                    f"escape_distance={escape_distance:.4f} > "
                    f"confirm_threshold={confirm_threshold:.4f} "
                    f"(peak={self._peak_close}, "
                    f"current={current_close}, "
                    f"observations={self._observation_count})"
                )
                return self._exit("escape_confirmed")

            # 假逃顶：价格回到峰值附近
            if escape_distance < -tolerance_threshold:
                logger.info(
                    f"false escape detected: "
                    f"escape_distance={escape_distance:.4f} < "
                    f"-tolerance_threshold={-tolerance_threshold:.4f} "
                    f"(peak={self._peak_close}, "
                    f"current={current_close}, "
                    f"observations={self._observation_count})"
                )
                return self._exit("false_escape")

        # ---- 7. 观察超时 ----
        if self._observation_count > self.max_observation_bars:
            logger.info(
                f"observation timeout: "
                f"{self._observation_count} > "
                f"{self.max_observation_bars} bars"
            )
            return self._exit("observation_timeout")

        # ---- 8. 继续观察 ----
        return self._hold(context)

    # ========================================================================
    # 逃顶质量判断
    # ========================================================================

    def _compute_escape_distance(
        self, current_close: Any, peak_close: Decimal
    ) -> float:
        """
        计算逃顶距离。

        escape_distance = (current - peak) * escape_direction

        - 正数：价格继续远离 peak（逃顶成功的方向）
        - 负数：价格反向回到 peak 附近（假逃顶）
        - 0：价格回到 peak

        示例（多单逃顶）：
        - peak=100, current=95, escape_direction=-1
          → (95-100)*-1 = 5（正数，继续下跌，逃顶成功）
        - peak=100, current=102, escape_direction=-1
          → (102-100)*-1 = -2（负数，价格回到 peak 之上，假逃顶）
        """
        try:
            current = Decimal(str(current_close))
            return float(
                (current - peak_close) * self._escape_direction
            )
        except (ArithmeticError, ValueError, TypeError) as e:
            logger.warning(
                f"escape_distance computation error: {e}"
            )
            return 0.0

    def _compute_atr(
        self,
        context: StateContext,
        period: Optional[int] = None,
    ) -> Optional[float]:
        """
        计算 ATR。

        契约：
        - 数据不足返回 None
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
    # meta 加载与 fallback
    # ========================================================================

    def _load_from_meta(self, meta: Dict[str, Any]) -> None:
        """从 context.meta 加载信息。"""
        # trend_direction
        td = meta.get("trend_direction")
        if td is not None:
            try:
                self._trend_direction = int(td)
            except (ValueError, TypeError):
                self._trend_direction = 0

        # peak_close
        pc = meta.get("peak_close")
        if pc is not None:
            try:
                self._peak_close = Decimal(str(pc))
            except Exception:
                self._peak_close = None

        # peak_bar_ts
        pbt = meta.get("peak_bar_ts")
        if pbt is not None:
            try:
                self._peak_bar_ts = int(pbt)
            except (ValueError, TypeError):
                self._peak_bar_ts = None

        # entry_price
        ep = meta.get("entry_price")
        if ep is not None:
            try:
                self._entry_price = Decimal(str(ep))
            except Exception:
                self._entry_price = None

        # signal_id
        sid = meta.get("signal_id")
        if sid is not None:
            self._entry_signal_id = str(sid)

    def _derive_fallback(self, context: StateContext) -> None:
        """
        从 recent_bars 推导 trend_direction 和 peak_close。

        契约：
        - trend_direction：最近 5 根 close 的累计差 > 0 → +1，否则 -1
        - peak_close：多单场景找 max(high)，空单场景找 min(low)
        - 数据不足 → 使用当前 bar
        """
        bars = getattr(context, "recent_bars", [])

        # ---- trend_direction 推导 ----
        if len(bars) >= DEFAULT_TREND_LOOKBACK_BARS + 1:
            recent = bars[-DEFAULT_TREND_LOOKBACK_BARS:]
            try:
                total_change = sum(
                    float(b.close) for b in recent
                ) - float(bars[-DEFAULT_TREND_LOOKBACK_BARS - 1].close)
                self._trend_direction = 1 if total_change > 0 else -1
            except (TypeError, ValueError):
                self._trend_direction = 1
        else:
            self._trend_direction = 1
            logger.warning(
                f"insufficient bars for trend derivation: "
                f"{len(bars)}, defaulting trend_direction=1"
            )

        # ---- peak_close 推导 ----
        if len(bars) >= 2:
            try:
                if self._trend_direction == 1:
                    # 多单逃顶：峰值 = 最高价
                    peak_bar = max(
                        bars, key=lambda b: float(b.high)
                    )
                    self._peak_close = Decimal(str(peak_bar.high))
                    self._peak_bar_ts = int(peak_bar.ts)
                else:
                    # 空单逃底：谷值 = 最低价
                    peak_bar = min(
                        bars, key=lambda b: float(b.low)
                    )
                    self._peak_close = Decimal(str(peak_bar.low))
                    self._peak_bar_ts = int(peak_bar.ts)
            except (TypeError, ValueError, AttributeError) as e:
                logger.warning(
                    f"peak derivation failed: {e}, "
                    f"using current bar.close"
                )
                try:
                    self._peak_close = Decimal(
                        str(context.bar.close)
                    )
                    self._peak_bar_ts = int(context.bar.ts)
                except Exception:
                    self._peak_close = None
        else:
            try:
                self._peak_close = Decimal(str(context.bar.close))
                self._peak_bar_ts = int(context.bar.ts)
            except Exception:
                self._peak_close = None

    # ========================================================================
    # 转换构造
    # ========================================================================

    def _hold(self, context: StateContext) -> StateTransition:
        """HOLD 转换（继续观察）。"""
        self._stats["holds"] += 1

        current_close = None
        try:
            current_close = str(context.bar.close)
        except Exception:
            pass

        return StateTransition(
            target=None,
            action=StateAction.HOLD,
            reason="observing",
            meta={
                "peak_close": (
                    str(self._peak_close)
                    if self._peak_close is not None else None
                ),
                "peak_bar_ts": self._peak_bar_ts,
                "current_close": current_close,
                "observation_count": self._observation_count,
                "trend_direction": self._trend_direction,
                "meta_received": self._meta_received,
            },
        )

    def _exit(self, reason: str) -> StateTransition:
        """
        EXIT 转换。所有退出都回 IDLE。

        契约：
        - target="idle"
        - action=EXIT
        - direction=0
        - meta 记录完整的观察结果（供 Step 3 分析）
        """
        self._stats["exits"] += 1
        reason_key = reason.split(":")[0]
        if reason_key in self._stats["exit_reasons"]:
            self._stats["exit_reasons"][reason_key] += 1

        logger.info(
            f"TopDetectState exit: reason={reason} "
            f"observations={self._observation_count} "
            f"peak_close={self._peak_close} "
            f"trend_direction={self._trend_direction} "
            f"meta_received={self._meta_received}"
        )

        return StateTransition(
            target="idle",
            action=StateAction.EXIT,
            direction=0,
            reason=reason,
            meta={
                "exit_reason": reason,
                "peak_close": (
                    str(self._peak_close)
                    if self._peak_close is not None else None
                ),
                "peak_bar_ts": self._peak_bar_ts,
                "trend_direction": self._trend_direction,
                "entry_price": (
                    str(self._entry_price)
                    if self._entry_price is not None else None
                ),
                "signal_id": self._entry_signal_id,
                "observation_count": self._observation_count,
                "meta_received": self._meta_received,
            },
        )

    def _noop(self, reason: str) -> StateTransition:
        """NOOP 转换。"""
        self._stats["noops"] += 1
        return StateTransition(
            target=None,
            action=StateAction.NOOP,
            reason=reason,
            meta={
                "trend_direction": self._trend_direction,
                "observation_count": self._observation_count,
            },
        )

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def reset(self) -> None:
        """重置状态。"""
        self._trend_direction = 0
        self._escape_direction = 0
        self._peak_close = None
        self._peak_bar_ts = None
        self._entry_price = None
        self._entry_signal_id = None
        self._observation_count = 0
        self._last_bar_ts = None
        self._meta_received = False
        for k in self._stats:
            if k == "exit_reasons":
                for rk in self._stats["exit_reasons"]:
                    self._stats["exit_reasons"][rk] = 0
            else:
                self._stats[k] = 0
        logger.info("TopDetectState reset")

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        total = self._stats["bar_closes"]
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "trend_direction": self._trend_direction,
            "escape_direction": self._escape_direction,
            "peak_close": (
                str(self._peak_close)
                if self._peak_close is not None else None
            ),
            "peak_bar_ts": self._peak_bar_ts,
            "observation_count": self._observation_count,
            "max_observation_bars": self.max_observation_bars,
            "meta_received": self._meta_received,
            "signal_id": self._entry_signal_id,
            "stats": dict(self._stats),
            "summary": {
                "bar_closes": total,
                "holds": self._stats["holds"],
                "exits": self._stats["exits"],
                "meta_missing": self._stats["meta_missing"],
                "exit_reasons": dict(self._stats["exit_reasons"]),
            },
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> TopDetectState:
    """模块工厂入口。"""
    return TopDetectState(config)
