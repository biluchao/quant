"""
risk/policy_minimal.py — Step 1 最小风控策略

系统的"刹车"。

职责（唯一）：
1. 单笔风险 R0 限制（per_trade_risk）
2. 日回撤分阶段熔断（warn/reduce/close_only/halt）
3. 连续亏损降频
4. 保证金率监控
5. 市场突变防护
6. 保护期减仓

不做什么：
- 不知道策略、不知道信号
- 不做精度校验（gateway 做）
- 不做名义价值校验（gateway 做）
- 不做余额校验（gateway 做）
- 不做重试（RestClient 做）
- 不修改 Order（只返回 approved_qty）

职责边界：
- PolicyMinimal：审批订单 + 维护风控状态
- RoundManager：调用风控，决定是否开仓/平仓
- StateMachine：触发状态机逻辑
- Gateway：实际下单

关键契约（"四必批"）：
1. 平仓单必批（reduce_only=True 或 close_position=True）
2. 止损单必批（STOP_MARKET）
3. HALT 等级下的平仓必批
4. 内部异常时平仓必批（异常路径也放行平仓）

关键契约（"五必拒"）：
1. qty <= 0 必拒
2. 单笔风险 R0 超限必拒
3. 日回撤 >= halt 必拒（halt 级别）
4. 日回撤 >= close_only 必拒（close_only 级别）
5. 连续亏损 >= pause 阈值必拒（暂停期内）

关键契约（"三降级"）：
1. 日回撤 >= warn → WARN
2. 日回撤 >= reduce → REDUCE（approved_qty = 原 qty × 0.5）
3. 连续亏损 >= reduce 阈值 → REDUCE

风控等级（RiskLevel，从宽到严）：
- NORMAL:    正常
- WARN:      预警，所有订单可过，记录日志
- REDUCE:    降频，新开仓仓位 × 0.5
- CLOSE_ONLY:只平仓
- HALT:      熔断，只允许紧急平仓（与 CLOSE_ONLY 的区别：HALT 通常由保证金率触发）

等级判定（优先级从高到低）：
1. 保证金率 >= margin_ratio_halt (0.8) → HALT
2. 日回撤 >= daily_dd_halt (3%) → HALT
3. 保证金率 >= margin_ratio_warn (0.6) → CLOSE_ONLY
4. 日回撤 >= daily_dd_close_only (2.5%) → CLOSE_ONLY
5. 市场突变未清除 → REDUCE
6. 连续亏损 >= 8 → CLOSE_ONLY（暂停期内）
7. 日回撤 >= daily_dd_reduce (2%) → REDUCE
8. 连续亏损 >= 5 → REDUCE
9. 日回撤 >= daily_dd_warn (1%) → WARN
10. 否则 → NORMAL

单笔风险 R0 计算：
    R0 = |entry_price - stop_price| * qty
    R0_max = equity * per_trade_risk
    保护期内 R0_max *= protection_qty_ratio

    如果 R0 > R0_max：
    - 计算 approved_qty = R0_max / |entry - stop|
    - 如果 approved_qty < min_qty（精度后为 0）→ REJECT
    - 否则 REDUCE

日回撤计算：
    drawdown = (peak_equity - current_equity) / peak_equity
    current_equity = initial_equity + daily_pnl + unrealized_pnl
    peak_equity = max(峰值, current_equity)

    注意：current_equity 用 realised（daily_pnl）为基础，
    不包含 unrealized（因为 state_trend 的止损会限制浮亏）
    peak_equity 每次 on_round_closed 后更新

连续亏损：
    on_round_closed 时，pnl < 0 → _consecutive_losses += 1
                     pnl > 0 → _consecutive_losses = 0

保护期：
    _completed_rounds < protection_rounds 时为保护期
    on_round_closed 时 _completed_rounds += 1
    保护期结束时打 INFO

与 RoundManager 的协作：
    RoundManager._open_round_impl()
      → risk_policy.approve_order(context)
      → 若 REJECT：返回 None（状态机留在原状态）
      → 若 REDUCE：用 approved_qty 重构造订单
      → 若 APPROVE：用原订单
      → gateway.place_order()

    RoundManager.close_round()
      → （平仓不经过风控审批，直接执行）
      → PositionRound.close()
      → risk_policy.on_round_closed(record)  # 更新日统计

    main.py._on_account_update()
      → risk_policy.on_account_update(account)  # 更新保证金率

    main.py 日切调度
      → risk_policy.daily_reset(now_ms)

    main.py 启动时
      → risk_policy.restore(snapshot)  # 从持久化恢复

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.risk import (
    RejectReason,
    RiskContext,
    RiskDecision,
    RiskLevel,
    RiskPolicy,
    RiskResult,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "policy_minimal",
    "type": "risk",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": [
        "minimal_risk", "per_trade_risk", "daily_drawdown",
        "consecutive_loss", "margin_ratio", "protection_period",
    ],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Minimal Step 1 risk policy: per-trade R0 + daily drawdown + margin ratio + protection period",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_PER_TRADE_RISK = Decimal("0.007")
DEFAULT_DAILY_DD_WARN = Decimal("0.01")
DEFAULT_DAILY_DD_REDUCE = Decimal("0.02")
DEFAULT_DAILY_DD_CLOSE_ONLY = Decimal("0.025")
DEFAULT_DAILY_DD_HALT = Decimal("0.03")
DEFAULT_CONSECUTIVE_LOSS_REDUCE = 5
DEFAULT_CONSECUTIVE_LOSS_PAUSE = 8
DEFAULT_CONSECUTIVE_LOSS_PAUSE_S = 3600
DEFAULT_MARGIN_RATIO_WARN = Decimal("0.6")
DEFAULT_MARGIN_RATIO_HALT = Decimal("0.8")
DEFAULT_PROTECTION_ROUNDS = 20
DEFAULT_PROTECTION_QTY_RATIO = Decimal("0.3")
DEFAULT_MARKET_SHOCK_THRESHOLD = Decimal("0.03")
DEFAULT_MARKET_SHOCK_PAUSE_S = 60

SNAPSHOT_VERSION = 1
CONSECUTIVE_LOSS_MAX = 1000
REDUCE_QTY_MULTIPLIER = Decimal("0.5")


# ============================================================================
# PolicyMinimal
# ============================================================================
class PolicyMinimal(RiskPolicy):
    """
    Step 1 最小风控策略。详见模块 docstring。

    用法：
        policy = PolicyMinimal(config)

        # 启动时恢复
        policy.restore(snapshot)

        # 每笔订单前
        result = policy.approve_order(context)
        if result.decision == RiskDecision.APPROVE:
            place_order(original_qty)
        elif result.decision == RiskDecision.REDUCE:
            place_order(result.approved_qty)
        else:  # REJECT
            logger.warning(f"rejected: {result.reason}")

        # 每轮结束后
        policy.on_round_closed(round_record)

        # 账户更新（从账户流）
        policy.on_account_update(account_state)

        # 日切（UTC 00:00 由调度器触发）
        policy.daily_reset(now_ms)

        # 定期健康检查
        policy.health_check()
    """

    name = "policy_minimal"
    version = "1.0.0"

    def __init__(self, config: Optional[dict] = None):
        cfg = (config or {}).get("risk", {})
        self._config = config or {}

        # ---- 配置 ----
        self._per_trade_risk = Decimal(str(cfg.get(
            "per_trade_risk", str(DEFAULT_PER_TRADE_RISK)
        )))
        self._daily_dd_warn = Decimal(str(cfg.get(
            "daily_dd_warn", str(DEFAULT_DAILY_DD_WARN)
        )))
        self._daily_dd_reduce = Decimal(str(cfg.get(
            "daily_dd_reduce", str(DEFAULT_DAILY_DD_REDUCE)
        )))
        self._daily_dd_close_only = Decimal(str(cfg.get(
            "daily_dd_close_only", str(DEFAULT_DAILY_DD_CLOSE_ONLY)
        )))
        self._daily_dd_halt = Decimal(str(cfg.get(
            "daily_dd_halt", str(DEFAULT_DAILY_DD_HALT)
        )))
        self._consecutive_loss_reduce = int(cfg.get(
            "consecutive_loss_reduce", DEFAULT_CONSECUTIVE_LOSS_REDUCE
        ))
        self._consecutive_loss_pause = int(cfg.get(
            "consecutive_loss_pause", DEFAULT_CONSECUTIVE_LOSS_PAUSE
        ))
        self._consecutive_loss_pause_s = int(cfg.get(
            "consecutive_loss_pause_s", DEFAULT_CONSECUTIVE_LOSS_PAUSE_S
        ))
        self._margin_ratio_warn = Decimal(str(cfg.get(
            "margin_ratio_warn", str(DEFAULT_MARGIN_RATIO_WARN)
        )))
        self._margin_ratio_halt = Decimal(str(cfg.get(
            "margin_ratio_halt", str(DEFAULT_MARGIN_RATIO_HALT)
        )))
        self._protection_rounds = int(cfg.get(
            "protection_rounds", DEFAULT_PROTECTION_ROUNDS
        ))
        self._protection_qty_ratio = Decimal(str(cfg.get(
            "protection_qty_ratio", str(DEFAULT_PROTECTION_QTY_RATIO)
        )))
        self._market_shock_threshold = Decimal(str(cfg.get(
            "market_shock_threshold", str(DEFAULT_MARKET_SHOCK_THRESHOLD)
        )))
        self._market_shock_pause_s = int(cfg.get(
            "market_shock_pause_s", DEFAULT_MARKET_SHOCK_PAUSE_S
        ))

        # ---- 参数校验 ----
        self._validate_config()

        # ---- 内部状态（风控的"记忆"）----
        self._equity = Decimal(str(
            (config or {}).get("initial_equity", 10000)
        ))
        self._initial_equity = self._equity
        self._daily_pnl = Decimal("0")
        self._peak_equity = self._equity
        self._consecutive_losses = 0
        self._margin_ratio = Decimal("0")
        self._completed_rounds = 0
        self._protection_ended = (
            self._protection_rounds == 0
        )
        self._market_shock_until_ms = 0
        self._last_reset_ts = self._safe_now_ms()
        self._next_reset_ts = self._compute_next_utc_midnight(
            self._last_reset_ts
        )

        # ---- 线程安全 ----
        self._lock = threading.RLock()

        # ---- 统计 ----
        self._stats = {
            "approvals": 0,
            "approvals_reduced": 0,
            "rejections": 0,
            "rejections_by_reason": {},
            "levels_seen": {lvl.value: 0 for lvl in RiskLevel},
            "internal_errors": 0,
        }
        self._last_level: Optional[str] = None
        self._last_error: Optional[str] = None

        logger.info(
            f"PolicyMinimal initialized "
            f"per_trade_risk={self._per_trade_risk} "
            f"dd_warn={self._daily_dd_warn} "
            f"dd_reduce={self._daily_dd_reduce} "
            f"dd_close_only={self._daily_dd_close_only} "
            f"dd_halt={self._daily_dd_halt} "
            f"margin_warn={self._margin_ratio_warn} "
            f"margin_halt={self._margin_ratio_halt} "
            f"protection_rounds={self._protection_rounds} "
            f"equity={self._equity}"
        )

    # ========================================================================
    # 配置校验
    # ========================================================================

    def _validate_config(self) -> None:
        """参数组约束校验。"""
        if self._per_trade_risk <= 0 or self._per_trade_risk > Decimal("0.02"):
            raise ValueError(
                f"per_trade_risk must be in (0, 0.02], "
                f"got {self._per_trade_risk}"
            )
        if not (
            Decimal("0") < self._daily_dd_warn
            < self._daily_dd_reduce
            < self._daily_dd_close_only
            < self._daily_dd_halt
        ):
            raise ValueError(
                f"dd thresholds must satisfy "
                f"0 < warn < reduce < close_only < halt, got "
                f"warn={self._daily_dd_warn} "
                f"reduce={self._daily_dd_reduce} "
                f"close_only={self._daily_dd_close_only} "
                f"halt={self._daily_dd_halt}"
            )
        if self._consecutive_loss_reduce < 1:
            raise ValueError(
                f"consecutive_loss_reduce must be >= 1, "
                f"got {self._consecutive_loss_reduce}"
            )
        if self._consecutive_loss_pause <= self._consecutive_loss_reduce:
            raise ValueError(
                f"consecutive_loss_pause ({self._consecutive_loss_pause}) "
                f"must be > consecutive_loss_reduce "
                f"({self._consecutive_loss_reduce})"
            )
        if not (
            Decimal("0") < self._margin_ratio_warn
            < self._margin_ratio_halt <= Decimal("1")
        ):
            raise ValueError(
                f"margin_ratio must satisfy "
                f"0 < warn < halt <= 1, got "
                f"warn={self._margin_ratio_warn} halt={self._margin_ratio_halt}"
            )
        if self._protection_rounds < 0:
            raise ValueError(
                f"protection_rounds must be >= 0, "
                f"got {self._protection_rounds}"
            )
        if not (Decimal("0") < self._protection_qty_ratio <= Decimal("1")):
            raise ValueError(
                f"protection_qty_ratio must be in (0, 1], "
                f"got {self._protection_qty_ratio}"
            )

    # ========================================================================
    # 核心：approve_order
    # ========================================================================

    def approve_order(self, context: RiskContext) -> RiskResult:
        """
        审批订单。

        契约：
        - 平仓单必批（reduce_only / close_position / STOP_MARKET）
        - qty <= 0 必拒
        - 单笔风险 R0 超限 → REDUCE 或 REJECT
        - 日回撤分阶段熔断
        - 连续亏损降频
        - 内部异常返回 REJECT（但平仓单放行）

        Returns:
            RiskResult
        """
        try:
            return self._approve_order_impl(context)
        except Exception as e:
            # 内部异常：平仓单放行，其他 REJECT
            self._stats["internal_errors"] += 1
            self._last_error = f"{type(e).__name__}: {e}"
            logger.error(
                f"PolicyMinimal.approve_order error: {e}",
                exc_info=True,
            )
            is_close = self._is_close_order_safe(context.order)
            if is_close:
                return self._approve_result(
                    context,
                    metrics={"note": "internal_error_but_close_passes"},
                )
            return self._reject_result(
                context,
                RejectReason.RISK_INTERNAL_ERROR,
                metrics={"error": str(e)},
            )

    def _approve_order_impl(self, context: RiskContext) -> RiskResult:
        """approve_order 的实际实现。"""
        order = context.order
        if order is None:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                metrics={"note": "order_is_none"},
            )

        # ---- 1. 平仓单必批（三道判断）----
        if self._is_close_order_safe(order):
            return self._approve_result(
                context,
                metrics={"path": "close_order"},
            )

        # ---- 2. 止损单必批 ----
        if self._is_stop_order_safe(order):
            return self._approve_result(
                context,
                metrics={"path": "stop_order"},
            )

        # ---- 3. qty 校验 ----
        try:
            qty = Decimal(str(order.qty))
        except Exception:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                metrics={"qty_raw": str(getattr(order, "qty", None))},
            )
        if qty <= 0:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                metrics={"qty": str(qty)},
            )

        # ---- 4. 风控等级（后面多个检查都依赖）----
        level = self._compute_level(context)
        self._track_level(level)

        # ---- 5. HALT / CLOSE_ONLY 直接拒 ----
        if level == RiskLevel.HALT:
            return self._reject_result(
                context,
                RejectReason.DAILY_DD_HALT
                if self._daily_dd_breached_halt(context)
                else RejectReason.MARGIN_RATIO_HALT,
                level=level,
                metrics=self._level_metrics(context, level),
            )
        if level == RiskLevel.CLOSE_ONLY:
            return self._reject_result(
                context,
                self._close_only_reason(context),
                level=level,
                metrics=self._level_metrics(context, level),
            )

        # ---- 6. 单笔风险 R0 ----
        r0_result = self._check_per_trade_risk(context, order, qty, level)
        if r0_result is not None:
            return r0_result

        # ---- 7. REDUCE 等级：仓位减半 ----
        if level == RiskLevel.REDUCE:
            reduced_qty = self._reduce_qty_for_level(qty)
            return self._reduce_result(
                context,
                approved_qty=reduced_qty,
                reason=RejectReason.DAILY_DD_CLOSE_ONLY,  # 用独立 reason
                level=level,
                metrics=self._level_metrics(context, level),
            )

        # ---- 8. WARN 等级：全过 ----
        if level == RiskLevel.WARN:
            return self._approve_result(
                context,
                level=level,
                metrics=self._level_metrics(context, level),
            )

        # ---- 9. NORMAL：全过 ----
        return self._approve_result(
            context,
            level=level,
            metrics=self._level_metrics(context, level),
        )

    # ========================================================================
    # 单笔风险 R0
    # ========================================================================

    def _check_per_trade_risk(
        self,
        context: RiskContext,
        order: Any,
        qty: Decimal,
        level: RiskLevel,
    ) -> Optional[RiskResult]:
        """
        检查单笔风险 R0。

        返回 None 表示通过，RiskResult 表示拒绝或降级。
        """
        entry_price, stop_price = self._extract_prices(context, order)
        if entry_price is None or stop_price is None:
            logger.warning(
                f"cannot determine prices for R0 check: "
                f"entry={entry_price} stop={stop_price}"
            )
            # 无法计算 R0，保守拒绝
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                level=level,
                metrics={"note": "cannot_determine_prices"},
            )

        if entry_price <= 0 or stop_price <= 0:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                level=level,
                metrics={
                    "entry_price": str(entry_price),
                    "stop_price": str(stop_price),
                },
            )

        if entry_price == stop_price:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                level=level,
                metrics={"note": "entry_equals_stop"},
            )

        stop_distance = abs(entry_price - stop_price)
        r0 = stop_distance * qty

        # 保护期内：R0_max 缩小
        protection_factor = (
            self._protection_qty_ratio
            if not self._protection_ended else Decimal("1")
        )
        r0_max = self._equity * self._per_trade_risk * protection_factor

        if r0 <= r0_max:
            return None  # 通过

        # 超限：尝试降级
        if stop_distance <= 0:
            return self._reject_result(
                context,
                RejectReason.INVALID_QTY,
                level=level,
                metrics={"note": "zero_stop_distance"},
            )

        approved_qty = r0_max / stop_distance
        if approved_qty <= 0:
            return self._reject_result(
                context,
                RejectReason.PER_TRADE_RISK_EXCEEDED,
                level=level,
                metrics={
                    "r0": str(r0),
                    "r0_max": str(r0_max),
                    "entry": str(entry_price),
                    "stop": str(stop_price),
                    "protection": not self._protection_ended,
                },
            )

        return self._reduce_result(
            context,
            approved_qty=approved_qty,
            reason=RejectReason.PER_TRADE_RISK_EXCEEDED,
            level=level,
            metrics={
                "r0": str(r0),
                "r0_max": str(r0_max),
                "original_qty": str(qty),
                "approved_qty": str(approved_qty),
                "protection": not self._protection_ended,
            },
        )

    def _extract_prices(
        self, context: RiskContext, order: Any
    ) -> tuple:
        """从 Order 和 context 提取 entry / stop 价格。"""
        entry_price: Optional[Decimal] = None
        stop_price: Optional[Decimal] = None

        # entry_price: LIMIT 用 order.price，MARKET 用 metadata.expected_price，兜底用 position.mark_price
        order_price = getattr(order, "price", None)
        if order_price is not None:
            try:
                entry_price = Decimal(str(order_price))
            except Exception:
                entry_price = None

        if entry_price is None:
            metadata = getattr(order, "metadata", {}) or {}
            expected = metadata.get("expected_price")
            if expected is not None:
                try:
                    entry_price = Decimal(str(expected))
                except Exception:
                    entry_price = None

        if entry_price is None:
            position = getattr(context, "position", None)
            if position is not None:
                mark = getattr(position, "mark_price", None)
                if mark is not None:
                    try:
                        entry_price = Decimal(str(mark))
                    except Exception:
                        entry_price = None

        # stop_price
        order_stop = getattr(order, "stop_price", None)
        if order_stop is not None:
            try:
                stop_price = Decimal(str(order_stop))
            except Exception:
                stop_price = None

        if stop_price is None:
            metadata = getattr(order, "metadata", {}) or {}
            stop_hint = metadata.get("stop_price_hint")
            if stop_hint is not None:
                try:
                    stop_price = Decimal(str(stop_hint))
                except Exception:
                    stop_price = None

        return entry_price, stop_price

    # ========================================================================
    # 风控等级计算
    # ========================================================================

    def _compute_level(self, context: RiskContext) -> RiskLevel:
        """
        计算当前风控等级。

        优先级（从高到低）：
        1. 保证金率 >= halt → HALT
        2. 日回撤 >= halt → HALT
        3. 保证金率 >= warn → CLOSE_ONLY
        4. 日回撤 >= close_only → CLOSE_ONLY
        5. 连续亏损 >= pause 且在暂停期内 → CLOSE_ONLY
        6. 市场突变未清除 → REDUCE
        7. 日回撤 >= reduce → REDUCE
        8. 连续亏损 >= reduce → REDUCE
        9. 日回撤 >= warn → WARN
        10. 否则 → NORMAL
        """
        now_ms = context.now_ms if context.now_ms > 0 else self._safe_now_ms()
        drawdown = self._compute_drawdown()

        # 1-2. HALT
        if self._margin_ratio >= self._margin_ratio_halt:
            return RiskLevel.HALT
        if drawdown >= self._daily_dd_halt:
            return RiskLevel.HALT

        # 3-4. CLOSE_ONLY
        if self._margin_ratio >= self._margin_ratio_warn:
            return RiskLevel.CLOSE_ONLY
        if drawdown >= self._daily_dd_close_only:
            return RiskLevel.CLOSE_ONLY

        # 5. 连续亏损暂停
        if (
            self._consecutive_losses >= self._consecutive_loss_pause
            and now_ms < self._market_shock_until_ms
        ):
            return RiskLevel.CLOSE_ONLY

        # 6. 市场突变
        if now_ms < self._market_shock_until_ms:
            return RiskLevel.REDUCE

        # 7. 日回撤 REDUCE
        if drawdown >= self._daily_dd_reduce:
            return RiskLevel.REDUCE

        # 8. 连续亏损 REDUCE
        if self._consecutive_losses >= self._consecutive_loss_reduce:
            return RiskLevel.REDUCE

        # 9. WARN
        if drawdown >= self._daily_dd_warn:
            return RiskLevel.WARN

        # 10. NORMAL
        return RiskLevel.NORMAL

    def _compute_drawdown(self) -> Decimal:
        """计算当前回撤。"""
        # 当日权益 = 初始权益 + 已实现盈亏（不含未实现）
        current = self._initial_equity + self._daily_pnl
        if current <= 0:
            return Decimal("1")

        if current > self._peak_equity:
            self._peak_equity = current
        if self._peak_equity <= 0:
            return Decimal("1")

        return (self._peak_equity - current) / self._peak_equity

    def _daily_dd_breached_halt(
        self, context: RiskContext
    ) -> bool:
        """判断 halt 是否由日回撤触发。"""
        return self._compute_drawdown() >= self._daily_dd_halt

    def _close_only_reason(
        self, context: RiskContext
    ) -> RejectReason:
        """CLOSE_ONLY 的 reason。"""
        if self._margin_ratio >= self._margin_ratio_warn:
            return RejectReason.MARGIN_RATIO_HALT
        if self._compute_drawdown() >= self._daily_dd_close_only:
            return RejectReason.DAILY_DD_CLOSE_ONLY
        return RejectReason.CONSECUTIVE_LOSS_PAUSE

    def _level_metrics(
        self, context: RiskContext, level: RiskLevel
    ) -> Dict[str, Any]:
        """风控等级的诊断指标。"""
        return {
            "level": level.value,
            "drawdown": str(self._compute_drawdown()),
            "margin_ratio": str(self._margin_ratio),
            "consecutive_losses": self._consecutive_losses,
            "protection": not self._protection_ended,
            "market_shock_until": self._market_shock_until_ms,
            "now_ms": context.now_ms,
        }

    # ========================================================================
    # 事件回调
    # ========================================================================

    def on_round_closed(self, round_record: Dict[str, Any]) -> None:
        """
        轮次结束。更新日统计、连续亏损、保护期计数。

        契约：
        - pnl < 0 → consecutive_losses += 1
        - pnl > 0 → consecutive_losses = 0
        - daily_pnl += pnl
        - completed_rounds += 1
        - 更新 peak_equity（若盈利）
        """
        try:
            pnl_raw = round_record.get("realized_pnl", "0")
            try:
                pnl = Decimal(str(pnl_raw))
            except Exception:
                pnl = Decimal("0")

            with self._lock:
                self._daily_pnl += pnl
                self._completed_rounds += 1

                if pnl < 0:
                    self._consecutive_losses = min(
                        self._consecutive_losses + 1,
                        CONSECUTIVE_LOSS_MAX,
                    )
                elif pnl > 0:
                    self._consecutive_losses = 0

                # 盈利时更新峰值
                current = self._initial_equity + self._daily_pnl
                if current > self._peak_equity:
                    self._peak_equity = current

                # 保护期结束检测
                was_in_protection = not self._protection_ended
                if (
                    not self._protection_ended
                    and self._completed_rounds >= self._protection_rounds
                ):
                    self._protection_ended = True
                    logger.info(
                        f"protection period ended: "
                        f"rounds={self._completed_rounds}"
                    )

                logger.info(
                    f"round closed: pnl={pnl} "
                    f"daily_pnl={self._daily_pnl} "
                    f"consecutive_losses={self._consecutive_losses} "
                    f"drawdown={self._compute_drawdown():.4f} "
                    f"completed={self._completed_rounds} "
                    f"protection_was={was_in_protection}"
                )
        except Exception as e:
            logger.error(
                f"on_round_closed error: {e}", exc_info=True
            )

    def on_order_filled(self, order_result: Any) -> None:
        """订单成交回调。仅统计，不更新风控状态。"""
        # Step 1 不处理（日统计由 on_round_closed 更新）
        pass

    def on_account_update(self, account: Any) -> None:
        """
        账户更新。只更新保证金率。

        契约：
        - 从 account 提取 total_maint_margin / total_wallet_balance
        - 保证金率 = total_maint_margin / total_wallet_balance
        - 值超过 1.0 视为异常，记录 WARN，取 1.0
        """
        try:
            if account is None:
                return

            maint = getattr(account, "total_maint_margin", None)
            wallet = getattr(account, "total_wallet_balance", None)

            if maint is None or wallet is None:
                return

            try:
                maint_dec = Decimal(str(maint))
                wallet_dec = Decimal(str(wallet))
            except Exception:
                return

            if wallet_dec <= 0:
                return

            ratio = maint_dec / wallet_dec

            if ratio > Decimal("1"):
                logger.warning(
                    f"margin_ratio > 1.0 detected: {ratio}, "
                    f"clamping to 1.0"
                )
                ratio = Decimal("1")

            with self._lock:
                self._margin_ratio = ratio
        except Exception as e:
            logger.error(
                f"on_account_update error: {e}", exc_info=True
            )

    def on_market_shock(self, now_ms: int) -> None:
        """
        触发市场突变。

        契约：
        - 设置 market_shock_until_ms = now + pause_s
        - 期间风控等级至少为 REDUCE
        """
        with self._lock:
            until = now_ms + self._market_shock_pause_s * 1000
            if until > self._market_shock_until_ms:
                self._market_shock_until_ms = until
            logger.warning(
                f"market shock triggered: "
                f"until={self._market_shock_until_ms}"
            )

    def clear_market_shock(self) -> None:
        """清除市场突变（状态机主动调用）。"""
        with self._lock:
            self._market_shock_until_ms = 0
            logger.info("market shock cleared")

    # ========================================================================
    # 日切
    # ========================================================================

    def daily_reset(self, now_ms: int) -> None:
        """
        日切重置。

        契约：
        - 幂等：now_ms < _next_reset_ts 时直接返回
        - 清空 daily_pnl / consecutive_losses
        - 保留：_peak_equity（跨日峰值）
        - 保留：_completed_rounds（保护期跨日）
        - 保留：_margin_ratio（账户状态跨日）
        """
        with self._lock:
            if now_ms < self._next_reset_ts:
                return

            logger.info(
                f"daily reset: now={now_ms} "
                f"previous_reset={self._last_reset_ts} "
                f"old_daily_pnl={self._daily_pnl} "
                f"old_consecutive_losses={self._consecutive_losses}"
            )

            # 保留 peak_equity 作为跨日基准
            self._initial_equity = self._peak_equity

            self._daily_pnl = Decimal("0")
            self._consecutive_losses = 0
            self._last_reset_ts = now_ms
            self._next_reset_ts = self._compute_next_utc_midnight(now_ms)

    @staticmethod
    def _compute_next_utc_midnight(now_ms: int) -> int:
        """计算下一个 UTC 00:00 的毫秒时间戳。"""
        try:
            dt = datetime.fromtimestamp(
                now_ms / 1000.0, tz=timezone.utc
            )
        except (ValueError, OSError):
            # 兜底：24 小时后
            return now_ms + 86400 * 1000

        tomorrow = dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp()
        next_midnight = int(tomorrow * 1000)
        if next_midnight <= now_ms:
            next_midnight += 86400 * 1000
        return next_midnight

    # ========================================================================
    # 状态查询
    # ========================================================================

    def get_risk_level(self) -> RiskLevel:
        """返回当前风控等级（无 context 版本）。"""
        try:
            with self._lock:
                now_ms = self._safe_now_ms()
                drawdown = self._compute_drawdown()
                if self._margin_ratio >= self._margin_ratio_halt:
                    return RiskLevel.HALT
                if drawdown >= self._daily_dd_halt:
                    return RiskLevel.HALT
                if self._margin_ratio >= self._margin_ratio_warn:
                    return RiskLevel.CLOSE_ONLY
                if drawdown >= self._daily_dd_close_only:
                    return RiskLevel.CLOSE_ONLY
                if (
                    self._consecutive_losses >= self._consecutive_loss_pause
                    and now_ms < self._market_shock_until_ms
                ):
                    return RiskLevel.CLOSE_ONLY
                if now_ms < self._market_shock_until_ms:
                    return RiskLevel.REDUCE
                if drawdown >= self._daily_dd_reduce:
                    return RiskLevel.REDUCE
                if (
                    self._consecutive_losses
                    >= self._consecutive_loss_reduce
                ):
                    return RiskLevel.REDUCE
                if drawdown >= self._daily_dd_warn:
                    return RiskLevel.WARN
                return RiskLevel.NORMAL
        except Exception as e:
            logger.error(
                f"get_risk_level error: {e}", exc_info=True
            )
            return RiskLevel.HALT

    # ========================================================================
    # 辅助：结果构造
    # ========================================================================

    def _approve_result(
        self,
        context: RiskContext,
        level: Optional[RiskLevel] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> RiskResult:
        """构造 APPROVE 结果。"""
        lvl = level or self.get_risk_level()
        with self._lock:
            self._stats["approvals"] += 1
            self._track_level_locked(lvl)
        return RiskResult(
            decision=RiskDecision.APPROVE,
            approved_qty=None,
            reason="approved",
            level=lvl,
            risk_metrics=dict(metrics or {}),
        )

    def _reduce_result(
        self,
        context: RiskContext,
        approved_qty: Decimal,
        reason: RejectReason,
        level: Optional[RiskLevel] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> RiskResult:
        """构造 REDUCE 结果。"""
        lvl = level or self.get_risk_level()
        with self._lock:
            self._stats["approvals_reduced"] += 1
            self._track_level_locked(lvl)
        return RiskResult(
            decision=RiskDecision.REDUCE,
            approved_qty=approved_qty,
            reason=(
                reason.value if not isinstance(reason, str) else reason
            ),
            level=lvl,
            risk_metrics=dict(metrics or {}),
        )

    def _reject_result(
        self,
        context: RiskContext,
        reason: RejectReason,
        level: Optional[RiskLevel] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> RiskResult:
        """构造 REJECT 结果。"""
        lvl = level or self.get_risk_level()
        reason_str = (
            reason.value if not isinstance(reason, str) else reason
        )
        with self._lock:
            self._stats["rejections"] += 1
            self._stats["rejections_by_reason"][reason_str] = (
                self._stats["rejections_by_reason"].get(reason_str, 0) + 1
            )
            self._track_level_locked(lvl)
        return RiskResult(
            decision=RiskDecision.REJECT,
            approved_qty=None,
            reason=reason_str,
            level=lvl,
            risk_metrics=dict(metrics or {}),
        )

    def _track_level(self, level: RiskLevel) -> None:
        """记录风控等级变化。"""
        with self._lock:
            self._track_level_locked(level)

    def _track_level_locked(self, level: RiskLevel) -> None:
        """记录风控等级（调用者已持锁）。"""
        self._stats["levels_seen"][level.value] = (
            self._stats["levels_seen"].get(level.value, 0) + 1
        )
        if self._last_level != level.value:
            if self._last_level is not None:
                logger.info(
                    f"risk level changed: {self._last_level} -> {level.value}"
                )
            self._last_level = level.value

    # ========================================================================
    # 辅助：平仓单/止损单判断
    # ========================================================================

    @staticmethod
    def _is_close_order_safe(order: Any) -> bool:
        """判断是否为平仓单（异常安全）。"""
        try:
            if order is None:
                return False
            reduce_only = getattr(order, "reduce_only", False)
            close_position = getattr(order, "close_position", False)
            return bool(reduce_only or close_position)
        except Exception:
            return False

    @staticmethod
    def _is_stop_order_safe(order: Any) -> bool:
        """判断是否为止损单（异常安全）。"""
        try:
            if order is None:
                return False
            order_type = getattr(order, "type", None)
            if order_type is None:
                return False
            type_str = (
                order_type.value if hasattr(order_type, "value")
                else str(order_type)
            )
            return type_str == "STOP_MARKET"
        except Exception:
            return False

    def _reduce_qty_for_level(self, qty: Decimal) -> Decimal:
        """REDUCE 等级下的仓位缩放。"""
        return qty * REDUCE_QTY_MULTIPLIER

    # ========================================================================
    # 安全工具
    # ========================================================================

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        try:
            clock = (self._config or {}).get("instances", {}).get("clock")
            if clock is not None:
                return int(clock.now_ms())
        except Exception:
            pass
        return int(time.time() * 1000)

    # ========================================================================
    # 持久化
    # ========================================================================

    def snapshot(self) -> Dict[str, Any]:
        """导出风控状态。"""
        with self._lock:
            return {
                "snapshot_version": SNAPSHOT_VERSION,
                "ts": self._safe_now_ms(),
                "equity": str(self._equity),
                "initial_equity": str(self._initial_equity),
                "daily_pnl": str(self._daily_pnl),
                "peak_equity": str(self._peak_equity),
                "consecutive_losses": self._consecutive_losses,
                "margin_ratio": str(self._margin_ratio),
                "completed_rounds": self._completed_rounds,
                "protection_ended": self._protection_ended,
                "market_shock_until_ms": self._market_shock_until_ms,
                "last_reset_ts": self._last_reset_ts,
                "next_reset_ts": self._next_reset_ts,
                "last_level": self._last_level,
                "stats": dict(self._stats),
                "config": {
                    "per_trade_risk": str(self._per_trade_risk),
                    "daily_dd_warn": str(self._daily_dd_warn),
                    "daily_dd_reduce": str(self._daily_dd_reduce),
                    "daily_dd_close_only": str(self._daily_dd_close_only),
                    "daily_dd_halt": str(self._daily_dd_halt),
                    "margin_ratio_warn": str(self._margin_ratio_warn),
                    "margin_ratio_halt": str(self._margin_ratio_halt),
                    "protection_rounds": self._protection_rounds,
                },
            }

    def restore(self, snapshot: Optional[Dict[str, Any]]) -> None:
        """恢复风控状态。"""
        if not snapshot:
            logger.info("restore: no snapshot, using defaults")
            return

        sv = snapshot.get("snapshot_version", 0)
        if sv != SNAPSHOT_VERSION:
            logger.warning(
                f"restore: snapshot_version mismatch "
                f"({sv} != {SNAPSHOT_VERSION}), using defaults"
            )
            return

        def _dec(key: str, default: str = "0") -> Decimal:
            try:
                return Decimal(str(snapshot.get(key, default)))
            except Exception:
                return Decimal(default)

        with self._lock:
            self._equity = _dec("equity", str(self._initial_equity))
            self._initial_equity = _dec(
                "initial_equity", str(self._initial_equity)
            )
            self._daily_pnl = _dec("daily_pnl")
            self._peak_equity = _dec(
                "peak_equity", str(self._initial_equity)
            )
            self._consecutive_losses = int(
                snapshot.get("consecutive_losses", 0)
            )
            self._margin_ratio = _dec("margin_ratio")
            self._completed_rounds = int(
                snapshot.get("completed_rounds", 0)
            )
            self._protection_ended = bool(
                snapshot.get(
                    "protection_ended",
                    self._protection_rounds == 0,
                )
            )
            self._market_shock_until_ms = int(
                snapshot.get("market_shock_until_ms", 0)
            )
            self._last_reset_ts = int(
                snapshot.get("last_reset_ts", self._safe_now_ms())
            )
            self._next_reset_ts = int(
                snapshot.get(
                    "next_reset_ts",
                    self._compute_next_utc_midnight(self._last_reset_ts),
                )
            )
            self._last_level = snapshot.get("last_level")
            for k, v in (snapshot.get("stats") or {}).items():
                if k in self._stats:
                    self._stats[k] = v

        logger.info(
            f"restored risk state: "
            f"daily_pnl={self._daily_pnl} "
            f"peak_equity={self._peak_equity} "
            f"consecutive_losses={self._consecutive_losses} "
            f"completed_rounds={self._completed_rounds} "
            f"protection_ended={self._protection_ended}"
        )

    # ========================================================================
    # 健康检查与重置
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            return {
                "healthy": True,
                "name": self.name,
                "version": self.version,
                "level": self.get_risk_level().value,
                "equity": str(self._equity),
                "initial_equity": str(self._initial_equity),
                "daily_pnl": str(self._daily_pnl),
                "peak_equity": str(self._peak_equity),
                "drawdown": str(self._compute_drawdown()),
                "consecutive_losses": self._consecutive_losses,
                "margin_ratio": str(self._margin_ratio),
                "completed_rounds": self._completed_rounds,
                "protection_ended": self._protection_ended,
                "market_shock_until_ms": self._market_shock_until_ms,
                "next_reset_ts": self._next_reset_ts,
                "last_level": self._last_level,
                "last_error": self._last_error,
                "stats": dict(self._stats),
                "timestamp": self._safe_now_ms(),
            }

    def reset(self) -> None:
        """重置（测试用）。全清。"""
        with self._lock:
            self._equity = self._initial_equity
            self._daily_pnl = Decimal("0")
            self._peak_equity = self._initial_equity
            self._consecutive_losses = 0
            self._margin_ratio = Decimal("0")
            self._completed_rounds = 0
            self._protection_ended = (self._protection_rounds == 0)
            self._market_shock_until_ms = 0
            self._last_reset_ts = self._safe_now_ms()
            self._next_reset_ts = self._compute_next_utc_midnight(
                self._last_reset_ts
            )
            self._last_level = None
            self._last_error = None
            for k in self._stats:
                if k in ("rejections_by_reason", "levels_seen"):
                    for sk in self._stats[k]:
                        self._stats[k][sk] = 0
                else:
                    self._stats[k] = 0
        logger.info("PolicyMinimal reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> PolicyMinimal:
    """
    模块工厂入口。

    config 需要：
    - initial_equity（或在 instances.risk_policy.equity）

    config 可选：
    - risk.per_trade_risk: 默认 0.007
    - risk.daily_dd_warn: 默认 0.01
    - risk.daily_dd_reduce: 默认 0.02
    - risk.daily_dd_close_only: 默认 0.025
    - risk.daily_dd_halt: 默认 0.03
    - risk.consecutive_loss_reduce: 默认 5
    - risk.consecutive_loss_pause: 默认 8
    - risk.consecutive_loss_pause_s: 默认 3600
    - risk.margin_ratio_warn: 默认 0.6
    - risk.margin_ratio_halt: 默认 0.8
    - risk.protection_rounds: 默认 20
    - risk.protection_qty_ratio: 默认 0.3
    """
    return PolicyMinimal(config=config)


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "risk/policy_minimal.py")
    return MANIFEST
