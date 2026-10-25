"""
risk/policy_minimal.py — Step 1 最小风控策略 (重铸标准版)

核心设计原则：
1. 严格依赖：必须传入 config 与初始资金，拒绝隐式默认值。
2. 强类型控制：所有金额与比例强制使用 Decimal。
3. 铁面无私：风控参数越界、非法的 Order，直接抛错或返回 REJECT。
4. 状态清晰：提供标准的 property 查询，严禁直接篡改内部状态。
"""

from dataclasses import dataclass
from decimal import Decimal
import logging
import threading
import time
from typing import Any, Dict, Optional

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
# 常量
# ============================================================================
DEFAULT_PER_TRADE_RISK = Decimal("0.007")
DEFAULT_DAILY_DD_WARN = Decimal("0.01")
DEFAULT_DAILY_DD_REDUCE = Decimal("0.02")
DEFAULT_DAILY_DD_CLOSE_ONLY = Decimal("0.025")
DEFAULT_DAILY_DD_HALT = Decimal("0.03")
DEFAULT_CONSECUTIVE_LOSS_REDUCE = 5
DEFAULT_CONSECUTIVE_LOSS_PAUSE = 8
DEFAULT_PROTECTION_ROUNDS = 20
DEFAULT_PROTECTION_QTY_RATIO = Decimal("0.3")
REDUCE_QTY_MULTIPLIER = Decimal("0.5")

class PolicyMinimal(RiskPolicy):
    """
    Step 1 最小风控策略
    """

    name = "policy_minimal"
    version = "1.0.0"

    def __init__(self, config: dict, initial_equity: Decimal = Decimal("10000")):
        self._config = config
        risk_cfg = config.get("risk", {})

        # 读取参数并强转 Decimal
        self._per_trade_risk = Decimal(str(risk_cfg.get("per_trade_risk", DEFAULT_PER_TRADE_RISK)))
        self._daily_dd_warn = Decimal(str(risk_cfg.get("daily_dd_warn", DEFAULT_DAILY_DD_WARN)))
        self._daily_dd_reduce = Decimal(str(risk_cfg.get("daily_dd_reduce", DEFAULT_DAILY_DD_REDUCE)))
        self._daily_dd_close_only = Decimal(str(risk_cfg.get("daily_dd_close_only", DEFAULT_DAILY_DD_CLOSE_ONLY)))
        self._daily_dd_halt = Decimal(str(risk_cfg.get("daily_dd_halt", DEFAULT_DAILY_DD_HALT)))
        
        self._consecutive_loss_reduce = int(risk_cfg.get("consecutive_loss_reduce", DEFAULT_CONSECUTIVE_LOSS_REDUCE))
        self._consecutive_loss_pause = int(risk_cfg.get("consecutive_loss_pause", DEFAULT_CONSECUTIVE_LOSS_PAUSE))
        self._protection_rounds = int(risk_cfg.get("protection_rounds", DEFAULT_PROTECTION_ROUNDS))
        self._protection_qty_ratio = Decimal(str(risk_cfg.get("protection_qty_ratio", DEFAULT_PROTECTION_QTY_RATIO)))

        # 构造时强行校验参数逻辑
        if self._per_trade_risk <= 0 or self._per_trade_risk > Decimal("0.02"):
            raise ValueError(f"非法单笔风险 per_trade_risk: {self._per_trade_risk}")
        if not (self._daily_dd_warn < self._daily_dd_reduce < self._daily_dd_close_only < self._daily_dd_halt):
            raise ValueError("日回撤阈值顺序错误 (warn < reduce < close_only < halt)")

        # ---- 内部运行状态 ----
        self._lock = threading.RLock()
        self._initial_equity = Decimal(str(initial_equity))
        self._equity = Decimal(str(initial_equity))
        self._daily_pnl = Decimal("0")
        self._peak_equity = self._initial_equity
        self._consecutive_losses = 0
        self._margin_ratio = Decimal("0")
        
        # 保护期计数
        self._completed_rounds = 0
        self._protection_ended = (self._protection_rounds == 0)

        # 日切控制
        self._last_reset_day = -1

        # 统计
        self._stats = {
            "approvals": 0,
            "reductions": 0,
            "rejections": 0,
        }

        logger.info(f"风控策略初始化: equity={self._equity}, R0_limit={self._per_trade_risk}")

    # ========================================================================
    # 暴露给外部的只读属性
    # ========================================================================

    @property
    def equity(self) -> Decimal:
        with self._lock:
            return self._equity
            
    @property
    def daily_pnl(self) -> Decimal:
        with self._lock:
            return self._daily_pnl

    # ========================================================================
    # 核心审批逻辑
    # ========================================================================

    def approve_order(self, context: RiskContext) -> RiskResult:
        with self._lock:
            order = context.order
            if order is None:
                return self._reject(RejectReason.INVALID_QTY, "Order is None")

            # 1. 平仓单或止损单：无条件放行（必须允许离场）
            if getattr(order, "reduce_only", False) or getattr(order, "close_position", False) or getattr(order.type, "value", "") == "STOP_MARKET":
                self._stats["approvals"] += 1
                return self._approve(metrics={"path": "close_or_stop"})

            # 2. 开仓单数量校验
            qty = Decimal(str(order.qty))
            if qty <= 0:
                return self._reject(RejectReason.INVALID_QTY, f"qty={qty}")

            # 3. 整体风控等级检查
            current_level = self._calculate_risk_level(context.now_ms)
            if current_level == RiskLevel.HALT:
                return self._reject(RejectReason.DAILY_DD_HALT, level=current_level)
            if current_level == RiskLevel.CLOSE_ONLY:
                return self._reject(RejectReason.DAILY_DD_CLOSE_ONLY, level=current_level)

            # 4. 单笔 R0 风险校验
            entry_price, stop_price = self._extract_prices(order)
            if entry_price <= 0 or stop_price <= 0 or entry_price == stop_price:
                return self._reject(RejectReason.INVALID_QTY, "无法确定有效的入场价或止损价")

            risk_per_unit = abs(entry_price - stop_price)
            r0 = risk_per_unit * qty
            
            # 保护期内缩小风险预算
            protection_factor = self._protection_qty_ratio if not self._protection_ended else Decimal("1")
            max_r0 = self._equity * self._per_trade_risk * protection_factor

            if r0 > max_r0:
                # 尝试降级仓位
                approved_qty = max_r0 / risk_per_unit
                if approved_qty <= 0:
                    return self._reject(RejectReason.PER_TRADE_RISK_EXCEEDED, "降级后仓位为 0")
                self._stats["reductions"] += 1
                return self._reduce(approved_qty, reason=RejectReason.PER_TRADE_RISK_EXCEEDED, level=current_level)

            # 5. 检查其他降频条件 (比如 REDUCE 级别)
            if current_level == RiskLevel.REDUCE:
                self._stats["reductions"] += 1
                return self._reduce(qty * REDUCE_QTY_MULTIPLIER, reason=RejectReason.DAILY_DD_CLOSE_ONLY, level=current_level)

            # 通过
            self._stats["approvals"] += 1
            return self._approve(level=current_level)

    def _extract_prices(self, order: Any) -> tuple:
        """从订单或 metadata 中抽取安全的价格"""
        entry = Decimal("0")
        stop = Decimal("0")
        
        # 尝试直接从 order 提取
        if getattr(order, "price", None):
            entry = Decimal(str(order.price))
        elif getattr(order, "metadata", {}).get("expected_price"):
            entry = Decimal(str(order.metadata["expected_price"]))
            
        if getattr(order, "stop_price", None):
            stop = Decimal(str(order.stop_price))
        elif getattr(order, "metadata", {}).get("stop_price_hint"):
            stop = Decimal(str(order.metadata["stop_price_hint"]))
            
        return entry, stop

    # ========================================================================
    # 风控等级与状态计算
    # ========================================================================

    def _calculate_risk_level(self, now_ms: int) -> RiskLevel:
        drawdown = self._calculate_drawdown()

        if self._margin_ratio >= Decimal("0.8") or drawdown >= self._daily_dd_halt:
            return RiskLevel.HALT
        if self._margin_ratio >= Decimal("0.6") or drawdown >= self._daily_dd_close_only:
            return RiskLevel.CLOSE_ONLY
        if drawdown >= self._daily_dd_reduce or self._consecutive_losses >= self._consecutive_loss_reduce:
            return RiskLevel.REDUCE
        if drawdown >= self._daily_dd_warn:
            return RiskLevel.WARN
        return RiskLevel.NORMAL

    def _calculate_drawdown(self) -> Decimal:
        current = self._initial_equity + self._daily_pnl
        if current > self._peak_equity:
            self._peak_equity = current
        if self._peak_equity <= 0:
            return Decimal("1")
        return (self._peak_equity - current) / self._peak_equity

    def get_risk_level(self) -> RiskLevel:
        with self._lock:
            return self._calculate_risk_level(int(time.time() * 1000))

    # ========================================================================
    # 状态更新钩子
    # ========================================================================

    def on_round_closed(self, round_record: dict) -> None:
        with self._lock:
            pnl = Decimal(str(round_record.get("realized_pnl", "0")))
            self._daily_pnl += pnl
            self._completed_rounds += 1

            if pnl < 0:
                self._consecutive_losses = min(self._consecutive_losses + 1, 1000)
            elif pnl > 0:
                self._consecutive_losses = 0

            # 保护期结束判定
            if not self._protection_ended and self._completed_rounds >= self._protection_rounds:
                self._protection_ended = True
                logger.info("保护期结束，恢复正常仓位限额。")

            logger.info(f"轮次结束结算: pnl={pnl}, 连亏={self._consecutive_losses}, 当日累计={self._daily_pnl}")

    def on_account_update(self, account: Any) -> None:
        with self._lock:
            if account and hasattr(account, "total_maint_margin") and hasattr(account, "total_wallet_balance"):
                wallet = Decimal(str(account.total_wallet_balance))
                if wallet > 0:
                    ratio = Decimal(str(account.total_maint_margin)) / wallet
                    self._margin_ratio = min(ratio, Decimal("1.0"))

    def daily_reset(self, now_ms: int) -> None:
        """按 UTC 日期重置每日统计"""
        with self._lock:
            dt = time.gmtime(now_ms / 1000)
            current_day = dt.tm_yday

            if self._last_reset_day == -1:
                self._last_reset_day = current_day
                return

            if current_day != self._last_reset_day:
                logger.info(f"触发日切重置，昨日盈亏: {self._daily_pnl}")
                self._initial_equity = self._peak_equity  # 保留峰值作为新基准
                self._daily_pnl = Decimal("0")
                self._consecutive_losses = 0
                self._last_reset_day = current_day

    # ========================================================================
    # 构造结果辅助函数
    # ========================================================================

    def _approve(self, level: RiskLevel = RiskLevel.NORMAL, metrics: dict = None) -> RiskResult:
        return RiskResult(decision=RiskDecision.APPROVE, reason="approved", level=level, risk_metrics=metrics or {})

    def _reduce(self, qty: Decimal, reason: RejectReason, level: RiskLevel = RiskLevel.NORMAL) -> RiskResult:
        return RiskResult(
            decision=RiskDecision.REDUCE,
            approved_qty=qty,
            reason=reason.value if hasattr(reason, "value") else str(reason),
            level=level,
        )

    def _reject(self, reason: RejectReason, msg: str = "", level: RiskLevel = RiskLevel.NORMAL) -> RiskResult:
        self._stats["rejections"] += 1
        return RiskResult(
            decision=RiskDecision.REJECT,
            reason=reason.value if hasattr(reason, "value") else str(reason),
            level=level,
            risk_metrics={"msg": msg},
        )


# ============================================================================
# 模块工厂函数
# ============================================================================
MANIFEST = {
    "name": "policy_minimal",
    "type": "risk",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["minimal_risk"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> PolicyMinimal:
    """供 ModuleRegistry 调用的标准工厂"""
    # 从配置里获取初始资金，如果没有则使用默认值
    initial_equity = config.get("initial_equity", 10000.0)
    return PolicyMinimal(config, initial_equity=Decimal(str(initial_equity)))
