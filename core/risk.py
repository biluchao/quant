"""
core/risk.py — 风控抽象基类与通用数据结构

风控是"守门人"，不是决策者，也不是执行者。

风控职责：
- 审批订单：APPROVE / REDUCE / REJECT
- 管理单笔风险 R0
- 管理日回撤熔断
- 管理组合敞口
- 管理保证金率
- 连续亏损熔断

风控不做：
- 不决定方向（策略的事）
- 不决定价格（执行层的事）
- 不下单（网关的事）
- 不修改 Order（只返回 approved_qty）
- 不知道策略
- 不知道信号
- 不持有 Gateway / RoundManager / StrategyProvider 引用

关键契约：
1. 风控是纯函数式门卫——输入 Order + Context，输出 RiskResult
2. 风控不修改 Order，只返回 approved_qty
3. 风控不抛异常，所有异常映射为 REJECT(reason="risk_internal_error")
4. 风控内部异常必须记录完整 traceback
5. 风控状态（日统计、连续亏损）必须持久化
6. 风控必须线程安全
7. 风控必须可审计——每次决策记录 risk_metrics
8. 风控不知策略、不知信号、不知轮次 ID
9. 平仓/止损单强制放行（只检查保证金率）
10. 风控必须幂等——同一 client_order_id 重复审批结果一致

风控等级（RiskLevel）：
- NORMAL:    正常，所有订单可过
- WARN:      预警，仓位正常，记录日志
- REDUCE:    降频，新订单仓位 × 0.5
- CLOSE_ONLY:只平仓，拒绝所有开仓/加仓
- HALT:      熔断，拒绝所有订单，只允许紧急平仓

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import threading
import traceback

logger = logging.getLogger(__name__)


# ============================================================================
# 枚举：风控决策与等级
# ============================================================================
class RiskDecision(str, Enum):
    APPROVE = "approve"     # 批准
    REDUCE = "reduce"       # 批准但缩减数量
    REJECT = "reject"       # 拒绝


class RiskLevel(str, Enum):
    """风控等级。由 RiskPolicy 根据日回撤/保证金率/连续亏损动态判定。"""
    NORMAL = "normal"
    WARN = "warn"
    REDUCE = "reduce"
    CLOSE_ONLY = "close_only"
    HALT = "halt"


# 拒绝原因的枚举值（snake_case）
class RejectReason(str, Enum):
    PER_TRADE_RISK_EXCEEDED = "per_trade_risk_exceeded"
    DAILY_DD_HALT = "daily_dd_halt"
    DAILY_DD_CLOSE_ONLY = "daily_dd_close_only"
    MARGIN_RATIO_HALT = "margin_ratio_halt"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    INVALID_QTY = "invalid_qty"
    INVALID_NOTIONAL = "invalid_notional"
    CONSECUTIVE_LOSS_PAUSE = "consecutive_loss_pause"
    MARKET_SHOCK = "market_shock"
    POSITION_MISMATCH = "position_mismatch"
    RISK_INTERNAL_ERROR = "risk_internal_error"
    UNKNOWN = "unknown"


# ============================================================================
# 风控输入上下文
# ============================================================================
@dataclass(frozen=True)
class RiskContext:
    """
    风控审批的输入上下文。由调用方（OrderRouter）组装。
    风控只读，不得修改。
    """
    # 待审批的订单
    order: Any                              # core.gateway.Order

    # 当前持仓（无持仓为 None）
    position: Optional[Any]                 # core.gateway.Position

    # 账户状态
    account: Any                            # core.gateway.AccountState

    # 活跃轮次列表（只读快照，风控不基于 round_id 做特殊判断）
    # 每个元素含：direction / total_qty / weighted_avg_cost / current_stop / risk_amount
    active_rounds: List[Dict[str, Any]]

    # 交易对信息（min_notional / step_size / tick_size / leverage）
    symbol_info: Dict[str, Any]

    # 当前时间（单调时钟，毫秒）
    now_ms: int

    # 日统计
    daily_pnl: Decimal                      # 当日已实现盈亏
    daily_peak_equity: Decimal              # 当日权益峰值

    # 连续亏损轮数
    consecutive_losses: int

    # 资金费率结算保护窗口
    funding_guard_active: bool = False
    funding_guard_multiplier: float = 1.0   # 保护窗口内的仓位乘数

    # 市场突变冷却期
    market_shock_active: bool = False


# ============================================================================
# 风控输出结果
# ============================================================================
@dataclass(frozen=True)
class RiskResult:
    """
    风控审批结果。不可变。

    契约：
    - APPROVE: approved_qty=None（用原 qty）
    - REDUCE:  approved_qty=缩减后的数量（已 round_step_size）
    - REJECT:  approved_qty=None
    """
    decision: RiskDecision
    approved_qty: Optional[Decimal] = None
    reason: str = ""
    level: RiskLevel = RiskLevel.NORMAL
    risk_metrics: Dict[str, Any] = field(default_factory=dict)

    def is_approved(self) -> bool:
        return self.decision in (RiskDecision.APPROVE, RiskDecision.REDUCE)

    def effective_qty(self, original_qty: Decimal) -> Decimal:
        """调用方使用此方法得到最终下单数量。"""
        if self.decision == RiskDecision.APPROVE:
            return original_qty
        if self.decision == RiskDecision.REDUCE:
            return self.approved_qty if self.approved_qty is not None else Decimal("0")
        return Decimal("0")


# ============================================================================
# 风控 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class RiskManifest:
    """风控 MANIFEST 字典解析后的结构。"""
    name: str
    type: str
    version: str
    step: int
    capabilities: tuple
    owner: str
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "RiskManifest":
        required = ("name", "type", "version", "step", "owner")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")
        if d["type"] != "risk":
            raise ValueError(
                f"MANIFEST type must be 'risk', got '{d['type']}'"
            )
        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            capabilities=tuple(d.get("capabilities", [])),
            owner=d["owner"],
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 风控抽象基类
# ============================================================================
class RiskPolicy(ABC):
    """
    风控抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.name / self.version
    2. 实现 approve_order(context) -> RiskResult
    3. 不持有 Gateway / RoundManager / StrategyProvider 引用

    子类必须覆盖的钩子：
    - on_order_filled(result)：订单成交后更新统计
    - on_round_closed(record)：轮次结束后更新连续亏损计数
    - on_account_update(account)：账户状态更新（保证金率）
    - on_round_opened(snapshot)：轮次开始（可选）

    子类必须实现的持久化：
    - snapshot()：导出风控状态
    - restore(state)：恢复风控状态
    """

    # 子类必须设置
    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        self._lock = threading.RLock()

    # ------------------------------------------------------------------------
    # 核心：订单审批
    # ------------------------------------------------------------------------

    @abstractmethod
    def approve_order(self, context: RiskContext) -> RiskResult:
        """
        审批订单。线程安全、幂等。

        契约：
        - 内部异常必须 catch 并返回 REJECT(reason="risk_internal_error")
        - 平仓单/止损单强制放行（只检查保证金率）
        - 返回的 approved_qty 必须是合法的 step_size 整数倍
        - risk_metrics 记录决策依据
        """
        ...

    # ------------------------------------------------------------------------
    # 事件钩子
    # ------------------------------------------------------------------------

    @abstractmethod
    def on_order_filled(self, order_result: Any) -> None:
        """
        订单成交后调用。更新日统计。
        参数：core.gateway.OrderResult
        """
        ...

    @abstractmethod
    def on_round_closed(self, round_record: dict) -> None:
        """
        轮次结束后调用。更新连续亏损计数和日盈亏。
        round_record: 轮次结算记录，含 pnl / r_multiple / exit_reason
        """
        ...

    @abstractmethod
    def on_account_update(self, account: Any) -> None:
        """
        账户状态更新。更新保证金率。
        参数：core.gateway.AccountState
        """
        ...

    def on_round_opened(self, round_snapshot: dict) -> None:
        """轮次开始钩子。默认无操作。"""
        pass

    # ------------------------------------------------------------------------
    # 日切重置
    # ------------------------------------------------------------------------

    @abstractmethod
    def daily_reset(self, now_ms: int) -> None:
        """
        UTC 00:00 重置日统计。
        契约：调用方在 UTC 00:00 触发，风控内部不得自行触发。
        """
        ...

    # ------------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------------

    @abstractmethod
    def get_risk_level(self) -> RiskLevel:
        """返回当前风控等级。"""
        ...

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        健康检查。返回：
            {
                "healthy": bool,
                "name": str,
                "level": str,
                "daily_pnl": str,
                "daily_drawdown": str,
                "consecutive_losses": int,
                "margin_ratio": str,
                "errors": List[str],
                "timestamp": int,
            }
        """
        ...

    # ------------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------------

    @abstractmethod
    def snapshot(self) -> Dict[str, Any]:
        """
        导出风控状态。用于持久化。
        契约：状态可序列化为 JSON。
        """
        ...

    @abstractmethod
    def restore(self, state: Dict[str, Any]) -> None:
        """
        恢复风控状态。重启后调用。
        契约：state 为 snapshot() 的返回值。
        """
        ...

    # ------------------------------------------------------------------------
    # 工具方法（子类可用）
    # ------------------------------------------------------------------------

    @staticmethod
    def is_close_order(order: Any) -> bool:
        """判断是否为平仓类订单（强制放行大部分风控）。"""
        return bool(
            getattr(order, "reduce_only", False)
            or getattr(order, "close_position", False)
        )

    @staticmethod
    def is_stop_order(order: Any) -> bool:
        """判断是否为服务器端止损单（强制放行）。"""
        type_str = getattr(order, "type", "")
        return (
            type_str == "STOP_MARKET"
            or getattr(type_str, "value", "") == "STOP_MARKET"
        )

    @staticmethod
    def round_step_size(qty: Decimal, step_size: Decimal) -> Decimal:
        """向下取整到 step_size 的整数倍。"""
        if step_size <= 0:
            return qty
        return (qty / step_size).to_integral_value(
            rounding="ROUND_DOWN"
        ) * step_size

    def _reject(
        self,
        reason: RejectReason,
        level: Optional[RiskLevel] = None,
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> RiskResult:
        """辅助方法：构造 REJECT 结果。"""
        m = dict(metrics or {})
        if error:
            m["error"] = error
        if not isinstance(reason, str):
            reason_str = reason.value
        else:
            reason_str = reason
        return RiskResult(
            decision=RiskDecision.REJECT,
            approved_qty=None,
            reason=reason_str,
            level=level or self._safe_get_level(),
            risk_metrics=m,
        )

    def _approve(
        self,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> RiskResult:
        """辅助方法：构造 APPROVE 结果。"""
        return RiskResult(
            decision=RiskDecision.APPROVE,
            approved_qty=None,
            reason="approved",
            level=self._safe_get_level(),
            risk_metrics=dict(metrics or {}),
        )

    def _reduce(
        self,
        approved_qty: Decimal,
        reason: RejectReason,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> RiskResult:
        """辅助方法：构造 REDUCE 结果。"""
        reason_str = (
            reason.value if not isinstance(reason, str) else reason
        )
        return RiskResult(
            decision=RiskDecision.REDUCE,
            approved_qty=approved_qty,
            reason=reason_str,
            level=self._safe_get_level(),
            risk_metrics=dict(metrics or {}),
        )

    def _safe_get_level(self) -> RiskLevel:
        """异常安全地获取风控等级（get_risk_level 可能内部异常）。"""
        try:
            return self.get_risk_level()
        except Exception:
            logger.error(
                f"get_risk_level failed:\n{traceback.format_exc()}"
            )
            return RiskLevel.HALT


# ============================================================================
# 模块级工厂校验
# ============================================================================
def validate_risk_module(module, config: dict) -> RiskPolicy:
    """
    加载并校验一个风控模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 RiskPolicy 实例
    4. 实例的 name / version 与 MANIFEST 一致
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = RiskManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, RiskPolicy):
        raise TypeError(
            f"{module.__name__}.create() must return RiskPolicy, "
            f"got {type(instance).__name__}"
        )
    if instance.name != manifest.name:
        raise ValueError(
            f"{module.__name__}: instance.name='{instance.name}' "
            f"!= MANIFEST.name='{manifest.name}'"
        )
    if instance.version != manifest.version:
        raise ValueError(
            f"{module.__name__}: instance.version='{instance.version}' "
            f"!= MANIFEST.version='{manifest.version}'"
        )

    return instance
