"""
core/gateway.py — 执行网关抽象基类与通用数据结构

网关是"纯执行器"，不是"决策器"。

网关职责：
- 接收 Order，转换为交易所 API 调用
- 返回 OrderResult
- 管理订单状态（NEW/FILLED/CANCELED）
- 提供账户/持仓/订单查询

网关不做：
- 不知道策略（不知道 Order 从哪来）
- 不知道风控（不持有 RiskPolicy）
- 不知道轮次（不持有 RoundManager）
- 不做重试（重试是调用方的事）
- 不做限频（限频由注入的 RateLimiter 负责）

关键契约：
1. 所有价格/数量用 Decimal，不用 float
2. 所有写操作（下单/撤单/平仓）必须幂等，用 client_order_id
3. 所有方法必须线程安全
4. 网关内部异常必须映射到 GatewayError 子类，不得向上抛原始异常
5. 网关不重试（RateLimitError 由调用方退避重试）
6. 超时抛 NetworkError，调用方负责查 client_order_id 确认状态
7. 网关不得感知策略/风控/轮次
8. 网关的 transact_time 来自交易所，不伪造

错误分层：
- GatewayError（基类，不可重试）
  - AuthError（认证/权限错误）
  - InvalidOrderError（参数错误：精度、名义价值、方向）
  - InsufficientBalanceError（余额不足）
  - OrderRejectedError（交易所拒单）
  - UnknownOrderError（订单不存在）
  - PositionMismatchError（持仓与预期不符）
- RetryableError（可重试）
  - NetworkError（网络超时、连接失败）
  - RateLimitError（限频，需退避）
  - ServerError（交易所 5xx，可重试）

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 枚举：订单相关
# ============================================================================
class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """订单类型。Step 1 只支持 MARKET 和 STOP_MARKET。"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"                      # Step 2+
    STOP_MARKET = "STOP_MARKET"          # 服务器端止损
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"  # Step 3+
    STOP = "STOP"                        # Step 2+
    TAKE_PROFIT = "TAKE_PROFIT"          # Step 3+


class TimeInForce(str, Enum):
    GTC = "GTC"    # 撤销前一直有效
    IOC = "IOC"    # 立即成交否则取消
    FOK = "FOK"    # 全部成交否则取消
    GTX = "GTX"    # 只做 maker（Step 2+）


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"          # 本地无法确定

    def is_terminal(self) -> bool:
        return self in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        )


class WorkingType(str, Enum):
    """条件单触发价格类型。"""
    MARK_PRICE = "MARK_PRICE"    # 标记价格（推荐，抗插针）
    CONTRACT_PRICE = "CONTRACT_PRICE"  # 最新成交价


# ============================================================================
# 数据结构：下单请求
# ============================================================================
@dataclass(frozen=True)
class Order:
    """
    下单请求。不可变。

    契约：
    - 所有价格/数量用 Decimal
    - client_order_id 由调用方生成，网关不得修改
    - 网关负责 round_step_size() / round_tick_size()
    """
    # ---- 标识 ----
    client_order_id: str                    # 调用方生成，确定性，用于幂等
    symbol: str                             # 交易对（大写，如 BTCUSDT）

    # ---- 核心参数 ----
    side: OrderSide
    type: OrderType
    qty: Decimal                            # 数量（基础资产单位）

    # ---- 价格参数（MARKET 时可为 None）----
    price: Optional[Decimal] = None         # 限价单价格
    stop_price: Optional[Decimal] = None    # 条件单触发价

    # ---- 执行参数 ----
    time_in_force: Optional[TimeInForce] = None
    reduce_only: bool = False               # 只减仓
    close_position: bool = False            # 全平（与 reduce_only 互斥）
    working_type: Optional[WorkingType] = None  # 条件单触发价格类型

    # ---- 元数据（网关不解析，仅记录）----
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """构造时校验。"""
        if not self.client_order_id:
            raise ValueError("client_order_id is required")
        if self.qty <= 0:
            raise ValueError(f"qty must be positive, got {self.qty}")
        if self.type == OrderType.MARKET:
            if self.price is not None:
                raise ValueError("MARKET order must not have price")
        if self.type in (OrderType.LIMIT, OrderType.STOP, OrderType.TAKE_PROFIT):
            if self.price is None:
                raise ValueError(f"{self.type} requires price")
        if self.type in (
            OrderType.STOP_MARKET,
            OrderType.TAKE_PROFIT_MARKET,
            OrderType.STOP,
            OrderType.TAKE_PROFIT,
        ):
            if self.stop_price is None:
                raise ValueError(f"{self.type} requires stop_price")
        if self.reduce_only and self.close_position:
            raise ValueError("reduce_only and close_position are mutually exclusive")


# ============================================================================
# 数据结构：成交明细
# ============================================================================
@dataclass(frozen=True)
class Fill:
    """单笔成交。部分成交时，一个 OrderResult 含多个 Fill。"""
    trade_id: str
    price: Decimal
    qty: Decimal
    fee: Decimal                            # 手续费（正数）
    fee_asset: str                          # 手续费币种（通常 USDT）
    is_maker: bool                          # maker/taker
    ts: int                                 # 成交时间（交易所时间，毫秒）


# ============================================================================
# 数据结构：下单结果
# ============================================================================
@dataclass(frozen=True)
class OrderResult:
    """
    下单/撤单/查询的返回结果。不可变。
    """
    # ---- 标识 ----
    client_order_id: str
    exchange_order_id: str                  # 交易所订单 ID
    symbol: str

    # ---- 状态 ----
    status: OrderStatus
    side: OrderSide
    type: OrderType

    # ---- 执行结果 ----
    requested_qty: Decimal
    executed_qty: Decimal                   # 已成交数量
    avg_price: Optional[Decimal]            # 成交均价（未成交为 None）
    request_price: Optional[Decimal]        # 请求价格（MARKET 时为 None）
    stop_price: Optional[Decimal]           # 条件单触发价
    fills: List[Fill] = field(default_factory=list)

    # ---- 费用 ----
    total_fee: Decimal = Decimal("0")
    fee_asset: str = "USDT"

    # ---- 时间 ----
    transact_time: int = 0                  # 交易所时间（毫秒）
    update_time: int = 0                    # 最后更新时间（交易所时间）

    # ---- 错误（status=REJECTED 时）----
    reject_reason: Optional[str] = None
    reject_code: Optional[int] = None

    # ---- 虚拟券商标记 ----
    synthetic: bool = False                 # True = 虚拟券商撮合，非真实成交

    def is_terminal(self) -> bool:
        return self.status.is_terminal()

    def to_dict(self) -> dict:
        """用于日志。"""
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": self.symbol,
            "status": self.status.value,
            "side": self.side.value,
            "type": self.type.value,
            "requested_qty": str(self.requested_qty),
            "executed_qty": str(self.executed_qty),
            "avg_price": str(self.avg_price) if self.avg_price else None,
            "total_fee": str(self.total_fee),
            "transact_time": self.transact_time,
            "synthetic": self.synthetic,
            "fills_count": len(self.fills),
        }


# ============================================================================
# 数据结构：持仓
# ============================================================================
@dataclass(frozen=True)
class Position:
    """单个交易对的持仓。"""
    symbol: str
    side: OrderSide                         # BUY=LONG, SELL=SHORT
    qty: Decimal                            # 持仓数量（正数）
    entry_price: Decimal                    # 开仓均价
    mark_price: Decimal                     # 标记价格
    unrealized_pnl: Decimal                 # 未实现盈亏
    liquidation_price: Optional[Decimal]    # 强平价格
    margin: Decimal                         # 占用保证金
    leverage: int                           # 杠杆
    update_time: int                        # 交易所时间（毫秒）

    @property
    def notional(self) -> Decimal:
        return self.qty * self.mark_price


# ============================================================================
# 数据结构：账户状态
# ============================================================================
@dataclass(frozen=True)
class AccountState:
    """账户整体状态。"""
    total_wallet_balance: Decimal           # 钱包余额
    total_unrealized_pnl: Decimal           # 未实现盈亏
    total_margin_balance: Decimal           # 保证金余额
    total_maint_margin: Decimal             # 维持保证金
    available_balance: Decimal              # 可用余额
    update_time: int                        # 交易所时间（毫秒）

    @property
    def margin_ratio(self) -> Decimal:
        """维持保证金率。"""
        if self.total_wallet_balance <= 0:
            return Decimal("1")
        return self.total_maint_margin / self.total_wallet_balance


# ============================================================================
# 错误分层
# ============================================================================
class GatewayError(Exception):
    """网关错误基类。不可重试。"""
    code: int = -1

    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        if code is not None:
            self.code = code


class RetryableError(GatewayError):
    """可重试错误基类。调用方可以退避重试。"""
    pass


# ---- 不可重试 ----
class AuthError(GatewayError):
    """认证/权限错误。API Key 无效、权限不足。"""
    pass


class InvalidOrderError(GatewayError):
    """订单参数错误。精度、名义价值、方向。"""
    pass


class InsufficientBalanceError(GatewayError):
    """余额不足。"""
    pass


class OrderRejectedError(GatewayError):
    """交易所拒单。原因见 message 和 code。"""
    pass


class UnknownOrderError(GatewayError):
    """订单不存在。"""
    pass


class PositionMismatchError(GatewayError):
    """持仓与预期不符。对账失败时抛出。"""
    pass


# ---- 可重试 ----
class NetworkError(RetryableError):
    """网络超时、连接失败。"""
    pass


class RateLimitError(RetryableError):
    """触发限频。需要退避重试。"""
    pass


class ServerError(RetryableError):
    """交易所 5xx 错误。可重试。"""
    pass


# ============================================================================
# 网关 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class GatewayManifest:
    """网关 MANIFEST 字典解析后的结构。"""
    name: str
    type: str
    version: str
    step: int
    exchange: str
    capabilities: tuple
    owner: str
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "GatewayManifest":
        required = ("name", "type", "version", "step", "owner", "exchange")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")
        if d["type"] != "gateway":
            raise ValueError(
                f"MANIFEST type must be 'gateway', got '{d['type']}'"
            )
        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            exchange=d["exchange"],
            capabilities=tuple(d.get("capabilities", [])),
            owner=d["owner"],
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 网关抽象基类
# ============================================================================
class ExecutionGateway(ABC):
    """
    执行网关抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.exchange_name / self.version
    2. 实现所有 abstractmethod
    3. 不持有 StrategyProvider / RiskPolicy / RoundManager 引用

    子类可选覆盖：
    - on_order_update(callback)：注册订单状态推送回调（默认轮询）
    - health_check()：自定义健康检查
    """

    # 子类必须设置
    exchange_name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        self._order_callbacks: List[Callable[[OrderResult], None]] = []

    # ------------------------------------------------------------------------
    # 下单与撤单（写操作，必须幂等）
    # ------------------------------------------------------------------------

    @abstractmethod
    def place_order(self, order: Order) -> OrderResult:
        """
        下单。幂等：相同 client_order_id 重复调用返回相同结果。

        契约：
        - 价格/数量精度由网关处理（round_step_size / round_tick_size）
        - 名义价值 < min_notional 抛 InvalidOrderError
        - 余额不足抛 InsufficientBalanceError
        - 网络超时抛 NetworkError（调用方需查 client_order_id 确认状态）
        - 限频抛 RateLimitError
        - 交易所拒单抛 OrderRejectedError
        """
        ...

    @abstractmethod
    def cancel_order(self, client_order_id: str) -> OrderResult:
        """
        撤单。幂等：撤已完结订单返回原状态，不报错。

        契约：
        - 订单不存在抛 UnknownOrderError
        - 网络超时抛 NetworkError
        """
        ...

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        撤掉所有未完结订单。尽力撤单，返回成功数量和失败列表。

        Returns:
            {"canceled": int, "failed": List[{"client_order_id": str, "error": str}]}
        """
        open_orders = self.get_open_orders(symbol)
        canceled = 0
        failed = []
        for o in open_orders:
            try:
                self.cancel_order(o.client_order_id)
                canceled += 1
            except Exception as e:
                failed.append({
                    "client_order_id": o.client_order_id,
                    "error": str(e),
                })
        return {"canceled": canceled, "failed": failed}

    # ------------------------------------------------------------------------
    # 订单查询（读操作）
    # ------------------------------------------------------------------------

    @abstractmethod
    def get_order(self, client_order_id: str) -> OrderResult:
        """
        查询订单。已完结订单 24 小时内可查。
        不存在抛 UnknownOrderError。
        """
        ...

    @abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderResult]:
        """查询所有未完结订单。symbol=None 表示全部交易对。"""
        ...

    # ------------------------------------------------------------------------
    # 持仓与账户（读操作）
    # ------------------------------------------------------------------------

    @abstractmethod
    def get_positions(self) -> List[Position]:
        """查询所有持仓。无持仓返回空列表。"""
        ...

    def get_position(self, symbol: str) -> Optional[Position]:
        """查询单个交易对的持仓。无持仓返回 None。默认实现调 get_positions()。"""
        for p in self.get_positions():
            if p.symbol == symbol:
                return p
        return None

    @abstractmethod
    def get_account_state(self) -> AccountState:
        """查询账户整体状态。"""
        ...

    # ------------------------------------------------------------------------
    # 平仓（写操作，必须幂等）
    # ------------------------------------------------------------------------

    @abstractmethod
    def close_position(
        self, symbol: str, reason: str = "manual"
    ) -> Optional[OrderResult]:
        """
        市价平掉指定交易对的全部持仓。幂等：无持仓返回 None。

        契约：
        - 强制 reduce_only=True
        - 用市价单
        - reason 仅记录，不传给交易所
        """
        ...

    def close_all_positions(self, reason: str = "manual") -> List[OrderResult]:
        """平掉所有持仓。尽力平仓，返回结果列表。"""
        results = []
        for p in self.get_positions():
            if p.qty > 0:
                try:
                    r = self.close_position(p.symbol, reason=reason)
                    if r is not None:
                        results.append(r)
                except Exception as e:
                    logger.error(f"close_position failed for {p.symbol}: {e}")
        return results

    # ------------------------------------------------------------------------
    # 服务器端止损（写操作，必须幂等）
    # ------------------------------------------------------------------------

    @abstractmethod
    def ensure_server_side_stop(
        self,
        symbol: str,
        stop_price: Decimal,
        side: OrderSide,                    # 平仓方向：LONG 持仓用 SELL
        qty: Decimal,
        client_order_id: str,
    ) -> OrderResult:
        """
        挂服务器端止损单。幂等：相同 client_order_id 重复调用返回相同结果。

        契约：
        - 止损单类型：STOP_MARKET
        - 触发价格类型：MARK_PRICE
        - 强制 reduce_only=True
        - 挂单失败抛 ServerError（可重试）
        """
        ...

    def cancel_server_side_stop(self, client_order_id: str) -> OrderResult:
        """撤掉服务器端止损单。默认实现调 cancel_order()。"""
        return self.cancel_order(client_order_id)

    # ------------------------------------------------------------------------
    # 杠杆设置（写操作，必须幂等）
    # ------------------------------------------------------------------------

    @abstractmethod
    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        设置杠杆。幂等：重复设置相同杠杆不报错。
        返回 True 表示设置成功。
        """
        ...

    # ------------------------------------------------------------------------
    # 订单状态推送（可选）
    # ------------------------------------------------------------------------

    def on_order_update(self, callback: Callable[[OrderResult], None]) -> None:
        """
        注册订单状态推送回调。网关收到推送时调用所有回调。
        默认实现：不支持推送，调用方应使用轮询 get_order()。
        """
        self._order_callbacks.append(callback)

    def _emit_order_update(self, result: OrderResult) -> None:
        """子类收到推送时调用此方法通知回调。"""
        for cb in self._order_callbacks:
            try:
                cb(result)
            except Exception as e:
                logger.error(f"order_update callback error: {e}")

    # ------------------------------------------------------------------------
    # 健康检查
    # ------------------------------------------------------------------------

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        健康检查。返回：
            {
                "healthy": bool,
                "exchange": str,
                "latency_ms": float,
                "errors": List[str],
                "timestamp": int,
            }
        """
        ...

    # ------------------------------------------------------------------------
    # 工具方法（子类可用）
    # ------------------------------------------------------------------------

    @staticmethod
    def round_step_size(qty: Decimal, step_size: Decimal) -> Decimal:
        """向下取整到 step_size 的整数倍。"""
        if step_size <= 0:
            return qty
        return (qty / step_size).to_integral_value(
            rounding="ROUND_DOWN"
        ) * step_size

    @staticmethod
    def round_tick_size(price: Decimal, tick_size: Decimal) -> Decimal:
        """取整到 tick_size 的整数倍。"""
        if tick_size <= 0:
            return price
        return (price / tick_size).to_integral_value(
            rounding="ROUND_HALF_UP"
        ) * tick_size


# ============================================================================
# 模块级工厂校验
# ============================================================================
def validate_gateway_module(module, config: dict) -> ExecutionGateway:
    """
    加载并校验一个网关模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 ExecutionGateway 实例
    4. 实例的 exchange_name / version 与 MANIFEST 一致
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = GatewayManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, ExecutionGateway):
        raise TypeError(
            f"{module.__name__}.create() must return ExecutionGateway, "
            f"got {type(instance).__name__}"
        )
    if instance.exchange_name != manifest.exchange:
        raise ValueError(
            f"{module.__name__}: instance.exchange_name='{instance.exchange_name}' "
            f"!= MANIFEST.exchange='{manifest.exchange}'"
        )
    if instance.version != manifest.version:
        raise ValueError(
            f"{module.__name__}: instance.version='{instance.version}' "
            f"!= MANIFEST.version='{manifest.version}'"
        )

    return instance
