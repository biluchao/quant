"""
core/gateway.py — 执行网关抽象基类与通用数据结构 (重铸标准版)

核心设计原则：
1. 数据不可变：所有订单、结果、账户状态统一使用 frozen dataclass。
2. 金融级精度：价格、数量、金额 100% 强制使用 Decimal，严禁 float。
3. 严密异常树：细分网络、限频、拒单、余额不足等异常，指导上层决策。
4. 职责绝对清晰：网关只管执行与回执，绝不包含策略逻辑或风控判断。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

# ============================================================================
# 枚举定义 (彻底消灭字符串裸奔)
# ============================================================================

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"

class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"

    def is_terminal(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED)

class WorkingType(str, Enum):
    MARK_PRICE = "MARK_PRICE"
    CONTRACT_PRICE = "CONTRACT_PRICE"


# ============================================================================
# 异常体系 (指导上层风控与重试)
# ============================================================================

class GatewayError(Exception):
    """网关异常基类"""
    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code

class RetryableError(GatewayError):
    """可重试异常基类"""
    pass

class NetworkError(RetryableError):
    """网络超时或断连"""
    pass

class RateLimitError(RetryableError):
    """触发交易所限频"""
    pass

class ServerError(RetryableError):
    """交易所服务端 5xx 错误"""
    pass

class AuthError(GatewayError):
    """API Key 鉴权失败"""
    pass

class InvalidOrderError(GatewayError):
    """订单参数不合法（精度、最小名义价值等）"""
    pass

class InsufficientBalanceError(GatewayError):
    """保证金不足"""
    pass

class OrderRejectedError(GatewayError):
    """交易所拒绝订单（非余额原因）"""
    pass

class UnknownOrderError(GatewayError):
    """订单不存在（可能已成交或已撤销）"""
    pass


# ============================================================================
# 核心数据结构
# ============================================================================

@dataclass(frozen=True)
class Order:
    """
    标准化的下单请求。
    契约：
    1. client_order_id 必须全局唯一，网关依赖它实现幂等。
    2. qty 与 price 必须为 Decimal。
    """
    client_order_id: str
    symbol: str
    side: OrderSide
    type: OrderType
    qty: Decimal
    
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    reduce_only: bool = False
    close_position: bool = False
    working_type: Optional[WorkingType] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.client_order_id:
            raise ValueError("client_order_id 不能为空")
        if not isinstance(self.qty, Decimal) or self.qty <= 0:
            raise ValueError(f"qty 必须为合法的 Decimal 且 > 0，当前为 {self.qty}")
        if self.price is not None and (not isinstance(self.price, Decimal) or self.price <= 0):
            raise ValueError(f"price 必须为合法的 Decimal 且 > 0，当前为 {self.price}")
        if self.type in (OrderType.LIMIT,) and self.price is None:
            raise ValueError(f"限价单必须提供 price")
        if self.type in (OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET) and self.stop_price is None:
            raise ValueError(f"条件单必须提供 stop_price")

@dataclass(frozen=True)
class Fill:
    """单笔成交明细"""
    trade_id: str
    price: Decimal
    qty: Decimal
    fee: Decimal
    fee_asset: str
    is_maker: bool
    ts: int

@dataclass(frozen=True)
class OrderResult:
    """订单执行的最终回执"""
    client_order_id: str
    exchange_order_id: str
    symbol: str
    status: OrderStatus
    side: OrderSide
    type: OrderType
    
    requested_qty: Decimal
    executed_qty: Decimal
    avg_price: Optional[Decimal]
    stop_price: Optional[Decimal]
    
    total_fee: Decimal = Decimal("0")
    fee_asset: str = "USDT"
    transact_time: int = 0
    
    fills: List[Fill] = field(default_factory=list)
    reject_reason: Optional[str] = None
    synthetic: bool = False  # 标识是否为虚拟撮合

@dataclass(frozen=True)
class Position:
    """持仓信息"""
    symbol: str
    side: OrderSide
    qty: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    leverage: int
    update_time: int
    liquidation_price: Optional[Decimal] = None
    margin: Decimal = Decimal("0")

@dataclass(frozen=True)
class AccountState:
    """账户资金与保证金信息"""
    total_wallet_balance: Decimal
    total_unrealized_pnl: Decimal
    total_margin_balance: Decimal
    total_maint_margin: Decimal
    available_balance: Decimal
    update_time: int

    @property
    def margin_ratio(self) -> Decimal:
        if self.total_wallet_balance <= 0:
            return Decimal("1.0")
        return self.total_maint_margin / self.total_wallet_balance


# ============================================================================
# 网关抽象基类
# ============================================================================

class ExecutionGateway(ABC):
    """
    执行网关基类。
    所有与交易所或虚拟撮合相关的实现，都必须继承此类。
    """
    exchange_name: str = ""

    # ------------------------------------------------------------------------
    # 核心交易操作 (必须幂等)
    # ------------------------------------------------------------------------

    @abstractmethod
    def place_order(self, order: Order) -> OrderResult:
        """
        下单操作。必须幂等。
        如果订单被拒，应抛出 OrderRejectedError 等具体异常。
        """
        ...

    @abstractmethod
    def cancel_order(self, client_order_id: str) -> OrderResult:
        """
        撤单操作。必须幂等。
        如果订单已完结，不应报错，应返回原状态。
        """
        ...

    @abstractmethod
    def close_position(self, symbol: str, reason: str = "manual") -> Optional[OrderResult]:
        """
        市价平掉该交易对的全部持仓。
        如果没有持仓，返回 None。
        """
        ...

    @abstractmethod
    def ensure_server_side_stop(self, symbol: str, stop_price: Decimal, side: OrderSide, qty: Decimal, client_order_id: str) -> OrderResult:
        """
        挂服务器端止损单。
        必须使用 reduce_only=True 和 MARK_PRICE 触发。
        """
        ...

    # ------------------------------------------------------------------------
    # 查询操作
    # ------------------------------------------------------------------------

    @abstractmethod
    def get_order(self, client_order_id: str) -> OrderResult:
        """查询指定订单"""
        ...

    @abstractmethod
    def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        ...

    @abstractmethod
    def get_account_state(self) -> AccountState:
        """获取账户资金状态"""
        ...

    # ------------------------------------------------------------------------
    # 配置操作
    # ------------------------------------------------------------------------

    @abstractmethod
    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆"""
        ...


# ============================================================================
# 模块工厂 (仅做类型声明，实际无法直接实例化)
# ============================================================================
MANIFEST = {
    "name": "gateway_base",
    "type": "gateway",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["execution_base"],
    "module_dependencies": [],
    "owner": "lego",
    "exchange": "base",
}

def create(config: dict) -> ExecutionGateway:
    raise NotImplementedError("ExecutionGateway 是抽象基类，请实例化具体的网关实现")
