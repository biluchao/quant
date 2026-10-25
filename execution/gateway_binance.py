"""
execution/gateway_binance.py — 币安实盘网关 (重铸标准版)

核心设计原则：
1. 纯翻译官：只负责将 Order 翻译为 REST 请求，将响应翻译为 OrderResult。
2. 杜绝底层纠缠：网络、签名、限频、重试全部交由 RestClient 处理。
3. 精度绝对安全：所有浮点数转为字符串时，强制执行定点格式化。
"""

import logging
from decimal import Decimal
from typing import Any, List, Optional

from core.gateway import (
    ExecutionGateway,
    Fill,
    InvalidOrderError,
    Order,
    OrderResult,
    OrderRejectedError,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    AccountState,
    WorkingType,
)
from data.rest_client import RestClient

logger = logging.getLogger(__name__)

class BinanceGateway(ExecutionGateway):
    """
    币安合约实盘网关。
    """
    exchange_name = "binance"
    version = "1.0.0"

    def __init__(self, rest_client: RestClient, config: dict):
        if not rest_client:
            raise ValueError("BinanceGateway 必须注入 RestClient")
        self.rest = rest_client
        self.config = config

    # ========================================================================
    # 核心交易操作
    # ========================================================================

    def place_order(self, order: Order) -> OrderResult:
        """向币安发送下单请求"""
        params = {
            "symbol": order.symbol.upper(),
            "side": order.side.value,
            "type": order.type.value,
            "quantity": self._format_dec(order.qty),
            "newClientOrderId": order.client_order_id,
        }

        if order.price is not None:
            params["price"] = self._format_dec(order.price)
        if order.stop_price is not None:
            params["stopPrice"] = self._format_dec(order.stop_price)
        if order.reduce_only:
            params["reduceOnly"] = "true"
        if order.close_position:
            params["closePosition"] = "true"
        if order.working_type:
            params["workingType"] = order.working_type.value

        # 发送请求（RestClient 已经处理了签名、限频和错误抛出）
        resp = self.rest.private_request("POST", "/fapi/v1/order", params=params, weight=1)
        
        return self._parse_order_response(resp)

    def cancel_order(self, client_order_id: str) -> OrderResult:
        """撤销订单"""
        params = {
            "symbol": self.config.get("symbol", "BTCUSDT").upper(),
            "origClientOrderId": client_order_id
        }
        resp = self.rest.private_request("DELETE", "/fapi/v1/order", params=params, weight=1)
        return self._parse_order_response(resp)

    def close_position(self, symbol: str, reason: str = "manual") -> Optional[OrderResult]:
        """市价平掉全部持仓"""
        pos_list = self.rest.get_positions(symbol)
        if not pos_list:
            return None
            
        pos = pos_list[0]
        side = OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY
        
        # 构造平仓单
        from system.order_idempotent import OrderIdGenerator, OrderKind
        # 实盘环境由外部注入，这里直接给个临时ID，或用全量平仓参数
        cid = f"close-{int(self.rest.clock.now_ms())}"
        
        order = Order(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            type=OrderType.MARKET,
            qty=pos.qty,
            reduce_only=True
        )
        return self.place_order(order)

    def ensure_server_side_stop(self, symbol: str, stop_price: Decimal, side: OrderSide, qty: Decimal, client_order_id: str) -> OrderResult:
        """挂服务器端止损单"""
        order = Order(
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            type=OrderType.STOP_MARKET,
            qty=qty,
            stop_price=stop_price,
            reduce_only=True,
            working_type=WorkingType.MARK_PRICE
        )
        return self.place_order(order)

    # ========================================================================
    # 查询操作
    # ========================================================================

    def get_order(self, client_order_id: str) -> OrderResult:
        """查询订单详情"""
        params = {
            "symbol": self.config.get("symbol", "BTCUSDT").upper(),
            "origClientOrderId": client_order_id
        }
        resp = self.rest.private_request("GET", "/fapi/v1/order", params=params, weight=1)
        return self._parse_order_response(resp)

    def get_positions(self) -> List[Position]:
        return self.rest.get_positions()

    def get_account_state(self) -> AccountState:
        return self.rest.get_account()

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        try:
            self.rest.private_request("POST", "/fapi/v1/leverage", params={"symbol": symbol, "leverage": leverage}, weight=1)
            return True
        except Exception as e:
            logger.error(f"设置杠杆失败: {e}")
            return False

    # ========================================================================
    # 内部解析与格式转换
    # ========================================================================

    def _parse_order_response(self, resp: dict) -> OrderResult:
        """将币安 JSON 翻译为强类型 OrderResult"""
        try:
            status_str = resp.get("status", "NEW")
            # 强转枚举
            status_map = {
                "NEW": OrderStatus.NEW,
                "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                "FILLED": OrderStatus.FILLED,
                "CANCELED": OrderStatus.CANCELED,
                "REJECTED": OrderStatus.REJECTED,
                "EXPIRED": OrderStatus.EXPIRED,
            }
            status = status_map.get(status_str, OrderStatus.NEW)

            side = OrderSide.BUY if resp.get("side") == "BUY" else OrderSide.SELL
            
            type_map = {
                "MARKET": OrderType.MARKET,
                "LIMIT": OrderType.LIMIT,
                "STOP_MARKET": OrderType.STOP_MARKET,
            }
            order_type = type_map.get(resp.get("type", "MARKET"), OrderType.MARKET)

            # 解析金额（使用 Decimal）
            executed_qty = Decimal(str(resp.get("executedQty", "0")))
            avg_price = Decimal(str(resp.get("avgPrice", "0")))
            if avg_price <= 0:
                avg_price = None
                
            stop_price = Decimal(str(resp.get("stopPrice", "0")))
            if stop_price <= 0:
                stop_price = None

            return OrderResult(
                client_order_id=resp.get("clientOrderId", ""),
                exchange_order_id=str(resp.get("orderId", "")),
                symbol=resp.get("symbol", ""),
                status=status,
                side=side,
                type=order_type,
                requested_qty=Decimal(str(resp.get("origQty", "0"))),
                executed_qty=executed_qty,
                avg_price=avg_price,
                stop_price=stop_price,
                total_fee=Decimal("0"), # 实际需从用户流获取
                fee_asset="USDT",
                transact_time=int(resp.get("updateTime", 0)),
                synthetic=False
            )
        except Exception as e:
            logger.error(f"解析币安订单响应异常: {e}, raw={resp}")
            raise InvalidOrderError(f"解析订单结果失败: {e}")

    @staticmethod
    def _format_dec(d: Decimal) -> str:
        """格式化 Decimal 为字符串，防止科学计数法，保留 6 位小数（兜底）"""
        if d is None:
            return "0"
        s = format(d, 'f')
        if '.' in s:
            s = s.rstrip('0').rstrip('.')
        return s if s else "0"


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "gateway_binance",
    "type": "gateway",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["binance_execution"],
    "module_dependencies": [],
    "owner": "lego",
    "exchange": "binance",
}

def create(config: dict) -> BinanceGateway:
    # 由 main.py 注入 rest_client
    raise NotImplementedError("BinanceGateway 需要显式注入 RestClient")
