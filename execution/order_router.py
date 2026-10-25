"""
execution/order_router.py — 订单路由与前置拦截器 (重铸标准版)

核心设计原则：
1. 统一闸门：所有订单（实盘/虚拟）必须经由 Router 进入网关。
2. 严格安检：拦截任何精度错误、类型错误、方向缺失的非法订单。
3. 统筹限频：强制接入限频器，防止被交易所封禁。
4. 审计留痕：所有成功或失败的请求，必须落入事件日志，用于 TCA 分析。
"""

import logging
import time
from decimal import Decimal
from typing import Any, Dict

from core.gateway import (
    ExecutionGateway,
    InvalidOrderError,
    Order,
    OrderResult,
    OrderSide,
    OrderType,
    GatewayError,
)

logger = logging.getLogger(__name__)

class OrderRouterError(Exception):
    """路由层异常基类"""
    pass

class OrderRouter:
    name = "order_router"
    version = "1.0.0"

    def __init__(
        self,
        gateway: ExecutionGateway,
        rate_limiter: Any,
        persister: Any,
        config: dict
    ):
        # 强依赖注入
        if not gateway or not rate_limiter or not persister:
            raise ValueError("OrderRouter 缺少必要依赖 (gateway, rate_limiter, persister)")

        self.gateway = gateway
        self.rate_limiter = rate_limiter
        self.persister = persister
        self.config = config
        
        # 提取限频权重配置（假设每单默认消耗 1 权重）
        self.weight_per_order = int(config.get("rate_limit", {}).get("weight_per_order", 1))

    # ========================================================================
    # 核心路由逻辑
    # ========================================================================

    def route_order(self, order: Order) -> OrderResult:
        """
        核心路由：校验 -> 限频 -> 执行 -> 审计
        """
        t0 = time.perf_counter()
        
        # 1. 严格安检 (Fail-Fast)
        self._validate_order(order)
        
        # 2. 获取令牌（限频）
        if not self.rate_limiter.acquire(weight=self.weight_per_order):
            self._audit(order, "REJECTED", "rate_limit_timeout", t0)
            raise OrderRouterError(f"限频拒绝: 无法获取令牌")

        # 3. 转发至网关执行
        try:
            result = self.gateway.place_order(order)
            latency_ms = (time.perf_counter() - t0) * 1000
            
            # 4. 记录成交或状态
            status_str = result.status.value if hasattr(result.status, "value") else str(result.status)
            self._audit(order, status_str, f"latency={latency_ms:.1f}ms", t0)
            
            logger.info(f"订单路由成功: {order.client_order_id} | 状态: {status_str} | 耗时: {latency_ms:.1f}ms")
            return result
            
        except GatewayError as e:
            latency_ms = (time.perf_counter() - t0) * 1000
            self._audit(order, "ERROR", f"gateway_error={str(e)}", t0)
            logger.error(f"订单网关异常 [{order.client_order_id}]: {e} | 耗时: {latency_ms:.1f}ms")
            raise
            
        except Exception as e:
            latency_ms = (time.perf_counter() - t0) * 1000
            self._audit(order, "ERROR", f"unknown_error={str(e)}", t0)
            logger.critical(f"订单路由未知异常 [{order.client_order_id}]: {e}", exc_info=True)
            raise

    def route_cancel(self, client_order_id: str) -> OrderResult:
        """
        路由撤单请求。同样受限频控制。
        """
        t0 = time.perf_counter()
        
        if not client_order_id:
            raise InvalidOrderError("client_order_id 不能为空")

        if not self.rate_limiter.acquire(weight=self.weight_per_order):
            raise OrderRouterError("撤单限频拒绝")

        try:
            result = self.gateway.cancel_order(client_order_id)
            latency_ms = (time.perf_counter() - t0) * 1000
            logger.info(f"撤单路由成功: {client_order_id} | 耗时: {latency_ms:.1f}ms")
            return result
        except Exception as e:
            logger.error(f"撤单网关异常 [{client_order_id}]: {e}")
            raise

    # ========================================================================
    # 内部辅助
    # ========================================================================

    def _validate_order(self, order: Order) -> None:
        """
        严苛的强类型与逻辑校验，拦截一切低级错误。
        """
        # 1. 基础类型
        if not isinstance(order.qty, Decimal) or order.qty <= 0:
            raise TypeError(f"路由拦截: qty 必须为合法的 Decimal，当前为 {type(order.qty)}")
            
        if order.price is not None and not isinstance(order.price, Decimal):
            raise TypeError(f"路由拦截: price 必须为 Decimal，当前为 {type(order.price)}")

        # 2. 方向与类型匹配
        if order.side not in (OrderSide.BUY, OrderSide.SELL):
            raise InvalidOrderError(f"非法方向: {order.side}")

        if order.type == OrderType.LIMIT and order.price is None:
            raise InvalidOrderError("限价单必须提供 price")

        # 3. 币安规则保护 (极其粗略的防呆，具体精度校验在网关层)
        if order.qty < Decimal("0.0001"):
            raise InvalidOrderError(f"qty 极小，可能触发交易所精度拒单: {order.qty}")

    def _audit(self, order: Order, status: str, detail: str, t0: float) -> None:
        """
        将路由行为写入持久化事件中，用于后期 TCA (交易成本分析)。
        """
        if not self.persister:
            return
            
        latency_ms = (time.perf_counter() - t0) * 1000
        try:
            self.persister.write_event(
                event_type="order_routed",
                severity="INFO" if status in ("NEW", "FILLED", "PARTIALLY_FILLED", "CANCELED") else "WARN",
                data={
                    "client_order_id": order.client_order_id,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "type": order.type.value,
                    "qty": str(order.qty),
                    "status": status,
                    "latency_ms": round(latency_ms, 2),
                    "detail": detail
                }
            )
        except Exception as e:
            logger.error(f"路由审计日志写入失败: {e}")


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "order_router",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["order_routing", "rate_limiting_integration"],
    "module_dependencies": [],
    "owner": "lego",
}

def create(config: dict) -> OrderRouter:
    # 实际实例化由 main.py 完成，需注入 gateway 和 rate_limiter
    raise NotImplementedError("OrderRouter 需要显式注入 gateway 和 rate_limiter")
