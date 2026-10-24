"""
execution/order_router.py — 订单执行路由层（实盘专用）

"把订单的执行策略和实际执行解耦"。

职责（唯一）：
1. 前置校验：client_order_id 格式、订单字段类型、symbol 一致性
2. 订单类型分流：MARKET / STOP_MARKET（Step 1），LIMIT（Step 2）
3. 执行统计：每类订单的成交率、平均延迟、错误分布
4. 活跃订单跟踪：哪些订单还在途

不做什么：
- 不做精度校验（BinanceGateway 负责）
- 不做名义价值校验（BinanceGateway 负责）
- 不做单订单重试（RestClient / BinanceGateway 负责）
- 不做限价排队（Step 2）
- 不做 urgency 分层（Step 3）
- 不做风控（RoundManager 负责）
- 不做轮次记账（PositionRound 负责）
- 不持久化（RoundManager 负责）

职责边界（三层清晰）：
- RoundManager：轮次记账、风控审批、持久化
- OrderRouter：前置校验、类型分流、执行统计
- BinanceGateway：精度处理、名义价值、错误恢复、幂等缓存

为什么 OrderRouter 在 Step 1 存在？
1. 统一入口：所有实盘订单都走 submit()，便于监控和统计
2. 前置校验：尽早拒绝非法订单，避免到 gateway 才失败
3. 类型分流：MARKET / STOP_MARKET 走不同参数预处理（Step 2 扩展）
4. 执行统计：成交率、延迟分布、错误分类，回测调参用得上
5. Step 2 落脚点：限价排队、urgency 分层都要有地方放

关键契约（"五不重复"）：
1. 不重复精度校验（gateway 做）
2. 不重复名义价值校验（gateway 做）
3. 不重复单订单重试（RestClient / gateway 做）
4. 不重复幂等缓存（gateway 做）
5. 不重复风控（RoundManager 做）

关键契约（"五必有"）：
1. 必有前置校验（client_order_id 格式 / 字段类型）
2. 必有活跃订单跟踪（用于 close() 报告）
3. 必有执行统计（用于回测调参）
4. 必有延迟分桶（用于诊断慢执行）
5. 必有健康检查（用于监控）

前置校验规则：
- client_order_id: ^[A-Za-z0-9_-]{1,36}$
- order.qty: 必须是 Decimal 且 > 0
- order.price: 限价单必须 Decimal 且 > 0，市价单必须为 None
- order.stop_price: 止损单必须 Decimal 且 > 0，其他必须为 None
- order.side: 必须是 OrderSide 枚举
- order.type: 必须是 OrderType 枚举
- order.symbol: 必须匹配 config.symbol（大写）
- reduce_only + close_position 互斥

订单类型分流（Step 1）：
- MARKET: 直接调 gateway.place_order
- STOP_MARKET: 直接调 gateway.place_order
- LIMIT: 抛 UnsupportedOrderType（Step 2 实现）
- STOP / TAKE_PROFIT / TAKE_PROFIT_MARKET: 抛 UnsupportedOrderType

活跃订单生命周期：
    submit()
      → 前置校验通过
      → 加入 _active_orders
      → 调 gateway.place_order
      → 成功：
         - 如果是终结状态（FILLED / CANCELED / REJECTED / EXPIRED）→ 移除
         - 如果是 NEW / PARTIALLY_FILLED → 保留
      → 异常：
         - 移除（失败订单不跟踪）

    cancel()
      → 前置校验通过
      → 调 gateway.cancel_order
      → 成功：移除活跃订单

    后续状态更新（Step 2 加）：
    on_order_update(order_result)
      → 若状态变为终结，移除活跃订单

延迟分桶：
- 快速：[0, 10) ms
- 正常：[10, 100) ms
- 慢速：[100, 500) ms
- 极慢：[500, +∞) ms

延迟计算：gateway.place_order 返回时刻 - submit 调用时刻

与 RoundManager 的协作（Step 2 切换）：
    RoundManager._open_round_impl()
      → risk_policy.approve_order()      # 风控
      → order_router.submit(entry_order)  # OrderRouter（Step 2 切换点）
          → 前置校验
          → gateway.place_order()
          → 统计
      → 创建 PositionRound
      → order_router.submit(stop_order)
      → persister.insert_round_open()

    RoundManager.close_round()
      → order_router.cancel(stop_id)     # 撤止损
      → order_router.submit(exit_order)   # 平仓
      → PositionRound.close()
      → persister.update_round_close()

Step 1 部署说明：
- OrderRouter 在 Step 1 只在 live 模式创建（`main.py._init_execution`）
- RoundManager 在 Step 1 仍直接调 `gateway.place_order`（本文件提供接口，Step 2 切换）
- 切换成本低：把 RoundManager 里的 `self._gateway.place_order(...)` 改为 `self._order_router.submit(...)` 即可
- 本文件先建好，Step 2 直接启用

作者：乐高
审查：比蒙（策略）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from core.gateway import (
    ExecutionGateway,
    InvalidOrderError,
    Order,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "order_router",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "owner": "lego",
    "capabilities": [
        "order_routing", "order_statistics", "active_order_tracking",
    ],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Order execution router: pre-validation + type dispatch + statistics (live only)",
}


# ============================================================================
# 常量
# ============================================================================
CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]{1,36}$")

LATENCY_BUCKETS = [
    (0, 10, "fast"),
    (10, 100, "normal"),
    (100, 500, "slow"),
    (500, float("inf"), "very_slow"),
]

SUPPORTED_ORDER_TYPES = frozenset({
    OrderType.MARKET,
    OrderType.STOP_MARKET,
})

TERMINAL_STATUSES = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
})


# ============================================================================
# 错误
# ============================================================================
class ExecutionError(Exception):
    """OrderRouter 执行错误（包装底层异常）。"""

    def __init__(self, message: str, original: Optional[Exception] = None):
        super().__init__(message)
        self.original = original


class UnsupportedOrderType(ExecutionError):
    """Step 1 不支持的订单类型。"""


class OrderValidationError(ExecutionError):
    """订单前置校验失败。"""


# ============================================================================
# 活跃订单
# ============================================================================
@dataclass
class _ActiveOrder:
    """活跃订单的内部记录。"""
    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    qty: Decimal
    created_ts: int
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# OrderRouter
# ============================================================================
class OrderRouter:
    """
    订单执行路由层。详见模块 docstring。

    用法：
        router = OrderRouter(
            gateway=gateway,           # BinanceGateway
            config=config,
            clock=clock,
        )

        # 下单
        try:
            result = router.submit(order, metadata={"round_id": "..."})
            # result 是 OrderResult
        except UnsupportedOrderType as e:
            logger.error(f"unsupported: {e}")
        except OrderValidationError as e:
            logger.error(f"invalid order: {e}")
        except ExecutionError as e:
            logger.error(f"execution failed: {e}")

        # 撤单
        router.cancel("bm-ent-0001-00-00")

        # 健康检查
        router.health()
    """

    name = "order_router"
    version = "1.0.0"

    def __init__(
        self,
        gateway: ExecutionGateway,
        config: Optional[dict] = None,
        clock: Any = None,
    ):
        if gateway is None:
            raise ValueError("gateway is required")

        self._gateway = gateway
        self._config = config or {}
        self._clock = clock
        self._symbol = self._config.get("symbol", "").upper()

        self._lock = threading.RLock()
        self._active_orders: Dict[str, _ActiveOrder] = {}

        self._stats = {
            "submit_attempts": 0,
            "submit_successes": 0,
            "submit_failures": 0,
            "submit_validation_failures": 0,
            "submit_unsupported_type": 0,
            "cancel_attempts": 0,
            "cancel_successes": 0,
            "cancel_failures": 0,
            "orders_by_type": {
                OrderType.MARKET.value: 0,
                OrderType.STOP_MARKET.value: 0,
            },
            "latency_buckets": {
                "fast": 0,
                "normal": 0,
                "slow": 0,
                "very_slow": 0,
            },
            "total_latency_ms": 0.0,
        }
        self._last_error: Optional[str] = None

        # 可选回调（Step 2 启用）
        self._on_execution_event: List[Callable[[Dict[str, Any]], None]] = []

        logger.info(
            f"OrderRouter initialized "
            f"symbol={self._symbol} "
            f"gateway={self._gateway.exchange_name} "
            f"supported_types={sorted(t.value for t in SUPPORTED_ORDER_TYPES)}"
        )

    # ========================================================================
    # 主入口：submit
    # ========================================================================

    def submit(
        self,
        order: Order,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> OrderResult:
        """
        提交订单。

        契约：
        - 前置校验：非法订单抛 OrderValidationError
        - 类型分流：不支持的类型抛 UnsupportedOrderType
        - 调 gateway.place_order
        - 成功：记录活跃订单（若未终结）
        - 失败：包装成 ExecutionError 抛出，原异常在 __cause__

        Raises:
            OrderValidationError: 前置校验失败
            UnsupportedOrderType: 订单类型不支持
            ExecutionError: gateway 执行失败（原始异常在 .original）
        """
        t0 = time.perf_counter()

        with self._lock:
            self._stats["submit_attempts"] += 1

        # ---- 1. 前置校验 ----
        try:
            self._validate_order(order)
        except OrderValidationError as e:
            with self._lock:
                self._stats["submit_validation_failures"] += 1
                self._stats["submit_failures"] += 1
                self._last_error = str(e)
            logger.warning(f"order validation failed: {e}")
            raise

        # ---- 2. 类型分流 ----
        try:
            self._dispatch_order_type(order)
        except UnsupportedOrderType as e:
            with self._lock:
                self._stats["submit_unsupported_type"] += 1
                self._stats["submit_failures"] += 1
                self._last_error = str(e)
            logger.error(f"unsupported order type: {e}")
            raise

        # ---- 3. 登记活跃订单 ----
        active = _ActiveOrder(
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.type,
            qty=order.qty,
            created_ts=self._safe_now_ms(),
            metadata=dict(metadata or {}),
        )
        with self._lock:
            self._active_orders[order.client_order_id] = active

        # ---- 4. 执行 ----
        try:
            result = self._gateway.place_order(order)
        except Exception as e:
            # 失败：移除活跃订单 + 包装异常
            with self._lock:
                self._active_orders.pop(order.client_order_id, None)
                self._stats["submit_failures"] += 1
                self._last_error = f"{type(e).__name__}: {e}"
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(
                f"gateway.place_order failed: "
                f"client_order_id={order.client_order_id} "
                f"type={order.type.value} "
                f"error={type(e).__name__}: {e} "
                f"elapsed_ms={elapsed_ms:.1f}"
            )
            self._emit_event(
                event="order_failed",
                order=order,
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
            raise ExecutionError(
                f"gateway.place_order failed: {e}", original=e
            ) from e

        # ---- 5. 成功：更新活跃订单 + 统计 ----
        elapsed_ms = (time.perf_counter() - t0) * 1000

        with self._lock:
            self._stats["submit_successes"] += 1
            self._stats["total_latency_ms"] += elapsed_ms
            self._stats["orders_by_type"][order.type.value] = (
                self._stats["orders_by_type"].get(order.type.value, 0) + 1
            )
            bucket = self._latency_bucket(elapsed_ms)
            self._stats["latency_buckets"][bucket] += 1

            # 若订单未终结，保留跟踪；终结则移除
            if result.status in TERMINAL_STATUSES:
                self._active_orders.pop(order.client_order_id, None)

        # ---- 6. 日志 ----
        logger.info(
            f"order submitted: "
            f"client_order_id={order.client_order_id} "
            f"type={order.type.value} side={order.side.value} "
            f"qty={order.qty} "
            f"status={result.status.value} "
            f"executed_qty={result.executed_qty} "
            f"avg_price={result.avg_price} "
            f"elapsed_ms={elapsed_ms:.1f} "
            f"bucket={bucket}"
        )

        self._emit_event(
            event="order_submitted",
            order=order,
            result=result,
            elapsed_ms=elapsed_ms,
        )

        return result

    # ========================================================================
    # 撤单
    # ========================================================================

    def cancel(self, client_order_id: str) -> OrderResult:
        """
        撤单。

        契约：
        - 前置校验 client_order_id
        - 调 gateway.cancel_order
        - 成功：移除活跃订单
        """
        with self._lock:
            self._stats["cancel_attempts"] += 1

        # 前置校验
        self._validate_client_order_id(client_order_id)

        try:
            result = self._gateway.cancel_order(client_order_id)
        except Exception as e:
            with self._lock:
                self._stats["cancel_failures"] += 1
                self._last_error = f"{type(e).__name__}: {e}"
            logger.error(
                f"gateway.cancel_order failed: "
                f"client_order_id={client_order_id} "
                f"error={type(e).__name__}: {e}"
            )
            raise ExecutionError(
                f"gateway.cancel_order failed: {e}", original=e
            ) from e

        with self._lock:
            self._stats["cancel_successes"] += 1
            self._active_orders.pop(client_order_id, None)

        logger.info(
            f"order cancelled: client_order_id={client_order_id} "
            f"status={result.status.value}"
        )
        return result

    # ========================================================================
    # 前置校验
    # ========================================================================

    def _validate_order(self, order: Order) -> None:
        """
        前置校验：基础字段 + 类型 + symbol 一致性。

        契约：
        - 不做精度校验（gateway 做）
        - 不做名义价值校验（gateway 做）
        - 只校验必然错误的字段
        """
        if order is None:
            raise OrderValidationError("order is None")

        if not isinstance(order, Order):
            raise OrderValidationError(
                f"order must be Order, got {type(order).__name__}"
            )

        # client_order_id
        self._validate_client_order_id(order.client_order_id)

        # symbol 一致性
        if self._symbol and order.symbol.upper() != self._symbol:
            raise OrderValidationError(
                f"symbol mismatch: order={order.symbol} "
                f"config={self._symbol}"
            )

        # side
        if not isinstance(order.side, OrderSide):
            raise OrderValidationError(
                f"side must be OrderSide, got {type(order.side).__name__}"
            )

        # type
        if not isinstance(order.type, OrderType):
            raise OrderValidationError(
                f"type must be OrderType, got {type(order.type).__name__}"
            )

        # qty
        if not isinstance(order.qty, Decimal):
            raise OrderValidationError(
                f"qty must be Decimal, got {type(order.qty).__name__}"
            )
        if order.qty <= 0:
            raise OrderValidationError(
                f"qty must be > 0, got {order.qty}"
            )

        # price
        if order.type == OrderType.MARKET:
            if order.price is not None:
                raise OrderValidationError(
                    "MARKET order must not have price"
                )
        else:
            if order.price is not None and not isinstance(
                order.price, Decimal
            ):
                raise OrderValidationError(
                    f"price must be Decimal, "
                    f"got {type(order.price).__name__}"
                )

        # stop_price
        if order.type in (
            OrderType.STOP_MARKET,
            OrderType.TAKE_PROFIT_MARKET,
        ):
            if order.stop_price is None:
                raise OrderValidationError(
                    f"{order.type.value} requires stop_price"
                )
            if not isinstance(order.stop_price, Decimal):
                raise OrderValidationError(
                    f"stop_price must be Decimal, "
                    f"got {type(order.stop_price).__name__}"
                )
            if order.stop_price <= 0:
                raise OrderValidationError(
                    f"stop_price must be > 0, got {order.stop_price}"
                )

        # reduce_only + close_position 互斥
        if order.reduce_only and order.close_position:
            raise OrderValidationError(
                "reduce_only and close_position are mutually exclusive"
            )

    def _validate_client_order_id(self, client_order_id: str) -> None:
        """校验 client_order_id 格式。"""
        if not client_order_id:
            raise OrderValidationError("client_order_id is empty")
        if not isinstance(client_order_id, str):
            raise OrderValidationError(
                f"client_order_id must be str, "
                f"got {type(client_order_id).__name__}"
            )
        if not CLIENT_ORDER_ID_PATTERN.match(client_order_id):
            raise OrderValidationError(
                f"client_order_id format invalid: "
                f"{client_order_id[:20]}"
            )

    # ========================================================================
    # 类型分流
    # ========================================================================

    def _dispatch_order_type(self, order: Order) -> None:
        """
        订单类型分流。

        契约：
        - 支持的类型：MARKET / STOP_MARKET
        - 不支持的类型：抛 UnsupportedOrderType
        - 不做参数调整（Step 1）
        - Step 2 会在这里加限价单参数调整
        """
        if order.type not in SUPPORTED_ORDER_TYPES:
            raise UnsupportedOrderType(
                f"order type {order.type.value} not supported in Step 1 "
                f"(supported: "
                f"{[t.value for t in SUPPORTED_ORDER_TYPES]})"
            )

        # Step 2 扩展点：
        # - MARKET 分支：可能加 max_slippage 保护
        # - LIMIT 分支：可能设置 time_in_force、价格调整
        # - STOP_MARKET 分支：可能调整 working_type

    # ========================================================================
    # 统计辅助
    # ========================================================================

    @staticmethod
    def _latency_bucket(elapsed_ms: float) -> str:
        """判断延迟分桶。"""
        for lo, hi, name in LATENCY_BUCKETS:
            if lo <= elapsed_ms < hi:
                return name
        return "very_slow"

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        if self._clock is None:
            return int(time.time() * 1000)
        try:
            return int(self._clock.now_ms())
        except Exception:
            return int(time.time() * 1000)

    def _emit_event(self, **kwargs) -> None:
        """触发执行事件回调（Step 2 启用）。"""
        for cb in self._on_execution_event:
            try:
                cb(kwargs)
            except Exception as e:
                logger.error(
                    f"on_execution_event failed: {e}", exc_info=True
                )

    def register_execution_event_callback(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """注册执行事件回调（Step 2 使用）。"""
        self._on_execution_event.append(callback)

    # ========================================================================
    # 活跃订单查询
    # ========================================================================

    def get_active_orders(self) -> List[Dict[str, Any]]:
        """返回所有活跃订单快照。"""
        with self._lock:
            return [
                {
                    "client_order_id": a.client_order_id,
                    "symbol": a.symbol,
                    "side": a.side.value,
                    "order_type": a.order_type.value,
                    "qty": str(a.qty),
                    "created_ts": a.created_ts,
                    "metadata": dict(a.metadata),
                }
                for a in self._active_orders.values()
            ]

    def get_active_order(
        self, client_order_id: str
    ) -> Optional[Dict[str, Any]]:
        """按 client_order_id 查活跃订单。"""
        with self._lock:
            a = self._active_orders.get(client_order_id)
            if a is None:
                return None
            return {
                "client_order_id": a.client_order_id,
                "symbol": a.symbol,
                "side": a.side.value,
                "order_type": a.order_type.value,
                "qty": str(a.qty),
                "created_ts": a.created_ts,
                "metadata": dict(a.metadata),
            }

    def update_order_status(self, result: OrderResult) -> None:
        """
        更新订单状态（Step 2 使用）。

        契约：
        - 若订单终结，移除活跃订单
        - 若未终结，保持跟踪
        """
        if result is None:
            return
        if result.status in TERMINAL_STATUSES:
            with self._lock:
                self._active_orders.pop(result.client_order_id, None)

    # ========================================================================
    # 关闭与重置
    # ========================================================================

    def close(self) -> None:
        """
        关闭。报告未完成订单（供人工确认）。

        契约：
        - 打 CRITICAL 日志列出所有活跃订单
        - 清空活跃订单表
        """
        with self._lock:
            active_count = len(self._active_orders)
            if active_count > 0:
                ids = list(self._active_orders.keys())
                logger.critical(
                    f"OrderRouter closing with {active_count} "
                    f"active orders: {ids[:10]}"
                    f"{'...' if len(ids) > 10 else ''}"
                )
            self._active_orders.clear()
        logger.info("OrderRouter closed")

    def reset(self) -> None:
        """重置（测试用）。"""
        with self._lock:
            self._active_orders.clear()
            for k in self._stats:
                if k in ("orders_by_type", "latency_buckets"):
                    for sk in self._stats[k]:
                        self._stats[k][sk] = 0
                elif k == "total_latency_ms":
                    self._stats[k] = 0.0
                else:
                    self._stats[k] = 0
            self._last_error = None
        logger.info("OrderRouter reset")

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            stats = dict(self._stats)
            stats["orders_by_type"] = dict(stats["orders_by_type"])
            stats["latency_buckets"] = dict(stats["latency_buckets"])

            total_success = self._stats["submit_successes"]
            avg_latency = (
                self._stats["total_latency_ms"] / total_success
                if total_success > 0 else 0.0
            )

            return {
                "healthy": True,
                "name": self.name,
                "version": self.version,
                "symbol": self._symbol,
                "gateway": self._gateway.exchange_name,
                "active_orders_count": len(self._active_orders),
                "active_order_ids": list(
                    self._active_orders.keys()
                )[:10],
                "stats": stats,
                "avg_latency_ms": avg_latency,
                "last_error": self._last_error,
                "timestamp": self._safe_now_ms(),
            }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> OrderRouter:
    """
    模块工厂入口。

    config 需要：
    - instances.gateway: BinanceGateway
    - instances.clock: ClockProvider
    - symbol

    注意：本模块只在 live 模式创建（main.py._init_execution）。
    回测/影子模式不使用 OrderRouter（VirtualGateway 直接由 RoundManager 调用）。
    """
    instances = config.get("instances", {})
    gateway = instances.get("gateway")

    if gateway is None:
        raise ValueError(
            "OrderRouter.create requires config.instances.gateway. "
            "OrderRouter is live-only; backtest/shadow modes do not use it."
        )

    # 只接受实盘网关
    if gateway.exchange_name == "virtual":
        raise ValueError(
            "OrderRouter only supports live gateway, "
            "got virtual gateway. "
            "In backtest/shadow mode, RoundManager calls VirtualGateway directly."
        )

    return OrderRouter(
        gateway=gateway,
        config=config,
        clock=instances.get("clock"),
    )


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "execution/order_router.py")
    return MANIFEST
