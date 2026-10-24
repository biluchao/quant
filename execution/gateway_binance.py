"""
execution/gateway_binance.py — 币安实盘网关

执行层最危险的文件。它把 Order 变成币安的一条真实订单，并回传 OrderResult。

职责（唯一）：
1. 接收 Order，做精度处理、名义价值校验
2. 调用 RestClient.place_order_raw 下单
3. 处理 -2010 Duplicate / NetworkError / -2011 三类特殊异常
4. 解析币安响应为 OrderResult
5. 幂等缓存（避免重复下单）

不做什么：
- 不做风控决策（RoundManager 负责）
- 不做策略判断（StateMachine 负责）
- 不做轮次管理（RoundManager 负责）
- 不持久化（RoundManager 负责）
- 不等待异步成交（MARKET 单通常是 FILLED）

职责边界：
- BinanceGateway：接收 Order → 调 RestClient → 处理异常 → 返回 OrderResult
- RestClient：HTTP、签名、限频、错误映射、重试
- RoundManager：风控、记账、持久化、回调
- StateMachine：决策

关键契约（"四保险"）：
1. 幂等缓存：_recent_orders[client_order_id] 命中直接返回
2. -2010 Duplicate：识别 message，查 get_order_raw 返回原订单
3. NetworkError：查 get_order_raw 确认是否已下，查到返回，查不到抛出
4. -2011 撤单时已成交：查 get_order_raw 返回当前状态

关键契约（"三校验"）：
1. 精度校验：qty 必须 round_step_size，price/stop_price 必须 round_tick_size
2. 名义价值校验：notional >= min_notional
3. 状态校验：response.status == REJECTED 时主动抛 OrderRejectedError

订单状态映射（币安 → 我方）：
NEW → NEW
PARTIALLY_FILLED → PARTIALLY_FILLED
FILLED → FILLED
CANCELED → CANCELED
REJECTED → REJECTED
EXPIRED → EXPIRED
EXPIRED_IN_MATCH → EXPIRED
其他 → UNKNOWN

返回字段说明：
- requested_qty：实际发送给交易所的数量（已 round）
- executed_qty：交易所返回的成交数量
- avg_price：交易所返回的成交均价，缺失时用 cumQuote/cumQty
- request_price：LIMIT 单的挂单价，MARKET/STOP_MARKET 为 None
- stop_price：STOP_MARKET 单的触发价
- fills：Step 1 不填充（MARKET 单一次成交），Step 2 从用户流填充
- transact_time：交易所的 updateTime
- exchange_order_id：币安的 orderId（str）

与 RoundManager 的协作：
    RoundManager._open_round_impl()
      → risk_policy.approve_order()      # 风控
      → gateway.place_order(entry_order) # 本文件
          → rest_client.place_order_raw()
          → 处理响应/异常
          → 返回 OrderResult
      → 检查 OrderResult.status
      → 创建 PositionRound
      → gateway.place_order(stop_order)
      → persister.insert_round_open()

    RoundManager.close_round()
      → gateway.cancel_order(stop_id)    # 撤止损
      → gateway.place_order(exit_order)  # 平仓
      → PositionRound.close()
      → persister.update_round_close()

    emergency_stop 路径
      → gateway.close_all_positions()
          → 逐个 close_position()

作者：乐高
审查：比蒙（策略）、张三（系统）
版本：v1.0.0
"""

from collections import OrderedDict
from decimal import Decimal, InvalidOperation
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.gateway import (
    ExecutionGateway,
    Fill,
    InvalidOrderError,
    Order,
    OrderRejectedError,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    NetworkError,
    Position,
    AccountState,
    UnknownOrderError,
    WorkingType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "gateway_binance",
    "type": "gateway",
    "version": "1.0.0",
    "step": 1,
    "owner": "lego",
    "exchange": "binance",
    "capabilities": [
        "place_order", "cancel_order", "close_position",
        "server_side_stop", "set_leverage", "get_positions",
    ],
    "module_dependencies": [],
    "fallback": "gateway_virtual",
    "enabled": True,
    "description": "Binance futures execution gateway with idempotency cache and error recovery",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_ORDER_CACHE_SIZE = 1000
DUPLICATE_ORDER_HINTS = (
    "duplicate order",
    "duplicate",
    "already exists",
)


# ============================================================================
# BinanceGateway
# ============================================================================
class BinanceGateway(ExecutionGateway):
    """
    币安合约执行网关。详见模块 docstring。

    用法：
        gateway = BinanceGateway(
            rest_client=rest,
            clock=clock,
            id_generator=id_gen,
            symbol="BTCUSDT",
            config=config,
            order_cache_size=1000,
        )

        result = gateway.place_order(order)
        if result.status == OrderStatus.FILLED:
            logger.info(f"filled at {result.avg_price}")

        gateway.cancel_order("bm-ent-0001-00-00")

        gateway.close_all_positions(reason="emergency")

        health = gateway.health_check()
    """

    exchange_name = "binance"
    version = "1.0.0"

    def __init__(
        self,
        rest_client: Any,
        clock: Any,
        id_generator: Any,
        symbol: str,
        config: Optional[dict] = None,
        order_cache_size: int = DEFAULT_ORDER_CACHE_SIZE,
    ):
        super().__init__(config or {})

        if rest_client is None:
            raise ValueError("rest_client is required")
        if clock is None:
            raise ValueError("clock is required")
        if not symbol:
            raise ValueError("symbol is required")

        self._rest = rest_client
        self._clock = clock
        self._id_gen = id_generator
        self._symbol = symbol.upper()
        self._config = config or {}
        self._cache_size = int(order_cache_size)

        # ---- 幂等缓存 ----
        # OrderedDict 用于按插入顺序淘汰
        self._recent_orders: "OrderedDict[str, OrderResult]" = OrderedDict()

        # ---- 线程锁 ----
        self._lock = threading.RLock()

        # ---- 统计 ----
        self._stats = {
            "place_attempts": 0,
            "place_successes": 0,
            "place_duplicates": 0,
            "place_network_recovered": 0,
            "place_rejected": 0,
            "place_failures": 0,
            "cancel_attempts": 0,
            "cancel_successes": 0,
            "cancel_recovered": 0,
            "cancel_failures": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_evictions": 0,
            "close_position_attempts": 0,
            "close_position_successes": 0,
            "close_position_failures": 0,
            "close_all_attempts": 0,
            "close_all_partial_failures": 0,
            "server_stop_attempts": 0,
            "server_stop_successes": 0,
            "server_stop_failures": 0,
            "errors": 0,
        }
        self._last_error: Optional[str] = None

        logger.info(
            f"BinanceGateway initialized "
            f"symbol={self._symbol} "
            f"cache_size={self._cache_size}"
        )

    # ========================================================================
    # 写操作：place_order
    # ========================================================================

    def place_order(self, order: Order) -> OrderResult:
        """
        下单。

        契约：
        - 幂等：同一 client_order_id 重复调用返回缓存结果
        - 精度处理：qty → round_step_size，price/stop_price → round_tick_size
        - 名义价值校验：notional >= min_notional
        - 异常处理：-2010 Duplicate 视为已存在，NetworkError 查状态
        - status=REJECTED 主动抛 OrderRejectedError

        Raises：
            InvalidOrderError：qty/price 精度处理失败，或名义价值过小
            OrderRejectedError：币安拒单（非 duplicate）
            NetworkError：网络异常且查不到原订单
            InsufficientBalanceError：余额不足
            AuthError：签名/权限错误
        """
        # 1. 幂等缓存查
        with self._lock:
            cached = self._lookup_cache_locked(order.client_order_id)
            if cached is not None:
                self._stats["cache_hits"] += 1
                self._stats["place_duplicates"] += 1
                logger.debug(
                    f"place_order hit cache: "
                    f"client_order_id={order.client_order_id} "
                    f"status={cached.status.value}"
                )
                return cached
            self._stats["cache_misses"] += 1
            self._stats["place_attempts"] += 1

        # 2. 委托实现
        try:
            result = self._place_order_impl(order)
        except Exception as e:
            with self._lock:
                self._stats["place_failures"] += 1
                self._last_error = str(e)
            raise

        # 3. 缓存
        with self._lock:
            self._cache_order_locked(result)
            self._stats["place_successes"] += 1

        return result

    def _place_order_impl(self, order: Order) -> OrderResult:
        """place_order 的实际实现（无缓存逻辑）。"""
        # 1. 精度处理
        try:
            rounded_qty = self._rest.round_step_size(
                order.qty, order.symbol
            )
        except Exception as e:
            raise InvalidOrderError(
                f"round_step_size failed for {order.symbol}: {e}"
            ) from e

        if rounded_qty <= 0:
            raise InvalidOrderError(
                f"rounded qty is 0 for {order.symbol} "
                f"(original qty={order.qty})"
            )

        rounded_price: Optional[Decimal] = None
        if order.price is not None:
            try:
                rounded_price = self._rest.round_tick_size(
                    order.price, order.symbol
                )
            except Exception as e:
                raise InvalidOrderError(
                    f"round_tick_size failed for price: {e}"
                ) from e
            if rounded_price <= 0:
                raise InvalidOrderError(
                    f"rounded price is 0 (original={order.price})"
                )

        rounded_stop: Optional[Decimal] = None
        if order.stop_price is not None:
            try:
                rounded_stop = self._rest.round_tick_size(
                    order.stop_price, order.symbol
                )
            except Exception as e:
                raise InvalidOrderError(
                    f"round_tick_size failed for stop_price: {e}"
                ) from e
            if rounded_stop <= 0:
                raise InvalidOrderError(
                    f"rounded stop_price is 0 "
                    f"(original={order.stop_price})"
                )

        # 2. 名义价值校验
        ref_price = (
            rounded_price or rounded_stop
            or self._get_mark_price(order.symbol)
        )
        if ref_price is not None and ref_price > 0:
            try:
                self._rest.validate_notional(
                    ref_price, rounded_qty, order.symbol
                )
            except InvalidOrderError:
                raise
            except Exception as e:
                logger.warning(
                    f"validate_notional failed (proceeding): {e}"
                )

        # 3. 构造 rest 参数
        time_in_force = None
        if (
            order.time_in_force is not None
            and order.type == OrderType.LIMIT
        ):
            time_in_force = order.time_in_force.value

        working_type = None
        if order.working_type is not None:
            working_type = order.working_type.value
        elif order.type in (
            OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET,
        ):
            working_type = WorkingType.MARK_PRICE.value

        # 4. 调用 RestClient
        try:
            raw = self._rest.place_order_raw(
                symbol=order.symbol,
                side=order.side.value,
                order_type=order.type.value,
                qty=rounded_qty,
                client_order_id=order.client_order_id,
                price=rounded_price,
                stop_price=rounded_stop,
                reduce_only=order.reduce_only,
                close_position=order.close_position,
                time_in_force=time_in_force,
                working_type=working_type,
            )
        except OrderRejectedError as e:
            # -2010 可能是 Duplicate
            if self._is_duplicate_error(e):
                logger.warning(
                    f"duplicate order detected: "
                    f"client_order_id={order.client_order_id}, "
                    f"looking up original"
                )
                with self._lock:
                    self._stats["place_duplicates"] += 1
                try:
                    raw = self._rest.get_order_raw(
                        order.symbol, order.client_order_id
                    )
                except UnknownOrderError:
                    logger.error(
                        f"duplicate error but order not found: "
                        f"client_order_id={order.client_order_id}"
                    )
                    raise OrderRejectedError(
                        f"duplicate reported but order not found "
                        f"for {order.client_order_id}"
                    ) from e
                except Exception as lookup_e:
                    logger.error(
                        f"duplicate error, lookup failed: {lookup_e}"
                    )
                    raise OrderRejectedError(
                        f"duplicate reported, lookup failed: {lookup_e}"
                    ) from e
            else:
                with self._lock:
                    self._stats["place_rejected"] += 1
                raise
        except NetworkError as e:
            # 网络超时：查订单状态
            logger.warning(
                f"place_order network error, checking order status: "
                f"client_order_id={order.client_order_id}, error={e}"
            )
            try:
                raw = self._rest.get_order_raw(
                    order.symbol, order.client_order_id
                )
                with self._lock:
                    self._stats["place_network_recovered"] += 1
                logger.warning(
                    f"order actually placed despite network error: "
                    f"client_order_id={order.client_order_id}"
                )
            except UnknownOrderError:
                # 订单确实没下成功
                logger.warning(
                    f"order confirmed not placed: "
                    f"client_order_id={order.client_order_id}"
                )
                raise e
            except Exception as lookup_e:
                logger.error(
                    f"network error, lookup failed: {lookup_e}"
                )
                raise e

        # 5. 解析响应
        result = self._parse_order_response(
            raw,
            requested_qty=rounded_qty,
            requested_price=rounded_price,
            requested_stop=rounded_stop,
        )

        # 6. status=REJECTED 主动抛
        if result.status == OrderStatus.REJECTED:
            with self._lock:
                self._stats["place_rejected"] += 1
            raise OrderRejectedError(
                f"order rejected: {result.reject_reason}",
                code=result.reject_code,
            )

        # 7. 市价单 avgPrice 缺失兜底
        if (
            order.type == OrderType.MARKET
            and result.avg_price is None
            and result.executed_qty > 0
        ):
            try:
                raw2 = self._rest.get_order_raw(
                    order.symbol, order.client_order_id
                )
                result = self._parse_order_response(
                    raw2,
                    requested_qty=rounded_qty,
                    requested_price=rounded_price,
                    requested_stop=rounded_stop,
                )
            except Exception as lookup_e:
                logger.warning(
                    f"avgPrice lookup failed: {lookup_e}, "
                    f"using partial result"
                )

        return result

    # ========================================================================
    # 写操作：cancel_order
    # ========================================================================

    def cancel_order(self, client_order_id: str) -> OrderResult:
        """
        撤单。

        契约：
        - 成功：返回 CANCELED 状态
        - -2011（订单已成交/已撤）：查状态返回
        - 网络错误：抛出
        """
        with self._lock:
            self._stats["cancel_attempts"] += 1

        try:
            raw = self._rest.cancel_order_raw(
                self._symbol, client_order_id
            )
        except UnknownOrderError as e:
            # -2011 撤单时订单已成交/已撤
            logger.warning(
                f"cancel returned unknown order, looking up state: "
                f"client_order_id={client_order_id}"
            )
            try:
                raw = self._rest.get_order_raw(
                    self._symbol, client_order_id
                )
                with self._lock:
                    self._stats["cancel_recovered"] += 1
            except Exception as lookup_e:
                logger.error(
                    f"cancel -2011 and lookup failed: {lookup_e}"
                )
                with self._lock:
                    self._stats["cancel_failures"] += 1
                raise UnknownOrderError(
                    f"cancel failed and order lookup also failed: "
                    f"{lookup_e}"
                ) from e
        except Exception as e:
            with self._lock:
                self._stats["cancel_failures"] += 1
                self._last_error = str(e)
            raise

        result = self._parse_order_response(raw)

        with self._lock:
            self._cache_order_locked(result)
            self._stats["cancel_successes"] += 1

        return result

    def cancel_all_orders(
        self, symbol: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        撤掉所有未完结订单。

        Returns:
            {"canceled": int, "failed": List[{"client_order_id": str, "error": str}]}
        """
        target = (symbol or self._symbol).upper()
        canceled = 0
        failed: List[Dict[str, str]] = []

        try:
            open_orders = self.get_open_orders(target)
        except Exception as e:
            logger.error(f"cancel_all_orders: get_open_orders failed: {e}")
            return {
                "canceled": 0,
                "failed": [{"client_order_id": "*", "error": str(e)}],
            }

        for o in open_orders:
            try:
                self.cancel_order(o.client_order_id)
                canceled += 1
            except Exception as e:
                logger.warning(
                    f"cancel {o.client_order_id} failed: {e}"
                )
                failed.append({
                    "client_order_id": o.client_order_id,
                    "error": str(e),
                })

        return {"canceled": canceled, "failed": failed}

    # ========================================================================
    # 读操作：订单查询
    # ========================================================================

    def get_order(self, client_order_id: str) -> OrderResult:
        """
        查询订单。

        契约：
        - 优先查交易所
        - UnknownOrderError 时查缓存兜底
        """
        try:
            raw = self._rest.get_order_raw(
                self._symbol, client_order_id
            )
        except UnknownOrderError:
            with self._lock:
                cached = self._lookup_cache_locked(client_order_id)
            if cached is not None:
                return cached
            raise

        result = self._parse_order_response(raw)
        with self._lock:
            self._cache_order_locked(result)
        return result

    def get_open_orders(
        self, symbol: Optional[str] = None
    ) -> List[OrderResult]:
        """查询所有未完结订单。"""
        target = (symbol or self._symbol).upper()
        try:
            raws = self._rest.get_open_orders(target)
        except Exception as e:
            logger.error(f"get_open_orders failed: {e}")
            return []

        results = []
        for raw in raws or []:
            try:
                results.append(self._parse_order_response(raw))
            except Exception as e:
                logger.warning(f"parse open order failed: {e}")
        return results

    # ========================================================================
    # 读操作：账户与持仓
    # ========================================================================

    def get_positions(self) -> List[Position]:
        """查询所有持仓。"""
        try:
            return self._rest.get_positions()
        except Exception as e:
            logger.error(f"get_positions failed: {e}")
            raise

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        查询单个交易对持仓。

        覆盖基类默认实现：直接用 rest_client.get_positions(symbol)
        避免查询所有交易对。
        """
        try:
            positions = self._rest.get_positions(symbol)
        except Exception as e:
            logger.error(f"get_position({symbol}) failed: {e}")
            raise

        for p in positions:
            if p.symbol == symbol.upper():
                return p
        return None

    def get_account_state(self) -> AccountState:
        """查询账户状态。"""
        try:
            return self._rest.get_account()
        except Exception as e:
            logger.error(f"get_account_state failed: {e}")
            raise

    # ========================================================================
    # 写操作：平仓
    # ========================================================================

    def close_position(
        self, symbol: str, reason: str = "manual"
    ) -> Optional[OrderResult]:
        """
        市价平掉指定交易对的全部持仓。

        契约：
        - 无持仓返回 None（幂等）
        - 有持仓：反向市价单 reduce_only=True
        - 失败抛异常

        Raises：
            InvalidOrderError: 持仓量 round 后为 0
            OrderRejectedError: 币安拒单
            NetworkError: 网络异常
        """
        with self._lock:
            self._stats["close_position_attempts"] += 1

        try:
            pos = self.get_position(symbol)
        except Exception as e:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise

        if pos is None or pos.qty <= 0:
            logger.info(
                f"close_position({symbol}): no position, skip"
            )
            return None

        # 反向
        close_side = (
            OrderSide.SELL if pos.side == OrderSide.BUY
            else OrderSide.BUY
        )

        # 精度
        try:
            rounded_qty = self._rest.round_step_size(
                pos.qty, symbol
            )
        except Exception as e:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise InvalidOrderError(
                f"round_step_size failed: {e}"
            ) from e

        if rounded_qty <= 0:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise InvalidOrderError(
                f"close qty rounded to 0 (original={pos.qty})"
            )

        # 生成 client_order_id
        if self._id_gen is None:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise InvalidOrderError(
                "close_position requires id_generator"
            )

        try:
            from system.order_idempotent import OrderKind
            client_id = self._id_gen.generate(
                OrderKind.CLOSE_ALL, 0, role_seq=0, attempt=0
            )
        except Exception as e:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise InvalidOrderError(
                f"generate close order id failed: {e}"
            ) from e

        # 构造订单
        try:
            order = Order(
                client_order_id=client_id,
                symbol=symbol.upper(),
                side=close_side,
                type=OrderType.MARKET,
                qty=rounded_qty,
                reduce_only=True,
            )
        except Exception as e:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise InvalidOrderError(
                f"construct close order failed: {e}"
            ) from e

        try:
            result = self.place_order(order)
        except Exception as e:
            with self._lock:
                self._stats["close_position_failures"] += 1
            raise

        with self._lock:
            self._stats["close_position_successes"] += 1

        logger.info(
            f"close_position({symbol}): reason={reason} "
            f"qty={rounded_qty} side={close_side.value} "
            f"status={result.status.value} "
            f"client_order_id={client_id}"
        )

        return result

    def close_all_positions(
        self, reason: str = "manual"
    ) -> List[OrderResult]:
        """平掉所有持仓。逐个平仓，失败不阻断。"""
        with self._lock:
            self._stats["close_all_attempts"] += 1

        results = []
        try:
            positions = self.get_positions()
        except Exception as e:
            logger.error(
                f"close_all_positions: get_positions failed: {e}"
            )
            return results

        for pos in positions:
            if pos.qty <= 0:
                continue
            try:
                r = self.close_position(pos.symbol, reason=reason)
                if r is not None:
                    results.append(r)
            except Exception as e:
                logger.error(
                    f"close_all_positions: close {pos.symbol} "
                    f"failed: {e}"
                )
                with self._lock:
                    self._stats["close_all_partial_failures"] += 1

        return results

    # ========================================================================
    # 写操作：服务器端止损
    # ========================================================================

    def ensure_server_side_stop(
        self,
        symbol: str,
        stop_price: Decimal,
        side: OrderSide,
        qty: Decimal,
        client_order_id: str,
    ) -> OrderResult:
        """
        挂服务器端止损单。

        契约：
        - 类型：STOP_MARKET
        - 触发：MARK_PRICE
        - reduce_only=True
        - 幂等：client_order_id 相同返回缓存
        """
        with self._lock:
            self._stats["server_stop_attempts"] += 1

        # 幂等缓存
        with self._lock:
            cached = self._lookup_cache_locked(client_order_id)
            if cached is not None:
                self._stats["cache_hits"] += 1
                self._stats["server_stop_successes"] += 1
                return cached

        # 精度处理
        try:
            rounded_stop = self._rest.round_tick_size(
                stop_price, symbol
            )
            rounded_qty = self._rest.round_step_size(qty, symbol)
        except Exception as e:
            with self._lock:
                self._stats["server_stop_failures"] += 1
            raise InvalidOrderError(
                f"round for server stop failed: {e}"
            ) from e

        if rounded_stop <= 0 or rounded_qty <= 0:
            with self._lock:
                self._stats["server_stop_failures"] += 1
            raise InvalidOrderError(
                f"server stop rounded to 0: stop={rounded_stop} "
                f"qty={rounded_qty}"
            )

        # 构造 STOP_MARKET 订单
        try:
            order = Order(
                client_order_id=client_order_id,
                symbol=symbol.upper(),
                side=side,
                type=OrderType.STOP_MARKET,
                qty=rounded_qty,
                stop_price=rounded_stop,
                reduce_only=True,
                working_type=WorkingType.MARK_PRICE,
            )
        except Exception as e:
            with self._lock:
                self._stats["server_stop_failures"] += 1
            raise InvalidOrderError(
                f"construct server stop order failed: {e}"
            ) from e

        try:
            result = self.place_order(order)
        except Exception as e:
            with self._lock:
                self._stats["server_stop_failures"] += 1
            raise

        with self._lock:
            self._stats["server_stop_successes"] += 1

        return result

    def cancel_server_side_stop(
        self, client_order_id: str
    ) -> OrderResult:
        """撤服务器端止损单。"""
        return self.cancel_order(client_order_id)

    # ========================================================================
    # 写操作：杠杆
    # ========================================================================

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        设置杠杆。

        契约：
        - 幂等：重复设置相同杠杆不报错
        - 失败返回 False（不抛出）
        """
        try:
            self._rest.set_leverage_raw(symbol, leverage)
            logger.info(
                f"set_leverage: symbol={symbol} leverage={leverage}"
            )
            return True
        except Exception as e:
            logger.error(
                f"set_leverage failed: symbol={symbol} "
                f"leverage={leverage} error={e}"
            )
            return False

    # ========================================================================
    # 解析响应
    # ========================================================================

    def _parse_order_response(
        self,
        raw: dict,
        *,
        requested_qty: Optional[Decimal] = None,
        requested_price: Optional[Decimal] = None,
        requested_stop: Optional[Decimal] = None,
    ) -> OrderResult:
        """
        解析币安订单响应。

        契约：
        - requested_qty 缺失时用 raw["origQty"]
        - avg_price 缺失时用 cumQuote / cumQty
        - status 映射为枚举
        """
        # ---- 基础字段 ----
        client_order_id = str(raw.get("clientOrderId", ""))
        exchange_order_id = str(raw.get("orderId", ""))
        symbol = str(raw.get("symbol", self._symbol)).upper()

        # ---- status ----
        status_raw = str(raw.get("status", "UNKNOWN")).upper()
        status = self._map_status(status_raw)

        # ---- side / type ----
        side = self._map_side(str(raw.get("side", "BUY")))
        order_type = self._map_type(
            str(raw.get("origType", raw.get("type", "MARKET")))
        )

        # ---- 数量与价格 ----
        if requested_qty is None:
            try:
                requested_qty = Decimal(
                    str(raw.get("origQty", "0"))
                )
            except (InvalidOperation, ValueError):
                requested_qty = Decimal("0")

        executed_qty_str = raw.get("executedQty", "0")
        try:
            executed_qty = Decimal(str(executed_qty_str))
        except (InvalidOperation, ValueError):
            executed_qty = Decimal("0")

        # ---- avg_price ----
        avg_price: Optional[Decimal] = None
        avg_price_str = raw.get("avgPrice", "0")
        try:
            avg_price_dec = Decimal(str(avg_price_str))
        except (InvalidOperation, ValueError):
            avg_price_dec = Decimal("0")

        if avg_price_dec > 0:
            avg_price = avg_price_dec
        else:
            # 用 cumQuote / cumQty 算
            cum_qty_str = raw.get("cumQty", raw.get("executedQty", "0"))
            cum_quote_str = raw.get("cumQuote", raw.get("cummulativeQuoteQty", "0"))
            try:
                cum_qty = Decimal(str(cum_qty_str))
                cum_quote = Decimal(str(cum_quote_str))
                if cum_qty > 0 and cum_quote > 0:
                    avg_price = cum_quote / cum_qty
            except (InvalidOperation, ValueError):
                pass

        # ---- request_price ----
        request_price = requested_price
        if request_price is None:
            price_str = raw.get("price", "0")
            try:
                price_dec = Decimal(str(price_str))
                if price_dec > 0:
                    request_price = price_dec
            except (InvalidOperation, ValueError):
                pass

        # ---- stop_price ----
        stop_price = requested_stop
        if stop_price is None:
            stop_str = raw.get("stopPrice", "0")
            try:
                stop_dec = Decimal(str(stop_str))
                if stop_dec > 0:
                    stop_price = stop_dec
            except (InvalidOperation, ValueError):
                pass

        # ---- 时间 ----
        try:
            update_time = int(
                raw.get("updateTime", raw.get("time", 0))
            )
        except (ValueError, TypeError):
            update_time = 0

        # ---- reject 字段 ----
        reject_reason = None
        reject_code = None
        if status == OrderStatus.REJECTED:
            reject_reason = str(raw.get("msg", "unknown"))

        # ---- 构造 OrderResult ----
        return OrderResult(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            symbol=symbol,
            status=status,
            side=side,
            type=order_type,
            requested_qty=requested_qty,
            executed_qty=executed_qty,
            avg_price=avg_price,
            request_price=request_price,
            stop_price=stop_price,
            fills=[],
            total_fee=Decimal("0"),
            fee_asset="USDT",
            transact_time=update_time,
            update_time=update_time,
            reject_reason=reject_reason,
            reject_code=reject_code,
            synthetic=False,
        )

    # ========================================================================
    # 映射辅助
    # ========================================================================

    @staticmethod
    def _map_status(binance_status: str) -> OrderStatus:
        """币安状态 → 我方 OrderStatus。"""
        mapping = {
            "NEW": OrderStatus.NEW,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
            "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
        }
        return mapping.get(binance_status, OrderStatus.UNKNOWN)

    @staticmethod
    def _map_side(binance_side: str) -> OrderSide:
        """币安 side → 我方 OrderSide。"""
        if binance_side.upper() == "BUY":
            return OrderSide.BUY
        return OrderSide.SELL

    @staticmethod
    def _map_type(binance_type: str) -> OrderType:
        """币安 type → 我方 OrderType。"""
        mapping = {
            "MARKET": OrderType.MARKET,
            "LIMIT": OrderType.LIMIT,
            "STOP_MARKET": OrderType.STOP_MARKET,
            "TAKE_PROFIT_MARKET": OrderType.TAKE_PROFIT_MARKET,
            "STOP": OrderType.STOP,
            "TAKE_PROFIT": OrderType.TAKE_PROFIT,
        }
        return mapping.get(
            binance_type.upper(), OrderType.MARKET
        )

    @staticmethod
    def _is_duplicate_error(error: Exception) -> bool:
        """判断 -2010 是否是 duplicate error。"""
        msg = str(error).lower()
        return any(h in msg for h in DUPLICATE_ORDER_HINTS)

    # ========================================================================
    # 缓存辅助
    # ========================================================================

    def _cache_order_locked(self, result: OrderResult) -> None:
        """缓存订单（调用者必须持锁）。"""
        if not result.client_order_id:
            return
        self._recent_orders[result.client_order_id] = result
        self._recent_orders.move_to_end(result.client_order_id)

        while len(self._recent_orders) > self._cache_size:
            evicted_id, _ = self._recent_orders.popitem(last=False)
            self._stats["cache_evictions"] += 1
            logger.debug(
                f"cache evicted: client_order_id={evicted_id}"
            )

    def _lookup_cache_locked(
        self, client_order_id: str
    ) -> Optional[OrderResult]:
        """查缓存（调用者必须持锁）。"""
        if not client_order_id:
            return None
        return self._recent_orders.get(client_order_id)

    def _lookup_cache(
        self, client_order_id: str
    ) -> Optional[OrderResult]:
        """线程安全地查缓存。"""
        with self._lock:
            return self._lookup_cache_locked(client_order_id)

    # ========================================================================
    # 辅助
    # ========================================================================

    def _get_mark_price(self, symbol: str) -> Optional[Decimal]:
        """获取标记价格（用于名义价值校验）。"""
        try:
            raw = self._rest.get_positions(symbol)
            for p in raw:
                if p.symbol == symbol.upper():
                    if p.mark_price > 0:
                        return p.mark_price
        except Exception as e:
            logger.debug(f"get_mark_price failed: {e}")
        return None

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            cache_size = len(self._recent_orders)
            stats = dict(self._stats)

        rest_health: Dict[str, Any] = {}
        try:
            rest_health = self._rest.health_check()
        except Exception as e:
            rest_health = {"healthy": False, "error": str(e)}

        total_attempts = (
            stats["place_attempts"] + stats["cache_hits"]
        )
        cache_hit_rate = (
            stats["cache_hits"] / total_attempts
            if total_attempts > 0 else 0.0
        )

        return {
            "healthy": rest_health.get("healthy", False),
            "name": self.name,
            "version": self.version,
            "exchange": self.exchange_name,
            "symbol": self._symbol,
            "cache_size": cache_size,
            "cache_capacity": self._cache_size,
            "cache_hit_rate": cache_hit_rate,
            "rest_client": rest_health,
            "stats": stats,
            "last_error": self._last_error,
            "timestamp": int(time.time() * 1000),
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> BinanceGateway:
    """
    模块工厂入口。

    config 需要：
    - instances.rest_client
    - instances.clock
    - instances.order_idempotent_gen
    - symbol

    config 可选：
    - gateway.order_cache_size: 默认 1000
    """
    instances = config.get("instances", {})
    gateway_cfg = config.get("gateway", {})

    return BinanceGateway(
        rest_client=instances.get("rest_client"),
        clock=instances.get("clock"),
        id_generator=instances.get("order_idempotent_gen"),
        symbol=config.get("symbol", "btcusdt"),
        config=config,
        order_cache_size=int(
            gateway_cfg.get("order_cache_size", DEFAULT_ORDER_CACHE_SIZE)
        ),
    )


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "execution/gateway_binance.py")
    return MANIFEST
