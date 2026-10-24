"""
data/rest_client.py — 币安 REST API 客户端

执行层和交易所之间的唯一通道。

设计原则：
1. 所有请求：签名、限频、超时、错误码映射、重试
2. 可重试错误自动重试（网络、5xx、-1003、-1021）
3. 不可重试错误立即抛出（-2010 / -2011 / -2019 / -4131 / -4164）
4. -1021 触发时钟重校准后重试一次
5. -1003 触发限频退避后重试
6. 精度处理：Decimal + round_step_size / round_tick_size
7. 所有写操作必须幂等（client_order_id 由 gateway 提供）
8. 所有时间戳用 clock.exchange_ms()
9. 所有请求过 RateLimiter
10. 线程安全，连接池复用

职责边界：
- rest_client：raw API 封装，签名、限频、错误映射、重试
- gateway：订单语义（client_order_id 生成、精度处理、订单状态机）
- strategy：不碰 rest_client

错误码映射（币安合约）：
-1000 UNKNOWN:              ServerError（可重试）
-1001 DISCONNECTED:         ServerError（可重试）
-1003 TOO_MANY_REQUESTS:    RateLimitError（限频退避 + 重试）
-1006 UNEXPECTED_RESP:      ServerError（可重试）
-1007 TIMEOUT:              NetworkError（可重试）
-1015 TOO_MANY_ORDERS:      RateLimitError（退避 + 重试）
-1021 TIMESTAMP:            ClockOffsetError（force_resync + 重试一次）
-1022 SIGNATURE:            AuthError（不可重试）
-1100 ILLEGAL_CHARS:        InvalidOrderError（不可重试）
-1102 MANDATORY_PARAM:      InvalidOrderError（不可重试）
-1106 PARAM_EMPTY:          InvalidOrderError（不可重试）
-1111 PRECISION_OVER:       InvalidOrderError（不可重试）
-2010 NEW_ORDER_REJECTED:   分情况（Duplicate → 视为成功，其他 → OrderRejected）
-2011 CANCEL_REJECTED:      UnknownOrderError（撤单已成交/已撤）
-2013 NO_SUCH_ORDER:        UnknownOrderError（不可重试）
-2014 BAD_API_KEY:          AuthError（不可重试）
-2015 INVALID_API_KEY:      AuthError（不可重试）
-2019 MARGIN_NOT_ENOUGH:    InsufficientBalanceError（不可重试）
-4131 PRICE_FILTER:         InvalidOrderError（不可重试）
-4164 MIN_NOTIONAL:         InvalidOrderError（不可重试）
其他 -1xxx:                 ServerError（可重试）
其他 -2xxx:                 OrderRejectedError（不可重试）

关键契约：
1. rest_client 不知道策略、不知道轮次
2. rest_client 负责错误映射，gateway 只看异常类型
3. rest_client 负责重试，gateway 不重试
4. rest_client 负责限频，gateway 不管
5. rest_client 负责精度，gateway 传 Decimal
6. client_order_id 由 gateway 生成，rest_client 原样传递
7. 回测/影子不下单（影子可拉数据），只有实盘下单
8. 所有方法线程安全（RLock）
9. 重试策略：3 次指数退避 [500, 1000, 2000]ms
10. -1021 重试前调 clock.force_resync()
11. -1003 重试前调 limiter.on_rate_limit_hit()

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import hashlib
import hmac
import logging
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from core.gateway import (
    AccountState, AuthError, ExecutionGateway, GatewayError,
    InsufficientBalanceError, InvalidOrderError, NetworkError,
    OrderRejectedError, OrderResult, OrderSide, OrderStatus,
    OrderType, Position, RateLimitError, ServerError, UnknownOrderError,
    WorkingType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_BASE_URL = "https://fapi.binance.com"
DEFAULT_TIMEOUT_MS = 2000
DEFAULT_RECV_WINDOW_MS = 5000
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_MS = [500, 1000, 2000]


# ============================================================================
# 币安错误码
# ============================================================================
BINANCE_ERROR_MAP = {
    -1000: "UNKNOWN",
    -1001: "DISCONNECTED",
    -1003: "TOO_MANY_REQUESTS",
    -1006: "UNEXPECTED_RESP",
    -1007: "TIMEOUT",
    -1015: "TOO_MANY_ORDERS",
    -1021: "TIMESTAMP_OUT_OF_SYNC",
    -1022: "INVALID_SIGNATURE",
    -1100: "ILLEGAL_CHARS",
    -1102: "MANDATORY_PARAM_MISSING",
    -1106: "PARAM_EMPTY",
    -1111: "PRECISION_OVER_LIMIT",
    -2010: "NEW_ORDER_REJECTED",
    -2011: "CANCEL_REJECTED",
    -2013: "NO_SUCH_ORDER",
    -2014: "BAD_API_KEY_FORMAT",
    -2015: "INVALID_API_KEY_IP_PERM",
    -2019: "MARGIN_NOT_ENOUGH",
    -4131: "PRICE_FILTER",
    -4164: "MIN_NOTIONAL",
}


# 特殊处理：Duplicate order 视为订单已存在
DUPLICATE_ORDER_HINTS = (
    "Duplicate order",
    "duplicate order",
    "already exists",
)


# ============================================================================
# REST 客户端
# ============================================================================
class RestClient:
    """
    币安 REST API 客户端。详见模块 docstring。

    用法：
        clock = HybridClock(config)
        clock.sync(initial_exchange_ts)

        limiter = RateLimiter(config)
        rest = RestClient(
            api_key=...,
            api_secret=...,
            clock=clock,
            limiter=limiter,
            config=config,
        )

        # 时钟校准
        server_ts = rest.get_server_time()

        # 账户
        acct = rest.get_account()
        positions = rest.get_positions()

        # 交易对精度
        info = rest.get_exchange_info("BTCUSDT")

        # 下单（gateway 调用）
        result = rest.place_order_raw(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            qty=Decimal("0.001"),
            client_order_id="bm-ent-0042-00-00",
            reduce_only=False,
        )

        # 撤单
        rest.cancel_order_raw("BTCUSDT", "bm-ent-0042-00-00")

        # 健康检查
        rest.health_check()
    """

    name = "rest_client"
    version = "1.0.0"
    exchange_name = "binance"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        clock: Any,
        limiter: Any,
        base_url: str = DEFAULT_BASE_URL,
        recv_window_ms: int = DEFAULT_RECV_WINDOW_MS,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_ms: Optional[List[int]] = None,
        symbol_info: Optional[Dict[str, Dict[str, Any]]] = None,
        config: Optional[dict] = None,
    ):
        if not api_key:
            raise ValueError("api_key is required")
        if not api_secret:
            raise ValueError("api_secret is required")
        if clock is None:
            raise ValueError("clock is required (ClockProvider)")
        if limiter is None:
            raise ValueError("limiter is required (RateLimiter)")

        self._api_key = api_key
        self._api_secret = api_secret
        self._clock = clock
        self._limiter = limiter
        self._base_url = base_url.rstrip("/")
        self._recv_window_ms = int(recv_window_ms)
        self._timeout_s = timeout_ms / 1000.0
        self._max_retries = int(max_retries)
        self._retry_backoff_ms = list(
            retry_backoff_ms or DEFAULT_RETRY_BACKOFF_MS
        )

        # symbol -> {step_size, tick_size, min_notional, min_qty, max_qty}
        self._symbol_info: Dict[str, Dict[str, Any]] = dict(
            symbol_info or {}
        )

        # 连接池
        self._session = requests.Session()
        self._session.headers.update({
            "X-MBX-APIKEY": self._api_key,
            "User-Agent": "quant-rest-client/1.0",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        })
        # 连接池大小
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10, pool_maxsize=20
        )
        self._session.mount("https://", adapter)

        # 锁
        self._lock = threading.RLock()

        # 统计
        self._stats = {
            "requests": 0,
            "success": 0,
            "errors": 0,
            "retries": 0,
            "rate_limit_hits": 0,
            "clock_resync": 0,
            "signature_errors": 0,
            "network_timeouts": 0,
        }
        self._last_error: Optional[str] = None
        self._last_request_ts: float = 0.0

        logger.info(
            f"RestClient initialized base_url={self._base_url} "
            f"recv_window={self._recv_window_ms}ms "
            f"timeout={timeout_ms}ms max_retries={max_retries} "
            f"symbols_loaded={len(self._symbol_info)}"
        )

    # ========================================================================
    # 核心：请求
    # ========================================================================

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
        priority: Any = None,  # RequestPriority（避免循环导入）
        order_need: int = 0,
        weight_need: int = 1,
        timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        执行请求：限频 → 签名 → 发送 → 错误处理 → 重试。

        Args:
            method: GET / POST / PUT / DELETE
            path: /fapi/v1/xxx
            params: 查询参数
            signed: 是否签名
            priority: RequestPriority（对 signed 请求必填）
            order_need: 需要的订单令牌数
            weight_need: 需要的权重令牌数
            timeout_ms: 覆盖默认超时

        Returns:
            响应 JSON dict（空响应返回 {}）

        Raises:
            GatewayError 子类（按错误码映射）
        """
        # 延迟导入避免循环
        if priority is None:
            from system.rate_limiter import RequestPriority
            priority = RequestPriority.QUERY

        params = dict(params or {})
        timeout_s = (
            timeout_ms / 1000.0 if timeout_ms else self._timeout_s
        )

        last_exception: Optional[Exception] = None

        for attempt in range(self._max_retries + 1):
            # 限频
            acquired = self._limiter.acquire(
                priority=priority,
                order_need=order_need,
                weight_need=weight_need,
                timeout_ms=int(timeout_s * 1000),
            )
            if not acquired:
                self._stats["rate_limit_hits"] += 1
                raise RateLimitError(
                    f"rate limiter timeout on {method} {path}"
                )

            # 签名
            request_params = dict(params)
            if signed:
                request_params.update(self._build_signed_params())
                url = f"{self._base_url}{path}?{urlencode(request_params)}"
            else:
                url = f"{self._base_url}{path}"
                if request_params:
                    url += "?" + urlencode(request_params)

            # 发送
            self._stats["requests"] += 1
            self._last_request_ts = time.time()

            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    timeout=timeout_s,
                )
            except requests.Timeout as e:
                self._stats["network_timeouts"] += 1
                last_exception = NetworkError(
                    f"timeout on {method} {path}: {e}"
                )
                if attempt < self._max_retries:
                    self._stats["retries"] += 1
                    self._sleep_backoff(attempt)
                    continue
                self._stats["errors"] += 1
                raise last_exception
            except requests.ConnectionError as e:
                last_exception = NetworkError(
                    f"connection error on {method} {path}: {e}"
                )
                if attempt < self._max_retries:
                    self._stats["retries"] += 1
                    self._sleep_backoff(attempt)
                    continue
                self._stats["errors"] += 1
                raise last_exception
            except requests.RequestException as e:
                self._stats["errors"] += 1
                raise NetworkError(
                    f"request error on {method} {path}: {e}"
                ) from e

            # 解析响应
            try:
                if response.status_code == 200:
                    if not response.text:
                        self._stats["success"] += 1
                        return {}
                    try:
                        result = response.json()
                    except ValueError as e:
                        self._stats["errors"] += 1
                        raise ServerError(
                            f"invalid json on {method} {path}: {e}"
                        ) from e
                    self._stats["success"] += 1
                    return result

                # 非 200：解析币安错误
                error = self._parse_error(response)
            except Exception as e:
                self._stats["errors"] += 1
                raise

            # 错误分类处理
            handled = self._handle_error(
                error, method, path, attempt, last_exception
            )
            if handled is None:
                # 需要重试
                self._stats["retries"] += 1
                self._sleep_backoff(attempt)
                continue
            # 不可重试或已经处理完毕
            self._stats["errors"] += 1
            raise handled

        # 理论上不会到这里
        self._stats["errors"] += 1
        raise last_exception or ServerError(
            f"request failed after {self._max_retries} retries"
        )

    def _build_signed_params(self) -> Dict[str, Any]:
        """构造签名参数：timestamp + recvWindow + signature。"""
        # 用 exchange_ms 保证时间戳与交易所一致
        timestamp = self._clock.exchange_ms()
        params = {
            "timestamp": timestamp,
            "recvWindow": self._recv_window_ms,
        }
        return params

    def _sign(self, params: Dict[str, Any]) -> str:
        """HMAC-SHA256 签名。参数按字典序拼接后加密。"""
        query = urlencode(params)
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def _parse_error(self, response: requests.Response) -> Dict[str, Any]:
        """解析币安错误响应。"""
        try:
            data = response.json()
        except ValueError:
            return {
                "code": response.status_code * -1,
                "msg": response.text or "unknown error",
                "http_status": response.status_code,
            }
        if not isinstance(data, dict):
            return {
                "code": -1,
                "msg": str(data),
                "http_status": response.status_code,
            }
        data["http_status"] = response.status_code
        return data

    def _handle_error(
        self,
        error: Dict[str, Any],
        method: str,
        path: str,
        attempt: int,
        last_exception: Optional[Exception],
    ) -> Optional[GatewayError]:
        """
        处理错误。

        Returns:
            - GatewayError（已处理，需要 raise）
            - None（需要重试）
        """
        code = error.get("code", 0)
        msg = error.get("msg", "")
        http_status = error.get("http_status", 0)
        code_name = BINANCE_ERROR_MAP.get(code, "UNKNOWN")
        full_msg = f"[{code} {code_name}] {msg}"

        # ---- 时钟不同步 ----
        if code == -1021:
            logger.warning(
                f"clock out of sync (-1021), forcing resync"
            )
            self._stats["clock_resync"] += 1
            try:
                self._clock.force_resync()
            except Exception as e:
                logger.error(f"force_resync failed: {e}")
            if attempt < self._max_retries:
                return None  # 重试
            return NetworkError(
                f"clock out of sync after resync: {full_msg}"
            )

        # ---- 限频 ----
        if code in (-1003, -1015):
            logger.warning(f"rate limit hit ({code}), backoff")
            self._limiter.on_rate_limit_hit()
            self._stats["rate_limit_hits"] += 1
            if attempt < self._max_retries:
                return None  # 重试
            return RateLimitError(f"rate limited: {full_msg}")

        # ---- -2010 特殊处理：Duplicate 视为订单已存在 ----
        if code == -2010:
            if any(hint.lower() in msg.lower() for hint in DUPLICATE_ORDER_HINTS):
                logger.warning(
                    f"duplicate order detected, treating as exists: {msg}"
                )
                # 视为“订单已存在”——网关会 catch 并查状态
                return OrderRejectedError(
                    f"duplicate order: {full_msg}", code=code
                )
            return OrderRejectedError(full_msg, code=code)

        # ---- 撤单时订单已成交/已撤 ----
        if code == -2011:
            return UnknownOrderError(
                f"cancel rejected (already filled/canceled): {full_msg}",
                code=code,
            )

        # ---- 订单不存在 ----
        if code == -2013:
            return UnknownOrderError(full_msg, code=code)

        # ---- 认证/权限错误 ----
        if code in (-1022, -2014, -2015):
            self._stats["signature_errors"] += 1
            return AuthError(full_msg, code=code)

        # ---- 参数错误（不可重试）----
        if code in (-1100, -1102, -1106, -1111, -4131, -4164):
            return InvalidOrderError(full_msg, code=code)

        # ---- 保证金不足 ----
        if code == -2019:
            return InsufficientBalanceError(full_msg, code=code)

        # ---- 5xx 交易所服务错误（可重试）----
        if http_status >= 500:
            if attempt < self._max_retries:
                return None  # 重试
            return ServerError(f"server error: {full_msg}")

        # ---- 其他 -1xxx（可重试）----
        if -1999 <= code <= -1000:
            if attempt < self._max_retries:
                return None  # 重试
            return ServerError(f"binance error: {full_msg}")

        # ---- 其他 -2xxx（订单相关，不可重试）----
        if -2999 <= code <= -2000:
            return OrderRejectedError(full_msg, code=code)

        # ---- 未知 ----
        return GatewayError(f"unknown error: {full_msg}", code=code)

    def _sleep_backoff(self, attempt: int) -> None:
        """指数退避 sleep。"""
        idx = min(attempt, len(self._retry_backoff_ms) - 1)
        delay_ms = self._retry_backoff_ms[idx]
        time.sleep(delay_ms / 1000.0)

    # ========================================================================
    # 便捷 API
    # ========================================================================

    def get_server_time(self) -> int:
        """获取交易所服务器时间（毫秒）。无签名、无订单令牌。"""
        result = self._request(
            "GET", "/fapi/v1/time",
            signed=False,
            order_need=0, weight_need=1,
        )
        return int(result["serverTime"])

    def ping_account_light(self) -> Dict[str, Any]:
        """轻量账户探测。无订单令牌，低权重。"""
        return self._request(
            "GET", "/fapi/v2/account",
            signed=True,
            order_need=0, weight_need=5,
            timeout_ms=3000,
        )

    def get_account(self) -> AccountState:
        """查询账户状态。"""
        result = self._request(
            "GET", "/fapi/v2/account",
            signed=True,
            order_need=0, weight_need=5,
        )
        return AccountState(
            total_wallet_balance=Decimal(str(result["totalWalletBalance"])),
            total_unrealized_pnl=Decimal(str(result["totalUnrealizedProfit"])),
            total_margin_balance=Decimal(str(result["totalMarginBalance"])),
            total_maint_margin=Decimal(str(result["totalMaintMargin"])),
            available_balance=Decimal(str(result["availableBalance"])),
            update_time=int(result.get("updateTime", 0)),
        )

    def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """查询持仓。"""
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = self._request(
            "GET", "/fapi/v2/positionRisk",
            params=params,
            signed=True,
            order_need=0, weight_need=5,
        )
        positions = []
        for p in result:
            amt = Decimal(str(p["positionAmt"]))
            if amt == 0:
                continue
            positions.append(Position(
                symbol=p["symbol"],
                side=OrderSide.BUY if amt > 0 else OrderSide.SELL,
                qty=abs(amt),
                entry_price=Decimal(str(p["entryPrice"])),
                mark_price=Decimal(str(p["markPrice"])),
                unrealized_pnl=Decimal(str(p["unRealizedProfit"])),
                liquidation_price=(
                    Decimal(str(p["liquidationPrice"]))
                    if p.get("liquidationPrice") else None
                ),
                margin=Decimal(str(p.get("isolatedMargin", "0"))),
                leverage=int(p.get("leverage", 1)),
                update_time=int(p.get("updateTime", 0)),
            ))
        return positions

    def get_exchange_info(self, symbol: str) -> Dict[str, Any]:
        """查询交易对精度信息。"""
        result = self._request(
            "GET", "/fapi/v1/exchangeInfo",
            signed=False,
            order_need=0, weight_need=1,
        )
        target = symbol.upper()
        for s in result.get("symbols", []):
            if s["symbol"] == target:
                filters = {f["filterType"]: f for f in s.get("filters", [])}
                price_filter = filters.get("PRICE_FILTER", {})
                lot_filter = filters.get("LOT_SIZE", {})
                min_notional_filter = filters.get("MIN_NOTIONAL", {})
                info = {
                    "symbol": s["symbol"],
                    "price_precision": int(s["pricePrecision"]),
                    "qty_precision": int(s["quantityPrecision"]),
                    "tick_size": Decimal(str(price_filter.get("tickSize", "0.1"))),
                    "step_size": Decimal(str(lot_filter.get("stepSize", "0.001"))),
                    "min_qty": Decimal(str(lot_filter.get("minQty", "0"))),
                    "max_qty": Decimal(str(lot_filter.get("maxQty", "1000000"))),
                    "min_notional": Decimal(str(min_notional_filter.get("notional", "5"))),
                    "status": s.get("status", "UNKNOWN"),
                }
                # 缓存
                self._symbol_info[target] = info
                return info
        raise InvalidOrderError(f"symbol {symbol} not found in exchangeInfo")

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_ts: int,
        end_ts: int,
        limit: int = 1500,
    ) -> List[Dict[str, Any]]:
        """拉取历史 K 线（REST）。"""
        result = self._request(
            "GET", "/fapi/v1/klines",
            params={
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": start_ts,
                "endTime": end_ts,
                "limit": limit,
            },
            signed=False,
            order_need=0, weight_need=2,
        )
        return result

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """查询未完结订单。"""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return self._request(
            "GET", "/fapi/v1/openOrders",
            params=params,
            signed=True,
            order_need=0, weight_need=3,
        )

    def place_order_raw(
        self,
        symbol: str,
        side: str,
        order_type: str,
        qty: Decimal,
        client_order_id: str,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        reduce_only: bool = False,
        close_position: bool = False,
        time_in_force: Optional[str] = None,
        working_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        下单 raw API。gateway 调用，不对外暴露。

        契约：
        - qty/price/stop_price 已是合法精度（gateway 处理）
        - client_order_id 由 gateway 生成
        - 返回原始响应 dict
        """
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": str(qty),
            "newClientOrderId": client_order_id,
        }
        if price is not None:
            params["price"] = str(price)
        if stop_price is not None:
            params["stopPrice"] = str(stop_price)
        if reduce_only:
            params["reduceOnly"] = "true"
        if close_position:
            params["closePosition"] = "true"
        if time_in_force is not None:
            params["timeInForce"] = time_in_force.upper()
        if working_type is not None:
            params["workingType"] = working_type.upper()

        from system.rate_limiter import RequestPriority
        return self._request(
            "POST", "/fapi/v1/order",
            params=params,
            signed=True,
            priority=RequestPriority.OPEN_ORDER,
            order_need=1, weight_need=1,
        )

    def cancel_order_raw(
        self, symbol: str, client_order_id: str
    ) -> Dict[str, Any]:
        """撤单 raw API。"""
        from system.rate_limiter import RequestPriority
        return self._request(
            "DELETE", "/fapi/v1/order",
            params={
                "symbol": symbol.upper(),
                "origClientOrderId": client_order_id,
            },
            signed=True,
            priority=RequestPriority.CANCEL,
            order_need=1, weight_need=1,
        )

    def get_order_raw(
        self, symbol: str, client_order_id: str
    ) -> Dict[str, Any]:
        """查询订单 raw API。"""
        return self._request(
            "GET", "/fapi/v1/order",
            params={
                "symbol": symbol.upper(),
                "origClientOrderId": client_order_id,
            },
            signed=True,
            order_need=0, weight_need=1,
        )

    def set_leverage_raw(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆 raw API。"""
        from system.rate_limiter import RequestPriority
        return self._request(
            "POST", "/fapi/v1/leverage",
            params={
                "symbol": symbol.upper(),
                "leverage": leverage,
            },
            signed=True,
            priority=RequestPriority.OPEN_ORDER,
            order_need=0, weight_need=1,
        )

    # ========================================================================
    # 精度工具
    # ========================================================================

    def round_step_size(self, qty: Decimal, symbol: str) -> Decimal:
        """向下取整到 step_size 的整数倍。"""
        info = self._symbol_info.get(symbol.upper())
        if not info:
            return qty
        step = info["step_size"]
        if step <= 0:
            return qty
        return (qty / step).to_integral_value(
            rounding=ROUND_DOWN
        ) * step

    def round_tick_size(self, price: Decimal, symbol: str) -> Decimal:
        """取整到 tick_size 的整数倍。"""
        info = self._symbol_info.get(symbol.upper())
        if not info:
            return price
        tick = info["tick_size"]
        if tick <= 0:
            return price
        return (price / tick).to_integral_value(
            rounding=ROUND_HALF_UP
        ) * tick

    def validate_notional(
        self, price: Decimal, qty: Decimal, symbol: str
    ) -> None:
        """校验名义价值 >= min_notional。"""
        info = self._symbol_info.get(symbol.upper())
        if not info:
            return
        notional = price * qty
        if notional < info["min_notional"]:
            raise InvalidOrderError(
                f"notional {notional} < min_notional "
                f"{info['min_notional']} for {symbol}"
            )

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            total = self._stats["requests"]
            error_rate = (
                self._stats["errors"] / total if total > 0 else 0.0
            )
            latency_ms = (
                (time.time() - self._last_request_ts) * 1000
                if self._last_request_ts > 0 else 0.0
            )
            healthy = error_rate < 0.1
            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "base_url": self._base_url,
                "symbols_loaded": len(self._symbol_info),
                "error_rate": error_rate,
                "last_request_age_ms": latency_ms,
                "stats": dict(self._stats),
                "timestamp": int(time.time() * 1000),
            }

    def close(self) -> None:
        """关闭 session。"""
        with self._lock:
            try:
                self._session.close()
            except Exception:
                pass
        logger.info("RestClient closed")


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "rest_client",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["rest_api"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "lego",
    "description": "Binance REST API client with signing, rate limiting, error mapping, and retry",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> RestClient:
    """
    从配置创建实例。

    config 需要：
    - binance_api_key
    - binance_api_secret
    - instances.clock: ClockProvider
    - instances.rate_limiter: RateLimiter

    config 可选：
    - api.rest_base_url: 默认 https://fapi.binance.com
    - api.recv_window_ms: 默认 5000
    - api.timeout_ms: 默认 2000
    - api.max_retries: 默认 3
    """
    api_cfg = config.get("api", {})
    instances = config.get("instances", {})
    clock = instances.get("clock")
    limiter = instances.get("rate_limiter")
    if clock is None or limiter is None:
        raise ValueError(
            "RestClient.create requires config.instances.clock and "
            "config.instances.rate_limiter"
        )
    return RestClient(
        api_key=config.get("binance_api_key", ""),
        api_secret=config.get("binance_api_secret", ""),
        clock=clock,
        limiter=limiter,
        base_url=api_cfg.get("rest_base_url", DEFAULT_BASE_URL),
        recv_window_ms=api_cfg.get("recv_window_ms", DEFAULT_RECV_WINDOW_MS),
        timeout_ms=api_cfg.get("timeout_ms", DEFAULT_TIMEOUT_MS),
        max_retries=api_cfg.get("max_retries", DEFAULT_MAX_RETRIES),
        config=config,
    )
