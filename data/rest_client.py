"""
data/rest_client.py — 币安 REST API 客户端 (重铸标准版)

核心设计原则：
1. 安全边界：公有接口与私有接口严格分离。缺少 API Key 时，私有接口直接抛异常拒绝启动。
2. 签名准确性：强制使用注入的系统时钟校准时间戳，杜绝因时区或漂移导致的 -1021 错误。
3. 智能重试：对网络超时、5xx 进行指数退避重试；对业务错误（如余额不足、签名错误）绝不重试。
4. 异常精准映射：将币安繁杂的错误码映射为系统标准的异常类型。
"""

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from core.gateway import (
    AccountState,
    AuthError,
    GatewayError,
    InsufficientBalanceError,
    InvalidOrderError,
    NetworkError,
    OrderRejectedError,
    Position,
    RateLimitError,
    ServerError,
    UnknownOrderError,
    OrderSide,
)

logger = logging.getLogger(__name__)

# 币安错误码映射
BINANCE_ERROR_MAP = {
    -1000: "UNKNOWN",
    -1001: "DISCONNECTED",
    -1003: "TOO_MANY_REQUESTS",
    -1021: "TIMESTAMP_OUT_OF_SYNC",
    -1022: "INVALID_SIGNATURE",
    -2010: "NEW_ORDER_REJECTED",
    -2011: "CANCEL_REJECTED",
    -2013: "NO_SUCH_ORDER",
    -2014: "BAD_API_KEY_FORMAT",
    -2015: "INVALID_API_KEY_IP_PERM",
    -2019: "MARGIN_NOT_ENOUGH",
    -4131: "PRICE_FILTER",
}

class RestClient:
    def __init__(self, api_key: str, api_secret: str, clock: Any, limiter: Any, config: dict):
        # 严苛依赖注入校验
        if not clock or not limiter:
            raise ValueError("RestClient 必须注入 clock 和 limiter")

        self.api_key = api_key
        self.api_secret = api_secret
        self.clock = clock
        self.limiter = limiter

        self.base_url = config.get("rest_base_url", "https://fapi.binance.com")
        self.timeout = config.get("timeout_ms", 2000) / 1000.0
        self.recv_window = config.get("recv_window_ms", 5000)

        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "QuantSystem/1.0",
        })
        if self.api_key:
            self._session.headers.update({"X-MBX-APIKEY": self.api_key})

    # ========================================================================
    # 核心请求路由
    # ========================================================================

    def public_request(self, method: str, path: str, params: dict = None, weight: int = 1) -> dict:
        """执行无需签名的公有请求（如拉取 K 线、时间）"""
        return self._request(method, path, params, signed=False, weight=weight)

    def private_request(self, method: str, path: str, params: dict = None, weight: int = 1) -> dict:
        """执行私有签名请求（如账户、下单）"""
        if not self.api_key or not self.api_secret:
            raise AuthError(f"私有接口 {path} 请求失败: 缺少 API Key 或 Secret")
        return self._request(method, path, params, signed=True, weight=weight)

    def _request(self, method: str, path: str, params: Optional[dict], signed: bool, weight: int, retries: int = 3) -> dict:
        params = params or {}
        
        # 序列化 Decimal 为 str，防止精度丢失
        safe_params = {k: str(v) if isinstance(v, Decimal) else v for k, v in params.items()}
        
        if signed:
            safe_params["timestamp"] = self.clock.exchange_ms()
            safe_params["recvWindow"] = self.recv_window
            query_string = urlencode(safe_params)
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            safe_params["signature"] = signature

        url = f"{self.base_url}{path}"

        for attempt in range(retries):
            # 请求前先扣减限频权重
            self.limiter.acquire(weight=weight)
            
            try:
                resp = self._session.request(method, url, params=safe_params, timeout=self.timeout)
                
                # 成功返回
                if resp.status_code == 200:
                    return resp.json()
                    
                # 解析错误
                err_data = resp.json() if resp.text else {}
                err_code = err_data.get("code", 0)
                err_msg = err_data.get("msg", resp.text)
                
                self._handle_error(err_code, err_msg, resp.status_code)
                
            except requests.exceptions.Timeout:
                logger.warning(f"网络超时: {path} (尝试 {attempt+1}/{retries})")
                if attempt == retries - 1:
                    raise NetworkError(f"请求超时: {path}")
                time.sleep(1.0 * (attempt + 1))
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"网络异常: {path} - {e} (尝试 {attempt+1}/{retries})")
                if attempt == retries - 1:
                    raise NetworkError(f"网络异常: {e}")
                time.sleep(1.0 * (attempt + 1))
                
        raise ServerError(f"请求最终失败: {path}")

    def _handle_error(self, code: int, msg: str, http_status: int):
        """将币安错误码映射为系统标准异常"""
        full_msg = f"[{code} {BINANCE_ERROR_MAP.get(code, 'UNKNOWN')}] {msg}"
        
        if code == -1003:
            raise RateLimitError(full_msg)
        elif code == -1021:
            # 时钟不同步，触发强制重校准（由上层捕捉处理）
            raise NetworkError(full_msg)
        elif code in (-1022, -2014, -2015):
            raise AuthError(full_msg)
        elif code == -2019:
            raise InsufficientBalanceError(full_msg)
        elif code in (-2010, -2011, -2013):
            raise OrderRejectedError(full_msg)
        elif code in (-4131, -1100, -1102, -1106, -1111):
            raise InvalidOrderError(full_msg)
        elif http_status >= 500:
            raise ServerError(full_msg)
        else:
            raise GatewayError(full_msg, code)

    # ========================================================================
    # 业务封装方法
    # ========================================================================

    def get_server_time(self) -> int:
        """获取服务器时间"""
        return self.public_request("GET", "/fapi/v1/time")["serverTime"]

    def get_klines(self, symbol: str, interval: str, start_ts: int, end_ts: int, limit: int = 500) -> list:
        """获取历史 K 线"""
        return self.public_request("GET", "/fapi/v1/klines", params={
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": limit
        })

    def get_account(self) -> AccountState:
        """获取账户状态"""
        data = self.private_request("GET", "/fapi/v2/account", weight=5)
        return AccountState(
            total_wallet_balance=Decimal(data["totalWalletBalance"]),
            total_unrealized_pnl=Decimal(data["totalUnrealizedProfit"]),
            total_margin_balance=Decimal(data["totalMarginBalance"]),
            total_maint_margin=Decimal(data["totalMaintMargin"]),
            available_balance=Decimal(data["availableBalance"]),
            update_time=data["updateTime"],
        )

    def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """获取当前持仓"""
        params = {"symbol": symbol.upper()} if symbol else {}
        data = self.private_request("GET", "/fapi/v2/positionRisk", params=params, weight=5)
        
        positions = []
        for p in data:
            amt = Decimal(p["positionAmt"])
            if amt == 0:
                continue
            positions.append(Position(
                symbol=p["symbol"],
                side=OrderSide.BUY if amt > 0 else OrderSide.SELL,
                qty=abs(amt),
                entry_price=Decimal(p["entryPrice"]),
                mark_price=Decimal(p["markPrice"]),
                unrealized_pnl=Decimal(p["unRealizedProfit"]),
                liquidation_price=Decimal(p["liquidationPrice"]) if p["liquidationPrice"] else None,
                margin=Decimal(p["isolatedMargin"]),
                leverage=int(p["leverage"]),
                update_time=int(p["updateTime"]),
            ))
        return positions

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "rest_client",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["rest_api"],
    "module_dependencies": [],
    "owner": "lego",
}

def create(config: dict) -> RestClient:
    # 实际由 main.py 注入实例
    raise NotImplementedError("RestClient 需要显式注入 clock 和 limiter 实例")
