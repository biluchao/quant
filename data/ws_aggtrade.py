"""
data/ws_aggtrade.py — 币安聚合成交 WebSocket 数据流

提供聚合成交（aggTrade），供 DDE / 聪明钱因子使用。

与 kline/depth 流的差异：

1. 频率最高：每秒数百条，队列满时丢弃最旧
2. on_message 节流：每秒只触发一次回调（连接监控不需要每条都触发）
3. 成交可丢：丢少量成交对 DDE 影响可控，丢多了打 WARN
4. 用 aggTrade 不用 trade：币安聚合相同价格和方向的多笔成交，数据量小
5. 提供完整成交信息：ts / trade_ts / price / qty / is_buyer_maker / trade_id

消息格式（币安 aggTrade）：
    {
        "e": "aggTrade",     // 事件类型
        "E": 123456789,      // 事件时间（毫秒）
        "s": "BNBBTC",       // 交易对
        "a": 12345,          // 聚合成交 ID
        "p": "0.001",        // 价格
        "q": "100",          // 数量
        "f": 100,            // 首笔成交 ID
        "l": 105,            // 末笔成交 ID
        "T": 123456785,      // 成交时间
        "m": true            // 买方是否是 maker
    }

is_buyer_maker 语义：
- true:  买方是 maker → 卖方主动 → 卖出压力
- false: 买方是 taker → 买方主动 → 买入压力

成交格式（传给 KlineAuthority.on_agg_trade）：
    {
        "ts": int,              # 事件时间（E）
        "trade_ts": int,        # 成交时间（T）
        "price": Decimal,       # 价格
        "qty": Decimal,         # 数量
        "is_buyer_maker": bool, # 买方是否是 maker
        "trade_id": int,        # 聚合成交 ID（用于去重）
    }

关键契约：
1. on_trade(trade) 收到的是解析后的 dict
2. is_buyer_maker=True 表示卖出压力（重要！）
3. 高频节流：on_message 每秒最多触发一次（_last_message_ts 仍然每条都更新）
4. 队列满时丢弃最旧的（保最新，统计 queue_drops）
5. 丢弃率 > 10% 打 WARN，> 30% 打 CRITICAL
6. 单条成交可以丢，但不能成片丢
7. 消息超时 30 秒（aggTrade 频率高，30 秒无消息明显异常）
8. 重连策略与 kline/depth 一致
9. 所有方法线程安全
10. stop() 最多 1 秒响应

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

import json
import logging
import queue
import threading
import time
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_WS_BASE_URL = "wss://fstream.binance.com"
DEFAULT_RECONNECT_DELAY_S = 1.0
DEFAULT_MAX_RECONNECT_DELAY_S = 60.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_PING_INTERVAL_S = 20.0
DEFAULT_CONNECT_TIMEOUT_S = 10.0
DEFAULT_MESSAGE_TIMEOUT_S = 30.0
DEFAULT_MAX_CONSECUTIVE_FAILURES = 10
DEFAULT_MAX_RECONNECTS_PER_HOUR = 100
DEFAULT_QUEUE_SIZE = 5000            # 高频流，队列大一些
DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024
DEFAULT_ON_MESSAGE_THROTTLE_S = 1.0  # on_message 节流
RECV_POLL_INTERVAL_S = 1.0

DROP_RATE_WARN = 0.10
DROP_RATE_CRITICAL = 0.30


# ============================================================================
# 健康状态
# ============================================================================
class StreamHealth(str, Enum):
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    FAILED = "failed"
    DEGRADED = "degraded"  # 队列丢弃率过高


# ============================================================================
# AggTradeStream
# ============================================================================
class AggTradeStream:
    """
    币安聚合成交 WebSocket 流。详见模块 docstring。

    用法：
        stream = AggTradeStream(
            symbol="btcusdt",
            on_trade=lambda trade: kline_authority.on_agg_trade(trade),
            on_message=lambda: conn_monitor.on_message("aggtrade"),
            on_disconnect=lambda reason: logger.warning(f"aggtrade disconnected: {reason}"),
            config=config,
        )

        # 在单独线程中运行
        threading.Thread(target=stream.run, daemon=True).start()

        # 停机
        stream.stop()
    """

    name = "ws_aggtrade"
    version = "1.0.0"

    def __init__(
        self,
        symbol: str,
        on_trade: Callable[[dict], None],
        on_message: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[str], None]] = None,
        config: Optional[dict] = None,
        websocket_factory: Optional[Callable] = None,
    ):
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        self.on_trade = on_trade
        self.on_message = on_message
        self.on_disconnect = on_disconnect
        self._ws_factory = websocket_factory

        cfg = (config or {}).get("ws_aggtrade", {})
        api_cfg = (config or {}).get("api", {})

        self.ws_base_url = api_cfg.get("ws_base_url", DEFAULT_WS_BASE_URL)
        self.ws_url = (
            f"{self.ws_base_url}/ws/{self.symbol_lower}@aggTrade"
        )

        self.reconnect_delay_s = float(
            cfg.get("reconnect_delay_s", DEFAULT_RECONNECT_DELAY_S)
        )
        self.max_reconnect_delay_s = float(
            cfg.get("max_reconnect_delay_s", DEFAULT_MAX_RECONNECT_DELAY_S)
        )
        self.backoff_multiplier = float(
            cfg.get("backoff_multiplier", DEFAULT_BACKOFF_MULTIPLIER)
        )
        self.ping_interval_s = float(
            cfg.get("ping_interval_s", DEFAULT_PING_INTERVAL_S)
        )
        self.connect_timeout_s = float(
            cfg.get("connect_timeout_s", DEFAULT_CONNECT_TIMEOUT_S)
        )
        self.message_timeout_s = float(
            cfg.get("message_timeout_s", DEFAULT_MESSAGE_TIMEOUT_S)
        )
        self.max_consecutive_failures = int(
            cfg.get("max_consecutive_failures", DEFAULT_MAX_CONSECUTIVE_FAILURES)
        )
        self.max_reconnects_per_hour = int(
            cfg.get("max_reconnects_per_hour", DEFAULT_MAX_RECONNECTS_PER_HOUR)
        )
        self.queue_size = int(cfg.get("queue_size", DEFAULT_QUEUE_SIZE))
        self.max_message_bytes = int(
            cfg.get("max_message_bytes", DEFAULT_MAX_MESSAGE_BYTES)
        )
        self.on_message_throttle_s = float(
            cfg.get("on_message_throttle_s", DEFAULT_ON_MESSAGE_THROTTLE_S)
        )

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._queue: queue.Queue = queue.Queue(maxsize=self.queue_size)
        self._shutdown_event = threading.Event()
        self._consumer_thread: Optional[threading.Thread] = None
        self._stop_ts: Optional[float] = None

        self._health: StreamHealth = StreamHealth.DISCONNECTED
        self._last_message_ts: float = 0.0
        self._last_on_message_ts: float = 0.0
        self._consecutive_failures: int = 0
        self._reconnect_attempts: list = []

        # ---- 统计 ----
        self._stats = {
            "messages": 0,
            "trades_published": 0,
            "queue_drops": 0,
            "oversize_dropped": 0,
            "invalid_dropped": 0,
            "consumer_errors": 0,
            "connect_failures": 0,
            "on_message_throttled": 0,
        }
        self._last_error: Optional[str] = None
        self._last_disconnect_reason: Optional[str] = None

        logger.info(
            f"AggTradeStream initialized symbol={self.symbol} "
            f"url={self.ws_url} queue_size={self.queue_size} "
            f"on_message_throttle={self.on_message_throttle_s}s"
        )

    # ========================================================================
    # 主入口
    # ========================================================================

    def run(self) -> None:
        """主循环。阻塞直到 stop() 被调用。"""
        logger.info(f"AggTradeStream.run started symbol={self.symbol}")

        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"aggtrade-consumer-{self.symbol_lower}",
        )
        self._consumer_thread.start()

        try:
            self._reader_loop()
        except Exception as e:
            self._last_error = str(e)
            logger.error(
                f"AggTradeStream reader loop crashed: {e}",
                exc_info=True,
            )
        finally:
            self._shutdown_event.set()
            if self._consumer_thread is not None:
                self._consumer_thread.join(timeout=5.0)
                if self._consumer_thread.is_alive():
                    logger.warning(
                        "AggTradeStream consumer did not finish within 5s"
                    )
            logger.info(f"AggTradeStream.run stopped symbol={self.symbol}")

    def stop(self) -> None:
        """停止流。最多 1 秒内响应。"""
        with self._lock:
            if self._stop_ts is not None:
                return
            self._stop_ts = time.time()
        self._shutdown_event.set()
        logger.info(f"AggTradeStream.stop requested symbol={self.symbol}")

    # ========================================================================
    # 读循环
    # ========================================================================

    def _reader_loop(self) -> None:
        """读循环：连接 → 读 → 断开 → 重连。"""
        while not self._shutdown_event.is_set():
            try:
                self._connect_and_read()
            except Exception as e:
                self._last_error = str(e)
                logger.error(
                    f"AggTradeStream connect/read error: {e}",
                    exc_info=True,
                )

            if self._shutdown_event.is_set():
                break

            if not self._should_stop():
                delay = self._compute_reconnect_delay()
                logger.info(
                    f"AggTradeStream reconnecting in {delay:.1f}s "
                    f"(consecutive_failures={self._consecutive_failures})"
                )
                if self._shutdown_event.wait(timeout=delay):
                    break
            else:
                self._health = StreamHealth.FAILED
                logger.critical(
                    f"AggTradeStream FAILED: too many reconnects in last hour, "
                    f"manual restart required"
                )
                break

    def _connect_and_read(self) -> None:
        """连接 + 读取。断开时返回。"""
        with self._lock:
            self._health = StreamHealth.CONNECTING

        conn = None
        connected = False

        try:
            conn = self._open_connection()
            connected = True

            with self._lock:
                self._health = StreamHealth.HEALTHY
                self._consecutive_failures = 0
                self._last_message_ts = time.time()
                self._last_on_message_ts = time.time()
                self._reconnect_attempts.append(time.time())

            logger.info(
                f"AggTradeStream connected symbol={self.symbol} "
                f"url={self.ws_url}"
            )

            while not self._shutdown_event.is_set():
                try:
                    raw = conn.recv(timeout=RECV_POLL_INTERVAL_S)
                except TimeoutError:
                    with self._lock:
                        last_ts = self._last_message_ts
                    if (
                        last_ts > 0
                        and time.time() - last_ts > self.message_timeout_s
                    ):
                        raise ConnectionError(
                            f"no message in {self.message_timeout_s}s"
                        )
                    continue

                if raw is None:
                    raise ConnectionError("connection closed by server")

                self._handle_raw_message(raw)

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

            if connected:
                with self._lock:
                    if self._health not in (StreamHealth.FAILED, StreamHealth.DEGRADED):
                        self._health = StreamHealth.DISCONNECTED
                    self._last_disconnect_reason = (
                        self._last_error or "connection_lost"
                    )

                if self.on_disconnect is not None:
                    try:
                        self.on_disconnect(
                            self._last_disconnect_reason or "connection_lost"
                        )
                    except Exception as e:
                        logger.error(
                            f"on_disconnect callback error: {e}",
                            exc_info=True,
                        )
            else:
                with self._lock:
                    self._consecutive_failures += 1
                    self._stats["connect_failures"] += 1
                    if self._health != StreamHealth.FAILED:
                        self._health = StreamHealth.DISCONNECTED

    def _open_connection(self) -> Any:
        """打开 WebSocket 连接。支持测试注入 websocket_factory。"""
        if self._ws_factory is not None:
            return self._ws_factory(
                self.ws_url,
                open_timeout=self.connect_timeout_s,
                ping_interval=self.ping_interval_s,
            )

        try:
            from websockets.sync.client import connect as ws_connect
        except ImportError as e:
            raise RuntimeError(
                "websockets>=12.0 required for sync client. "
                "Install with: pip install 'websockets>=12.0'"
            ) from e

        return ws_connect(
            self.ws_url,
            open_timeout=self.connect_timeout_s,
            ping_interval=self.ping_interval_s,
            ping_timeout=10,
            max_size=self.max_message_bytes,
        )

    # ========================================================================
    # 消息处理
    # ========================================================================

    def _handle_raw_message(self, raw: Any) -> None:
        """处理原始消息：解析 → 转换 → 入队。"""
        # 大小检查
        if isinstance(raw, (str, bytes)) and len(raw) > self.max_message_bytes:
            with self._lock:
                self._stats["oversize_dropped"] += 1
            logger.warning(
                f"AggTradeStream oversize message "
                f"({len(raw)} > {self.max_message_bytes})"
            )
            return

        # bytes 解码
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                with self._lock:
                    self._stats["invalid_dropped"] += 1
                return

        if not raw:
            return

        # JSON 解析
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            with self._lock:
                self._stats["invalid_dropped"] += 1
            return

        if not isinstance(msg, dict):
            return

        # 只处理 aggTrade
        if msg.get("e") != "aggTrade":
            return

        # 解析必需字段
        try:
            event_ts = int(msg["E"])
            trade_ts = int(msg["T"])
            price = Decimal(str(msg["p"]))
            qty = Decimal(str(msg["q"]))
            is_buyer_maker = bool(msg["m"])
            trade_id = int(msg["a"])
        except (KeyError, ValueError, InvalidOperation, TypeError):
            with self._lock:
                self._stats["invalid_dropped"] += 1
            return

        # 价格、数量必须 > 0
        if price <= 0 or qty <= 0:
            with self._lock:
                self._stats["invalid_dropped"] += 1
            return

        local_ts = int(time.time() * 1000)

        with self._lock:
            self._last_message_ts = time.time()
            self._stats["messages"] += 1

        # 节流触发 on_message
        self._maybe_trigger_on_message()

        # 构造成交
        trade = {
            "ts": event_ts,
            "trade_ts": trade_ts,
            "price": price,
            "qty": qty,
            "is_buyer_maker": is_buyer_maker,
            "trade_id": trade_id,
            "local_receive_ts": local_ts,
        }

        self._enqueue_trade(trade)

    def _maybe_trigger_on_message(self) -> None:
        """
        节流触发 on_message 回调。

        契约：
        - 距上次触发超过 on_message_throttle_s 才触发
        - 触发后更新 _last_on_message_ts
        - 节流不更新 _last_message_ts（超时判断用 _last_message_ts）
        """
        if self.on_message is None:
            return

        now = time.time()
        with self._lock:
            if now - self._last_on_message_ts < self.on_message_throttle_s:
                self._stats["on_message_throttled"] += 1
                return
            self._last_on_message_ts = now

        try:
            self.on_message()
        except Exception as e:
            logger.error(
                f"on_message callback error: {e}",
                exc_info=True,
            )

    def _enqueue_trade(self, trade: dict) -> None:
        """
        成交入队。队列满时丢弃最旧的（保最新）。
        """
        try:
            self._queue.put_nowait(trade)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(trade)
                with self._lock:
                    self._stats["queue_drops"] += 1
                    self._check_drop_rate()
            except (queue.Empty, queue.Full):
                with self._lock:
                    self._stats["queue_drops"] += 1

    def _check_drop_rate(self) -> None:
        """检查丢弃率，超过阈值升级健康状态。"""
        total = self._stats["messages"] + self._stats["queue_drops"]
        if total < 100:
            return
        drop_rate = self._stats["queue_drops"] / total

        if drop_rate >= DROP_RATE_CRITICAL:
            if self._health != StreamHealth.DEGRADED:
                self._health = StreamHealth.DEGRADED
                logger.critical(
                    f"AggTradeStream DEGRADED: drop_rate={drop_rate:.1%} "
                    f"(> {DROP_RATE_CRITICAL:.0%})"
                )
        elif drop_rate >= DROP_RATE_WARN:
            dropped = self._stats["queue_drops"]
            if dropped % 1000 == 1:
                logger.warning(
                    f"AggTradeStream drop rate warning: "
                    f"{drop_rate:.1%} (> {DROP_RATE_WARN:.0%})"
                )

    # ========================================================================
    # 消费循环
    # ========================================================================

    def _consumer_loop(self) -> None:
        """消费循环：从队列取成交 → 调 on_trade。"""
        while True:
            try:
                trade = self._queue.get(timeout=1.0)
            except queue.Empty:
                if self._shutdown_event.is_set():
                    return
                continue

            try:
                self.on_trade(trade)
                with self._lock:
                    self._stats["trades_published"] += 1
            except Exception as e:
                with self._lock:
                    self._stats["consumer_errors"] += 1
                logger.error(
                    f"AggTradeStream on_trade error: {e}",
                    exc_info=True,
                )
                # 不退出，继续下一条

    # ========================================================================
    # 重连控制
    # ========================================================================

    def _compute_reconnect_delay(self) -> float:
        """指数退避。"""
        if self._consecutive_failures <= 0:
            return self.reconnect_delay_s
        delay = self.reconnect_delay_s * (
            self.backoff_multiplier ** self._consecutive_failures
        )
        return min(delay, self.max_reconnect_delay_s)

    def _should_stop(self) -> bool:
        """是否应停止重连（超过每小时上限）。"""
        if self._shutdown_event.is_set():
            return True
        now = time.time()
        cutoff = now - 3600.0
        with self._lock:
            self._reconnect_attempts = [
                t for t in self._reconnect_attempts if t > cutoff
            ]
            return (
                len(self._reconnect_attempts)
                >= self.max_reconnects_per_hour
            )

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            now = time.time()
            message_age = (
                now - self._last_message_ts
                if self._last_message_ts > 0 else None
            )
            healthy = (
                self._health == StreamHealth.HEALTHY
                and message_age is not None
                and message_age < self.message_timeout_s
            )
            reconnect_window = [
                t for t in self._reconnect_attempts
                if t > now - 3600.0
            ]
            total = (
                self._stats["messages"] + self._stats["queue_drops"]
            )
            drop_rate = (
                self._stats["queue_drops"] / total if total > 0 else 0.0
            )
            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "symbol": self.symbol,
                "ws_url": self.ws_url,
                "health": self._health.value,
                "last_message_age_s": message_age,
                "message_timeout_s": self.message_timeout_s,
                "queue_size": self._queue.qsize(),
                "queue_capacity": self.queue_size,
                "drop_rate": drop_rate,
                "drop_rate_warn": DROP_RATE_WARN,
                "drop_rate_critical": DROP_RATE_CRITICAL,
                "consecutive_failures": self._consecutive_failures,
                "reconnects_last_hour": len(reconnect_window),
                "max_reconnects_per_hour": self.max_reconnects_per_hour,
                "last_disconnect_reason": self._last_disconnect_reason,
                "last_error": self._last_error,
                "stats": dict(self._stats),
                "stopped": self._stop_ts is not None,
                "timestamp": int(now * 1000),
            }


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "ws_aggtrade",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["trade_stream"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Binance aggTrade WebSocket stream with throttled on_message and drop protection",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> AggTradeStream:
    """
    从配置创建实例。

    注意：本模块通常由 main.py 直接构造（需要注入回调），
    create() 仅用于 ModuleRegistry 加载（需要 config 提供 on_trade 回调）。

    config 需要：
    - symbol: 交易对
    - ws_aggtrade.on_trade: Callable[[dict], None]

    config 可选：
    - ws_aggtrade.on_message: Callable[[], None]
    - ws_aggtrade.on_disconnect: Callable[[str], None]
    - ws_aggtrade.on_message_throttle_s: 默认 1.0
    - api.ws_base_url: 默认 "wss://fstream.binance.com"
    """
    ws_cfg = config.get("ws_aggtrade", {})
    on_trade = ws_cfg.get("on_trade")
    if on_trade is None:
        raise ValueError(
            "AggTradeStream.create requires config.ws_aggtrade.on_trade callback. "
            "This module is typically constructed directly from main.py, "
            "not loaded via ModuleRegistry."
        )
    return AggTradeStream(
        symbol=config.get("symbol", "btcusdt"),
        on_trade=on_trade,
        on_message=ws_cfg.get("on_message"),
        on_disconnect=ws_cfg.get("on_disconnect"),
        config=config,
    )
