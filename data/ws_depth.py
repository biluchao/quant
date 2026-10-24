"""
data/ws_depth.py — 币安部分盘口深度 WebSocket 数据流

提供部分盘口深度快照（前 N 档），供 OBI 因子使用。

关键设计决策：使用币安的部分盘口深度（`@depth20`）而不是完整盘口增量
（`@depth`）。理由：

- OBI 只需要前 10-20 档
- 部分盘口深度每条消息就是前 N 档的完整快照，无需本地重建
- 无需处理 lastUpdateId 连续性（丢一条就全崩）
- 无需 REST 同步初始快照
- 重连后无需重新同步
- 每条消息独立、完整、幂等

对比完整盘口增量（@depth）：
- 需要本地维护订单簿（bid/ask 两个 dict）
- 需要处理 lastUpdateId 连续性（U/u/pu 字段）
- 需要 REST 拉初始快照，缓存增量，找衔接点
- 重连后必须重新走一遍同步流程
- 任何一步出错，整个订单簿不可信

对于 OBI 因子，部分盘口深度足够，且大幅简化实现。

消息格式（币安部分盘口深度）：
    {
        "e": "depthUpdate",   // 事件类型
        "E": 1571889248277,   // 事件时间（毫秒）
        "T": 1571889248276,   // 交易时间
        "s": "BTCUSDT",
        "U": 157,             // 首个更新 ID（本实现不用）
        "u": 160,             // 最终更新 ID（本实现不用）
        "pu": 149,            // 前一最终 ID（本实现不用）
        "b": [["0.0024", "10"]],  // 买盘档位（降序，最优价在前）
        "a": [["0.0026", "100"]]  // 卖盘档位（升序，最优价在前）
    }

    注意：对于部分盘口深度，b/a 是前 N 档的完整状态，不是增量。
    U/u/pu 字段存在但不用于增量合并（我们只用它做日志/统计）。

快照格式（传给 KlineAuthority）：
    {
        "ts": int,                    # 交易所事件时间（E）
        "local_receive_ts": int,      # 本地接收时间
        "bids": tuple,                # ((Decimal, Decimal), ...)，降序
        "asks": tuple,                # ((Decimal, Decimal), ...)，升序
    }

关键契约：
1. on_snapshot(snapshot) 收到的是完整快照，不是增量
2. bids 降序（最优价在前），asks 升序（最优价在前）——币安原样，不做调整
3. 交叉盘口（bids[0] >= asks[0]）直接丢弃，不推送
4. qty=0 的档位防御性过滤（理论不会出现）
5. 队列满时丢弃最旧的（快照幂等，只保留最新）
6. 重连后无需重新同步（每条消息独立）
7. 消费线程 on_snapshot 抛异常不退出，继续下一条
8. 消息超时 30 秒（深度流每秒 4 次，30 秒无消息明显异常）
9. 所有方法线程安全
10. stop() 最多 1 秒响应

与 kline 流的差异：
- 无本地订单簿重建（部分盘口深度每条是完整快照）
- 无 lastUpdateId 连续性检查
- 无 REST 同步
- 队列满丢弃最旧的（不是阻塞）
- 消息超时更短（30 秒 vs 120 秒）

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
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_WS_BASE_URL = "wss://fstream.binance.com"
DEFAULT_LEVELS = 20
DEFAULT_UPDATE_SPEED_MS = 250
DEFAULT_RECONNECT_DELAY_S = 1.0
DEFAULT_MAX_RECONNECT_DELAY_S = 60.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_PING_INTERVAL_S = 20.0
DEFAULT_CONNECT_TIMEOUT_S = 10.0
DEFAULT_MESSAGE_TIMEOUT_S = 30.0
DEFAULT_MAX_CONSECUTIVE_FAILURES = 10
DEFAULT_MAX_RECONNECTS_PER_HOUR = 100
DEFAULT_QUEUE_SIZE = 200
DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024
RECV_POLL_INTERVAL_S = 1.0


# ============================================================================
# 健康状态
# ============================================================================
class StreamHealth(str, Enum):
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    FAILED = "failed"


# ============================================================================
# DepthStream
# ============================================================================
class DepthStream:
    """
    币安部分盘口深度 WebSocket 流。详见模块 docstring。

    用法：
        stream = DepthStream(
            symbol="btcusdt",
            on_snapshot=lambda snap: kline_authority.on_depth(snap),
            on_message=lambda: conn_monitor.on_message("depth"),
            on_disconnect=lambda reason: logger.warning(f"depth disconnected: {reason}"),
            config=config,
        )

        # 在单独线程中运行
        threading.Thread(target=stream.run, daemon=True).start()

        # 停机
        stream.stop()
    """

    name = "ws_depth"
    version = "1.0.0"

    def __init__(
        self,
        symbol: str,
        on_snapshot: Callable[[dict], None],
        on_message: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[str], None]] = None,
        config: Optional[dict] = None,
        websocket_factory: Optional[Callable] = None,
        levels: Optional[int] = None,
        update_speed_ms: Optional[int] = None,
    ):
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        self.on_snapshot = on_snapshot
        self.on_message = on_message
        self.on_disconnect = on_disconnect
        self._ws_factory = websocket_factory

        cfg = (config or {}).get("ws_depth", {})
        api_cfg = (config or {}).get("api", {})

        self.levels = int(levels or cfg.get("levels", DEFAULT_LEVELS))
        self.update_speed_ms = int(
            update_speed_ms
            or cfg.get("update_speed_ms", DEFAULT_UPDATE_SPEED_MS)
        )

        if self.levels not in (5, 10, 20):
            raise ValueError(
                f"levels must be 5, 10, or 20 (binance restriction), "
                f"got {self.levels}"
            )
        if self.update_speed_ms not in (100, 250, 500):
            raise ValueError(
                f"update_speed_ms must be 100, 250, or 500 "
                f"(binance restriction), got {self.update_speed_ms}"
            )

        self.ws_base_url = api_cfg.get("ws_base_url", DEFAULT_WS_BASE_URL)
        self.ws_url = (
            f"{self.ws_base_url}/ws/{self.symbol_lower}@depth{self.levels}"
            f"@{self.update_speed_ms}ms"
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

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._queue: queue.Queue = queue.Queue(maxsize=self.queue_size)
        self._shutdown_event = threading.Event()
        self._consumer_thread: Optional[threading.Thread] = None
        self._stop_ts: Optional[float] = None

        self._health: StreamHealth = StreamHealth.DISCONNECTED
        self._last_message_ts: float = 0.0
        self._consecutive_failures: int = 0
        self._reconnect_attempts: list = []

        # ---- 统计 ----
        self._stats = {
            "messages": 0,
            "snapshots_published": 0,
            "queue_drops": 0,
            "crossed_book_dropped": 0,
            "oversize_dropped": 0,
            "invalid_dropped": 0,
            "consumer_errors": 0,
            "connect_failures": 0,
        }
        self._last_error: Optional[str] = None
        self._last_disconnect_reason: Optional[str] = None

        logger.info(
            f"DepthStream initialized symbol={self.symbol} "
            f"levels={self.levels} speed={self.update_speed_ms}ms "
            f"url={self.ws_url} queue_size={self.queue_size}"
        )

    # ========================================================================
    # 主入口
    # ========================================================================

    def run(self) -> None:
        """主循环。阻塞直到 stop() 被调用。"""
        logger.info(f"DepthStream.run started symbol={self.symbol}")

        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"depth-consumer-{self.symbol_lower}",
        )
        self._consumer_thread.start()

        try:
            self._reader_loop()
        except Exception as e:
            self._last_error = str(e)
            logger.error(
                f"DepthStream reader loop crashed: {e}",
                exc_info=True,
            )
        finally:
            self._shutdown_event.set()
            if self._consumer_thread is not None:
                self._consumer_thread.join(timeout=5.0)
                if self._consumer_thread.is_alive():
                    logger.warning(
                        "DepthStream consumer did not finish within 5s"
                    )
            logger.info(f"DepthStream.run stopped symbol={self.symbol}")

    def stop(self) -> None:
        """停止流。最多 1 秒内响应。"""
        with self._lock:
            if self._stop_ts is not None:
                return
            self._stop_ts = time.time()
        self._shutdown_event.set()
        logger.info(f"DepthStream.stop requested symbol={self.symbol}")

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
                    f"DepthStream connect/read error: {e}",
                    exc_info=True,
                )

            if self._shutdown_event.is_set():
                break

            if not self._should_stop():
                delay = self._compute_reconnect_delay()
                logger.info(
                    f"DepthStream reconnecting in {delay:.1f}s "
                    f"(consecutive_failures={self._consecutive_failures})"
                )
                if self._shutdown_event.wait(timeout=delay):
                    break
            else:
                self._health = StreamHealth.FAILED
                logger.critical(
                    f"DepthStream FAILED: too many reconnects in last hour, "
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
                self._reconnect_attempts.append(time.time())

            logger.info(
                f"DepthStream connected symbol={self.symbol} "
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
                    if self._health != StreamHealth.FAILED:
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
                f"DepthStream oversize message "
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
        except json.JSONDecodeError as e:
            with self._lock:
                self._stats["invalid_dropped"] += 1
            logger.warning(f"DepthStream invalid json: {e}")
            return

        if not isinstance(msg, dict):
            return

        # 只处理 depthUpdate
        if msg.get("e") != "depthUpdate":
            return

        # 事件时间
        try:
            exchange_ts = int(msg["E"])
        except (KeyError, ValueError, TypeError):
            with self._lock:
                self._stats["invalid_dropped"] += 1
            return

        local_ts = int(time.time() * 1000)

        # 解析买卖盘
        try:
            bids_raw = msg.get("b") or msg.get("bids") or []
            asks_raw = msg.get("a") or msg.get("asks") or []
            bids = self._parse_levels(bids_raw)
            asks = self._parse_levels(asks_raw)
        except (KeyError, ValueError, InvalidOperation, TypeError) as e:
            with self._lock:
                self._stats["invalid_dropped"] += 1
            logger.warning(f"DepthStream invalid levels: {e}")
            return

        # 交叉盘口检查
        if bids and asks and bids[0][0] >= asks[0][0]:
            with self._lock:
                self._stats["crossed_book_dropped"] += 1
            logger.warning(
                f"DepthStream crossed book detected: "
                f"best_bid={bids[0][0]} >= best_ask={asks[0][0]}, "
                f"snapshot dropped"
            )
            return

        # 更新统计
        with self._lock:
            self._last_message_ts = time.time()
            self._stats["messages"] += 1

        # 通知上游
        if self.on_message is not None:
            try:
                self.on_message()
            except Exception as e:
                logger.error(
                    f"on_message callback error: {e}",
                    exc_info=True,
                )

        # 构造快照
        snapshot = {
            "ts": exchange_ts,
            "local_receive_ts": local_ts,
            "bids": bids,
            "asks": asks,
        }

        self._enqueue_snapshot(snapshot)

    def _parse_levels(
        self, levels: list
    ) -> Tuple[Tuple[Decimal, Decimal], ...]:
        """
        解析价格档位。过滤 qty=0 的档位（防御性）。

        Args:
            levels: [[price, qty], ...]

        Returns:
            ((price, qty), ...)，保持原顺序（币安原样：bids 降序，asks 升序）
        """
        result = []
        for item in levels:
            try:
                price = Decimal(str(item[0]))
                qty = Decimal(str(item[1]))
            except (IndexError, InvalidOperation, TypeError):
                continue
            if qty > 0:
                result.append((price, qty))
        return tuple(result)

    def _enqueue_snapshot(self, snapshot: dict) -> None:
        """
        快照入队。队列满时丢弃最旧的（快照幂等，只保留最新）。
        """
        try:
            self._queue.put_nowait(snapshot)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(snapshot)
                with self._lock:
                    self._stats["queue_drops"] += 1
                    dropped = self._stats["queue_drops"]
                if dropped % 100 == 1:
                    logger.warning(
                        f"DepthStream queue full, dropped oldest "
                        f"(total dropped={dropped})"
                    )
            except (queue.Empty, queue.Full):
                with self._lock:
                    self._stats["queue_drops"] += 1

    # ========================================================================
    # 消费循环
    # ========================================================================

    def _consumer_loop(self) -> None:
        """消费循环：从队列取快照 → 调 on_snapshot。"""
        while True:
            try:
                snapshot = self._queue.get(timeout=1.0)
            except queue.Empty:
                if self._shutdown_event.is_set():
                    return
                continue

            try:
                self.on_snapshot(snapshot)
                with self._lock:
                    self._stats["snapshots_published"] += 1
            except Exception as e:
                with self._lock:
                    self._stats["consumer_errors"] += 1
                logger.error(
                    f"DepthStream on_snapshot error: {e}",
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
            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "symbol": self.symbol,
                "levels": self.levels,
                "update_speed_ms": self.update_speed_ms,
                "ws_url": self.ws_url,
                "health": self._health.value,
                "last_message_age_s": message_age,
                "message_timeout_s": self.message_timeout_s,
                "queue_size": self._queue.qsize(),
                "queue_capacity": self.queue_size,
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
    "name": "ws_depth",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["orderbook_snapshot"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Binance partial depth WebSocket stream (top N levels, no local book rebuild)",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> DepthStream:
    """
    从配置创建实例。

    注意：本模块通常由 main.py 直接构造（需要注入回调），
    create() 仅用于 ModuleRegistry 加载（需要 config 提供 on_snapshot 回调）。

    config 需要：
    - symbol: 交易对
    - ws_depth.on_snapshot: Callable[[dict], None]

    config 可选：
    - ws_depth.on_message: Callable[[], None]
    - ws_depth.on_disconnect: Callable[[str], None]
    - ws_depth.levels: 5/10/20，默认 20
    - ws_depth.update_speed_ms: 100/250/500，默认 250
    - api.ws_base_url: 默认 "wss://fstream.binance.com"
    """
    ws_cfg = config.get("ws_depth", {})
    on_snapshot = ws_cfg.get("on_snapshot")
    if on_snapshot is None:
        raise ValueError(
            "DepthStream.create requires config.ws_depth.on_snapshot callback. "
            "This module is typically constructed directly from main.py, "
            "not loaded via ModuleRegistry."
        )
    return DepthStream(
        symbol=config.get("symbol", "btcusdt"),
        on_snapshot=on_snapshot,
        on_message=ws_cfg.get("on_message"),
        on_disconnect=ws_cfg.get("on_disconnect"),
        config=config,
        levels=ws_cfg.get("levels"),
        update_speed_ms=ws_cfg.get("update_speed_ms"),
    )
