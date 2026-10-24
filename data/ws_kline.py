"""
data/ws_kline.py — 币安 K 线 WebSocket 数据流

这条流是系统里唯一 24×7 不断的东西。它必须：
1. 自动重连（指数退避 + 每小时上限）
2. 读/算分离（读循环不阻塞在回调上）
3. 优雅关闭（排空队列，只处理已收线）
4. 断线可感知（on_disconnect 回调，上游触发补拉）

架构：
    读循环线程                消费线程
    ─────────                ─────────
    recv() → 解析 → 入队  →  出队 → on_bar(msg)
                                     ↓
                              KlineAuthority.on_exchange_kline

    队列满策略：
    - 已收线（x=true）：阻塞等待（不能丢）
    - 未收线（x=false）：丢弃（幂等，下次更新会覆盖）

    已收线阻塞到队列满 → 抛异常 → 强制重连 → on_disconnect → main.py 补拉

关键契约：
1. on_bar(msg) 收到的是原始币安消息字典（含 k.t / k.o / k.x 等）
2. on_message() 每条消息触发一次（上游用于连接监控）
3. on_disconnect(reason) 每次断开触发一次（上游用于补拉触发）
4. 已连接后断开才触发 on_disconnect，连接失败不触发
5. 空消息、二进制非 utf-8、非 kline 消息 → 静默丢弃
6. 消息超过 max_message_bytes → 丢弃并 WARN
7. 消费线程 on_bar 抛异常 → 打 ERROR，不退出，继续下一条
8. 重连超过 max_reconnects_per_hour → 标记 FAILED，不再重连，等外部重启
9. 所有方法线程安全
10. stop() 最多 1 秒内响应，最多 drain_timeout_s 内完成排空

重连策略：
- 指数退避：1s → 2s → 4s → 8s → … → 上限 60s
- 成功连接重置退避
- 每小时最多 max_reconnects_per_hour 次（默认 100）
- 超限后停止重连，health=FAILED

消息超时：
- message_timeout_s 内未收到任何消息，视为断开
- 币安 K 线流 250ms 一拍，正常不会超时
- 超时视为假死，主动断开重连

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

import json
import logging
import queue
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_WS_BASE_URL = "wss://fstream.binance.com"
DEFAULT_INTERVAL = "3m"
DEFAULT_RECONNECT_DELAY_S = 1.0
DEFAULT_MAX_RECONNECT_DELAY_S = 60.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_PING_INTERVAL_S = 20.0
DEFAULT_CONNECT_TIMEOUT_S = 10.0
DEFAULT_MESSAGE_TIMEOUT_S = 120.0
DEFAULT_MAX_CONSECUTIVE_FAILURES = 10
DEFAULT_MAX_RECONNECTS_PER_HOUR = 100
DEFAULT_QUEUE_SIZE = 1000
DEFAULT_DRAIN_TIMEOUT_S = 10.0
DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024
RECV_POLL_INTERVAL_S = 1.0
CLOSED_BAR_PUT_TIMEOUT_S = 5.0


# ============================================================================
# 健康状态
# ============================================================================
class StreamHealth(str, Enum):
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    FAILED = "failed"


# ============================================================================
# KlineStream
# ============================================================================
class KlineStream:
    """
    币安 K 线 WebSocket 流。详见模块 docstring。

    用法：
        stream = KlineStream(
            symbol="btcusdt",
            on_bar=lambda msg: kline_authority.on_exchange_kline(msg),
            on_message=lambda: conn_monitor.on_message("kline"),
            on_disconnect=lambda reason: logger.warning(f"kline disconnected: {reason}"),
            config=config,
        )

        # 在单独线程中运行
        threading.Thread(target=stream.run, daemon=True).start()

        # 停机
        stream.stop()
    """

    name = "ws_kline"
    version = "1.0.0"

    def __init__(
        self,
        symbol: str,
        on_bar: Callable[[dict], None],
        on_message: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[str], None]] = None,
        config: Optional[dict] = None,
        websocket_factory: Optional[Callable] = None,
        interval: Optional[str] = None,
    ):
        self.symbol = symbol.upper()
        self.symbol_lower = symbol.lower()
        self.on_bar = on_bar
        self.on_message = on_message
        self.on_disconnect = on_disconnect
        self._ws_factory = websocket_factory

        cfg = (config or {}).get("ws_kline", {})
        data_cfg = (config or {}).get("data", {})
        api_cfg = (config or {}).get("api", {})

        self.interval = interval or data_cfg.get("kline_interval", DEFAULT_INTERVAL)
        self.ws_base_url = api_cfg.get("ws_base_url", DEFAULT_WS_BASE_URL)
        self.ws_url = (
            f"{self.ws_base_url}/ws/{self.symbol_lower}@kline_{self.interval}"
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
        self.drain_timeout_s = float(
            cfg.get("drain_timeout_s", DEFAULT_DRAIN_TIMEOUT_S)
        )
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
        self._reconnect_attempts: list = []  # 最近一小时重连时间戳

        # ---- 统计 ----
        self._stats = {
            "messages": 0,
            "closed_bars": 0,
            "open_bar_updates": 0,
            "dropped_open_updates": 0,
            "dropped_oversize": 0,
            "dropped_invalid": 0,
            "consumer_errors": 0,
            "reconnects": 0,
            "connect_failures": 0,
        }
        self._last_error: Optional[str] = None
        self._last_disconnect_reason: Optional[str] = None

        logger.info(
            f"KlineStream initialized symbol={self.symbol} "
            f"interval={self.interval} url={self.ws_url} "
            f"queue_size={self.queue_size}"
        )

    # ========================================================================
    # 主入口
    # ========================================================================

    def run(self) -> None:
        """
        主循环。阻塞直到 stop() 被调用。

        启动消费线程，然后进入读循环。
        """
        logger.info(f"KlineStream.run started symbol={self.symbol}")

        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"kline-consumer-{self.symbol_lower}",
        )
        self._consumer_thread.start()

        try:
            self._reader_loop()
        except Exception as e:
            self._last_error = str(e)
            logger.error(
                f"KlineStream reader loop crashed: {e}",
                exc_info=True,
            )
        finally:
            self._shutdown_event.set()
            # 排空：消费线程处理完队列后自然退出
            if self._consumer_thread is not None:
                self._consumer_thread.join(
                    timeout=self.drain_timeout_s + 2.0
                )
                if self._consumer_thread.is_alive():
                    logger.warning(
                        f"KlineStream consumer did not finish within "
                        f"{self.drain_timeout_s}s drain timeout"
                    )
            logger.info(f"KlineStream.run stopped symbol={self.symbol}")

    def stop(self) -> None:
        """
        停止流。最多 1 秒内响应。

        契约：
        - 设置 shutdown_event
        - 读循环检测到后退出
        - 消费线程排空队列（只处理已收线）
        """
        with self._lock:
            if self._stop_ts is not None:
                return
            self._stop_ts = time.time()
        self._shutdown_event.set()
        logger.info(f"KlineStream.stop requested symbol={self.symbol}")

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
                    f"KlineStream connect/read error: {e}",
                    exc_info=True,
                )

            if self._shutdown_event.is_set():
                break

            if not self._should_stop():
                delay = self._compute_reconnect_delay()
                logger.info(
                    f"KlineStream reconnecting in {delay:.1f}s "
                    f"(consecutive_failures={self._consecutive_failures})"
                )
                if self._shutdown_event.wait(timeout=delay):
                    break
            else:
                # 超过每小时重连上限
                self._health = StreamHealth.FAILED
                logger.critical(
                    f"KlineStream FAILED: too many reconnects in last hour, "
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
                f"KlineStream connected symbol={self.symbol} "
                f"url={self.ws_url}"
            )

            # 读循环：短超时 + 循环，保证 shutdown 最多 1s 响应
            while not self._shutdown_event.is_set():
                try:
                    raw = conn.recv(timeout=RECV_POLL_INTERVAL_S)
                except TimeoutError:
                    # 检查消息超时
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
                    # 某些 WS 实现对关闭返回 None
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
                    self._last_disconnect_reason = self._last_error or "connection_lost"

                # 通知上游
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
        """
        打开 WebSocket 连接。支持测试注入 websocket_factory。

        契约：
        - 返回对象必须有 recv(timeout=...) 方法
        - 返回对象必须有 close() 方法
        """
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
        """处理原始消息：解析 → 分类 → 入队。"""
        # 大小检查
        if isinstance(raw, (str, bytes)) and len(raw) > self.max_message_bytes:
            with self._lock:
                self._stats["dropped_oversize"] += 1
            logger.warning(
                f"KlineStream oversize message "
                f"({len(raw)} > {self.max_message_bytes})"
            )
            return

        # bytes 解码
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                with self._lock:
                    self._stats["dropped_invalid"] += 1
                logger.warning("KlineStream binary message not valid utf-8")
                return

        if not raw:
            return

        # JSON 解析
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            with self._lock:
                self._stats["dropped_invalid"] += 1
            logger.warning(f"KlineStream invalid json: {e}")
            return

        if not isinstance(msg, dict):
            return

        # 只处理 kline 消息
        if msg.get("e") != "kline":
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

        # 分类：已收线 vs 未收线
        k = msg.get("k", {})
        is_closed = bool(k.get("x", False))

        if is_closed:
            self._enqueue_closed(msg)
        else:
            self._enqueue_open(msg)

    def _enqueue_closed(self, msg: dict) -> None:
        """
        已收线 K 线入队。阻塞等待，不丢。

        队列满超时 → 抛异常 → 强制重连 → on_disconnect → 上游补拉。
        """
        try:
            self._queue.put(msg, timeout=CLOSED_BAR_PUT_TIMEOUT_S)
            with self._lock:
                self._stats["closed_bars"] += 1
        except queue.Full:
            with self._lock:
                self._stats["dropped_invalid"] += 1
                self._last_error = "queue full, closed bar dropped"
            logger.critical(
                f"KlineStream queue full, CLOSED bar dropped "
                f"ts={msg.get('k', {}).get('t')}, forcing reconnect"
            )
            # 抛异常 → 重连 → on_disconnect → 上游补拉
            raise ConnectionError(
                "queue full, closed bar dropped, forcing reconnect"
            )

    def _enqueue_open(self, msg: dict) -> None:
        """
        未收线 K 线更新入队。可丢弃（幂等）。

        丢弃计数，每 100 次打一条 WARN。
        """
        try:
            self._queue.put_nowait(msg)
            with self._lock:
                self._stats["open_bar_updates"] += 1
        except queue.Full:
            with self._lock:
                self._stats["dropped_open_updates"] += 1
                dropped = self._stats["dropped_open_updates"]
            if dropped % 100 == 1:
                logger.warning(
                    f"KlineStream queue full, dropped open bar update "
                    f"(total dropped={dropped})"
                )

    # ========================================================================
    # 消费循环
    # ========================================================================

    def _consumer_loop(self) -> None:
        """消费循环：从队列取消息 → 调 on_bar。"""
        while True:
            try:
                msg = self._queue.get(timeout=1.0)
            except queue.Empty:
                if self._shutdown_event.is_set():
                    return
                continue

            if msg is None:  # 毒丸
                return

            try:
                self.on_bar(msg)
            except Exception as e:
                with self._lock:
                    self._stats["consumer_errors"] += 1
                logger.error(
                    f"KlineStream on_bar error: {e}",
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
                "interval": self.interval,
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
    "name": "ws_kline",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["kline_stream"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Binance kline WebSocket stream with auto-reconnect and reader/consumer split",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> KlineStream:
    """
    从配置创建实例。

    注意：本模块通常由 main.py 直接构造（需要注入回调），
    create() 仅用于 ModuleRegistry 加载（需要 config 提供 on_bar 回调）。

    config 需要：
    - symbol: 交易对
    - ws_kline.on_bar: Callable[[dict], None]

    config 可选：
    - ws_kline.on_message: Callable[[], None]
    - ws_kline.on_disconnect: Callable[[str], None]
    - data.kline_interval: 默认 "3m"
    - api.ws_base_url: 默认 "wss://fstream.binance.com"
    """
    ws_cfg = config.get("ws_kline", {})
    on_bar = ws_cfg.get("on_bar")
    if on_bar is None:
        raise ValueError(
            "KlineStream.create requires config.ws_kline.on_bar callback. "
            "This module is typically constructed directly from main.py, "
            "not loaded via ModuleRegistry."
        )
    return KlineStream(
        symbol=config.get("symbol", "btcusdt"),
        on_bar=on_bar,
        on_message=ws_cfg.get("on_message"),
        on_disconnect=ws_cfg.get("on_disconnect"),
        config=config,
          )
