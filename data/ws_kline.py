"""
data/ws_kline.py — 币安 K 线 WebSocket 数据流 (重铸标准版)

核心设计原则：
1. 读写分离：读线程只负责网络拉取与入队，消费线程独立调用下游回调，绝不相互阻塞。
2. 优雅停机：外部调用 stop() 后，能在一秒内干净退出，不留后台幽灵线程。
3. 智能重连：指数退避策略，抗网络抖动，不因短暂断网导致系统彻底假死。
4. 零异常穿透：任何解析错误、断网异常都被内部消化并记录，外层调用者无感知。
"""

import json
import logging
import queue
import threading
import time
import random
from typing import Callable, Optional

logger = logging.getLogger(__name__)

class KlineStream:
    """
    币安实时 K 线数据流
    """
    def __init__(self, symbol: str, on_message: Callable[[dict], None], config: dict):
        if not symbol or not on_message:
            raise ValueError("KlineStream 必须提供 symbol 和 on_message 回调")

        self.symbol = symbol.lower()
        self.on_message = on_message
        
        # 获取基础 URL
        ws_base = config.get("binance_ws_base_url", "wss://fstream.binance.com")
        self.ws_url = f"{ws_base}/ws/{self.symbol}@kline_3m"

        # 内部状态
        self._shutdown_event = threading.Event()
        self._queue: queue.Queue = queue.Queue(maxsize=5000)
        self._reader_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None
        
        # 统计
        self._stats = {"received": 0, "parsed": 0, "dropped": 0, "reconnects": 0}

    # ========================================================================
    # 生命周期管理
    # ========================================================================

    def start(self) -> None:
        """启动读线程和消费线程"""
        if self._reader_thread is not None:
            return
            
        self._shutdown_event.clear()
        
        self._reader_thread = threading.Thread(target=self._reader_loop, name="ws-kline-reader", daemon=True)
        self._consumer_thread = threading.Thread(target=self._consumer_loop, name="ws-kline-consumer", daemon=True)
        
        self._reader_thread.start()
        self._consumer_thread.start()
        logger.info(f"KlineStream 启动: {self.ws_url}")

    def stop(self) -> None:
        """优雅停机"""
        self._shutdown_event.set()
        # 塞入空数据唤醒可能阻塞的消费线程
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
            
        if self._reader_thread:
            self._reader_thread.join(timeout=2.0)
        if self._consumer_thread:
            self._consumer_thread.join(timeout=2.0)
            
        logger.info(f"KlineStream 已停止。统计: {self._stats}")

    # ========================================================================
    # 读线程 (只管拿数据入队)
    # ========================================================================

    def _reader_loop(self) -> None:
        from websockets.sync.client import connect

        backoff = 1.0

        while not self._shutdown_event.is_set():
            try:
                with connect(self.ws_url, open_timeout=10) as ws:
                    logger.info("K 线 WebSocket 连接成功。")
                    backoff = 1.0  # 连接成功重置退避
                    self._stats["reconnects"] = 0
                    
                    while not self._shutdown_event.is_set():
                        try:
                            # 设置 1s 超时，保证能及时响应 shutdown
                            raw_msg = ws.recv(timeout=1.0)
                            self._stats["received"] += 1
                            
                            if isinstance(raw_msg, bytes):
                                raw_msg = raw_msg.decode('utf-8')
                                
                            msg = json.loads(raw_msg)
                            if msg.get("e") == "kline":
                                # 不管收没收线，都扔进队列让下游判断
                                self._enqueue(msg)
                                self._stats["parsed"] += 1
                                
                        except TimeoutError:
                            # 正常的超时，检查 loop 条件继续
                            continue
                            
            except Exception as e:
                if self._shutdown_event.is_set():
                    break
                    
                # 网络断开重连
                self._stats["reconnects"] += 1
                jitter = random.uniform(0.8, 1.2)
                sleep_time = backoff * jitter
                
                logger.warning(f"K 线 WS 断开: {e}，{sleep_time:.1f}秒后重连...")
                time.sleep(sleep_time)
                
                backoff = min(backoff * 2, 60.0)

    def _enqueue(self, msg: dict) -> None:
        """线程安全入队，队列满则丢弃最旧数据（防止内存爆炸）"""
        try:
            self._queue.put_nowait(msg)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(msg)
                self._stats["dropped"] += 1
                if self._stats["dropped"] % 100 == 1:
                    logger.warning("K 线队列已满，丢弃最旧数据以保最新。")
            except queue.Empty:
                pass

    # ========================================================================
    # 消费线程 (只管调回调)
    # ========================================================================

    def _consumer_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                msg = self._queue.get(timeout=1.0)
                if msg is None:
                    break  # 毒丸，退出
                    
                # 严格异常隔离，确保回调报错不影响数据流
                try:
                    self.on_message(msg)
                except Exception as e:
                    logger.error(f"KlineAuthority 处理 K 线回调异常: {e}", exc_info=True)
                    
                self._queue.task_done()
            except queue.Empty:
                continue

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "ws_kline",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["kline_stream"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> KlineStream:
    # 由 main.py 显式组装并注入 symbol 和 on_message
    raise NotImplementedError("KlineStream 需要显式注入 on_message 回调")
