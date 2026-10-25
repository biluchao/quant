"""
data/ws_aggtrade.py — 币安逐笔成交 WebSocket 数据流 (重铸标准版)

核心设计原则：
1. 高频清洗：在接入层完成 Decimal 强类型转换，过滤无效的价格或数量。
2. 极速消费：队列满时直接丢弃旧帧，保证下游永远拿到最新鲜的成交数据。
3. 读写分离：网络读取与下游回调彻底解耦，防止计算卡顿导致 WS 断连。
4. 绝对安全停机：一秒内响应 stop 请求，不留后台幽灵线程。
"""

import json
import logging
import queue
import threading
import time
import random
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

logger = logging.getLogger(__name__)

class AggTradeStream:
    """
    币安实时逐笔成交数据流（aggTrade）
    """
    def __init__(self, symbol: str, on_message: Callable[[dict], None], config: dict):
        if not symbol or not on_message:
            raise ValueError("AggTradeStream 必须提供 symbol 和 on_message 回调")

        self.symbol = symbol.lower()
        self.on_message = on_message
        
        # 获取基础 URL 与流参数
        ws_base = config.get("binance_ws_base_url", "wss://fstream.binance.com")
        self.ws_url = f"{ws_base}/ws/{self.symbol}@aggTrade"

        # 内部状态
        self._shutdown_event = threading.Event()
        self._queue: queue.Queue = queue.Queue(maxsize=5000)  # 逐笔成交极高，队列容量给足
        self._reader_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None
        
        # 统计
        self._stats = {
            "received": 0, 
            "parsed": 0, 
            "dropped_old": 0, 
            "invalid": 0, 
            "reconnects": 0
        }

    # ========================================================================
    # 生命周期管理
    # ========================================================================

    def start(self) -> None:
        if self._reader_thread is not None:
            return
            
        self._shutdown_event.clear()
        
        self._reader_thread = threading.Thread(target=self._reader_loop, name="ws-aggTrade-reader", daemon=True)
        self._consumer_thread = threading.Thread(target=self._consumer_loop, name="ws-aggTrade-consumer", daemon=True)
        
        self._reader_thread.start()
        self._consumer_thread.start()
        logger.info(f"AggTradeStream 启动: {self.ws_url}")

    def stop(self) -> None:
        self._shutdown_event.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
            
        if self._reader_thread:
            self._reader_thread.join(timeout=2.0)
        if self._consumer_thread:
            self._consumer_thread.join(timeout=2.0)
            
        logger.info(f"AggTradeStream 已停止。统计: {self._stats}")

    # ========================================================================
    # 读线程
    # ========================================================================

    def _reader_loop(self) -> None:
        from websockets.sync.client import connect

        backoff = 1.0

        while not self._shutdown_event.is_set():
            try:
                with connect(self.ws_url, open_timeout=10) as ws:
                    logger.info("逐笔成交 WebSocket 连接成功。")
                    backoff = 1.0
                    self._stats["reconnects"] = 0
                    
                    while not self._shutdown_event.is_set():
                        try:
                            raw_msg = ws.recv(timeout=1.0)
                            self._stats["received"] += 1
                            
                            if isinstance(raw_msg, bytes):
                                raw_msg = raw_msg.decode('utf-8')
                                
                            msg = json.loads(raw_msg)
                            # 只处理 aggTrade 事件
                            if msg.get("e") == "aggTrade":
                                self._parse_and_enqueue(msg)
                                
                        except TimeoutError:
                            continue
                            
            except Exception as e:
                if self._shutdown_event.is_set():
                    break
                    
                self._stats["reconnects"] += 1
                jitter = random.uniform(0.8, 1.2)
                sleep_time = backoff * jitter
                logger.warning(f"逐笔成交 WS 断开: {e}，{sleep_time:.1f}秒后重连...")
                time.sleep(sleep_time)
                backoff = min(backoff * 2, 60.0)

    def _parse_and_enqueue(self, msg: dict) -> None:
        """核心：强类型转换与清洗"""
        try:
            # 提取字段，强制转 Decimal
            price = Decimal(msg["p"])
            qty = Decimal(msg["q"])
            ts = int(msg["T"])  # 交易时间
            is_buyer_maker = bool(msg["m"])
            
            # 过滤无效数据
            if price <= 0 or qty <= 0:
                self._stats["invalid"] += 1
                return

            # 组装标准结构
            trade = {
                "ts": ts,
                "price": price,
                "qty": qty,
                "is_buyer_maker": is_buyer_maker,
            }
            
            self._stats["parsed"] += 1
            self._enqueue(trade)
            
        except (KeyError, InvalidOperation, ValueError, TypeError):
            self._stats["invalid"] += 1
            # 解析失败的脏数据直接丢弃

    def _enqueue(self, trade: dict) -> None:
        """线程安全入队。满了直接挤掉最旧的。"""
        try:
            self._queue.put_nowait(trade)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(trade)
                self._stats["dropped_old"] += 1
                if self._stats["dropped_old"] % 1000 == 1:
                    logger.warning("逐笔成交队列已满，正在丢弃旧帧。")
            except queue.Empty:
                pass

    # ========================================================================
    # 消费线程
    # ========================================================================

    def _consumer_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                trade = self._queue.get(timeout=1.0)
                if trade is None:
                    break
                    
                try:
                    self.on_message(trade)
                except Exception as e:
                    logger.error(f"KlineAuthority 处理逐笔成交回调异常: {e}", exc_info=True)
                    
                self._queue.task_done()
            except queue.Empty:
                continue

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "ws_aggtrade",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["aggtrade_stream"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> AggTradeStream:
    raise NotImplementedError("AggTradeStream 需要显式注入 on_message 回调")
