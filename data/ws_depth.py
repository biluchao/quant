"""
data/ws_depth.py — 币安盘口深度 WebSocket 数据流 (重铸标准版)

核心设计原则：
1. 高频清洗：在接入层完成 Decimal 强类型转换与交叉盘口过滤。
2. 极速消费：队列满时直接丢弃旧帧，保证下游永远拿到最新的盘口快照。
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

class DepthStream:
    """
    币安实时盘口数据流（局部深度，如 depth20@100ms）
    """
    def __init__(self, symbol: str, on_message: Callable[[dict], None], config: dict):
        if not symbol or not on_message:
            raise ValueError("DepthStream 必须提供 symbol 和 on_message 回调")

        self.symbol = symbol.lower()
        self.on_message = on_message
        
        # 获取基础 URL 与流参数（默认取前20档，100ms频率）
        ws_base = config.get("binance_ws_base_url", "wss://fstream.binance.com")
        levels = config.get("depth_levels", 20)
        speed = config.get("depth_speed", "100ms")
        self.ws_url = f"{ws_base}/ws/{self.symbol}@depth{levels}@{speed}"

        # 内部状态
        self._shutdown_event = threading.Event()
        self._queue: queue.Queue = queue.Queue(maxsize=2000)  # 深度极高频，队列给大点
        self._reader_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None
        
        # 统计
        self._stats = {
            "received": 0, 
            "parsed": 0, 
            "dropped_old": 0, 
            "invalid_crossed": 0, 
            "reconnects": 0
        }

    # ========================================================================
    # 生命周期管理
    # ========================================================================

    def start(self) -> None:
        if self._reader_thread is not None:
            return
            
        self._shutdown_event.clear()
        
        self._reader_thread = threading.Thread(target=self._reader_loop, name="ws-depth-reader", daemon=True)
        self._consumer_thread = threading.Thread(target=self._consumer_loop, name="ws-depth-consumer", daemon=True)
        
        self._reader_thread.start()
        self._consumer_thread.start()
        logger.info(f"DepthStream 启动: {self.ws_url}")

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
            
        logger.info(f"DepthStream 已停止。统计: {self._stats}")

    # ========================================================================
    # 读线程
    # ========================================================================

    def _reader_loop(self) -> None:
        from websockets.sync.client import connect

        backoff = 1.0

        while not self._shutdown_event.is_set():
            try:
                with connect(self.ws_url, open_timeout=10) as ws:
                    logger.info("盘口 WebSocket 连接成功。")
                    backoff = 1.0
                    self._stats["reconnects"] = 0
                    
                    while not self._shutdown_event.is_set():
                        try:
                            raw_msg = ws.recv(timeout=1.0)
                            self._stats["received"] += 1
                            
                            if isinstance(raw_msg, bytes):
                                raw_msg = raw_msg.decode('utf-8')
                                
                            # 币安局部深度流格式不同，可能没有 e 字段，直接是 u,s,b,a
                            msg = json.loads(raw_msg)
                            self._parse_and_enqueue(msg)
                                
                        except TimeoutError:
                            continue
                            
            except Exception as e:
                if self._shutdown_event.is_set():
                    break
                    
                self._stats["reconnects"] += 1
                jitter = random.uniform(0.8, 1.2)
                sleep_time = backoff * jitter
                logger.warning(f"盘口 WS 断开: {e}，{sleep_time:.1f}秒后重连...")
                time.sleep(sleep_time)
                backoff = min(backoff * 2, 60.0)

    def _parse_and_enqueue(self, msg: dict) -> None:
        """核心：强类型清洗与交叉盘口过滤"""
        try:
            # 兼容不同的推送格式
            bids_raw = msg.get("b") or msg.get("bids") or []
            asks_raw = msg.get("a") or msg.get("asks") or []
            
            if not bids_raw or not asks_raw:
                return

            # 强类型转换
            bids = tuple((Decimal(p), Decimal(q)) for p, q in bids_raw if Decimal(q) > 0)
            asks = tuple((Decimal(p), Decimal(q)) for p, q in asks_raw if Decimal(q) > 0)
            
            if not bids or not asks:
                return

            # 交叉盘口过滤（买一 >= 卖一）
            if bids[0][0] >= asks[0][0]:
                self._stats["invalid_crossed"] += 1
                logger.warning(f"拦截交叉盘口脏数据: Bid1={bids[0][0]} >= Ask1={asks[0][0]}")
                return

            # 组装标准结构，推入队列
            snapshot = {
                "ts": int(msg.get("E", msg.get("T", time.time() * 1000))),
                "bids": bids,
                "asks": asks,
            }
            
            self._stats["parsed"] += 1
            self._enqueue(snapshot)
            
        except (InvalidOperation, ValueError, TypeError):
            pass  # 解析失败的脏数据直接丢弃，不刷屏报错

    def _enqueue(self, snapshot: dict) -> None:
        """线程安全入队。满了直接挤掉最旧的。"""
        try:
            self._queue.put_nowait(snapshot)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(snapshot)
                self._stats["dropped_old"] += 1
                if self._stats["dropped_old"] % 1000 == 1:
                    logger.warning("盘口队列已满，正在丢弃旧帧。")
            except queue.Empty:
                pass

    # ========================================================================
    # 消费线程
    # ========================================================================

    def _consumer_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                snapshot = self._queue.get(timeout=1.0)
                if snapshot is None:
                    break
                    
                try:
                    self.on_message(snapshot)
                except Exception as e:
                    logger.error(f"KlineAuthority 处理盘口回调异常: {e}", exc_info=True)
                    
                self._queue.task_done()
            except queue.Empty:
                continue

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "ws_depth",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["depth_stream"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> DepthStream:
    raise NotImplementedError("DepthStream 需要显式注入 on_message 回调")
