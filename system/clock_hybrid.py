"""
system/clock_hybrid.py — 混合高精度时钟 (重铸标准版)

核心设计原则：
1. 绝对单调：交易逻辑使用 monotonic_ns，绝不受系统墙钟回拨影响。
2. 三域隔离：now_ms（内部交易）、wall_ms（日志展示）、exchange_ms（API签名）。
3. 稳健校准：中位数滤波抗网络抖动，超限偏移直接拒绝。
4. 零隐式依赖：严格接收配置，必须通过 REST 客户端同步。
"""

import logging
import threading
import time
from collections import deque
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# 异常定义
# ============================================================================
class ClockError(Exception):
    pass

class ClockSyncError(ClockError):
    """同步失败（网络异常或解析失败）"""
    pass

class ClockOffsetError(ClockError):
    """偏移量超过安全阈值，拒绝应用"""
    pass

# ============================================================================
# 混合时钟实现
# ============================================================================

class HybridClock:
    def __init__(self, config: dict):
        if not config:
            raise ValueError("HybridClock 初始化必须提供 config")

        # 提取配置
        clock_cfg = config.get("clock", {})
        self.max_offset_ms = float(clock_cfg.get("max_offset_ms", 5000.0))
        self.warn_offset_ms = float(clock_cfg.get("warn_offset_ms", 200.0))
        self.window_size = int(clock_cfg.get("offset_window", 100))

        self._lock = threading.RLock()

        # --- 基准时钟锚点 ---
        self._mono_base_ns = time.monotonic_ns()
        self._wall_base_ms = int(time.time() * 1000)
        
        # --- 偏移与滤波 ---
        self._current_offset_ms = 0.0
        self._offset_history = deque(maxlen=self.window_size)
        
        # --- 记录 ---
        self._last_sync_ts = 0
        self._last_sync_accepted = True
        self._last_sync_reason = ""

        logger.info("HybridClock 初始化完成 (三时间域分离)")

    # ========================================================================
    # 时间域 1：内部单调时钟 (用于交易逻辑)
    # ========================================================================

    def now_ms(self) -> int:
        """
        获取当前内部单调时间（毫秒）。
        底层使用 monotonic_ns，保证永不倒退，不受系统 NTP 调时影响。
        """
        with self._lock:
            elapsed_ns = time.monotonic_ns() - self._mono_base_ns
            return self._wall_base_ms + (elapsed_ns // 1_000_000)

    # ========================================================================
    # 时间域 2：墙钟时间 (用于日志与展示)
    # ========================================================================

    def wall_ms(self) -> int:
        """
        获取系统真实墙钟时间（毫秒）。
        此时间可能会因为 NTP 同步而向后跳跃，仅用于日志和前端展示。
        """
        return int(time.time() * 1000)

    # ========================================================================
    # 时间域 3：交易所时间 (用于 API 签名)
    # ========================================================================

    def exchange_ms(self) -> int:
        """
        获取校准后的交易所时间（毫秒）。
        公式：exchange_ms = now_ms + current_offset
        """
        with self._lock:
            return int(round(self.now_ms() + self._current_offset_ms))

    # ========================================================================
    # 时钟校准逻辑
    # ========================================================================

    def sync(self, exchange_ts: int, rtt_ms: Optional[float] = None) -> None:
        """
        使用交易所返回的时间戳进行校准。
        """
        with self._lock:
            local_now = self.now_ms()
            raw_offset = float(exchange_ts - local_now)

            self._last_sync_ts = local_now

            # 1. 超过最大偏移限制，拒绝校准
            if abs(raw_offset) > self.max_offset_ms:
                self._last_sync_accepted = False
                self._last_sync_reason = f"offset {raw_offset:.1f}ms > max {self.max_offset_ms}ms"
                logger.error(f"拒绝时钟校准，偏移超限: {self._last_sync_reason}")
                return

            # 2. 记录并滤波
            self._offset_history.append(raw_offset)
            sorted_offsets = sorted(self._offset_history)
            n = len(sorted_offsets)
            
            # 中位数滤波
            if n % 2 == 1:
                median_offset = sorted_offsets[n // 2]
            else:
                median_offset = (sorted_offsets[n // 2 - 1] + sorted_offsets[n // 2]) / 2.0

            old_offset = self._current_offset_ms
            self._current_offset_ms = median_offset
            self._last_sync_accepted = True
            self._last_sync_reason = ""

            log_level = logging.WARNING if abs(median_offset) > self.warn_offset_ms else logging.INFO
            logger.log(
                log_level,
                f"时钟校准成功: 本地快慢 {median_offset:.1f}ms "
                f"(原偏移 {old_offset:.1f}ms, RTT: {rtt_ms or 0:.1f}ms)"
            )

    def sync_from_binance(self, rest_client: Any) -> None:
        """
        从币安服务器同步时间。
        强制要求传入 rest_client，不允许静默失败。
        """
        if rest_client is None:
            raise ValueError("sync_from_binance 需要真实的 RestClient 实例")

        try:
            t0 = self.now_ms()
            server_ts = rest_client.get_server_time()
            t1 = self.now_ms()
            
            rtt = (t1 - t0) / 2.0  # 粗略估计单程延迟
            # 真实网络请求中，服务器时间戳大概率在 RTT 之间产生
            # 为避免引入网络延迟偏差，通常直接把 server_ts 当做校准锚点
            self.sync(server_ts, rtt_ms=rtt)
            
        except Exception as e:
            self._last_sync_accepted = False
            self._last_sync_reason = str(e)
            raise ClockSyncError(f"从交易所同步时间失败: {e}") from e

    # ========================================================================
    # 辅助工具
    # ========================================================================

    def get_market_offset_ms(self) -> float:
        """获取当前应用的时间偏移量"""
        with self._lock:
            return self._current_offset_ms

    def health_check(self) -> dict:
        with self._lock:
            return {
                "healthy": self._last_sync_accepted,
                "offset_ms": self._current_offset_ms,
                "last_sync_ts": self._last_sync_ts,
                "history_size": len(self._offset_history),
                "last_reason": self._last_sync_reason,
            }

# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "clock_hybrid",
    "type": "clock",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["monotonic_clock", "exchange_clock"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> HybridClock:
    return HybridClock(config)
