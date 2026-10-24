"""
system/clock_hybrid.py — 混合时钟实现（单调时钟 + 交易所校准）

这是 core/clock.py 抽象基类的唯一实盘/影子实现。

设计原则：
1. 单调时钟用 time.monotonic_ns()，纳秒精度转毫秒
2. 墙钟用 time.time()，仅用于日志和展示
3. 交易所时间 = 单调时间 + 中位数滤波后的偏移
4. 校准失败不改变当前偏移
5. 时间倒退抛 ClockBackwardError（防御性）
6. 线程安全

三个时钟域：
- now_ms():     monotonic_ns 基准 + 偏移，严格单调
- wall_ms():    time.time() * 1000，可被 NTP 调整
- exchange_ms(): now_ms() + current_offset，用于 API timestamp

校准算法：
1. sync(exchange_ts_ms) 计算 raw_offset = exchange_ts - now_ms()
2. 检查 |raw_offset| <= max_offset_ms，超限拒绝
3. 接受则加入 offset_history（deque，maxlen=offset_window）
4. current_offset = median(offset_history)
5. 每次 sync 记录到 SyncRecord，包含 accepted 和 reason

偏移符号：
- offset = exchange_ts - local_now
- offset > 0：本地慢，需要加 offset
- offset < 0：本地快，需要减 offset
- exchange_ms() = now_ms() + offset

RTT 处理：
- 币安 /fapi/v1/time 返回服务器处理时间
- 不补偿 RTT（补偿反而引入偏差）
- rtt_ms 仅记录到 SyncRecord 用于诊断

时间倒退检测：
- time.monotonic_ns() 理论上不倒退
- 但计算过程可能出错（浮点、平台差异）
- 防御性：now < last_now - tolerance 时抛异常
- 小幅倒退（容差内）返回 last_now，对外保持单调

健康状态优先级：
1. BACKWARD：检测到倒退
2. REJECTED：最近一次校准被拒绝
3. STALE：超过 2 * resync_interval_s 未校准
4. WARN：偏移超过 warn_offset_ms
5. HEALTHY：正常

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from collections import deque
import logging
import threading
import time
from typing import Any, Deque, Dict, Optional

from core.clock import (
    ClockProvider,
    ClockBackwardError,
    ClockHealth,
    ClockManifest,
    ClockSyncError,
    SyncRecord,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "hybrid",
    "type": "clock",
    "version": "1.0.0",
    "step": 1,
    "capabilities": [
        "monotonic_clock",
        "wall_clock",
        "exchange_clock",
        "clock_sync",
    ],
    "owner": "zhangsan",
    "description": "Hybrid clock: monotonic clock + exchange time sync",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_OFFSET_HISTORY_SIZE = 200  # 校准历史保留条数


# ============================================================================
# HybridClock
# ============================================================================
class HybridClock(ClockProvider):
    """
    混合时钟。详见模块 docstring。

    用法：
        clock = HybridClock(config)
        clock.set_rest_client(rest)           # 用于 force_resync
        clock.sync(rest.get_server_time())    # 首次校准

        now = clock.now_ms()                  # 单调时钟
        wall = clock.wall_ms()                # 墙钟
        ex = clock.exchange_ms()              # 交易所时间

        # 定期校准（由调度器调用）
        if clock.should_resync():
            clock.sync(rest.get_server_time())
    """

    name = "hybrid"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        # ---- 单调时钟基准 ----
        # 用 monotonic_ns() 保证纳秒精度，转毫秒无累积误差
        self._mono_base_ns: int = time.monotonic_ns()

        # ---- 墙钟基准 ----
        self._wall_base_ms: int = int(time.time() * 1000)

        # ---- 单调时钟与墙钟的对齐基准 ----
        # 构造时 now_ms() == wall_ms()，保证初始一致
        self._now_base_ms: int = self._wall_base_ms

        # ---- 当前偏移 ----
        self._current_offset_ms: float = 0.0

        # ---- 偏移历史（中位数滤波）----
        self._offset_history: Deque[float] = deque(
            maxlen=self.offset_window
        )

        # ---- 校准记录 ----
        self._last_sync_ts: int = 0
        self._last_sync_exchange_ts: int = 0
        self._last_sync_accepted: bool = True
        self._last_sync_reason: str = ""
        self._last_rtt_ms: Optional[float] = None

        # ---- 时间倒退检测 ----
        self._last_now_ms: int = self._now_base_ms
        self._backward_count: int = 0

        # ---- REST 客户端（用于 force_resync）----
        self._rest_client: Optional[Any] = None

        # ---- 独立锁（基类的 _lock 不够细）----
        self._clock_lock = threading.RLock()

        logger.info(
            f"HybridClock initialized "
            f"mono_base_ns={self._mono_base_ns} "
            f"wall_base_ms={self._wall_base_ms}"
        )

    # ------------------------------------------------------------------------
    # 注入
    # ------------------------------------------------------------------------

    def set_rest_client(self, rest_client: Any) -> None:
        """
        注入 REST 客户端。用于 force_resync() 拉取交易所时间。

        契约：
        - rest_client 必须有 get_server_time() -> int 方法
        - 返回毫秒时间戳
        """
        with self._clock_lock:
            self._rest_client = rest_client
        logger.debug("HybridClock rest_client injected")

    # ------------------------------------------------------------------------
    # 三个时钟域
    # ------------------------------------------------------------------------

    def now_ms(self) -> int:
        """
        单调时钟（毫秒）。

        契约：
        - 严格单调递增（容差内倒退返回 last_now）
        - 倒退超容差抛 ClockBackwardError
        - 不受 NTP 调整影响
        """
        with self._clock_lock:
            elapsed_ns = time.monotonic_ns() - self._mono_base_ns
            now = self._now_base_ms + elapsed_ns // 1_000_000

            # 倒退检测
            if now < self._last_now_ms - self.backward_tolerance_ms:
                self._backward_count += 1
                logger.error(
                    f"clock backward detected: {now} < "
                    f"{self._last_now_ms} "
                    f"(backward_count={self._backward_count})"
                )
                raise ClockBackwardError(
                    f"clock backward: {now} < {self._last_now_ms}, "
                    f"tolerance={self.backward_tolerance_ms}ms"
                )

            # 容差内倒退：返回 last_now，保持对外单调
            if now < self._last_now_ms:
                return self._last_now_ms

            # 正常前进
            self._last_now_ms = now
            return now

    def wall_ms(self) -> int:
        """
        墙钟时间（毫秒）。仅用于日志和展示。

        契约：
        - 可能被 NTP 调整，可能回拨
        - 绝不用于交易逻辑
        """
        return int(time.time() * 1000)

    def exchange_ms(self) -> int:
        """
        交易所时间（毫秒）。

        契约：
        - exchange_ms() = now_ms() + current_offset
        - 内部 offset 用 float，返回 int
        """
        with self._clock_lock:
            return int(round(self.now_ms() + self._current_offset_ms))

    # ------------------------------------------------------------------------
    # 校准
    # ------------------------------------------------------------------------

    def sync(
        self,
        exchange_ts_ms: int,
        rtt_ms: Optional[float] = None,
    ) -> SyncRecord:
        """
        用交易所时间戳校准。

        契约：
        - offset = exchange_ts - now_ms()
        - |offset| > max_offset_ms 拒绝，accepted=False
        - 接受则加入中位数滤波历史
        - 失败不改变 current_offset
        - 记录到 SyncRecord

        Args:
            exchange_ts_ms: 交易所返回的时间戳（毫秒）
            rtt_ms:         可选，请求往返延迟（毫秒），仅用于诊断

        Returns:
            SyncRecord，accepted=False 表示拒绝
        """
        with self._clock_lock:
            local_now_ms = self.now_ms()
            raw_offset = float(exchange_ts_ms - local_now_ms)

            # 检查偏移
            reject_reason = self._check_offset(raw_offset)

            record = SyncRecord(
                ts=local_now_ms,
                exchange_ts=exchange_ts_ms,
                offset_ms=raw_offset,
                accepted=(reject_reason is None),
                reason=reject_reason,
                rtt_ms=rtt_ms,
            )

            self._last_sync_ts = local_now_ms
            self._last_sync_exchange_ts = exchange_ts_ms
            self._last_rtt_ms = rtt_ms

            if reject_reason:
                self._last_sync_accepted = False
                self._last_sync_reason = reject_reason
                logger.warning(
                    f"clock sync rejected: {reject_reason} "
                    f"(exchange_ts={exchange_ts_ms}, "
                    f"local_ts={local_now_ms})"
                )
                return record

            # 接受：加入历史
            self._offset_history.append(raw_offset)

            # 中位数滤波
            new_offset = self._median_offset(
                list(self._offset_history)
            )
            old_offset = self._current_offset_ms
            self._current_offset_ms = new_offset

            self._last_sync_accepted = True
            self._last_sync_reason = ""

            if abs(new_offset) > self.warn_offset_ms:
                logger.warning(
                    f"clock offset {new_offset:.1f}ms exceeds "
                    f"warn threshold {self.warn_offset_ms:.1f}ms"
                )
            else:
                logger.info(
                    f"clock sync offset={new_offset:.1f}ms "
                    f"(was {old_offset:.1f}ms, "
                    f"history={len(self._offset_history)})"
                )

            return record

    def force_resync(self) -> SyncRecord:
        """
        强制重新校准。收到 -1021 错误时调用。

        契约：
        - 必须先 set_rest_client()
        - 拉取交易所时间后调 sync()
        - 网络失败抛 ClockSyncError
        - 不持锁调网络请求
        """
        with self._clock_lock:
            client = self._rest_client

        if client is None:
            raise ClockSyncError(
                "force_resync requires rest_client, "
                "call set_rest_client() first"
            )

        try:
            exchange_ts = client.get_server_time()
        except Exception as e:
            raise ClockSyncError(
                f"force_resync: get_server_time failed: {e}"
            ) from e

        return self.sync(exchange_ts)

    def should_resync(self) -> bool:
        """
        是否应该定期校准。

        契约：
        - 首次调用（_last_sync_ts == 0）返回 True
        - 距上次校准 >= resync_interval_s 返回 True
        - 由调度器周期调用，不在时钟内部自动触发
        """
        with self._clock_lock:
            if self._last_sync_ts == 0:
                return True
            elapsed_ms = self.now_ms() - self._last_sync_ts
            return elapsed_ms >= self.resync_interval_s * 1000

    def get_market_offset_ms(self) -> float:
        """当前市场偏移（毫秒）。exchange_ms() - now_ms()。"""
        with self._clock_lock:
            return self._current_offset_ms

    # ------------------------------------------------------------------------
    # 健康检查
    # ------------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """
        健康检查。

        状态优先级：BACKWARD > REJECTED > STALE > WARN > HEALTHY
        """
        with self._clock_lock:
            now = self.now_ms()
            wall = self.wall_ms()

            # 判断状态
            if self._backward_count > 0:
                health = ClockHealth.BACKWARD.value
                healthy = False
                errors = [f"clock backward detected {self._backward_count} times"]
            elif not self._last_sync_accepted:
                health = ClockHealth.REJECTED.value
                healthy = False
                errors = [f"last sync rejected: {self._last_sync_reason}"]
            elif (
                self._last_sync_ts > 0
                and (now - self._last_sync_ts) > self.resync_interval_s * 2 * 1000
            ):
                health = ClockHealth.STALE.value
                healthy = False
                age_s = (now - self._last_sync_ts) / 1000
                errors = [f"clock not synced for {age_s:.1f}s"]
            elif abs(self._current_offset_ms) > self.warn_offset_ms:
                health = ClockHealth.WARN.value
                healthy = True
                errors = []
            else:
                health = ClockHealth.HEALTHY.value
                healthy = True
                errors = []

            last_sync_age_s = (
                (now - self._last_sync_ts) / 1000
                if self._last_sync_ts > 0 else -1.0
            )

            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "health": health,
                "offset_ms": self._current_offset_ms,
                "offset_history_size": len(self._offset_history),
                "last_sync_ts": self._last_sync_ts,
                "last_sync_exchange_ts": self._last_sync_exchange_ts,
                "last_sync_age_s": last_sync_age_s,
                "last_sync_accepted": self._last_sync_accepted,
                "last_sync_reason": self._last_sync_reason,
                "last_rtt_ms": self._last_rtt_ms,
                "backward_count": self._backward_count,
                "errors": errors,
                "timestamp": wall,
            }

    # ------------------------------------------------------------------------
    # 重置
    # ------------------------------------------------------------------------

    def reset(self) -> None:
        """重置所有时钟状态。测试和热重启时使用。"""
        with self._clock_lock:
            self._mono_base_ns = time.monotonic_ns()
            self._wall_base_ms = int(time.time() * 1000)
            self._now_base_ms = self._wall_base_ms
            self._current_offset_ms = 0.0
            self._offset_history.clear()
            self._last_sync_ts = 0
            self._last_sync_exchange_ts = 0
            self._last_sync_accepted = True
            self._last_sync_reason = ""
            self._last_rtt_ms = None
            self._last_now_ms = self._now_base_ms
            self._backward_count = 0
        logger.info("HybridClock reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> ClockProvider:
    """模块工厂入口。"""
    return HybridClock(config)


# ============================================================================
# 模块级校验（供 ModuleRegistry 调用）
# ============================================================================
def validate_module() -> ClockManifest:
    """校验 MANIFEST 合法。加载器调用。"""
    return ClockManifest.from_dict(MANIFEST)
