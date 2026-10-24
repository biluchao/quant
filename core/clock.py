"""
core/clock.py — 时钟抽象基类与通用数据结构

时钟是所有时间相关 bug 的源头。

核心原则：
1. 三个时钟域分离，不可混用
   - now_ms():     单调时钟，用于交易逻辑（K 线收线、止损触发、订单超时、限频、重试退避）
   - wall_ms():    墙钟时间，仅用于日志和展示（可被 NTP 调整，会回拨）
   - exchange_ms(): 交易所时间，用于 API 请求的 timestamp 参数

2. 交易逻辑绝不用 wall_ms()
   - NTP 回拨一秒，用 wall_ms() 的代码全乱
   - 用 now_ms()，单调递增，构造后永不倒退

3. exchange_ms() 是校准后的交易所时间
   - 用中位数滤波估算偏移
   - 偏移超过阈值拒绝校准
   - 校准失败不改变当前偏移

4. 所有时间单位毫秒（int）
   - 不用 float 秒，避免精度问题
   - 不用 datetime，避免时区问题

5. 所有方法线程安全

关键契约：
1. now_ms() 严格单调递增，构造后永不倒退
2. now_ms() 检测到倒退抛 ClockBackwardError
3. sync() 只接收时间戳，不接收 REST 客户端
4. sync() 偏移超过 max_offset_ms 拒绝，抛 ClockOffsetError
5. sync() 失败不改变当前偏移
6. wall_ms() 可被 NTP 调整，可能回拨，仅用于日志
7. exchange_ms() = now_ms() + current_offset
8. 回测用 SimulatedClock，实盘用 HybridClock，两者接口一致
9. 策略和状态只能通过 context.now_ms 访问时间，不能直接访问 clock

时钟域使用场景：
┌─────────────────┬──────────────────────┬─────────────────┐
│ 场景            │ 时钟                 │ 原因            │
├─────────────────┼──────────────────────┼─────────────────┤
│ K 线收线判断    │ now_ms()             │ 不受 NTP 影响   │
│ 止损触发判断    │ now_ms()             │ 不受 NTP 影响   │
│ 订单超时判断    │ now_ms()             │ 不受 NTP 影响   │
│ 限频令牌桶      │ now_ms()             │ 不受 NTP 影响   │
│ 重试退避        │ now_ms()             │ 不受 NTP 影响   │
│ 状态机超时      │ now_ms()             │ 不受 NTP 影响   │
│ 日志时间戳      │ wall_ms()            │ 需可读的 UTC    │
│ 前端展示        │ wall_ms()            │ 需可读的本地时间│
│ API timestamp   │ exchange_ms()        │ 需与交易所一致  │
│ 订单对账        │ exchange_ms()        │ 需与交易所一致  │
└─────────────────┴──────────────────────┴─────────────────┘

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import logging
import threading
import time

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_OFFSET_MS = 5000.0        # 偏移超过此值拒绝校准
DEFAULT_WARN_OFFSET_MS = 200.0        # 偏移超过此值打 WARN
DEFAULT_OFFSET_WINDOW = 100           # 中位数滤波窗口
DEFAULT_RESYNC_INTERVAL_S = 21600     # 6 小时定期校准
DEFAULT_RECV_WINDOW_MS = 5000         # 币安 recvWindow 默认值
DEFAULT_BACKWARD_TOLERANCE_MS = 0     # 时间倒退容忍度（0 = 不允许倒退）


# ============================================================================
# 时钟错误
# ============================================================================
class ClockError(Exception):
    """时钟错误基类。"""
    pass


class ClockBackwardError(ClockError):
    """检测到时间倒退。"""
    pass


class ClockOffsetError(ClockError):
    """偏移超过阈值，拒绝校准。"""
    pass


class ClockSyncError(ClockError):
    """校准失败（网络、解析等）。"""
    pass


# ============================================================================
# 时钟健康状态
# ============================================================================
class ClockHealth(str, Enum):
    HEALTHY = "healthy"           # 正常
    WARN = "warn"                 # 偏移偏大
    STALE = "stale"               # 长期未校准
    REJECTED = "rejected"         # 最近一次校准被拒绝
    BACKWARD = "backward"         # 检测到时间倒退


# ============================================================================
# 校准记录
# ============================================================================
@dataclass(frozen=True)
class SyncRecord:
    """单次校准记录。"""
    ts: int                          # 校准时的本地单调时间（毫秒）
    exchange_ts: int                 # 交易所时间（毫秒）
    offset_ms: float                 # 计算的偏移（exchange_ts - now_ms）
    accepted: bool                   # 是否被接受
    reason: Optional[str] = None     # 拒绝原因
    rtt_ms: Optional[float] = None   # 请求往返延迟（如有）


# ============================================================================
# 时钟 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class ClockManifest:
    """时钟 MANIFEST 字典解析后的结构。"""
    name: str
    type: str
    version: str
    step: int
    capabilities: tuple
    owner: str
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "ClockManifest":
        required = ("name", "type", "version", "step", "owner")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")
        if d["type"] != "clock":
            raise ValueError(
                f"MANIFEST type must be 'clock', got '{d['type']}'"
            )
        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            capabilities=tuple(d.get("capabilities", [])),
            owner=d["owner"],
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 时钟抽象基类
# ============================================================================
class ClockProvider(ABC):
    """
    时钟抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.name / self.version
    2. 实现 now_ms() / wall_ms() / exchange_ms()
    3. 实现 sync() / force_resync()
    4. 实现 health_check()

    子类实现参考：
    - HybridClock:   实盘/影子，单调时钟 + 交易所校准
    - SimulatedClock: 回测，模拟时间推进
    """

    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        clock_cfg = self._extract_clock_config(config)

        self.max_offset_ms = float(
            clock_cfg.get("max_offset_ms", DEFAULT_MAX_OFFSET_MS)
        )
        self.warn_offset_ms = float(
            clock_cfg.get("warn_offset_ms", DEFAULT_WARN_OFFSET_MS)
        )
        self.offset_window = int(
            clock_cfg.get("offset_window", DEFAULT_OFFSET_WINDOW)
        )
        self.resync_interval_s = float(
            clock_cfg.get("resync_interval_s", DEFAULT_RESYNC_INTERVAL_S)
        )
        self.recv_window_ms = int(
            clock_cfg.get("recv_window_ms", DEFAULT_RECV_WINDOW_MS)
        )
        self.backward_tolerance_ms = int(
            clock_cfg.get("backward_tolerance_ms", DEFAULT_BACKWARD_TOLERANCE_MS)
        )

        self._lock = threading.RLock()

    def _extract_clock_config(self, config: dict) -> dict:
        """从 config 中提取时钟配置段。"""
        clock_cfg = config.get("clock", {})
        if not isinstance(clock_cfg, dict):
            return {}
        return clock_cfg

    # ------------------------------------------------------------------------
    # 三个时钟域（子类必须实现）
    # ------------------------------------------------------------------------

    @abstractmethod
    def now_ms(self) -> int:
        """
        单调时钟（毫秒）。交易逻辑唯一允许使用的时间。

        契约：
        - 严格单调递增，构造后永不倒退
        - 检测到倒退抛 ClockBackwardError
        - 不受 NTP 调整影响
        - 线程安全

        Returns:
            毫秒时间戳（int）
        """
        ...

    @abstractmethod
    def wall_ms(self) -> int:
        """
        墙钟时间（毫秒）。仅用于日志和展示。

        契约：
        - 可能被 NTP 调整，可能回拨
        - 绝不用于交易逻辑
        - 线程安全

        Returns:
            UTC 毫秒时间戳（int）
        """
        ...

    @abstractmethod
    def exchange_ms(self) -> int:
        """
        交易所时间（毫秒）。用于 API 请求的 timestamp 参数。

        契约：
        - exchange_ms() = now_ms() + current_offset
        - 偏移由 sync() 校准
        - 校准失败时用上次已知偏移
        - 线程安全

        Returns:
            交易所时间毫秒（int）
        """
        ...

    # ------------------------------------------------------------------------
    # 校准
    # ------------------------------------------------------------------------

    @abstractmethod
    def sync(self, exchange_ts_ms: int) -> SyncRecord:
        """
        用交易所时间戳校准。只接收时间戳，不接收 REST 客户端。

        契约：
        - 计算 offset = exchange_ts_ms - now_ms()
        - 偏移超过 max_offset_ms 拒绝，返回 SyncRecord(accepted=False)
        - 用中位数滤波（窗口 offset_window）更新 current_offset
        - 失败不改变 current_offset
        - 记录到 SyncRecord 历史
        - 线程安全

        Args:
            exchange_ts_ms: 交易所返回的时间戳（毫秒）

        Returns:
            SyncRecord，accepted=False 表示拒绝
        """
        ...

    def force_resync(self) -> SyncRecord:
        """
        强制重新校准。收到 -1021 错误时调用。
        默认实现：子类覆盖，或抛 NotImplementedError。

        实盘实现应：拉取交易所时间，调 sync()。
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement force_resync()"
        )

    # ------------------------------------------------------------------------
    # 时间转换
    # ------------------------------------------------------------------------

    @abstractmethod
    def get_market_offset_ms(self) -> float:
        """
        当前市场偏移（毫秒）。exchange_ms() - now_ms()。

        契约：
        - 偏移为正表示本地时钟慢
        - 偏移为负表示本地时钟快
        - 线程安全

        Returns:
            偏移（float 毫秒）
        """
        ...

    def to_exchange_ts(self, local_ms: int) -> int:
        """
        本地时间戳 → 交易所时间戳。

        契约：
        - exchange_ts = local_ms + current_offset
        - 用于对账：本地记录的时间戳怎么映射到交易所时间
        """
        return int(round(local_ms + self.get_market_offset_ms()))

    def from_exchange_ts(self, exchange_ms: int) -> int:
        """
        交易所时间戳 → 本地时间戳。

        契约：
        - local_ms = exchange_ms - current_offset
        """
        return int(round(exchange_ms - self.get_market_offset_ms()))

    # ------------------------------------------------------------------------
    # 健康检查与重置
    # ------------------------------------------------------------------------

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        健康检查。监控系统定期调用。

        Returns:
            {
                "healthy": bool,
                "name": str,
                "version": str,
                "health": ClockHealth 的 value,
                "offset_ms": float,
                "last_sync_ts": int,
                "last_sync_exchange_ts": int,
                "last_sync_age_s": float,
                "backward_detected": bool,
                "errors": List[str],
                "timestamp": int,   # wall_ms()
            }
        """
        ...

    def reset(self) -> None:
        """
        重置时钟状态。测试和热重启时使用。
        默认实现：子类覆盖。
        """
        logger.info(f"clock {self.name} reset")

    # ------------------------------------------------------------------------
    # 工具方法（子类可用）
    # ------------------------------------------------------------------------

    def _median_offset(self, offsets: List[float]) -> float:
        """中位数滤波。抗单次尖峰。"""
        if not offsets:
            return 0.0
        sorted_offsets = sorted(offsets)
        n = len(sorted_offsets)
        if n % 2 == 1:
            return sorted_offsets[n // 2]
        return (sorted_offsets[n // 2 - 1] + sorted_offsets[n // 2]) / 2.0

    def _check_offset(self, offset_ms: float) -> Optional[str]:
        """
        检查偏移是否可接受。返回拒绝原因（None 表示接受）。
        """
        if abs(offset_ms) > self.max_offset_ms:
            return (
                f"offset {offset_ms:.1f}ms exceeds "
                f"max_offset {self.max_offset_ms:.1f}ms"
            )
        return None


# ============================================================================
# 模块级工厂校验
# ============================================================================
def validate_clock_module(module, config: dict) -> ClockProvider:
    """
    加载并校验一个时钟模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 ClockProvider 实例
    4. 实例的 name / version 与 MANIFEST 一致
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = ClockManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, ClockProvider):
        raise TypeError(
            f"{module.__name__}.create() must return ClockProvider, "
            f"got {type(instance).__name__}"
        )
    if instance.name != manifest.name:
        raise ValueError(
            f"{module.__name__}: instance.name='{instance.name}' "
            f"!= MANIFEST.name='{manifest.name}'"
        )
    if instance.version != manifest.version:
        raise ValueError(
            f"{module.__name__}: instance.version='{instance.version}' "
            f"!= MANIFEST.version='{manifest.version}'"
        )

    return instance
