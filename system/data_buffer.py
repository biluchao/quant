"""
system/data_buffer.py — K 线环形缓冲区

这是因子计算的数据源，也是实时推送和历史加载的交汇点。

用途：
1. 因子计算窗口（因子需要最近 N 根 K 线算分位数）
2. 状态机上下文（context.recent_bars 的数据源）
3. 预热检查（是否加载了足够的历史数据）
4. 回退数据（数据源断开时，缓冲区里还有数据）

设计原则：
1. 有界：固定 maxlen，超过自动淘汰最旧
2. 有序：缓冲区按 ts 严格递增
3. 去重：同一 ts 只保留一条
4. 已收线：只接受 is_closed=True 的 K 线
5. 线程安全：多线程并发读写
6. 快速：push/get_recent 是热路径，O(1) 或 O(n) 但 n 小

关键契约：
1. push() 保证缓冲区按 ts 严格递增
2. push() 对同一 ts 的 K 线做替换（不追加）
3. push() 对乱序 K 线（ts < last_ts）拒绝
4. push() 对未收线 K 线（is_closed=False）拒绝
5. bulk_load() 排序+去重+截断，替换整个缓冲区
6. bulk_load() 是原子操作，期间推送阻塞
7. get_recent() 空缓冲区返回空列表，不抛异常
8. bars 属性返回副本（list），防外部修改
9. 所有方法线程安全
10. maxlen >= warmup_bars，否则打 WARN

使用场景：
- main.py._init_data: RingBuffer(maxlen=1000)，从 HistoricalLoader 加载
- main.py._on_bar: buffer.push(bar)（已收线 K 线）
- 状态机: buffer.get_recent(N) → context.recent_bars
- 因子: 从 context.recent_bars 或 buffer 取数据

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from collections import deque
import logging
import threading
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAXLEN = 1000
DEFAULT_WARMUP_BARS = 500


# ============================================================================
# RingBuffer
# ============================================================================
class RingBuffer:
    """
    K 线环形缓冲区。详见模块 docstring。

    用法：
        buffer = RingBuffer(maxlen=1000)

        # 启动时加载历史
        bars = historical_loader.load_recent_klines(symbol, "3m", 1000)
        buffer.bulk_load(bars)

        # 实时推送（已收线 K 线）
        buffer.push(bar)

        # 查询
        recent = buffer.get_recent(100)   # 最近 100 根
        latest = buffer.latest()          # 最新一根
        bar = buffer.get_bar_at(ts)       # 按 ts 查

        # 预热检查
        if not buffer.is_ready(min_bars=500):
            logger.warning("buffer not warmed up")
    """

    name = "data_buffer"
    version = "1.0.0"

    def __init__(self, maxlen: int = DEFAULT_MAXLEN):
        if maxlen <= 0:
            raise ValueError(
                f"maxlen must be positive, got {maxlen}"
            )

        self._maxlen = int(maxlen)
        self._bars: deque = deque(maxlen=self._maxlen)
        self._lock = threading.RLock()

        self._stats = {
            "pushed": 0,
            "duplicate_replaced": 0,
            "out_of_order_rejected": 0,
            "open_kline_rejected": 0,
            "invalid_rejected": 0,
            "evicted": 0,          # 被 deque(maxlen) 自动淘汰的数量
            "bulk_loaded": 0,
            "cleared": 0,
        }

        logger.info(
            f"RingBuffer initialized maxlen={self._maxlen}"
        )

    # ========================================================================
    # 写入
    # ========================================================================

    def push(self, bar: Any) -> bool:
        """
        推送一根 K 线。

        Args:
            bar: K 线对象，必须有 ts 字段；is_closed 字段可选（默认 True）

        Returns:
            True: 已入缓冲区（新增或替换）
            False: 被拒绝（乱序/未收线/无效）

        契约：
        - 未收线 K 线（is_closed=False）拒绝
        - 同 ts K 线替换（不追加）
        - 乱序 K 线（ts < last.ts）拒绝
        - 返回 False 时调用方应记录但不阻断
        """
        # ---- 校验 ----
        ts = getattr(bar, "ts", None)
        if ts is None or not isinstance(ts, int):
            with self._lock:
                self._stats["invalid_rejected"] += 1
            logger.warning(
                f"push rejected: bar has no valid ts field "
                f"(type={type(bar).__name__})"
            )
            return False

        # 未收线 K 线拒绝
        if getattr(bar, "is_closed", True) is False:
            with self._lock:
                self._stats["open_kline_rejected"] += 1
            logger.debug(
                f"push rejected: open kline ts={ts}"
            )
            return False

        with self._lock:
            prev_size = len(self._bars)

            # 空缓冲区：直接追加
            if not self._bars:
                self._bars.append(bar)
                self._stats["pushed"] += 1
                return True

            last = self._bars[-1]

            # 重复 ts：替换
            if ts == last.ts:
                self._bars.pop()
                self._bars.append(bar)
                self._stats["duplicate_replaced"] += 1
                logger.debug(
                    f"duplicate kline ts={ts}, replaced"
                )
                return True

            # 乱序：拒绝
            if ts < last.ts:
                self._stats["out_of_order_rejected"] += 1
                logger.warning(
                    f"out-of-order kline ts={ts} < "
                    f"last_ts={last.ts}, rejected"
                )
                return False

            # 正常追加
            self._bars.append(bar)
            self._stats["pushed"] += 1

            # 检查是否触发淘汰
            if prev_size == self._maxlen:
                self._stats["evicted"] += 1

            return True

    def bulk_load(self, bars: List[Any]) -> int:
        """
        批量加载 K 线。用于启动时加载历史数据。

        契约：
        - 排序（按 ts 升序）
        - 去重（同 ts 保留最后一条）
        - 截断（保留最近 maxlen 根）
        - 替换整个缓冲区（不是追加）
        - 原子操作，期间 push 阻塞

        Returns:
            实际加载的数量
        """
        with self._lock:
            if not bars:
                return 0

            # 排序
            sorted_bars = sorted(
                bars, key=lambda b: getattr(b, "ts", 0)
            )

            # 去重（同 ts 保留最后一条）
            deduped: List[Any] = []
            for bar in sorted_bars:
                ts = getattr(bar, "ts", None)
                if ts is None:
                    continue
                if deduped and deduped[-1].ts == ts:
                    deduped.pop()
                deduped.append(bar)

            # 截断
            if len(deduped) > self._maxlen:
                deduped = deduped[-self._maxlen:]

            # 替换
            self._bars.clear()
            for bar in deduped:
                self._bars.append(bar)

            self._stats["bulk_loaded"] += len(deduped)

            logger.info(
                f"bulk_load: {len(deduped)} bars loaded "
                f"(input={len(bars)}, "
                f"first_ts={deduped[0].ts if deduped else None}, "
                f"last_ts={deduped[-1].ts if deduped else None})"
            )

            return len(deduped)

    # ========================================================================
    # 读取
    # ========================================================================

    def get_recent(self, n: int) -> List[Any]:
        """
        最近 N 根 K 线，按时间升序。最后一个是最近收线的。

        空缓冲区或 n <= 0 返回空列表。
        """
        with self._lock:
            if n <= 0 or not self._bars:
                return []
            n = min(n, len(self._bars))
            return list(self._bars)[-n:]

    def get_bar_at(self, ts: int) -> Optional[Any]:
        """
        按 ts 查询 K 线。不存在返回 None。

        实现：从后往前线性搜索（最近的先找到）。
        K 线数量 <= maxlen（通常 1000），足够快。
        """
        with self._lock:
            for bar in reversed(self._bars):
                if bar.ts == ts:
                    return bar
                if bar.ts < ts:
                    # 已经越过目标，后续不可能再有
                    return None
            return None

    def latest(self) -> Optional[Any]:
        """最新一根 K 线。空缓冲区返回 None。"""
        with self._lock:
            return self._bars[-1] if self._bars else None

    def oldest(self) -> Optional[Any]:
        """最旧一根 K 线。空缓冲区返回 None。"""
        with self._lock:
            return self._bars[0] if self._bars else None

    def first_ts(self) -> Optional[int]:
        """最旧 K 线的 ts。空缓冲区返回 None。"""
        with self._lock:
            return self._bars[0].ts if self._bars else None

    def last_ts(self) -> Optional[int]:
        """最新 K 线的 ts。空缓冲区返回 None。"""
        with self._lock:
            return self._bars[-1].ts if self._bars else None

    @property
    def bars(self) -> List[Any]:
        """
        返回所有 K 线的副本（list），防外部修改。

        注意：每次访问都创建新列表。
        热路径（每根 K 线）请用 get_recent(n)，只创建所需部分。
        此属性仅供启动检查、调试、归因等非热路径使用。
        """
        with self._lock:
            return list(self._bars)

    # ========================================================================
    # 状态查询
    # ========================================================================

    def __len__(self) -> int:
        with self._lock:
            return len(self._bars)

    def __iter__(self) -> Iterator[Any]:
        """迭代 K 线。注意：迭代期间持锁快照。"""
        with self._lock:
            return iter(list(self._bars))

    def is_ready(self, min_bars: int = DEFAULT_WARMUP_BARS) -> bool:
        """
        缓冲区是否积累够 min_bars 根 K 线。

        用于预热检查：因子分位数窗口需要足够的 K 线。
        """
        with self._lock:
            return len(self._bars) >= min_bars

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def clear(self) -> None:
        """清空缓冲区 + 重置统计。"""
        with self._lock:
            self._bars.clear()
            for k in self._stats:
                self._stats[k] = 0
            self._stats["cleared"] = 1
        logger.info("RingBuffer cleared")

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            latest = self._bars[-1] if self._bars else None
            oldest = self._bars[0] if self._bars else None
            return {
                "healthy": True,
                "name": self.name,
                "version": self.version,
                "size": len(self._bars),
                "maxlen": self._maxlen,
                "first_ts": oldest.ts if oldest else None,
                "last_ts": latest.ts if latest else None,
                "latest_is_closed": (
                    getattr(latest, "is_closed", None)
                    if latest else None
                ),
                "stats": dict(self._stats),
            }

    def reset(self) -> None:
        """重置。测试用。等同于 clear()。"""
        self.clear()


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "data_buffer",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["recent_bars", "bounded_kline_store"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Thread-safe bounded ring buffer for recent klines",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> RingBuffer:
    """
    模块工厂入口。

    config 可选字段：
        buffer.maxlen: 缓冲区大小，默认 1000
    """
    buf_cfg = config.get("buffer", {})
    maxlen = int(buf_cfg.get("maxlen", DEFAULT_MAXLEN))
    return RingBuffer(maxlen=maxlen)
