"""
system/kline_repair.py — K 线缺口修复

处理"系统失忆的那段时间"。

场景：
1. WebSocket 断线 30 秒，本地 buffer 停在 14:20，交易所已到 14:30，缺 3 根 K 线
2. 进程重启，buffer 为空，用 HistoricalLoader 预热（不走本模块）
3. 定时检查，发现缺口主动补

核心职责：
1. 精确计算缺口起止（不开区间外）
2. 通过 REST 拉取缺失的 K 线
3. 过滤未收线 K 线
4. 过滤区间外 K 线
5. 返回 KlineData 列表（调用方负责推入 buffer）
6. 可选持久化到 TickDB

职责边界：
- KlineRepair：检测缺口 + 拉取 + 返回
- KlineAuthority：接收 KlineData，推入自己的 buffer
- HistoricalLoader：启动时的批量预热（一次加载 500 根，不走本模块）
- main.py / 状态机：决定何时触发修复

不做什么：
- 不推入 buffer（返回给调用方）
- 不触发状态机（修复的 K 线只用于历史）
- 不重试（RestClient 内部已重试）
- 不修复快照/成交（只修 K 线）

缺口检测算法：
    interval_ms = 解析(interval)
    expected_last_closed_ts = ((now_ms - interval_ms) // interval_ms) * interval_ms
    if last_local_ts is None: 无缺口（空 buffer）
    if expected_last_closed_ts <= last_local_ts: 无缺口
    gap_start = last_local_ts + interval_ms
    gap_end = expected_last_closed_ts + interval_ms  # 排他
    gap_bars = (gap_end - gap_start) // interval_ms

    示例（interval=3min，now=14:24:30）：
    - last_local_ts=14:21:00 → expected=14:21:00 → 无缺口
    - last_local_ts=14:18:00 → expected=14:21:00 → gap=[14:21, 14:24) 1 根
    - last_local_ts=14:15:00 → expected=14:21:00 → gap=[14:18, 14:24) 2 根

    为什么是 now - interval？
    因为在 now=14:24:30 时，14:24 那根 K 线还没收线（收盘在 14:27:00），
    14:21 那根刚收线 30 秒（收盘在 14:24:00）。
    所以最后已收线的 K 线 ts = ((now - interval) // interval) * interval。

关键契约：
1. 重入保护：正在修复时返回 IN_PROGRESS
2. 只返回已收线 K 线（ts + interval_ms <= now_ms）
3. 只返回区间内 K 线（start_ts <= ts < end_ts）
4. 返回列表按 ts 升序
5. 单根 K 线非法跳过，继续其他
6. 拉取失败返回 FETCH_ERROR，不重试
7. 缺口超过 max_repair_bars 返回 TOO_LARGE
8. 修复的 K 线 source="rest"，is_complete=False
9. 可选持久化到 TickDB，失败只打 WARN
10. 连续失败 3 次打 CRITICAL

修复触发时机（推荐）：
1. WebSocket 重连成功后立即调一次
2. 之后每 60 秒调一次，直到 NO_GAP
3. 手动触发（前端按钮）
4. 不要每根 K 线都调（修复是低频操作）

调用示例：
    repair = KlineRepair(rest, tickdb, config)

    # WebSocket 重连后
    last = kline_authority.get_latest_bar()
    last_ts = last.ts if last else None
    result = repair.repair(
        symbol="btcusdt",
        last_local_ts=last_ts,
        now_ms=int(time.time() * 1000),
        interval_str="3m",
    )
    if result.status == RepairStatus.REPAIRED:
        for bar in result.klines:
            kline_authority.on_exchange_kline_raw(bar)  # 或直接推 buffer
    elif result.status == RepairStatus.TOO_LARGE:
        logger.critical("gap too large, manual intervention required")

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
import logging
import threading
import time
from typing import Any, List, Optional

from system.kline_authority import KlineData

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_REPAIR_BARS = 200
DEFAULT_INTERVAL = "3m"
CONSECUTIVE_FAILURE_WARN = 3
INTERVAL_MS = {
    "1m": 60 * 1000,
    "3m": 3 * 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}
MAX_REST_KLINES = 1500


# ============================================================================
# 错误
# ============================================================================
class KlineRepairError(Exception):
    """K 线修复错误。"""
    pass


# ============================================================================
# 修复状态
# ============================================================================
class RepairStatus(str, Enum):
    REPAIRED = "repaired"
    NO_GAP = "no_gap"
    NO_BUFFER = "no_buffer"
    IN_PROGRESS = "in_progress"
    TOO_LARGE = "too_large"
    FETCH_ERROR = "fetch_error"


# ============================================================================
# 缺口信息
# ============================================================================
@dataclass(frozen=True)
class GapInfo:
    """缺口信息。"""
    start_ts: int                    # 缺口起始（含）
    end_ts: int                      # 缺口结束（不含）
    bars: int                        # 缺失 K 线数量
    duration_ms: int                 # 缺口时长

    def to_dict(self) -> dict:
        return {
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "bars": self.bars,
            "duration_ms": self.duration_ms,
        }


# ============================================================================
# 修复结果
# ============================================================================
@dataclass
class RepairResult:
    """修复结果。"""
    status: RepairStatus
    gap: Optional[GapInfo] = None
    klines: List[KlineData] = field(default_factory=list)
    error: Optional[str] = None
    fetch_ms: float = 0.0

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "gap": self.gap.to_dict() if self.gap else None,
            "klines_count": len(self.klines),
            "error": self.error,
            "fetch_ms": self.fetch_ms,
        }


# ============================================================================
# KlineRepair
# ============================================================================
class KlineRepair:
    """
    K 线缺口修复。详见模块 docstring。
    """

    name = "kline_repair"
    version = "1.0.0"

    def __init__(
        self,
        rest: Any,
        tickdb: Optional[Any] = None,
        config: Optional[dict] = None,
    ):
        if rest is None:
            raise KlineRepairError("rest client is required")

        self._rest = rest
        self._tickdb = tickdb

        cfg = (config or {}).get("kline_repair", {})
        data_cfg = (config or {}).get("data", {})

        self.max_repair_bars = int(
            cfg.get("max_repair_bars", DEFAULT_MAX_REPAIR_BARS)
        )
        self.default_interval = data_cfg.get(
            "kline_interval", DEFAULT_INTERVAL
        )

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._repairing = False
        self._consecutive_failures = 0

        # ---- 统计 ----
        self._stats = {
            "repairs_attempted": 0,
            "repairs_succeeded": 0,
            "bars_repaired": 0,
            "no_gap_count": 0,
            "no_buffer_count": 0,
            "in_progress_count": 0,
            "too_large_count": 0,
            "fetch_errors": 0,
            "invalid_klines_skipped": 0,
            "persist_failures": 0,
        }
        self._last_error: Optional[str] = None

        logger.info(
            f"KlineRepair initialized "
            f"max_repair_bars={self.max_repair_bars} "
            f"default_interval={self.default_interval} "
            f"tickdb={'enabled' if tickdb else 'disabled'}"
        )

    # ========================================================================
    # 缺口检测
    # ========================================================================

    def detect_gap(
        self,
        last_local_ts: Optional[int],
        now_ms: int,
        interval_ms: int,
    ) -> Optional[GapInfo]:
        """
        检测缺口。

        Args:
            last_local_ts: 本地最后已收线 K 线的 ts（None 表示空 buffer）
            now_ms: 当前时间（毫秒）
            interval_ms: K 线间隔（毫秒）

        Returns:
            GapInfo（有缺口）或 None（无缺口）

        契约：
        - last_local_ts 为 None → None（无缺口）
        - expected_last_closed_ts <= last_local_ts → None（无缺口）
        - 否则返回 GapInfo
        """
        if interval_ms <= 0:
            raise KlineRepairError(
                f"interval_ms must be > 0, got {interval_ms}"
            )
        if last_local_ts is None:
            return None
        if now_ms <= last_local_ts:
            return None

        # 最后已收线 K 线的 ts
        expected_last_closed_ts = (
            (now_ms - interval_ms) // interval_ms
        ) * interval_ms

        if expected_last_closed_ts <= last_local_ts:
            return None

        gap_start = last_local_ts + interval_ms
        gap_end = expected_last_closed_ts + interval_ms
        gap_bars = (gap_end - gap_start) // interval_ms

        if gap_bars <= 0:
            return None

        return GapInfo(
            start_ts=gap_start,
            end_ts=gap_end,
            bars=gap_bars,
            duration_ms=gap_end - gap_start,
        )

    # ========================================================================
    # 主入口：修复
    # ========================================================================

    def repair(
        self,
        symbol: str,
        last_local_ts: Optional[int],
        now_ms: int,
        interval_str: Optional[str] = None,
    ) -> RepairResult:
        """
        修复缺口。返回的 K 线由调用方推入 buffer。

        Args:
            symbol: 交易对
            last_local_ts: 本地最后已收线 K 线的 ts（None 表示空 buffer）
            now_ms: 当前时间（毫秒）
            interval_str: K 线间隔（默认从 config 读）

        Returns:
            RepairResult

        契约：
        - 重入保护：正在修复时返回 IN_PROGRESS
        - 空 buffer 返回 NO_BUFFER
        - 无缺口返回 NO_GAP
        - 缺口太大返回 TOO_LARGE
        - 拉取失败返回 FETCH_ERROR
        - 成功返回 REPAIRED + klines 列表
        """
        with self._lock:
            if self._repairing:
                self._stats["in_progress_count"] += 1
                return RepairResult(status=RepairStatus.IN_PROGRESS)

            self._repairing = True
            self._stats["repairs_attempted"] += 1

        try:
            return self._do_repair(
                symbol, last_local_ts, now_ms, interval_str
            )
        finally:
            with self._lock:
                self._repairing = False

    def _do_repair(
        self,
        symbol: str,
        last_local_ts: Optional[int],
        now_ms: int,
        interval_str: Optional[str],
    ) -> RepairResult:
        # ---- 1. 空 buffer ----
        if last_local_ts is None:
            self._stats["no_buffer_count"] += 1
            return RepairResult(status=RepairStatus.NO_BUFFER)

        # ---- 2. 解析 interval ----
        interval_str = interval_str or self.default_interval
        try:
            interval_ms = self._parse_interval_ms(interval_str)
        except KlineRepairError as e:
            logger.error(f"invalid interval {interval_str}: {e}")
            return RepairResult(
                status=RepairStatus.FETCH_ERROR,
                error=str(e),
            )

        # ---- 3. 检测缺口 ----
        try:
            gap = self.detect_gap(last_local_ts, now_ms, interval_ms)
        except Exception as e:
            logger.error(f"detect_gap failed: {e}", exc_info=True)
            return RepairResult(
                status=RepairStatus.FETCH_ERROR,
                error=str(e),
            )

        if gap is None:
            self._stats["no_gap_count"] += 1
            return RepairResult(status=RepairStatus.NO_GAP)

        # ---- 4. 缺口大小检查 ----
        if gap.bars > self.max_repair_bars:
            self._stats["too_large_count"] += 1
            logger.critical(
                f"gap too large: {gap.bars} bars > "
                f"max_repair_bars {self.max_repair_bars}, "
                f"symbol={symbol} gap={gap.to_dict()}"
            )
            return RepairResult(
                status=RepairStatus.TOO_LARGE,
                gap=gap,
                error=f"gap {gap.bars} bars exceeds max {self.max_repair_bars}",
            )

        # ---- 5. 拉取 ----
        logger.info(
            f"repairing gap: symbol={symbol} "
            f"gap_bars={gap.bars} "
            f"range=[{gap.start_ts}, {gap.end_ts})"
        )
        t0 = time.perf_counter()

        try:
            klines = self._fetch_and_parse(
                symbol, gap, interval_ms, interval_str, now_ms
            )
        except Exception as e:
            fetch_ms = (time.perf_counter() - t0) * 1000
            self._stats["fetch_errors"] += 1
            self._consecutive_failures += 1
            self._last_error = str(e)
            logger.error(
                f"fetch failed: symbol={symbol} gap={gap.to_dict()} "
                f"error={e}",
                exc_info=True,
            )
            if self._consecutive_failures >= CONSECUTIVE_FAILURE_WARN:
                logger.critical(
                    f"consecutive repair failures: "
                    f"{self._consecutive_failures}"
                )
            return RepairResult(
                status=RepairStatus.FETCH_ERROR,
                gap=gap,
                error=str(e),
                fetch_ms=fetch_ms,
            )

        fetch_ms = (time.perf_counter() - t0) * 1000
        self._consecutive_failures = 0

        # ---- 6. 持久化（可选）----
        if self._tickdb is not None and klines:
            self._persist_klines(klines)

        # ---- 7. 更新统计 ----
        self._stats["repairs_succeeded"] += 1
        self._stats["bars_repaired"] += len(klines)

        logger.info(
            f"repair succeeded: symbol={symbol} "
            f"repaired={len(klines)} bars "
            f"(expected {gap.bars}) fetch_ms={fetch_ms:.1f}"
        )

        if len(klines) < gap.bars:
            logger.warning(
                f"repair incomplete: got {len(klines)} of "
                f"{gap.bars} expected bars"
            )

        return RepairResult(
            status=RepairStatus.REPAIRED,
            gap=gap,
            klines=klines,
            fetch_ms=fetch_ms,
        )

    # ========================================================================
    # 拉取与解析
    # ========================================================================

    def _fetch_and_parse(
        self,
        symbol: str,
        gap: GapInfo,
        interval_ms: int,
        interval_str: str,
        now_ms: int,
    ) -> List[KlineData]:
        """
        通过 REST 拉取并解析 K 线。

        契约：
        - 单根非法跳过，继续其他
        - 返回按 ts 升序
        - 只返回区间内已收线 K 线
        """
        limit = min(self.max_repair_bars + 10, MAX_REST_KLINES)
        # 币安 endTime 是排他的，传 gap.end_ts - 1
        raw = self._rest.get_klines(
            symbol=symbol.upper(),
            interval=interval_str,
            start_ts=gap.start_ts,
            end_ts=gap.end_ts - 1,
            limit=limit,
        )

        klines: List[KlineData] = []
        local_ts = int(time.time() * 1000)

        for item in raw or []:
            try:
                ts = int(item[0])
            except (IndexError, ValueError, TypeError):
                self._stats["invalid_klines_skipped"] += 1
                continue

            # 区间过滤
            if ts < gap.start_ts or ts >= gap.end_ts:
                continue

            # 未收线过滤
            if ts + interval_ms > now_ms:
                continue

            # 解析 OHLCV
            try:
                open_p = Decimal(str(item[1]))
                high_p = Decimal(str(item[2]))
                low_p = Decimal(str(item[3]))
                close_p = Decimal(str(item[4]))
                volume = Decimal(str(item[5]))
            except (IndexError, InvalidOperation, TypeError):
                self._stats["invalid_klines_skipped"] += 1
                continue

            # 构造 KlineData
            try:
                bar = KlineData(
                    symbol=symbol.upper(),
                    ts=ts,
                    open=open_p,
                    high=high_p,
                    low=low_p,
                    close=close_p,
                    volume=volume,
                    is_closed=True,
                    exchange_event_ts=ts + interval_ms,
                    local_receive_ts=local_ts,
                    source="rest",
                    is_complete=False,
                )
                klines.append(bar)
            except Exception as e:
                self._stats["invalid_klines_skipped"] += 1
                logger.debug(f"invalid kline ts={ts}: {e}")
                continue

        # 排序
        klines.sort(key=lambda k: k.ts)
        return klines

    def _persist_klines(self, klines: List[KlineData]) -> None:
        """持久化到 TickDB。失败只打 WARN。"""
        persisted = 0
        for bar in klines:
            try:
                self._tickdb.insert_kline(
                    symbol=bar.symbol,
                    ts=bar.ts,
                    open=str(bar.open),
                    high=str(bar.high),
                    low=str(bar.low),
                    close=str(bar.close),
                    volume=str(bar.volume),
                    is_closed=True,
                )
                persisted += 1
            except Exception as e:
                self._stats["persist_failures"] += 1
                logger.warning(
                    f"persist repair kline failed ts={bar.ts}: {e}"
                )
        if persisted > 0:
            logger.debug(
                f"persisted {persisted} repair klines to tick_db"
            )

    # ========================================================================
    # 辅助
    # ========================================================================

    def _parse_interval_ms(self, interval: str) -> int:
        """解析 interval 为毫秒。"""
        if interval not in INTERVAL_MS:
            raise KlineRepairError(
                f"unsupported interval: {interval}, "
                f"supported: {list(INTERVAL_MS.keys())}"
            )
        return INTERVAL_MS[interval]

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> dict:
        """健康检查。"""
        with self._lock:
            return {
                "healthy": self._consecutive_failures < CONSECUTIVE_FAILURE_WARN,
                "name": self.name,
                "version": self.version,
                "repairing": self._repairing,
                "consecutive_failures": self._consecutive_failures,
                "last_error": self._last_error,
                "stats": dict(self._stats),
                "timestamp": int(time.time() * 1000),
            }

    def reset_stats(self) -> None:
        """重置统计。测试用。"""
        with self._lock:
            for k in self._stats:
                self._stats[k] = 0
            self._consecutive_failures = 0
            self._last_error = None


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "kline_repair",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["kline_repair", "gap_detection"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Kline gap repair via REST with reentrancy protection and gap size limits",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> KlineRepair:
    """
    从配置创建实例。

    config 需要：
    - instances.rest_client: RestClient

    config 可选：
    - instances.tick_db: TickDB
    - kline_repair.max_repair_bars: 默认 200
    - data.kline_interval: 默认 "3m"
    """
    instances = config.get("instances", {})
    rest = instances.get("rest_client")
    if rest is None:
        raise KlineRepairError(
            "KlineRepair.create requires config.instances.rest_client"
        )
    tickdb = instances.get("tick_db")
    return KlineRepair(rest=rest, tickdb=tickdb, config=config)
