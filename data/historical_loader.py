"""
data/historical_loader.py — 历史 K 线加载器（REST）

从币安 REST 拉取历史 K 线，用于：
1. 预热：启动时加载最近 N 根 K 线
2. 补拉：断线后补拉缺失区间（由 KlineAuthority 触发，但实际调用通过 RestClient）
3. 回测数据准备：可选写入 TickDB

设计原则：
1. 复用 RestClient：限频、错误映射、重试都在 RestClient 内部
2. 分页：币安单次最多 1500 根，自动分页
3. 未收线过滤：ts + interval_ms > now_ms 的 K 线丢弃
4. 时间对齐：按 K 线开盘时间
5. 去重：按 ts 去重
6. 排序：按 ts 升序返回
7. 数据校验：OHLCV 合法，非法丢弃
8. 缺口记录：中间缺口不填补，记录统计
9. 范围保护：max_range_days / max_klines_per_call
10. 无缓存：每次调用重新拉取（调用方控制）

职责边界：
- HistoricalLoader：拉取、校验、返回 KlineData 列表
- RestClient：HTTP、限频、错误映射、重试
- KlineAuthority：接收 KlineData，管缓冲、修复
- main.py：调用 loader 预热
- 回测：从 TickDB 加载，不用 loader

关键契约：
1. 返回 List[KlineData]，按 ts 升序
2. 所有 KlineData 的 is_closed=True（已收线）
3. 未收线的 K 线（ts + interval_ms > now_ms）丢弃
4. 范围超出 max_range_days 抛 LoaderError
5. 数量超出 max_klines_per_call 抛 LoaderError
6. 单页失败在 strict=False 时记录并继续，strict=True 时抛
7. 中间缺口记录到 stats，不填补
8. min_required 参数：返回数量低于阈值抛 LoaderError
9. 不缓存（每次调用重新拉）
10. REST 异常（GatewayError）向上抛，由调用方处理

分页算法：
    cursor = 对齐后的 start_ts
    while cursor < end_ts:
        page_end = min(cursor + 1500 * interval_ms + 1, end_ts + 1)
        raw = rest.get_klines(symbol, interval, cursor, page_end, limit=1500)
        for item in raw:
            if start_ts <= item.ts < end_ts:
                校验 + 未收线过滤 + 转 KlineData
        cursor = last_ts + interval_ms
        progress_cb(page, total, loaded)

    为什么 page_end = cursor + 1500 * interval_ms + 1？
    币安 endTime 包含/排他不确定，+1 保证边界 K 线不漏，
    本地过滤 ts < end_ts 保证不超。

    为什么 cursor = last_ts + interval_ms？
    币安返回的 K 线按 openTime 排序，last_ts 是最后一根的开盘时间，
    下一根开盘时间 = last_ts + interval_ms。

与 KlineAuthority.repair_klines 的关系：
- repair_klines 直接调 rest.get_klines，不走 loader
- 原因：repair 需要精细控制，且只拉少量 K 线（最多 200 根）
- loader 适合大批量拉取（预热、回测准备）

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from decimal import Decimal, InvalidOperation
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from system.kline_authority import KlineData

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
PAGE_SIZE = 1500                     # 币安单次请求上限
DEFAULT_MAX_RANGE_DAYS = 90          # 单次拉取最大天数
DEFAULT_MAX_KLINES_PER_CALL = 100000  # 单次拉取最大 K 线数
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


# ============================================================================
# 错误
# ============================================================================
class LoaderError(Exception):
    """加载器错误。"""
    pass


# ============================================================================
# HistoricalLoader
# ============================================================================
class HistoricalLoader:
    """
    历史 K 线加载器。详见模块 docstring。

    用法：
        from data.rest_client import RestClient

        rest = RestClient(...)
        loader = HistoricalLoader(rest)

        # 加载最近 500 根 K 线
        bars = loader.load_recent_klines("btcusdt", "3m", 500)
        for bar in bars:
            buffer.push(bar)

        # 加载指定区间
        bars = loader.load_klines(
            "btcusdt", "3m",
            start_ts=1700000000000,
            end_ts=1705000000000,
        )

        # 加载并持久化到 TickDB
        bars = loader.load_and_persist(
            "btcusdt", "3m",
            start_ts=..., end_ts=...,
            tick_db=tick_db,
        )

        # 健康检查
        loader.health_check()
    """

    name = "historical_loader"
    version = "1.0.0"

    def __init__(
        self,
        rest_client: Any,
        config: Optional[dict] = None,
    ):
        if rest_client is None:
            raise LoaderError("rest_client is required")

        self._rest = rest_client
        cfg = (config or {}).get("historical_loader", {})

        self.max_range_days = int(
            cfg.get("max_range_days", DEFAULT_MAX_RANGE_DAYS)
        )
        self.max_klines_per_call = int(
            cfg.get("max_klines_per_call", DEFAULT_MAX_KLINES_PER_CALL)
        )

        # ---- 统计 ----
        self._stats = {
            "calls": 0,
            "pages": 0,
            "klines_loaded": 0,
            "gaps_detected": 0,
            "invalid_klines": 0,
            "open_klines_dropped": 0,
            "range_errors": 0,
        }
        self._last_error: Optional[str] = None

        logger.info(
            f"HistoricalLoader initialized "
            f"max_range_days={self.max_range_days} "
            f"max_klines_per_call={self.max_klines_per_call}"
        )

    # ========================================================================
    # 公共方法
    # ========================================================================

    def load_recent_klines(
        self,
        symbol: str,
        interval: str,
        limit: int,
        *,
        min_required: Optional[int] = None,
        strict: bool = False,
    ) -> List[KlineData]:
        """
        加载最近 N 根已收线 K 线。

        Args:
            symbol: 交易对（大小写均可）
            interval: K 线周期（3m / 5m / 1h 等）
            limit: 期望加载数量
            min_required: 最小返回数量，低于则抛异常（None 则不检查）
            strict: True 时，单页失败抛异常；False 时记录并继续

        Returns:
            List[KlineData]，按 ts 升序，可能少于 limit（数据缺口）
        """
        if limit <= 0:
            raise LoaderError(f"limit must be positive, got {limit}")
        if limit > self.max_klines_per_call:
            raise LoaderError(
                f"limit {limit} > max_klines_per_call "
                f"{self.max_klines_per_call}"
            )

        interval_ms = self._parse_interval_ms(interval)
        now_ms = int(time.time() * 1000)

        # 计算 [start, end] 区间，留出 10% 余量
        # （缺口可能导致返回不足 limit 根，10% 余量提供缓冲）
        total_ms = limit * interval_ms * 11 // 10

        # 范围保护
        max_range_ms = self.max_range_days * 86400 * 1000
        if total_ms > max_range_ms:
            raise LoaderError(
                f"requested range {total_ms // 86400000}d > "
                f"max_range_days {self.max_range_days}"
            )

        start_ts = now_ms - total_ms
        end_ts = now_ms

        bars = self.load_klines(
            symbol, interval, start_ts, end_ts,
            strict=strict,
        )

        # 截取最后 limit 根
        if len(bars) > limit:
            bars = bars[-limit:]

        if min_required is not None and len(bars) < min_required:
            raise LoaderError(
                f"loaded only {len(bars)} of {limit} required "
                f"(min_required={min_required})"
            )

        return bars

    def load_klines(
        self,
        symbol: str,
        interval: str,
        start_ts: int,
        end_ts: int,
        *,
        progress_cb: Optional[Callable[[int, int, int], None]] = None,
        strict: bool = False,
    ) -> List[KlineData]:
        """
        加载 [start_ts, end_ts) 区间的 K 线。

        Args:
            symbol: 交易对
            interval: K 线周期
            start_ts: 起始时间戳（含）
            end_ts: 结束时间戳（不含）
            progress_cb: (page, total_pages, loaded) 进度回调
            strict: True 时，单页失败抛异常；False 时记录并继续

        Returns:
            List[KlineData]，按 ts 升序，可能有缺口

        契约：
        - 包含开盘时间 >= start_ts 且 < end_ts 的 K 线
        - 未收线的 K 线丢弃
        - 范围超出 max_range_days 抛 LoaderError
        - 数量超出 max_klines_per_call 抛 LoaderError
        """
        self._stats["calls"] += 1

        # 参数校验
        if not symbol:
            raise LoaderError("symbol is required")
        if interval not in INTERVAL_MS:
            raise LoaderError(
                f"unsupported interval: {interval}, "
                f"supported: {list(INTERVAL_MS.keys())}"
            )
        if start_ts >= end_ts:
            raise LoaderError(
                f"start_ts {start_ts} >= end_ts {end_ts}"
            )

        interval_ms = INTERVAL_MS[interval]
        symbol_upper = symbol.upper()

        # 范围保护
        range_ms = end_ts - start_ts
        max_range_ms = self.max_range_days * 86400 * 1000
        if range_ms > max_range_ms:
            self._stats["range_errors"] += 1
            raise LoaderError(
                f"range {range_ms // 86400000}d > "
                f"max_range_days {self.max_range_days}"
            )

        expected_klines = range_ms // interval_ms
        if expected_klines > self.max_klines_per_call:
            self._stats["range_errors"] += 1
            raise LoaderError(
                f"expected {expected_klines} klines > "
                f"max_klines_per_call {self.max_klines_per_call}"
            )

        # 对齐 start_ts 到 interval_ms 的整数倍（向上对齐）
        aligned_start = (
            (start_ts + interval_ms - 1) // interval_ms
        ) * interval_ms

        if aligned_start >= end_ts:
            logger.warning(
                f"aligned_start {aligned_start} >= end_ts {end_ts}, "
                f"returning empty"
            )
            return []

        now_ms = int(time.time() * 1000)
        all_bars: List[KlineData] = []
        seen_ts: set = set()
        cursor = aligned_start
        page = 0
        total_pages = max(
            1,
            (
                (end_ts - aligned_start)
                + (PAGE_SIZE * interval_ms) - 1
            ) // (PAGE_SIZE * interval_ms),
        )

        while cursor < end_ts:
            page += 1
            # page_end 加 1 保证边界 K 线不漏
            page_end = min(
                cursor + PAGE_SIZE * interval_ms + 1,
                end_ts + 1,
            )

            try:
                raw = self._rest.get_klines(
                    symbol=symbol_upper,
                    interval=interval,
                    start_ts=cursor,
                    end_ts=page_end,
                    limit=PAGE_SIZE,
                )
            except Exception as e:
                self._last_error = str(e)
                self._stats["errors"] += 1
                if strict:
                    raise LoaderError(
                        f"page {page} failed: {e}"
                    ) from e
                logger.error(
                    f"page {page} failed (cursor={cursor}): {e}, "
                    f"continuing"
                )
                # 跳过该页，前进 cursor
                cursor = page_end - 1
                continue

            if not raw:
                # 空页 - 数据缺口
                gap_ms = (page_end - cursor) // interval_ms
                self._stats["gaps_detected"] += 1
                logger.warning(
                    f"gap detected: cursor={cursor} "
                    f"page_end={page_end} "
                    f"(~{gap_ms} klines missing)"
                )
                cursor = page_end - 1
                continue

            # 解析该页
            page_klines = 0
            for item in raw:
                try:
                    ts = int(item[0])
                except (IndexError, ValueError, TypeError):
                    self._stats["invalid_klines"] += 1
                    continue

                # 过滤：开盘时间必须在 [start_ts, end_ts) 内
                if ts < start_ts or ts >= end_ts:
                    continue

                # 去重
                if ts in seen_ts:
                    continue

                # 未收线过滤
                if ts + interval_ms > now_ms:
                    self._stats["open_klines_dropped"] += 1
                    continue

                # 转换为 KlineData
                kline = self._raw_to_kline(
                    symbol_upper, interval, item, interval_ms
                )
                if kline is None:
                    self._stats["invalid_klines"] += 1
                    continue

                all_bars.append(kline)
                seen_ts.add(ts)
                page_klines += 1

            self._stats["pages"] += 1
            self._stats["klines_loaded"] += page_klines

            # 推进 cursor
            try:
                last_ts = int(raw[-1][0])
            except (IndexError, ValueError, TypeError):
                logger.error(
                    f"cannot determine last_ts from page {page}, "
                    f"breaking to avoid infinite loop"
                )
                break

            if last_ts + interval_ms <= cursor:
                logger.error(
                    f"no progress: cursor={cursor} last_ts={last_ts}, "
                    f"breaking"
                )
                break

            cursor = last_ts + interval_ms

            if progress_cb is not None:
                try:
                    progress_cb(page, total_pages, len(all_bars))
                except Exception as e:
                    logger.error(f"progress_cb error: {e}")

        # 排序（防御性，理论上已经有序）
        all_bars.sort(key=lambda b: b.ts)

        logger.info(
            f"loaded {len(all_bars)} klines "
            f"symbol={symbol_upper} interval={interval} "
            f"range=[{start_ts}, {end_ts}) pages={page}"
        )

        return all_bars

    def load_and_persist(
        self,
        symbol: str,
        interval: str,
        start_ts: int,
        end_ts: int,
        tick_db: Any,
        *,
        progress_cb: Optional[Callable[[int, int, int], None]] = None,
    ) -> List[KlineData]:
        """
        加载并持久化到 TickDB。用于回测数据准备。

        Args:
            symbol, interval, start_ts, end_ts: 同 load_klines
            tick_db: TickDB 实例
            progress_cb: 进度回调

        Returns:
            加载的 K 线列表
        """
        if tick_db is None:
            raise LoaderError("tick_db is required")

        bars = self.load_klines(
            symbol, interval, start_ts, end_ts,
            progress_cb=progress_cb,
        )

        persisted = 0
        for bar in bars:
            try:
                tick_db.insert_kline(
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
                logger.warning(
                    f"failed to persist kline ts={bar.ts}: {e}"
                )

        logger.info(
            f"persisted {persisted}/{len(bars)} klines to tick_db"
        )
        return bars

    # ========================================================================
    # 内部
    # ========================================================================

    def _parse_interval_ms(self, interval: str) -> int:
        """解析 interval 为毫秒。"""
        if interval not in INTERVAL_MS:
            raise LoaderError(
                f"unsupported interval: {interval}, "
                f"supported: {list(INTERVAL_MS.keys())}"
            )
        return INTERVAL_MS[interval]

    def _raw_to_kline(
        self,
        symbol: str,
        interval: str,
        item: list,
        interval_ms: int,
    ) -> Optional[KlineData]:
        """
        将币安原始 K 线数组转换为 KlineData。

        币安格式：[openTime, open, high, low, close, volume, closeTime, ...]

        Returns:
            KlineData 或 None（校验失败）
        """
        try:
            ts = int(item[0])
            open_price = Decimal(str(item[1]))
            high_price = Decimal(str(item[2]))
            low_price = Decimal(str(item[3]))
            close_price = Decimal(str(item[4]))
            volume = Decimal(str(item[5]))
        except (IndexError, ValueError, InvalidOperation, TypeError):
            return None

        close_ts = ts + interval_ms
        local_ts = int(time.time() * 1000)

        try:
            return KlineData(
                symbol=symbol,
                ts=ts,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                is_closed=True,
                exchange_event_ts=close_ts,
                local_receive_ts=local_ts,
                source="rest",
                is_complete=True,
            )
        except Exception as e:
            logger.debug(
                f"invalid kline ts={ts}: {e}"
            )
            return None

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "stats": dict(self._stats),
            "last_error": self._last_error,
            "max_range_days": self.max_range_days,
            "max_klines_per_call": self.max_klines_per_call,
            "timestamp": int(time.time() * 1000),
        }

    def reset_stats(self) -> None:
        """重置统计。测试用。"""
        for k in self._stats:
            self._stats[k] = 0
        self._last_error = None


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "historical_loader",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["historical_data"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Historical kline loader via REST with pagination and gap detection",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> HistoricalLoader:
    """
    从配置创建实例。

    config 需要：
    - instances.rest_client: RestClient

    config 可选：
    - historical_loader.max_range_days: 默认 90
    - historical_loader.max_klines_per_call: 默认 100000
    """
    instances = config.get("instances", {})
    rest = instances.get("rest_client")
    if rest is None:
        raise LoaderError(
            "HistoricalLoader.create requires "
            "config.instances.rest_client"
        )
    return HistoricalLoader(rest_client=rest, config=config)
