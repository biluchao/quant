"""
system/kline_authority.py — K 线权威源（交易所 kline 流 + 盘口快照 + 成交）

K 线是策略的"食材"。食材不新鲜，菜就废了。

核心原则：
1. 交易所的 kline 流是 OHLC 的唯一权威
2. 收线判断用交易所的 `x` 字段，不用本地时钟
3. aggTrade 只供因子计算，不做 K 线聚合
4. depth 快照与 K 线收线对齐，供因子计算 OBI
5. 断线重连后通过 REST 修复缺失 K 线
6. 所有数据带时间戳，用于延迟监控和可重放

三条数据流：
- kline:  交易所 K 线（权威 OHLC）
- depth:  盘口增量（重建盘口快照，供 OBI 因子）
- aggTrade: 逐笔成交（供 DDE / 聪明钱因子）

对齐规则：
- K 线收线时触发回调，回调参数包含：
  * 收线的 K 线（KlineData）
  * 收线那一刻的盘口快照（dict 或 None）
  * 该 K 线周期内的成交列表（List[dict]）
- 因子层在回调中计算因子，用的正是这根 K 线的数据

关键契约：
1. KlineData 是不可变数据类，构造时校验 OHLC 合法性
2. 收线检测只看交易所 `x` 字段，不看本地时钟
3. 未收线 K 线不进决策链，只通过 on_open_kline_update 推前端
4. 同一 ts 的 K 线，收线版本覆盖未收线版本
5. ts < last_closed_ts 的 K 线丢弃并打 WARN（乱序）
6. 回调异常不阻断其他回调
7. 单个回调超过 50ms 打 WARN
8. 缓冲区满时淘汰最旧的（deque maxlen）
9. 修复 K 线时按 ts 去重，WS 优先
10. 修复失败不阻断数据流，标记 degraded

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional
import logging
import threading
import time

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_BUFFER_SIZE = 1000
DEFAULT_SNAPSHOT_BUFFER_SIZE = 1000
DEFAULT_TRADE_BUFFER_SIZE = 5000
DEFAULT_SNAPSHOT_MAX_AGE_MS = 3000
CALLBACK_WARN_MS = 50.0
KLINE_INTERVAL_MS = 3 * 60 * 1000  # 3 分钟
REPAIR_MAX_BARS = 200


# ============================================================================
# 错误
# ============================================================================
class KlineAuthorityError(Exception):
    """K 线权威源错误。"""
    pass


class InvalidKlineError(KlineAuthorityError):
    """K 线数据非法（OHLC 不合法、时间戳不合法）。"""
    pass


# ============================================================================
# K 线数据
# ============================================================================
@dataclass(frozen=True)
class KlineData:
    """
    K 线数据。不可变。

    所有字段在构造时校验。
    """
    symbol: str
    ts: int                         # K 线开盘时间（交易所时间，毫秒）
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    is_closed: bool                 # 交易所 `x` 字段
    exchange_event_ts: int          # 交易所推送事件时间
    local_receive_ts: int           # 本地接收时间
    source: str = "ws"              # "ws" / "rest" / "synthetic"
    is_complete: bool = True        # 是否完整（未因断线拼接）

    def __post_init__(self):
        """校验。"""
        if not self.symbol:
            raise InvalidKlineError("symbol is required")
        if self.ts < 0:
            raise InvalidKlineError(f"ts must be >= 0, got {self.ts}")
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise InvalidKlineError(
                f"OHLC must be positive: o={self.open} h={self.high} "
                f"l={self.low} c={self.close}"
            )
        if self.high < self.low:
            raise InvalidKlineError(
                f"high={self.high} < low={self.low}"
            )
        if self.high < self.open or self.high < self.close:
            raise InvalidKlineError(
                f"high={self.high} below open={self.open} or close={self.close}"
            )
        if self.low > self.open or self.low > self.close:
            raise InvalidKlineError(
                f"low={self.low} above open={self.open} or close={self.close}"
            )
        if self.volume < 0:
            raise InvalidKlineError(
                f"volume must be >= 0, got {self.volume}"
            )

    @property
    def close_ts(self) -> int:
        """K 线收盘时间戳。"""
        return self.ts + KLINE_INTERVAL_MS

    @property
    def freshness_ms(self) -> int:
        """数据新鲜度：本地接收时间 - 交易所事件时间。"""
        return max(0, self.local_receive_ts - self.exchange_event_ts)

    def to_dict(self) -> dict:
        """序列化。Decimal 转字符串。"""
        return {
            "symbol": self.symbol,
            "ts": self.ts,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "is_closed": self.is_closed,
            "exchange_event_ts": self.exchange_event_ts,
            "local_receive_ts": self.local_receive_ts,
            "source": self.source,
            "is_complete": self.is_complete,
            "freshness_ms": self.freshness_ms,
        }


# ============================================================================
# 盘口快照
# ============================================================================
@dataclass(frozen=True)
class SnapshotData:
    """盘口快照。"""
    ts: int                         # 快照时间戳（交易所时间）
    local_receive_ts: int           # 本地接收时间
    bids: tuple                     # ((price, qty), ...)
    asks: tuple                     # ((price, qty), ...)


# ============================================================================
# 健康状态
# ============================================================================
class AuthorityHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"           # 修复失败/回调慢/延迟大
    STALE = "stale"                 # 超过 2 个周期没新 K 线
    FAILED = "failed"               # 无法恢复


# ============================================================================
# KlineAuthority
# ============================================================================
class KlineAuthority:
    """
    K 线权威源。详见模块 docstring。
    """

    name = "kline_authority"
    version = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        auth_cfg = config.get("kline_authority", {})

        self.symbol = config.get("symbol", "btcusdt").lower()
        self.symbol_upper = self.symbol.upper()
        self.buffer_size = int(
            auth_cfg.get("buffer_size", DEFAULT_BUFFER_SIZE)
        )
        self.snapshot_buffer_size = int(
            auth_cfg.get("snapshot_buffer_size", DEFAULT_SNAPSHOT_BUFFER_SIZE)
        )
        self.trade_buffer_size = int(
            auth_cfg.get("trade_buffer_size", DEFAULT_TRADE_BUFFER_SIZE)
        )
        self.snapshot_max_age_ms = int(
            auth_cfg.get("snapshot_max_age_ms", DEFAULT_SNAPSHOT_MAX_AGE_MS)
        )

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._bars: Deque[KlineData] = deque(maxlen=self.buffer_size)
        self._current_open_bar: Optional[KlineData] = None
        self._snapshots: Deque[SnapshotData] = deque(
            maxlen=self.snapshot_buffer_size
        )
        self._trades: Deque[dict] = deque(maxlen=self.trade_buffer_size)

        # ---- 回调 ----
        self._on_close_callbacks: List[Callable] = []
        self._on_open_update_callbacks: List[Callable] = []

        # ---- 统计 ----
        self._stats = {
            "bars_received": 0,
            "bars_closed": 0,
            "bars_duplicate": 0,
            "bars_out_of_order": 0,
            "bars_invalid": 0,
            "snapshots_received": 0,
            "trades_received": 0,
            "callbacks_errors": 0,
            "callbacks_slow": 0,
            "repair_attempts": 0,
            "repair_success": 0,
            "repair_failed": 0,
        }
        self._last_closed_ts: int = 0
        self._last_receive_ts: int = 0
        self._health: AuthorityHealth = AuthorityHealth.HEALTHY
        self._rest_client: Optional[Any] = None

    # ========================================================================
    # 数据输入
    # ========================================================================

    def on_exchange_kline(self, kline_msg: dict) -> None:
        """
        交易所 kline 流推送。

        Args:
            kline_msg: 币安 kline 流数据，含 k/t/o/h/l/c/v/x/E 字段

        契约：
        - 收线判断只看 x 字段，不看本地时钟
        - 未收线只更新 _current_open_bar
        - 收线后入缓冲，触发回调
        """
        try:
            k = kline_msg.get("k", kline_msg)
            ts = int(k["t"])
            is_closed = bool(k["x"])
            exchange_event_ts = int(kline_msg.get("E", ts))
            local_receive_ts = int(time.time() * 1000)

            # 时间间隔校验
            if ts % KLINE_INTERVAL_MS != 0:
                logger.warning(
                    f"kline ts={ts} not aligned to {KLINE_INTERVAL_MS}ms "
                    f"(symbol={self.symbol})"
                )

            bar = KlineData(
                symbol=self.symbol_upper,
                ts=ts,
                open=Decimal(str(k["o"])),
                high=Decimal(str(k["h"])),
                low=Decimal(str(k["l"])),
                close=Decimal(str(k["c"])),
                volume=Decimal(str(k["v"])),
                is_closed=is_closed,
                exchange_event_ts=exchange_event_ts,
                local_receive_ts=local_receive_ts,
                source="ws",
            )
        except (KeyError, ValueError, InvalidKlineError) as e:
            self._stats["bars_invalid"] += 1
            logger.error(f"invalid kline: {e}")
            return

        with self._lock:
            self._stats["bars_received"] += 1
            self._last_receive_ts = local_receive_ts

            # 延迟监控
            if bar.freshness_ms > 500:
                logger.warning(
                    f"kline delay {bar.freshness_ms}ms "
                    f"(ts={ts}, symbol={self.symbol})"
                )

            if is_closed:
                self._on_closed_kline(bar)
            else:
                self._on_open_kline_update(bar)

    def _on_closed_kline(self, bar: KlineData) -> None:
        """处理收线 K 线。"""
        # 乱序检查
        if bar.ts < self._last_closed_ts:
            self._stats["bars_out_of_order"] += 1
            logger.warning(
                f"out-of-order kline: ts={bar.ts} < "
                f"last_closed={self._last_closed_ts}, dropped"
            )
            return

        # 重复检查
        if self._bars and self._bars[-1].ts == bar.ts:
            self._stats["bars_duplicate"] += 1
            logger.debug(
                f"duplicate kline ts={bar.ts}, replacing"
            )
            self._bars.pop()

        self._bars.append(bar)
        self._last_closed_ts = bar.ts
        self._current_open_bar = None
        self._stats["bars_closed"] += 1

        # 构造回调参数：K 线 + 盘口快照 + 该周期成交
        snapshot = self._get_snapshot_at(bar.close_ts)
        period_trades = self._get_trades_in_range(
            bar.ts, bar.close_ts
        )

        self._trigger_callbacks(
            bar, snapshot, period_trades
        )

    def _on_open_kline_update(self, bar: KlineData) -> None:
        """处理未收线 K 线更新。"""
        with self._lock:
            self._current_open_bar = bar

        # 推送给前端（不阻断决策链）
        for cb in self._on_open_update_callbacks:
            try:
                cb(bar)
            except Exception as e:
                logger.error(
                    f"on_open_update callback error: {e}"
                )

    def on_depth(self, snapshot_msg: dict) -> None:
        """
        盘口快照推送。

        Args:
            snapshot_msg: 含 ts / bids / asks 字段
        """
        try:
            ts = int(snapshot_msg["ts"])
            bids = tuple(
                (Decimal(str(p)), Decimal(str(q)))
                for p, q in snapshot_msg["bids"]
            )
            asks = tuple(
                (Decimal(str(p)), Decimal(str(q)))
                for p, q in snapshot_msg["asks"]
            )
            snap = SnapshotData(
                ts=ts,
                local_receive_ts=int(time.time() * 1000),
                bids=bids,
                asks=asks,
            )
        except (KeyError, ValueError) as e:
            logger.error(f"invalid snapshot: {e}")
            return

        with self._lock:
            self._snapshots.append(snap)
            self._stats["snapshots_received"] += 1

    def on_agg_trade(self, trade_msg: dict) -> None:
        """
        逐笔成交推送。

        Args:
            trade_msg: 含 ts / price / qty / is_buyer_maker 字段
        """
        try:
            ts = int(trade_msg["ts"])
            price = Decimal(str(trade_msg["price"]))
            qty = Decimal(str(trade_msg["qty"]))
            is_buyer_maker = bool(trade_msg.get("is_buyer_maker", False))
        except (KeyError, ValueError) as e:
            logger.error(f"invalid trade: {e}")
            return

        with self._lock:
            self._trades.append({
                "ts": ts,
                "price": price,
                "qty": qty,
                "is_buyer_maker": is_buyer_maker,
            })
            self._stats["trades_received"] += 1

    # ========================================================================
    # 回调
    # ========================================================================

    def register_on_close(
        self, callback: Callable[[KlineData, Optional[SnapshotData], List[dict]], None]
    ) -> None:
        """
        注册收线回调。

        callback(bar, snapshot, period_trades)
        - bar: 收线的 K 线
        - snapshot: 收线时刻最近的盘口快照（可能为 None）
        - period_trades: 该 K 线周期内的成交列表

        契约：
        - 回调按注册顺序调用
        - 单个回调异常不影响其他回调
        - 单个回调超过 50ms 打 WARN
        """
        self._on_close_callbacks.append(callback)

    def register_on_open_update(
        self, callback: Callable[[KlineData], None]
    ) -> None:
        """注册未收线 K 线更新回调（供前端展示用，不阻断决策）。"""
        self._on_open_update_callbacks.append(callback)

    def _trigger_callbacks(
        self,
        bar: KlineData,
        snapshot: Optional[SnapshotData],
        period_trades: List[dict],
    ) -> None:
        """触发收线回调。"""
        for cb in self._on_close_callbacks:
            t0 = time.time()
            try:
                cb(bar, snapshot, period_trades)
            except Exception as e:
                self._stats["callbacks_errors"] += 1
                logger.error(
                    f"on_close callback error: {e}", exc_info=True
                )
            finally:
                elapsed_ms = (time.time() - t0) * 1000
                if elapsed_ms > CALLBACK_WARN_MS:
                    self._stats["callbacks_slow"] += 1
                    logger.warning(
                        f"on_close callback slow: {elapsed_ms:.1f}ms"
                    )

    # ========================================================================
    # 查询
    # ========================================================================

    def get_latest_bar(self) -> Optional[KlineData]:
        """最近收线的 K 线。"""
        with self._lock:
            return self._bars[-1] if self._bars else None

    def get_current_open_bar(self) -> Optional[KlineData]:
        """当前未收线 K 线。"""
        with self._lock:
            return self._current_open_bar

    def get_recent_bars(self, n: int) -> List[KlineData]:
        """最近 N 根收线 K 线，按时间升序。"""
        with self._lock:
            if n <= 0:
                return []
            n = min(n, len(self._bars))
            return list(self._bars)[-n:]

    def get_bar_at(self, ts: int) -> Optional[KlineData]:
        """按 ts 查询 K 线。"""
        with self._lock:
            for bar in reversed(self._bars):
                if bar.ts == ts:
                    return bar
                if bar.ts < ts:
                    return None
            return None

    def get_current_snapshot(self) -> Optional[SnapshotData]:
        """最近盘口快照。"""
        with self._lock:
            return self._snapshots[-1] if self._snapshots else None

    def get_snapshot_at(self, ts: int) -> Optional[SnapshotData]:
        """
        按 ts 查询最近的盘口快照。
        超过 snapshot_max_age_ms 返回 None。
        """
        with self._lock:
            return self._get_snapshot_at(ts)

    def _get_snapshot_at(self, ts: int) -> Optional[SnapshotData]:
        """内部方法（调用者需持锁）。"""
        if not self._snapshots:
            return None
        # 找 ts 最接近的
        best = None
        best_diff = None
        for snap in self._snapshots:
            diff = abs(snap.ts - ts)
            if best_diff is None or diff < best_diff:
                best = snap
                best_diff = diff
        if best is None or best_diff is None:
            return None
        if best_diff > self.snapshot_max_age_ms:
            return None
        return best

    def get_recent_trades(self, n: int) -> List[dict]:
        """最近 N 笔成交。"""
        with self._lock:
            if n <= 0:
                return []
            n = min(n, len(self._trades))
            return list(self._trades)[-n:]

    def _get_trades_in_range(
        self, start_ts: int, end_ts: int
    ) -> List[dict]:
        """内部方法：按时间范围查询成交（调用者需持锁）。"""
        return [
            t for t in self._trades
            if start_ts <= t["ts"] < end_ts
        ]

    # ========================================================================
    # 修复（断线重连后）
    # ========================================================================

    def set_rest_client(self, rest_client: Any) -> None:
        """注入 REST 客户端，用于修复缺失 K 线。"""
        self._rest_client = rest_client

    def repair_klines(
        self, start_ts: int, end_ts: int
    ) -> int:
        """
        修复缺失 K 线。通过 REST 拉取。

        Args:
            start_ts: 起始 ts（含）
            end_ts: 结束 ts（不含）

        Returns:
            修复的 K 线数量

        契约：
        - 按 ts 去重，WS 已有的跳过
        - 超过 REPAIR_MAX_BARS 拒绝，抛异常
        - REST 失败不抛，标记 degraded
        """
        if self._rest_client is None:
            logger.warning("repair_klines: no rest_client, skipped")
            return 0

        n_expected = (end_ts - start_ts) // KLINE_INTERVAL_MS
        if n_expected > REPAIR_MAX_BARS:
            raise KlineAuthorityError(
                f"repair too many bars: {n_expected} > {REPAIR_MAX_BARS}, "
                f"manual intervention required"
            )

        self._stats["repair_attempts"] += 1
        try:
            klines = self._rest_client.get_klines(
                symbol=self.symbol_upper,
                interval="3m",
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except Exception as e:
            self._stats["repair_failed"] += 1
            self._health = AuthorityHealth.DEGRADED
            logger.warning(f"repair_klines failed: {e}")
            return 0

        repaired = 0
        with self._lock:
            existing_ts = {b.ts for b in self._bars}
            for k in klines:
                try:
                    ts = int(k["t"])
                    if ts in existing_ts:
                        continue
                    bar = KlineData(
                        symbol=self.symbol_upper,
                        ts=ts,
                        open=Decimal(str(k["o"])),
                        high=Decimal(str(k["h"])),
                        low=Decimal(str(k["l"])),
                        close=Decimal(str(k["c"])),
                        volume=Decimal(str(k["v"])),
                        is_closed=True,
                        exchange_event_ts=int(k.get("T", ts)),
                        local_receive_ts=int(time.time() * 1000),
                        source="rest",
                        is_complete=False,  # 标记为修复数据
                    )
                    self._bars.append(bar)
                    repaired += 1
                except (KeyError, ValueError, InvalidKlineError) as e:
                    logger.warning(
                        f"repair kline invalid: {e}"
                    )

            # 按 ts 排序
            sorted_bars = sorted(self._bars, key=lambda b: b.ts)
            self._bars.clear()
            for b in sorted_bars[-self.buffer_size:]:
                self._bars.append(b)

        self._stats["repair_success"] += repaired
        if repaired > 0:
            self._health = AuthorityHealth.HEALTHY
            logger.info(f"repaired {repaired} klines")
        return repaired

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            now = int(time.time() * 1000)
            latest = self._bars[-1] if self._bars else None
            last_age_ms = (
                now - latest.local_receive_ts if latest else None
            )

            # 判断是否陈旧（超过 2 个周期没新 K 线）
            if last_age_ms is not None:
                if last_age_ms > 2 * KLINE_INTERVAL_MS:
                    self._health = AuthorityHealth.STALE
                elif self._health == AuthorityHealth.STALE and last_age_ms < KLINE_INTERVAL_MS:
                    self._health = AuthorityHealth.HEALTHY

            return {
                "healthy": self._health == AuthorityHealth.HEALTHY,
                "name": self.name,
                "version": self.version,
                "health": self._health.value,
                "symbol": self.symbol_upper,
                "bars_in_buffer": len(self._bars),
                "buffer_size": self.buffer_size,
                "latest_bar_ts": latest.ts if latest else None,
                "latest_bar_age_ms": last_age_ms,
                "latest_bar_freshness_ms": (
                    latest.freshness_ms if latest else None
                ),
                "snapshots_in_buffer": len(self._snapshots),
                "trades_in_buffer": len(self._trades),
                "last_closed_ts": self._last_closed_ts,
                "stats": dict(self._stats),
                "timestamp": now,
            }

    def reset(self) -> None:
        """重置状态。测试用。"""
        with self._lock:
            self._bars.clear()
            self._snapshots.clear()
            self._trades.clear()
            self._current_open_bar = None
            self._last_closed_ts = 0
            self._last_receive_ts = 0
            self._health = AuthorityHealth.HEALTHY
            for k in self._stats:
                self._stats[k] = 0
        logger.info("kline_authority reset")


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "kline_authority",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": [
        "authoritative_ohlc",
        "recent_bars",
        "current_snapshot",
        "recent_trades",
        "kline_repair",
    ],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Authoritative kline source: exchange kline stream + depth + aggTrade",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> KlineAuthority:
    """模块工厂入口。"""
    return KlineAuthority(config)
