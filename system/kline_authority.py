"""
system/kline_authority.py — K 线权威源与聚合中心 (重铸标准版)

核心设计原则：
1. 唯一的 OHLC 权威：所有策略和因子只能从这里获取 K 线。
2. 强类型保障：输出给下游的 KlineData 全部强制转为 Decimal 和 int。
3. 对齐收线：只有交易所推送 x=true 才触发 on_bar_close，避免算在半根 K 线上。
4. 回调隔离：某个下游模块崩溃，绝不影响数据流的正常推进。
"""

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import logging
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# 数据结构
# ============================================================================

@dataclass(frozen=True)
class KlineData:
    """强类型的 K 线数据结构，直接提供给状态机和因子层"""
    symbol: str
    ts: int                         # 开盘时间（毫秒）
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    is_closed: bool
    exchange_event_ts: int
    
    @property
    def close_ts(self) -> int:
        # 这里默认是 3m 周期，也可以根据配置动态获取
        return self.ts + (3 * 60 * 1000)

@dataclass(frozen=True)
class SnapshotData:
    """盘口快照"""
    ts: int
    bids: tuple                     # ((price, qty), ...)
    asks: tuple                     # ((price, qty), ...)

# ============================================================================
# 核心聚合器
# ============================================================================

class KlineAuthority:
    """
    K 线权威聚合器
    """
    def __init__(self, config: dict):
        self.config = config
        self.symbol = config.get("symbol", "BTCUSDT").upper()
        
        # 缓冲配置
        self.max_bars = config.get("data", {}).get("kline_max_bars", 1000)
        self.max_snapshots = 200
        
        # 内部状态
        self._lock = threading.RLock()
        self._bars: Deque[KlineData] = deque(maxlen=self.max_bars)
        self._snapshots: Deque[SnapshotData] = deque(maxlen=self.max_snapshots)
        self._trades: Deque[dict] = deque(maxlen=1000)
        
        self._current_open_bar: Optional[KlineData] = None
        self._last_closed_ts: int = 0
        
        # 回调注册表
        self._on_close_callbacks: List[Callable[[KlineData, Optional[SnapshotData], List[dict]], None]] = []
        
        # 统计
        self._stats = {"received": 0, "closed": 0, "invalid": 0, "callbacks_errors": 0}

    # ------------------------------------------------------------------------
    # 数据接入 (由 data/ws_*.py 调用)
    # ------------------------------------------------------------------------

    def on_exchange_kline(self, msg: dict) -> None:
        """接收原始交易所 K 线数据"""
        try:
            k = msg.get("k", msg)
            ts = int(k["t"])
            is_closed = bool(k["x"])
            
            # 强类型转换并校验合法性
            o = Decimal(str(k["o"]))
            h = Decimal(str(k["h"]))
            l = Decimal(str(k["l"]))
            c = Decimal(str(k["c"]))
            v = Decimal(str(k["v"]))
            
            if h < l or o <= 0 or c <= 0 or v < 0:
                raise ValueError(f"Invalid OHLCV data: H={h}, L={l}, O={o}, C={c}, V={v}")

            bar = KlineData(
                symbol=self.symbol,
                ts=ts,
                open=o, high=h, low=l, close=c, volume=v,
                is_closed=is_closed,
                exchange_event_ts=int(msg.get("E", ts))
            )

            with self._lock:
                self._stats["received"] += 1
                
                if is_closed:
                    # 重复或乱序检查
                    if ts <= self._last_closed_ts:
                        return
                    
                    self._bars.append(bar)
                    self._last_closed_ts = ts
                    self._current_open_bar = None
                    self._stats["closed"] += 1
                    
                    # 获取与该 K 线收盘时间最接近的盘口快照
                    snapshot = self._get_nearest_snapshot(bar.close_ts)
                    # 获取该 K 线周期内的成交明细
                    trades = self._get_period_trades(bar.ts, bar.close_ts)
                else:
                    self._current_open_bar = bar
                    
        except Exception as e:
            self._stats["invalid"] += 1
            logger.error(f"解析 K 线数据失败: {e}, raw={msg}")
            return

        # 收线后，在锁外触发回调，防止阻塞数据接收
        if is_closed:
            self._trigger_callbacks(bar, snapshot, trades)

    def on_depth(self, msg: dict) -> None:
        """接收盘口快照"""
        try:
            ts = int(msg["ts"])
            bids = tuple((Decimal(str(p)), Decimal(str(q))) for p, q in msg["bids"])
            asks = tuple((Decimal(str(p)), Decimal(str(q))) for p, q in msg["asks"])
            
            with self._lock:
                self._snapshots.append(SnapshotData(ts=ts, bids=bids, asks=asks))
        except Exception as e:
            logger.error(f"解析盘口数据失败: {e}")

    def on_agg_trade(self, msg: dict) -> None:
        """接收逐笔成交"""
        try:
            with self._lock:
                self._trades.append({
                    "ts": int(msg["ts"]),
                    "price": Decimal(str(msg["price"])),
                    "qty": Decimal(str(msg["qty"])),
                    "is_buyer_maker": bool(msg.get("is_buyer_maker", False))
                })
        except Exception as e:
            logger.error(f"解析逐笔成交失败: {e}")

    # ------------------------------------------------------------------------
    # 回调与数据获取
    # ------------------------------------------------------------------------

    def register_on_close(self, callback: Callable[[KlineData, Optional[SnapshotData], List[dict]], None]) -> None:
        """注册收线回调（状态机在此接入）"""
        self._on_close_callbacks.append(callback)

    def _trigger_callbacks(self, bar: KlineData, snapshot: Optional[SnapshotData], trades: List[dict]) -> None:
        """安全触发所有回调"""
        for cb in self._on_close_callbacks:
            t0 = time.perf_counter()
            try:
                cb(bar, snapshot, trades)
            except Exception as e:
                self._stats["callbacks_errors"] += 1
                logger.error(f"K线收线回调异常: {e}", exc_info=True)
            finally:
                cost = (time.perf_counter() - t0) * 1000
                if cost > 50:
                    logger.warning(f"收线回调耗时过长: {cost:.1f}ms")

    def _get_nearest_snapshot(self, target_ts: int) -> Optional[SnapshotData]:
        """寻找与目标时间戳最接近的快照（必须在锁内调用）"""
        if not self._snapshots:
            return None
        # 简单线性查找，因为 deque 不大
        best = min(self._snapshots, key=lambda s: abs(s.ts - target_ts))
        if abs(best.ts - target_ts) > 3000: # 超过 3 秒认为过期
            return None
        return best

    def _get_period_trades(self, start_ts: int, end_ts: int) -> List[dict]:
        """获取指定时间段内的成交（必须在锁内调用）"""
        return [t for t in self._trades if start_ts <= t["ts"] < end_ts]

    def get_recent_bars(self, n: int) -> List[KlineData]:
        """获取最近的 N 根 K 线（供状态机/API 调用）"""
        with self._lock:
            if n <= 0: return []
            return list(self._bars)[-n:]
            
    def get_latest_bar(self) -> Optional[KlineData]:
        with self._lock:
            return self._bars[-1] if self._bars else None

    def health_check(self) -> dict:
        with self._lock:
            return {
                "healthy": True,
                "bars_count": len(self._bars),
                "snapshots_count": len(self._snapshots),
                "last_closed_ts": self._last_closed_ts,
                "stats": dict(self._stats)
            }

# ============================================================================
# 模块工厂函数
# ============================================================================

MANIFEST = {
    "name": "kline_authority",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["authoritative_ohlc", "recent_bars", "current_snapshot"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> KlineAuthority:
    return KlineAuthority(config)
