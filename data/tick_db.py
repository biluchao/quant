"""
data/tick_db.py — 行情数据持久化（逐笔成交 + K 线）

职责边界：
- StatePersister：系统数据（订单/轮次/状态/事件/信号/因子）
- TickDB：行情数据（逐笔成交 + K 线）

两者**独立数据库文件**，避免高频逐笔成交写入阻塞低频订单写入。

数据库文件派生规则：
- 传入 db_path = "data/live_quant.db"
- tick_db 实际路径 = "data/live_quant_tick.db"
- 后缀 _tick 由本模块自动派生，调用方无需关心

表结构：
- tick_trades：逐笔成交（高频）
- tick_klines：K 线（低频，来自 HistoricalLoader）
- schema_version：版本管理

写入模式：
- append(trade)：异步队列，批量写入。逐笔成交高频，不能阻塞消费线程。
- insert_kline(bar)：同步写入。K 线低频，来自加载器或修复。
- flush()：阻塞直到队列清空。
- close()：flush + 关连接。

批量写入：
- 每批最多 100 条（或 500ms 触发）
- 一个事务写一批
- 队列满时丢弃最旧的（保留最新）
- 丢弃率 > 5% WARN，> 20% CRITICAL

关键契约：
1. append() 是 O(1) 入队，不阻塞
2. insert_kline() 同步写入，失败抛异常
3. 所有价格/数量用 Decimal 转字符串存 TEXT
4. 时间戳毫秒 int（UTC）
5. tick_trades 主键 (symbol, trade_id)，去重
6. tick_klines 主键 (symbol, ts)，去重
7. insert_kline 拒绝 is_closed=False
8. query_trades / query_klines 返回 List[dict]
9. query_* 的 limit 默认 10000，最大 100000
10. WAL 模式 + NORMAL synchronous（性能优先）
11. 数据库独立文件（派生 _tick 后缀）
12. 启动时 integrity_check

与其他人物的边界：
- KlineAuthority.on_agg_trade → tick_db.append(trade)
- HistoricalLoader.load_and_persist → tick_db.insert_kline(bar)
- main.py._on_trade → tick_db.append(trade)
- 策略层 / 状态机 / 因子层：碰不到 tick_db

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
import logging
import queue
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
SCHEMA_VERSION = 1
DEFAULT_BATCH_SIZE = 100
DEFAULT_FLUSH_INTERVAL_MS = 500
DEFAULT_QUEUE_SIZE = 5000
DEFAULT_QUERY_LIMIT = 10000
MAX_QUERY_LIMIT = 100000
DEFAULT_BUSY_TIMEOUT_MS = 5000

DROP_RATE_WARN = 0.05
DROP_RATE_CRITICAL = 0.20


# ============================================================================
# 错误
# ============================================================================
class TickDBError(Exception):
    """tick_db 错误。"""
    pass


# ============================================================================
# 日志表
# ============================================================================
@dataclass
class _Batch:
    """批量写入的数据。"""
    trades: List[Dict[str, Any]]
    ts: float


# ============================================================================
# TickDB
# ============================================================================
class TickDB:
    """
    行情数据持久化。详见模块 docstring。

    用法：
        db = TickDB("data/live_quant.db")

        # 逐笔成交（高频，异步）
        db.append({
            "ts": 1234567890123,
            "trade_ts": 1234567890000,
            "price": Decimal("67000.5"),
            "qty": Decimal("0.001"),
            "is_buyer_maker": True,
            "trade_id": 12345,
            "local_receive_ts": 1234567890200,
        })

        # K 线（低频，同步）
        db.insert_kline(
            symbol="BTCUSDT",
            ts=1234567890000,
            open="67000.5", high="67100.0",
            low="66900.0", close="67050.0",
            volume="10.5", is_closed=True,
        )

        # 查询
        trades = db.query_trades("BTCUSDT", 1234567890000, 1234567900000)
        klines = db.query_klines("BTCUSDT", 1234567890000, 1234567900000)

        # 维护
        db.cleanup_old_data(before_ts=int(time.time() * 1000) - 7 * 86400 * 1000)
        db.flush()
        db.close()
    """

    name = "tick_db"
    version = "1.0.0"

    def __init__(
        self,
        db_path: str,
        config: Optional[dict] = None,
    ):
        # 派生自己的数据库文件
        p = Path(db_path)
        self._db_path = str(
            p.with_name(p.stem + "_tick" + p.suffix)
        )

        cfg = (config or {}).get("tick_db", {})
        self._batch_size = int(
            cfg.get("batch_size", DEFAULT_BATCH_SIZE)
        )
        self._flush_interval_ms = int(
            cfg.get("flush_interval_ms", DEFAULT_FLUSH_INTERVAL_MS)
        )
        self._queue_size = int(
            cfg.get("queue_size", DEFAULT_QUEUE_SIZE)
        )
        self._busy_timeout_ms = int(
            cfg.get("busy_timeout_ms", DEFAULT_BUSY_TIMEOUT_MS)
        )

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._queue: queue.Queue = queue.Queue(maxsize=self._queue_size)
        self._stop_flag = threading.Event()
        self._writer_thread: Optional[threading.Thread] = None
        self._conn: Optional[sqlite3.Connection] = None
        self._closed = False

        # ---- 统计 ----
        self._stats = {
            "trades_queued": 0,
            "trades_written": 0,
            "trades_dropped": 0,
            "klines_written": 0,
            "batches": 0,
            "errors": 0,
            "flushes": 0,
        }

        self._init_schema()

        logger.info(
            f"TickDB initialized db_path={self._db_path} "
            f"queue_size={self._queue_size} batch_size={self._batch_size}"
        )

    # ========================================================================
    # Schema
    # ========================================================================

    def _init_schema(self) -> None:
        """初始化数据库 schema。幂等。"""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,  # 手动事务
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")

        # 完整性检查
        result = self._conn.execute(
            "PRAGMA integrity_check"
        ).fetchone()
        if result[0] != "ok":
            raise TickDBError(
                f"database integrity check failed: {result[0]}"
            )

        cur = self._conn.cursor()

        # schema_version
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            )
        """)

        # tick_trades
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tick_trades (
                symbol TEXT NOT NULL,
                trade_id INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                trade_ts INTEGER NOT NULL,
                price TEXT NOT NULL,
                qty TEXT NOT NULL,
                is_buyer_maker INTEGER NOT NULL,
                local_receive_ts INTEGER NOT NULL,
                PRIMARY KEY (symbol, trade_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tick_trades_ts
            ON tick_trades(symbol, ts)
        """)

        # tick_klines
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tick_klines (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                volume TEXT NOT NULL,
                is_closed INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (symbol, ts)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tick_klines_ts
            ON tick_klines(symbol, ts)
        """)

        # 版本记录
        row = cur.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        current = row[0] or 0
        if current < SCHEMA_VERSION:
            self._conn.execute(
                "INSERT INTO schema_version (version, applied_at) "
                "VALUES (?, ?)",
                (SCHEMA_VERSION, int(time.time() * 1000)),
            )

        self._conn.commit()

        # 启动写入线程
        self._start_writer()

    def _start_writer(self) -> None:
        """启动异步写入线程。"""
        if self._writer_thread is not None:
            return
        self._stop_flag.clear()
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name=f"tickdb-writer-{Path(self._db_path).stem}",
        )
        self._writer_thread.start()

    # ========================================================================
    # 写入
    # ========================================================================

    def append(self, trade: Dict[str, Any]) -> None:
        """
        逐笔成交入队。异步写入，不阻塞。

        Args:
            trade: {ts, trade_ts, price, qty, is_buyer_maker, trade_id,
                    local_receive_ts, [symbol]}

        契约：
        - O(1) 入队，立即返回
        - 队列满时丢弃最旧的（保留最新）
        - 统计 queue_drops
        """
        # 补 symbol（如果调用方没传）
        if "symbol" not in trade:
            # 让调用方传 symbol，或者从 config 读
            return  # 静默丢弃，避免脏数据

        with self._lock:
            self._stats["trades_queued"] += 1

        try:
            self._queue.put_nowait(trade)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(trade)
                with self._lock:
                    self._stats["trades_dropped"] += 1
                    self._check_drop_rate()
            except (queue.Empty, queue.Full):
                with self._lock:
                    self._stats["trades_dropped"] += 1

    def _check_drop_rate(self) -> None:
        """检查丢弃率。"""
        total = (
            self._stats["trades_written"] + self._stats["trades_dropped"]
        )
        if total < 100:
            return
        rate = self._stats["trades_dropped"] / total
        if rate >= DROP_RATE_CRITICAL:
            logger.critical(
                f"TickDB trade drop rate critical: {rate:.1%}"
            )
        elif rate >= DROP_RATE_WARN:
            dropped = self._stats["trades_dropped"]
            if dropped % 1000 == 1:
                logger.warning(
                    f"TickDB trade drop rate warning: {rate:.1%}"
                )

    def insert_kline(
        self,
        symbol: str,
        ts: int,
        open: str,
        high: str,
        low: str,
        close: str,
        volume: str,
        is_closed: bool = True,
    ) -> None:
        """
        插入 K 线。同步写入，失败抛异常。

        契约：
        - 拒绝 is_closed=False（未收线 K 线不入库）
        - PRIMARY KEY (symbol, ts)，重复则覆盖
        """
        if not is_closed:
            raise TickDBError(
                f"refuse to insert open kline: symbol={symbol} ts={ts}"
            )
        if not symbol or ts <= 0:
            raise TickDBError(
                f"invalid kline: symbol={symbol} ts={ts}"
            )

        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO tick_klines (
                        symbol, ts, open, high, low, close, volume, is_closed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (symbol, ts, open, high, low, close, volume, 1),
                )
                self._stats["klines_written"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise TickDBError(
                    f"insert_kline failed: {e}"
                ) from e

    # ========================================================================
    # 写入线程
    # ========================================================================

    def _writer_loop(self) -> None:
        """后台线程：批量写入逐笔成交。"""
        batch: List[Dict[str, Any]] = []
        last_flush = time.time()

        while not self._stop_flag.is_set():
            # 尝试取数据
            try:
                trade = self._queue.get(timeout=0.1)
                batch.append(trade)
            except queue.Empty:
                pass

            now = time.time()
            should_flush = (
                len(batch) >= self._batch_size
                or (
                    batch
                    and (now - last_flush) * 1000
                    >= self._flush_interval_ms
                )
                or (self._stop_flag.is_set() and batch)
            )

            if should_flush:
                self._flush_batch(batch)
                batch = []
                last_flush = now

            if self._stop_flag.is_set() and self._queue.empty():
                # 最后一批
                if batch:
                    self._flush_batch(batch)
                break

    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """一个事务写一批逐笔成交。"""
        if not batch:
            return

        rows = []
        for trade in batch:
            try:
                rows.append((
                    trade["symbol"],
                    int(trade["trade_id"]),
                    int(trade["ts"]),
                    int(trade["trade_ts"]),
                    str(trade["price"]),
                    str(trade["qty"]),
                    int(bool(trade["is_buyer_maker"])),
                    int(trade["local_receive_ts"]),
                ))
            except (KeyError, ValueError, TypeError) as e:
                with self._lock:
                    self._stats["errors"] += 1
                logger.warning(f"invalid trade skipped: {e}")
                continue

        if not rows:
            return

        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.execute("BEGIN")
                self._conn.executemany(
                    """
                    INSERT OR IGNORE INTO tick_trades (
                        symbol, trade_id, ts, trade_ts, price, qty,
                        is_buyer_maker, local_receive_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                self._conn.execute("COMMIT")
                self._stats["trades_written"] += len(rows)
                self._stats["batches"] += 1
            except sqlite3.Error as e:
                try:
                    self._conn.execute("ROLLBACK")
                except Exception:
                    pass
                self._stats["errors"] += 1
                logger.error(f"batch write failed: {e}")

    # ========================================================================
    # 查询
    # ========================================================================

    def query_trades(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> List[Dict[str, Any]]:
        """
        查询逐笔成交，返回 [start_ts, end_ts) 内最早的 limit 条。

        Returns:
            List[dict]，按 ts 升序
        """
        if limit <= 0 or limit > MAX_QUERY_LIMIT:
            raise TickDBError(
                f"limit must be in (0, {MAX_QUERY_LIMIT}], got {limit}"
            )

        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            rows = self._conn.execute(
                """
                SELECT symbol, trade_id, ts, trade_ts, price, qty,
                       is_buyer_maker, local_receive_ts
                FROM tick_trades
                WHERE symbol = ? AND ts >= ? AND ts < ?
                ORDER BY ts ASC
                LIMIT ?
                """,
                (symbol, start_ts, end_ts, limit),
            ).fetchall()

        return [dict(r) for r in rows]

    def query_klines(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> List[Dict[str, Any]]:
        """
        查询 K 线，返回 [start_ts, end_ts) 内的 K 线。

        Returns:
            List[dict]，按 ts 升序
        """
        if limit <= 0 or limit > MAX_QUERY_LIMIT:
            raise TickDBError(
                f"limit must be in (0, {MAX_QUERY_LIMIT}], got {limit}"
            )

        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            rows = self._conn.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume, is_closed
                FROM tick_klines
                WHERE symbol = ? AND ts >= ? AND ts < ?
                ORDER BY ts ASC
                LIMIT ?
                """,
                (symbol, start_ts, end_ts, limit),
            ).fetchall()

        return [dict(r) for r in rows]

    def count_trades(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> int:
        """统计逐笔成交数量。"""
        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            row = self._conn.execute(
                "SELECT COUNT(*) FROM tick_trades "
                "WHERE symbol = ? AND ts >= ? AND ts < ?",
                (symbol, start_ts, end_ts),
            ).fetchone()
            return int(row[0]) if row else 0

    def count_klines(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> int:
        """统计 K 线数量。"""
        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            row = self._conn.execute(
                "SELECT COUNT(*) FROM tick_klines "
                "WHERE symbol = ? AND ts >= ? AND ts < ?",
                (symbol, start_ts, end_ts),
            ).fetchone()
            return int(row[0]) if row else 0

    # ========================================================================
    # 维护
    # ========================================================================

    def cleanup_old_data(
        self,
        before_ts: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        清理旧数据。

        Args:
            before_ts: 删除 ts < before_ts 的数据
            symbol: 只清理该交易对

        Returns:
            {"trades_deleted": int, "klines_deleted": int}
        """
        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")

            conditions = []
            params: List[Any] = []
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
            if before_ts:
                conditions.append("ts < ?")
                params.append(before_ts)

            if not conditions:
                raise TickDBError(
                    "cleanup_old_data requires at least one of "
                    "before_ts or symbol"
                )

            where = " AND ".join(conditions)
            try:
                cur = self._conn.execute(
                    f"DELETE FROM tick_trades WHERE {where}", params
                )
                trades_deleted = cur.rowcount
                cur = self._conn.execute(
                    f"DELETE FROM tick_klines WHERE {where}", params
                )
                klines_deleted = cur.rowcount
                logger.info(
                    f"cleanup: {trades_deleted} trades, "
                    f"{klines_deleted} klines deleted"
                )
                return {
                    "trades_deleted": trades_deleted,
                    "klines_deleted": klines_deleted,
                }
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise TickDBError(f"cleanup failed: {e}") from e

    def vacuum(self) -> None:
        """VACUUM。回收磁盘空间。"""
        with self._lock:
            if self._conn is None:
                raise TickDBError("database closed")
            self._conn.execute("VACUUM")
        logger.info("vacuum completed")

    # ========================================================================
    # 生命周期
    # ========================================================================

    def flush(self, timeout_s: float = 10.0) -> bool:
        """阻塞直到队列清空。返回 True 表示已清空。"""
        deadline = time.time() + timeout_s
        while not self._queue.empty():
            if time.time() > deadline:
                logger.warning(
                    f"flush timeout, {self._queue.qsize()} items remaining"
                )
                return False
            time.sleep(0.05)
        time.sleep(0.1)  # 等最后一批
        with self._lock:
            self._stats["flushes"] += 1
        return True

    def close(self) -> None:
        """停机：flush + 停线程 + 关连接。幂等。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        self.flush()
        self._stop_flag.set()
        if self._writer_thread:
            self._writer_thread.join(timeout=5)
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
        logger.info("TickDB closed")

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            db_size = 0
            try:
                db_size = Path(self._db_path).stat().st_size
            except OSError:
                pass
            total = (
                self._stats["trades_written"]
                + self._stats["trades_dropped"]
            )
            drop_rate = (
                self._stats["trades_dropped"] / total
                if total > 0 else 0.0
            )
            return {
                "healthy": drop_rate < DROP_RATE_CRITICAL,
                "name": self.name,
                "version": self.version,
                "db_path": self._db_path,
                "db_size_bytes": db_size,
                "queue_size": self._queue.qsize(),
                "queue_capacity": self._queue_size,
                "drop_rate": drop_rate,
                "stats": dict(self._stats),
                "timestamp": int(time.time() * 1000),
            }

    def reset_stats(self) -> None:
        """重置统计。测试用。"""
        with self._lock:
            for k in self._stats:
                self._stats[k] = 0


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "tick_db",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["tick_storage"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Market data persistence (trades + klines) with async batch writes",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> TickDB:
    """
    从配置创建实例。

    config 需要：
    - db_path: 主数据库路径（tick_db 会派生 _tick 后缀）

    config 可选：
    - tick_db.batch_size: 默认 100
    - tick_db.flush_interval_ms: 默认 500
    - tick_db.queue_size: 默认 5000
    """
    db_path = config.get("db_path")
    if not db_path:
        raise TickDBError("TickDB.create requires config.db_path")
    return TickDB(db_path=db_path, config=config)
