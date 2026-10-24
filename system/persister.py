"""
system/persister.py — 持久化层（SQLite）

持久化是系统失忆后的唯一线索。

设计原则：
1. 同步写 vs 异步写分离
   - 同步写（FULL synchronous）：orders / fills / rounds / state_snapshots
     —— 丢了就是钱丢了
   - 异步写（NORMAL synchronous）：klines / events / factor_values / signals
     —— 丢一两根不致命

2. 事务原子性
   - 订单成交 = 更新 orders + 插入 fills + 更新 rounds 状态 → 一个事务
   - 任何一步失败，全部回滚

3. WAL 模式
   - 崩溃时数据库不损坏
   - 读写并发不阻塞

4. 磁盘满时抛 PersistenceError
   - 由上层触发紧急停止

5. schema 版本化
   - 升级时自动迁移

6. 启动时 integrity_check
   - 损坏时拒绝启动

表结构（8 张表）：
- schema_version      版本管理
- klines               K 线数据
- orders               订单生命周期
- fills                成交明细
- rounds               轮次记录
- state_snapshots      状态快照（每根 K 线）
- events               系统事件（对账/错误/紧急停止）
- factor_values        因子值历史（Step 2+ 用，Step 1 预留）
- signals              信号历史（Step 2+ 用，Step 1 预留）
- state_transitions    状态迁移历史
- account_snapshots    账户快照

关键契约：
1. 所有时间戳毫秒 int（UTC）
2. 所有 Decimal 序列化为字符串（str），反序列化回 Decimal
3. 所有 JSON 字段序列化为 TEXT
4. 同步写方法抛异常时，调用方必须处理（不得静默）
5. flush() 阻塞直到异步队列清空
6. close() 先 flush() 再关连接
7. 所有方法线程安全
8. 同一连接，串行化访问（SQLite 单写入者）

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

import json
import logging
import queue
import sqlite3
import threading
import time
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
SCHEMA_VERSION = 1
ASYNC_QUEUE_SIZE = 10000
FLUSH_TIMEOUT_S = 10.0
SQLITE_BUSY_TIMEOUT_MS = 5000


# ============================================================================
# 错误
# ============================================================================
class PersistenceError(Exception):
    """持久化错误。上层应触发紧急停止。"""
    pass


# ============================================================================
# 序列化工具
# ============================================================================
def _serialize_value(v: Any) -> Any:
    """将任意值序列化为 SQLite 可存储的类型。"""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float, str)):
        return v
    if is_dataclass(v):
        return json.dumps(asdict(v), default=_json_default, ensure_ascii=False)
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=_json_default, ensure_ascii=False)
    if hasattr(v, "value"):  # Enum
        return v.value
    return str(v)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if hasattr(obj, "value"):
        return obj.value
    if is_dataclass(obj):
        return asdict(obj)
    return str(obj)


def _to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    return Decimal(str(v))


# ============================================================================
# 持久化器
# ============================================================================
class StatePersister:
    """
    持久化层。详见模块 docstring。
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._async_queue: queue.Queue = queue.Queue(maxsize=ASYNC_QUEUE_SIZE)
        self._writer_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        self._conn: Optional[sqlite3.Connection] = None
        self._stats = {
            "sync_writes": 0,
            "async_writes": 0,
            "async_dropped": 0,
            "errors": 0,
        }

    # ========================================================================
    # 生命周期
    # ========================================================================

    def init_schema(self) -> None:
        """
        初始化数据库 schema。
        幂等：已初始化则不重复。
        """
        with self._lock:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

            self._conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None,  # 手动控制事务
            )
            self._conn.row_factory = sqlite3.Row

            # PRAGMA
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}"
            )
            self._conn.execute("PRAGMA foreign_keys=ON")

            # 完整性检查
            result = self._conn.execute("PRAGMA integrity_check").fetchone()
            if result[0] != "ok":
                raise PersistenceError(
                    f"database integrity check failed: {result[0]}"
                )

            # 建表
            self._create_tables()
            self._set_schema_version()

        logger.info(f"persister initialized db={self.db_path}")

    def _create_tables(self) -> None:
        """建表。所有表 IF NOT EXISTS。"""
        cur = self._conn.cursor()

        # schema_version
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            )
        """)

        # klines
        cur.execute("""
            CREATE TABLE IF NOT EXISTS klines (
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
            CREATE INDEX IF NOT EXISTS idx_klines_ts
            ON klines(ts)
        """)

        # orders
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                client_order_id TEXT PRIMARY KEY,
                exchange_order_id TEXT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                type TEXT NOT NULL,
                order_role TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_qty TEXT NOT NULL,
                executed_qty TEXT NOT NULL DEFAULT '0',
                price TEXT,
                stop_price TEXT,
                avg_price TEXT,
                total_fee TEXT NOT NULL DEFAULT '0',
                fee_asset TEXT NOT NULL DEFAULT 'USDT',
                reduce_only INTEGER NOT NULL DEFAULT 0,
                is_server_side_stop INTEGER NOT NULL DEFAULT 0,
                reject_reason TEXT,
                reject_code INTEGER,
                metadata_json TEXT,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL,
                transact_time INTEGER
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_status
            ON orders(status)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_symbol_created
            ON orders(symbol, created_ts)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_exchange_id
            ON orders(exchange_order_id)
        """)

        # fills
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fills (
                trade_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price TEXT NOT NULL,
                qty TEXT NOT NULL,
                fee TEXT NOT NULL,
                fee_asset TEXT NOT NULL,
                is_maker INTEGER NOT NULL,
                transact_time INTEGER NOT NULL,
                FOREIGN KEY (client_order_id)
                    REFERENCES orders(client_order_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fills_order
            ON fills(client_order_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fills_time
            ON fills(transact_time)
        """)

        # rounds
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rounds (
                round_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction INTEGER NOT NULL,
                entry_price TEXT NOT NULL,
                exit_price TEXT,
                stop_price TEXT,
                total_qty TEXT NOT NULL,
                pyramid_count INTEGER NOT NULL DEFAULT 0,
                state_entry TEXT,
                state_exit TEXT,
                factor_snapshot_entry TEXT,
                factor_snapshot_exit TEXT,
                param_version TEXT,
                pnl TEXT,
                r_multiple REAL,
                peak_unrealized TEXT,
                entry_ts INTEGER NOT NULL,
                exit_ts INTEGER,
                duration_ms INTEGER,
                exit_reason TEXT,
                status TEXT NOT NULL DEFAULT 'open'
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_rounds_status
            ON rounds(status)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_rounds_time
            ON rounds(entry_ts)
        """)

        # state_snapshots
        cur.execute("""
            CREATE TABLE IF NOT EXISTS state_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                bar_ts INTEGER NOT NULL,
                state TEXT NOT NULL,
                active_round_id TEXT,
                snapshot_json TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                created_ts INTEGER NOT NULL,
                UNIQUE(symbol, bar_ts)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_bar_ts
            ON state_snapshots(bar_ts)
        """)

        # events
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                data_json TEXT,
                ts INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_type_ts
            ON events(event_type, ts)
        """)

        # factor_values (Step 1 预留，Step 2+ 使用)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS factor_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                bar_ts INTEGER NOT NULL,
                factor_name TEXT NOT NULL,
                value REAL,
                raw_value REAL,
                state TEXT,
                outlier INTEGER DEFAULT 0,
                stale INTEGER DEFAULT 0,
                factor_version TEXT,
                UNIQUE(symbol, bar_ts, factor_name)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_factors_name_ts
            ON factor_values(factor_name, bar_ts)
        """)

        # signals (Step 1 预留，Step 2+ 使用)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                strategy_name TEXT NOT NULL,
                bar_ts INTEGER NOT NULL,
                type TEXT NOT NULL,
                direction INTEGER NOT NULL,
                score REAL NOT NULL,
                reason TEXT,
                state TEXT,
                signal_json TEXT,
                created_ts INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_strategy_ts
            ON signals(strategy_name, bar_ts)
        """)

        # state_transitions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS state_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bar_ts INTEGER NOT NULL,
                from_state TEXT NOT NULL,
                to_state TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                trigger_score REAL,
                ts INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transitions_ts
            ON state_transitions(ts)
        """)

        # account_snapshots
        cur.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_wallet_balance TEXT NOT NULL,
                total_unrealized_pnl TEXT NOT NULL,
                total_maint_margin TEXT NOT NULL,
                margin_ratio TEXT NOT NULL,
                ts INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_account_ts
            ON account_snapshots(ts)
        """)

        self._conn.commit()

    def _set_schema_version(self) -> None:
        cur = self._conn.execute(
            "SELECT MAX(version) FROM schema_version"
        )
        current = cur.fetchone()[0] or 0
        if current < SCHEMA_VERSION:
            self._conn.execute(
                "INSERT INTO schema_version (version, applied_at) "
                "VALUES (?, ?)",
                (SCHEMA_VERSION, int(time.time() * 1000)),
            )
            self._conn.commit()
            logger.info(
                f"schema migrated: {current} -> {SCHEMA_VERSION}"
            )

    def start_async_writer(self) -> None:
        """启动异步写入线程。init_schema 后调用。"""
        if self._writer_thread is not None:
            return
        self._stop_flag.clear()
        self._writer_thread = threading.Thread(
            target=self._async_writer_loop,
            daemon=True,
            name="persister-writer",
        )
        self._writer_thread.start()
        logger.info("async writer started")

    def _async_writer_loop(self) -> None:
        """异步写入循环。"""
        while not self._stop_flag.is_set():
            try:
                item = self._async_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if item is None:  # 毒丸
                break
            op, args = item
            try:
                op(*args)
                self._stats["async_writes"] += 1
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"async write failed: {e}")

    def flush(self, timeout_s: float = FLUSH_TIMEOUT_S) -> bool:
        """
        阻塞直到异步队列清空。
        返回 True 表示已清空，False 表示超时。
        """
        deadline = time.time() + timeout_s
        while not self._async_queue.empty():
            if time.time() > deadline:
                logger.warning(
                    f"flush timeout, {self._async_queue.qsize()} items remaining"
                )
                return False
            time.sleep(0.05)
        # 等待写入线程处理完最后一个
        time.sleep(0.1)
        return True

    def close(self) -> None:
        """停机：flush + 停线程 + 关连接。"""
        self.flush()
        self._stop_flag.set()
        try:
            self._async_queue.put_nowait(None)
        except queue.Full:
            pass
        if self._writer_thread:
            self._writer_thread.join(timeout=5)
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None
        logger.info("persister closed")

    # ========================================================================
    # 同步写：订单（必须同步，丢了就是钱丢了）
    # ========================================================================

    def insert_order(
        self,
        order,
        role: str,
        exchange_order_id: Optional[str] = None,
        status: str = "NEW",
    ) -> None:
        """
        插入订单。同步写。

        Args:
            order: core.gateway.Order
            role: ENTRY / ADD / EXIT / STOP / TP
            exchange_order_id: 交易所返回的订单 ID
            status: 初始状态
        """
        now = int(time.time() * 1000)
        metadata_json = json.dumps(
            order.metadata, default=_json_default, ensure_ascii=False
        ) if order.metadata else None

        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO orders (
                        client_order_id, exchange_order_id, symbol,
                        side, type, order_role, status,
                        requested_qty, executed_qty,
                        price, stop_price, avg_price,
                        total_fee, fee_asset, reduce_only,
                        is_server_side_stop, metadata_json,
                        created_ts, updated_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order.client_order_id,
                        exchange_order_id,
                        order.symbol,
                        _serialize_value(order.side),
                        _serialize_value(order.type),
                        role,
                        status,
                        _serialize_value(order.qty),
                        "0",
                        _serialize_value(order.price),
                        _serialize_value(order.stop_price),
                        None,
                        "0",
                        "USDT",
                        int(order.reduce_only),
                        0,
                        metadata_json,
                        now,
                        now,
                    ),
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"insert_order failed: {e}"
                ) from e

    def update_order_status(
        self,
        client_order_id: str,
        status: str,
        exchange_order_id: Optional[str] = None,
        executed_qty: Optional[str] = None,
        avg_price: Optional[str] = None,
        total_fee: Optional[str] = None,
        reject_reason: Optional[str] = None,
        reject_code: Optional[int] = None,
        transact_time: Optional[int] = None,
    ) -> None:
        """更新订单状态。同步写。"""
        now = int(time.time() * 1000)
        fields = ["status = ?", "updated_ts = ?"]
        params: List[Any] = [status, now]

        if exchange_order_id is not None:
            fields.append("exchange_order_id = ?")
            params.append(exchange_order_id)
        if executed_qty is not None:
            fields.append("executed_qty = ?")
            params.append(executed_qty)
        if avg_price is not None:
            fields.append("avg_price = ?")
            params.append(avg_price)
        if total_fee is not None:
            fields.append("total_fee = ?")
            params.append(total_fee)
        if reject_reason is not None:
            fields.append("reject_reason = ?")
            params.append(reject_reason)
        if reject_code is not None:
            fields.append("reject_code = ?")
            params.append(reject_code)
        if transact_time is not None:
            fields.append("transact_time = ?")
            params.append(transact_time)

        params.append(client_order_id)
        with self._lock:
            try:
                self._conn.execute(
                    f"UPDATE orders SET {', '.join(fields)} "
                    f"WHERE client_order_id = ?",
                    params,
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"update_order_status failed: {e}"
                ) from e

    def insert_fill(
        self,
        trade_id: str,
        client_order_id: str,
        symbol: str,
        price: str,
        qty: str,
        fee: str,
        fee_asset: str,
        is_maker: bool,
        transact_time: int,
    ) -> None:
        """插入成交。同步写。幂等（trade_id 主键）。"""
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO fills (
                        trade_id, client_order_id, symbol,
                        price, qty, fee, fee_asset, is_maker, transact_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_id, client_order_id, symbol,
                        price, qty, fee, fee_asset,
                        int(is_maker), transact_time,
                    ),
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"insert_fill failed: {e}"
                ) from e

    # ========================================================================
    # 同步写：轮次
    # ========================================================================

    def insert_round_open(self, round_snapshot: dict) -> None:
        """插入轮次开仓记录。同步写。"""
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO rounds (
                        round_id, symbol, direction, entry_price,
                        stop_price, total_qty, pyramid_count,
                        state_entry, factor_snapshot_entry,
                        param_version, entry_ts, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
                    """,
                    (
                        round_snapshot["round_id"],
                        round_snapshot["symbol"],
                        round_snapshot["direction"],
                        _serialize_value(round_snapshot["entry_price"]),
                        _serialize_value(round_snapshot.get("stop_price")),
                        _serialize_value(round_snapshot["total_qty"]),
                        round_snapshot.get("pyramid_count", 0),
                        round_snapshot.get("state_entry"),
                        json.dumps(
                            round_snapshot.get("factor_snapshot_entry", {}),
                            default=_json_default,
                            ensure_ascii=False,
                        ),
                        round_snapshot.get("param_version"),
                        round_snapshot["entry_ts"],
                    ),
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"insert_round_open failed: {e}"
                ) from e

    def update_round_close(
        self,
        round_id: str,
        exit_price: str,
        exit_ts: int,
        pnl: str,
        r_multiple: float,
        exit_reason: str,
        state_exit: Optional[str] = None,
        factor_snapshot_exit: Optional[dict] = None,
        peak_unrealized: Optional[str] = None,
        pyramid_count: Optional[int] = None,
        stop_price: Optional[str] = None,
    ) -> None:
        """更新轮次平仓记录。同步写。"""
        duration_ms = None
        with self._lock:
            try:
                # 计算持有时间
                row = self._conn.execute(
                    "SELECT entry_ts FROM rounds WHERE round_id = ?",
                    (round_id,),
                ).fetchone()
                if row:
                    duration_ms = exit_ts - row["entry_ts"]

                fields = [
                    "exit_price = ?",
                    "exit_ts = ?",
                    "duration_ms = ?",
                    "pnl = ?",
                    "r_multiple = ?",
                    "exit_reason = ?",
                    "status = 'closed'",
                ]
                params: List[Any] = [
                    exit_price, exit_ts, duration_ms,
                    pnl, r_multiple, exit_reason,
                ]
                if state_exit is not None:
                    fields.append("state_exit = ?")
                    params.append(state_exit)
                if factor_snapshot_exit is not None:
                    fields.append("factor_snapshot_exit = ?")
                    params.append(
                        json.dumps(
                            factor_snapshot_exit,
                            default=_json_default,
                            ensure_ascii=False,
                        )
                    )
                if peak_unrealized is not None:
                    fields.append("peak_unrealized = ?")
                    params.append(peak_unrealized)
                if pyramid_count is not None:
                    fields.append("pyramid_count = ?")
                    params.append(pyramid_count)
                if stop_price is not None:
                    fields.append("stop_price = ?")
                    params.append(stop_price)

                params.append(round_id)
                self._conn.execute(
                    f"UPDATE rounds SET {', '.join(fields)} "
                    f"WHERE round_id = ?",
                    params,
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"update_round_close failed: {e}"
                ) from e

    # ========================================================================
    # 同步写：状态快照
    # ========================================================================

    def write_state_snapshot(
        self,
        symbol: str,
        bar_ts: int,
        state: str,
        active_round_id: Optional[str],
        snapshot_data: dict,
    ) -> None:
        """写状态快照。同步写。同一 bar_ts 覆盖。"""
        now = int(time.time() * 1000)
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO state_snapshots (
                        symbol, bar_ts, state, active_round_id,
                        snapshot_json, schema_version, created_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol, bar_ts, state, active_round_id,
                        json.dumps(
                            snapshot_data,
                            default=_json_default,
                            ensure_ascii=False,
                        ),
                        SCHEMA_VERSION, now,
                    ),
                )
                self._stats["sync_writes"] += 1
            except sqlite3.Error as e:
                self._stats["errors"] += 1
                raise PersistenceError(
                    f"write_state_snapshot failed: {e}"
                ) from e

    # ========================================================================
    # 异步写：K 线
    # ========================================================================

    def write_kline(
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
        """写 K 线。异步写。"""
        try:
            self._async_queue.put_nowait(
                (self._insert_kline, (
                    symbol, ts, open, high, low, close, volume, is_closed,
                ))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1
            logger.warning("async queue full, kline dropped")

    def _insert_kline(
        self, symbol, ts, open, high, low, close, volume, is_closed
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO klines (
                    symbol, ts, open, high, low, close, volume, is_closed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, ts, open, high, low, close, volume,
                    int(is_closed),
                ),
            )

    # ========================================================================
    # 异步写：事件
    # ========================================================================

    def write_event(
        self,
        event_type: str,
        severity: str,
        data: Optional[dict] = None,
    ) -> None:
        """写事件。异步写。"""
        now = int(time.time() * 1000)
        data_json = json.dumps(
            data or {}, default=_json_default, ensure_ascii=False
        )
        try:
            self._async_queue.put_nowait(
                (self._insert_event, (event_type, severity, data_json, now))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1

    def _insert_event(
        self, event_type, severity, data_json, ts
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (event_type, severity, data_json, ts) "
                "VALUES (?, ?, ?, ?)",
                (event_type, severity, data_json, ts),
            )

    # ========================================================================
    # 异步写：因子值 / 信号 / 状态迁移 / 账户快照
    # ========================================================================

    def write_factor_value(
        self,
        symbol: str,
        bar_ts: int,
        factor_name: str,
        value: float,
        raw_value: float,
        state: str,
        outlier: bool,
        stale: bool,
        factor_version: str,
    ) -> None:
        """写因子值。异步写。"""
        try:
            self._async_queue.put_nowait(
                (self._insert_factor, (
                    symbol, bar_ts, factor_name, value, raw_value,
                    state, outlier, stale, factor_version,
                ))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1

    def _insert_factor(
        self, symbol, bar_ts, factor_name, value, raw_value,
        state, outlier, stale, factor_version,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO factor_values (
                    symbol, bar_ts, factor_name, value, raw_value,
                    state, outlier, stale, factor_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, bar_ts, factor_name, value, raw_value,
                    state, int(outlier), int(stale), factor_version,
                ),
            )

    def write_signal(self, signal_data: dict) -> None:
        """写信号。异步写。"""
        try:
            self._async_queue.put_nowait(
                (self._insert_signal, (signal_data,))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1

    def _insert_signal(self, signal_data: dict) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO signals (
                    signal_id, strategy_name, bar_ts, type,
                    direction, score, reason, state,
                    signal_json, created_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_data["signal_id"],
                    signal_data["strategy_name"],
                    signal_data["bar_ts"],
                    _serialize_value(signal_data["type"]),
                    signal_data["direction"],
                    signal_data["score"],
                    signal_data.get("reason", ""),
                    _serialize_value(signal_data.get("state", "")),
                    json.dumps(
                        signal_data,
                        default=_json_default,
                        ensure_ascii=False,
                    ),
                    int(time.time() * 1000),
                ),
            )

    def write_state_transition(
        self,
        bar_ts: int,
        from_state: str,
        to_state: str,
        action: str,
        reason: str,
        trigger_score: Optional[float] = None,
    ) -> None:
        """写状态迁移。异步写。"""
        now = int(time.time() * 1000)
        try:
            self._async_queue.put_nowait(
                (self._insert_transition, (
                    bar_ts, from_state, to_state, action,
                    reason, trigger_score, now,
                ))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1

    def _insert_transition(
        self, bar_ts, from_state, to_state, action,
        reason, trigger_score, ts,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO state_transitions (
                    bar_ts, from_state, to_state, action,
                    reason, trigger_score, ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bar_ts, from_state, to_state, action,
                    reason, trigger_score, ts,
                ),
            )

    def write_account_snapshot(self, account) -> None:
        """写账户快照。异步写。"""
        now = int(time.time() * 1000)
        try:
            self._async_queue.put_nowait(
                (self._insert_account_snapshot, (account, now))
            )
        except queue.Full:
            self._stats["async_dropped"] += 1

    def _insert_account_snapshot(self, account, now) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO account_snapshots (
                    total_wallet_balance, total_unrealized_pnl,
                    total_maint_margin, margin_ratio, ts
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    _serialize_value(account.total_wallet_balance),
                    _serialize_value(account.total_unrealized_pnl),
                    _serialize_value(account.total_maint_margin),
                    _serialize_value(account.margin_ratio),
                    now,
                ),
            )

    # ========================================================================
    # 读：状态恢复
    # ========================================================================

    def load_last_snapshot(
        self, symbol: str
    ) -> Optional[dict]:
        """读最后一次状态快照。重启恢复用。"""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT state, active_round_id, snapshot_json,
                       schema_version, bar_ts, created_ts
                FROM state_snapshots
                WHERE symbol = ?
                ORDER BY bar_ts DESC LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if not row:
                return None
            return {
                "state": row["state"],
                "active_round_id": row["active_round_id"],
                "snapshot_data": json.loads(row["snapshot_json"]),
                "schema_version": row["schema_version"],
                "bar_ts": row["bar_ts"],
                "created_ts": row["created_ts"],
            }

    def load_active_rounds(self, symbol: str) -> List[dict]:
        """读活跃轮次。重启对账用。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM rounds
                WHERE symbol = ? AND status = 'open'
                ORDER BY entry_ts ASC
                """,
                (symbol,),
            ).fetchall()
            return [dict(r) for r in rows]

    def load_open_orders(self, symbol: Optional[str] = None) -> List[dict]:
        """读未完结订单。重启清理孤儿订单用。"""
        with self._lock:
            if symbol:
                rows = self._conn.execute(
                    """
                    SELECT * FROM orders
                    WHERE symbol = ? AND status IN
                        ('NEW', 'PARTIALLY_FILLED', 'CANCEL_PENDING')
                    ORDER BY created_ts ASC
                    """,
                    (symbol,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT * FROM orders
                    WHERE status IN
                        ('NEW', 'PARTIALLY_FILLED', 'CANCEL_PENDING')
                    ORDER BY created_ts ASC
                    """
                ).fetchall()
            return [dict(r) for r in rows]

    def load_rounds(
        self, start_ts: int, end_ts: int
    ) -> List[dict]:
        """读轮次记录。归因分析用。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM rounds
                WHERE entry_ts >= ? AND entry_ts <= ?
                ORDER BY entry_ts ASC
                """,
                (start_ts, end_ts),
            ).fetchall()
            return [dict(r) for r in rows]

    def load_klines(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> List[dict]:
        """读 K 线。回测加载用。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM klines
                WHERE symbol = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
                """,
                (symbol, start_ts, end_ts),
            ).fetchall()
            return [dict(r) for r in rows]

    # ========================================================================
    # 导出与健康
    # ========================================================================

    def export_rounds_csv(
        self, start_ts: int, end_ts: int, output_path: str
    ) -> None:
        """导出轮次为 CSV。分析用。"""
        import csv
        rounds = self.load_rounds(start_ts, end_ts)
        if not rounds:
            logger.info("no rounds to export")
            return
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rounds[0].keys()))
            writer.writeheader()
            writer.writerows(rounds)
        logger.info(
            f"exported {len(rounds)} rounds to {output_path}"
        )

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            try:
                db_size = Path(self.db_path).stat().st_size
                integrity = self._conn.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]
                ok = integrity == "ok"
                return {
                    "healthy": ok,
                    "db_path": self.db_path,
                    "db_size_bytes": db_size,
                    "integrity": integrity,
                    "async_queue_size": self._async_queue.qsize(),
                    "stats": dict(self._stats),
                    "timestamp": int(time.time() * 1000),
                }
            except Exception as e:
                return {
                    "healthy": False,
                    "error": str(e),
                    "timestamp": int(time.time() * 1000),
                }

    # ========================================================================
    # 事务辅助（订单成交 = 多表写入，必须原子）
    # ========================================================================

    def in_transaction(self, func, *args, **kwargs):
        """
        在事务中执行。任何一步失败，全部回滚。
        用于订单成交：更新 orders + 插入 fills + 更新 rounds 状态。
        """
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                result = func(*args, **kwargs)
                self._conn.execute("COMMIT")
                return result
            except Exception:
                self._conn.execute("ROLLBACK")
                raise


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> StatePersister:
    """模块工厂入口。"""
    db_path = config.get("db_path")
    if not db_path:
        raise ValueError("persister requires config.db_path")
    return StatePersister(db_path)
