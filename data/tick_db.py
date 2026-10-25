"""
data/tick_db.py — 行情数据持久化 (重铸标准版)

核心设计原则：
1. 物理隔离：独立的 SQLite 文件，绝不与交易状态库混用，防止写锁冲突。
2. 强类型输出：查询返回的直接就是包含 Decimal 和 int 的标准字典列表。
3. 严格排序：查询结果绝对按时间戳 (ts) 升序，杜绝乱序。
4. 安全并发：开启 WAL 模式，并加 RLock，确保读写不崩溃。
"""

import logging
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class TickDB:
    def __init__(self, db_path: str):
        # 强制生成独立的 tick 数据库文件，避免和系统状态库同名
        p = Path(db_path)
        self.db_path = str(p.with_name(p.stem + "_tick" + p.suffix))
        
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = None
        self._init_schema()

    def _init_schema(self) -> None:
        """初始化表结构"""
        with self._lock:
            self._conn = sqlite3.connect(
                self.db_path, 
                check_same_thread=False, 
                isolation_level=None  # 手动管理事务
            )
            self._conn.row_factory = sqlite3.Row
            
            # 性能与并发核心
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            
            # 建表
            self._conn.execute("""
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
            self._conn.commit()
        logger.info(f"TickDB 初始化完成: {self.db_path}")

    # ========================================================================
    # 写入接口
    # ========================================================================

    def insert_kline(self, symbol: str, ts: int, open_p: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: Decimal) -> None:
        """插入单根 K 线（幂等）"""
        with self._lock:
            self._conn.execute("""
                INSERT OR REPLACE INTO tick_klines 
                (symbol, ts, open, high, low, close, volume, is_closed) 
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                symbol.upper(), int(ts), str(open_p), str(high), 
                str(low), str(close), str(volume)
            ))

    def bulk_insert_klines(self, symbol: str, klines_data: List[Dict[str, Any]]) -> int:
        """批量插入 K 线，返回成功插入数量"""
        if not klines_data:
            return 0
            
        rows = []
        for k in klines_data:
            rows.append((
                symbol.upper(), int(k["ts"]), str(k["open"]), str(k["high"]),
                str(k["low"]), str(k["close"]), str(k["volume"])
            ))
            
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                self._conn.executemany("""
                    INSERT OR REPLACE INTO tick_klines 
                    (symbol, ts, open, high, low, close, volume, is_closed) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, rows)
                self._conn.execute("COMMIT")
                return len(rows)
            except sqlite3.Error as e:
                self._conn.execute("ROLLBACK")
                logger.error(f"批量写入 K 线失败: {e}")
                return 0

    # ========================================================================
    # 查询接口 (强类型输出)
    # ========================================================================

    def query_klines(self, symbol: str, start_ts: int, end_ts: int, limit: int = 50000) -> List[Dict[str, Any]]:
        """
        查询指定时间范围内的 K 线。
        返回标准化格式的字典列表（包含 Decimal），并且时间严格升序。
        """
        with self._lock:
            cursor = self._conn.execute("""
                SELECT ts, open, high, low, close, volume, is_closed 
                FROM tick_klines 
                WHERE symbol = ? AND ts >= ? AND ts <= ? 
                ORDER BY ts ASC 
                LIMIT ?
            """, (symbol.upper(), int(start_ts), int(end_ts), limit))
            
            rows = cursor.fetchall()
            
        result = []
        for r in rows:
            result.append({
                "ts": int(r["ts"]),
                "open": Decimal(str(r["open"])),
                "high": Decimal(str(r["high"])),
                "low": Decimal(str(r["low"])),
                "close": Decimal(str(r["close"])),
                "volume": Decimal(str(r["volume"])),
                "is_closed": bool(r["is_closed"])
            })
        return result

    def get_latest_ts(self, symbol: str) -> Optional[int]:
        """获取指定交易对最新 K 线的时间戳，用于断点续传"""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT MAX(ts) as max_ts FROM tick_klines WHERE symbol = ?",
                (symbol.upper(),)
            )
            row = cursor.fetchone()
            return int(row["max_ts"]) if row and row["max_ts"] else None

    # ========================================================================
    # 生命周期
    # ========================================================================

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None
                logger.info("TickDB 连接已关闭。")


# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "tick_db",
    "type": "data",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["tick_storage"],
    "module_dependencies": [],
    "owner": "zhangsan",
}

def create(config: dict) -> TickDB:
    # 默认使用 data 目录下的回测库
    db_path = config.get("db_path", "data/backtest_quant.db")
    return TickDB(db_path)
