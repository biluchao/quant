"""
system/log_setup.py — 日志初始化与配置

日志不是"打印字符串"，是系统出问题后唯一能翻的账。

设计原则：
1. 结构化的半结构化日志：`[ts] [level] [logger] [event] key=value ...`
2. 异步写入：交易线程不阻塞在日志上
3. 磁盘保护：磁盘满时降级，不静默丢弃关键日志
4. 脱敏：API Key/Secret/Token 永不入日志
5. UTC 时间戳：统一时区，避免混乱
6. 双文件：main.log（INFO+）+ debug.log（DEBUG 全量）
7. 按小时滚动 + 压缩 + 保留策略

日志格式：
    [2024-06-15T14:23:00.123Z] [INFO] [state_machine] [state_transition] from=BREAKOUT_CONFIRM to=TREND_RIDE trigger_score=0.6234

关键字段规范：
- event: snake_case 事件名（order_submit / order_filled / state_transition / round_open / round_close / risk_reject / emergency_stop）
- 订单类日志必须含：client_order_id / side / qty / status
- 信号类日志必须含：signal_id / bar_ts / direction / score
- 轮次类日志必须含：round_id / direction / entry_price / exit_price / pnl
- 风控类日志必须含：decision / reason
- 所有日志：ts / level / logger / event

级别语义：
- DEBUG:   因子值、中间计算、心跳、未收线 K 线
- INFO:    状态迁移、订单成交、轮次开始/结束、参数变更
- WARN:    数据降级、健康度下降、参数偏离、风控拒绝
- ERROR:   订单失败、对账偏差、因子异常、网络错误
- CRITICAL: 紧急停止、保证金率超标、连续亏损熔断

异步写入：
- 主线程 logger.info() → 队列 → 后台线程写盘
- 队列满：丢弃 DEBUG，保留 INFO+
- 队列严重满（>90%）：打 WARN 到控制台
- 进程退出：shutdown_logging() 阻塞清空队列

磁盘保护：
- 每 60 秒检查磁盘剩余
- < min_free_gb: 打 WARN，清理旧日志
- < critical_free_gb: 停止 DEBUG 写入，只写 WARN+，打 CRITICAL

脱敏：
- 正则匹配 api_key / secret / token / signature / password
- 替换为 ***
- 值长度 > 100 时截断，加 ...truncated

与归因的关系：
- 日志可 grep（快速排查）
- 数据库是权威（完整归因）
- 两者互补，不互相替代

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
import gzip
import logging
import os
import queue
import re
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_QUEUE_SIZE = 10000
DEFAULT_KEEP_HOURS = 720            # 30 天
DEFAULT_DEBUG_KEEP_HOURS = 24       # debug.log 保留 1 天
DEFAULT_MIN_FREE_GB = 10.0
DEFAULT_CRITICAL_FREE_GB = 5.0
DEFAULT_CLEANUP_OLD_DAYS = 30
DEFAULT_MAX_LOG_BYTES = 8 * 1024    # 单条日志 8KB 截断
DISK_CHECK_INTERVAL_S = 60

# 敏感字段正则（命中即脱敏）
SENSITIVE_PATTERNS = [
    re.compile(r"(api_key|apikey|api_secret|secret|token|signature|password)=([^\s&]+)", re.IGNORECASE),
    re.compile(r"(authorization)[:=]\s*(\S+)", re.IGNORECASE),
    re.compile(r"(x-mbx-apikey)[:=]\s*(\S+)", re.IGNORECASE),
]

# 日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


# ============================================================================
# 错误
# ============================================================================
class LogSetupError(Exception):
    """日志初始化错误。"""
    pass


# ============================================================================
# 日志配置
# ============================================================================
@dataclass
class LogConfig:
    """日志配置。"""
    log_dir: str
    level: str = DEFAULT_LOG_LEVEL
    queue_size: int = DEFAULT_QUEUE_SIZE
    keep_hours: int = DEFAULT_KEEP_HOURS
    debug_keep_hours: int = DEFAULT_DEBUG_KEEP_HOURS
    min_free_gb: float = DEFAULT_MIN_FREE_GB
    critical_free_gb: float = DEFAULT_CRITICAL_FREE_GB
    cleanup_old_days: int = DEFAULT_CLEANUP_OLD_DAYS
    enable_debug_file: bool = True
    enable_console: bool = True


# ============================================================================
# 脱敏
# ============================================================================
def _mask_sensitive(msg: str) -> str:
    """脱敏：正则匹配敏感字段，替换为 ***。"""
    if not msg:
        return msg
    for pattern in SENSITIVE_PATTERNS:
        msg = pattern.sub(lambda m: f"{m.group(1)}=***", msg)
    return msg


# ============================================================================
# 格式化器
# ============================================================================
class SanitizedFormatter(logging.Formatter):
    """
    日志格式化器：
    1. UTC 时间戳（ISO 8601）
    2. 脱敏
    3. 超长截断
    4. 结构化 key=value
    """

    def format(self, record: logging.LogRecord) -> str:
        # 时间戳
        ts = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"

        # 基础消息
        msg = record.getMessage() if record.msg else ""

        # 结构化字段（从 extra 中提取）
        event = getattr(record, "event", "")
        extra_parts = []
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "taskName",
                "event",
            ):
                continue
            # 只取自定义字段
            if key in (
                "client_order_id", "signal_id", "round_id", "symbol",
                "side", "qty", "status", "direction", "score", "reason",
                "from_state", "to_state", "trigger_score", "event_type",
                "entry_price", "exit_price", "pnl", "decision", "bar_ts",
                "factor_name", "value", "approved_qty", "level",
            ):
                extra_parts.append(f"{key}={self._fmt_val(value)}")

        # 组装
        parts = [ts, f"{record.levelname:<7}", f"{record.name:<20}"]
        if event:
            parts.append(f"[{event}]")
        if msg:
            parts.append(msg)
        if extra_parts:
            parts.append(" ".join(extra_parts))

        line = " | ".join(parts)

        # 脱敏
        line = _mask_sensitive(line)

        # 截断
        if len(line) > DEFAULT_MAX_LOG_BYTES:
            line = line[:DEFAULT_MAX_LOG_BYTES] + "...truncated"

        # 异常堆栈
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            line += "\n" + record.exc_text

        return line

    @staticmethod
    def _fmt_val(v: Any) -> str:
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "true" if v else "false"
        s = str(v)
        if " " in s or "=" in s:
            return f'"{s}"'
        return s


# ============================================================================
# 异步队列 Handler
# ============================================================================
class AsyncQueueHandler(logging.Handler):
    """
    异步队列 Handler：日志写入队列，后台线程消费。
    队列满时丢弃 DEBUG，保留 INFO+。
    """

    def __init__(self, queue_size: int):
        super().__init__()
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._stop_flag = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._dropped_debug = 0
        self._target_handlers: list = []

    def add_target_handler(self, handler: logging.Handler) -> None:
        """添加最终写入的 handler。"""
        self._target_handlers.append(handler)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._consumer_loop, daemon=True, name="log-consumer"
        )
        self._thread.start()

    def _consumer_loop(self) -> None:
        while not self._stop_flag.is_set():
            try:
                record = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if record is None:
                break
            for handler in self._target_handlers:
                try:
                    handler.handle(record)
                except Exception:
                    pass  # 日志失败不影响主流程

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            if record.levelno <= logging.DEBUG:
                self._dropped_debug += 1
            # INFO+ 也丢（极端情况），但计数

    def flush(self) -> None:
        """阻塞直到队列清空（最多 5 秒）。"""
        deadline = time.time() + 5.0
        while not self._queue.empty():
            if time.time() > deadline:
                break
            time.sleep(0.05)
        time.sleep(0.1)  # 等最后一个处理完

    def close(self) -> None:
        self.flush()
        self._stop_flag.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._thread:
            self._thread.join(timeout=5)
        super().close()


# ============================================================================
# 磁盘检查线程
# ============================================================================
class DiskGuard:
    """磁盘空间监控。低于阈值时清理旧日志。"""

    def __init__(
        self,
        log_dir: Path,
        min_free_gb: float,
        critical_free_gb: float,
        cleanup_old_days: int,
    ):
        self.log_dir = log_dir
        self.min_free_gb = min_free_gb
        self.critical_free_gb = critical_free_gb
        self.cleanup_old_days = cleanup_old_days
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.status: str = "ok"  # ok / warn / critical

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="disk-guard"
        )
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.check()
            except Exception:
                pass
            self._stop.wait(DISK_CHECK_INTERVAL_S)

    def check(self) -> None:
        free_gb = shutil.disk_usage(self.log_dir).free / (1024 ** 3)
        if free_gb < self.critical_free_gb:
            self.status = "critical"
            logger.critical(
                f"disk critical: {free_gb:.2f}GB free, "
                f"cleaning old logs"
            )
            self._cleanup()
        elif free_gb < self.min_free_gb:
            self.status = "warn"
            logger.warning(
                f"disk low: {free_gb:.2f}GB free "
                f"(< {self.min_free_gb}GB)"
            )
            self._cleanup()
        else:
            self.status = "ok"

    def _cleanup(self) -> None:
        """清理旧日志。"""
        cutoff = time.time() - self.cleanup_old_days * 86400
        for f in self.log_dir.glob("*.log*"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    logger.info(f"removed old log: {f.name}")
            except OSError:
                pass

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)


# ============================================================================
# 全局状态
# ============================================================================
_setup_lock = threading.Lock()
_setup_done = False
_async_handler: Optional[AsyncQueueHandler] = None
_disk_guard: Optional[DiskGuard] = None
_config: Optional[LogConfig] = None


# ============================================================================
# 主入口
# ============================================================================
def setup_logging(
    log_dir: str,
    level: str = DEFAULT_LOG_LEVEL,
    mode: str = "live",
    symbol: str = "",
    queue_size: int = DEFAULT_QUEUE_SIZE,
    keep_hours: int = DEFAULT_KEEP_HOURS,
    debug_keep_hours: int = DEFAULT_DEBUG_KEEP_HOURS,
    min_free_gb: float = DEFAULT_MIN_FREE_GB,
    critical_free_gb: float = DEFAULT_CRITICAL_FREE_GB,
    cleanup_old_days: int = DEFAULT_CLEANUP_OLD_DAYS,
    enable_debug_file: bool = True,
    enable_console: bool = True,
) -> logging.Logger:
    """
    初始化日志系统。幂等：重复调用返回已有 root logger。

    Args:
        log_dir: 日志目录
        level: 日志级别
        mode: 运行模式（live / shadow / backtest）
        symbol: 交易对（用于文件名）
        queue_size: 异步队列大小
        keep_hours: main.log 保留小时
        debug_keep_hours: debug.log 保留小时
        min_free_gb: 磁盘警告阈值
        critical_free_gb: 磁盘停止写入阈值
        cleanup_old_days: 紧急清理天数
        enable_debug_file: 是否启用 debug.log
        enable_console: 是否输出到控制台

    Returns:
        root logger
    """
    global _setup_done, _async_handler, _disk_guard, _config

    with _setup_lock:
        if _setup_done:
            return logging.getLogger()

        log_path = Path(log_dir)
        try:
            log_path.mkdir(parents=True, exist_ok=True)
            os.chmod(log_path, 0o700)
        except OSError as e:
            raise LogSetupError(
                f"failed to create log dir {log_dir}: {e}"
            ) from e

        cfg = LogConfig(
            log_dir=str(log_path),
            level=level,
            queue_size=queue_size,
            keep_hours=keep_hours,
            debug_keep_hours=debug_keep_hours,
            min_free_gb=min_free_gb,
            critical_free_gb=critical_free_gb,
            cleanup_old_days=cleanup_old_days,
            enable_debug_file=enable_debug_file,
            enable_console=enable_console,
        )
        _config = cfg

        # ---- 异步 Handler ----
        async_handler = AsyncQueueHandler(queue_size=queue_size)

        # ---- main.log（INFO+）----
        prefix = f"{mode}_{symbol}" if symbol else mode
        main_log_file = log_path / f"{prefix}_main.log"
        main_file_handler = TimedRotatingFileHandler(
            main_log_file,
            when="H",
            interval=1,
            backupCount=keep_hours,
            encoding="utf-8",
            utc=True,
        )
        main_file_handler.setLevel(logging.INFO)
        main_file_handler.setFormatter(SanitizedFormatter())
        _set_file_permission(main_log_file)
        async_handler.add_target_handler(main_file_handler)

        # ---- debug.log（DEBUG 全量）----
        if enable_debug_file:
            debug_log_file = log_path / f"{prefix}_debug.log"
            debug_file_handler = TimedRotatingFileHandler(
                debug_log_file,
                when="H",
                interval=1,
                backupCount=debug_keep_hours,
                encoding="utf-8",
                utc=True,
            )
            debug_file_handler.setLevel(logging.DEBUG)
            debug_file_handler.setFormatter(SanitizedFormatter())
            _set_file_permission(debug_log_file)
            async_handler.add_target_handler(debug_file_handler)

        # ---- 控制台（仅 INFO+）----
        if enable_console:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            console.setFormatter(SanitizedFormatter())
            async_handler.add_target_handler(console)

        async_handler.start()
        _async_handler = async_handler

        # ---- root logger ----
        root = logging.getLogger()
        root.setLevel(getattr(logging, level.upper(), logging.INFO))
        # 移除已有 handler（避免重复）
        for h in list(root.handlers):
            root.removeHandler(h)
        root.addHandler(async_handler)

        # ---- 第三方库降级 ----
        for noisy in (
            "websockets", "urllib3", "asyncio", "uvicorn.access",
            "httpx", "httpcore",
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)

        # ---- 磁盘监控 ----
        guard = DiskGuard(
            log_path, min_free_gb, critical_free_gb, cleanup_old_days
        )
        guard.start()
        _disk_guard = guard

        _setup_done = True

    logging.getLogger("log_setup").info(
        f"logging initialized dir={log_dir} level={level} "
        f"mode={mode} symbol={symbol} "
        f"debug_file={enable_debug_file}"
    )
    return logging.getLogger()


def _set_file_permission(path: Path) -> None:
    """设置日志文件权限为 600。"""
    try:
        if path.exists():
            os.chmod(path, 0o600)
    except OSError:
        pass


# ============================================================================
# 便捷函数
# ============================================================================
def get_logger(name: str) -> logging.Logger:
    """获取 logger。便捷方法，等价于 logging.getLogger(name)。"""
    return logging.getLogger(name)


def shutdown_logging() -> None:
    """停机：清空异步队列，停止磁盘监控。"""
    global _async_handler, _disk_guard, _setup_done
    if _async_handler is not None:
        try:
            _async_handler.close()
        except Exception:
            pass
        _async_handler = None
    if _disk_guard is not None:
        try:
            _disk_guard.stop()
        except Exception:
            pass
        _disk_guard = None
    _setup_done = False
    logging.getLogger("log_setup").info("logging shutdown")


def get_logging_status() -> Dict[str, Any]:
    """健康检查。"""
    root = logging.getLogger()
    handlers = root.handlers
    async_info = {}
    if _async_handler is not None:
        async_info = {
            "queue_size": _async_handler._queue.qsize(),
            "dropped_debug": _async_handler._dropped_debug,
        }
    disk_info = {}
    if _disk_guard is not None:
        disk_info = {"disk_status": _disk_guard.status}
    return {
        "initialized": _setup_done,
        "level": logging.getLevelName(root.level),
        "handlers": len(handlers),
        "async": async_info,
        "disk": disk_info,
        "timestamp": int(time.time() * 1000),
    }


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "log_setup",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["logging"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Async structured logging with disk guard and sanitization",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> logging.Logger:
    """模块工厂入口。"""
    log_cfg = config.get("logging", {})
    return setup_logging(
        log_dir=config.get("log_dir", "logs"),
        level=log_cfg.get("level", DEFAULT_LOG_LEVEL),
        mode=config.get("mode", "live"),
        symbol=config.get("symbol", ""),
        queue_size=log_cfg.get("queue_size", DEFAULT_QUEUE_SIZE),
        keep_hours=log_cfg.get("keep_hours", DEFAULT_KEEP_HOURS),
        min_free_gb=log_cfg.get("min_free_gb", DEFAULT_MIN_FREE_GB),
        critical_free_gb=log_cfg.get(
            "critical_free_gb", DEFAULT_CRITICAL_FREE_GB
        ),
  )
