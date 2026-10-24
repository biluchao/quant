#!/usr/bin/env python3
"""
run_shadow.py - 实时影子模式入口（Step 1 最小运行集）

与 main.py 的区别：
  - 用实时数据流（WebSocket），但订单走 VirtualGateway，不碰真钱
  - 增加 SignalTracker，记录每个信号的后续表现
  - 增加每日归因报告，不等到 72 小时结束
  - API 默认关闭，--with-api 显式开启
  - 强制最小运行 72 小时，中途停也生成报告但标记 INCOMPLETE

与 run_backtest.py 的区别：
  - 数据源是实时 WebSocket，不是历史库
  - 虚拟成交用实时盘口快照撮合（吃对手盘前 5 档），不是 K 线收盘价
  - 包含成交延迟模拟（默认 80ms）
  - 滑点模型每 6 小时用实时盘口校准

设计目标：
  验证系统层——数据流稳定性、K 线一致性、状态机闭环、虚拟成交合理性
  验证策略层——信号质量、假信号率、因子 IC、状态迁移频率

作者：张三
审查：比蒙（策略）、乐高（执行）
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
import traceback
from collections import deque, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import yaml

from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.rate_limiter import RateLimiter
from system.kline_authority import KlineAuthority
from system.data_buffer import RingBuffer
from system.kline_repair import KlineRepair
from system.order_idempotent import OrderIdGenerator

from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream
from data.rest_client import RestClient
from data.historical_loader import HistoricalLoader
from data.tick_db import TickDB

from factors.factor_registry import FactorRegistry
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor

from states.state_machine import StateMachine
from rounds.round_manager import RoundManager

from execution.gateway_virtual import VirtualGateway
from execution.slippage_model import SlippageModel
from execution.fill_model_market import MarketFillModel

from risk.policy_minimal import MinimalRiskPolicy
from risk.emergency_stop import EmergencyStop


# ==================== 常量 ====================
WARMUP_BARS = 500              # 因子预热
DEFAULT_MIN_HOURS = 72         # 最小运行时长
SNAPSHOT_STALE_MS = 3000       # 盘口快照过期阈值
RECALIBRATE_INTERVAL_S = 6 * 3600  # 时钟和滑点每 6 小时校准
MAX_CLOSED_ROUNDS_IN_MEMORY = 100  # 内存里最多保留的已结束轮次
PROGRESS_INTERVAL_S = 3600     # 每小时打印进度

EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_DATA_ERROR = 3
EXIT_RUNTIME_ERROR = 4
EXIT_INCOMPLETE = 5


class ShadowError(Exception):
    def __init__(self, msg, code=EXIT_CONFIG_ERROR):
        super().__init__(msg)
        self.code = code


class ConnectionMonitor:
    """所有数据流共享：任一流断线，全局标记 data_degraded。"""
    def __init__(self):
        self._lock = threading.Lock()
        self._status = {}
        self._last_msg_ts = {}

    def register(self, name):
        with self._lock:
            self._status[name] = True
            self._last_msg_ts[name] = time.time() * 1000

    def on_message(self, name):
        with self._lock:
            self._status[name] = True
            self._last_msg_ts[name] = time.time() * 1000

    def on_disconnect(self, name):
        with self._lock:
            self._status[name] = False

    def is_degraded(self) -> bool:
        with self._lock:
            if not all(self._status.values()):
                return True
            now = time.time() * 1000
            for name, ts in self._last_msg_ts.items():
                if now - ts > 10000:  # 10 秒没消息
                    return True
            return False

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._status)


class LiveSnapshotFeed:
    """
    实时盘口快照缓存，供 VirtualGateway 按时间戳查询。
    维护最近 60 秒的快照，按 ts 索引。
    """
    def __init__(self, stale_ms=SNAPSHOT_STALE_MS):
        self.stale_ms = stale_ms
        self._lock = threading.Lock()
        self._snapshots = deque(maxlen=600)  # 200ms 一拍，60 秒约 300 个

    def push(self, snapshot):
        with self._lock:
            self._snapshots.append(snapshot)

    def get_snapshot_at(self, ts):
        with self._lock:
            if not self._snapshots:
                return None
            # 找时间戳最接近的
            best = min(self._snapshots, key=lambda s: abs(s["ts"] - ts))
            if abs(best["ts"] - ts) > self.stale_ms:
                return None  # 过期
            return best

    def latest(self):
        with self._lock:
            return self._snapshots[-1] if self._snapshots else None


class SignalTracker:
    """
    影子模式的核心：记录每个信号，评估信号质量。
    每个信号记录：触发时刻、方向、入场价、后续 3/6/12/24 根 K 线的价格。
    """
    def __init__(self, horizon_bars=(3, 6, 12, 24), persist_dir=None):
        self.horizons = horizon_bars
        self.signals = []
        self._pending = deque()  # 等待后续 K 线的信号
        self.persist_dir = Path(persist_dir or "reports/shadow")
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._signal_seq = 0
        self._counter_lock = threading.Lock()

    def next_signal_id(self) -> str:
        with self._counter_lock:
            self._signal_seq += 1
            return f"sh-{datetime.utcnow().strftime('%Y%m%d')}-{self._signal_seq:06d}"

    def record_signal(self, signal: dict):
        """
        signal: {
            direction, entry_price, trigger_score, state_from, state_to,
            factor_snapshot (dict), bar_ts
        }
        """
        signal["signal_id"] = self.next_signal_id()
        signal["record_ts"] = time.time() * 1000
        signal["forward_prices"] = {}
        self.signals.append(signal)
        self._pending.append(dict(signal))

    def on_bar_close(self, bar):
        """每根 K 线收线，更新待评估信号的后续价格。"""
        still_pending = deque()
        for sig in self._pending:
            bars_elapsed = self._count_bars_elapsed(sig["bar_ts"], bar.ts)
            if bars_elapsed in self.horizons:
                sig["forward_prices"][str(bars_elapsed)] = bar.close
                if bars_elapsed == max(self.horizons):
                    # 已到最长 horizon，评估完成
                    self._finalize_signal(sig)
                else:
                    still_pending.append(sig)
            else:
                still_pending.append(sig)
        self._pending = still_pending

    def _count_bars_elapsed(self, from_ts, to_ts) -> int:
        # 3 分钟周期
        return int((to_ts - from_ts) / (3 * 60 * 1000))

    def _finalize_signal(self, sig):
        """计算信号质量指标。"""
        entry = sig["entry_price"]
        direction = sig["direction"]
        fwd = sig["forward_prices"]

        sig["direction_correct"] = {}
        for h_str, price in fwd.items():
            move = (price - entry) / entry * direction
            sig["direction_correct"][h_str] = float(move > 0)
            sig["return_pct"][h_str] = float(move * 100)

        # 反向假设：如果按反方向做，各 horizon 的收益
        sig["reverse_return_pct"] = {
            h: -v for h, v in sig["return_pct"].items()
        }

    def flush(self):
        """把已评估的信号和待评估的信号落盘。"""
        if not self.signals:
            return
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self.persist_dir / f"signals_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "finalized": self.signals,
                    "pending": list(self._pending),
                },
                f, ensure_ascii=False, indent=2, default=str,
            )

    def stats(self) -> dict:
        if not self.signals:
            return {"count": 0}
        by_state = defaultdict(lambda: {"count": 0, "correct_6": 0, "ret_6": []})
        for sig in self.signals:
            state = sig.get("state_to", "unknown")
            by_state[state]["count"] += 1
            if sig.get("direction_correct", {}).get("6"):
                by_state[state]["correct_6"] += 1
            if "6" in sig.get("return_pct", {}):
                by_state[state]["ret_6"].append(
                    sig["return_pct"]["6"]
                )

        result = {"total": len(self.signals), "by_state": {}}
        for state, d in by_state.items():
            result["by_state"][state] = {
                "count": d["count"],
                "correct_rate_6bars": (
                    d["correct_6"] / d["count"] if d["count"] else 0
                ),
                "avg_return_6bars": (
                    float(np.mean(d["ret_6"])) if d["ret_6"] else 0
                ),
            }
        return result


class FactorICRecorder:
    """影子模式下滚动计算因子 IC。"""
    def __init__(self, horizons=(3, 6, 12)):
        self.horizons = horizons
        self._factor_history = deque(maxlen=2000)  # 每根 K 线一条
        self._price_history = deque(maxlen=2000)

    def on_bar_close(self, bar, factor_values: dict):
        self._factor_history.append((bar.ts, factor_values))
        self._price_history.append((bar.ts, bar.close))

    def compute_ic(self) -> dict:
        if len(self._factor_history) < 100:
            return {}
        # 对每个 horizon 算 IC
        result = {}
        prices = {ts: p for ts, p in self._price_history}
        for h in self.horizons:
            factor_vals = []
            forward_rets = []
            for i, (ts, fv) in enumerate(self._factor_history):
                future_idx = i + h
                if future_idx >= len(self._price_history):
                    break
                future_ts, future_price = self._price_history[future_idx]
                if ts not in prices:
                    continue
                forward_ret = (future_price - prices[ts]) / prices[ts]
                for name, val in fv.items():
                    factor_vals.append((name, val))
                    forward_rets.append(forward_ret)
            # 按因子分组算 spearman
            from collections import defaultdict
            grouped = defaultdict(lambda: ([], []))
            for (name, val), ret in zip(factor_vals, forward_rets):
                grouped[name][0].append(val)
                grouped[name][1].append(ret)

            ic_h = {}
            for name, (vals, rets) in grouped.items():
                if len(vals) < 30:
                    continue
                try:
                    from scipy.stats import spearmanr
                    corr, _ = spearmanr(vals, rets)
                    ic_h[name] = float(corr) if not np.isnan(corr) else 0.0
                except Exception:
                    ic_h[name] = 0.0
            result[f"h{h}"] = ic_h
        return result


class ShadowRunner:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.instances = {}
        self.logger = None
        self.services = {"streams": []}
        self.shutdown_event = threading.Event()
        self._shutdown_done = False
        self._start_ts = None
        self._min_hours = args.min_hours
        self._emergency_triggered = False
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5
        self._last_progress_ts = 0
        self._last_recalibrate_ts = 0

        # 当前盘口快照
        self._current_snapshot = None
        self._snapshot_lock = threading.Lock()

        # 连接监控
        self.conn_monitor = ConnectionMonitor()

        # 信号追踪
        self.signal_tracker = None
        self.factor_ic_recorder = None

        # 订单 ID 生成
        self._order_seq = 0
        self._order_seq_lock = threading.Lock()

    # ==================== 启动 ====================

    def bootstrap(self):
        self._load_config()
        self._setup_logging()
        self._acquire_pid_lock()
        self._check_disk()
        self._init_clock()
        self._init_risk()
        self._init_data()
        self._init_execution()
        self._init_strategy()
        self._init_tracker()
        self._init_api_if_needed()
        self._register_signals()
        self._start_ts = time.time()
        self._last_progress_ts = self._start_ts
        self._last_recalibrate_ts = self._start_ts

    def _load_config(self):
        config_dir = Path(self.args.config)
        base = config_dir / "base.yaml"
        step = config_dir / "step1.yaml"
        sym = config_dir / "symbols" / f"{self.args.symbol}.yaml"
        for p in (base, step, sym):
            if not p.exists():
                raise ShadowError(f"配置文件不存在: {p}")
        merged = {}
        for p in (base, step, sym):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                    if not isinstance(data, dict):
                        raise ShadowError(f"{p} 顶层必须是字典")
                    merged.update(data)
            except yaml.YAMLError as e:
                raise ShadowError(f"YAML 解析失败 {p}: {e}")

        merged["mode"] = "shadow"
        merged["symbol"] = self.args.symbol
        merged["current_step"] = 1
        merged["log_dir"] = str(Path(merged.get("log_dir", "logs")) / "shadow")
        merged["db_path"] = str(Path("data") / "shadow_quant.db")
        merged["initial_equity"] = float(self.args.initial_equity)

        # 参数 mtime 检查
        step_mtime = step.stat().st_mtime
        main_mtime = Path(__file__).stat().st_mtime
        if step_mtime > main_mtime:
            self.logger = None
            print(
                f"[WARN] step1.yaml 比 run_shadow.py 新，"
                f"确认参数是否与实盘一致",
                file=sys.stderr,
            )

        required = [
            "log_level", "log_dir", "db_path", "mode", "symbol",
            "initial_equity", "per_trade_risk",
            "daily_dd_limit", "daily_dd_halt", "entry_threshold",
        ]
        missing = [k for k in required if k not in merged]
        if missing:
            raise ShadowError(f"配置缺失字段: {missing}")
        self.config = merged

    def _setup_logging(self):
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging(
            log_dir=self.config["log_dir"],
            level=self.config["log_level"],
            mode="shadow",
            symbol=self.args.symbol,
            rotate_hours=1,          # 影子模式按小时滚动
            keep_hours=self._min_hours + 24,
            min_free_gb=10,
        )
        self.logger.info("=" * 60)
        self.logger.info(
            f"影子模式启动 symbol={self.args.symbol} "
            f"min_hours={self._min_hours} pid={os.getpid()}"
        )
        self.logger.info(
            f"equity={self.config['initial_equity']} "
            f"per_trade_risk={self.config['per_trade_risk']}"
        )
        self.logger.warning(
            "WARN: 虚拟成交用实时盘口撮合，但仍有 80ms 延迟模拟，"
            "实盘真实滑点以实盘数据为准"
        )
        self.logger.info("=" * 60)

    def _acquire_pid_lock(self):
        pid_path = Path(self.config["log_dir"]) / ".shadow.pid"
        try:
            fd = os.open(pid_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            try:
                old = int(pid_path.read_text().strip())
                os.kill(old, 0)
                raise ShadowError(
                    f"另一个影子实例正在运行 (pid={old})",
                    EXIT_RUNTIME_ERROR,
                )
            except (ProcessLookupError, ValueError):
                pid_path.unlink()
                fd = os.open(
                    pid_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
                )
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        self._pid_file = pid_path
        self.logger.info(f"PID 锁获取 pid={os.getpid()}")

    def _check_disk(self):
        import shutil
        _, _, free = shutil.disk_usage(self.config["log_dir"])
        free_gb = free / (1024 ** 3)
        self.logger.info(f"磁盘剩余 {free_gb:.2f} GB")
        if free_gb < 10:
            raise ShadowError(
                f"磁盘不足 10GB，拒绝启动: {free_gb:.2f} GB",
                EXIT_RUNTIME_ERROR,
            )

    def _init_clock(self):
        rest = RestClient(
            api_key=self.config.get("binance_api_key", ""),
            api_secret=self.config.get("binance_api_secret", ""),
            base_url=self.config.get("rest_base_url", "https://fapi.binance.com"),
        )
        self.instances["rest_client"] = rest
        clk = HybridClock()
        try:
            clk.sync_from_binance(rest)
        except Exception as e:
            self.logger.warning(f"时钟同步失败: {e}")
        self.instances["clock"] = clk
        offset = clk.get_market_offset_ms()
        self.logger.info(f"时钟偏移 market_offset={offset:.1f} ms")

    def _init_risk(self):
        policy = MinimalRiskPolicy(
            equity=float(self.config["initial_equity"]),
            per_trade_risk=float(self.config["per_trade_risk"]),
            daily_dd_limit=float(self.config["daily_dd_limit"]),
            daily_dd_halt=float(self.config["daily_dd_halt"]),
        )
        self.instances["risk_policy"] = policy
        self.logger.info(f"风控就绪 equity={policy.equity:.2f}")

    def _init_data(self):
        tickdb = TickDB(self.config["db_path"])
        tickdb.set_batch_write(1000)  # 影子模式批量写入
        self.instances["tick_db"] = tickdb

        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer
        self.live_feed = LiveSnapshotFeed()

        # 预热
        try:
            loader = HistoricalLoader(self.instances["rest_client"])
            bars = loader.load_recent_klines(self.args.symbol, "3m", WARMUP_BARS)
            for b in bars:
                buffer.push(b)
            self.logger.info(f"历史 K 线预热 {len(bars)} 根")
        except Exception as e:
            raise ShadowError(
                f"预热失败，无法启动: {e}", EXIT_DATA_ERROR
            )

        # 数据流
        self.conn_monitor.register("kline")
        self.conn_monitor.register("depth")
        self.conn_monitor.register("aggtrade")

        self.services["streams"] = [
            KlineStream(
                symbol=self.args.symbol,
                on_bar=self._on_bar,
                on_message=lambda: self.conn_monitor.on_message("kline"),
                on_disconnect=lambda: self.conn_monitor.on_disconnect("kline"),
            ),
            DepthStream(
                symbol=self.args.symbol,
                on_snapshot=self._on_depth,
                on_message=lambda: self.conn_monitor.on_message("depth"),
                on_disconnect=lambda: self.conn_monitor.on_disconnect("depth"),
            ),
            AggTradeStream(
                symbol=self.args.symbol,
                on_trade=self._on_trade,
                on_message=lambda: self.conn_monitor.on_message("aggtrade"),
                on_disconnect=lambda: self.conn_monitor.on_disconnect("aggtrade"),
            ),
        ]
        self.logger.info("数据流就绪 3 个")

    def _init_execution(self):
        slippage = SlippageModel()
        # 影子模式：用实时盘口快照撮合
        gateway = VirtualGateway(
            feed=self.live_feed,
            slippage_model=slippage,
            clock=self.instances["clock"],
            latency_ms=80,           # 成交延迟模拟
            fee_rate=0.0004,
            id_generator=self._next_order_id,
        )
        self.instances["gateway"] = gateway
        self.instances["slippage"] = slippage
        self.logger.info("虚拟券商就绪 延迟=80ms 实时盘口撮合")

    def _next_order_id(self) -> str:
        with self._order_seq_lock:
            self._order_seq += 1
            return f"sh-{datetime.utcnow().strftime('%Y%m%d')}-{self._order_seq:08d}"

    def _init_strategy(self):
        registry = FactorRegistry()
        registry.register("momentum", MomentumFactor(self.config))
        registry.register("obi", OBIFactor(self.config))
        registry.register("volume", VolumeFactor(self.config))
        self.instances["factor_registry"] = registry

        persister = StatePersister(self.config["db_path"])
        persister.init_schema()
        self.instances["persister"] = persister

        round_mgr = RoundManager(
            risk_policy=self.instances["risk_policy"],
            persister=persister,
            max_in_memory=MAX_CLOSED_ROUNDS_IN_MEMORY,
        )
        self.instances["round_manager"] = round_mgr

        sm = StateMachine(
            factor_registry=registry,
            config=self.config,
            persister=persister,
            risk_policy=self.instances["risk_policy"],
            order_gateway=self.instances["gateway"],
            round_manager=round_mgr,
            on_signal=self._on_signal,       # 信号追踪钩子
            on_state_transition=self._on_state_transition,
        )
        self.instances["state_machine"] = sm
        self.logger.info("状态机/轮次/因子 就绪")

    def _init_tracker(self):
        self.signal_tracker = SignalTracker(
            horizons=(3, 6, 12, 24),
            persist_dir=str(Path(self.config["log_dir"]) / "signals"),
        )
        self.factor_ic_recorder = FactorICRecorder(horizons=(3, 6, 12))
        self.logger.info("信号追踪器就绪 horizon=3/6/12/24 bars")

    def _init_api_if_needed(self):
        if not self.args.with_api:
            self.logger.info("API 未启用（--with-api 可开启）")
            self._api_server = None
            return
        try:
            from api.server import create_app
            import uvicorn
            app = create_app(self)
            cfg = uvicorn.Config(
                app,
                host=self.config.get("api_host", "0.0.0.0"),
                port=int(self.config.get("api_port", 8001)),
                log_level="warning",
            )
            self._api_server = uvicorn.Server(cfg)
            self.logger.info(f"API 就绪 :{cfg.port}")
        except Exception as e:
            self.logger.warning(f"API 启动失败: {e}")
            self._api_server = None

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ==================== 运行时回调 ====================

    def _on_bar(self, bar):
        try:
            self.instances["buffer"].push(bar)
            # 供虚拟券商撮合
            with self._snapshot_lock:
                snap = self._current_snapshot

            # 因子 IC 记录
            factors = {}
            try:
                factors = self.instances["factor_registry"].compute_all(bar, snap)
            except Exception as e:
                self.logger.warning(f"因子计算异常: {e}")
            self.factor_ic_recorder.on_bar_close(bar, factors)

            # 状态机决策
            self.instances["state_machine"].on_bar_close(bar, snap)

            # 信号追踪器评估
            self.signal_tracker.on_bar_close(bar)

            self._consecutive_errors = 0
        except Exception:
            self._consecutive_errors += 1
            self.logger.error(
                f"on_bar 异常 (连续 {self._consecutive_errors}/"
                f"{self._max_consecutive_errors}):\n"
                f"{traceback.format_exc()}"
            )
            if self._consecutive_errors >= self._max_consecutive_errors:
                self.logger.critical("连续异常触发紧急停止")
                self._trigger_emergency()

    def _on_depth(self, snap):
        try:
            with self._snapshot_lock:
                self._current_snapshot = snap
            self.live_feed.push(snap)
        except Exception:
            self.logger.error(f"on_depth 异常:\n{traceback.format_exc()}")

    def _on_trade(self, trade):
        try:
            self.instances["tick_db"].append(trade)
        except Exception:
            self.logger.error(f"on_trade 异常:\n{traceback.format_exc()}")

    def _on_signal(self, signal: dict):
        """状态机触发信号时调用（在实际下单前）。"""
        try:
            self.signal_tracker.record_signal(signal)
            self.logger.info(
                f"信号 {signal.get('signal_id')} "
                f"dir={signal.get('direction')} "
                f"score={signal.get('trigger_score'):.3f} "
                f"state={signal.get('state_from')}->{signal.get('state_to')}"
            )
        except Exception:
            self.logger.error(f"信号记录异常:\n{traceback.format_exc()}")

    def _on_state_transition(self, from_state, to_state, trigger_score):
        try:
            self.logger.debug(
                f"状态迁移 {from_state}->{to_state} score={trigger_score:.3f}"
            )
        except Exception:
            pass

    # ==================== 主循环 ====================

    def run(self):
        self.logger.info("影子模式启动，进入主循环")
        if self._api_server:
            threading.Thread(
                target=self._api_server.run, daemon=True, name="api-server"
            ).start()
        for s in self.services["streams"]:
            threading.Thread(
                target=s.run, daemon=True, name=s.__class__.__name__
            ).start()

        try:
            while not self.shutdown_event.wait(timeout=1.0):
                now = time.time()
                runtime_s = now - self._start_ts
                runtime_h = runtime_s / 3600

                # 检查是否达到最小运行时长
                if runtime_h >= self._min_hours and self._min_hours > 0:
                    self.logger.info(
                        f"已运行 {runtime_h:.2f} 小时，达到最小要求，"
                        f"准备停止"
                    )
                    self.shutdown_event.set()
                    break

                # 定期进度
                if now - self._last_progress_ts >= PROGRESS_INTERVAL_S:
                    self._log_progress(runtime_h)
                    self._last_progress_ts = now

                # 定期校准
                if now - self._last_recalibrate_ts >= RECALIBRATE_INTERVAL_S:
                    self._recalibrate()
                    self._last_recalibrate_ts = now

                # 数据降级监控
                if self.conn_monitor.is_degraded():
                    if not getattr(self, "_degraded_warned", False):
                        self.logger.warning(
                            f"数据流降级 status={self.conn_monitor.snapshot()}"
                        )
                        self._degraded_warned = True
                else:
                    self._degraded_warned = False

                if self._emergency_triggered:
                    break
        except KeyboardInterrupt:
            self.logger.warning("KeyboardInterrupt")
            self.shutdown_event.set()

    def _log_progress(self, runtime_h):
        rm = self.instances["round_manager"]
        sm = self.instances["state_machine"]
        active = len(getattr(rm, "active_rounds", {}))
        closed_total = getattr(rm, "total_closed", len(rm.closed_rounds))
        state = getattr(sm, "current_state", "unknown")
        degraded = self.conn_monitor.is_degraded()
        self.logger.info(
            f"进度 {runtime_h:.2f}/{self._min_hours}h "
            f"state={state} active={active} closed={closed_total} "
            f"degraded={degraded}"
        )

    def _recalibrate(self):
        self.logger.info("定期校准")
        try:
            self.instances["clock"].sync_from_binance(
                self.instances["rest_client"]
            )
            self.logger.info(
                f"时钟重校准 offset={self.instances['clock'].get_market_offset_ms():.1f}ms"
            )
        except Exception as e:
            self.logger.warning(f"时钟重校准失败: {e}")
        try:
            self.instances["slippage"].calibrate_from_snapshots(
                self.live_feed.recent(100)
            )
            self.logger.info("滑点模型重校准完成")
        except Exception as e:
            self.logger.warning(f"滑点重校准失败: {e}")

    def _handle_signal(self, signum, frame):
        self.logger.warning(f"收到信号 {signum}，准备停机")
        self.shutdown_event.set()

    def _trigger_emergency(self):
        self._emergency_triggered = True
        try:
            self.instances["gateway"].cancel_all()
            self.instances["gateway"].close_all_positions()
        except Exception:
            self.logger.error(
                f"紧急停止异常:\n{traceback.format_exc()}"
            )
        self.shutdown_event.set()

    # ==================== 停机与报告 ====================

    def shutdown(self):
        if self._shutdown_done:
            return
        self._shutdown_done = True

        self.logger.info("=== 开始停机 ===")
        runtime_h = (time.time() - self._start_ts) / 3600 if self._start_ts else 0
        complete = runtime_h >= self._min_hours and not self._emergency_triggered

        try:
            # 停流
            for s in self.services.get("streams", []):
                try:
                    s.stop()
                except Exception as e:
                    self.logger.warning(f"停流失败: {e}")

            # 平掉所有虚拟仓位
            try:
                self.instances["gateway"].close_all_positions(
                    reason="shutdown"
                )
            except Exception as e:
                self.logger.warning(f"虚拟平仓失败: {e}")

            # 追踪器落盘
            try:
                self.signal_tracker.flush()
            except Exception as e:
                self.logger.warning(f"信号落盘失败: {e}")

            # 生成报告
            try:
                self._generate_final_report(complete, runtime_h)
            except Exception as e:
                self.logger.error(f"报告生成失败: {e}")

            # 持久化
            try:
                self.instances["persister"].flush()
            except Exception as e:
                self.logger.error(f"持久化失败: {e}")

            self.logger.info(
                f"=== 停机完成 runtime={runtime_h:.2f}h complete={complete} ==="
            )
        finally:
            if self._pid_file and self._pid_file.exists():
                try:
                    self._pid_file.unlink()
                except Exception:
                    pass

    def _generate_final_report(self, complete: bool, runtime_h: float):
        rm = self.instances["round_manager"]
        sm = self.instances["state_machine"]

        report = {
            "meta": {
                "mode": "shadow",
                "symbol": self.args.symbol,
                "start_ts": self._start_ts,
                "end_ts": time.time(),
                "runtime_hours": runtime_h,
                "min_hours_required": self._min_hours,
                "complete": complete,
                "status": "COMPLETE" if complete else "INCOMPLETE",
                "emergency_triggered": self._emergency_triggered,
                "seed_hint": "shadow uses real data, no seed",
            },
            "summary": {
                "total_closed_rounds": getattr(
                    rm, "total_closed", len(rm.closed_rounds)
                ),
                "active_rounds": len(rm.active_rounds),
                "final_state": getattr(sm, "current_state", "unknown"),
            },
            "signal_stats": self.signal_tracker.stats(),
            "factor_ic": self.factor_ic_recorder.compute_ic(),
            "state_machine_stats": getattr(
                sm, "get_state_stats", lambda: {}
            )(),
            "connection_status": self.conn_monitor.snapshot(),
        }

        out_dir = Path(self.config["log_dir"]) / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = out_dir / f"shadow_report_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        self.logger.info(f"最终报告: {path}")

        # 控制台摘要
        print("\n" + "=" * 60)
        print(f"影子模式报告  {self.args.symbol}  status={report['meta']['status']}")
        print("=" * 60)
        print(f"运行时长:      {runtime_h:.2f} h / {self._min_hours} h")
        print(f"结束轮次:      {report['summary']['total_closed_rounds']}")
        print(f"活跃轮次:      {report['summary']['active_rounds']}")
        ss = report["signal_stats"]
        print(f"信号总数:      {ss.get('total', 0)}")
        if ss.get("by_state"):
            print("按状态分组:")
            for state, d in ss["by_state"].items():
                print(
                    f"  {state:20s} count={d['count']:4d} "
                    f"6bar_correct={d['correct_rate_6bars']*100:.1f}% "
                    f"6bar_ret={d['avg_return_6bars']:+.2f}%"
                )
        ic = report["factor_ic"]
        if ic:
            print("因子 IC:")
            for h, ic_dict in ic.items():
                print(f"  {h}: " + " ".join(
                    f"{k}={v:+.3f}" for k, v in ic_dict.items()
                ))
        print("=" * 60 + "\n")


# ==================== 入口 ====================

def parse_args():
    p = argparse.ArgumentParser(description="Quant System Shadow Mode")
    p.add_argument("--config", default="config")
    p.add_argument("--symbol", default="btcusdt")
    p.add_argument("--min-hours", type=float, default=DEFAULT_MIN_HOURS)
    p.add_argument("--initial_equity", type=float, default=10000.0)
    p.add_argument(
        "--with-api", action="store_true",
        help="启用 API 服务（默认关闭）",
    )
    return p.parse_args()


def main():
    args = parse_args()
    runner = ShadowRunner(args)
    code = EXIT_OK
    try:
        runner.bootstrap()
        runner.run()
    except ShadowError as e:
        print(f"[SHADOW ERROR] {e}", file=sys.stderr)
        code = e.code
    except KeyboardInterrupt:
        code = 130
    except Exception as e:
        print(f"[FATAL] {e}\n{traceback.format_exc()}", file=sys.stderr)
        code = EXIT_RUNTIME_ERROR
    finally:
        try:
            runner.shutdown()
        except Exception:
            pass

    # 退出码：如果未跑满最小时长，返回 INCOMPLETE
    if code == EXIT_OK:
        runtime = (time.time() - (runner._start_ts or time.time())) / 3600
        if runtime < runner._min_hours and not runner._emergency_triggered:
            code = EXIT_INCOMPLETE
    sys.exit(code)


if __name__ == "__main__":
    main()
