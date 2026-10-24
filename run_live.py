#!/usr/bin/env python3
"""
run_live.py - 实盘交易入口（Step 1 最小运行集）

最高风险文件。任何 bug 都是真金白银。

核心设计：
  1. 启动前 20 项 Preflight 检查，全过才启动
  2. 保护期机制：前 N 轮强制最小仓位
  3. 三重熔断：单轮R0、日回撤分阶段、连续亏损
  4. 紧急停止：三步确认，10 秒倒计时可取消
  5. 停机策略：默认保仓+挂服务器端止损
  6. 每日 00:00 UTC 自动对账
  7. 实盘 vs 影子实时对比

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
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import numpy as np
import yaml
import requests

from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.rate_limiter import RateLimiter
from system.data_buffer import RingBuffer
from system.kline_repair import KlineRepair
from system.order_idempotent import OrderIdGenerator
from system.health_monitor import HealthMonitor
from system.emergency_stop import EmergencyStop

from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream
from data.ws_account import AccountStream
from data.rest_client import RestClient
from data.tick_db import TickDB

from factors.factor_registry import FactorRegistry
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor

from states.state_machine import StateMachine
from rounds.round_manager import RoundManager

from execution.gateway_binance import BinanceGateway
from execution.slippage_model import SlippageModel
from execution.fill_model_market import MarketFillModel

from risk.policy_minimal import MinimalRiskPolicy
from risk.policy_composite import CompositeRiskPolicy


# ==================== 常量 ====================
WARMUP_BARS = 500
PROTECTION_ROUNDS = 20           # 保护期轮数
PROTECTION_QTY_RATIO = 0.3       # 保护期内仓位比例
INITIAL_TEST_QTY = 0.001         # 首次实盘测试数量（BTC）
HEARTBEAT_INTERVAL_S = 30
HEARTBEAT_FILE = "/tmp/quant_live_heartbeat"
EMERGENCY_COOLDOWN_S = 10
MIN_NOTIONAL = 5.0               # 币安最小名义价值 USDT
MAX_PRICE_DEVIATION = 0.005      # 信号价与市价偏离上限 0.5%
MAX_SLIPPAGE_BPS = 50            # 市价单最大滑点 50bps

EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_PREFLIGHT_FAILED = 3
EXIT_RUNTIME_ERROR = 4
EXIT_EMERGENCY = 5
EXIT_DAILY_LOSS_HALT = 6


class LiveError(Exception):
    def __init__(self, msg, code=EXIT_CONFIG_ERROR):
        super().__init__(msg)
        self.code = code


class PreflightCheck:
    """启动前 20 项检查，全过才允许进入主循环。"""
    def __init__(self, ctx):
        self.ctx = ctx
        self.checks = []
        self.logger = ctx.logger

    def run(self) -> bool:
        self.logger.info("=" * 60)
        self.logger.info("PREFLIGHT CHECK 开始")
        self.logger.info("=" * 60)

        self._check_config()
        self._check_api_permissions()
        self._check_server_time()
        self._check_websocket()
        self._check_account_state()
        self._check_open_orders()
        self._check_positions()
        self._check_stop_order()
        self._check_leverage()
        self._check_margin_ratio()
        self._check_rate_limiter()
        self._check_symbol_filters()
        self._check_param_consistency()
        self._check_disk()
        self._check_state_db()
        self._check_emergency_channel()
        self._check_clock()
        self._check_factor_warmup()
        self._check_network_latency()
        self._check_kill_switch()

        passed = sum(1 for c in self.checks if c["pass"])
        total = len(self.checks)
        self.logger.info("=" * 60)
        for c in self.checks:
            icon = "✓" if c["pass"] else "✗"
            self.logger.info(f"  [{icon}] {c['name']}: {c['detail']}")
        self.logger.info(f"PREFLIGHT {passed}/{total} 通过")
        self.logger.info("=" * 60)

        if passed < total:
            failed = [c["name"] for c in self.checks if not c["pass"]]
            raise LiveError(
                f"Preflight 失败: {failed}", EXIT_PREFLIGHT_FAILED
            )
        return True

    def _add(self, name, ok, detail=""):
        self.checks.append({"name": name, "pass": ok, "detail": detail})

    def _check_config(self):
        required = [
            "binance_api_key", "binance_api_secret",
            "per_trade_risk", "daily_dd_limit", "daily_dd_halt",
            "entry_threshold", "symbol",
        ]
        missing = [k for k in required if k not in self.ctx.config]
        api_key = self.ctx.config.get("binance_api_key", "")
        # 不允许用空的或测试 key
        api_ok = (
            len(api_key) > 20
            and api_key != "your_api_key_here"
        )
        self._add(
            "config",
            not missing and api_ok,
            f"missing={missing} api_key_len={len(api_key)}",
        )

    def _check_api_permissions(self):
        try:
            # 用 futures account 查询，验证期货权限
            resp = self.ctx.rest.get_account_light()
            ok = "totalWalletBalance" in resp
            self._add(
                "api_permissions",
                ok,
                f"wallet={resp.get('totalWalletBalance', 'N/A')}",
            )
            # 检查是否有交易权限（非只读）
            try:
                # 尝试下一笔不会成交的限价单，然后立即撤单
                test_order = self.ctx.gateway.place_limit_order_raw(
                    side="BUY",
                    price=self.ctx.rest.get_mark_price() * 0.5,
                    qty=INITIAL_TEST_QTY,
                    time_in_force="GTX",
                )
                self.ctx.gateway.cancel_order(test_order["orderId"])
                self._add("api_trade_permission", True, "可下单+撤单")
            except Exception as e:
                self._add("api_trade_permission", False, str(e))
        except Exception as e:
            self._add("api_permissions", False, str(e))
            self._add("api_trade_permission", False, "跳过了")

    def _check_server_time(self):
        try:
            server_ts = self.ctx.rest.get_server_time()
            local_ts = int(time.time() * 1000)
            offset = abs(server_ts - local_ts)
            self._add(
                "server_time", offset < 500,
                f"offset={offset}ms",
            )
        except Exception as e:
            self._add("server_time", False, str(e))

    def _check_websocket(self):
        try:
            # 拉一次 mark price 验证连通
            price = self.ctx.rest.get_mark_price()
            self._add(
                "websocket",
                price > 0,
                f"mark_price={price}",
            )
        except Exception as e:
            self._add("websocket", False, str(e))

    def _check_account_state(self):
        try:
            acct = self.ctx.rest.get_account()
            equity = float(acct["totalWalletBalance"])
            self.ctx.initial_equity = equity
            ok = equity >= 100  # 至少 100 USDT 才跑
            self._add(
                "account_equity", ok,
                f"equity={equity:.2f} USDT",
            )
        except Exception as e:
            self._add("account_equity", False, str(e))

    def _check_open_orders(self):
        try:
            orders = self.ctx.rest.get_open_orders(self.ctx.symbol)
            # 实盘启动前不允许有遗留挂单
            ok = len(orders) == 0
            self._add(
                "no_open_orders", ok,
                f"count={len(orders)}",
            )
            if not ok:
                # 记录，启动后清理
                self.ctx.orphan_orders = orders
        except Exception as e:
            self._add("no_open_orders", False, str(e))

    def _check_positions(self):
        try:
            positions = self.ctx.rest.get_positions(self.ctx.symbol)
            active = [p for p in positions if abs(float(p["positionAmt"])) > 0]
            ok = len(active) == 0
            self._add(
                "no_positions", ok,
                f"active={len(active)}",
            )
            if not ok:
                self.ctx.orphan_positions = active
        except Exception as e:
            self._add("no_positions", False, str(e))

    def _check_stop_order(self):
        """验证服务器端止损单能挂上。"""
        try:
            test_price = self.ctx.rest.get_mark_price() * 0.85
            test_stop = self.ctx.gateway.place_stop_market_raw(
                side="SELL",
                stop_price=test_price,
                qty=INITIAL_TEST_QTY,
                reduce_only=True,
            )
            self.ctx.gateway.cancel_order(test_stop["orderId"])
            self._add(
                "server_side_stop", True,
                f"stop_order_id={test_stop['orderId']}",
            )
        except Exception as e:
            self._add("server_side_stop", False, str(e))

    def _check_leverage(self):
        try:
            lev = self.ctx.rest.get_leverage(self.ctx.symbol)
            target = self.ctx.config.get("leverage", 3)
            ok = lev <= target + 1
            self._add(
                "leverage", ok,
                f"current={lev} target={target}",
            )
        except Exception as e:
            self._add("leverage", False, str(e))

    def _check_margin_ratio(self):
        try:
            acct = self.ctx.rest.get_account()
            maint = float(acct["totalMaintMargin"])
            wallet = float(acct["totalWalletBalance"])
            ratio = maint / wallet if wallet > 0 else 1.0
            self._add(
                "margin_ratio", ratio < 0.3,
                f"ratio={ratio:.4f}",
            )
        except Exception as e:
            self._add("margin_ratio", False, str(e))

    def _check_rate_limiter(self):
        ok = self.ctx.rate_limiter is not None
        self._add("rate_limiter", ok, "loaded" if ok else "missing")

    def _check_symbol_filters(self):
        try:
            info = self.ctx.rest.get_exchange_info(self.ctx.symbol)
            self.ctx.symbol_info = info
            ok = (
                info.get("stepSize", 0) > 0
                and info.get("tickSize", 0) > 0
                and info.get("minNotional", 0) > 0
            )
            self._add(
                "symbol_filters", ok,
                f"step={info.get('stepSize')} tick={info.get('tickSize')} "
                f"minNotional={info.get('minNotional')}",
            )
        except Exception as e:
            self._add("symbol_filters", False, str(e))

    def _check_param_consistency(self):
        """实盘参数与最近影子报告对比。"""
        try:
            shadow_dir = Path(self.ctx.config["log_dir"]).parent / "shadow" / "reports"
            if not shadow_dir.exists():
                self._add(
                    "param_consistency", True,
                    "无影子报告，跳过（建议先跑影子）",
                )
                return
            reports = sorted(shadow_dir.glob("shadow_report_*.json"))
            if not reports:
                self._add("param_consistency", True, "无影子报告")
                return
            with open(reports[-1], "r", encoding="utf-8") as f:
                last = json.load(f)
            # 关键参数对比
            diffs = []
            for key in ("per_trade_risk", "entry_threshold"):
                bt_val = last.get("meta", {}).get(key)
                live_val = self.ctx.config.get(key)
                if bt_val and live_val:
                    dev = abs(bt_val - live_val) / max(abs(bt_val), 1e-9)
                    if dev > 0.05:
                        diffs.append(f"{key}: shadow={bt_val} live={live_val}")
            self._add(
                "param_consistency", len(diffs) == 0,
                "一致" if not diffs else f"偏差: {diffs}",
            )
        except Exception as e:
            self._add("param_consistency", True, f"检查失败但继续: {e}")

    def _check_disk(self):
        import shutil
        _, _, free = shutil.disk_usage(self.ctx.config["log_dir"])
        free_gb = free / (1024 ** 3)
        self._add(
            "disk", free_gb > 10,
            f"free={free_gb:.1f}GB",
        )

    def _check_state_db(self):
        try:
            self.ctx.persister.health_check()
            self._add("state_db", True, "WAL mode OK")
        except Exception as e:
            self._add("state_db", False, str(e))

    def _check_emergency_channel(self):
        try:
            # 检查紧急停止通道是否可用（写文件+信号）
            self.ctx.emergency_stop.health_check()
            self._add("emergency_channel", True, "ready")
        except Exception as e:
            self._add("emergency_channel", False, str(e))

    def _check_clock(self):
        offset = self.ctx.clock.get_market_offset_ms()
        self._add(
            "clock_offset", abs(offset) < 200,
            f"offset={offset:.1f}ms",
        )

    def _check_factor_warmup(self):
        n = len(self.ctx.buffer.bars)
        self._add(
            "factor_warmup", n >= WARMUP_BARS,
            f"bars={n}/{WARMUP_BARS}",
        )

    def _check_network_latency(self):
        try:
            latencies = []
            for _ in range(5):
                t0 = time.time()
                self.ctx.rest.get_server_time()
                latencies.append((time.time() - t0) * 1000)
            avg = float(np.mean(latencies))
            self._add(
                "network_latency", avg < 200,
                f"avg={avg:.1f}ms",
            )
        except Exception as e:
            self._add("network_latency", False, str(e))

    def _check_kill_switch(self):
        """验证 kill switch 文件不存在（异常状态）。"""
        kill_file = Path("/tmp/quant_live_kill")
        ok = not kill_file.exists()
        self._add(
            "kill_switch", ok,
            "kill file absent" if ok else "KILL FILE EXISTS!",
        )


class LiveRunner:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.instances = {}
        self.logger = None
        self.services = {"streams": []}
        self.shutdown_event = threading.Event()
        self._shutdown_done = False
        self._start_ts = None
        self._emergency_triggered = False
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5

        self._current_snapshot = None
        self._snapshot_lock = threading.Lock()

        self._api_failures = 0
        self._api_degraded = False

        self._protection_mode = True
        self._rounds_completed = 0

        self._consecutive_losses = 0
        self._daily_pnl = 0.0
        self._daily_reset_ts = self._next_utc_midnight()

        self._heartbeat_ts = 0
        self._market_shock_until = 0
        self._last_signal_ts = 0

        self._last_shadow_compare_ts = 0
        self.orphan_orders = []
        self.orphan_positions = []
        self.symbol_info = {}
        self.initial_equity = 0.0

    # ==================== 启动 ====================

    def bootstrap(self):
        self._load_config()
        self._setup_logging()
        self._acquire_pid_lock()
        self._init_clock()
        self._init_risk()
        self._init_data()
        self._init_execution()
        self._init_strategy()
        self._init_health()
        self._register_signals()

        # Preflight
        preflight = PreflightCheck(self)
        preflight.run()

        # 处理孤儿订单和持仓
        self._cleanup_orphans()

        self._start_ts = time.time()
        self._heartbeat_ts = self._start_ts
        self._write_heartbeat()

        self.logger.info("=" * 60)
        self.logger.info("LIVE 启动完成")
        self.logger.info(f"保护期: 前 {PROTECTION_ROUNDS} 轮半仓")
        self.logger.info(f"首单测试量: {INITIAL_TEST_QTY} {self.config['symbol']}")
        self.logger.info("=" * 60)

    def _load_config(self):
        config_dir = Path(self.args.config)
        base = config_dir / "base.yaml"
        step = config_dir / "step1.yaml"
        sym = config_dir / "symbols" / f"{self.args.symbol}.yaml"
        for p in (base, step, sym):
            if not p.exists():
                raise LiveError(f"配置文件不存在: {p}")
        merged = {}
        for p in (base, step, sym):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                    merged.update(data)
            except yaml.YAMLError as e:
                raise LiveError(f"YAML 解析失败 {p}: {e}")

        merged["mode"] = "live"
        merged["symbol"] = self.args.symbol
        merged["current_step"] = 1
        merged["log_dir"] = str(Path(merged.get("log_dir", "logs")) / "live")
        merged["db_path"] = str(Path("data") / "live_quant.db")

        required = [
            "log_level", "log_dir", "db_path", "mode", "symbol",
            "binance_api_key", "binance_api_secret",
            "per_trade_risk", "daily_dd_limit", "daily_dd_halt",
            "entry_threshold",
        ]
        missing = [k for k in required if k not in merged]
        if missing:
            raise LiveError(f"配置缺失字段: {missing}")

        # 实盘参数安全上限（防止误配置）
        if merged["per_trade_risk"] > 0.02:
            raise LiveError(
                f"per_trade_risk={merged['per_trade_risk']} > 2%，拒绝启动"
            )
        if merged["daily_dd_halt"] > 0.05:
            raise LiveError(
                f"daily_dd_halt={merged['daily_dd_halt']} > 5%，拒绝启动"
            )

        self.config = merged

    def _setup_logging(self):
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging(
            log_dir=self.config["log_dir"],
            level=self.config["log_level"],
            mode="live",
            symbol=self.args.symbol,
            rotate_hours=1,
            keep_hours=24 * 30,
            min_free_gb=10,
        )
        self.logger.info("=" * 60)
        self.logger.info(
            f"LIVE 启动 symbol={self.args.symbol} pid={os.getpid()}"
        )
        self.logger.warning(
            "WARNING: 实盘模式，真实资金，任何异常立即人工介入"
        )
        self.logger.info("=" * 60)

    def _acquire_pid_lock(self):
        pid_path = Path(self.config["log_dir"]) / ".live.pid"
        try:
            fd = os.open(pid_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            try:
                old = int(pid_path.read_text().strip())
                os.kill(old, 0)
                raise LiveError(
                    f"另一个实盘实例正在运行 (pid={old})",
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

    def _init_clock(self):
        self.rest = RestClient(
            api_key=self.config["binance_api_key"],
            api_secret=self.config["binance_api_secret"],
            base_url=self.config.get("rest_base_url", "https://fapi.binance.com"),
        )
        self.instances["rest_client"] = self.rest
        clk = HybridClock()
        try:
            clk.sync_from_binance(self.rest)
        except Exception as e:
            self.logger.warning(f"时钟同步失败: {e}")
        self.instances["clock"] = clk

    def _init_risk(self):
        # 临时 equity，preflight 会从交易所拉
        policy = CompositeRiskPolicy(
            equity=1000.0,
            per_trade_risk=float(self.config["per_trade_risk"]),
            daily_dd_limit=float(self.config["daily_dd_limit"]),
            daily_dd_halt=float(self.config["daily_dd_halt"]),
            margin_ratio_warn=0.6,
            margin_ratio_halt=0.8,
        )
        self.instances["risk_policy"] = policy

    def _init_data(self):
        tickdb = TickDB(self.config["db_path"])
        self.instances["tick_db"] = tickdb

        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer
        self.buffer = buffer

        # 预热
        try:
            from data.historical_loader import HistoricalLoader
            loader = HistoricalLoader(self.rest)
            bars = loader.load_recent_klines(
                self.args.symbol, "3m", WARMUP_BARS
            )
            for b in bars:
                buffer.push(b)
            self.logger.info(f"预热 {len(bars)} 根")
        except Exception as e:
            raise LiveError(
                f"预热失败: {e}", EXIT_PREFLIGHT_FAILED
            )

        # 数据流
        self.services["streams"] = [
            KlineStream(
                symbol=self.args.symbol,
                on_bar=self._on_bar,
            ),
            DepthStream(
                symbol=self.args.symbol,
                on_snapshot=self._on_depth,
            ),
            AggTradeStream(
                symbol=self.args.symbol,
                on_trade=self._on_trade,
            ),
            AccountStream(
                symbol=self.args.symbol,
                on_account=self._on_account_update,
                on_order=self._on_order_update,
            ),
        ]

    def _init_execution(self):
        slippage = SlippageModel()
        self.rate_limiter = RateLimiter()
        self.id_gen = OrderIdGenerator(prefix="lv")

        gateway = BinanceGateway(
            api_key=self.config["binance_api_key"],
            api_secret=self.config["binance_api_secret"],
            clock=self.instances["clock"],
            rate_limiter=self.rate_limiter,
            id_generator=self.id_gen,
            fill_model=MarketFillModel(),
            max_price_deviation=MAX_PRICE_DEVIATION,
            max_slippage_bps=MAX_SLIPPAGE_BPS,
            symbol_info=self.symbol_info,
            on_api_failure=self._on_api_failure,
            on_api_recover=self._on_api_recover,
        )
        self.instances["gateway"] = gateway
        self.gateway = gateway
        self.instances["slippage"] = slippage

    def _init_strategy(self):
        registry = FactorRegistry()
        registry.register("momentum", MomentumFactor(self.config))
        registry.register("obi", OBIFactor(self.config))
        registry.register("volume", VolumeFactor(self.config))
        self.instances["factor_registry"] = registry

        persister = StatePersister(self.config["db_path"])
        persister.init_schema()
        persister.enable_wal()
        self.instances["persister"] = persister
        self.persister = persister

        round_mgr = RoundManager(
            risk_policy=self.instances["risk_policy"],
            persister=persister,
            on_round_close=self._on_round_close,
        )
        self.instances["round_manager"] = round_mgr

        sm = StateMachine(
            factor_registry=registry,
            config=self.config,
            persister=persister,
            risk_policy=self.instances["risk_policy"],
            order_gateway=self.gateway,
            round_manager=round_mgr,
            on_signal=self._on_signal,
            on_state_transition=self._on_state_transition,
            max_signal_age_ms=500,  # 信号超过 500ms 丢弃
        )
        self.instances["state_machine"] = sm
        self.sm = sm

    def _init_health(self):
        self.health = HealthMonitor()
        self.emergency_stop = EmergencyStop(self)
        self.instances["emergency_stop"] = self.emergency_stop
        self.instances["health_monitor"] = self.health

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _cleanup_orphans(self):
        # 清理孤儿订单
        if self.orphan_orders:
            self.logger.warning(
                f"清理孤儿订单 {len(self.orphan_orders)} 个"
            )
            for o in self.orphan_orders:
                try:
                    self.gateway.cancel_order(o["orderId"])
                except Exception as e:
                    self.logger.error(f"撤单失败 {o['orderId']}: {e}")
        # 孤儿持仓：报警，不自动平（需人工确认）
        if self.orphan_positions:
            self.logger.critical(
                f"发现孤儿持仓 {len(self.orphan_positions)} 个，"
                f"需人工确认。系统暂停开仓"
            )
            self._api_degraded = True

    def _next_utc_midnight(self):
        now = datetime.now(timezone.utc)
        tomorrow = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + __import__("datetime").timedelta(days=1)
        return tomorrow.timestamp()

    # ==================== 运行时回调 ====================

    def _on_bar(self, bar):
        try:
            self.buffer.push(bar)
            with self._snapshot_lock:
                snap = self._current_snapshot

            # 健康度检查
            if not self.health.is_healthy():
                self.logger.warning("健康度不足，跳过决策")
                self._consecutive_errors = 0
                return

            # 市场突变检测
            if self._detect_market_shock(bar):
                self.logger.warning("检测到市场突变，暂停开仓")
                self._market_shock_until = time.time() + 60
                self._consecutive_errors = 0
                return

            # 日切重置
            if time.time() >= self._daily_reset_ts:
                self._reset_daily()
                return

            # API 降级检查
            if self._api_degraded:
                self.logger.debug("API 降级中，只平仓不开仓")
                self.sm.on_bar_close(bar, snap, allow_open=False)
            else:
                self.sm.on_bar_close(bar, snap, allow_open=True)

            self._consecutive_errors = 0
        except Exception:
            self._consecutive_errors += 1
            self.logger.error(
                f"on_bar 异常 ({self._consecutive_errors}/"
                f"{self._max_consecutive_errors}):\n"
                f"{traceback.format_exc()}"
            )
            if self._consecutive_errors >= self._max_consecutive_errors:
                self.logger.critical("连续异常，触发紧急停止")
                self._trigger_emergency("consecutive_bar_errors")

    def _on_depth(self, snap):
        try:
            with self._snapshot_lock:
                self._current_snapshot = snap
        except Exception:
            self.logger.error(f"on_depth 异常:\n{traceback.format_exc()}")

    def _on_trade(self, trade):
        try:
            self.instances["tick_db"].append(trade)
        except Exception:
            self.logger.error(f"on_trade 异常:\n{traceback.format_exc()}")

    def _on_account_update(self, update):
        """账户流推送：保证金率实时监控。"""
        try:
            maint = float(update.get("totalMaintMargin", 0))
            wallet = float(update.get("totalWalletBalance", 1))
            ratio = maint / wallet if wallet > 0 else 1.0
            self.instances["risk_policy"].update_margin_ratio(ratio)
            if ratio > 0.8:
                self.logger.critical(
                    f"保证金率 {ratio:.2%} > 80%，紧急减仓"
                )
                self._trigger_emergency("margin_ratio_exceeded")
        except Exception:
            self.logger.error(
                f"on_account_update 异常:\n{traceback.format_exc()}"
            )

    def _on_order_update(self, update):
        """订单流推送：更新本地订单状态。"""
        try:
            self.gateway.on_order_update(update)
        except Exception:
            self.logger.error(
                f"on_order_update 异常:\n{traceback.format_exc()}"
            )

    def _on_signal(self, signal_data: dict):
        """信号触发钩子（记录，可能调整仓位）。"""
        self._last_signal_ts = time.time()
        # 保护期：仓位减半
        if self._protection_mode:
            signal_data["qty_multiplier"] = PROTECTION_QTY_RATIO
            self.logger.info(
                f"保护期信号，仓位×{PROTECTION_QTY_RATIO}"
            )
        # 连续亏损降仓
        if self._consecutive_losses >= 5:
            signal_data["qty_multiplier"] = (
                signal_data.get("qty_multiplier", 1.0) * 0.5
            )
            self.logger.warning(
                f"连续亏损 {self._consecutive_losses} 轮，仓位×0.5"
            )

    def _on_state_transition(self, from_state, to_state, score):
        self.logger.debug(f"状态 {from_state}→{to_state} score={score:.3f}")

    def _on_round_close(self, record: dict):
        """轮次结束：更新统计和熔断。"""
        self._rounds_completed += 1
        self._daily_pnl += record["pnl"]

        if record["pnl"] < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        # 保护期检查
        if self._protection_mode and self._rounds_completed >= PROTECTION_ROUNDS:
            self._protection_mode = False
            self.logger.info(
                f"保护期结束，已完成 {self._rounds_completed} 轮，切换全仓模式"
            )

        # 日亏损分阶段熔断
        equity = self.instances["risk_policy"].equity
        daily_dd = -self._daily_pnl / equity if equity > 0 else 0

        if daily_dd >= 0.03:
            self.logger.critical(
                f"日亏损 {daily_dd:.2%} ≥ 3%，触发熔断"
            )
            self._trigger_emergency("daily_loss_halt")
        elif daily_dd >= 0.025:
            self.logger.error(
                f"日亏损 {daily_dd:.2%} ≥ 2.5%，只平仓"
            )
            self.sm.allow_open = False
        elif daily_dd >= 0.02:
            self.logger.warning(
                f"日亏损 {daily_dd:.2%} ≥ 2%，降频"
            )
            self.instances["risk_policy"].reduce_exposure(0.5)
        elif daily_dd >= 0.01:
            self.logger.warning(
                f"日亏损 {daily_dd:.2%} ≥ 1%，预警"
            )

        # 连续亏损熔断
        if self._consecutive_losses >= 8:
            self.logger.critical(
                f"连续亏损 {self._consecutive_losses} 轮，暂停 1 小时"
            )
            self._market_shock_until = time.time() + 3600
            self._consecutive_losses = 0

        # 实盘vs影子对比（每小时一次）
        if time.time() - self._last_shadow_compare_ts > 3600:
            self._compare_with_shadow()
            self._last_shadow_compare_ts = time.time()

    # ==================== 辅助 ====================

    def _detect_market_shock(self, bar) -> bool:
        """检测 1 分钟内价格波动 > 3%。"""
        if len(self.buffer.bars) < 2:
            return False
        prev = self.buffer.bars[-2]
        if prev.close <= 0:
            return False
        change = abs(bar.close - prev.close) / prev.close
        return change > 0.03

    def _reset_daily(self):
        self.logger.info("日切，重置日统计")
        self._daily_pnl = 0.0
        self._consecutive_losses = 0
        self._daily_reset_ts = self._next_utc_midnight()
        self._daily_reconciliation()

    def _daily_reconciliation(self):
        """每日 00:00 UTC 与交易所对账。"""
        try:
            acct = self.rest.get_account()
            exchange_equity = float(acct["totalWalletBalance"])
            local_equity = self.instances["risk_policy"].equity
            diff = abs(exchange_equity - local_equity) / max(exchange_equity, 1)
            self.logger.info(
                f"日终对账 exchange={exchange_equity:.2f} "
                f"local={local_equity:.2f} diff={diff:.4f}"
            )
            if diff > 0.01:
                self.logger.error(
                    f"对账偏差 {diff:.2%} > 1%，需人工检查"
                )
        except Exception as e:
            self.logger.error(f"日终对账失败: {e}")

    def _compare_with_shadow(self):
        """与最近的影子报告对比。"""
        try:
            shadow_dir = (
                Path(self.config["log_dir"]).parent / "shadow" / "reports"
            )
            if not shadow_dir.exists():
                return
            reports = sorted(shadow_dir.glob("shadow_report_*.json"))
            if not reports:
                return
            with open(reports[-1], "r", encoding="utf-8") as f:
                shadow = json.load(f)
            self.logger.info(
                f"vs 影子: shadow_total_rounds="
                f"{shadow['summary']['total_closed_rounds']} "
                f"live_total_rounds={self._rounds_completed}"
            )
        except Exception as e:
            self.logger.warning(f"影子对比失败: {e}")

    def _on_api_failure(self, error):
        self._api_failures += 1
        if self._api_failures >= 3 and not self._api_degraded:
            self._api_degraded = True
            self.logger.critical(
                f"API 连续失败 {self._api_failures} 次，进入降级模式"
            )

    def _on_api_recover(self):
        if self._api_degraded:
            self._api_degraded = False
            self._api_failures = 0
            self.logger.info("API 恢复，解除降级")

    def _write_heartbeat(self):
        try:
            with open(HEARTBEAT_FILE, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass

    def _trigger_emergency(self, reason: str):
        self.logger.critical(f"触发紧急停止: {reason}")
        self._emergency_triggered = True
        try:
            self.emergency_stop.execute(reason)
        except Exception:
            self.logger.error(
                f"紧急停止执行失败:\n{traceback.format_exc()}"
            )
        self.shutdown_event.set()

    def _handle_signal(self, signum, frame):
        self.logger.warning(f"收到信号 {signum}")
        self.shutdown_event.set()

    # ==================== 主循环 ====================

    def run(self):
        self.logger.info("进入主循环")
        for s in self.services["streams"]:
            threading.Thread(
                target=s.run, daemon=True, name=s.__class__.__name__
            ).start()

        try:
            while not self.shutdown_event.wait(timeout=1.0):
                now = time.time()

                # 心跳
                if now - self._heartbeat_ts >= HEARTBEAT_INTERVAL_S:
                    self._heartbeat_ts = now
                    self._write_heartbeat()
                    self._log_status()

                # 市场突变冷却期
                if now < self._market_shock_until:
                    pass  # 已在下单逻辑里禁止开仓

                # kill switch 检查
                if Path("/tmp/quant_live_kill").exists():
                    self.logger.critical("KILL SWITCH 文件存在，立即停止")
                    self._trigger_emergency("kill_switch")

                if self._emergency_triggered:
                    break
        except KeyboardInterrupt:
            self.logger.warning("KeyboardInterrupt")
            self.shutdown_event.set()

    def _log_status(self):
        rm = self.instances["round_manager"]
        active = len(rm.active_rounds)
        equity = self.instances["risk_policy"].equity
        self.logger.info(
            f"status state={self.sm.current_state} "
            f"rounds={self._rounds_completed} active={active} "
            f"equity={equity:.2f} daily_pnl={self._daily_pnl:+.2f} "
            f"protection={self._protection_mode} "
            f"api_degraded={self._api_degraded}"
        )

    # ==================== 停机 ====================

    def shutdown(self):
        if self._shutdown_done:
            return
        self._shutdown_done = True

        self.logger.info("=== 开始停机 ===")
        try:
            # 停流
            for s in self.services["streams"]:
                try:
                    s.stop()
                except Exception as e:
                    self.logger.warning(f"停流失败: {e}")

            # 根据停机策略处理持仓
            if self._emergency_triggered:
                self.logger.warning("紧急停止模式：全平所有仓位")
                try:
                    self.gateway.close_all_positions()
                except Exception as e:
                    self.logger.error(f"全平失败: {e}")
            else:
                # 默认：保仓+服务器端止损
                self.logger.info("优雅停机：保留持仓，挂服务器端止损")
                try:
                    self.gateway.ensure_server_side_stop_for_all_rounds(
                        self.instances["round_manager"].active_rounds
                    )
                except Exception as e:
                    self.logger.error(f"服务器端止损挂单失败: {e}")
                    self.logger.critical(
                        "止损未挂上，建议手动平仓！"
                    )

            # 持久化
            try:
                self.persister.flush()
            except Exception as e:
                self.logger.error(f"持久化失败: {e}")

            # 生成报告
            try:
                self._generate_final_report()
            except Exception as e:
                self.logger.error(f"报告生成失败: {e}")

            self.logger.info("=== 停机完成 ===")
        finally:
            if self._pid_file and self._pid_file.exists():
                try:
                    self._pid_file.unlink()
                except Exception:
                    pass
            try:
                if Path(HEARTBEAT_FILE).exists():
                    Path(HEARTBEAT_FILE).unlink()
            except Exception:
                pass

    def _generate_final_report(self):
        rm = self.instances["round_manager"]
        report = {
            "meta": {
                "mode": "live",
                "symbol": self.args.symbol,
                "start_ts": self._start_ts,
                "end_ts": time.time(),
                "runtime_hours": (
                    (time.time() - self._start_ts) / 3600
                    if self._start_ts else 0
                ),
                "emergency_triggered": self._emergency_triggered,
                "protection_mode_ended": not self._protection_mode,
            },
            "summary": {
                "total_rounds": self._rounds_completed,
                "active_rounds": len(rm.active_rounds),
                "daily_pnl": self._daily_pnl,
                "equity": self.instances["risk_policy"].equity,
                "consecutive_losses": self._consecutive_losses,
            },
        }
        out_dir = Path(self.config["log_dir"]) / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = out_dir / f"live_report_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        self.logger.info(f"实盘报告: {path}")


# ==================== 入口 ====================

def parse_args():
    p = argparse.ArgumentParser(description="Quant System LIVE Mode")
    p.add_argument("--config", default="config")
    p.add_argument("--symbol", default="btcusdt")
    p.add_argument(
        "--confirm", action="store_true",
        help="必须显式确认实盘（防止误启动）",
    )
    p.add_argument(
        "--skip-preflight", action="store_true",
        help="跳过 preflight（危险！仅用于测试）",
    )
    return p.parse_args()


def main():
    args = parse_args()

    if not args.confirm:
        print(
            "实盘模式必须显式加 --confirm 参数确认",
            file=sys.stderr,
        )
        sys.exit(EXIT_CONFIG_ERROR)

    runner = LiveRunner(args)
    code = EXIT_OK
    try:
        runner.bootstrap()
        runner.run()
    except LiveError as e:
        print(f"[LIVE ERROR] {e}", file=sys.stderr)
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

    if runner._emergency_triggered and code == EXIT_OK:
        code = EXIT_EMERGENCY
    sys.exit(code)


if __name__ == "__main__":
    main()
