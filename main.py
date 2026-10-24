#!/usr/bin/env python3
"""
main.py - 量化系统 Step 1 主入口

职责：
  1. 配置加载与校验
  2. 日志初始化
  3. 模块自动发现与组装（MANIFEST 驱动）
  4. 服务启动（数据流 / 状态机 / API）
  5. 状态恢复（live 模式）
  6. 优雅停机

启动顺序（不可调换）：
  配置 → 日志 → PID 锁 → 磁盘检查 → 时钟 → 模块发现 → 模块组装
  → 风控 → 数据 → 执行 → 策略 → 状态恢复 → API → 信号注册

作者：张三
审查：比蒙（策略层）、乐高（执行层）
版本：Step 1 v1.0
"""

import argparse
import os
import signal
import sys
import threading
import time
import traceback
from pathlib import Path

import yaml

# ---- 核心抽象 ----
from core.manifest import validate_manifest, ManifestError

# ---- 系统层 ----
from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.param_store import ParamStore
from system.persister import StatePersister
from system.rate_limiter import RateLimiter
from system.kline_authority import KlineAuthority
from system.data_buffer import RingBuffer
from system.kline_repair import KlineRepair
from system.order_idempotent import OrderIdGenerator
from system.module_registry import ModuleRegistry

# ---- 数据层 ----
from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream
from data.rest_client import RestClient
from data.historical_loader import HistoricalLoader
from data.tick_db import TickDB

# ---- 因子 / 策略 / 状态 / 轮次 ----
from factors.factor_registry import FactorRegistry
from states.state_machine import StateMachine
from rounds.round_manager import RoundManager

# ---- 执行 ----
from execution.gateway_binance import BinanceGateway
from execution.gateway_virtual import VirtualGateway
from execution.slippage_model import SlippageModel
from execution.fill_model_market import MarketFillModel

# ---- 风控 ----
from risk.policy_minimal import MinimalRiskPolicy
from risk.emergency_stop import EmergencyStop

# ---- API ----
from api.server import create_app
import uvicorn


# ==================== 退出码 ====================
EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_DEPENDENCY_ERROR = 3
EXIT_RUNTIME_ERROR = 4
EXIT_EMERGENCY = 5


class BootError(Exception):
    """启动阶段的可预期错误。捕获后打印清晰信息，按 code 退出。"""
    def __init__(self, msg, code=EXIT_CONFIG_ERROR):
        super().__init__(msg)
        self.code = code


class QuantSystem:
    def __init__(self, mode: str, config_dir: str, symbol: str):
        self.mode = mode
        self.config_dir = Path(config_dir)
        self.symbol = symbol.lower()

        self.config: dict = {}
        self.instances: dict = {}
        self.services: dict = {"streams": []}

        self.shutdown_event = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutdown_done = False
        self._pid_file: Path | None = None
        self._api_server: uvicorn.Server | None = None

        self._hb_ts: float = 0.0
        self._consecutive_errors: int = 0
        self._max_consecutive_errors: int = 5

        # 最近盘口快照，供 K 线收线时算 OBI
        self._current_snapshot: dict | None = None
        self._snapshot_lock = threading.Lock()

        self._emergency_triggered = False

    # ==================== 启动阶段 ====================

    def bootstrap(self):
        self._load_config()         # 1. 配置（失败走 stderr）
        self._setup_logging()       # 2. 日志（成功后一切走 logger）
        self._acquire_pid_lock()    # 3. PID 原子锁
        self._check_disk()          # 4. 磁盘
        self._init_clock()          # 5. 时钟（REST 校准）
        self._discover_modules()    # 6. 模块发现 + fallback 循环检测
        self._assemble_modules()    # 7. 按 step 组装
        self._init_risk()           # 8. 风控（先于策略）
        self._init_data()           # 9. 数据流 + 缓冲
        self._init_execution()      # 10. 执行网关（依赖数据）
        self._init_strategy()       # 11. 策略 + 状态机 + 轮次
        self._recover_state()       # 12. live 状态恢复
        self._init_api()            # 13. API
        self._register_signals()    # 14. 信号

    def _load_config(self):
        base = self.config_dir / "base.yaml"
        step = self.config_dir / "step1.yaml"
        sym = self.config_dir / "symbols" / f"{self.symbol}.yaml"
        for p in (base, step, sym):
            if not p.exists():
                raise BootError(f"配置文件不存在: {p}", EXIT_CONFIG_ERROR)
        try:
            merged = {}
            for p in (base, step, sym):
                with open(p, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                    if not isinstance(data, dict):
                        raise BootError(f"{p} 顶层必须是字典", EXIT_CONFIG_ERROR)
                    merged.update(data)
        except yaml.YAMLError as e:
            raise BootError(f"YAML 解析失败: {e}", EXIT_CONFIG_ERROR)

        required = [
            "log_level", "log_dir", "db_path", "api_port",
            "current_step", "mode", "symbol",
            "binance_api_key", "binance_api_secret",
        ]
        missing = [k for k in required if k not in merged]
        if missing:
            raise BootError(f"配置缺失字段: {missing}", EXIT_CONFIG_ERROR)

        if merged["symbol"] != self.symbol:
            raise BootError(
                f"命令行 symbol={self.symbol} 与配置 symbol={merged['symbol']} 不一致",
                EXIT_CONFIG_ERROR,
            )
        if merged["mode"] != self.mode:
            raise BootError(
                f"命令行 mode={self.mode} 与配置 mode={merged['mode']} 不一致",
                EXIT_CONFIG_ERROR,
            )
        if merged["current_step"] != 1:
            raise BootError(
                f"main.py 是 Step 1 入口，配置 current_step 必须为 1，"
                f"当前={merged['current_step']}",
                EXIT_CONFIG_ERROR,
            )
        self.config = merged

    def _setup_logging(self):
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        global logger
        logger = setup_logging(
            log_dir=self.config["log_dir"],
            level=self.config["log_level"],
            mode=self.mode,
            symbol=self.symbol,
        )
        logger.info("=" * 60)
        logger.info(f"Quant System Step1 启动  mode={self.mode} symbol={self.symbol} pid={os.getpid()}")
        logger.info("=" * 60)

    def _acquire_pid_lock(self):
        pid_path = self.config_dir / f".quant.{self.symbol}.pid"
        try:
            fd = os.open(pid_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            try:
                old = int(pid_path.read_text().strip())
            except Exception:
                old = -1
            if old > 0:
                try:
                    os.kill(old, 0)
                    raise BootError(
                        f"另一个实例正在运行 (pid={old})", EXIT_RUNTIME_ERROR
                    )
                except ProcessLookupError:
                    logger.warning(f"发现陈旧 pid 文件 (pid={old} 已死)，覆盖")
                    pid_path.unlink()
            # 重试一次
            try:
                fd = os.open(pid_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                raise BootError(f"pid 文件锁竞争失败: {pid_path}", EXIT_RUNTIME_ERROR)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        self._pid_file = pid_path
        logger.info(f"PID 锁获取 pid={os.getpid()}")

    def _check_disk(self):
        import shutil
        log_dir = Path(self.config["log_dir"])
        _, _, free = shutil.disk_usage(log_dir)
        free_gb = free / (1024 ** 3)
        logger.info(f"磁盘剩余 {free_gb:.2f} GB")
        if free_gb < 5:
            raise BootError(f"磁盘不足 5GB，拒绝启动: {free_gb:.2f} GB", EXIT_RUNTIME_ERROR)

    def _init_clock(self):
        rest = self._make_rest_client()
        self.instances["rest_client"] = rest
        clk = HybridClock()
        try:
            clk.sync_from_binance(rest)
        except Exception as e:
            logger.warning(f"时钟同步失败，用本地时间兜底: {e}")
        self.instances["clock"] = clk
        offset = clk.get_market_offset_ms()
        logger.info(f"时钟偏移 market_offset={offset:.1f} ms")
        if abs(offset) > 1000:
            logger.warning("时钟偏移 > 1s，请检查 NTP")

    def _make_rest_client(self) -> RestClient:
        return RestClient(
            api_key=self.config.get("binance_api_key", ""),
            api_secret=self.config.get("binance_api_secret", ""),
            base_url=self.config.get("rest_base_url", "https://fapi.binance.com"),
        )

    def _discover_modules(self):
        reg = ModuleRegistry(
            root_dir=self.config.get("quant_root", "quant/"),
            current_step=self.config["current_step"],
        )
        try:
            self._module_registry = reg
            self._discovered = reg.discover()
        except Exception as e:
            raise BootError(f"模块发现失败: {e}", EXIT_DEPENDENCY_ERROR)

        logger.info(f"发现模块 {len(self._discovered)} 个")

        # fallback 链循环检测
        for name in self._discovered:
            seen, cur = set(), name
            while cur:
                if cur in seen:
                    raise BootError(
                        f"fallback 链循环: {name} → … → {cur}",
                        EXIT_DEPENDENCY_ERROR,
                    )
                seen.add(cur)
                info = self._discovered.get(cur)
                if not info:
                    break
                cur = info["manifest"].get("fallback")

    def _assemble_modules(self):
        try:
            self.instances.update(self._module_registry.assemble(self.config))
        except Exception as e:
            raise BootError(f"模块组装失败: {e}", EXIT_DEPENDENCY_ERROR)

        logger.info(f"组装实例 {len(self.instances)} 个")

        # 关键能力检查（Step1 必须有）
        required_caps = {
            "monotonic_clock", "logging", "rate_limit", "order_idempotent",
            "orderbook_snapshot", "kline_stream", "trade_stream",
        }
        have = set()
        for info in self._discovered.values():
            have.update(info["manifest"].get("capabilities", []))
        missing = required_caps - have
        if missing:
            raise BootError(f"缺少关键能力: {missing}", EXIT_DEPENDENCY_ERROR)

        # 别名：让 gateway_binance 能用统一的 rate_limiter / id_gen
        self.instances.setdefault("rate_limiter", self.instances.get("rate_limit"))
        self.instances.setdefault("order_idempotent_gen",
                                  self.instances.get("order_idempotent"))

    def _init_risk(self):
        equity = float(self.config.get("initial_equity", 1000.0))

        # live 模式：从交易所拿真实权益
        if self.mode == "live":
            try:
                acct = self.instances["rest_client"].get_account_light(
                    self.config["binance_api_key"],
                    self.config["binance_api_secret"],
                )
                equity = float(acct.get("totalWalletBalance", equity))
                logger.info(f"实盘权益来自交易所: {equity:.2f}")
            except Exception as e:
                logger.warning(f"实盘权益拉取失败，用配置兜底: {e}")

        self.instances["equity_snapshot"] = equity
        policy = MinimalRiskPolicy(
            equity=equity,
            per_trade_risk=float(self.config.get("per_trade_risk", 0.007)),
            daily_dd_limit=float(self.config.get("daily_dd_limit", 0.02)),
            daily_dd_halt=float(self.config.get("daily_dd_halt", 0.03)),
        )
        self.instances["risk_policy"] = policy
        logger.info(
            f"风控就绪 equity={equity:.2f} "
            f"per_trade_risk={policy.per_trade_risk} "
            f"dd_limit={policy.daily_dd_limit}"
        )

    def _init_data(self):
        tickdb = TickDB(self.config["db_path"])
        self.instances["tick_db"] = tickdb

        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer

        # live：预热缓冲
        if self.mode in ("live", "shadow"):
            try:
                loader = HistoricalLoader(self.instances["rest_client"])
                bars = loader.load_recent_klines(self.symbol, "3m", 1000)
                for b in bars:
                    buffer.push(b)
                logger.info(f"历史 K 线预热 {len(bars)} 根")
            except Exception as e:
                logger.warning(f"历史 K 线预热失败（缓冲为空，谨慎运行）: {e}")

        self.instances["kline_repair"] = KlineRepair(
            rest=self.instances["rest_client"],
            tickdb=tickdb,
        )

        # 数据流
        if self.mode in ("live", "shadow"):
            streams = [
                KlineStream(
                    symbol=self.symbol,
                    on_bar=self._on_bar,
                ),
                DepthStream(symbol=self.symbol, on_snapshot=self._on_depth),
                AggTradeStream(symbol=self.symbol, on_trade=self._on_trade),
            ]
            self.services["streams"] = streams
            logger.info(f"数据流 {len(streams)} 个（live/shadow）")
        else:
            logger.info("backtest 模式：不启动实时数据流")

    def _init_execution(self):
        clock = self.instances["clock"]
        slippage = SlippageModel()
        self.instances["slippage"] = slippage

        if self.mode == "live":
            api_key = self.config["binance_api_key"]
            api_secret = self.config["binance_api_secret"]
            if not api_key or not api_secret:
                raise BootError("live 模式必须配置 API Key/Secret", EXIT_CONFIG_ERROR)

            gw = BinanceGateway(
                api_key=api_key,
                api_secret=api_secret,
                clock=clock,
                rate_limiter=self.instances["rate_limiter"],
                id_generator=self.instances["order_idempotent_gen"],
                fill_model=MarketFillModel(),
            )
            # 权限探测
            try:
                gw.ping_account_light()
                logger.info("账户权限探测通过")
            except Exception as e:
                raise BootError(f"账户探测失败，检查 API 权限: {e}", EXIT_DEPENDENCY_ERROR)
        else:
            # shadow / backtest：虚拟券商，按 K 线收盘价撮合
            from execution.ring_buffer_feed import RingBufferFeed
            feed = RingBufferFeed(self.instances["buffer"])
            gw = VirtualGateway(
                feed=feed,
                slippage_model=slippage,
                clock=clock,
            )
            if self.mode == "shadow":
                logger.warning(
                    "WARN: shadow 模式虚拟成交只按 K 线收盘价撮合，"
                    "滑点由 SlippageModel 估算，非真实盘口成交。"
                )

        self.instances["gateway"] = gw

    def _init_strategy(self):
        # 因子注册：用 MANIFEST name 查实例
        registry = FactorRegistry()
        for fname in ("momentum", "obi", "volume"):
            inst = self.instances.get(fname)
            if inst is None:
                raise BootError(f"因子缺失: {fname}", EXIT_DEPENDENCY_ERROR)
            registry.register(fname, inst)
        self.instances["factor_registry"] = registry

        # 轮次管理
        round_mgr = RoundManager(
            risk_policy=self.instances["risk_policy"],
            persister=self.instances["persister"],
        )
        self.instances["round_manager"] = round_mgr

        # 状态机：注入完整依赖
        sm = StateMachine(
            factor_registry=registry,
            config=self.config,
            persister=self.instances["persister"],
            risk_policy=self.instances["risk_policy"],
            order_gateway=self.instances["gateway"],
            round_manager=round_mgr,
        )
        self.instances["state_machine"] = sm
        logger.info("策略/状态机/轮次 就绪")

    def _init_persister(self):
        pers = StatePersister(self.config["db_path"])
        pers.init_schema()
        self.instances["persister"] = pers
        logger.info(f"持久化就绪 db={self.config['db_path']}")

    def _recover_state(self):
        if self.mode != "live":
            return
        pers = self.instances["persister"]
        try:
            snap = pers.load_last_snapshot(self.symbol)
        except Exception as e:
            raise BootError(f"读取状态快照失败: {e}", EXIT_RUNTIME_ERROR)

        if not snap or not snap.get("active_round"):
            logger.info("无未完成轮次，跳过恢复")
            return

        logger.warning(f"发现未完成轮次: {snap['active_round']}")

        # 拉交易所持仓（重试 3 次）
        positions = None
        for attempt in range(3):
            try:
                positions = self.instances["gateway"].get_positions()
                break
            except Exception as e:
                logger.warning(f"拉持仓失败 (第{attempt+1}/3): {e}")
                time.sleep(0.5 * (2 ** attempt))
        if positions is None:
            raise BootError(
                "启动时拉取持仓失败，需人工确认（避免裸奔）",
                EXIT_RUNTIME_ERROR,
            )

        try:
            result = self.instances["round_manager"].recover(
                snap["active_round"], positions
            )
            logger.info(f"恢复结果: {result}")
        except Exception as e:
            raise BootError(f"状态恢复失败，需人工确认: {e}", EXIT_RUNTIME_ERROR)

    def _init_api(self):
        app = create_app(self)
        cfg = uvicorn.Config(
            app,
            host=self.config.get("api_host", "0.0.0.0"),
            port=int(self.config.get("api_port", 8000)),
            log_level="warning",
        )
        self._api_server = uvicorn.Server(cfg)
        logger.info(f"API 就绪 :{cfg.port}")

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ==================== 运行时回调 ====================

    def _on_bar(self, bar):
        try:
            self.instances["buffer"].push(bar)
            with self._snapshot_lock:
                snap = self._current_snapshot
            self.instances["state_machine"].on_bar_close(bar, snap)
            self._consecutive_errors = 0
        except Exception:
            self._consecutive_errors += 1
            logger.error(
                f"on_bar 异常 (连续 {self._consecutive_errors}/"
                f"{self._max_consecutive_errors}):\n{traceback.format_exc()}"
            )
            if self._consecutive_errors >= self._max_consecutive_errors:
                logger.critical("连续异常触发紧急停止")
                self._trigger_emergency()

    def _on_depth(self, snap):
        try:
            with self._snapshot_lock:
                self._current_snapshot = snap
        except Exception:
            logger.error(f"on_depth 异常:\n{traceback.format_exc()}")

    def _on_trade(self, trade):
        try:
            self.instances["tick_db"].append(trade)
        except Exception:
            logger.error(f"on_trade 异常:\n{traceback.format_exc()}")

    # ==================== 主循环 ====================

    def run(self):
        logger.info("服务启动")
        if self._api_server:
            threading.Thread(
                target=self._api_server.run, daemon=True, name="api-server"
            ).start()

        for s in self.services.get("streams", []):
            threading.Thread(
                target=s.run, daemon=True, name=s.__class__.__name__
            ).start()

        logger.info("所有线程已启动，进入主循环")
        try:
            while not self.shutdown_event.wait(timeout=1.0):
                self._heartbeat()
                if self._emergency_triggered:
                    raise SystemExit(EXIT_EMERGENCY)
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt")
            self.shutdown_event.set()

    def _heartbeat(self):
        now = time.time()
        if now - self._hb_ts < 30:
            return
        self._hb_ts = now
        sm = self.instances.get("state_machine")
        rm = self.instances.get("round_manager")
        state = getattr(sm, "current_state", "unknown")
        n_rounds = len(getattr(rm, "active_rounds", {})) if rm else 0
        logger.info(f"heartbeat state={state} active_rounds={n_rounds}")

    # ==================== 停机 ====================

    def _handle_signal(self, signum, frame):
        logger.warning(f"收到信号 {signum}，准备停机")
        self.shutdown_event.set()

    def _trigger_emergency(self):
        self._emergency_triggered = True
        try:
            self.instances["gateway"].cancel_all()
            self.instances["gateway"].close_all_positions()
        except Exception:
            logger.error(f"紧急停止异常:\n{traceback.format_exc()}")
        self.shutdown_event.set()

    def shutdown(self):
        """幂等：第二次调用直接返回。"""
        with self._shutdown_lock:
            if self._shutdown_done:
                return
            self._shutdown_done = True

        logger.info("=== 开始停机 ===")
        try:
            # 1. 停 API
            if self._api_server:
                self._api_server.should_exit = True
                deadline = time.time() + 5
                while self._api_server.started and time.time() < deadline:
                    time.sleep(0.1)

            # 2. 停数据流
            for s in self.services.get("streams", []):
                try:
                    s.stop()
                except Exception as e:
                    logger.warning(f"停流失败 {s.__class__.__name__}: {e}")

            # 3. live 模式：服务器端止损兜底（本地进程退出后仍有效）
            if self.mode == "live":
                try:
                    self.instances["gateway"].ensure_server_side_stop()
                    logger.info("服务器端止损已挂，持仓受保护")
                except Exception as e:
                    logger.error(f"服务器端止损挂单失败: {e}")

            # 4. 持久化最后状态
            try:
                self.instances["persister"].flush()
            except Exception as e:
                logger.error(f"持久化失败: {e}")

            logger.info("=== 停机完成 ===")
        except Exception:
            logger.error(f"停机异常:\n{traceback.format_exc()}")
        finally:
            if self._pid_file and self._pid_file.exists():
                try:
                    self._pid_file.unlink()
                except Exception:
                    pass


# ==================== 入口 ====================

def parse_args():
    p = argparse.ArgumentParser(description="Quant System Step1")
    p.add_argument("--mode", choices=["backtest", "shadow", "live"], required=True)
    p.add_argument("--config", default="config", help="配置目录")
    p.add_argument("--symbol", default="btcusdt")
    return p.parse_args()


def main():
    args = parse_args()
    sys_obj = QuantSystem(args.mode, args.config, args.symbol)
    code = EXIT_OK

    try:
        sys_obj.bootstrap()
        sys_obj.run()
    except BootError as e:
        print(f"[BOOT ERROR] {e}", file=sys.stderr)
        code = e.code
    except SystemExit as e:
        code = int(e.code) if e.code is not None else EXIT_OK
    except Exception as e:
        print(f"[FATAL] {e}\n{traceback.format_exc()}", file=sys.stderr)
        code = EXIT_RUNTIME_ERROR
    finally:
        try:
            sys_obj.shutdown()
        except Exception:
            pass

    sys.exit(code)


if __name__ == "__main__":
    main()
