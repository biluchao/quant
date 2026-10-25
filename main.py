"""
main.py — 系统终极调度中心 (重铸标准版)

核心设计原则：
1. 绝对显式装配：拒绝魔法扫描，所有组件人工注入，缺依赖立刻崩溃。
2. 严格数据流转：WS 数据源 -> KlineAuthority -> StateMachine。
3. 纯净生命周期：主线程阻塞等待信号，子线程负责数据与 API。
"""

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from decimal import Decimal
import yaml

# 底层基建
from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.data_buffer import RingBuffer
from system.order_idempotent import OrderIdGenerator
from system.rate_limiter import RateLimiter
from system.kline_authority import KlineAuthority

# 数据流
from data.rest_client import RestClient
from data.tick_db import TickDB
from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream

# 执行层
from execution.gateway_virtual import VirtualGateway, MarketFeed
from execution.gateway_binance import BinanceGateway
from execution.slippage_model import SlippageModel, SlippageConfig
from execution.order_router import OrderRouter

# 因子与策略
from factors.factor_registry import FactorRegistry
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor
from strategies.strategy_trend_round import TrendRoundStrategy

# 轮次与风控
from rounds.round_manager import RoundManager
from risk.policy_minimal import PolicyMinimal
from risk.emergency_stop import EmergencyStop

# 状态机
from states.state_machine import StateMachine
from states.state_idle import IdleState
from states.state_breakout import BreakoutConfirmState
from states.state_trend import TrendRideState
from states.state_top_detect import TopDetectState

# API 服务
import uvicorn
from api.server import create_app

logger = logging.getLogger("main")

class LiveSnapshotFeed(MarketFeed):
    """虚拟网关在影子模式下使用的快照源，从 KlineAuthority 读取"""
    def __init__(self, kline_authority: KlineAuthority):
        self.ka = kline_authority

    def get_snapshot_at(self, ts: int) -> dict:
        # 这里依赖 KlineAuthority 内部维护的快照缓存
        # 如果 KlineAuthority 尚未实现 get_snapshot，我们可以返回 None 让其拒单
        if hasattr(self.ka, "_snapshots") and self.ka._snapshots:
            import bisect
            # 简化逻辑：取最新的一帧
            latest = self.ka._snapshots[-1]
            return {"ts": latest.ts, "bids": latest.bids, "asks": latest.asks}
        return None


class QuantSystem:
    def __init__(self, mode: str, symbol: str, config_dir: str):
        self.mode = mode
        self.symbol = symbol.upper()
        self.config_dir = Path(config_dir)
        self.config = {}
        self.instances = {}
        
        self.shutdown_event = threading.Event()
        self.api_server = None
        self._threads = []

    def load_config(self):
        """加载并校验配置"""
        base_file = self.config_dir / "base.yaml"
        step_file = self.config_dir / "step1.yaml"
        sym_file = self.config_dir / "symbols" / f"{self.symbol.lower()}.yaml"

        for p in [base_file, step_file, sym_file]:
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    self.config.update(yaml.safe_load(f) or {})

        # 强制覆盖配置
        self.config.update({
            "symbol": self.symbol,
            "mode": self.mode,
            "log_dir": f"logs/{self.mode}",
            "db_path": f"data/{self.mode}_quant.db",
        })

    def bootstrap(self):
        """显式装配所有核心组件"""
        # 1. 日志与基础
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        setup_logging(log_dir=self.config["log_dir"], level="INFO", mode=self.mode, symbol=self.symbol)
        logger.info(f"=== 系统启动 {self.mode.upper()} 模式 | {self.symbol} ===")

        clock = HybridClock(config=self.config)
        self.instances["clock"] = clock

        persister = StatePersister(self.config["db_path"])
        persister.init_schema()
        self.instances["persister"] = persister

        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer

        # 2. 网络与核心数据源
        rest_client = RestClient(
            api_key=self.config.get("binance_api_key", ""),
            api_secret=self.config.get("binance_api_secret", ""),
            clock=clock,
            limiter=RateLimiter(self.config),
            config=self.config
        )
        self.instances["rest_client"] = rest_client

        # 同步时钟
        try:
            rest_client.get_server_time()
            # clock.sync(...)
        except Exception as e:
            logger.warning(f"时钟同步失败: {e}")

        kline_authority = KlineAuthority(config=self.config)
        self.instances["kline_authority"] = kline_authority

        # 3. 风控 (严格注入)
        risk_policy = PolicyMinimal(
            config=self.config, 
            initial_equity=Decimal(str(self.config.get("initial_equity", 10000)))
        )
        self.instances["risk_policy"] = risk_policy

        # 4. 执行网关
        slippage_model = SlippageModel(SlippageConfig(), rng=__import__("numpy").random.default_rng(42))
        id_gen = OrderIdGenerator({"order_id": {"prefix": self.mode[:2]}})

        if self.mode == "live":
            gateway = BinanceGateway(rest_client=rest_client, config=self.config)
        else:
            feed = LiveSnapshotFeed(kline_authority)
            gateway = VirtualGateway(
                feed=feed, clock=clock, id_generator=id_gen,
                symbol=self.symbol, config=self.config,
                slippage_model=slippage_model
            )
        self.instances["gateway"] = gateway

        order_router = OrderRouter(
            gateway=gateway, 
            rate_limiter=rest_client.limiter,
            persister=persister,
            config=self.config
        )
        self.instances["order_router"] = order_router

        # 5. 因子与策略
        registry = FactorRegistry(self.config)
        registry.register("momentum", MomentumFactor(self.config), weight=0.35)
        registry.register("obi", OBIFactor(self.config), weight=0.40)
        registry.register("volume", VolumeFactor(self.config), weight=0.25)
        self.instances["factor_registry"] = registry

        strategy = TrendRoundStrategy(self.config)
        self.instances["strategy"] = strategy

        # 6. 轮次与状态机
        round_manager = RoundManager(
            risk_policy=risk_policy, persister=persister, clock=clock,
            order_gateway=gateway, id_generator=id_gen,
            symbol=self.symbol, config=self.config,
            on_round_close=risk_policy.on_round_closed
        )
        self.instances["round_manager"] = round_manager

        state_machine = StateMachine(
            factor_registry=registry,
            strategies={"trend_round": strategy},
            config=self.config,
            persister=persister,
            risk_policy=risk_policy,
            order_gateway=gateway,
            round_manager=round_manager,
            clock=clock,
            symbol=self.symbol,
            kline_buffer=buffer
        )
        state_machine.register_state("idle", IdleState(self.config))
        state_machine.register_state("breakout_confirm", BreakoutConfirmState(self.config))
        state_machine.register_state("trend_ride", TrendRideState(self.config))
        state_machine.register_state("top_detect", TopDetectState(self.config))
        state_machine.validate()
        self.instances["state_machine"] = state_machine

        # 7. 紧急停止
        emergency_stop = EmergencyStop(
            gateway=gateway, round_manager=round_manager, state_machine=state_machine,
            clock=clock, config=self.config
        )
        self.instances["emergency_stop"] = emergency_stop

        logger.info("✅ 所有核心组件显式装配完毕！")

    def start_data_streams(self):
        """启动底层数据流，并将数据接入 KlineAuthority"""
        ka = self.instances["kline_authority"]
        sm = self.instances["state_machine"]
        persister = self.instances["persister"]

        # 1. 注册 K 线收线回调 -> 驱动状态机 + 广播前端
        def on_bar_close(bar, snapshot, trades):
            # 状态机决策
            sm.on_bar_close(bar, snapshot)
            
            # WebSocket 广播
            ws_mgr = self.instances.get("ws_manager")
            if ws_mgr:
                ws_mgr.broadcast_threadsafe("kline", {
                    "ts": bar.ts, "open": str(bar.open), "high": str(bar.high),
                    "low": str(bar.low), "close": str(bar.close), "volume": str(bar.volume),
                    "is_closed": True
                })
            
            # 状态广播（为方便前端，每根 K 线同步一次状态）
            if ws_mgr:
                ws_mgr.broadcast_threadsafe("system_status", {
                    "state": sm.current_state,
                    "risk": self.instances["risk_policy"].get_risk_level().value,
                    "active_round": self.instances["round_manager"].get_active_round()
                })

        ka.register_on_close(on_bar_close)

        # 2. 实例化数据流
        kline_stream = KlineStream(self.symbol, ka.on_exchange_kline, self.config)
        depth_stream = DepthStream(self.symbol, ka.on_depth, self.config)
        trade_stream = AggTradeStream(self.symbol, ka.on_agg_trade, self.config)

        # 3. 启动线程
        for stream in (kline_stream, depth_stream, trade_stream):
            t = threading.Thread(target=stream.start, daemon=True)
            t.start()
            self._threads.append(t)

        logger.info("🚀 三条 WebSocket 实时数据流已启动！")

    def start_api(self):
        """启动 FastAPI"""
        app = create_app(self)
        port = self.config.get("api_port", 8000)
        
        def run():
            self.api_server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning"))
            self.api_server.run()
            
        t = threading.Thread(target=run, daemon=True)
        t.start()
        self._threads.append(t)
        logger.info(f"API 服务已启动: http://0.0.0.0:{port}")

    def stop(self):
        """优雅停机"""
        logger.warning("收到停机信号，准备关闭系统...")
        self.shutdown_event.set()
        if self.api_server:
            self.api_server.should_exit = True
        # 由于 ws 线程是 daemon，主线程退出即可销毁
        logger.info("系统已安全停止。")
        sys.exit(0)

    def run(self, with_api: bool):
        self.bootstrap()
        if with_api:
            self.start_api()
            time.sleep(1) # 等 API 启动
        self.start_data_streams()

        logger.info("主循环运行中，按 Ctrl+C 退出...")
        while not self.shutdown_event.is_set():
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["shadow", "live"], default="shadow")
    parser.add_argument("--symbol", default="btcusdt")
    parser.add_argument("--config", default="config")
    parser.add_argument("--with-api", action="store_true")
    args = parser.parse_args()

    system = QuantSystem(args.mode, args.symbol, args.config)
    system.load_config()

    signal.signal(signal.SIGINT, lambda sig, frame: system.stop())
    signal.signal(signal.SIGTERM, lambda sig, frame: system.stop())

    try:
        system.run(with_api=args.with_api)
    except Exception as e:
        logging.critical(f"系统致命崩溃: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
