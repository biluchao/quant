"""
main.py — Quant System 主调度中心（模块化重铸版）

【设计初衷】
1. 绝不揉搓业务逻辑：本文件不负责解析 K 线，不负责计算因子，更不负责拼接 WebSocket 数据。
2. 回归自动装配：废弃手写硬编码组装，全面启用 ModuleRegistry，让所有符合 MANIFEST 规范的模块自动就位。
3. 确立数据总线：以 KlineAuthority（K线权威源）作为系统的唯一数据心脏，由它汇聚底层 WS 数据，统一下发给状态机和前端。

【数据流向】
币安 WebSocket → data/ws_*.py → KlineAuthority → 因子/策略计算 → 状态机迁移
                                     ↓
                              (收线回调 on_close)
                                     ↓
                              WebSocket 广播 → 前端面板
"""

import argparse
import logging
import sys
import time
import threading
import asyncio
import signal
from pathlib import Path

import yaml
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

# 1. 核心抽象层 (契约)
from core.manifest import ModuleRegistry

# 2. 系统基建层
from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.kline_authority import KlineAuthority
from system.data_buffer import RingBuffer
from system.param_store import ParamStore

# 3. 数据接入层 (底层的网络通信)
from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream

# 4. 策略核心与轮次管理
from states.state_machine import StateMachine
from rounds.round_manager import RoundManager

logger = logging.getLogger("main")


class QuantSystem:
    def __init__(self, mode: str, symbol: str, config_dir: str):
        self.mode = mode
        self.symbol = symbol.upper()
        self.config_dir = Path(config_dir)
        self.config = {}
        
        # 核心实例仓库（由 ModuleRegistry 自动填充）
        self.instances = {}
        
        # 后台服务与生命周期控制
        self.services = {"streams": []}
        self.api_server = None
        self.api_loop = None
        self.active_ws_clients = set()
        self.shutdown_event = threading.Event()

    def load_config(self):
        """分层加载配置：base -> step1 -> symbol，后者覆盖前者"""
        base_file = self.config_dir / "base.yaml"
        step_file = self.config_dir / "step1.yaml"
        sym_file = self.config_dir / "symbols" / f"{self.symbol.lower()}.yaml"

        for p in [base_file, step_file, sym_file]:
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    self.config.update(yaml.safe_load(f) or {})

        # 强制覆盖运行时必须的参数，防止配置文件漏写
        self.config.update({
            "symbol": self.symbol,
            "mode": self.mode,
            "log_dir": "logs/runtime",
            "db_path": "data/runtime_quant.db",
        })

    def bootstrap(self):
        """
        核心装配区（回归初衷）：
        1. 初始化基础设施
        2. 使用 ModuleRegistry 扫描并装配因子、策略、网关
        3. 将核心数据源 (KlineAuthority) 和轮次、状态机连接起来
        """
        # ================= 1. 基础系统层 =================
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        setup_logging(log_dir=self.config["log_dir"], level="INFO", mode=self.mode, symbol=self.symbol)
        logger.info(f"=== 系统启动 mode={self.mode} symbol={self.symbol} ===")
        
        self.instances["config"] = self.config
        self.instances["clock"] = HybridClock(config=self.config)
        self.instances["persister"] = StatePersister(self.config["db_path"])
        self.instances["persister"].init_schema()
        self.instances["buffer"] = RingBuffer(maxlen=1000)
        self.instances["param_store"] = ParamStore(self.config)

        # ================= 2. 数据权威源 =================
        # 所有的行情数据将汇入这里，它负责统一时间戳、缓存盘口和K线。
        self.instances["kline_authority"] = KlineAuthority(self.config)

        # ================= 3. 模块自动装配 =================
        # 扫描当前目录下的所有模块，读取其 MANIFEST，自动实例化
        logger.info("开始扫描并自动组装模块...")
        registry = ModuleRegistry(root_dir=".", current_step=1)
        registry.set_config(self.config)
        assembled = registry.assemble(self.config)
        self.instances.update(assembled.instances)

        # ================= 4. 组装轮次管理器 =================
        # 依赖 registry 中装配好的 risk_policy 和 gateway
        self.instances["round_manager"] = RoundManager(
            risk_policy=self.instances.get("risk_policy"),
            persister=self.instances["persister"],
            clock=self.instances["clock"],
            order_gateway=self.instances.get("gateway"),
            id_generator=self.instances.get("order_idempotent"),
            symbol=self.symbol,
            config=self.config
        )

        # ================= 5. 组装状态机 =================
        # 提取策略，并把所有的上下文依赖注入状态机
        strategies = {
            name: inst for name, inst in self.instances.items() 
            if getattr(inst, "name", "") == "trend_round" or name == "trend_round"
        }
        
        sm = StateMachine(
            factor_registry=self.instances.get("factor_registry"),
            strategies=strategies,
            config=self.config,
            persister=self.instances["persister"],
            risk_policy=self.instances.get("risk_policy"),
            order_gateway=self.instances.get("gateway"),
            round_manager=self.instances["round_manager"],
            clock=self.instances["clock"],
            symbol=self.symbol,
            kline_buffer=self.instances["buffer"]
        )
        
        # 显式注册四大状态（如果未来状态机也支持自动扫描，这部分可省略）
        from states.state_idle import IdleState
        from states.state_breakout import BreakoutConfirmState
        from states.state_trend import TrendRideState
        from states.state_top_detect import TopDetectState
        
        sm.register_state("idle", IdleState(self.config))
        sm.register_state("breakout_confirm", BreakoutConfirmState(self.config))
        sm.register_state("trend_ride", TrendRideState(self.config))
        sm.register_state("top_detect", TopDetectState(self.config))
        sm.validate()
        
        self.instances["state_machine"] = sm
        logger.info("✅ 所有组件装配完毕！")

    def start_data_streams(self):
        """
        启动底层 WebSocket 数据流，并将其接入 KlineAuthority。
        """
        ka = self.instances["kline_authority"]
        sm = self.instances["state_machine"]

        # 1. 挂载 K 线收线回调（这是系统的心跳）
        # 当底层 WS 拼凑出一根完整的 3 分钟 K 线时，触发此回调
        def on_bar_close(bar, snapshot, trades):
            # 驱动状态机（策略决策在这里发生）
            sm.on_bar_close(bar, snapshot)
            
            # 打包推送到前端展示（只推必要的精简数据）
            self.broadcast_to_ws("kline", {
                "ts": bar.ts, 
                "open": float(bar.open), 
                "high": float(bar.high),
                "low": float(bar.low), 
                "close": float(bar.close), 
                "volume": float(bar.volume),
                "is_closed": True
            })
            
        ka.register_on_close(on_bar_close)

        # 2. 实例化底层的 WebSocket 流，数据一律喂给 KlineAuthority
        kline_ws = KlineStream(
            symbol=self.symbol.lower(),
            on_bar=lambda msg: ka.on_exchange_kline(msg),
            config=self.config
        )
        depth_ws = DepthStream(
            symbol=self.symbol.lower(),
            on_snapshot=lambda snap: ka.on_depth(snap),
            config=self.config
        )
        trade_ws = AggTradeStream(
            symbol=self.symbol.lower(),
            on_trade=lambda trade: ka.on_agg_trade(trade),
            config=self.config
        )
        
        self.services["streams"] = [kline_ws, depth_ws, trade_ws]
        
        # 3. 启动后台线程接收数据
        for s in self.services["streams"]:
            threading.Thread(target=s.run, daemon=True).start()
        logger.info("🚀 底层数据流启动成功！正在接收币安行情...")

    def start_api(self):
        """启动 FastAPI 后端，处理前端请求与 WebSocket 连接"""
        app = FastAPI()

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.active_ws_clients.add(websocket)
            
            # 连接建立后，立刻发一次历史 K 线，让前端图表瞬间填满
            recent = self.instances["kline_authority"].get_recent_bars(100)
            if recent:
                bars_data = [
                    {"ts": b.ts, "open": float(b.open), "high": float(b.high), 
                     "low": float(b.low), "close": float(b.close), "volume": float(b.volume)} 
                    for b in recent
                ]
                await websocket.send_json({"type": "history", "ts": int(time.time()*1000), "data": bars_data})
                
            try:
                while True:
                    await websocket.receive_text()
            except WebSocketDisconnect:
                self.active_ws_clients.remove(websocket)

        # 挂载前端静态文件
        if Path("frontend").exists():
            app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

        def run_server():
            self.api_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.api_loop)
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

        threading.Thread(target=run_server, daemon=True).start()
        logger.info("API 与前端服务已启动: http://0.0.0.0:8000")

    def start_status_broadcaster(self):
        """每秒向前端广播一次系统的风控状态和活跃持仓"""
        def run_broadcast():
            while not self.shutdown_event.is_set():
                try:
                    sm = self.instances.get("state_machine")
                    rp = self.instances.get("risk_policy")
                    rm = self.instances.get("round_manager")
                    
                    lvl = rp.get_risk_level() if rp else "normal"
                    active_round = rm.get_active_round() if rm else None

                    self.broadcast_to_ws("system_status", {
                        "state": getattr(sm, "current_state", "idle") if sm else "idle",
                        "risk": lvl.value if hasattr(lvl, "value") else str(lvl),
                        "active_round": active_round
                    })
                except Exception:
                    pass
                time.sleep(1.0)
        threading.Thread(target=run_broadcast, daemon=True).start()

    def broadcast_to_ws(self, msg_type: str, data: dict):
        """跨线程安全：同步线程 -> 异步 FastAPI 的 WebSocket 广播"""
        if not self.active_ws_clients or self.api_loop is None:
            return
        msg = {"type": msg_type, "ts": int(time.time() * 1000), "data": data}
        asyncio.run_coroutine_threadsafe(self._async_broadcast(msg), self.api_loop)

    async def _async_broadcast(self, msg: dict):
        for ws in list(self.active_ws_clients):
            try:
                await ws.send_json(msg)
            except Exception:
                self.active_ws_clients.remove(ws)

    def stop(self):
        """优雅停机处理"""
        logger.info("收到停止信号，准备优雅停机...")
        self.shutdown_event.set()
        for s in self.services.get("streams", []):
            if hasattr(s, "stop"):
                s.stop()
        logger.info("数据流已关闭，系统安全退出。")
        sys.exit(0)

    def run(self, with_api=False):
        self.bootstrap()
        
        if with_api:
            self.start_api()
            time.sleep(1.5) # 等待 API 服务就绪
            
        self.start_status_broadcaster()
        self.start_data_streams()
        
        logger.info("系统主循环运行中，按 Ctrl+C 退出...")
        while not self.shutdown_event.is_set():
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="shadow")
    parser.add_argument("--symbol", default="btcusdt")
    parser.add_argument("--config", default="config")
    parser.add_argument("--with-api", action="store_true")
    args = parser.parse_args()

    system = QuantSystem(args.mode, args.symbol, args.config)
    system.load_config()
    
    # 注册信号，保证按 Ctrl+C 时能安全停止 WS 线程
    signal.signal(signal.SIGINT, lambda sig, frame: system.stop())
    signal.signal(signal.SIGTERM, lambda sig, frame: system.stop())

    try:
        system.run(with_api=args.with_api)
    except Exception as e:
        logger.critical(f"系统崩溃: {e}", exc_info=True)

if __name__ == "__main__":
    main()
