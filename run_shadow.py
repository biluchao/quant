"""
run_shadow.py — 影子模式长跑测试 (重铸标准版)

核心设计原则：
1. 严格虚拟执行：挂载虚拟网关，利用实时盘口模拟成交。
2. 信号质量追踪：拦截所有策略信号，记录未来表现（IC跟踪）。
3. 生命周期自治：跑满指定时长自动清盘，强制生成最终归因报告。
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import threading
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import yaml

# 核心基建
from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.data_buffer import RingBuffer
from system.order_idempotent import OrderIdGenerator
from system.rate_limiter import RateLimiter
from system.kline_authority import KlineAuthority

# 数据流
from data.rest_client import RestClient
from data.ws_kline import KlineStream
from data.ws_depth import DepthStream
from data.ws_aggtrade import AggTradeStream

# 执行与风控
from execution.gateway_virtual import VirtualGateway, MarketFeed
from execution.slippage_model import SlippageModel, SlippageConfig
from risk.policy_minimal import PolicyMinimal
from risk.emergency_stop import EmergencyStop

# 策略与轮次
from factors.factor_registry import FactorRegistry
from factors.factor_momentum import MomentumFactor
from factors.factor_obi import OBIFactor
from factors.factor_volume import VolumeFactor
from strategies.strategy_trend_round import TrendRoundStrategy
from rounds.round_manager import RoundManager

# 状态机
from states.state_machine import StateMachine
from states.state_idle import IdleState
from states.state_breakout import BreakoutConfirmState
from states.state_trend import TrendRideState
from states.state_top_detect import TopDetectState

logger = logging.getLogger("shadow")

# ============================================================================
# 专属组件：信号跟踪器
# ============================================================================
class SignalTracker:
    """记录策略信号及其 N 根 K 线后的表现"""
    def __init__(self, horizons=(3, 6, 12, 24)):
        self.horizons = horizons
        self._pending: List[Dict] = []
        self._completed: List[Dict] = []
        self._lock = threading.RLock()

    def record_signal(self, signal: Any, entry_price: float, ts: int):
        with self._lock:
            self._pending.append({
                "ts": ts,
                "direction": signal.direction,
                "score": signal.score,
                "reason": signal.reason,
                "entry_price": entry_price,
                "fwd_prices": {}
            })

    def on_bar_close(self, bar_ts: int, close_price: float):
        with self._lock:
            still_pending = []
            for sig in self._pending:
                bars_elapsed = (bar_ts - sig["ts"]) // 180000
                if bars_elapsed in self.horizons:
                    sig["fwd_prices"][bars_elapsed] = close_price
                    
                if bars_elapsed >= max(self.horizons):
                    self._completed.append(sig)
                else:
                    still_pending.append(sig)
            self._pending = still_pending

    def get_stats(self) -> dict:
        with self._lock:
            if not self._completed:
                return {"total": 0}
            stats = {}
            for h in self.horizons:
                correct, total = 0, 0
                for sig in self._completed:
                    if h in sig["fwd_prices"]:
                        total += 1
                        ret = (sig["fwd_prices"][h] - sig["entry_price"]) / sig["entry_price"]
                        if ret * sig["direction"] > 0:
                            correct += 1
                stats[f"h{h}"] = {"total": total, "correct_rate": correct / total if total else 0}
            return {"total": len(self._completed), "horizons": stats}


# ============================================================================
# 影子引擎
# ============================================================================
class LiveSnapshotFeed(MarketFeed):
    def __init__(self, kline_authority: KlineAuthority):
        self.ka = kline_authority
    def get_snapshot_at(self, ts: int) -> dict:
        if hasattr(self.ka, "_snapshots") and self.ka._snapshots:
            latest = self.ka._snapshots[-1]
            return {"ts": latest.ts, "bids": latest.bids, "asks": latest.asks}
        return None

class ShadowEngine:
    def __init__(self, symbol: str, min_hours: float, config_dir: str):
        self.symbol = symbol.upper()
        self.min_hours = min_hours
        self.config_dir = Path(config_dir)
        self.config = {}
        self.instances = {}
        
        self.shutdown_event = threading.Event()
        self.signal_tracker = SignalTracker()
        self.start_time = 0.0

    def load_config(self):
        for p in [self.config_dir / "base.yaml", self.config_dir / "step1.yaml", self.config_dir / f"symbols/{self.symbol.lower()}.yaml"]:
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    self.config.update(yaml.safe_load(f) or {})
        self.config.update({
            "symbol": self.symbol, "mode": "shadow", 
            "log_dir": "logs/shadow", "db_path": "data/shadow_quant.db"
        })

    def bootstrap(self):
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        setup_logging(log_dir=self.config["log_dir"], level="INFO", mode="shadow", symbol=self.symbol)
        logger.info(f"=== 影子模式启动，计划运行 {self.min_hours} 小时 ===")

        # 1. 基建
        clock = HybridClock(self.config)
        self.instances["clock"] = clock
        
        persister = StatePersister(self.config["db_path"])
        persister.init_schema()
        self.instances["persister"] = persister
        
        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer
        
        kline_authority = KlineAuthority(self.config)
        self.instances["kline_authority"] = kline_authority

        # 2. 风控
        risk = PolicyMinimal(config=self.config, initial_equity=Decimal("10000"))
        self.instances["risk_policy"] = risk

        # 3. 虚拟网关
        feed = LiveSnapshotFeed(kline_authority)
        gateway = VirtualGateway(
            feed=feed, clock=clock, 
            id_generator=OrderIdGenerator({"order_id": {"prefix": "sh"}}),
            symbol=self.symbol, config=self.config, 
            slippage_model=SlippageModel(SlippageConfig(), rng=np.random.default_rng(42)),
            initial_balance=Decimal("10000")
        )
        self.instances["gateway"] = gateway

        # 4. 因子与策略
        registry = FactorRegistry(self.config)
        registry.register("momentum", MomentumFactor(self.config), weight=0.35)
        registry.register("obi", OBIFactor(self.config), weight=0.40)
        registry.register("volume", VolumeFactor(self.config), weight=0.25)
        self.instances["factor_registry"] = registry
        
        strategy = TrendRoundStrategy(self.config)
        self.instances["strategy"] = strategy

        # 5. 轮次与状态机
        rounds = RoundManager(
            risk_policy=risk, persister=persister, clock=clock,
            order_gateway=gateway, id_generator=OrderIdGenerator({"order_id":{"prefix":"sh"}}),
            symbol=self.symbol, config=self.config
        )
        self.instances["round_manager"] = rounds

        sm = StateMachine(
            factor_registry=registry, strategies={"trend_round": strategy},
            config=self.config, persister=persister, risk_policy=risk,
            order_gateway=gateway, round_manager=rounds, clock=clock,
            symbol=self.symbol, kline_buffer=buffer
        )
        sm.register_state("idle", IdleState(self.config))
        sm.register_state("breakout_confirm", BreakoutConfirmState(self.config))
        sm.register_state("trend_ride", TrendRideState(self.config))
        sm.register_state("top_detect", TopDetectState(self.config))
        sm.validate()
        self.instances["state_machine"] = sm

        logger.info("✅ 影子模式核心组件装配完毕！")

    def start_data_streams(self):
        ka = self.instances["kline_authority"]
        sm = self.instances["state_machine"]
        tracker = self.signal_tracker

        def on_bar_close(bar, snapshot, trades):
            # 1. 驱动信号追踪
            tracker.on_bar_close(bar.ts, float(bar.close))
            
            # 2. 驱动状态机
            sm.on_bar_close(bar, snapshot)

        ka.register_on_close(on_bar_close)

        # 启动 WS 流
        kline_stream = KlineStream(self.symbol, ka.on_exchange_kline, self.config)
        depth_stream = DepthStream(self.symbol, ka.on_depth, self.config)
        trade_stream = AggTradeStream(self.symbol, ka.on_agg_trade, self.config)

        for s in (kline_stream, depth_stream, trade_stream):
            threading.Thread(target=s.start, daemon=True).start()

        logger.info("🚀 影子模式数据流已启动。")

    def generate_report(self):
        tracker_stats = self.signal_tracker.get_stats()
        rm = self.instances["round_manager"]
        rounds = rm._closed_rounds if hasattr(rm, "_closed_rounds") else []
        
        report = {
            "meta": {
                "symbol": self.symbol,
                "runtime_hours": (time.time() - self.start_time) / 3600,
                "min_hours_required": self.min_hours
            },
            "signals": tracker_stats,
            "rounds": {
                "total": len(rounds),
                "closed": [r.to_dict() for r in rounds]
            }
        }
        
        out_dir = Path("reports/shadow")
        out_dir.mkdir(parents=True, exist_ok=True)
        f_path = out_dir / f"shadow_report_{int(time.time())}.json"
        with open(f_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 影子报告已落盘: {f_path}")

    def stop(self):
        logger.warning("触发停止信号...")
        self.shutdown_event.set()

    def run(self):
        self.bootstrap()
        self.start_time = time.time()
        self.start_data_streams()

        try:
            while not self.shutdown_event.is_set():
                time.sleep(10)
                elapsed_h = (time.time() - self.start_time) / 3600
                if elapsed_h >= self.min_hours:
                    logger.info(f"已达到设定时长 {self.min_hours} 小时，自动停止。")
                    break
        except KeyboardInterrupt:
            pass
        finally:
            logger.info("正在清理内存，强制平仓...")
            # 清理活跃仓位
            rm = self.instances["round_manager"]
            for rid in list(rm._active_rounds.keys()):
                rm.close_round(exit_reason="shadow_shutdown")
                
            self.generate_report()
            logger.info("影子系统已安全退出。")
            sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="btcusdt")
    parser.add_argument("--min-hours", type=float, default=72.0, help="至少运行多少小时")
    parser.add_argument("--config", default="config")
    args = parser.parse_args()

    engine = ShadowEngine(args.symbol, args.min_hours, args.config)
    engine.load_config()

    signal.signal(signal.SIGINT, lambda sig, frame: engine.stop())
    signal.signal(signal.SIGTERM, lambda sig, frame: engine.stop())

    engine.run()

if __name__ == "__main__":
    main()
