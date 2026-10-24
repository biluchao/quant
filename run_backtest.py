#!/usr/bin/env python3
"""
run_backtest.py - 历史回测入口（Step 1 最小运行集）

与 main.py 的关系：
  - 共用同一套 StateMachine / FactorRegistry / RoundManager / RiskPolicy
  - 唯一区别：数据来自历史库，订单走 VirtualGateway，无 API、无 WebSocket 流

设计约束：
  - 可复现：固定随机种子，虚拟订单 ID 确定性
  - 无前视（Step 1 范围内）：快照时间戳 <= bar 收盘时间戳
  - 隔离：回测数据库与实盘数据库物理分离
  - 预热：前 WARMUP_BARS 根只推缓冲，不决策

已知限制（Step 2 修复）：
  - 成交价使用 bar 收盘价 + 滑点（实盘会在收盘后 50-200ms 成交）
  - 部分成交未模拟（Step 1 每档全额成交）
  - 资金费率未扣（Step 1 持仓周期短）

作者：张三
审查：比蒙（策略）、乐高（执行）
"""

import argparse
import csv
import json
import random
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import numpy as np
import yaml

from system.log_setup import setup_logging
from system.clock_hybrid import HybridClock
from system.persister import StatePersister
from system.data_buffer import RingBuffer

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


# ==================== 常量 ====================
WARMUP_BARS = 500            # 因子分位数窗口预热
PROGRESS_INTERVAL = 1000     # 每 1000 根 bar 打一次进度
TAKER_FEE_RATE = 0.0004      # 币安永续 taker 0.04%
EXIT_OK = 0
EXIT_CONFIG_ERROR = 2
EXIT_DATA_ERROR = 3
EXIT_RUNTIME_ERROR = 4

# 与实盘 step1.yaml 必须一致的关键参数（不一致报警）
CRITICAL_PARAM_KEYS = [
    "per_trade_risk",
    "daily_dd_limit",
    "daily_dd_halt",
    "entry_threshold",
]


class BacktestError(Exception):
    def __init__(self, msg, code=EXIT_CONFIG_ERROR):
        super().__init__(msg)
        self.code = code


class SimulatedClock:
    """
    回测时钟：now_ms() 返回当前模拟时间，advance_to(ts) 由每根 bar 推进。
    接口与 HybridClock 一致，可被状态机透传使用。
    """
    def __init__(self):
        self._now = 0

    def advance_to(self, ts):
        if ts <= self._now:
            raise BacktestError(
                f"时间倒退: current={self._now} new={ts}",
                EXIT_RUNTIME_ERROR,
            )
        self._now = ts

    def now_ms(self):
        return self._now

    def get_market_offset_ms(self):
        return 0


class HistoricalFeed:
    """
    虚拟券商的数据 feed：按 timestamp 返回历史盘口快照。
    优先使用真实历史快照；无快照时用 bar OHLCV 合成近似盘口。
    """
    def __init__(self, snapshots: dict, logger):
        self.snapshots = snapshots
        self.logger = logger
        self._synthetic_count = 0

    def get_snapshot_at(self, ts):
        snap = self.snapshots.get(ts)
        if snap is not None:
            snap = dict(snap)
            snap["ts"] = ts  # 对齐时间戳
            return snap
        # 无历史快照时不应被调用（调用方负责合成）
        return None

    def report_synthetic(self, count):
        if count > 0:
            self.logger.warning(
                f"WARN: synthetic snapshot used {count} times "
                f"(missing orderbook history)"
            )


class BacktestRunner:
    def __init__(self, args):
        self.args = args
        self.config = {}
        self.instances = {}
        self.logger = None
        self._bars = []
        self._snapshots = {}
        self._synthetic_count = 0
        self._rng = None

    # ==================== 启动阶段 ====================

    def run(self) -> dict:
        self._load_config()
        self._setup_logging()
        self._seed()
        self._init_clock()
        self._init_risk()
        self._init_data()
        self._init_execution()
        self._init_strategy()

        rounds = self._run_loop()
        report = self._generate_report(rounds)
        return report

    def _load_config(self):
        config_dir = Path(self.args.config)
        base = config_dir / "base.yaml"
        step = config_dir / "step1.yaml"
        sym = config_dir / "symbols" / f"{self.args.symbol}.yaml"
        for p in (base, step, sym):
            if not p.exists():
                raise BacktestError(f"配置文件不存在: {p}")
        merged = {}
        for p in (base, step, sym):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                    if not isinstance(data, dict):
                        raise BacktestError(f"{p} 顶层必须是字典")
                    merged.update(data)
            except yaml.YAMLError as e:
                raise BacktestError(f"YAML 解析失败 {p}: {e}")

        # 回测专用覆盖
        merged["mode"] = "backtest"
        merged["symbol"] = self.args.symbol
        merged["current_step"] = 1
        merged["log_dir"] = str(Path(merged.get("log_dir", "logs")) / "backtest")
        merged["db_path"] = self.args.backtest_db  # 回测库独立
        merged["seed"] = self.args.seed
        merged["initial_equity"] = float(self.args.initial_equity)

        required = [
            "log_level", "log_dir", "db_path", "mode", "symbol",
            "initial_equity", "seed",
            "per_trade_risk", "daily_dd_limit", "daily_dd_halt",
            "entry_threshold",
        ]
        missing = [k for k in required if k not in merged]
        if missing:
            raise BacktestError(f"配置缺失字段: {missing}")

        # 安全检查：回测库不能是实盘库
        try:
            live_db = Path(merged.get("live_db_path", "live_quant.db")).resolve()
            bt_db = Path(merged["db_path"]).resolve()
            if live_db == bt_db:
                raise BacktestError(
                    "回测数据库路径与实盘数据库相同，拒绝启动"
                )
        except OSError:
            pass

        self.config = merged

    def _setup_logging(self):
        Path(self.config["log_dir"]).mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging(
            log_dir=self.config["log_dir"],
            level=self.config["log_level"],
            mode="backtest",
            symbol=self.args.symbol,
        )
        self.logger.info("=" * 60)
        self.logger.info(
            f"回测启动 symbol={self.args.symbol} "
            f"start={self.args.start} end={self.args.end}"
        )
        self.logger.info(
            f"seed={self.config['seed']} equity={self.config['initial_equity']}"
        )
        self.logger.info("=" * 60)

    def _seed(self):
        random.seed(self.config["seed"])
        np.random.seed(self.config["seed"])
        # 滑点模型独立随机源，避免与其他随机用法耦合
        self._rng = np.random.default_rng(self.config["seed"])
        self.logger.info(f"随机种子 {self.config['seed']}（可复现）")

    def _init_clock(self):
        self.instances["clock"] = SimulatedClock()

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
        self.instances["tick_db"] = tickdb

        self.logger.info(f"加载 K 线 [{self.args.start} ~ {self.args.end}]")
        try:
            bars = tickdb.query_klines(
                symbol=self.args.symbol,
                interval="3m",
                start=self.args.start,
                end=self.args.end,
            )
        except Exception as e:
            raise BacktestError(f"K 线加载失败: {e}", EXIT_DATA_ERROR)

        if not bars:
            raise BacktestError(
                "K 线为空，检查数据库和时间范围", EXIT_DATA_ERROR
            )
        if len(bars) <= WARMUP_BARS:
            raise BacktestError(
                f"K 线数量 {len(bars)} <= 预热 {WARMUP_BARS}，无法回测",
                EXIT_DATA_ERROR,
            )

        # 时间戳必须严格递增
        for i in range(1, len(bars)):
            if bars[i].ts <= bars[i - 1].ts:
                raise BacktestError(
                    f"K 线时间戳非递增 bar#{i} "
                    f"prev={bars[i-1].ts} curr={bars[i].ts}",
                    EXIT_DATA_ERROR,
                )

        self._bars = bars
        self.logger.info(f"K 线 {len(bars)} 根")

        # 盘口快照
        try:
            snaps = tickdb.query_snapshots(
                symbol=self.args.symbol,
                start=self.args.start,
                end=self.args.end,
            )
            self._snapshots = {s["ts"]: s for s in snaps}
            self.logger.info(f"盘口快照 {len(self._snapshots)} 个")
            if not self._snapshots:
                self.logger.warning(
                    "无历史盘口，OBI 将使用合成快照，结果仅供参考"
                )
        except Exception as e:
            self.logger.warning(f"盘口加载失败，使用合成: {e}")
            self._snapshots = {}

    def _init_execution(self):
        slippage = SlippageModel(rng=self._rng)
        feed = HistoricalFeed(self._snapshots, self.logger)
        gateway = VirtualGateway(
            feed=feed,
            slippage_model=slippage,
            clock=self.instances["clock"],
            fee_rate=TAKER_FEE_RATE,
            seed=self.config["seed"],
        )
        self.instances["gateway"] = gateway
        self.instances["feed"] = feed
        self.logger.info("虚拟券商就绪")
        self.logger.warning(
            "WARN: fill at bar close + slippage，存在前视偏差 "
            "(Step 2 改为 next_bar_open)"
        )

    def _init_strategy(self):
        # 因子（Step 1 三个）
        registry = FactorRegistry()
        registry.register("momentum", MomentumFactor(self.config))
        registry.register("obi", OBIFactor(self.config))
        registry.register("volume", VolumeFactor(self.config))
        self.instances["factor_registry"] = registry

        # 持久化（回测独立库）
        persister = StatePersister(self.config["db_path"])
        persister.init_schema()
        self.instances["persister"] = persister

        # 轮次
        round_mgr = RoundManager(
            risk_policy=self.instances["risk_policy"],
            persister=persister,
        )
        self.instances["round_manager"] = round_mgr

        # 状态机（依赖完整注入）
        sm = StateMachine(
            factor_registry=registry,
            config=self.config,
            persister=persister,
            risk_policy=self.instances["risk_policy"],
            order_gateway=self.instances["gateway"],
            round_manager=round_mgr,
        )
        self.instances["state_machine"] = sm
        self.logger.info("状态机/轮次/因子 就绪")

    # ==================== 主循环 ====================

    def _run_loop(self) -> list:
        bars = self._bars
        n = len(bars)
        buffer = RingBuffer(maxlen=1000)
        self.instances["buffer"] = buffer

        self.logger.info(f"开始回测 {n} 根 bar，预热 {WARMUP_BARS} 根")
        t0 = time.time()

        for i, bar in enumerate(bars):
            buffer.push(bar)

            if i < WARMUP_BARS:
                continue

            # 推进模拟时钟
            self.instances["clock"].advance_to(bar.ts)

            # 获取快照：优先历史，否则合成
            snap = self._make_snapshot(bar)

            try:
                self.instances["state_machine"].on_bar_close(bar, snap)
            except BacktestError:
                raise
            except Exception:
                self.logger.error(
                    f"状态机异常 bar#{i} ts={bar.ts}\n"
                    f"{traceback.format_exc()}"
                )
                raise BacktestError(
                    f"状态机异常 bar#{i}", EXIT_RUNTIME_ERROR
                )

            if i % PROGRESS_INTERVAL == 0:
                elapsed = time.time() - t0
                done = i - WARMUP_BARS + 1
                speed = done / max(elapsed, 1e-6)
                eta = (n - i) / max(speed, 1e-6)
                self.logger.info(
                    f"进度 {i}/{n} ({100*i/n:.1f}%) "
                    f"elapsed={elapsed:.1f}s eta={eta:.1f}s"
                )

        # 强制平仓
        self._force_close_all(bars[-1])

        # 合成快照使用情况
        if self._synthetic_count > 0:
            self.logger.warning(
                f"WARN: 合成快照使用 {self._synthetic_count} 次"
            )

        self.logger.info(f"回测结束 耗时 {time.time() - t0:.1f}s")
        return list(self.instances["round_manager"].closed_rounds)

    def _make_snapshot(self, bar) -> dict:
        snap = self.instances["feed"].get_snapshot_at(bar.ts)
        if snap is not None:
            return snap
        # 合成：用 bar OHLCV 近似盘口
        self._synthetic_count += 1
        spread = max((bar.high - bar.low) * 0.05, 0.5)
        mid = bar.close
        bids = [
            [mid - spread - i * 0.5, bar.volume * 0.05 / (i + 1)]
            for i in range(10)
        ]
        asks = [
            [mid + spread + i * 0.5, bar.volume * 0.05 / (i + 1)]
            for i in range(10)
        ]
        return {"ts": bar.ts, "bids": bids, "asks": asks, "_synthetic": True}

    def _force_close_all(self, last_bar):
        rm = self.instances["round_manager"]
        for rid, r in list(rm.active_rounds.items()):
            self.logger.warning(f"回测结束强制平仓 round={rid}")
            record = r.close_all(
                last_bar.close, last_bar.ts, reason="end_of_backtest"
            )
            rm.closed_rounds.append(record)
            del rm.active_rounds[rid]

    # ==================== 报告 ====================

    def _generate_report(self, rounds: list) -> dict:
        self.logger.info(f"生成归因报告 rounds={len(rounds)}")

        stats = self._compute_stats(rounds)
        report = {
            "meta": {
                "symbol": self.args.symbol,
                "start": self.args.start,
                "end": self.args.end,
                "seed": self.config["seed"],
                "initial_equity": self.config["initial_equity"],
                "bars": len(self._bars),
                "warmup_bars": WARMUP_BARS,
                "synthetic_snapshots": self._synthetic_count,
                "generated_at": datetime.now().isoformat(),
                "warnings": [
                    "fill at bar close + slippage (forward bias)",
                    "partial fill not simulated",
                    "funding rate skipped",
                ],
            },
            "summary": stats,
            "rounds": rounds,
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(self.args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        json_path = out_dir / f"backtest_report_{ts}.json"
        csv_path = out_dir / f"backtest_trades_{ts}.csv"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        if rounds:
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rounds[0].keys()))
                writer.writeheader()
                writer.writerows(rounds)

        self.logger.info(f"报告: {json_path}")
        if rounds:
            self.logger.info(f"交易明细: {csv_path}")

        self._print_summary(stats)
        return report

    def _compute_stats(self, rounds: list) -> dict:
        if not rounds:
            return {"count": 0, "note": "no rounds executed"}

        r_multiples = np.array([r["r_multiple"] for r in rounds])
        pnls = np.array([r["pnl"] for r in rounds])
        wins = pnls > 0

        # 权益曲线
        equities = np.empty(len(rounds) + 1)
        equities[0] = float(self.config["initial_equity"])
        equities[1:] = equities[0] + np.cumsum(pnls)

        # 最大回撤
        peak = np.maximum.accumulate(equities)
        dd = (equities - peak) / np.maximum(peak, 1e-9)
        max_dd = float(dd.min())

        # 夏普（按轮次近似）
        rets = np.diff(equities) / np.maximum(equities[:-1], 1e-9)
        if len(rets) > 1 and rets.std() > 1e-9:
            sharpe = float(rets.mean() / rets.std() * np.sqrt(len(rets)))
        else:
            sharpe = 0.0

        # 按出场原因分组
        by_reason = {}
        for r in rounds:
            reason = r.get("exit_reason", "unknown")
            by_reason.setdefault(reason, []).append(r)

        reason_stats = {}
        for reason, rs in by_reason.items():
            reason_stats[reason] = {
                "count": len(rs),
                "avg_r": float(np.mean([x["r_multiple"] for x in rs])),
                "total_pnl": float(sum(x["pnl"] for x in rs)),
                "win_rate": float(
                    sum(1 for x in rs if x["pnl"] > 0) / len(rs)
                ),
            }

        return {
            "count": len(rounds),
            "win_rate": float(wins.mean()),
            "avg_r": float(r_multiples.mean()),
            "median_r": float(np.median(r_multiples)),
            "total_pnl": float(pnls.sum()),
            "final_equity": float(equities[-1]),
            "return_pct": float((equities[-1] / equities[0] - 1) * 100),
            "max_drawdown_pct": float(max_dd * 100),
            "sharpe_approx": sharpe,
            "profit_factor": float(
                pnls[wins].sum() / abs(pnls[~wins].sum())
                if (~wins).any() and pnls[~wins].sum() != 0
                else float("inf")
            ),
            "by_exit_reason": reason_stats,
        }

    def _print_summary(self, s: dict):
        print("\n" + "=" * 60)
        print(f"回测摘要  {self.args.symbol}  {self.args.start} ~ {self.args.end}")
        print("=" * 60)
        if s.get("count", 0) == 0:
            print("无轮次成交")
            return
        print(f"轮次数量:     {s['count']}")
        print(f"胜率:         {s['win_rate']*100:.1f}%")
        print(f"平均 R:       {s['avg_r']:.3f}")
        print(f"中位数 R:     {s['median_r']:.3f}")
        print(f"总盈亏:       {s['total_pnl']:.2f} USDT")
        print(f"期末权益:     {s['final_equity']:.2f} USDT")
        print(f"收益率:       {s['return_pct']:.2f}%")
        print(f"最大回撤:     {s['max_drawdown_pct']:.2f}%")
        print(f"夏普(近似):   {s['sharpe_approx']:.2f}")
        print(f"盈亏比:       {s['profit_factor']:.2f}")
        print("\n按出场原因:")
        for reason, rs in s["by_exit_reason"].items():
            print(
                f"  {reason:20s} count={rs['count']:4d} "
                f"avg_r={rs['avg_r']:+.2f} "
                f"pnl={rs['total_pnl']:+.2f} "
                f"win={rs['win_rate']*100:.1f}%"
            )
        print("=" * 60 + "\n")


# ==================== 入口 ====================

def parse_args():
    p = argparse.ArgumentParser(
        description="Quant System Step1 Backtest"
    )
    p.add_argument("--config", default="config")
    p.add_argument("--symbol", default="btcusdt")
    p.add_argument(
        "--start", required=True,
        help="ISO datetime, e.g. 2024-01-01 或 2024-01-01T00:00:00",
    )
    p.add_argument("--end", required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--initial_equity", type=float, default=10000.0)
    p.add_argument(
        "--backtest_db", default="backtest_quant.db",
        help="回测专用数据库，必须与实盘物理分离",
    )
    p.add_argument("--output_dir", default="reports/backtest")
    return p.parse_args()


def main():
    args = parse_args()

    # 路径安全检查（早于任何 import 之外的副作用）
    bt_db_name = Path(args.backtest_db).name.lower()
    if "live" in bt_db_name:
        print(
            "[FATAL] 回测数据库名称含 'live'，拒绝启动，避免污染实盘",
            file=sys.stderr,
        )
        sys.exit(EXIT_CONFIG_ERROR)

    try:
        runner = BacktestRunner(args)
        runner.run()
        sys.exit(EXIT_OK)
    except BacktestError as e:
        print(f"[BACKTEST ERROR] {e}", file=sys.stderr)
        sys.exit(e.code)
    except KeyboardInterrupt:
        print("[BACKTEST] 用户中断", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"[FATAL] {e}\n{traceback.format_exc()}", file=sys.stderr)
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == "__main__":
    main()
