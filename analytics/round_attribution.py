"""
analytics/round_attribution.py — 轮次归因分析（纯函数）

整个系统里唯一一个"只读不写"的文件。不参与决策，不下单，不改状态。
只从 closed_rounds 里挖数据，告诉你"这套策略到底赚的是什么钱、亏的是什么钱"。

职责（唯一）：
1. 接收 List[dict]（RoundRecord.to_dict() 的列表）
2. 计算 20+ 个归因指标
3. 按多个维度分组统计
4. 输出结构化的 AttributionReport

不做什么：
- 不调时钟（纯函数）
- 不写日志（纯计算）
- 不碰数据库（数据来源由调用方负责）
- 不做决策（只分析）
- 不做持久化（报告由调用方决定落哪里）
- 不做 AI 归因（Step 3）

职责边界：
- RoundAttribution：纯计算，输入 rounds 列表，输出报告
- RoundManager：产出 rounds 列表
- Persister：存储 rounds 列表
- AI Advisor（Step 3）：读报告，生成自然语言总结

关键契约（"五纯"）：
1. 纯函数：无副作用，同输入 → 同输出
2. 不抛异常：单条记录失败跳过，整体不崩
3. 内部用 Decimal：避免浮点累积误差
4. 输出全序列化：Decimal → str，float → float，支持 JSON
5. 分组稳定：按 key 排序，报告可复现

核心指标：
- total_rounds:       总轮次
- malformed_rounds:   解析失败的轮次数（数据问题）
- wins / losses /   盈亏轮次
- win_rate:          胜率（wins / total_rounds）
- avg_r / median_r:  R 倍数均值 / 中位数
- total_pnl:         总盈亏（USDT）
- profit_factor:     盈亏比（盈利总额 / 亏损总额）
- max_drawdown_pct:  最大回撤（基于 equity 曲线）
- sharpe_estimate:   夏普估算（简化版）
- avg_bars_held:     平均持仓 K 线数
- avg_capture_ratio: 峰值捕获率（逃顶质量，r / peak_r）
- false_signal_rate: 假信号率（3 根 K 线内止损的比例）

分组维度：
- by_exit_reason:    按出场原因（stop / top_escape / timeout / ...）
- by_direction:      按方向（long / short）
- by_pyramid_count:  按加仓次数（0 / 1 / 2 / 3）
- by_date:           按 UTC 日期（YYYY-MM-DD）
- by_hour_of_day:    按 UTC 小时（0-23）

极端轮次：
- best_rounds:       盈利 Top 5（按 r_multiple）
- worst_rounds:      亏损 Top 5（按 r_multiple）

字段缺省处理：
- r_multiple 缺失：跳过该轮，记入 malformed_rounds
- realized_pnl 缺失：同上
- closed_ts 缺失：同上
- exit_reason 缺失：计入 "unknown" 组
- direction 缺失：同上
- pyramid_count 缺失：默认 0
- peak_r 缺失：capture_ratio 为 None（不计入 avg_capture_ratio）
- bars_held 缺失：默认 0

为什么用 closed_ts 做时间过滤：
- 决定"哪天赚的钱"的是平仓时间
- 开仓时间可能是前一天，但盈亏发生在平仓时
- 日切统计必须用 closed_ts

为什么用 Decimal 内部计算：
- r_multiple 累加 float 会有误差
- 100 轮累加误差可达 1e-6 量级
- 用 Decimal 避免，输出时转 float

为什么按原值分组 exit_reason：
- "stop" 和 "stop_triggered" 虽然语义相近，但来源不同
- 强行归一化会掩盖问题（例如"两种止损的表现是否不同"）
- 让数据自己说话

为什么报告要排序：
- 同一批数据两次跑，报告的字段顺序不同 = 不可复现
- 排序保证输出稳定 = 可 diff = 可回归测试

与 AI Advisor 的关系（Step 3）：
- 本模块产出结构化报告（JSON）
- AI Advisor 读报告，用自然语言总结
- 例如："过去 30 天赚了 200 USDT，其中 60% 来自趋势段，
  30% 来自假信号止损，10% 来自逃顶后的空单（Step 3）"

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import logging
import math
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "round_attribution",
    "type": "analytics",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["round_attribution", "performance_metrics"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Pure-function round attribution: performance metrics + multi-dimensional grouping",
}


# ============================================================================
# 常量
# ============================================================================
FALSE_SIGNAL_BARS_THRESHOLD = 3      # 3 根 K 线内止损算假信号
TOP_N_EXTREMES = 5                    # 最优/最差 Top 5
DEFAULT_BARS_PER_DAY = 480            # 3m 周期每天 480 根


# ============================================================================
# 报告结构
# ============================================================================
@dataclass
class AttributionReport:
    """
    归因报告。不可变，可序列化。

    所有字段都能通过 to_dict() 转成 JSON 兼容格式。
    """
    # ---- 元数据 ----
    total_rounds: int = 0
    malformed_rounds: int = 0
    window_start_ts: Optional[int] = None
    window_end_ts: Optional[int] = None

    # ---- 核心指标 ----
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    win_rate: float = 0.0
    avg_r: float = 0.0
    median_r: float = 0.0
    total_pnl: str = "0"
    profit_factor: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_estimate: float = 0.0
    avg_bars_held: float = 0.0
    avg_capture_ratio: Optional[float] = None
    false_signal_rate: float = 0.0

    # ---- 分组 ----
    by_exit_reason: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )
    by_direction: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )
    by_pyramid_count: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )
    by_date: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )
    by_hour_of_day: Dict[str, Dict[str, Any]] = field(
        default_factory=dict
    )

    # ---- 极端轮次 ----
    best_rounds: List[Dict[str, Any]] = field(default_factory=list)
    worst_rounds: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """序列化为 JSON 兼容字典。"""
        return {
            "meta": {
                "total_rounds": self.total_rounds,
                "malformed_rounds": self.malformed_rounds,
                "window_start_ts": self.window_start_ts,
                "window_end_ts": self.window_end_ts,
            },
            "core": {
                "wins": self.wins,
                "losses": self.losses,
                "breakeven": self.breakeven,
                "win_rate": self.win_rate,
                "avg_r": self.avg_r,
                "median_r": self.median_r,
                "total_pnl": self.total_pnl,
                "profit_factor": self.profit_factor,
                "max_drawdown_pct": self.max_drawdown_pct,
                "sharpe_estimate": self.sharpe_estimate,
                "avg_bars_held": self.avg_bars_held,
                "avg_capture_ratio": self.avg_capture_ratio,
                "false_signal_rate": self.false_signal_rate,
            },
            "by_exit_reason": self.by_exit_reason,
            "by_direction": self.by_direction,
            "by_pyramid_count": self.by_pyramid_count,
            "by_date": self.by_date,
            "by_hour_of_day": self.by_hour_of_day,
            "extremes": {
                "best": self.best_rounds,
                "worst": self.worst_rounds,
            },
        }


# ============================================================================
# RoundAttribution
# ============================================================================
class RoundAttribution:
    """
    轮次归因分析（纯函数）。详见模块 docstring。

    用法：
        attribution = RoundAttribution()

        # 从 persister 拉数据
        rounds = persister.query_rounds(
            start_ts=..., end_ts=...,
        )

        # 归因
        report = attribution.analyze(
            rounds=rounds,
            start_ts=...,
            end_ts=...,
        )

        # 输出
        print(report.to_dict())

        # 或转 CSV / JSON
        with open("report.json", "w") as f:
            json.dump(report.to_dict(), f, indent=2)
    """

    name = "round_attribution"
    version = "1.0.0"

    def analyze(
        self,
        rounds: List[Dict[str, Any]],
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> AttributionReport:
        """
        分析轮次列表。

        Args:
            rounds: List[dict]，每项是 RoundRecord.to_dict() 的结果
            start_ts: 时间窗口起点（含），None 表示不限制
            end_ts: 时间窗口终点（不含），None 表示不限制

        Returns:
            AttributionReport

        契约：
        - 空输入返回空报告
        - 单条记录失败跳过，计入 malformed_rounds
        - 不抛异常
        """
        report = AttributionReport()
        report.window_start_ts = start_ts
        report.window_end_ts = end_ts

        if not rounds:
            return report

        # ---- 1. 过滤与解析 ----
        parsed: List[Dict[str, Any]] = []
        for r in rounds:
            try:
                item = self._parse_round(r, start_ts, end_ts)
                if item is not None:
                    parsed.append(item)
                else:
                    report.malformed_rounds += 1
            except Exception as e:
                logger.warning(
                    f"skip malformed round: {e}"
                )
                report.malformed_rounds += 1

        if not parsed:
            return report

        report.total_rounds = len(parsed)

        # ---- 2. 核心指标 ----
        self._compute_core_metrics(report, parsed)

        # ---- 3. 分组 ----
        report.by_exit_reason = self._group_by(
            parsed, key_fn=self._key_exit_reason
        )
        report.by_direction = self._group_by(
            parsed, key_fn=self._key_direction
        )
        report.by_pyramid_count = self._group_by(
            parsed, key_fn=self._key_pyramid_count
        )
        report.by_date = self._group_by(
            parsed, key_fn=self._key_date
        )
        report.by_hour_of_day = self._group_by(
            parsed, key_fn=self._key_hour_of_day
        )

        # ---- 4. 极端轮次 ----
        report.best_rounds = self._extract_extremes(
            parsed, n=TOP_N_EXTREMES, best=True
        )
        report.worst_rounds = self._extract_extremes(
            parsed, n=TOP_N_EXTREMES, best=False
        )

        return report

    # ========================================================================
    # 解析单条
    # ========================================================================

    def _parse_round(
        self,
        raw: Any,
        start_ts: Optional[int],
        end_ts: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        """
        解析单条轮次记录。失败返回 None。

        契约：
        - r_multiple 和 realized_pnl 必须有
        - closed_ts 必须有
        - 时间过滤用 closed_ts
        """
        if not isinstance(raw, dict):
            return None

        # 核心字段
        r_multiple = self._to_float(raw.get("r_multiple"))
        realized_pnl = self._to_decimal(raw.get("realized_pnl"))
        closed_ts = self._to_int(raw.get("closed_ts"))

        if r_multiple is None or realized_pnl is None or closed_ts is None:
            return None

        # 时间窗过滤（左闭右开）
        if start_ts is not None and closed_ts < start_ts:
            return None
        if end_ts is not None and closed_ts >= end_ts:
            return None

        # 其他字段（可缺失）
        peak_r = self._to_float(raw.get("peak_r"))
        bars_held = self._to_int(raw.get("bars_held"))
        pyramid_count = self._to_int(raw.get("pyramid_count"))
        direction = self._to_int(raw.get("direction"))
        exit_reason = str(raw.get("exit_reason", "unknown"))
        round_id = str(raw.get("round_id", ""))
        opened_ts = self._to_int(raw.get("opened_ts"))

        # 缺失时的推断
        if bars_held is None and opened_ts is not None:
            duration_ms = closed_ts - opened_ts
            if duration_ms > 0:
                # 假设 3m 周期，3600000 / 180000 = 20 根/小时
                bars_held = duration_ms // (3 * 60 * 1000)

        capture_ratio: Optional[float] = None
        if peak_r is not None and peak_r > 0:
            capture_ratio = r_multiple / peak_r

        return {
            "round_id": round_id,
            "r_multiple": r_multiple,
            "realized_pnl": realized_pnl,
            "closed_ts": closed_ts,
            "peak_r": peak_r,
            "capture_ratio": capture_ratio,
            "bars_held": bars_held or 0,
            "pyramid_count": pyramid_count or 0,
            "direction": direction,
            "exit_reason": exit_reason,
        }

    # ========================================================================
    # 核心指标
    # ========================================================================

    def _compute_core_metrics(
        self,
        report: AttributionReport,
        parsed: List[Dict[str, Any]],
    ) -> None:
        """计算核心指标。"""
        r_values = [p["r_multiple"] for p in parsed]
        pnl_values = [p["realized_pnl"] for p in parsed]
        bars_values = [
            p["bars_held"] for p in parsed if p["bars_held"] > 0
        ]
        capture_values = [
            p["capture_ratio"]
            for p in parsed
            if p["capture_ratio"] is not None
        ]

        # 胜负
        wins = [p for p in parsed if p["realized_pnl"] > 0]
        losses = [p for p in parsed if p["realized_pnl"] < 0]
        breakeven = [
            p for p in parsed if p["realized_pnl"] == 0
        ]

        report.wins = len(wins)
        report.losses = len(losses)
        report.breakeven = len(breakeven)
        report.win_rate = self._safe_div(
            len(wins), len(parsed), as_float=True
        )

        # R 倍数
        if r_values:
            report.avg_r = float(sum(r_values) / len(r_values))
            report.median_r = self._median(r_values)

        # 总盈亏
        total_pnl = sum(pnl_values, Decimal("0"))
        report.total_pnl = str(total_pnl)

        # 盈亏比
        gross_profit = sum(
            (p for p in pnl_values if p > 0), Decimal("0")
        )
        gross_loss = abs(sum(
            (p for p in pnl_values if p < 0), Decimal("0")
        ))
        if gross_loss > 0:
            report.profit_factor = float(gross_profit / gross_loss)
        elif gross_profit > 0:
            report.profit_factor = float("inf")
        else:
            report.profit_factor = 0.0

        # 持仓时长
        if bars_values:
            report.avg_bars_held = sum(bars_values) / len(bars_values)

        # 峰值捕获率
        if capture_values:
            report.avg_capture_ratio = (
                sum(capture_values) / len(capture_values)
            )

        # 假信号率：3 根 K 线内止损的比例
        false_signals = [
            p for p in parsed
            if p["exit_reason"] == "stop"
            and p["bars_held"] <= FALSE_SIGNAL_BARS_THRESHOLD
        ]
        report.false_signal_rate = self._safe_div(
            len(false_signals), len(parsed), as_float=True
        )

        # 最大回撤（基于 equity 曲线）
        report.max_drawdown_pct = self._compute_max_drawdown(
            parsed
        )

        # 夏普估算（简化：用 r_multiple 序列）
        if r_values and len(r_values) > 1:
            report.sharpe_estimate = self._compute_sharpe(r_values)

    def _compute_max_drawdown(
        self, parsed: List[Dict[str, Any]]
    ) -> float:
        """
        基于 equity 曲线计算最大回撤。

        契约：
        - 按 closed_ts 排序
        - 从初始权益 0 开始累加 realized_pnl
        - 计算峰值回撤
        """
        sorted_rounds = sorted(parsed, key=lambda p: p["closed_ts"])
        equity = Decimal("0")
        peak = Decimal("0")
        max_dd = Decimal("0")

        for p in sorted_rounds:
            equity += p["realized_pnl"]
            if equity > peak:
                peak = equity
            if peak > 0:
                dd = (peak - equity) / peak
                if dd > max_dd:
                    max_dd = dd

        return float(max_dd) * 100  # 百分比

    def _compute_sharpe(self, r_values: List[float]) -> float:
        """
        简化的夏普估算。

        公式：mean(r) / std(r) * sqrt(n)

        契约：
        - 不做年化（Step 1 简化）
        - std 接近 0 时返回 0
        """
        n = len(r_values)
        if n < 2:
            return 0.0
        mean = sum(r_values) / n
        variance = sum((x - mean) ** 2 for x in r_values) / (n - 1)
        std = math.sqrt(variance)
        if std < 1e-9:
            return 0.0
        return mean / std * math.sqrt(n)

    # ========================================================================
    # 分组
    # ========================================================================

    def _group_by(
        self,
        parsed: List[Dict[str, Any]],
        key_fn: Any,
    ) -> Dict[str, Dict[str, Any]]:
        """
        按 key 函数分组。

        契约：
        - 空组不出现
        - 组按 key 排序
        - 每组包含 count / wins / losses / win_rate / avg_r / total_pnl
        """
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for p in parsed:
            try:
                key = key_fn(p)
            except Exception:
                key = "unknown"
            groups.setdefault(key, []).append(p)

        result: Dict[str, Dict[str, Any]] = {}
        for key in sorted(groups.keys()):
            items = groups[key]
            wins = [i for i in items if i["realized_pnl"] > 0]
            losses = [i for i in items if i["realized_pnl"] < 0]
            r_vals = [i["r_multiple"] for i in items]
            total_pnl = sum(
                (i["realized_pnl"] for i in items), Decimal("0")
            )
            result[key] = {
                "count": len(items),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": self._safe_div(
                    len(wins), len(items), as_float=True
                ),
                "avg_r": (
                    float(sum(r_vals) / len(r_vals))
                    if r_vals else 0.0
                ),
                "total_pnl": str(total_pnl),
            }
        return result

    @staticmethod
    def _key_exit_reason(p: Dict[str, Any]) -> str:
        return p.get("exit_reason", "unknown")

    @staticmethod
    def _key_direction(p: Dict[str, Any]) -> str:
        d = p.get("direction")
        if d == 1:
            return "long"
        elif d == -1:
            return "short"
        return "unknown"

    @staticmethod
    def _key_pyramid_count(p: Dict[str, Any]) -> str:
        return f"pyramid_{p.get('pyramid_count', 0)}"

    @staticmethod
    def _key_date(p: Dict[str, Any]) -> str:
        ts = p.get("closed_ts")
        if ts is None:
            return "unknown"
        try:
            dt = datetime.fromtimestamp(
                ts / 1000.0, tz=timezone.utc
            )
            return dt.strftime("%Y-%m-%d")
        except (ValueError, OSError):
            return "unknown"

    @staticmethod
    def _key_hour_of_day(p: Dict[str, Any]) -> str:
        ts = p.get("closed_ts")
        if ts is None:
            return "unknown"
        try:
            dt = datetime.fromtimestamp(
                ts / 1000.0, tz=timezone.utc
            )
            return f"{dt.hour:02d}"
        except (ValueError, OSError):
            return "unknown"

    # ========================================================================
    # 极端轮次
    # ========================================================================

    def _extract_extremes(
        self,
        parsed: List[Dict[str, Any]],
        n: int = TOP_N_EXTREMES,
        best: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        提取最优/最差 N 轮。

        契约：
        - 按 r_multiple 排序
        - best=True 取最大
        - best=False 取最小
        """
        if not parsed:
            return []

        sorted_list = sorted(
            parsed,
            key=lambda p: p["r_multiple"],
            reverse=best,
        )
        top = sorted_list[:n]

        result = []
        for p in top:
            result.append({
                "round_id": p.get("round_id", ""),
                "r_multiple": p["r_multiple"],
                "realized_pnl": str(p["realized_pnl"]),
                "exit_reason": p.get("exit_reason", "unknown"),
                "direction": p.get("direction"),
                "bars_held": p.get("bars_held", 0),
                "pyramid_count": p.get("pyramid_count", 0),
                "closed_ts": p["closed_ts"],
            })
        return result

    # ========================================================================
    # 工具函数
    # ========================================================================

    @staticmethod
    def _to_decimal(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _to_float(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_int(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_div(
        num: Any, den: Any, as_float: bool = False
    ) -> Any:
        """除零保护。"""
        try:
            num_f = float(num)
            den_f = float(den)
            if den_f == 0:
                return 0.0 if as_float else Decimal("0")
            result = num_f / den_f
            return float(result) if as_float else Decimal(str(result))
        except (ValueError, TypeError):
            return 0.0 if as_float else Decimal("0")

    @staticmethod
    def _median(values: List[float]) -> float:
        """中位数。"""
        if not values:
            return 0.0
        s = sorted(values)
        n = len(s)
        if n % 2 == 1:
            return float(s[n // 2])
        return float((s[n // 2 - 1] + s[n // 2]) / 2.0)

    # ========================================================================
    # 报告格式化（便于打印/日志）
    # ========================================================================

    def format_summary(self, report: AttributionReport) -> str:
        """
        生成人类可读的摘要字符串。用于日志或控制台。

        契约：
        - 纯函数（不写日志，不打印，只返回字符串）
        - 所有信息在 30 行内
        """
        lines = [
            "=" * 60,
            "Round Attribution Report",
            "=" * 60,
            f"Total rounds:      {report.total_rounds}",
            f"Malformed rounds:  {report.malformed_rounds}",
            "",
            f"Wins / Losses:     {report.wins} / {report.losses} "
            f"(breakeven: {report.breakeven})",
            f"Win rate:          {report.win_rate*100:.1f}%",
            f"Average R:         {report.avg_r:+.3f}",
            f"Median R:          {report.median_r:+.3f}",
            f"Total PnL:         {report.total_pnl} USDT",
            f"Profit factor:     {report.profit_factor:.2f}",
            f"Max drawdown:      {report.max_drawdown_pct:.2f}%",
            f"Sharpe estimate:   {report.sharpe_estimate:.2f}",
            f"Avg bars held:     {report.avg_bars_held:.1f}",
            f"Avg capture ratio: "
            f"{report.avg_capture_ratio*100:.1f}%"
            if report.avg_capture_ratio is not None
            else "Avg capture ratio: N/A",
            f"False signal rate: {report.false_signal_rate*100:.1f}%",
            "",
            "By Exit Reason:",
        ]

        for reason, stats in report.by_exit_reason.items():
            lines.append(
                f"  {reason:<18} "
                f"count={stats['count']:>3} "
                f"win={stats['win_rate']*100:>5.1f}% "
                f"avg_r={stats['avg_r']:+.2f} "
                f"pnl={stats['total_pnl']}"
            )

        lines.append("")
        lines.append("=" * 60)
        return "\n".join(lines)


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: Optional[dict] = None) -> RoundAttribution:
    """模块工厂入口。无状态，返回无状态实例。"""
    return RoundAttribution()


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "analytics/round_attribution.py")
    return MANIFEST
