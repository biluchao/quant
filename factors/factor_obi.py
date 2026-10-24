"""
factors/factor_obi.py — 订单簿失衡因子（Order Book Imbalance）

因子定义（见 FACTOR_SPEC.md）：
    距离权重 w_i = exp(-decay_k * i)，i 从 0 开始（0 = 最优价）
    bid_weighted = Σ(w_i * bid_qty_i)  for i in 0..levels-1
    ask_weighted = Σ(w_i * ask_qty_i)  for i in 0..levels-1
    raw = (bid_weighted - ask_weighted) / (bid_weighted + ask_weighted + ε)

    时间平滑（指数加权）：
        alpha = 1 - exp(-ln(2) * kline_interval_s / smooth_half_life_s)
        smoothed = alpha * raw + (1 - alpha) * prev_smoothed

归一化：滚动分位数归一化（窗口 quantile_window=500），映射到 [-1, 1]

经济学解释：
    OBI 衡量盘口挂单的方向性偏好。买盘量大于卖盘量时，OBI 为正，表示
    买方力量强。距离加权是因为靠近盘口的挂单更可能成交，信息含量更高。
    时间平滑是为了过滤单拍噪声——币安盘口变化快，单拍 OBI 噪声大，
    平滑后更稳定。

关键契约：
1. snapshot 为 None 或过期（与 bar.close_ts 偏差 > snapshot_max_age_ms）
   → 返回上一根的值，标记 stale，不更新 _prev_smoothed，不污染 _raw_history
2. 交叉盘口（bids[0] >= asks[0]）→ 视为无效快照，同上处理
3. 空盘口（bid 或 ask 为空）→ 返回 0.0，分母 ε 保护
4. 档位不足（快照少于 levels）→ 使用实际可用档位，不补 0
5. qty=0 的档位 → 跳过（防御性）
6. 距离权重不归一化：因为 (bid-ask)/(bid+ask) 中权重在分子分母对消
7. 平滑在归一化之前：raw → smoothed → _make_value（内部归一化）
8. 平滑的 alpha 由 half_life_s 和 kline_interval_s 推导，不是直接配置
9. 默认 half_life_s=2s，kline_interval=180s → alpha≈1.0（实际不平滑）
   如需平滑，配置 300s 或更长（如 5min K 线配 300s 半衰期）
10. 前 500 根 K 线分位数窗口未满 → WARMUP，_make_value 不追加历史

参数敏感性分析（见 FACTOR_SPEC.md）：
    decay_k=0.10: 假信号率 14.1%，IC 均值 0.12
    decay_k=0.15: 假信号率 13.7%，IC 均值 0.15 ← 默认
    decay_k=0.20: 假信号率 13.9%，IC 均值 0.14
    levels=5:    假信号率 14.5%，IC 均值 0.11
    levels=10:   假信号率 13.7%，IC 均值 0.15 ← 默认
    levels=20:   假信号率 13.5%，IC 均值 0.16（计算量翻倍，Step 2 考虑）

已知限制：
1. 盘口深度不足 10 档时（山寨币常见），按实际档位计算，权重重新归一化
2. 大单挂单（>500 张）在盘口停留，OBI 会持续偏高，但可能是虚假挂单（冰山单/诱导单）
3. 币安 depth 流是 100ms 一拍，快照时间戳与 K 线收线时间可能差 50-200ms
4. 极端行情下盘口瞬间变薄，OBI 会剧烈跳动

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

import logging
import math
import time
from typing import Any, Optional

from core.factor import FactorProvider, FactorState, FactorValue

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "obi",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["obi", "orderbook_imbalance"],
    "module_dependencies": [],
    "normalized_range": (-1.0, 1.0),
    "fallback": None,
    "enabled": True,
    "description": "Order Book Imbalance factor with distance weighting and time smoothing",
}


# ============================================================================
# 默认参数（可被 config 覆盖）
# ============================================================================
DEFAULT_LEVELS = 10
DEFAULT_DECAY_K = 0.15
DEFAULT_SMOOTH_HALF_LIFE_S = 2.0
DEFAULT_KLINE_INTERVAL_MS = 3 * 60 * 1000
DEFAULT_SNAPSHOT_MAX_AGE_MS = 500
EPSILON = 1e-12


# ============================================================================
# OBIFactor
# ============================================================================
class OBIFactor(FactorProvider):
    """
    订单簿失衡因子。详见模块 docstring。

    用法：
        factor = OBIFactor(config)
        registry.register("obi", factor)

        # 每根 K 线收线时（顺序由状态机保证）
        factor.on_bar_close(bar)             # 基类：更新 bars_seen
        fv = factor.compute(bar, snapshot)   # 使用 snapshot 计算
    """

    name = "obi"
    version = "1.0.0"
    normalized_range = (-1.0, 1.0)

    def __init__(self, config: dict):
        super().__init__(config)

        # ---- 提取因子专属参数 ----
        factor_cfg = self._extract_factor_config(config)
        data_cfg = config.get("data", {})

        self.levels = int(factor_cfg.get("levels", DEFAULT_LEVELS))
        self.decay_k = float(factor_cfg.get("decay_k", DEFAULT_DECAY_K))
        self.smooth_half_life_s = float(
            factor_cfg.get("smooth_half_life_s", DEFAULT_SMOOTH_HALF_LIFE_S)
        )
        self.snapshot_max_age_ms = int(
            factor_cfg.get("snapshot_max_age_ms", DEFAULT_SNAPSHOT_MAX_AGE_MS)
        )

        # K 线间隔（用于计算平滑 alpha）
        interval_str = data_cfg.get("kline_interval", "3m")
        self.kline_interval_ms = self._parse_interval_ms(interval_str)

        # ---- 参数校验 ----
        if self.levels < 1:
            raise ValueError(f"levels must be >= 1, got {self.levels}")
        if self.decay_k < 0:
            raise ValueError(f"decay_k must be >= 0, got {self.decay_k}")
        if self.smooth_half_life_s <= 0:
            raise ValueError(
                f"smooth_half_life_s must be > 0, "
                f"got {self.smooth_half_life_s}"
            )
        if self.snapshot_max_age_ms < 0:
            raise ValueError(
                f"snapshot_max_age_ms must be >= 0, "
                f"got {self.snapshot_max_age_ms}"
            )

        # ---- 计算平滑 alpha ----
        # alpha = 1 - exp(-ln(2) * kline_s / half_life_s)
        # 当 half_life_s=2s, kline=180s → alpha≈1.0（不平滑）
        # 当 half_life_s=300s, kline=180s → alpha≈0.34（有平滑）
        kline_s = self.kline_interval_ms / 1000.0
        exponent = -math.log(2) * kline_s / self.smooth_half_life_s
        self._alpha = 1.0 - math.exp(exponent)

        # alpha 可能因浮点误差略超出 (0, 1]
        if self._alpha <= 0:
            raise ValueError(
                f"computed alpha {self._alpha:.6f} <= 0, "
                f"check smooth_half_life_s={self.smooth_half_life_s}"
            )
        if self._alpha > 1.0:
            self._alpha = 1.0

        # ---- 内部状态 ----
        self._prev_smoothed: Optional[float] = None

        # ---- 统计 ----
        self._stats = {
            "snapshots_received": 0,
            "stale_none": 0,
            "stale_outdated": 0,
            "stale_crossed": 0,
            "stale_empty": 0,
            "valid_computes": 0,
            "synthetic_skipped": 0,
        }

        logger.info(
            f"OBIFactor initialized "
            f"levels={self.levels} "
            f"decay_k={self.decay_k} "
            f"smooth_half_life_s={self.smooth_half_life_s} "
            f"kline_interval={interval_str} ({self.kline_interval_ms}ms) "
            f"alpha={self._alpha:.4f} "
            f"snapshot_max_age={self.snapshot_max_age_ms}ms"
        )
        if self._alpha > 0.999:
            logger.info(
                "OBIFactor: alpha≈1.0, time smoothing effectively disabled "
                "(half_life_s << kline_interval_s). "
                "To enable smoothing, set smooth_half_life_s >= 300s."
            )

    def _parse_interval_ms(self, interval: str) -> int:
        """解析 interval 为毫秒。"""
        mapping = {
            "1m": 60 * 1000,
            "3m": 3 * 60 * 1000,
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000,
        }
        if interval not in mapping:
            raise ValueError(
                f"unsupported interval: {interval}, "
                f"supported: {list(mapping.keys())}"
            )
        return mapping[interval]

    # ========================================================================
    # 核心：计算
    # ========================================================================

    def compute(
        self,
        bar: Any,
        snapshot: Optional[dict] = None,
    ) -> FactorValue:
        """
        计算 OBI 因子值。

        契约：
        - snapshot 为 None 或过期 → 返回上一根值，标记 stale
        - 交叉盘口 → 视为无效，返回上一根值，标记 stale
        - 空盘口 → 返回 0.0，标记 stale
        - 内部异常包装成 ERROR
        """
        t0 = time.perf_counter()

        try:
            bar_ts = getattr(bar, "ts", 0)
            bar_close_ts = getattr(
                bar, "close_ts", bar_ts + self.kline_interval_ms
            )

            # ---- 1. 快照有效性检查 ----
            if snapshot is None:
                self._stats["stale_none"] += 1
                return self._stale_return(
                    bar_ts, "snapshot is None", t0
                )

            # 检查快照字段
            snap_ts = snapshot.get("ts")
            bids = snapshot.get("bids")
            asks = snapshot.get("asks")

            if snap_ts is None or bids is None or asks is None:
                self._stats["stale_none"] += 1
                return self._stale_return(
                    bar_ts, "snapshot missing required fields", t0
                )

            # 快照过期检查
            if abs(snap_ts - bar_close_ts) > self.snapshot_max_age_ms:
                self._stats["stale_outdated"] += 1
                return self._stale_return(
                    bar_ts,
                    f"snapshot ts={snap_ts} outdated vs "
                    f"bar_close_ts={bar_close_ts} "
                    f"(diff > {self.snapshot_max_age_ms}ms)",
                    t0,
                )

            self._stats["snapshots_received"] += 1

            # ---- 2. 解析盘口 ----
            # 取前 levels 档（含最优价），跳过 qty=0 的防御性检查
            bid_levels = self._parse_levels(bids, self.levels)
            ask_levels = self._parse_levels(asks, self.levels)

            # 空盘口检查
            if not bid_levels and not ask_levels:
                self._stats["stale_empty"] += 1
                return self._stale_return(
                    bar_ts, "both bid and ask empty", t0
                )

            # 交叉盘口检查
            if bid_levels and ask_levels:
                best_bid = bid_levels[0][0]
                best_ask = ask_levels[0][0]
                if best_bid >= best_ask:
                    self._stats["stale_crossed"] += 1
                    logger.warning(
                        f"OBIFactor crossed book: "
                        f"best_bid={best_bid} >= best_ask={best_ask}, "
                        f"using previous value"
                    )
                    return self._stale_return(
                        bar_ts,
                        f"crossed book: bid={best_bid} ask={best_ask}",
                        t0,
                    )

            # ---- 3. 计算距离加权 ----
            bid_weighted = self._weighted_sum(bid_levels)
            ask_weighted = self._weighted_sum(ask_levels)

            total = bid_weighted + ask_weighted
            if total < EPSILON:
                # 双方加权量都极小（空盘口的变体）
                self._stats["stale_empty"] += 1
                return self._stale_return(
                    bar_ts, "weighted volume near zero", t0
                )

            raw = (bid_weighted - ask_weighted) / total

            # 数值稳定性：raw 应在 [-1, 1]
            raw = max(-1.0, min(1.0, raw))

            # ---- 4. 时间平滑 ----
            if self._prev_smoothed is None:
                smoothed = raw
            else:
                smoothed = (
                    self._alpha * raw
                    + (1.0 - self._alpha) * self._prev_smoothed
                )
            self._prev_smoothed = smoothed

            self._stats["valid_computes"] += 1

            # ---- 5. 归一化 + 构造输出 ----
            compute_ms = (time.perf_counter() - t0) * 1000
            return self._make_value(smoothed, bar_ts, compute_ms=compute_ms)

        except Exception as e:
            logger.error(
                f"OBIFactor.compute error: {e}",
                exc_info=True,
            )
            return self._make_value(
                0.0,
                getattr(bar, "ts", 0),
                error=f"{type(e).__name__}: {e}",
                compute_ms=(time.perf_counter() - t0) * 1000,
            )

    # ========================================================================
    # 辅助
    # ========================================================================

    def _parse_levels(
        self, levels_raw: Any, limit: int
    ) -> list:
        """
        解析盘口档位，取前 limit 档。

        币安约定：
        - bids 降序（最优价在前）
        - asks 升序（最优价在前）

        返回：[(price, qty), ...] 保持原顺序
        跳过 qty <= 0 的档位
        """
        if not isinstance(levels_raw, (list, tuple)):
            return []
        result = []
        for item in levels_raw[:limit]:
            try:
                price, qty = item[0], item[1]
                qty_f = float(qty)
            except (IndexError, TypeError, ValueError):
                continue
            if qty_f > 0:
                result.append((price, qty_f))
        return result

    def _weighted_sum(self, levels: list) -> float:
        """
        计算距离加权求和。

        w_i = exp(-decay_k * i)，i 从 0 开始（最优价权重最大）
        """
        total = 0.0
        for i, (_price, qty) in enumerate(levels):
            w = math.exp(-self.decay_k * i)
            total += w * qty
        return total

    def _stale_return(
        self, bar_ts: int, reason: str, t0: float
    ) -> FactorValue:
        """
        构造 stale 返回值。

        契约：
        - 有上一根值 → 返回上一根值，state=STALE，stale=True
        - 无上一根值（预热期）→ 返回 0.0，state=WARMUP，stale=True
        - 不更新 _prev_smoothed（保持上一个有效值）
        - 不追加到 _raw_history（不污染分位数窗口）
        - 不触发 error 计数（stale 不是因子 bug）
        """
        compute_ms = (time.perf_counter() - t0) * 1000

        if self._last_value is not None:
            return FactorValue(
                name=self.name,
                value=self._last_value.value,
                raw_value=self._last_value.raw_value,
                bar_ts=bar_ts,
                state=FactorState.STALE,
                version=self.version,
                outlier=False,
                stale=True,
                compute_ms=compute_ms,
                error=reason,
            )

        # 预热期，无上一根值
        return FactorValue(
            name=self.name,
            value=0.0,
            raw_value=0.0,
            bar_ts=bar_ts,
            state=FactorState.WARMUP,
            version=self.version,
            outlier=False,
            stale=True,
            compute_ms=compute_ms,
            error=reason,
        )

    # ========================================================================
    # 重置
    # ========================================================================

    def reset(self) -> None:
        """重置因子状态。"""
        super().reset()
        self._prev_smoothed = None
        for k in self._stats:
            self._stats[k] = 0
        logger.info(f"OBIFactor reset")

    # ========================================================================
    # 健康检查扩展
    # ========================================================================

    def health(self) -> dict:
        """健康检查（扩展基类）。"""
        base = super().health()
        base.update({
            "levels": self.levels,
            "decay_k": self.decay_k,
            "alpha": self._alpha,
            "smooth_half_life_s": self.smooth_half_life_s,
            "snapshot_max_age_ms": self.snapshot_max_age_ms,
            "stats": dict(self._stats),
        })
        # 计算 stale 率
        total_snapshots = (
            self._stats["snapshots_received"]
            + self._stats["stale_none"]
            + self._stats["stale_outdated"]
            + self._stats["stale_crossed"]
            + self._stats["stale_empty"]
        )
        if total_snapshots > 0:
            stale_total = (
                self._stats["stale_none"]
                + self._stats["stale_outdated"]
                + self._stats["stale_crossed"]
                + self._stats["stale_empty"]
            )
            base["stale_rate"] = stale_total / total_snapshots
        return base


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> OBIFactor:
    """模块工厂入口。"""
    return OBIFactor(config)
