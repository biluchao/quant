"""
execution/slippage_model.py — 滑点模型（额外滑点估算 + 尾部注入 + 实盘校准）

fill_model_market.py 算的是"盘口能吃到什么价"，本文件算的是"除了盘口之外
还要滑多少"。

两者是叠加关系，不是替代关系：
    final_price = fill_model.avg_price
                  + slippage_model.extra_bps 的影响

职责（唯一）：
1. 估算额外滑点（bps）：基础滑点 + 延迟滑点 + 尾部滑点
2. 尾部注入：5% 概率模拟"盘口瞬间变薄"，滑点放大 5 倍
3. 实盘校准：用实盘成交数据校准 base_bps 和 tail_extra_bps
4. 应用于 fill_model 的成交结果

不做什么：
- 不接触盘口（fill_model 负责）
- 不修改执行流程（gateway_virtual 负责）
- 不抛异常（所有异常以 extra_bps=0 返回）
- 不写日志（调用方负责）
- 不持久化（Step 1 不落库）

职责边界：
- SlippageModel：估算额外 bps，不关心盘口
- FillModel：盘口撮合，不关心额外滑点
- GatewayVirtual：两者都调，叠加结果

关键契约（"六不变"）：
1. 可复现：固定 seed，同输入 → 同输出
2. 不抛异常：所有失败返回 SlippageResult(extra_bps=0)
3. 纯估算：只算 bps，不接触盘口
4. 悲观默认：默认参数偏保守（base 2 bps + latency 8 bps + 5% 尾部）
5. 校准幂等：重复校准覆盖旧值，保留历史
6. 校准有界：base_bps ∈ [1, 50]，tail_extra_bps ∈ [5, 200]

滑点组成：
    total_bps = base_bps + latency_bps + tail_bps

    base_bps:       基础滑点，默认 2 bps（约 0.02%）
    latency_bps:    延迟滑点 = latency_ms * latency_per_ms_bps
                    默认 0.1 bps/ms → 80ms 延迟 = 8 bps
    tail_bps:       尾部滑点，5% 概率触发，默认 20 bps（5 × 4 bps 基础）
                    期望值 = 5% × 20 = 1 bps

    默认总期望：2 + 8 + 1 = 11 bps（约 0.11%）

校准算法：
    输入：实盘每笔成交的 realized_slippage_bps（正数表示不利）
    统计：
        median = 中位数
        p90 = 90 分位
    校准：
        base_bps = clamp(median, 1, 50)
        tail_extra_bps = clamp(p90 - median, 5, 200)

    为什么用 median 而不是 mean？
    - mean 被极端值拉高（少数大滑点会推高 mean）
    - median 反映"典型滑点"，更稳健

    为什么用 p90 而不是 p95？
    - p95 太极端，容易过拟合
    - p90 能覆盖 90% 的场景，更稳健

    为什么 base 有下限 1 bps？
    - 防止校准成 0.1 bps，忽略真实滑点
    - 实盘总有摩擦成本

    为什么 base 有上限 50 bps？
    - 防止极端数据污染
    - 50 bps（0.5%）已经很大了，超过说明模型或数据有问题

应用算法（BUY）：
    adj_factor = 1 + total_bps / 10000
    new_price = base_price * adj_factor

应用算法（SELL）：
    adj_factor = 1 - total_bps / 10000
    new_price = base_price * adj_factor

    注意：qty 不变，只有 price 受影响。
    滑点让我们买得更贵、卖得更便宜，但不影响成交数量。

与 GatewayVirtual 的协作：
    gateway_virtual.place_order()
      → fill_model.compute() 得到 FillResult
      → slippage_model.estimate(latency_ms) 得到 SlippageResult
      → slippage_model.apply_to_result(FillResult, SlippageResult) 得到调整后的 FillResult
      → 用调整后的 FillResult 更新虚拟账户

与实盘对比：
    - 实盘每笔成交记录 realized_slippage_bps
    - 影子模式跑满 7 天后，用这些数据校准
    - 校准后模型可信度从"猜"变成"测"
    - health() 返回 calibration_bias_bps 监控偏差

作者：乐高
审查：比蒙（策略）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import logging
from typing import Any, Dict, List, Optional

import numpy as np

from core.gateway import Fill, OrderSide

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "slippage_model",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "owner": "lego",
    "capabilities": ["slippage_model", "tail_injection", "calibration"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Extra slippage estimator with tail injection and real-trade calibration",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_BASE_BPS = Decimal("2.0")              # 基础滑点 2 bps
DEFAULT_LATENCY_PER_MS_BPS = Decimal("0.1")    # 每毫秒 0.1 bps
DEFAULT_TAIL_PROBABILITY = 0.05                # 5% 概率触发尾部
DEFAULT_TAIL_EXTRA_BPS = Decimal("20.0")       # 尾部额外 20 bps
DEFAULT_CALIBRATION_MIN_SAMPLES = 100          # 校准最少样本
DEFAULT_BASE_BPS_MIN = Decimal("1.0")          # 校准后 base 下限
DEFAULT_BASE_BPS_MAX = Decimal("50.0")         # 校准后 base 上限
DEFAULT_TAIL_EXTRA_BPS_MIN = Decimal("5.0")    # 校准后 tail 下限
DEFAULT_TAIL_EXTRA_BPS_MAX = Decimal("200.0")  # 校准后 tail 上限
DEFAULT_CALIBRATION_BIAS_THRESHOLD_BPS = 5.0   # 偏差阈值
BPS_MULTIPLIER = Decimal("10000")
DEFAULT_RNG_SEED = 42


# ============================================================================
# 配置
# ============================================================================
@dataclass(frozen=True)
class SlippageConfig:
    """
    滑点模型配置。不可变。

    所有参数在构造时校验。
    """
    base_bps: Decimal = DEFAULT_BASE_BPS
    latency_per_ms_bps: Decimal = DEFAULT_LATENCY_PER_MS_BPS
    tail_probability: float = DEFAULT_TAIL_PROBABILITY
    tail_extra_bps: Decimal = DEFAULT_TAIL_EXTRA_BPS
    calibration_min_samples: int = DEFAULT_CALIBRATION_MIN_SAMPLES
    calibration_bias_threshold_bps: float = (
        DEFAULT_CALIBRATION_BIAS_THRESHOLD_BPS
    )

    def __post_init__(self):
        if self.base_bps < 0:
            raise ValueError(
                f"base_bps must be >= 0, got {self.base_bps}"
            )
        if self.latency_per_ms_bps < 0:
            raise ValueError(
                f"latency_per_ms_bps must be >= 0, "
                f"got {self.latency_per_ms_bps}"
            )
        if not (0.0 <= self.tail_probability <= 1.0):
            raise ValueError(
                f"tail_probability must be in [0, 1], "
                f"got {self.tail_probability}"
            )
        if self.tail_extra_bps < 0:
            raise ValueError(
                f"tail_extra_bps must be >= 0, "
                f"got {self.tail_extra_bps}"
            )
        if self.calibration_min_samples < 1:
            raise ValueError(
                f"calibration_min_samples must be >= 1, "
                f"got {self.calibration_min_samples}"
            )
        if self.calibration_bias_threshold_bps <= 0:
            raise ValueError(
                f"calibration_bias_threshold_bps must be > 0, "
                f"got {self.calibration_bias_threshold_bps}"
            )


# ============================================================================
# 输出
# ============================================================================
@dataclass
class SlippageResult:
    """
    滑点估算结果。

    契约：
    - extra_bps 永远是 >= 0（滑点不利）
    - tail_triggered 表示是否触发尾部
    - 各分量相加等于 extra_bps
    """
    extra_bps: float = 0.0
    base_bps: float = 0.0
    latency_bps: float = 0.0
    tail_bps: float = 0.0
    tail_triggered: bool = False
    latency_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """序列化。"""
        return {
            "extra_bps": self.extra_bps,
            "base_bps": self.base_bps,
            "latency_bps": self.latency_bps,
            "tail_bps": self.tail_bps,
            "tail_triggered": self.tail_triggered,
            "latency_ms": self.latency_ms,
        }


# ============================================================================
# 校准记录
# ============================================================================
@dataclass(frozen=True)
class CalibrationRecord:
    """单次校准记录。"""
    ts: int
    samples: int
    median_bps: float
    p90_bps: float
    old_base_bps: float
    new_base_bps: float
    old_tail_extra_bps: float
    new_tail_extra_bps: float
    accepted: bool
    reject_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "samples": self.samples,
            "median_bps": self.median_bps,
            "p90_bps": self.p90_bps,
            "old_base_bps": self.old_base_bps,
            "new_base_bps": self.new_base_bps,
            "old_tail_extra_bps": self.old_tail_extra_bps,
            "new_tail_extra_bps": self.new_tail_extra_bps,
            "accepted": self.accepted,
            "reject_reason": self.reject_reason,
        }


# ============================================================================
# SlippageModel
# ============================================================================
class SlippageModel:
    """
    滑点模型。详见模块 docstring。

    用法：
        rng = np.random.default_rng(42)
        model = SlippageModel(
            config=SlippageConfig(),
            rng=rng,
        )

        # 估算滑点
        result = model.estimate(latency_ms=80)
        # result.extra_bps ≈ 2 + 8 + (5% ? 20 : 0)

        # 应用到 FillResult（由 gateway_virtual 调用）
        # new_fills, new_avg = model.apply_to_fills(
        #     fills=fill_result.fills,
        #     side=OrderSide.BUY,
        #     slippage_bps=result.extra_bps,
        # )

        # 校准（影子模式跑满后）
        ok = model.calibrate_from_trades(realized_bps_list)
    """

    name = "slippage_model"
    version = "1.0.0"

    def __init__(
        self,
        config: Optional[SlippageConfig] = None,
        rng: Optional[np.random.Generator] = None,
    ):
        self._config = config or SlippageConfig()
        self._rng = rng or np.random.default_rng(DEFAULT_RNG_SEED)

        # ---- 校准状态 ----
        self._calibrated = False
        self._calibration_history: List[CalibrationRecord] = []
        self._last_calibration: Optional[CalibrationRecord] = None

        # ---- 统计 ----
        self._stats = {
            "estimates": 0,
            "tail_triggered": 0,
            "calibrations": 0,
            "calibration_rejections": 0,
        }

        logger.info(
            f"SlippageModel initialized "
            f"base_bps={self._config.base_bps} "
            f"latency_per_ms_bps={self._config.latency_per_ms_bps} "
            f"tail_probability={self._config.tail_probability} "
            f"tail_extra_bps={self._config.tail_extra_bps}"
        )

    # ------------------------------------------------------------------------
    # 核心：估算
    # ------------------------------------------------------------------------

    def estimate(self, latency_ms: int = 0) -> SlippageResult:
        """
        估算额外滑点。

        Args:
            latency_ms: 模拟的成交延迟（毫秒），<= 0 视为无延迟

        Returns:
            SlippageResult

        契约：
        - 纯估算，不接触盘口
        - 不抛异常（内部异常返回 extra_bps=0）
        - 可复现：同 rng 状态 → 同结果
        - 尾部触发：5% 概率（可配）
        """
        try:
            # 1. 延迟滑点
            latency_ms_eff = max(0, int(latency_ms))
            latency_bps = float(
                self._config.latency_per_ms_bps
                * Decimal(str(latency_ms_eff))
            )

            # 2. 尾部触发
            tail_triggered = (
                self._config.tail_probability > 0.0
                and float(self._rng.random())
                < self._config.tail_probability
            )
            tail_bps = (
                float(self._config.tail_extra_bps)
                if tail_triggered else 0.0
            )

            # 3. 基础滑点
            base_bps = float(self._config.base_bps)

            # 4. 汇总
            extra_bps = base_bps + latency_bps + tail_bps

            with self._lock_if_needed():
                self._stats["estimates"] += 1
                if tail_triggered:
                    self._stats["tail_triggered"] += 1

            return SlippageResult(
                extra_bps=extra_bps,
                base_bps=base_bps,
                latency_bps=latency_bps,
                tail_bps=tail_bps,
                tail_triggered=tail_triggered,
                latency_ms=latency_ms_eff,
            )
        except Exception as e:
            logger.error(
                f"SlippageModel.estimate error: {e}", exc_info=True
            )
            return SlippageResult(extra_bps=0.0)

    # ------------------------------------------------------------------------
    # 应用：调整 fills
    # ------------------------------------------------------------------------

    def apply_to_fills(
        self,
        fills: List[Fill],
        side: OrderSide,
        extra_bps: float,
    ) -> tuple:
        """
        将滑点应用到 fills 的成交价上。

        Args:
            fills: 原始 fills（来自 fill_model）
            side: 订单方向
            extra_bps: 额外滑点（bps，正数表示不利）

        Returns:
            (adjusted_fills, adjusted_avg_price)

        契约：
        - qty 不变，只调整 price
        - BUY: price * (1 + extra_bps / 10000)
        - SELL: price * (1 - extra_bps / 10000)
        - extra_bps <= 0 直接返回原始 fills
        - 内部异常返回原始 fills
        """
        if not fills:
            return [], None

        if extra_bps <= 0:
            avg = self._weighted_avg_price(fills)
            return fills, avg

        try:
            factor = Decimal(str(extra_bps)) / BPS_MULTIPLIER

            adjusted_fills: List[Fill] = []
            total_notional = Decimal("0")
            total_qty = Decimal("0")

            for f in fills:
                if side == OrderSide.BUY:
                    new_price = f.price * (1 + factor)
                else:
                    new_price = f.price * (1 - factor)

                # 价格必须 > 0
                if new_price <= 0:
                    new_price = f.price  # 回退

                # 重算 fee（fee 与 price 成正比）
                new_fee = (
                    f.fee * new_price / f.price
                    if f.price > 0 else f.fee
                )

                adjusted_fills.append(Fill(
                    trade_id=f.trade_id,
                    price=new_price,
                    qty=f.qty,
                    fee=new_fee,
                    fee_asset=f.fee_asset,
                    is_maker=f.is_maker,
                    ts=f.ts,
                ))

                total_notional += new_price * f.qty
                total_qty += f.qty

            avg_price = (
                total_notional / total_qty
                if total_qty > 0 else None
            )
            return adjusted_fills, avg_price
        except Exception as e:
            logger.error(
                f"SlippageModel.apply_to_fills error: {e}",
                exc_info=True,
            )
            avg = self._weighted_avg_price(fills)
            return fills, avg

    @staticmethod
    def _weighted_avg_price(
        fills: List[Fill]
    ) -> Optional[Decimal]:
        """计算加权平均价。"""
        total_notional = Decimal("0")
        total_qty = Decimal("0")
        for f in fills:
            total_notional += f.price * f.qty
            total_qty += f.qty
        if total_qty <= 0:
            return None
        return total_notional / total_qty

    # ------------------------------------------------------------------------
    # 校准
    # ------------------------------------------------------------------------

    def calibrate_from_trades(
        self,
        realized_slippage_bps: List[float],
        now_ms: Optional[int] = None,
    ) -> bool:
        """
        用实盘成交数据校准。

        Args:
            realized_slippage_bps: 每笔成交的滑点（bps，正数表示不利）
            now_ms: 校准时间戳（毫秒）

        Returns:
            True: 校准成功
            False: 拒绝（样本不足/新参数更差/内部异常）

        契约：
        - 样本数 < calibration_min_samples → 拒绝
        - 新参数让偏差更大 → 拒绝
        - 校准后 base_bps ∈ [1, 50]，tail_extra_bps ∈ [5, 200]
        - 幂等：重复校准覆盖旧值
        - 保留历史到 _calibration_history
        """
        if not realized_slippage_bps:
            logger.warning("calibrate: no samples")
            return False

        try:
            samples = np.array(realized_slippage_bps, dtype=float)
            samples = samples[np.isfinite(samples)]

            if len(samples) < self._config.calibration_min_samples:
                logger.warning(
                    f"calibrate rejected: {len(samples)} samples < "
                    f"{self._config.calibration_min_samples} required"
                )
                self._stats["calibration_rejections"] += 1
                return False

            median_bps = float(np.median(samples))
            p90_bps = float(np.percentile(samples, 90))

            # 计算新参数
            new_base_bps = float(np.clip(
                median_bps,
                float(DEFAULT_BASE_BPS_MIN),
                float(DEFAULT_BASE_BPS_MAX),
            ))
            new_tail_extra_bps = float(np.clip(
                p90_bps - median_bps,
                float(DEFAULT_TAIL_EXTRA_BPS_MIN),
                float(DEFAULT_TAIL_EXTRA_BPS_MAX),
            ))

            # 旧参数
            old_base_bps = float(self._config.base_bps)
            old_tail_extra_bps = float(self._config.tail_extra_bps)

            # 接受判断：新参数与实盘中位数的偏差 vs 旧参数偏差
            old_bias = abs(old_base_bps - median_bps)
            new_bias = abs(new_base_bps - median_bps)
            accepted = new_bias <= old_bias or not self._calibrated
            reject_reason = None if accepted else (
                f"new_bias {new_bias:.2f} > old_bias {old_bias:.2f}"
            )

            # 构造记录
            record = CalibrationRecord(
                ts=now_ms or 0,
                samples=len(samples),
                median_bps=median_bps,
                p90_bps=p90_bps,
                old_base_bps=old_base_bps,
                new_base_bps=new_base_bps,
                old_tail_extra_bps=old_tail_extra_bps,
                new_tail_extra_bps=new_tail_extra_bps,
                accepted=accepted,
                reject_reason=reject_reason,
            )
            self._calibration_history.append(record)
            self._last_calibration = record

            if not accepted:
                logger.warning(
                    f"calibrate rejected: {reject_reason}"
                )
                self._stats["calibration_rejections"] += 1
                return False

            # 应用新参数
            self._config = SlippageConfig(
                base_bps=Decimal(str(new_base_bps)),
                latency_per_ms_bps=self._config.latency_per_ms_bps,
                tail_probability=self._config.tail_probability,
                tail_extra_bps=Decimal(str(new_tail_extra_bps)),
                calibration_min_samples=self._config.calibration_min_samples,
                calibration_bias_threshold_bps=(
                    self._config.calibration_bias_threshold_bps
                ),
            )
            self._calibrated = True
            self._stats["calibrations"] += 1

            logger.info(
                f"calibrate accepted: "
                f"samples={len(samples)} "
                f"median={median_bps:.2f}bps p90={p90_bps:.2f}bps "
                f"base {old_base_bps:.2f}→{new_base_bps:.2f} "
                f"tail {old_tail_extra_bps:.2f}→{new_tail_extra_bps:.2f}"
            )
            return True
        except Exception as e:
            logger.error(
                f"calibrate error: {e}", exc_info=True
            )
            self._stats["calibration_rejections"] += 1
            return False

    # ------------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------------

    def get_config(self) -> SlippageConfig:
        """获取当前配置。"""
        return self._config

    def is_calibrated(self) -> bool:
        """是否已校准。"""
        return self._calibrated

    def get_calibration_history(self) -> List[Dict[str, Any]]:
        """校准历史。"""
        return [r.to_dict() for r in self._calibration_history]

    def _lock_if_needed(self):
        """无锁实现的占位（模型是单线程调用）。"""
        from contextlib import nullcontext
        return nullcontext()

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        bias_bps = 0.0
        if self._last_calibration and self._last_calibration.accepted:
            current_base = float(self._config.base_bps)
            bias_bps = abs(
                current_base - self._last_calibration.median_bps
            )

        healthy = (
            bias_bps <= self._config.calibration_bias_threshold_bps
        )

        return {
            "healthy": healthy,
            "name": self.name,
            "version": self.version,
            "calibrated": self._calibrated,
            "calibration_bias_bps": bias_bps,
            "calibration_bias_threshold_bps": (
                self._config.calibration_bias_threshold_bps
            ),
            "config": {
                "base_bps": str(self._config.base_bps),
                "latency_per_ms_bps": str(
                    self._config.latency_per_ms_bps
                ),
                "tail_probability": self._config.tail_probability,
                "tail_extra_bps": str(self._config.tail_extra_bps),
            },
            "last_calibration": (
                self._last_calibration.to_dict()
                if self._last_calibration else None
            ),
            "stats": dict(self._stats),
            "timestamp": int(
                __import__("time").time() * 1000
            ),
        }

    def reset(self) -> None:
        """重置（测试用）。保留配置，清空校准历史。"""
        self._calibrated = False
        self._calibration_history = []
        self._last_calibration = None
        for k in self._stats:
            self._stats[k] = 0
        self._rng = np.random.default_rng(DEFAULT_RNG_SEED)
        logger.info("SlippageModel reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> SlippageModel:
    """
    模块工厂入口。

    config 可选：
    - slippage.base_bps: 默认 2.0
    - slippage.latency_per_ms_bps: 默认 0.1
    - slippage.tail_probability: 默认 0.05
    - slippage.tail_extra_bps: 默认 20.0
    - slippage.calibration_min_samples: 默认 100
    - slippage.seed: 默认 42

    注意：seed 通过 config.slippage.seed 或命令行传入，保证回测可复现。
    """
    cfg = (config or {}).get("slippage", {})

    slippage_config = SlippageConfig(
        base_bps=Decimal(str(cfg.get(
            "base_bps", str(DEFAULT_BASE_BPS)
        ))),
        latency_per_ms_bps=Decimal(str(cfg.get(
            "latency_per_ms_bps", str(DEFAULT_LATENCY_PER_MS_BPS)
        ))),
        tail_probability=float(cfg.get(
            "tail_probability", DEFAULT_TAIL_PROBABILITY
        )),
        tail_extra_bps=Decimal(str(cfg.get(
            "tail_extra_bps", str(DEFAULT_TAIL_EXTRA_BPS)
        ))),
        calibration_min_samples=int(cfg.get(
            "calibration_min_samples", DEFAULT_CALIBRATION_MIN_SAMPLES
        )),
        calibration_bias_threshold_bps=float(cfg.get(
            "calibration_bias_threshold_bps",
            DEFAULT_CALIBRATION_BIAS_THRESHOLD_BPS,
        )),
    )

    seed = int(cfg.get("seed", DEFAULT_RNG_SEED))
    rng = np.random.default_rng(seed)

    return SlippageModel(config=slippage_config, rng=rng)


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "execution/slippage_model.py")
    return MANIFEST
