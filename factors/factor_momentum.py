"""
factors/factor_momentum.py — 动量因子（ROC + 加速度）

因子定义（见 FACTOR_SPEC.md）：
    原始值 raw = roc_weight * ROC(roc_period) + accel_weight * accel
    ROC(3) = (close[t] - close[t-3]) / close[t-3]
    accel  = ROC(3)[t] - ROC(3)[t-1]
    默认 roc_weight=0.7, accel_weight=0.3

归一化：滚动分位数归一化（窗口 quantile_window=500），映射到 [-1, 1]

经济学解释：
    动量因子衡量价格趋势的持续性。ROC(3) 捕捉短期价格变化方向，
    加速度捕捉变化速度的增减。两者加权合成，既看方向也看强度。
    趋势市中 IC 通常较高（0.12-0.18），震荡市中 IC 下降（0.05-0.10）。

关键契约：
1. 计算在 compute() 内完成，禁止 I/O
2. 内部异常必须包装为 FactorValue(state=ERROR)
3. 前 roc_period 根 K 线（历史不足）返回 WARMUP，不污染分位数窗口
4. 归一化使用 _make_value()，自动处理前视偏差
5. 所有状态由基类 on_bar_close() 管理（WARMUP → READY）
6. 只依赖 bar.close
7. 不使用 snapshot（动量因子不需要盘口）
8. 单线程调用（由状态机序列化调用）
9. 不感知资金费率、策略状态、风控等级
10. 回测/影子/实盘使用同一份代码

参数敏感性分析（见 PARAM_TABLE.md）：
    roc_period=2：假信号率 15.2%，IC 均值 0.11
    roc_period=3：假信号率 13.7%，IC 均值 0.15 ← 默认
    roc_period=5：假信号率 12.9%，IC 均值 0.13
    roc_period=8：假信号率 11.4%，IC 均值 0.09

已知限制：
1. 震荡市中动量因子容易失效，IC 降到 0.05 以下
2. 资金费率结算前后 15 分钟，IC 下降 50%，状态机应降频
3. 极端行情（1 分钟波动 >3%）时，ROC(3) 被噪声主导，可能标记 outlier
4. 依赖 bar.close，不需要盘口/逐笔成交

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from collections import deque
import logging
import time
from typing import Any, Optional

from core.factor import FactorProvider, FactorState, FactorValue

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "momentum",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["momentum", "roc", "acceleration"],
    "module_dependencies": [],
    "normalized_range": (-1.0, 1.0),
    "fallback": None,
    "enabled": True,
    "description": "Momentum factor: weighted combination of ROC and acceleration",
}


# ============================================================================
# 默认参数（可被 config 覆盖）
# ============================================================================
DEFAULT_ROC_PERIOD = 3
DEFAULT_ROC_WEIGHT = 0.7
DEFAULT_ACCEL_WEIGHT = 0.3
MIN_WEIGHT_SUM_TOLERANCE = 1e-3


# ============================================================================
# MomentumFactor
# ============================================================================
class MomentumFactor(FactorProvider):
    """
    动量因子。详见模块 docstring。

    用法：
        factor = MomentumFactor(config)
        registry.register("momentum", factor)

        # 每根 K 线收线时（顺序由状态机保证）
        factor.on_bar_close(bar)             # 基类：更新 bars_seen，预热完成切 READY
        fv = factor.compute(bar, snapshot)   # snapshot 不使用，可传 None
    """

    name = "momentum"
    version = "1.0.0"
    normalized_range = (-1.0, 1.0)

    def __init__(self, config: dict):
        super().__init__(config)

        # ---- 提取因子专属参数 ----
        factor_cfg = self._extract_factor_config(config)

        self.roc_period = int(
            factor_cfg.get("roc_period", DEFAULT_ROC_PERIOD)
        )
        self.roc_weight = float(
            factor_cfg.get("roc_weight", DEFAULT_ROC_WEIGHT)
        )
        self.accel_weight = float(
            factor_cfg.get("accel_weight", DEFAULT_ACCEL_WEIGHT)
        )

        # ---- 参数校验 ----
        if self.roc_period < 1:
            raise ValueError(
                f"roc_period must be >= 1, got {self.roc_period}"
            )
        if self.roc_weight < 0 or self.accel_weight < 0:
            raise ValueError(
                f"weights must be non-negative, "
                f"got roc_weight={self.roc_weight} "
                f"accel_weight={self.accel_weight}"
            )
        total_weight = self.roc_weight + self.accel_weight
        if abs(total_weight - 1.0) > MIN_WEIGHT_SUM_TOLERANCE:
            raise ValueError(
                f"roc_weight + accel_weight must be 1.0, "
                f"got {total_weight:.6f} "
                f"(roc_weight={self.roc_weight}, "
                f"accel_weight={self.accel_weight})"
            )

        # ---- 内部状态 ----
        # 保留最近 roc_period + 1 个收盘价，计算 ROC(roc_period)
        self._close_history: deque = deque(
            maxlen=self.roc_period + 1
        )
        self._prev_roc: Optional[float] = None

        logger.info(
            f"MomentumFactor initialized "
            f"roc_period={self.roc_period} "
            f"roc_weight={self.roc_weight} "
            f"accel_weight={self.accel_weight} "
            f"quantile_window={self.quantile_window} "
            f"warmup_bars={self.warmup_bars}"
        )

    # ========================================================================
    # 核心：计算
    # ========================================================================

    def compute(
        self,
        bar: Any,
        snapshot: Optional[dict] = None,
    ) -> FactorValue:
        """
        计算动量因子值。

        契约：
        - 内部异常包装成 FactorValue(state=ERROR)
        - 历史不足时返回 WARMUP
        - snapshot 参数不使用（动量不需要盘口），保留接口一致性
        - 前 roc_period 根 K 线的值不进入 _raw_history 分位数窗口
          （直接返回 WARMUP，不调 _make_value 的追加路径）
        """
        t0 = time.perf_counter()
        try:
            # 获取收盘价（Decimal，来自 KlineData）
            close = getattr(bar, "close", None)
            if close is None:
                return self._make_value(
                    0.0, getattr(bar, "ts", 0),
                    error="bar has no close field",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # 追加收盘价到历史
            self._close_history.append(close)

            # 历史不足：需要至少 roc_period + 1 个收盘价
            if len(self._close_history) < self.roc_period + 1:
                return self._make_value(
                    0.0, bar.ts,
                    state=FactorState.WARMUP,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # 计算 ROC
            close_now = self._close_history[-1]
            close_past = self._close_history[-(self.roc_period + 1)]

            # 除零保护（close_past 应 > 0，防御性检查）
            if close_past <= 0:
                logger.warning(
                    f"non-positive close_past: {close_past}, "
                    f"returning 0.0"
                )
                return self._make_value(
                    0.0, bar.ts,
                    state=FactorState.READY,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # ROC 用 Decimal 减法，一次性转 float
            roc = float((close_now - close_past) / close_past)

            # 计算加速度
            if self._prev_roc is not None:
                accel = roc - self._prev_roc
            else:
                accel = 0.0
            self._prev_roc = roc

            # 合成原始值
            raw = self.roc_weight * roc + self.accel_weight * accel

            compute_ms = (time.perf_counter() - t0) * 1000
            return self._make_value(raw, bar.ts, compute_ms=compute_ms)

        except Exception as e:
            logger.error(
                f"MomentumFactor.compute error: {e}",
                exc_info=True,
            )
            return self._make_value(
                0.0,
                getattr(bar, "ts", 0),
                error=f"{type(e).__name__}: {e}",
                compute_ms=(time.perf_counter() - t0) * 1000,
            )

    # ========================================================================
    # 重置
    # ========================================================================

    def reset(self) -> None:
        """重置因子状态。测试和热重启时使用。"""
        super().reset()
        self._close_history.clear()
        self._prev_roc = None
        logger.info(f"MomentumFactor reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> MomentumFactor:
    """模块工厂入口。"""
    return MomentumFactor(config)
