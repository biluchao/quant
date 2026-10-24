"""
factors/factor_volume.py — 成交量因子（相对成交量分位数）

因子定义（见 FACTOR_SPEC.md）：
    ratio = volume[t] / mean(volume[t-ma_window : t])
    归一化：滚动分位数归一化（窗口 quantile_window=500），映射到 [0, 1]

经济学解释：
    成交量因子衡量当前 K 线成交量相对于近期均值的放大程度。放量意味着
    市场参与度提升，价格运动更可信。缩量意味着参与度下降，价格运动
    可能是噪声。成交量因子本身没有方向，只衡量强度，因此归一化到 [0, 1]。

关键契约：
1. ma_window 是"历史窗口"，不含当前 K 线
   - 计算 ratio 时用 mean(volume[t-N : t])，即前 N 根的均值
   - 当前 K 线的 volume 只在算完 ratio 之后才加入历史窗口
   - 如果含当前，会陷入"自相关陷阱"：当前量越大，均值越大，ratio 越接近 1
2. 归一化区间 [0, 1]：
   - 0.5 是分位数中位数，不是算术平均值
   - > 0.5 表示相对历史处于较高分位（放量）
   - < 0.5 表示相对历史处于较低分位（缩量）
3. 因子无方向：
   - 不区分阳线放量还是阴线放量
   - 不判断放量是健康还是衰竭
   - 这些是策略层的判断，因子只回答"放大了多少倍"
4. 不平滑：
   - volume 本身是聚合量，已足够平滑
   - 时间平滑会延迟信号，不适合
5. 预热期：
   - 前 ma_window 根 K 线，历史不足
   - 返回 WARMUP，不进 _raw_history 分位数窗口
6. 除零保护：
   - mean(volume) < EPSILON 时返回 0.5（中性）
   - 单根 volume = 0 正常处理（ratio = 0）
   - 单根 volume < 0 异常（防御性，理论上不会发生）

参数敏感性分析（见 FACTOR_SPEC.md）：
    ma_window=10:  假信号率 14.8%，IC 均值 0.08
    ma_window=20:  假信号率 13.7%，IC 均值 0.10 ← 默认
    ma_window=50:  假信号率 13.2%，IC 均值 0.09

已知限制：
1. 成交量因子本身没有方向，只衡量强度，需要和其他因子组合使用
2. 资金费率结算前后成交量异常放大，因子值偏高但不可信
3. 交易所维护或网络异常导致成交量数据缺失时，沿用上一根值
4. 极端行情（闪崩）下 volume 暴增，可能触发 5σ 异常值检测

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
    "name": "volume",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["volume", "volume_percentile", "relative_volume"],
    "module_dependencies": [],
    "normalized_range": (0.0, 1.0),
    "fallback": None,
    "enabled": True,
    "description": "Relative volume factor: current volume vs recent N-bar mean",
}


# ============================================================================
# 默认参数（可被 config 覆盖）
# ============================================================================
DEFAULT_MA_WINDOW = 20
NEUTRAL_VALUE = 0.5
EPSILON = 1e-9


# ============================================================================
# VolumeFactor
# ============================================================================
class VolumeFactor(FactorProvider):
    """
    成交量因子。详见模块 docstring。

    用法：
        factor = VolumeFactor(config)
        registry.register("volume", factor)

        # 每根 K 线收线时（顺序由状态机保证）
        factor.on_bar_close(bar)             # 基类：更新 bars_seen
        fv = factor.compute(bar, snapshot)   # snapshot 不使用
    """

    name = "volume"
    version = "1.0.0"
    normalized_range = (0.0, 1.0)

    def __init__(self, config: dict):
        super().__init__(config)

        # ---- 提取因子专属参数 ----
        factor_cfg = self._extract_factor_config(config)
        self.ma_window = int(
            factor_cfg.get("ma_window", DEFAULT_MA_WINDOW)
        )

        # ---- 参数校验 ----
        if self.ma_window < 2:
            raise ValueError(
                f"ma_window must be >= 2, got {self.ma_window}"
            )

        # ---- 内部状态 ----
        # _volume_history 保存最近 ma_window 根 K 线的成交量
        # 不含当前 K 线（当前 K 线在 compute 中读取，算完 ratio 后再追加）
        self._volume_history: deque = deque(maxlen=self.ma_window)
        self._last_ratio: Optional[float] = None

        # ---- 统计 ----
        self._stats = {
            "computes": 0,
            "warmup": 0,
            "errors": 0,
            "zero_mean": 0,
            "zero_volume": 0,
            "negative_volume": 0,
        }

        logger.info(
            f"VolumeFactor initialized "
            f"ma_window={self.ma_window} "
            f"quantile_window={self.quantile_window} "
            f"warmup_bars={self.warmup_bars} "
            f"normalized_range={self.normalized_range}"
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
        计算成交量因子值。

        契约：
        - 用 [t-ma_window, t-1] 的历史均值算 ratio
        - 算完 ratio 后，把当前 volume 加入历史窗口
        - 历史不足 ma_window 时返回 WARMUP（不追加入 _raw_history）
        - snapshot 参数不使用（volume 只依赖 K 线）
        """
        t0 = time.perf_counter()

        try:
            bar_ts = getattr(bar, "ts", 0)
            volume = getattr(bar, "volume", None)

            if volume is None:
                self._stats["errors"] += 1
                return self._make_value(
                    0.0, bar_ts,
                    error="bar has no volume field",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # Decimal → float 用于计算
            try:
                volume_f = float(volume)
            except (TypeError, ValueError):
                self._stats["errors"] += 1
                return self._make_value(
                    0.0, bar_ts,
                    error=f"invalid volume type: {type(volume).__name__}",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # 负成交量：防御性检查，理论上不会出现
            if volume_f < 0:
                self._stats["negative_volume"] += 1
                logger.warning(
                    f"VolumeFactor: negative volume {volume_f} "
                    f"at ts={bar_ts}"
                )
                return self._make_value(
                    0.0, bar_ts,
                    error=f"negative volume: {volume_f}",
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # 零成交量：正常处理，但记录统计
            if volume_f == 0:
                self._stats["zero_volume"] += 1

            # ---- 1. 预热期检查 ----
            if len(self._volume_history) < self.ma_window:
                self._stats["warmup"] += 1
                # 把当前 volume 加入历史（用于后续计算）
                self._volume_history.append(volume_f)
                return self._make_value(
                    0.0, bar_ts,
                    state=FactorState.WARMUP,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # ---- 2. 计算历史均值（不含当前 K 线）----
            # _volume_history 长度为 ma_window，正好是 [t-N, t-1]
            hist_mean = (
                sum(self._volume_history) / self.ma_window
            )

            # ---- 3. 除零保护 ----
            if hist_mean < EPSILON:
                # 历史均值接近 0（连续多根零成交量，异常情况）
                self._stats["zero_mean"] += 1
                logger.warning(
                    f"VolumeFactor: mean volume near zero "
                    f"({hist_mean:.2e}) at ts={bar_ts}, "
                    f"returning neutral"
                )
                # 追加当前量后返回中性值
                self._volume_history.append(volume_f)
                return self._make_value(
                    NEUTRAL_VALUE, bar_ts,
                    state=FactorState.READY,
                    compute_ms=(time.perf_counter() - t0) * 1000,
                )

            # ---- 4. 计算相对成交量 ----
            ratio = volume_f / hist_mean

            # 更新历史窗口（追加当前量，deque 自动淘汰最旧）
            self._volume_history.append(volume_f)
            self._last_ratio = ratio

            self._stats["computes"] += 1

            # ---- 5. 归一化 + 构造输出 ----
            compute_ms = (time.perf_counter() - t0) * 1000
            return self._make_value(ratio, bar_ts, compute_ms=compute_ms)

        except Exception as e:
            logger.error(
                f"VolumeFactor.compute error: {e}",
                exc_info=True,
            )
            self._stats["errors"] += 1
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
        """重置因子状态。"""
        super().reset()
        self._volume_history.clear()
        self._last_ratio = None
        for k in self._stats:
            self._stats[k] = 0
        logger.info(f"VolumeFactor reset")

    # ========================================================================
    # 健康检查扩展
    # ========================================================================

    def health(self) -> dict:
        """健康检查（扩展基类）。"""
        base = super().health()
        base.update({
            "ma_window": self.ma_window,
            "volume_history_size": len(self._volume_history),
            "last_ratio": self._last_ratio,
            "stats": dict(self._stats),
        })
        return base


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> VolumeFactor:
    """模块工厂入口。"""
    return VolumeFactor(config)
