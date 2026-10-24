"""
factors/factor_registry.py — 因子注册表（批量计算 + 权重归一化 + 错误隔离）

职责：
1. 注册因子（name -> FactorProvider）
2. 批量计算所有因子（错误隔离，每个因子独立 try/except）
3. 权重归一化（部分因子不可用时，可用因子按比例重归一化）
4. 预热状态管理（所有因子 READY 才算 ready）
5. 计算耗时监控（单因子超时打 WARN）
6. 聚合健康检查

职责边界：
- FactorProvider：单个因子的计算逻辑
- FactorRegistry：因子集合的编排
- Strategy：用 FactorValue 和权重算 trend_score
- AlphaMonitor：从持久化数据算 IC

关键契约：
1. 注册同名因子抛 ValueError
2. compute_all 按注册顺序计算，保证可复现
3. 每个因子独立 try/except，异常包装成 ERROR 状态的 FactorValue
4. 因子返回值必须是 FactorValue，否则包装成 ERROR
5. 因子返回值 name 必须与注册名一致，否则重写
6. get_weights(usable_only=True) 返回归一化权重，和恒为 1（或空 dict）
7. 所有因子可用时才 is_ready() 返回 True
8. 单因子耗时 > max_compute_ms 打 WARN，不拦截结果
9. process_bar() 组合 on_bar_close + compute_all，调用方只需调一次
10. 只 catch Exception，不 catch BaseException

权重归一化算法：
    原始权重：{"momentum": 0.35, "obi": 0.40, "volume": 0.25}
    所有因子可用 → 归一化后不变（和已经为 1）

    场景 1：OBI DEGRADED
    可用因子：momentum 0.35, volume 0.25
    原始和 = 0.35 + 0.25 = 0.60
    归一化：momentum = 0.35/0.60 ≈ 0.5833, volume = 0.25/0.60 ≈ 0.4167

    场景 2：所有因子 DEGRADED
    返回空 dict

    场景 3：权重和 = 0（config 未配权重）
    返回空 dict，调用方应视为配置错误

职责链：
    main.py
      → registry.process_bar(bar, snapshot)
          → registry.on_bar_close(bar)        # 推进预热状态
          → registry.compute_all(bar, snap)   # 计算因子值
      → strategy.compute(trend_score)         # 策略层加权
          → weights = registry.get_weights()
          → score = sum(fv.value * weights[name])

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.factor import FactorProvider, FactorState, FactorValue

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "factor_registry",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["factor_registry", "factor_aggregation"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Factor registry with batch compute, weight normalization, and error isolation",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_COMPUTE_MS = 5.0
WEIGHT_SUM_TOLERANCE = 1e-6
WEIGHT_SUM_MIN = 1e-9


# ============================================================================
# FactorRegistry
# ============================================================================
class FactorRegistry:
    """
    因子注册表。详见模块 docstring。

    用法：
        registry = FactorRegistry(config)
        registry.register("momentum", MomentumFactor(config), weight=0.35)
        registry.register("obi", OBIFactor(config), weight=0.40)
        registry.register("volume", VolumeFactor(config), weight=0.25)

        # 每根 K 线收线
        factors = registry.process_bar(bar, snapshot)
        # -> {"momentum": FactorValue, "obi": FactorValue, "volume": FactorValue}

        # 策略层加权
        weights = registry.get_weights()
        score = sum(
            fv.value * weights[name]
            for name, fv in factors.items()
            if fv.is_usable()
        )

        # 健康检查
        registry.health()
    """

    name = "factor_registry"
    version = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        cfg = config.get("factors", {})

        self._max_compute_ms = float(
            cfg.get("max_compute_ms", DEFAULT_MAX_COMPUTE_MS)
        )

        # ---- 内部状态 ----
        self._lock = threading.RLock()
        self._factors: Dict[str, FactorProvider] = {}
        self._weights: Dict[str, float] = {}
        self._registration_order: List[str] = []

        # ---- 统计 ----
        self._stats = {
            "computes": 0,
            "errors": 0,
            "slow_computes": 0,
            "weight_normalizations": 0,
        }

        logger.info(
            f"FactorRegistry initialized "
            f"max_compute_ms={self._max_compute_ms}"
        )

    # ========================================================================
    # 注册
    # ========================================================================

    def register(
        self,
        name: str,
        factor: FactorProvider,
        weight: float = 0.0,
    ) -> None:
        """
        注册因子。

        Args:
            name: 因子名（与 MANIFEST.name 一致）
            factor: FactorProvider 实例
            weight: 策略层权重（>= 0）

        契约：
        - 同名重复注册抛 ValueError
        - 权重为负抛 ValueError
        - 因子实例为 None 抛 ValueError
        """
        if not name:
            raise ValueError("factor name is required")
        if factor is None:
            raise ValueError(f"factor '{name}' is None")
        if not isinstance(factor, FactorProvider):
            raise ValueError(
                f"factor '{name}' must be FactorProvider, "
                f"got {type(factor).__name__}"
            )
        if weight < 0:
            raise ValueError(
                f"weight must be >= 0, got {weight} for '{name}'"
            )

        with self._lock:
            if name in self._factors:
                raise ValueError(
                    f"factor '{name}' already registered"
                )
            self._factors[name] = factor
            self._weights[name] = float(weight)
            self._registration_order.append(name)

        logger.info(
            f"factor registered: name={name} "
            f"version={factor.version} weight={weight}"
        )

    def get(self, name: str) -> Optional[FactorProvider]:
        """按名字查因子。不存在返回 None。"""
        with self._lock:
            return self._factors.get(name)

    def names(self) -> List[str]:
        """返回所有因子的名字（按注册顺序）。"""
        with self._lock:
            return list(self._registration_order)

    def __len__(self) -> int:
        with self._lock:
            return len(self._factors)

    # ========================================================================
    # 批量计算
    # ========================================================================

    def process_bar(
        self,
        bar: Any,
        snapshot: Optional[dict] = None,
    ) -> Dict[str, FactorValue]:
        """
        组合方法：推进预热状态 + 计算所有因子。

        调用方只需调这一个方法，无需手动调 on_bar_close。

        Returns:
            Dict[name -> FactorValue]，按注册顺序
        """
        self.on_bar_close(bar)
        return self.compute_all(bar, snapshot)

    def on_bar_close(self, bar: Any) -> None:
        """
        每根 K 线收线时推进所有因子的预热状态。

        契约：
        - 每个因子独立 try/except
        - 异常打 ERROR，不影响其他因子
        """
        with self._lock:
            factors_snapshot = list(self._factors.items())

        for name, factor in factors_snapshot:
            try:
                factor.on_bar_close(bar)
            except Exception as e:
                logger.error(
                    f"factor '{name}' on_bar_close error: {e}",
                    exc_info=True,
                )

    def compute_all(
        self,
        bar: Any,
        snapshot: Optional[dict] = None,
    ) -> Dict[str, FactorValue]:
        """
        计算所有因子。

        契约：
        - 每个因子独立 try/except
        - 因子抛异常 → 包装成 ERROR 状态的 FactorValue
        - 因子返回非 FactorValue → 包装成 ERROR
        - 因子返回的 name 与注册名不一致 → 重写为注册名
        - 单因子耗时 > max_compute_ms → WARN，不拦截结果
        - 返回顺序按注册顺序

        Returns:
            Dict[name -> FactorValue]
        """
        with self._lock:
            factors_snapshot = list(self._factors.items())
            self._stats["computes"] += 1

        result: Dict[str, FactorValue] = {}

        for name, factor in factors_snapshot:
            t0 = time.perf_counter()
            try:
                fv = factor.compute(bar, snapshot)
                if not isinstance(fv, FactorValue):
                    self._stats["errors"] += 1
                    fv = self._make_error_value(
                        name,
                        bar,
                        f"compute returned {type(fv).__name__}, "
                        f"expected FactorValue",
                        (time.perf_counter() - t0) * 1000,
                    )
                elif fv.name != name:
                    # name 不一致：重写
                    self._stats["errors"] += 1
                    logger.warning(
                        f"factor '{name}' returned value with "
                        f"name='{fv.name}', rewriting to '{name}'"
                    )
                    fv = FactorValue(
                        name=name,
                        value=fv.value,
                        raw_value=fv.raw_value,
                        bar_ts=fv.bar_ts,
                        state=fv.state,
                        version=fv.version,
                        outlier=fv.outlier,
                        stale=fv.stale,
                        compute_ms=fv.compute_ms,
                        error=fv.error,
                    )
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(
                    f"factor '{name}' compute error: {e}",
                    exc_info=True,
                )
                fv = self._make_error_value(
                    name,
                    bar,
                    f"{type(e).__name__}: {e}",
                    (time.perf_counter() - t0) * 1000,
                )

            result[name] = fv

            # 耗时监控
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if elapsed_ms > self._max_compute_ms:
                self._stats["slow_computes"] += 1
                logger.warning(
                    f"factor '{name}' slow: {elapsed_ms:.2f}ms > "
                    f"{self._max_compute_ms}ms"
                )

        return result

    def _make_error_value(
        self,
        name: str,
        bar: Any,
        error: str,
        compute_ms: float,
    ) -> FactorValue:
        """构造 ERROR 状态的 FactorValue。"""
        return FactorValue(
            name=name,
            value=0.0,
            raw_value=0.0,
            bar_ts=getattr(bar, "ts", 0),
            state=FactorState.ERROR,
            version="unknown",
            outlier=False,
            stale=False,
            compute_ms=compute_ms,
            error=error,
        )

    # ========================================================================
    # 权重
    # ========================================================================

    def get_weights(
        self, usable_only: bool = True
    ) -> Dict[str, float]:
        """
        返回归一化后的权重。

        Args:
            usable_only: True 时只返回可用因子，权重和重归一化为 1
                         False 时返回所有因子，权重和保持原始值

        Returns:
            Dict[name -> weight]

        契约：
        - 权重和为空/0 → 返回空 dict
        - 部分因子不可用时，可用因子按原比例重归一化到 1
        - 归一化后总和在 [1-1e-6, 1+1e-6]
        """
        with self._lock:
            if usable_only:
                items = [
                    (name, self._weights[name], self._factors[name])
                    for name in self._registration_order
                    if self._is_factor_usable(self._factors[name])
                ]
            else:
                items = [
                    (name, self._weights[name], self._factors[name])
                    for name in self._registration_order
                ]

            if not items:
                return {}

            total = sum(w for _, w, _ in items)
            if total < WEIGHT_SUM_MIN:
                logger.warning(
                    f"weight sum near zero ({total:.2e}), "
                    f"returning empty weights"
                )
                return {}

            self._stats["weight_normalizations"] += 1
            normalized = {
                name: w / total for name, w, _ in items
            }

            # 浮点误差校验
            final_sum = sum(normalized.values())
            if abs(final_sum - 1.0) > WEIGHT_SUM_TOLERANCE:
                logger.warning(
                    f"normalized weights sum to {final_sum:.8f} "
                    f"(expected 1.0)"
                )

            return normalized

    def get_raw_weight(self, name: str) -> Optional[float]:
        """返回因子的原始权重（未归一化）。"""
        with self._lock:
            return self._weights.get(name)

    def _is_factor_usable(self, factor: FactorProvider) -> bool:
        """
        判断因子是否可用。

        可用条件：
        - state == READY
        - 最近一次 FactorValue 存在且 is_usable()

        注意：直接访问 factor 的私有属性（_state / _last_value），
        是 registry 和 FactorProvider 之间的内部契约。
        Step 2 会在 FactorProvider 上加 is_ready() 方法，届时替换。
        """
        state = getattr(factor, "_state", None)
        if state != FactorState.READY:
            return False
        last_value = getattr(factor, "_last_value", None)
        if last_value is None:
            return False
        return last_value.is_usable()

    # ========================================================================
    # 状态查询
    # ========================================================================

    def is_ready(self) -> bool:
        """所有因子都 READY 时才返回 True。"""
        with self._lock:
            factors = list(self._factors.values())
        if not factors:
            return False
        return all(
            getattr(f, "_state", None) == FactorState.READY
            for f in factors
        )

    def get_usable_names(self) -> List[str]:
        """返回可用因子的名字列表。"""
        with self._lock:
            return [
                name for name in self._registration_order
                if self._is_factor_usable(self._factors[name])
            ]

    def get_status(self) -> Dict[str, str]:
        """返回所有因子的状态。"""
        with self._lock:
            return {
                name: getattr(
                    self._factors[name], "_state", FactorState.WARMUP
                ).value
                for name in self._registration_order
            }

    # ========================================================================
    # 重置与健康
    # ========================================================================

    def reset(self) -> None:
        """重置 registry 状态。不重置因子本身。"""
        with self._lock:
            for k in self._stats:
                self._stats[k] = 0
        logger.info("FactorRegistry reset (factors not reset)")

    def health(self) -> Dict[str, Any]:
        """聚合健康检查。"""
        with self._lock:
            factors = list(self._factors.items())
            stats_snapshot = dict(self._stats)

        factor_health = {}
        for name, factor in factors:
            try:
                factor_health[name] = factor.health()
            except Exception as e:
                factor_health[name] = {
                    "healthy": False,
                    "error": str(e),
                }

        usable_count = sum(
            1 for name in self._registration_order
            if self._is_factor_usable(self._factors[name])
        )
        total_count = len(self._registration_order)

        return {
            "healthy": total_count > 0 and usable_count > 0,
            "name": self.name,
            "version": self.version,
            "factor_count": total_count,
            "usable_count": usable_count,
            "usable_rate": (
                usable_count / total_count if total_count > 0 else 0.0
            ),
            "is_ready": self.is_ready(),
            "status": self.get_status(),
            "weights_raw": dict(self._weights),
            "weights_normalized": self.get_weights(usable_only=True),
            "factors": factor_health,
            "stats": stats_snapshot,
            "timestamp": int(time.time() * 1000),
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> FactorRegistry:
    """
    模块工厂入口。

    注意：因子的注册需要 main.py 显式调用 registry.register()，
    因为因子实例需要从 config 创建，权重也需要从 config 读取。
    create() 只返回空的 registry。
    """
    return FactorRegistry(config)
