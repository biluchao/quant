"""
core/factor.py — 因子抽象基类与通用数据结构

所有因子必须：
1. 继承 FactorProvider
2. 在 __init__ 中设置 name / version / normalized_range
3. 实现 compute(bar, snapshot) -> FactorValue
4. 模块顶部提供 MANIFEST 字典
5. 模块提供 create(config) -> FactorProvider 工厂函数

因子输出统一为 FactorValue，包含：
- value（归一化后的值，按 normalized_range 夹紧）
- raw_value（原始值，用于调试和异常值检查）
- state（WARMUP / READY / STALE / DEGRADED / ERROR）
- outlier / stale 标记
- bar_ts（对应的 K 线收线时间戳，交易所时间，毫秒）
- version（因子版本）
- compute_ms（计算耗时，毫秒）

契约：
- 因子内部异常必须包装成 FactorValue(state=ERROR)，不得向上抛
- 因子 compute() 内禁止 I/O（网络、磁盘）
- 因子只能修改自身实例状态，不得修改全局状态
- 因子不感知资金费率结算，降频由策略层统一处理

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Dict, Optional, Tuple
import logging

import numpy as np

logger = logging.getLogger(__name__)


# ============================================================================
# 默认值（可被 config 覆盖）
# ============================================================================
DEFAULT_QUANTILE_WINDOW = 500
DEFAULT_WARMUP_BARS = 500
DEFAULT_OUTPUT_PRECISION = 6
DEFAULT_OUTLIER_SIGMA = 5.0
DEFAULT_MAX_COMPUTE_MS = 5.0
DEFAULT_FALLBACK_VALUE = 0.0
DEGRADE_THRESHOLD = 3  # 连续 3 次 ERROR 进入 DEGRADED


# ============================================================================
# 因子状态
# ============================================================================
class FactorState(str, Enum):
    """因子生命周期状态。"""
    WARMUP = "warmup"       # 预热期，bars_seen < warmup_bars
    READY = "ready"         # 正常可用
    STALE = "stale"         # 本次计算使用了过期数据（快照太旧）
    DEGRADED = "degraded"   # 连续 DEGRADE_THRESHOLD 次 ERROR，自动降级
    ERROR = "error"         # 本次计算失败，返回上一根值或 fallback


# ============================================================================
# 因子输出值
# ============================================================================
@dataclass(frozen=True)
class FactorValue:
    """
    因子输出值。不可变。

    策略层只读此结构，不关心因子内部实现。
    """
    name: str                       # 因子名（与 MANIFEST.name 一致）
    value: float                    # 归一化后的值，按 normalized_range 夹紧
    raw_value: float                # 原始值（未归一化），用于调试和异常值检查
    bar_ts: int                     # 对应的 K 线收线时间戳（毫秒）
    state: FactorState              # 计算时的状态
    version: str                    # 因子版本
    outlier: bool = False           # 是否被标记为异常值（5σ）
    stale: bool = False             # 是否使用了过期数据
    compute_ms: float = 0.0         # 计算耗时（毫秒）
    error: Optional[str] = None     # 错误信息（state=ERROR 时）

    def is_usable(self) -> bool:
        """
        策略层可用的因子值：
        - state == READY
        - 非异常值
        - 非过期数据
        """
        return (
            self.state == FactorState.READY
            and not self.outlier
            and not self.stale
        )


# ============================================================================
# 因子 MANIFEST
# ============================================================================
@dataclass(frozen=True)
class FactorManifest:
    """
    因子 MANIFEST 字典解析后的结构。
    每个因子模块顶部的 MANIFEST 字典通过 from_dict() 解析为此结构。
    """
    name: str
    type: str
    version: str
    step: int
    capabilities: Tuple[str, ...]
    dependencies: Tuple[str, ...]
    normalized_range: Tuple[float, float]
    owner: str
    fallback: Optional[str] = None
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "FactorManifest":
        """强校验：必填字段缺失或类型不对，抛 ValueError。"""
        required = ("name", "type", "version", "step", "owner")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"MANIFEST missing fields: {missing}")

        if d["type"] != "factor":
            raise ValueError(
                f"MANIFEST type must be 'factor', got '{d['type']}'"
            )

        normalized_range = tuple(d.get("normalized_range", (-1.0, 1.0)))
        if normalized_range not in ((-1.0, 1.0), (0.0, 1.0)):
            raise ValueError(
                f"normalized_range must be (-1.0,1.0) or (0.0,1.0), "
                f"got {normalized_range}"
            )

        return cls(
            name=d["name"],
            type=d["type"],
            version=str(d["version"]),
            step=int(d["step"]),
            capabilities=tuple(d.get("capabilities", [])),
            dependencies=tuple(d.get("dependencies", [])),
            normalized_range=normalized_range,
            owner=d["owner"],
            fallback=d.get("fallback"),
            enabled=bool(d.get("enabled", True)),
        )


# ============================================================================
# 因子抽象基类
# ============================================================================
class FactorProvider(ABC):
    """
    因子抽象基类。详见模块 docstring。

    子类必须：
    1. 在 __init__ 中设置 self.name / self.version / self.normalized_range
    2. 实现 compute(bar, snapshot) -> FactorValue

    子类可选覆盖：
    - on_bar_close(bar)：每根 K 线收线后的钩子（更新窗口等）
    - health()：自定义健康检查（默认实现已够用）

    子类不应覆盖：
    - reset()：重置逻辑固定
    - _quantile_normalize() / _is_outlier() / _make_value()：通用逻辑固定
    """

    # 子类必须在 __init__ 中设置以下属性
    name: str = ""
    version: str = "1.0.0"
    normalized_range: Tuple[float, float] = (-1.0, 1.0)

    def __init__(self, config: dict):
        self.config = config

        # 通用配置：优先从 config["factors"][self.name] 读，其次 config["factors"]，
        # 最后全局默认值
        factor_cfg = self._extract_factor_config(config)

        self.quantile_window = int(
            factor_cfg.get("quantile_window", DEFAULT_QUANTILE_WINDOW)
        )
        self.warmup_bars = int(
            factor_cfg.get("warmup_bars", DEFAULT_WARMUP_BARS)
        )
        self.output_precision = int(
            factor_cfg.get("output_precision", DEFAULT_OUTPUT_PRECISION)
        )
        self.outlier_sigma = float(
            factor_cfg.get("outlier_sigma", DEFAULT_OUTLIER_SIGMA)
        )
        self.max_compute_ms = float(
            factor_cfg.get("max_compute_ms", DEFAULT_MAX_COMPUTE_MS)
        )
        self.fallback_value = float(
            factor_cfg.get("fallback_value", DEFAULT_FALLBACK_VALUE)
        )

        # 运行状态
        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = FactorState.WARMUP
        self._last_value: Optional[FactorValue] = None
        self._raw_history: Deque[float] = deque(maxlen=self.quantile_window)

    def _extract_factor_config(self, config: dict) -> dict:
        """从 config 中提取本因子的配置段。"""
        factors_cfg = config.get("factors", {})
        if not isinstance(factors_cfg, dict):
            return {}
        # 优先读 factors.{self.name}
        if self.name and self.name in factors_cfg:
            section = factors_cfg[self.name]
            if isinstance(section, dict):
                # 合并通用配置
                merged = {k: v for k, v in factors_cfg.items() if not isinstance(v, dict)}
                merged.update(section)
                return merged
        # 其次读 factors 顶层通用配置
        return {
            k: v for k, v in factors_cfg.items()
            if not isinstance(v, dict)
        }

    # ------------------------------------------------------------------------
    # 抽象方法：子类必须实现
    # ------------------------------------------------------------------------

    @abstractmethod
    def compute(self, bar, snapshot: Optional[dict]) -> FactorValue:
        """
        计算因子值。每根 K 线收线后调用一次。

        Args:
            bar: 已收线的 K 线，必须含 ts/close/open/high/low/volume
            snapshot: 盘口快照，可能为 None（快照缺失或过期）

        Returns:
            FactorValue

        契约：
        - 内部异常不得向上抛，必须包装成 FactorValue(state=ERROR)
        - 数据不足（预热期）返回 FactorValue(state=WARMUP)
        - 使用 self._make_value() 构造输出，自动处理归一化、异常值、状态追踪
        - 禁止 I/O 操作
        - 禁止修改全局状态

        正确写法示例：
            def compute(self, bar, snapshot):
                import time
                t0 = time.perf_counter()
                try:
                    raw = self._compute_raw(bar, snapshot)
                    if raw is None:
                        return self._make_value(0.0, bar.ts,
                                                state=FactorState.WARMUP)
                    compute_ms = (time.perf_counter() - t0) * 1000
                    return self._make_value(raw, bar.ts, compute_ms=compute_ms)
                except Exception as e:
                    return self._make_value(0.0, bar.ts, error=str(e))
        """
        ...

    # ------------------------------------------------------------------------
    # 可选钩子
    # ------------------------------------------------------------------------

    def on_bar_close(self, bar) -> None:
        """
        每根 K 线收线后调用，用于更新内部窗口/计数。
        由调用方（FactorRegistry）在 compute() 之前调用。

        默认实现：递增 bars_seen，预热完成时切换 WARMUP → READY。
        """
        self._bars_seen += 1
        if (
            self._state == FactorState.WARMUP
            and self._bars_seen >= self.warmup_bars
        ):
            self._state = FactorState.READY
            logger.info(
                f"factor {self.name} warmup complete after "
                f"{self._bars_seen} bars"
            )

    def health(self) -> Dict[str, Any]:
        """健康检查。监控系统定期调用。"""
        return {
            "name": self.name,
            "version": self.version,
            "state": self._state.value,
            "bars_seen": self._bars_seen,
            "warmup_progress": min(
                1.0, self._bars_seen / max(self.warmup_bars, 1)
            ),
            "consecutive_errors": self._consecutive_errors,
            "raw_history_size": len(self._raw_history),
            "quantile_window": self.quantile_window,
            "last_value_ts": (
                self._last_value.bar_ts if self._last_value else None
            ),
        }

    def reset(self) -> None:
        """重置因子状态。测试和热重启时使用。"""
        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = FactorState.WARMUP
        self._last_value = None
        self._raw_history.clear()
        logger.info(f"factor {self.name} reset")

    # ------------------------------------------------------------------------
    # 辅助方法：子类在 compute() 中调用
    # ------------------------------------------------------------------------

    def _quantile_normalize(self, raw: float) -> float:
        """
        基于滚动分位数归一化。

        关键契约：归一化时使用的历史窗口不包含当前值，
        归一化完成后再将当前值追加到窗口（防前视偏差）。
        """
        if not np.isfinite(raw):
            return self.fallback_value

        # 窗口未填满：用当前历史算分位数（可能不稳定），返回 0（中性）
        if len(self._raw_history) < 2:
            self._raw_history.append(raw)
            return 0.0

        # 用已有历史算分位数（不含当前值）
        arr = np.asarray(self._raw_history, dtype=float)
        p = float(np.mean(arr <= raw))  # 经验分位数，0 到 1

        # 按 normalized_range 映射
        lo, hi = self.normalized_range
        if lo == -1.0 and hi == 1.0:
            normalized = 2.0 * p - 1.0
        else:
            normalized = p

        # 追加当前值（归一化之后）
        self._raw_history.append(raw)
        return normalized

    def _is_outlier(self, raw: float) -> bool:
        """
        5σ 异常值检测。

        关键契约：检查时使用的历史窗口不包含当前值。
        必须在 _quantile_normalize() 之前调用。
        """
        if len(self._raw_history) < 30:
            return False  # 样本太少，不判定
        arr = np.asarray(self._raw_history, dtype=float)
        std = float(np.std(arr))
        if std < 1e-12:
            return False
        mean = float(np.mean(arr))
        return abs(raw - mean) > self.outlier_sigma * std

    def _make_value(
        self,
        raw: float,
        bar_ts: int,
        *,
        state: Optional[FactorState] = None,
        stale: bool = False,
        error: Optional[str] = None,
        compute_ms: float = 0.0,
    ) -> FactorValue:
        """
        构造 FactorValue。子类在 compute() 中调用此方法生成输出。

        内部自动处理：
        - 异常值检查（在归一化之前）
        - 分位数归一化（历史窗口不含当前值）
        - 精度舍入
        - 区间夹紧
        - 状态追踪（成功/失败计数、DEGRADED 切换）
        - 计算耗时告警
        """
        # 错误路径：构造 ERROR 状态，不追加到历史，不调 _track_success
        if error is not None:
            fv = FactorValue(
                name=self.name,
                value=self.fallback_value,
                raw_value=round(raw, self.output_precision),
                bar_ts=bar_ts,
                state=FactorState.ERROR,
                version=self.version,
                stale=stale,
                compute_ms=compute_ms,
                error=error,
            )
            self._track_error(error)
            return fv

        # 正常路径：先检查异常值，再归一化
        outlier = self._is_outlier(raw)
        normalized = self._quantile_normalize(raw)
        normalized = round(normalized, self.output_precision)

        # 区间夹紧（防止浮点误差越界）
        lo, hi = self.normalized_range
        normalized = max(lo, min(hi, normalized))

        # 状态判定：默认用当前状态；DEGRADED 状态下本次成功 → 恢复 READY
        if state is None:
            state = self._state
            if state == FactorState.DEGRADED:
                state = FactorState.READY

        fv = FactorValue(
            name=self.name,
            value=normalized,
            raw_value=round(raw, self.output_precision),
            bar_ts=bar_ts,
            state=state,
            version=self.version,
            outlier=outlier,
            stale=stale,
            compute_ms=compute_ms,
        )
        self._track_success(fv)
        return fv

    # ------------------------------------------------------------------------
    # 内部状态追踪
    # ------------------------------------------------------------------------

    def _track_error(self, error: str) -> None:
        """连续错误计数，达到阈值切换 DEGRADED。"""
        self._consecutive_errors += 1
        if self._consecutive_errors >= DEGRADE_THRESHOLD:
            if self._state != FactorState.DEGRADED:
                logger.warning(
                    f"factor {self.name} DEGRADED after "
                    f"{self._consecutive_errors} consecutive errors: {error}"
                )
            self._state = FactorState.DEGRADED

    def _track_success(self, fv: FactorValue) -> None:
        """成功计算：重置错误计数，DEGRADED 恢复 READY，记录耗时告警。"""
        self._consecutive_errors = 0
        if self._state == FactorState.DEGRADED:
            self._state = FactorState.READY
            logger.info(f"factor {self.name} recovered from DEGRADED")
        self._last_value = fv
        if fv.compute_ms > self.max_compute_ms:
            logger.warning(
                f"factor {self.name} slow: "
                f"{fv.compute_ms:.1f}ms > {self.max_compute_ms}ms"
            )


# ============================================================================
# 模块级工厂校验（供 FactorRegistry 调用）
# ============================================================================
def validate_factor_module(module, config: dict) -> FactorProvider:
    """
    加载并校验一个因子模块：
    1. 必须有 MANIFEST
    2. 必须有 create()
    3. create() 返回 FactorProvider 实例
    4. 实例的 name / version 与 MANIFEST 一致
    """
    if not hasattr(module, "MANIFEST"):
        raise ValueError(f"module {module.__name__} missing MANIFEST")
    if not hasattr(module, "create"):
        raise ValueError(f"module {module.__name__} missing create()")

    manifest = FactorManifest.from_dict(module.MANIFEST)
    instance = module.create(config)

    if not isinstance(instance, FactorProvider):
        raise TypeError(
            f"{module.__name__}.create() must return FactorProvider, "
            f"got {type(instance).__name__}"
        )
    if instance.name != manifest.name:
        raise ValueError(
            f"{module.__name__}: instance.name='{instance.name}' "
            f"!= MANIFEST.name='{manifest.name}'"
        )
    if instance.version != manifest.version:
        raise ValueError(
            f"{module.__name__}: instance.version='{instance.version}' "
            f"!= MANIFEST.version='{manifest.version}'"
        )

    return instance
