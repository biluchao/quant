"""
core/factor.py — 因子抽象基类与数据结构 (重铸标准版)

核心设计原则：
1. 绝对异常隔离：因子计算内部任何异常都不得向上抛出，必须封装为 ERROR。
2. 防前视偏差：分位数窗口计算必须在当前值加入前进行，历史窗口绝不包含未来数据。
3. 强类型输出：所有因子统一输出不可变的 FactorValue。
4. 集中化归一化与异常值处理：提供统一的 _quantile_normalize 与 _is_outlier。
"""

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Dict, Optional, Tuple
import logging
import math

import numpy as np

logger = logging.getLogger(__name__)

# ============================================================================
# 常量与枚举
# ============================================================================
DEFAULT_QUANTILE_WINDOW = 500
DEFAULT_WARMUP_BARS = 500
DEFAULT_OUTPUT_PRECISION = 6
DEFAULT_OUTLIER_SIGMA = 5.0
DEGRADE_THRESHOLD = 3

class FactorState(str, Enum):
    """因子状态"""
    WARMUP = "warmup"       # 预热期，数据不足
    READY = "ready"         # 正常输出
    DEGRADED = "degraded"   # 连续异常，已降级
    ERROR = "error"         # 本次计算发生错误
    STALE = "stale"         # 数据过期（如快照缺失）

@dataclass(frozen=True)
class FactorValue:
    """因子输出的不可变值对象"""
    name: str
    value: float                    # 归一化后的值
    raw_value: float                # 原始计算值
    bar_ts: int
    state: FactorState
    version: str = "1.0.0"
    outlier: bool = False
    stale: bool = False
    compute_ms: float = 0.0
    error: Optional[str] = None

    def is_usable(self) -> bool:
        """下游判断该因子是否可用于决策"""
        return (
            self.state == FactorState.READY
            and not self.outlier
            and not self.stale
        )

# ============================================================================
# 因子基类
# ============================================================================

class FactorProvider(ABC):
    """
    所有因子的抽象基类。
    子类必须实现 _compute_raw，可选重写 on_bar_close 维护内部状态。
    """
    name: str = ""
    version: str = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        factor_cfg = config.get("factors", {}).get(self.name, {}) if self.name else {}
        
        self.quantile_window = int(factor_cfg.get("quantile_window", DEFAULT_QUANTILE_WINDOW))
        self.warmup_bars = int(factor_cfg.get("warmup_bars", DEFAULT_WARMUP_BARS))
        self.output_precision = int(factor_cfg.get("output_precision", DEFAULT_OUTPUT_PRECISION))
        self.outlier_sigma = float(factor_cfg.get("outlier_sigma", DEFAULT_OUTLIER_SIGMA))
        
        # 运行状态
        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = FactorState.WARMUP
        self._last_value: Optional[FactorValue] = None
        
        # 归一化历史窗口
        self._raw_history: Deque[float] = deque(maxlen=self.quantile_window)

    # ========================================================================
    # 核心执行入口 (由外部调用)
    # ========================================================================
    
    def compute(self, bar: Any, snapshot: Optional[dict] = None) -> FactorValue:
        """
        安全执行因子计算。屏蔽一切内部异常。
        """
        import time
        t0 = time.perf_counter()
        
        try:
            raw = self._compute_raw(bar, snapshot)
            compute_ms = (time.perf_counter() - t0) * 1000
            
            if raw is None:
                # 数据不足，未产出合法值
                return self._make_value(0.0, bar.ts, state=FactorState.WARMUP, compute_ms=compute_ms)
                
            return self._make_value(raw, bar.ts, compute_ms=compute_ms)
            
        except Exception as e:
            compute_ms = (time.perf_counter() - t0) * 1000
            err_msg = f"{type(e).__name__}: {e}"
            return self._make_value(0.0, getattr(bar, "ts", 0), error=err_msg, compute_ms=compute_ms)

    @abstractmethod
    def _compute_raw(self, bar: Any, snapshot: Optional[dict]) -> Optional[float]:
        """
        子类实现实际的计算逻辑。
        返回 None 表示预热期数据不足，或快照缺失导致无法计算。
        """
        ...

    # ========================================================================
    # 状态更新与归一化工具
    # ========================================================================

    def on_bar_close(self, bar: Any) -> None:
        """每根 K 线收线时更新状态（可用于计数和 WARMUP 解除）"""
        self._bars_seen += 1
        if self._state == FactorState.WARMUP and self._bars_seen >= self.warmup_bars:
            self._state = FactorState.READY
            logger.info(f"因子 {self.name} 预热完成。")

    def _make_value(
        self, raw: float, bar_ts: int, 
        state: Optional[FactorState] = None, 
        stale: bool = False, 
        error: Optional[str] = None, 
        compute_ms: float = 0.0
    ) -> FactorValue:
        """核心标准化构造：处理异常值、归一化、降级状态"""
        
        # 1. 错误处理路径
        if error is not None:
            self._consecutive_errors += 1
            if self._consecutive_errors >= DEGRADE_THRESHOLD:
                self._state = FactorState.DEGRADED
            return FactorValue(
                name=self.name, value=0.0, raw_value=raw, bar_ts=bar_ts,
                state=FactorState.ERROR, stale=stale, error=error, compute_ms=compute_ms
            )

        # 2. 恢复路径
        self._consecutive_errors = 0
        if self._state == FactorState.DEGRADED:
            self._state = FactorState.READY

        # 3. 正常路径
        actual_state = state or self._state
        
        # 异常值判断必须使用历史窗口（在归一化追加当前值之前）
        is_outlier = self._is_outlier(raw)
        
        # 归一化
        normalized = self._quantile_normalize(raw)
        normalized = round(normalized, self.output_precision)

        fv = FactorValue(
            name=self.name, value=normalized, raw_value=raw, bar_ts=bar_ts,
            state=actual_state, outlier=is_outlier, stale=stale, compute_ms=compute_ms
        )
        self._last_value = fv
        return fv

    def _quantile_normalize(self, raw: float) -> float:
        """
        经验分位数归一化。防前视偏差。
        """
        if not math.isfinite(raw):
            return 0.5
            
        # 窗口未填满时返回 0.5（中性）
        if len(self._raw_history) < 10:
            self._raw_history.append(raw)
            return 0.5

        # 1. 使用历史窗口计算分位数 (不含当前值)
        arr = np.array(self._raw_history)
        p = float(np.mean(arr <= raw))  # 经验分位数 [0, 1]
        
        # 2. 将当前值追加进历史窗口
        self._raw_history.append(raw)
        
        return p

    def _is_outlier(self, raw: float) -> bool:
        """基于历史窗口的 5σ 异常检测（不含当前值）"""
        if len(self._raw_history) < 30:
            return False
        arr = np.array(self._raw_history)
        std = float(np.std(arr))
        if std < 1e-9:
            return False
        mean = float(np.mean(arr))
        return abs(raw - mean) > self.outlier_sigma * std

    # ========================================================================
    # 健康与重置
    # ========================================================================

    def health(self) -> dict:
        return {
            "name": self.name,
            "state": self._state.value,
            "bars_seen": self._bars_seen,
            "consecutive_errors": self._consecutive_errors,
            "history_size": len(self._raw_history),
        }

    def reset(self) -> None:
        self._bars_seen = 0
        self._consecutive_errors = 0
        self._state = FactorState.WARMUP
        self._last_value = None
        self._raw_history.clear()
