"""
factors/factor_momentum.py — 动量因子 (重铸标准版)

因子定义：
    raw = roc_weight * ROC(roc_period) + accel_weight * accel
    ROC(3) = (close[t] - close[t-3]) / close[t-3]
    accel  = ROC(3)[t] - ROC(3)[t-1]

核心设计原则：
1. 只负责原始值：异常拦截、分位数归一化、状态流转全部由基类处理。
2. 防前视偏差：数据不足时直接返回 None，绝不提前计算。
3. 数据强隔离：脏数据绝不向上抛异常，静默返回 None。
"""

import logging
from collections import deque
from typing import Any, Deque, Optional

from core.factor import FactorProvider

logger = logging.getLogger(__name__)

class MomentumFactor(FactorProvider):
    """
    动量因子实现。
    """

    name = "momentum"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)
        
        # 提取专属参数
        factor_cfg = config.get("factors", {}).get(self.name, {})
        self.roc_period = int(factor_cfg.get("roc_period", 3))
        self.roc_weight = float(factor_cfg.get("roc_weight", 0.7))
        self.accel_weight = float(factor_cfg.get("accel_weight", 0.3))

        if self.roc_period < 1:
            raise ValueError(f"roc_period 必须 >= 1，当前为 {self.roc_period}")

        # 内部状态：保留 roc_period + 1 个收盘价，足够计算 ROC
        self._close_history: Deque[float] = deque(maxlen=self.roc_period + 1)
        self._prev_roc: Optional[float] = None

    def _compute_raw(self, bar: Any, snapshot: Optional[dict] = None) -> Optional[float]:
        """
        计算动量原始值。
        返回 None 表示数据不足或数据非法，基类会将其归为 WARMUP。
        """
        # 1. 安全提取收盘价
        try:
            close = float(bar.close)
            if close <= 0:
                return None
        except (AttributeError, ValueError, TypeError):
            # 数据异常，交由基类标记为 ERROR/WARMUP
            return None

        # 2. 追加到历史队列
        self._close_history.append(close)

        # 3. 判断历史数据是否充足
        if len(self._close_history) < self.roc_period + 1:
            return None

        # 4. 计算 ROC
        close_past = self._close_history[-(self.roc_period + 1)]
        if close_past <= 0:
            return None  # 除零保护
            
        roc = (close - close_past) / close_past

        # 5. 计算加速度 (Accel)
        if self._prev_roc is not None:
            accel = roc - self._prev_roc
        else:
            accel = 0.0
            
        # 更新上一次的 ROC 记录
        self._prev_roc = roc

        # 6. 合成原始值
        raw_value = (self.roc_weight * roc) + (self.accel_weight * accel)
        return raw_value

    def reset(self) -> None:
        """重置因子内部状态"""
        super().reset()
        self._close_history.clear()
        self._prev_roc = None


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "momentum",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["momentum", "roc", "acceleration"],
    "module_dependencies": [],
    "owner": "bimon",
    # 补充因子专属元数据
    "normalized_range": (-1.0, 1.0),
}

def create(config: dict) -> MomentumFactor:
    return MomentumFactor(config)
