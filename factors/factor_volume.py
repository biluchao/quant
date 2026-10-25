"""
factors/factor_volume.py — 成交量因子 (重铸标准版)

因子定义：
    ratio = current_volume / mean(volume[t-ma_window : t-1])

核心设计原则：
1. 杜绝自相关陷阱：计算均值时绝不包含当前 K 线，计算完比值后再将当前量入队。
2. 安全极值处理：历史均值为 0 时返回 None，防止除以 0 异常。
3. 专注计算：异常拦截与状态标记全交由基类处理。
"""

import logging
from collections import deque
from typing import Any, Optional

from core.factor import FactorProvider

logger = logging.getLogger(__name__)

class VolumeFactor(FactorProvider):
    """
    相对成交量因子实现。
    """

    name = "volume"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        # 提取专属参数
        factor_cfg = config.get("factors", {}).get(self.name, {})
        self.ma_window = int(factor_cfg.get("ma_window", 20))

        if self.ma_window < 2:
            raise ValueError(f"ma_window 必须 >= 2，当前为 {self.ma_window}")

        # 内部状态：仅维护近期的成交量
        self._volume_history: deque = deque(maxlen=self.ma_window)

    def _compute_raw(self, bar: Any, snapshot: Optional[dict] = None) -> Optional[float]:
        """
        计算相对成交量。
        返回 None 表示预热中或数据异常。
        """
        # 1. 安全提取成交量
        try:
            volume = float(bar.volume)
            if volume < 0:
                return None
        except (AttributeError, ValueError, TypeError):
            return None

        # 2. 预热期检查（队列未满）
        if len(self._volume_history) < self.ma_window:
            self._volume_history.append(volume)
            return None

        # 3. 计算历史均值（此时队列中不含当前这根 K 线的 volume）
        hist_mean = sum(self._volume_history) / self.ma_window

        # 4. 极端值保护：如果历史均值极小，说明是死水行情，忽略
        if hist_mean < 1e-6:
            self._volume_history.append(volume)
            return None

        # 5. 计算放量比例
        ratio = volume / hist_mean

        # 6. 计算完毕，将当前量推入历史队列
        self._volume_history.append(volume)

        return ratio

    def reset(self) -> None:
        """重置因子内部状态"""
        super().reset()
        self._volume_history.clear()


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "volume",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["volume", "volume_percentile"],
    "module_dependencies": [],
    "owner": "bimon",
    "normalized_range": (0.0, 1.0),
}

def create(config: dict) -> VolumeFactor:
    return VolumeFactor(config)
