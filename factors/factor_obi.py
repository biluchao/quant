"""
factors/factor_obi.py — 订单簿失衡因子 (重铸标准版)

因子定义：
    距离权重 w_i = exp(-decay_k * i)，i 从 0 开始（0 = 盘口最优价）
    raw = (bid_weighted - ask_weighted) / (bid_weighted + ask_weighted)
    时间平滑：smoothed = alpha * raw + (1 - alpha) * prev_smoothed

核心设计原则：
1. 脏数据零容忍：交叉盘口、过期快照、空盘口直接返回 None。
2. 状态隔离：利用基类捕获所有计算异常，子类只专注数学运算。
3. 平滑连续性：只有计算成功时才更新平滑状态，防止脏数据污染。
"""

import logging
import math
from typing import Any, Optional

from core.factor import FactorProvider

logger = logging.getLogger(__name__)

class OBIFactor(FactorProvider):
    """
    订单簿失衡因子实现。
    """

    name = "obi"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        # 提取专属参数
        factor_cfg = config.get("factors", {}).get(self.name, {})
        data_cfg = config.get("data", {})
        
        self.levels = int(factor_cfg.get("levels", 10))
        self.decay_k = float(factor_cfg.get("decay_k", 0.15))
        self.smooth_half_life_s = float(factor_cfg.get("smooth_half_life_s", 2.0))
        self.snapshot_max_age_ms = int(factor_cfg.get("snapshot_max_age_ms", 500))

        if self.levels < 1 or self.decay_k < 0 or self.smooth_half_life_s <= 0:
            raise ValueError("OBI 因子参数不合法")

        # 计算 K 线周期与平滑 alpha
        kline_interval_str = data_cfg.get("kline_interval", "3m")
        interval_ms = {"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000}.get(kline_interval_str, 180000)
        
        # alpha 计算：1 - exp(-ln(2) * K线周期 / 半衰期)
        kline_s = interval_ms / 1000.0
        self._alpha = 1.0 - math.exp(-math.log(2) * kline_s / self.smooth_half_life_s)
        self._alpha = min(max(self._alpha, 0.0), 1.0) # 限制在 0~1 之间

        # 平滑内部状态
        self._prev_smoothed: Optional[float] = None

    def _compute_raw(self, bar: Any, snapshot: Optional[dict] = None) -> Optional[float]:
        """
        计算 OBI 原始值。
        """
        # 1. 基础数据校验
        if not snapshot:
            return None

        snap_ts = snapshot.get("ts")
        if not snap_ts or abs(snap_ts - bar.ts) > self.snapshot_max_age_ms:
            # 快照过期或缺失时间戳
            return None

        bids = snapshot.get("bids", [])
        asks = snapshot.get("asks", [])
        if not bids or not asks:
            return None

        # 2. 交叉盘口校验 (Bid1 >= Ask1)
        # 假设底层传来的已经是转好 Decimal 的 tuple: (price, qty)
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        if best_bid >= best_ask:
            return None

        # 3. 计算距离加权
        def calc_weighted_volume(levels_data) -> float:
            vol = 0.0
            for i, (price, qty) in enumerate(levels_data[:self.levels]):
                q = float(qty)
                if q <= 0:
                    continue
                weight = math.exp(-self.decay_k * i)
                vol += weight * q
            return vol

        bid_vol = calc_weighted_volume(bids)
        ask_vol = calc_weighted_volume(asks)
        total_vol = bid_vol + ask_vol

        if total_vol < 1e-9:
            return None

        # 4. 计算原始值
        raw = (bid_vol - ask_vol) / total_vol

        # 5. 时间平滑 (EMA)
        if self._prev_smoothed is None or self._alpha >= 0.99:
            smoothed = raw
        else:
            smoothed = (self._alpha * raw) + ((1.0 - self._alpha) * self._prev_smoothed)

        # 6. 更新状态并返回
        self._prev_smoothed = smoothed
        return smoothed

    def reset(self) -> None:
        """重置因子内部状态"""
        super().reset()
        self._prev_smoothed = None


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "obi",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["obi", "orderbook_imbalance"],
    "module_dependencies": [],
    "owner": "bimon",
    "normalized_range": (-1.0, 1.0),
}

def create(config: dict) -> OBIFactor:
    return OBIFactor(config)
