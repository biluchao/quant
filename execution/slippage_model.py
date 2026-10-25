"""
execution/slippage_model.py — 滑点模型 (重铸标准版)

核心设计原则：
1. 绝对可复现：必须注入带种子的 rng，确保相同的行情产生完全一致的滑点。
2. 强类型精度：内部计算全用 Decimal，杜绝浮点误差积累。
3. 尾部隔离：尾部冲击概率可控，极端行情下才生效。
4. 安全校准：在线校准带有硬性边界限制，防止模型被脏数据带偏。
"""

from dataclasses import dataclass
from decimal import Decimal
import logging
import numpy as np
from typing import List, Optional, Tuple

from core.gateway import Fill, OrderSide

logger = logging.getLogger(__name__)

# ============================================================================
# 常量与配置
# ============================================================================

@dataclass(frozen=True)
class SlippageConfig:
    """滑点模型配置"""
    base_bps: Decimal = Decimal("2.0")
    latency_per_ms_bps: Decimal = Decimal("0.1")
    tail_probability: float = 0.05
    tail_extra_bps: Decimal = Decimal("20.0")

    def __post_init__(self):
        if self.base_bps < 0 or self.latency_per_ms_bps < 0:
            raise ValueError("基础滑点或延迟滑点比例不能为负")
        if not (0.0 <= self.tail_probability <= 1.0):
            raise ValueError("尾部触发概率必须在 0 ~ 1 之间")

@dataclass(frozen=True)
class SlippageResult:
    """滑点估算结果"""
    base_bps: Decimal
    latency_bps: Decimal
    tail_bps: Decimal
    total_extra_bps: Decimal
    is_tail_triggered: bool


# ============================================================================
# 滑点模型
# ============================================================================

class SlippageModel:
    name = "slippage_model"
    version = "1.0.0"

    def __init__(self, config: SlippageConfig, rng: np.random.Generator):
        # 强制要求注入 rng
        if rng is None:
            raise ValueError("SlippageModel 必须注入 numpy.random.Generator (rng) 以保障回测可复现")
            
        self._config = config
        self._rng = rng
        
        # 记录校准数据
        self._calibration_history: List[float] = []

    # ========================================================================
    # 核心估算逻辑
    # ========================================================================

    def estimate(self, latency_ms: int) -> SlippageResult:
        """
        估算除盘口消耗外的额外滑点。
        """
        latency_ms = max(0, latency_ms)

        # 1. 基础滑点 + 延迟滑点
        base = self._config.base_bps
        latency = self._config.latency_per_ms_bps * Decimal(str(latency_ms))

        # 2. 尾部概率判定 (使用注入的 rng)
        is_tail = False
        tail = Decimal("0")
        
        if self._config.tail_probability > 0:
            # rng.random() 返回 [0.0, 1.0) 的 float，仅在判定时短暂使用 float，立即转回逻辑
            if self._rng.random() < self._config.tail_probability:
                is_tail = True
                tail = self._config.tail_extra_bps
                logger.debug("触发滑点尾部冲击 (流动性枯竭)")

        total = base + latency + tail

        return SlippageResult(
            base_bps=base,
            latency_bps=latency,
            tail_bps=tail,
            total_extra_bps=total,
            is_tail_triggered=is_tail
        )

    # ========================================================================
    # 应用到成交列表
    # ========================================================================

    def apply_to_fills(self, fills: List[Fill], side: OrderSide, extra_bps: Decimal) -> Tuple[List[Fill], Optional[Decimal]]:
        """
        将额外滑点应用到成交明细中。
        """
        if not fills:
            return [], None

        if extra_bps <= 0:
            return fills, self._weighted_avg(fills)

        # 转为比例因子
        factor = extra_bps / Decimal("10000")
        
        adjusted_fills = []
        for f in fills:
            # 买单：价格变高；卖单：价格变低
            if side == OrderSide.BUY:
                new_price = f.price * (Decimal("1") + factor)
            else:
                new_price = f.price * (Decimal("1") - factor)

            # 精度取整 (通常按照 0.1 或 0.01 取，这里简化为 4 位小数)
            new_price = new_price.quantize(Decimal("0.0001"))
            
            # 手续费随成交额增加而增加
            new_fee = (f.fee / f.price * new_price) if f.price > 0 else f.fee

            adjusted_fills.append(Fill(
                trade_id=f.trade_id,
                price=new_price,
                qty=f.qty,
                fee=new_fee,
                fee_asset=f.fee_asset,
                is_maker=f.is_maker,
                ts=f.ts
            ))

        return adjusted_fills, self._weighted_avg(adjusted_fills)

    # ========================================================================
    # 在线校准
    # ========================================================================

    def calibrate(self, realized_slippage_bps: List[float]) -> None:
        """
        使用实盘收集的滑点数据进行在线校准。
        """
        if not realized_slippage_bps:
            return
            
        self._calibration_history.extend(realized_slippage_bps)
        
        # 只保留最近的 500 笔数据用于计算，防止历史记忆过长
        if len(self._calibration_history) > 500:
            self._calibration_history = self._calibration_history[-500:]

        if len(self._calibration_history) < 50:
            logger.info(f"滑点校准数据不足 ({len(self._calibration_history)}/50)，暂不调整参数。")
            return

        # 使用中位数对抗极端异常值
        median_slip = float(np.median(self._calibration_history))
        
        # 加上防呆边界 (比如基准滑点控制在 1 ~ 50 bps 之间)
        new_base = Decimal(str(max(1.0, min(50.0, median_slip))))
        
        logger.info(f"滑点模型校准生效: 基础滑点 {self._config.base_bps} -> {new_base} bps")
        
        # 重新构造配置并替换
        self._config = SlippageConfig(
            base_bps=new_base,
            latency_per_ms_bps=self._config.latency_per_ms_bps,
            tail_probability=self._config.tail_probability,
            tail_extra_bps=self._config.tail_extra_bps
        )

    # ========================================================================
    # 辅助工具
    # ========================================================================

    @staticmethod
    def _weighted_avg(fills: List[Fill]) -> Optional[Decimal]:
        if not fills:
            return None
        total_notional = sum((f.price * f.qty for f in fills), Decimal("0"))
        total_qty = sum((f.qty for f in fills), Decimal("0"))
        if total_qty <= 0:
            return None
        return total_notional / total_qty


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "slippage_model",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["slippage_estimation", "slippage_calibration"],
    "module_dependencies": [],
    "owner": "lego",
}

def create(config: dict) -> SlippageModel:
    # 使用默认配置和 42 种子，实际由 main.py 注入特定的 rng
    return SlippageModel(SlippageConfig(), np.random.default_rng(42))
