"""
execution/fill_model_market.py — 市价单物理撮合引擎 (重铸标准版)

核心设计原则：
1. 纯函数执行：根据输入直接输出结果，绝无副作用。
2. 绝对精度：全链路 Decimal，杜绝浮点污染。
3. 拒单不崩溃：遇到脏盘口返回带 reject_reason 的结果对象，不抛异常。
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import logging
from typing import Any, Dict, List, Optional

from core.gateway import Fill, OrderSide

logger = logging.getLogger(__name__)

# ============================================================================
# 配置与数据结构
# ============================================================================

@dataclass(frozen=True)
class FillConfig:
    """撮合引擎配置"""
    max_level_pct: Decimal = Decimal("0.30")  # 每档最多吃掉挂单量的30%
    fee_rate: Decimal = Decimal("0.0004")     # taker 手续费
    max_levels_used: int = 20                 # 最多看前20档
    enable_fee: bool = True

    def __post_init__(self):
        if not (Decimal("0") < self.max_level_pct <= Decimal("1")):
            raise ValueError(f"max_level_pct 必须在 (0, 1] 之间")
        if self.fee_rate < 0:
            raise ValueError(f"fee_rate 不能为负")

@dataclass(frozen=True)
class FillOrder:
    """撮合输入订单（剥离了不必要的字段）"""
    side: OrderSide
    qty: Decimal
    client_order_id: str

@dataclass
class FillResult:
    """撮合结果"""
    requested_qty: Decimal
    executed_qty: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    fills: List[Fill] = field(default_factory=list)
    total_fee: Decimal = Decimal("0")
    slippage_bps: float = 0.0
    levels_consumed: int = 0
    is_partial: bool = False
    reject_reason: Optional[str] = None


# ============================================================================
# 物理撮合引擎
# ============================================================================

class MarketFillModel:
    name = "fill_model_market"
    version = "1.0.0"

    def __init__(self, config: FillConfig = None):
        self._config = config or FillConfig()

    def compute(self, order: FillOrder, snapshot: Optional[dict], now_ms: int) -> FillResult:
        """
        执行撮合。纯函数，绝不抛异常。
        """
        # 1. 基础校验
        if order.qty <= 0:
            return self._reject(order, "invalid_qty")
        if not snapshot:
            return self._reject(order, "no_market_data")

        bids = snapshot.get("bids", [])
        asks = snapshot.get("asks", [])
        
        if not bids or not asks:
            return self._reject(order, "empty_orderbook")

        # 2. 选择对手盘
        if order.side == OrderSide.BUY:
            levels = asks
            best_price = self._get_best_price(asks)
        else:
            levels = bids
            best_price = self._get_best_price(bids)

        if not best_price:
            return self._reject(order, "no_valid_levels")

        # 3. 逐档撮合
        result = self._match_levels(order, levels, now_ms)
        
        if result.executed_qty <= 0:
            result.reject_reason = "no_fills"
            return result

        # 4. 计算滑点与部分成交状态
        if result.executed_qty < order.qty:
            result.is_partial = True

        if result.avg_price and best_price > 0:
            # 计算滑点（bps）：吃亏为正，占便宜为负
            if order.side == OrderSide.BUY:
                diff = result.avg_price - best_price
            else:
                diff = best_price - result.avg_price
            
            result.slippage_bps = float((diff / best_price) * Decimal("10000"))

        return result

    # ========================================================================
    # 内部方法
    # ========================================================================

    def _match_levels(self, order: FillOrder, levels: list, now_ms: int) -> FillResult:
        """逐档吃对手盘"""
        result = FillResult(requested_qty=order.qty)
        remaining = order.qty
        filled_notional = Decimal("0")
        max_levels = min(len(levels), self._config.max_levels_used)

        for i in range(max_levels):
            if remaining <= 0:
                break

            try:
                # 强类型解包
                price = Decimal(str(levels[i][0]))
                level_qty = Decimal(str(levels[i][1]))
            except (IndexError, InvalidOperation, ValueError, TypeError):
                continue  # 脏数据跳过

            if price <= 0 or level_qty <= 0:
                continue

            # 计算本档最大可吃数量
            max_from_level = level_qty * self._config.max_level_pct
            fill_qty = min(remaining, max_from_level)

            if fill_qty <= 0:
                continue

            # 手续费计算
            fee = (fill_qty * price * self._config.fee_rate) if self._config.enable_fee else Decimal("0")

            result.fills.append(Fill(
                trade_id=f"vt-{order.client_order_id}-{i}",
                price=price,
                qty=fill_qty,
                fee=fee,
                fee_asset="USDT",
                is_maker=False,
                ts=now_ms,
            ))

            result.executed_qty += fill_qty
            result.total_fee += fee
            filled_notional += (fill_qty * price)
            remaining -= fill_qty
            result.levels_consumed = i + 1

        # 计算均价
        if result.executed_qty > 0:
            result.avg_price = filled_notional / result.executed_qty

        return result

    @staticmethod
    def _get_best_price(levels: list) -> Optional[Decimal]:
        """获取最优价格"""
        for level in levels:
            try:
                price = Decimal(str(level[0]))
                qty = Decimal(str(level[1]))
                if price > 0 and qty > 0:
                    return price
            except (IndexError, InvalidOperation, ValueError, TypeError):
                continue
        return None

    @staticmethod
    def _reject(order: FillOrder, reason: str) -> FillResult:
        """构造拒单结果"""
        return FillResult(requested_qty=order.qty, reject_reason=reason)


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "fill_model_market",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["market_fill"],
    "module_dependencies": [],
    "owner": "lego",
}

def create(config: dict) -> MarketFillModel:
    return MarketFillModel()
