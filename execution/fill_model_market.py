"""
execution/fill_model_market.py — 市价单成交模型（纯函数）

把"市价单如何吃对手盘"抽象成一个纯函数。

职责（唯一）：
1. 接收 side + qty + snapshot + config
2. 逐档吃对手盘，每档最多吃 max_level_pct
3. 返回 fills + executed_qty + avg_price + fee + slippage_bps
4. 失败以 reject_reason 返回，不抛异常

不做什么：
- 不修改账户/持仓（gateway_virtual 负责）
- 不缓存订单（gateway_virtual 负责）
- 不调时钟（now_ms 由调用方注入）
- 不写日志（调用方负责）
- 不检查快照时效性（调用方负责）
- 不处理限价单（Step 2 扩展）
- 不处理止损触发（gateway_virtual 触发后转市价）

职责边界：
- FillModel：纯撮合逻辑
- GatewayVirtual：账户/持仓/缓存/止损单管理
- PositionRound：轮次记账

关键契约（"六不变"）：
1. 纯函数：同输入 → 同输出，无随机、无时间依赖、无 I/O
2. 不抛异常：所有失败以 reject_reason 返回
3. 确定性：给定 snapshot 和 config，结果可复现
4. Decimal 全程：输入输出都用 Decimal，float() 只用于 slippage_bps
5. 悲观成交：每档最多 30%（默认），吃穿盘口即部分成交
6. 可配置：max_level_pct / fee_rate / enable_fee / max_levels_used

成交算法：
    对 BUY（吃 asks）：
        for level in asks[:max_levels_used]:
            price, level_qty = level
            if price <= 0 or level_qty <= 0: skip
            max_from_level = level_qty * max_level_pct
            fill_qty = min(remaining, max_from_level)
            fills.append(Fill(price, fill_qty, fee))
            remaining -= fill_qty
            if remaining <= 0: break

    对 SELL（吃 bids）：同上，遍历 bids

    结果：
    - executed_qty = sum(fill.qty)
    - avg_price = sum(fill.price * fill.qty) / executed_qty
    - fee = sum(fill.fee)
    - slippage_bps = (avg_price - best_ask) / best_ask * 10000（buy）
                    (best_bid - avg_price) / best_bid * 10000（sell）
    - is_partial = executed_qty < requested_qty
    - reject_reason：none / no_market_data / empty_orderbook /
                     invalid_qty / no_valid_levels / no_fills

为什么"每档最多 30%"：
- 假设我们的订单会消耗盘口的一部分
- 但我们不是唯一的参与者——市场还有其他订单
- 每档 100% 是乐观假设（我们吃掉整档）
- 每档 30% 是悲观假设（我们只吃掉 30%，剩下的被别人吃掉或撤单）
- 回测偏保守 = 实盘有可能更好 = 能活

为什么"不追单"：
- 部分成交意味着我们下大了
- 追单会引入新的市价单，可能吃到更差的价格
- 保守做法：用实际成交数量建立仓位，剩余不追
- 这与 RoundManager 的"开仓失败回滚"一致：宁可小，不可大

与 gateway_virtual 的协作：
    gateway_virtual.place_order(order)
      → fill_model.compute(FillOrder, snapshot, now_ms)
      → FillResult
      → 根据 FillResult.executed_qty 更新虚拟持仓/账户
      → 构造 OrderResult 返回

    gateway_virtual.check_stop_orders(...)
      → 对每个待触发止损单：
      → 转市价 FillOrder
      → fill_model.compute(...)
      → 更新账户

作者：乐高
审查：比蒙（策略）、张三（系统）
版本：v1.0.0
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import logging
from typing import Any, Dict, List, Optional, Tuple

from core.gateway import Fill, OrderSide

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "fill_model_market",
    "type": "execution",
    "version": "1.0.0",
    "step": 1,
    "owner": "lego",
    "capabilities": ["fill_model", "market_fill"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Pure-function market order fill model for virtual gateway",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_LEVEL_PCT = Decimal("0.30")
DEFAULT_FEE_RATE = Decimal("0.0004")
DEFAULT_MAX_LEVELS_USED = 20
DEFAULT_ENABLE_FEE = True
BPS_MULTIPLIER = Decimal("10000")


# ============================================================================
# 配置
# ============================================================================
@dataclass(frozen=True)
class FillConfig:
    """
    成交模型配置。不可变。

    所有参数在构造时校验。
    """
    max_level_pct: Decimal = DEFAULT_MAX_LEVEL_PCT
    fee_rate: Decimal = DEFAULT_FEE_RATE
    max_levels_used: int = DEFAULT_MAX_LEVELS_USED
    enable_fee: bool = DEFAULT_ENABLE_FEE

    def __post_init__(self):
        if not (Decimal("0") < self.max_level_pct <= Decimal("1")):
            raise ValueError(
                f"max_level_pct must be in (0, 1], "
                f"got {self.max_level_pct}"
            )
        if self.fee_rate < 0:
            raise ValueError(
                f"fee_rate must be >= 0, got {self.fee_rate}"
            )
        if self.max_levels_used < 1:
            raise ValueError(
                f"max_levels_used must be >= 1, "
                f"got {self.max_levels_used}"
            )


# ============================================================================
# 输入
# ============================================================================
@dataclass(frozen=True)
class FillOrder:
    """
    成交模型的输入（市价单）。不可变。

    与 core.gateway.Order 解耦，只含撮合必需字段。
    """
    side: OrderSide           # BUY / SELL
    qty: Decimal              # 期望成交数量（正数）
    client_order_id: str      # 用于生成 fill trade_id


# ============================================================================
# 输出
# ============================================================================
@dataclass
class FillResult:
    """
    成交模型的输出。

    契约：
    - reject_reason 为 None 表示成交（可能部分成交）
    - reject_reason 非 None 表示完全未成交（fills 为空）
    - is_partial = executed_qty < requested_qty
    """
    requested_qty: Decimal
    executed_qty: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    fills: List[Fill] = field(default_factory=list)
    total_fee: Decimal = Decimal("0")
    slippage_bps: float = 0.0
    levels_consumed: int = 0
    is_partial: bool = False
    reject_reason: Optional[str] = None

    def is_filled(self) -> bool:
        """是否完全成交。"""
        return (
            self.reject_reason is None
            and self.executed_qty >= self.requested_qty
        )

    def is_filled_some(self) -> bool:
        """是否至少部分成交。"""
        return self.reject_reason is None and self.executed_qty > 0

    def to_dict(self) -> Dict[str, Any]:
        """序列化，用于归因和调试。"""
        return {
            "requested_qty": str(self.requested_qty),
            "executed_qty": str(self.executed_qty),
            "avg_price": (
                str(self.avg_price)
                if self.avg_price is not None else None
            ),
            "fills_count": len(self.fills),
            "total_fee": str(self.total_fee),
            "slippage_bps": self.slippage_bps,
            "levels_consumed": self.levels_consumed,
            "is_partial": self.is_partial,
            "reject_reason": self.reject_reason,
        }


# ============================================================================
# FillModel
# ============================================================================
class MarketFillModel:
    """
    市价单成交模型（纯函数）。详见模块 docstring。

    用法：
        model = MarketFillModel(FillConfig(
            max_level_pct=Decimal("0.3"),
            fee_rate=Decimal("0.0004"),
        ))

        fill_order = FillOrder(
            side=OrderSide.BUY,
            qty=Decimal("0.5"),
            client_order_id="bm-ent-0001-00-00",
        )

        result = model.compute(
            order=fill_order,
            snapshot={"bids": [...], "asks": [...]},
            now_ms=1700000000000,
        )

        if result.reject_reason:
            logger.warning(f"fill rejected: {result.reject_reason}")
        elif result.is_partial:
            logger.info(f"partial fill: {result.executed_qty}")
        else:
            logger.info(f"filled at {result.avg_price}")
    """

    name = "fill_model_market"
    version = "1.0.0"

    def __init__(self, config: Optional[FillConfig] = None):
        self._config = config or FillConfig()

        logger.info(
            f"MarketFillModel initialized "
            f"max_level_pct={self._config.max_level_pct} "
            f"fee_rate={self._config.fee_rate} "
            f"max_levels_used={self._config.max_levels_used} "
            f"enable_fee={self._config.enable_fee}"
        )

    # ------------------------------------------------------------------------
    # 核心：compute（纯函数）
    # ------------------------------------------------------------------------

    def compute(
        self,
        order: FillOrder,
        snapshot: Optional[Dict[str, Any]],
        now_ms: int,
    ) -> FillResult:
        """
        计算市价单成交。

        Args:
            order: 撮合输入
            snapshot: 盘口快照 {"bids": [(price, qty), ...], "asks": [...]}
            now_ms: 当前时间戳（用于 Fill.ts）

        Returns:
            FillResult

        契约：
        - 纯函数：无副作用，同输入 → 同输出
        - 不抛异常：所有失败以 reject_reason 返回
        - 不检查 snapshot.ts 时效性（调用方负责）
        - 不检查 qty 精度（调用方负责）
        """
        # ---- 1. 基础校验 ----
        if order.qty <= 0:
            return self._reject(order, "invalid_qty")

        # ---- 2. snapshot 校验 ----
        if snapshot is None:
            return self._reject(order, "no_market_data")

        bids = snapshot.get("bids")
        asks = snapshot.get("asks")

        if not bids or not asks:
            return self._reject(order, "empty_orderbook")

        # ---- 3. 选择对手盘 ----
        if order.side == OrderSide.BUY:
            levels = asks            # 买单吃卖盘
            best_price = self._best_price(asks)
        else:
            levels = bids            # 卖单吃买盘
            best_price = self._best_price(bids)

        if best_price is None:
            return self._reject(order, "no_valid_levels")

        # ---- 4. 逐档撮合 ----
        result = self._match_levels(
            order=order,
            levels=levels,
            now_ms=now_ms,
        )

        # ---- 5. 完全未成交 ----
        if result.executed_qty <= 0:
            result.reject_reason = "no_fills"
            return result

        # ---- 6. 计算指标 ----
        if result.executed_qty < order.qty:
            result.is_partial = True

        # 滑点（bps）
        result.slippage_bps = self._compute_slippage_bps(
            side=order.side,
            best_price=best_price,
            avg_price=result.avg_price,
        )

        return result

    # ------------------------------------------------------------------------
    # 内部：逐档撮合
    # ------------------------------------------------------------------------

    def _match_levels(
        self,
        order: FillOrder,
        levels: Any,
        now_ms: int,
    ) -> FillResult:
        """逐档吃对手盘，构造 FillResult。"""
        result = FillResult(requested_qty=order.qty)
        remaining = order.qty
        filled_notional = Decimal("0")

        max_levels = min(len(levels), self._config.max_levels_used)

        for i in range(max_levels):
            if remaining <= 0:
                break

            level = levels[i]
            try:
                price = Decimal(str(level[0]))
                level_qty = Decimal(str(level[1]))
            except (IndexError, InvalidOperation, ValueError, TypeError):
                logger.debug(
                    f"skip invalid level at index {i}: {level}"
                )
                continue

            if price <= 0 or level_qty <= 0:
                continue

            # 每档最多吃 max_level_pct
            max_from_level = level_qty * self._config.max_level_pct
            fill_qty = min(remaining, max_from_level)

            if fill_qty <= 0:
                continue

            # 手续费
            fee = (
                fill_qty * price * self._config.fee_rate
                if self._config.enable_fee
                else Decimal("0")
            )

            fill = Fill(
                trade_id=f"vt-{order.client_order_id}-{i}",
                price=price,
                qty=fill_qty,
                fee=fee,
                fee_asset="USDT",
                is_maker=False,
                ts=now_ms,
            )

            result.fills.append(fill)
            result.executed_qty += fill_qty
            result.total_fee += fee
            result.levels_consumed = i + 1
            filled_notional += fill_qty * price
            remaining -= fill_qty

        # 加权平均价
        if result.executed_qty > 0:
            result.avg_price = filled_notional / result.executed_qty

        return result

    # ------------------------------------------------------------------------
    # 内部：滑点
    # ------------------------------------------------------------------------

    @staticmethod
    def _compute_slippage_bps(
        side: OrderSide,
        best_price: Decimal,
        avg_price: Optional[Decimal],
    ) -> float:
        """
        计算滑点（bps）。

        公式：
            BUY:  (avg_price - best_ask) / best_ask * 10000
            SELL: (best_bid - avg_price) / best_bid * 10000

        正数：成交比最优价差（不利于我们）
        负数：成交比最优价好（有利于我们，几乎不会发生）
        0：完美成交在最优价
        """
        if avg_price is None or avg_price <= 0:
            return 0.0
        if best_price is None or best_price <= 0:
            return 0.0

        try:
            if side == OrderSide.BUY:
                diff = avg_price - best_price
            else:
                diff = best_price - avg_price

            bps = (diff / best_price) * BPS_MULTIPLIER
            return float(bps)
        except (ArithmeticError, InvalidOperation):
            return 0.0

    # ------------------------------------------------------------------------
    # 内部：辅助
    # ------------------------------------------------------------------------

    @staticmethod
    def _best_price(levels: Any) -> Optional[Decimal]:
        """获取第一个有效档位价格。"""
        for level in levels:
            try:
                price = Decimal(str(level[0]))
                qty = Decimal(str(level[1]))
            except (IndexError, InvalidOperation, ValueError, TypeError):
                continue
            if price > 0 and qty > 0:
                return price
        return None

    @staticmethod
    def _reject(
        order: FillOrder, reason: str
    ) -> FillResult:
        """构造拒单结果。"""
        return FillResult(
            requested_qty=order.qty,
            reject_reason=reason,
        )

    # ------------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------------

    def get_config(self) -> FillConfig:
        """获取当前配置。"""
        return self._config

    def health(self) -> Dict[str, Any]:
        """健康检查。模型无状态，返回配置。"""
        return {
            "healthy": True,
            "name": self.name,
            "version": self.version,
            "config": {
                "max_level_pct": str(self._config.max_level_pct),
                "fee_rate": str(self._config.fee_rate),
                "max_levels_used": self._config.max_levels_used,
                "enable_fee": self._config.enable_fee,
            },
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> MarketFillModel:
    """
    模块工厂入口。

    config 可选：
    - fill_model.max_level_pct: 默认 0.30
    - fill_model.fee_rate: 默认 0.0004
    - fill_model.max_levels_used: 默认 20
    - fill_model.enable_fee: 默认 True
    """
    cfg = (config or {}).get("fill_model", {})

    fill_config = FillConfig(
        max_level_pct=Decimal(str(cfg.get(
            "max_level_pct", str(DEFAULT_MAX_LEVEL_PCT)
        ))),
        fee_rate=Decimal(str(cfg.get(
            "fee_rate", str(DEFAULT_FEE_RATE)
        ))),
        max_levels_used=int(cfg.get(
            "max_levels_used", DEFAULT_MAX_LEVELS_USED
        )),
        enable_fee=bool(cfg.get(
            "enable_fee", DEFAULT_ENABLE_FEE
        )),
    )

    return MarketFillModel(fill_config)


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "execution/fill_model_market.py")
    return MANIFEST
