"""
strategies/strategy_trend_round.py — 趋势轮次策略 (重铸标准版)

核心设计原则：
1. 严格防重：同一根 K 线只计算一次，防止重试导致信号泛滥。
2. 动态权重：剔除降级或不可用的因子，动态归一化剩余权重。
3. 强风控隔离：严格遵守 context.risk_approved 的限制。
4. 清晰的多空对称：统一使用乘法进行方向判定。
"""

import logging
from typing import Any, Dict, Optional

from core.strategy import (
    SignalType,
    StrategyContext,
    StrategyProvider,
    StrategySignal,
    StrategyState,
)

logger = logging.getLogger(__name__)

class TrendRoundStrategy(StrategyProvider):
    """
    趋势轮次策略实现。
    """
    name = "trend_round"
    version = "1.0.0"

    def __init__(self, config: dict):
        super().__init__(config)

        # 提取策略参数
        strat_cfg = config.get("strategy", {}).get(self.name, {})
        self.entry_threshold = float(strat_cfg.get("entry_threshold", 0.5))
        self.direction_threshold = float(strat_cfg.get("direction_threshold", 0.1))
        self.reverse_threshold = float(strat_cfg.get("reverse_threshold", 0.3))
        self.max_bars_in_round = int(strat_cfg.get("max_bars_in_round", 60))
        
        # 内部状态：用于同一根 K 线防重
        self._last_bar_ts: Optional[int] = None

    def _compute_impl(self, context: StrategyContext) -> StrategySignal:
        """
        策略核心计算逻辑。
        """
        # 1. 防重复计算（同一根 K 线）
        if self._last_bar_ts == context.bar.ts:
            return self._make_noop(context, reason="duplicate_bar")
        self._last_bar_ts = context.bar.ts

        # 2. 预热期检查
        if self._state == StrategyState.WARMUP:
            return self._make_noop(context, reason="warming_up")

        # 3. 提取可用因子，并计算分数
        score_result = self._calculate_scores(context.factors)
        trend_score = score_result["trend_score"]
        direction = score_result["direction"]
        direction_score = score_result["direction_score"]

        # 4. 存在活跃轮次（持仓状态）
        if context.active_round is not None:
            return self._handle_active_round(context, direction_score, trend_score)

        # 5. 无持仓（寻找开仓机会）
        return self._handle_entry(context, direction, direction_score, trend_score)

    # ========================================================================
    # 内部分支逻辑
    # ========================================================================

    def _handle_active_round(self, context: StrategyContext, direction_score: float, trend_score: float) -> StrategySignal:
        """有持仓时的逻辑：判断是否需要平仓"""
        round_dir = int(context.active_round.get("direction", 0))
        bars_held = int(context.active_round.get("bars_held", 0))

        if round_dir == 0:
            return self._make_noop(context, reason="invalid_round_direction")

        # 1. 判断反向（趋势反转）
        # 如果持仓是 LONG (1)，且 direction_score < -threshold (强空信号)
        # 则 round_dir * direction_score < -reverse_threshold
        if (round_dir * direction_score) < -self.reverse_threshold:
            return StrategySignal(
                signal_id=f"exit-rev-{context.bar.ts}",
                strategy_name=self.name,
                bar_ts=context.bar.ts,
                signal_type=SignalType.EXIT,
                direction=0,
                score=trend_score,
                reason="trend_reversal"
            )

        # 2. 判断超时（时间止损）
        if bars_held > self.max_bars_in_round:
            return StrategySignal(
                signal_id=f"exit-time-{context.bar.ts}",
                strategy_name=self.name,
                bar_ts=context.bar.ts,
                signal_type=SignalType.EXIT,
                direction=0,
                score=trend_score,
                reason="timeout"
            )

        # 3. 继续持有
        return self._hold(reason="holding")

    def _handle_entry(self, context: StrategyContext, direction: int, direction_score: float, trend_score: float) -> StrategySignal:
        """无持仓时的逻辑：判断是否开仓"""
        
        # 1. 方向不明确，不操作
        if direction == 0:
            return self._make_noop(context, reason="direction_below_threshold")

        # 2. 分数不足，不操作
        if trend_score <= self.entry_threshold:
            return self._make_noop(context, reason="score_below_threshold")

        # 3. 风控拦截（必须有风控通过才能发信号）
        if not context.risk_approved:
            return self._make_noop(context, reason="risk_not_approved")

        # 4. 条件满足，发出进场信号
        return self._make_entry(
            context=context,
            direction=direction,
            score=trend_score,
            reason="breakout_confirmed"
        )

    # ========================================================================
    # 数学计算工具
    # ========================================================================

    def _calculate_scores(self, factors: Dict[str, Any]) -> dict:
        """
        基于可用因子计算 trend_score 和 direction。
        采用动态权重分配，防止因子不可用导致分母为0。
        """
        # 定义方向因子集合
        directional_names = {"momentum", "obi"}
        
        # 原始权重（假设与 config 配置一致）
        raw_weights = {"momentum": 0.35, "obi": 0.40, "volume": 0.25}

        usable_factors = {}
        for name, fv in factors.items():
            if hasattr(fv, "is_usable") and fv.is_usable():
                usable_factors[name] = fv

        if not usable_factors:
            return {"trend_score": 0.0, "direction": 0, "direction_score": 0.0}

        # 重新计算可用权重的总和
        total_weight = 0.0
        for name in usable_factors:
            total_weight += raw_weights.get(name, 0.0)

        if total_weight <= 0:
            return {"trend_score": 0.0, "direction": 0, "direction_score": 0.0}

        # 1. 计算趋势总分 trend_score (0.0 - 1.0)
        # 注意：基类因子的 value 已经归一化到了 [0,1] 或 [-1,1]
        # 为了计算 [0,1] 的趋势分，方向因子 [-1,1] 需要映射到 [0,1]
        weighted_sum = 0.0
        dir_sum = 0.0
        dir_weight_sum = 0.0

        for name, fv in usable_factors.items():
            w = raw_weights.get(name, 0.0)
            normalized_w = w / total_weight
            
            # 计算 direction_score (仅看方向因子)
            if name in directional_names:
                dir_sum += fv.value * normalized_w
                dir_weight_sum += normalized_w

            # 计算 trend_score 
            # 方向因子 [-1,1] 映射到 [0,1]：(val + 1) / 2
            if name in directional_names:
                mapped_val = (fv.value + 1.0) / 2.0
            else:
                mapped_val = fv.value  # 成交量本身已经是 [0,1]

            weighted_sum += mapped_val * normalized_w

        trend_score = weighted_sum
        
        # 2. 计算方向 direction (-1, 0, 1)
        # direction_score 需要重新归一化，因为只累加了方向因子
        direction_score = (dir_sum / dir_weight_sum) if dir_weight_sum > 0 else 0.0
        
        direction = 0
        if direction_score > self.direction_threshold:
            direction = 1
        elif direction_score < -self.direction_threshold:
            direction = -1

        return {
            "trend_score": trend_score,
            "direction": direction,
            "direction_score": direction_score
        }

    def reset(self) -> None:
        super().reset()
        self._last_bar_ts = None


# ============================================================================
# 模块工厂与 MANIFEST
# ============================================================================
MANIFEST = {
    "name": "trend_round",
    "type": "strategy",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["trend_following"],
    "module_dependencies": ["momentum", "obi", "volume"],
    "owner": "bimon",
    "required_factors": ["momentum", "obi", "volume"],
}

def create(config: dict) -> TrendRoundStrategy:
    return TrendRoundStrategy(config)
