"""
factors/factor_registry.py — 因子注册与调度中心 (重铸标准版)

核心设计原则：
1. 严苛注册：禁止重复注册，权重必须合法。
2. 异常隔离：单因子计算崩溃不得中断整个计算流。
3. 动态归一化：根据因子的实时可用状态，自动重新计算权重。
"""

import logging
import threading
from typing import Any, Dict, List, Optional

from core.factor import FactorProvider, FactorState, FactorValue

logger = logging.getLogger(__name__)

class FactorRegistry:
    name = "factor_registry"
    version = "1.0.0"

    def __init__(self, config: dict):
        if not isinstance(config, dict):
            raise ValueError("FactorRegistry 必须接收 config 字典")

        self.config = config
        self._lock = threading.RLock()
        
        # 内部状态
        self._factors: Dict[str, FactorProvider] = {}
        self._weights: Dict[str, float] = {}
        self._registration_order: List[str] = []

    # ========================================================================
    # 注册接口
    # ========================================================================

    def register(self, name: str, factor: FactorProvider, weight: float) -> None:
        """
        注册一个因子并分配初始权重。
        """
        if not name or not isinstance(name, str):
            raise ValueError("因子名称必须是非空字符串")
        if not isinstance(factor, FactorProvider):
            raise TypeError(f"因子 '{name}' 必须继承自 FactorProvider")
        if weight < 0:
            raise ValueError(f"因子 '{name}' 的权重不能为负数")

        with self._lock:
            if name in self._factors:
                raise ValueError(f"因子 '{name}' 已经被注册，禁止重复覆盖")

            self._factors[name] = factor
            self._weights[name] = float(weight)
            self._registration_order.append(name)
            
            logger.info(f"因子注册成功: {name} (权重={weight})")

    # ========================================================================
    # 计算调度
    # ========================================================================

    def process_bar(self, bar: Any, snapshot: Optional[dict] = None) -> Dict[str, FactorValue]:
        """
        驱动所有因子计算当前 K 线的值。
        绝对异常隔离：任何因子的内部崩溃都会被捕获并转为 ERROR 状态。
        """
        with self._lock:
            factors_snapshot = list(self._factors.items())

        results: Dict[str, FactorValue] = {}

        for name, factor in factors_snapshot:
            try:
                # 1. 推进因子的预热状态
                factor.on_bar_close(bar)
                
                # 2. 计算因子值
                fv = factor.compute(bar, snapshot)
                
                # 3. 防御性检查：确保返回了正确的类型
                if not isinstance(fv, FactorValue):
                    raise TypeError(f"因子返回了非法类型: {type(fv)}")
                    
                if fv.name != name:
                    logger.warning(f"因子内部名称 {fv.name} 与注册名称 {name} 不一致")

                results[name] = fv

            except Exception as e:
                # 拦截一切异常，构造一个 ERROR 状态的值，保证策略层不断流
                logger.error(f"因子 '{name}' 计算发生严重异常: {e}", exc_info=True)
                results[name] = FactorValue(
                    name=name,
                    value=0.0,
                    raw_value=0.0,
                    bar_ts=getattr(bar, "ts", 0),
                    state=FactorState.ERROR,
                    error=f"Registry caught: {str(e)}"
                )

        return results

    # ========================================================================
    # 权重与状态查询
    # ========================================================================

    def get_weights(self, usable_only: bool = True) -> Dict[str, float]:
        """
        获取归一化后的因子权重。
        """
        with self._lock:
            items = []
            for name in self._registration_order:
                factor = self._factors[name]
                
                if usable_only:
                    # 借助因子基类的 health() 或者直接看它的内部状态
                    # 这里为了严谨，假设不可用（预热、降级、错误）时不参与权重分配
                    status = factor._state if hasattr(factor, "_state") else FactorState.READY
                    if status != FactorState.READY:
                        continue
                        
                items.append((name, self._weights[name]))

            if not items:
                return {}

            total_weight = sum(w for _, w in items)
            if total_weight <= 0:
                # 如果配置有误导致总权重为 0，均分处理
                avg = 1.0 / len(items)
                return {name: avg for name, _ in items}

            return {name: w / total_weight for name, w in items}

    def health(self) -> dict:
        """健康检查，用于监控面板"""
        with self._lock:
            factor_health = {}
            for name, f in self._factors.items():
                factor_health[name] = f.health()
                
            return {
                "count": len(self._factors),
                "weights_raw": dict(self._weights),
                "weights_active": self.get_weights(usable_only=True),
                "factors": factor_health
            }


# ============================================================================
# 模块工厂
# ============================================================================
MANIFEST = {
    "name": "factor_registry",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["factor_registry"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> FactorRegistry:
    return FactorRegistry(config)
