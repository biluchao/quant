"""
api/routes_status.py — 状态与健康检查 API (重铸标准版)

核心设计原则：
1. 强类型序列化：所有 Decimal 强制转 str，保证前端精度。
2. 绝对只读：仅调用 getter，绝不修改系统内部状态。
3. 聚合健康：细粒度检查每个核心组件，任何一个宕机都会反映在总体状态上。
"""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

# ============================================================================
# 工具函数
# ============================================================================

def _fmt(val: Any) -> Optional[str]:
    """强类型转字符串，防止浮点误差"""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return format(val, 'f')
    return str(val)

def _get_component_health(engine: Any, name: str) -> dict:
    """检查单个组件健康度"""
    instances = getattr(engine, "instances", {})
    comp = instances.get(name)
    
    if comp is None:
        return {"status": "DOWN", "reason": "component_not_found"}
        
    try:
        if hasattr(comp, "health_check"):
            health = comp.health_check()
            return {"status": "OK" if health.get("healthy", True) else "WARN", "details": health}
        elif hasattr(comp, "health"):
            health = comp.health()
            return {"status": "OK", "details": health}
        return {"status": "OK"}
    except Exception as e:
        logger.error(f"组件 {name} 健康检查报错: {e}")
        return {"status": "ERROR", "reason": str(e)}


# ============================================================================
# 路由工厂
# ============================================================================

def create_status_router(engine: Any) -> APIRouter:
    router = APIRouter()

    @router.get("/status", summary="完整系统状态")
    async def get_status() -> Dict[str, Any]:
        """
        返回系统当前状态、风控级别、活跃轮次等。
        """
        instances = getattr(engine, "instances", {})
        
        # 1. 状态机快照
        sm = instances.get("state_machine")
        if not sm:
            raise HTTPException(status_code=503, detail="状态机未初始化")
            
        state_info = {
            "current_state": sm.current_state,
            "bars_in_state": sm.bars_in_state,
            "enter_reason": sm.enter_reason
        }

        # 2. 活跃轮次
        rm = instances.get("round_manager")
        active_round = rm.get_active_round() if rm else None

        # 3. 风控信息
        rp = instances.get("risk_policy")
        risk_info = {}
        if rp:
            risk_info = {
                "level": rp.get_risk_level().value if hasattr(rp.get_risk_level(), "value") else str(rp.get_risk_level()),
                "equity": _fmt(rp.equity),
                "daily_pnl": _fmt(rp.daily_pnl),
                "protection_ended": getattr(rp, "_protection_ended", False)
            }

        # 4. 系统时间与模式
        clock = instances.get("clock")
        system_info = {
            "symbol": getattr(engine, "symbol", "UNKNOWN"),
            "mode": getattr(engine, "mode", "unknown"),
            "now_ms": clock.now_ms() if clock else None,
        }

        return {
            "system": system_info,
            "state_machine": state_info,
            "risk": risk_info,
            "active_round": active_round,
            "status": "running"
        }

    @router.get("/config", summary="前端启动配置")
    async def get_config() -> Dict[str, Any]:
        """
        返回前端所需的静态配置信息。
        """
        instances = getattr(engine, "instances", {})
        return {
            "symbol": getattr(engine, "symbol", "UNKNOWN"),
            "mode": getattr(engine, "mode", "unknown"),
            "api_version": "1.0.0",
            "features": {
                "emergency_stop": "emergency_stop" in instances,
                "trading": "gateway" in instances,
            }
        }

    @router.get("/health", summary="聚合健康检查")
    async def get_health() -> Dict[str, Any]:
        """
        细粒度检查各个核心组件的健康状态，供监控系统使用。
        """
        components = ["clock", "state_machine", "risk_policy", "gateway", "persister"]
        results = {}
        overall_healthy = True

        for comp_name in components:
            res = _get_component_health(engine, comp_name)
            results[comp_name] = res
            if res["status"] in ("DOWN", "ERROR"):
                overall_healthy = False

        status_code = "OK" if overall_healthy else "WARN"
        
        return {
            "healthy": overall_healthy,
            "status": status_code,
            "components": results,
            "version": "1.0.0"
        }

    return router
