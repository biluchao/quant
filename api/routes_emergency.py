"""
api/routes_emergency.py — 紧急停止 API (重铸标准版)

核心设计原则：
1. 严格校验：必填 reason 与 operator，拒绝模糊调用。
2. 异步防卡死：危险操作扔进 to_thread，绝不阻塞主事件循环。
3. 超时防护：限制 10 秒超时，保障前端响应。
4. 状态机约束：严格检查当前阶段，拒绝非法清除。
"""

import asyncio
import logging
import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from risk.emergency_stop import StopPhase, TriggerSource

logger = logging.getLogger(__name__)

# ============================================================================
# 请求体模型
# ============================================================================

class TriggerRequest(BaseModel):
    reason: str = Field(..., min_length=2, max_length=200, description="触发原因")
    operator: str = Field("api_user", max_length=50, description="操作人")

class ClearRequest(BaseModel):
    reason: str = Field(..., min_length=2, max_length=200, description="清除原因")
    operator: str = Field("api_user", max_length=50, description="操作人")

# 操作超时限制（秒）
OPERATION_TIMEOUT = 10.0


# ============================================================================
# 路由工厂
# ============================================================================

def create_emergency_router(engine: Any) -> APIRouter:
    router = APIRouter()

    def get_emergency():
        """从 engine 提取紧急停止实例"""
        instances = getattr(engine, "instances", {})
        es = instances.get("emergency_stop")
        if not es:
            raise HTTPException(status_code=503, detail="紧急停止服务未加载")
        return es

    @router.post("/emergency_stop", summary="触发紧急停止")
    async def trigger_emergency(req: TriggerRequest) -> Dict[str, Any]:
        """
        触发紧急停止：撤单、平仓并锁死系统。
        """
        es = get_emergency()
        logger.critical(f"API 请求紧急停止: {req.reason} (Operator: {req.operator})")

        # 幂等检查：如果已经在触发中或已触发，直接返回
        if es.phase in (StopPhase.TRIGGERING, StopPhase.TRIGGERED, StopPhase.FAILED):
            return {
                "status": "ok",
                "message": "系统已处于紧急停止状态，无需重复触发",
                "phase": es.phase.value,
                "timestamp": int(time.time() * 1000)
            }

        try:
            # 异步执行，防止阻塞 FastAPI 事件循环
            event = await asyncio.wait_for(
                asyncio.to_thread(
                    es.trigger,
                    source=TriggerSource.EXTERNAL_API,
                    reason=req.reason,
                    operator=req.operator
                ),
                timeout=OPERATION_TIMEOUT
            )
            
            return {
                "status": "ok" if event.phase == StopPhase.TRIGGERED else "failed",
                "message": "紧急停止处理完成",
                "event_id": event.event_id,
                "phase": event.phase.value,
                "error": event.error
            }
            
        except asyncio.TimeoutError:
            logger.critical(f"API 触发紧急停止超时 ({OPERATION_TIMEOUT}s)，清仓仍在后台进行")
            return {
                "status": "triggering",
                "message": "平仓处理超时，系统仍在后台继续执行清仓",
                "timestamp": int(time.time() * 1000)
            }
        except Exception as e:
            logger.error(f"触发紧急停止 API 异常: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/emergency_clear", summary="清除紧急停止状态")
    async def clear_emergency(req: ClearRequest) -> Dict[str, Any]:
        """
        解除紧急停止状态，允许系统恢复。
        """
        es = get_emergency()
        
        # 状态检查：只有确实触发了，才允许清除
        if es.phase not in (StopPhase.TRIGGERED, StopPhase.FAILED):
            raise HTTPException(
                status_code=400, 
                detail=f"当前状态为 {es.phase.value}，无法执行清除操作"
            )

        logger.warning(f"API 请求清除紧急停止: {req.reason} (Operator: {req.operator})")

        try:
            # 清除动作也可能涉及外部回调，同样防阻塞
            success = await asyncio.wait_for(
                asyncio.to_thread(es.clear, operator=req.operator, reason=req.reason),
                timeout=OPERATION_TIMEOUT
            )
            
            if success:
                return {
                    "status": "ok",
                    "message": "紧急停止已解除",
                    "new_phase": es.phase.value
                }
            else:
                raise HTTPException(status_code=500, detail="清除失败，请检查后台日志")
                
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="清除操作超时")
        except Exception as e:
            logger.error(f"清除紧急停止 API 异常: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    return router
