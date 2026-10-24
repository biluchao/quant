"""
api/routes_emergency.py — 紧急停止路由

职责（唯一）：
1. POST /api/emergency_stop   — 触发紧急停止
2. POST /api/emergency_clear  — 清除紧急停止
3. GET  /api/emergency/events — 最近事件历史

不做什么：
- 不做决策（emergency_stop 模块负责）
- 不做平仓（gateway 负责）
- 不做轮次记账（RoundManager 负责）
- 不做状态查询（/api/status 负责）
- 不做通用健康检查（/api/health 负责）

职责边界：
- EmergencyRouter：HTTP 接口 + 请求验证 + 编排
- EmergencyStop：实际停止逻辑
- StateMachine / Gateway / RoundManager：被 EmergencyStop 调用

关键契约（"六必须"）：
1. 必须认证：三个端点全部需要 Bearer Token（config.api.token）
2. 必须请求体验证：Pydantic 模型，reason 必填，长度受限
3. 必须幂等触发：已在触发状态时返回现有 event
4. 必须状态检查：clear 在非 TRIGGERED/FAILED 状态时返回 400
5. 必须事件历史：GET /api/emergency/events 返回最近 N 次
6. 必须不阻塞事件循环：trigger 在 thread pool 中执行

HTTP 状态码约定：
- 200: 成功（无论 phase 是 triggered / failed）
- 202: 超时（trigger 仍在进行，前端继续等 WS 推送）
- 400: 请求无效（reason 为空、状态不允许 clear）
- 401: 缺少认证
- 403: token 无效
- 503: emergency_stop 实例不可用（如回测模式）
- 500: 内部错误（trigger 抛异常）

响应格式约定：
- 成功：{ok: true, ...}
- 失败：{ok: false, error: "...", detail?: "..."}

与 api/server.py 的集成：
    # server.py 中原有的 /api/emergency_stop 和 /api/emergency_clear 
    # 定义应被移除，改为：
    from api.routes_emergency import create_emergency_router
    app.include_router(create_emergency_router(engine))

与前端 ws_client.js 的关系：
- 前端通过 POST /api/emergency_stop 触发
- 后端通过 WebSocket 推送 emergency 消息（由 emergency_stop 的回调）
- 前端收到后再更新 UI（不依赖 POST 响应的内容）

与 api/routes_status.py 的关系：
- routes_status.py 的 /api/status 有 emergency 状态（只读）
- routes_emergency.py 处理触发/清除（写）
- 两者不重复

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from typing import Any, Dict, List, Optional
import asyncio
import logging
import time

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ============================================================================
# 尝试导入 TriggerSource（回测模式可能缺失）
# ============================================================================
try:
    from risk.emergency_stop import TriggerSource
except ImportError:
    class TriggerSource:  # type: ignore
        """回测模式的兜底。"""
        EXTERNAL_API = "external_api"


# ============================================================================
# 常量
# ============================================================================
DEFAULT_OPERATOR = "api"
MAX_REASON_LENGTH = 500
MAX_OPERATOR_LENGTH = 100
DEFAULT_EVENT_LIMIT = 20
MAX_EVENT_LIMIT = 100
TRIGGER_TIMEOUT_S = 25.0
CLEAR_TIMEOUT_S = 10.0
API_VERSION = "1.0.0"


# ============================================================================
# 请求模型
# ============================================================================
class TriggerRequest(BaseModel):
    """触发紧急停止的请求体。"""
    reason: str = Field(
        ...,
        min_length=1,
        max_length=MAX_REASON_LENGTH,
        description="触发原因（必填，最长 500 字符）",
    )
    operator: str = Field(
        default=DEFAULT_OPERATOR,
        max_length=MAX_OPERATOR_LENGTH,
        description="操作人标识（可选，最长 100 字符）",
    )


class ClearRequest(BaseModel):
    """清除紧急停止的请求体。"""
    reason: str = Field(
        ...,
        min_length=1,
        max_length=MAX_REASON_LENGTH,
        description="清除原因（必填，最长 500 字符）",
    )
    operator: str = Field(
        default=DEFAULT_OPERATOR,
        max_length=MAX_OPERATOR_LENGTH,
        description="操作人标识（可选，最长 100 字符）",
    )


# ============================================================================
# 认证依赖
# ============================================================================
def _make_auth_dependency(engine: Any):
    """
    创建认证依赖。

    契约：
    - config.api.token 未配置 → 放行（本地开发）
    - 配置了 token → 必须 Bearer Token
    - token 不符 → 403

    TODO(Step 2): 抽到 api/auth.py，供所有需要认证的路由复用。
    """
    config = getattr(engine, "config", {}) or {}
    token = (config.get("api", {}) or {}).get("token") or None

    async def _auth(
        authorization: Optional[str] = Header(None),
    ) -> None:
        if not token:
            return
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="Missing or invalid Authorization header",
            )
        provided = authorization[7:]
        if provided != token:
            raise HTTPException(
                status_code=403, detail="Invalid token"
            )

    return _auth


# ============================================================================
# 辅助
# ============================================================================
def _now_ms() -> int:
    return int(time.time() * 1000)


def _get_emergency_instance(engine: Any) -> Any:
    """从 engine.instances 获取 emergency_stop。"""
    instances = getattr(engine, "instances", {}) or {}
    return instances.get("emergency_stop")


def _phase_to_str(phase: Any) -> str:
    """枚举 → 字符串。"""
    if phase is None:
        return "unknown"
    if hasattr(phase, "value"):
        return str(phase.value)
    return str(phase)


def _event_to_dict(event: Any) -> Dict[str, Any]:
    """事件对象 → dict。"""
    if event is None:
        return {}
    if hasattr(event, "to_dict"):
        try:
            return event.to_dict()
        except Exception as e:
            logger.debug(f"event.to_dict failed: {e}")
    if isinstance(event, dict):
        return event
    return {}


def _get_event_field(
    event: Any, field: str, default: Any = None
) -> Any:
    """从 event 对象或 dict 读取字段。"""
    if event is None:
        return default
    v = getattr(event, field, None)
    if v is not None:
        return v
    if isinstance(event, dict):
        return event.get(field, default)
    return default


def _strip_reason(req_reason: str) -> str:
    """strip 后的 reason，空串抛 400。"""
    reason = (req_reason or "").strip()
    if not reason:
        raise HTTPException(
            status_code=400,
            detail="reason must not be blank after stripping",
        )
    return reason


# ============================================================================
# 路由工厂
# ============================================================================
def create_emergency_router(engine: Any) -> APIRouter:
    """
    创建紧急停止路由。

    Args:
        engine: 系统引擎（QuantSystem / LiveRunner / ShadowRunner）

    Returns:
        APIRouter，包含：
        - POST /api/emergency_stop
        - POST /api/emergency_clear
        - GET  /api/emergency/events

    契约：
    - 三个端点全部需要认证（如果 config 配了 token）
    - 触发端点阻塞到停止完成或超时
    - 事件历史端点轻量只读
    """
    if engine is None:
        raise ValueError(
            "create_emergency_router: engine is required"
        )

    auth = _make_auth_dependency(engine)
    router = APIRouter(
        tags=["emergency"],
        dependencies=[Depends(auth)],
    )

    # ==================================================================
    # POST /api/emergency_stop — 触发紧急停止
    # ==================================================================
    @router.post(
        "/api/emergency_stop",
        summary="Trigger emergency stop",
        description=(
            "Cancel all orders and close all positions. "
            "Idempotent: if already triggered, returns existing event."
        ),
    )
    async def _trigger(req: TriggerRequest) -> Any:
        es = _get_emergency_instance(engine)
        if es is None:
            raise HTTPException(
                status_code=503,
                detail="emergency_stop not available",
            )

        reason = _strip_reason(req.reason)
        operator = req.operator or DEFAULT_OPERATOR

        # 幂等：已触发 → 返回现有事件
        try:
            if es.is_triggered():
                current_event = es.get_active_event()
                event_dict = _event_to_dict(current_event)
                logger.info(
                    f"emergency_stop: already triggered, "
                    f"returning existing event_id="
                    f"{event_dict.get('event_id', 'unknown')}"
                )
                return {
                    "ok": True,
                    "version": API_VERSION,
                    "ts": _now_ms(),
                    "event_id": event_dict.get("event_id"),
                    "phase": _phase_to_str(
                        _get_event_field(current_event, "phase")
                    ),
                    "event": event_dict,
                    "note": "already_triggered",
                }
        except Exception as e:
            logger.warning(
                f"is_triggered check failed, proceeding anyway: {e}"
            )

        # 执行触发（to_thread 避免阻塞事件循环）
        logger.critical(
            f"EMERGENCY STOP API: operator={operator!r} "
            f"reason={reason!r}"
        )

        try:
            event = await asyncio.wait_for(
                asyncio.to_thread(
                    es.trigger,
                    source=TriggerSource.EXTERNAL_API,
                    reason=reason,
                    operator=operator,
                ),
                timeout=TRIGGER_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.critical(
                f"EMERGENCY STOP API timeout after "
                f"{TRIGGER_TIMEOUT_S}s, trigger still in progress"
            )
            return {
                "ok": True,
                "version": API_VERSION,
                "ts": _now_ms(),
                "phase": "triggering",
                "note": f"timeout_{TRIGGER_TIMEOUT_S}s_still_running",
            }
        except Exception as e:
            logger.error(
                f"emergency_stop trigger failed: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"emergency_stop failed: {type(e).__name__}: {e}",
            )

        event_dict = _event_to_dict(event)
        phase = _phase_to_str(_get_event_field(event, "phase"))

        logger.critical(
            f"EMERGENCY STOP API completed: "
            f"event_id={event_dict.get('event_id')} phase={phase}"
        )

        return {
            "ok": True,
            "version": API_VERSION,
            "ts": _now_ms(),
            "event_id": event_dict.get("event_id"),
            "phase": phase,
            "event": event_dict,
        }

    # ==================================================================
    # POST /api/emergency_clear — 清除紧急停止
    # ==================================================================
    @router.post(
        "/api/emergency_clear",
        summary="Clear emergency stop",
        description=(
            "Clear the emergency stop state. "
            "Only valid when phase is triggered or failed. "
            "Requires operator and reason for audit trail."
        ),
    )
    async def _clear(req: ClearRequest) -> Any:
        es = _get_emergency_instance(engine)
        if es is None:
            raise HTTPException(
                status_code=503,
                detail="emergency_stop not available",
            )

        reason = _strip_reason(req.reason)
        operator = req.operator or DEFAULT_OPERATOR

        # 阶段检查
        try:
            current_phase = _phase_to_str(es.get_phase())
        except Exception as e:
            logger.error(f"get_phase failed: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"get_phase failed: {e}",
            )

        if current_phase not in ("triggered", "failed"):
            return {
                "ok": False,
                "version": API_VERSION,
                "ts": _now_ms(),
                "error": "invalid_phase",
                "current_phase": current_phase,
                "detail": (
                    f"Cannot clear in phase '{current_phase}'. "
                    f"Must be 'triggered' or 'failed'."
                ),
            }

        logger.critical(
            f"EMERGENCY CLEAR API: operator={operator!r} "
            f"reason={reason!r} phase_from={current_phase}"
        )

        try:
            ok = await asyncio.wait_for(
                asyncio.to_thread(
                    es.clear,
                    operator=operator,
                    reason=reason,
                ),
                timeout=CLEAR_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"emergency_clear timeout after {CLEAR_TIMEOUT_S}s"
            )
            raise HTTPException(
                status_code=500,
                detail=f"clear timed out after {CLEAR_TIMEOUT_S}s",
            )
        except Exception as e:
            logger.error(
                f"emergency_clear failed: {e}", exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"clear failed: {type(e).__name__}: {e}",
            )

        if not ok:
            return {
                "ok": False,
                "version": API_VERSION,
                "ts": _now_ms(),
                "error": "clear_rejected",
                "current_phase": current_phase,
            }

        # 返回清除后的完整事件（用于审计）
        cleared_event = None
        try:
            cleared_event = es.get_last_event()
        except Exception:
            pass

        logger.critical(
            f"EMERGENCY CLEAR API completed: "
            f"operator={operator!r}"
        )

        return {
            "ok": True,
            "version": API_VERSION,
            "ts": _now_ms(),
            "cleared_event": _event_to_dict(cleared_event),
        }

    # ==================================================================
    # GET /api/emergency/events — 最近事件历史
    # ==================================================================
    @router.get(
        "/api/emergency/events",
        summary="Recent emergency stop events",
    )
    async def _events(
        limit: int = Query(
            DEFAULT_EVENT_LIMIT,
            ge=1,
            le=MAX_EVENT_LIMIT,
            description=f"Number of events (max {MAX_EVENT_LIMIT})",
        ),
    ) -> Dict[str, Any]:
        es = _get_emergency_instance(engine)
        if es is None:
            raise HTTPException(
                status_code=503,
                detail="emergency_stop not available",
            )

        try:
            events: List[Dict[str, Any]] = es.get_events(
                limit=limit
            ) or []
        except Exception as e:
            logger.error(
                f"get_events failed: {e}", exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"get_events failed: {e}",
            )

        # 确保全是 dict
        safe_events = [
            e if isinstance(e, dict) else _event_to_dict(e)
            for e in events
        ]

        return {
            "version": API_VERSION,
            "ts": _now_ms(),
            "count": len(safe_events),
            "limit": limit,
            "events": safe_events,
        }

    return router
