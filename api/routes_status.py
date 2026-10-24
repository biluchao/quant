"""
api/routes_status.py — 状态相关路由

职责（唯一）：
1. `/api/status`  — 完整状态快照（给操作员看："系统在做什么"）
2. `/api/config`  — 前端启动配置（给前端初始化用）
3. `/api/health`  — 健康检查（给监控看："系统活着吗"）

不做什么：
- 不做认证（三个端点全部公开）
- 不做业务逻辑（只读 + 编排）
- 不涉及写操作（不修改任何状态）
- 不做参数校验（只序列化）
- 不做分页 / 过滤（状态是单例，不是列表）

职责边界：
- StatusRouter：只读编排 + 序列化
- api/server.py：挂载 router，处理认证、CORS、WebSocket
- 各模块：提供 health_check() / get_active_round() / get_version()

关键契约（"六必须"）：
1. 必须公开：三个端点无需认证（监控系统、前端启动时调用）
2. 必须轻量：所有组件访问都是内存操作，无 I/O
3. 必须独立异常：每个组件访问独立 try/except，一个崩不影响其他
4. 必须分级健康：ok / warn / fail / unknown 四态，聚合取最坏
5. 必须组件化：每个组件的健康状态独立返回，可定位问题
6. 必须序列化安全：所有值转 str / int / bool / None，无 Decimal / Enum 直接暴露

健康分级语义：
- ok:      组件完全正常
- warn:    组件可用但有降级（如 risk_policy 等级为 warn）
- fail:    组件不可用（异常或健康检查 false）
- unknown: 组件未初始化（启动过程中）

聚合规则（取最坏值）：
fail > warn > unknown > ok

健康检查的"廉价"约定：
- 所有组件的 health_check() 必须是内存操作
- 禁止在 health_check() 中查询数据库 / 调网络 / 读写磁盘
- 若未来有慢检查，必须加 TTL 缓存（Step 2）

与 api/server.py 的集成：
    from api.routes_status import create_status_router
    app.include_router(create_status_router(engine))

与前端 ws_client.js 的关系：
- 三个端点都是 HTTP GET，不通过 WebSocket
- 前端启动时调 /api/config 获取 symbol / features
- 前端定期（如 10s）调 /api/status 做状态同步（WebSocket 的兜底）
- 监控系统调 /api/health（不需要 WebSocket）

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from typing import Any, Dict, Optional
import logging
import time

from fastapi import APIRouter


logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================

# 标准化健康状态
STATUS_OK = "ok"
STATUS_WARN = "warn"
STATUS_FAIL = "fail"
STATUS_UNKNOWN = "unknown"

# 聚合优先级（数字越大越严重）
STATUS_PRIORITY = {
    STATUS_OK: 0,
    STATUS_UNKNOWN: 1,
    STATUS_WARN: 2,
    STATUS_FAIL: 3,
}

# 需要检查健康的组件
# 顺序影响 /api/health 输出顺序，前端可依赖
HEALTH_COMPONENT_NAMES = [
    "state_machine",
    "risk_policy",
    "round_manager",
    "param_store",
    "kline_authority",
    "rest_client",
    "persister",
    "clock",
    "emergency_stop",
]

# /api/status 中 risk 段要挑选的关键字段
RISK_SNAPSHOT_KEYS = [
    "daily_pnl",
    "drawdown",
    "margin_ratio",
    "consecutive_losses",
    "peak_equity",
    "equity",
    "protection_ended",
    "completed_rounds",
]

# /api/status 中 clock 段要挑选的关键字段
CLOCK_SNAPSHOT_KEYS = [
    "health",
    "offset_ms",
    "last_sync_ts",
    "last_sync_age_s",
    "backward_count",
]


# ============================================================================
# 通用工具
# ============================================================================

def _now_ms() -> int:
    """当前毫秒时间戳（UTC）。"""
    return int(time.time() * 1000)


def _safe_call(obj: Any, method: str) -> Any:
    """安全调用对象方法，异常返回 None。"""
    if obj is None:
        return None
    fn = getattr(obj, method, None)
    if fn is None:
        return None
    try:
        return fn()
    except Exception as e:
        logger.debug(f"{method} on {type(obj).__name__} raised: {e}")
        return None


def _to_jsonable(value: Any) -> Any:
    """
    将任意值转为 JSON 可序列化。

    - Decimal / Enum → str
    - int / float / str / bool / None → 原样
    - 其他 → str
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "value"):  # Enum
        return value.value
    return str(value)


# ============================================================================
# 健康检查
# ============================================================================

def _check_component_health(
    name: str, component: Any
) -> Dict[str, Any]:
    """
    标准化单个组件的健康状态。

    契约：
    - 组件为 None → unknown
    - 组件无 health_check / health 方法 → unknown
    - 组件抛异常 → fail（含 reason）
    - 组件返回非 dict → ok/fail 按 truthy
    - 组件返回 dict → 标准化（healthy 字段映射为 ok/fail）
    """
    if component is None:
        return {
            "name": name,
            "status": STATUS_UNKNOWN,
            "reason": "not_initialized",
        }

    fn = getattr(component, "health_check", None)
    if fn is None:
        fn = getattr(component, "health", None)
    if fn is None:
        return {
            "name": name,
            "status": STATUS_UNKNOWN,
            "reason": "no_health_method",
        }

    try:
        result = fn()
    except Exception as e:
        logger.warning(
            f"component {name} health check raised: {e}",
            exc_info=True,
        )
        return {
            "name": name,
            "status": STATUS_FAIL,
            "reason": f"{type(e).__name__}: {e}",
        }

    # 非 dict：按 truthy 映射
    if not isinstance(result, dict):
        return {
            "name": name,
            "status": STATUS_OK if result else STATUS_FAIL,
        }

    # dict：标准化
    healthy = result.get("healthy", True)
    if healthy:
        # 组件可以自己声明 warn 等级（如风控等级为 warn）
        declared = result.get("status", STATUS_OK)
        if declared not in STATUS_PRIORITY:
            declared = STATUS_OK
        if declared == STATUS_OK:
            status = STATUS_OK
        elif STATUS_PRIORITY[declared] > STATUS_PRIORITY[STATUS_OK]:
            status = declared
        else:
            status = STATUS_OK
    else:
        status = STATUS_FAIL

    return {
        "name": name,
        "status": status,
        "details": result,
    }


def _aggregate_status(
    components: Dict[str, Dict[str, Any]]
) -> str:
    """
    聚合组件状态到整体状态。

    规则：取最坏值（fail > warn > unknown > ok）
    """
    if not components:
        return STATUS_UNKNOWN

    worst = STATUS_OK
    for c in components.values():
        s = c.get("status", STATUS_UNKNOWN)
        if STATUS_PRIORITY.get(s, 0) > STATUS_PRIORITY.get(worst, 0):
            worst = s
    return worst


def _count_by_status(
    components: Dict[str, Dict[str, Any]]
) -> Dict[str, int]:
    """按状态计数。"""
    counts: Dict[str, int] = {
        STATUS_OK: 0,
        STATUS_WARN: 0,
        STATUS_FAIL: 0,
        STATUS_UNKNOWN: 0,
    }
    for c in components.values():
        s = c.get("status", STATUS_UNKNOWN)
        if s in counts:
            counts[s] += 1
        else:
            counts[STATUS_UNKNOWN] += 1
    return counts


# ============================================================================
# 状态快照提取
# ============================================================================

def _get_state_snapshot(engine: Any) -> Dict[str, Any]:
    """状态机快照。"""
    instances = getattr(engine, "instances", {}) or {}
    sm = instances.get("state_machine")
    if sm is None:
        return {}
    return {
        "current": _to_jsonable(
            getattr(sm, "current_state", "unknown")
        ),
        "bars_in_state": int(getattr(sm, "bars_in_state", 0) or 0),
        "enter_reason": _to_jsonable(
            getattr(sm, "enter_reason", None)
        ),
    }


def _get_risk_snapshot(engine: Any) -> Dict[str, Any]:
    """风控快照。"""
    instances = getattr(engine, "instances", {}) or {}
    rp = instances.get("risk_policy")
    if rp is None:
        return {}

    result: Dict[str, Any] = {}

    # 等级
    try:
        lvl = rp.get_risk_level()
        result["level"] = _to_jsonable(lvl)
    except Exception as e:
        logger.debug(f"get_risk_level failed: {e}")

    # 健康检查里的关键字段
    hc = _safe_call(rp, "health_check")
    if isinstance(hc, dict):
        for key in RISK_SNAPSHOT_KEYS:
            if key in hc:
                result[key] = _to_jsonable(hc[key])

    return result


def _get_round_snapshot(engine: Any) -> Optional[Dict[str, Any]]:
    """活跃轮次快照。"""
    instances = getattr(engine, "instances", {}) or {}
    rm = instances.get("round_manager")
    if rm is None:
        return None
    try:
        snapshot = rm.get_active_round()
        return snapshot if isinstance(snapshot, dict) else None
    except Exception as e:
        logger.debug(f"get_active_round failed: {e}")
        return None


def _get_param_version(engine: Any) -> Optional[int]:
    """参数版本。"""
    instances = getattr(engine, "instances", {}) or {}
    ps = instances.get("param_store")
    if ps is None:
        return None
    try:
        v = ps.get_version()
        return int(v) if v is not None else None
    except Exception:
        return None


def _get_clock_snapshot(engine: Any) -> Dict[str, Any]:
    """时钟快照。"""
    instances = getattr(engine, "instances", {}) or {}
    clk = instances.get("clock")
    if clk is None:
        return {}

    result: Dict[str, Any] = {}

    # 偏移
    try:
        result["offset_ms"] = float(clk.get_market_offset_ms())
    except Exception:
        pass

    # 健康检查里的关键字段
    hc = _safe_call(clk, "health_check")
    if isinstance(hc, dict):
        for key in CLOCK_SNAPSHOT_KEYS:
            if key in hc and key not in result:
                result[key] = _to_jsonable(hc[key])

    return result


def _get_emergency_snapshot(engine: Any) -> Dict[str, Any]:
    """紧急停止快照。"""
    instances = getattr(engine, "instances", {}) or {}
    es = instances.get("emergency_stop")
    if es is None:
        return {"available": False}

    try:
        phase = es.get_phase()
        triggered = es.is_triggered()
    except Exception as e:
        logger.debug(f"emergency snapshot failed: {e}")
        return {"available": True, "error": "unavailable"}

    return {
        "available": True,
        "phase": _to_jsonable(phase),
        "triggered": bool(triggered),
    }


def _get_ws_snapshot(engine: Any) -> Dict[str, Any]:
    """WebSocket 管理器快照。"""
    instances = getattr(engine, "instances", {}) or {}
    ws = instances.get("ws_manager")
    if ws is None:
        return {}
    try:
        stats = ws.stats()
        return stats if isinstance(stats, dict) else {}
    except Exception:
        return {}


# ============================================================================
# 响应构造
# ============================================================================

def _build_status(engine: Any) -> Dict[str, Any]:
    """构造 /api/status 响应。"""
    return {
        "version": "1.0.0",
        "ts": _now_ms(),
        "mode": _to_jsonable(getattr(engine, "mode", "unknown")),
        "symbol": _to_jsonable(
            getattr(engine, "symbol", "unknown")
        ),
        "state": _get_state_snapshot(engine),
        "risk": _get_risk_snapshot(engine),
        "active_round": _get_round_snapshot(engine),
        "param_version": _get_param_version(engine),
        "clock": _get_clock_snapshot(engine),
        "emergency": _get_emergency_snapshot(engine),
        "ws": _get_ws_snapshot(engine),
    }


def _build_config(engine: Any) -> Dict[str, Any]:
    """构造 /api/config 响应。"""
    instances = getattr(engine, "instances", {}) or {}
    return {
        "version": "1.0.0",
        "api_version": "v1",
        "symbol": _to_jsonable(
            getattr(engine, "symbol", "unknown")
        ),
        "mode": _to_jsonable(getattr(engine, "mode", "unknown")),
        "step": 1,
        "ws_url": "/ws/live",
        "features": {
            "emergency_stop": instances.get("emergency_stop") is not None,
            "params": instances.get("param_store") is not None,
            "attribution": True,
            "rounds": instances.get("round_manager") is not None,
            "ws_push": instances.get("ws_manager") is not None,
        },
    }


def _build_health(engine: Any) -> Dict[str, Any]:
    """构造 /api/health 响应。"""
    instances = getattr(engine, "instances", {}) or {}

    components: Dict[str, Dict[str, Any]] = {}
    for name in HEALTH_COMPONENT_NAMES:
        components[name] = _check_component_health(
            name, instances.get(name)
        )

    overall = _aggregate_status(components)
    counts = _count_by_status(components)

    return {
        "healthy": overall == STATUS_OK,
        "status": overall,
        "version": "1.0.0",
        "ts": _now_ms(),
        "mode": _to_jsonable(getattr(engine, "mode", "unknown")),
        "symbol": _to_jsonable(
            getattr(engine, "symbol", "unknown")
        ),
        "components": components,
        "summary": counts,
        "ws": _get_ws_snapshot(engine),
    }


# ============================================================================
# Router 工厂
# ============================================================================

def create_status_router(engine: Any) -> APIRouter:
    """
    创建状态路由。

    Args:
        engine: 系统引擎（QuantSystem / LiveRunner / ShadowRunner），
                必须有 .instances 属性

    Returns:
        APIRouter，包含：
        - GET /api/status  — 完整状态快照
        - GET /api/config  — 前端启动配置
        - GET /api/health  — 健康检查

    契约：
    - 所有端点均为 GET，无需认证
    - 所有端点不抛异常（内部已处理）
    - 响应结构见 _build_* 函数
    """
    if engine is None:
        raise ValueError(
            "create_status_router: engine is required"
        )

    router = APIRouter(tags=["status"])

    # ------------------------------------------------------------------
    # GET /api/status
    # ------------------------------------------------------------------
    @router.get(
        "/api/status",
        summary="Full system status snapshot",
    )
    async def _status() -> Dict[str, Any]:
        """
        完整状态快照。

        包含：
        - state: 状态机当前状态
        - risk:  风控等级与关键指标
        - active_round: 活跃轮次（无则 null）
        - param_version: 参数版本
        - clock: 时钟状态
        - emergency: 紧急停止状态
        - ws: WebSocket 连接统计

        用途：操作员面板、状态同步的兜底。
        """
        try:
            return _build_status(engine)
        except Exception as e:
            logger.error(
                f"build_status failed: {e}", exc_info=True
            )
            return {
                "version": "1.0.0",
                "ts": _now_ms(),
                "error": "status_unavailable",
            }

    # ------------------------------------------------------------------
    # GET /api/config
    # ------------------------------------------------------------------
    @router.get(
        "/api/config",
        summary="Frontend bootstrap configuration",
    )
    async def _config() -> Dict[str, Any]:
        """
        前端启动配置。

        包含：
        - symbol / mode / step
        - ws_url: WebSocket 路径
        - features: 可用功能列表

        用途：前端初始化时获取必要的启动参数。
        """
        try:
            return _build_config(engine)
        except Exception as e:
            logger.error(
                f"build_config failed: {e}", exc_info=True
            )
            return {
                "version": "1.0.0",
                "error": "config_unavailable",
            }

    # ------------------------------------------------------------------
    # GET /api/health
    # ------------------------------------------------------------------
    @router.get(
        "/api/health",
        summary="Component-level health check",
    )
    async def _health() -> Dict[str, Any]:
        """
        健康检查。

        包含：
        - healthy: 整体是否健康
        - status:  整体状态（ok/warn/fail/unknown）
        - components: 每个组件的独立状态
        - summary: 各状态计数
        - ws: WebSocket 统计

        用途：监控系统、前端健康指示灯、运维诊断。
        """
        try:
            return _build_health(engine)
        except Exception as e:
            logger.error(
                f"build_health failed: {e}", exc_info=True
            )
            return {
                "healthy": False,
                "status": STATUS_FAIL,
                "version": "1.0.0",
                "ts": _now_ms(),
                "error": "health_unavailable",
            }

    return router
