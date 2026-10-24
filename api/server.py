"""
api/server.py — FastAPI 应用入口

前后端之间唯一的桥。

职责（唯一）：
1. 创建 FastAPI app
2. 挂载所有路由
3. 提供 WebSocket 实时推送
4. 生命周期管理（startup / shutdown）
5. 全局异常处理
6. CORS
7. 静态文件服务（前端）

不做什么：
- 不做业务逻辑（各模块负责）
- 不直接访问数据库（通过 persister）
- 不直接访问交易所（通过 gateway）
- 不做参数校验（param_store 负责）
- 不做风控（risk_policy 负责）

职责边界：
- API 层：HTTP 请求 → 调用 engine.instances 中的实例 → 序列化返回
- WebSocket：engine.instances 中的模块通过回调 → ws_manager.broadcast → 推给前端

关键契约（"七必须"）：
1. 必须健康检查端点：`/api/health`（无认证，供监控）
2. 必须配置端点：`/api/config`（前端启动时拉配置）
3. 必须状态端点：`/api/status`（完整状态）
4. 必须参数端点：`/api/params`（查询 + 更新提案）
5. 必须轮次端点：`/api/rounds`（列表 + 详情）
6. 必须归因端点：`/api/attribution`（从 persister 或 round_manager 拉数据）
7. 必须紧急停止端点：`/api/emergency_stop` + `/api/emergency_clear`（需 token）

WebSocket 契约：
- 路径：`/ws/live`
- 认证：`?token=xxx`（config 配了 token 才需要）
- 消息格式：`{type, ts, data}` 三段式
- 消息类型：`snapshot` / `kline` / `state` / `position` / `risk` / `emergency` / `emergency_clear` / `pong`
- 心跳：前端 25s ping → 后端 pong；后端 90s 无消息 → 关闭

生命周期：
- startup：
  1. 附加事件循环到 ws_manager
  2. 注册 kline_authority.on_close → broadcast("kline")
  3. 注册 emergency_stop.register_callback → broadcast("emergency")
  4. 注册 emergency_stop.register_clear_callback → broadcast("emergency_clear")
  5. 启动状态轮询任务（1s）
- shutdown：
  1. 取消状态轮询任务
  2. 关闭所有 WS 连接

认证：
- Bearer Token（`Authorization: Bearer xxx`）
- 从 config.api.token 读
- 未配置 token 时不验证（本地开发）
- 关键端点（params 更新、emergency_stop）需要认证

与 main.py 的集成：
    app = create_app(engine)         # engine 是 QuantSystem
    uvicorn.Config(app, host, port)
    uvicorn.Server(cfg).run()

    engine.instances["ws_manager"] 被填充，其他模块可主动广播：
    ws_mgr = engine.instances.get("ws_manager")
    if ws_mgr:
        ws_mgr.broadcast_threadsafe("kline", bar_dict)

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from contextlib import asynccontextmanager
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_WS_MAX_CONNECTIONS = 50
DEFAULT_STATE_POLL_INTERVAL_S = 1.0
DEFAULT_WS_RECEIVE_TIMEOUT_S = 90.0
DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 500

WS_MSG_SNAPSHOT = "snapshot"
WS_MSG_KLINE = "kline"
WS_MSG_STATE = "state"
WS_MSG_POSITION = "position"
WS_MSG_RISK = "risk"
WS_MSG_EMERGENCY = "emergency"
WS_MSG_EMERGENCY_CLEAR = "emergency_clear"
WS_MSG_PONG = "pong"


# ============================================================================
# WebSocket 连接管理
# ============================================================================
class WebSocketManager:
    """
    管理所有 WebSocket 连接 + 广播。

    契约：
    - 每个连接一个 asyncio.Lock，防止并发 send 乱序
    - 广播时遍历快照（不阻塞新连接）
    - 发送失败自动移除连接
    - 从非 asyncio 线程调用用 broadcast_threadsafe
    """

    def __init__(self, max_connections: int = DEFAULT_WS_MAX_CONNECTIONS):
        self._connections: Set[WebSocket] = set()
        self._locks: Dict[WebSocket, asyncio.Lock] = {}
        self._max_connections = int(max_connections)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._stats = {
            "connections_total": 0,
            "messages_sent": 0,
            "send_errors": 0,
            "rejected_connections": 0,
        }

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def connect(self, ws: WebSocket) -> bool:
        """接受连接。超过上限返回 False。"""
        await ws.accept()

        if len(self._connections) >= self._max_connections:
            self._stats["rejected_connections"] += 1
            try:
                await ws.close(code=4000, reason="too many connections")
            except Exception:
                pass
            return False

        self._connections.add(ws)
        self._locks[ws] = asyncio.Lock()
        self._stats["connections_total"] += 1
        logger.info(
            f"WS connected, total={len(self._connections)}"
        )
        return True

    def disconnect(self, ws: WebSocket) -> None:
        """移除连接。"""
        was_present = ws in self._connections
        self._connections.discard(ws)
        self._locks.pop(ws, None)
        if was_present:
            logger.info(
                f"WS disconnected, total={len(self._connections)}"
            )

    async def send(self, ws: WebSocket, msg: Dict[str, Any]) -> bool:
        """发送消息到单个连接。失败时自动移除。"""
        lock = self._locks.get(ws)
        if lock is None:
            return False

        try:
            async with lock:
                await ws.send_json(msg)
            self._stats["messages_sent"] += 1
            return True
        except Exception as e:
            self._stats["send_errors"] += 1
            logger.debug(f"WS send failed: {e}")
            self.disconnect(ws)
            return False

    async def broadcast(self, msg_type: str, data: Any) -> None:
        """广播消息到所有连接。"""
        if not self._connections:
            return

        msg = {
            "type": msg_type,
            "ts": int(time.time() * 1000),
            "data": data,
        }

        # 快照，防止遍历时修改
        targets = list(self._connections)
        tasks = [self.send(ws, msg) for ws in targets]
        await asyncio.gather(*tasks, return_exceptions=True)

    def broadcast_threadsafe(self, msg_type: str, data: Any) -> None:
        """
        从非 asyncio 线程广播。

        契约：
        - 没有 loop 或没有连接时静默返回
        - 用 run_coroutine_threadsafe 投递
        - 失败只打日志，不抛异常
        """
        if self._loop is None or not self._connections:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self.broadcast(msg_type, data), self._loop
            )
        except RuntimeError:
            # loop 已关闭
            pass
        except Exception as e:
            logger.error(f"broadcast_threadsafe failed: {e}")

    async def close_all(self, code: int = 1001, reason: str = "shutdown") -> None:
        """关闭所有连接。用于 shutdown。"""
        targets = list(self._connections)
        for ws in targets:
            try:
                await ws.close(code=code, reason=reason)
            except Exception:
                pass
        self._connections.clear()
        self._locks.clear()

    def connection_count(self) -> int:
        return len(self._connections)

    def stats(self) -> Dict[str, Any]:
        return {
            "active_connections": len(self._connections),
            "max_connections": self._max_connections,
            **self._stats,
        }


# ============================================================================
# 认证依赖
# ============================================================================
def _make_auth_dependency(engine: Any):
    """
    创建认证依赖。配置了 token 则验证，否则跳过。
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
# 辅助函数
# ============================================================================

def _to_ms(ts: Any, default: int = 0) -> int:
    """安全转换为毫秒时间戳。"""
    try:
        v = int(ts)
        return v if v > 0 else default
    except (ValueError, TypeError):
        return default


def _bar_to_dict(bar: Any) -> Dict[str, Any]:
    """KlineData → dict。"""
    return {
        "ts": _to_ms(getattr(bar, "ts", 0)),
        "open": str(getattr(bar, "open", "0")),
        "high": str(getattr(bar, "high", "0")),
        "low": str(getattr(bar, "low", "0")),
        "close": str(getattr(bar, "close", "0")),
        "volume": str(getattr(bar, "volume", "0")),
        "is_closed": bool(getattr(bar, "is_closed", True)),
    }


def _safe_get_risk_level(risk_policy: Any) -> Optional[str]:
    """安全获取风控等级字符串。"""
    if risk_policy is None:
        return None
    try:
        lvl = risk_policy.get_risk_level()
        return lvl.value if hasattr(lvl, "value") else str(lvl)
    except Exception:
        return None


def _safe_check_health(obj: Any, preferred_method: str = "health_check") -> Dict[str, Any]:
    """安全调用组件健康检查。"""
    if obj is None:
        return {"healthy": False, "error": "not_available"}
    try:
        fn = getattr(obj, preferred_method, None)
        if fn is None:
            fn = getattr(obj, "health", None)
        if fn is None:
            return {"healthy": True, "note": "no_health_method"}
        result = fn()
        if isinstance(result, dict):
            return result
        return {"healthy": bool(result)}
    except Exception as e:
        return {"healthy": False, "error": str(e)}


def _build_snapshot(engine: Any) -> Dict[str, Any]:
    """构造初始快照，前端首次连接时推送。"""
    instances = getattr(engine, "instances", {}) or {}
    snapshot: Dict[str, Any] = {
        "ts": int(time.time() * 1000),
        "mode": getattr(engine, "mode", "unknown"),
        "symbol": getattr(engine, "symbol", "unknown"),
    }

    sm = instances.get("state_machine")
    if sm is not None:
        snapshot["state"] = {
            "current": getattr(sm, "current_state", "unknown"),
            "bars_in_state": getattr(sm, "bars_in_state", 0),
            "enter_reason": getattr(sm, "enter_reason", None),
        }

    risk_level = _safe_get_risk_level(instances.get("risk_policy"))
    if risk_level:
        snapshot["risk"] = {"level": risk_level}

    rm = instances.get("round_manager")
    if rm is not None:
        try:
            snapshot["active_round"] = rm.get_active_round()
        except Exception:
            snapshot["active_round"] = None

    ps = instances.get("param_store")
    if ps is not None:
        try:
            snapshot["param_version"] = ps.get_version()
        except Exception:
            pass

    ka = instances.get("kline_authority")
    if ka is not None and hasattr(ka, "get_recent_bars"):
        try:
            bars = ka.get_recent_bars(200)
            snapshot["bars"] = [_bar_to_dict(b) for b in bars]
        except Exception:
            pass

    return snapshot


# ============================================================================
# 状态轮询任务
# ============================================================================
async def _state_poll_loop(
    engine: Any,
    ws_manager: WebSocketManager,
    interval_s: float = DEFAULT_STATE_POLL_INTERVAL_S,
) -> None:
    """
    定期检查状态变化并广播。

    契约：
    - 每 interval_s 检查一次
    - 只有变化时才广播
    - 任何异常不中断循环
    """
    instances = getattr(engine, "instances", {}) or {}
    last_state = None
    last_risk = None
    last_round_id = None

    # 初始化为当前值（避免首轮立即广播）
    sm0 = instances.get("state_machine")
    if sm0 is not None:
        last_state = getattr(sm0, "current_state", None)
    last_risk = _safe_get_risk_level(instances.get("risk_policy"))
    rm0 = instances.get("round_manager")
    if rm0 is not None:
        try:
            snap = rm0.get_active_round()
            last_round_id = snap.get("round_id") if snap else None
        except Exception:
            last_round_id = None

    while True:
        try:
            await asyncio.sleep(interval_s)

            sm = instances.get("state_machine")
            if sm is not None:
                cur = getattr(sm, "current_state", None)
                if cur and cur != last_state:
                    last_state = cur
                    ws_manager.broadcast_threadsafe(WS_MSG_STATE, {
                        "state": cur,
                        "bars_in_state": getattr(sm, "bars_in_state", 0),
                        "enter_reason": getattr(sm, "enter_reason", None),
                    })

            risk_level = _safe_get_risk_level(instances.get("risk_policy"))
            if risk_level and risk_level != last_risk:
                last_risk = risk_level
                ws_manager.broadcast_threadsafe(
                    WS_MSG_RISK, {"level": risk_level}
                )

            rm = instances.get("round_manager")
            if rm is not None:
                try:
                    snap = rm.get_active_round()
                    cur_id = snap.get("round_id") if snap else None
                    if cur_id != last_round_id:
                        last_round_id = cur_id
                        ws_manager.broadcast_threadsafe(
                            WS_MSG_POSITION, snap or {}
                        )
                except Exception:
                    pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"state poll error: {e}", exc_info=True)


# ============================================================================
# 路由注册
# ============================================================================
def _register_routes(
    app: FastAPI,
    engine: Any,
    ws_manager: WebSocketManager,
) -> None:
    """注册所有 API 路由。"""
    instances = getattr(engine, "instances", {}) or {}
    auth = _make_auth_dependency(engine)

    # ------------------------------------------------------------------
    # /api/health — 健康检查（无认证）
    # ------------------------------------------------------------------
    @app.get("/api/health", tags=["system"])
    async def _health() -> Dict[str, Any]:
        components: Dict[str, Dict[str, Any]] = {}
        components["state_machine"] = _safe_check_health(
            instances.get("state_machine")
        )
        components["risk_policy"] = _safe_check_health(
            instances.get("risk_policy")
        )
        components["emergency_stop"] = _safe_check_health(
            instances.get("emergency_stop"), preferred_method="health"
        )
        components["kline_authority"] = _safe_check_health(
            instances.get("kline_authority")
        )
        components["rest_client"] = _safe_check_health(
            instances.get("rest_client")
        )
        components["persister"] = _safe_check_health(
            instances.get("persister")
        )

        overall = all(
            bool(c.get("healthy", False)) for c in components.values()
        )

        return {
            "healthy": overall,
            "ts": int(time.time() * 1000),
            "mode": getattr(engine, "mode", "unknown"),
            "symbol": getattr(engine, "symbol", "unknown"),
            "components": components,
            "ws": ws_manager.stats(),
        }

    # ------------------------------------------------------------------
    # /api/config — 前端启动配置
    # ------------------------------------------------------------------
    @app.get("/api/config", tags=["system"])
    async def _config() -> Dict[str, Any]:
        has_emergency = instances.get("emergency_stop") is not None
        has_params = instances.get("param_store") is not None
        has_attribution = True

        return {
            "symbol": getattr(engine, "symbol", "unknown"),
            "mode": getattr(engine, "mode", "unknown"),
            "step": 1,
            "version": "1.0.0",
            "ws_url": "/ws/live",
            "features": {
                "emergency_stop": has_emergency,
                "params": has_params,
                "attribution": has_attribution,
            },
        }

    # ------------------------------------------------------------------
    # /api/status — 完整状态
    # ------------------------------------------------------------------
    @app.get("/api/status", tags=["system"])
    async def _status() -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "mode": getattr(engine, "mode", "unknown"),
            "symbol": getattr(engine, "symbol", "unknown"),
        }

        sm = instances.get("state_machine")
        if sm is not None:
            result["state"] = {
                "current": getattr(sm, "current_state", "unknown"),
                "bars_in_state": getattr(sm, "bars_in_state", 0),
                "enter_reason": getattr(sm, "enter_reason", None),
            }

        risk_level = _safe_get_risk_level(instances.get("risk_policy"))
        if risk_level:
            result["risk"] = {"level": risk_level}
            rp = instances.get("risk_policy")
            if hasattr(rp, "health_check"):
                try:
                    result["risk"].update(rp.health_check())
                except Exception:
                    pass

        rm = instances.get("round_manager")
        if rm is not None:
            try:
                result["active_round"] = rm.get_active_round()
            except Exception:
                result["active_round"] = None

        ps = instances.get("param_store")
        if ps is not None:
            try:
                result["param_version"] = ps.get_version()
            except Exception:
                pass

        es = instances.get("emergency_stop")
        if es is not None:
            try:
                result["emergency"] = {
                    "phase": es.get_phase().value,
                    "triggered": es.is_triggered(),
                }
            except Exception:
                result["emergency"] = {"error": "unavailable"}

        clock = instances.get("clock")
        if clock is not None:
            try:
                result["clock"] = {
                    "offset_ms": clock.get_market_offset_ms(),
                }
            except Exception:
                pass

        return result

    # ------------------------------------------------------------------
    # /api/params — 参数查询与更新提案
    # ------------------------------------------------------------------
    @app.get("/api/params", tags=["params"])
    async def _get_params() -> Dict[str, Any]:
        ps = instances.get("param_store")
        if ps is None:
            raise HTTPException(503, "param_store not available")
        return {
            "version": ps.get_version(),
            "params": ps.get_all(),
            "pending": ps.get_pending(),
        }

    @app.put("/api/params", tags=["params"], dependencies=[Depends(auth)])
    async def _update_params(body: Dict[str, Any]) -> Dict[str, Any]:
        ps = instances.get("param_store")
        if ps is None:
            raise HTTPException(503, "param_store not available")

        changes = body.get("changes") or {}
        operator = body.get("operator") or "api"
        reason = body.get("reason") or ""

        if not isinstance(changes, dict) or not changes:
            raise HTTPException(400, "changes must be a non-empty object")
        if not reason:
            raise HTTPException(400, "reason is required")

        ok, errors = ps.propose_update(
            changes=changes,
            operator=operator,
            reason=reason,
        )
        return {
            "ok": bool(ok),
            "errors": errors,
            "pending": ps.get_pending() if ok else None,
        }

    # ------------------------------------------------------------------
    # /api/rounds — 轮次列表与详情
    # ------------------------------------------------------------------
    @app.get("/api/rounds", tags=["rounds"])
    async def _rounds(
        limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
        start_ts: Optional[int] = Query(None),
        end_ts: Optional[int] = Query(None),
    ) -> Dict[str, Any]:
        rm = instances.get("round_manager")
        if rm is None:
            raise HTTPException(503, "round_manager not available")

        try:
            rounds: List[Dict[str, Any]] = rm.list_closed_rounds() or []
        except Exception as e:
            raise HTTPException(500, f"failed to list rounds: {e}")

        if start_ts is not None:
            rounds = [r for r in rounds if _to_ms(r.get("closed_ts")) >= start_ts]
        if end_ts is not None:
            rounds = [r for r in rounds if _to_ms(r.get("closed_ts")) < end_ts]

        rounds = rounds[-limit:]
        return {"count": len(rounds), "rounds": rounds}

    @app.get("/api/rounds/{round_id}", tags=["rounds"])
    async def _round_detail(round_id: str) -> Dict[str, Any]:
        rm = instances.get("round_manager")
        if rm is None:
            raise HTTPException(503, "round_manager not available")

        try:
            detail = rm.get_round(round_id)
        except Exception as e:
            raise HTTPException(500, f"get_round failed: {e}")

        if detail is None:
            raise HTTPException(404, f"round not found: {round_id}")
        return detail

    # ------------------------------------------------------------------
    # /api/attribution — 归因报告
    # ------------------------------------------------------------------
    @app.get("/api/attribution", tags=["analytics"])
    async def _attribution(
        start_ts: Optional[int] = Query(None),
        end_ts: Optional[int] = Query(None),
    ) -> Dict[str, Any]:
        try:
            from analytics.round_attribution import RoundAttribution
        except ImportError as e:
            raise HTTPException(500, f"analytics unavailable: {e}")

        rounds: List[Dict[str, Any]] = []

        persister = instances.get("persister")
        if persister is not None and hasattr(persister, "load_rounds"):
            try:
                rounds = persister.load_rounds(
                    start_ts or 0,
                    end_ts or int(time.time() * 1000),
                ) or []
            except Exception as e:
                logger.warning(f"persister.load_rounds failed: {e}")

        if not rounds:
            rm = instances.get("round_manager")
            if rm is not None:
                try:
                    rounds = rm.list_closed_rounds() or []
                except Exception:
                    rounds = []

        try:
            report = RoundAttribution().analyze(
                rounds, start_ts=start_ts, end_ts=end_ts
            )
        except Exception as e:
            raise HTTPException(500, f"attribution failed: {e}")

        return report.to_dict()

    # ------------------------------------------------------------------
    # /api/emergency_stop — 触发紧急停止
    # ------------------------------------------------------------------
    @app.post(
        "/api/emergency_stop",
        tags=["emergency"],
        dependencies=[Depends(auth)],
    )
    async def _emergency_stop(body: Dict[str, Any]) -> Dict[str, Any]:
        es = instances.get("emergency_stop")
        if es is None:
            raise HTTPException(503, "emergency_stop not available")

        reason = body.get("reason") or "api_trigger"
        operator = body.get("operator") or "api"

        try:
            from risk.emergency_stop import TriggerSource
        except ImportError:
            class TriggerSource:
                EXTERNAL_API = "external_api"

        try:
            event = es.trigger(
                source=TriggerSource.EXTERNAL_API,
                reason=reason,
                operator=operator,
            )
        except Exception as e:
            logger.error(f"emergency_stop failed: {e}", exc_info=True)
            raise HTTPException(500, f"emergency_stop failed: {e}")

        return {
            "ok": True,
            "event": event.to_dict() if hasattr(event, "to_dict") else {},
        }

    # ------------------------------------------------------------------
    # /api/emergency_clear — 清除紧急停止
    # ------------------------------------------------------------------
    @app.post(
        "/api/emergency_clear",
        tags=["emergency"],
        dependencies=[Depends(auth)],
    )
    async def _emergency_clear(body: Dict[str, Any]) -> Dict[str, Any]:
        es = instances.get("emergency_stop")
        if es is None:
            raise HTTPException(503, "emergency_stop not available")

        operator = body.get("operator") or "api"
        reason = body.get("reason") or ""
        if not reason:
            raise HTTPException(400, "reason is required")

        try:
            ok = es.clear(operator=operator, reason=reason)
        except Exception as e:
            logger.error(f"emergency_clear failed: {e}", exc_info=True)
            raise HTTPException(500, f"emergency_clear failed: {e}")

        return {"ok": bool(ok)}

    # ------------------------------------------------------------------
    # /ws/live — WebSocket 实时推送
    # ------------------------------------------------------------------
    @app.websocket("/ws/live")
    async def _ws_live(ws: WebSocket) -> None:
        # 认证（如果配置了 token）
        config = getattr(engine, "config", {}) or {}
        token = (config.get("api", {}) or {}).get("token") or None
        if token:
            query_token = ws.query_params.get("token")
            if query_token != token:
                await ws.close(code=4001, reason="unauthorized")
                return

        connected = await ws_manager.connect(ws)
        if not connected:
            return

        # 初始快照
        try:
            snapshot = _build_snapshot(engine)
            await ws_manager.send(ws, {
                "type": WS_MSG_SNAPSHOT,
                "ts": int(time.time() * 1000),
                "data": snapshot,
            })
        except Exception as e:
            logger.error(f"send snapshot failed: {e}")

        # 接收循环
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(
                        ws.receive_text(),
                        timeout=DEFAULT_WS_RECEIVE_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    try:
                        await ws.close(
                            code=4002, reason="receive timeout"
                        )
                    except Exception:
                        pass
                    break

                if not raw:
                    continue

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if not isinstance(msg, dict):
                    continue

                msg_type = msg.get("type")
                if msg_type == "ping":
                    try:
                        await ws.send_json({
                            "type": WS_MSG_PONG,
                            "ts": int(time.time() * 1000),
                        })
                    except Exception:
                        break

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error(f"ws loop error: {e}", exc_info=True)
        finally:
            ws_manager.disconnect(ws)


# ============================================================================
# App 工厂
# ============================================================================
def create_app(engine: Any) -> FastAPI:
    """
    创建 FastAPI app。

    契约：
    - 幂等：重复调用复用已有 ws_manager
    - 生命周期：startup 注册回调，shutdown 清理
    - 全局异常处理
    - CORS
    - 静态文件服务（前端）

    Args:
        engine: 系统引擎实例（QuantSystem / LiveRunner / ShadowRunner），
                必须有 .instances 和 .config
    """
    if engine is None:
        raise ValueError("create_app: engine is required")

    instances = getattr(engine, "instances", None)
    if instances is None:
        raise ValueError(
            "create_app: engine must have .instances attribute"
        )

    config = getattr(engine, "config", {}) or {}

    # ---- WebSocket Manager（复用或创建）----
    existing_ws = instances.get("ws_manager")
    if existing_ws is not None:
        logger.warning(
            "create_app called twice, reusing existing ws_manager"
        )
        ws_manager = existing_ws
    else:
        max_conn = int(
            (config.get("api", {}) or {}).get(
                "ws_max_connections", DEFAULT_WS_MAX_CONNECTIONS
            )
        )
        ws_manager = WebSocketManager(max_connections=max_conn)
        instances["ws_manager"] = ws_manager

    # ---- Lifespan ----
    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        ws_manager.attach_loop(loop)

        logger.info(
            f"API startup: mode={getattr(engine, 'mode', 'unknown')} "
            f"symbol={getattr(engine, 'symbol', 'unknown')}"
        )

        # 注册 K 线回调
        ka = instances.get("kline_authority")
        if ka is not None and hasattr(ka, "register_on_close"):
            def _on_bar(bar: Any, snapshot: Any, trades: Any) -> None:
                try:
                    ws_manager.broadcast_threadsafe(
                        WS_MSG_KLINE, _bar_to_dict(bar)
                    )
                except Exception as e:
                    logger.error(f"kline broadcast failed: {e}")
            try:
                ka.register_on_close(_on_bar)
            except Exception as e:
                logger.error(f"register kline callback failed: {e}")

        # 注册紧急停止回调
        es = instances.get("emergency_stop")
        if es is not None and hasattr(es, "register_callback"):
            def _on_emergency(event: Any) -> None:
                try:
                    data = event.to_dict() if hasattr(event, "to_dict") else {"event": str(event)}
                    ws_manager.broadcast_threadsafe(WS_MSG_EMERGENCY, data)
                except Exception as e:
                    logger.error(f"emergency broadcast failed: {e}")
            try:
                es.register_callback(_on_emergency)
            except Exception as e:
                logger.error(f"register emergency callback failed: {e}")

        if es is not None and hasattr(es, "register_clear_callback"):
            def _on_emergency_clear(event: Any) -> None:
                try:
                    data = event.to_dict() if (event and hasattr(event, "to_dict")) else {}
                    ws_manager.broadcast_threadsafe(
                        WS_MSG_EMERGENCY_CLEAR, data
                    )
                except Exception as e:
                    logger.error(f"emergency_clear broadcast failed: {e}")
            try:
                es.register_clear_callback(_on_emergency_clear)
            except Exception as e:
                logger.error(
                    f"register emergency_clear callback failed: {e}"
                )

        # 启动状态轮询
        poll_task = asyncio.create_task(
            _state_poll_loop(engine, ws_manager, DEFAULT_STATE_POLL_INTERVAL_S)
        )

        try:
            yield
        finally:
            logger.info("API shutdown")
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"poll task shutdown error: {e}")

            try:
                await ws_manager.close_all(code=1001, reason="server shutdown")
            except Exception as e:
                logger.error(f"close_all WS failed: {e}")

    # ---- App ----
    debug_enabled = bool(
        (config.get("api", {}) or {}).get("debug", False)
    )

    app = FastAPI(
        title="Quant System API",
        description="Step 1 minimal trading system API",
        version="1.0.0",
        lifespan=_lifespan,
        docs_url="/api/docs" if debug_enabled else None,
        redoc_url=None,
        openapi_url="/api/openapi.json" if debug_enabled else None,
    )

    app.state.engine = engine
    app.state.ws_manager = ws_manager

    # ---- CORS ----
    cors_origins = (config.get("api", {}) or {}).get("cors_origins")
    if not cors_origins:
        cors_origins = [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://localhost:3000",
            "http://localhost:5173",
        ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
        allow_credentials=False,
    )

    # ---- 全局异常 ----
    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.error(
            f"Unhandled exception on "
            f"{request.method} {request.url.path}: {exc}",
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error"},
        )

    # ---- 路由 ----
    _register_routes(app, engine, ws_manager)

    # ---- 静态文件（最后挂载，避免吞路由）----
    frontend_dir = _resolve_frontend_dir(config)
    if frontend_dir is not None and frontend_dir.exists():
        try:
            app.mount(
                "/",
                StaticFiles(directory=str(frontend_dir), html=True),
                name="frontend",
            )
            logger.info(f"Frontend mounted at / from {frontend_dir}")
        except Exception as e:
            logger.error(f"mount frontend failed: {e}")
    else:
        logger.warning(
            f"frontend directory not found: {frontend_dir}, "
            f"serving API only"
        )

        @app.get("/")
        async def _root() -> Dict[str, Any]:
            return {
                "message": "API running, frontend not mounted",
                "docs": "/api/docs" if debug_enabled else None,
            }

    return app


def _resolve_frontend_dir(config: Dict[str, Any]) -> Optional[Path]:
    """解析前端目录路径。"""
    api_cfg = config.get("api", {}) or {}
    frontend = api_cfg.get("frontend_dir", "frontend")
    if not frontend:
        return None

    path = Path(frontend)
    if path.is_absolute():
        return path

    system_root = (config.get("system", {}) or {}).get("root", ".")
    return Path(system_root) / path


# ============================================================================
# 便捷函数（供调试 / 测试）
# ============================================================================
def run_dev(engine: Any, host: str = "0.0.0.0", port: int = 8000) -> None:
    """开发环境启动。生产环境用 uvicorn.Server。"""
    try:
        import uvicorn
    except ImportError:
        raise RuntimeError(
            "uvicorn is required for run_dev()"
        )

    app = create_app(engine)
    uvicorn.run(app, host=host, port=port, log_level="info")
