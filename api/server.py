"""
api/server.py — FastAPI 服务入口 (重铸标准版)

核心设计原则：
1. 依赖注入：接收已经初始化完毕的 engine，绝不在此处手工装配组件。
2. 职责单一：只负责 HTTP 路由挂载、WebSocket 连接管理与静态文件服务。
3. 线程安全广播：WebSocketManager 支持跨线程（如同步的 K 线读取线程）安全推送消息。
4. 标准生命周期：接入 FastAPI lifespan，启动时挂载回调，停机时清理所有连接。
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Set

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routes_emergency import create_emergency_router
from api.routes_status import create_status_router

logger = logging.getLogger(__name__)

# ============================================================================
# WebSocket 连接与广播管理
# ============================================================================
class WebSocketManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self.loop = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(f"WebSocket 客户端已连接，当前总数: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info(f"WebSocket 客户端已断开，当前总数: {len(self.active_connections)}")

    async def broadcast(self, msg_type: str, data: dict):
        """向所有客户端广播数据"""
        if not self.active_connections:
            return
            
        msg = {"type": msg_type, "ts": int(time.time() * 1000), "data": data}
        disconnected = set()
        
        async with self._lock:
            connections = list(self.active_connections)
            
        for ws in connections:
            try:
                await ws.send_json(msg)
            except Exception:
                disconnected.add(ws)
                
        for ws in disconnected:
            await self.disconnect(ws)

    def broadcast_threadsafe(self, msg_type: str, data: dict):
        """供非异步线程（如 WS 数据流线程）调用"""
        if self.loop and self.loop.is_running() and self.active_connections:
            asyncio.run_coroutine_threadsafe(self.broadcast(msg_type, data), self.loop)


# ============================================================================
# FastAPI 实例创建
# ============================================================================

def create_app(engine: Any) -> FastAPI:
    """
    创建并配置 FastAPI 应用。
    """
    if not engine:
        raise ValueError("create_app 必须传入有效的 engine")

    # 从 engine 中提取依赖
    config = getattr(engine, "config", {})
    ws_manager = WebSocketManager()
    
    # 将 ws_manager 挂载到 engine 的实例中，供 K 线回调等使用
    if hasattr(engine, "instances"):
        engine.instances["ws_manager"] = ws_manager

    # ---- Lifespan 生命周期 ----
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 启动阶段
        ws_manager.set_loop(asyncio.get_running_loop())
        logger.info("API 服务启动，WebSocket 管理器已就绪")
        
        # 可以在这里给 engine 里的数据源注册 WebSocket 广播回调
        # 例如：kline_authority.register_on_close(lambda bar, *_: ws_manager.broadcast_threadsafe("kline", ...))
        
        yield
        
        # 停机阶段
        logger.info("API 服务正在停机，清理所有 WebSocket 连接...")
        async with ws_manager._lock:
            for ws in list(ws_manager.active_connections):
                try:
                    await ws.close()
                except:
                    pass
            ws_manager.active_connections.clear()

    app = FastAPI(
        title="Quant Trading System API",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/api/docs"
    )

    # ---- CORS 跨域配置 ----
    cors_origins = config.get("api", {}).get("cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- 全局异常拦截 ----
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"API 未捕获异常: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "internal_server_error", "detail": str(exc)}
        )

    # ---- 挂载 RESTful 路由 ----
    app.include_router(create_status_router(engine), prefix="/api", tags=["status"])
    app.include_router(create_emergency_router(engine), prefix="/api", tags=["emergency"])

    # ---- WebSocket 路由 ----
    @app.websocket("/ws/live")
    async def websocket_endpoint(websocket: WebSocket):
        await ws_manager.connect(websocket)
        
        # 连上后先推送历史快照
        try:
            recent_bars = engine.instances.get("buffer", {}).get_recent(100) if hasattr(engine, "instances") else []
            if recent_bars:
                history_data = [{"ts": b.ts, "open": float(b.open), "high": float(b.high), "low": float(b.low), "close": float(b.close), "volume": float(b.volume)} for b in recent_bars]
                await websocket.send_json({"type": "history", "ts": int(time.time()*1000), "data": history_data})
        except Exception as e:
            logger.error(f"推送初始历史数据失败: {e}")

        # 维持连接
        try:
            while True:
                # 接收前端心跳或指令
                msg = await websocket.receive_text()
                if msg == "ping":
                    await websocket.send_text("pong")
        except WebSocketDisconnect:
            await ws_manager.disconnect(websocket)
        except Exception as e:
            logger.warning(f"WebSocket 异常: {e}")
            await ws_manager.disconnect(websocket)

    # ---- 静态文件服务 (必须挂载在最后) ----
    frontend_dir = Path("frontend")
    if frontend_dir.exists() and frontend_dir.is_dir():
        app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")
        logger.info("已挂载前端静态文件目录: /frontend")
    else:
        logger.warning("未找到 frontend 目录，仅提供 API 服务。")

    return app


# ============================================================================
# 独立启动工具 (可选，通常由 main.py 启动)
# ============================================================================
def run_server(app: FastAPI, host: str = "0.0.0.0", port: int = 8000):
    """阻塞式运行 FastAPI"""
    logger.info(f"启动 API 服务: http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
