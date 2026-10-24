/**
 * js/ws_client.js — WebSocket 客户端
 * ============================================================================
 *
 * 定位：
 * - 前端所有模块的数据入口
 * - kline_chart / state_panel / position_panel 都订阅它，不直接连 WebSocket
 * - 唯一能感知"后端活没活"的模块
 *
 * 关键契约（"七必须"）：
 * 1. 必须自动重连：指数退避 + 随机抖动，10 次后固定 30s 低频
 * 2. 必须心跳：25s 一次 ping，60s 无消息视为断线
 * 3. 必须资源完整：dispose() 后无 timer、无 ws、无订阅者
 * 4. 必须幂等：连接过程中再次 connect 不重复创建
 * 5. 必须单连接：同一时间只有一个 ws，旧连接回调丢弃
 * 6. 必须异常隔离：单个订阅者异常不影响其他
 * 7. 必须消息大小限制：64KB 上限，超大消息丢弃
 *
 * 消息格式（约定）：
 * - 后端推送：{ type, ts, data }
 * - 前端不推消息（除 ping 外）
 * - 未知 type 静默分发到通配符订阅者，不崩
 *
 * 后端必须实现的协议：
 * - 收到 { type: 'ping' } 返回 { type: 'pong' }
 * - 否则 60 秒无消息，前端会认为断线并重连
 *
 * 重连策略：
 * - 前 10 次：指数退避 1s → 2s → 4s → ... → 30s
 * - 10 次后：固定 30s 重连
 * - 所有延迟带 ±20% 抖动，避免多客户端同时重连
 * - 每次重连都会通过 onStateChange 通知，UI 应显示"重连中 (第 N 次)"
 *
 * 前端订阅者应处理的场景：
 * - 首次连接（state='connected', reconnectCount=0）：加载历史数据
 * - 重连成功（state='connected', reconnectCount>0）：请求 snapshot 同步状态
 * - 断线期间的消息会丢失，订阅者应设计幂等的状态更新逻辑
 *
 * 资源清理：
 * - dispose() 后：ws 关闭、所有 timer 清除、订阅者清空、状态重置
 * - disconnect() 后：ws 关闭、不重连，但订阅者和配置保留（可重新 connect）
 *
 * 浏览器兼容性：
 * - 依赖原生 WebSocket（所有现代浏览器支持）
 * - 依赖 location（Node.js 环境需手动传 url）
 *
 * 作者：比蒙
 * 审查：乐高（执行）、张三（系统）
 * 版本：v1.0.0
 * ============================================================================
 */

// ============================================================================
// 常量
// ============================================================================

/**
 * 消息类型常量。与后端推送的 type 字段对应。
 */
export const MSG_TYPES = Object.freeze({
    KLINE: 'kline',
    STATE: 'state',
    POSITION: 'position',
    TRADE: 'trade',
    RISK: 'risk',
    HEALTH: 'health',
    EMERGENCY: 'emergency',
    SNAPSHOT: 'snapshot',
    PONG: 'pong',
    ERROR: 'error',
});

/**
 * 连接状态。
 */
export const CONN_STATES = Object.freeze({
    DISCONNECTED: 'disconnected',
    CONNECTING: 'connecting',
    CONNECTED: 'connected',
    RECONNECTING: 'reconnecting',
});

const DEFAULT_RECONNECT = Object.freeze({
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    multiplier: 2,
    jitter: 0.2,
    maxFastRetries: 10,
});

const DEFAULT_HEARTBEAT = Object.freeze({
    intervalMs: 25000,
    timeoutMs: 60000,
});

const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_MESSAGE_LENGTH = MAX_MESSAGE_BYTES;


// ============================================================================
// 工具
// ============================================================================

function defaultUrl() {
    if (typeof location === 'undefined' || !location.host) {
        return 'ws://localhost:8000/ws/live';
    }
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${location.host}/ws/live`;
}


// ============================================================================
// WsClient 类
// ============================================================================
export class WsClient {

    /**
     * @param {Object} options
     * @param {string} [options.url]                 WebSocket URL，默认从 location 推导
     * @param {Object} [options.reconnect]           { initialDelayMs, maxDelayMs, multiplier, jitter, maxFastRetries }
     * @param {Object} [options.heartbeat]           { intervalMs, timeoutMs }
     * @param {Function} [options.onStateChange]     (state, meta) => void
     * @param {Function} [options.onError]           (error) => void
     */
    constructor(options = {}) {
        this._url = options.url || defaultUrl();
        this._reconnectCfg = { ...DEFAULT_RECONNECT, ...(options.reconnect || {}) };
        this._heartbeatCfg = { ...DEFAULT_HEARTBEAT, ...(options.heartbeat || {}) };
        this._onStateChange = options.onStateChange || null;
        this._onError = options.onError || null;

        // ---- 连接状态 ----
        this._state = CONN_STATES.DISCONNECTED;
        this._ws = null;
        this._connectedAt = null;
        this._reconnectCount = 0;
        this._lastMessageTs = null;
        this._manualDisconnect = false;
        this._disposed = false;

        // ---- 定时器 ----
        this._reconnectTimer = null;
        this._heartbeatTimer = null;
        this._heartbeatTimeoutTimer = null;

        // ---- 订阅者 ----
        this._subscribers = new Map();

        // ---- 统计 ----
        this._stats = {
            messagesReceived: 0,
            messagesDispatched: 0,
            malformedMessages: 0,
            oversizedMessages: 0,
            handlerErrors: 0,
            pingSent: 0,
        };
    }

    // ========================================================================
    // 公共 API
    // ========================================================================

    /**
     * 建立连接。如果已连接或正在连接，不重复创建。
     */
    connect() {
        if (this._disposed) return;
        if (this._ws && (
            this._ws.readyState === WebSocket.OPEN ||
            this._ws.readyState === WebSocket.CONNECTING
        )) {
            return;
        }
        this._manualDisconnect = false;
        this._connect();
    }

    /**
     * 主动断开连接。不自动重连，但保留订阅者和配置（可重新 connect）。
     */
    disconnect() {
        this._manualDisconnect = true;
        this._stopHeartbeat();
        this._clearReconnectTimer();
        this._closeWebSocket();
        this._setState(CONN_STATES.DISCONNECTED, { manual: true });
    }

    /**
     * 销毁。不可恢复。
     */
    dispose() {
        if (this._disposed) return;
        this._disposed = true;
        this._manualDisconnect = true;

        this._stopHeartbeat();
        this._clearReconnectTimer();
        this._closeWebSocket();

        this._subscribers.clear();
        this._state = CONN_STATES.DISCONNECTED;
        this._ws = null;
        this._connectedAt = null;
        this._lastMessageTs = null;
    }

    /**
     * 订阅消息。
     * @param {string} type  消息类型，或 '*' 表示通配符
     * @param {Function} handler
     * @returns {Function} 取消订阅函数
     */
    subscribe(type, handler) {
        if (typeof type !== 'string' || !type) {
            throw new Error('subscribe: type must be a non-empty string');
        }
        if (typeof handler !== 'function') {
            throw new Error('subscribe: handler must be a function');
        }

        let handlers = this._subscribers.get(type);
        if (!handlers) {
            handlers = [];
            this._subscribers.set(type, handlers);
        }
        handlers.push(handler);

        return () => this._unsubscribe(type, handler);
    }

    /**
     * 获取当前状态快照。
     */
    getState() {
        return {
            state: this._state,
            connectedAt: this._connectedAt,
            reconnectCount: this._reconnectCount,
            lastMessageTs: this._lastMessageTs,
        };
    }

    /**
     * 获取健康度。用于监控面板展示。
     */
    getHealth() {
        const now = Date.now();
        const lastMessageAge = this._lastMessageTs ? now - this._lastMessageTs : null;
        const connectedFor = this._connectedAt ? now - this._connectedAt : 0;
        const heartbeatTimeout = this._heartbeatCfg.timeoutMs;

        return {
            state: this._state,
            connected: this._state === CONN_STATES.CONNECTED,
            connectedAt: this._connectedAt,
            connectedFor,
            reconnectCount: this._reconnectCount,
            lastMessageTs: this._lastMessageTs,
            lastMessageAge,
            healthy: this._state === CONN_STATES.CONNECTED
                && lastMessageAge !== null
                && lastMessageAge < heartbeatTimeout,
            stats: { ...this._stats },
        };
    }

    // ========================================================================
    // 连接管理
    // ========================================================================

    _connect() {
        if (this._disposed) return;

        // 清理旧连接
        this._closeWebSocket();

        const isReconnect = this._reconnectCount > 0;
        this._setState(
            isReconnect ? CONN_STATES.RECONNECTING : CONN_STATES.CONNECTING,
            { reconnectCount: this._reconnectCount, url: this._url }
        );

        let ws;
        try {
            ws = new WebSocket(this._url);
        } catch (e) {
            this._handleError(e);
            this._scheduleReconnect();
            return;
        }

        this._ws = ws;

        ws.onopen = (event) => {
            if (this._ws !== ws || this._disposed) return;
            this._handleOpen();
        };

        ws.onmessage = (event) => {
            if (this._ws !== ws || this._disposed) return;
            this._handleMessage(event);
        };

        ws.onerror = (event) => {
            if (this._ws !== ws || this._disposed) return;
            // onerror 只记录，onclose 会做实际清理
            this._handleError(
                new Error('WebSocket error')
            );
        };

        ws.onclose = (event) => {
            if (this._ws !== ws || this._disposed) return;
            this._handleClose(event);
        };
    }

    _handleOpen() {
        this._connectedAt = Date.now();
        this._lastMessageTs = Date.now(); // 刚连上算"有消息"，给心跳留余地
        this._reconnectCount = 0;
        this._setState(CONN_STATES.CONNECTED, { url: this._url });
        this._startHeartbeat();
        this._sendPing();
    }

    _handleClose(event) {
        this._stopHeartbeat();
        this._ws = null;

        if (this._disposed) return;

        if (this._manualDisconnect) {
            this._setState(CONN_STATES.DISCONNECTED, {
                code: event.code,
                reason: event.reason,
                manual: true,
            });
            return;
        }

        // 自动重连
        this._reconnectCount++;
        this._setState(CONN_STATES.RECONNECTING, {
            code: event.code,
            reason: event.reason,
            reconnectCount: this._reconnectCount,
            willRetryInMs: this._computeReconnectDelay(),
        });
        this._scheduleReconnect();
    }

    _closeWebSocket() {
        const ws = this._ws;
        if (!ws) return;

        // 先断开回调引用，避免旧 ws 的回调污染新连接
        ws.onopen = null;
        ws.onmessage = null;
        ws.onerror = null;
        ws.onclose = null;

        try {
            if (
                ws.readyState === WebSocket.OPEN
                || ws.readyState === WebSocket.CONNECTING
            ) {
                ws.close(1000, 'client disconnect');
            }
        } catch (e) {
            // ignore
        }

        if (this._ws === ws) {
            this._ws = null;
        }
    }

    // ========================================================================
    // 重连
    // ========================================================================

    _scheduleReconnect() {
        if (this._disposed) return;
        if (this._reconnectTimer !== null) return;

        const delay = this._computeReconnectDelay();
        this._reconnectTimer = setTimeout(() => {
            this._reconnectTimer = null;
            if (!this._disposed) {
                this._connect();
            }
        }, delay);
    }

    _clearReconnectTimer() {
        if (this._reconnectTimer !== null) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
    }

    _computeReconnectDelay() {
        const {
            initialDelayMs, maxDelayMs, multiplier, jitter, maxFastRetries,
        } = this._reconnectCfg;

        let base;
        if (this._reconnectCount >= maxFastRetries) {
            base = maxDelayMs;
        } else {
            base = Math.min(
                maxDelayMs,
                initialDelayMs * Math.pow(multiplier, this._reconnectCount)
            );
        }

        // 抖动：±jitter * base
        const j = 1 - jitter + Math.random() * jitter * 2;
        return Math.round(base * j);
    }

    // ========================================================================
    // 心跳
    // ========================================================================

    _startHeartbeat() {
        this._stopHeartbeat();

        const interval = this._heartbeatCfg.intervalMs;
        const timeout = this._heartbeatCfg.timeoutMs;

        // 发送 ping
        this._heartbeatTimer = setInterval(() => {
            if (this._disposed) return;
            if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                this._sendPing();
            }
        }, interval);

        // 检查超时
        this._heartbeatTimeoutTimer = setInterval(() => {
            if (this._disposed) return;
            if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

            const now = Date.now();
            const lastMsg = this._lastMessageTs || this._connectedAt || now;
            const age = now - lastMsg;

            if (age > timeout) {
                // 超时，主动断开触发重连
                this._handleError(
                    new Error(`heartbeat timeout: no message for ${age}ms`)
                );
                try {
                    this._ws.close(4000, 'heartbeat timeout');
                } catch (e) {
                    // ignore
                }
            }
        }, Math.floor(interval / 2));
    }

    _stopHeartbeat() {
        if (this._heartbeatTimer !== null) {
            clearInterval(this._heartbeatTimer);
            this._heartbeatTimer = null;
        }
        if (this._heartbeatTimeoutTimer !== null) {
            clearInterval(this._heartbeatTimeoutTimer);
            this._heartbeatTimeoutTimer = null;
        }
    }

    _sendPing() {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
        try {
            this._ws.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
            this._stats.pingSent++;
        } catch (e) {
            this._handleError(e);
        }
    }

    // ========================================================================
    // 消息处理
    // ========================================================================

    _handleMessage(event) {
        this._lastMessageTs = Date.now();
        this._stats.messagesReceived++;

        const raw = event.data;

        // 忽略非字符串（不支持二进制）
        if (typeof raw !== 'string') {
            return;
        }

        // 大小限制
        if (raw.length > MAX_MESSAGE_LENGTH) {
            this._stats.oversizedMessages++;
            return;
        }

        let msg;
        try {
            msg = JSON.parse(raw);
        } catch (e) {
            this._stats.malformedMessages++;
            return;
        }

        if (!msg || typeof msg !== 'object') {
            this._stats.malformedMessages++;
            return;
        }

        const type = msg.type;
        if (typeof type !== 'string' || !type) {
            this._stats.malformedMessages++;
            return;
        }

        // pong 只更新 lastMessageTs，不分发
        if (type === MSG_TYPES.PONG) {
            return;
        }

        this._dispatch(type, msg);
    }

    _dispatch(type, msg) {
        // 具体类型订阅者
        const handlers = this._subscribers.get(type);
        if (handlers && handlers.length > 0) {
            const payload = Object.prototype.hasOwnProperty.call(msg, 'data')
                ? msg.data
                : msg;
            for (const h of handlers) {
                try {
                    h(payload);
                    this._stats.messagesDispatched++;
                } catch (e) {
                    this._stats.handlerErrors++;
                    this._handleError(e);
                }
            }
        }

        // 通配符订阅者
        const wildcard = this._subscribers.get('*');
        if (wildcard && wildcard.length > 0) {
            for (const h of wildcard) {
                try {
                    h(msg);
                    this._stats.messagesDispatched++;
                } catch (e) {
                    this._stats.handlerErrors++;
                    this._handleError(e);
                }
            }
        }
    }

    _unsubscribe(type, handler) {
        const handlers = this._subscribers.get(type);
        if (!handlers) return;
        const idx = handlers.indexOf(handler);
        if (idx >= 0) {
            handlers.splice(idx, 1);
        }
        if (handlers.length === 0) {
            this._subscribers.delete(type);
        }
    }

    // ========================================================================
    // 状态与错误
    // ========================================================================

    _setState(state, meta = {}) {
        if (this._state === state && !meta.force) return;
        this._state = state;
        if (this._onStateChange) {
            try {
                this._onStateChange(state, meta);
            } catch (e) {
                // 回调异常不能影响连接
                console.error('onStateChange error:', e);
            }
        }
    }

    _handleError(error) {
        if (this._onError) {
            try {
                this._onError(error);
            } catch (e) {
                console.error('onError error:', e);
            }
        }
    }
}


// ============================================================================
// 工厂函数
// ============================================================================
export function createWsClient(options = {}) {
    return new WsClient(options);
}


export default WsClient;
