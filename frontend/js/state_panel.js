/**
 * js/state_panel.js — 状态机面板（策略的心电监护仪）
 * ============================================================================
 *
 * 定位：
 * - 前端所有图表都是给"看市场"的，只有这个面板是给"看系统自己"的
 * - 五个核心问题：状态机在哪、待了多久、为什么进去、风控紧张不紧张、信号强不强
 * - 不参与决策，只呈现决策
 *
 * 关键契约（"六必须"）：
 * 1. 必须缓存 DOM 引用：构造时查询一次，之后只更新文本/属性
 * 2. 必须 textContent：绝不使用 innerHTML，杜绝 XSS
 * 3. 必须属性驱动：状态/风控等级用 data-* 属性，CSS 负责颜色
 * 4. 必须生命周期完整：dispose() 后无定时器、无监听、无 raf
 * 5. 必须主题友好：监听 data-theme 变化，重绘必要样式
 * 6. 必须未知状态兜底：UNKNOWN 状态不崩
 *
 * 性能设计：
 * - 缓存所有 DOM 引用（构造时查询一次）
 * - 状态变化时才触发渲染（未变跳过）
 * - 用 requestAnimationFrame 批量更新（一帧一次）
 * - bars_in_state 变化时只更新计数器，不触发完整渲染
 * - 状态历史最多 20 条，超出淘汰最旧
 *
 * 主题适配：
 * - 所有颜色由 CSS 变量驱动（css/dashboard.css）
 * - JS 只设置 data-state / data-risk 属性
 * - 主题切换时通过 MutationObserver 感知
 *
 * 与 kline_chart.js 的协作：
 * - 状态变化时，外部调用方负责同步调用 kline_chart.setState()
 * - 本模块只管自己的面板，不感知图表
 *
 * 数据结构：
 * - state:        'idle' | 'breakout_confirm' | 'trend_ride' | 'top_detect'
 * - riskLevel:    'normal' | 'warn' | 'reduce' | 'close_only' | 'halt'
 * - signal:       { signal_id, score, direction, reason } | null
 * - stateHistory: [{ ts, state }]
 *
 * 状态标签（中文描述）：
 * - idle:           IDLE (空闲)
 * - breakout_confirm: BREAKOUT (突破确认)
 * - trend_ride:     TREND (趋势持仓)
 * - top_detect:     TOP DETECT (逃顶观察)
 *
 * 作者：比蒙
 * 审查：乐高（执行）、张三（系统）
 * 版本：v1.0.0
 * ============================================================================
 */

// ============================================================================
// 状态配置
// ============================================================================

/**
 * 状态机状态标签。
 * - label: 英文名（显示）
 * - labelCn: 中文名（tooltip）
 * - color: 主色（用于状态时间线）
 * - dataState: 用于 CSS 的 data-state 值
 */
export const STATE_CONFIG = {
    idle: {
        label: 'IDLE',
        labelCn: '空闲',
        color: 'var(--text-muted)',
        dataState: 'idle',
    },
    breakout_confirm: {
        label: 'BREAKOUT',
        labelCn: '突破确认',
        color: 'var(--accent)',
        dataState: 'breakout',
    },
    trend_ride: {
        label: 'TREND',
        labelCn: '趋势持仓',
        color: 'var(--color-profit)',
        dataState: 'trend',
    },
    top_detect: {
        label: 'TOP DETECT',
        labelCn: '逃顶观察',
        color: 'var(--risk-reduce)',
        dataState: 'top-detect',
    },
};

/**
 * 风控等级配置。
 */
export const RISK_CONFIG = {
    normal: {
        label: 'NORMAL',
        labelCn: '正常交易',
        dataRisk: 'normal',
    },
    warn: {
        label: 'WARN',
        labelCn: '预警',
        dataRisk: 'warn',
    },
    reduce: {
        label: 'REDUCE',
        labelCn: '降频（仓位减半）',
        dataRisk: 'reduce',
    },
    close_only: {
        label: 'CLOSE ONLY',
        labelCn: '只平仓',
        dataRisk: 'close_only',
    },
    halt: {
        label: 'HALT',
        labelCn: '熔断',
        dataRisk: 'halt',
    },
};

export const MAX_STATE_HISTORY = 20;
export const STATE_FLASH_MS = 500;
export const DEFAULT_MAX_BARS_IN_STATE = 5;  // breakout_confirm 的超时阈值
export const SIGNAL_STRENGTH_LEVELS = 5;


// ============================================================================
// 工具
// ============================================================================

function safeString(v, fallback = '--') {
    if (v === null || v === undefined || v === '') return fallback;
    return String(v);
}

function safeNumber(v, fallback = null) {
    const n = Number(v);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function formatScore(v) {
    const n = safeNumber(v);
    if (n === null) return '--';
    return n.toFixed(4);
}

function truncateMiddle(s, maxLen = 24) {
    if (!s) return '--';
    if (s.length <= maxLen) return s;
    const head = s.slice(0, Math.floor(maxLen / 2) - 1);
    const tail = s.slice(-Math.floor(maxLen / 2) + 1);
    return `${head}…${tail}`;
}

function formatUtcTimeShort(tsMs) {
    const d = new Date(tsMs);
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mm = String(d.getUTCMinutes()).padStart(2, '0');
    const ss = String(d.getUTCSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
}


// ============================================================================
// StatePanel 类
// ============================================================================
export class StatePanel {

    /**
     * @param {HTMLElement} container 面板容器
     * @param {Object} options
     * @param {number} options.maxBarsInState  当前状态的 K 线数上限（用于进度条），默认 5
     */
    constructor(container, options = {}) {
        if (!container) {
            throw new Error('StatePanel: container is required');
        }

        this.container = container;
        this.maxBarsInState = options.maxBarsInState || DEFAULT_MAX_BARS_IN_STATE;

        // ---- 内部状态 ----
        this.currentState = null;       // 状态名
        this.currentRiskLevel = null;   // 风控等级
        this.currentBarsInState = 0;
        this.currentEnterReason = '--';
        this.currentSignal = null;      // { signal_id, score, direction, reason }
        this.stateHistory = [];         // [{ ts, state }]
        this._lastRenderedState = null;

        // ---- 调度 ----
        this._rafId = null;
        this._disposed = false;
        this._flashTimer = null;
        this._themeObserver = null;

        // ---- DOM 引用（缓存）----
        this._els = {};

        // ---- 初始化 ----
        this._initDOM();
        this._bindEvents();
        this._observeTheme();
        this._render(true);
    }

    // ========================================================================
    // 初始化
    // ========================================================================

    _initDOM() {
        // 容器清空（可能已有骨架屏）
        this.container.innerHTML = '';

        // 检查是否已有骨架结构，没有则创建
        if (!this.container.querySelector('.state-panel')) {
            this._buildSkeleton();
        }

        const query = (sel) => {
            const el = this.container.querySelector(sel);
            if (!el) {
                throw new Error(`StatePanel: missing element ${sel}`);
            }
            return el;
        };

        // 缓存所有 DOM 引用
        this._els = {
            root: query('.state-panel'),

            // 状态卡片
            stateCard: query('.state-card'),
            stateLabel: query('.state-label'),
            stateValue: query('.state-value'),
            stateCn: query('.state-cn'),
            stateBars: query('.state-bars'),
            stateProgress: query('.state-progress'),
            stateProgressBar: query('.state-progress-bar'),
            enterReason: query('.enter-reason'),

            // 风控卡片
            riskCard: query('.risk-card'),
            riskValue: query('.risk-value'),
            riskCn: query('.risk-cn'),
            riskMetrics: query('.risk-metrics'),

            // 信号卡片
            signalCard: query('.signal-card'),
            signalId: query('.signal-id'),
            signalScore: query('.signal-score'),
            signalDirection: query('.signal-direction'),
            signalStrength: query('.signal-strength'),

            // 状态历史
            history: query('.state-history'),
            historyEmpty: query('.state-history-empty'),
        };
    }

    _buildSkeleton() {
        // 构建面板的 HTML 结构
        // 注意：这是一个静态结构，不含任何动态数据
        const html = `
            <div class="state-panel">

                <section class="state-card" data-state="idle">
                    <div class="card-header">
                        <span class="state-label">STATE</span>
                        <span class="state-value" data-state="idle">--</span>
                    </div>
                    <div class="state-cn">--</div>
                    <div class="state-meta">
                        <span class="state-bars" title="当前状态的 K 线数">0 bars</span>
                        <span class="enter-reason" title="进入原因">--</span>
                    </div>
                    <div class="state-progress" role="progressbar" aria-valuemin="0" aria-valuemax="5" aria-valuenow="0">
                        <div class="state-progress-bar"></div>
                    </div>
                </section>

                <section class="risk-card">
                    <div class="card-header">
                        <span class="risk-label">RISK</span>
                        <span class="risk-value" data-risk="normal">--</span>
                    </div>
                    <div class="risk-cn">--</div>
                    <div class="risk-metrics"></div>
                </section>

                <section class="signal-card">
                    <div class="card-header">
                        <span class="signal-label">SIGNAL</span>
                        <span class="signal-direction">--</span>
                    </div>
                    <div class="signal-detail">
                        <span class="signal-id" title="信号 ID">--</span>
                        <span class="signal-score">--</span>
                    </div>
                    <div class="signal-strength" data-level="0" aria-label="信号强度">
                        <span class="bar"></span>
                        <span class="bar"></span>
                        <span class="bar"></span>
                        <span class="bar"></span>
                        <span class="bar"></span>
                    </div>
                </section>

                <section class="history-card">
                    <div class="card-header">
                        <span class="history-label">RECENT</span>
                    </div>
                    <div class="state-history"></div>
                    <div class="state-history-empty" style="display:none;text-align:center;color:var(--text-muted);padding:8px;font-size:12px;">
                        暂无记录
                    </div>
                </section>

            </div>
        `;
        this.container.innerHTML = html;
    }

    _bindEvents() {
        // 状态变化时浏览器原生 MutationObserver 不适用（这里只是占位）
        // 未来可扩展：点击状态卡片查看详情等
    }

    _observeTheme() {
        if (typeof MutationObserver === 'undefined') return;
        this._themeObserver = new MutationObserver((mutations) => {
            for (const m of mutations) {
                if (m.type === 'attributes' && m.attributeName === 'data-theme') {
                    // 主题变化时重新渲染（颜色值会变）
                    this._scheduleRender();
                    break;
                }
            }
        });
        this._themeObserver.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ['data-theme'],
        });
    }

    // ========================================================================
    // 公共 API
    // ========================================================================

    /**
     * 设置当前状态。
     * @param {string} state
     * @param {string} enterReason  进入原因（可选）
     * @param {number} barsInState  进入后已过的 K 线数（可选）
     */
    setState(state, enterReason, barsInState) {
        if (this._disposed) return;

        const changed = this.currentState !== state;
        this.currentState = state;

        if (enterReason !== undefined && enterReason !== null) {
            this.currentEnterReason = String(enterReason);
        }

        if (barsInState !== undefined && barsInState !== null) {
            this.currentBarsInState = Number(barsInState) || 0;
        } else if (changed) {
            this.currentBarsInState = 0;
        }

        if (changed) {
            // 加入历史
            this.stateHistory.push({
                ts: Date.now(),
                state,
            });
            if (this.stateHistory.length > MAX_STATE_HISTORY) {
                this.stateHistory.shift();
            }
            this._scheduleRender();
        } else {
            // 只更新计数器和进度条
            this._updateStateCounter();
        }
    }

    /**
     * 单独更新 bars_in_state（状态未变时使用，避免全量渲染）。
     */
    setBarsInState(n) {
        if (this._disposed) return;
        this.currentBarsInState = Number(n) || 0;
        this._updateStateCounter();
    }

    /**
     * 设置风控等级。
     * @param {string} level
     * @param {Object} metrics  可选的详细指标
     */
    setRiskLevel(level, metrics) {
        if (this._disposed) return;
        const changed = this.currentRiskLevel !== level;
        this.currentRiskLevel = level;
        if (metrics) {
            this._riskMetrics = metrics;
        }
        if (changed) {
            this._scheduleRender();
        }
    }

    /**
     * 设置当前信号。
     * @param {Object|null} signal  { signal_id, score, direction, reason }
     */
    setSignal(signal) {
        if (this._disposed) return;
        this.currentSignal = signal || null;
        this._scheduleRender();
    }

    /**
     * 设置状态历史（用于批量加载）。
     * @param {Array} history  [{ ts, state }]
     */
    setStateHistory(history) {
        if (this._disposed) return;
        if (!Array.isArray(history)) return;
        this.stateHistory = history.slice(-MAX_STATE_HISTORY);
        this._scheduleRender();
    }

    /**
     * 清空所有数据。
     */
    clear() {
        if (this._disposed) return;
        this.currentState = null;
        this.currentRiskLevel = null;
        this.currentBarsInState = 0;
        this.currentEnterReason = '--';
        this.currentSignal = null;
        this.stateHistory = [];
        this._scheduleRender();
    }

    // ========================================================================
    // 渲染
    // ========================================================================

    _scheduleRender() {
        if (this._disposed) return;
        if (this._rafId !== null) return;
        this._rafId = requestAnimationFrame(() => {
            this._rafId = null;
            if (!this._disposed) {
                this._render();
            }
        });
    }

    _updateStateCounter() {
        if (this._disposed) return;
        const { stateBars, stateProgress, stateProgressBar } = this._els;
        if (!stateBars || !stateProgress || !stateProgressBar) return;

        stateBars.textContent = `${this.currentBarsInState} bars`;

        const pct = Math.min(
            100,
            (this.currentBarsInState / Math.max(1, this.maxBarsInState)) * 100
        );
        stateProgressBar.style.width = `${pct}%`;
        stateProgress.setAttribute('aria-valuenow', String(this.currentBarsInState));
    }

    _render(initial = false) {
        if (this._disposed) return;

        this._renderState(initial);
        this._renderRisk();
        this._renderSignal();
        this._renderHistory();
    }

    _renderState(initial) {
        const cfg = STATE_CONFIG[this.currentState] || {
            label: this.currentState || '--',
            labelCn: '未知',
            color: 'var(--text-muted)',
            dataState: 'unknown',
        };

        const { stateCard, stateValue, stateCn, stateBars, stateProgress, stateProgressBar, enterReason } = this._els;

        // 更新值
        stateValue.textContent = cfg.label;
        stateValue.setAttribute('data-state', cfg.dataState);
        stateCard.setAttribute('data-state', cfg.dataState);
        stateCn.textContent = cfg.labelCn;
        enterReason.textContent = safeString(this.currentEnterReason);
        enterReason.title = safeString(this.currentEnterReason);

        // 更新计数器
        stateBars.textContent = `${this.currentBarsInState} bars`;
        const pct = Math.min(
            100,
            (this.currentBarsInState / Math.max(1, this.maxBarsInState)) * 100
        );
        stateProgressBar.style.width = `${pct}%`;
        stateProgress.setAttribute('aria-valuenow', String(this.currentBarsInState));

        // 状态变化闪烁（首次渲染不闪）
        if (!initial && this._lastRenderedState !== null && this._lastRenderedState !== this.currentState) {
            stateCard.classList.add('state-flash');
            if (this._flashTimer) clearTimeout(this._flashTimer);
            this._flashTimer = setTimeout(() => {
                this._flashTimer = null;
                if (!this._disposed) {
                    stateCard.classList.remove('state-flash');
                }
            }, STATE_FLASH_MS);
        }

        this._lastRenderedState = this.currentState;
    }

    _renderRisk() {
        const cfg = RISK_CONFIG[this.currentRiskLevel] || {
            label: this.currentRiskLevel || '--',
            labelCn: '未知',
            dataRisk: 'unknown',
        };

        const { riskValue, riskCn, riskCard, riskMetrics } = this._els;

        riskValue.textContent = cfg.label;
        riskValue.setAttribute('data-risk', cfg.dataRisk);
        riskCard.setAttribute('data-risk', cfg.dataRisk);
        riskCn.textContent = cfg.labelCn;

        // 详细指标（可选）
        if (riskMetrics && this._riskMetrics) {
            const parts = [];
            if (this._riskMetrics.drawdown !== undefined) {
                parts.push(`DD ${(Number(this._riskMetrics.drawdown) * 100).toFixed(2)}%`);
            }
            if (this._riskMetrics.margin_ratio !== undefined) {
                parts.push(`MR ${(Number(this._riskMetrics.margin_ratio) * 100).toFixed(1)}%`);
            }
            if (this._riskMetrics.consecutive_losses !== undefined) {
                parts.push(`Loss ${this._riskMetrics.consecutive_losses}`);
            }
            riskMetrics.textContent = parts.join(' · ');
        } else {
            riskMetrics.textContent = '';
        }
    }

    _renderSignal() {
        const sig = this.currentSignal;
        const { signalId, signalScore, signalDirection, signalStrength, signalCard } = this._els;

        if (!sig) {
            signalId.textContent = '--';
            signalScore.textContent = '--';
            signalDirection.textContent = '--';
            signalDirection.removeAttribute('data-side');
            signalStrength.setAttribute('data-level', '0');
            signalCard.removeAttribute('data-side');
            return;
        }

        signalId.textContent = truncateMiddle(safeString(sig.signal_id), 16);
        signalId.title = safeString(sig.signal_id);

        signalScore.textContent = formatScore(sig.score);

        // 方向
        if (sig.direction === 1) {
            signalDirection.textContent = '▲ LONG';
            signalDirection.setAttribute('data-side', 'long');
            signalCard.setAttribute('data-side', 'long');
        } else if (sig.direction === -1) {
            signalDirection.textContent = '▼ SHORT';
            signalDirection.setAttribute('data-side', 'short');
            signalCard.setAttribute('data-side', 'short');
        } else {
            signalDirection.textContent = '--';
            signalDirection.removeAttribute('data-side');
            signalCard.removeAttribute('data-side');
        }

        // 信号强度（0-5 级，按 score 映射）
        const score = safeNumber(sig.score, 0);
        const level = Math.max(
            0,
            Math.min(SIGNAL_STRENGTH_LEVELS, Math.ceil(score * SIGNAL_STRENGTH_LEVELS))
        );
        signalStrength.setAttribute('data-level', String(level));
    }

    _renderHistory() {
        const { history, historyEmpty } = this._els;
        if (!history || !historyEmpty) return;

        if (this.stateHistory.length === 0) {
            history.innerHTML = '';
            historyEmpty.style.display = 'block';
            return;
        }

        historyEmpty.style.display = 'none';

        // 用 DocumentFragment 批量构建
        const frag = document.createDocumentFragment();
        // 倒序显示（最新的在上）
        const reversed = [...this.stateHistory].reverse();
        for (const item of reversed) {
            const cfg = STATE_CONFIG[item.state] || {
                label: item.state || '?',
                color: 'var(--text-muted)',
                dataState: 'unknown',
            };

            const row = document.createElement('div');
            row.className = 'history-row';

            const dot = document.createElement('span');
            dot.className = 'history-dot';
            dot.setAttribute('data-state', cfg.dataState);
            dot.style.background = cfg.color;

            const time = document.createElement('span');
            time.className = 'history-time';
            time.textContent = formatUtcTimeShort(item.ts);

            const label = document.createElement('span');
            label.className = 'history-label';
            label.textContent = cfg.label;
            label.setAttribute('data-state', cfg.dataState);

            row.appendChild(dot);
            row.appendChild(time);
            row.appendChild(label);
            frag.appendChild(row);
        }

        history.innerHTML = '';
        history.appendChild(frag);
    }

    // ========================================================================
    // 生命周期
    // ========================================================================

    /**
     * 销毁。清理所有资源。
     */
    dispose() {
        if (this._disposed) return;
        this._disposed = true;

        if (this._rafId !== null) {
            cancelAnimationFrame(this._rafId);
            this._rafId = null;
        }

        if (this._flashTimer !== null) {
            clearTimeout(this._flashTimer);
            this._flashTimer = null;
        }

        if (this._themeObserver) {
            this._themeObserver.disconnect();
            this._themeObserver = null;
        }

        // 清空容器
        if (this.container) {
            this.container.innerHTML = '';
        }

        // 清空引用
        this._els = {};
        this.stateHistory = [];
    }
}


// ============================================================================
// 工厂函数
// ============================================================================
export function createStatePanel(container, options = {}) {
    return new StatePanel(container, options);
}


export default StatePanel;
