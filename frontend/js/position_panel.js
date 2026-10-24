/**
 * js/position_panel.js — 持仓面板
 * ============================================================================
 *
 * 定位：
 * - 操作员最常看的面板：活跃轮次的全部关键字段
 * - 与 state_panel.js 风格一致：属性驱动颜色、DOM 缓存、生命周期完整
 *
 * 关键契约（"六必须"）：
 * 1. 必须拆分高频/低频更新路径：浮盈高频更新不走完整渲染
 * 2. 必须 DOM 缓存：构造时查询一次，之后只改文本/属性
 * 3. 必须 textContent：绝不使用 innerHTML（骨架除外）
 * 4. 必须属性驱动：方向/浮盈颜色由 data-* 属性驱动
 * 5. 必须生命周期完整：dispose() 后无定时器、无监听、无 raf
 * 6. 必须空态设计：空仓时显示"无持仓，等待信号"，不是一堆 `--`
 *
 * 性能设计：
 * - 完整渲染（_render）：方向/入场价/数量/止损变化时触发
 * - 高频更新（_updateFloatingFields）：浮盈/R/mark price 变化时触发
 * - 两条路径共用同一批 DOM 引用，避免重复查询
 * - requestAnimationFrame 批量更新（一帧一次）
 *
 * 止损变化高亮：
 * - 止损变化时短暂高亮 300ms
 * - 目的：提示操作员"止损上移了"
 * - 用 setTimeout + clearTimeout 控制，避免累积
 *
 * 空态设计：
 * - 大图标 + 一行文案："当前无持仓"
 * - 次级文案："等待信号入场"
 * - 目的：让操作员 90% 的时间看到"明确的空态"，不是一堆 `--`
 *
 * 数据结构：
 * - activeRound: {
 *     round_id, symbol, direction, entry_price, total_qty,
 *     current_stop, initial_stop, peak_unrealized,
 *     opened_ts, bars_held, signal_id, pyramid_count,
 *   } | null
 * - floating: {
 *     mark_price, unrealized_pnl, r_multiple,
 *   }
 * - recentRounds: [{ round_id, direction, exit_reason, realized_pnl, r_multiple }]
 *
 * 作者：比蒙
 * 审查：乐高（执行）、张三（系统）
 * 版本：v1.0.0
 * ============================================================================
 */

// ============================================================================
// 常量
// ============================================================================
export const STOP_FLASH_MS = 300;
export const MAX_RECENT_ROUNDS = 5;
export const DEFAULT_PRICE_PRECISION = 1;

const EXIT_REASON_LABELS = {
    stop: '止损',
    top_escape: '逃顶',
    timeout: '超时',
    risk_halt: '风控熔断',
    emergency: '紧急停止',
    recovered_dead: '恢复-死轮次',
    end_of_backtest: '回测结束',
};


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

function formatPrice(p, precision = DEFAULT_PRICE_PRECISION) {
    const n = safeNumber(p);
    if (n === null) return '--';
    return n.toFixed(precision);
}

function formatUsdt(v) {
    const n = safeNumber(v);
    if (n === null) return '--';
    const sign = n >= 0 ? '+' : '';
    return `${sign}${n.toFixed(2)}`;
}

function formatUsdtPlain(v) {
    const n = safeNumber(v);
    if (n === null) return '--';
    return n.toFixed(2);
}

function formatR(v) {
    const n = safeNumber(v);
    if (n === null) return '--';
    const sign = n >= 0 ? '+' : '';
    return `${sign}${n.toFixed(3)}`;
}

function formatUtcTime(tsMs) {
    const n = safeNumber(tsMs);
    if (n === null) return '--';
    const d = new Date(n);
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mm = String(d.getUTCMinutes()).padStart(2, '0');
    return `${hh}:${mm}`;
}

function truncateMiddle(s, maxLen = 16) {
    if (!s) return '--';
    const str = String(s);
    if (str.length <= maxLen) return str;
    const head = str.slice(0, Math.floor(maxLen / 2) - 1);
    const tail = str.slice(-Math.floor(maxLen / 2) + 1);
    return `${head}…${tail}`;
}

function formatBarsHeld(bars, intervalMs = 180000) {
    const n = safeNumber(bars, 0);
    const minutes = Math.round((n * intervalMs) / 60000);
    if (minutes < 60) return `${n} bars · ${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${n} bars · ${hours}h${mins > 0 ? mins + 'm' : ''}`;
}

function getDirectionLabel(direction) {
    if (direction === 1) return { text: '▲ LONG', side: 'long' };
    if (direction === -1) return { text: '▼ SHORT', side: 'short' };
    return { text: '--', side: null };
}


// ============================================================================
// PositionPanel 类
// ============================================================================
export class PositionPanel {

    constructor(container, options = {}) {
        if (!container) {
            throw new Error('PositionPanel: container is required');
        }

        this.container = container;
        this.pricePrecision =
            options.pricePrecision !== undefined
                ? options.pricePrecision
                : DEFAULT_PRICE_PRECISION;
        this.intervalMs = options.intervalMs || 180000;

        // ---- 内部状态 ----
        this.activeRound = null;
        this.floating = {
            mark_price: null,
            unrealized_pnl: null,
            r_multiple: null,
        };
        this.recentRounds = [];

        // ---- 调度 ----
        this._rafId = null;
        this._disposed = false;
        this._stopFlashTimer = null;
        this._themeObserver = null;
        this._lastStopPrice = null;

        // ---- DOM 引用 ----
        this._els = {};

        // ---- 初始化 ----
        this._initDOM();
        this._observeTheme();
        this._render(true);
    }

    // ========================================================================
    // 初始化
    // ========================================================================

    _initDOM() {
        this.container.innerHTML = '';
        this._buildSkeleton();

        const query = (sel) => {
            const el = this.container.querySelector(sel);
            if (!el) {
                throw new Error(`PositionPanel: missing element ${sel}`);
            }
            return el;
        };

        this._els = {
            root: query('.position-panel'),
            emptyState: query('.position-empty'),
            activeContent: query('.position-active'),

            // 头部：方向 + 信号 ID
            direction: query('.position-direction'),
            signalId: query('.position-signal'),

            // 轮次 ID
            roundId: query('.round-id'),

            // 价格字段
            entryPrice: query('.entry-price'),
            markPrice: query('.mark-price'),
            currentStop: query('.current-stop'),
            stopRow: query('.stop-row'),

            // 数量
            totalQty: query('.total-qty'),

            // 浮盈
            unrealizedPnl: query('.unrealized-pnl'),
            unrealizedR: query('.unrealized-r'),
            pnlCard: query('.pnl-card'),

            // 峰值
            peakUnrealized: query('.peak-unrealized'),
            peakR: query('.peak-r'),

            // 时间
            openedTime: query('.opened-time'),
            barsHeld: query('.bars-held'),

            // 最近轮次
            recentList: query('.recent-rounds-list'),
            recentEmpty: query('.recent-empty'),
        };
    }

    _buildSkeleton() {
        const html = `
            <div class="position-panel">

                <!-- 空态 -->
                <div class="position-empty">
                    <div class="empty-icon">◯</div>
                    <div class="empty-title">当前无持仓</div>
                    <div class="empty-hint">等待信号入场</div>
                </div>

                <!-- 活跃轮次 -->
                <div class="position-active" style="display:none;">

                    <div class="position-header">
                        <span class="position-direction" data-side="">--</span>
                        <span class="position-signal" title="信号 ID">--</span>
                    </div>

                    <div class="position-meta">
                        <span class="meta-label">ROUND</span>
                        <span class="round-id num" title="轮次 ID">--</span>
                    </div>

                    <!-- 价格网格 -->
                    <div class="price-grid">
                        <div class="price-field">
                            <span class="label">入场</span>
                            <span class="value num entry-price">--</span>
                        </div>
                        <div class="price-field">
                            <span class="label">标记</span>
                            <span class="value num mark-price">--</span>
                        </div>
                        <div class="price-field stop-row">
                            <span class="label">止损</span>
                            <span class="value num current-stop">--</span>
                        </div>
                        <div class="price-field">
                            <span class="label">数量</span>
                            <span class="value num total-qty">--</span>
                        </div>
                    </div>

                    <!-- 盈亏卡片 -->
                    <div class="pnl-card" data-pnl="neutral">
                        <div class="pnl-main">
                            <span class="pnl-label">浮盈</span>
                            <span class="num unrealized-pnl">--</span>
                        </div>
                        <div class="pnl-sub">
                            <span class="num unrealized-r">--</span>
                            <span class="pnl-sep">·</span>
                            <span class="peak-label">峰值</span>
                            <span class="num peak-unrealized">--</span>
                            <span class="num peak-r">(--)</span>
                        </div>
                    </div>

                    <!-- 时间信息 -->
                    <div class="time-info">
                        <span class="opened-time">--</span>
                        <span class="time-sep">·</span>
                        <span class="bars-held num">--</span>
                    </div>

                </div>

                <!-- 最近轮次 -->
                <div class="recent-section">
                    <div class="recent-header">RECENT</div>
                    <div class="recent-rounds-list"></div>
                    <div class="recent-empty" style="display:none;">暂无记录</div>
                </div>

            </div>
        `;
        this.container.innerHTML = html;
    }

    _observeTheme() {
        if (typeof MutationObserver === 'undefined') return;
        this._themeObserver = new MutationObserver((mutations) => {
            for (const m of mutations) {
                if (m.type === 'attributes' && m.attributeName === 'data-theme') {
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
     * 设置活跃轮次。
     * @param {Object|null} round
     */
    setActiveRound(round) {
        if (this._disposed) return;

        const prevId = this.activeRound ? this.activeRound.round_id : null;
        const nextId = round ? round.round_id : null;

        if (prevId !== nextId) {
            // 轮次变化，重置浮盈
            this.floating = {
                mark_price: null,
                unrealized_pnl: null,
                r_multiple: null,
            };
            this._lastStopPrice = null;
        }

        this.activeRound = round || null;
        this._scheduleRender();
    }

    /**
     * 高频更新：浮盈 / R / 标记价。
     * 不走完整渲染，只更新三个文本字段。
     */
    setFloating(markPrice, unrealizedPnl, rMultiple) {
        if (this._disposed) return;
        if (!this.activeRound) return;

        this.floating.mark_price = safeNumber(markPrice);
        this.floating.unrealized_pnl = safeNumber(unrealizedPnl);
        this.floating.r_multiple = safeNumber(rMultiple);

        this._scheduleFloatingUpdate();
    }

    /**
     * 设置最近已平仓轮次列表。
     */
    setRecentRounds(rounds) {
        if (this._disposed) return;
        this.recentRounds = Array.isArray(rounds)
            ? rounds.slice(0, MAX_RECENT_ROUNDS)
            : [];
        this._scheduleRender();
    }

    /**
     * 清空。
     */
    clear() {
        if (this._disposed) return;
        this.activeRound = null;
        this.floating = {
            mark_price: null,
            unrealized_pnl: null,
            r_multiple: null,
        };
        this.recentRounds = [];
        this._lastStopPrice = null;
        this._scheduleRender();
    }

    // ========================================================================
    // 调度
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

    _scheduleFloatingUpdate() {
        if (this._disposed) return;
        if (this._floatingRafId != null) return;
        this._floatingRafId = requestAnimationFrame(() => {
            this._floatingRafId = null;
            if (!this._disposed) {
                this._updateFloatingFields();
            }
        });
    }

    // ========================================================================
    // 渲染：完整路径
    // ========================================================================

    _render(initial = false) {
        if (this._disposed) return;

        if (!this.activeRound) {
            this._renderEmptyState();
        } else {
            this._renderActiveRound(initial);
        }

        this._renderRecentRounds();
    }

    _renderEmptyState() {
        const { emptyState, activeContent } = this._els;
        if (!emptyState || !activeContent) return;
        emptyState.style.display = 'flex';
        activeContent.style.display = 'none';
    }

    _renderActiveRound(initial) {
        const { emptyState, activeContent } = this._els;
        if (!emptyState || !activeContent) return;
        emptyState.style.display = 'none';
        activeContent.style.display = 'flex';

        const round = this.activeRound;

        // ---- 方向 ----
        const dir = getDirectionLabel(round.direction);
        this._els.direction.textContent = dir.text;
        if (dir.side) {
            this._els.direction.setAttribute('data-side', dir.side);
        } else {
            this._els.direction.removeAttribute('data-side');
        }

        // ---- 信号 ID ----
        this._els.signalId.textContent = truncateMiddle(round.signal_id, 14);
        this._els.signalId.title = safeString(round.signal_id);

        // ---- 轮次 ID ----
        this._els.roundId.textContent = truncateMiddle(round.round_id, 18);
        this._els.roundId.title = safeString(round.round_id);

        // ---- 价格 ----
        this._els.entryPrice.textContent = formatPrice(round.entry_price);
        this._els.totalQty.textContent = safeString(round.total_qty);

        // ---- 止损：变化时高亮 ----
        const newStop = safeNumber(round.current_stop);
        this._els.currentStop.textContent = formatPrice(newStop);

        if (
            !initial &&
            this._lastStopPrice !== null &&
            newStop !== null &&
            newStop !== this._lastStopPrice
        ) {
            this._flashStop();
        }
        this._lastStopPrice = newStop;

        // ---- 峰值（低频，每次完整渲染都更新）----
        const peak = safeNumber(round.peak_unrealized);
        if (peak !== null && peak > 0) {
            this._els.peakUnrealized.textContent = formatUsdtPlain(peak);
            const initialRisk = this._estimateInitialRisk();
            if (initialRisk && initialRisk > 0) {
                const peakR = peak / initialRisk;
                this._els.peakR.textContent = `(${formatR(peakR)})`;
            } else {
                this._els.peakR.textContent = '(--)';
            }
        } else {
            this._els.peakUnrealized.textContent = '--';
            this._els.peakR.textContent = '(--)';
        }

        // ---- 时间 ----
        this._els.openedTime.textContent = formatUtcTime(round.opened_ts);
        this._els.barsHeld.textContent = formatBarsHeld(
            round.bars_held,
            this.intervalMs
        );

        // ---- 浮盈（复用高频路径）----
        this._updateFloatingFields();
    }

    _renderRecentRounds() {
        const { recentList, recentEmpty } = this._els;
        if (!recentList || !recentEmpty) return;

        if (this.recentRounds.length === 0) {
            recentList.innerHTML = '';
            recentEmpty.style.display = 'block';
            return;
        }

        recentEmpty.style.display = 'none';

        const frag = document.createDocumentFragment();
        for (const r of this.recentRounds) {
            const dir = getDirectionLabel(r.direction);
            const pnl = safeNumber(r.realized_pnl, 0);
            const rMultiple = safeNumber(r.r_multiple, 0);

            const row = document.createElement('div');
            row.className = 'recent-row';
            row.setAttribute('data-pnl', pnl >= 0 ? 'positive' : 'negative');

            const dirEl = document.createElement('span');
            dirEl.className = 'recent-dir';
            dirEl.textContent = dir.side === 'long' ? 'L' : 'S';
            if (dir.side) dirEl.setAttribute('data-side', dir.side);

            const reasonEl = document.createElement('span');
            reasonEl.className = 'recent-reason';
            reasonEl.textContent = EXIT_REASON_LABELS[r.exit_reason] || safeString(r.exit_reason);
            reasonEl.title = safeString(r.exit_reason);

            const rEl = document.createElement('span');
            rEl.className = 'recent-r num';
            rEl.textContent = formatR(rMultiple);
            rEl.setAttribute('data-pnl', rMultiple >= 0 ? 'positive' : 'negative');

            row.appendChild(dirEl);
            row.appendChild(reasonEl);
            row.appendChild(rEl);
            frag.appendChild(row);
        }

        recentList.innerHTML = '';
        recentList.appendChild(frag);
    }

    // ========================================================================
    // 渲染：高频路径
    // ========================================================================

    _updateFloatingFields() {
        if (this._disposed) return;
        if (!this.activeRound) return;

        const {
            markPrice, unrealizedPnl, unrealizedR, pnlCard,
        } = this._els;
        if (!markPrice || !unrealizedPnl || !unrealizedR || !pnlCard) return;

        markPrice.textContent = formatPrice(this.floating.mark_price);

        const pnl = this.floating.unrealized_pnl;
        const r = this.floating.r_multiple;

        // 浮盈文本
        if (pnl === null) {
            unrealizedPnl.textContent = '--';
            pnlCard.setAttribute('data-pnl', 'neutral');
        } else {
            unrealizedPnl.textContent = formatUsdt(pnl);
            if (pnl > 0) {
                pnlCard.setAttribute('data-pnl', 'positive');
            } else if (pnl < 0) {
                pnlCard.setAttribute('data-pnl', 'negative');
            } else {
                pnlCard.setAttribute('data-pnl', 'neutral');
            }
        }

        // R 倍数
        if (r === null) {
            unrealizedR.textContent = '--';
        } else {
            unrealizedR.textContent = formatR(r);
            unrealizedR.setAttribute('data-pnl', r >= 0 ? 'positive' : 'negative');
        }
    }

    // ========================================================================
    // 止损变化高亮
    // ========================================================================

    _flashStop() {
        const { stopRow } = this._els;
        if (!stopRow) return;

        stopRow.classList.add('stop-flash');

        if (this._stopFlashTimer !== null) {
            clearTimeout(this._stopFlashTimer);
        }
        this._stopFlashTimer = setTimeout(() => {
            this._stopFlashTimer = null;
            if (!this._disposed && stopRow) {
                stopRow.classList.remove('stop-flash');
            }
        }, STOP_FLASH_MS);
    }

    // ========================================================================
    // 辅助
    // ========================================================================

    _estimateInitialRisk() {
        // 从 round 数据估算 initial_risk：
        // |entry_price - initial_stop| * total_qty
        const round = this.activeRound;
        if (!round) return null;

        const entry = safeNumber(round.entry_price);
        const initialStop = safeNumber(round.initial_stop);
        const qty = safeNumber(round.total_qty);
        if (entry === null || initialStop === null || qty === null) {
            return null;
        }
        const risk = Math.abs(entry - initialStop) * qty;
        return risk > 0 ? risk : null;
    }

    // ========================================================================
    // 生命周期
    // ========================================================================

    dispose() {
        if (this._disposed) return;
        this._disposed = true;

        if (this._rafId !== null) {
            cancelAnimationFrame(this._rafId);
            this._rafId = null;
        }
        if (this._floatingRafId != null) {
            cancelAnimationFrame(this._floatingRafId);
            this._floatingRafId = null;
        }
        if (this._stopFlashTimer !== null) {
            clearTimeout(this._stopFlashTimer);
            this._stopFlashTimer = null;
        }
        if (this._themeObserver) {
            this._themeObserver.disconnect();
            this._themeObserver = null;
        }

        if (this.container) {
            this.container.innerHTML = '';
        }

        this._els = {};
        this.activeRound = null;
        this.recentRounds = [];
    }
}


// ============================================================================
// 工厂函数
// ============================================================================
export function createPositionPanel(container, options = {}) {
    return new PositionPanel(container, options);
}


export default PositionPanel;
