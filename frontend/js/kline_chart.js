/**
 * js/kline_chart.js — K 线图表模块
 * ============================================================================
 *
 * 定位：
 * - 前端最复杂的模块，负责 K 线渲染 + 实时更新 + 多周期切换 + 主题适配
 * - 不参与决策，只呈现决策
 * - 与后端 KlineAuthority 的 K 线数据保持一致
 *
 * 关键契约（"七必须"）：
 * 1. 必须与后端 K 线一致：用 bar.ts（K 线开盘时间，毫秒），不用 close_ts
 * 2. 必须 UTC 时区：所有时间显示为 UTC，不跟随本地时区
 * 3. 必须有数据窗口：MAX_BARS = 500，超出淘汰最旧
 * 4. 必须节流：渲染节流到 100ms，用 raf 调度
 * 5. 必须清理：dispose() 后无定时器、无监听、无 chart 实例
 * 6. 必须处理错误：初始化失败、数据异常、容器尺寸为 0
 * 7. 必须主题适配：切换主题不重建图表，只 setOption
 *
 * ECharts 依赖：
 * - 必须 ECharts 5.x
 * - 通过 window.echarts 获取（index.html 中通过 CDN 加载）
 * - 如果不可用，抛错，由调用方处理
 *
 * 数据结构：
 * - bar: { ts, open, high, low, close, volume, is_closed? }
 * - level: { price, type, strength }
 * - trade: { ts, price, side, reason }
 * - state: { ts, state }
 *
 * ECharts candlestick 数据格式（易错）：
 *   [open, close, low, high]  ← 注意顺序！不是 [open, high, low, close]
 *
 * 主题色板：
 * - 绿涨红跌（国际惯例，不是中国习惯的红涨绿跌）
 * - 深色主题：夜盘实盘刚需
 *
 * 作者：比蒙
 * 审查：乐高（执行）、张三（系统）
 * 版本：v1.0.0
 * ============================================================================
 */

const echarts = typeof window !== 'undefined' ? window.echarts : null;
if (!echarts) {
    throw new Error(
        'ECharts is required. ' +
        'Include it via <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script> ' +
        'before loading this module.'
    );
}


// ============================================================================
// 常量
// ============================================================================
export const MAX_BARS = 500;
export const DEFAULT_INTERVAL = '3m';
export const MA_PERIOD = 25;
export const RENDER_THROTTLE_MS = 100;
export const MAX_LEVELS = 5;
export const MAX_TRADES = 10;

export const INTERVAL_MS = {
    '3m': 180000,
    '5m': 300000,
    '15m': 900000,
    '1h': 3600000,
};

const STATE_LABELS = {
    idle: 'IDLE',
    breakout_confirm: 'BREAKOUT',
    trend_ride: 'TREND',
    top_detect: 'TOP',
};

const THEMES = {
    dark: {
        bg: '#0d1117',
        text: '#e6edf3',
        textMuted: '#8b949e',
        grid: '#21262d',
        border: '#30363d',
        up: '#3fb950',
        down: '#f85149',
        ma25: '#d29922',
        resistance: '#f85149',
        support: '#3fb950',
        stateIdle: 'rgba(139, 148, 158, 0.05)',
        stateBreakout: 'rgba(88, 166, 255, 0.08)',
        stateTrend: 'rgba(63, 185, 80, 0.08)',
        stateTopDetect: 'rgba(251, 133, 0, 0.08)',
        entryLong: '#3fb950',
        entryShort: '#f85149',
        exitMark: '#d29922',
    },
    light: {
        bg: '#ffffff',
        text: '#1f2328',
        textMuted: '#656d76',
        grid: '#eaeef2',
        border: '#d0d7de',
        up: '#1a7f37',
        down: '#cf222e',
        ma25: '#9a6700',
        resistance: '#cf222e',
        support: '#1a7f37',
        stateIdle: 'rgba(101, 109, 118, 0.05)',
        stateBreakout: 'rgba(9, 105, 218, 0.08)',
        stateTrend: 'rgba(26, 127, 55, 0.08)',
        stateTopDetect: 'rgba(191, 135, 0, 0.08)',
        entryLong: '#1a7f37',
        entryShort: '#cf222e',
        exitMark: '#9a6700',
    },
};


// ============================================================================
// 工具函数
// ============================================================================

/**
 * 格式化 UTC 时间为 HH:MM
 */
function formatUtcTime(tsMs) {
    const d = new Date(tsMs);
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mm = String(d.getUTCMinutes()).padStart(2, '0');
    return `${hh}:${mm}`;
}

/**
 * 格式化 UTC 日期为 MM-DD HH:MM
 */
function formatUtcDateTime(tsMs) {
    const d = new Date(tsMs);
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mi = String(d.getUTCMinutes()).padStart(2, '0');
    return `${mm}-${dd} ${hh}:${mi}`;
}

/**
 * 数值格式化
 */
function formatPrice(p, precision = 2) {
    if (p === null || p === undefined || isNaN(Number(p))) return '--';
    return Number(p).toFixed(precision);
}

/**
 * 深拷贝（简单版）
 */
function clone(obj) {
    if (obj === null || typeof obj !== 'object') return obj;
    if (Array.isArray(obj)) return obj.map(clone);
    const out = {};
    for (const k in obj) out[k] = clone(obj[k]);
    return out;
}


// ============================================================================
// KlineChart 类
// ============================================================================
export class KlineChart {

    /**
     * @param {HTMLElement} container  图表容器
     * @param {Object} options
     * @param {string} options.symbol          交易对，默认 'BTCUSDT'
     * @param {string} options.interval        K 线周期，默认 '3m'
     * @param {string} options.theme           'dark' | 'light'，默认 'dark'
     * @param {string} options.linkGroup       ECharts 联动组名（可选）
     * @param {Function} options.onIntervalChange  周期切换回调
     */
    constructor(container, options = {}) {
        if (!container) {
            throw new Error('KlineChart: container is required');
        }

        this.container = container;
        this.symbol = (options.symbol || 'BTCUSDT').toUpperCase();
        this.interval = options.interval || DEFAULT_INTERVAL;
        this.theme = options.theme || 'dark';
        this.linkGroup = options.linkGroup || null;
        this.onIntervalChange = options.onIntervalChange || null;

        this.chart = null;
        this.bars = [];              // K 线数据（升序）
        this.maData = [];            // MA25 数据（与 bars 对齐）
        this.levels = [];            // 支撑阻力
        this.trades = [];            // 交易标记
        this.stateHistory = [];      // 状态历史
        this.currentState = 'idle';
        this.barPhase = 'close';     // 当前 K 线阶段

        this._rafId = null;
        this._rafType = null;        // 'raf' | 'timeout'
        this._lastRenderTs = 0;
        this._resizeObserver = null;
        this._resizeHandler = null;
        this._disposed = false;

        this._initChart();
    }

    // ========================================================================
    // 初始化
    // ========================================================================

    _initChart() {
        if (this._disposed) return;

        // 容器尺寸检查：ECharts 需要非零尺寸
        if (this.container.clientHeight === 0 || this.container.clientWidth === 0) {
            requestAnimationFrame(() => {
                if (!this._disposed) this._initChart();
            });
            return;
        }

        try {
            this.chart = echarts.init(this.container, null, {
                renderer: 'canvas',
                useDirtyRect: true,
            });

            // 联动
            if (this.linkGroup) {
                this.chart.group = this.linkGroup;
                if (echarts.connect) {
                    echarts.connect(this.linkGroup);
                }
            }

            // 事件：十字线联动
            this.chart.on('updateAxisPointer', (event) => {
                // 如果需要与其他图联动，可以在这里扩展
                // 当前用 linkGroup 已经处理
            });

            // 清理骨架屏
            this.container.classList.remove('skeleton');

            // 首次渲染
            this._render(true);

            // 尺寸监听
            this._resizeObserver = new ResizeObserver(() => {
                if (!this._disposed && this.chart) {
                    this.chart.resize();
                }
            });
            this._resizeObserver.observe(this.container);

        } catch (e) {
            console.error('KlineChart init failed:', e);
            this._showError('图表初始化失败');
        }
    }

    _showError(message) {
        this.container.classList.remove('skeleton');
        this.container.classList.add('state-error');
        this.container.textContent = message;
    }

    // ========================================================================
    // 主题切换
    // ========================================================================

    setTheme(theme) {
        if (this._disposed) return;
        if (theme !== 'dark' && theme !== 'light') return;
        if (this.theme === theme) return;
        this.theme = theme;
        this._scheduleRender();
    }

    // ========================================================================
    // 周期切换
    // ========================================================================

    setInterval(interval) {
        if (this._disposed) return;
        if (!INTERVAL_MS[interval]) return;
        if (this.interval === interval) return;

        this.interval = interval;
        // 清空数据，等待重新加载
        this.bars = [];
        this.maData = [];
        this.trades = [];
        this.stateHistory = [];

        if (this.onIntervalChange) {
            this.onIntervalChange(interval);
        }

        this._scheduleRender();
    }

    // ========================================================================
    // 数据更新
    // ========================================================================

    /**
     * 加载历史 K 线（切换周期时调用）
     * @param {Array} bars  [{ ts, open, high, low, close, volume, is_closed }]
     */
    loadHistory(bars) {
        if (this._disposed) return;
        if (!Array.isArray(bars)) return;

        // 校验并规范化
        const normalized = [];
        for (const b of bars) {
            const n = this._normalizeBar(b);
            if (n) normalized.push(n);
        }

        // 排序 + 去重
        normalized.sort((a, b) => a.ts - b.ts);
        const deduped = [];
        for (const b of normalized) {
            if (deduped.length > 0 && deduped[deduped.length - 1].ts === b.ts) {
                deduped.pop();
            }
            deduped.push(b);
        }

        // 截取最近 MAX_BARS
        if (deduped.length > MAX_BARS) {
            deduped.splice(0, deduped.length - MAX_BARS);
        }

        this.bars = deduped;
        this._recomputeMA();
        this._scheduleRender();
    }

    /**
     * 更新单根 K 线（WebSocket 实时推送）
     * @param {Object} bar
     */
    updateBar(bar) {
        if (this._disposed) return;
        const n = this._normalizeBar(bar);
        if (!n) return;

        if (this.bars.length === 0) {
            this.bars.push(n);
        } else {
            const last = this.bars[this.bars.length - 1];
            if (n.ts === last.ts) {
                // 同一根 K 线：覆盖
                this.bars[this.bars.length - 1] = n;
            } else if (n.ts > last.ts) {
                // 新 K 线：追加
                this.bars.push(n);
                if (this.bars.length > MAX_BARS) {
                    this.bars.shift();
                }
            } else {
                // 乱序：忽略
                return;
            }
        }

        // 增量更新 MA
        this._recomputeMALast();

        // 清理超出窗口的 trades 和 stateHistory
        this._pruneHistory();

        this._scheduleRender();
    }

    /**
     * 更新当前 K 线阶段
     * @param {string} phase  'open' | 'body' | 'tail' | 'close'
     */
    setBarPhase(phase) {
        if (this._disposed) return;
        if (this.barPhase === phase) return;
        this.barPhase = phase;
        this._scheduleRender();
    }

    // ========================================================================
    // 状态与信号
    // ========================================================================

    /**
     * 更新状态机状态
     * @param {string} state  'idle' | 'breakout_confirm' | 'trend_ride' | 'top_detect'
     * @param {number} ts     状态变化时间戳（毫秒）
     */
    setState(state, ts) {
        if (this._disposed) return;
        const tsNum = typeof ts === 'number' ? ts : Date.now();
        this.currentState = state;
        this.stateHistory.push({ ts: tsNum, state });

        // 清理：只保留最近 MAX_BARS 范围内的状态
        if (this.bars.length > 0) {
            const earliest = this.bars[0].ts;
            while (this.stateHistory.length > 0 && this.stateHistory[0].ts < earliest) {
                this.stateHistory.shift();
            }
        }

        this._scheduleRender();
    }

    /**
     * 更新支撑阻力线
     * @param {Array} levels  [{ price, type: 'support' | 'resistance', strength }]
     */
    setLevels(levels) {
        if (this._disposed) return;
        if (!Array.isArray(levels)) return;
        // 按 strength 排序，只保留最强 MAX_LEVELS 个
        this.levels = levels
            .filter(l => l && l.price > 0)
            .sort((a, b) => (b.strength || 0) - (a.strength || 0))
            .slice(0, MAX_LEVELS);
        this._scheduleRender();
    }

    /**
     * 添加交易标记
     * @param {Object} trade  { ts, price, side: 'long' | 'short', action: 'entry' | 'exit', reason }
     */
    addTrade(trade) {
        if (this._disposed) return;
        if (!trade || !trade.ts || !trade.price) return;
        this.trades.push(trade);
        // 只保留最近 MAX_TRADES 个
        if (this.trades.length > MAX_TRADES) {
            this.trades.shift();
        }
        this._pruneHistory();
        this._scheduleRender();
    }

    /**
     * 清空所有数据（重新加载前调用）
     */
    clear() {
        if (this._disposed) return;
        this.bars = [];
        this.maData = [];
        this.levels = [];
        this.trades = [];
        this.stateHistory = [];
        this._scheduleRender();
    }

    // ========================================================================
    // 数据规范化与计算
    // ========================================================================

    _normalizeBar(raw) {
        if (!raw) return null;
        const ts = Number(raw.ts);
        const open = Number(raw.open);
        const high = Number(raw.high);
        const low = Number(raw.low);
        const close = Number(raw.close);
        const volume = Number(raw.volume);

        if (!Number.isFinite(ts) || ts <= 0) return null;
        if (!Number.isFinite(open) || open <= 0) return null;
        if (!Number.isFinite(high) || high <= 0) return null;
        if (!Number.isFinite(low) || low <= 0) return null;
        if (!Number.isFinite(close) || close <= 0) return null;
        if (!Number.isFinite(volume) || volume < 0) return null;
        if (high < low) return null;

        return {
            ts,
            open,
            high,
            low,
            close,
            volume,
            is_closed: raw.is_closed !== false,
        };
    }

    _recomputeMA() {
        this.maData = [];
        for (let i = 0; i < this.bars.length; i++) {
            if (i < MA_PERIOD - 1) {
                this.maData.push(null);
                continue;
            }
            let sum = 0;
            for (let j = i - MA_PERIOD + 1; j <= i; j++) {
                sum += this.bars[j].close;
            }
            this.maData.push(+(sum / MA_PERIOD).toFixed(2));
        }
    }

    _recomputeMALast() {
        const i = this.bars.length - 1;
        if (i < 0) return;
        if (i < MA_PERIOD - 1) {
            this.maData[i] = null;
            return;
        }
        let sum = 0;
        for (let j = i - MA_PERIOD + 1; j <= i; j++) {
            sum += this.bars[j].close;
        }
        this.maData[i] = +(sum / MA_PERIOD).toFixed(2);
        // 如果追加了新 K 线，确保 maData 长度对齐
        while (this.maData.length > this.bars.length) this.maData.pop();
        while (this.maData.length < this.bars.length) this.maData.push(null);
    }

    _pruneHistory() {
        if (this.bars.length === 0) return;
        const earliest = this.bars[0].ts;
        this.trades = this.trades.filter(t => t.ts >= earliest);
        this.stateHistory = this.stateHistory.filter(s => s.ts >= earliest);
    }

    // ========================================================================
    // 渲染
    // ========================================================================

    _scheduleRender() {
        if (this._disposed || !this.chart) return;
        if (this._rafId !== null) return;

        const now = Date.now();
        const elapsed = now - this._lastRenderTs;
        const delay = Math.max(0, RENDER_THROTTLE_MS - elapsed);

        if (delay === 0) {
            this._rafType = 'raf';
            this._rafId = requestAnimationFrame(() => {
                this._rafId = null;
                this._rafType = null;
                this._lastRenderTs = Date.now();
                this._render();
            });
        } else {
            this._rafType = 'timeout';
            this._rafId = setTimeout(() => {
                this._rafType = 'raf';
                this._rafId = requestAnimationFrame(() => {
                    this._rafId = null;
                    this._rafType = null;
                    this._lastRenderTs = Date.now();
                    this._render();
                });
            }, delay);
        }
    }

    _cancelScheduledRender() {
        if (this._rafId !== null) {
            if (this._rafType === 'raf') {
                cancelAnimationFrame(this._rafId);
            } else {
                clearTimeout(this._rafId);
            }
            this._rafId = null;
            this._rafType = null;
        }
    }

    _render(fullRebuild = false) {
        if (this._disposed || !this.chart) return;

        const theme = THEMES[this.theme] || THEMES.dark;

        // x 轴数据（用 K 线开盘时间）
        const timestamps = this.bars.map(b => b.ts);
        const xData = timestamps.map(ts => formatUtcTime(ts));

        // candlestick 数据格式：[open, close, low, high]
        const candleData = this.bars.map(b => [b.open, b.close, b.low, b.high]);

        // 颜色：涨绿跌红
        const itemStyle = {
            color: theme.up,
            color0: theme.down,
            borderColor: theme.up,
            borderColor0: theme.down,
        };

        // MA25 线
        const maSeries = {
            name: `MA${MA_PERIOD}`,
            type: 'line',
            data: this.maData,
            smooth: false,
            symbol: 'none',
            lineStyle: {
                width: 1.5,
                color: theme.ma25,
                opacity: 0.9,
            },
            z: 3,
            connectNulls: false,
        };

        // 状态背景（markArea）
        const markAreaData = this._buildMarkAreaData(theme);

        // 支撑阻力线（markLine）
        const markLineData = this._buildMarkLineData(theme);

        // 交易标记（markPoint）
        const markPointData = this._buildMarkPointData(theme);

        const option = {
            animation: fullRebuild,
            backgroundColor: theme.bg,
            grid: {
                left: 60,
                right: 20,
                top: 30,
                bottom: 40,
            },
            xAxis: {
                type: 'category',
                data: xData,
                boundaryGap: true,
                axisLine: { lineStyle: { color: theme.border } },
                axisLabel: {
                    color: theme.textMuted,
                    fontSize: 11,
                    hideOverlap: true,
                },
                axisPointer: {
                    label: {
                        backgroundColor: theme.border,
                        color: theme.text,
                    },
                },
                splitLine: { show: false },
            },
            yAxis: {
                type: 'value',
                scale: true,
                position: 'right',
                axisLine: { lineStyle: { color: theme.border } },
                axisLabel: {
                    color: theme.textMuted,
                    fontSize: 11,
                    formatter: v => v.toFixed(1),
                },
                splitLine: {
                    lineStyle: { color: theme.grid },
                },
            },
            dataZoom: [
                {
                    type: 'inside',
                    xAxisIndex: [0],
                    start: 70,
                    end: 100,
                },
            ],
            series: [
                {
                    name: 'KLine',
                    type: 'candlestick',
                    data: candleData,
                    itemStyle,
                    markArea: markAreaData ? { data: markAreaData } : undefined,
                    markLine: markLineData ? { data: markLineData, symbol: 'none' } : undefined,
                    markPoint: markPointData ? { data: markPointData, symbolSize: 24 } : undefined,
                },
                maSeries,
            ],
            graphic: this._buildStateGraphic(theme),
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'cross' },
                backgroundColor: theme.bg,
                borderColor: theme.border,
                textStyle: { color: theme.text, fontSize: 12 },
                formatter: params => this._formatTooltip(params, theme),
            },
        };

        this.chart.setOption(option, {
            notMerge: false,
            lazyUpdate: true,
        });
    }

    _buildMarkAreaData(theme) {
        if (this.stateHistory.length === 0 || this.bars.length === 0) return null;

        const xData = this.bars.map(b => formatUtcTime(b.ts));
        const xIndexMap = new Map();
        xData.forEach((x, i) => xIndexMap.set(x, i));

        // 状态色
        const stateColors = {
            idle: theme.stateIdle,
            breakout_confirm: theme.stateBreakout,
            trend_ride: theme.stateTrend,
            top_detect: theme.stateTopDetect,
        };

        const areas = [];
        for (let i = 0; i < this.stateHistory.length; i++) {
            const start = this.stateHistory[i];
            const end = this.stateHistory[i + 1] || { ts: this.bars[this.bars.length - 1].ts };
            const startX = formatUtcTime(start.ts);
            const endX = formatUtcTime(end.ts);
            if (!xIndexMap.has(startX)) continue;
            const color = stateColors[start.state] || theme.stateIdle;
            areas.push([
                {
                    xAxis: startX,
                    itemStyle: { color },
                },
                {
                    xAxis: endX,
                },
            ]);
        }
        return areas.length > 0 ? areas : null;
    }

    _buildMarkLineData(theme) {
        if (this.levels.length === 0) return null;
        return this.levels.map(l => ({
            yAxis: l.price,
            lineStyle: {
                color: l.type === 'resistance' ? theme.resistance : theme.support,
                type: 'dashed',
                width: 1,
                opacity: 0.6,
            },
            label: {
                formatter: () => `$${formatPrice(l.price)}`,
                position: 'insideEndTop',
                color: theme.textMuted,
                fontSize: 10,
            },
        }));
    }

    _buildMarkPointData(theme) {
        if (this.trades.length === 0) return null;
        return this.trades.map(t => {
            const isEntry = t.action === 'entry';
            const isLong = t.side === 'long';
            let symbol = 'circle';
            let color = theme.textMuted;
            let text = '';

            if (isEntry && isLong) {
                symbol = 'triangle';
                color = theme.entryLong;
                text = 'L';
            } else if (isEntry && !isLong) {
                symbol = 'triangle';
                color = theme.entryShort;
                text = 'S';
                // 空头三角向下（ECharts 用符号方向控制，此处简化）
            } else {
                symbol = 'diamond';
                color = theme.exitMark;
                text = 'X';
            }

            return {
                name: t.action,
                coord: [formatUtcTime(t.ts), t.price],
                symbol,
                symbolSize: isEntry ? 14 : 10,
                itemStyle: { color },
                label: {
                    show: true,
                    formatter: text,
                    color: '#fff',
                    fontSize: 10,
                    fontWeight: 'bold',
                },
                tooltip: {
                    formatter: () =>
                        `${t.action.toUpperCase()} ${t.side} @ ${formatPrice(t.price)}` +
                        (t.reason ? ` (${t.reason})` : ''),
                },
            };
        });
    }

    _buildStateGraphic(theme) {
        const label = STATE_LABELS[this.currentState] || this.currentState;
        const phaseLabel = (this.barPhase || 'close').toUpperCase();

        return [
            {
                type: 'text',
                left: 10,
                top: 10,
                style: {
                    text: `STATE: ${label}`,
                    fill: theme.text,
                    fontSize: 12,
                    fontWeight: 'bold',
                },
            },
            {
                type: 'text',
                right: 10,
                top: 10,
                style: {
                    text: `BAR: ${phaseLabel}`,
                    fill: theme.textMuted,
                    fontSize: 11,
                },
            },
        ];
    }

    _formatTooltip(params, theme) {
        if (!Array.isArray(params) || params.length === 0) return '';

        const candle = params.find(p => p.seriesType === 'candlestick');
        if (!candle) return '';

        const idx = candle.dataIndex;
        const bar = this.bars[idx];
        if (!bar) return '';

        const change = bar.close - bar.open;
        const changePct = bar.open > 0 ? (change / bar.open) * 100 : 0;
        const color = change >= 0 ? theme.up : theme.down;

        const lines = [
            `<div style="font-family: monospace; font-size: 12px;">`,
            `<div style="color: ${theme.textMuted};">${formatUtcDateTime(bar.ts)} UTC</div>`,
            `<div>开: <span style="color:${theme.text}">${formatPrice(bar.open)}</span></div>`,
            `<div>高: <span style="color:${theme.text}">${formatPrice(bar.high)}</span></div>`,
            `<div>低: <span style="color:${theme.text}">${formatPrice(bar.low)}</span></div>`,
            `<div>收: <span style="color:${color}">${formatPrice(bar.close)}</span></div>`,
            `<div>量: <span style="color:${theme.text}">${bar.volume.toFixed(2)}</span></div>`,
            `<div>涨跌: <span style="color:${color}">${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePct.toFixed(2)}%)</span></div>`,
            `</div>`,
        ];
        return lines.join('');
    }

    // ========================================================================
    // 生命周期
    // ========================================================================

    /**
     * 手动触发 resize（例如窗口大小变化）
     */
    resize() {
        if (this._disposed || !this.chart) return;
        this.chart.resize();
    }

    /**
     * 销毁图表，清理所有资源。
     * 销毁后所有方法都是 no-op。
     */
    dispose() {
        if (this._disposed) return;
        this._disposed = true;

        this._cancelScheduledRender();

        if (this._resizeObserver) {
            this._resizeObserver.disconnect();
            this._resizeObserver = null;
        }

        if (this.chart) {
            try {
                this.chart.dispose();
            } catch (e) {
                console.warn('KlineChart dispose error:', e);
            }
            this.chart = null;
        }

        // 清空数据
        this.bars = [];
        this.maData = [];
        this.levels = [];
        this.trades = [];
        this.stateHistory = [];
    }
}


// ============================================================================
// 工厂函数
// ============================================================================
export function createKlineChart(container, options = {}) {
    return new KlineChart(container, options);
}


// ============================================================================
// 默认导出
// ============================================================================
export default KlineChart;
