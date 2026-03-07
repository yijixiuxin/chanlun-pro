window.cl_show_config = JSON.parse(localStorage.getItem('cl_show_config')) || { fx: true, bi: true, xd: true, zsd: true, zs: true, bc: true, mmd: true };
window.cl_independent_drawings = JSON.parse(localStorage.getItem('cl_independent_drawings')) || false;
// -----------------------------------------------------------------------
// 文件名: charts.js
// 修复版: V48_Registry_And_Fix
// -----------------------------------------------------------------------

const CHART_CONFIG = {
    COLORS: {
        DING: "#FA8072", DI: "#1E90FF", BI: "#708090", XD: "#00BFFF", ZSD: "#FFA710",
        BI_ZSS: "#708090", XD_ZSS: "#00BFFF", ZSD_ZSS: "#FFA710",
        BCS: "#D1D4DC", BC_TEXT: "#fccbcd",
        MMD_UP: "#FA8072", MMD_DOWN: "#1E90FF",
        AREA_POS: "#ef5350", AREA_NEG: "#26a69a",
    },
    LINE_STYLES: { SOLID: 0, DOTTED: 1, DASHED: 2 },
    CHART_TYPES: ["fxs", "bis", "xds", "zsds", "bi_zss", "xd_zss", "zsd_zss", "bcs", "mmds"],
};

const DEFAULT_COLORS = {
    bis: CHART_CONFIG.COLORS.BI, xds: CHART_CONFIG.COLORS.XD, zsds: CHART_CONFIG.COLORS.ZSD,
    bi_zss: CHART_CONFIG.COLORS.BI_ZSS, xd_zss: CHART_CONFIG.COLORS.XD_ZSS, zsd_zss: CHART_CONFIG.COLORS.ZSD_ZSS,
};

const DYNAMIC_CHART_COLORS = {
    "1": { ...DEFAULT_COLORS, bis: "#DF8344", xds: "#9C27B0", xd_zss: "#4FADEA", zsds: "#4FADEA", bi_zss: "#FFFF55" },
    "5": { ...DEFAULT_COLORS, bis: "#9C27B0", xds: "#4FADEA", xd_zss: "#EA3323", zsds: "#EA3323", bi_zss: "#4FADEA" },
    "30": { ...DEFAULT_COLORS, bis: "#4FADEA", xds: "#EA3323", xd_zss: "#9FCE63", zsds: "#9FCE63", bi_zss: "#EA3323" },
    "1D": { ...DEFAULT_COLORS, bis: "#EA3323", xds: "#9FCE63", xd_zss: "#4274B1", zsds: "#4274B1", bi_zss: "#9FCE63" },
    "1W": { ...DEFAULT_COLORS, bis: "#9FCE63", xds: "#4274B1", xd_zss: "#C638DD", zsds: "#C638DD", bi_zss: "#4274B1" },
    "1M": { ...DEFAULT_COLORS, bis: "#4274B1", xds: "#C638DD", xd_zss: "#5E813F", zsds: "#5E813F", bi_zss: "#C638DD" },
};

function getDynamicColor(interval, elementType) {
    if (DYNAMIC_CHART_COLORS[interval] && DYNAMIC_CHART_COLORS[interval][elementType]) {
        return DYNAMIC_CHART_COLORS[interval][elementType];
    }
    return DEFAULT_COLORS[elementType] || "#FFFFFF";
}

function debounce(func, wait) {
    let timeout;
    return function (...args) {
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(this, args), wait);
    };
}

function getTVRegistry() {
    if (!window.ChanlunTVRegistry) {
        window.ChanlunTVRegistry = {
            chartManagers: new Map(),
            datafeeds: new Map(),
            widgets: new Map(),
            activeManagerId: null,
        };
    }
    return window.ChanlunTVRegistry;
}

const ChartUtils = {
    createShape(chart, points, options = {}) {
        const defaults = {
            lock: true, disableSelection: true, disableSave: true, disableUndo: true,
            showInObjectsTree: false, overrides: {},
        };
        const config = { ...defaults, ...options };
        try {
            if (!chart) return Promise.reject("Chart object is null");

            return config.shape === "trend_line" || config.shape === "rectangle" || config.shape === "circle"
                ? chart.createMultipointShape(points, config)
                : chart.createShape(points, config);
        } catch (e) {
            console.error("[DEBUG-CHARTS] Shape create failed:", e);
            return Promise.reject(e);
        }
    },
    createFxShape(chart, fx, options = {}) {
        const color = fx.text === "ding" ? CHART_CONFIG.COLORS.DING : CHART_CONFIG.COLORS.DI;
        return this.createShape(chart, fx.points, { shape: "circle", overrides: { backgroundColor: color, color: color, linewidth: 4, ...options.overrides }, ...options });
    },
    createLineShape(chart, line, options = {}) {
        return this.createShape(chart, line.points, { shape: "trend_line", overrides: { linestyle: parseInt(line.linestyle) || 0, linewidth: options.linewidth || 1, linecolor: options.color || CHART_CONFIG.COLORS.BI, ...options.overrides }, ...options });
    },
    createZhongshuShape(chart, zs, options = {}) {
        return this.createShape(chart, zs.points, { shape: "rectangle", overrides: { linestyle: parseInt(zs.linestyle) || 0, linewidth: options.linewidth || 1, linecolor: options.color || CHART_CONFIG.COLORS.BI, backgroundColor: options.color || CHART_CONFIG.COLORS.BI, transparency: 95, color: options.color, "trendline.linecolor": options.color, fillBackground: true, filled: true, ...options.overrides }, ...options });
    },
    createMmdShape(chart, mmd, options = {}) {
        const isBuy = mmd.text.includes("B");
        const color = isBuy ? CHART_CONFIG.COLORS.MMD_UP : CHART_CONFIG.COLORS.MMD_DOWN;
        const shape = isBuy ? "arrow_up" : "arrow_down";
        return this.createShape(chart, mmd.points, { shape, text: mmd.text, overrides: { markerColor: color, backgroundColor: color, color: color, fontsize: 12, transparency: 80, ...options.overrides }, ...options });
    },
    createBcShape(chart, bc, options = {}) {
        return this.createShape(chart, bc.points, { shape: "balloon", text: bc.text, overrides: { markerColor: CHART_CONFIG.COLORS.BCS, backgroundColor: CHART_CONFIG.COLORS.BCS, textColor: CHART_CONFIG.COLORS.BC_TEXT, transparency: 70, backgroundTransparency: 70, fontsize: 12, ...options.overrides }, ...options });
    },
};

class ChartManager {
    constructor(id) {
        this.id = id;
        this.instanceId = `chart-manager-${id}`;
        this.obj_charts = {};
        this.widget = null;
        this.udf_datafeed = null;
        this.chart = null;
        this.debouncedDrawChanlun = debounce(() => this.draw_chanlun(), 300);
        this.macdStudyId = null;
        this._initialLoadDone = false; // 标记初始数据加载完成，之后才响应 visibleRangeChange
        this._intervalVersion = 0;
        this._drawingsCache = new Map();  // Lightweight cache for drawings per interval
        this._intervalSwitchSeq = 0;
        this._drawingsRequestSeq = 0;
        this._latestAppliedBarTime = null;
        this._is_switching_interval = false;
        this._is_drawing_chanlun = false;
        this._drawingStatePhase = null;
        this._activeDrawingMutations = new Set();
        this.isApplyingDrawingState = false;
        this._pendingSaveState = null;
        this._saveScheduled = false;
        this._saveInFlight = null;
        this._barReadyHandler = null;
    }

    getDrawingsCacheKey(symbol, interval) {
        const mode = window.cl_independent_drawings ? "ind" : "shared";
        const resolutionKey = window.cl_independent_drawings ? interval : "all";
        return `${symbol}_${resolutionKey}_${mode}`;
    }

    getCurrentChartIdentity() {
        if (!this.chart) return null;
        return {
            symbol: this.chart.symbol(),
            interval: this.chart.resolution(),
        };
    }

    createContextToken(symbol, interval) {
        return `${this.instanceId}:${symbol}:${interval}:${++this._drawingsRequestSeq}`;
    }

    beginContextSwitch(reason, symbol, interval) {
        const token = this.createContextToken(symbol, interval);
        this._activeContextToken = token;
        this._drawingsLoadToken = token;
        this._is_switching_interval = true;
        this._drawingStatePhase = reason;
        return token;
    }

    isTokenCurrent(token) {
        return !!token && token === this._activeContextToken;
    }

    finishContextSwitch(token) {
        if (!this.isTokenCurrent(token)) return;
        this._is_switching_interval = false;
        this._drawingStatePhase = null;
    }

    markDrawingMutationStart(phase) {
        const mutationPhase = phase || 'unknown';
        this._activeDrawingMutations.add(mutationPhase);
        this._is_drawing_chanlun = this._activeDrawingMutations.size > 0;
        this._drawingStatePhase = mutationPhase;
    }

    markDrawingMutationEnd(phase) {
        const mutationPhase = phase || 'unknown';
        this._activeDrawingMutations.delete(mutationPhase);
        this._is_drawing_chanlun = this._activeDrawingMutations.size > 0;
        this._drawingStatePhase = this._is_drawing_chanlun
            ? Array.from(this._activeDrawingMutations)[this._activeDrawingMutations.size - 1]
            : null;
    }

    shouldSuppressDrawingSave() {
        return this._is_switching_interval || this._activeDrawingMutations.size > 0 || this.isApplyingDrawingState;
    }

    setDrawingsCache(key, state) {
        this._drawingsCache.set(key, state);
        if (this._drawingsCache.size > 200) {
            const oldestKey = this._drawingsCache.keys().next().value;
            if (oldestKey !== undefined) {
                this._drawingsCache.delete(oldestKey);
            }
        }
    }

    isDrawingStateEmpty(state) {
        if (!state || !state.sources) return true;
        if (state.sources instanceof Map) {
            return state.sources.size === 0;
        }
        return Object.keys(state.sources).length === 0;
    }

    scheduleDrawingsSave(reason = 'unspecified') {
        if (!this.chart || this.shouldSuppressDrawingSave()) return Promise.resolve();
        if (typeof this.chart.getLineToolsState !== 'function') return Promise.resolve();

        try {
            this._pendingSaveState = this.chart.getLineToolsState();
        } catch (e) {
            console.debug('[DEBUG-CHARTS] getLineToolsState failed', e);
            return Promise.resolve();
        }

        if (this._saveScheduled) {
            return this._saveInFlight || Promise.resolve();
        }

        this._saveScheduled = true;
        this._saveInFlight = new Promise((resolve) => {
            setTimeout(async () => {
                this._saveScheduled = false;
                const state = this._pendingSaveState;
                this._pendingSaveState = null;

                if (!state || this.shouldSuppressDrawingSave()) {
                    this._saveInFlight = null;
                    resolve();
                    return;
                }

                try {
                    if (this.save_load_adapter && typeof this.save_load_adapter.saveLineToolsAndGroups === 'function') {
                        await this.save_load_adapter.saveLineToolsAndGroups('default', 'default', state, { reason });
                    } else if (typeof this.widget?.saveChartToServer === 'function') {
                        await this.widget.saveChartToServer();
                    }
                } catch (e) {
                    console.debug('[DEBUG-CHARTS] drawing save skipped', e);
                }

                if (this._pendingSaveState && !this.shouldSuppressDrawingSave()) {
                    this._saveInFlight = null;
                    resolve(this.scheduleDrawingsSave(reason));
                    return;
                }

                this._saveInFlight = null;
                resolve();
            }, 300);
        });

        return this._saveInFlight;
    }

    async applyUserDrawingsState(state, token, cacheKey) {
        if (!state || !this.chart || !this.isTokenCurrent(token)) {
            return false;
        }
        this.isApplyingDrawingState = true;
        this.markDrawingMutationStart('apply-user-drawings');
        try {
            this.chart.removeAllShapes();
            if (!this.isTokenCurrent(token)) {
                return false;
            }
            this.chart.applyLineToolsState(state);
            if (cacheKey) {
                this.setDrawingsCache(cacheKey, state);
            }
            this.debouncedDrawChanlun();
            return true;
        } finally {
            this.isApplyingDrawingState = false;
            this.markDrawingMutationEnd('apply-user-drawings');
        }
    }

    async reloadDrawingsForCurrentContext(reason) {
        if (!this.chart || !this.save_load_adapter || typeof this.save_load_adapter.loadLineToolsAndGroups !== 'function') {
            return;
        }

        const identity = this.getCurrentChartIdentity();
        if (!identity) return;

        const { symbol, interval } = identity;
        const token = this.beginContextSwitch(reason, symbol, interval);
        const cacheKey = this.getDrawingsCacheKey(symbol, interval);
        const cachedDrawings = this._drawingsCache.get(cacheKey);

        try {
            if (!this.isDrawingStateEmpty(cachedDrawings)) {
                await this.applyUserDrawingsState(cachedDrawings, token, cacheKey);
                return;
            }

            if (this.chart) {
                this.debouncedDrawChanlun();
            }

            const state = await this.save_load_adapter.loadLineToolsAndGroups('default', 'default', 'load', {
                resolution: interval,
                symbol,
                token,
            });

            if (!this.isTokenCurrent(token)) {
                return;
            }

            if (!this.isDrawingStateEmpty(state)) {
                await this.applyUserDrawingsState(state, token, cacheKey);
            }
        } catch (e) {
            console.error(`[DEBUG-CHARTS] Failed to reload drawings (${reason})`, e);
        } finally {
            this.finishContextSwitch(token);
        }
    }

    handleBarsReadyEvent(event) {
        const detail = event?.detail || {};
        if (detail.managerId && detail.managerId !== this.instanceId) {
            return;
        }
        const identity = this.getCurrentChartIdentity();
        if (!identity) return;
        if (detail.symbol && detail.symbol !== identity.symbol.toLowerCase()) {
            return;
        }
        if (detail.resolution && detail.resolution !== identity.interval.toLowerCase()) {
            return;
        }
        this._initialLoadDone = true;
        this.debouncedDrawChanlun();
    }

    init() {
        this.udf_datafeed = new Datafeeds.UDFCompatibleDatafeed("/tv", 3000, undefined, {
            managerId: this.instanceId,
        });

        const registry = getTVRegistry();
        registry.chartManagers.set(this.instanceId, this);
        registry.datafeeds.set(this.instanceId, this.udf_datafeed);
        registry.activeManagerId = this.instanceId;

        // --- 核心修复：多图表 Datafeed 注册机制 ---
        if (!window.GlobalTVDatafeeds) {
            window.GlobalTVDatafeeds = [];
        }
        if (window.GlobalTVDatafeeds.length > 10) {
            window.GlobalTVDatafeeds.shift();
        }
        window.GlobalTVDatafeeds.push(this.udf_datafeed);
        window.tvDatafeed = this.udf_datafeed; // 兼容旧代码
        // ---------------------------------------

        this._barReadyHandler = this.handleBarsReadyEvent.bind(this);
        window.addEventListener('chanlun-bars-ready', this._barReadyHandler);

        const self = this;
        const client_id = "chanlun_pro_" + Utils.get_market() + "_" + this.id;
        const user_id = "999";
        const save_load_adapter = {
            getAllCharts: function () {
                return fetch("/tv/1.1/charts?client=" + client_id + "&user=" + user_id)
                    .then(res => res.json())
                    .then(res => res.status === 'ok' ? res.data : []);
            },
            removeChart: function (chartId) {
                return fetch("/tv/1.1/charts?client=" + client_id + "&user=" + user_id + "&chart=" + chartId, { method: "DELETE" })
                    .then(res => res.json())
                    .then(res => res.status === 'ok');
            },
            saveChart: function (chartData) {
                console.log("[DEBUG-CHARTS] saveChart called", chartData);
                return fetch("/tv/1.1/charts?client=" + client_id + "&user=" + user_id + (chartData.id ? "&chart=" + chartData.id : ""), {
                    method: "POST",
                    body: new URLSearchParams({
                        name: chartData.name,
                        symbol: chartData.symbol,
                        resolution: chartData.resolution,
                        content: chartData.content
                    })
                })
                    .then(res => res.json())
                    .then(res => {
                        console.log("[DEBUG-CHARTS] saveChart response", res);
                        return res.status === 'ok' ? (res.id || chartData.id || "default") : null;
                    })
                    .catch(err => {
                        console.error("[DEBUG-CHARTS] saveChart error", err);
                        return null;
                    });
            },
            getChartContent: function (chartId) {
                console.log("[DEBUG-CHARTS] getChartContent called", chartId);
                return fetch("/tv/1.1/charts?client=" + client_id + "&user=" + user_id + "&chart=" + chartId)
                    .then(res => res.json())
                    .then(res => res.status === 'ok' ? res.data.content : null);
            },
            getAllStudyTemplates: function () {
                return fetch("/tv/1.1/study_templates?client=" + client_id + "&user=" + user_id)
                    .then(res => res.json())
                    .then(res => res.status === 'ok' ? res.data : []);
            },
            removeStudyTemplate: function (templateData) {
                return fetch("/tv/1.1/study_templates?client=" + client_id + "&user=" + user_id + "&template=" + templateData.name, { method: "DELETE" })
                    .then(res => res.json())
                    .then(res => res.status === 'ok');
            },
            saveStudyTemplate: function (templateData) {
                return fetch("/tv/1.1/study_templates?client=" + client_id + "&user=" + user_id, {
                    method: "POST",
                    body: new URLSearchParams({
                        name: templateData.name,
                        content: templateData.content
                    })
                })
                    .then(res => res.json())
                    .then(res => res.status === 'ok');
            },
            getStudyTemplateContent: function (templateData) {
                return fetch("/tv/1.1/study_templates?client=" + client_id + "&user=" + user_id + "&template=" + templateData.name)
                    .then(res => res.json())
                    .then(res => res.status === 'ok' ? res.data.content : null);
            },
            saveLineToolsAndGroups: function (layoutId, chartId, state, options = {}) {
                console.log("[DEBUG-CHARTS] saveLineToolsAndGroups called", { layoutId, chartId, state, options });
                return new Promise((resolve) => {
                    if (self.shouldSuppressDrawingSave()) {
                        console.log("[DEBUG-CHARTS] Skip saveLineToolsAndGroups due to active drawing mutation");
                        return resolve();
                    }
                    const rawResolution = self.chart ? self.chart.resolution() : Utils.get_local_data(Utils.get_market() + "_interval_" + self.id);
                    const resolution = window.cl_independent_drawings ? rawResolution : 'all';
                    const symbol = self.chart ? self.chart.symbol() : Utils.get_market() + ":" + Utils.get_code();
                    const cacheKey = self.getDrawingsCacheKey(symbol, rawResolution);

                    let processedState = { ...state };
                    if (state.sources) {
                        if (state.sources instanceof Map || typeof state.sources.entries === 'function') {
                            try {
                                processedState.sources = Object.fromEntries(state.sources);
                            } catch (e) {
                                processedState.sources = {};
                                for (let [k, v] of state.sources.entries()) {
                                    processedState.sources[k] = v;
                                }
                            }
                        } else if (typeof state.sources === 'object') {
                            try {
                                processedState.sources = JSON.parse(JSON.stringify(state.sources));
                            } catch (e) {
                                processedState.sources = state.sources;
                            }
                        }
                    }

                    console.log("[DEBUG-CHARTS] Saving drawings for", { symbol, resolution, sourcesCount: Object.keys(processedState.sources || {}).length, rawSources: state.sources, reason: options.reason });

                    fetch("/tv/1.1/drawings?client=" + client_id + "&user=" + user_id + "&chart=" + chartId + "&layout=" + layoutId + "&symbol=" + symbol + "&resolution=" + resolution, {
                        method: "POST",
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ state: processedState })
                    }).then(res => res.json()).then(res => {
                        console.log("[DEBUG-CHARTS] saveLineToolsAndGroups response", res);
                        if (state && state.sources) {
                            self.setDrawingsCache(cacheKey, state);
                        }
                        resolve();
                    }).catch(err => {
                        console.error("[DEBUG-CHARTS] saveLineToolsAndGroups error", err);
                        resolve();
                    });
                });
            },
            loadLineToolsAndGroups: function (layoutId, chartId, requestType, requestContext = {}) {
                console.log("[DEBUG-CHARTS] loadLineToolsAndGroups called", { layoutId, chartId, requestType, requestContext });
                return new Promise((resolve) => {
                    const resolution = requestContext.resolution;
                    const symbol = requestContext.symbol;
                    const token = requestContext.token;

                    if (requestType !== 'mainSeriesLineTools' && requestType !== 'load') {
                        return resolve(null);
                    }
                    const loadSymbol = symbol || (self.chart ? self.chart.symbol() : '');
                    const rawResolution = resolution || (self.chart ? self.chart.resolution() : '');
                    const loadResolution = window.cl_independent_drawings ? rawResolution : 'all';
                    if (!loadSymbol || !loadResolution) {
                        return resolve(null);
                    }

                    fetch("/tv/1.1/drawings?client=" + client_id + "&user=" + user_id + "&chart=" + chartId + "&layout=" + layoutId + "&symbol=" + loadSymbol + "&resolution=" + loadResolution)
                        .then(res => res.json())
                        .then(res => {
                            if (token && !self.isTokenCurrent(token)) {
                                return resolve(null);
                            }
                            if (res.status === 'ok' && res.data && Object.keys(res.data).length > 0) {
                                let loadedState = res.data;
                                const sources = new Map();
                                if (loadedState.sources) {
                                    for (let [key, state] of Object.entries(loadedState.sources)) {
                                        sources.set(key, state);
                                    }
                                }
                                const groups = new Map();
                                if (loadedState.groups) {
                                    for (let [key, state] of Object.entries(loadedState.groups)) {
                                        groups.set(key, state);
                                    }
                                }
                                resolve({ sources, groups });
                            } else {
                                resolve(null);
                            }
                        }).catch(err => {
                            console.error("[DEBUG-CHARTS] loadLineToolsAndGroups error:", err);
                            resolve(null);
                        });
                });
            }
        };
        this.save_load_adapter = save_load_adapter;

        this.widget = window.tvWidget = new TradingView.widget({
            debug: false, autosize: true, fullscreen: false,
            container: "tv_chart_container_" + this.id,
            symbol: Utils.get_market() + ":" + Utils.get_code(),
            interval: Utils.get_local_data(Utils.get_market() + "_interval_" + this.id),
            datafeed: this.udf_datafeed,
            library_path: "static/charting_library/",
            theme: Utils.get_local_data("theme"),
            numeric_formatting: { decimal_sign: "." },
            time_frames: [], timezone: "Asia/Shanghai", locale: "zh",
            symbol_search_request_delay: 100, auto_save_delay: 5, study_count_limit: 100,
            disabled_features: ["go_to_date"],
            enabled_features: ["study_templates", "seconds_resolution", "saveload_separate_drawings_storage"],
            saved_data_meta_info: { uid: 1, name: "default", description: "default" },
            save_load_adapter: save_load_adapter,
            client_id: "chanlun_pro_" + Utils.get_market() + "_" + this.id,
            user_id: "999", load_last_chart: true,
            custom_indicators_getter: this.getCustomIndicators,
            time_scale: { min_bar_spacing: 0.05, max_bar_spacing: 800 },
        });
        this.setupEventListeners();
        return this;
    }

    getCustomIndicators(PineJS) {
        if (typeof TvIdxMACDBackend === 'undefined') {
            return Promise.resolve([]);
        }
        return Promise.resolve([
            TvIdxMACDBackend.idx(PineJS),
            TvIdxAMA.idx(PineJS), TvIdxATR.idx(PineJS), TvIdxCDBB.idx(PineJS),
            TvIdxCMCM.idx(PineJS), TvIdxDemo.idx(PineJS), TvIdxFCX.idx(PineJS),
            TvIdxHDLY.idx(PineJS), TvIdxHeima.idx(PineJS), TvIdxHLBLW.idx(PineJS),
            TvIdxHLFTX.idx(PineJS), TvIdxKDJ.idx(PineJS), TvIdxLTQS.idx(PineJS),
            TvIdxMA.idx(PineJS), TvIdxMACDBL.idx(PineJS), TvIdxVegasMA.idx(PineJS),
            TvIdxVOL.idx(PineJS),
        ]);
    }

    setupEventListeners() {
        const global_widget = this.widget;
        const self = this;
        this.measureShapeId = null;

        // 深度扫描函数：在复杂的对象里寻找坐标点
        const scanForPoints = (obj, depth = 0) => {
            if (!obj || depth > 3) return null;
            try {
                // 1. 检查当前层级是否有 points
                if (Array.isArray(obj.points) && obj.points.length >= 2 && obj.points[0].time) return obj.points;
                if (Array.isArray(obj._points) && obj._points.length >= 2 && obj._points[0].time) return obj._points;

                // 2. 遍历属性寻找
                const keys = Object.keys(obj);
                for (let k of keys) {
                    const val = obj[k];
                    if (val && typeof val === 'object') {
                        // 特征匹配：如果是数组且看起来像坐标
                        if (Array.isArray(val) && val.length >= 2 && val[0] && val[0].hasOwnProperty('time')) {
                            console.log(`[MACD] 通过深度扫描在属性 [${k}] 中找到坐标!`);
                            return val;
                        }
                        // 递归查找 (限制深度防止死循环)
                        if (!Array.isArray(val) && k !== 'parent' && k !== 'chart') {
                            const found = scanForPoints(val, depth + 1);
                            if (found) return found;
                        }
                    }
                }
            } catch (e) { }
            return null;
        };

        this.widget.headerReady().then(function () {
            var btnDisplay = global_widget.createButton();
            btnDisplay.textContent = "缠论显示设置 ▾";
            btnDisplay.addEventListener("click", function () {
                if ($('#cl_display_menu').length > 0) {
                    $('#cl_display_menu').remove();
                    $('#cl_menu_backdrop').remove();
                    return;
                }

                let html = `
                    <div id="cl_display_menu" style="position: absolute; z-index: 99999999; background: #fff; border: 1px solid #ccc; box-shadow: 0 2px 10px rgba(0,0,0,0.2); border-radius: 4px; padding: 10px; line-height: 28px; font-size: 14px; color: #333;">
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_fx" ${window.cl_show_config.fx ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 分型</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_bi" ${window.cl_show_config.bi ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 笔</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_xd" ${window.cl_show_config.xd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 线段</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_zsd" ${window.cl_show_config.zsd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 走势段</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_zs" ${window.cl_show_config.zs ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 中枢</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_bc" ${window.cl_show_config.bc ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 背驰</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_mmd" ${window.cl_show_config.mmd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 买卖点</label>
                        <hr style="margin: 5px 0;">
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_independent_drawings" ${window.cl_independent_drawings ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 独立周期画线</label>
                    </div>
                `;
                $('body').append(html);

                // Get the button's position
                const btnRect = btnDisplay.getBoundingClientRect();

                // Position the menu right below the button
                $('#cl_display_menu').css({
                    top: (btnRect.bottom + window.scrollY + 5) + 'px',
                    left: (btnRect.left + window.scrollX) + 'px'
                });

                const keys = ['fx', 'bi', 'xd', 'zsd', 'zs', 'bc', 'mmd'];
                keys.forEach(k => {
                    $('#cl_cb_' + k).change(function () {
                        window.cl_show_config[k] = $(this).is(':checked');
                        localStorage.setItem('cl_show_config', JSON.stringify(window.cl_show_config));
                        self.debouncedDrawChanlun();
                    });
                });

                $('#cl_cb_independent_drawings').change(function () {
                    window.cl_independent_drawings = $(this).is(':checked');
                    localStorage.setItem('cl_independent_drawings', JSON.stringify(window.cl_independent_drawings));
                    self._drawingsCache.clear();
                    if (self.chart && self.save_load_adapter) {
                        self.reloadDrawingsForCurrentContext('toggle-drawing-mode');
                    }
                    layer.msg(window.cl_independent_drawings ? '已切换为独立周期画线' : '已切换为共享画线', { time: 1000 });
                });

                // 点击非弹框区域时关闭菜单（使用透明遮罩确保可靠捕获点击）
                const backdrop = $('<div id="cl_menu_backdrop" style="position:fixed;top:0;left:0;width:100%;height:100%;z-index:99999998;background:transparent;"></div>');
                $('body').append(backdrop);
                backdrop.on('click', function () {
                    $('#cl_display_menu').remove();
                    $(this).remove();
                });
            });

            var buttonReload = global_widget.createButton();
            buttonReload.textContent = "重新加载数据";
            buttonReload.addEventListener("click", function () { global_widget.resetCache(); global_widget.activeChart().resetData(); });

            var buttonHideMark = global_widget.createButton();
            buttonHideMark.textContent = "隐藏标记";
            buttonHideMark.addEventListener("click", function () { global_widget.activeChart().clearMarks(); });

            var buttonDeleteMark = global_widget.createButton();
            buttonDeleteMark.textContent = "删除标记";
            buttonDeleteMark.addEventListener("click", function () {
                let symbol = global_widget.symbolInterval();
                $.post({ type: "POST", url: "/tv/del_marks", dataType: "json", data: { symbol: symbol.symbol }, success: function (res) { if (res.status == "ok") { global_widget.activeChart().clearMarks(); layer.msg("删除标记成功"); } } });
            });

        });
        this.widget.onChartReady(() => {
            this.chart = this.widget.activeChart();
            if (!this.chart) return;
            // 默认指标加载已移至 handleDataReady()，确保数据就绪后再创建
            this.chart.applyOverrides({ "mainSeriesProperties.candleStyle.upColor": "#ef5350", "mainSeriesProperties.candleStyle.downColor": "#26a69a" });
            const registry = getTVRegistry();
            registry.widgets.set(this.instanceId, this.widget);
            registry.activeManagerId = this.instanceId;
            window.tvWidget = this.widget;
            this.widget._chanlunManagerId = this.instanceId;
            this.udf_datafeed._chanlunManagerId = this.instanceId;

            this.chart.onSymbolChanged().subscribe(null, (s) => this.handleSymbolChange(s));
            this.chart.onIntervalChanged().subscribe(null, (i) => this.handleIntervalChange(i));
            this.chart.onDataLoaded().subscribe(null, () => this.handleDataReady(), true);
            this.chart.dataReady(() => this.handleDataReady());
            this.widget.subscribe("onTick", () => this.handleTick());
            this.chart.onVisibleRangeChanged().subscribe(null, () => this.handleVisibleRangeChange());

            this.reloadDrawingsForCurrentContext('initial-load');

            this.widget.subscribe('drawing_event', (id, eventType) => {
                if (this.shouldSuppressDrawingSave()) return;
                console.log("[DEBUG-CHARTS] drawing_event", id, eventType);
                this.scheduleDrawingsSave('drawing_event');
            });
            this.widget.subscribe('onAutoSaveNeeded', () => {
                if (this.shouldSuppressDrawingSave()) return;
                console.log("[DEBUG-CHARTS] onAutoSaveNeeded");
                this.scheduleDrawingsSave('auto_save');
            });
        });
    }

    handleSymbolChange(symbol) {
        if (!symbol?.ticker) return;
        const [market, code] = symbol.ticker.split(":");
        if (!market || !code) return;
        if (Utils.get_market() !== market) { Utils.set_local_data("market", market); location.reload(); return; }
        Utils.set_local_data("market", market); Utils.set_local_data(`${market}_code`, code);
        this._initialLoadDone = false;
        this._latestAppliedBarTime = null;
        this.clear_draw_chanlun();
        this.reloadDrawingsForCurrentContext('symbol-change');
        if (typeof ZiXuan.render_zixuan_opts === "function") ZiXuan.render_zixuan_opts();
    }
    handleIntervalChange(interval) {
        if (!interval) return;
        const market = Utils.get_market(); if (!market) return;

        this._initialLoadDone = false;
        this._drawRetryCount = 0;
        this._latestAppliedBarTime = null;
        const currentSeq = ++this._intervalSwitchSeq;
        this._intervalVersion++;
        console.log("[DEBUG-CHARTS] Interval Changed to:", interval, "version:", this._intervalVersion, "seq:", currentSeq);
        Utils.set_local_data(`${market}_interval_${this.id}`, interval);
        this.clear_draw_chanlun();
        this.reloadDrawingsForCurrentContext('interval-change');
    }

    handleDataReady() {
        this._initialLoadDone = true;
        this.debouncedDrawChanlun();
    }
    handleTick() {
        const identity = this.getCurrentChartIdentity();
        if (!identity) return;
        const symbolResKey = `${identity.symbol.toString().toLowerCase()}${identity.interval.toString().toLowerCase()}`;
        const barsResult = this.udf_datafeed?._historyProvider?.bars_result?.get(symbolResKey);
        const latestBar = barsResult?.bars?.[barsResult.bars.length - 1];
        if (!latestBar || latestBar.time === this._latestAppliedBarTime) {
            return;
        }
        this._latestAppliedBarTime = latestBar.time;
        this.debouncedDrawChanlun();
    }
    handleVisibleRangeChange() { if (this._initialLoadDone) this.debouncedDrawChanlun(); }

    safeRemove(entityId) {
        if (!entityId) return Promise.resolve();
        if (typeof entityId.then === 'function') {
            return entityId.then(id => {
                if (id) {
                    try { this.chart.removeEntity(id); } catch (e) { }
                }
            }).catch(e => { });
        } else {
            try { this.chart.removeEntity(entityId); } catch (e) { }
            return Promise.resolve();
        }
    }

    clear_draw_chanlun(clear_type) {
        this.markDrawingMutationStart('chanlun-clear');
        const removePromises = [];
        if (clear_type == "last") {
            for (const symbolKey in this.obj_charts) {
                for (const chartType in this.obj_charts[symbolKey]) {
                    if (this.obj_charts[symbolKey][chartType].length == 0) continue;
                    const maxTime = Math.max(...this.obj_charts[symbolKey][chartType].map((item) => item.time));
                    for (const _i in this.obj_charts[symbolKey][chartType]) {
                        const item = this.obj_charts[symbolKey][chartType][_i];
                        if (item.time == maxTime) {
                            removePromises.push(this.safeRemove(item.id));
                        }
                    }
                    this.obj_charts[symbolKey][chartType] = this.obj_charts[symbolKey][chartType].filter((item) => item.time != maxTime);
                }
            }
        } else {
            Object.values(this.obj_charts).forEach((symbolData) => {
                Object.values(symbolData).forEach((chartItems) => {
                    chartItems.forEach((item) => {
                        removePromises.push(this.safeRemove(item.id));
                    });
                });
            });
            this.obj_charts = {};
        }
        Promise.allSettled(removePromises).then(() => {
            this.markDrawingMutationEnd('chanlun-clear');
        });
    }

    getChartData() {
        const symbolInterval = this.widget.symbolInterval(); if (!symbolInterval) return null;
        const symbolResKey = `${symbolInterval.symbol.toString().toLowerCase()}${symbolInterval.interval.toString().toLowerCase()}`;
        const barsResult = this.udf_datafeed?._historyProvider?.bars_result?.get(symbolResKey);

        if (!barsResult) {
            const availableKeys = this.udf_datafeed?._historyProvider?.bars_result ? Array.from(this.udf_datafeed._historyProvider.bars_result.keys()) : [];
            console.warn(`[DEBUG-CHARTS] getChartData for ${symbolResKey}: NOT FOUND. Available keys:`, availableKeys);
            return null;
        }

        if (!this.chart) {
            console.warn("[DEBUG-CHARTS] getChartData aborted: this.chart is null.");
            return null;
        }
        const visibleRange = this.chart.getVisibleRange();
        if (!visibleRange || !visibleRange.from || !visibleRange.to) {
            console.warn("[DEBUG-CHARTS] getChartData aborted: VisibleRange invalid (chart loading).");
            return null;
        }

        const from = visibleRange.from;
        const symbolKey = `${symbolInterval.symbol}_${symbolInterval.interval}`;
        return { symbolKey, barsResult, from };
    }



    makeKey(item) {
        if (Array.isArray(item.points)) {
            return item.points.map(p => `${p.time}_${p.price}`).join('_') + `_${item.linestyle || 0}`;
        } else if (item.points?.time !== undefined) {
            return `${item.points.time}_${item.points.price}_${item.text || ''}`;
        }
        return `${item.id || Math.random()}`;
    }

    getUniqueRenderList(sourceList) {
        if (!sourceList || !Array.isArray(sourceList)) return [];
        const finished = [];
        const unfinished = [];
        sourceList.forEach(item => {
            if (item.linestyle == '1' || item.linestyle == 1) {
                unfinished.push(item);
            } else {
                finished.push(item);
            }
        });
        if (unfinished.length > 0) {
            finished.push(unfinished[unfinished.length - 1]);
        }
        return finished;
    }

    reconcile(type, sourceList, from, symbolKey, createFunc, useUnique = true) {
        const container = this.obj_charts[symbolKey][type];
        let renderList = sourceList || [];
        if (useUnique) {
            renderList = this.getUniqueRenderList(renderList);
        }

        const newKeys = new Set();
        const itemsToProcess = [];

        renderList.forEach(item => {
            const itemTime = Array.isArray(item.points) ? item.points[0]?.time : item.points?.time;
            if (itemTime >= from) {
                const key = this.makeKey(item);
                newKeys.add(key);
                itemsToProcess.push({ item, key, time: itemTime });
            }
        });

        // 1. Remove items not in new list or outside window (batch filter for O(n) performance)
        const toKeep = [];
        for (const existing of container) {
            if (newKeys.has(existing.key) && existing.time >= from) {
                toKeep.push(existing);
            } else {
                this.safeRemove(existing.id);
            }
        }
        container.length = 0;
        toKeep.forEach(item => container.push(item));

        // 2. Create new items
        const existingKeys = new Set(container.map(item => item.key));
        itemsToProcess.forEach(({ item, key, time }) => {
            if (!existingKeys.has(key)) {
                const id = createFunc(item);
                container.push({
                    time: time,
                    key: key,
                    isUnfinished: (item.linestyle == '1' || item.linestyle == 1),
                    id: id
                });
            }
        });
    }

    initChartContainer(symbolKey) {
        if (!this.obj_charts[symbolKey]) {
            this.obj_charts[symbolKey] = {};
            CHART_CONFIG.CHART_TYPES.forEach((type) => { this.obj_charts[symbolKey][type] = []; });
        }
        return this.obj_charts[symbolKey];
    }

    getMACDStudyId() {
        if (this.macdStudyId) return this.macdStudyId;
        const studies = this.chart.getAllStudies();
        const macdStudy = studies.find(s => s.name === 'MACD_HTF');
        if (macdStudy) { this.macdStudyId = macdStudy.id; return macdStudy.id; }
        return null;
    }

    drawChartElements(chartData, currentInterval) {
        const { symbolKey, barsResult, from } = chartData;
        if (!barsResult) return;
        this.initChartContainer(symbolKey);

        const safeCreate = (promise, type) => {
            if (promise && typeof promise.then === 'function') {
                return promise.catch(e => {
                    console.error(`[DEBUG-CHARTS] Error creating shape (${type}):`, e);
                    return null;
                });
            }
            return promise;
        };

        // Reconcile each type
        this.reconcile('fxs', window.cl_show_config.fx ? barsResult.fxs : [], from, symbolKey, (item) => safeCreate(ChartUtils.createFxShape(this.chart, item), 'fx'), false);
        this.reconcile('bis', window.cl_show_config.bi ? barsResult.bis : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "bis"), linewidth: 2 }), 'bi'));
        this.reconcile('xds', window.cl_show_config.xd ? barsResult.xds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "xds"), linewidth: 2 }), 'xd'));
        this.reconcile('zsds', window.cl_show_config.zsd ? barsResult.zsds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "zsds"), linewidth: 3 }), 'zsd'));
        this.reconcile('bi_zss', window.cl_show_config.zs ? barsResult.bi_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: CHART_CONFIG.COLORS.BI_ZSS, linewidth: 1 }), 'bi_zs'));
        this.reconcile('xd_zss', window.cl_show_config.zs ? barsResult.xd_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: getDynamicColor(currentInterval, "xd_zss"), linewidth: 2 }), 'xd_zs'));
        this.reconcile('zsd_zss', window.cl_show_config.zs ? barsResult.zsd_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: CHART_CONFIG.COLORS.ZSD_ZSS, linewidth: 2 }), 'zsd_zs'));
        this.reconcile('bcs', window.cl_show_config.bc ? barsResult.bcs : [], from, symbolKey, (item) => safeCreate(ChartUtils.createBcShape(this.chart, item), 'bc'), false);
        this.reconcile('mmds', window.cl_show_config.mmd ? barsResult.mmds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createMmdShape(this.chart, item), 'mmd'), false);
    }

    async draw_chanlun() {
        const currentVersion = this._intervalVersion;
        const capturedSeq = this._intervalSwitchSeq;

        if (!this.chart) {
            try {
                this.chart = this.widget.activeChart();
            } catch (e) {
                console.warn("[DEBUG-CHARTS] draw_chanlun: activeChart not available");
                return;
            }
        }

        await new Promise(resolve => setTimeout(resolve, 0));

        if (this._intervalVersion !== currentVersion || capturedSeq !== this._intervalSwitchSeq) {
            console.warn("周期已切换，丢弃过期的缠论渲染任务");
            return;
        }

        const chartData = this.getChartData();
        if (!chartData) {
            if (!this._drawRetryCount) this._drawRetryCount = 0;
            if (this._drawRetryCount < 10) {
                this._drawRetryCount++;
                setTimeout(() => this.debouncedDrawChanlun(), 500);
            }
            return;
        }
        this._drawRetryCount = 0;

        const symbolInterval = this.widget.symbolInterval();
        if (!symbolInterval) {
            console.warn("[DEBUG-CHARTS] draw_chanlun aborted: symbolInterval is null");
            return;
        }

        console.log("[DEBUG-CHARTS] draw_chanlun executing for", symbolInterval.interval);
        console.log("[DEBUG-CHARTS] barsResult shapes:", {
            fxs: chartData.barsResult.fxs?.length || 0,
            bis: chartData.barsResult.bis?.length || 0,
            xds: chartData.barsResult.xds?.length || 0,
            zsds: chartData.barsResult.zsds?.length || 0,
            bi_zss: chartData.barsResult.bi_zss?.length || 0,
            bcs: chartData.barsResult.bcs?.length || 0,
            mmds: chartData.barsResult.mmds?.length || 0,
        });
        this.markDrawingMutationStart('chanlun-redraw');
        try {
            this.drawChartElements(chartData, symbolInterval.interval);
        } finally {
            this.markDrawingMutationEnd('chanlun-redraw');
        }
    }

    dispose() {
        if (this._barReadyHandler) {
            window.removeEventListener('chanlun-bars-ready', this._barReadyHandler);
            this._barReadyHandler = null;
        }
        const registry = getTVRegistry();
        registry.chartManagers.delete(this.instanceId);
        registry.datafeeds.delete(this.instanceId);
        registry.widgets.delete(this.instanceId);
        if (registry.activeManagerId === this.instanceId) {
            registry.activeManagerId = null;
        }
    }
}

var Charts = (function () {
    return {
        show_tv_chart: function (id) {
            const chartManager = new ChartManager(id).init();
            return chartManager.widget;
        },
    };
})();
