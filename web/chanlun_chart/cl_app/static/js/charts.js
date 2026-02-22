window.cl_show_config = JSON.parse(localStorage.getItem('cl_show_config')) || { fx: true, bi: true, xd: true, zsd: true, zs: true, bc: true, mmd: true };
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
        this.obj_charts = {};
        this.widget = null;
        this.udf_datafeed = null;
        this.chart = null;
        this.debouncedDrawChanlun = debounce(() => this.draw_chanlun(), 500);
        this.macdStudyId = null;
    }

    init() {
        this.udf_datafeed = new Datafeeds.UDFCompatibleDatafeed("/tv", 3000);

        // --- 核心修复：多图表 Datafeed 注册机制 ---
        if (!window.GlobalTVDatafeeds) {
            window.GlobalTVDatafeeds = [];
        }
        // 清理旧的
        if (window.GlobalTVDatafeeds.length > 10) {
            window.GlobalTVDatafeeds.shift();
        }
        window.GlobalTVDatafeeds.push(this.udf_datafeed);
        window.tvDatafeed = this.udf_datafeed; // 兼容旧代码
        // ---------------------------------------

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
            enabled_features: ["study_templates", "seconds_resolution"],
            saved_data_meta_info: { uid: 1, name: "default", description: "default" },
            charts_storage_url: "/tv", charts_storage_api_version: "1.1",
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
                let html = `
                    <div style="padding: 15px; line-height: 28px; font-size: 14px;">
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_fx" ${window.cl_show_config.fx ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 分型</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_bi" ${window.cl_show_config.bi ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 笔</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_xd" ${window.cl_show_config.xd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 线段</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_zsd" ${window.cl_show_config.zsd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 走势段</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_zs" ${window.cl_show_config.zs ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 中枢</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_bc" ${window.cl_show_config.bc ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 背驰</label>
                        <label style="display:block; cursor:pointer;"><input type="checkbox" id="cl_cb_mmd" ${window.cl_show_config.mmd ? 'checked' : ''} style="margin-right: 8px; vertical-align: middle;"> 买卖点</label>
                    </div>
                `;
                layer.open({
                    type: 1,
                    title: '缠论显示/隐藏',
                    area: ['200px', '320px'],
                    shade: 0,
                    offset: 'rt',
                    content: html,
                    success: function() {
                        const keys = ['fx', 'bi', 'xd', 'zsd', 'zs', 'bc', 'mmd'];
                        keys.forEach(k => {
                            $('#cl_cb_' + k).change(function() {
                                window.cl_show_config[k] = $(this).is(':checked');
                                localStorage.setItem('cl_show_config', JSON.stringify(window.cl_show_config));
                                self.debouncedDrawChanlun();
                            });
                        });
                    }
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
            this.chart.onSymbolChanged().subscribe(null, (s) => this.handleSymbolChange(s));
            this.chart.onIntervalChanged().subscribe(null, (i) => this.handleIntervalChange(i));
            this.chart.onDataLoaded().subscribe(null, () => { setTimeout(() => this.debouncedDrawChanlun(), 200); }, true);
            this.chart.dataReady(() => this.handleDataReady());
            this.widget.subscribe("onTick", () => this.handleTick());
            this.chart.onVisibleRangeChanged().subscribe(null, () => this.handleVisibleRangeChange());
        });
    }

    handleSymbolChange(symbol) {
        if (!symbol?.ticker) return;
        const [market, code] = symbol.ticker.split(":");
        if (!market || !code) return;
        if (Utils.get_market() !== market) { Utils.set_local_data("market", market); location.reload(); return; }
        Utils.set_local_data("market", market); Utils.set_local_data(`${market}_code`, code);
        this.clear_draw_chanlun();
        if (typeof ZiXuan.render_zixuan_opts === "function") ZiXuan.render_zixuan_opts();
        this.debouncedDrawChanlun();
    }
    handleIntervalChange(interval) {
        if (!interval) return;
        const market = Utils.get_market(); if (!market) return;
        console.log("[DEBUG-CHARTS] Interval Changed to:", interval);
        Utils.set_local_data(`${market}_interval_${this.id}`, interval);
        this.clear_draw_chanlun();
        this.debouncedDrawChanlun();
    }
    handleDataReady() { this.debouncedDrawChanlun(); }
    handleTick() { this.debouncedDrawChanlun(); }
    handleVisibleRangeChange() { this.debouncedDrawChanlun(); }

    safeRemove(entityId) {
        if (!entityId) return;
        if (typeof entityId.then === 'function') {
            entityId.then(id => {
                if (id) {
                    try { this.chart.removeEntity(id); } catch (e) { }
                }
            }).catch(e => { });
        } else {
            try { this.chart.removeEntity(entityId); } catch (e) { }
        }
    }

    clear_draw_chanlun(clear_type) {
        if (clear_type == "last") {
            for (const symbolKey in this.obj_charts) {
                for (const chartType in this.obj_charts[symbolKey]) {
                    if (this.obj_charts[symbolKey][chartType].length == 0) continue;
                    const maxTime = Math.max(...this.obj_charts[symbolKey][chartType].map((item) => item.time));
                    for (const _i in this.obj_charts[symbolKey][chartType]) {
                        const item = this.obj_charts[symbolKey][chartType][_i];
                        if (item.time == maxTime) {
                            this.safeRemove(item.id);
                        }
                    }
                    this.obj_charts[symbolKey][chartType] = this.obj_charts[symbolKey][chartType].filter((item) => item.time != maxTime);
                }
            }
        } else {
            Object.values(this.obj_charts).forEach((symbolData) => {
                Object.values(symbolData).forEach((chartItems) => {
                    chartItems.forEach((item) => {
                        this.safeRemove(item.id);
                    });
                });
            });
            this.obj_charts = {};
        }
    }

    getChartData() {
        const symbolInterval = this.widget.symbolInterval(); if (!symbolInterval) return null;
        const symbolResKey = `${symbolInterval.symbol.toString().toLowerCase()}${symbolInterval.interval.toString().toLowerCase()}`;
        const barsResult = this.udf_datafeed?._historyProvider?.bars_result?.get(symbolResKey);

        console.log(`[DEBUG-CHARTS] getChartData for ${symbolResKey}: Found=${!!barsResult}`);
        if (!barsResult) return null;

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

        // 1. Remove items not in new list or outside window
        for (let i = container.length - 1; i >= 0; i--) {
            const existing = container[i];
            if (!newKeys.has(existing.key) || existing.time < from) {
                this.safeRemove(existing.id);
                container.splice(i, 1);
            }
        }

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
        this.reconcile('bis', window.cl_show_config.bi ? barsResult.bis : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "bis"), linewidth: 1 }), 'bi'));
        this.reconcile('xds', window.cl_show_config.xd ? barsResult.xds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "xds"), linewidth: 2 }), 'xd'));
        this.reconcile('zsds', window.cl_show_config.zsd ? barsResult.zsds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createLineShape(this.chart, item, { color: getDynamicColor(currentInterval, "zsds"), linewidth: 3 }), 'zsd'));
        this.reconcile('bi_zss', window.cl_show_config.zs ? barsResult.bi_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: CHART_CONFIG.COLORS.BI_ZSS, linewidth: 1 }), 'bi_zs'));
        this.reconcile('xd_zss', window.cl_show_config.zs ? barsResult.xd_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: getDynamicColor(currentInterval, "xd_zss"), linewidth: 2 }), 'xd_zs'));
        this.reconcile('zsd_zss', window.cl_show_config.zs ? barsResult.zsd_zss : [], from, symbolKey, (item) => safeCreate(ChartUtils.createZhongshuShape(this.chart, item, { color: CHART_CONFIG.COLORS.ZSD_ZSS, linewidth: 2 }), 'zsd_zs'));
        this.reconcile('bcs', window.cl_show_config.bc ? barsResult.bcs : [], from, symbolKey, (item) => safeCreate(ChartUtils.createBcShape(this.chart, item), 'bc'), false);
        this.reconcile('mmds', window.cl_show_config.mmd ? barsResult.mmds : [], from, symbolKey, (item) => safeCreate(ChartUtils.createMmdShape(this.chart, item), 'mmd'), false);
    }

    draw_chanlun() {
        if (!this.chart) {
            try {
                this.chart = this.widget.activeChart();
            } catch (e) {
                console.warn("[DEBUG-CHARTS] draw_chanlun: activeChart not available");
                return;
            }
        }

        const chartData = this.getChartData();
        if (!chartData) {
            console.warn("[DEBUG-CHARTS] draw_chanlun aborted: No chart data or chart not ready.");
            return;
        }
        const symbolInterval = this.widget.symbolInterval();
        if (!symbolInterval) return;

        console.log("[DEBUG-CHARTS] draw_chanlun executing for", symbolInterval.interval);
        this.drawChartElements(chartData, symbolInterval.interval);
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