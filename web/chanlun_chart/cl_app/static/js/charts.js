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
  CHART_TYPES: [ "fxs", "bis", "xds", "zsds", "bi_zss", "xd_zss", "zsd_zss", "bcs", "mmds", "macd_areas" ],
};

const DEFAULT_COLORS = {
  bis: CHART_CONFIG.COLORS.BI, xds: CHART_CONFIG.COLORS.XD, zsds: CHART_CONFIG.COLORS.ZSD,
  bi_zss: CHART_CONFIG.COLORS.BI_ZSS, xd_zss: CHART_CONFIG.COLORS.XD_ZSS, zsd_zss: CHART_CONFIG.COLORS.ZSD_ZSS,
};

const DYNAMIC_CHART_COLORS = {
  "1": { ...DEFAULT_COLORS, bis: "#FFA500", xds: "#DAA520", xd_zss: "#ADD8E6" },
  "5": { ...DEFAULT_COLORS, bi_zss: "#ADD8E6", xds: "#ADD8E6", xd_zss: "#FF0000", zsds: "#FF0000" },
  "30": { ...DEFAULT_COLORS, xds: "#FF0000", xd_zss: "#008000", zsds: "#008000" },
  "1D": { ...DEFAULT_COLORS, xds: "#008000", xd_zss: "#00008B", zsds: "#00008B" },
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
        if(!chart) return Promise.reject("Chart object is null");

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
    this.udf_datafeed = new Datafeeds.UDFCompatibleDatafeed("/tv", 60000);

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
    this.widget.headerReady().then(function () {
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
        $.post({
          type: "POST", url: "/tv/del_marks", dataType: "json", data: { symbol: symbol.symbol },
          success: function (res) {
            if (res.status == "ok") { global_widget.activeChart().clearMarks(); layer.msg("删除标记成功"); }
            else { layer.msg("删除标记失败"); }
          },
        });
      });
    });
    this.widget.onChartReady(() => {
      this.chart = this.widget.activeChart();
      if (!this.chart) return;
      if (this.udf_datafeed) window.tvDatafeed = this.udf_datafeed;

      setTimeout(() => {
          const studies = this.chart.getAllStudies();
          const hasMacd = studies.some(s => s.name === 'macd_pro_area');
          if (!hasMacd) {
              this.chart.createStudy('macd_pro_area', false, false)
                  .then(id => {
                      this.macdStudyId = id;
                  })
                  .catch(e => { console.log("Create study failed (benign):", e); });
          } else {
              const existing = studies.find(s => s.name === 'macd_pro_area');
              if(existing) this.macdStudyId = existing.id;
          }
      }, 1000);

      this.chart.applyOverrides({
        "mainSeriesProperties.candleStyle.upColor": "#ef5350", "mainSeriesProperties.candleStyle.downColor": "#26a69a",
        "mainSeriesProperties.candleStyle.borderUpColor": "#ef5350", "mainSeriesProperties.candleStyle.borderDownColor": "#26a69a",
        "mainSeriesProperties.candleStyle.wickUpColor": "#ef5350", "mainSeriesProperties.candleStyle.wickDownColor": "#26a69a",
      });
      this.chart.onSymbolChanged().subscribe(null, (symbol) => this.handleSymbolChange(symbol));
      this.chart.onIntervalChanged().subscribe(null, (interval) => this.handleIntervalChange(interval));
      this.chart.onDataLoaded().subscribe(null, () => {
          this.clear_draw_chanlun();
          setTimeout(() => this.debouncedDrawChanlun(), 200);
      }, true);

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
  handleDataReady() { this.clear_draw_chanlun(); this.debouncedDrawChanlun(); }
  handleTick() { this.clear_draw_chanlun(); this.debouncedDrawChanlun(); }
  handleVisibleRangeChange() { this.debouncedDrawChanlun(); }

  safeRemove(entityId) {
      if (!entityId) return;
      if (typeof entityId.then === 'function') {
          entityId.then(id => {
              if (id) {
                  try { this.chart.removeEntity(id); } catch (e) {}
              }
          }).catch(e => {});
      } else {
          try { this.chart.removeEntity(entityId); } catch (e) {}
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
      const macdStudy = studies.find(s => s.name === 'macd_pro_area');
      if (macdStudy) { this.macdStudyId = macdStudy.id; return macdStudy.id; }
      return null;
  }

  drawChartElements(chartData, currentInterval) {
    const { symbolKey, barsResult, from } = chartData;

    const bisCount = barsResult.bis ? barsResult.bis.length : 0;
    console.log(`[DEBUG-CHARTS] drawChartElements: symbol=${symbolKey}, from=${from}, Bis Count=${bisCount}`);

    if (!barsResult) return;

    const chartContainer = this.initChartContainer(symbolKey);

    const safeCreate = (promise, type) => {
        if (promise && typeof promise.then === 'function') {
            return promise.catch(e => {
                console.error(`[DEBUG-CHARTS] Error creating shape (${type}):`, e);
                return null;
            });
        }
        return promise;
    };

    let stats = { bis: 0, xds: 0, zsds: 0, skipped_bis: 0 };

    if (barsResult.fxs) { barsResult.fxs.forEach((fx) => { if (fx.points?.[0]?.time >= from) { const key = JSON.stringify(fx); if (!chartContainer.fxs.find(item => item.key === key)) chartContainer.fxs.push({ time: fx.points[0].time, key, id: safeCreate(ChartUtils.createFxShape(this.chart, fx), 'fx') }); } }); }

    if (barsResult.bis) {
        barsResult.bis.forEach((bi) => {
            if (bi.points?.[0]?.time >= from) {
                const key = JSON.stringify(bi);
                if (!chartContainer.bis.find(item => item.key === key)) {
                    chartContainer.bis.push({ time: bi.points[0].time, key, id: safeCreate(ChartUtils.createLineShape(this.chart, bi, { color: getDynamicColor(currentInterval, "bis"), linewidth: 1 }), 'bi') });
                    stats.bis++;
                }
            } else {
                stats.skipped_bis++;
            }
        });
    }

    if (barsResult.xds) { barsResult.xds.forEach((xd) => { if (xd.points?.[0]?.time >= from) { const key = JSON.stringify(xd); if (!chartContainer.xds.find(item => item.key === key)) { chartContainer.xds.push({ time: xd.points[0].time, key, id: safeCreate(ChartUtils.createLineShape(this.chart, xd, { color: getDynamicColor(currentInterval, "xds"), linewidth: 2 }), 'xd') }); stats.xds++; } } }); }
    if (barsResult.zsds) { barsResult.zsds.forEach((zsd) => { if (zsd.points?.[0]?.time >= from) { const key = JSON.stringify(zsd); if (!chartContainer.zsds.find(item => item.key === key)) { chartContainer.zsds.push({ time: zsd.points[0].time, key, id: safeCreate(ChartUtils.createLineShape(this.chart, zsd, { color: getDynamicColor(currentInterval, "zsds"), linewidth: 3 }), 'zsd') }); stats.zsds++; } } }); }
    if (barsResult.bi_zss) { barsResult.bi_zss.forEach((bi_zs) => { if (bi_zs.points?.[0]?.time >= from) { const key = JSON.stringify(bi_zs); if (!chartContainer.bi_zss.find(item => item.key === key)) chartContainer.bi_zss.push({ time: bi_zs.points[0].time, key, id: safeCreate(ChartUtils.createZhongshuShape(this.chart, bi_zs, { color: CHART_CONFIG.COLORS.BI_ZSS, linewidth: 1 }), 'bi_zs') }); } }); }
    if (barsResult.xd_zss) { barsResult.xd_zss.forEach((xd_zs) => { if (xd_zs.points?.[0]?.time >= from) { const key = JSON.stringify(xd_zs); if (!chartContainer.xd_zss.find(item => item.key === key)) chartContainer.xd_zss.push({ time: xd_zs.points[0].time, key, id: safeCreate(ChartUtils.createZhongshuShape(this.chart, xd_zs, { color: getDynamicColor(currentInterval, "xd_zss"), linewidth: 2 }), 'xd_zs') }); } }); }
    if (barsResult.zsd_zss) { barsResult.zsd_zss.forEach((zsd_zs) => { if (zsd_zs.points?.[0]?.time >= from) { const key = JSON.stringify(zsd_zs); if (!chartContainer.zsd_zss.find(item => item.key === key)) chartContainer.zsd_zss.push({ time: zsd_zs.points[0].time, key, id: safeCreate(ChartUtils.createZhongshuShape(this.chart, zsd_zs, { color: CHART_CONFIG.COLORS.ZSD_ZSS, linewidth: 2 }), 'zsd_zs') }); } }); }
    if (barsResult.bcs) { barsResult.bcs.forEach((bc) => { if (bc.points?.time >= from) { const key = JSON.stringify(bc); if (!chartContainer.bcs.find(item => item.key === key)) chartContainer.bcs.push({ time: bc.points.time, key, id: safeCreate(ChartUtils.createBcShape(this.chart, bc), 'bc') }); } }); }
    if (barsResult.mmds) { barsResult.mmds.forEach((mmd) => { if (mmd.points?.time >= from) { const key = JSON.stringify(mmd); if (!chartContainer.mmds.find(item => item.key === key)) chartContainer.mmds.push({ time: mmd.points.time, key, id: safeCreate(ChartUtils.createMmdShape(this.chart, mmd), 'mmd') }); } }); }

    console.log(`[DEBUG-CHARTS] Draw Stats: Created Bis=${stats.bis}, Skipped Bis=${stats.skipped_bis}, Created Xds=${stats.xds}`);

    if (barsResult.macd_hist && barsResult.times) {
        const macdId = this.getMACDStudyId();
        if (macdId) {
            const hist = barsResult.macd_hist;
            const areas = barsResult.macd_area || [];
            const times = barsResult.times;
            const line1 = barsResult.macd_dif || barsResult.dif || [];
            const line2 = barsResult.macd_dea || barsResult.dea || [];
            const hasLines = line1.length > 0 && line2.length > 0;

            const len = Math.min(hist.length, times.length);
            const visibleRange = this.chart.getVisibleRange();
            const chartVisibleFrom = visibleRange ? visibleRange.from : 0;
            const isChartSeconds = chartVisibleFrom < 10000000000;

            let startIndex = 0;

            while(startIndex < len) {
                let val = hist[startIndex];
                if (val === 0 || isNaN(val)) { startIndex++; continue; }
                const isPos = val > 0;
                let endIndex = startIndex;

                let maxAbs = -1;
                let maxIdx = -1;
                let segmentHigh = -Infinity;
                let segmentLow = Infinity;

                while(endIndex < len) {
                    const v = hist[endIndex];

                    // 修复逻辑：忽略 NaN 和 0，保持段落连续性
                    // 只有当数值有效(非0非NaN) 且 符号反转时，才断开段落
                    if (v !== 0 && !isNaN(v)) {
                        if (v > 0 !== isPos) break; // 符号反转，断开
                    }
                    // 注意：如果 v 是 0 或 NaN，循环继续执行，将其包含在当前段内（或直接跳过计算）
                    // 这样 "红-0-红" 会被视为一个完整段落，而不是断开

                    if (!isNaN(v)) {
                        if (Math.abs(v) >= maxAbs) { maxAbs = Math.abs(v); maxIdx = endIndex; }
                        if (hasLines) {
                            const l1 = line1[endIndex] || 0;
                            const l2 = line2[endIndex] || 0;
                            const h = v;
                            const currentMax = Math.max(l1, l2, h);
                            const currentMin = Math.min(l1, l2, h);
                            if (currentMax > segmentHigh) segmentHigh = currentMax;
                            if (currentMin < segmentLow) segmentLow = currentMin;
                        } else {
                            if (v > segmentHigh) segmentHigh = v;
                            if (v < segmentLow) segmentLow = v;
                        }
                    }
                    endIndex++;
                }

                if (maxIdx !== -1) {
                    let peakTime = times[maxIdx];
                    if (isChartSeconds && peakTime > 10000000000) peakTime /= 1000;

                    if (peakTime >= chartVisibleFrom) {
                        let areaVal = 0;
                        if (areas.length > maxIdx) areaVal = areas[maxIdx];

                        const text = areaVal.toFixed(2);
                        const color = isPos ? CHART_CONFIG.COLORS.AREA_POS : CHART_CONFIG.COLORS.AREA_NEG;
                        const key = `macd_area_${peakTime}`;
                        let basePrice = isPos ? segmentHigh : segmentLow;
                        if (basePrice === -Infinity || basePrice === Infinity) basePrice = hist[maxIdx];
                        const range = segmentHigh - segmentLow;
                        let padding = range * 0.15;
                        if (padding === 0 || isNaN(padding)) padding = Math.abs(hist[maxIdx]) * 0.2;
                        let offsetPrice = isPos ? basePrice + padding : basePrice - padding;

                        const isActiveSegment = (endIndex >= len - 1);
                        const existingIdx = chartContainer.macd_areas.findIndex(item => item.key === key);

                        if (isActiveSegment) {
                            let boundaryTimeRaw = -Infinity;

                            // 【关键修改】
                            // 原逻辑：boundaryTimeRaw = times[startIndex - 1];
                            // 新逻辑：向前回溯 8 根 K 线作为“禁区”。
                            // 含义：只要是最近 8 根 K 线内产生的旧标记，不管是否属于严格意义上的“当前段”，统统视为“抖动残影”并清除，只保留最新的这一个。
                            // 这能完美解决日线/周线因微小波动导致的段落断裂问题。
                            const LOOKBACK_BARS = 8;
                            let safeIndex = startIndex - LOOKBACK_BARS;
                            if (safeIndex < 0) safeIndex = 0;

                            if (times.length > safeIndex) {
                                boundaryTimeRaw = times[safeIndex];
                            }

                            // 调试日志（确认回溯生效）
                            // console.log(`[MACD-FIX] 活跃段Start: ${startIndex}, 回溯至: ${safeIndex}, 边界时间: ${new Date(boundaryTimeRaw).toLocaleString()}`);

                            for (let k = chartContainer.macd_areas.length - 1; k >= 0; k--) {
                                const oldItem = chartContainer.macd_areas[k];

                                // 获取旧标记时间 (优先 rawTime)
                                let oldItemTimeRaw = oldItem.rawTime;
                                if (!oldItemTimeRaw) {
                                    oldItemTimeRaw = oldItem.time > 10000000000 ? oldItem.time : oldItem.time * 1000;
                                }

                                // 只要旧标记的时间晚于这个“放宽了的边界”，就删掉
                                if (oldItemTimeRaw > boundaryTimeRaw) {
                                    this.safeRemove(oldItem.id);
                                    chartContainer.macd_areas.splice(k, 1);
                                }
                            }
                        } else {
                            // 历史段逻辑（保持不变）
                            const existingIdx = chartContainer.macd_areas.findIndex(item => item.key === key);
                            if (existingIdx !== -1) {
                                const oldItem = chartContainer.macd_areas[existingIdx];
                                this.safeRemove(oldItem.id);
                                chartContainer.macd_areas.splice(existingIdx, 1);
                            }
                        }

                        if (!chartContainer.macd_areas.find(item => item.key === key)) {
                            // DEBUG: 打印新增标记的动作
                            if (isActiveSegment) {
                                console.log(`[MACD-DEBUG] %c[新增标记] Time: ${times[maxIdx]} (${new Date(times[maxIdx]).toLocaleString()})`, "color: green");
                            }

                            chartContainer.macd_areas.push({
                                time: peakTime,
                                rawTime: times[maxIdx], // 务必确保这里保存了 times[maxIdx]
                                key: key,
                                id: safeCreate(this.chart.createShape({time: peakTime, price: offsetPrice}, {
                                    shape: 'text', text: text, ownerStudyId: macdId, lock: true, disableSelection: true,
                                    overrides: { color: color, fontsize: 11, linewidth: 0, transparency: 0, bold: true }
                                }), 'macd_area')
                            });
                        }
                    }
                }
                startIndex = endIndex;
            }
        }
    }
  }

  draw_chanlun() {
    if (!this.chart) {
        try {
            this.chart = this.widget.activeChart();
        } catch(e) {
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