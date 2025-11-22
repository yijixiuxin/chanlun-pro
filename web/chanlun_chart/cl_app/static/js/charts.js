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
  "1": { ...DEFAULT_COLORS, bis: "#DF8344", xds: "#FFFF55", xd_zss: "#4FADEA", zsds: "#4FADEA", bi_zss: "#FFFF55" },
  "5": { ...DEFAULT_COLORS, bis: "#ffff55", xds: "#4FADEA", xd_zss: "#EA3323", zsds: "#EA3323", bi_zss: "#4FADEA" },
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
        } catch(e) {}
        return null;
    };

    this.widget.headerReady().then(function () {
      // ... 原有按钮保持不变 ...
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

      // --- 终极扫描版：MACD 统计按钮 ---
      var buttonCalcArea = global_widget.createButton();
      buttonCalcArea.textContent = "创建统计框";
      buttonCalcArea.style.color = "#1E90FF";
      buttonCalcArea.style.fontWeight = "bold";

      buttonCalcArea.addEventListener("click", function () {
        try {
            const chart = global_widget.activeChart();
            let targetId = self.measureShapeId;
            let targetObj = null;

            // 1. 确定目标 ID
            if (!targetId) {
                // 尝试搜索
                try {
                    const allShapes = chart.getAllShapes();
                    for (let i = allShapes.length - 1; i >= 0; i--) {
                        const id = allShapes[i].id;
                        // 简单粗暴：通过 getShapeById 拿对象，看有没有我们的标签
                        try {
                            const s = chart.getShapeById(id);
                            if (s && (s.text === "MACD_MEASURE_TAG_v1" || (s.options && s.options.text === "MACD_MEASURE_TAG_v1"))) {
                                targetId = id;
                                targetObj = s;
                                break;
                            }
                        } catch(e){}
                    }
                } catch(e){}
            }

            // 2. 如果有 ID，尝试获取数据
            if (targetId) {
                console.log(`[MACD] 锁定目标 ID: ${targetId}`);

                // 策略 A: getShapeState (新版API)
                if (!targetObj && typeof chart.getShapeState === 'function') {
                    try { targetObj = chart.getShapeState(targetId); } catch(e){}
                }
                // 策略 B: getShapeById (旧版API)
                if (!targetObj) {
                    try { targetObj = chart.getShapeById(targetId); } catch(e){}
                }

                if (targetObj) {
                    console.log("[MACD] 获取到对象:", targetObj);

                    // === 核心：提取 Points ===
                    let points = null;

                    // 1. 直接访问
                    if (targetObj.points) points = targetObj.points;
                    else if (targetObj.data && targetObj.data.points) points = targetObj.data.points;
                    else if (targetObj.json && targetObj.json.points) points = targetObj.json.points;
                    else if (targetObj._points) points = targetObj._points;

                    // 2. 深度扫描 (如果上面没找到)
                    if (!points || !Array.isArray(points)) {
                        console.log("[MACD] 常规路径未找到坐标，启动深度扫描...");
                        points = scanForPoints(targetObj);
                    }

                    if (points && Array.isArray(points) && points.length >= 2) {
                         // >>> 执行计算
                         const t1 = points[0].time || points[0];
                         const t2 = points[1].time || points[1];
                         console.log(`[MACD] 提取到坐标: ${t1} - ${t2}`);

                         const result = self.calculateMACDArea(t1, t2);
                         if (result) {
                            const msg = `📊 MACD 统计\n多头: ${result.sumUp}\n空头: ${result.sumDown}\n净值: ${result.netArea}\nK线: ${result.count}`;
                            alert(msg);
                            self.measureShapeId = targetId; // 记住这个有效的ID
                            return; // 成功结束
                         }
                    } else {
                        console.warn("[MACD] ❌ 对象中完全找不到 points 数据。打印 Keys 以便排查:", Object.keys(targetObj));
                    }
                }
            }

            // ===========================
            // 如果没找到或没数据 -> 创建新框
            // ===========================
            console.log("[MACD] 创建新框...");
            const range = chart.getVisibleRange();
            const t1 = range.from + (range.to - range.from) * 0.35;
            const t2 = range.from + (range.to - range.from) * 0.65;

            // 价格计算
            let pTop=100, pBottom=0;
            const d = self.getChartData();
            if(d?.barsResult?.bars){
                 const b = d.barsResult.bars.slice(-100);
                 let max=-Infinity; b.forEach(x=>{if(x.high>max)max=x.high});
                 if(max>-Infinity) { pTop=max*1.1; pBottom=max*0.9; }
            }

            const cfg = {
                shape: "rectangle", lock: false, disableSelection: false,
                text: "MACD_MEASURE_TAG_v1",
                overrides: { color: "#2962FF", backgroundColor: "#2962FF", transparency: 85, linewidth: 2 }
            };

            const res = chart.createMultipointShape([{time:t1, price:pTop}, {time:t2, price:pBottom}], cfg);

            Promise.resolve(res).then(id => {
                if(id) {
                    self.measureShapeId = id;
                    buttonCalcArea.textContent = "📊 点击计算";
                    buttonCalcArea.style.color = "#ff6d00";
                    if(typeof layer !== 'undefined') layer.msg("框已生成，请拖动后再次点击");
                    else alert("框已生成，请拖动后再次点击");
                }
            });

        } catch(e) {
            console.error("[MACD] Error:", e);
        }
      });
    });
    this.widget.onChartReady(() => {
      this.chart = this.widget.activeChart();
      if (!this.chart) return;
      setTimeout(() => {
          const studies = this.chart.getAllStudies();
          if (!studies.some(s => s.name === 'MACD')) { this.chart.createStudy('MACD', false, false).catch(()=>{}); }
      }, 1000);
      this.chart.applyOverrides({ "mainSeriesProperties.candleStyle.upColor": "#ef5350", "mainSeriesProperties.candleStyle.downColor": "#26a69a" });
      this.chart.onSymbolChanged().subscribe(null, (s) => this.handleSymbolChange(s));
      this.chart.onIntervalChanged().subscribe(null, (i) => this.handleIntervalChange(i));
      this.chart.onDataLoaded().subscribe(null, () => { this.clear_draw_chanlun(); setTimeout(() => this.debouncedDrawChanlun(), 200); }, true);
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

  // --- 新增功能：计算指定时间区间的 MACD 红绿柱面积 ---
  calculateMACDArea(startTime, endTime) {
    // 1. 获取数据
    const chartData = this.getChartData();
    if (!chartData || !chartData.barsResult) {
        console.warn("无法获取图表数据，计算中止");
        return null;
    }

    const { times, macd_hist } = chartData.barsResult;
    if (!times || !macd_hist || times.length !== macd_hist.length) {
        console.warn("MACD数据不完整");
        return null;
    }

    // 2. 确保时间戳单位一致（假设 times 是毫秒，传入的参数可能是秒）
    // TradingView 绘图返回的通常是秒级时间戳，而 bundle.js 中 times 存的是毫秒
    const t1 = startTime * 1000;
    const t2 = endTime * 1000;
    const start = Math.min(t1, t2);
    const end = Math.max(t1, t2);

    let sumUp = 0.0;   // 红柱总和（正值）
    let sumDown = 0.0; // 绿柱总和（负值）
    let count = 0;

    // 3. 遍历并累加
    for (let i = 0; i < times.length; i++) {
        const t = times[i];
        if (t >= start && t <= end) {
            const val = macd_hist[i];
            // 排除无效值 NaN
            if (val !== undefined && val !== null && !isNaN(val)) {
                if (val > 0) sumUp += val;
                else sumDown += val;
                count++;
            }
        }
    }

    // 4. 格式化结果
    const result = {
        start: new Date(start).toLocaleString(),
        end: new Date(end).toLocaleString(),
        count: count,
        sumUp: parseFloat(sumUp.toFixed(4)),     // 多头力度
        sumDown: parseFloat(sumDown.toFixed(4)), // 空头力度
        netArea: parseFloat((sumUp + sumDown).toFixed(4)) // 净力度
    };

    console.log("[MACD统计]", result);
    return result;
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
    // [修复] 即使没有数据也要继续，以便执行清理逻辑(虽然此处保留原逻辑判断)
    if (!barsResult) return;

    const bisCount = barsResult.bis ? barsResult.bis.length : 0;
    // console.log(`[DEBUG-CHARTS] drawChartElements: symbol=${symbolKey}, from=${from}, Bis Count=${bisCount}`);

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

    // --- [新增] 核心修复逻辑开始 ---

    // 1. 清理画布上旧的“未完成”元素
    const removeOldUnfinished = (containerList) => {
        if (!containerList || containerList.length === 0) return;
        // 倒序遍历以安全删除
        for (let i = containerList.length - 1; i >= 0; i--) {
            if (containerList[i].isUnfinished) {
                this.safeRemove(containerList[i].id);
                containerList.splice(i, 1);
            }
        }
    };

    // 2. 清理数据源：分离已完成和未完成，未完成的只保留最后一个(最新的)
    const getUniqueRenderList = (sourceList) => {
        if (!sourceList || !Array.isArray(sourceList)) return [];
        const finished = [];
        const unfinished = [];

        sourceList.forEach(item => {
            // 兼容字符串和数字类型的 linestyle
            if (item.linestyle == '1' || item.linestyle == 1) {
                unfinished.push(item);
            } else {
                finished.push(item);
            }
        });

        // 关键：如果有多个未完成的，只取最后一个（最新的状态）
        if (unfinished.length > 0) {
            finished.push(unfinished[unfinished.length - 1]);
        }
        return finished;
    };
    // --- [新增] 核心修复逻辑结束 ---

    let stats = { bis: 0, xds: 0, zsds: 0, skipped_bis: 0 };

    // 分型 (FX) 通常是点，不涉及 linestyle 动态延伸问题，保持原样
    if (barsResult.fxs) { barsResult.fxs.forEach((fx) => { if (fx.points?.[0]?.time >= from) { const key = JSON.stringify(fx); if (!chartContainer.fxs.find(item => item.key === key)) chartContainer.fxs.push({ time: fx.points[0].time, key, id: safeCreate(ChartUtils.createFxShape(this.chart, fx), 'fx') }); } }); }

    // --- 修复 笔 (Bis) ---
    if (barsResult.bis) {
        removeOldUnfinished(chartContainer.bis); // 步骤1: 删旧
        const renderList = getUniqueRenderList(barsResult.bis); // 步骤2: 筛新

        renderList.forEach((bi) => {
            if (bi.points?.[0]?.time >= from) {
                const key = JSON.stringify(bi);
                // 只有当它不存在，或者它是未完成状态(因为未完成状态每次都要重画最新的)时才处理
                // 但由于上面已经删除了旧的未完成，这里只要判断 key 不存在即可
                if (!chartContainer.bis.find(item => item.key === key)) {
                    const isUnfinished = (bi.linestyle == '1' || bi.linestyle == 1);
                    chartContainer.bis.push({
                        time: bi.points[0].time,
                        key,
                        isUnfinished: isUnfinished, // [新增标记]
                        id: safeCreate(ChartUtils.createLineShape(this.chart, bi, { color: getDynamicColor(currentInterval, "bis"), linewidth: 1 }), 'bi')
                    });
                    stats.bis++;
                }
            } else {
                stats.skipped_bis++;
            }
        });
    }

    // --- 修复 线段 (Xds) ---
    if (barsResult.xds) {
        removeOldUnfinished(chartContainer.xds);
        const renderList = getUniqueRenderList(barsResult.xds);
        renderList.forEach((xd) => {
            if (xd.points?.[0]?.time >= from) {
                const key = JSON.stringify(xd);
                if (!chartContainer.xds.find(item => item.key === key)) {
                    const isUnfinished = (xd.linestyle == '1' || xd.linestyle == 1);
                    chartContainer.xds.push({
                        time: xd.points[0].time,
                        key,
                        isUnfinished: isUnfinished,
                        id: safeCreate(ChartUtils.createLineShape(this.chart, xd, { color: getDynamicColor(currentInterval, "xds"), linewidth: 2 }), 'xd')
                    });
                    stats.xds++;
                }
            }
        });
    }

    // --- 修复 走势段 (Zsds) ---
    if (barsResult.zsds) {
        removeOldUnfinished(chartContainer.zsds);
        const renderList = getUniqueRenderList(barsResult.zsds);
        renderList.forEach((zsd) => {
            if (zsd.points?.[0]?.time >= from) {
                const key = JSON.stringify(zsd);
                if (!chartContainer.zsds.find(item => item.key === key)) {
                    const isUnfinished = (zsd.linestyle == '1' || zsd.linestyle == 1);
                    chartContainer.zsds.push({
                        time: zsd.points[0].time,
                        key,
                        isUnfinished: isUnfinished,
                        id: safeCreate(ChartUtils.createLineShape(this.chart, zsd, { color: getDynamicColor(currentInterval, "zsds"), linewidth: 3 }), 'zsd')
                    });
                    stats.zsds++;
                }
            }
        });
    }

    // --- 修复 笔中枢 (Bi_Zss) ---
    if (barsResult.bi_zss) {
        removeOldUnfinished(chartContainer.bi_zss);
        const renderList = getUniqueRenderList(barsResult.bi_zss);
        renderList.forEach((bi_zs) => {
            if (bi_zs.points?.[0]?.time >= from) {
                const key = JSON.stringify(bi_zs);
                if (!chartContainer.bi_zss.find(item => item.key === key)) {
                    const isUnfinished = (bi_zs.linestyle == '1' || bi_zs.linestyle == 1);
                    chartContainer.bi_zss.push({
                        time: bi_zs.points[0].time,
                        key,
                        isUnfinished: isUnfinished,
                        id: safeCreate(ChartUtils.createZhongshuShape(this.chart, bi_zs, { color: CHART_CONFIG.COLORS.BI_ZSS, linewidth: 1 }), 'bi_zs')
                    });
                }
            }
        });
    }

    // --- 修复 线段中枢 (Xd_Zss) ---
    if (barsResult.xd_zss) {
        removeOldUnfinished(chartContainer.xd_zss);
        const renderList = getUniqueRenderList(barsResult.xd_zss);
        renderList.forEach((xd_zs) => {
            if (xd_zs.points?.[0]?.time >= from) {
                const key = JSON.stringify(xd_zs);
                if (!chartContainer.xd_zss.find(item => item.key === key)) {
                    const isUnfinished = (xd_zs.linestyle == '1' || xd_zs.linestyle == 1);
                    chartContainer.xd_zss.push({
                        time: xd_zs.points[0].time,
                        key,
                        isUnfinished: isUnfinished,
                        id: safeCreate(ChartUtils.createZhongshuShape(this.chart, xd_zs, { color: getDynamicColor(currentInterval, "xd_zss"), linewidth: 2 }), 'xd_zs')
                    });
                }
            }
        });
    }

    // --- 修复 走势段中枢 (Zsd_Zss) ---
    if (barsResult.zsd_zss) {
        removeOldUnfinished(chartContainer.zsd_zss);
        const renderList = getUniqueRenderList(barsResult.zsd_zss);
        renderList.forEach((zsd_zs) => {
            if (zsd_zs.points?.[0]?.time >= from) {
                const key = JSON.stringify(zsd_zs);
                if (!chartContainer.zsd_zss.find(item => item.key === key)) {
                    const isUnfinished = (zsd_zs.linestyle == '1' || zsd_zs.linestyle == 1);
                    chartContainer.zsd_zss.push({
                        time: zsd_zs.points[0].time,
                        key,
                        isUnfinished: isUnfinished,
                        id: safeCreate(ChartUtils.createZhongshuShape(this.chart, zsd_zs, { color: CHART_CONFIG.COLORS.ZSD_ZSS, linewidth: 2 }), 'zsd_zs')
                    });
                }
            }
        });
    }

    if (barsResult.bcs) { barsResult.bcs.forEach((bc) => { if (bc.points?.time >= from) { const key = JSON.stringify(bc); if (!chartContainer.bcs.find(item => item.key === key)) chartContainer.bcs.push({ time: bc.points.time, key, id: safeCreate(ChartUtils.createBcShape(this.chart, bc), 'bc') }); } }); }
    if (barsResult.mmds) { barsResult.mmds.forEach((mmd) => { if (mmd.points?.time >= from) { const key = JSON.stringify(mmd); if (!chartContainer.mmds.find(item => item.key === key)) chartContainer.mmds.push({ time: mmd.points.time, key, id: safeCreate(ChartUtils.createMmdShape(this.chart, mmd), 'mmd') }); } }); }

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
                    if (v !== 0 && !isNaN(v)) {
                        if (v > 0 !== isPos) break;
                    }
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

                        if (isActiveSegment) {
                            let boundaryTimeRaw = -Infinity;
                            const LOOKBACK_BARS = 8;
                            let safeIndex = startIndex - LOOKBACK_BARS;
                            if (safeIndex < 0) safeIndex = 0;

                            if (times.length > safeIndex) {
                                boundaryTimeRaw = times[safeIndex];
                            }

                            for (let k = chartContainer.macd_areas.length - 1; k >= 0; k--) {
                                const oldItem = chartContainer.macd_areas[k];
                                let oldItemTimeRaw = oldItem.rawTime;
                                if (!oldItemTimeRaw) {
                                    oldItemTimeRaw = oldItem.time > 10000000000 ? oldItem.time : oldItem.time * 1000;
                                }
                                if (oldItemTimeRaw > boundaryTimeRaw) {
                                    this.safeRemove(oldItem.id);
                                    chartContainer.macd_areas.splice(k, 1);
                                }
                            }
                        } else {
                            const existingIdx = chartContainer.macd_areas.findIndex(item => item.key === key);
                            if (existingIdx !== -1) {
                                const oldItem = chartContainer.macd_areas[existingIdx];
                                this.safeRemove(oldItem.id);
                                chartContainer.macd_areas.splice(existingIdx, 1);
                            }
                        }

                        if (!chartContainer.macd_areas.find(item => item.key === key)) {
                            chartContainer.macd_areas.push({
                                time: peakTime,
                                rawTime: times[maxIdx],
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