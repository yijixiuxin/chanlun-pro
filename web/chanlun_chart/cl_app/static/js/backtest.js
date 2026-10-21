// === 常量 ===
const BT_CONFIG = {
  INITIAL_CAPITAL: 100000,
  LS_KEY_PREFIX: "bt_trades_",
  DRAW_DEBOUNCE_MS: 500,
  COLORS: {
    DING: "#FA8072",
    DI: "#1E90FF",
    BI: "#708090",
    XD: "#00BFFF",
    ZSD: "#FFA710",
    BI_ZSS: "#708090",
    XD_ZSS: "#00BFFF",
    ZSD_ZSS: "#FFA710",
    BCS: "#D1D4DC",
    BC_TEXT: "#fccbcd",
    MMD_UP: "#FA8072",
    MMD_DOWN: "#1E90FF",
    TRADE_BUY: "#F23645",
    TRADE_SELL: "#089981",
  },
  CHART_TYPES: ["fxs", "bis", "xds", "zsds", "bi_zss", "xd_zss", "zsd_zss", "bcs", "mmds"],
};

// === 工具函数 ===
function btDebounce(func, wait) {
  let timeout;
  return function (...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

// === localStorage 工具 ===
const BTLocalStore = {
  loadTrades(sessionKey) {
    const raw = localStorage.getItem(BT_CONFIG.LS_KEY_PREFIX + sessionKey);
    try { return raw ? JSON.parse(raw) : []; } catch (e) { return []; }
  },
  saveTrades(sessionKey, records) {
    localStorage.setItem(BT_CONFIG.LS_KEY_PREFIX + sessionKey, JSON.stringify(records));
  },
  removeTrades(sessionKey) {
    localStorage.removeItem(BT_CONFIG.LS_KEY_PREFIX + sessionKey);
  },
};

// === ChartUtils ===
const BTChartUtils = {
  createShape(chart, points, options = {}) {
    const defaults = {
      lock: true, disableSelection: true, disableSave: true,
      disableUndo: true, showInObjectsTree: false, overrides: {},
    };
    const config = { ...defaults, ...options };
    return config.shape === "trend_line" || config.shape === "rectangle" || config.shape === "circle"
      ? chart.createMultipointShape(points, config)
      : chart.createShape(points, config);
  },

  createFxShape(chart, fx) {
    const color = fx.text === "ding" ? BT_CONFIG.COLORS.DING : BT_CONFIG.COLORS.DI;
    return this.createShape(chart, fx.points, {
      shape: "circle",
      overrides: { backgroundColor: color, color: color, linewidth: 4 },
    });
  },

  createLineShape(chart, line, color, linewidth) {
    return this.createShape(chart, line.points, {
      shape: "trend_line",
      overrides: {
        linestyle: parseInt(line.linestyle) || 0,
        linewidth: linewidth || 1,
        linecolor: color,
      },
    });
  },

  createZhongshuShape(chart, zs, color, linewidth) {
    return this.createShape(chart, zs.points, {
      shape: "rectangle",
      overrides: {
        linestyle: parseInt(zs.linestyle) || 0,
        linewidth: linewidth || 1,
        linecolor: color,
        backgroundColor: color, transparency: 95,
        fillBackground: true, filled: true,
      },
    });
  },

  createMmdShape(chart, mmd) {
    const isBuy = mmd.text.includes("B");
    const color = isBuy ? BT_CONFIG.COLORS.MMD_UP : BT_CONFIG.COLORS.MMD_DOWN;
    const shape = isBuy ? "arrow_up" : "arrow_down";
    return this.createShape(chart, mmd.points, {
      shape, text: mmd.text,
      overrides: {
        markerColor: color, backgroundColor: color,
        color: color, fontsize: 12, transparency: 80,
      },
    });
  },

  createBcShape(chart, bc) {
    return this.createShape(chart, bc.points, {
      shape: "balloon", text: bc.text,
      overrides: {
        markerColor: BT_CONFIG.COLORS.BCS,
        backgroundColor: BT_CONFIG.COLORS.BCS,
        textColor: BT_CONFIG.COLORS.BC_TEXT,
        transparency: 70, fontsize: 12,
      },
    });
  },

  // 买卖点使用画图标记（arrow_up / arrow_down），不再使用 datafeed Marks
  createTradeShape(chart, point, trade) {
    return this.createShape(chart, point, {
      shape: trade.shape, text: trade.text,
      overrides: {
        arrowColor: trade.color,
        color: trade.color,
        fontsize: 12,
        showLabel: true,
      },
    });
  },
};

// === DataFeed API ===
const backtestConfigData = {
  supports_search: false,
  supports_group_request: false,
  supported_resolutions: ["1D", "1W"],
  supports_timescale_marks: true,
  supports_time: false,
  exchanges: [{ value: "backtest", name: "回测", desc: "回测学习" }],
  symbols_types: [{ name: "stock", value: "stock" }],
};

const symbolInfoSmall = {
  name: "缠论学习",
  ticker: "999999",
  full_name: "缠论:999999",
  description: "缠论回测学习",
  exchange: "backtest",
  type: "stock",
  session: "24x7",
  timezone: "Asia/Shanghai",
  minmov: 1,
  minmov2: 0,
  pricescale: 100,
  has_intraday: false,
  has_seconds: false,
  has_daily: true,
  daily_multipliers: ["1"],
  has_weekly_and_monthly: false,
  supported_resolutions: ["1D"],
  visible_plots_set: "ohlcv",
};

const symbolInfoHigh = {
  name: "缠论学习",
  ticker: "999999",
  full_name: "缠论:999999",
  description: "缠论回测学习",
  exchange: "backtest",
  type: "stock",
  session: "24x7",
  timezone: "Asia/Shanghai",
  minmov: 1,
  minmov2: 0,
  pricescale: 100,
  has_intraday: false,
  has_seconds: false,
  has_daily: true,
  daily_multipliers: ["1"],
  has_weekly_and_monthly: true,
  supported_resolutions: ["1W"],
  visible_plots_set: "ohlcv",
};

function createBacktestDatafeed(symbolName) {
  const symbolInfo = symbolName === "small" ? symbolInfoSmall : symbolInfoHigh;

  return {
    _realtimeCallback: null,
    _bars: {},

    onReady: function (callback) {
      setTimeout(() => callback(backtestConfigData), 0);
    },

    searchSymbols: function (userInput, exchange, symbolType, onResultReadyCallback) {
      onResultReadyCallback([]);
    },

    resolveSymbol: function (symbolName, onSymbolResolvedCallback, onResolveErrorCallback) {
      onSymbolResolvedCallback(symbolInfo);
    },

    getBars: function (symbolInfo, resolution, periodParams, onHistoryCallback, onErrorCallback) {
      const { from, to, firstDataRequest } = periodParams;
      const self = this;

      // 非首次请求不调接口，后续数据由 /backtest/step 通过 pushBar 推送
      if (!firstDataRequest) {
        onHistoryCallback([], { noData: true });
        return;
      }
      if (from < 0 || to < 0) {
        onHistoryCallback([], { noData: true });
        return;
      }

      $.getJSON("/backtest/tv/history", {
        symbol: symbolName,
        resolution: resolution,
      }, function (data) {
        if (data.s === "ok") {
          const bars = [];
          for (let i = 0; i < data.t.length; i++) {
            bars.push({
              time: data.t[i] * 1000,
              close: data.c[i],
              open: data.o[i],
              high: data.h[i],
              low: data.l[i],
              volume: data.v[i],
            });
          }
          self._bars = data;
          onHistoryCallback(bars, { noData: bars.length === 0 });
        } else {
          onHistoryCallback([], { noData: true });
        }
      }).fail(function () { onHistoryCallback([], { noData: true }); });
    },

    subscribeBars: function (symbolInfo, resolution, onRealtimeCallback, subscriberUID) {
      this._realtimeCallback = onRealtimeCallback;
    },

    unsubscribeBars: function (subscriberUID) {
      this._realtimeCallback = null;
    },

    pushBar: function (bar) {
      if (this._realtimeCallback) {
        this._realtimeCallback({
          time: bar.time * 1000,
          close: bar.close, open: bar.open,
          high: bar.high, low: bar.low, volume: bar.volume,
        });
      }
    },
  };
}

// === BacktestApp ===
const BacktestApp = {
  widgetSmall: null, widgetHigh: null,
  chartSmall: null, chartHigh: null,
  datafeedSmall: null, datafeedHigh: null,
  saveLoadAdapters: null,
  timerId: null, speedMs: 2000, currentBarTime: 0,
  sessionLoaded: false, sessionKey: null, startPos: 0,
  running: false, paused: false,

  // 画图状态 — 按图表分别跟踪已画的元素（参照 charts.js 的 obj_charts 模式）
  objCharts: { small: null, high: null },
  debouncedDraw: { small: null, high: null },

  // 交易状态
  capital: BT_CONFIG.INITIAL_CAPITAL,
  position: { type: null, qty: 0, price: 0 },
  tradeRecords: [],
  resultSubmitted: false,

  init() {
    this.bindEvents();

    const self = this;
    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      self.sessionKey = res.session_key;
      self.sessionLoaded = true;
      self.startPos = res.start_pos || 0;
      self.totalBars = res.total_bars || 0;
      self.currentBarTime = res.current_bar_time || 0;

      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text("日线 / 周线");
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-progress").text("0%");

      self.resetTradingState();
      self.tradeRecords = BTLocalStore.loadTrades(self.sessionKey);
      self.renderTradeRecordsFromData();

      self.objCharts = { small: null, high: null };
      self.createCharts();
    });
  },

  createCharts() {
    this.datafeedSmall = createBacktestDatafeed("small");
    this.datafeedHigh = createBacktestDatafeed("high");

    // 回测学习图表布局/指标设置只保存在浏览器本地，不走 /tv/storage 服务端接口。
    // 两张图周期固定不同（日线/周线），用 namespace 隔离各自的布局列表。
    this.saveLoadAdapters = {
      small: BacktestSaveLoad.createBacktestLocalSaveLoadAdapter({ namespace: "small" }),
      high: BacktestSaveLoad.createBacktestLocalSaveLoadAdapter({ namespace: "high" }),
    };

    this.widgetSmall = new TradingView.widget({
      debug: false, autosize: true, fullscreen: false,
      container: "tv_chart_small", symbol: "small", interval: "1D",
      datafeed: this.datafeedSmall,
      library_path: "static/charting_library/",
      theme: "Dark", timezone: "Asia/Shanghai", locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay", 'header_screenshot', 'use_localstorage_for_settings', 'save_chart_properties_to_local_storage'],
      // study_templates：允许保存指标模板；saveload_storage_customization：本版本库必须开启才会使用 save_load_adapter
      enabled_features: ["study_templates", "saveload_storage_customization"], time_frames: [],
      save_load_adapter: this.saveLoadAdapters.small,
      load_last_chart: true,
      custom_indicators_getter: function (PineJS) {
        return Promise.resolve([
          TvIdxAMA.idx(PineJS), TvIdxATR.idx(PineJS), TvIdxCDBB.idx(PineJS),
          TvIdxCMCM.idx(PineJS), TvIdxDemo.idx(PineJS), TvIdxFCX.idx(PineJS),
          TvIdxHDLY.idx(PineJS), TvIdxHeima.idx(PineJS), TvIdxHLBLW.idx(PineJS),
          TvIdxHLFTX.idx(PineJS), TvIdxKDJ.idx(PineJS), TvIdxPinbar.idx(PineJS),
          TvIdxLTQS.idx(PineJS), TvIdxMA.idx(PineJS), TvIdxMACDBL.idx(PineJS),
          TvIdxVegasMA.idx(PineJS), TvIdxVOL.idx(PineJS), TvIdxRSX.idx(PineJS),
        ]);
      },
    });

    this.widgetHigh = new TradingView.widget({
      debug: false, autosize: true, fullscreen: false,
      container: "tv_chart_high", symbol: "high", interval: "1W",
      datafeed: this.datafeedHigh,
      library_path: "static/charting_library/",
      theme: "Dark", timezone: "Asia/Shanghai", locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay", 'header_screenshot', 'use_localstorage_for_settings', 'save_chart_properties_to_local_storage'],
      enabled_features: ["study_templates", "saveload_storage_customization"], time_frames: [],
      save_load_adapter: this.saveLoadAdapters.high,
      load_last_chart: true,
      custom_indicators_getter: function (PineJS) {
        return Promise.resolve([
          TvIdxAMA.idx(PineJS), TvIdxATR.idx(PineJS), TvIdxCDBB.idx(PineJS),
          TvIdxCMCM.idx(PineJS), TvIdxDemo.idx(PineJS), TvIdxFCX.idx(PineJS),
          TvIdxHDLY.idx(PineJS), TvIdxHeima.idx(PineJS), TvIdxHLBLW.idx(PineJS),
          TvIdxHLFTX.idx(PineJS), TvIdxKDJ.idx(PineJS), TvIdxPinbar.idx(PineJS),
          TvIdxLTQS.idx(PineJS), TvIdxMA.idx(PineJS), TvIdxMACDBL.idx(PineJS),
          TvIdxVegasMA.idx(PineJS), TvIdxVOL.idx(PineJS), TvIdxRSX.idx(PineJS),
        ]);
      },
    });

    const self = this;
    this.widgetSmall.onChartReady(() => {
      self.chartSmall = self.widgetSmall.activeChart();
      self.debouncedDraw.small = btDebounce(() => self.drawChanlun("small"), BT_CONFIG.DRAW_DEBOUNCE_MS);
      self.chartSmall.onDataLoaded().subscribe(null, () => self.debouncedDraw.small());
      self.chartSmall.onVisibleRangeChanged().subscribe(null, () => self.debouncedDraw.small());
    });
    this.widgetHigh.onChartReady(() => {
      self.chartHigh = self.widgetHigh.activeChart();
      self.debouncedDraw.high = btDebounce(() => self.drawChanlun("high"), BT_CONFIG.DRAW_DEBOUNCE_MS);
      self.chartHigh.onDataLoaded().subscribe(null, () => self.debouncedDraw.high());
      self.chartHigh.onVisibleRangeChanged().subscribe(null, () => self.debouncedDraw.high());
    });
  },

  // === 数据加载 ===
  reloadSession() {
    const self = this;
    if (this.widgetSmall) { this.widgetSmall.remove(); this.widgetSmall = null; this.chartSmall = null; }
    if (this.widgetHigh) { this.widgetHigh.remove(); this.widgetHigh = null; this.chartHigh = null; }
    $("#tv_chart_small, #tv_chart_high").empty();

    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      self.sessionKey = res.session_key;
      self.sessionLoaded = true;
      self.startPos = res.start_pos || 0;
      self.totalBars = res.total_bars || 0;
      self.currentBarTime = res.current_bar_time || 0;

      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text("日线 / 周线");
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-progress").text("0%");

      self.resetTradingState();
      self.tradeRecords = BTLocalStore.loadTrades(self.sessionKey);
      self.renderTradeRecordsFromData();

      self.objCharts = { small: null, high: null };
      self.createCharts();
    });
  },

  resetTradingState() {
    this.capital = BT_CONFIG.INITIAL_CAPITAL;
    this.position = { type: null, qty: 0, price: 0 };
    this.tradeRecords = [];
    this.resultSubmitted = false;
    this.updateCapitalDisplay();
    this.updatePositionDisplay();
    $("#bt-trade-records tbody").empty();
  },

  renderTradeRecordsFromData() {
    $("#bt-trade-records tbody").empty();
    this.tradeRecords.forEach(rec => {
      const pnlStr = rec.pnl !== undefined ? "¥" + rec.pnl.toFixed(2) : "--";
      const row = `<tr>
        <td>${rec.time}</td>
        <td>${rec.direction}</td>
        <td>¥${rec.price.toFixed(2)}</td>
        <td>${rec.qty}</td>
        <td>${pnlStr}</td>
      </tr>`;
      $("#bt-trade-records tbody").prepend(row);
    });
  },

  // === 回放控制 ===
  startReplay() {
    if (!this.sessionLoaded) { layer.msg("数据加载中..."); return; }
    if (this.running) return;

    this.running = true;
    this.paused = false;
    $("#bt-btn-start").addClass("layui-btn-disabled").attr("disabled", true);
    $("#bt-btn-pause").text("暂停");
    this.startTimer();
  },

  startTimer() {
    this.stopTimer();
    const self = this;
    this.timerId = setInterval(() => self.stepForward(), this.speedMs);
  },

  stepForward() {
    const self = this;
    $.post("/backtest/step", function (res) {
      if (res.finished) {
        self.stopTimer();
        self.running = false;
        $("#bt-progress").text("100%");
        layer.msg("回放已结束");
        self.submitResult();
        return;
      }
      if (!res.ok) return;

      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      const range = self.totalBars - self.startPos || 1;
      const progress = (((res.current_pos - self.startPos) / range) * 100).toFixed(1);
      $("#bt-progress").text(progress + "%");

      if (res.new_bar) { self.datafeedSmall.pushBar(res.new_bar); }
      if (res.new_high_bar) { self.datafeedHigh.pushBar(res.new_high_bar); }

      self.currentBarTime = res.current_bar_time;
      $("#bt-current-time").text(res.current_time);
      console.log("current time: ", res.current_time, "current bar time: ", res.current_bar_time);


      if (res.cl_small) {
        self.datafeedSmall._bars = res.cl_small;
        self.debouncedDraw.small();
      }
      if (res.cl_high) {
        self.datafeedHigh._bars = res.cl_high;
        self.debouncedDraw.high();
      }

      self.updateCapitalDisplay();
    });
  },

  togglePause() {
    if (!this.running) return;
    if (this.paused) {
      this.startTimer();
      this.paused = false;
      $("#bt-btn-pause").text("暂停");
    } else {
      this.stopTimer();
      this.paused = true;
      $("#bt-btn-pause").text("继续");
    }
  },

  stopTimer() {
    if (this.timerId) { clearInterval(this.timerId); this.timerId = null; }
  },

  restartTimer() {
    this.stopTimer();
    const self = this;
    this.timerId = setInterval(() => self.stepForward(), this.speedMs);
  },

  restartSession() {
    this.stopTimer();
    this.running = false;
    this.paused = false;

    if (this.sessionKey) { BTLocalStore.removeTrades(this.sessionKey); }

    const doStop = () => {
      $.post("/backtest/stop", () => {
        this.sessionLoaded = false;
        this.sessionKey = null;
        $("#bt-btn-start").removeClass("layui-btn-disabled").attr("disabled", false);
        $("#bt-btn-pause").text("暂停");
        $("#bt-progress").text("--");
        this.reloadSession();
      });
    };

    // 未跑到结束就重新开始时，先把当前进度保存为一次回测记录
    if (this.tradeRecords.length > 0 && !this.resultSubmitted) {
      this.submitResult(false, doStop);
    } else {
      doStop();
    }
  },

  // === 回测结果上报与记录弹框 ===
  submitResult(showPopup = true, onDone = null) {
    const finish = () => { if (onDone) onDone(); };
    if (!this.sessionKey || this.resultSubmitted) { finish(); return; }
    this.resultSubmitted = true;

    const priceText = $("#bt-current-price").text().replace("¥", "");
    const payload = {
      session_key: this.sessionKey,
      initial_capital: BT_CONFIG.INITIAL_CAPITAL,
      capital: this.capital,
      position: this.position,
      current_price: parseFloat(priceText) || 0,
      current_time: $("#bt-current-time").text(),
      trade_records: this.tradeRecords,
    };

    const self = this;
    const loading = layer.load(1);
    $.ajax({
      type: "POST",
      url: "/backtest/finish",
      contentType: "application/json",
      dataType: "json",
      data: JSON.stringify(payload),
      success: function (res) {
        layer.close(loading);
        if (!res.ok) {
          if (showPopup) layer.msg(res.msg || "回测记录保存失败");
        } else {
          if (showPopup) {
            layer.msg("回测完成，记录已保存");
            self.openRecordsPopup();
          }
        }
        finish();
      },
      error: function () {
        layer.close(loading);
        if (showPopup) layer.msg("回测记录保存失败，请检查服务是否正常");
        finish();
      },
    });
  },

  openRecordsPopup() {
    layer.open({
      type: 2,
      title: "回测学习记录",
      area: ["1180px", "86vh"],
      content: "/backtest/records/page",
      anim: 1,
      fixed: true,
      shadeClose: true,
    });
  },

  // === 缠论绘制（参照 charts.js 的 draw_chanlun / drawChartElements 模式） ===

  // 初始化图表容器
  initChartContainer(chartKey) {
    if (!this.objCharts[chartKey]) {
      this.objCharts[chartKey] = {};
      BT_CONFIG.CHART_TYPES.forEach(type => { this.objCharts[chartKey][type] = []; });
    }
    return this.objCharts[chartKey];
  },

  // 清除已画的图形：clearType='last' 先按 zs_type 聚合，再各自删除 time 最大的元素，否则全删
  clearDrawChanlun(chartKey, clearType) {
    const chart = chartKey === "small" ? this.chartSmall : this.chartHigh;
    if (!chart || !this.objCharts[chartKey]) return;

    const container = this.objCharts[chartKey];

    // 获取元素的 zs_type（只有中枢对象带该字段，其他图形返回空字符串）
    const getZsType = item => {
      try {
        return JSON.parse(item.key)?.zs_type || "";
      } catch (e) {
        return "";
      }
    };

    if (clearType === "last") {
      BT_CONFIG.CHART_TYPES.forEach(chartType => {
        const items = container[chartType] || [];
        if (items.length === 0) return;

        // 按 zs_type 聚合，避免多种类型的中枢混在一起排序
        const groups = {};
        items.forEach(item => {
          const zsType = getZsType(item);
          if (!groups[zsType]) groups[zsType] = [];
          groups[zsType].push(item);
        });

        // 每组分别删除 time 最大的元素
        const removed = new Set();
        Object.values(groups).forEach(groupItems => {
          const maxTime = Math.max(...groupItems.map(item => item.time));
          groupItems.forEach(item => {
            if (item.time === maxTime) {
              item.id.then(_id => chart.removeEntity(_id));
              removed.add(item);
            }
          });
        });

        // 原地过滤保留非最新元素
        const kept = [];
        for (let i = 0; i < items.length; i++) {
          if (!removed.has(items[i])) kept.push(items[i]);
        }
        container[chartType] = kept;
      });
    } else {
      BT_CONFIG.CHART_TYPES.forEach(chartType => {
        const items = container[chartType] || [];
        items.forEach(item => {
          try {
            item.id.then(_id => {
              try { chart.removeEntity(_id); } catch (e) { /* ignore */ }
            });
          } catch (e) { /* ignore */ }
        });
        container[chartType] = [];
      });
    }
  },

  // 绘制图表元素
  drawChartElements(chartKey) {
    const datafeed = chartKey === "small" ? this.datafeedSmall : this.datafeedHigh;
    const chart = chartKey === "small" ? this.chartSmall : this.chartHigh;
    const bars = datafeed._bars;
    if (!bars || !chart) return;

    // 获取可见范围（单位与 cl_data_to_tv_chart 的 points.time 一致，都是秒级时间戳）
    const visibleRange = chart.getVisibleRange();
    const from = (visibleRange && visibleRange.from) ? visibleRange.from : 0;
    
    const container = this.initChartContainer(chartKey);

    // 先清除最新一次画的元素（避免更新时出现重复的末段）
    this.clearDrawChanlun(chartKey, "last");

    // 分型
    (bars.fxs || []).forEach(fx => {
      if (fx.points && fx.points[0] && fx.points[0].time >= from) {
        const key = JSON.stringify(fx);
        if (container.fxs.find(item => item.key === key)) return;
        container.fxs.push({
          time: fx.points[0].time,
          key,
          id: BTChartUtils.createFxShape(chart, fx),
        });
      }
    });

    // 笔
    (bars.bis || []).forEach(bi => {
      if (bi.points && bi.points[0] && bi.points[0].time >= from) {
        const key = JSON.stringify(bi);
        if (container.bis.find(item => item.key === key)) return;
        container.bis.push({
          time: bi.points[0].time,
          key,
          id: BTChartUtils.createLineShape(chart, bi, BT_CONFIG.COLORS.BI, 1),
        });
      }
    });

    // 线段
    (bars.xds || []).forEach(xd => {
      if (xd.points && xd.points[0] && xd.points[0].time >= from) {
        const key = JSON.stringify(xd);
        if (container.xds.find(item => item.key === key)) return;
        container.xds.push({
          time: xd.points[0].time,
          key,
          id: BTChartUtils.createLineShape(chart, xd, BT_CONFIG.COLORS.XD, 2),
        });
      }
    });

    // 走势段
    (bars.zsds || []).forEach(zsd => {
      if (zsd.points && zsd.points[0] && zsd.points[0].time >= from) {
        const key = JSON.stringify(zsd);
        if (container.zsds.find(item => item.key === key)) return;
        container.zsds.push({
          time: zsd.points[0].time,
          key,
          id: BTChartUtils.createLineShape(chart, zsd, BT_CONFIG.COLORS.ZSD, 3),
        });
      }
    });

    // 笔中枢
    (bars.bi_zss || []).forEach(zs => {
      if (zs.points && zs.points[0] && zs.points[0].time >= from) {
        const key = JSON.stringify(zs);
        if (container.bi_zss.find(item => item.key === key)) return;
        container.bi_zss.push({
          time: zs.points[0].time,
          key,
          id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.BI_ZSS, 1),
        });
      }
    });

    // 线段中枢
    (bars.xd_zss || []).forEach(zs => {
      if (zs.points && zs.points[0] && zs.points[0].time >= from) {
        const key = JSON.stringify(zs);
        if (container.xd_zss.find(item => item.key === key)) return;
        container.xd_zss.push({
          time: zs.points[0].time,
          key,
          id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.XD_ZSS, 2),
        });
      }
    });

    // 走势段中枢
    (bars.zsd_zss || []).forEach(zs => {
      if (zs.points && zs.points[0] && zs.points[0].time >= from) {
        const key = JSON.stringify(zs);
        if (container.zsd_zss.find(item => item.key === key)) return;
        container.zsd_zss.push({
          time: zs.points[0].time,
          key,
          id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.ZSD_ZSS, 2),
        });
      }
    });

    // 背驰
    (bars.bcs || []).forEach(bc => {
      if (bc.points && bc.points.time >= from) {
        const key = JSON.stringify(bc);
        if (container.bcs.find(item => item.key === key)) return;
        container.bcs.push({
          time: bc.points.time,
          key,
          id: BTChartUtils.createBcShape(chart, bc),
        });
      }
    });

    // 买卖点
    (bars.mmds || []).forEach(mmd => {
      if (mmd.points && mmd.points.time >= from) {
        const key = JSON.stringify(mmd);
        if (container.mmds.find(item => item.key === key)) return;
        container.mmds.push({
          time: mmd.points.time,
          key,
          id: BTChartUtils.createMmdShape(chart, mmd),
        });
      }
    });
  },

  // 绘制缠论图表
  drawChanlun(chartKey) {
    this.drawChartElements(chartKey);
  },

  // === 事件绑定 ===
  bindEvents() {
    const self = this;

    $("#bt-btn-start").click(() => self.startReplay());
    $("#bt-btn-pause").click(() => self.togglePause());
    $("#bt-btn-stop").click(() => self.restartSession());
    $("#bt-btn-buy").click(() => self.trade("buy"));
    $("#bt-btn-sell").click(() => self.trade("sell"));
    $("#bt-btn-close").click(() => self.trade("close"));
    $("#bt-btn-records").click(() => self.openRecordsPopup());

    $("#bt-speed-slider").on("input", function () {
      const val = parseInt($(this).val());
      self.speedMs = val * 500;
      $("#bt-speed-label").text((self.speedMs / 1000).toFixed(1) + "s");
      // 回放运行中立即生效：重启定时器
      if (self.running && !self.paused) self.restartTimer();
    });

    // 仓位百分比快捷输入：Math.floor(资金 * 百分比 / 价格 / 100) * 100
    $(".bt-pct-btn").click(function () {
      const pct = parseFloat($(this).data("pct"));
      const priceText = $("#bt-current-price").text().replace("¥", "");
      const price = parseFloat(priceText);
      if (isNaN(price) || !self.sessionLoaded) return;
      const qty = Math.max(100, Math.floor(self.capital * pct / price / 100) * 100);
      $("#bt-trade-qty").val(qty);
    });
  },

  // === 交易逻辑 ===
  trade(action) {
    if (!this.sessionLoaded) { layer.msg("请等待数据加载完成"); return; }

    const priceText = $("#bt-current-price").text().replace("¥", "");
    const price = parseFloat(priceText);
    if (isNaN(price)) return;

    const qty = parseInt($("#bt-trade-qty").val()) || 0;
    if (qty <= 0) { layer.msg("请输入有效数量"); return; }

    if (action === "buy") {
      if (this.position.type === "short") { layer.msg("请先平掉空仓"); return; }
      const cost = price * qty;
      if (cost > this.capital) { layer.msg("资金不足"); return; }
      this.capital -= cost;
      if (this.position.type === "long") {
        const totalQty = this.position.qty + qty;
        this.position.price = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
      } else {
        this.position = { type: "long", qty: qty, price: price };
      }
      this.addTradeRecord("买入", price, qty);
    } else if (action === "sell") {
      if (this.position.type === "long") { layer.msg("请先平掉多仓"); return; }
      const cost = price * qty;
      if (cost > this.capital) { layer.msg("资金不足"); return; }
      this.capital -= cost;
      if (this.position.type === "short") {
        const totalQty = this.position.qty + qty;
        this.position.price = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
      } else {
        this.position = { type: "short", qty: qty, price: price };
      }
      this.addTradeRecord("卖出", price, qty);
    } else if (action === "close") {
      if (!this.position.type) { layer.msg("无持仓可平"); return; }
      const closedType = this.position.type;
      let pnl = 0;
      if (this.position.type === "long") {
        pnl = (price - this.position.price) * this.position.qty;
      } else if (this.position.type === "short") {
        pnl = (this.position.price - price) * this.position.qty;
      }
      this.capital += this.position.price * this.position.qty + pnl;
      this.addTradeRecord("平仓", price, this.position.qty, pnl, closedType);
      this.position = { type: null, qty: 0, price: 0 };
    }

    this.updateCapitalDisplay();
    this.updatePositionDisplay();
  },

  addTradeRecord(direction, price, qty, pnl, positionType) {
    const time = $("#bt-current-time").text();
    const rec = { time, direction, price, qty, pnl: pnl || 0 };
    this.tradeRecords.push(rec);

    if (this.sessionKey) { BTLocalStore.saveTrades(this.sessionKey, this.tradeRecords); }

    const row = `<tr>
      <td>${time}</td>
      <td>${direction}</td>
      <td>¥${price.toFixed(2)}</td>
      <td>${qty}</td>
      <td>${pnl !== undefined ? "¥" + pnl.toFixed(2) : "--"}</td>
    </tr>`;
    $("#bt-trade-records tbody").prepend(row);

    // 使用图表画图标记买卖点
    this.addTradeShape(direction, price, qty, pnl, positionType);
  },

  // 在图表上画箭头标记买卖点（替代 datafeed Marks）
  addTradeShape(direction, price, qty, pnl, positionType) {
    const barTime = this.currentBarTime;
    if (!barTime) return;

    let isBuy, color, text;
    if (direction === "买入") {
      isBuy = true;
      color = BT_CONFIG.COLORS.TRADE_BUY;
      text = `买入 ${qty}股 @¥${price.toFixed(2)}`;
    } else if (direction === "卖出") {
      isBuy = false;
      color = BT_CONFIG.COLORS.TRADE_SELL;
      text = `卖出 ${qty}股 @¥${price.toFixed(2)}`;
    } else if (direction === "平仓") {
      // 平多 = 卖出（箭头向下），平空 = 买入（箭头向上）
      isBuy = positionType === "short";
      color = pnl >= 0 ? BT_CONFIG.COLORS.TRADE_BUY : BT_CONFIG.COLORS.TRADE_SELL;
      const pnlStr = (pnl >= 0 ? "+¥" : "-¥") + Math.abs(pnl).toFixed(2);
      text = `平仓 ${qty}股 @¥${price.toFixed(2)} ${pnlStr}`;
    } else {
      return;
    }

    // 买点箭头贴 bar 低点，卖点箭头贴 bar 高点（时间会自动对齐到当前周期的 bar）
    const point = { time: barTime, channel: isBuy ? "low" : "high" };
    const trade = { shape: isBuy ? "arrow_up" : "arrow_down", color, text };

    [this.chartSmall, this.chartHigh].forEach(chart => {
      if (!chart) return;
      BTChartUtils.createTradeShape(chart, point, trade)
        .catch(() => { /* 图表未就绪时忽略，不影响交易 */ });
    });
  },

  updateCapitalDisplay() {
    const priceText = $("#bt-current-price").text().replace("¥", "");
    const price = parseFloat(priceText);

    // 持仓市值
    let positionValue = 0;
    if (this.position.type === "long" && !isNaN(price)) {
      positionValue = this.position.qty * price;
    } else if (this.position.type === "short" && !isNaN(price)) {
      positionValue = -this.position.qty * price;
    }

    // 浮动盈亏（未实现）：仅当前持仓的盈亏
    let unrealizedPnl = 0;
    if (this.position.type === "long" && !isNaN(price)) {
      unrealizedPnl = (price - this.position.price) * this.position.qty;
    } else if (this.position.type === "short" && !isNaN(price)) {
      unrealizedPnl = (this.position.price - price) * this.position.qty;
    }
    const unrealizedPnlPct = ((unrealizedPnl / BT_CONFIG.INITIAL_CAPITAL) * 100).toFixed(2);

    // 总市值 = 现金 + 持仓市值（含已平仓的已实现盈亏在 capital 中）
    const totalValue = this.capital + positionValue;

    $("#bt-current-capital").text("¥" + this.capital.toFixed(2));
    $("#bt-position-value").text("¥" + positionValue.toFixed(2));
    // 浮动盈亏显示金额 + 比例
    const pnlEl = $("#bt-pnl");
    const sign = unrealizedPnl >= 0 ? "+" : "";
    pnlEl.text(`${sign}¥${unrealizedPnl.toFixed(2)}  (${sign}${unrealizedPnlPct}%)`);
    pnlEl.removeClass("positive negative");
    pnlEl.addClass(unrealizedPnl >= 0 ? "positive" : "negative");
  },

  updatePositionDisplay() {
    if (!this.position.type || this.position.qty === 0) {
      $("#bt-position-display").text("无持仓");
    } else {
      const typeLabel = this.position.type === "long" ? "做多" : "做空";
      $("#bt-position-display").text(
        `${typeLabel} ${this.position.qty}股 @¥${this.position.price.toFixed(2)}`
      );
    }
  },
};

// === 页面启动 ===
$(function () {
  layui.use("layer", function () {
    window.layer = layui.layer;
    BacktestApp.init();
  });
});
