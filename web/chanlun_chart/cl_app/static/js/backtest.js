// === 常量 ===
const BT_CONFIG = {
  INITIAL_CAPITAL: 100000,
  LS_KEY_PREFIX: "bt_trades_",
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
  },
};

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

// === ChartUtils（简化版，仅绘制，无交互） ===
const BTChartUtils = {
  createShape(chart, points, options = {}) {
    const defaults = {
      lock: true, disableSelection: true, disableSave: true,
      disableUndo: true, showInObjectsTree: false, overrides: {},
    };
    const config = { ...defaults, ...options };
    return config.shape === "trend_line" || config.shape === "rectangle"
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

  createLineShape(chart, line, color) {
    return this.createShape(chart, line.points, {
      shape: "trend_line",
      overrides: {
        linestyle: parseInt(line.linestyle) || 0,
        linewidth: 1, linecolor: color,
      },
    });
  },

  createZhongshuShape(chart, zs, color) {
    return this.createShape(chart, zs.points, {
      shape: "rectangle",
      overrides: {
        linestyle: parseInt(zs.linestyle) || 0,
        linewidth: 1, linecolor: color,
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
};

// === DataFeed API ===
// 配置数据（DatafeedConfiguration）
const backtestConfigData = {
  supports_search: false,
  supports_group_request: false,
  supported_resolutions: ["30", "1D"],
  supports_marks: false,
  supports_timescale_marks: false,
  supports_time: false,
  exchanges: [{ value: "backtest", name: "回测", desc: "回测学习" }],
  symbols_types: [{ name: "stock", value: "stock" }],
};

// 小级别 symbol 信息
const symbolInfoSmall = {
  name: "small",
  ticker: "small",
  description: "small",
  exchange: "backtest",
  listed_exchange: "backtest",
  type: "stock",
  session: "24x7",
  timezone: "Asia/Shanghai",
  minmov: 1,
  pricescale: 100,
  has_intraday: true,
  intraday_multipliers: ["30"],
  has_daily: true,
  daily_multipliers: ["1"],
  has_weekly_and_monthly: true,
  supported_resolutions: ["30"],
  visible_plots_set: "ohlcv",
  data_status: "streaming",
};

// 大级别 symbol 信息
const symbolInfoHigh = {
  name: "high",
  ticker: "high",
  description: "high",
  exchange: "backtest",
  listed_exchange: "backtest",
  type: "stock",
  session: "24x7",
  timezone: "Asia/Shanghai",
  minmov: 1,
  pricescale: 100,
  has_intraday: true,
  intraday_multipliers: ["30"],
  has_daily: true,
  daily_multipliers: ["1"],
  has_weekly_and_monthly: true,
  supported_resolutions: ["1D"],
  visible_plots_set: "ohlcv",
  data_status: "streaming",
};

// 创建 DataFeed 对象
function createBacktestDatafeed(symbolName) {
  const symbolInfo = symbolName === "small" ? symbolInfoSmall : symbolInfoHigh;

  return {
    _realtimeCallback: null,
    _bars: {},

    // DatafeedConfiguration
    onReady: function (callback) {
      setTimeout(() => callback(backtestConfigData), 0);
    },

    // 不支持搜索
    searchSymbols: function (userInput, exchange, symbolType, onResultReadyCallback) {
      onResultReadyCallback([]);
    },

    // 解析 symbol
    resolveSymbol: function (symbolName, onSymbolResolvedCallback, onResolveErrorCallback) {
      onSymbolResolvedCallback(symbolInfo);
    },

    // 获取历史 K 线
    getBars: function (symbolInfo, resolution, periodParams, onHistoryCallback, onErrorCallback) {
      const { from, to, firstDataRequest } = periodParams;
      const self = this;
      $.getJSON("/backtest/tv/history", {
        symbol: symbolName,
        resolution: resolution,
        from: from,
        to: to,
        firstDataRequest: firstDataRequest ? "true" : "false",
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
      }).fail(function () {
        onHistoryCallback([], { noData: true });
      });
    },

    // 订阅实时更新
    subscribeBars: function (symbolInfo, resolution, onRealtimeCallback, subscriberUID) {
      this._realtimeCallback = onRealtimeCallback;
    },

    // 取消订阅
    unsubscribeBars: function (subscriberUID) {
      this._realtimeCallback = null;
    },

    // 供回放使用：推送新 bar
    pushBar: function (bar) {
      if (this._realtimeCallback) {
        this._realtimeCallback({
          time: bar.time * 1000,
          close: bar.close,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          volume: bar.volume,
        });
      }
    },
  };
}

// === BacktestApp ===
const BacktestApp = {
  widgetSmall: null,
  widgetHigh: null,
  chartSmall: null,
  chartHigh: null,
  datafeedSmall: null,
  datafeedHigh: null,
  timerId: null,
  speedMs: 2000,
  sessionLoaded: false,
  sessionKey: null,
  running: false,
  paused: false,

  // 交易状态
  capital: BT_CONFIG.INITIAL_CAPITAL,
  position: { type: null, qty: 0, price: 0 },
  tradeRecords: [],
  drawnShapeIds: { small: [], high: [] },

  init() {
    this.bindEvents();

    // 先加载回放 session，再创建图表（避免图表在 session 就绪前请求数据）
    const self = this;
    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      self.sessionKey = res.session_key;
      self.sessionLoaded = true;

      // 更新 UI
      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text("日线 / 30m");
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-progress").text("0%");

      // 重置交易状态
      self.resetTradingState();

      // 从 localStorage 加载交易记录
      self.tradeRecords = BTLocalStore.loadTrades(self.sessionKey);
      self.renderTradeRecordsFromData();

      // session 就绪后创建图表
      self.createCharts();
    });
  },

  createCharts() {
    this.datafeedSmall = createBacktestDatafeed("small");
    this.datafeedHigh = createBacktestDatafeed("high");

    this.widgetSmall = new TradingView.widget({
      debug: false, autosize: true, fullscreen: false,
      container: "tv_chart_small", symbol: "small", interval: "30",
      datafeed: this.datafeedSmall,
      library_path: "static/charting_library/",
      theme: "Light", timezone: "Asia/Shanghai", locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay"],
      enabled_features: [], time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1", client_id: "bt_small",
    });

    this.widgetHigh = new TradingView.widget({
      debug: false, autosize: true, fullscreen: false,
      container: "tv_chart_high", symbol: "high", interval: "1D",
      datafeed: this.datafeedHigh,
      library_path: "static/charting_library/",
      theme: "Light", timezone: "Asia/Shanghai", locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay",
        "header_widget_dom_node", "header_interval_dialog_button"],
      enabled_features: [], time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1", client_id: "bt_high",
    });

    const self = this;
    this.widgetSmall.onChartReady(() => {
      self.chartSmall = self.widgetSmall.activeChart();
      self.chartSmall.onDataLoaded().subscribe(null, () => self.redrawShapes("small"));
    });
    this.widgetHigh.onChartReady(() => {
      self.chartHigh = self.widgetHigh.activeChart();
      self.chartHigh.onDataLoaded().subscribe(null, () => self.redrawShapes("high"));
    });
  },

  // === 数据加载（重新随机选股时调用） ===
  reloadSession() {
    const self = this;

    // 销毁旧图表
    if (this.widgetSmall) { this.widgetSmall.remove(); this.widgetSmall = null; this.chartSmall = null; }
    if (this.widgetHigh) { this.widgetHigh.remove(); this.widgetHigh = null; this.chartHigh = null; }
    $("#tv_chart_small, #tv_chart_high").empty();

    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      self.sessionKey = res.session_key;
      self.sessionLoaded = true;

      // 更新 UI
      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text("日线 / 30m");
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-progress").text("0%");

      // 重置交易状态
      self.resetTradingState();

      // 从 localStorage 加载交易记录
      self.tradeRecords = BTLocalStore.loadTrades(self.sessionKey);
      self.renderTradeRecordsFromData();

      // 重新创建图表
      self.createCharts();
    });
  },

  resetTradingState() {
    this.capital = BT_CONFIG.INITIAL_CAPITAL;
    this.position = { type: null, qty: 0, price: 0 };
    this.tradeRecords = [];
    this.updateCapitalDisplay();
    this.updatePositionDisplay();
    $("#bt-trade-records tbody").empty();
  },

  renderTradeRecordsFromData() {
    $("#bt-trade-records tbody").empty();
    this.tradeRecords.forEach(rec => {
      const row = `<tr>
        <td>${rec.time}</td>
        <td>${rec.direction}</td>
        <td>¥${rec.price.toFixed(2)}</td>
        <td>${rec.qty}</td>
        <td>${rec.pnl !== undefined ? "¥" + rec.pnl.toFixed(2) : "--"}</td>
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
    const self = this;
    this.stopTimer();
    this.timerId = setInterval(() => self.stepForward(), this.speedMs);
  },

  stepForward() {
    const self = this;
    $.post("/backtest/step", function (res) {
      if (res.finished) {
        self.stopTimer();
        self.running = false;
        layer.msg("回放已结束");
        return;
      }
      if (!res.ok) return;

      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      const progress = ((res.current_pos / res.total_bars) * 100).toFixed(1);
      $("#bt-progress").text(progress + "%");

      if (res.new_bar) { self.datafeedSmall.pushBar(res.new_bar); }
      if (res.new_high_bar) { self.datafeedHigh.pushBar(res.new_high_bar); }

      if (res.cl_small) {
        self.datafeedSmall._bars = res.cl_small;
        self.redrawShapes("small");
      }
      if (res.cl_high) {
        self.datafeedHigh._bars = res.cl_high;
        self.redrawShapes("high");
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
    if (this.timerId) {
      clearInterval(this.timerId);
      this.timerId = null;
    }
  },

  restartSession() {
    this.stopTimer();
    this.running = false;
    this.paused = false;

    if (this.sessionKey) {
      BTLocalStore.removeTrades(this.sessionKey);
    }

    $.post("/backtest/stop", () => {
      this.sessionLoaded = false;
      this.sessionKey = null;
      $("#bt-btn-start").removeClass("layui-btn-disabled").attr("disabled", false);
      $("#bt-btn-pause").text("暂停");
      $("#bt-progress").text("--");
      this.reloadSession();
    });
  },

  // === 缠论绘制 ===
  redrawShapes(chartKey) {
    const datafeed = chartKey === "small" ? this.datafeedSmall : this.datafeedHigh;
    const chart = chartKey === "small" ? this.chartSmall : this.chartHigh;
    const bars = datafeed._bars;
    if (!bars || !chart) return;

    this.drawnShapeIds[chartKey].forEach(id => {
      try { id.then(i => chart.removeEntity(i)); } catch (e) {}
    });
    this.drawnShapeIds[chartKey] = [];

    const ids = this.drawnShapeIds[chartKey];

    (bars.fxs || []).forEach(fx => {
      ids.push({ id: BTChartUtils.createFxShape(chart, fx) });
    });
    (bars.bis || []).forEach(bi => {
      ids.push({ id: BTChartUtils.createLineShape(chart, bi, BT_CONFIG.COLORS.BI) });
    });
    (bars.xds || []).forEach(xd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, xd, BT_CONFIG.COLORS.XD) });
    });
    (bars.zsds || []).forEach(zsd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, zsd, BT_CONFIG.COLORS.ZSD) });
    });
    (bars.bi_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.BI_ZSS) });
    });
    (bars.xd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.XD_ZSS) });
    });
    (bars.zsd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.ZSD_ZSS) });
    });
    (bars.bcs || []).forEach(bc => {
      ids.push({ id: BTChartUtils.createBcShape(chart, bc) });
    });
    (bars.mmds || []).forEach(mmd => {
      ids.push({ id: BTChartUtils.createMmdShape(chart, mmd) });
    });
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

    $("#bt-speed-slider").on("input", function () {
      const val = parseInt($(this).val());
      self.speedMs = val * 500;
      $("#bt-speed-label").text((self.speedMs / 1000).toFixed(1) + "s");
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
        const avgPrice = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
        this.position.price = avgPrice;
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
        const avgPrice = (this.position.price * this.position.qty + price * qty) / totalQty;
        this.position.qty = totalQty;
        this.position.price = avgPrice;
      } else {
        this.position = { type: "short", qty: qty, price: price };
      }
      this.addTradeRecord("卖出", price, qty);
    } else if (action === "close") {
      if (!this.position.type) { layer.msg("无持仓可平"); return; }
      let pnl = 0;
      if (this.position.type === "long") {
        pnl = (price - this.position.price) * this.position.qty;
      } else if (this.position.type === "short") {
        pnl = (this.position.price - price) * this.position.qty;
      }
      this.capital += this.position.price * this.position.qty + pnl;
      this.addTradeRecord("平仓", price, this.position.qty, pnl);
      this.position = { type: null, qty: 0, price: 0 };
    }

    this.updateCapitalDisplay();
    this.updatePositionDisplay();
  },

  addTradeRecord(direction, price, qty, pnl) {
    const time = $("#bt-current-time").text();
    const rec = { time, direction, price, qty, pnl: pnl || 0 };
    this.tradeRecords.push(rec);

    if (this.sessionKey) {
      BTLocalStore.saveTrades(this.sessionKey, this.tradeRecords);
    }

    const row = `<tr>
      <td>${time}</td>
      <td>${direction}</td>
      <td>¥${price.toFixed(2)}</td>
      <td>${qty}</td>
      <td>${pnl !== undefined ? "¥" + pnl.toFixed(2) : "--"}</td>
    </tr>`;
    $("#bt-trade-records tbody").prepend(row);
  },

  updateCapitalDisplay() {
    const priceText = $("#bt-current-price").text().replace("¥", "");
    const price = parseFloat(priceText);
    let positionValue = 0;
    if (this.position.type === "long" && !isNaN(price)) {
      positionValue = this.position.qty * price;
    } else if (this.position.type === "short" && !isNaN(price)) {
      positionValue = -this.position.qty * price;
    }

    const totalValue = this.capital + positionValue;
    const pnl = totalValue - BT_CONFIG.INITIAL_CAPITAL;
    const pnlPct = ((pnl / BT_CONFIG.INITIAL_CAPITAL) * 100).toFixed(2);

    $("#bt-current-capital").text("¥" + totalValue.toFixed(2));
    $("#bt-position-value").text("¥" + positionValue.toFixed(2));
    const pnlEl = $("#bt-pnl");
    pnlEl.text((pnl >= 0 ? "+" : "") + pnlPct + "%");
    pnlEl.removeClass("positive negative");
    pnlEl.addClass(pnl >= 0 ? "positive" : "negative");
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
