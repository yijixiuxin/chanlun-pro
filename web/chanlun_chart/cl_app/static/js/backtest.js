// === 常量 ===
const BT_CONFIG = {
  INITIAL_CAPITAL: 100000,
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

// === 自定义 Datafeed ===
function createBacktestDatafeed(chartKey) {
  return {
    _realtimeCallback: null,
    _bars: {},

    onReady(callback) {
      $.getJSON("/backtest/tv/config", callback);
    },

    searchSymbols(userInput, exchange, symbolType, onResult) {
      onResult([]);
    },

    resolveSymbol(symbolName, onResult, onError) {
      $.getJSON("/backtest/tv/symbols", { symbol: symbolName }, function (data) {
        onResult(data);
      }).fail(onError);
    },

    getBars(symbolInfo, resolution, from, to, onResult, onError, firstDataRequest) {
      const self = this;
      $.getJSON("/backtest/tv/history", {
        symbol: symbolInfo.name,
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
          // 存储缠论数据以便重绘
          self._bars = data;
          onResult(bars, { noData: bars.length === 0 });
        } else {
          onResult([], { noData: true });
        }
      }).fail(function () { onResult([], { noData: true }); });
    },

    subscribeBars(symbolInfo, resolution, onRealtimeCallback, subscriberUID) {
      this._realtimeCallback = onRealtimeCallback;
    },

    unsubscribeBars(subscriberUID) {
      this._realtimeCallback = null;
    },

    pushBar(bar) {
      if (this._realtimeCallback) {
        const tvBar = {
          time: bar.time * 1000,
          close: bar.close,
          open: bar.open,
          high: bar.high,
          low: bar.low,
          volume: bar.volume,
        };
        this._realtimeCallback(tvBar);
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
  running: false,
  paused: false,

  // 交易状态
  capital: BT_CONFIG.INITIAL_CAPITAL,
  position: { type: null, qty: 0, price: 0 },  // type: 'long'|'short'|null
  tradeRecords: [],
  drawnShapeIds: { small: [], high: [] },

  init() {
    const winHeight = window.innerHeight;
    const smallHeight = winHeight * 0.7;
    const highHeight = winHeight * 0.3;

    this.datafeedSmall = createBacktestDatafeed("small");
    this.datafeedHigh = createBacktestDatafeed("high");

    // 创建小级别图表
    this.widgetSmall = new TradingView.widget({
      debug: false,
      autosize: true,
      fullscreen: false,
      container: "tv_chart_small",
      symbol: "small",
      interval: "1D",
      datafeed: this.datafeedSmall,
      library_path: "static/charting_library/",
      theme: "Light",
      timezone: "Asia/Shanghai",
      locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay"],
      enabled_features: [],
      time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1",
      client_id: "bt_small",
    });

    // 创建大级别图表
    this.widgetHigh = new TradingView.widget({
      debug: false,
      autosize: true,
      fullscreen: false,
      container: "tv_chart_high",
      symbol: "high",
      interval: "1D",
      datafeed: this.datafeedHigh,
      library_path: "static/charting_library/",
      theme: "Light",
      timezone: "Asia/Shanghai",
      locale: "zh",
      disabled_features: ["go_to_date", "header_symbol_search", "header_compare",
        "display_market_status", "symbol_info", "volume_force_overlay",
        "header_widget_dom_node", "header_interval_dialog_button"],
      enabled_features: [],
      time_frames: [],
      charts_storage_url: "/backtest/tv",
      charts_storage_api_version: "1.1",
      client_id: "bt_high",
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

    this.bindEvents();
  },

  redrawShapes(chartKey) {
    const datafeed = chartKey === "small" ? this.datafeedSmall : this.datafeedHigh;
    const chart = chartKey === "small" ? this.chartSmall : this.chartHigh;
    const bars = datafeed._bars;
    if (!bars || !chart) return;

    // 清除旧形状
    this.drawnShapeIds[chartKey].forEach(id => {
      try { id.then(i => chart.removeEntity(i)); } catch (e) {}
    });
    this.drawnShapeIds[chartKey] = [];

    const ids = this.drawnShapeIds[chartKey];

    // 绘制分型
    (bars.fxs || []).forEach(fx => {
      ids.push({ id: BTChartUtils.createFxShape(chart, fx) });
    });
    // 绘制笔
    (bars.bis || []).forEach(bi => {
      ids.push({ id: BTChartUtils.createLineShape(chart, bi, BT_CONFIG.COLORS.BI) });
    });
    // 绘制线段
    (bars.xds || []).forEach(xd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, xd, BT_CONFIG.COLORS.XD) });
    });
    // 绘制走势段
    (bars.zsds || []).forEach(zsd => {
      ids.push({ id: BTChartUtils.createLineShape(chart, zsd, BT_CONFIG.COLORS.ZSD) });
    });
    // 绘制笔中枢
    (bars.bi_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.BI_ZSS) });
    });
    // 绘制线段中枢
    (bars.xd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.XD_ZSS) });
    });
    // 绘制走势段中枢
    (bars.zsd_zss || []).forEach(zs => {
      ids.push({ id: BTChartUtils.createZhongshuShape(chart, zs, BT_CONFIG.COLORS.ZSD_ZSS) });
    });
    // 绘制背驰
    (bars.bcs || []).forEach(bc => {
      ids.push({ id: BTChartUtils.createBcShape(chart, bc) });
    });
    // 绘制买卖点
    (bars.mmds || []).forEach(mmd => {
      ids.push({ id: BTChartUtils.createMmdShape(chart, mmd) });
    });
  },

  bindEvents() {
    const self = this;

    $("#bt-btn-start").click(() => self.startReplay());
    $("#bt-btn-pause").click(() => self.togglePause());
    $("#bt-btn-stop").click(() => self.stopReplay());
    $("#bt-btn-buy").click(() => self.trade("buy"));
    $("#bt-btn-sell").click(() => self.trade("sell"));
    $("#bt-btn-close").click(() => self.trade("close"));

    $("#bt-speed-slider").on("input", function () {
      const val = parseInt($(this).val());
      self.speedMs = val * 500;  // 1-20 映射到 0.5s-10s
      $("#bt-speed-label").text((self.speedMs / 1000).toFixed(1) + "s");
    });
  },

  startReplay() {
    const self = this;
    $.post("/backtest/start", function (res) {
      if (!res.ok) { layer.msg(res.msg); return; }

      // 更新 UI
      $("#bt-stock-id").text(res.display_id);
      $("#bt-freqs").text(res.small_freq + " / " + res.high_freq);
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      $("#bt-current-time").text(res.current_time);
      $("#bt-btn-start").addClass("layui-btn-disabled").attr("disabled", true);

      // 重置交易状态
      self.capital = BT_CONFIG.INITIAL_CAPITAL;
      self.position = { type: null, qty: 0, price: 0 };
      self.tradeRecords = [];
      self.updateCapitalDisplay();
      self.updatePositionDisplay();
      $("#bt-trade-records tbody").empty();

      // 重新加载图表数据
      self.widgetSmall.activeChart().resetData();
      self.widgetHigh.activeChart().resetData();

      self.running = true;

      // 等待 1 秒让图表初始化完成后开始回放
      setTimeout(() => self.startTimer(), 1000);
    });
  },

  startTimer() {
    const self = this;
    this.timerId = setInterval(() => self.stepForward(), this.speedMs);
  },

  stepForward() {
    const self = this;
    $.post("/backtest/step", function (res) {
      if (res.finished) {
        self.stopTimer();
        layer.msg("回放已结束");
        return;
      }
      if (!res.ok) return;

      // 更新 UI
      $("#bt-current-price").text("¥" + res.current_price.toFixed(2));
      const progress = ((res.current_pos / res.total_bars) * 100).toFixed(1);
      $("#bt-progress").text(progress + "%");

      // 推送新 bar 到小级别图表
      if (res.new_bar) {
        self.datafeedSmall.pushBar(res.new_bar);
      }

      // 推送新 bar 到大级别图表
      if (res.new_high_bar) {
        self.datafeedHigh.pushBar(res.new_high_bar);
      }

      // 存储缠论数据并重绘
      if (res.cl_small) {
        self.datafeedSmall._bars = res.cl_small;
        self.redrawShapes("small");
      }
      if (res.cl_high) {
        self.datafeedHigh._bars = res.cl_high;
        self.redrawShapes("high");
      }

      // 更新持仓市值
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

  stopReplay() {
    this.stopTimer();
    this.running = false;
    this.paused = false;
    $.post("/backtest/stop");
    $("#bt-btn-start").removeClass("layui-btn-disabled").attr("disabled", false);
    $("#bt-btn-pause").text("暂停");
    layer.msg("回放已结束");
  },

  trade(action) {
    if (!this.running || this.paused) {
      layer.msg("请先开始回放"); return;
    }

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
        // 加仓
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
    this.tradeRecords.push({ time, direction, price, qty, pnl: pnl || 0 });
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
