// 常量定义
const CHART_CONFIG = {
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
  LINE_STYLES: {
    SOLID: 0,
    DOTTED: 1,
    DASHED: 2,
  },
  CHART_TYPES: [
    "fxs",
    "bis",
    "xds",
    "zsds",
    "bi_zss",
    "xd_zss",
    "zsd_zss",
    "bcs",
    "mmds",
  ],
};

// 防抖函数
function debounce(func, wait) {
  let timeout;
  return function (...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

// 图表工具类
const ChartUtils = {
  // 创建图表形状
  createShape(chart, points, options = {}) {
    const defaults = {
      lock: true,
      disableSelection: true,
      disableSave: true,
      disableUndo: true,
      showInObjectsTree: false,
      overrides: {},
    };

    const config = { ...defaults, ...options };
    return config.shape === "trend_line" ||
      config.shape === "rectangle" ||
      config.shape === "circle"
      ? chart.createMultipointShape(points, config)
      : chart.createShape(points, config);
  },

  // 创建分型点
  createFxShape(chart, fx, options = {}) {
    const color =
      fx.text === "ding" ? CHART_CONFIG.COLORS.DING : CHART_CONFIG.COLORS.DI;
    return this.createShape(chart, fx.points, {
      shape: "circle",
      overrides: {
        backgroundColor: color,
        color: color,
        linewidth: 4,
        ...options.overrides,
      },
      ...options,
    });
  },

  // 创建线段
  createLineShape(chart, line, options = {}) {
    return this.createShape(chart, line.points, {
      shape: "trend_line",
      overrides: {
        linestyle: parseInt(line.linestyle) || 0,
        linewidth: options.linewidth || 1,
        linecolor: options.color || CHART_CONFIG.COLORS.BI,
        ...options.overrides,
      },
      ...options,
    });
  },

  // 创建中枢
  createZhongshuShape(chart, zs, options = {}) {
    return this.createShape(chart, zs.points, {
      shape: "rectangle",
      overrides: {
        linestyle: parseInt(zs.linestyle) || 0,
        linewidth: options.linewidth || 1,
        linecolor: options.color || CHART_CONFIG.COLORS.BI,
        backgroundColor: options.color || CHART_CONFIG.COLORS.BI,
        transparency: 95,
        color: options.color,
        "trendline.linecolor": options.color,
        fillBackground: true,
        filled: true,
        ...options.overrides,
      },
      ...options,
    });
  },

  // 创建买卖点
  createMmdShape(chart, mmd, options = {}) {
    const isBuy = mmd.text.includes("B");
    const color = isBuy
      ? CHART_CONFIG.COLORS.MMD_UP
      : CHART_CONFIG.COLORS.MMD_DOWN;
    const shape = isBuy ? "arrow_up" : "arrow_down";

    return this.createShape(chart, mmd.points, {
      shape,
      text: mmd.text,
      overrides: {
        markerColor: color,
        backgroundColor: color,
        color: color,
        fontsize: 12,
        transparency: 80,
        ...options.overrides,
      },
      ...options,
    });
  },

  // 创建背驰点
  createBcShape(chart, bc, options = {}) {
    return this.createShape(chart, bc.points, {
      shape: "balloon",
      text: bc.text,
      overrides: {
        markerColor: CHART_CONFIG.COLORS.BCS,
        backgroundColor: CHART_CONFIG.COLORS.BCS,
        textColor: CHART_CONFIG.COLORS.BC_TEXT,
        transparency: 70,
        backgroundTransparency: 70,
        fontsize: 12,
        ...options.overrides,
      },
      ...options,
    });
  },
};

// 图表管理类
class ChartManager {
  constructor(id) {
    this.id = id;
    this.obj_charts = {};
    this.widget = null;
    this.udf_datafeed = null;
    this.chart = null;
    this.debouncedDrawChanlun = debounce(() => this.draw_chanlun(), 1000);
  }

  // 初始化图表
  init() {
    this.udf_datafeed = new Datafeeds.UDFCompatibleDatafeed("/tv", 30000);
    this.widget = window.tvWidget = new TradingView.widget({
      debug: false,
      autosize: true,
      fullscreen: false,
      container: "tv_chart_container_" + this.id,
      symbol: Utils.get_market() + ":" + Utils.get_code(),
      interval: Utils.get_local_data(
        Utils.get_market() + "_interval_" + this.id
      ),
      datafeed: this.udf_datafeed,
      library_path: "static/charting_library/",
      theme: Utils.get_local_data("theme"),
      numeric_formatting: { decimal_sign: "." },
      time_frames: [],
      timezone: "Asia/Shanghai",
      locale: "zh",
      symbol_search_request_delay: 100,
      auto_save_delay: 5,
      study_count_limit: 100,
      disabled_features: ["go_to_date"],
      enabled_features: ["study_templates", "seconds_resolution"],
      saved_data_meta_info: {
        uid: 1,
        name: "default",
        description: "default",
      },
      charts_storage_url: "/tv",
      charts_storage_api_version: "1.1",
      client_id: "chanlun_pro_" + Utils.get_market() + "_" + this.id,
      user_id: "999",
      load_last_chart: true,
      custom_indicators_getter: this.getCustomIndicators,
    });

    this.setupEventListeners();
    return this;
  }

  // 获取自定义指标
  getCustomIndicators(PineJS) {
    return Promise.resolve([
      TvIdxAMA.idx(PineJS),
      TvIdxATR.idx(PineJS),
      TvIdxCDBB.idx(PineJS),
      TvIdxCMCM.idx(PineJS),
      TvIdxDemo.idx(PineJS),
      TvIdxFCX.idx(PineJS),
      TvIdxHDLY.idx(PineJS),
      TvIdxHeima.idx(PineJS),
      TvIdxHLBLW.idx(PineJS),
      TvIdxHLFTX.idx(PineJS),
      TvIdxKDJ.idx(PineJS),
      TvIdxLTQS.idx(PineJS),
      TvIdxMA.idx(PineJS),
      TvIdxMACDBL.idx(PineJS),
      TvIdxVegasMA.idx(PineJS),
      TvIdxVOL.idx(PineJS),
      TvIdxZhixing.idx(PineJS),
    ]);
  }

  // 设置事件监听
  setupEventListeners() {
    // 创建几个 button 按钮
    const global_widget = this.widget;
    this.widget.headerReady().then(function () {
      // 重新加载数据的按钮
      var buttonReload = global_widget.createButton();
      buttonReload.textContent = "重新加载数据";
      buttonReload.addEventListener("click", function () {
        global_widget.resetCache();
        global_widget.activeChart().resetData();
      });
      // 增加隐藏标记的按钮
      var buttonDeleteMark = global_widget.createButton();
      buttonDeleteMark.textContent = "隐藏标记";
      buttonDeleteMark.addEventListener("click", function () {
        global_widget.activeChart().clearMarks();
      });
      // 增加删除标记的按钮
      var buttonDeleteMark = global_widget.createButton();
      buttonDeleteMark.textContent = "删除标记";
      buttonDeleteMark.addEventListener("click", function () {
        let symbol = global_widget.symbolInterval();
        console.log(symbol);
        $.post({
          type: "POST",
          url: "/tv/del_marks",
          dataType: "json",
          data: {
            symbol: symbol.symbol,
          },
          success: function (res) {
            if (res.status == "ok") {
              global_widget.activeChart().clearMarks();
              layer.msg("删除标记成功");
            } else {
              layer.msg("删除标记失败");
            }
          },
        });
      });
    });
    this.widget.onChartReady(() => {
      this.chart = this.widget.activeChart();
      if (!this.chart) {
        console.error("Failed to get active chart");
        return;
      }

      // 订阅事件
      this.chart
        .onSymbolChanged()
        .subscribe(null, (symbol) => this.handleSymbolChange(symbol));
      this.chart
        .onIntervalChanged()
        .subscribe(null, (interval) => this.handleIntervalChange(interval));

      // 数据加载事件
      this.chart
        .onDataLoaded()
        .subscribe(null, () => this.handleDataLoaded(), true);

      // 数据准备事件
      this.chart.dataReady(() => this.handleDataReady());

      // 数据更新事件
      this.widget.subscribe("onTick", () => this.handleTick());

      // 可视区域变化事件
      this.chart
        .onVisibleRangeChanged()
        .subscribe(null, () => this.handleVisibleRangeChange());
    });
  }

  // 处理标的变化
  handleSymbolChange(symbol) {
    if (!symbol?.ticker) return;

    const [market, code] = symbol.ticker.split(":");
    if (!market || !code) return;

    if (Utils.get_market() !== market) {
      Utils.set_local_data("market", market);
      location.reload();
      return;
    }

    Utils.set_local_data("market", market);
    Utils.set_local_data(`${market}_code`, code);

    console.log(`${this.id} 标的变化：${symbol.ticker}`);

    this.clear_draw_chanlun();

    if (typeof ZiXuan.render_zixuan_opts === "function") {
      ZiXuan.render_zixuan_opts();
    }

    this.debouncedDrawChanlun();
  }

  // 处理周期变化
  handleIntervalChange(interval) {
    if (!interval) return;

    const market = Utils.get_market();
    if (!market) return;

    Utils.set_local_data(`${market}_interval_${this.id}`, interval);
    console.log(`${this.id} 周期变化: ${interval}`);

    this.clear_draw_chanlun();
    this.debouncedDrawChanlun();
  }

  // 处理数据加载
  handleDataLoaded() {
    console.log("数据重新加载");
    this.clear_draw_chanlun();
    this.debouncedDrawChanlun();
  }

  // 处理数据准备
  handleDataReady() {
    console.log("数据准备");
    this.clear_draw_chanlun();
    this.debouncedDrawChanlun();
  }

  // 处理tick事件
  handleTick() {
    console.log("数据更新");
    this.clear_draw_chanlun("last");
    this.debouncedDrawChanlun();
  }

  // 处理可视区域变化
  handleVisibleRangeChange() {
    this.debouncedDrawChanlun();
  }

  // 清除已绘制的图表
  clear_draw_chanlun(clear_type) {
    // 如果  clear_type == 'last' ，则按照 time 从低到高排序，删除 time 值最大的一个对象
    console.log("清除已绘制的图表 : " + clear_type);
    if (clear_type == "last") {
      for (const symbolKey in this.obj_charts) {
        for (const chartType in this.obj_charts[symbolKey]) {
          if (this.obj_charts[symbolKey][chartType].length == 0) {
            continue;
          }
          const maxTime = Math.max(
            ...this.obj_charts[symbolKey][chartType].map((item) => item.time)
          );
          for (const _i in this.obj_charts[symbolKey][chartType]) {
            const item = this.obj_charts[symbolKey][chartType][_i];
            if (item.time == maxTime) {
              item.id.then((_id) => this.chart.removeEntity(_id));
              // console.log("remove ", symbolKey, chartType, item);
            }
          }
          this.obj_charts[symbolKey][chartType] = this.obj_charts[symbolKey][
            chartType
          ].filter((item) => item.time != maxTime);
        }
      }
    } else {
      Object.values(this.obj_charts).forEach((symbolData) => {
        Object.values(symbolData).forEach((chartItems) => {
          chartItems.forEach((item) => {
            try {
              item.id.then((_id) => this.chart.removeEntity(_id));
            } catch (e) {
              console.warn("Failed to remove chart entity:", e);
            }
          });
        });
      });
      // 清空引用
      this.obj_charts = {};
    }
  }

  // 获取图表数据
  getChartData() {
    const symbolInterval = this.widget.symbolInterval();
    if (!symbolInterval) return null;

    const symbolResKey = `${symbolInterval.symbol
      .toString()
      .toLowerCase()}${symbolInterval.interval.toString().toLowerCase()}`;
    const barsResult =
      this.udf_datafeed?._historyProvider?.bars_result?.get(symbolResKey);
    if (!barsResult) return null;

    const visibleRange = this.chart.getVisibleRange();
    const from = visibleRange?.from || 0;
    const symbolKey = `${symbolInterval.symbol}_${symbolInterval.interval}`;

    return { symbolKey, barsResult, from };
  }

  // 初始化图表容器
  initChartContainer(symbolKey) {
    if (!this.obj_charts[symbolKey]) {
      this.obj_charts[symbolKey] = {};
      CHART_CONFIG.CHART_TYPES.forEach((type) => {
        this.obj_charts[symbolKey][type] = [];
      });
    }
    return this.obj_charts[symbolKey];
  }

  // 绘制图表元素
  drawChartElements(chartData) {
    const { symbolKey, barsResult, from } = chartData;
    const chartContainer = this.initChartContainer(symbolKey);

    // console.log("bars result", barsResult);
    // console.log("chart container ", chartContainer);

    // 绘制分型
    if (barsResult.fxs) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.fxs.forEach((fx) => {
        if (fx.points?.[0]?.time >= from) {
          validTimes.add(fx.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.fxs.length - 1; i >= 0; i--) {
        const item = chartContainer.fxs[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.fxs.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.fxs.forEach((fx) => {
        if (fx.points?.[0]?.time >= from) {
          const key = JSON.stringify(fx);
          
          // 查找是否存在相同时间的分型
          const existingIndex = chartContainer.fxs.findIndex((item) => item.time === fx.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.fxs[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.fxs.splice(existingIndex, 1);
          }

          chartContainer.fxs.push({
            time: fx.points[0].time,
            key,
            id: ChartUtils.createFxShape(this.chart, fx),
          });
        }
      });
    }

    // 绘制笔
    if (barsResult.bis) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.bis.forEach((bi) => {
        if (bi.points?.[0]?.time >= from) {
          validTimes.add(bi.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素（处理起始点变化的情况）
      for (let i = chartContainer.bis.length - 1; i >= 0; i--) {
        const item = chartContainer.bis[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.bis.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.bis.forEach((bi) => {
        if (bi.points?.[0]?.time >= from) {
          const key = JSON.stringify(bi);
          
          // 查找是否存在相同时间的笔
          const existingIndex = chartContainer.bis.findIndex((item) => item.time === bi.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.bis[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.bis.splice(existingIndex, 1);
          }

          // 根据确认状态设置不同的颜色和线宽
          const isConfirmed = bi.confirmed !== false; // 默认为已确认
          const lineColor = isConfirmed ? CHART_CONFIG.COLORS.BI : "#4e4c4cff"; // 未确认笔使用灰色
          const lineWidth = isConfirmed ? 1 : 2; // 未确认笔使用更粗的线
          const lineStyle = isConfirmed ? CHART_CONFIG.LINE_STYLES.SOLID : CHART_CONFIG.LINE_STYLES.DASHED; // 未确认笔使用虚线
          
          chartContainer.bis.push({
            time: bi.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, bi, {
              color: lineColor,
              linewidth: lineWidth,
              linestyle: lineStyle,
            }),
          });
        }
      });
    }

    // 绘制线段
    if (barsResult.xds) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.xds.forEach((xd) => {
        if (xd.points?.[0]?.time >= from) {
          validTimes.add(xd.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素（处理起始点变化的情况）
      for (let i = chartContainer.xds.length - 1; i >= 0; i--) {
        const item = chartContainer.xds[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.xds.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.xds.forEach((xd) => {
        if (xd.points?.[0]?.time >= from) {
          const key = JSON.stringify(xd);
          
          // 查找是否存在相同时间的线段
          const existingIndex = chartContainer.xds.findIndex((item) => item.time === xd.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.xds[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.xds.splice(existingIndex, 1);
          }

          chartContainer.xds.push({
            time: xd.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, xd, {
              color: CHART_CONFIG.COLORS.XD,
              linewidth: 2,
            }),
          });
        }
      });
    }

    // 绘制走势段
    if (barsResult.zsds) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.zsds.forEach((zsd) => {
        if (zsd.points?.[0]?.time >= from) {
          validTimes.add(zsd.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.zsds.length - 1; i >= 0; i--) {
        const item = chartContainer.zsds[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.zsds.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.zsds.forEach((zsd) => {
        if (zsd.points?.[0]?.time >= from) {
          const key = JSON.stringify(zsd);
          
          // 查找是否存在相同时间的走势段
          const existingIndex = chartContainer.zsds.findIndex((item) => item.time === zsd.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.zsds[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.zsds.splice(existingIndex, 1);
          }

          chartContainer.zsds.push({
            time: zsd.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, zsd, {
              color: CHART_CONFIG.COLORS.ZSD,
              linewidth: 3,
            }),
          });
        }
      });
    }

    // 绘制笔中枢
    if (barsResult.bi_zss) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.bi_zss.forEach((bi_zs) => {
        if (bi_zs.points?.[0]?.time >= from) {
          validTimes.add(bi_zs.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.bi_zss.length - 1; i >= 0; i--) {
        const item = chartContainer.bi_zss[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.bi_zss.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.bi_zss.forEach((bi_zs) => {
        if (bi_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(bi_zs);
          
          // 查找是否存在相同时间的中枢
          const existingIndex = chartContainer.bi_zss.findIndex((item) => item.time === bi_zs.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.bi_zss[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.bi_zss.splice(existingIndex, 1);
          }

          chartContainer.bi_zss.push({
            time: bi_zs.points[0].time,
            key,
            id: ChartUtils.createZhongshuShape(this.chart, bi_zs, {
              color: CHART_CONFIG.COLORS.BI_ZSS,
              linewidth: 1,
            }),
          });
        }
      });
    }

    // 绘制线段中枢
    if (barsResult.xd_zss) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.xd_zss.forEach((xd_zs) => {
        if (xd_zs.points?.[0]?.time >= from) {
          validTimes.add(xd_zs.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.xd_zss.length - 1; i >= 0; i--) {
        const item = chartContainer.xd_zss[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.xd_zss.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.xd_zss.forEach((xd_zs) => {
        if (xd_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(xd_zs);
          
          // 查找是否存在相同时间的中枢
          const existingIndex = chartContainer.xd_zss.findIndex((item) => item.time === xd_zs.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.xd_zss[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.xd_zss.splice(existingIndex, 1);
          }

          chartContainer.xd_zss.push({
            time: xd_zs.points[0].time,
            key,
            id: ChartUtils.createZhongshuShape(this.chart, xd_zs, {
              color: CHART_CONFIG.COLORS.XD_ZSS,
              linewidth: 2,
            }),
          });
        }
      });
    }

    // 绘制走势段中枢
    if (barsResult.zsd_zss) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.zsd_zss.forEach((zsd_zs) => {
        if (zsd_zs.points?.[0]?.time >= from) {
          validTimes.add(zsd_zs.points[0].time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.zsd_zss.length - 1; i >= 0; i--) {
        const item = chartContainer.zsd_zss[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.zsd_zss.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.zsd_zss.forEach((zsd_zs) => {
        if (zsd_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(zsd_zs);
          
          // 查找是否存在相同时间的中枢
          const existingIndex = chartContainer.zsd_zss.findIndex((item) => item.time === zsd_zs.points[0].time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.zsd_zss[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.zsd_zss.splice(existingIndex, 1);
          }

          chartContainer.zsd_zss.push({
            time: zsd_zs.points[0].time,
            key,
            id: ChartUtils.createZhongshuShape(this.chart, zsd_zs, {
              color: CHART_CONFIG.COLORS.ZSD_ZSS,
              linewidth: 2,
            }),
          });
        }
      });
    }

    // 绘制背驰
    if (barsResult.bcs) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.bcs.forEach((bc) => {
        if (bc.points?.time >= from) {
          validTimes.add(bc.points.time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.bcs.length - 1; i >= 0; i--) {
        const item = chartContainer.bcs[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.bcs.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.bcs.forEach((bc) => {
        if (bc.points?.time >= from) {
          const key = JSON.stringify(bc);
          
          // 查找是否存在相同时间的背驰
          const existingIndex = chartContainer.bcs.findIndex((item) => item.time === bc.points.time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.bcs[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.bcs.splice(existingIndex, 1);
          }

          chartContainer.bcs.push({
            time: bc.points.time,
            key,
            id: ChartUtils.createBcShape(this.chart, bc),
          });
        }
      });
    }

    // 绘制买卖点
    if (barsResult.mmds) {
      // 1. 收集当前数据中所有有效的时间点
      const validTimes = new Set();
      barsResult.mmds.forEach((mmd) => {
        if (mmd.points?.time >= from) {
          validTimes.add(mmd.points.time);
        }
      });

      // 2. 清理图表中不存在于新数据中的元素
      for (let i = chartContainer.mmds.length - 1; i >= 0; i--) {
        const item = chartContainer.mmds[i];
        if (item.time >= from && !validTimes.has(item.time)) {
          item.id.then((_id) => this.chart.removeEntity(_id));
          chartContainer.mmds.splice(i, 1);
        }
      }

      // 3. 绘制/更新元素
      barsResult.mmds.forEach((mmd) => {
        if (mmd.points?.time >= from) {
          const key = JSON.stringify(mmd);
          
          // 查找是否存在相同时间的买卖点
          const existingIndex = chartContainer.mmds.findIndex((item) => item.time === mmd.points.time);
          if (existingIndex !== -1) {
            const existingItem = chartContainer.mmds[existingIndex];
            if (existingItem.key === key) return; // 完全相同，跳过
            // key 不同，说明有变化，删除旧的
            existingItem.id.then((_id) => this.chart.removeEntity(_id));
            chartContainer.mmds.splice(existingIndex, 1);
          }

          chartContainer.mmds.push({
            time: mmd.points.time,
            key,
            id: ChartUtils.createMmdShape(this.chart, mmd),
          });
        }
      });
    }
  }

  // 绘制缠论图表
  draw_chanlun() {
    const code_start = performance.now();

    const chartData = this.getChartData();
    if (!chartData) return;

    console.log("Drawing chart for:", chartData.symbolKey);

    // 绘制所有图表元素
    this.drawChartElements(chartData);

    const code_end = performance.now();
    console.log(
      `${chartData.symbolKey} 运行时间: ${code_end - code_start} 毫秒`
    );
  }
}

var Charts = (function () {
  return {
    // 图表展示
    show_tv_chart: function (id) {
      const chartManager = new ChartManager(id).init();
      return chartManager.widget;
    },
  };
})();
