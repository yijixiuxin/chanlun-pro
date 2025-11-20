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

// 默认颜色配置 (回退用)
const DEFAULT_COLORS = {
  bis: CHART_CONFIG.COLORS.BI,
  xds: CHART_CONFIG.COLORS.XD,
  zsds: CHART_CONFIG.COLORS.ZSD,
  bi_zss: CHART_CONFIG.COLORS.BI_ZSS,
  xd_zss: CHART_CONFIG.COLORS.XD_ZSS,
  zsd_zss: CHART_CONFIG.COLORS.ZSD_ZSS,
};

// 基于周期的动态颜色配置
// "1" = 1分钟, "5" = 5分钟, "30" = 30分钟, "1D" = 日线
const DYNAMIC_CHART_COLORS = {
  "1": {
    // 1F 分钟图
    ...DEFAULT_COLORS,
    bis: "#FFA500", // 橙色 笔
    xds: "#DAA520", // 黄色 (GoldenRod)
    xd_zss: "#ADD8E6", // 蓝色(稍浅蓝色) 线段中枢
  },
  "5": {
    // 5F 分钟图
    ...DEFAULT_COLORS,
    bi_zss: "#ADD8E6", // 浅蓝色 笔中枢 (对应 "一分钟线段形成的中枢")
    xds: "#ADD8E6", // 浅蓝色 5分钟线段
    xd_zss: "#FF0000", // 红色 5分钟线段中枢
    zsds: "#FF0000", // 红色 30分钟线段 (走势段)
  },
  "30": {
    // 30F 分钟图
    ...DEFAULT_COLORS,
    xds: "#FF0000", // 红色 30分钟线段
    xd_zss: "#008000", // 绿色 30分钟线段中枢
    zsds: "#008000", // 绿色 日线线段 (走势段)
  },
  "1D": {
    // 1D 日线图
    ...DEFAULT_COLORS,
    xds: "#008000", // 绿色 日线线段
    xd_zss: "#00008B", // 深蓝色 日线线段中枢
    zsds: "#00008B", // 深蓝色 周线线段 (走势段)
  },
};

/**
 * 获取基于周期的动态颜色
 * @param {string} interval - 当前图表周期 (例如 "1", "5", "1D")
 * @param {string} elementType - 图表元素类型 (例如 "bis", "xds", "xd_zss")
 * @returns {string} 颜色值
 */
function getDynamicColor(interval, elementType) {
  // 检查特定周期的配置
  if (
    DYNAMIC_CHART_COLORS[interval] &&
    DYNAMIC_CHART_COLORS[interval][elementType]
  ) {
    return DYNAMIC_CHART_COLORS[interval][elementType];
  }
  // 回退到默认配置
  return DEFAULT_COLORS[elementType] || "#FFFFFF"; // 最终回退
}

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
    this.udf_datafeed = new Datafeeds.UDFCompatibleDatafeed("/tv", 60000);
    window.tvDatafeed = this.udf_datafeed;
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

      // --- 缩放设置 ---
      time_scale: {
        min_bar_spacing: 0.05,
        max_bar_spacing: 800,
      },
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
        TvIdxMACDBackend.idx(PineJS),
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


      // 1. 确保暴露数据源 (如果在 init 方法里没加，这里再加一次保险)
        if (this.udf_datafeed) {
            window.tvDatafeed = this.udf_datafeed;
            console.log("window.tvDatafeed 已暴露");
        }

        if (this.chart.onDataLoaded) {
            this.chart.onDataLoaded().subscribe(null, () => {
                console.log("图表数据加载完毕，准备检查指标...");

                // 获取所有指标
                const studies = this.chart.getAllStudies();

                // 遍历并删除所有名字里带 MACD 的指标 (防止旧的缓存残留)
                studies.forEach(s => {
                    if (s.name === 'MACD_Backend' || s.name === 'macd_backend' || s.name === 'MACD') {
                        console.log("发现旧指标，正在移除:", s.name, s.id);
                        this.chart.removeEntity(s.id);
                    }
                });

                // 强制重新创建 (延迟 500ms 确保移除完成)
                setTimeout(() => {
                    console.log("正在重新创建 macd_backend 指标...");
                    this.chart.createStudy('macd_backend', false, false);
                }, 500);

            }, true);
        }
      // 应用K线颜色
      this.chart.applyOverrides({
        "mainSeriesProperties.candleStyle.upColor": "#ef5350",
        "mainSeriesProperties.candleStyle.downColor": "#26a69a",
        "mainSeriesProperties.candleStyle.borderUpColor": "#ef5350",
        "mainSeriesProperties.candleStyle.borderDownColor": "#26a69a",
        "mainSeriesProperties.candleStyle.wickUpColor": "#ef5350",
        "mainSeriesProperties.candleStyle.wickDownColor": "#26a69a",
      });

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
    // 正确：调用全量清除
    this.clear_draw_chanlun();
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
  drawChartElements(chartData, currentInterval) {
    const { symbolKey, barsResult, from } = chartData;
    const chartContainer = this.initChartContainer(symbolKey);

    // console.log("bars result", barsResult);
    // console.log("chart container ", chartContainer);

    // 绘制分型
    if (barsResult.fxs) {
      barsResult.fxs.forEach((fx) => {
        if (fx.points?.[0]?.time >= from) {
          const key = JSON.stringify(fx);
          // 检查，如果 chartContainer.fxs 中，有 key 的值，则跳过
          const existed = chartContainer.fxs.find((item) => item.key === key);
          if (existed) return;
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
      barsResult.bis.forEach((bi) => {
        if (bi.points?.[0]?.time >= from) {
          const key = JSON.stringify(bi);
          const existed = chartContainer.bis.find((item) => item.key === key);
          if (existed) return;
          chartContainer.bis.push({
            time: bi.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, bi, {
              color: getDynamicColor(currentInterval, "bis"), // 动态颜色
              linewidth: 1,
            }),
          });
        }
      });
    }

    // 绘制线段
    if (barsResult.xds) {
      barsResult.xds.forEach((xd) => {
        if (xd.points?.[0]?.time >= from) {
          const key = JSON.stringify(xd);
          const existed = chartContainer.xds.find((item) => item.key === key);
          if (existed) return;
          chartContainer.xds.push({
            time: xd.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, xd, {
              color: getDynamicColor(currentInterval, "xds"), // 动态颜色
              linewidth: 2,
            }),
          });
        }
      });
    }

    // 绘制走势段
    if (barsResult.zsds) {
      barsResult.zsds.forEach((zsd) => {
        if (zsd.points?.[0]?.time >= from) {
          const key = JSON.stringify(zsd);
          const existed = chartContainer.zsds.find((item) => item.key === key);
          if (existed) return;
          chartContainer.zsds.push({
            time: zsd.points[0].time,
            key,
            id: ChartUtils.createLineShape(this.chart, zsd, {
              color: getDynamicColor(currentInterval, "zsds"), // 动态颜色
              linewidth: 3,
            }),
          });
        }
      });
    }

    // 绘制笔中枢
    if (barsResult.bi_zss) {
      barsResult.bi_zss.forEach((bi_zs) => {
        if (bi_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(bi_zs);
          const existed = chartContainer.bi_zss.find(
            (item) => item.key === key
          );
          if (existed) return;
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
      barsResult.xd_zss.forEach((xd_zs) => {
        if (xd_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(xd_zs);
          const existed = chartContainer.xd_zss.find(
            (item) => item.key === key
          );
          if (existed) return;
          chartContainer.xd_zss.push({
            time: xd_zs.points[0].time,
            key,
            id: ChartUtils.createZhongshuShape(this.chart, xd_zs, {
              color: getDynamicColor(currentInterval, "xd_zss"), // 动态颜色
              linewidth: 2,
            }),
          });
        }
      });
    }

    // 绘制走势段中枢
    if (barsResult.zsd_zss) {
      barsResult.zsd_zss.forEach((zsd_zs) => {
        if (zsd_zs.points?.[0]?.time >= from) {
          const key = JSON.stringify(zsd_zs);
          const existed = chartContainer.zsd_zss.find(
            (item) => item.key === key
          );
          if (existed) return;
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
      barsResult.bcs.forEach((bc) => {
        if (bc.points?.time >= from) {
          const key = JSON.stringify(bc);
          const existed = chartContainer.bcs.find((item) => item.key === key);
          if (existed) return;
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
      barsResult.mmds.forEach((mmd) => {
        if (mmd.points?.time >= from) {
          const key = JSON.stringify(mmd);
          const existed = chartContainer.mmds.find((item) => item.key === key);
          if (existed) return;
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

    // 获取当前周期
    const symbolInterval = this.widget.symbolInterval();
    if (!symbolInterval) return;
    const currentInterval = symbolInterval.interval;

    console.log(
      "Drawing chart for:",
      chartData.symbolKey,
      "Interval:",
      currentInterval
    );

    // 绘制所有图表元素，传入当前周期
    this.drawChartElements(chartData, currentInterval);

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