// -----------------------------------------------------------------------
// 文件名: chart_idx_macd_backend.js
// 版本: V5 (智能语义对齐版)
// 功能: 解决 TV 前端(OpenTime) 与 后端(CloseTime) 时间戳不一致导致的指标丢失问题
// -----------------------------------------------------------------------
console.log("%c[SYSTEM] MACD Backend V5 (Smart-Align) Loaded", "color: #2196F3; font-weight: bold;");

var TvIdxMACDBackend = (function () {

  /**
   * 智能查找函数
   * @param {Array} times - 有序的时间戳数组 (后端数据)
   * @param {Number} target - 目标时间戳 (前端图表时间)
   * @returns {Number} 匹配到的索引，未找到返回 -1
   */
  function smartSearch(times, target) {
    let left = 0;
    let right = times.length - 1;
    let resultIndex = -1;

    // 1. 二分查找：寻找 >= target 的第一个元素 (Lower Bound)
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (times[mid] >= target) {
        resultIndex = mid;
        right = mid - 1; // 继续向左找更小的符合条件的
      } else {
        left = mid + 1;
      }
    }

    // 2. 验证匹配结果
    if (resultIndex !== -1) {
        const foundTime = times[resultIndex];

        // 情况 A: 精确匹配 (完美对齐)
        // 适用于分钟线、小时线等
        if (foundTime === target) {
            return resultIndex;
        }

        // 情况 B: 模糊匹配 (处理日线 Open vs Close 问题)
        // 如果找到的时间 比 目标时间 大，但相差在 24 小时以内
        // 说明这是同一天的 K 线 (一个是0点，一个是15点)
        const diff = foundTime - target;
        // 允许误差范围：16小时 (57600000ms)
        // A股收盘是15:00，差异是15小时，所以在允许范围内。
        // 同时也避免了匹配到第二天的数据 (24h+)
        if (diff > 0 && diff <= 57600000) {
            return resultIndex;
        }
    }

    return -1;
  }

  return {
    idx: function (PineJS) {
      return {
        name: "macd_backend",
        metainfo: {
          _metainfoVersion: 53,
          id: "macd_backend@tv-basicstudies-1",
          name: "macd_backend",
          description: "macd_backend",
          shortDescription: "macd_backend",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            { id: "plot_hist", type: "line", target: "plot_macd_pane" },
            { id: "plot_hist_color", type: "colorer", target: "plot_hist", palette: "paletteHist" },
            { id: "plot_dif", type: "line", target: "plot_macd_pane" },
            { id: "plot_dea", type: "line", target: "plot_macd_pane" },
            { id: "plot_area", type: "line", target: "plot_macd_pane" },
          ],
          palettes: {
            paletteHist: {
              colors: {
                0: { name: "Color 0" },
                1: { name: "Color 1" },
                2: { name: "Color 2" },
                3: { name: "Color 3" },
              }
            },
          },
          defaults: {
            styles: {
              plot_hist: { linestyle: 0, linewidth: 1, plottype: 5, trackPrice: false, transparency: 0, visible: true },
              plot_dif: { linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0, visible: true, color: "#2962FF" },
              plot_dea: { linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0, visible: true, color: "#FF6D00" },
              plot_area: { linestyle: 0, linewidth: 0, plottype: 0, trackPrice: false, transparency: 100, visible: true, title: "Area" },
            },
            palettes: {
              paletteHist: {
                colors: {
                  0: { color: "#26a69a", width: 1, style: 1 },
                  1: { color: "#b2dfdb", width: 1, style: 1 },
                  2: { color: "#ffcdd2", width: 1, style: 1 },
                  3: { color: "#ef5350", width: 1, style: 1 },
                }
              }
            },
            inputs: {},
          },
          styles: {
            plot_hist: { title: "直方图", histogramBase: 0 },
            plot_dif: { title: "MACD", histogramBase: 0 },
            plot_dea: { title: "信号", histogramBase: 0 },
            plot_area: { title: "Area", histogramBase: 0 },
          },
          inputs: [],
          format: { type: "price", precision: 4 },
        },
        constructor: function () {
          this.init = function (context, inputCallback) {};

          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            let v_dif = NaN;
            let v_dea = NaN;
            let v_hist = NaN;
            let v_area = NaN;
            let prev_hist = 0;

            try {
              const currentTime = context.symbol.time;

              // 1. 基础检查：数据源是否存在
              if (window.tvDatafeed && window.tvDatafeed._historyProvider && window.tvDatafeed._historyProvider.bars_result) {
                  const symbolInfo = window.tvWidget ? window.tvWidget.symbolInterval() : null;

                  if (symbolInfo) {
                    // 2. 构建 Key
                    const rawSymbol = symbolInfo.symbol.toString().toLowerCase();
                    const interval = symbolInfo.interval.toString().toLowerCase();
                    let key = rawSymbol + interval;
                    const barsMap = window.tvDatafeed._historyProvider.bars_result;
                    let barsResult = barsMap.get(key);

                    // 3. 容错 Key 查找 (处理 "a:sz..." 前缀)
                    if (!barsResult && rawSymbol.indexOf(':') !== -1) {
                         key = rawSymbol.split(':')[1] + interval;
                         barsResult = barsMap.get(key);
                    }

                    // 4. 获取数据
                    if (barsResult && barsResult.times && barsResult.macd_dif) {

                          // [核心变更] 使用 smartSearch 替代 binarySearch
                          // 既能处理精确时间，也能自动兼容 00:00 vs 15:00 的偏移
                          const alignedIndex = smartSearch(barsResult.times, currentTime);

                          if (alignedIndex !== -1) {
                            v_dif = Number(barsResult.macd_dif[alignedIndex]);
                            v_dea = Number(barsResult.macd_dea[alignedIndex]);
                            v_hist = Number(barsResult.macd_hist[alignedIndex]);

                            if (barsResult.macd_area) v_area = Number(barsResult.macd_area[alignedIndex]);
                            if (alignedIndex > 0) prev_hist = Number(barsResult.macd_hist[alignedIndex - 1]);

                            // Area 前端实时计算 (兜底策略)
                            // 如果后端没算 Area，前端根据 Hist 的红绿柱连续性自己累加
                            if ((v_area === undefined || v_area === null || isNaN(v_area)) && !isNaN(v_hist)) {
                                let current_sum = 0;
                                let i = alignedIndex;
                                const isPositive = v_hist >= 0;
                                while (i >= 0) {
                                    let h = Number(barsResult.macd_hist[i]);
                                    if (isNaN(h)) break;
                                    if ((h >= 0) !== isPositive) break;
                                    current_sum += h;
                                    i--;
                                }
                                v_area = current_sum;
                            }
                          }
                    }
                  }
              }
            } catch (e) {
                // 生产环境静默，避免控制台报错干扰
            }

            // 5. 计算颜色
            let colorIndex = 0;
            if (!isNaN(v_hist)) {
                if (v_hist >= 0) {
                    colorIndex = (v_hist >= prev_hist) ? 0 : 1;
                } else {
                    colorIndex = (v_hist > prev_hist) ? 2 : 3;
                }
            }

            return [v_hist, colorIndex, v_dif, v_dea, v_area];
          };
        },
      };
    },
  };
})();