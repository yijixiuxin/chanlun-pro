// -----------------------------------------------------------------------
// 文件名: chart_idx_macd_backend.js
// 版本: V6 (Area Peak Annotation)
// 功能: MACD后端对齐 + 区域面积峰值标注
// -----------------------------------------------------------------------
console.log("%c[SYSTEM] MACD Backend V6 (Area-Peak) Loaded", "color: #2196F3; font-weight: bold;");

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

    // 1. 二分查找
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (times[mid] >= target) {
        resultIndex = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    // 2. 验证匹配结果
    if (resultIndex !== -1) {
        const foundTime = times[resultIndex];
        if (foundTime === target) {
            return resultIndex;
        }
        const diff = foundTime - target;
        // 允许 16小时 误差 (兼容日线 Open/Close 差异)
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
            // Area 修改为圆点显示，以便观察孤立的数值
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
              // Area 样式修改：可见(transparency:0)，线宽加大，颜色醒目
              plot_area: { linestyle: 0, linewidth: 2, plottype: 0, trackPrice: true, transparency: 0, visible: true, title: "Region Area", color: "#9C27B0" },
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
            plot_area: { title: "区域面积", histogramBase: 0 },
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
            let v_area = NaN; // 默认为 NaN，只在峰值处有值
            let prev_hist = 0;

            try {
              const currentTime = context.symbol.time;

              if (window.tvDatafeed && window.tvDatafeed._historyProvider && window.tvDatafeed._historyProvider.bars_result) {
                  const symbolInfo = window.tvWidget ? window.tvWidget.symbolInterval() : null;

                  if (symbolInfo) {
                    const rawSymbol = symbolInfo.symbol.toString().toLowerCase();
                    const interval = symbolInfo.interval.toString().toLowerCase();
                    let key = rawSymbol + interval;
                    const barsMap = window.tvDatafeed._historyProvider.bars_result;
                    let barsResult = barsMap.get(key);

                    if (!barsResult && rawSymbol.indexOf(':') !== -1) {
                         key = rawSymbol.split(':')[1] + interval;
                         barsResult = barsMap.get(key);
                    }

                    if (barsResult && barsResult.times && barsResult.macd_dif) {
                          const alignedIndex = smartSearch(barsResult.times, currentTime);

                          if (alignedIndex !== -1) {
                            v_dif = Number(barsResult.macd_dif[alignedIndex]);
                            v_dea = Number(barsResult.macd_dea[alignedIndex]);
                            v_hist = Number(barsResult.macd_hist[alignedIndex]);
                            if (alignedIndex > 0) prev_hist = Number(barsResult.macd_hist[alignedIndex - 1]);

                            // -----------------------------------------------------
                            // [核心逻辑修改] 计算连续区域面积，并只在最长柱子上显示
                            // -----------------------------------------------------
                            if (!isNaN(v_hist) && v_hist !== 0) {
                                const hists = barsResult.macd_hist;
                                const len = hists.length;
                                const isPositive = v_hist > 0;

                                // 1. 向左寻找起点 (Start)
                                let start = alignedIndex;
                                while (start > 0) {
                                    const p = Number(hists[start - 1]);
                                    if (isNaN(p) || p === 0 || (p > 0 !== isPositive)) break;
                                    start--;
                                }

                                // 2. 向右寻找终点 (End)
                                let end = alignedIndex;
                                while (end < len - 1) {
                                    const n = Number(hists[end + 1]);
                                    if (isNaN(n) || n === 0 || (n > 0 !== isPositive)) break;
                                    end++;
                                }

                                // 3. 计算区域总和 & 寻找峰值索引
                                let regionSum = 0;
                                let maxAbsVal = -1;
                                let maxIdx = -1;

                                for (let i = start; i <= end; i++) {
                                    const val = Number(hists[i]);
                                    regionSum += val;
                                    const absVal = Math.abs(val);

                                    // 寻找绝对值最大的柱子
                                    // 使用 >= 是为了在有多个相同峰值时，标记在靠后的位置(或根据需求调整)
                                    // 这里使用 > ，遇到相同值保留第一个
                                    if (absVal > maxAbsVal) {
                                        maxAbsVal = absVal;
                                        maxIdx = i;
                                    }
                                }

                                // 4. 只有当前是峰值柱子时，才输出 Area
                                if (alignedIndex === maxIdx) {
                                    v_area = regionSum;
                                }
                            }
                            // -----------------------------------------------------
                          }
                    }
                  }
              }
            } catch (e) {}

            // 计算颜色
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