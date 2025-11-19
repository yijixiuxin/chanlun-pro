var TvIdxMACDBackend = (function () {
  // 二分查找辅助函数：在有序数组 times 中查找 target 时间对应的索引
  function binarySearch(arr, target) {
    let left = 0;
    let right = arr.length - 1;
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (arr[mid] === target) return mid;
      if (arr[mid] < target) left = mid + 1;
      else right = mid - 1;
    }
    return -1;
  }

  return {
    idx: function (PineJS) {
      return {
        name: "MACD",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsMACD@tv-basicstudies-1",
          name: "MACD",
          description: "MACD",
          shortDescription: "MACD",
          is_price_study: false,
          isCustomIndicator: true,
          // 定义绘图输出
          plots: [
            // 0. 直方图数值
            {
              id: "plot_hist",
              type: "line",
              target: "plot_macd_pane",
            },
            // 1. 直方图颜色 (Colorer)
            {
              id: "plot_hist_color",
              type: "colorer",
              target: "plot_hist",
              palette: "paletteHist",
            },
            // 2. DIF线
            {
              id: "plot_dif",
              type: "line",
              target: "plot_macd_pane",
            },
            // 3. DEA线
            {
              id: "plot_dea",
              type: "line",
              target: "plot_macd_pane",
            },
            // 4. 面积数值 (Area)
            // 我们将其定义为 line，但在样式中隐藏线条，仅用于显示数值
            {
              id: "plot_area",
              type: "line",
              target: "plot_macd_pane",
            },
          ],

          // 调色板结构
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
              plot_hist: {
                linestyle: 0,
                linewidth: 1,
                plottype: 5, // Columns (列状图)
                trackPrice: false,
                transparency: 0,
                visible: true,
              },
              plot_dif: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#2962FF", // 蓝色
              },
              plot_dea: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF6D00", // 橙色
              },
              plot_area: {
                linestyle: 0,
                linewidth: 0,   // 线宽为0，不显示线条
                plottype: 0,
                trackPrice: false,
                transparency: 100, // 完全透明
                visible: true,  // 必须为 true，否则状态栏不显示数值
                title: "Area"   // 显示名称
              },
            },

            // 调色板默认颜色
            palettes: {
              paletteHist: {
                colors: {
                  0: { color: "#26a69a", width: 1, style: 1 }, // 深绿
                  1: { color: "#b2dfdb", width: 1, style: 1 }, // 浅绿
                  2: { color: "#ffcdd2", width: 1, style: 1 }, // 浅红
                  3: { color: "#ef5350", width: 1, style: 1 }, // 深红
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
          format: {
            type: "price",
            precision: 4,
          },
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
              // 使用时间戳而不是索引
              const currentTime = context.symbol.time;

              if (window.tvWidget && window.tvWidget._options && window.tvWidget._options.datafeed) {
                const symbolInfo = window.tvWidget.symbolInterval();
                if (symbolInfo) {
                  const key = symbolInfo.symbol.toString().toLowerCase() + symbolInfo.interval.toString().toLowerCase();
                  const datafeed = window.tvWidget._options.datafeed;
                  if (datafeed._historyProvider && datafeed._historyProvider.bars_result) {
                    const barsResult = datafeed._historyProvider.bars_result.get(key);

                    // 确保我们有时间数组和 MACD 数据
                    if (barsResult && barsResult.macd_dif && barsResult.times) {
                      // [核心逻辑] 通过二分查找，在 barsResult.times 中找到 currentTime 对应的索引
                      // 此时 barsResult.times 在 bundle.js 中已被强制转换为毫秒并排序
                      const alignedIndex = binarySearch(barsResult.times, currentTime);

                      if (alignedIndex !== -1) {
                        v_dif = barsResult.macd_dif[alignedIndex];
                        v_dea = barsResult.macd_dea[alignedIndex];
                        v_hist = barsResult.macd_hist[alignedIndex];

                        // 优先使用后端返回的 macd_area
                        if (barsResult.macd_area) {
                            v_area = barsResult.macd_area[alignedIndex];
                        }

                        if (alignedIndex > 0) {
                            prev_hist = barsResult.macd_hist[alignedIndex - 1];
                        }

                        // 兜底逻辑：如果后端没有返回 area，或者是 NaN，我们在前端实时计算
                        // 注意：这里的回溯也依赖于缓存数组的连续性
                        if ((v_area === undefined || v_area === null || isNaN(v_area)) && !isNaN(v_hist)) {
                            let current_sum = 0;
                            let i = alignedIndex;
                            // 当前柱子的方向（>=0 为正，<0 为负）
                            const isPositive = v_hist >= 0;

                            // 向前回溯
                            while (i >= 0) {
                                let h = barsResult.macd_hist[i];
                                if (h === undefined || h === null || isNaN(h)) break;

                                // 检查方向是否一致
                                const hPositive = h >= 0;
                                if (hPositive !== isPositive) {
                                    break; // 遇到反向柱子，停止累加
                                }

                                current_sum += h;
                                i--;
                            }
                            v_area = current_sum;
                        }
                      }

                      // 处理未找到数据的情况（保持 NaN）
                      if (v_dif === null) v_dif = NaN;
                      if (v_dea === null) v_dea = NaN;
                      if (v_hist === null) v_hist = NaN;
                      if (v_area === null) v_area = NaN;
                      if (prev_hist === null) prev_hist = 0;
                    }
                  }
                }
              }
            } catch (e) {
                console.error("MACD calc error:", e);
            }

            // 颜色逻辑
            let colorIndex = 0;
            if (!isNaN(v_hist)) {
                if (v_hist >= 0) {
                    // 正值区域
                    colorIndex = (v_hist >= prev_hist) ? 0 : 1;
                } else {
                    // 负值区域
                    colorIndex = (v_hist > prev_hist) ? 2 : 3;
                }
            }

            // 返回数组：[直方图, 颜色索引, DIF, DEA, 面积]
            return [v_hist, colorIndex, v_dif, v_dea, v_area];
          };
        },
      };
    },
  };
})();