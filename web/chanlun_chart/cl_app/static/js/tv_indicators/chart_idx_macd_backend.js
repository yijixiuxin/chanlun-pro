// -----------------------------------------------------------------------
// 文件名: chart_idx_macd_backend.js
// 版本: V31 (Final Fix - Widget Sync)
// 功能: 强制同步全局 Widget 周期，解决 "1" vs "1D" 匹配错误
// -----------------------------------------------------------------------
console.error("[SYSTEM] MACD Backend V31 Loaded - CACHE CLEARED");

var TvIdxMACDBackend = (function () {

  // 智能时间搜索 (保持 5天 容错)
  function smartSearch(times, target, intervalStr) {
    if (target === undefined || target === null || isNaN(target)) return -1;
    const isSeconds = target < 10000000000;
    let tolerance = isSeconds ? 3600 : 3600000;

    if (intervalStr.includes('w')) tolerance = isSeconds ? 432000 : 432000000;
    else if (intervalStr.includes('d') || intervalStr === '1440') tolerance = isSeconds ? 172800 : 172800000;

    let left = 0;
    let right = times.length - 1;
    let idx = -1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (times[mid] >= target) {
        idx = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    let bestIdx = -1;
    let minDiff = Infinity;

    if (idx !== -1) {
        const diff = Math.abs(times[idx] - target);
        if (diff <= tolerance && diff < minDiff) {
            minDiff = diff;
            bestIdx = idx;
        }
    }

    let prevIdx = (idx === -1) ? times.length - 1 : idx - 1;
    if (prevIdx >= 0) {
        const diff = Math.abs(times[prevIdx] - target);
        if (diff <= tolerance && diff < minDiff) {
            minDiff = diff;
            bestIdx = prevIdx;
        }
    }
    return bestIdx;
  }

  return {
    idx: function (PineJS) {
      return {
        name: "macd_pro_area",
        metainfo: {
          _metainfoVersion: 53,
          id: "macd_pro_area@tv-basicstudies-1",
          name: "macd_pro_area",
          description: "macd_pro_area",
          shortDescription: "macd_pro_area",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            { id: "plot_hist", type: "line", target: "plot_macd_pane" },
            { id: "plot_hist_color", type: "colorer", target: "plot_hist", palette: "paletteHist" },
            { id: "plot_dif", type: "line", target: "plot_macd_pane" },
            { id: "plot_dea", type: "line", target: "plot_macd_pane" },
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
            plot_hist: { title: "Histogram", histogramBase: 0 },
            plot_dif: { title: "MACD", histogramBase: 0 },
            plot_dea: { title: "Signal", histogramBase: 0 },
          },
          inputs: [],
          format: { type: "price", precision: 4 },
        },
        constructor: function () {
          this.init = function (context, inputCallback) {};
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            let v_dif = NaN, v_dea = NaN, v_hist = NaN, prev_hist = 0;

            try {
              const currentTime = context.symbol.time;
              if (currentTime === undefined || currentTime === null || isNaN(currentTime)) {
                  return [NaN, 0, NaN, NaN];
              }

              // 1. 获取并修正输入参数 (关键修复!)
              let rawTicker = String(context.symbol.ticker || "").toLowerCase();
              let rawInterval = String(context.symbol.interval || "").toLowerCase();

              // [FIX V31] 尝试从全局 Widget 获取真实的周期
              // 解决 context 传入 "1" 但实际是 "1d" 的问题
              if (window.tvWidget) {
                  try {
                      // 检查 widget 是否就绪并获取周期
                      if (window.tvWidget.symbolInterval) {
                        const realRes = window.tvWidget.symbolInterval().interval.toString().toLowerCase();
                        if (realRes && realRes !== rawInterval) {
                            // 仅当 context 是纯数字 (如 "1") 而 realRes 是复杂周期 (如 "1d") 时才覆盖
                            // 或者当 realRes 包含 'd'/'w' 时强制覆盖
                            if (/^\d+$/.test(rawInterval) && !/^\d+$/.test(realRes)) {
                                // console.warn(`[MACD-FIX] Override Interval: ${rawInterval} -> ${realRes}`);
                                rawInterval = realRes;
                            } else if (realRes.includes('d') || realRes.includes('w')) {
                                rawInterval = realRes;
                            }
                        }
                      }
                  } catch(e) {
                      // console.warn("[MACD-FIX] Widget access failed", e);
                  }
              }

              // 2. 获取数据源
              let datafeeds = [];
              if (window.GlobalTVDatafeeds && window.GlobalTVDatafeeds.length > 0) {
                  datafeeds = window.GlobalTVDatafeeds;
              } else if (window.tvDatafeed) {
                  datafeeds = [window.tvDatafeed];
              }

              // 3. 解析目标
              let targetCode = rawTicker;
              const parts = rawTicker.split(':');
              if (parts.length > 1) {
                  targetCode = parts[1].replace(/[^\d]/g, '');
              } else {
                  targetCode = rawTicker.replace(/[^\d]/g, '');
              }

              let targetInterval = rawInterval;
              const mappings = { 'd': '1d', '1d': 'd', 'w': '1w', '1w': 'w', 'm': '1m', '1m': 'm', '1440': '1d', '240': '4h' };
              if (mappings[rawInterval]) targetInterval = mappings[rawInterval];

              let barsResult = null;

              // 4. 遍历查找 (Fuzzy Match)
              for (const df of datafeeds) {
                  if (df._historyProvider && df._historyProvider.bars_result) {
                      const barsMap = df._historyProvider.bars_result;
                      for (const key of barsMap.keys()) {
                          const k = String(key);

                          if (!k.includes(targetCode)) continue;

                          let intervalMatch = false;
                          // 严格后缀
                          if (k.endsWith(targetInterval)) intervalMatch = true;
                          // 映射后缀
                          else if (mappings[targetInterval] && k.endsWith(mappings[targetInterval])) intervalMatch = true;
                          // 补m后缀 (仅当目标是纯数字时)
                          else if (/^\d+$/.test(targetInterval) && k.endsWith(targetInterval + 'm')) intervalMatch = true;

                          if (intervalMatch) {
                              barsResult = barsMap.get(k);
                              break;
                          }
                      }
                  }
                  if (barsResult) break;
              }

              // 5. 提取数据
              if (barsResult && barsResult.times && barsResult.macd_dif) {
                  const dataTime = barsResult.times[barsResult.times.length - 1];
                  let searchTime = currentTime;
                  if (dataTime < 10000000000 && searchTime > 10000000000) {
                      searchTime = Math.floor(searchTime / 1000);
                  }

                  const alignedIndex = smartSearch(barsResult.times, searchTime, rawInterval);

                  if (alignedIndex !== -1) {
                    v_dif = Number(barsResult.macd_dif[alignedIndex]);
                    v_dea = Number(barsResult.macd_dea[alignedIndex]);
                    v_hist = Number(barsResult.macd_hist[alignedIndex]);
                    if (alignedIndex > 0) prev_hist = Number(barsResult.macd_hist[alignedIndex - 1]);
                  }
              }

            } catch (e) {
                console.error("[MACD Crash]", e);
            }

            let colorIndex = 0;
            if (!isNaN(v_hist)) {
                if (v_hist >= 0) colorIndex = (v_hist >= prev_hist) ? 0 : 1;
                else colorIndex = (v_hist > prev_hist) ? 2 : 3;
            }
            return [v_hist, colorIndex, v_dif, v_dea];
          };
        },
      };
    },
  };
})();