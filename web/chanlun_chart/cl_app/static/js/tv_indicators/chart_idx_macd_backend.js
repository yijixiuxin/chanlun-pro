// -----------------------------------------------------------------------
// 文件名: chart_idx_macd_backend.js
// 版本: V23 (Final Stable)
// 功能: 移除不稳定依赖，强制类型转换，多重Key匹配，详细日志
// -----------------------------------------------------------------------
console.log("%c[SYSTEM] MACD Backend V23 Loaded", "color: #2196F3; font-weight: bold;");

var TvIdxMACDBackend = (function () {
  // 智能搜索：二分查找 + 容错
  function smartSearch(times, target) {
    if (target === undefined || target === null || isNaN(target)) return -1;

    let left = 0;
    let right = times.length - 1;
    let resultIndex = -1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (times[mid] >= target) {
        resultIndex = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    if (resultIndex !== -1) {
        const foundTime = times[resultIndex];
        if (foundTime === target) return resultIndex;

        // 容错逻辑：秒 vs 毫秒
        const isSeconds = target < 10000000000;
        const tolerance = isSeconds ? 300 : 57600000;
        const diff = foundTime - target;
        if (diff > 0 && diff <= tolerance) return resultIndex;
    }
    return -1;
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
              // 1. 获取当前时间 (最基础的方式，不依赖 PineJS 高级特性)
              const currentTime = context.symbol.time;

              // 如果时间无效，直接返回，不要继续计算
              if (currentTime === undefined || currentTime === null || isNaN(currentTime)) {
                  return [NaN, 0, NaN, NaN];
              }

              // 2. 获取所有注册的数据源 (Multi-Chart 支持)
              let datafeeds = [];
              if (window.GlobalTVDatafeeds && window.GlobalTVDatafeeds.length > 0) {
                  datafeeds = window.GlobalTVDatafeeds;
              } else if (window.tvDatafeed) {
                  datafeeds = [window.tvDatafeed];
              }

              // 3. 强制类型转换 (防止 .toLowerCase 崩溃)
              let rawTicker = String(context.symbol.ticker || "").toLowerCase();
              let rawInterval = String(context.symbol.interval || "").toLowerCase();

              // 4. 构建候选 Key 列表 (解决 30 vs 30m, 1 vs 1d 问题)
              // 基础 Key: "a:sz.300600" + "30" -> "a:sz.30060030"
              let baseKey = rawTicker + rawInterval;
              // 无前缀 Key: "sz.300600" + "30" -> "sz.30060030"
              let baseKeyNoMarket = (rawTicker.indexOf(':') !== -1) ? rawTicker.split(':')[1] + rawInterval : null;

              let candidateKeys = [baseKey];
              if (baseKeyNoMarket) candidateKeys.push(baseKeyNoMarket);

              // 如果 interval 是纯数字 (例如 "30"), 尝试添加 "d" 或 "m" 后缀
              // 因为后端缓存的可能是 "30m" 或 "1d"
              if (/^\d+$/.test(rawInterval)) {
                  candidateKeys.push(baseKey + 'd'); // 尝试日线
                  candidateKeys.push(baseKey + 'm'); // 尝试分钟
                  candidateKeys.push(baseKey + 'w'); // 尝试周线
                  if (baseKeyNoMarket) {
                      candidateKeys.push(baseKeyNoMarket + 'd');
                      candidateKeys.push(baseKeyNoMarket + 'm');
                      candidateKeys.push(baseKeyNoMarket + 'w');
                  }
              }

              // 5. 遍历数据源和候选 Key 查找数据
              let barsResult = null;

              // 外层循环：遍历所有 Datafeeds (解决多图表隔离)
              for (const df of datafeeds) {
                  if (df._historyProvider && df._historyProvider.bars_result) {
                      const barsMap = df._historyProvider.bars_result;

                      // 内层循环：遍历所有可能的 Key 格式
                      for (const k of candidateKeys) {
                          const res = barsMap.get(k);
                          if (res && res.times && res.times.length > 0) {
                              barsResult = res;
                              break; // 找到了！停止 Key 循环
                          }
                      }
                  }
                  if (barsResult) break; // 找到了！停止 Datafeed 循环
              }

              // 6. 如果找到了数据，进行时间对齐和提取
              if (barsResult && barsResult.times && barsResult.macd_dif) {
                  const dataTime = barsResult.times[barsResult.times.length - 1];
                  let searchTime = currentTime;

                  // 时间单位自动检测 (秒 vs 毫秒)
                  if (dataTime < 10000000000 && searchTime > 10000000000) {
                      searchTime = Math.floor(searchTime / 1000);
                  }

                  const alignedIndex = smartSearch(barsResult.times, searchTime);

                  if (alignedIndex !== -1) {
                    v_dif = Number(barsResult.macd_dif[alignedIndex]);
                    v_dea = Number(barsResult.macd_dea[alignedIndex]);
                    v_hist = Number(barsResult.macd_hist[alignedIndex]);
                    if (alignedIndex > 0) prev_hist = Number(barsResult.macd_hist[alignedIndex - 1]);
                  } else {
                      // Debug: 找到了数据但时间没对上 (偶尔发生是正常的，比如最新的一根K线还没推送到)
                      // console.warn(`[MACD] Time Mismatch. Search: ${searchTime}, LastData: ${dataTime}`);
                  }
              }

            } catch (e) {
                console.error("[MACD Crash]", e);
            }

            // 7. 计算颜色
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