// -----------------------------------------------------------------------
// 文件名: chart_idx_macd_backend.js
// 版本: V15 (Stable - macd_pro_area)
// 功能: MACD 数据定义
// -----------------------------------------------------------------------
console.log("%c[SYSTEM] MACD Backend V15 Loaded", "color: #2196F3; font-weight: bold;");

var TvIdxMACDBackend = (function () {
  function smartSearch(times, target) {
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
        const diff = foundTime - target;
        if (diff > 0 && diff <= 57600000) return resultIndex;
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
                          }
                    }
                  }
              }
            } catch (e) {}
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