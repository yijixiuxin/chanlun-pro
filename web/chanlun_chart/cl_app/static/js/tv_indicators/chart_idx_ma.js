var TvIdxMA = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "MA均线", // 注意：这里的名字必须和 createStudy 中的一致
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsMA@tv-basicstudies-1",
          description: "MA均线",
          shortDescription: "MA均线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            { id: "plot_ma5", type: "line" },
            { id: "plot_ma10", type: "line" },
            { id: "plot_ma20", type: "line" },
            { id: "plot_ma60", type: "line" },
            { id: "plot_ma120", type: "line" },
            { id: "plot_ma250", type: "line" },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_ma5: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: true,
                color: "#FFA500", // 橙色
              },
              plot_ma10: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: true,
                color: "#00BFFF", // 浅蓝
              },
              plot_ma20: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: false,
                color: "#FF00FF",
              },
              plot_ma60: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: false,
                color: "#00FF00",
              },
              plot_ma120: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: false,
                color: "#00FFFF",
              },
              plot_ma250: {
                linestyle: 0, linewidth: 1, plottype: 0, trackPrice: false, transparency: 0,
                visible: false,
                color: "#FF0000",
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_ma5: { title: "MA5", histogramBase: 0 },
            plot_ma10: { title: "MA10", histogramBase: 0 },
            plot_ma20: { title: "MA20", histogramBase: 0 },
            plot_ma60: { title: "MA60", histogramBase: 0 },
            plot_ma120: { title: "MA120", histogramBase: 0 },
            plot_ma250: { title: "MA250", histogramBase: 0 },
          },
          inputs: [],
          format: { type: "price", precision: 4 },
        },
        constructor: function () {
          this.init = function () {};
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;
            const c = this._context.new_var(PineJS.Std.close(this._context));
            const ma5 = PineJS.Std.sma(c, 5, this._context);
            const ma10 = PineJS.Std.sma(c, 10, this._context);
            const ma20 = PineJS.Std.sma(c, 20, this._context);
            const ma60 = PineJS.Std.sma(c, 60, this._context);
            const ma120 = PineJS.Std.sma(c, 120, this._context);
            const ma250 = PineJS.Std.sma(c, 250, this._context);
            return [ma5, ma10, ma20, ma60, ma120, ma250];
          };
        },
      };
    },
  };
})();