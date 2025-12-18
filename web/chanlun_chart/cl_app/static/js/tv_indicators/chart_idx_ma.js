var TvIdxMA = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "MA均线",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsMA@tv-basicstudies-1",
          description: "MA均线",
          shortDescription: "MA均线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_ma5",
              type: "line",
            },
            {
              id: "plot_ma10",
              type: "line",
            },
            {
              id: "plot_ma20",
              type: "line",
            },
            {
              id: "plot_ma60",
              type: "line",
            },
            {
              id: "plot_ma120",
              type: "line",
            },
            {
              id: "plot_ma250",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_ma5: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFA500", // 橙色 - 5日均线
              },
              plot_ma10: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#00BFFF", // 浅蓝 - 10日均线
              },
              plot_ma20: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false,
                color: "#FF00FF", // 紫红 - 20日均线
              },
              plot_ma60: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false,
                color: "#00FF00", // 绿色 - 60日均线
              },
              plot_ma120: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false,
                color: "#00FFFF", // 青色 - 120日均线
              },
              plot_ma250: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false,
                color: "#FF0000", // 红色 - 250日均线
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_ma5: {
              title: "MA5",
              histogramBase: 0,
            },
            plot_ma10: {
              title: "MA10",
              histogramBase: 0,
            },
            plot_ma20: {
              title: "MA20",
              histogramBase: 0,
            },
            plot_ma60: {
              title: "MA60",
              histogramBase: 0,
            },
            plot_ma120: {
              title: "MA120",
              histogramBase: 0,
            },
            plot_ma250: {
              title: "MA250",
              histogramBase: 0,
            },
          },
          inputs: [],
          format: {
            type: "price",
            precision: 4,
          },
        },
        constructor: function () {
          this.init = function () {
            // 初始化
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取收盘价
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // 计算各周期简单移动平均线
            const ma5 = PineJS.Std.sma(c, 5, this._context);
            const ma10 = PineJS.Std.sma(c, 10, this._context);
            const ma20 = PineJS.Std.sma(c, 20, this._context);
            const ma60 = PineJS.Std.sma(c, 60, this._context);
            const ma120 = PineJS.Std.sma(c, 120, this._context);
            const ma250 = PineJS.Std.sma(c, 250, this._context);

            return [
              ma5, // 0: 5日均线
              ma10, // 2: 10日均线
              ma20, // 4: 20日均线
              ma60, // 9: 60日均线
              ma120, // 11: 120日均线
              ma250, // 13: 250日均线
            ];
          };
        },
      };
    },
  };
})();
