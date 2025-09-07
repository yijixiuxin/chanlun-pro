var TvIdxMA = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "MA均线",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsMA@tv-basicstudies-1",
          description: "东@MA均线",
          shortDescription: "MA均线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_ma5",
              type: "line",
            },
            {
              id: "plot_ma8",
              type: "line",
            },
            {
              id: "plot_ma10",
              type: "line",
            },
            {
              id: "plot_ma13",
              type: "line",
            },
            {
              id: "plot_ma20",
              type: "line",
            },
            {
              id: "plot_ma21",
              type: "line",
            },
            {
              id: "plot_ma30",
              type: "line",
            },
            {
              id: "plot_ma34",
              type: "line",
            },
            {
              id: "plot_ma55",
              type: "line",
            },
            {
              id: "plot_ma60",
              type: "line",
            },
            {
              id: "plot_ma89",
              type: "line",
            },
            {
              id: "plot_ma120",
              type: "line",
            },
            {
              id: "plot_ma144",
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
                color: "#FFFFFF", // 白色 - 5日均线
              },
              plot_ma8: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFD700", // 金色 - 8日均线
              },
              plot_ma10: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFA500", // 橙色 - 10日均线
              },
              plot_ma13: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFF00", // 黄色 - 13日均线
              },
              plot_ma20: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF69B4", // 粉红色 - 20日均线
              },
              plot_ma21: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF1493", // 深粉色 - 21日均线
              },
              plot_ma30: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF0000", // 红色 - 30日均线
              },
              plot_ma34: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF6B6B", // 浅红色 - 34日均线
              },
              plot_ma55: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#00FF00", // 绿色 - 55日均线
              },
              plot_ma60: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#32CD32", // 酸橙绿 - 60日均线
              },
              plot_ma89: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#00FFFF", // 青色 - 89日均线
              },
              plot_ma120: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#4ECDC4", // 青蓝色 - 120日均线
              },
              plot_ma144: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#0080FF", // 蓝色 - 144日均线
              },
              plot_ma250: {
                linestyle: 0,
                linewidth: 4,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#8A2BE2", // 蓝紫色 - 250日均线
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
            plot_ma8: {
              title: "MA8",
              histogramBase: 0,
            },
            plot_ma10: {
              title: "MA10",
              histogramBase: 0,
            },
            plot_ma13: {
              title: "MA13",
              histogramBase: 0,
            },
            plot_ma20: {
              title: "MA20",
              histogramBase: 0,
            },
            plot_ma21: {
              title: "MA21",
              histogramBase: 0,
            },
            plot_ma30: {
              title: "MA30",
              histogramBase: 0,
            },
            plot_ma34: {
              title: "MA34",
              histogramBase: 0,
            },
            plot_ma55: {
              title: "MA55",
              histogramBase: 0,
            },
            plot_ma60: {
              title: "MA60",
              histogramBase: 0,
            },
            plot_ma89: {
              title: "MA89",
              histogramBase: 0,
            },
            plot_ma120: {
              title: "MA120",
              histogramBase: 0,
            },
            plot_ma144: {
              title: "MA144",
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
            const ma8 = PineJS.Std.sma(c, 8, this._context);
            const ma10 = PineJS.Std.sma(c, 10, this._context);
            const ma13 = PineJS.Std.sma(c, 13, this._context);
            const ma20 = PineJS.Std.sma(c, 20, this._context);
            const ma21 = PineJS.Std.sma(c, 21, this._context);
            const ma30 = PineJS.Std.sma(c, 30, this._context);
            const ma34 = PineJS.Std.sma(c, 34, this._context);
            const ma55 = PineJS.Std.sma(c, 55, this._context);
            const ma60 = PineJS.Std.sma(c, 60, this._context);
            const ma89 = PineJS.Std.sma(c, 89, this._context);
            const ma120 = PineJS.Std.sma(c, 120, this._context);
            const ma144 = PineJS.Std.sma(c, 144, this._context);
            const ma250 = PineJS.Std.sma(c, 250, this._context);

            return [
              ma5, // 0: 5日均线
              ma8, // 1: 8日均线
              ma10, // 2: 10日均线
              ma13, // 3: 13日均线
              ma20, // 4: 20日均线
              ma21, // 5: 21日均线
              ma30, // 6: 30日均线
              ma34, // 7: 34日均线
              ma55, // 8: 55日均线
              ma60, // 9: 60日均线
              ma89, // 10: 89日均线
              ma120, // 11: 120日均线
              ma144, // 12: 144日均线
              ma250, // 13: 250日均线
            ];
          };
        },
      };
    },
  };
})();
