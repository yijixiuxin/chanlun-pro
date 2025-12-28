var TvIdxZhixing = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "知行指标",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsZhixing@tv-basicstudies-1",
          description: "知行指标",
          shortDescription: "知行指标",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_short_trend",
              type: "line",
            },
            {
              id: "plot_long_short",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_short_trend: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFFFF", // 白色 - 短期趋势线
              },
              plot_long_short: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFF00", // 黄色 - 多空线
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_short_trend: {
              title: "短期趋势线",
              histogramBase: 0,
            },
            plot_long_short: {
              title: "多空线",
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

            // 计算短期趋势线: EMA(EMA(C,10),10)
            const ema10 = PineJS.Std.ema(c, 10, this._context);
            const ema10_series = this._context.new_var(ema10);
            const short_trend = PineJS.Std.ema(ema10_series, 10, this._context);

            // 计算多空线: (MA(CLOSE,14)+MA(CLOSE,28)+MA(CLOSE,57)+MA(CLOSE,114))/4
            const ma14 = PineJS.Std.sma(c, 14, this._context);
            const ma28 = PineJS.Std.sma(c, 28, this._context);
            const ma57 = PineJS.Std.sma(c, 57, this._context);
            const ma114 = PineJS.Std.sma(c, 114, this._context);
            const long_short = (ma14 + ma28 + ma57 + ma114) / 4;

            return [
              short_trend, // 0: 短期趋势线
              long_short, // 1: 多空线
            ];
          };
        },
      };
    },
  };
})();
