var TvIdxFCX = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "发财线",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsHMA@tv-basicstudies-1",
          description: "东@发财线",
          shortDescription: "发财线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_hma",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_hma: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF8D1E",
              },
            },
            inputs: {
              N: 88,
            },
          },
          styles: {
            plot_hma: {
              title: "HMA",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "N",
              name: "周期",
              type: "integer",
              defval: 88,
              min: 1,
              max: 500,
            },
          ],
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

            var N = this._input(0);
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // N:=88;
            // WMA1:=WMA(C,N/2);
            const wma1 = this._context.new_var(
              PineJS.Std.wma(c, Math.floor(N / 2), this._context)
            );

            // WMA2:= WMA(C,N);
            const wma2 = this._context.new_var(
              PineJS.Std.wma(c, N, this._context)
            );

            // TEMP:= 2*WMA1-WMA2;
            const temp = this._context.new_var(2 * wma1.get(0) - wma2.get(0));

            // HMA:WMA(TEMP, SQRT(N))
            const hma = PineJS.Std.wma(
              temp,
              Math.floor(Math.sqrt(N)),
              this._context
            );

            return [hma];
          };
        },
      };
    },
  };
})();
