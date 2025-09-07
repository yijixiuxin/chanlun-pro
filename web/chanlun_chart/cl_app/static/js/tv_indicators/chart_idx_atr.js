var TvIdxATR = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Price ATR",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsATR@tv-basicstudies-1",
          description: "Price ATR",
          shortDescription: "ATR 价格区间",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_atr_up",
              type: "line",
            },
            {
              id: "plot_atr_down",
              type: "line",
            },
          ],
          defaults: {
            palettes: {
              paletteId1: {
                colors: {
                  0: {
                    color: "red",
                    width: 1,
                    style: 0,
                  },
                  1: {
                    color: "blue",
                    width: 1,
                    style: 0,
                  },
                },
              },
            },
            styles: {
              plot_atr_up: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFCC33",
              },
              plot_atr_down: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#33CCFF",
              },
            },
            inputs: {
              ATR: 14,
              BS: 2,
              N: 0,
            },
          },
          palettes: {
            paletteId1: {
              colors: {
                0: {
                  name: "First color",
                },
                1: {
                  name: "Second color",
                },
              },
            },
          },
          styles: {
            plot_atr_up: {
              title: "ATR UP",
              histogramBase: 0,
            },
            plot_atr_down: {
              title: "ATR DOWN",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "ATR",
              name: "ATR",
              type: "integer",
              defval: 14,
              min: 1,
              max: 100,
            },
            {
              id: "BS",
              name: "BS",
              type: "integer",
              defval: 2,
              min: 1,
              max: 100,
            },
            {
              id: "N",
              name: "N",
              type: "integer",
              defval: 0,
              min: 0,
              max: 100,
            },
          ],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            context.amas = [];
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            const ATR_LENGTH = this._input(0);
            const BS = this._input(1);
            const N = this._input(2);

            const atr_vals = this._context.new_var(
              PineJS.Std.atr(ATR_LENGTH, this._context)
            );

            const c = this._context.new_var(PineJS.Std.close(this._context));

            if (N == 0) {
              const atr_up_val = c + atr_vals * BS;
              const atr_down_val = c - atr_vals * BS;

              return [atr_up_val, atr_down_val];
            } else {
              const atr_max = PineJS.Std.highest(atr_vals, N, this._context);
              const atr_min = PineJS.Std.lowest(atr_vals, N, this._context);

              const c_max = PineJS.Std.highest(c, N, this._context);
              const c_low = PineJS.Std.lowest(c, N, this._context);

              const atr_up_val = c_max + atr_max * BS;
              const atr_down_val = c_low - atr_min * BS;
              return [atr_up_val, atr_down_val];
            }
          };
        },
      };
    },
  };
})();
