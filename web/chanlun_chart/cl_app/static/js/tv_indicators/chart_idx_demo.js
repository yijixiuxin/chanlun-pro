var TvIdxDemo = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom Indicators DEMO",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsDemo@tv-basicstudies-1",
          description: "自定义指标示例",
          shortDescription: "自定义指标示例",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_0",
              type: "line",
            },
          ],
          palettes: {
            paletteId1: {
              colors: {
                0: {
                  name: "First color",
                },
              },
            },
          },
          defaults: {
            palettes: {
              paletteId1: {
                colors: {
                  0: {
                    color: "red",
                    width: 1,
                    style: 0,
                  },
                },
              },
            },
            styles: {},
            precision: 4,
            inputs: {},
          },
          styles: {
            plot_0: {
              title: "Equity value",
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
            this._highs = [];
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            const h = this._context.new_var(PineJS.Std.high(this._context));
            const high_val = PineJS.Std.highest(h, 20, this._context);
            // console.log(high_val);

            return [high_val];
          };
        },
      };
    },
  };
})();
