var TvIdxKDJ = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom KDJ",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsKDJ@tv-basicstudies-1",
          description: "KDJ",
          shortDescription: "KDJ 随机指标",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_k",
              type: "line",
            },
            {
              id: "plot_d",
              type: "line",
            },
            {
              id: "plot_j",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_k: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFCC33",
              },
              plot_d: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#33CCFF",
              },
              plot_j: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF3366",
              },
            },
            inputs: {
              N: 9,
              M1: 3,
              M2: 3,
            },
          },
          palettes: {},
          styles: {
            plot_k: {
              title: "K",
              histogramBase: 0,
            },
            plot_d: {
              title: "D",
              histogramBase: 0,
            },
            plot_j: {
              title: "J",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "N",
              name: "N",
              type: "integer",
              defval: 9,
              min: 1,
              max: 100,
            },
            {
              id: "M1",
              name: "M1",
              type: "integer",
              defval: 3,
              min: 1,
              max: 100,
            },
            {
              id: "M2",
              name: "M2",
              type: "integer",
              defval: 3,
              min: 1,
              max: 100,
            },
          ],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {};
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            var N = this._input(0);
            var M1 = this._input(1);
            var M2 = this._input(2);

            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            const hh = PineJS.Std.highest(h, N, this._context);
            const ll = PineJS.Std.lowest(l, N, this._context);
            const rsv = this._context.new_var(((c - ll) / (hh - ll)) * 100);

            const k = this._context.new_var(
              PineJS.Std.ema(rsv, M1 * 2 - 1, this._context)
            );
            const d = this._context.new_var(
              PineJS.Std.ema(k, M2 * 2 - 1, this._context)
            );
            const j = this._context.new_var(3 * k - 2 * d);

            return [k.get(0), d.get(0), j.get(0)];
          };
        },
      };
    },
  };
})();
