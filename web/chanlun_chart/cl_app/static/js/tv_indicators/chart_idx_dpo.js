var TvIdxDPO = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom DPO",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsDPO@tv-basicstudies-1",
          description: "DPO",
          shortDescription: "DPO 双线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_dpo",
              type: "line",
            },
            {
              id: "plot_signal",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_dpo: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#2196F3",
              },
              plot_signal: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF5252",
              },
            },
            inputs: {
              length: 21,
              signal_length: 6,
            },
          },
          palettes: {},
          styles: {
            plot_dpo: {
              title: "DPO",
              histogramBase: 0,
            },
            plot_signal: {
              title: "Signal",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "length",
              name: "Length",
              type: "integer",
              defval: 21,
              min: 1,
              max: 200,
            },
            {
              id: "signal_length",
              name: "Signal Length",
              type: "integer",
              defval: 6,
              min: 1,
              max: 200,
            },
          ],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            var length = this._input(0);
            var signal_length = this._input(1);

            var c = this._context.new_var(PineJS.Std.close(this._context));
            var sma = this._context.new_var(PineJS.Std.sma(c, length, this._context));
            
            // DPO = close - sma[length/2 + 1]
            var shift = Math.floor(length / 2) + 1;
            var dpo_val = c.get(0) - sma.get(shift);
            
            // Handle NaN at the beginning
            if (isNaN(dpo_val)) {
                dpo_val = 0;
            }

            var dpo = this._context.new_var(dpo_val);
            var signal = this._context.new_var(PineJS.Std.sma(dpo, signal_length, this._context));

            return [dpo.get(0), signal.get(0)];
          };
        },
      };
    },
  };
})();
