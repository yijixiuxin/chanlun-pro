var TvIdxASI = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom ASI",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsASI@tv-basicstudies-1",
          description: "ASI",
          shortDescription: "ASI 双线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_asi",
              type: "line",
            },
            {
              id: "plot_ma",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_asi: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#2196F3",
              },
              plot_ma: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF9800",
              },
            },
            inputs: {
              limit_move: 0,
              ma_length: 20,
            },
          },
          palettes: {},
          styles: {
            plot_asi: {
              title: "ASI",
              histogramBase: 0,
            },
            plot_ma: {
              title: "MA",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "limit_move",
              name: "Limit Move (0 for auto)",
              type: "float",
              defval: 0,
            },
            {
              id: "ma_length",
              name: "MA Length",
              type: "integer",
              defval: 20,
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

            var limit_move = this._input(0);
            var ma_length = this._input(1);

            var o = this._context.new_var(PineJS.Std.open(this._context));
            var h = this._context.new_var(PineJS.Std.high(this._context));
            var l = this._context.new_var(PineJS.Std.low(this._context));
            var c = this._context.new_var(PineJS.Std.close(this._context));
            
            // Current bar values
            var curr_o = o.get(0);
            var curr_h = h.get(0);
            var curr_l = l.get(0);
            var curr_c = c.get(0);
            
            // Previous bar values
            var prev_o = o.get(1);
            var prev_c = c.get(1);
            
            var si = 0;
            
            if (!isNaN(prev_c) && !isNaN(prev_o)) {
                var limit = limit_move;
                if (limit <= 0) {
                    limit = prev_c * 0.1;
                }
                
                var A = Math.abs(curr_h - prev_c);
                var B = Math.abs(curr_l - prev_c);
                var C = Math.abs(curr_h - curr_l);
                var D = Math.abs(prev_c - prev_o);
                
                var R = 0;
                var K = Math.max(A, B);
                if (A >= Math.max(B, C)) {
                    R = A - 0.5 * B + 0.25 * D;
                } else if (B >= Math.max(A, C)) {
                    R = B - 0.5 * A + 0.25 * D;
                } else {
                    R = C + 0.25 * D;
                }

                var X = (curr_c - prev_c) + 0.5 * (curr_c - curr_o) + 0.25 * (prev_c - prev_o);
                
                if (R !== 0 && limit !== 0) {
                     si = 50 * (X / R) * (K / limit);
                }
            }
            
            var asi_val = PineJS.Std.cum(si, this._context);
            var asi_series = this._context.new_var(asi_val);
            var asi_ma = this._context.new_var(
              PineJS.Std.sma(asi_series, ma_length, this._context)
            );

            return [asi_val, asi_ma.get(0)];
          };
        },
      };
    },
  };
})();
