var TvIdxROC = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom ROC",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsROC@tv-basicstudies-1",
          description: "ROC",
          shortDescription: "ROC 双线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_roc",
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
              plot_roc: {
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
                color: "#FF9800",
              },
            },
            inputs: {
              length: 12,
              use_ma: true, // Switch between MA and Slow ROC?
              // User said: "ROC double line needs to support both slow line and moving average line (e.g. 6) switchable"
              // So we need a boolean input.
              // In TV JS, boolean input? 'bool' type.
              // And parameters for both.
              ma_length: 6,
              slow_length: 25,
            },
          },
          palettes: {},
          styles: {
            plot_roc: {
              title: "ROC",
              histogramBase: 0,
            },
            plot_signal: {
              title: "Signal / Slow ROC",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "length",
              name: "Length",
              type: "integer",
              defval: 12,
              min: 1,
              max: 200,
            },
            {
              id: "use_ma",
              name: "Use MA as Signal? (Uncheck for Slow ROC)",
              type: "bool",
              defval: true,
            },
            {
              id: "ma_length",
              name: "MA Length",
              type: "integer",
              defval: 6,
              min: 1,
              max: 200,
            },
            {
              id: "slow_length",
              name: "Slow ROC Length",
              type: "integer",
              defval: 25,
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
            var use_ma = this._input(1);
            var ma_length = this._input(2);
            var slow_length = this._input(3);

            var c = this._context.new_var(PineJS.Std.close(this._context));
            var roc = this._context.new_var(PineJS.Std.roc(c, length, this._context));
            
            var signal;
            if (use_ma) {
                signal = this._context.new_var(PineJS.Std.sma(roc, ma_length, this._context));
            } else {
                signal = this._context.new_var(PineJS.Std.roc(c, slow_length, this._context));
            }

            return [roc.get(0), signal.get(0)];
          };
        },
      };
    },
  };
})();
