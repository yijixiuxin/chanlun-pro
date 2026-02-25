var TvIdxCCI = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom CCI",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsCCI@tv-basicstudies-1",
          description: "CCI",
          shortDescription: "CCI 双线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_fast",
              type: "line",
            },
            {
              id: "plot_slow",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_fast: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#2196F3",
              },
              plot_slow: {
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
              fast_length: 14,
              slow_length: 40,
            },
          },
          palettes: {},
          styles: {
            plot_fast: {
              title: "Fast CCI",
              histogramBase: 0,
            },
            plot_slow: {
              title: "Slow CCI",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "fast_length",
              name: "Fast Length",
              type: "integer",
              defval: 14,
              min: 1,
              max: 200,
            },
            {
              id: "slow_length",
              name: "Slow Length",
              type: "integer",
              defval: 40,
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

            var fast_length = this._input(0);
            var slow_length = this._input(1);

            // 1. 计算 Typical Price (TP)
            var h = this._context.new_var(PineJS.Std.high(this._context));
            var l = this._context.new_var(PineJS.Std.low(this._context));
            var c = this._context.new_var(PineJS.Std.close(this._context));
            
            var tp_val = (h.get(0) + l.get(0) + c.get(0)) / 3;
            var tp = this._context.new_var(tp_val);

            // 2. 辅助函数：计算 CCI
            // CCI = (TP - SMA(TP, N)) / (0.015 * MeanDeviation)
            // MeanDeviation = SMA(Abs(TP - SMA(TP, N)), N)
            
            function calculateCCI(tp_series, length, ctx) {
                // SMA of TP
                var ma_tp_val = PineJS.Std.sma(tp_series, length, ctx);
                // 这里我们得到的是数值。但是为了计算 Mean Deviation，我们需要 Series 吗？
                // MD = SMA(Abs(TP - MA_TP))
                // Abs(TP - MA_TP) 这个序列中的每一项，是当时的 TP 减去当时的 MA_TP。
                // 所以我们需要构建一个 AbsDev 序列。
                
                var abs_dev_val = Math.abs(tp_series.get(0) - ma_tp_val);
                var abs_dev = ctx.new_var(abs_dev_val);
                
                var md_val = PineJS.Std.sma(abs_dev, length, ctx);
                
                if (md_val === 0) return 0; // 避免除以0
                return (tp_series.get(0) - ma_tp_val) / (0.015 * md_val);
            }

            var cci_fast_val = calculateCCI(tp, fast_length, this._context);
            var cci_slow_val = calculateCCI(tp, slow_length, this._context);

            var cci_fast = this._context.new_var(cci_fast_val);
            var cci_slow = this._context.new_var(cci_slow_val);

            return [cci_fast.get(0), cci_slow.get(0)];
          };
        },
      };
    },
  };
})();
