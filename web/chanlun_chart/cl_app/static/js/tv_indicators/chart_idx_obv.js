var TvIdxOBV = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "Custom OBV",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsOBV@tv-basicstudies-1",
          description: "OBV",
          shortDescription: "OBV 双线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_obv",
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
              plot_obv: {
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
              ma_length: 20,
            },
          },
          palettes: {},
          styles: {
            plot_obv: {
              title: "OBV",
              histogramBase: 0,
            },
            plot_ma: {
              title: "MA",
              histogramBase: 0,
            },
          },
          inputs: [
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
            // 初始化 OBV 累加值
            this._context.obv_val = 0;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            var ma_length = this._input(0);

            var c = this._context.new_var(PineJS.Std.close(this._context));
            var v = this._context.new_var(PineJS.Std.volume(this._context));
            
            // 计算当前 bar 的变化
            // 注意：c.get(1) 在第一个点可能是 NaN
            var curr_close = c.get(0);
            var prev_close = c.get(1);
            var curr_vol = v.get(0);

            if (!isNaN(prev_close) && !isNaN(curr_close) && !isNaN(curr_vol)) {
                if (curr_close > prev_close) {
                    this._context.obv_val += curr_vol;
                } else if (curr_close < prev_close) {
                    this._context.obv_val -= curr_vol;
                }
                // 如果相等，obv_val 不变
            } else {
                // 如果是第一个点，或者数据缺失，可以初始化为0或者当前vol
                // 这里保持为0，或者重置为0（如果需要）
                // 如果是第0个点，prev_close是NaN，obv_val保持0
            }

            // 将计算出的 OBV 值转为 Series，以便计算 MA
            var obv_series = this._context.new_var(this._context.obv_val);
            
            // 计算 MA
            var obv_ma_val = PineJS.Std.sma(obv_series, ma_length, this._context);
            var obv_ma = this._context.new_var(obv_ma_val);

            return [obv_series.get(0), obv_ma.get(0)];
          };
        },
      };
    },
  };
})();
