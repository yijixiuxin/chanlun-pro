var TvIdxPinbar = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "K Pinbar",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsPINBAR@tv-basicstudies-1",
          description: "K线Pinbar形态识别：锤子线与流星线",
          shortDescription: "K Pinbar",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_bullish_pinbar",
              type: "shapes",
            },
            {
              id: "plot_bearish_pinbar",
              type: "shapes",
            },
          ],
          defaults: {
            styles: {
              plot_bullish_pinbar: {
                plottype: "shape_arrow_up",
                location: "BelowBar",
                color: "#FF0000", // RED - 看涨Pinbar
                size: "normal",
              },
              plot_bearish_pinbar: {
                plottype: "shape_arrow_down",
                location: "AboveBar",
                color: "#00FF00", // GREEN - 看跌Pinbar
                size: "normal",
              },
            },
            inputs: {
              body_wick_ratio: 2.0,
              reverse_wick_ratio: 0.3,
              use_trend_filter: false,
            },
          },
          styles: {
            plot_bullish_pinbar: {
              title: "看涨Pinbar (锤子线)",
              size: "normal",
            },
            plot_bearish_pinbar: {
              title: "看跌Pinbar (流星线)",
              size: "normal",
            },
          },
          inputs: [
            {
              id: "body_wick_ratio",
              name: "影线/实体最小倍数",
              type: "float",
              defval: 2.0,
              min: 1.0,
              max: 10.0,
              step: 0.1,
            },
            {
              id: "reverse_wick_ratio",
              name: "反向影线/实体最大比例",
              type: "float",
              defval: 0.3,
              min: 0.0,
              max: 1.0,
              step: 0.05,
            },
            {
              id: "use_trend_filter",
              name: "启用趋势过滤",
              type: "bool",
              defval: false,
            },
          ],
          format: {
            type: "price",
            precision: 4,
          },
        },
        constructor: function () {
          this.init = function () {};
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            const bodyWickRatio = this._input(0);
            const reverseWickRatio = this._input(1);
            const useTrendFilter = this._input(2);

            const o = this._context.new_var(PineJS.Std.open(this._context));
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // 参照 chart_idx_ma.js 方式，无条件调用 sma (不可放在条件分支内)
            const sma20 = PineJS.Std.sma(c, 20, this._context);

            // K线实体 (绝对值)
            const body = Math.abs(c.get(0) - o.get(0));
            // 上影线
            const upperWick = h.get(0) - Math.max(o.get(0), c.get(0));
            // 下影线
            const lowerWick = Math.min(o.get(0), c.get(0)) - l.get(0);

            var bullishSignal = NaN;
            var bearishSignal = NaN;

            // 实体不能为0 (避免除零)
            if (body > 0) {
              // 看涨Pinbar (锤子线): 下影线长, 上影线短
              var isBullishPinbar =
                lowerWick >= bodyWickRatio * body &&
                upperWick <= reverseWickRatio * body;

              // 看跌Pinbar (流星线): 上影线长, 下影线短
              var isBearishPinbar =
                upperWick >= bodyWickRatio * body &&
                lowerWick <= reverseWickRatio * body;

              if (isBullishPinbar) {
                if (useTrendFilter) {
                  // 看涨Pinbar需要价格在均线下方 (下跌趋势末端)
                  if (!PineJS.Std.na(sma20) && c.get(0) < sma20) {
                    bullishSignal = l.get(0);
                  }
                } else {
                  bullishSignal = l.get(0);
                }
              }

              if (isBearishPinbar) {
                if (useTrendFilter) {
                  // 看跌Pinbar需要价格在均线上方 (上涨趋势末端)
                  if (!PineJS.Std.na(sma20) && c.get(0) > sma20) {
                    bearishSignal = h.get(0);
                  }
                } else {
                  bearishSignal = h.get(0);
                }
              }
            }

            return [bullishSignal, bearishSignal];
          };
        },
      };
    },
  };
})();
