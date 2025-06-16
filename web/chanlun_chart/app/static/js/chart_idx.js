var TvIdx = (function () {
  return {
    idx_demo: function (PineJS) {
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
    idx_kdj: function (PineJS) {
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
    idx_ama: function (PineJS) {
      return {
        name: "Custom AMA",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsAMA@tv-basicstudies-1",
          description: "AMA",
          shortDescription: "AMA 自适应移动均线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_ama",
              type: "line",
            },
            {
              id: "plot_1",
              type: "colorer",
              target: "plot_ama",
              palette: "paletteId1",
            },
          ],
          defaults: {
            palettes: {
              paletteId1: {
                colors: {
                  0: {
                    color: "red",
                    width: 2,
                    style: 0,
                  },
                  1: {
                    color: "blue",
                    width: 2,
                    style: 0,
                  },
                },
              },
            },
            styles: {
              plot_ama: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFCC33",
              },
            },
            inputs: {
              N: 10,
              fast_n: 2,
              slow_n: 30,
              cal_type: 0,
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
            plot_ama: {
              title: "AMA",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "N",
              name: "N",
              type: "integer",
              defval: 10,
              min: 1,
              max: 100,
            },
            {
              id: "fast_n",
              name: "Fast n",
              type: "integer",
              defval: 2,
              min: 1,
              max: 100,
            },
            {
              id: "slow_n",
              name: "Slow n",
              type: "integer",
              defval: 30,
              min: 1,
              max: 100,
            },
            {
              id: "cal_type",
              name: "计算方式",
              type: "integer",
              defval: 0,
              min: 0,
              max: 1,
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

            var N = this._input(0);
            var fast_n = this._input(1);
            var slow_n = this._input(2);
            var cal_type = this._input(3);
            // console.log(
            //   "N",
            //   N,
            //   "fast_n",
            //   fast_n,
            //   "slow_n",
            //   slow_n,
            //   "cal_type",
            //   cal_type
            // );

            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // 原始的计算公式
            let direction = NaN;
            let volatility = NaN;
            if (cal_type === 0) {
              // direction = math.abs(close - close[n])
              // volatility = math.sum(math.abs(close - close[1]), n)
              direction = PineJS.Std.abs(c - c.get(N));
              const close_diff = this._context.new_var(
                PineJS.Std.abs(c - c.get(1))
              );
              volatility = PineJS.Std.sum(close_diff, N, this._context);
            } else {
              // 改进后的计算公式
              // direction = 周期内的最高价 - 最低价
              // volatility = 周期内的 TR 的和
              direction =
                PineJS.Std.highest(h, N, this._context) -
                PineJS.Std.lowest(l, N, this._context);
              const trs = this._context.new_var(
                PineJS.Std.tr(true, this._context)
              );
              volatility = PineJS.Std.sum(trs, N, this._context);
            }

            // ER = direction / volatility
            const er = direction / volatility;
            // sc = math.pow(ER * (fastest - slowest) + slowest, 2)
            const sc = Math.pow(
              er * (2 / (fast_n + 1) - 2 / (slow_n + 1)) + 2 / (slow_n + 1),
              2
            );

            if (PineJS.Std.na(sc)) {
              this._context.amas.push(c.get(0));
            } else {
              // ama.get(1) + sc * (c.get(0) - ama.get(1)));
              this._context.amas.push(
                this._context.amas[this._context.amas.length - 1] +
                  sc *
                    (c.get(0) -
                      this._context.amas[this._context.amas.length - 1])
              );
            }

            // 颜色设置
            const colorIndex =
              this._context.amas.length >= 2 &&
              this._context.amas[this._context.amas.length - 1] >
                this._context.amas[this._context.amas.length - 2]
                ? 0
                : 1;

            // 返回 this._context.amas 数组最后一个
            return [
              this._context.amas[this._context.amas.length - 1],
              colorIndex,
            ];
          };
        },
      };
    },
    idx_atr: function (PineJS) {
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
