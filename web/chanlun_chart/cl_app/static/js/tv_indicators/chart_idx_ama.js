var TvIdxAMA = (function () {
  return {
    idx: function (PineJS) {
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
  };
})();
