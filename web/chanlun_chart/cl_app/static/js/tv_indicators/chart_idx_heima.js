var TvIdxHeima = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "黑马",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsHEIMA@tv-basicstudies-1",
          description: "东@黑马",
          shortDescription: "黑马",
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
              id: "plot_buy_signal",
              type: "shapes",
            },
            {
              id: "plot_heima_signal",
              type: "shapes",
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
                color: "#FFBB00", // COLORFFBB00
              },
              plot_d: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF77FF", // COLORFF77FF
              },
              plot_buy_signal: {
                plottype: "shape_flag",
                location: "Bottom",
                color: "#FFFF00", // COLORYELLOW
                size: "large",
              },
              plot_heima_signal: {
                plottype: "shape_flag",
                location: "Bottom",
                color: "#FF0000", // COLORRED
                size: "normal",
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
              title: "K线",
              histogramBase: 0,
            },
            plot_d: {
              title: "D线",
              histogramBase: 0,
            },
            plot_buy_signal: {
              title: "掘底买点",
              size: "large",
            },
            plot_heima_signal: {
              title: "黑马信号",
              size: "normal",
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
          this.init = function (context, inputCallback) {
            // 初始化ZIG变量的存储
            context.zigValues = [];
            context.prevZigValue = NaN;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            var N = this._input(0);
            var M1 = this._input(1);
            var M2 = this._input(2);

            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // RSV计算
            const hh = PineJS.Std.highest(h, N, this._context);
            const ll = PineJS.Std.lowest(l, N, this._context);
            const rsv = this._context.new_var(
              ((c.get(0) - ll) / (hh - ll)) * 100
            );

            // K和D计算 - 使用SMA(X,N,M)公式，等同于EMA中权重为M/N
            const k = this._context.new_var(
              PineJS.Std.ema(rsv, (2 * M1) / 1 - 1, this._context)
            );
            const d = this._context.new_var(
              PineJS.Std.ema(k, (2 * M2) / 1 - 1, this._context)
            );

            // VAR1计算：(HIGH+LOW+CLOSE)/3
            const var1 = this._context.new_var(
              (h.get(0) + l.get(0) + c.get(0)) / 3
            );

            // VAR2计算：(VAR1-MA(VAR1,14))/(0.015*AVEDEV(VAR1,14))
            const ma_var1_14 = PineJS.Std.sma(var1, 14, this._context);
            // 计算平均绝对偏差 AVEDEV
            let sum_abs_dev = 0;
            for (let i = 0; i < 14; i++) {
              const var1_i = var1.get(i);
              if (!PineJS.Std.na(var1_i)) {
                sum_abs_dev += Math.abs(var1_i - ma_var1_14);
              }
            }
            const avedev_var1_14 = sum_abs_dev / 14;
            const var2 = this._context.new_var(
              avedev_var1_14 !== 0
                ? (var1.get(0) - ma_var1_14) / (0.015 * avedev_var1_14)
                : 0
            );

            // VAR3计算：IF(TROUGHBARS(3,16,1)=0 AND HIGH>LOW+0.04,80,0)
            // 简化实现：检查是否为近期低点且有足够波动
            const ll_3 = PineJS.Std.lowest(l, 3, this._context);
            const var3 = this._context.new_var(
              l.get(0) === ll_3 && h.get(0) > l.get(0) + 0.04 ? 80 : 0
            );

            // VAR4计算：简化ZIG函数实现
            // 这里使用简化的趋势判断替代复杂的ZIG函数
            const ma_short = PineJS.Std.sma(c, 3, this._context);
            const ma_long = PineJS.Std.sma(c, 22, this._context);
            const ma_short_1 = this._context.new_var(ma_short).get(1);
            const ma_short_2 = this._context.new_var(ma_short).get(2);
            const ma_short_3 = this._context.new_var(ma_short).get(3);

            const var4 = this._context.new_var(
              ma_short > ma_short_1 &&
                ma_short_1 <= ma_short_2 &&
                ma_short_2 <= ma_short_3 &&
                ma_short > ma_long
                ? 50
                : 0
            );

            // 信号判断
            // 掘底买点：VAR2<-110 AND VAR3>0
            const buy_signal = var2.get(0) < -110 && var3.get(0) > 0 ? 87 : NaN;

            // 黑马信号：VAR2<-110 AND VAR4>0
            const heima_signal =
              var2.get(0) < -110 && var4.get(0) > 0 ? 57 : NaN;

            return [
              k.get(0), // K线
              d.get(0), // D线
              buy_signal, // 掘底买点信号
              heima_signal, // 黑马信号
            ];
          };
        },
      };
    },
  };
})();
