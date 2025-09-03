var TvIdxLTQS = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "龙头趋势",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsLTQS@tv-basicstudies-1",
          description: "东@龙头趋势",
          shortDescription: "龙头趋势",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_ltqs",
              type: "line",
            },
            {
              id: "plot_up_stick",
              type: "histogram",
            },
            {
              id: "plot_down_stick",
              type: "histogram",
            },
            {
              id: "plot_zero_line",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_ltqs: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false, // NODRAW
                color: "#CC33FF",
              },
              plot_up_stick: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FF05F8", // COLORFF05F8
                linewidth: 3,
                histogramBase: 0,
              },
              plot_down_stick: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FF05F8", // COLORFF05F8
                linewidth: 3,
                histogramBase: 0,
              },
              plot_zero_line: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#808080", // 灰色参考线
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_ltqs: {
              title: "龙头趋势",
              histogramBase: 0,
            },
            plot_up_stick: {
              title: "上升柱",
              histogramBase: 0,
            },
            plot_down_stick: {
              title: "下降柱",
              histogramBase: 0,
            },
            plot_zero_line: {
              title: "零轴线",
              histogramBase: 0,
            },
          },
          inputs: [],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            // 初始化
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取收盘价
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // VAR1:=EMA(EMA(CLOSE,9),9);
            const ema1 = this._context.new_var(
              PineJS.Std.ema(c, 9, this._context)
            );
            const var1 = this._context.new_var(
              PineJS.Std.ema(ema1, 9, this._context)
            );

            // VAR2:=(VAR1-REF(VAR1,1))/REF(VAR1,1)*1000;
            const var1_ref1 = var1.get(1); // REF(VAR1,1)
            const var2 = this._context.new_var(
              var1_ref1 !== 0
                ? ((var1.get(0) - var1_ref1) / var1_ref1) * 1000
                : 0
            );

            // 龙头趋势:VAR2,NODRAW;
            const ltqs = var2.get(0);

            // COND:=CROSS(VAR2,0);
            const var2_prev = var2.get(1);
            const cross_zero = var2_prev < 0 && var2.get(0) >= 0 ? 1 : 0;

            // STICKLINE条件判断
            const var2_current = var2.get(0);
            const var2_ref = var2.get(1); // REF(VAR2,1)

            // STICKLINE(VAR2>REF(VAR2,1) AND VAR2>0,VAR2,0,0.5,0),COLORFF05F8;
            const up_stick =
              var2_current > var2_ref && var2_current > 0 ? var2_current : NaN;

            // STICKLINE(VAR2<REF(VAR2,1) AND VAR2>0,VAR2,0,0.5,0),COLORFF05F8;
            const down_stick =
              var2_current < var2_ref && var2_current > 0 ? var2_current : NaN;

            // 零轴线
            const zero_line = 0;

            return [
              ltqs, // 0: 龙头趋势线（不显示）
              up_stick, // 1: 上升柱状图
              down_stick, // 2: 下降柱状图
              zero_line, // 3: 零轴参考线
            ];
          };
        },
      };
    },
  };
})();
