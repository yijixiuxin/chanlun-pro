var TvIdxHLFTX = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "弘历飞天线",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsHLFTX@tv-basicstudies-1",
          description: "东@弘历飞天线",
          shortDescription: "弘历飞天线",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_over_bought",
              type: "line",
            },
            {
              id: "plot_over_sold",
              type: "line",
            },
            {
              id: "plot_zero_line",
              type: "line",
            },
            {
              id: "plot_hlftx",
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
              plot_over_bought: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#808080", // COLORGRAY 灰色
              },
              plot_over_sold: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#808080", // COLORGRAY 灰色
              },
              plot_zero_line: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#EB09EC", // COLOREB09EC 紫色
              },
              plot_hlftx: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#03F2F2", // COLOR03F2F2 青色
              },
              plot_signal: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#D59D06", // COLORD59D06 黄色
              },
            },
            inputs: {
              N1: 45,
              N2: 12,
              N3: 26,
              N4: 9,
            },
          },
          palettes: {},
          styles: {
            plot_over_bought: {
              title: "超买线",
              histogramBase: 0,
            },
            plot_over_sold: {
              title: "超卖线",
              histogramBase: 0,
            },
            plot_zero_line: {
              title: "零轴线",
              histogramBase: 0,
            },
            plot_hlftx: {
              title: "随机MACD",
              histogramBase: 0,
            },
            plot_signal: {
              title: "信号线",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "N1",
              name: "STOCHLENGTH",
              type: "integer",
              defval: 45,
              min: 1,
              max: 200,
            },
            {
              id: "N2",
              name: "FASTLENGTH",
              type: "integer",
              defval: 12,
              min: 1,
              max: 100,
            },
            {
              id: "N3",
              name: "SLOWLENGTH",
              type: "integer",
              defval: 26,
              min: 1,
              max: 100,
            },
            {
              id: "N4",
              name: "SIGNALLENGTH",
              type: "integer",
              defval: 9,
              min: 1,
              max: 50,
            },
          ],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            // 初始化
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取输入参数
            var N1 = this._input(0); // STOCHLENGTH
            var N2 = this._input(1); // FASTLENGTH
            var N3 = this._input(2); // SLOWLENGTH
            var N4 = this._input(3); // SIGNALLENGTH

            // 获取价格和成交量数据
            const o = this._context.new_var(PineJS.Std.open(this._context));
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));
            const v = this._context.new_var(PineJS.Std.volume(this._context));

            // 成交量柱状图逻辑
            // STICKLINE(CLOSE>=OPEN,VOL,0,0.8,1),COLORFF3232;
            // STICKLINE(CLOSE<OPEN,VOL,0,0.8,0),COLOR00A843;
            const vol_up = c.get(0) >= o.get(0) ? v.get(0) : NaN;
            const vol_down = c.get(0) < o.get(0) ? v.get(0) : NaN;

            // 参考线
            const over_bought = 10;
            const over_sold = -10;
            const zero_line = 0;

            // HIGHEST HIGH OVER N1 PERIOD
            const highhv = PineJS.Std.highest(h, N1, this._context);

            // LOWEST LOW OVER N1 PERIOD
            const lowlv = PineJS.Std.lowest(l, N1, this._context);

            // EXPONENTIAL MOVING AVERAGE OF CLOSE OVER N2 PERIOD
            const fast_ma = this._context.new_var(
              PineJS.Std.ema(c, N2, this._context)
            );

            // EXPONENTIAL MOVING AVERAGE OF CLOSE OVER N3 PERIOD
            const slow_ma = this._context.new_var(
              PineJS.Std.ema(c, N3, this._context)
            );

            // STOCH_FASTMA:=IF(HIGHHV-LOWLV,(FAST_MA-LOWLV)/(HIGHHV-LOWLV),0);
            const range = highhv - lowlv;
            const stoch_fastma = this._context.new_var(
              range !== 0 ? (fast_ma.get(0) - lowlv) / range : 0
            );

            // STOCH_SLOWMA:=IF(HIGHHV-LOWLV,(SLOW_MA-LOWLV)/(HIGHHV-LOWLV),0);
            const stoch_slowma = this._context.new_var(
              range !== 0 ? (slow_ma.get(0) - lowlv) / range : 0
            );

            // STOCHASTIC_MACD:(STOCH_FASTMA-STOCH_SLOWMA)*100,COLOR03F2F2;
            const stochastic_macd = this._context.new_var(
              (stoch_fastma.get(0) - stoch_slowma.get(0)) * 100
            );

            // SIGNAL:EMA(STOCHASTIC_MACD,N4),COLORD59D06;
            const signal = this._context.new_var(
              PineJS.Std.ema(stochastic_macd, N4, this._context)
            );

            return [
              over_bought, // 2: 超买线 (10)
              over_sold, // 3: 超卖线 (-10)
              zero_line, // 4: 零轴线 (0)
              stochastic_macd.get(0), // 5: 随机MACD线
              signal.get(0), // 6: 信号线
            ];
          };
        },
      };
    },
  };
})();
