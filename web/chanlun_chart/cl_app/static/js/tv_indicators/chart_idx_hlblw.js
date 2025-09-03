var TvIdxHLBLW = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "弘历背离王",
        metainfo: {
          _metainfoVersion: 51,
          id: "CustomIndicatorsTower@tv-basicstudies-1",
          scriptIdPart: "",
          description: "东@弘历背离王",
          shortDescription: "弘历背离王",
          is_hidden_study: false,
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_towerc",
              type: "line",
            },
            {
              id: "plot_forecast_3",
              type: "line",
            },
            {
              id: "plot_forecast_4",
              type: "line",
            },
            {
              id: "plot_forecast_5",
              type: "line",
            },
            {
              id: "plot_forecast_6",
              type: "line",
            },
            {
              id: "plot_forecast_7",
              type: "line",
            },
            {
              id: "plot_forecast_8",
              type: "line",
            },
            {
              id: "plot_forecast_9",
              type: "line",
            },
            {
              id: "plot_forecast_10",
              type: "line",
            },
            {
              id: "plot_forecast_11",
              type: "line",
            },
            {
              id: "plot_forecast_12",
              type: "line",
            },
            {
              id: "plot_forecast_13",
              type: "line",
            },
            {
              id: "plot_forecast_14",
              type: "line",
            },
            {
              id: "plot_forecast_15",
              type: "line",
            },
            {
              id: "plot_forecast_16",
              type: "line",
            },
            {
              id: "plot_forecast_17",
              type: "line",
            },
            {
              id: "plot_up_open",
              type: "ohlc_open",
              target: "plotcandle_up",
            },
            {
              id: "plot_up_high",
              type: "ohlc_high",
              target: "plotcandle_up",
            },
            {
              id: "plot_up_low",
              type: "ohlc_low",
              target: "plotcandle_up",
            },
            {
              id: "plot_up_close",
              type: "ohlc_close",
              target: "plotcandle_up",
            },
            {
              id: "plot_down_open",
              type: "ohlc_open",
              target: "plotcandle_down",
            },
            {
              id: "plot_down_high",
              type: "ohlc_high",
              target: "plotcandle_down",
            },
            {
              id: "plot_down_low",
              type: "ohlc_low",
              target: "plotcandle_down",
            },
            {
              id: "plot_down_close",
              type: "ohlc_close",
              target: "plotcandle_down",
            },
          ],

          ohlcPlots: {
            plotcandle_up: {
              title: "上升蜡烛",
              isHidden: false,
            },
            plotcandle_down: {
              title: "下降蜡烛",
              isHidden: false,
            },
          },

          defaults: {
            ohlcPlots: {
              plotcandle_up: {
                borderColor: "#0CFAB1",
                color: "#0CFAB1",
                drawBorder: true,
                drawWick: true,
                plottype: "ohlc_candles",
                visible: true,
                wickColor: "#0CFAB1",
              },
              plotcandle_down: {
                borderColor: "#FF0000",
                color: "#FF0000",
                drawBorder: true,
                drawWick: true,
                plottype: "ohlc_candles",
                visible: true,
                wickColor: "#FF0000",
              },
            },
            styles: {
              plot_towerc: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: false,
                color: "#CC33FF",
              },
              plot_forecast_3: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_4: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_5: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_6: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_7: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_8: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_9: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_10: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_11: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_12: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_13: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_14: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_15: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_16: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
              plot_forecast_17: {
                linestyle: 0,
                linewidth: 2,
                plottype: 6,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#27b94d",
              },
            },
            precision: 4,
            inputs: {},
          },
          styles: {
            plot_towerc: {
              title: "TOWERC",
              histogramBase: 0,
            },
            plot_forecast_3: {
              title: "预测3",
              histogramBase: 0,
            },
            plot_forecast_4: {
              title: "预测4",
              histogramBase: 0,
            },
            plot_forecast_5: {
              title: "预测5",
              histogramBase: 0,
            },
            plot_forecast_6: {
              title: "预测6",
              histogramBase: 0,
            },
            plot_forecast_7: {
              title: "预测7",
              histogramBase: 0,
            },
            plot_forecast_8: {
              title: "预测8",
              histogramBase: 0,
            },
            plot_forecast_9: {
              title: "预测9",
              histogramBase: 0,
            },
            plot_forecast_10: {
              title: "预测10",
              histogramBase: 0,
            },
            plot_forecast_11: {
              title: "预测11",
              histogramBase: 0,
            },
            plot_forecast_12: {
              title: "预测12",
              histogramBase: 0,
            },
            plot_forecast_13: {
              title: "预测13",
              histogramBase: 0,
            },
            plot_forecast_14: {
              title: "预测14",
              histogramBase: 0,
            },
            plot_forecast_15: {
              title: "预测15",
              histogramBase: 0,
            },
            plot_forecast_16: {
              title: "预测16",
              histogramBase: 0,
            },
            plot_forecast_17: {
              title: "预测17",
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
          this.init = function (context, inputCallback) {
            // 初始化状态变量
            context.prevTowerc = NaN;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取价格数据
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // 计算A1-A5: FORCAST(EMA(CLOSE,N),6)
            const ema5 = this._context.new_var(
              PineJS.Std.ema(c, 5, this._context)
            );
            const ema8 = this._context.new_var(
              PineJS.Std.ema(c, 8, this._context)
            );
            const ema11 = this._context.new_var(
              PineJS.Std.ema(c, 11, this._context)
            );
            const ema14 = this._context.new_var(
              PineJS.Std.ema(c, 14, this._context)
            );
            const ema17 = this._context.new_var(
              PineJS.Std.ema(c, 17, this._context)
            );

            // FF函数的实现：3 * WMA(X, 6) - 2 * SMA(X, 6)
            function FF(X) {
              const wma_x = PineJS.Std.wma(X, 6, this._context);
              const sma_x = PineJS.Std.sma(X, 6, this._context);
              return 3 * wma_x - 2 * sma_x;
            }

            // A1-A5: FF(EMA(CLOSE,N))，相当于原来的FORCAST(EMA(CLOSE,N),6)
            const a1 = FF.call(this, ema5);
            const a2 = FF.call(this, ema8);
            const a3 = FF.call(this, ema11);
            const a4 = FF.call(this, ema14);
            const a5 = FF.call(this, ema17);

            // 计算B:=A1+A2+A3+A4-4*A5;
            const b = this._context.new_var(a1 + a2 + a3 + a4 - 4 * a5);
            // 计算TOWERC:=EMA(B,2);
            const towerc = this._context.new_var(
              PineJS.Std.ema(b, 2, this._context)
            );

            // 计算15条预测线: FORCAST(EMA(B,N),6)
            const ema_b_3 = this._context.new_var(
              PineJS.Std.ema(b, 3, this._context)
            );
            const ema_b_4 = this._context.new_var(
              PineJS.Std.ema(b, 4, this._context)
            );
            const ema_b_5 = this._context.new_var(
              PineJS.Std.ema(b, 5, this._context)
            );
            const ema_b_6 = this._context.new_var(
              PineJS.Std.ema(b, 6, this._context)
            );
            const ema_b_7 = this._context.new_var(
              PineJS.Std.ema(b, 7, this._context)
            );
            const ema_b_8 = this._context.new_var(
              PineJS.Std.ema(b, 8, this._context)
            );
            const ema_b_9 = this._context.new_var(
              PineJS.Std.ema(b, 9, this._context)
            );
            const ema_b_10 = this._context.new_var(
              PineJS.Std.ema(b, 10, this._context)
            );
            const ema_b_11 = this._context.new_var(
              PineJS.Std.ema(b, 11, this._context)
            );
            const ema_b_12 = this._context.new_var(
              PineJS.Std.ema(b, 12, this._context)
            );
            const ema_b_13 = this._context.new_var(
              PineJS.Std.ema(b, 13, this._context)
            );
            const ema_b_14 = this._context.new_var(
              PineJS.Std.ema(b, 14, this._context)
            );
            const ema_b_15 = this._context.new_var(
              PineJS.Std.ema(b, 15, this._context)
            );
            const ema_b_16 = this._context.new_var(
              PineJS.Std.ema(b, 16, this._context)
            );
            const ema_b_17 = this._context.new_var(
              PineJS.Std.ema(b, 17, this._context)
            );

            // DD3-DD17: FF(EMA(B,N))，相当于原来的FORCAST(EMA(B,N),6)
            const forecast3 = FF.call(this, ema_b_3);
            const forecast4 = FF.call(this, ema_b_4);
            const forecast5 = FF.call(this, ema_b_5);
            const forecast6 = FF.call(this, ema_b_6);
            const forecast7 = FF.call(this, ema_b_7);
            const forecast8 = FF.call(this, ema_b_8);
            const forecast9 = FF.call(this, ema_b_9);
            const forecast10 = FF.call(this, ema_b_10);
            const forecast11 = FF.call(this, ema_b_11);
            const forecast12 = FF.call(this, ema_b_12);
            const forecast13 = FF.call(this, ema_b_13);
            const forecast14 = FF.call(this, ema_b_14);
            const forecast15 = FF.call(this, ema_b_15);
            const forecast16 = FF.call(this, ema_b_16);
            const forecast17 = FF.call(this, ema_b_17);

            // 实现STICKLINE逻辑：OHLC蜡烛图显示
            const currentTowerc = towerc.get(0);
            const prevTowerc = towerc.get(1);

            let upOpen = NaN,
              upHigh = NaN,
              upLow = NaN,
              upClose = NaN;
            let downOpen = NaN,
              downHigh = NaN,
              downLow = NaN,
              downClose = NaN;

            if (!PineJS.Std.na(currentTowerc) && !PineJS.Std.na(prevTowerc)) {
              if (currentTowerc >= prevTowerc) {
                // 上升蜡烛：当前值大于等于前值
                upOpen = prevTowerc;
                upClose = currentTowerc;
                upHigh = Math.max(prevTowerc, currentTowerc);
                upLow = Math.min(prevTowerc, currentTowerc);
              } else {
                // 下降蜡烛：当前值小于前值
                downOpen = prevTowerc;
                downClose = currentTowerc;
                downHigh = Math.max(prevTowerc, currentTowerc);
                downLow = Math.min(prevTowerc, currentTowerc);
              }
            }

            return [
              towerc.get(0),
              forecast3,
              forecast4,
              forecast5,
              forecast6,
              forecast7,
              forecast8,
              forecast9,
              forecast10,
              forecast11,
              forecast12,
              forecast13,
              forecast14,
              forecast15,
              forecast16,
              forecast17,
              upOpen,
              upHigh,
              upLow,
              upClose,
              downOpen,
              downHigh,
              downLow,
              downClose,
            ];
          };
        },
      };
    },
  };
})();
