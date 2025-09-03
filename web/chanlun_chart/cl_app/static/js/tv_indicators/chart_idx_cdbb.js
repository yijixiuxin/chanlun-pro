var TvIdxCDBB = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "抄底必备",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsCDBB@tv-basicstudies-1",
          description: "东@抄底必备",
          shortDescription: "抄底必备",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_cdbb_signal",
              type: "shapes",
            },
          ],
          defaults: {
            styles: {
              plot_cdbb_signal: {
                plottype: "shape_flag",
                location: "BelowBar",
                color: "#FFFF00",
              },
            },
            inputs: {},
          },
          styles: {
            plot_cdbb_signal: {
              title: "抄底信号",
              size: "small",
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
            // 初始化历史数据存储
            context.yaod17_history = [];
            context.yaod19_history = [];
            context.yaod20_history = [];
            context.yaod4_filter_last = -999;
            context.yaod19_filter_last = -999;
            context.yaod20_filter_last = -999;
            context.bar_count = 0;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取价格数据
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // YAOD1: ((MA(C,30)-L)/MA(C,60))*200
            const ma_c_30 = PineJS.Std.sma(c, 30, this._context);
            const ma_c_60 = PineJS.Std.sma(c, 60, this._context);
            const yaod1 = this._context.new_var(
              ma_c_60 !== 0 ? ((ma_c_30 - l.get(0)) / ma_c_60) * 200 : 0
            );

            // YAOD2: REF(CLOSE,1)
            const yaod2 = this._context.new_var(c.get(1));

            // YAOD3: SMA(MAX(CLOSE-YAOD2,0),7,1)/SMA(ABS(CLOSE-YAOD2),7,1)*100
            const max_close_yaod2 = this._context.new_var(
              Math.max(c.get(0) - yaod2.get(0), 0)
            );
            const abs_close_yaod2 = this._context.new_var(
              Math.abs(c.get(0) - yaod2.get(0))
            );
            // SMA(X,N,M) = EMA with weight M/N, using EMA(X, 2*N/M-1)
            const sma_max = this._context.new_var(
              PineJS.Std.ema(max_close_yaod2, (2 * 7) / 1 - 1, this._context)
            );
            const sma_abs = this._context.new_var(
              PineJS.Std.ema(abs_close_yaod2, (2 * 7) / 1 - 1, this._context)
            );
            const yaod3 = this._context.new_var(
              sma_abs.get(0) !== 0 ? (sma_max.get(0) / sma_abs.get(0)) * 100 : 0
            );

            // YAOD4: FILTER(REF(YAOD3,1)<20 AND YAOD3>REF(YAOD3,1),5)
            const yaod3_ref1 = yaod3.get(1);
            const yaod4_condition =
              yaod3_ref1 < 20 && yaod3.get(0) > yaod3_ref1;
            let yaod4 = 0;
            if (
              yaod4_condition &&
              this._context.bar_count - this._context.yaod4_filter_last >= 5
            ) {
              yaod4 = 1;
              this._context.yaod4_filter_last = this._context.bar_count;
            }

            // YAOD5: C/MA(C,40)<0.74
            const ma_c_40 = PineJS.Std.sma(c, 40, this._context);
            const yaod5 = c.get(0) / ma_c_40 < 0.74;

            // YAOD6: =5
            const yaod6 = 5;

            // YAOD7: EMA(C,YAOD6)
            const yaod7 = this._context.new_var(
              PineJS.Std.ema(c, yaod6, this._context)
            );

            // YAOD8: EMA(YAOD7,YAOD6)
            const yaod8 = this._context.new_var(
              PineJS.Std.ema(yaod7, yaod6, this._context)
            );

            // YAOD9: YAOD7 - REF(YAOD7,1)
            const yaod9 = this._context.new_var(yaod7.get(0) - yaod7.get(1));

            // YAOD10: YAOD8 - REF(YAOD8,1)
            const yaod10 = this._context.new_var(yaod8.get(0) - yaod8.get(1));

            // YAOD11: ABS(YAOD7 - YAOD8)
            const yaod11 = this._context.new_var(
              Math.abs(yaod7.get(0) - yaod8.get(0))
            );

            // YAOD12: (H-L)/REF(C,1)>0.05
            const c_ref1 = c.get(1);
            const yaod12 =
              c_ref1 !== 0 ? (h.get(0) - l.get(0)) / c_ref1 > 0.05 : false;

            // YAOD13: (YAOD9+YAOD10)/2
            const yaod13 = this._context.new_var(
              (yaod9.get(0) + yaod10.get(0)) / 2
            );

            // YAOD14: POW(YAOD11,1)*POW(YAOD13,3)
            const yaod14 = this._context.new_var(
              Math.pow(yaod11.get(0), 1) * Math.pow(yaod13.get(0), 3)
            );

            // YAOD15: YAOD14/HHV(ABS(YAOD14),YAOD6*3)
            const abs_yaod14 = this._context.new_var(Math.abs(yaod14.get(0)));
            const hhv_abs_yaod14 = PineJS.Std.highest(
              abs_yaod14,
              yaod6 * 3,
              this._context
            );
            const yaod15 = this._context.new_var(
              hhv_abs_yaod14 !== 0 ? yaod14.get(0) / hhv_abs_yaod14 : 0
            );

            // YAOD16: COUNT(YAOD12,5)>1
            let count_yaod12 = 0;
            for (let i = 0; i < 5; i++) {
              const c_ref_i = c.get(i + 1);
              if (c_ref_i !== 0) {
                const condition = (h.get(i) - l.get(i)) / c_ref_i > 0.05;
                if (condition) count_yaod12++;
              }
            }
            const yaod16 = count_yaod12 > 1;

            // YAOD17: YAOD5 AND YAOD12 AND YAOD16
            const yaod17 = yaod5 && yaod12 && yaod16;

            // 存储YAOD17历史值
            this._context.yaod17_history.push(yaod17);
            if (this._context.yaod17_history.length > 50) {
              this._context.yaod17_history.shift();
            }

            // YAOD18: CROSS(YAOD15,-0.9)
            const yaod15_ref1 = yaod15.get(1);
            const yaod18 = yaod15_ref1 < -0.9 && yaod15.get(0) >= -0.9;

            // 简化的MACD计算
            const ema_12 = this._context.new_var(
              PineJS.Std.ema(c, 12, this._context)
            );
            const ema_26 = this._context.new_var(
              PineJS.Std.ema(c, 26, this._context)
            );
            const macd_line = this._context.new_var(
              ema_12.get(0) - ema_26.get(0)
            );
            const macd_signal = this._context.new_var(
              PineJS.Std.ema(macd_line, 9, this._context)
            );
            const macd_macd = this._context.new_var(
              macd_line.get(0) - macd_signal.get(0)
            );

            // 获取REF(YAOD17,1) - 前一个周期的YAOD17值
            const yaod17_ref1 =
              this._context.yaod17_history.length >= 2
                ? this._context.yaod17_history[
                    this._context.yaod17_history.length - 2
                  ]
                : false;

            // YAOD19: FILTER((YAOD4 AND YAOD1>20 OR C>REF(C,1)) AND REF(YAOD17,1),10)
            const yaod19_condition =
              ((yaod4 && yaod1.get(0) > 20) || c.get(0) > c.get(1)) &&
              yaod17_ref1;
            let yaod19 = 0;
            if (
              yaod19_condition &&
              this._context.bar_count - this._context.yaod19_filter_last >= 10
            ) {
              yaod19 = 1;
              this._context.yaod19_filter_last = this._context.bar_count;
            }

            // 存储YAOD19历史值
            this._context.yaod19_history.push(yaod19);
            if (this._context.yaod19_history.length > 20) {
              this._context.yaod19_history.shift();
            }

            // YAOD20: FILTER(REF(YAOD17,1) AND (YAOD18 OR C>REF(C,1)) AND "MACD.MACD">-1.5,10)
            const yaod20_condition =
              yaod17_ref1 &&
              (yaod18 || c.get(0) > c.get(1)) &&
              macd_macd.get(0) > -1.5;
            let yaod20 = 0;
            if (
              yaod20_condition &&
              this._context.bar_count - this._context.yaod20_filter_last >= 10
            ) {
              yaod20 = 1;
              this._context.yaod20_filter_last = this._context.bar_count;
            }

            // 存储YAOD20历史值
            this._context.yaod20_history.push(yaod20);
            if (this._context.yaod20_history.length > 15) {
              this._context.yaod20_history.shift();
            }

            // 抄底必备: COUNT(YAOD20,13)>=1 AND YAOD19
            let count_yaod20 = 0;
            // 统计过去13个周期内YAOD20为1的次数
            const lookback_periods = Math.min(
              13,
              this._context.yaod20_history.length
            );
            for (let i = 0; i < lookback_periods; i++) {
              const index = this._context.yaod20_history.length - 1 - i;
              if (index >= 0 && this._context.yaod20_history[index] === 1) {
                count_yaod20++;
              }
            }

            const cdbb_signal = count_yaod20 >= 1 && yaod19 ? c.get(0) : NaN;

            // 更新bar_count
            this._context.bar_count++;

            return [cdbb_signal];
          };
        },
      };
    },
  };
})();
