var TvIdxHDLY = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "海底捞月",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsHDLY@tv-basicstudies-1",
          description: "东@海底捞月",
          shortDescription: "海底捞月",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_red",
              type: "line",
            },
            {
              id: "plot_green",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_red: {
                linestyle: 0,
                linewidth: 1,
                plottype: 5,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#f23645",
              },
              plot_green: {
                linestyle: 0,
                linewidth: 1,
                plottype: 5,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#089981",
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_red: {
              title: "红色线",
              histogramBase: 0,
            },
            plot_green: {
              title: "绿色线",
              histogramBase: 0,
            },
          },
          inputs: [],
          format: {},
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            this.prev_big_money = NaN;
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // VARB:REF(LOW,1),NODRAW;
            const varb = this._context.new_var(l.get(1));
            // VARC:SMA(ABS(LOW-VARB),3,1)/SMA(MAX(LOW-VARB,0),3,1)*100,NODRAW;
            const abs_low_varb = this._context.new_var(
              PineJS.Std.abs(l.get(0) - varb.get(0))
            );
            const max_low_varb = this._context.new_var(
              PineJS.Std.max(l.get(0) - varb.get(0), 0)
            );

            // 实现SMA(X,N,M)函数，等同于EMA中的权重M/N
            const sma_abs = this._context.new_var(
              PineJS.Std.ema(abs_low_varb, 5, this._context)
            );
            const sma_max = this._context.new_var(
              PineJS.Std.ema(max_low_varb, 5, this._context)
            );
            const varc = this._context.new_var(
              (sma_abs.get(0) / sma_max.get(0)) * 100
            );
            // VARD:EMA(IF(CLOSE*1.35,VARC*10,VARC/10),3),NODRAW;
            const if_value = this._context.new_var(varc.get(0) * 10);

            const vard = this._context.new_var(
              PineJS.Std.ema(if_value, 3, this._context)
            );

            // VARE:LLV(LOW,30),NODRAW;
            const vare = this._context.new_var(
              PineJS.Std.lowest(l, 30, this._context)
            );
            // VARF:HHV(VARD,30),NODRAW;
            const varf = this._context.new_var(
              PineJS.Std.highest(vard, 30, this._context)
            );
            // BIG_MONEY: EMA(IF(LOW<=VARE,(VARD+VARF*2)/2,0),3)/618,NODRAW;
            const if_money = this._context.new_var(
              l.get(0) <= vare.get(0) ? (vard.get(0) + varf.get(0) * 2) / 2 : 0
            );
            const big_money = this._context.new_var(
              PineJS.Std.ema(if_money, 3, this._context) / 618
            );
            // STICKLINE(BIG_MONEY>-150,0,BIG_MONEY*2,1,0),COLORRED;
            let red_value = NaN;
            let green_value = NaN;

            if (big_money.get(0) > -150) {
              try {
                if (big_money.get(0) == NaN || big_money.get(1) == NaN) {
                  return [NaN, NaN];
                }
              } catch (error) {
                return [NaN, NaN];
              }

              if (big_money.get(0) > this.prev_big_money) {
                red_value = big_money.get(0) * 2;
              } else if (big_money.get(0) < this.prev_big_money) {
                green_value = big_money.get(0) * 2;
              }
            }
            // 更新前一个周期的big_money
            this.prev_big_money = big_money.get(0);
            return [red_value, green_value];
          };
        },
      };
    },
  };
})();
