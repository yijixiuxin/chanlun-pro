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
    idx_hdly: function (PineJS) {
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
    idx_cmcm: function (PineJS) {
      return {
        name: "超买超卖",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsWLDKC@tv-basicstudies-1",
          description: "东@超买超卖",
          shortDescription: "超买超卖",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_a",
              type: "line",
            },
            {
              id: "plot_ref10",
              type: "line",
            },
            {
              id: "plot_ref50",
              type: "line",
            },
            {
              id: "plot_ref20",
              type: "line",
            },
            {
              id: "plot_ref60",
              type: "line",
            },
            {
              id: "plot_buy",
              type: "shapes",
            },
            {
              id: "plot_high50",
              type: "histogram",
            },
            {
              id: "plot_high50_down",
              type: "histogram",
            },
            {
              id: "plot_high60",
              type: "histogram",
            },
            {
              id: "plot_low20",
              type: "histogram",
            },
            {
              id: "plot_low20_up",
              type: "histogram",
            },
            {
              id: "plot_low10",
              type: "histogram",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_a: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#CC33FF", // COLORLIMAGENTA
              },
              plot_ref10: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#808080", // COLORGRAY
              },
              plot_ref50: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF3333", // COLORRED
              },
              plot_ref20: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#808080", // COLORGRAY
              },
              plot_ref60: {
                linestyle: 1, // 虚线
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#A52A2A", // COLORBROWN
              },
              plot_buy: {
                plottype: "shape_flag",
                location: "Bottom",
                color: "#FFFF00",
              },
              plot_high50: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FFFF00", // COLORYELLOW
                linewidth: 4,
                histogramBase: 50,
              },
              plot_high50_down: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#27b94d", // COLORGREEN
                linewidth: 4,
                histogramBase: 50,
              },
              plot_high60: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FF0000", // COLORRED
                linewidth: 4,
                histogramBase: 60,
              },
              plot_low20: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#C0C0C0", // COLORLIGRAY
                linewidth: 4,
                histogramBase: 20,
              },
              plot_low20_up: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FF3366", // COLORLIRED
                linewidth: 4,
                histogramBase: 20,
              },
              plot_low10: {
                plottype: 1, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FFFFFF", // COLORWHITE
                linewidth: 4,
                histogramBase: 10,
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_a: {
              title: "A线",
              histogramBase: 0,
            },
            plot_ref10: {
              title: "参考线10",
              histogramBase: 0,
            },
            plot_ref50: {
              title: "参考线50",
              histogramBase: 0,
            },
            plot_ref20: {
              title: "参考线20",
              histogramBase: 0,
            },
            plot_ref60: {
              title: "参考线60",
              histogramBase: 0,
            },
            plot_buy: {
              title: "买点",
              size: "small",
            },
            plot_high50: {
              title: "高位线50",
              histogramBase: 50,
            },
            plot_high50_down: {
              title: "高位下降",
              histogramBase: 50,
            },
            plot_high60: {
              title: "高位线60",
              histogramBase: 60,
            },
            plot_low20: {
              title: "低位线20",
              histogramBase: 20,
            },
            plot_low20_up: {
              title: "低位上升",
              histogramBase: 20,
            },
            plot_low10: {
              title: "低位线10",
              histogramBase: 10,
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

            // 获取价格数据
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // WL:=(HHV(H,8)-C)/(HHV(H,8)-LLV(L,8))*100-70;
            const hhv_h_8 = PineJS.Std.highest(h, 8, this._context);
            const llv_l_8 = PineJS.Std.lowest(l, 8, this._context);
            const wl = this._context.new_var(
              ((hhv_h_8 - c.get(0)) / (hhv_h_8 - llv_l_8)) * 100 - 70
            );

            // MWL:=SMA(WL,8,1);
            // SMA(X,N,M) 相当于 EMA中权重为 M/N 的EMA，使用公式 EMA(X, 2*N/M-1)
            const mwl = this._context.new_var(
              PineJS.Std.ema(wl, (2 * 8) / 1 - 1, this._context)
            );

            // RSV:=(C-LLV(L,8))/(HHV(H,8)-LLV(L,8))*100;
            const rsv = this._context.new_var(
              ((c.get(0) - llv_l_8) / (hhv_h_8 - llv_l_8)) * 100
            );

            // K:=SMA(RSV,3,1);
            const k = this._context.new_var(
              PineJS.Std.ema(rsv, (2 * 3) / 1 - 1, this._context)
            );

            // D:=SMA(K,3,1);
            const d = this._context.new_var(
              PineJS.Std.ema(k, (2 * 3) / 1 - 1, this._context)
            );

            // DKC:=(D+100)-(MWL+100);
            const dkc = this._context.new_var(
              d.get(0) + 100 - (mwl.get(0) + 100)
            );

            // GL:=((C-MA(C,6))/MA(C,6)*200)+20;
            const ma_c_6 = PineJS.Std.sma(c, 6, this._context);
            const gl = this._context.new_var(
              ((c.get(0) - ma_c_6) / ma_c_6) * 200 + 20
            );

            // A:DKC/2,COLORLIMAGENTA,LINETHICK2;
            const a = dkc.get(0) / 2;
            const a_1 = this._context.new_var(a).get(1); // REF(A,1) - 前一个周期的A值

            // 买点:REF(A<20,1)AND A>REF(A,1)AND CROSS(GL,19.5),COLORYELLOW,NODRAW;
            const a_less_20_1 = this._context.new_var(a < 20 ? 1 : 0).get(1); // REF(A<20,1)

            // 检测GL穿过19.5的情况
            const gl_1 = gl.get(1);
            const cross_gl_195 = gl_1 < 19.5 && gl.get(0) >= 19.5 ? 1 : 0;

            const buy_point = a_less_20_1 && a > a_1 && cross_gl_195 ? 1 : 0;

            // 低位:A<20,COLORLIGRAY,NODRAW;
            const low_position = a < 20 ? 1 : 0;

            // 高位:A>50,COLORYELLOW,NODRAW;
            const high_position = a > 50 ? 1 : 0;

            // 参考线
            const ref_10 = 10;
            const ref_20 = 20;
            const ref_50 = 50;
            const ref_60 = 60;

            // STICKLINE部分的实现，转换为条件绘制

            // STICKLINE(A>50,50,A,0.5,0),COLORYELLOW; - 高位黄色
            // 注意：需要排除A>50 AND A<REF(A,1)的情况，因为那种情况要显示绿色
            const high50_line = a > 50 && !(a < a_1) ? a : NaN;

            // STICKLINE(A>50 AND A<REF(A,1),50,A,0.5,0),COLORGREEN; - 高位且下降(绿色)
            const high50_down_line = a > 50 && a < a_1 ? a : NaN;

            // STICKLINE(A>60,60,A,0.5,0),COLORRED; - 超高位(红色)
            const high60_line = a > 60 ? a : NaN;

            // STICKLINE(A<20,20,A,0.5,0),COLORLIGRAY; - 低位(灰色)
            // 注意：需要排除A<20 AND A>REF(A,1)的情况，因为那种情况要显示红色
            const low20_line = a < 20 && !(a > a_1) ? a : NaN;

            // STICKLINE(A<20 AND A>REF(A,1),20,A,0.5,0),COLORLIRED; - 低位且上升(红色)
            const low20_up_line = a < 20 && a > a_1 ? a : NaN;

            // STICKLINE(A<10,10,A,0.5,0),COLORWHITE; - 超低位(白色)
            const low10_line = a < 10 ? a : NaN;

            // STICKLINE(买点=1,22,A,0.5,0),COLORBLUE; - 买点信号
            const buy_signal = buy_point ? a : NaN;

            // 返回所有计算结果
            return [
              a, // 0: A线，主要指标线
              ref_10, // 1: 参考线10
              ref_50, // 2: 参考线50
              ref_20, // 3: 参考线20
              ref_60, // 4: 参考线60
              buy_signal, // 5: 买点信号
              high50_line, // 6: 高位线50
              high50_down_line, // 7: 高位下降
              high60_line, // 8: 高位线60
              low20_line, // 9: 低位线20
              low20_up_line, // 10: 低位上升
              low10_line, // 11: 低位线10
            ];
          };
        },
      };
    },
    idx_fcx: function (PineJS) {
      return {
        name: "发财线",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsHMA@tv-basicstudies-1",
          description: "东@发财线",
          shortDescription: "发财线",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_hma",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_hma: {
                linestyle: 0,
                linewidth: 3,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF8D1E",
              },
            },
            inputs: {
              N: 88,
            },
          },
          styles: {
            plot_hma: {
              title: "HMA",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "N",
              name: "周期",
              type: "integer",
              defval: 88,
              min: 1,
              max: 500,
            },
          ],
          format: {
            type: "price",
            precision: 4,
          },
        },
        constructor: function () {
          this.init = function () {
            // 初始化
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            var N = this._input(0);
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // N:=88;
            // WMA1:=WMA(C,N/2);
            const wma1 = this._context.new_var(
              PineJS.Std.wma(c, Math.floor(N / 2), this._context)
            );

            // WMA2:= WMA(C,N);
            const wma2 = this._context.new_var(
              PineJS.Std.wma(c, N, this._context)
            );

            // TEMP:= 2*WMA1-WMA2;
            const temp = this._context.new_var(2 * wma1.get(0) - wma2.get(0));

            // HMA:WMA(TEMP, SQRT(N))
            const hma = PineJS.Std.wma(
              temp,
              Math.floor(Math.sqrt(N)),
              this._context
            );

            return [hma];
          };
        },
      };
    },
    idx_hlblw: function (PineJS) {
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
    idx_ltqs: function (PineJS) {
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
    idx_heima: function (PineJS) {
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
    idx_cdbb: function (PineJS) {
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
    idx_vol: function (PineJS) {
      return {
        name: "成交量",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsVOL@tv-basicstudies-1",
          description: "东@成交量",
          shortDescription: "成交量",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_vol_up",
              type: "histogram",
            },
            {
              id: "plot_vol_down",
              type: "histogram",
            },
            {
              id: "plot_mavol1",
              type: "line",
            },
            {
              id: "plot_mavol2",
              type: "line",
            },
            {
              id: "plot_mavol3",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              plot_vol_up: {
                plottype: 5, // 柱状图
                transparency: 0,
                visible: true,
                color: "#FF3232", // COLORFF3232 红色
                linewidth: 2,
                histogramBase: 0,
              },
              plot_vol_down: {
                plottype: 5, // 柱状图
                transparency: 0,
                visible: true,
                color: "#00A843", // COLOR00A843 绿色
                linewidth: 2,
                histogramBase: 0,
              },
              plot_mavol1: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF8D1E", // COLORFF8D1E 橙色
              },
              plot_mavol2: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#0CAEE6", // COLOR0CAEE6 蓝色
              },
              plot_mavol3: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#E970DC", // COLORE970DC 紫色
              },
            },
            inputs: {},
          },
          palettes: {},
          styles: {
            plot_vol_up: {
              title: "上涨成交量",
              histogramBase: 0,
            },
            plot_vol_down: {
              title: "下跌成交量",
              histogramBase: 0,
            },
            plot_mavol1: {
              title: "MAVOL1",
              histogramBase: 0,
            },
            plot_mavol2: {
              title: "MAVOL2",
              histogramBase: 0,
            },
            plot_mavol3: {
              title: "MAVOL3",
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

            // 获取价格和成交量数据
            const o = this._context.new_var(PineJS.Std.open(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));
            const v = this._context.new_var(PineJS.Std.volume(this._context));

            // STICKLINE(CLOSE>=OPEN,VOL,0,0.8,1),COLORFF3232;
            // STICKLINE(CLOSE<OPEN,VOL,0,0.8,0),COLOR00A843;
            const vol_up = c.get(0) >= o.get(0) ? v.get(0) : NaN;
            const vol_down = c.get(0) < o.get(0) ? v.get(0) : NaN;

            // MAVOL1:MA(VOL,5),COLORFF8D1E;
            const mavol1 = PineJS.Std.sma(v, 5, this._context);

            // MAVOL2:MA(VOL,10),COLOR0CAEE6;
            const mavol2 = PineJS.Std.sma(v, 10, this._context);

            // MAVOL3:MA(VOL,20),COLORE970DC;
            const mavol3 = PineJS.Std.sma(v, 20, this._context);

            return [
              vol_up, // 0: 上涨成交量柱状图
              vol_down, // 1: 下跌成交量柱状图
              mavol1, // 2: 5周期成交量均线
              mavol2, // 3: 10周期成交量均线
              mavol3, // 4: 20周期成交量均线
            ];
          };
        },
      };
    },
    idx_hlftx: function (PineJS) {
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
