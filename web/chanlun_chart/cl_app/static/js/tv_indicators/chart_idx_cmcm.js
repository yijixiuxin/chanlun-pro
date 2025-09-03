var TvIdxCMCM = (function () {
  return {
    idx: function (PineJS) {
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
  };
})();
