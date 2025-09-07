var TvIdxVegasMA = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "VegasMA+BOLL",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsVegasMA@tv-basicstudies-1",
          description: "SQ@VegasMA+BOLL",
          shortDescription: "VegasMA+BOLL",
          is_price_study: true,
          isCustomIndicator: true,
          plots: [
            // Vegas 隧道模式
            {
              id: "plot_fil1",
              type: "line",
            },
            {
              id: "plot_fast1Ema",
              type: "line",
            },
            {
              id: "plot_slow1Ema",
              type: "line",
            },
            {
              id: "plot_fast4Ema",
              type: "line",
            },
            {
              id: "plot_slow4Ema",
              type: "line",
            },
            // 均线系统模式
            {
              id: "plot_fastEma",
              type: "line",
            },
            {
              id: "plot_midEma",
              type: "line",
            },
            {
              id: "plot_slowEma",
              type: "line",
            },
            // 布林带模式
            {
              id: "plot_basis",
              type: "line",
            },
            {
              id: "plot_upper",
              type: "line",
            },
            {
              id: "plot_lower",
              type: "line",
            },
            // ATR 真实波动率
            {
              id: "plot_atrHigh",
              type: "line",
            },
            {
              id: "plot_atrLow",
              type: "line",
            },
          ],
          defaults: {
            palettes: {},
            styles: {
              // Vegas 隧道样式
              plot_fil1: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#fdd835", // 黄色 - 过滤线
              },
              plot_fast1Ema: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#0000FF", // 蓝色 - 快隧道快线
              },
              plot_slow1Ema: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#0000FF", // 蓝色 - 快隧道慢线
              },
              plot_fast4Ema: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#00FF00", // 绿色 - 慢隧道快线
              },
              plot_slow4Ema: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#00FF00", // 绿色 - 慢隧道慢线
              },
              // 均线系统样式
              plot_fastEma: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFF00", // 黄色 - 快线
              },
              plot_midEma: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF0000", // 红色 - 中线
              },
              plot_slowEma: {
                linestyle: 0,
                linewidth: 2,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#0000FF", // 蓝色 - 慢线
              },
              // 布林带样式
              plot_basis: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#872323", // 中轨
              },
              plot_upper: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#008080", // 青色 - 上轨
              },
              plot_lower: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#008080", // 青色 - 下轨
              },
              // ATR 样式
              plot_atrHigh: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFF00", // 黄色
              },
              plot_atrLow: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FFFF00", // 黄色
              },
            },
            inputs: {
              flag: "vegas",
              ma12: 12,
              bei4: 4,
              ma7: 7,
              ma14: 14,
              ma30: 30,
              boll: 30,
              atrFlag: false,
            },
          },
          palettes: {},
          styles: {
            plot_fil1: {
              title: "过滤线",
              histogramBase: 0,
            },
            plot_fast1Ema: {
              title: "快隧道快线",
              histogramBase: 0,
            },
            plot_slow1Ema: {
              title: "快隧道慢线",
              histogramBase: 0,
            },
            plot_fast4Ema: {
              title: "慢隧道快线",
              histogramBase: 0,
            },
            plot_slow4Ema: {
              title: "慢隧道慢线",
              histogramBase: 0,
            },
            plot_fastEma: {
              title: "快线-7ema",
              histogramBase: 0,
            },
            plot_midEma: {
              title: "中线-14ema",
              histogramBase: 0,
            },
            plot_slowEma: {
              title: "慢线-30ema",
              histogramBase: 0,
            },
            plot_basis: {
              title: "中轨",
              histogramBase: 0,
            },
            plot_upper: {
              title: "上轨",
              histogramBase: 0,
            },
            plot_lower: {
              title: "下轨",
              histogramBase: 0,
            },
            plot_atrHigh: {
              title: "ATR上轨",
              histogramBase: 0,
            },
            plot_atrLow: {
              title: "ATR下轨",
              histogramBase: 0,
            },
          },
          inputs: [
            {
              id: "flag",
              name: "主图常用指标",
              type: "text",
              defval: "vegas",
              options: ["vegas", "均线", "boll带"],
            },
            {
              id: "ma12",
              name: "vegas过滤线",
              type: "integer",
              defval: 12,
              min: 1,
              max: 100,
            },
            {
              id: "bei4",
              name: "Vegas倍数",
              type: "integer",
              defval: 4,
              min: 1,
              max: 10,
            },
            {
              id: "ma7",
              name: "均线系统快线",
              type: "integer",
              defval: 7,
              min: 1,
              max: 100,
            },
            {
              id: "ma14",
              name: "均线系统中线",
              type: "integer",
              defval: 14,
              min: 1,
              max: 100,
            },
            {
              id: "ma30",
              name: "均线系统慢线",
              type: "integer",
              defval: 30,
              min: 1,
              max: 100,
            },
            {
              id: "boll",
              name: "布林带中轨",
              type: "integer",
              defval: 30,
              min: 1,
              max: 100,
            },
            {
              id: "atrFlag",
              name: "真实波动率",
              type: "bool",
              defval: false,
            },
          ],
          format: {
            type: "price",
            precision: 4,
          },
        },
        constructor: function () {
          this.init = function (context, inputCallback) {
            // 初始化
          };
          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取输入参数
            const flag = this._input(0);
            const ma12 = this._input(1);
            const bei4 = this._input(2);
            const ma7 = this._input(3);
            const ma14 = this._input(4);
            const ma30 = this._input(5);
            const boll = this._input(6);
            const atrFlag = this._input(7);

            // 获取价格数据
            const c = this._context.new_var(PineJS.Std.close(this._context));
            const o = this._context.new_var(PineJS.Std.open(this._context));
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));

            // 计算 ATR
            const atr = this._context.new_var(
              PineJS.Std.atr(14, this._context)
            );

            // 根据模式计算参数
            let parameter1, parameter2, parameter3;
            if (flag === "vegas") {
              parameter1 = ma12;
              parameter2 = bei4;
              parameter3 = NaN;
            } else if (flag === "均线") {
              parameter1 = ma7;
              parameter2 = ma14;
              parameter3 = ma30;
            } else if (flag === "boll带") {
              parameter1 = boll;
              parameter2 = ma14;
              parameter3 = NaN;
            }

            // 计算 ATR 上下轨
            let atrHigh, atrLow;
            if (c.get(0) > o.get(0)) {
              atrHigh = c.get(0) + atr.get(0) / 2;
              atrLow = o.get(0) - atr.get(0) / 2;
            } else {
              atrHigh = o.get(0) + atr.get(0) / 2;
              atrLow = c.get(0) - atr.get(0) / 2;
            }

            // Vegas 隧道模式计算
            let fil1 = NaN,
              fast1Ema = NaN,
              slow1Ema = NaN,
              fast4Ema = NaN,
              slow4Ema = NaN;
            if (flag === "vegas") {
              fil1 = PineJS.Std.ema(c, parameter1, this._context);
              fast1Ema = PineJS.Std.ema(c, 144, this._context);
              slow1Ema = PineJS.Std.ema(c, 169, this._context);
              fast4Ema = PineJS.Std.ema(c, parameter2 * 144, this._context);
              slow4Ema = PineJS.Std.ema(c, parameter2 * 169, this._context);
            }

            // 均线系统模式计算
            let fastEma = NaN,
              midEma = NaN,
              slowEma = NaN;
            if (flag === "均线") {
              fastEma = PineJS.Std.ema(c, parameter1, this._context);
              midEma = PineJS.Std.ema(c, parameter2, this._context);
              slowEma = PineJS.Std.ema(c, parameter3, this._context);
            }

            // 布林带模式计算
            let basis = NaN,
              upper = NaN,
              lower = NaN;
            if (flag === "boll带") {
              basis = PineJS.Std.sma(c, parameter1, this._context);
              const dev = 2 * PineJS.Std.stdev(c, parameter1, this._context);
              upper = basis + dev;
              lower = basis - dev;
            }

            // 返回所有指标值
            return [
              fil1, // 0: 过滤线
              fast1Ema, // 1: 快隧道快线
              slow1Ema, // 2: 快隧道慢线
              fast4Ema, // 3: 慢隧道快线
              slow4Ema, // 4: 慢隧道慢线
              fastEma, // 5: 快线-7ema
              midEma, // 6: 中线-14ema
              slowEma, // 7: 慢线-30ema
              basis, // 8: 布林带中轨
              upper, // 9: 布林带上轨
              lower, // 10: 布林带下轨
              atrFlag ? atrHigh : NaN, // 11: ATR上轨
              atrFlag ? atrLow : NaN, // 12: ATR下轨
            ];
          };
        },
      };
    },
  };
})();
