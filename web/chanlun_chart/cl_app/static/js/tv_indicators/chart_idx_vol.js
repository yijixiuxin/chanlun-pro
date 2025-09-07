var TvIdxVOL = (function () {
  return {
    idx: function (PineJS) {
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
  };
})();
