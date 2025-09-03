var TvIdxMACDBL = (function () {
  return {
    idx: function (PineJS) {
      return {
        name: "MACD背离版+金死叉",
        metainfo: {
          _metainfoVersion: 53,
          id: "CustomIndicatorsMACDBL@tv-basicstudies-1",
          description: "SQ@MACD背离版+金死叉",
          shortDescription: "MACD背离版+金死叉",
          is_price_study: false,
          isCustomIndicator: true,
          plots: [
            {
              id: "plot_hist",
              type: "line",
              target: "plot_macd_bl",
              palette: "paletteHistColorer",
            },
            {
              id: "plot_macd",
              type: "line",
              target: "plot_macd_bl",
            },
            {
              id: "plot_signal",
              type: "line",
              target: "plot_macd_bl",
            },
            {
              id: "plot_crossGold",
              type: "shapes",
              target: "plot_macd_bl",
            },
            {
              id: "plot_crossDead",
              type: "shapes",
              target: "plot_macd_bl",
            },
            {
              id: "plot_bullShape",
              type: "shapes",
              target: "plot_macd_bl",
            },
            {
              id: "plot_bearShape",
              type: "shapes",
              target: "plot_macd_bl",
            },
          ],
          defaults: {
            palettes: {
              paletteHistColorer: {
                colors: [{ color: "#26A69A" }, { color: "#F44336" }],
              },
            },
            styles: {
              plot_hist: {
                linestyle: 0,
                linewidth: 1,
                plottype: 1,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#26A69A", // 增长绿柱
              },
              plot_macd: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#2962FF", // MACD线
              },
              plot_signal: {
                linestyle: 0,
                linewidth: 1,
                plottype: 0,
                trackPrice: false,
                transparency: 0,
                visible: true,
                color: "#FF6D00", // 信号线
              },
              plot_crossGold: {
                color: "#FF5252",
                textColor: "#2196F3",
                plottype: "shape_xcross",
                location: "Absolute",
                visible: true,
              },
              plot_crossDead: {
                color: "#00FF00", // 死叉标识
                textColor: "#2196F3",
                plottype: "shape_xcross",
                location: "Absolute",
                visible: true,
              },
              plot_bullShape: {
                color: "#4CAF50",
                textColor: "#4CAF50",
                plottype: "shape_triangle_down",
                location: "Absolute",
                visible: true,
              },
              plot_bearShape: {
                color: "#F44336",
                textColor: "#F44336",
                plottype: "shape_triangle_up",
                location: "Absolute",
                visible: true,
              },
            },
            inputs: {
              fast_length: 12,
              slow_length: 26,
              signal_length: 9,
              plotGold: true,
              plotDead: true,
              plotBull: true,
              plotBear: true,
            },
          },
          palettes: {
            paletteHistColorer: {
              colors: [{ name: "UP" }, { name: "DOWN" }],
            },
          },
          styles: {
            plot_hist: {
              title: "直方图",
              histogramBase: 0,
            },
            plot_macd: {
              title: "MACD",
              histogramBase: 0,
            },
            plot_signal: {
              title: "信号线",
              histogramBase: 0,
            },
            plot_crossGold: {
              title: "金叉标识",
              isHidden: false,
              location: "Absolute",
              // text: "Gold",
            },
            plot_crossDead: {
              title: "死叉标识",
              isHidden: false,
              location: "Absolute",
              // text: "Dead",
            },
            plot_bullShape: {
              title: "底背离标识",
              isHidden: false,
              location: "Absolute",
              text: "底",
            },
            plot_bearShape: {
              title: "顶背离标识",
              isHidden: false,
              location: "Absolute",
              text: "顶",
            },
          },
          inputs: [
            {
              id: "fast_length",
              name: "MACD快线长度",
              type: "integer",
              defval: 12,
              min: 1,
              max: 100,
            },
            {
              id: "slow_length",
              name: "MACD慢线长度",
              type: "integer",
              defval: 26,
              min: 1,
              max: 100,
            },
            {
              id: "signal_length",
              name: "MACD信号线长度",
              type: "integer",
              defval: 9,
              min: 1,
              max: 100,
            },
            {
              id: "plotGold",
              name: "金叉标识",
              type: "bool",
              defval: true,
            },
            {
              id: "plotDead",
              name: "死叉标识",
              type: "bool",
              defval: true,
            },
            {
              id: "plotBull",
              name: "底背离标识",
              type: "bool",
              defval: true,
            },
            {
              id: "plotBear",
              name: "顶背离标识",
              type: "bool",
              defval: true,
            },
          ],
          format: {
            type: "price",
            precision: 4,
          },
        },
        constructor: function () {
          // 实现 PineJS 中没有的函数

          // 简化的金叉死叉检测，不使用 barssince
          this.detectCross = function (macd, signal, context) {
            // 获取当前和前一根K线的值
            const macdCurrent = macd.get(0);
            const macdPrevious = macd.get(1);
            const signalCurrent = signal.get(0);
            const signalPrevious = signal.get(1);

            // 金叉：MACD从下方穿过信号线
            const crossGold =
              macdPrevious <= signalPrevious && macdCurrent > signalCurrent;

            // 死叉：MACD从上方穿过信号线
            const crossDead =
              macdPrevious >= signalPrevious && macdCurrent < signalCurrent;

            return { crossGold, crossDead };
          };

          /**
           * 检测MACD的枢轴点
           * @param {Object} hist - MACD直方图数据序列
           * @param {Object} high_price - 最高价数据序列
           * @param {Object} low_price - 最低价数据序列
           * @param {Object} context - PineJS执行上下文
           * @returns {Object} 包含枢轴点信息的对象
           */
          this.checkPivotPoints = function (
            hist,
            high_price,
            low_price,
            context
          ) {
            // 从hist变量中依次取出5-0的值复制到数组中
            const histArray = [];
            const highPriceArray = [];
            const lowPriceArray = [];
            for (let i = 4; i >= 0; i--) {
              const histValue = hist.get(i);
              histArray.push(histValue);
              const highPriceValue = high_price.get(i);
              highPriceArray.push(highPriceValue);
              const lowPriceValue = low_price.get(i);
              lowPriceArray.push(lowPriceValue);
            }

            // 检查数组中是否有无效值
            if (histArray.some((value) => isNaN(value))) {
              return {
                hasTop: false,
                hasBottom: false,
                topInfo: null,
                bottomInfo: null,
              };
            }

            // 中心点在数组中的索引（第2个位置，对应原来的findBackNum）
            const centerIndex = 2;
            const centerValue = histArray[centerIndex];

            // 获取对应的价格值
            const centerHighPrice = highPriceArray[centerIndex];
            const centerLowPrice = lowPriceArray[centerIndex];

            let result = {
              hasTop: false,
              hasBottom: false,
              topInfo: null,
              bottomInfo: null,
            };

            // 检测顶部枢轴点：中心点比前后值都高
            let isTop = true;
            let isBottom = true;

            // 在数组中进行比较，检查中心点是否比前后都有高或低
            for (let i = 0; i < histArray.length; i++) {
              if (i === centerIndex) continue; // 跳过中心点

              const histValue = histArray[i];

              // 顶部检测：中心点必须比所有其他点都高
              if (
                histValue >= centerValue ||
                histValue < 0 ||
                centerValue < 0
              ) {
                isTop = false;
              }
              // 底部检测：中心点必须比所有其他点都低
              if (
                histValue <= centerValue ||
                histValue > 0 ||
                centerValue > 0
              ) {
                isBottom = false;
              }
              // 如果既不是顶部也不是底部，可以提前退出
              if (!isTop && !isBottom) {
                break;
              }
            }

            // 设置顶部枢轴点信息
            if (isTop) {
              result.hasTop = true;
              result.topInfo = {
                hist: centerValue,
                price: centerHighPrice,
              };
            }

            // 设置底部枢轴点信息
            if (isBottom) {
              result.hasBottom = true;
              result.bottomInfo = {
                hist: centerValue,
                price: centerLowPrice,
              };
            }

            return result;
          };

          /**
           * 背离检测管理器
           * 用于记录和管理枢轴点，检测背离
           */
          this.divergenceManager = {
            // 记录顶部的枢轴点
            tops: [],
            // 记录底部的枢轴点
            bottoms: [],

            /**
             * 添加顶部枢轴点
             * @param {number} price - 价格
             * @param {number} hist - MACD值
             */
            addTop: function (price, hist) {
              this.tops.push({
                price: price,
                hist: hist,
              });

              // 只保留最近的几个顶部点，避免内存占用过多
              if (this.tops.length > 10) {
                this.tops.shift();
              }
            },

            /**
             * 添加底部枢轴点
             * @param {number} price - 价格
             * @param {number} hist - MACD值
             */
            addBottom: function (price, hist) {
              this.bottoms.push({
                price: price,
                hist: hist,
              });

              // 只保留最近的几个底部点，避免内存占用过多
              if (this.bottoms.length > 10) {
                this.bottoms.shift();
              }
            },

            /**
             * 检测顶背离
             * @param {number} currentPrice - 当前价格
             * @param {number} currentHist - 当前HIST值
             * @returns {boolean} 是否存在顶背离
             */
            checkBearishDivergence: function (currentPrice, currentHist) {
              if (this.tops.length < 2) return false;

              const lastTop = this.tops[this.tops.length - 2];

              // 价格创新高但MACD不创新高
              const priceHigher = currentPrice > lastTop.price;
              const histLower = currentHist < lastTop.hist;

              return priceHigher && histLower;
            },

            /**
             * 检测底背离
             * @param {number} currentPrice - 当前价格
             * @param {number} currentHist - 当前HIST值
             * @returns {boolean} 是否存在底背离
             */
            checkBullishDivergence: function (currentPrice, currentHist) {
              if (this.bottoms.length < 2) return false;

              const lastBottom = this.bottoms[this.bottoms.length - 2];

              // 价格创新低但MACD不创新低
              const priceLower = currentPrice < lastBottom.price;
              const histHigher = currentHist > lastBottom.hist;

              return priceLower && histHigher;
            },
          };

          this.init = function (context, inputCallback) {
            // 初始化
          };

          this.main = function (context, inputCallback) {
            this._context = context;
            this._input = inputCallback;

            // 获取输入参数
            const fast_length = this._input(0);
            const slow_length = this._input(1);
            const signal_length = this._input(2);
            const plotGold = this._input(3);
            const plotDead = this._input(4);
            const plotBull = this._input(5);
            const plotBear = this._input(6);

            // 获取价格数据
            const h = this._context.new_var(PineJS.Std.high(this._context));
            const l = this._context.new_var(PineJS.Std.low(this._context));
            const c = this._context.new_var(PineJS.Std.close(this._context));

            // 计算MACD
            const fast_ma = PineJS.Std.ema(c, fast_length, this._context);
            const slow_ma = PineJS.Std.ema(c, slow_length, this._context);
            const macd = this._context.new_var(fast_ma - slow_ma);
            const signal = this._context.new_var(
              PineJS.Std.ema(macd, signal_length, this._context)
            );
            const hist = this._context.new_var(macd - signal);

            // 使用简化的金叉死叉检测
            const crossResult = this.detectCross(macd, signal, this._context);
            const crossGold = crossResult.crossGold;
            const crossDead = crossResult.crossDead;

            // // 直接标记金叉死叉，不使用复杂的范围判断
            const crossJudgeGold = plotGold && crossGold ? macd.get(0) : NaN;
            const crossJudgeDead = plotDead && crossDead ? macd.get(0) : NaN;

            // ==================== 背离检测逻辑 ====================
            /**
             * 背离检测的核心逻辑：
             * 1. 检测MACD的枢轴点（pivotlow和pivothigh）
             * 2. 记录枢轴点的位置、价格和MACD值
             * 3. 比较新出现的枢轴点与之前的枢轴点
             * 4. 检测价格与MACD的背离情况
             */

            // 检测MACD的枢轴点
            const pivotResult = this.checkPivotPoints(
              hist,
              h,
              l,
              this._context
            );

            let bullShape = NaN; // 底背离标识
            let bearShape = NaN; // 顶背离标识

            // 检测并记录枢轴低点
            if (pivotResult.hasBottom) {
              const bottomInfo = pivotResult.bottomInfo;

              // 记录底部枢轴点
              this.divergenceManager.addBottom(
                bottomInfo.price,
                bottomInfo.hist
              );

              // 检测底背离
              if (
                plotBull &&
                this.divergenceManager.checkBullishDivergence(
                  bottomInfo.price,
                  bottomInfo.hist
                )
              ) {
                // 在MACD不创新低的位置标记底背离
                bullShape = bottomInfo.hist;
              }
            }

            // 检测并记录枢轴高点
            if (pivotResult.hasTop) {
              const topInfo = pivotResult.topInfo;

              // 记录顶部枢轴点
              this.divergenceManager.addTop(topInfo.price, topInfo.hist);

              // 检测顶背离
              if (
                plotBear &&
                this.divergenceManager.checkBearishDivergence(
                  topInfo.price,
                  topInfo.hist
                )
              ) {
                // 在MACD不创新高的位置标记顶背离
                bearShape = topInfo.hist;
              }
            }

            // 返回所有指标值
            return [
              hist.get(0), // 0: 直方图
              macd.get(0), // 1: MACD线
              signal.get(0), // 2: 信号线
              crossJudgeGold, // 3: 金叉标识
              crossJudgeDead, // 4: 死叉标识
              bullShape, // 5: 底背离标识
              bearShape, // 6: 顶背离标识
            ];
          };
        },
      };
    },
  };
})();
