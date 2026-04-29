var AIPrediction = (function () {
  const resolutionToFrequency = {
    "10S": "10s",
    "30S": "30s",
    "1": "1m",
    "2": "2m",
    "3": "3m",
    "5": "5m",
    "10": "10m",
    "15": "15m",
    "30": "30m",
    "60": "60m",
    "120": "120m",
    "180": "3h",
    "240": "4h",
    "1D": "d",
    "2D": "2d",
    "1W": "w",
    "1M": "m",
    "12M": "y",
  };

  function symbolInfo(manager) {
    const symbolInterval = manager.widget.symbolInterval();
    const symbol = symbolInterval.symbol || "";
    const parts = symbol.split(":");
    const market = parts[0] || Utils.get_market();
    const code = parts[1] || Utils.get_code();
    const frequency =
      resolutionToFrequency[String(symbolInterval.interval)] ||
      String(symbolInterval.interval);
    return { market, code, frequency };
  }

  function ensureContainer(manager) {
    const chartData = manager.getChartData();
    const symbolKey = chartData
      ? chartData.symbolKey
      : manager.widget.symbolInterval().symbol +
        "_" +
        manager.widget.symbolInterval().interval;
    const container = manager.initChartContainer(symbolKey);
    container.ai_pred_bis = container.ai_pred_bis || [];
    container.ai_pred_labels = container.ai_pred_labels || [];
    return container;
  }

  function removeItems(manager, items) {
    items.forEach(function (item) {
      try {
        item.id.then(function (_id) {
          manager.chart.removeEntity(_id);
        });
      } catch (e) {
        console.warn("Failed to remove AI prediction entity:", e);
      }
    });
  }

  function draw(manager, predictions) {
    if (!manager || !manager.chart || !Array.isArray(predictions)) return;

    clear(manager);
    const container = ensureContainer(manager);
    predictions.forEach(function (prediction, predictionIndex) {
      const alpha = Math.max(35, 90 - predictionIndex * 20);
      (prediction.bis || []).forEach(function (bi, biIndex) {
        if (!bi.points || bi.points.length !== 2) return;
        const key = JSON.stringify({ predictionIndex, biIndex, bi });
        const line = {
          points: bi.points,
          linestyle: bi.linestyle || "2",
        };
        container.ai_pred_bis.push({
          time: bi.points[0].time,
          key,
          id: ChartUtils.createLineShape(manager.chart, line, {
            color: CHART_CONFIG.COLORS.AI_PRED,
            linewidth: 2,
            overrides: {
              transparency: alpha,
              linestyle: parseInt(line.linestyle) || 2,
            },
          }),
        });

        const endPoint = bi.points[1];
        const labelText =
          bi.text ||
          prediction.name +
            " " +
            Math.round(Number(prediction.probability || 0) * 100) +
            "%";
        try {
          container.ai_pred_labels.push({
            time: endPoint.time,
            key: key + "_label",
            id: ChartUtils.createShape(manager.chart, endPoint, {
              shape: "balloon",
              text: labelText,
              overrides: {
                markerColor: CHART_CONFIG.COLORS.AI_PRED,
                backgroundColor: CHART_CONFIG.COLORS.AI_PRED,
                textColor: "#FFFFFF",
                transparency: 65,
                backgroundTransparency: 65,
                fontsize: 12,
              },
            }),
          });
        } catch (e) {
          console.warn("Failed to draw AI prediction label:", e);
        }
      });
    });
  }

  function clear(manager) {
    if (!manager || !manager.obj_charts) return;
    Object.values(manager.obj_charts).forEach(function (container) {
      removeItems(manager, container.ai_pred_bis || []);
      removeItems(manager, container.ai_pred_labels || []);
      container.ai_pred_bis = [];
      container.ai_pred_labels = [];
    });
  }

  function loadLatest(manager) {
    const info = symbolInfo(manager);
    $.get({
      url: "/ai/predict_records/" + info.market,
      dataType: "json",
      data: {
        code: info.code,
        frequency: info.frequency,
        page: 1,
        limit: 1,
      },
      success: function (res) {
        if (res.count <= 0 || res.data.length === 0) {
          layer.msg("暂无AI预测记录");
          return;
        }
        draw(manager, res.data[0].predictions);
        layer.msg("AI预测已显示");
      },
      error: function () {
        layer.msg("获取AI预测失败");
      },
    });
  }

  function predict(manager) {
    const info = symbolInfo(manager);
    layer.msg("AI预测中...");
    $.post({
      url: "/ai/predict",
      dataType: "json",
      data: info,
      success: function (res) {
        if (res.ok === true) {
          draw(manager, res.predictions);
          layer.msg("AI预测完成");
          return;
        }
        layer.msg(res.msg || "AI预测失败");
      },
      error: function () {
        layer.msg("AI预测失败，查看控制台确认错误");
      },
    });
  }

  function deleteLatest(manager) {
    const info = symbolInfo(manager);
    $.get({
      url: "/ai/predict_records/" + info.market,
      dataType: "json",
      data: {
        code: info.code,
        frequency: info.frequency,
        page: 1,
        limit: 1,
      },
      success: function (res) {
        if (res.count <= 0 || res.data.length === 0) {
          layer.msg("暂无可删除AI预测记录");
          return;
        }
        $.post({
          url: "/ai/predict_del/" + info.market + "/" + res.data[0].id,
          dataType: "json",
          success: function (delRes) {
            if (delRes.ok === true) {
              clear(manager);
              layer.msg("AI预测已删除");
              return;
            }
            layer.msg("AI预测删除失败");
          },
        });
      },
      error: function () {
        layer.msg("获取AI预测记录失败");
      },
    });
  }

  return {
    predict,
    loadLatest,
    clear,
    deleteLatest,
    draw,
  };
})();

window.AIPrediction = AIPrediction;
