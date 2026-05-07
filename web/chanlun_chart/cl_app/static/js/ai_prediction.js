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
    container.ai_pred_levels = container.ai_pred_levels || [];
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

  function classificationClasses(completeClassification) {
    if (
      completeClassification &&
      Array.isArray(completeClassification.classes)
    ) {
      return completeClassification.classes;
    }
    return [];
  }

  function classColor(predictionClass, index) {
    const colors = {
      up: "#16A085",
      down: "#C0392B",
      range: "#8E44AD",
      shock: "#8E44AD",
    };
    return (
      predictionClass.color ||
      colors[predictionClass.direction] ||
      [CHART_CONFIG.COLORS.AI_PRED, "#2874A6", "#D68910"][index % 3]
    );
  }

  function classLabel(predictionClass) {
    const probability = Math.round(Number(predictionClass.probability || 0) * 100);
    const parts = [predictionClass.name + " " + probability + "%"];
    return parts.join("\n");
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function panelRoot(manager) {
    if (typeof document === "undefined") return null;
    const id = manager && manager.id ? "tv_chart_container_" + manager.id : "";
    const root = id ? document.getElementById(id) : null;
    if (!root) return null;
    const position =
      typeof getComputedStyle === "function"
        ? getComputedStyle(root).position
        : root.style.position;
    if (!position || position === "static") {
      root.style.position = "relative";
    }
    return root;
  }

  function classPanelHtml(predictionClass, index, color) {
    const probability = Math.round(Number(predictionClass.probability || 0) * 100);
    const open = index === 0 ? " open" : "";
    return (
      "<details class=\"ai-prediction-class\"" +
      open +
      ">" +
      "<summary>" +
      "<span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:" +
      color +
      ";margin-right:6px;\"></span>" +
      escapeHtml(predictionClass.name || "完全分类") +
      " <strong>" +
      probability +
      "%</strong>" +
      "</summary>" +
      "<div class=\"ai-prediction-detail\">" +
      detailRow("触发", predictionClass.trigger) +
      detailRow("边界", predictionClass.boundary) +
      detailRow("应对", predictionClass.action) +
      detailRow("依据", predictionClass.basis) +
      "</div>" +
      "</details>"
    );
  }

  function detailRow(label, value) {
    if (!value) return "";
    return (
      "<div class=\"ai-prediction-row\"><b>" +
      label +
      "：</b>" +
      escapeHtml(value) +
      "</div>"
    );
  }

  function renderPanel(manager, container, completeClassification, classes) {
    const root = panelRoot(manager);
    if (!root) return;
    if (container.ai_pred_panel) {
      container.ai_pred_panel.remove();
      container.ai_pred_panel = null;
    }
    const panel = document.createElement("div");
    panel.className = "ai-prediction-panel";
    panel.style.position = "absolute";
    panel.style.top = "44px";
    panel.style.right = "66px";
    panel.style.width = "360px";
    panel.style.maxWidth = "42%";
    panel.style.maxHeight = "52%";
    panel.style.overflowY = "auto";
    panel.style.zIndex = "20";
    panel.style.padding = "10px 12px";
    panel.style.border = "1px solid rgba(142, 68, 173, 0.22)";
    panel.style.borderRadius = "6px";
    panel.style.background = "rgba(255, 255, 255, 0.92)";
    panel.style.boxShadow = "0 4px 18px rgba(0, 0, 0, 0.14)";
    panel.style.fontSize = "12px";
    panel.style.lineHeight = "1.55";
    panel.style.color = "#1F2933";
    panel.style.pointerEvents = "auto";
    panel.innerHTML =
      "<div style=\"font-weight:700;font-size:13px;margin-bottom:6px;\">AI完全分类</div>" +
      "<div style=\"margin-bottom:6px;color:#4B5563;\">" +
      escapeHtml(completeClassification.summary || "") +
      "</div>" +
      "<div style=\"margin-bottom:8px;color:#6B7280;\">" +
      escapeHtml(completeClassification.current_structure || "") +
      "</div>" +
      classes
        .map(function (predictionClass, index) {
          return classPanelHtml(predictionClass, index, classColor(predictionClass, index));
        })
        .join("");
    root.appendChild(panel);
    container.ai_pred_panel = panel;
  }

  function drawLevel(manager, container, predictionClass, level, index, color) {
    if (!level || !level.price) return;
    const firstBi =
      predictionClass.bis &&
      predictionClass.bis[0] &&
      predictionClass.bis[0].points
        ? predictionClass.bis[0]
        : null;
    const startTime =
      level.time || (firstBi && firstBi.points[0] ? firstBi.points[0].time : null);
    if (!startTime) return;
    const endTime =
      firstBi && firstBi.points[1]
        ? firstBi.points[1].time
        : startTime + 1;
    const line = {
      points: [
        { time: startTime, price: level.price },
        { time: Math.max(endTime, startTime + 1), price: level.price },
      ],
      linestyle: level.type === "invalid" ? "2" : "1",
    };
    const key = JSON.stringify({ classKey: predictionClass.key, level, index });
    container.ai_pred_levels.push({
      time: startTime,
      key,
      id: ChartUtils.createLineShape(manager.chart, line, {
        color: color,
        linewidth: 1,
        overrides: {
          transparency: level.type === "invalid" ? 35 : 55,
          linestyle: parseInt(line.linestyle) || 1,
        },
      }),
    });
    container.ai_pred_labels.push({
      time: startTime,
      key: key + "_label",
      id: ChartUtils.createShape(
        manager.chart,
        { time: startTime, price: level.price },
        {
          shape: "text",
          text: level.text || level.type || "分类边界",
          overrides: {
            color: color,
            textColor: color,
            fontsize: 11,
          },
        }
      ),
    });
  }

  function draw(manager, completeClassification) {
    if (!manager || !manager.chart) return;
    const classes = classificationClasses(completeClassification);
    if (!classes.length) return;

    clear(manager);
    const container = ensureContainer(manager);
    renderPanel(manager, container, completeClassification, classes);
    classes.forEach(function (prediction, predictionIndex) {
      const alpha = Math.max(35, 90 - predictionIndex * 20);
      const color = classColor(prediction, predictionIndex);
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
            color: color,
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
          classLabel(prediction);
        try {
          container.ai_pred_labels.push({
            time: endPoint.time,
            key: key + "_label",
            id: ChartUtils.createShape(manager.chart, endPoint, {
              shape: "balloon",
              text: labelText,
              overrides: {
                markerColor: color,
                backgroundColor: color,
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
      (prediction.levels || []).forEach(function (level, levelIndex) {
        drawLevel(manager, container, prediction, level, levelIndex, color);
      });
    });
  }

  function clear(manager) {
    if (!manager || !manager.obj_charts) return;
    Object.values(manager.obj_charts).forEach(function (container) {
      removeItems(manager, container.ai_pred_bis || []);
      removeItems(manager, container.ai_pred_labels || []);
      removeItems(manager, container.ai_pred_levels || []);
      container.ai_pred_bis = [];
      container.ai_pred_labels = [];
      container.ai_pred_levels = [];
      if (container.ai_pred_panel) {
        container.ai_pred_panel.remove();
        container.ai_pred_panel = null;
      }
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
        draw(manager, res.data[0].complete_classification);
        layer.msg("AI预测已显示");
      },
      error: function () {
        layer.msg("获取AI预测失败");
      },
    });
  }

  function predict(manager) {
    const info = symbolInfo(manager);
    var loadingIndex = layer.msg("AI预测中", { icon: 16, shade: 0.3, time: 0 });
    $.post({
      url: "/ai/predict",
      dataType: "json",
      data: info,
      success: function (res) {
        layer.close(loadingIndex);
        if (res.ok === true) {
          draw(manager, res.complete_classification);
          layer.msg("AI预测完成");
          return;
        }
        layer.msg(res.msg || "AI预测失败");
      },
      error: function () {
        layer.close(loadingIndex);
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
