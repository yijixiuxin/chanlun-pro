// === 回测学习记录弹框 ===
(function () {
  "use strict";

  function toNumber(value) {
    const num = Number(value);
    return isNaN(num) ? 0 : num;
  }

  function fmtMoney(value) {
    return "¥" + toNumber(value).toFixed(2);
  }

  function fmtSignedMoney(value) {
    const num = toNumber(value);
    const sign = num > 0 ? "+" : num < 0 ? "-" : "";
    return sign + "¥" + Math.abs(num).toFixed(2);
  }

  function fmtPct(value) {
    return toNumber(value).toFixed(2) + "%";
  }

  function fmtSignedPct(value) {
    const num = toNumber(value);
    const sign = num > 0 ? "+" : num < 0 ? "-" : "";
    return sign + Math.abs(num).toFixed(2) + "%";
  }

  function profitClass(value) {
    const num = toNumber(value);
    return num > 0 ? "positive" : num < 0 ? "negative" : "";
  }

  function fmtFrequency(freq) {
    if (!freq) return "--";
    return freq === "d/w" ? "日线 / 周线" : freq;
  }

  function escapeHtml(text) {
    return String(text === undefined || text === null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // 顶部学习统计
  function renderStats(stats) {
    $("#bt-stat-count").text(toNumber(stats.total_count));
    $("#bt-stat-winrate").text(
      toNumber(stats.total_close_count) > 0
        ? fmtPct(stats.trade_win_rate)
        : "--"
    );
    $("#bt-stat-profit")
      .text(fmtSignedMoney(stats.total_profit))
      .removeClass("positive negative")
      .addClass(profitClass(stats.total_profit));
    $("#bt-stat-avg-rate")
      .text(fmtSignedPct(stats.avg_profit_rate))
      .removeClass("positive negative")
      .addClass(profitClass(stats.avg_profit_rate));
    $("#bt-stat-sessions").text(
      toNumber(stats.profit_count) + " / " + toNumber(stats.loss_count)
    );
    $("#bt-stat-trades").text(
      toNumber(stats.total_close_count) + " / " + toNumber(stats.total_win_count)
    );
  }

  // 单次回测详情
  function renderDetail(record, trades) {
    $("#bt-detail-hint").text(
      `${record.dt}  ${record.stock_name} ${record.stock_code}  ${fmtFrequency(
        record.frequency
      )}`
    );

    const items = [
      { label: "回测区间", value: `${record.start_time} ~ ${record.end_time}` },
      { label: "初始资金", value: fmtMoney(record.initial_capital) },
      { label: "期末资金", value: fmtMoney(record.final_capital) },
      { label: "持仓市值", value: fmtMoney(record.position_value) },
      {
        label: "浮动盈亏",
        value: fmtSignedMoney(record.unrealized_pnl),
        cls: profitClass(record.unrealized_pnl),
      },
      { label: "总资产", value: fmtMoney(record.total_value) },
      {
        label: "总盈亏",
        value: fmtSignedMoney(record.profit),
        cls: profitClass(record.profit),
      },
      {
        label: "收益率",
        value: fmtSignedPct(record.profit_rate),
        cls: profitClass(record.profit_rate),
      },
      { label: "交易操作", value: toNumber(record.total_trades) + " 次" },
      { label: "平仓次数", value: toNumber(record.close_count) + " 次" },
      {
        label: "盈利 / 亏损",
        value: toNumber(record.win_count) + " / " + toNumber(record.lose_count),
      },
      {
        label: "交易成功率",
        value:
          toNumber(record.close_count) > 0 ? fmtPct(record.win_rate) : "--",
      },
    ];

    $("#bt-detail-cards").html(
      items
        .map(
          (item) =>
            `<div class="bt-detail-item"><span>${item.label}</span><b class="${
              item.cls || ""
            }">${escapeHtml(item.value)}</b></div>`
        )
        .join("")
    );

    renderTrades(trades);
  }

  // 交易明细使用普通表格 + 滚动容器：高度由 CSS flex 自适应弹框剩余空间，超出才滚动
  function renderTrades(trades) {
    const rows = (trades || []).map(
      (d) => `<tr>
        <td>${toNumber(d.seq) + 1}</td>
        <td>${escapeHtml(d.time)}</td>
        <td><span class="${
          d.direction === "买入"
            ? "positive"
            : d.direction === "卖出"
            ? "negative"
            : ""
        }">${escapeHtml(d.direction)}</span></td>
        <td>${fmtMoney(d.price)}</td>
        <td>${toNumber(d.qty)}</td>
        <td>${
          d.direction === "平仓"
            ? `<span class="${profitClass(d.pnl)}">${fmtSignedMoney(
                d.pnl
              )}</span>`
            : "--"
        }</td>
      </tr>`
    );

    $("#bt-detail-trades-body").html(rows.join(""));
    $("#bt-detail-trades-empty").toggle(rows.length === 0);
    $("#bt-detail-trades-wrap").scrollTop(0);
  }

  $(function () {
    layui.use(["table", "layer"], function () {
      const table = layui.table;
      const layer = layui.layer;

      function renderList(records) {
        // 列表高度随记录条数增减（上限 220），把更多高度让给下方交易明细
        const listHeight = Math.max(
          100,
          Math.min((records || []).length * 40 + 44, 220)
        );
        table.render({
          elem: "#bt-records-table",
          data: records,
          page: false,
          size: "sm",
          height: listHeight,
          className: "layui-font-12",
          lineStyle: "height: auto;",
          cols: [
            [
              { field: "dt", title: "记录时间", width: 150 },
              {
                field: "stock_name",
                title: "标的",
                width: 170,
                templet: (d) =>
                  `${escapeHtml(d.stock_name)} <span class="bt-muted">${escapeHtml(
                    d.stock_code
                  )}</span>`,
              },
              {
                field: "frequency",
                title: "周期",
                width: 90,
                templet: (d) => fmtFrequency(d.frequency),
              },
              { field: "end_time", title: "回测终点", width: 150 },
              {
                field: "total_value",
                title: "期末总资产",
                width: 120,
                templet: (d) => fmtMoney(d.total_value),
              },
              {
                field: "profit",
                title: "盈亏",
                width: 120,
                templet: (d) =>
                  `<span class="${profitClass(d.profit)}">${fmtSignedMoney(
                    d.profit
                  )}</span>`,
              },
              {
                field: "profit_rate",
                title: "收益率",
                width: 100,
                templet: (d) =>
                  `<span class="${profitClass(d.profit_rate)}">${fmtSignedPct(
                    d.profit_rate
                  )}</span>`,
              },
              { field: "close_count", title: "平仓次数", width: 90 },
              {
                field: "win_rate",
                title: "成功率",
                templet: (d) =>
                  toNumber(d.close_count) > 0 ? fmtPct(d.win_rate) : "--",
              },
            ],
          ],
        });
      }

      function loadDetail(recordId) {
        $.getJSON("/backtest/records/" + recordId, function (res) {
          if (!res.ok) {
            layer.msg(res.msg || "回测详情加载失败");
            return;
          }
          renderDetail(res.record, res.trades || []);
        }).fail(function () {
          layer.msg("回测详情加载失败");
        });
      }

      // 列表行点击查看交易详情
      table.on("row(bt-records-table)", function (obj) {
        loadDetail(obj.data.id);
      });

      $.getJSON("/backtest/records", function (res) {
        if (!res.ok) {
          layer.msg(res.msg || "回测记录加载失败");
          return;
        }
        renderStats(res.stats || {});
        const records = res.data || [];
        renderList(records);
        if (records.length > 0) {
          loadDetail(records[0].id);
        }
      }).fail(function () {
        layer.msg("回测记录加载失败");
      });
    });
  });
})();
