/**
 * 同花顺 A 股特色数据前端入口
 *
 * - 悬浮功能条（仅在 A 股市场显示）
 * - 各类特色数据弹框
 * - 点击股票切换 TradingView 图表
 */
var ThsSpecial = (function () {
  "use strict";

  var API = "/ths/special";
  var UP_COLOR = "#ff5722";
  var DOWN_COLOR = "#16baaa";
  var bound_tables = {};
  var active_layer = null;

  // ------------------------------------------------------------------
  // 基础工具
  // ------------------------------------------------------------------
  function esc(value) {
    return String(value === null || value === undefined ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function is_a_market() {
    return typeof Utils !== "undefined" && Utils.get_market() === "a";
  }

  // 统一切换弹框暗色主题 class，后续新增弹框可直接复用
  function sync_theme_class() {
    var theme = "";
    try {
      theme = (typeof Utils !== "undefined" && Utils.get_local_data("theme")) || "";
    } catch (e) {
      theme = "";
    }
    $("body").toggleClass(
      "ths-dark-theme",
      String(theme).toLowerCase() === "dark",
    );
  }

  function is_dark_theme() {
    return $("body").hasClass("ths-dark-theme");
  }

  function request(action, params) {
    return new Promise(function (resolve, reject) {
      $.ajax({
        type: "GET",
        url: API + "/" + action,
        data: params || {},
        dataType: "json",
        success: function (res) {
          if (res && res.ok) {
            resolve(res.data || {});
          } else {
            reject({
              code: res && res.code,
              message: (res && res.msg) || "同花顺数据获取失败",
            });
          }
        },
        error: function (xhr) {
          reject({
            code: -1,
            message:
              "同花顺数据请求失败（" + (xhr.status || "网络错误") + "）",
          });
        },
      });
    });
  }

  function close_active_layer() {
    if (active_layer !== null) {
      layer.close(active_layer);
      active_layer = null;
    }
  }

  function open_layer(title, html, max_width, max_height) {
    sync_theme_class();
    var width = Math.min(max_width || 1120, window.innerWidth - 32);
    var height = Math.min(window.innerHeight - 40, max_height || 900);
    close_active_layer();
    var index = layer.open({
      type: 1,
      title: title,
      content: html,
      area: [width + "px", height + "px"],
      anim: 1,
      shade: 0.25,
      shadeClose: true,
      resize: false,
      scrollbar: false,
      end: function () {
        if (active_layer === index) {
          active_layer = null;
        }
      },
    });
    active_layer = index;
    return index;
  }

  function loading_html(text) {
    return (
      '<div class="ths-loading"><i class="layui-icon layui-icon-loading layui-anim layui-anim-rotate layui-anim-loop"></i> ' +
      esc(text || "数据加载中...") +
      "</div>"
    );
  }

  function empty_html(text) {
    return '<div class="ths-empty">' + esc(text || "暂无数据") + "</div>";
  }

  function pad2(value) {
    return value < 10 ? "0" + value : String(value);
  }

  function today_str() {
    var d = new Date();
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  function days_ago_str(days) {
    var d = new Date();
    d.setDate(d.getDate() - days);
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  function normalize_date(value) {
    var raw = String(value === null || value === undefined ? "" : value);
    if (/^\d{8}$/.test(raw)) {
      return raw.slice(0, 4) + "-" + raw.slice(4, 6) + "-" + raw.slice(6);
    }
    return raw;
  }

  function fmt_pct(value, digits) {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    var n = Number(value);
    if (!isFinite(n)) {
      return "-";
    }
    return n.toFixed(digits === undefined ? 2 : digits) + "%";
  }

  function fmt_ratio(value, digits) {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    var n = Number(value);
    if (!isFinite(n)) {
      return "-";
    }
    return (n * 100).toFixed(digits === undefined ? 2 : digits) + "%";
  }

  function fmt_money(value) {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    var n = Number(value);
    if (!isFinite(n)) {
      return "-";
    }
    var abs = Math.abs(n);
    if (abs >= 100000000) {
      return (n / 100000000).toFixed(2) + "亿";
    }
    if (abs >= 10000) {
      return (n / 10000).toFixed(2) + "万";
    }
    return n.toFixed(0);
  }

  function fmt_date_ms(ms) {
    if (!ms) {
      return "-";
    }
    var d = new Date(Number(ms));
    return (
      d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate())
    );
  }

  function fmt_time_ms(ms, with_seconds) {
    if (!ms) {
      return "-";
    }
    var d = new Date(Number(ms));
    var text = pad2(d.getHours()) + ":" + pad2(d.getMinutes());
    if (with_seconds) {
      text += ":" + pad2(d.getSeconds());
    }
    return text;
  }

  function tone(value) {
    var n = Number(value);
    if (!isFinite(n) || n === 0) {
      return "";
    }
    return n > 0 ? "color:" + UP_COLOR : "color:" + DOWN_COLOR;
  }

  function tone_html(value, text, digits) {
    var content = text === undefined ? fmt_pct(value, digits) : text;
    return '<span style="' + tone(value) + '">' + esc(content) + "</span>";
  }

  function ths_to_local(code) {
    var raw = String(code || "").toUpperCase();
    var parts = raw.split(".");
    if (parts.length !== 2) {
      return raw;
    }
    if (parts[0] === "SH" || parts[0] === "SZ" || parts[0] === "BJ") {
      return raw;
    }
    return parts[1] + "." + parts[0];
  }

  function local_to_ths(code) {
    var raw = String(code || "").toUpperCase();
    var parts = raw.split(".");
    if (parts.length !== 2) {
      return raw;
    }
    if (parts[0] === "SH" || parts[0] === "SZ" || parts[0] === "BJ") {
      return parts[1] + "." + parts[0];
    }
    return raw;
  }

  function switch_stock(code, name) {
    if (!code) {
      return;
    }
    change_chart_ticker("a", code);
    $("#ai_code").val(code);
    close_active_layer();
    layer.msg((name ? esc(name) + " " : "") + "已切换图表", {
      offset: "t",
      time: 900,
    });
  }

  function stock_html(code, name) {
    var local = ths_to_local(code);
    return (
      '<span class="ths-stock" data-code="' +
      esc(local) +
      '" data-name="' +
      esc(name || "") +
      '"><b>' +
      esc(name || code || "-") +
      "</b></span>"
    );
  }

  function stock_cell(d) {
    var sub = d.ticker;
    if (!sub && d.thscode) {
      sub = String(d.thscode).split(".")[0];
    }
    return (
      '<div class="ths-stock" data-code="' +
      esc(d.__local_code) +
      '" data-name="' +
      esc(d.__name) +
      '">' +
      "<b>" +
      esc(d.name || "-") +
      '</b><span class="ths-sub">' +
      esc(sub || "") +
      "</span></div>"
    );
  }

  function with_codes(items) {
    return (items || []).map(function (item) {
      var row = $.extend({}, item);
      row.__local_code = ths_to_local(item.thscode || "");
      row.__name = item.name || item.stock_name || "";
      return row;
    });
  }

  function table_height() {
    return Math.max(280, Math.min(660, Math.round(window.innerHeight * 0.6)));
  }

  function render_table(elem, id, data, cols, limit) {
    var page_size = limit || 30;
    layui.table.render({
      elem: elem,
      id: id,
      data: data,
      page: data.length > page_size,
      limit: page_size,
      limits: [20, 30, 50, 100, 200],
      size: "sm",
      skin: "row",
      even: true,
      className: "layui-font-12",
      maxHeight: table_height(),
      text: { none: "暂无数据" },
      cols: [cols],
    });
  }

  function bind_row(filter) {
    if (bound_tables[filter]) {
      return;
    }
    layui.table.on("row(" + filter + ")", function (obj) {
      if (obj.data && obj.data.__local_code) {
        switch_stock(obj.data.__local_code, obj.data.__name);
      }
    });
    bound_tables[filter] = true;
  }

  function render_pager(selector, page, has_next, total, on_change) {
    var $el = $(selector);
    if (!$el.length) {
      return;
    }
    $el.html(
      '<button type="button" class="layui-btn layui-btn-primary layui-btn-xs ths-page-prev" ' +
        (page <= 1 ? "disabled" : "") +
        ">上一页</button>" +
        '<span class="ths-page-info">第 ' +
        page +
        " 页" +
        (total === null || total === undefined ? "" : " · 共 " + total + " 条") +
        "</span>" +
        '<button type="button" class="layui-btn layui-btn-primary layui-btn-xs ths-page-next" ' +
        (!has_next ? "disabled" : "") +
        ">下一页</button>",
    );
    $el.find(".ths-page-prev").on("click", function () {
      on_change(page - 1);
    });
    $el.find(".ths-page-next").on("click", function () {
      on_change(page + 1);
    });
  }

  function set_active_tab($wrap, id) {
    $wrap.find(".ths-tab").removeClass("active");
    $wrap.find('.ths-tab[data-id="' + id + '"]').addClass("active");
  }

  function table_error(selector, message) {
    $(selector).html(
      '<div class="ths-empty ths-error">' + esc(message || "数据获取失败") + "</div>",
    );
  }

  // ------------------------------------------------------------------
  // 涨跌停与炸板
  // ------------------------------------------------------------------
  function render_limit_stats(stats) {
    var seal_rate =
      stats.seal_rate === null || stats.seal_rate === undefined
        ? "-"
        : (Number(stats.seal_rate) * 100).toFixed(2) + "%";
    var cards =
      '<div class="ths-stat-cards">' +
      '<div class="ths-stat-card"><span class="ths-stat-value">' +
      esc(seal_rate) +
      '</span><span class="ths-stat-label">封板率</span></div>' +
      '<div class="ths-stat-card"><span class="ths-stat-value">' +
      esc(stats.limit_up_stock_count === undefined ? "-" : stats.limit_up_stock_count) +
      '</span><span class="ths-stat-label">涨停家数</span></div>' +
      '<div class="ths-stat-card"><span class="ths-stat-value">' +
      esc(stats.open_stock_count === undefined ? "-" : stats.open_stock_count) +
      '</span><span class="ths-stat-label">炸板家数</span></div>' +
      '<div class="ths-stat-card"><span class="ths-stat-value">' +
      esc(
        stats.touched_limit_up_stock_count === undefined
          ? "-"
          : stats.touched_limit_up_stock_count,
      ) +
      '</span><span class="ths-stat-label">触板家数</span></div>' +
      '<div class="ths-stat-card"><span class="ths-stat-value">' +
      esc(normalize_date(stats.trade_date)) +
      '</span><span class="ths-stat-label">交易日</span></div>' +
      "</div>";

    var boards = stats.page_boards || [];
    var labels = {
      one: "首板",
      two: "2板",
      three: "3板",
      four: "4板",
      five_plus: "5板+",
    };
    var max_count = 1;
    boards.forEach(function (b) {
      max_count = Math.max(max_count, Number(b.stock_count) || 0);
    });
    var bars = boards
      .map(function (b) {
        var width = Math.round(((Number(b.stock_count) || 0) / max_count) * 100);
        var rate =
          b.promotion_rate === null || b.promotion_rate === undefined
            ? ""
            : " · 晋级 " + (Number(b.promotion_rate) * 100).toFixed(0) + "%";
        return (
          '<div class="ths-board-bar">' +
          '<span class="ths-board-bar-label">' +
          esc(labels[b.board_level] || b.board_level) +
          "</span>" +
          '<div class="ths-board-bar-track"><i style="width:' +
          Math.max(width, b.stock_count ? 4 : 0) +
          '%"></i></div>' +
          '<span class="ths-board-bar-value">' +
          esc(b.stock_count) +
          " 家" +
          esc(rate) +
          "</span></div>"
        );
      })
      .join("");

    $("#ths-limit-stats").html(
      cards + '<div class="ths-board-bars">' + bars + "</div>",
    );
  }

  function limit_cols(kind) {
    if (kind === "up") {
      return [
        {
          field: "name",
          title: "股票",
          width: 150,
          templet: stock_cell,
        },
        {
          field: "price_change_ratio_pct",
          title: "涨幅",
          width: 82,
          templet: function (d) {
            return tone_html(d.price_change_ratio_pct);
          },
        },
        { field: "continue_day_text", title: "连板", width: 90 },
        { field: "limit_up_time", title: "涨停时间", width: 86 },
        {
          field: "seal_money",
          title: "封单额",
          width: 100,
          templet: function (d) {
            return esc(fmt_money(d.seal_money));
          },
        },
        {
          field: "open_times",
          title: "开板",
          width: 64,
          templet: function (d) {
            return d.open_times === null || d.open_times === undefined
              ? "-"
              : d.open_times;
          },
        },
        {
          field: "limit_up_reason",
          title: "涨停原因",
          minWidth: 240,
          templet: function (d) {
            return esc(d.limit_up_reason || "-");
          },
        },
      ];
    }
    if (kind === "down") {
      return [
        { field: "name", title: "股票", width: 160, templet: stock_cell },
        {
          field: "price_change_ratio_pct",
          title: "涨幅",
          width: 90,
          templet: function (d) {
            return tone_html(d.price_change_ratio_pct);
          },
        },
        { field: "first_limit_time", title: "首次跌停", width: 100 },
        { field: "last_limit_time", title: "最后跌停", width: 100 },
        {
          field: "turnover_ratio_pct",
          title: "换手率",
          width: 100,
          templet: function (d) {
            return esc(fmt_pct(d.turnover_ratio_pct));
          },
        },
        {
          field: "last_price",
          title: "最新价",
          minWidth: 100,
          templet: function (d) {
            return esc(d.last_price);
          },
        },
      ];
    }
    return [
      { field: "name", title: "股票", width: 160, templet: stock_cell },
      {
        field: "price_change_ratio_pct",
        title: "涨幅",
        width: 90,
        templet: function (d) {
          return tone_html(d.price_change_ratio_pct);
        },
      },
      { field: "open_times", title: "开板次数", width: 96 },
      {
        field: "first_seal_time",
        title: "首次封板",
        width: 100,
        templet: function (d) {
          return esc(d.first_seal_time || "-");
        },
      },
      {
        field: "last_seal_time",
        title: "最后封板",
        width: 100,
        templet: function (d) {
          return esc(d.last_seal_time || "-");
        },
      },
      {
        field: "turnover_ratio_pct",
        title: "换手率",
        width: 90,
        templet: function (d) {
          return esc(fmt_pct(d.turnover_ratio_pct));
        },
      },
      {
        field: "turnover",
        title: "成交额",
        minWidth: 110,
        templet: function (d) {
          return esc(fmt_money(d.turnover));
        },
      },
    ];
  }

  function open_limit_pool() {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div id="ths-limit-stats">' +
        loading_html("正在获取涨停统计...") +
        "</div>" +
        '<div class="ths-toolbar">' +
        '<div class="ths-tabs" id="ths-limit-tabs">' +
        '<button type="button" class="ths-tab active" data-id="up">涨停池</button>' +
        '<button type="button" class="ths-tab" data-id="down">跌停池</button>' +
        '<button type="button" class="ths-tab" data-id="break">炸板池</button>' +
        "</div>" +
        '<div class="ths-toolbar-right">' +
        '<label class="ths-field">交易日 <input type="date" id="ths-limit-date" class="ths-date-input"></label>' +
        '<button type="button" class="layui-btn layui-btn-primary layui-btn-sm" id="ths-limit-refresh" title="刷新"><i class="layui-icon layui-icon-refresh"></i></button>' +
        "</div>" +
        "</div>" +
        '<div class="ths-table-wrap" id="ths-limit-table-wrap">' +
        '<table id="ths-limit-table" lay-filter="ths-limit-table"></table>' +
        "</div>" +
        "</div>";
      open_layer("同花顺 · 涨跌停与炸板", html, 1180);

      var state = { kind: "up", date: "" };
      var $wrap = $("#ths-limit-table-wrap");

      function load_pool() {
        var params = { kind: state.kind, size: 200 };
        if (state.date) {
          params.date = state.date;
        }
        request("limit_pool", params)
          .then(function (data) {
            var rows = with_codes(data.item);
            $wrap.html(
              '<table id="ths-limit-table" lay-filter="ths-limit-table"></table>',
            );
            render_table(
              "#ths-limit-table",
              "ths-limit-table",
              rows,
              limit_cols(state.kind),
              30,
            );
            bind_row("ths-limit-table");
          })
          .catch(function (err) {
            table_error("#ths-limit-table-wrap", err.message);
          });
      }

      function load_stats(keep_date) {
        var params = {};
        if (state.date) {
          params.date = state.date;
        }
        request("limit_statistics", params)
          .then(function (stats) {
            if (!keep_date && stats.trade_date) {
              state.date = normalize_date(stats.trade_date);
              $("#ths-limit-date").val(state.date);
            }
            render_limit_stats(stats);
            load_pool();
          })
          .catch(function (err) {
            $("#ths-limit-stats").html(
              '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
            );
            load_pool();
          });
      }

      $("#ths-limit-tabs").on("click", ".ths-tab", function () {
        state.kind = $(this).data("id");
        set_active_tab($("#ths-limit-tabs"), state.kind);
        load_pool();
      });

      $("#ths-limit-date").on("change", function () {
        state.date = $(this).val();
        load_stats(true);
      });

      $("#ths-limit-refresh").on("click", function () {
        load_stats(false);
      });

      load_stats(false);
    });
  }

  // ------------------------------------------------------------------
  // 连板天梯
  // ------------------------------------------------------------------
  function render_ladder(data, date) {
    var items = data.item || [];
    var current = null;
    for (var i = 0; i < items.length; i++) {
      if (normalize_date(items[i].date) === date) {
        current = items[i];
        break;
      }
    }
    if (!current) {
      $("#ths-ladder-body").html(empty_html("该交易日暂无连板天梯数据"));
      return;
    }

    var boards = [
      { key: "two_board", label: "2板" },
      { key: "three_board", label: "3板" },
      { key: "four_board", label: "4板" },
      { key: "five_board", label: "5板" },
      { key: "six_board", label: "6板" },
      { key: "seven_over", label: "7板+" },
    ];
    var total = 0;
    var html = boards
      .map(function (board) {
        var rows = (current.boards || {})[board.key] || [];
        total += rows.length;
        var chips = rows
          .map(function (row) {
            var badge = "";
            if (row.seal_nextday === true) {
              badge = '<em class="ths-chip-badge up">晋级</em>';
            } else if (row.seal_nextday === false) {
              badge = '<em class="ths-chip-badge down">断板</em>';
            }
            return (
              '<div class="ths-ladder-chip ths-stock" data-code="' +
              esc(ths_to_local(row.thscode)) +
              '" data-name="' +
              esc(row.name) +
              '">' +
              '<span class="ths-ladder-chip-name">' +
              esc(row.name) +
              "</span>" +
              '<span class="ths-ladder-chip-meta">' +
              esc(row.ticker) +
              badge +
              "</span></div>"
            );
          })
          .join("");
        return (
          '<div class="ths-ladder-col">' +
          '<div class="ths-ladder-head">' +
          esc(board.label) +
          '<span class="ths-ladder-count">' +
          rows.length +
          "</span></div>" +
          '<div class="ths-ladder-list">' +
          (chips || '<div class="ths-ladder-none">—</div>') +
          "</div></div>"
        );
      })
      .join("");

    $("#ths-ladder-body").html(
      '<div class="ths-ladder-summary">' +
        esc(date) +
        " · 连板 " +
        total +
        " 家（点击股票切换图表）</div>" +
        '<div class="ths-ladder">' +
        html +
        "</div>",
    );
  }

  function open_ladder() {
    var html =
      '<div class="ths-popup">' +
      '<div class="ths-toolbar">' +
      '<div class="ths-note">近 30 个交易日连板梯队</div>' +
      '<div class="ths-toolbar-right">' +
      '<label class="ths-field">交易日 <select id="ths-ladder-date" class="ths-select"></select></label>' +
      "</div></div>" +
      '<div id="ths-ladder-body">' +
      loading_html("正在获取连板天梯...") +
      "</div></div>";
    open_layer("同花顺 · 连板天梯", html, 1180);

    request("ladder")
      .then(function (data) {
        var dates = ((data.window || {}).date_list || []).map(normalize_date);
        if (!dates.length) {
          $("#ths-ladder-body").html(empty_html("暂无连板天梯数据"));
          return;
        }
        var options = dates
          .map(function (date) {
            return '<option value="' + esc(date) + '">' + esc(date) + "</option>";
          })
          .join("");
        $("#ths-ladder-date").html(options).val(dates[0]);
        render_ladder(data, dates[0]);

        $("#ths-ladder-date").on("change", function () {
          render_ladder(data, $(this).val());
        });
      })
      .catch(function (err) {
        $("#ths-ladder-body").html(
          '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 同花顺热榜
  // ------------------------------------------------------------------
  function hot_cols(is_history) {
    if (is_history) {
      return [
        { field: "rank", title: "排名", width: 80, sort: false },
        { field: "name", title: "股票", width: 180, templet: stock_cell },
        {
          field: "thscode",
          title: "代码",
          minWidth: 120,
          templet: function (d) {
            return esc(d.ticker || "");
          },
        },
      ];
    }
    return [
      { field: "rank", title: "排名", width: 76, sort: false },
      { field: "name", title: "股票", width: 180, templet: stock_cell },
      {
        field: "heat",
        title: "热度",
        width: 120,
        templet: function (d) {
          return esc(d.heat === null || d.heat === undefined ? "-" : d.heat);
        },
      },
      {
        field: "rank_change",
        title: "排名变化",
        width: 100,
        templet: function (d) {
          if (d.rank_change === null || d.rank_change === undefined) {
            return "-";
          }
          var value = Number(d.rank_change);
          var text = value > 0 ? "+" + value : String(value);
          return (
            '<span style="' +
            (value > 0 ? "color:" + UP_COLOR : value < 0 ? "color:" + DOWN_COLOR : "") +
            '">' +
            esc(text) +
            "</span>"
          );
        },
      },
      {
        field: "rank_trend",
        title: "趋势",
        minWidth: 80,
        templet: function (d) {
          if (d.rank_trend === "up") {
            return '<span style="color:' + UP_COLOR + '">▲ 上升</span>';
          }
          if (d.rank_trend === "down") {
            return '<span style="color:' + DOWN_COLOR + '">▼ 下降</span>';
          }
          if (d.rank_trend === "flat") {
            return '<span class="ths-sub">— 持平</span>';
          }
          return '<span class="ths-sub">-</span>';
        },
      },
    ];
  }

  function open_hot() {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div class="ths-toolbar">' +
        '<div class="ths-tabs" id="ths-hot-tabs">' +
        '<button type="button" class="ths-tab active" data-id="hot">热股榜</button>' +
        '<button type="button" class="ths-tab" data-id="skyrocket">飙升榜</button>' +
        '<button type="button" class="ths-tab" data-id="history">历史热股</button>' +
        "</div>" +
        '<div class="ths-toolbar-right">' +
        '<div class="ths-segment" id="ths-hot-period">' +
        '<button type="button" class="active" data-period="day">24小时</button>' +
        '<button type="button" data-period="hour">小时</button>' +
        "</div>" +
        '<label class="ths-field" id="ths-hot-date-wrap" style="display:none">日期 <input type="date" id="ths-hot-date" class="ths-date-input"></label>' +
        "</div></div>" +
        '<div class="ths-table-wrap" id="ths-hot-table-wrap">' +
        '<table id="ths-hot-table" lay-filter="ths-hot-table"></table>' +
        "</div></div>";
      open_layer("同花顺 · 热榜", html, 900);

      var state = { tab: "hot", period: "day", date: today_str() };
      $("#ths-hot-date").val(state.date);
      var $wrap = $("#ths-hot-table-wrap");

      function load() {
        $wrap.html(loading_html("正在获取热榜..."));
        var action = "hot";
        var params = {};
        var is_history = state.tab === "history";
        if (is_history) {
          action = "hot_history";
          params.date = state.date;
        } else {
          params.kind = state.tab;
          params.period = state.period;
          params.limit = 100;
        }
        request(action, params)
          .then(function (data) {
            var rows = with_codes(data.item);
            $wrap.html(
              '<table id="ths-hot-table" lay-filter="ths-hot-table"></table>',
            );
            render_table(
              "#ths-hot-table",
              "ths-hot-table",
              rows,
              hot_cols(is_history),
              30,
            );
            bind_row("ths-hot-table");
          })
          .catch(function (err) {
            table_error("#ths-hot-table-wrap", err.message);
          });
      }

      $("#ths-hot-tabs").on("click", ".ths-tab", function () {
        state.tab = $(this).data("id");
        set_active_tab($("#ths-hot-tabs"), state.tab);
        var is_history = state.tab === "history";
        $("#ths-hot-period").toggle(!is_history);
        $("#ths-hot-date-wrap").toggle(is_history);
        load();
      });

      $("#ths-hot-period").on("click", "button", function () {
        state.period = $(this).data("period");
        $("#ths-hot-period button").removeClass("active");
        $(this).addClass("active");
        load();
      });

      $("#ths-hot-date").on("change", function () {
        state.date = $(this).val();
        load();
      });

      load();
    });
  }

  // ------------------------------------------------------------------
  // 个股异动解读
  // ------------------------------------------------------------------
  function anomaly_cards(items) {
    if (!items || !items.length) {
      return empty_html("当日暂无个股异动解读（非交易日或数据尚未就绪）");
    }
    return items
      .map(function (item) {
        var keywords = (item.keyword_list || [])
          .map(function (keyword) {
            return '<span class="ths-keyword">' + esc(keyword) + "</span>";
          })
          .join("");
        return (
          '<div class="ths-anomaly-card ths-goto" data-code="' +
          esc(ths_to_local(item.thscode)) +
          '" data-name="' +
          esc(item.stock_name) +
          '">' +
          '<div class="ths-anomaly-head">' +
          '<span class="ths-anomaly-stock"><b>' +
          esc(item.stock_name || "-") +
          "</b><em>" +
          esc(item.thscode || "") +
          "</em></span>" +
          '<span class="ths-tag">' +
          esc(item.tag_name || "异动") +
          "</span></div>" +
          '<div class="ths-anomaly-content">' +
          esc(item.analysis_content || "-") +
          "</div>" +
          (keywords ? '<div class="ths-keywords">' + keywords + "</div>" : "") +
          "</div>"
        );
      })
      .join("");
  }

  function open_anomaly() {
    var tags = [
      { code: "", label: "全部" },
      { code: "LIMIT_UP", label: "涨停" },
      { code: "LIMIT_DOWN", label: "跌停" },
      { code: "SHARP_RISE", label: "大涨" },
      { code: "SHARP_FALL", label: "大跌" },
      { code: "RAPID_RALLY", label: "快速拉升" },
      { code: "RAPID_DECLINE", label: "快速下挫" },
    ];
    var html =
      '<div class="ths-popup">' +
      '<div class="ths-note">同花顺当日个股异动原因，点击卡片切换图表</div>' +
      '<div class="ths-filter" id="ths-anomaly-tags">' +
      tags
        .map(function (tag, index) {
          return (
            '<button type="button" class="ths-chip ' +
            (index === 0 ? "active" : "") +
            '" data-code="' +
            esc(tag.code) +
            '">' +
            esc(tag.label) +
            "</button>"
          );
        })
        .join("") +
      "</div>" +
      '<div class="ths-anomaly-list" id="ths-anomaly-list">' +
      loading_html("正在获取异动解读...") +
      "</div></div>";
    open_layer("同花顺 · 个股异动解读", html, 980, 620);

    function load(code) {
      $("#ths-anomaly-list").html(loading_html("正在获取异动解读..."));
      var params = {};
      if (code) {
        params.tags = code;
      }
      request("anomaly", params)
        .then(function (data) {
          $("#ths-anomaly-list").html(anomaly_cards(data.item));
        })
        .catch(function (err) {
          $("#ths-anomaly-list").html(
            '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
          );
        });
    }

    $("#ths-anomaly-tags").on("click", ".ths-chip", function () {
      $(this).siblings().removeClass("active");
      $(this).addClass("active");
      load($(this).data("code"));
    });

    load("");
  }

  function open_stock_anomaly(code, name) {
    var html =
      '<div class="ths-popup">' +
      '<div class="ths-note">' +
      esc(name || code) +
      " · 当日异动解读</div>" +
      '<div class="ths-anomaly-list" id="ths-anomaly-list">' +
      loading_html("正在获取异动解读...") +
      "</div></div>";
    open_layer(
      "同花顺 · 个股异动解读 · " + esc(name || code),
      html,
      760,
      540,
    );
    request("anomaly_stock", { codes: code })
      .then(function (data) {
        $("#ths-anomaly-list").html(
          data.item && data.item.length
            ? anomaly_cards(data.item)
            : empty_html("该股票当日暂无个股异动解读"),
        );
      })
      .catch(function (err) {
        $("#ths-anomaly-list").html(
          '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 龙虎榜
  // ------------------------------------------------------------------
  function dragon_tiger_cols() {
    return [
      { field: "name", title: "股票", width: 170, templet: stock_cell },
      {
        field: "change",
        title: "涨跌幅",
        width: 90,
        templet: function (d) {
          return tone_html(d.change, fmt_ratio(d.change));
        },
      },
      {
        field: "net_value",
        title: "净买入",
        width: 108,
        templet: function (d) {
          return (
            '<span style="' +
            tone(d.net_value) +
            '">' +
            esc(fmt_money(d.net_value)) +
            "</span>"
          );
        },
      },
      {
        field: "net_rate",
        title: "净买占比",
        width: 96,
        templet: function (d) {
          return esc(fmt_ratio(d.net_rate));
        },
      },
      {
        field: "org_net_value",
        title: "机构净买入",
        width: 110,
        templet: function (d) {
          return (
            '<span style="' +
            tone(d.org_net_value) +
            '">' +
            esc(fmt_money(d.org_net_value)) +
            "</span>"
          );
        },
      },
      { field: "hot_rank", title: "人气", width: 70 },
      {
        field: "range_days",
        title: "区间",
        width: 70,
        templet: function (d) {
          return d.range_days === 3 ? "3日榜" : "1日榜";
        },
      },
      {
        field: "limit_reason",
        title: "上榜原因",
        minWidth: 220,
        templet: function (d) {
          return esc(d.limit_reason || "-");
        },
      },
    ];
  }

  function render_hot_money(data) {
    var groups = data.hot_money_items || [];
    if (!groups.length) {
      $("#ths-dt-body").html(empty_html("该交易日暂无游资榜数据"));
      return;
    }
    var html = groups
      .map(function (group) {
        var rows = (group.rows || [])
          .map(function (row) {
            return (
              '<div class="ths-hm-row ths-goto" data-code="' +
              esc(ths_to_local(row.thscode)) +
              '" data-name="' +
              esc(row.name) +
              '">' +
              '<span class="ths-hm-stock"><b>' +
              esc(row.name) +
              '</b><em>' +
              esc(row.ticker || "") +
              "</em></span>" +
              '<span style="' +
              tone(row.change) +
              '">' +
              esc(fmt_ratio(row.change)) +
              "</span>" +
              '<span style="' +
              tone(row.hot_money_item_net_value) +
              '">净买 ' +
              esc(fmt_money(row.hot_money_item_net_value)) +
              "</span>" +
              '<span class="ths-sub">' +
              (row.range_days === 3 ? "3日榜" : "1日榜") +
              "</span></div>"
            );
          })
          .join("");
        return (
          '<div class="ths-hm-group">' +
          '<div class="ths-hm-head"><b>' +
          esc(group.name) +
          '</b><span>合计净买入 ' +
          esc(fmt_money(group.buying)) +
          "</span></div>" +
          rows +
          "</div>"
        );
      })
      .join("");
    $("#ths-dt-body").html('<div class="ths-hm-list">' + html + "</div>");
  }

  function open_dragon_tiger() {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div class="ths-toolbar">' +
        '<div class="ths-tabs" id="ths-dt-tabs">' +
        '<button type="button" class="ths-tab active" data-id="all">全部</button>' +
        '<button type="button" class="ths-tab" data-id="org">机构榜</button>' +
        '<button type="button" class="ths-tab" data-id="hot_money">游资榜</button>' +
        "</div>" +
        '<div class="ths-toolbar-right">' +
        '<label class="ths-field">交易日 <input type="date" id="ths-dt-date" class="ths-date-input"></label>' +
        "</div></div>" +
        '<div id="ths-dt-body">' +
        loading_html("正在获取龙虎榜...") +
        "</div></div>";
      open_layer("同花顺 · 龙虎榜", html, 1180);

      var state = { board: "all", date: "" };

      function load() {
        $("#ths-dt-body").html(loading_html("正在获取龙虎榜..."));
        var params = { board_type: state.board };
        if (state.date) {
          params.date = state.date;
        }
        request("dragon_tiger", params)
          .then(function (data) {
            if (!state.date && data.trade_date) {
              state.date = normalize_date(data.trade_date);
              $("#ths-dt-date").val(state.date);
            }
            if (state.board === "hot_money") {
              render_hot_money(data);
              return;
            }
            var rows = with_codes(data.stock_items);
            var body = $("#ths-dt-body");
            body.html(
              '<div class="ths-table-wrap"><table id="ths-dt-table" lay-filter="ths-dt-table"></table></div>',
            );
            render_table(
              "#ths-dt-table",
              "ths-dt-table",
              rows,
              dragon_tiger_cols(),
              30,
            );
            bind_row("ths-dt-table");
          })
          .catch(function (err) {
            table_error("#ths-dt-body", err.message);
          });
      }

      $("#ths-dt-tabs").on("click", ".ths-tab", function () {
        state.board = $(this).data("id");
        set_active_tab($("#ths-dt-tabs"), state.board);
        load();
      });

      $("#ths-dt-date").on("change", function () {
        state.date = $(this).val();
        load();
      });

      load();
    });
  }

  // ------------------------------------------------------------------
  // 个股机构交易详情 / 营业部
  // ------------------------------------------------------------------
  function dedupe_departments(items) {
    var seen = {};
    return (items || []).filter(function (item) {
      var key = item.department_id || item.name;
      if (seen[key]) {
        return false;
      }
      seen[key] = true;
      return true;
    });
  }

  function department_chips(items) {
    var list = dedupe_departments(items);
    if (!list.length) {
      return '<span class="ths-sub">暂无席位</span>';
    }
    return list
      .map(function (item) {
        return (
          '<span class="ths-dept" data-id="' +
          esc(item.department_id || "") +
          '" data-name="' +
          esc(item.name || "") +
          '">' +
          esc(item.name || item.department_id || "-") +
          "</span>"
        );
      })
      .join("");
  }

  function open_institution(code, name, date, range_days) {
    var html =
      '<div class="ths-popup">' +
      '<div class="ths-note">' +
      esc(name || code) +
      " · 机构买卖与席位明细</div>" +
      '<div id="ths-inst-body">' +
      loading_html("正在获取机构交易详情...") +
      "</div></div>";
    open_layer(
      "同花顺 · 龙虎榜机构详情 · " + esc(name || code),
      html,
      920,
      640,
    );

    var params = { code: code };
    if (date) {
      params.date = date;
    }
    if (range_days) {
      params.range_days = range_days;
    }
    request("institution", params)
      .then(function (data) {
        var items = data.item || [];
        if (!items.length) {
          $("#ths-inst-body").html(
            empty_html(
              "该股票在 " +
                (data.trade_date || "最近交易日") +
                " 暂无龙虎榜机构数据",
            ),
          );
          return;
        }
        var cards = items
          .map(function (item) {
            var title =
              (item.range_days === 3 ? "3日榜" : "当日榜") +
              " · " +
              esc(item.start_date || "") +
              (item.end_date && item.end_date !== item.start_date
                ? " ~ " + esc(item.end_date)
                : "");
            return (
              '<div class="ths-inst-card">' +
              '<div class="ths-inst-title">' +
              title +
              '<span class="ths-tag" style="' +
              tone(item.org_net_value) +
              '">净买入 ' +
              esc(fmt_money(item.org_net_value)) +
              "</span></div>" +
              '<div class="ths-inst-metrics">' +
              '<div><span>机构买入</span><b>' +
              esc(fmt_money(item.org_buy_value)) +
              "</b></div>" +
              '<div><span>机构卖出</span><b>' +
              esc(fmt_money(item.org_sell_value)) +
              "</b></div>" +
              '<div><span>机构净买入</span><b style="' +
              tone(item.org_net_value) +
              '">' +
              esc(fmt_money(item.org_net_value)) +
              "</b></div>" +
              '<div><span>净买占比</span><b>' +
              esc(fmt_ratio(item.org_net_rate)) +
              "</b></div>" +
              '<div><span>买入/卖出机构</span><b>' +
              esc(item.org_buy_num === null ? "-" : item.org_buy_num) +
              " / " +
              esc(item.org_sell_num === null ? "-" : item.org_sell_num) +
              "</b></div>" +
              "</div>" +
              '<div class="ths-inst-dept"><span class="ths-inst-dept-label">买入席位</span><div>' +
              department_chips(item.buy_departments) +
              "</div></div>" +
              '<div class="ths-inst-dept"><span class="ths-inst-dept-label">卖出席位</span><div>' +
              department_chips(item.sell_departments) +
              "</div></div>" +
              "</div>"
            );
          })
          .join("");
        $("#ths-inst-body").html(
          '<div class="ths-note">交易日：' +
            esc(data.trade_date || "-") +
            "，点击席位可查看营业部战绩</div>" +
            cards,
        );
      })
      .catch(function (err) {
        $("#ths-inst-body").html(
          '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
        );
      });
  }

  function open_department(department_id, name) {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div id="ths-dept-classic">' +
        loading_html("正在获取营业部战绩...") +
        "</div>" +
        '<div class="ths-subtitle">历史上榜记录</div>' +
        '<div id="ths-dept-history">' +
        loading_html("正在获取上榜记录...") +
        "</div></div>";
      open_layer(
        "同花顺 · 营业部 · " + esc(name || department_id),
        html,
        980,
        700,
      );

      var state = { page: 1, rows: [], has_next: false };

      function render_history() {
        layui.table.render({
          elem: "#ths-dept-table",
          id: "ths-dept-table",
          data: state.rows,
          page: false,
          size: "sm",
          skin: "row",
          even: true,
          className: "layui-font-12",
          maxHeight: table_height(),
          text: { none: "暂无上榜记录" },
          cols: [
            [
              { field: "trade_date", title: "上榜日期", width: 110 },
              {
                field: "name",
                title: "股票",
                width: 180,
                templet: stock_cell,
              },
              {
                field: "change_ratio",
                title: "涨跌幅",
                width: 90,
                templet: function (d) {
                  return tone_html(d.change_ratio, fmt_ratio(d.change_ratio));
                },
              },
              {
                field: "net_buy_amount",
                title: "净买入",
                minWidth: 120,
                templet: function (d) {
                  return (
                    '<span style="' +
                    tone(d.net_buy_amount) +
                    '">' +
                    esc(fmt_money(d.net_buy_amount)) +
                    "</span>"
                  );
                },
              },
            ],
          ],
        });
        bind_row("ths-dept-table");
      }

      function load_page(page) {
        request("department_history", { id: department_id, page: page, size: 30 })
          .then(function (data) {
            var rows = with_codes(data.item);
            state.rows = page === 1 ? rows : state.rows.concat(rows);
            state.page = page;
            state.has_next = !!(data.pagination && data.pagination.has_next);
            $("#ths-dept-history").html(
              '<table id="ths-dept-table" lay-filter="ths-dept-table"></table>' +
                '<div class="ths-more-wrap"><button type="button" class="layui-btn layui-btn-primary layui-btn-sm" id="ths-dept-more" ' +
                (state.has_next ? "" : "disabled") +
                ">" +
                (state.has_next ? "加载更多" : "已加载全部") +
                "</button></div>",
            );
            render_history();
            $("#ths-dept-more").on("click", function () {
              load_page(state.page + 1);
            });
          })
          .catch(function (err) {
            table_error("#ths-dept-history", err.message);
          });
      }

      Promise.all([
        request("department_classic", { id: department_id }),
        request("department_history", { id: department_id, page: 1, size: 30 }),
      ])
        .then(function (results) {
          var classic = results[0];
          var history = results[1];
          var record = classic.classic_record;
          $("#ths-dept-classic").html(
            '<div class="ths-dept-classic">' +
              '<div class="ths-dept-name">' +
              esc(classic.department_name || name || department_id) +
              "</div>" +
              (record
                ? '<div class="ths-dept-record">代表战绩：' +
                  stock_html(record.thscode, record.name) +
                  '<span class="ths-tag">' +
                  esc(record.board_pattern || "-") +
                  "</span></div>"
                : '<div class="ths-sub">暂无代表战绩</div>') +
              "</div>",
          );

          var rows = with_codes(history.item);
          state.rows = rows;
          state.has_next = !!(history.pagination && history.pagination.has_next);
          $("#ths-dept-history").html(
            '<table id="ths-dept-table" lay-filter="ths-dept-table"></table>' +
              '<div class="ths-more-wrap"><button type="button" class="layui-btn layui-btn-primary layui-btn-sm" id="ths-dept-more" ' +
              (state.has_next ? "" : "disabled") +
              ">" +
              (state.has_next ? "加载更多" : "已加载全部") +
              "</button></div>",
          );
          render_history();
          $("#ths-dept-more").on("click", function () {
            load_page(state.page + 1);
          });
        })
        .catch(function (err) {
          $("#ths-dept-classic").html(
            '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
          );
          table_error("#ths-dept-history", err.message);
        });
    });
  }

  // ------------------------------------------------------------------
  // 异动监控
  // ------------------------------------------------------------------
  function monitor_cols() {
    return [
      { field: "name", title: "股票", width: 190, templet: stock_cell },
      {
        field: "thscode",
        title: "代码",
        width: 120,
        templet: function (d) {
          return esc(d.thscode || "-");
        },
      },
      {
        field: "monitor_label",
        title: "监控类型",
        width: 120,
        templet: function (d) {
          return (
            '<span class="ths-tag ' +
            (String(d.monitor_label).indexOf("严重") >= 0 ? "danger" : "") +
            '">' +
            esc(d.monitor_label || "-") +
            "</span>"
          );
        },
      },
      {
        field: "start_date_ms",
        title: "开始日期",
        width: 120,
        templet: function (d) {
          return esc(fmt_date_ms(d.start_date_ms));
        },
      },
      {
        field: "end_date_ms",
        title: "结束日期",
        minWidth: 120,
        templet: function (d) {
          return d.end_date_ms ? esc(fmt_date_ms(d.end_date_ms)) : "持续中";
        },
      },
    ];
  }

  function open_abnormal_monitor() {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div class="ths-toolbar">' +
        '<div class="ths-tabs" id="ths-monitor-tabs">' +
        '<button type="button" class="ths-tab active" data-id="current">当前监控</button>' +
        '<button type="button" class="ths-tab" data-id="history">历史记录</button>' +
        "</div></div>" +
        '<div id="ths-monitor-body">' +
        loading_html("正在获取异动监控...") +
        "</div>" +
        '<div class="ths-pager" id="ths-monitor-pager"></div></div>';
      open_layer("同花顺 · 严重异动与重点监控", html, 860, 620);

      var state = { state: "current", page: 1 };

      function load(page) {
        state.page = page;
        $("#ths-monitor-body").html(loading_html("正在获取异动监控..."));
        request("abnormal_monitor", {
          state: state.state,
          page: page,
          size: 30,
        })
          .then(function (data) {
            var rows = with_codes(data.item);
            $("#ths-monitor-body").html(
              '<div class="ths-table-wrap"><table id="ths-monitor-table" lay-filter="ths-monitor-table"></table></div>',
            );
            render_table(
              "#ths-monitor-table",
              "ths-monitor-table",
              rows,
              monitor_cols(),
              30,
            );
            bind_row("ths-monitor-table");
            var pager = data.pagination || {};
            render_pager(
              "#ths-monitor-pager",
              pager.page || page,
              !!pager.has_next,
              pager.total,
              load,
            );
          })
          .catch(function (err) {
            table_error("#ths-monitor-body", err.message);
            $("#ths-monitor-pager").empty();
          });
      }

      $("#ths-monitor-tabs").on("click", ".ths-tab", function () {
        state.state = $(this).data("id");
        set_active_tab($("#ths-monitor-tabs"), state.state);
        load(1);
      });

      load(1);
    });
  }

  // ------------------------------------------------------------------
  // 集合竞价
  // ------------------------------------------------------------------
  function auction_cols() {
    return [
      {
        field: "warn_timestamp",
        title: "时间",
        width: 96,
        templet: function (d) {
          return esc(fmt_time_ms(d.warn_timestamp, true));
        },
      },
      { field: "name", title: "股票", width: 170, templet: stock_cell },
      {
        field: "warn_name",
        title: "机会类型",
        width: 110,
        templet: function (d) {
          return '<span class="ths-tag">' + esc(d.warn_name || d.warn_type) + "</span>";
        },
      },
      {
        field: "bid_change_ratio",
        title: "竞价涨幅",
        width: 92,
        templet: function (d) {
          return tone_html(d.bid_change_ratio, fmt_ratio(d.bid_change_ratio));
        },
      },
      {
        field: "bid_price",
        title: "竞价",
        width: 84,
        templet: function (d) {
          return esc(d.bid_price === null ? "-" : d.bid_price);
        },
      },
      {
        field: "bid_amount",
        title: "竞价额",
        width: 100,
        templet: function (d) {
          return esc(fmt_money(d.bid_amount));
        },
      },
      {
        field: "grade",
        title: "评级",
        width: 70,
        templet: function (d) {
          return esc(d.grade || "-");
        },
      },
      {
        field: "explain",
        title: "说明",
        minWidth: 120,
        templet: function (d) {
          return esc(d.explain || "-");
        },
      },
    ];
  }

  function open_auction() {
    layui.use(["table"], function () {
      var html =
        '<div class="ths-popup">' +
        '<div class="ths-toolbar">' +
        '<div class="ths-note" id="ths-auction-note">集合竞价个股机会</div>' +
        '<div class="ths-toolbar-right">' +
        '<label class="ths-field">交易日 <select id="ths-auction-date" class="ths-select"></select></label>' +
        "</div></div>" +
        '<div id="ths-auction-body">' +
        loading_html("正在获取集合竞价数据...") +
        "</div>" +
        '<div class="ths-pager" id="ths-auction-pager"></div></div>';
      open_layer("同花顺 · 集合竞价机会", html, 1080);

      var state = { date: "", page: 1 };

      function load(page) {
        state.page = page;
        $("#ths-auction-body").html(loading_html("正在获取集合竞价数据..."));
        var params = { page: page, size: 20 };
        if (state.date) {
          params.date = state.date;
        }
        request("auction", params)
          .then(function (data) {
            var rows = with_codes(data.item);
            if (!state.date && data.trade_date) {
              state.date = normalize_date(data.trade_date);
              $("#ths-auction-date").val(state.date);
            }
            $("#ths-auction-note").html(
              "交易日 " +
                esc(data.trade_date || state.date || "-") +
                ' <span class="ths-tag">' +
                (data.mode === "real_time" ? "实时" : "历史") +
                "</span> · 点击股票切换图表",
            );
            $("#ths-auction-body").html(
              '<div class="ths-table-wrap"><table id="ths-auction-table" lay-filter="ths-auction-table"></table></div>',
            );
            render_table(
              "#ths-auction-table",
              "ths-auction-table",
              rows,
              auction_cols(),
              20,
            );
            bind_row("ths-auction-table");
            var pager = data.pagination || {};
            render_pager(
              "#ths-auction-pager",
              pager.page || page,
              !!pager.has_next,
              pager.total,
              load,
            );
          })
          .catch(function (err) {
            table_error("#ths-auction-body", err.message);
            $("#ths-auction-pager").empty();
          });
      }

      request("auction_dates")
        .then(function (data) {
          var dates = (data.dates || []).slice().reverse().map(normalize_date);
          if (!dates.length) {
            $("#ths-auction-body").html(empty_html("暂无集合竞价可选日期"));
            return;
          }
          var options = dates
            .map(function (date) {
              return '<option value="' + esc(date) + '">' + esc(date) + "</option>";
            })
            .join("");
          state.date = normalize_date(data.latest_date) || dates[0];
          $("#ths-auction-date").html(options).val(state.date);
          load(1);
          $("#ths-auction-date").on("change", function () {
            state.date = $(this).val();
            load(1);
          });
        })
        .catch(function (err) {
          $("#ths-auction-body").html(
            '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
          );
        });
    });
  }

  // ------------------------------------------------------------------
  // 个股热榜排名走势
  // ------------------------------------------------------------------
  function rank_chart(items) {
    var points = (items || [])
      .filter(function (item) {
        return Number(item.rank) > 0;
      })
      .slice()
      .sort(function (a, b) {
        return String(a.date).localeCompare(String(b.date));
      });
    if (!points.length) {
      return empty_html("近 30 天暂无热榜排名数据");
    }

    var width = 860;
    var height = 300;
    var pad_l = 76;
    var pad_r = 60;
    var pad_t = 24;
    var pad_b = 42;
    var ranks = points.map(function (item) {
      return Number(item.rank);
    });
    var min_rank = Math.min.apply(null, ranks);
    var max_rank = Math.max.apply(null, ranks);
    if (min_rank === max_rank) {
      min_rank = Math.max(1, min_rank - 1);
      max_rank = max_rank + 1;
    }
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;

    function x(index) {
      if (points.length === 1) {
        return pad_l + plot_w / 2;
      }
      return pad_l + (index * plot_w) / (points.length - 1);
    }

    function y(rank) {
      return pad_t + ((rank - min_rank) / (max_rank - min_rank)) * plot_h;
    }

    var line = points
      .map(function (item, index) {
        return (index === 0 ? "M" : "L") + x(index) + " " + y(item.rank);
      })
      .join(" ");
    var dots = points
      .map(function (item, index) {
        return (
          '<circle cx="' +
          x(index) +
          '" cy="' +
          y(item.rank) +
          '" r="3.5" fill="#ff5722"><title>' +
          esc(item.date) +
          " 第 " +
          esc(item.rank) +
          " 名</title></circle>"
        );
      })
      .join("");
    var labels = [0, Math.floor((points.length - 1) / 2), points.length - 1];
    var x_labels = labels
      .filter(function (value, index, array) {
        return array.indexOf(value) === index;
      })
      .map(function (index) {
        return (
          '<text x="' +
          x(index) +
          '" y="' +
          (height - 16) +
          '" text-anchor="middle" fill="currentColor" opacity="0.55" font-size="12">' +
          esc(points[index].date) +
          "</text>"
        );
      })
      .join("");
    var y_labels =
      '<text x="' +
      (pad_l - 10) +
      '" y="' +
      (pad_t + 4) +
      '" text-anchor="end" fill="currentColor" opacity="0.55" font-size="12">第 ' +
      min_rank +
      " 名</text>" +
      '<text x="' +
      (pad_l - 10) +
      '" y="' +
      (height - pad_b + 4) +
      '" text-anchor="end" fill="currentColor" opacity="0.55" font-size="12">第 ' +
      max_rank +
      " 名</text>";

    return (
      '<div class="ths-rank-chart"><svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" preserveAspectRatio="xMidYMid meet" role="img">' +
      '<line x1="' +
      pad_l +
      '" y1="' +
      pad_t +
      '" x2="' +
      pad_l +
      '" y2="' +
      (height - pad_b) +
      '" stroke="currentColor" opacity="0.18"/>' +
      '<line x1="' +
      pad_l +
      '" y1="' +
      (height - pad_b) +
      '" x2="' +
      (width - pad_r) +
      '" y2="' +
      (height - pad_b) +
      '" stroke="currentColor" opacity="0.18"/>' +
      '<path d="' +
      line +
      '" fill="none" stroke="#ff5722" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>' +
      dots +
      x_labels +
      y_labels +
      "</svg></div>"
    );
  }

  function open_hot_rank(code, name) {
    var html =
      '<div class="ths-popup">' +
      '<div class="ths-note">' +
      esc(name || code) +
      " · 近 30 个自然日热度排名（数值越小越靠前）</div>" +
      '<div id="ths-rank-body">' +
      loading_html("正在获取热榜排名走势...") +
      "</div></div>";
    open_layer(
      "同花顺 · 热榜排名走势 · " + esc(name || code),
      html,
      940,
      620,
    );

    request("hot_rank", {
      code: code,
      start_date: days_ago_str(30),
      end_date: today_str(),
    })
      .then(function (data) {
        var items = data.item || [];
        var ranks = items
          .map(function (item) {
            return Number(item.rank);
          })
          .filter(function (value) {
            return value > 0;
          });
        var best = ranks.length ? Math.min.apply(null, ranks) : "-";
        var sorted_items = items.slice().sort(function (a, b) {
          return String(a.date).localeCompare(String(b.date));
        });
        var latest = sorted_items.length
          ? sorted_items[sorted_items.length - 1]
          : null;
        $("#ths-rank-body").html(
          '<div class="ths-stat-cards">' +
            '<div class="ths-stat-card"><span class="ths-stat-value">' +
            esc(latest ? latest.rank : "-") +
            '</span><span class="ths-stat-label">最新排名</span></div>' +
            '<div class="ths-stat-card"><span class="ths-stat-value">' +
            esc(best) +
            '</span><span class="ths-stat-label">区间最佳</span></div>' +
            '<div class="ths-stat-card"><span class="ths-stat-value">' +
            items.length +
            '</span><span class="ths-stat-label">数据天数</span></div>' +
            "</div>" +
            rank_chart(items),
        );
      })
      .catch(function (err) {
        $("#ths-rank-body").html(
          '<div class="ths-empty ths-error">' + esc(err.message) + "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 单股财务体检
  // ------------------------------------------------------------------
  var fin_chart_key = "revenue";
  var fin_chart_data = null;

  function fin_palette() {
    if (is_dark_theme()) {
      return {
        red: "#ff6b5a",
        orange: "#ff9f5a",
        green: "#4fd1b3",
        grid: "#34312e",
        muted: "rgba(255,255,255,.55)",
        dotFill: "#232324",
        ink: "#f5efe8",
        donutBg: "#34312e",
      };
    }
    return {
      red: "#b42318",
      orange: "#c2410c",
      green: "#1f7a5c",
      grid: "#e8dfd3",
      muted: "#8b8177",
      dotFill: "#fffdf9",
      ink: "#241d18",
      donutBg: "#efe4d6",
    };
  }

  function fin_yi(value) {
    if (value === null || value === undefined || value === "") {
      return "--";
    }
    var n = Number(value);
    if (!isFinite(n)) {
      return "--";
    }
    return (n / 100000000).toFixed(2);
  }

  function fin_pct(value, signed) {
    if (value === null || value === undefined || value === "") {
      return "--";
    }
    var n = Number(value);
    if (!isFinite(n)) {
      return "--";
    }
    return (signed && n > 0 ? "+" : "") + n.toFixed(2) + "%";
  }

  function fin_indicator(item) {
    if (!item || item.value === null || item.value === undefined) {
      return "--";
    }
    var n = Number(item.value);
    if (!isFinite(n)) {
      return esc(item.value);
    }
    if (item.unit === "%") {
      return n.toFixed(2) + "%";
    }
    if (item.unit) {
      return n.toFixed(2) + esc(item.unit);
    }
    return n.toFixed(2);
  }

  function fin_value_html(value, digits) {
    var text = fin_yi(value);
    if (text === "--") {
      return "--";
    }
    if (digits !== undefined) {
      text = (Number(value) / 100000000).toFixed(digits);
    }
    var negative = Number(value) < 0;
    return (
      '<span class="' +
      (negative ? "ths-fin-neg" : "") +
      '">' +
      esc(text) +
      "</span>"
    );
  }

  function fin_chart_svg(data, key) {
    var periods = (data && data.periods) || [];
    if (!periods.length) {
      return empty_html("暂无财务数据");
    }
    var palette = fin_palette();
    var meta = {
      revenue: { label: "营业收入", color: palette.red },
      net_profit: { label: "归母净利润", color: palette.orange },
      operating_cash_flow: { label: "经营现金流", color: palette.green },
    }[key] || { label: "营业收入", color: palette.red };

    var points = [];
    periods.forEach(function (period, index) {
      var value = period[key];
      if (value !== null && value !== undefined && isFinite(Number(value))) {
        points.push({ period: period, index: index, value: Number(value) });
      }
    });
    if (!points.length) {
      return empty_html(meta.label + "暂无可绘制的数据");
    }

    var compact = window.innerWidth < 720;
    var width = compact ? 420 : 760;
    var height = compact ? 270 : 310;
    var pad_l = compact ? 48 : 62;
    var pad_r = compact ? 14 : 18;
    var pad_t = compact ? 16 : 18;
    var pad_b = compact ? 38 : 42;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var values = points.map(function (point) {
      return point.value;
    });
    var min_value = Math.min.apply(null, [0].concat(values));
    var max_value = Math.max.apply(null, [0].concat(values));
    if (min_value === max_value) {
      max_value = min_value + 1;
    }

    function x(index) {
      if (periods.length <= 1) {
        return pad_l + plot_w / 2;
      }
      return pad_l + (index * plot_w) / (periods.length - 1);
    }

    function y(value) {
      return pad_t + ((max_value - value) / (max_value - min_value)) * plot_h;
    }

    var line = points
      .map(function (point, index) {
        return (index === 0 ? "M" : "L") + x(point.index) + " " + y(point.value);
      })
      .join(" ");
    var area =
      line +
      " L" +
      x(points[points.length - 1].index) +
      " " +
      y(0) +
      " L" +
      x(points[0].index) +
      " " +
      y(0) +
      " Z";

    var grid = "";
    var tick_count = compact ? 3 : 4;
    for (var i = 0; i <= tick_count; i++) {
      var tick_value = max_value - ((max_value - min_value) * i) / tick_count;
      var tick_y = pad_t + (plot_h * i) / tick_count;
      grid +=
        '<line x1="' +
        pad_l +
        '" y1="' +
        tick_y +
        '" x2="' +
        (width - pad_r) +
        '" y2="' +
        tick_y +
        '" stroke="' +
        palette.grid +
        '" stroke-width="1"/>' +
        '<text x="' +
        (pad_l - 8) +
        '" y="' +
        (tick_y + 4) +
        '" text-anchor="end" fill="' +
        palette.muted +
        '" font-size="' +
        (compact ? 9 : 11) +
        '">' +
        esc(fin_yi(tick_value)) +
        "</text>";
    }
    grid +=
      '<text x="' +
      (pad_l - 8) +
      '" y="' +
      (pad_t - 2) +
      '" text-anchor="end" fill="' +
      palette.muted +
      '" font-size="9">亿元</text>';

    var dots = points
      .map(function (point) {
        var yoy =
          key === "revenue"
            ? point.period.revenue_yoy
            : key === "net_profit"
              ? point.period.profit_yoy
              : point.period.ocf_yoy;
        var label =
          point.period.period +
          " · " +
          meta.label +
          " " +
          fin_yi(point.value) +
          " 亿元" +
          (yoy === null || yoy === undefined
            ? ""
            : " · 同比 " + fin_pct(yoy, true));
        return (
          '<circle class="ths-fin-dot" cx="' +
          x(point.index) +
          '" cy="' +
          y(point.value) +
          '" r="4.5" fill="' +
          palette.dotFill +
          '" stroke="' +
          meta.color +
          '" stroke-width="2.2" tabindex="0" role="img" aria-label="' +
          esc(label) +
          '" data-index="' +
          point.index +
          '"><title>' +
          esc(label) +
          "</title></circle>"
        );
      })
      .join("");

    var x_labels = periods
      .map(function (period, index) {
        return (
          '<text x="' +
          x(index) +
          '" y="' +
          (height - 14) +
          '" text-anchor="middle" fill="' +
          palette.muted +
          '" font-size="' +
          (compact ? 9 : 10) +
          '">' +
          esc(period.period) +
          "</text>"
        );
      })
      .join("");

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="八期' +
      esc(meta.label) +
      '轨迹" preserveAspectRatio="xMidYMid meet">' +
      '<defs><linearGradient id="ths-fin-area" x1="0" y1="0" x2="0" y2="1">' +
      '<stop offset="0%" stop-color="' +
      meta.color +
      '" stop-opacity=".2"/>' +
      '<stop offset="100%" stop-color="' +
      meta.color +
      '" stop-opacity="0"/>' +
      "</linearGradient></defs>" +
      grid +
      '<path d="' +
      area +
      '" fill="url(#ths-fin-area)" stroke="none"/>' +
      '<path d="' +
      line +
      '" fill="none" stroke="' +
      meta.color +
      '" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"/>' +
      dots +
      x_labels +
      "</svg>"
    );
  }

  function fin_chart_tip(data, key, index) {
    var period = ((data && data.periods) || [])[index];
    if (!period) {
      return "";
    }
    var meta = {
      revenue: { label: "营业收入", field: "revenue", yoy: "revenue_yoy" },
      net_profit: { label: "归母净利润", field: "net_profit", yoy: "profit_yoy" },
      operating_cash_flow: {
        label: "经营现金流",
        field: "operating_cash_flow",
        yoy: "ocf_yoy",
      },
    }[key] || { label: "营业收入", field: "revenue", yoy: "revenue_yoy" };
    var text = period.period + " · " + meta.label + " " + fin_yi(period[meta.field]) + " 亿元";
    if (period[meta.yoy] !== null && period[meta.yoy] !== undefined) {
      text += " · 同比 " + fin_pct(period[meta.yoy], true);
    }
    return text;
  }

  function render_fin_chart(data, key) {
    fin_chart_data = data;
    $(".ths-fin-tab").removeClass("active").attr("aria-pressed", "false");
    $('.ths-fin-tab[data-key="' + key + '"]')
      .addClass("active")
      .attr("aria-pressed", "true");
    $("#ths-fin-chart").html(fin_chart_svg(data, key));
    var latest_index = ((data && data.periods) || []).length - 1;
    $("#ths-fin-chart-tip").text(fin_chart_tip(data, key, latest_index));
    $("#ths-fin-chart")
      .off(".fin")
      .on("mouseenter.fin focusin.fin click.fin", ".ths-fin-dot", function () {
        var index = Number($(this).attr("data-index"));
        $("#ths-fin-chart-tip").text(fin_chart_tip(data, key, index));
      })
      .on("mouseleave.fin", function () {
        $("#ths-fin-chart-tip").text(fin_chart_tip(data, key, latest_index));
      });
  }

  function fin_donut_html(latest) {
    var palette = fin_palette();
    var ratio = latest ? latest.debt_ratio : null;
    var pct =
      ratio === null || ratio === undefined
        ? 0
        : Math.max(0, Math.min(100, Number(ratio)));
    var radius = 66;
    var circumference = 2 * Math.PI * radius;
    var dash = (circumference * pct) / 100;
    var ratio_text =
      ratio === null || ratio === undefined
        ? "--"
        : Number(ratio).toFixed(1) + "%";
    var rows = [
      { label: "货币资金", value: latest ? latest.cash : null },
      { label: "股东权益", value: latest ? latest.equity : null },
      { label: "研发费用", value: latest ? latest.rd_expense : null },
      { label: "投资现金流", value: latest ? latest.invest_cash_flow : null },
    ]
      .map(function (row) {
        return (
          '<div class="ths-fin-mini-row"><span>' +
          esc(row.label) +
          "</span><b>" +
          fin_value_html(row.value) +
          (row.value === null || row.value === undefined ? "" : " 亿") +
          "</b></div>"
        );
      })
      .join("");

    return (
      '<div class="ths-fin-donut-wrap"><svg viewBox="0 0 180 180" role="img" aria-label="负债占比 ' +
      esc(ratio_text) +
      '">' +
      '<circle cx="90" cy="90" r="' +
      radius +
      '" fill="none" stroke="' +
      palette.donutBg +
      '" stroke-width="18"/>' +
      '<circle cx="90" cy="90" r="' +
      radius +
      '" fill="none" stroke="' +
      palette.red +
      '" stroke-width="18" stroke-linecap="butt" stroke-dasharray="' +
      dash +
      " " +
      (circumference - dash) +
      '" transform="rotate(-90 90 90)"/>' +
      '<text x="90" y="86" text-anchor="middle" fill="' +
      palette.ink +
      '" font-size="24" font-weight="700">' +
      esc(ratio_text) +
      "</text>" +
      '<text x="90" y="106" text-anchor="middle" fill="' +
      palette.muted +
      '" font-size="11">负债占比</text>' +
      "</svg></div>" +
      '<div class="ths-fin-mini-list">' +
      rows +
      "</div>"
    );
  }

  function fin_indicators_html(groups) {
    if (!groups || !groups.length) {
      return empty_html("最新报告期暂无可用的财务指标");
    }
    return (
      '<div class="ths-fin-ind-grid">' +
      groups
        .map(function (group) {
          return (
            '<div class="ths-fin-ind-group"><h4>' +
            esc(group.name) +
            "</h4>" +
            group.items
              .map(function (item) {
                var missing = item.value === null || item.value === undefined;
                return (
                  '<div class="ths-fin-ind-row"><span>' +
                  esc(item.name) +
                  '</span><b class="' +
                  (missing ? "muted" : "") +
                  '">' +
                  fin_indicator(item) +
                  "</b></div>"
                );
              })
              .join("") +
            "</div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function fin_table_html(periods) {
    var rows = (periods || []).slice().reverse();
    var body = rows
      .map(function (period, index) {
        var latest = index === 0;
        return (
          '<tr class="' +
          (latest ? "latest" : "") +
          '"><td><b>' +
          esc(period.period) +
          "</b></td><td>" +
          fin_value_html(period.revenue) +
          "</td><td>" +
          fin_value_html(period.net_profit) +
          "</td><td>" +
          fin_value_html(period.operating_cash_flow) +
          "</td><td>" +
          fin_value_html(period.total_assets) +
          "</td><td>" +
          fin_value_html(period.total_liabilities) +
          "</td><td>" +
          fin_pct(period.debt_ratio) +
          '</td><td class="' +
          (period.revenue_yoy !== null && period.revenue_yoy < 0 ? "negative" : "") +
          '">' +
          fin_pct(period.revenue_yoy, true) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<div class="ths-fin-table-scroll"><table><thead><tr>' +
      "<th>报告期</th><th>营业收入</th><th>归母净利润</th><th>经营现金流</th>" +
      "<th>总资产</th><th>总负债</th><th>资产负债率</th><th>营收同比</th>" +
      "</tr></thead><tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function render_fin_dashboard(data) {
    var stock = data.stock || {};
    var report = data.report || {};
    var latest = data.latest || {};
    var periods = data.periods || [];
    $("#ths-fin-subtitle").html(
      "<b>" +
        esc(stock.name || stock.code) +
        "</b><span>" +
        esc(stock.code || "") +
        " · " +
        esc(stock.thscode || "") +
        "</span> · 报告期 <b>" +
        esc(report.period || "--") +
        "</b>" +
        (report.report_date_ms
          ? " · 披露 " + esc(fmt_date_ms(report.report_date_ms))
          : ""),
    );

    var cards = (data.cards || [])
      .map(function (card) {
        var value = card.is_pct
          ? fin_pct(card.value)
          : fin_value_html(card.value, 2);
        var unit = card.is_pct ? "" : "亿元";
        return (
          '<div class="ths-fin-card ths-fin-kpi"><span class="ths-fin-kpi-label">' +
          esc(card.label) +
          '</span><div class="ths-fin-kpi-value">' +
          value +
          '<em>' +
          unit +
          '</em></div><div class="ths-fin-kpi-sub">' +
          esc(card.sub || "") +
          "</div></div>"
        );
      })
      .join("");

    var tabs = [
      { key: "revenue", label: "收入" },
      { key: "net_profit", label: "利润" },
      { key: "operating_cash_flow", label: "现金流" },
    ]
      .map(function (tab) {
        return (
          '<button type="button" class="ths-fin-tab' +
          (fin_chart_key === tab.key ? " active" : "") +
          '" data-key="' +
          tab.key +
          '" aria-pressed="' +
          (fin_chart_key === tab.key ? "true" : "false") +
          '">' +
          tab.label +
          "</button>"
        );
      })
      .join("");

    var summary = (data.summary || [])
      .map(function (text) {
        return "<li>" + esc(text) + "</li>";
      })
      .join("");
    var warnings = (data.warnings || [])
      .map(function (text) {
        return "<li>" + esc(text) + "</li>";
      })
      .join("");

    var html =
      '<div class="ths-fin-cards">' +
      cards +
      "</div>" +
      (warnings
        ? '<div class="ths-fin-warning"><b>数据提示</b><ul>' + warnings + "</ul></div>"
        : "") +
      '<div class="ths-fin-grid">' +
      '<section class="ths-fin-card ths-fin-chart-card">' +
      '<div class="ths-fin-card-head"><div><h3>八期财务轨迹</h3><p>季报累计值；切换不会改变报告期口径</p></div>' +
      '<div class="ths-fin-tabs" role="tablist" aria-label="财务指标切换">' +
      tabs +
      "</div></div>" +
      '<div class="ths-fin-chart" id="ths-fin-chart"></div>' +
      '<div class="ths-fin-chart-tip" id="ths-fin-chart-tip" aria-live="polite"></div>' +
      "</section>" +
      '<section class="ths-fin-card ths-fin-structure-card">' +
      '<div class="ths-fin-card-head"><div><h3>资产结构</h3><p>' +
      esc(report.period || "--") +
      " 期末值</p></div></div>" +
      fin_donut_html(latest) +
      "</section>" +
      "</div>" +
      '<section class="ths-fin-card ths-fin-summary"><div class="ths-fin-card-head"><div><h3>体检结论</h3><p>仅基于已披露报表的事实性归纳</p></div></div><ul class="ths-fin-facts">' +
      (summary || "<li>暂无可归纳的数据。</li>") +
      "</ul></section>" +
      '<section class="ths-fin-card ths-fin-indicators"><div class="ths-fin-card-head"><div><h3>关键财务指标</h3><p>最新报告期 ' +
      esc(report.report || "--") +
      "</p></div></div>" +
      fin_indicators_html(data.indicators) +
      "</section>" +
      '<section class="ths-fin-card ths-fin-table-card"><div class="ths-fin-card-head"><div><h3>报告期对照</h3><p>null 保持缺失，不补零；金额单位：亿元</p></div></div>' +
      fin_table_html(periods) +
      "</section>" +
      '<footer class="ths-fin-footer"><span>数据源：' +
      esc((data.meta || {}).source || "同花顺金融数据服务") +
      " · " +
      esc(stock.currency || "CNY") +
      " · 季报累计口径 · " +
      esc(periods.length) +
      " 期</span><span>仅供信息展示，不构成投资建议</span></footer>";

    $("#ths-fin-body").html(html);
    render_fin_chart(data, fin_chart_key);

    $(".ths-fin-tab").on("click", function () {
      var key = $(this).data("key");
      if (key === fin_chart_key) {
        return;
      }
      fin_chart_key = key;
      render_fin_chart(fin_chart_data, fin_chart_key);
    });
  }

  function open_financial_health(code, name) {
    fin_chart_key = "revenue";
    fin_chart_data = null;
    var html =
      '<div class="ths-fin" id="ths-fin">' +
      '<div class="ths-fin-hero"><div class="ths-fin-hero-left">' +
      '<div class="ths-fin-kicker">QUARTERLY FUNDAMENTALS</div>' +
      "<h2>单股财务体检</h2>" +
      '<div class="ths-fin-subtitle" id="ths-fin-subtitle">' +
      esc(name || code) +
      " · 正在加载...</div></div>" +
      '<div class="ths-fin-hero-right">单位：人民币亿元<br><span>季报累计口径，不拆解为单季值</span></div></div>' +
      '<div id="ths-fin-body">' +
      loading_html("正在获取同花顺财务报表...") +
      "</div></div>";
    open_layer(
      "同花顺 · 单股财务体检 · " + esc(name || code),
      html,
      1220,
      900,
    );

    request("financial_health", { code: code, name: name || "" })
      .then(function (data) {
        render_fin_dashboard(data);
      })
      .catch(function (err) {
        $("#ths-fin-body").html(
          '<div class="ths-empty ths-error">' +
            esc(err.message || "财务数据获取失败") +
            "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 涨停情绪市场脉冲屏
  // ------------------------------------------------------------------
  function pulse_palette() {
    if (is_dark_theme()) {
      return {
        red: "#ff6b5a",
        green: "#4fd1b3",
        redRgb: "255,107,90",
        grid: "#34312e",
        muted: "rgba(255,255,255,.55)",
        card: "#232324",
      };
    }
    return {
      red: "#e23d2f",
      green: "#1f7a5c",
      redRgb: "226,61,47",
      grid: "#e5e7eb",
      muted: "#8a8f99",
      card: "#ffffff",
    };
  }

  function pulse_board_colors() {
    if (is_dark_theme()) {
      return ["#ffab91", "#ff7a66", "#f0523f", "#d23b2a", "#9b2318", "#5c0f0b"];
    }
    return ["#ff9a76", "#ff6b5a", "#e04b3a", "#b42318", "#7f1d1d", "#4c0519"];
  }

  function pulse_datetime(ms) {
    if (!ms) {
      return "--";
    }
    return fmt_date_ms(ms) + " " + fmt_time_ms(ms, false);
  }

  function pulse_delta_text(value, suffix, digits) {
    if (value === null || value === undefined) {
      return "无对比";
    }
    var precision = digits === undefined ? 1 : digits;
    var sign = value > 0 ? "+" : "";
    return (
      sign +
      Number(value).toFixed(precision) +
      (suffix ? " " + suffix : "")
    );
  }

  function pulse_delta_class(value) {
    if (value === null || value === undefined || Number(value) === 0) {
      return "flat";
    }
    return Number(value) > 0 ? "up" : "down";
  }

  function pulse_cards_html(data) {
    var stats = data.stats || {};
    var deltas = data.deltas || {};
    var previousDate = data.previous_date || "--";
    var cards = [
      {
        label: "涨停家数",
        value: stats.limit_up_count,
        unit: "家",
        delta: deltas.limit_up_count,
        suffix: "家",
        digits: 0,
      },
      {
        label: "连板家数",
        value: stats.continue_count,
        unit: "家",
        delta: deltas.continue_count,
        suffix: "家",
        digits: 0,
      },
      {
        label: "最高板",
        value: stats.max_board,
        unit: "板",
        delta: deltas.max_board,
        suffix: "板",
        digits: 0,
      },
      {
        label: "早封率",
        value: stats.early_rate,
        unit: "%",
        delta: deltas.early_rate,
        suffix: "pp",
        digits: 1,
        is_pct: true,
      },
      {
        label: "封单留存",
        value: stats.seal_retention,
        unit: "%",
        delta: deltas.seal_retention,
        suffix: "pp",
        digits: 1,
        is_pct: true,
      },
      {
        label: "总封单额",
        value: stats.seal_total,
        unit: "亿",
        delta:
          deltas.seal_total === null || deltas.seal_total === undefined
            ? null
            : deltas.seal_total / 100000000,
        suffix: "亿",
        digits: 2,
        is_money: true,
      },
    ];
    return cards
      .map(function (card) {
        var value = "--";
        if (card.value !== null && card.value !== undefined) {
          if (card.is_pct) {
            value = Number(card.value).toFixed(1);
          } else if (card.is_money) {
            value = fin_yi(card.value);
          } else {
            value = String(Math.round(Number(card.value)));
          }
        }
        var delta = pulse_delta_text(card.delta, card.suffix, card.digits);
        var unit =
          card.value === null || card.value === undefined ? "" : card.unit;
        var sub = data.previous_date
          ? "较 " + previousDate + " " + delta
          : "无对比数据";
        return (
          '<div class="ths-pulse-kpi"><span class="ths-pulse-kpi-label">' +
          esc(card.label) +
          '</span><div class="ths-pulse-kpi-value">' +
          esc(value) +
          '<em>' +
          esc(unit) +
          '</em></div><div class="ths-pulse-kpi-sub ' +
          pulse_delta_class(card.delta) +
          '">' +
          esc(sub) +
          "</div></div>"
        );
      })
      .join("");
  }

  function pulse_bars_svg(data) {
    var rows = data.ladder || [];
    var levels = data.ladder_levels || [];
    if (!rows.length) {
      return empty_html("连板天梯暂无数据");
    }
    var compact = window.innerWidth < 720;
    var width = compact ? 420 : 900;
    var height = compact ? 205 : 235;
    var pad_l = compact ? 34 : 42;
    var pad_r = 10;
    var pad_t = 12;
    var pad_b = 30;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var colors = pulse_board_colors();
    var max_total = Math.max(
      1,
      Math.max.apply(
        null,
        rows.map(function (row) {
          return row.total || 0;
        }),
      ),
    );

    var grid = "";
    var tick_count = compact ? 3 : 4;
    for (var i = 0; i <= tick_count; i++) {
      var tick_value = (max_total * (tick_count - i)) / tick_count;
      var tick_y = pad_t + (plot_h * i) / tick_count;
      grid +=
        '<line x1="' +
        pad_l +
        '" y1="' +
        tick_y +
        '" x2="' +
        (width - pad_r) +
        '" y2="' +
        tick_y +
        '" stroke="' +
        pulse_palette().grid +
        '" stroke-width="1"/>' +
        '<text x="' +
        (pad_l - 6) +
        '" y="' +
        (tick_y + 3) +
        '" text-anchor="end" fill="' +
        pulse_palette().muted +
        '" font-size="' +
        (compact ? 8 : 9) +
        '">' +
        Math.round(tick_value) +
        "</text>";
    }

    var group_w = plot_w / rows.length;
    var bar_w = Math.max(4, group_w * 0.62);
    var bars = rows
      .map(function (row, index) {
        var x = pad_l + index * group_w + (group_w - bar_w) / 2;
        var y = pad_t + plot_h;
        var segments = "";
        levels.forEach(function (level, level_index) {
          var count = (row.levels || {})[level.key] || 0;
          if (!count) {
            return;
          }
          var bar_h = (count / max_total) * plot_h;
          y -= bar_h;
          segments +=
            '<rect x="' +
            x +
            '" y="' +
            y +
            '" width="' +
            bar_w +
            '" height="' +
            bar_h +
            '" rx="1.5" fill="' +
            colors[level_index % colors.length] +
            '"><title>' +
            esc(row.date + " · " + level.label + " " + count + " 只") +
            "</title></rect>";
        });
        return segments;
      })
      .join("");

    var x_labels = rows
      .map(function (row, index) {
        if (index % 5 !== 0 && index !== rows.length - 1) {
          return "";
        }
        var x = pad_l + index * group_w + group_w / 2;
        return (
          '<text x="' +
          x +
          '" y="' +
          (height - 10) +
          '" text-anchor="middle" fill="' +
          pulse_palette().muted +
          '" font-size="' +
          (compact ? 8 : 9) +
          '">' +
          esc(row.date.slice(5)) +
          "</text>"
        );
      })
      .join("");

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="近30日连板天梯板位分布" preserveAspectRatio="xMidYMid meet">' +
      grid +
      bars +
      x_labels +
      "</svg>"
    );
  }

  function pulse_promotion_svg(data) {
    var by_date = (data.promotion || {}).by_date || [];
    var valid = by_date.filter(function (item) {
      return item.rate !== null && item.rate !== undefined;
    });
    if (!valid.length) {
      return "";
    }
    var compact = window.innerWidth < 720;
    var width = compact ? 420 : 900;
    var height = 78;
    var pad_l = compact ? 34 : 42;
    var pad_r = 10;
    var pad_t = 8;
    var pad_b = 18;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var step = by_date.length > 1 ? plot_w / (by_date.length - 1) : plot_w;

    function x(index) {
      return pad_l + index * step;
    }

    function y(rate) {
      return pad_t + (1 - Math.max(0, Math.min(100, rate)) / 100) * plot_h;
    }

    var segments = [];
    var previous = null;
    by_date.forEach(function (item, index) {
      if (item.rate === null || item.rate === undefined) {
        previous = null;
        return;
      }
      if (previous !== null && previous.index === index - 1) {
        segments.push(
          "M" + x(previous.index) + " " + y(previous.rate) + " L" + x(index) + " " + y(item.rate),
        );
      }
      previous = { index: index, rate: item.rate };
    });

    var dots = valid
      .map(function (item) {
        var index = by_date.indexOf(item);
        return (
          '<circle cx="' +
          x(index) +
          '" cy="' +
          y(item.rate) +
          '" r="2.8" fill="' +
          pulse_palette().card +
          '" stroke="' +
          pulse_palette().red +
          '" stroke-width="1.8"><title>' +
          esc(
            item.date +
              " · 样本内晋级率 " +
              Number(item.rate).toFixed(1) +
              "%（" +
              item.promoted +
              "/" +
              item.eligible +
              "）",
          ) +
          "</title></circle>"
        );
      })
      .join("");

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="天梯样本内晋级率" preserveAspectRatio="xMidYMid meet">' +
      '<line x1="' +
      pad_l +
      '" y1="' +
      y(0) +
      '" x2="' +
      (width - pad_r) +
      '" y2="' +
      y(0) +
      '" stroke="' +
      pulse_palette().grid +
      '"/>' +
      '<line x1="' +
      pad_l +
      '" y1="' +
      y(50) +
      '" x2="' +
      (width - pad_r) +
      '" y2="' +
      y(50) +
      '" stroke="' +
      pulse_palette().grid +
      '" stroke-dasharray="3 3"/>' +
      '<text x="' +
      (pad_l - 6) +
      '" y="' +
      (y(50) + 3) +
      '" text-anchor="end" fill="' +
      pulse_palette().muted +
      '" font-size="8">50%</text>' +
      '<text x="' +
      (pad_l - 6) +
      '" y="' +
      (y(0) + 3) +
      '" text-anchor="end" fill="' +
      pulse_palette().muted +
      '" font-size="8">0%</text>' +
      '<path d="' +
      segments.join(" ") +
      '" fill="none" stroke="' +
      pulse_palette().red +
      '" stroke-width="1.8"/>' +
      dots +
      "</svg>"
    );
  }

  function pulse_matrix_html(data) {
    var rows = data.ladder || [];
    var levels = data.ladder_levels || [];
    if (!rows.length) {
      return empty_html("连板天梯暂无数据");
    }
    var max_count = 1;
    rows.forEach(function (row) {
      levels.forEach(function (level) {
        max_count = Math.max(max_count, (row.levels || {})[level.key] || 0);
      });
    });
    var palette = pulse_palette();
    var head =
      "<thead><tr><th class=\"ths-pulse-matrix-head\">板位</th>" +
      rows
        .map(function (row) {
          return (
            '<th title="' +
            esc(row.date) +
            '">' +
            esc(row.date.slice(5)) +
            "</th>"
          );
        })
        .join("") +
      "</tr></thead>";
    var body = levels
      .map(function (level) {
        return (
          "<tr><td class=\"ths-pulse-matrix-head\">" +
          esc(level.label) +
          "</td>" +
          rows
            .map(function (row) {
              var count = (row.levels || {})[level.key] || 0;
              if (!count) {
                return "<td class=\"empty\"></td>";
              }
              var alpha = 0.14 + (count / max_count) * 0.72;
              return (
                '<td style="background:rgba(' +
                palette.redRgb +
                "," +
                alpha.toFixed(2) +
                ");color:" +
                (alpha > 0.55 ? "#fff" : "var(--pulse-ink)") +
                '" title="' +
                esc(row.date + " · " + level.label + " " + count + " 只") +
                '">' +
                count +
                "</td>"
              );
            })
            .join("") +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="ths-pulse-matrix-wrap"><table class="ths-pulse-matrix">' +
      head +
      "<tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function pulse_ladder_panel_html(data) {
    var panel = data.ladder_panel || [];
    if (!panel.length) {
      return empty_html("所选交易日暂无涨停梯队数据");
    }
    return panel
      .map(function (level) {
        var items = level.items || [];
        var chips = items
          .map(function (item) {
            return (
              '<span class="ths-pulse-chip ths-stock" data-code="' +
              esc(item.code) +
              '" data-name="' +
              esc(item.name) +
              '"><b>' +
              esc(item.name) +
              '</b><em>' +
              esc(fin_yi(item.seal_money)) +
              "亿</em></span>"
            );
          })
          .join("");
        if (!chips) {
          chips = '<span class="ths-pulse-empty">暂无</span>';
        }
        return (
          '<div class="ths-pulse-level"><div class="ths-pulse-level-head"><span>' +
          esc(level.label) +
          "</span><b>" +
          level.count +
          " 只</b></div><div class=\"ths-pulse-level-items\">" +
          chips +
          (level.remaining > 0
            ? '<span class="ths-pulse-rest">其余 ' +
              level.remaining +
              " 只</span>"
            : "") +
          "</div></div>"
        );
      })
      .join("");
  }

  function pulse_changes_html(data) {
    var rows = data.board_changes || [];
    var max_count = 1;
    rows.forEach(function (row) {
      max_count = Math.max(max_count, row.current || 0);
    });
    return rows
      .map(function (row) {
        var delta = row.delta;
        return (
          '<div class="ths-pulse-change-row"><span class="label">' +
          esc(row.label) +
          '</span><div class="track"><i style="width:' +
          Math.max(2, ((row.current || 0) / max_count) * 100) +
          '%"></i></div><b>' +
          row.current +
          '</b><span class="prev">前 ' +
          (row.previous === null || row.previous === undefined
            ? "--"
            : row.previous) +
          '</span><span class="delta ' +
          pulse_delta_class(delta) +
          '">' +
          esc(pulse_delta_text(delta, "只", 0)) +
          "</span></div>"
        );
      })
      .join("");
  }

  function pulse_seal_buckets_html(data) {
    var rows = data.seal_buckets || [];
    var max_count = 1;
    rows.forEach(function (row) {
      max_count = Math.max(max_count, row.count || 0);
    });
    return rows
      .map(function (row) {
        return (
          '<div class="ths-pulse-bucket-row"><span class="label">' +
          esc(row.label) +
          '</span><div class="track"><i style="width:' +
          Math.max(2, ((row.count || 0) / max_count) * 100) +
          '%"></i></div><b>' +
          row.count +
          ' 只</b><span class="amount">' +
          esc(fin_yi(row.amount)) +
          " 亿</span></div>"
        );
      })
      .join("");
  }

  function pulse_reasons_html(data) {
    var rows = data.reasons || [];
    if (!rows.length) {
      return empty_html("暂无涨停原因文本");
    }
    var max_count = 1;
    rows.forEach(function (row) {
      max_count = Math.max(max_count, row.count || 0);
    });
    var palette = pulse_palette();
    return (
      '<div class="ths-pulse-reasons">' +
      rows
        .map(function (row) {
          var alpha = 0.06 + ((row.count || 0) / max_count) * 0.16;
          return (
            '<div class="ths-pulse-reason" style="background:rgba(' +
            palette.redRgb +
            "," +
            alpha.toFixed(2) +
            ')"><b>' +
            esc(row.keyword) +
            "</b><span>" +
            row.count +
            " 次</span></div>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function pulse_definitions_html(data) {
    var definitions = data.definitions || {};
    var lines = Object.keys(definitions).map(function (key) {
      return definitions[key];
    });
    return lines
      .map(function (line) {
        return "<span>" + esc(line) + "</span>";
      })
      .join("");
  }

  function render_market_pulse(data) {
    var stats = data.stats || {};
    var promotion = data.promotion || {};
    var warnings = (data.warnings || [])
      .map(function (warning) {
        return "<li>" + esc(warning) + "</li>";
      })
      .join("");
    var levels = data.ladder_levels || [];
    var colors = pulse_board_colors();
    var legend = levels
      .map(function (level, index) {
        return (
          '<span class="ths-pulse-legend-item"><i style="background:' +
          colors[index % colors.length] +
          '"></i>' +
          esc(level.label) +
          "</span>"
        );
      })
      .join("");

    var promotion_text =
      promotion.rate === null || promotion.rate === undefined
        ? "样本不足"
        : Number(promotion.rate).toFixed(1) +
          "%（" +
          promotion.promoted +
          "/" +
          promotion.eligible +
          "）";

    var html =
      '<div class="ths-pulse-kpis">' +
      pulse_cards_html(data) +
      "</div>" +
      (warnings
        ? '<div class="ths-pulse-warning"><b>数据提示</b><ul>' +
          warnings +
          "</ul></div>"
        : "") +
      '<div class="ths-pulse-grid">' +
      '<section class="ths-pulse-card ths-pulse-main"><div class="ths-pulse-card-head"><div><h3>30日涨停天梯同步面板</h3><p>板位 × 交易日；天梯接口只覆盖 2 板以上，首板以右侧涨停池为准</p></div><div class="ths-pulse-legend">' +
      legend +
      "</div></div>" +
      '<div class="ths-pulse-bars">' +
      pulse_bars_svg(data) +
      "</div>" +
      '<div class="ths-pulse-promotion"><div class="ths-pulse-promotion-head"><span>样本内晋级率</span><b>' +
      esc(promotion_text) +
      '</b><em>非全市场晋级率</em></div>' +
      pulse_promotion_svg(data) +
      "</div>" +
      pulse_matrix_html(data) +
      "</section>" +
      '<section class="ths-pulse-card ths-pulse-side"><div class="ths-pulse-card-head"><div><h3>所选日期涨停梯队</h3><p>' +
      esc(data.trade_date || "--") +
      " · 涨停池按封单额取前 4 只</p></div></div>" +
      pulse_ladder_panel_html(data) +
      '<div class="ths-pulse-side-note">连板家数 ' +
      (stats.continue_count === undefined ? "--" : stats.continue_count) +
      " 家 · 最高 " +
      (stats.max_board === undefined ? "--" : stats.max_board) +
      " 板</div></section>" +
      "</div>" +
      '<div class="ths-pulse-lower">' +
      '<section class="ths-pulse-card"><div class="ths-pulse-card-head"><div><h3>连板层级变化</h3><p>较前一交易日 ' +
      esc(data.previous_date || "--") +
      "</p></div></div>" +
      pulse_changes_html(data) +
      "</section>" +
      '<section class="ths-pulse-card"><div class="ths-pulse-card-head"><div><h3>封单结构</h3><p>按当前封单额分档，单位亿元</p></div></div>' +
      pulse_seal_buckets_html(data) +
      "</section>" +
      '<section class="ths-pulse-card"><div class="ths-pulse-card-head"><div><h3>涨停原因关键词</h3><p>原始 limit_up_reason 文本拆分，非行业分类</p></div></div>' +
      pulse_reasons_html(data) +
      "</section>" +
      "</div>" +
      '<footer class="ths-pulse-footer"><div class="ths-pulse-sources">数据源：' +
      esc((data.sources || []).join("、")) +
      " · 交易日 " +
      esc(data.trade_date || "--") +
      " · " +
      (data.is_simulated ? "模拟数据" : "真实数据") +
      " · 取数时间 " +
      esc(pulse_datetime(data.data_time_ms)) +
      "</div><div class=\"ths-pulse-definitions\">" +
      pulse_definitions_html(data) +
      '</div><div class="ths-pulse-disclaimer">仅供信息展示，不构成投资建议</div></footer>';

    $("#ths-pulse-body").html(html);
    var matrix_wrap = document.querySelector(".ths-pulse-matrix-wrap");
    if (matrix_wrap) {
      matrix_wrap.scrollLeft = matrix_wrap.scrollWidth;
    }
  }

  function open_market_pulse(date) {
    var html =
      '<div class="ths-pulse" id="ths-pulse">' +
      '<div class="ths-pulse-hero"><div><div class="ths-pulse-kicker">LIMIT-UP SENTIMENT · 30D</div>' +
      "<h2>涨停情绪市场脉冲屏</h2>" +
      '<p>用涨停池与连板天梯观察市场情绪，不输出交易动作或仓位建议</p></div>' +
      '<div class="ths-pulse-hero-right"><span class="ths-pulse-badge">真实数据</span>' +
      '<div class="ths-pulse-time" id="ths-pulse-time">数据时间 --</div>' +
      '<label class="ths-pulse-date-label">交易日 <select id="ths-pulse-date" class="ths-select"></select></label></div></div>' +
      '<div id="ths-pulse-body">' +
      loading_html("正在获取涨停池与连板天梯...") +
      "</div></div>";
    open_layer(
      "同花顺 · 涨停情绪市场脉冲屏",
      html,
      1240,
      900,
    );

    function load(selected_date) {
      $("#ths-pulse-body").html(loading_html("正在获取涨停池与连板天梯..."));
      var params = {};
      if (selected_date) {
        params.date = selected_date;
      }
      request("market_pulse", params)
        .then(function (data) {
          $("#ths-pulse-date")
            .html(
              (data.date_list || [])
                .map(function (item) {
                  return (
                    '<option value="' +
                    esc(item) +
                    '">' +
                    esc(item) +
                    "</option>"
                  );
                })
                .join(""),
            )
            .val(data.trade_date);
          $("#ths-pulse-time").text(
            "取数时间 " +
              pulse_datetime(data.data_time_ms) +
              " · 交易日 " +
              (data.trade_date || "--") +
              " · 前一交易日 " +
              (data.previous_date || "--"),
          );
          render_market_pulse(data);
        })
        .catch(function (err) {
          $("#ths-pulse-body").html(
            '<div class="ths-empty ths-error">' +
              esc(err.message || "市场脉冲数据获取失败") +
              "</div>",
          );
        });
    }

    $("#ths-pulse-date")
      .off("change.pulse")
      .on("change.pulse", function () {
        load($(this).val());
      });
    load(date);
  }

  // ------------------------------------------------------------------
  // 龙虎榜机构与游资观察
  // ------------------------------------------------------------------
  var dtw_state = { data: null, tab: "all", sort: "net" };

  function dtw_palette() {
    if (is_dark_theme()) {
      return {
        red: "#ff6b5a",
        green: "#4fd1b3",
        redRgb: "255,107,90",
      };
    }
    return {
      red: "#e23d2f",
      green: "#1f7a5c",
      redRgb: "226,61,47",
    };
  }

  function dtw_time(ms) {
    if (!ms) {
      return "--";
    }
    return fmt_date_ms(ms) + " " + fmt_time_ms(ms, false);
  }

  function dtw_money(value) {
    if (value === null || value === undefined || value === "") {
      return "--";
    }
    return fin_yi(value) + "亿";
  }

  function dtw_signed(value) {
    if (value === null || value === undefined || value === "") {
      return "--";
    }
    var sign = Number(value) > 0 ? "+" : "";
    return sign + fin_yi(value) + "亿";
  }

  function dtw_pct(value) {
    if (value === null || value === undefined) {
      return "--";
    }
    return (Number(value) * 100).toFixed(2) + "%";
  }

  function dtw_num_class(value) {
    if (value === null || value === undefined || Number(value) === 0) {
      return "";
    }
    return Number(value) > 0 ? "up" : "down";
  }

  function dtw_metric_value(stock, metric) {
    if (metric === "org") {
      return stock.org_net_value;
    }
    if (metric === "hot") {
      return stock.hot_money_net_value;
    }
    return stock.net_value;
  }

  function dtw_bar(value, max_abs) {
    if (value === null || value === undefined || Number(value) === 0) {
      return '<div class="dtw-bar"><i class="dtw-mid"></i></div>';
    }
    var width = max_abs ? Math.min(50, (Math.abs(Number(value)) / max_abs) * 50) : 0;
    var fill =
      Number(value) > 0
        ? '<i class="dtw-fill pos" style="left:50%;width:' + width + '%"></i>'
        : '<i class="dtw-fill neg" style="right:50%;width:' + width + '%"></i>';
    return '<div class="dtw-bar"><i class="dtw-mid"></i>' + fill + "</div>";
  }

  function dtw_filter_rows() {
    var data = dtw_state.data || {};
    var rows = (data.stocks || []).slice();
    if (dtw_state.tab === "org") {
      rows = rows.filter(function (stock) {
        return stock.org_net_value !== null && stock.org_net_value !== undefined;
      });
    } else if (dtw_state.tab === "hot") {
      rows = rows.filter(function (stock) {
        return (
          (stock.hot_money_net_value !== null &&
            stock.hot_money_net_value !== undefined) ||
          (stock.hot_seats || []).length > 0
        );
      });
    }
    rows.sort(function (a, b) {
      var av = dtw_metric_value(a, dtw_state.sort);
      var bv = dtw_metric_value(b, dtw_state.sort);
      if (av === null || av === undefined) {
        return 1;
      }
      if (bv === null || bv === undefined) {
        return -1;
      }
      return Number(bv) - Number(av);
    });
    return rows;
  }

  function dtw_list_html() {
    var rows = dtw_filter_rows();
    if (!rows.length) {
      return empty_html("当前榜单暂无股票记录");
    }
    var max_abs = 1;
    rows.forEach(function (stock) {
      var value = dtw_metric_value(stock, dtw_state.sort);
      if (value !== null && value !== undefined) {
        max_abs = Math.max(max_abs, Math.abs(Number(value)));
      }
    });
    return rows
      .map(function (stock, index) {
        var value = dtw_metric_value(stock, dtw_state.sort);
        var concepts = (stock.concepts || []).slice(0, 3).join(" / ");
        return (
          '<div class="dtw-stock" tabindex="0" role="button" data-code="' +
          esc(stock.code) +
          '"><span class="dtw-rank">' +
          pad2(index + 1) +
          '</span><div class="dtw-stock-id"><b>' +
          esc(stock.name || "--") +
          "</b><em>" +
          esc(stock.ticker || stock.code) +
          "</em><small>" +
          esc(concepts || stock.limit_reason || "--") +
          '</small></div><div class="dtw-stock-bar">' +
          dtw_bar(value, max_abs) +
          '</div><span class="dtw-stock-value ' +
          dtw_num_class(value) +
          '">' +
          esc(dtw_signed(value)) +
          "</span></div>"
        );
      })
      .join("");
  }

  function dtw_render_list() {
    $("#dtw-list").html(dtw_list_html());
    $(".dtw-tab")
      .removeClass("active")
      .attr("aria-pressed", "false");
    $('.dtw-tab[data-tab="' + dtw_state.tab + '"]')
      .addClass("active")
      .attr("aria-pressed", "true");
    $("#dtw-sort").val(dtw_state.sort);
  }

  function dtw_stats_html(data) {
    var stats = data.stats || {};
    var cards = [
      {
        label: "上榜股票",
        value: stats.stock_count,
        unit: "只",
        sub:
          "买入 " +
          dtw_money(stats.buy_total) +
          " · 卖出 " +
          dtw_money(stats.sell_total),
        is_money: false,
      },
      {
        label: "全榜净额",
        value: stats.net_total,
        unit: "亿",
        sub: "买入额 - 卖出额",
        is_money: true,
      },
      {
        label: "机构净额",
        value: stats.org_net_total,
        unit: "亿",
        sub: (stats.org_stock_count || 0) + " 只股票有机构记录",
        is_money: true,
      },
      {
        label: "游资净额",
        value: stats.hot_net_total,
        unit: "亿",
        sub: (stats.seat_count || 0) + " 个活跃游资席位",
        is_money: true,
      },
    ];
    return cards
      .map(function (card) {
        var text =
          card.value === null || card.value === undefined
            ? "--"
            : card.is_money
              ? fin_yi(card.value)
              : String(Math.round(Number(card.value)));
        return (
          '<div class="dtw-kpi"><span class="dtw-kpi-label">' +
          esc(card.label) +
          '</span><div class="dtw-kpi-value ' +
          (card.is_money ? dtw_num_class(card.value) : "") +
          '">' +
          esc(text) +
          "<em>" +
          esc(card.value === null || card.value === undefined ? "" : card.unit) +
          '</em></div><div class="dtw-kpi-sub">' +
          esc(card.sub || "") +
          "</div></div>"
        );
      })
      .join("");
  }

  function dtw_seats_html(data) {
    var seats = data.seats || [];
    if (!seats.length) {
      return empty_html("暂无活跃游资席位");
    }
    var max_abs = 1;
    seats.forEach(function (seat) {
      if (seat.net_value !== null && seat.net_value !== undefined) {
        max_abs = Math.max(max_abs, Math.abs(Number(seat.net_value)));
      }
    });
    return seats
      .map(function (seat) {
        return (
          '<div class="dtw-seat" tabindex="0" role="button" data-seat="' +
          esc(seat.name) +
          '"><div class="dtw-seat-head"><b>' +
          esc(seat.name || "--") +
          '</b><span class="' +
          dtw_num_class(seat.net_value) +
          '">' +
          esc(dtw_signed(seat.net_value)) +
          "</span></div>" +
          dtw_bar(seat.net_value, max_abs) +
          '<div class="dtw-seat-meta">' +
          esc(seat.stock_count) +
          " 只股票</div></div>"
        );
      })
      .join("");
  }

  function dtw_detail_empty() {
    return (
      '<div class="dtw-detail-empty">点击左侧股票或右侧游资席位，查看有限数量的代表记录</div>'
    );
  }

  function dtw_show_detail(html) {
    $("#dtw-detail").html(html);
    var element = document.getElementById("dtw-detail");
    if (element) {
      element.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }

  function dtw_detail_stock(code) {
    var data = dtw_state.data || {};
    var stock = (data.stocks || []).find(function (item) {
      return item.code === code;
    });
    if (!stock) {
      return dtw_detail_empty();
    }
    var concepts = (stock.concepts || [])
      .map(function (concept) {
        return '<span class="dtw-tag">' + esc(concept) + "</span>";
      })
      .join("");
    var seats = (stock.hot_seats || [])
      .map(function (seat) {
        return (
          '<button type="button" class="dtw-detail-seat" data-seat="' +
          esc(seat.name) +
          '">' +
          esc(seat.name) +
          " " +
          esc(dtw_signed(seat.net_value)) +
          "</button>"
        );
      })
      .join("");
    var records = (stock.records || [])
      .map(function (record) {
        return (
          "<tr><td>" +
          esc(record.range_label) +
          "</td><td>" +
          esc(dtw_signed(record.net_value)) +
          "</td><td>" +
          esc(dtw_signed(record.buy_value)) +
          "</td><td>" +
          esc(dtw_signed(record.sell_value)) +
          "</td><td>" +
          esc(dtw_signed(record.org_net_value)) +
          "</td><td>" +
          esc(dtw_signed(record.hot_money_net_value)) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<div class="dtw-detail-head"><div><h4>' +
      esc(stock.name || "--") +
      " <em>" +
      esc(stock.ticker || stock.code) +
      "</em></h4><p>" +
      esc(stock.limit_reason || "—") +
      "</p></div><span class=\"dtw-tag\">" +
      esc(stock.range_label) +
      "</span></div>" +
      '<div class="dtw-detail-metrics"><div><span>涨跌幅</span><b>' +
      esc(dtw_pct(stock.change)) +
      "</b></div><div><span>净额</span><b class=\"" +
      dtw_num_class(stock.net_value) +
      '">' +
      esc(dtw_signed(stock.net_value)) +
      "</b></div><div><span>买入</span><b>" +
      esc(dtw_money(stock.buy_value)) +
      "</b></div><div><span>卖出</span><b>" +
      esc(dtw_money(stock.sell_value)) +
      "</b></div><div><span>机构净额</span><b class=\"" +
      dtw_num_class(stock.org_net_value) +
      '">' +
      esc(dtw_signed(stock.org_net_value)) +
      "</b></div><div><span>游资净额</span><b class=\"" +
      dtw_num_class(stock.hot_money_net_value) +
      '">' +
      esc(dtw_signed(stock.hot_money_net_value)) +
      "</b></div></div>" +
      (concepts ? '<div class="dtw-detail-tags">' + concepts + "</div>" : "") +
      (seats
        ? '<div class="dtw-detail-seats"><span>关联游资席位</span>' + seats + "</div>"
        : "") +
      '<div class="dtw-detail-table-wrap"><table class="dtw-detail-table"><thead><tr><th>榜单</th><th>净额</th><th>买入</th><th>卖出</th><th>机构净额</th><th>游资净额</th></tr></thead><tbody>' +
      records +
      "</tbody></table></div>"
    );
  }

  function dtw_detail_seat(name) {
    var data = dtw_state.data || {};
    var seat = (data.seats || []).find(function (item) {
      return item.name === name;
    });
    if (!seat) {
      return dtw_detail_empty();
    }
    var stocks = (seat.stocks || [])
      .map(function (stock) {
        return (
          '<button type="button" class="dtw-detail-stock" data-code="' +
          esc(stock.code) +
          '"><b>' +
          esc(stock.name || "--") +
          "</b><em>" +
          esc(stock.ticker || "") +
          "</em><span class=\"" +
          dtw_num_class(stock.net_value) +
          '">' +
          esc(dtw_signed(stock.net_value)) +
          "</span></button>"
        );
      })
      .join("");
    return (
      '<div class="dtw-detail-head"><div><h4>游资席位 · ' +
      esc(seat.name || "--") +
      "</h4><p>席位聚合净额 " +
      esc(dtw_signed(seat.net_value)) +
      " · 代表股票 " +
      esc(seat.stock_count) +
      " 只</p></div></div>" +
      '<div class="dtw-detail-stock-list">' +
      (stocks || empty_html("暂无代表股票")) +
      "</div>"
    );
  }

  function render_dragon_tiger_watch(data) {
    var warnings = (data.warnings || [])
      .map(function (warning) {
        return "<li>" + esc(warning) + "</li>";
      })
      .join("");
    var definitions = Object.keys(data.definitions || {}).map(function (key) {
      return data.definitions[key];
    });
    var html =
      '<div class="dtw-kpis">' +
      dtw_stats_html(data) +
      "</div>" +
      (warnings
        ? '<div class="dtw-warning"><b>数据提示</b><ul>' + warnings + "</ul></div>"
        : "") +
      '<div class="dtw-grid">' +
      '<section class="dtw-card dtw-main"><div class="dtw-card-head"><div><h3>资金净额结构</h3><p>中心线左侧为净流出，右侧为净流入；上榜不代表未来价格方向</p></div>' +
      '<div class="dtw-controls"><div class="dtw-tabs"><button type="button" class="dtw-tab active" data-tab="all" aria-pressed="true">全部榜</button><button type="button" class="dtw-tab" data-tab="org" aria-pressed="false">机构榜</button><button type="button" class="dtw-tab" data-tab="hot" aria-pressed="false">游资榜</button></div>' +
      '<select id="dtw-sort" class="dtw-sort" aria-label="排序方式"><option value="net">按净额</option><option value="org">按机构净额</option><option value="hot">按游资净额</option></select></div></div>' +
      '<div class="dtw-list" id="dtw-list">' +
      dtw_list_html() +
      "</div></section>" +
      '<section class="dtw-card dtw-side"><div class="dtw-card-head"><div><h3>活跃游资席位</h3><p>点击席位查看代表股票</p></div></div>' +
      '<div class="dtw-seats">' +
      dtw_seats_html(data) +
      "</div></section>" +
      "</div>" +
      '<section class="dtw-card dtw-detail" id="dtw-detail">' +
      dtw_detail_empty() +
      "</section>" +
      '<footer class="dtw-footer"><div class="dtw-sources">数据源：' +
      esc((data.sources || []).join("、")) +
      " · 交易日 " +
      esc(data.trade_date || "--") +
      " · " +
      (data.is_simulated ? "模拟数据" : "真实数据") +
      " · 数据时间 " +
      esc(dtw_time(data.data_time_ms)) +
      '</div><div class="dtw-definitions">' +
      definitions
        .map(function (line) {
          return "<span>" + esc(line) + "</span>";
        })
        .join("") +
      '</div><div class="dtw-disclaimer">仅供信息展示，不构成投资建议</div></footer>';

    $("#dtw-body").html(html);
    dtw_render_list();

    $("#ths-dtw-body")
      .off(".dtw")
      .on("click.dtw", ".dtw-tab", function () {
        dtw_state.tab = $(this).data("tab");
        dtw_state.sort =
          dtw_state.tab === "org"
            ? "org"
            : dtw_state.tab === "hot"
              ? "hot"
              : "net";
        dtw_render_list();
      })
      .on("change.dtw", "#dtw-sort", function () {
        dtw_state.sort = $(this).val();
        dtw_render_list();
      })
      .on("click.dtw", ".dtw-stock", function () {
        dtw_show_detail(dtw_detail_stock(String($(this).data("code"))));
      })
      .on("keydown.dtw", ".dtw-stock", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          dtw_show_detail(dtw_detail_stock(String($(this).data("code"))));
        }
      })
      .on("click.dtw", ".dtw-seat", function () {
        dtw_show_detail(dtw_detail_seat(String($(this).data("seat"))));
      })
      .on("keydown.dtw", ".dtw-seat", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          dtw_show_detail(dtw_detail_seat(String($(this).data("seat"))));
        }
      })
      .on("click.dtw", ".dtw-detail-stock", function () {
        dtw_show_detail(dtw_detail_stock(String($(this).data("code"))));
      })
      .on("click.dtw", ".dtw-detail-seat", function () {
        dtw_show_detail(dtw_detail_seat(String($(this).data("seat"))));
      });
  }

  function open_dragon_tiger_watch() {
    dtw_state = { data: null, tab: "all", sort: "net" };
    var html =
      '<div class="ths-dtw" id="ths-dtw-body">' +
      '<div class="dtw-hero"><div><div class="dtw-kicker">DRAGON-TIGER CAPITAL FLOW</div>' +
      "<h2>机构与游资资金观察</h2>" +
      '<p>以左右分流的资金条阅读净流入与净流出，并在机构榜、游资榜和席位视角间联动。</p></div>' +
      '<div class="dtw-hero-right"><span class="dtw-badge">真实数据</span><span class="dtw-hero-date" id="dtw-hero-date">交易日 --</span></div></div>' +
      '<div id="dtw-body">' +
      loading_html("正在获取龙虎榜全部/机构/游资数据...") +
      "</div></div>";
    open_layer(
      "同花顺 · 龙虎榜机构与游资观察",
      html,
      1240,
      900,
    );

    request("dragon_tiger_watch", {})
      .then(function (data) {
        dtw_state.data = data;
        dtw_state.tab = "all";
        dtw_state.sort = "net";
        $("#dtw-hero-date").text("交易日 " + (data.trade_date || "--"));
        render_dragon_tiger_watch(data);
      })
      .catch(function (err) {
        $("#dtw-body").html(
          '<div class="ths-empty ths-error">' +
            esc(err.message || "龙虎榜数据获取失败") +
            "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 龙虎榜资金流向拓扑台
  // ------------------------------------------------------------------
  var flow_state = {
    data: null,
    board: "all",
    range: 20,
    concept: null,
    stock: null,
  };

  function flow_board() {
    return (flow_state.data && flow_state.data.boards[flow_state.board]) || {};
  }

  function flow_window() {
    var dates = (flow_state.data && flow_state.data.dates) || [];
    return Math.min(flow_state.range, dates.length);
  }

  function flow_dates() {
    var dates = (flow_state.data && flow_state.data.dates) || [];
    return dates.slice(-flow_window());
  }

  function flow_slice(values) {
    return (values || []).slice(-flow_window());
  }

  function flow_sum(values) {
    return (values || []).reduce(function (total, value) {
      return total + (Number(value) || 0);
    }, 0);
  }

  function flow_cumulative(values) {
    var total = 0;
    return (values || []).map(function (value) {
      total += Number(value) || 0;
      return total;
    });
  }

  function flow_money(value) {
    if (value === null || value === undefined) {
      return "--";
    }
    return fin_yi(value) + "亿";
  }

  function flow_signed(value) {
    if (value === null || value === undefined) {
      return "--";
    }
    var sign = Number(value) > 0 ? "+" : "";
    return sign + fin_yi(value) + "亿";
  }

  function flow_sign_class(value) {
    if (value === null || value === undefined || Number(value) === 0) {
      return "";
    }
    return Number(value) > 0 ? "up" : "down";
  }

  function flow_truncate(text, length) {
    var value = String(text || "");
    return value.length > length ? value.slice(0, length) + "…" : value;
  }

  function flow_line_color(value, index) {
    var positive = ["#ff7a66", "#f0523f", "#d23b2a", "#ff9a76", "#e86a4a"];
    var negative = ["#4fd1b3", "#25a18e", "#7bd3c0", "#1f7a5c", "#5ec7b2"];
    return value >= 0
      ? positive[index % positive.length]
      : negative[index % negative.length];
  }

  function flow_bar(value, max_abs) {
    if (value === null || value === undefined || Number(value) === 0) {
      return '<div class="flow-bar"><i class="flow-mid"></i></div>';
    }
    var width = max_abs
      ? Math.min(50, (Math.abs(Number(value)) / max_abs) * 50)
      : 0;
    var fill =
      Number(value) > 0
        ? '<i class="flow-fill pos" style="left:50%;width:' + width + '%"></i>'
        : '<i class="flow-fill neg" style="right:50%;width:' + width + '%"></i>';
    return '<div class="flow-bar"><i class="flow-mid"></i>' + fill + "</div>";
  }

  function flow_concepts() {
    return flow_board().concepts || [];
  }

  function flow_concept_total(concept) {
    return flow_sum(flow_slice((flow_board().daily || {})[concept]));
  }

  function flow_ranking() {
    return flow_concepts()
      .map(function (concept) {
        return { name: concept, value: flow_concept_total(concept) };
      })
      .sort(function (a, b) {
        return b.value - a.value;
      });
  }

  function flow_ensure_concept() {
    var ranking = flow_ranking();
    if (!ranking.length) {
      flow_state.concept = null;
      return null;
    }
    var exists = ranking.some(function (item) {
      return item.name === flow_state.concept;
    });
    if (!exists) {
      flow_state.concept = ranking[0].name;
    }
    return flow_state.concept;
  }

  function flow_stock_totals(concept) {
    var stocks = (flow_board().stock_daily || {})[concept] || {};
    return Object.keys(stocks)
      .map(function (code) {
        return { code: code, value: flow_sum(flow_slice(stocks[code])) };
      })
      .filter(function (item) {
        return Math.abs(item.value) > 0.000001;
      })
      .sort(function (a, b) {
        return Math.abs(b.value) - Math.abs(a.value);
      });
  }

  function flow_category_sums(concept) {
    var board = flow_board();
    if (flow_state.board === "all" && board.category_daily && board.category_daily[concept]) {
      var category = board.category_daily[concept];
      return [
        { key: "org", label: "机构", value: flow_sum(flow_slice(category.org)), color: "#5b8ff9" },
        { key: "hot", label: "游资", value: flow_sum(flow_slice(category.hot)), color: "#f6bd16" },
        { key: "other", label: "其他", value: flow_sum(flow_slice(category.other)), color: "#8c8c8c" },
      ];
    }
    var total = flow_concept_total(concept);
    if (flow_state.board === "org") {
      return [{ key: "org", label: "机构", value: total, color: "#5b8ff9" }];
    }
    if (flow_state.board === "hot") {
      return [{ key: "hot", label: "游资", value: total, color: "#f6bd16" }];
    }
    return [{ key: "other", label: "其他", value: total, color: "#8c8c8c" }];
  }

  function flow_latest_stats() {
    var board = flow_board();
    return board.latest_stats || (board.stats_by_range || {})[String(flow_state.range)] || {};
  }

  function flow_stats_html() {
    var stats = flow_latest_stats();
    var cards = [
      { label: "上榜股票", value: stats.stock_count, unit: "只", money: false, sub: "最新交易日 · 仅1日榜" },
      { label: "买入额", value: stats.buy_total, unit: "亿", money: true, sub: "最新交易日" },
      { label: "卖出额", value: stats.sell_total, unit: "亿", money: true, sub: "最新交易日" },
      { label: "全榜净额", value: stats.net_total, unit: "亿", money: true, sub: "买入额 - 卖出额" },
      { label: "机构净额", value: stats.org_net_total, unit: "亿", money: true, sub: "机构专用席位" },
      { label: "游资净额", value: stats.hot_net_total, unit: "亿", money: true, sub: "活跃游资席位" },
    ];
    return cards
      .map(function (card) {
        var text =
          card.value === null || card.value === undefined
            ? "--"
            : card.money
              ? fin_yi(card.value)
              : String(Math.round(Number(card.value)));
        return (
          '<div class="flow-kpi"><span class="flow-kpi-label">' +
          esc(card.label) +
          '</span><div class="flow-kpi-value ' +
          (card.money ? flow_sign_class(card.value) : "") +
          '">' +
          esc(text) +
          "<em>" +
          esc(card.value === null || card.value === undefined ? "" : card.unit) +
          '</em></div><div class="flow-kpi-sub">' +
          esc(card.sub) +
          "</div></div>"
        );
      })
      .join("");
  }

  function flow_chart_svg() {
    var compact = window.innerWidth < 720;
    var concepts = flow_concepts().slice(0, compact ? 5 : 8);
    var dates = flow_dates();
    if (!concepts.length || !dates.length) {
      return empty_html("所选区间暂无概念净额数据");
    }
    var width = compact ? 360 : 840;
    var height = compact ? 310 : 370;
    var pad_l = compact ? 30 : 47;
    var pad_r = compact ? 110 : 176;
    var pad_t = 18;
    var pad_b = 28;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var series = concepts.map(function (concept, index) {
      var points = flow_cumulative(flow_slice((flow_board().daily || {})[concept]));
      return { name: concept, index: index, points: points, final: points[points.length - 1] };
    });
    var all_values = [0];
    series.forEach(function (item) {
      all_values = all_values.concat(item.points);
    });
    var min_value = Math.min.apply(null, all_values);
    var max_value = Math.max.apply(null, all_values);
    if (min_value === max_value) {
      min_value -= 1;
      max_value += 1;
    }
    var padding = (max_value - min_value) * 0.08;
    min_value -= padding;
    max_value += padding;

    function x(index) {
      return dates.length === 1
        ? pad_l + plot_w / 2
        : pad_l + (index / (dates.length - 1)) * plot_w;
    }

    function y(value) {
      return pad_t + ((max_value - value) / (max_value - min_value)) * plot_h;
    }

    var grid = "";
    for (var tick = 0; tick <= 4; tick++) {
      var tick_value = min_value + ((max_value - min_value) * tick) / 4;
      var tick_y = y(tick_value);
      grid +=
        '<line x1="' +
        pad_l +
        '" y1="' +
        tick_y +
        '" x2="' +
        (pad_l + plot_w) +
        '" y2="' +
        tick_y +
        '" stroke="var(--flow-grid)" stroke-width="1"/>' +
        '<text x="' +
        (pad_l - 6) +
        '" y="' +
        (tick_y + 3) +
        '" text-anchor="end" fill="var(--flow-muted)" font-size="' +
        (compact ? 8 : 9) +
        '">' +
        fin_yi(tick_value) +
        "</text>";
    }
    var zero_y = y(0);
    grid +=
      '<line x1="' +
      pad_l +
      '" y1="' +
      zero_y +
      '" x2="' +
      (pad_l + plot_w) +
      '" y2="' +
      zero_y +
      '" stroke="var(--flow-muted)" stroke-width="1" stroke-dasharray="3 3"/>';

    var labels = [];
    var paths = series
      .map(function (item) {
        var color = flow_line_color(item.final, item.index);
        var path =
          "M" +
          item.points
            .map(function (value, index) {
              return x(index) + " " + y(value);
            })
            .join(" L");
        var last_y = y(item.final);
        labels.push({
          name: item.name,
          value: item.final,
          py: last_y,
          color: color,
        });
        return (
          '<path d="' +
          path +
          '" fill="none" stroke="' +
          color +
          '" stroke-width="' +
          (compact ? 1.6 : 1.9) +
          '" stroke-linecap="round" stroke-linejoin="round"><title>' +
          esc(item.name + " " + flow_signed(item.final)) +
          "</title></path>" +
          '<circle cx="' +
          x(item.points.length - 1) +
          '" cy="' +
          last_y +
          '" r="2.6" fill="' +
          color +
          '"/>'
        );
      })
      .join("");

    labels.sort(function (a, b) {
      return a.py - b.py;
    });
    var min_label_y = pad_t + 8;
    var max_label_y = height - pad_b - 8;
    var gap = compact ? 30 : 32;
    var placed = [];
    labels.forEach(function (item) {
      var label_y = Math.max(min_label_y, Math.min(max_label_y, item.py));
      if (placed.length) {
        label_y = Math.max(label_y, placed[placed.length - 1].label_y + gap);
      }
      placed.push({ name: item.name, value: item.value, color: item.color, py: item.py, label_y: label_y });
    });
    if (placed.length) {
      var overflow = placed[placed.length - 1].label_y - max_label_y;
      if (overflow > 0) {
        placed.forEach(function (item) {
          item.label_y -= overflow;
        });
      }
      var underflow = min_label_y - placed[0].label_y;
      if (underflow > 0) {
        placed.forEach(function (item) {
          item.label_y += underflow;
        });
      }
    }
    var label_x = pad_l + plot_w + 9;
    var label_marks = placed
      .map(function (item) {
        return (
          '<line x1="' +
          (pad_l + plot_w) +
          '" y1="' +
          item.py +
          '" x2="' +
          (label_x - 3) +
          '" y2="' +
          item.label_y +
          '" stroke="' +
          item.color +
          '" stroke-width="0.8" opacity="0.65"/>' +
          '<text x="' +
          label_x +
          '" y="' +
          item.label_y +
          '" fill="var(--flow-ink)" font-size="' +
          (compact ? 9.5 : 10) +
          '">' +
          esc(flow_truncate(item.name, compact ? 7 : 9)) +
          "</text>" +
          '<text x="' +
          label_x +
          '" y="' +
          (item.label_y + (compact ? 10 : 10)) +
          '" fill="' +
          item.color +
          '" font-size="' +
          (compact ? 9 : 9) +
          '">' +
          esc(flow_signed(item.value)) +
          "</text>"
        );
      })
      .join("");

    var x_indices = [];
    [0, 0.25, 0.5, 0.75, 1].forEach(function (ratio) {
      var index = Math.round((dates.length - 1) * ratio);
      if (x_indices.indexOf(index) === -1) {
        x_indices.push(index);
      }
    });
    var x_labels = x_indices
      .map(function (index) {
        return (
          '<text x="' +
          x(index) +
          '" y="' +
          (height - 9) +
          '" text-anchor="middle" fill="var(--flow-muted)" font-size="' +
          (compact ? 8.5 : 9) +
          '">' +
          esc(dates[index].slice(5)) +
          "</text>"
        );
      })
      .join("");

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="概念累计净额轨迹" preserveAspectRatio="xMidYMid meet">' +
      '<text x="' +
      pad_l +
      '" y="' +
      (pad_t - 5) +
      '" fill="var(--flow-muted)" font-size="9">累计净额（亿元）</text>' +
      grid +
      paths +
      label_marks +
      x_labels +
      "</svg>"
    );
  }

  function flow_ranking_html() {
    var ranking = flow_ranking();
    if (!ranking.length) {
      return empty_html("暂无概念排行");
    }
    var max_abs = 1;
    ranking.forEach(function (item) {
      max_abs = Math.max(max_abs, Math.abs(item.value));
    });
    return ranking
      .map(function (item) {
        var active = item.name === flow_state.concept ? " active" : "";
        return (
          '<button type="button" class="flow-rank' +
          active +
          '" data-concept="' +
          esc(item.name) +
          '"><div class="flow-rank-head"><b>' +
          esc(item.name) +
          '</b><span class="' +
          flow_sign_class(item.value) +
          '">' +
          esc(flow_signed(item.value)) +
          "</span></div>" +
          flow_bar(item.value, max_abs) +
          "</button>"
        );
      })
      .join("");
  }

  function flow_path_svg(concept) {
    var categories = flow_category_sums(concept).filter(function (item) {
      return Math.abs(item.value) > 0.000001;
    });
    var stocks = flow_stock_totals(concept).slice(0, 6);
    if (!categories.length && !stocks.length) {
      return empty_html("该概念暂无资金路径");
    }
    var compact = window.innerWidth < 720;
    var width = compact ? 420 : 760;
    var height = compact ? 300 : 260;
    var left_x = 8;
    var left_w = compact ? 96 : 120;
    var concept_x = compact ? 186 : 320;
    var concept_w = compact ? 82 : 100;
    var right_x = compact ? 286 : 565;
    var right_w = compact ? 126 : 185;
    var top = 34;
    var bottom = height - 10;
    var available = bottom - top;

    function stack_nodes(items, x, w, total_span, gap, fixed_h) {
      if (!items.length) {
        return [];
      }
      var node_h = Math.min(
        fixed_h,
        (total_span - gap * Math.max(0, items.length - 1)) / items.length,
      );
      var total_h = node_h * items.length + gap * (items.length - 1);
      var cursor = top + (total_span - total_h) / 2;
      return items.map(function (item) {
        var node = {
          label: item.label || item.name,
          value: item.value,
          color: item.color,
          x: x,
          y: cursor,
          w: w,
          h: node_h,
          center: cursor + node_h / 2,
        };
        cursor += node_h + gap;
        return node;
      });
    }

    var source_nodes = stack_nodes(
      categories,
      left_x,
      left_w,
      available,
      9,
      compact ? 27 : 30,
    );
    var stock_nodes = stack_nodes(
      stocks.map(function (stock) {
        return {
          name: stock.code,
          label: (flow_state.data.stock_meta[stock.code] || {}).name || stock.code,
          value: stock.value,
          color: stock.value >= 0 ? "var(--flow-red)" : "var(--flow-green)",
        };
      }),
      right_x,
      right_w,
      available,
      7,
      compact ? 22 : 24,
    );
    var concept_value = flow_concept_total(concept);
    var concept_h = compact ? 50 : 60;
    var concept_node = {
      x: concept_x,
      y: height / 2 - concept_h / 2,
      w: concept_w,
      h: concept_h,
      center: height / 2,
      color: concept_value >= 0 ? "var(--flow-red)" : "var(--flow-green)",
    };
    var max_abs = Math.max(
      1,
      Math.max.apply(
        null,
        source_nodes
          .map(function (node) {
            return Math.abs(node.value);
          })
          .concat(
            stock_nodes.map(function (node) {
              return Math.abs(node.value);
            }),
          ),
      ),
    );

    var source_links = source_nodes
      .map(function (node) {
        var mid_x = (node.x + node.w + concept_node.x) / 2;
        return (
          '<path d="M' +
          (node.x + node.w) +
          " " +
          node.center +
          " C" +
          mid_x +
          " " +
          node.center +
          " " +
          mid_x +
          " " +
          concept_node.center +
          " " +
          concept_node.x +
          " " +
          concept_node.center +
          '" fill="none" stroke="' +
          node.color +
          '" stroke-width="' +
          Math.max(1.2, (Math.abs(node.value) / max_abs) * 16) +
          '" opacity="0.36"/>'
        );
      })
      .join("");
    var stock_links = stock_nodes
      .map(function (node) {
        var mid_x = (concept_node.x + concept_node.w + node.x) / 2;
        return (
          '<path d="M' +
          (concept_node.x + concept_node.w) +
          " " +
          concept_node.center +
          " C" +
          mid_x +
          " " +
          concept_node.center +
          " " +
          mid_x +
          " " +
          node.center +
          " " +
          node.x +
          " " +
          node.center +
          '" fill="none" stroke="' +
          node.color +
          '" stroke-width="' +
          Math.max(1.2, (Math.abs(node.value) / max_abs) * 16) +
          '" opacity="0.36"/>'
        );
      })
      .join("");

    var source_rects = source_nodes
      .map(function (node) {
        return (
          '<rect x="' +
          node.x +
          '" y="' +
          node.y +
          '" width="' +
          node.w +
          '" height="' +
          node.h +
          '" rx="4" fill="' +
          node.color +
          '" opacity="0.16" stroke="' +
          node.color +
          '" stroke-width="0.8"/>' +
          '<text x="' +
          (node.x + 6) +
          '" y="' +
          (node.y + Math.min(15, node.h / 2 + 3)) +
          '" fill="var(--flow-ink)" font-size="' +
          (compact ? 8.5 : 10) +
          '">' +
          esc(node.label) +
          "</text>" +
          '<text x="' +
          (node.x + 6) +
          '" y="' +
          (node.y + Math.min(26, node.h - 3)) +
          '" fill="' +
          node.color +
          '" font-size="' +
          (compact ? 8 : 9) +
          '">' +
          esc(flow_signed(node.value)) +
          "</text>"
        );
      })
      .join("");
    var stock_rects = stock_nodes
      .map(function (node) {
        return (
          '<rect x="' +
          node.x +
          '" y="' +
          node.y +
          '" width="' +
          node.w +
          '" height="' +
          node.h +
          '" rx="4" fill="' +
          node.color +
          '" opacity="0.12" stroke="' +
          node.color +
          '" stroke-width="0.8"/>' +
          '<text x="' +
          (node.x + 6) +
          '" y="' +
          (node.y + Math.min(15, node.h / 2 + 3)) +
          '" fill="var(--flow-ink)" font-size="' +
          (compact ? 8.5 : 10) +
          '">' +
          esc(flow_truncate(node.label, compact ? 6 : 10)) +
          "</text>" +
          '<text x="' +
          (node.x + 6) +
          '" y="' +
          (node.y + Math.min(26, node.h - 3)) +
          '" fill="' +
          node.color +
          '" font-size="' +
          (compact ? 8 : 9) +
          '">' +
          esc(flow_signed(node.value)) +
          "</text>"
        );
      })
      .join("");
    var concept_rect =
      '<rect x="' +
      concept_node.x +
      '" y="' +
      concept_node.y +
      '" width="' +
      concept_node.w +
      '" height="' +
      concept_node.h +
      '" rx="5" fill="' +
      concept_node.color +
      '" opacity="0.14" stroke="' +
      concept_node.color +
      '" stroke-width="1"/>' +
      '<text x="' +
      (concept_node.x + concept_node.w / 2) +
      '" y="' +
      (concept_node.center - 3) +
      '" text-anchor="middle" fill="var(--flow-ink)" font-size="' +
      (compact ? 9 : 11) +
      '">' +
      esc(flow_truncate(concept, compact ? 5 : 8)) +
      "</text>" +
      '<text x="' +
      (concept_node.x + concept_node.w / 2) +
      '" y="' +
      (concept_node.center + 10) +
      '" text-anchor="middle" fill="' +
      concept_node.color +
      '" font-size="' +
      (compact ? 8 : 9.5) +
      '">' +
      esc(flow_signed(concept_value)) +
      "</text>";

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="资金流向路径" preserveAspectRatio="xMidYMid meet">' +
      '<text x="' +
      left_x +
      '" y="16" fill="var(--flow-muted)" font-size="9">资金源</text>' +
      '<text x="' +
      concept_x +
      '" y="16" fill="var(--flow-muted)" font-size="9">概念</text>' +
      '<text x="' +
      right_x +
      '" y="16" fill="var(--flow-muted)" font-size="9">代表股票</text>' +
      source_links +
      stock_links +
      source_rects +
      concept_rect +
      stock_rects +
      "</svg>"
    );
  }

  function flow_contrib_html() {
    var concept = flow_state.concept;
    var stocks = flow_stock_totals(concept);
    if (!stocks.length) {
      return empty_html("该概念暂无个股贡献");
    }
    var positive = stocks.filter(function (stock) {
      return stock.value > 0;
    }).slice(0, 3);
    var negative = stocks.filter(function (stock) {
      return stock.value < 0;
    }).slice(0, 3);
    var max_abs = 1;
    stocks.forEach(function (stock) {
      max_abs = Math.max(max_abs, Math.abs(stock.value));
    });

    function rows(items) {
      if (!items.length) {
        return '<div class="flow-contrib-empty">暂无</div>';
      }
      return items
        .map(function (stock) {
          var meta = (flow_state.data.stock_meta || {})[stock.code] || {};
          var active = stock.code === flow_state.stock ? " active" : "";
          return (
            '<button type="button" class="flow-stock' +
            active +
            '" data-code="' +
            esc(stock.code) +
            '"><span class="flow-stock-name">' +
            esc(meta.name || stock.code) +
            '</span><span class="flow-stock-bar">' +
            flow_bar(stock.value, max_abs) +
            '</span><b class="' +
            flow_sign_class(stock.value) +
            '">' +
            esc(flow_signed(stock.value)) +
            "</b></button>"
          );
        })
        .join("");
    }

    return (
      '<div class="flow-contrib-groups"><div><h4>净流入</h4>' +
      rows(positive) +
      '</div><div><h4>净流出</h4>' +
      rows(negative) +
      "</div></div>"
    );
  }

  function flow_detail_html() {
    var code = flow_state.stock;
    if (!code) {
      return '<div class="flow-detail-empty">点击下方个股，查看其在所选概念中的代表贡献</div>';
    }
    var meta = (flow_state.data.stock_meta || {})[code] || {};
    var stocks = flow_stock_totals(flow_state.concept);
    var contribution = 0;
    stocks.forEach(function (stock) {
      if (stock.code === code) {
        contribution = stock.value;
      }
    });
    var concepts = (meta.concepts || [])
      .map(function (concept) {
        return '<span class="flow-tag">' + esc(concept) + "</span>";
      })
      .join("");
    return (
      '<div class="flow-detail-head"><div><h4>' +
      esc(meta.name || code) +
      " <em>" +
      esc(meta.ticker || code) +
      "</em></h4><p>所选概念：" +
      esc(flow_state.concept || "--") +
      " · 近 " +
      flow_window() +
      " 日</p></div><span class=\"flow-detail-value " +
      flow_sign_class(contribution) +
      '">' +
      esc(flow_signed(contribution)) +
      "</span></div>" +
      (concepts ? '<div class="flow-detail-tags">' + concepts + "</div>" : "") +
      '<div class="flow-detail-note">概念贡献按个股概念数等分；该数值仅代表所选概念的区间贡献，不是全市场推荐。</div>'
    );
  }

  function flow_show_detail(code) {
    flow_state.stock = code;
    $("#flow-detail").html(flow_detail_html());
    $("#flow-contrib").html(flow_contrib_html());
    var element = document.getElementById("flow-detail");
    if (element) {
      element.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }

  function flow_render_path() {
    var concept = flow_ensure_concept();
    $("#flow-path").html(concept ? flow_path_svg(concept) : empty_html("暂无路径"));
    $("#flow-contrib").html(concept ? flow_contrib_html() : empty_html("暂无个股贡献"));
  }

  function flow_render_all() {
    var concept = flow_ensure_concept();
    $("#flow-kpis").html(flow_stats_html());
    $("#flow-chart").html(flow_chart_svg());
    $("#flow-ranking").html(flow_ranking_html());
    flow_render_path();
    $("#flow-detail").html(flow_detail_html());
    $(".flow-board-tab")
      .removeClass("active")
      .attr("aria-pressed", "false");
    $('.flow-board-tab[data-board="' + flow_state.board + '"]')
      .addClass("active")
      .attr("aria-pressed", "true");
    $(".flow-range-tab")
      .removeClass("active")
      .attr("aria-pressed", "false");
    $('.flow-range-tab[data-range="' + flow_state.range + '"]')
      .addClass("active")
      .attr("aria-pressed", "true");
    $("#flow-chart-title").text("近" + flow_window() + "个交易日概念累计净额轨迹");
    var source_label =
      flow_state.board === "all"
        ? "机构 / 游资 / 其他"
        : flow_state.board === "org"
          ? "机构"
          : "游资";
    $("#flow-path-sub").text(source_label + " → 概念 → 代表股票；宽度按绝对净额");
  }

  function render_flow_topology(data) {
    flow_state.data = data;
    flow_state.board = "all";
    flow_state.range = Math.min(20, (data.dates || []).length);
    flow_state.concept = null;
    flow_state.stock = null;
    var warnings = (data.warnings || [])
      .map(function (warning) {
        return "<li>" + esc(warning) + "</li>";
      })
      .join("");
    var definitions = Object.keys(data.definitions || {}).map(function (key) {
      return data.definitions[key];
    });
    var date_label = data.trade_date || "--";
    var html =
      '<div class="flow-kpis" id="flow-kpis">' +
      flow_stats_html() +
      "</div>" +
      (warnings
        ? '<div class="flow-warning"><b>数据提示</b><ul>' + warnings + "</ul></div>"
        : "") +
      '<div class="flow-controls"><div class="flow-board-tabs"><button type="button" class="flow-board-tab" data-board="all" aria-pressed="false">全部榜</button><button type="button" class="flow-board-tab" data-board="org" aria-pressed="false">机构榜</button><button type="button" class="flow-board-tab" data-board="hot" aria-pressed="false">游资榜</button></div>' +
      '<div class="flow-range-tabs"><button type="button" class="flow-range-tab" data-range="5" aria-pressed="false">5日</button><button type="button" class="flow-range-tab" data-range="10" aria-pressed="false">10日</button><button type="button" class="flow-range-tab" data-range="20" aria-pressed="false">20日</button></div>' +
      '<span class="flow-control-note">核心指标为最新交易日；轨迹与路径按所选区间</span></div>' +
      '<div class="flow-grid"><section class="flow-card flow-chart-card"><div class="flow-card-head"><div><h3 id="flow-chart-title">近20个交易日概念累计净额轨迹</h3><p>同一股票多个概念按概念数等分净额；端点标签自动错位</p></div></div><div class="flow-chart" id="flow-chart"></div></section>' +
      '<section class="flow-card"><div class="flow-card-head"><div><h3>区间净流向排行</h3><p>点击概念联动路径与个股贡献</p></div></div><div class="flow-ranking" id="flow-ranking"></div></section></div>' +
      '<div class="flow-grid2"><section class="flow-card"><div class="flow-card-head"><div><h3>资金路径</h3><p id="flow-path-sub">机构 / 游资 / 其他 → 概念 → 代表股票；宽度按绝对净额</p></div></div><div class="flow-path" id="flow-path"></div></section>' +
      '<section class="flow-card"><div class="flow-card-head"><div><h3>所选概念个股贡献</h3><p>按净流入与净流出分组展示代表股票</p></div></div><div id="flow-contrib"></div></section></div>' +
      '<section class="flow-card flow-detail" id="flow-detail">' +
      flow_detail_html() +
      "</section>" +
      '<footer class="flow-footer"><div class="flow-sources">数据源：' +
      esc((data.sources || []).join("、")) +
      " · 交易日 " +
      esc(date_label) +
      " · " +
      (data.is_simulated ? "模拟数据" : "真实数据") +
      " · 数据时间 " +
      esc(dtw_time(data.data_time_ms)) +
      " · 窗口 " +
      esc(data.window_start || "--") +
      " ~ " +
      esc(date_label) +
      '</div><div class="flow-definitions">' +
      definitions
        .map(function (line) {
          return "<span>" + esc(line) + "</span>";
        })
        .join("") +
      '</div><div class="flow-disclaimer">仅供信息展示，不构成投资建议</div></footer>';

    $("#flow-body").html(html);
    flow_render_all();

    $("#ths-flow-body")
      .off(".flow")
      .on("click.flow", ".flow-board-tab", function () {
        flow_state.board = String($(this).data("board"));
        flow_state.concept = null;
        flow_state.stock = null;
        flow_render_all();
      })
      .on("click.flow", ".flow-range-tab", function () {
        flow_state.range = Number($(this).data("range"));
        flow_state.concept = null;
        flow_state.stock = null;
        flow_render_all();
      })
      .on("click.flow", ".flow-rank", function () {
        flow_state.concept = String($(this).data("concept"));
        flow_state.stock = null;
        $("#flow-ranking").html(flow_ranking_html());
        flow_render_path();
        $("#flow-detail").html(flow_detail_html());
      })
      .on("click.flow", ".flow-stock", function () {
        flow_show_detail(String($(this).data("code")));
      })
      .on("keydown.flow", ".flow-rank", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          flow_state.concept = String($(this).data("concept"));
          flow_state.stock = null;
          $("#flow-ranking").html(flow_ranking_html());
          flow_render_path();
          $("#flow-detail").html(flow_detail_html());
        }
      })
      .on("keydown.flow", ".flow-stock", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          flow_show_detail(String($(this).data("code")));
        }
      });
  }

  function open_flow_topology() {
    flow_state = {
      data: null,
      board: "all",
      range: 20,
      concept: null,
      stock: null,
    };
    var html =
      '<div class="ths-flow" id="ths-flow-body">' +
      '<div class="flow-hero"><div><div class="flow-kicker">DRAGON-TIGER CAPITAL TOPOLOGY</div>' +
      "<h2>龙虎榜资金流向拓扑台</h2>" +
      '<p>从近一年交易日历中取最近 20 个交易日，观察概念累计净额、资金路径与个股正负贡献。</p></div>' +
      '<div class="flow-hero-right"><span class="flow-badge" id="flow-badge">真实数据</span><span class="flow-hero-date" id="flow-hero-date">交易日 --</span></div></div>' +
      '<div id="flow-body">' +
      loading_html("正在读取近一年交易日历与近20个交易日榜单，约需数秒...") +
      "</div></div>";
    open_layer(
      "同花顺 · 龙虎榜资金流向拓扑台",
      html,
      1240,
      900,
    );

    request("flow_topology", {})
      .then(function (data) {
        $("#flow-badge").text(data.is_simulated ? "模拟数据" : "真实数据");
        $("#flow-hero-date").text(
          "交易日 " +
            (data.trade_date || "--") +
            " · 近" +
            data.dates.length +
            "日 · 日历" +
            data.calendar_count +
            "日",
        );
        render_flow_topology(data);
      })
      .catch(function (err) {
        $("#flow-body").html(
          '<div class="ths-empty ths-error">' +
            esc(err.message || "资金流向数据获取失败") +
            "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 行业强度作战矩阵
  // ------------------------------------------------------------------
  var ind_state = {
    data: null,
    window: 20,
    selected: null,
    heatSort: "strength",
    loading: false,
  };

  function ind_window_key() {
    return ind_state.window === 5 ? "rs5" : ind_state.window === 60 ? "rs60" : "rs20";
  }

  function ind_quadrant_color(x, y) {
    if (x >= 0 && y >= 0) {
      return "#e23d2f";
    }
    if (x >= 0 && y < 0) {
      return "#d98a00";
    }
    if (x < 0 && y >= 0) {
      return "#3c78d8";
    }
    return "#1f7a5c";
  }

  function ind_num(value, digits) {
    if (value === null || value === undefined) {
      return "--";
    }
    return Number(value).toFixed(digits === undefined ? 1 : digits);
  }

  function ind_stats_html(data) {
    var stats = data.stats || {};
    var cards = [
      {
        label: "强势行业",
        value: (stats.strong_count || 0) + " / " + (stats.industry_count || 0),
        sub: "20日相对强度 > 0",
      },
      {
        label: "上升行业",
        value: String(stats.rising_count || 0),
        sub: "5日相对强度 > 0",
      },
      {
        label: "轮动速度",
        value: ind_num(stats.rotation) + " 名",
        sub: "20日排名变化绝对值均值",
      },
      {
        label: "市场宽度",
        value: ind_num(stats.breadth) + "%",
        sub: "5日相对强度为正占比",
      },
    ];
    return cards
      .map(function (card) {
        return (
          '<div class="ind-kpi"><span class="ind-kpi-label">' +
          esc(card.label) +
          '</span><div class="ind-kpi-value">' +
          esc(card.value) +
          '</div><div class="ind-kpi-sub">' +
          esc(card.sub) +
          "</div></div>"
        );
      })
      .join("");
  }

  function ind_bubble_items() {
    var key = ind_window_key();
    return ((ind_state.data && ind_state.data.bubble) || [])
      .map(function (item) {
        return {
          code: item.code,
          name: item.name,
          x: item[key],
          y: item.rs5,
          pulse: item.volume_pulse,
          rank: item.rank,
          rankChange: item.rank_change,
          rs20: item.rs20,
          rs60: item.rs60,
        };
      })
      .filter(function (item) {
        return item.x !== null && item.x !== undefined && item.y !== null && item.y !== undefined;
      });
  }

  function ind_bubble_svg() {
    var items = ind_bubble_items();
    if (!items.length) {
      return empty_html("暂无可计算的行业强度");
    }
    var compact = window.innerWidth < 720;
    var width = compact ? 420 : 840;
    var height = compact ? 330 : 400;
    var pad_l = compact ? 38 : 48;
    var pad_r = compact ? 14 : 22;
    var pad_t = 18;
    var pad_b = 38;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var xs = items.map(function (item) {
      return item.x;
    });
    var ys = items.map(function (item) {
      return item.y;
    });
    var min_x = Math.min.apply(null, xs);
    var max_x = Math.max.apply(null, xs);
    var min_y = Math.min.apply(null, ys);
    var max_y = Math.max.apply(null, ys);
    if (min_x === max_x) {
      min_x -= 1;
      max_x += 1;
    }
    if (min_y === max_y) {
      min_y -= 1;
      max_y += 1;
    }
    var pad_x = (max_x - min_x) * 0.12;
    var pad_y = (max_y - min_y) * 0.12;
    min_x -= pad_x;
    max_x += pad_x;
    min_y -= pad_y;
    max_y += pad_y;

    function x(value) {
      return pad_l + ((value - min_x) / (max_x - min_x)) * plot_w;
    }

    function y(value) {
      return pad_t + ((max_y - value) / (max_y - min_y)) * plot_h;
    }

    var grid = "";
    for (var tick = 0; tick <= 4; tick++) {
      var gy = pad_t + (plot_h * tick) / 4;
      var gx = pad_l + (plot_w * tick) / 4;
      grid +=
        '<line x1="' +
        pad_l +
        '" y1="' +
        gy +
        '" x2="' +
        (pad_l + plot_w) +
        '" y2="' +
        gy +
        '" stroke="var(--ind-grid)" stroke-width="1"/>' +
        '<line x1="' +
        gx +
        '" y1="' +
        pad_t +
        '" x2="' +
        gx +
        '" y2="' +
        (pad_t + plot_h) +
        '" stroke="var(--ind-grid)" stroke-width="1"/>';
    }
    var zero_x = x(0);
    var zero_y = y(0);
    var axes =
      '<line x1="' +
      zero_x +
      '" y1="' +
      pad_t +
      '" x2="' +
      zero_x +
      '" y2="' +
      (pad_t + plot_h) +
      '" stroke="var(--ind-muted)" stroke-width="1" stroke-dasharray="4 3"/>' +
      '<line x1="' +
      pad_l +
      '" y1="' +
      zero_y +
      '" x2="' +
      (pad_l + plot_w) +
      '" y2="' +
      zero_y +
      '" stroke="var(--ind-muted)" stroke-width="1" stroke-dasharray="4 3"/>';

    var labels = [];
    var bubbles = items
      .map(function (item) {
        var px = x(item.x);
        var py = y(item.y);
        var pulse = item.pulse === null || item.pulse === undefined ? 1 : item.pulse;
        var radius = Math.max(4, Math.min(11, 4 + (Math.max(0.3, Math.min(3, pulse)) / 3) * 7));
        var color = ind_quadrant_color(item.x, item.y);
        var selected = item.code === ind_state.selected;
        labels.push({ item: item, px: px, py: py, radius: radius });
        return (
          '<g class="ind-bubble" tabindex="0" role="button" data-code="' +
          esc(item.code) +
          '"><circle cx="' +
          px +
          '" cy="' +
          py +
          '" r="' +
          radius +
          '" fill="' +
          color +
          '" fill-opacity="' +
          (selected ? 0.95 : 0.72) +
          '" stroke="' +
          (selected ? "var(--ind-ink)" : color) +
          '" stroke-width="' +
          (selected ? 2 : 0.8) +
          '"><title>' +
          esc(
            item.name +
              " · " +
              ind_state.window +
              "日RS " +
              ind_num(item.x) +
              "% · 5日RS " +
              ind_num(item.y) +
              "% · 成交额脉冲 " +
              ind_num(item.pulse, 2) +
              " · 排名变化 " +
              (item.rankChange === null || item.rankChange === undefined
                ? "--"
                : item.rankChange),
          ) +
          "</title></circle></g>"
        );
      })
      .join("");

    labels.sort(function (a, b) {
      return a.py - b.py;
    });
    var selectedLabels = labels
      .filter(function (label) {
        return label.item.code === ind_state.selected;
      })
      .concat(
        labels
          .filter(function (label) {
            return label.item.code !== ind_state.selected;
          })
          .sort(function (a, b) {
            return (
              Math.abs(b.item.x) +
              Math.abs(b.item.y) -
              (Math.abs(a.item.x) + Math.abs(a.item.y))
            );
          })
          .slice(0, compact ? 5 : 7),
      );
    selectedLabels.sort(function (a, b) {
      return a.py - b.py;
    });
    var placed = [];
    selectedLabels.forEach(function (label) {
      var label_y = Math.max(pad_t + 8, Math.min(pad_t + plot_h - 8, label.py));
      if (placed.length) {
        label_y = Math.max(label_y, placed[placed.length - 1].label_y + 12);
      }
      placed.push({ ...label, label_y: label_y });
    });
    var marks = placed
      .map(function (label) {
        var label_x = label.px + label.radius + 3;
        var anchor = "start";
        if (label_x > pad_l + plot_w - 42) {
          label_x = label.px - label.radius - 3;
          anchor = "end";
        }
        return (
          '<text x="' +
          label_x +
          '" y="' +
          (label.label_y + 3) +
          '" text-anchor="' +
          anchor +
          '" fill="var(--ind-ink)" font-size="' +
          (compact ? 8 : 9) +
          '">' +
          esc(flow_truncate(label.item.name, compact ? 5 : 7)) +
          "</text>"
        );
      })
      .join("");

    var x_ticks = [];
    for (var i = 0; i <= 4; i++) {
      x_ticks.push(min_x + ((max_x - min_x) * i) / 4);
    }
    var y_ticks = [];
    for (var j = 0; j <= 4; j++) {
      y_ticks.push(max_y - ((max_y - min_y) * j) / 4);
    }
    var tick_labels = x_ticks
      .map(function (value, index) {
        return (
          '<text x="' +
          (pad_l + (plot_w * index) / 4) +
          '" y="' +
          (height - 18) +
          '" text-anchor="middle" fill="var(--ind-muted)" font-size="8">' +
          ind_num(value) +
          "</text>"
        );
      })
      .join("") +
      y_ticks
        .map(function (value, index) {
          return (
            '<text x="' +
            (pad_l - 6) +
            '" y="' +
            (pad_t + (plot_h * index) / 4 + 3) +
            '" text-anchor="end" fill="var(--ind-muted)" font-size="8">' +
            ind_num(value) +
            "</text>"
          );
        })
        .join("");

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="行业强度气泡" preserveAspectRatio="xMidYMid meet">' +
      grid +
      axes +
      bubbles +
      marks +
      tick_labels +
      '<text x="' +
      (pad_l + plot_w / 2) +
      '" y="' +
      (height - 4) +
      '" text-anchor="middle" fill="var(--ind-muted)" font-size="9">' +
      ind_state.window +
      "日相对沪深300超额 (%)</text>" +
      '<text x="12" y="' +
      (pad_t + plot_h / 2) +
      '" text-anchor="middle" fill="var(--ind-muted)" font-size="9" transform="rotate(-90 12 ' +
      (pad_t + plot_h / 2) +
      ')">5日相对强度 (%)</text>' +
      "</svg>"
    );
  }

  function ind_spark_svg(detail) {
    var dates = detail.dates || [];
    var industry = detail.industry_series || [];
    var benchmark = detail.benchmark_series || [];
    if (!dates.length || !industry.length) {
      return empty_html("暂无行业走势");
    }
    var width = 430;
    var height = 168;
    var pad_l = 28;
    var pad_r = 46;
    var pad_t = 12;
    var pad_b = 22;
    var plot_w = width - pad_l - pad_r;
    var plot_h = height - pad_t - pad_b;
    var values = industry.concat(benchmark);
    var min_value = Math.min.apply(null, values);
    var max_value = Math.max.apply(null, values);
    var padding = Math.max(0.6, (max_value - min_value) * 0.12);
    min_value -= padding;
    max_value += padding;

    function x(index) {
      return dates.length === 1
        ? pad_l + plot_w / 2
        : pad_l + (index / (dates.length - 1)) * plot_w;
    }

    function y(value) {
      return pad_t + ((max_value - value) / (max_value - min_value)) * plot_h;
    }

    function path(values) {
      return (
        "M" +
        values
          .map(function (value, index) {
            return x(index) + " " + y(value);
          })
          .join(" L")
      );
    }

    var grid = "";
    for (var tick = 0; tick <= 3; tick++) {
      var value = min_value + ((max_value - min_value) * tick) / 3;
      grid +=
        '<line x1="' +
        pad_l +
        '" y1="' +
        y(value) +
        '" x2="' +
        (pad_l + plot_w) +
        '" y2="' +
        y(value) +
        '" stroke="var(--ind-grid)" stroke-width="1"/>' +
        '<text x="' +
        (pad_l - 5) +
        '" y="' +
        (y(value) + 3) +
        '" text-anchor="end" fill="var(--ind-muted)" font-size="8">' +
        ind_num(value) +
        "</text>";
    }

    return (
      '<svg viewBox="0 0 ' +
      width +
      " " +
      height +
      '" role="img" aria-label="行业与沪深300归一化走势" preserveAspectRatio="xMidYMid meet">' +
      grid +
      '<path d="' +
      path(industry) +
      '" fill="none" stroke="var(--ind-red)" stroke-width="1.6"/>' +
      '<path d="' +
      path(benchmark) +
      '" fill="none" stroke="var(--ind-muted)" stroke-width="1.1" stroke-dasharray="3 3"/>' +
      '<text x="' +
      (pad_l + plot_w + 4) +
      '" y="' +
      y(industry[industry.length - 1]) +
      '" fill="var(--ind-red)" font-size="9">行业</text>' +
      '<text x="' +
      (pad_l + plot_w + 4) +
      '" y="' +
      y(benchmark[benchmark.length - 1]) +
      '" fill="var(--ind-muted)" font-size="9">沪深300</text>' +
      "</svg>"
    );
  }

  function ind_stock_rows(items, positive) {
    if (!items || !items.length) {
      return '<div class="ind-empty">暂无</div>';
    }
    var max_abs = 1;
    items.forEach(function (item) {
      max_abs = Math.max(max_abs, Math.abs(item.pct || 0));
    });
    return items
      .map(function (item) {
        var width = Math.min(50, (Math.abs(item.pct || 0) / max_abs) * 50);
        var fill =
          (item.pct || 0) >= 0
            ? '<i class="ind-fill pos" style="left:50%;width:' + width + '%"></i>'
            : '<i class="ind-fill neg" style="right:50%;width:' + width + '%"></i>';
        return (
          '<div class="ind-stock-row"><span class="ind-stock-name">' +
          esc(item.name || item.ticker) +
          '</span><div class="ind-stock-bar"><i class="ind-mid"></i>' +
          fill +
          '</div><b class="' +
          ((item.pct || 0) >= 0 ? "up" : "down") +
          '">' +
          (item.pct > 0 ? "+" : "") +
          ind_num(item.pct, 2) +
          "%</b></div>"
        );
      })
      .join("");
  }

  function ind_detail_html() {
    if (ind_state.loading) {
      return loading_html("正在读取行业成分与股票快照...");
    }
    var detail = ind_state.data && ind_state.data.selected;
    if (!detail) {
      return empty_html("选择行业查看成分与强度");
    }
    var consts = detail.constituents || {};
    var rankText =
      detail.rank === null || detail.rank === undefined
        ? "--"
        : "#" + detail.rank;
    var rankChange = detail.rank_change;
    return (
      '<div class="ind-detail-head"><div><h3>' +
      esc(detail.name) +
      ' <em>' +
      esc(rankText) +
      '</em></h3><p>当前成分等权代理 · 非指数贡献 · 成交额脉冲 ' +
      esc(ind_num(detail.volume_pulse, 2)) +
      '</p></div><span class="ind-detail-rank ' +
      (rankChange > 0 ? "up" : rankChange < 0 ? "down" : "") +
      '">' +
      (rankChange === null || rankChange === undefined
        ? "--"
        : (rankChange > 0 ? "+" : "") + rankChange + " 名") +
      "</span></div>" +
      '<div class="ind-rs-row"><div><span>5日RS</span><b class="' +
      ((detail.rs5 || 0) >= 0 ? "up" : "down") +
      '">' +
      ind_num(detail.rs5) +
      "%</b></div><div><span>20日RS</span><b class=\"" +
      ((detail.rs20 || 0) >= 0 ? "up" : "down") +
      '">' +
      ind_num(detail.rs20) +
      "%</b></div><div><span>60日RS</span><b class=\"" +
      ((detail.rs60 || 0) >= 0 ? "up" : "down") +
      '">' +
      ind_num(detail.rs60) +
      "%</b></div></div>" +
      '<div class="ind-spark">' +
      ind_spark_svg(detail) +
      '<div class="ind-spark-note">归一化 100 · 近20个交易日</div></div>' +
      '<div class="ind-breadth"><div><span>上涨家数</span><b class="up">' +
      esc(consts.up || 0) +
      '</b></div><div><span>下跌家数</span><b class="down">' +
      esc(consts.down || 0) +
      '</b></div><div><span>平盘</span><b>' +
      esc(consts.flat || 0) +
      '</b></div><div><span>等权涨跌代理</span><b class="' +
      ((consts.equal_weight_pct || 0) >= 0 ? "up" : "down") +
      '">' +
      (consts.equal_weight_pct > 0 ? "+" : "") +
      ind_num(consts.equal_weight_pct, 2) +
      "%</b></div></div>" +
      '<div class="ind-constituents"><div><h4>涨幅前列</h4>' +
      ind_stock_rows(consts.top_gainers, true) +
      '</div><div><h4>跌幅前列</h4>' +
      ind_stock_rows(consts.top_losers, false) +
      "</div></div>" +
      '<div class="ind-proxy-note">当前成分快照 ' +
      esc(consts.count || 0) +
      " 只，目录成分 " +
      esc(consts.catalog_count || 0) +
      " 只，成交额 " +
      esc(fin_yi(consts.turnover_total || 0)) +
      " 亿；等权代理仅用于观察当前成分，不代表指数贡献。</div>"
    );
  }

  function ind_heatmap_html() {
    var heat = (ind_state.data && ind_state.data.heatmap) || {};
    var dates = heat.dates || [];
    var rows = (heat.rows || []).slice();
    if (!dates.length || !rows.length) {
      return empty_html("暂无热力带数据");
    }
    if (ind_state.heatSort === "change") {
      rows.sort(function (a, b) {
        return (b.rank_change || -9999) - (a.rank_change || -9999);
      });
    } else {
      rows.sort(function (a, b) {
        return b.latest - a.latest;
      });
    }
    var head =
      '<thead><tr><th class="ind-heat-rowhead">行业</th>' +
      dates
        .map(function (date) {
          return "<th>" + esc(date.slice(5)) + "</th>";
        })
        .join("") +
      "</tr></thead>";
    var body = rows
      .map(function (row) {
        var cells = (row.values || [])
          .map(function (value) {
            if (value === null || value === undefined) {
              return '<td class="ind-heat-cell empty"></td>';
            }
            var alpha = Math.min(0.8, 0.08 + (Math.abs(value) / 3) * 0.72);
            var background = value >= 0 ? "226,61,47" : "31,122,92";
            var text_color =
              alpha > 0.48 ? "rgba(255,255,255,.95)" : "var(--ind-ink)";
            return (
              '<td class="ind-heat-cell" style="background:rgba(' +
              background +
              "," +
              alpha.toFixed(2) +
              ");color:" +
              text_color +
              '">' +
              (Math.abs(value) >= 1.2 ? esc(ind_num(value, 1)) : "") +
              "</td>"
            );
          })
          .join("");
        return (
          '<tr class="ind-heat-tr' +
          (row.code === ind_state.selected ? " selected" : "") +
          '"><td class="ind-heat-rowhead"><button type="button" class="ind-heat-row" data-code="' +
          esc(row.code) +
          '"><b>' +
          esc(row.name) +
          '</b><em class="' +
          (row.rank_change > 0 ? "up" : row.rank_change < 0 ? "down" : "") +
          '">' +
          (row.rank_change === null || row.rank_change === undefined
            ? ""
            : (row.rank_change > 0 ? "+" : "") + row.rank_change) +
          "</em></button></td>" +
          cells +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="ind-heat-wrap"><table class="ind-heatmap">' +
      head +
      "<tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function ind_industry_options() {
    var selected = ind_state.selected;
    return ((ind_state.data && ind_state.data.industries) || [])
      .slice()
      .sort(function (a, b) {
        return String(a.name).localeCompare(String(b.name), "zh-CN");
      })
      .map(function (item) {
        return (
          '<option value="' +
          esc(item.code) +
          '"' +
          (item.code === selected ? " selected" : "") +
          ">" +
          esc(item.name) +
          "</option>"
        );
      })
      .join("");
  }

  function ind_render_all() {
    $("#ind-kpis").html(ind_stats_html(ind_state.data));
    $("#ind-bubble").html(ind_bubble_svg());
    $("#ind-detail").html(ind_detail_html());
    $("#ind-heatmap").html(ind_heatmap_html());
    $("#ind-select").html(ind_industry_options()).val(ind_state.selected);
    $(".ind-window-tab")
      .removeClass("active")
      .attr("aria-pressed", "false");
    $('.ind-window-tab[data-window="' + ind_state.window + '"]')
      .addClass("active")
      .attr("aria-pressed", "true");
    $(".ind-sort-tab").removeClass("active");
    $('.ind-sort-tab[data-sort="' + ind_state.heatSort + '"]').addClass("active");
  }

  function ind_select(code) {
    if (!code || ind_state.loading) {
      return;
    }
    ind_state.loading = true;
    $("#ind-detail").html(loading_html("正在读取行业成分与股票快照..."));
    request("industry_matrix", { thscode: code })
      .then(function (data) {
        ind_state.data = data;
        ind_state.selected = data.selected.code;
        ind_state.loading = false;
        ind_render_all();
      })
      .catch(function (err) {
        ind_state.loading = false;
        $("#ind-detail").html(
          '<div class="ths-empty ths-error">' +
            esc(err.message || "行业成分数据获取失败") +
            "</div>",
        );
      });
  }

  function render_industry_matrix(data) {
    ind_state.data = data;
    ind_state.selected = data.selected.code;
    ind_state.window = 20;
    ind_state.heatSort = "strength";
    ind_state.loading = false;
    var html =
      '<div id="ind-kpis" class="ind-kpis">' +
      ind_stats_html(data) +
      "</div>" +
      '<div class="ind-controls"><div class="ind-window-tabs"><button type="button" class="ind-window-tab" data-window="5" aria-pressed="false">5D</button><button type="button" class="ind-window-tab" data-window="20" aria-pressed="false">20D</button><button type="button" class="ind-window-tab" data-window="60" aria-pressed="false">60D</button></div>' +
      '<select id="ind-select" class="ind-select" aria-label="选择行业">' +
      ind_industry_options() +
      "</select>" +
      '<span class="ind-control-note">相对强度 = 行业收益 - 沪深300收益；气泡大小 = 成交额脉冲</span></div>' +
      '<div class="ind-grid"><section class="ind-card ind-bubble-card"><div class="ind-card-head"><div><h3>行业强度 × 加速度</h3><p>横轴为所选窗口相对沪深300超额，纵轴为5日相对强度</p></div><div class="ind-legend"><span><i class="ind-dot red"></i>强势加速</span><span><i class="ind-dot amber"></i>高位回落</span><span><i class="ind-dot blue"></i>弱势修复</span><span><i class="ind-dot green"></i>同步走弱</span></div></div><div id="ind-bubble" class="ind-bubble"></div></section>' +
      '<section class="ind-card ind-detail-card"><div id="ind-detail"></div></section></div>' +
      '<section class="ind-card ind-heat-card"><div class="ind-card-head"><div><h3>近20日行业强度热力带</h3><p>20日相对强度的截面 z-score；点击行业切换档案</p></div><div class="ind-sort-tabs"><button type="button" class="ind-sort-tab" data-sort="strength">强度排序</button><button type="button" class="ind-sort-tab" data-sort="change">排名变化</button></div></div><div id="ind-heatmap"></div></section>' +
      '<footer class="ind-footer"><div class="ind-sources">数据源：' +
      esc((data.sources || []).join("、")) +
      " · 交易日 " +
      esc(data.trade_date || "--") +
      " · " +
      (data.is_simulated ? "模拟数据" : "真实数据") +
      " · 数据时间 " +
      esc(dtw_time(data.data_time_ms)) +
      '</div><div class="ind-definitions">' +
      Object.keys(data.definitions || {})
        .map(function (key) {
          return "<span>" + esc(data.definitions[key]) + "</span>";
        })
        .join("") +
      '</div><div class="ind-disclaimer">仅供信息展示，不构成投资建议</div></footer>';

    $("#ind-body").html(html);
    ind_render_all();

    $("#ths-ind-body")
      .off(".ind")
      .on("click.ind", ".ind-window-tab", function () {
        ind_state.window = Number($(this).data("window"));
        $("#ind-bubble").html(ind_bubble_svg());
        $(".ind-window-tab").removeClass("active");
        $(this).addClass("active");
      })
      .on("change.ind", "#ind-select", function () {
        ind_select(String($(this).val()));
      })
      .on("click.ind", ".ind-sort-tab", function () {
        ind_state.heatSort = String($(this).data("sort"));
        $("#ind-heatmap").html(ind_heatmap_html());
        $(".ind-sort-tab").removeClass("active");
        $(this).addClass("active");
      })
      .on("click.ind", ".ind-heat-row", function () {
        ind_select(String($(this).data("code")));
      })
      .on("click.ind", ".ind-bubble", function () {
        ind_select(String($(this).data("code")));
      })
      .on("keydown.ind", ".ind-bubble", function (event) {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          ind_select(String($(this).data("code")));
        }
      });
  }

  function open_industry_matrix() {
    ind_state = {
      data: null,
      window: 20,
      selected: null,
      heatSort: "strength",
      loading: false,
    };
    var html =
      '<div class="ths-ind" id="ths-ind-body">' +
      '<div class="ind-hero"><div><div class="ind-kicker">INDUSTRY STRENGTH MATRIX</div>' +
      "<h2>行业强度作战矩阵</h2>" +
      '<p>用相对强度、成交额脉冲与市场宽度观察行业轮动，不输出组合或交易指令。</p></div>' +
      '<div class="ind-hero-right"><span class="ind-badge" id="ind-badge">真实数据</span><span class="ind-hero-date" id="ind-hero-date">交易日 --</span></div></div>' +
      '<div id="ind-body">' +
      loading_html("正在读取90个同花顺行业指数历史K线...") +
      "</div></div>";
    open_layer(
      "同花顺 · 行业强度作战矩阵",
      html,
      1240,
      900,
    );

    request("industry_matrix", {})
      .then(function (data) {
        $("#ind-badge").text(data.is_simulated ? "模拟数据" : "真实数据");
        $("#ind-hero-date").text(
          "交易日 " +
            (data.trade_date || "--") +
            " · " +
            data.stats.industry_count +
            " 个行业",
        );
        render_industry_matrix(data);
      })
      .catch(function (err) {
        $("#ind-body").html(
          '<div class="ths-empty ths-error">' +
            esc(err.message || "行业强度数据获取失败") +
            "</div>",
        );
      });
  }

  // ------------------------------------------------------------------
  // 悬浮入口
  // ------------------------------------------------------------------
  function init_toolbar() {
    if (!is_a_market() || $("#ths-floating-bar").length) {
      return;
    }
    var buttons = [
      { action: "limit", icon: "layui-icon-chart-screen", text: "涨停池", title: "涨停 / 跌停 / 炸板股票池" },
      { action: "pulse", icon: "layui-icon-chart", text: "情绪脉冲", title: "涨停情绪市场脉冲屏" },
      { action: "ladder", icon: "layui-icon-rank", text: "连板天梯", title: "近 30 日连板梯队" },
      { action: "hot", icon: "layui-icon-fire", text: "热榜", title: "同花顺热股榜 / 飙升榜" },
      { action: "anomaly", icon: "layui-icon-notice", text: "异动", title: "个股异动原因解读" },
      { action: "dragon", icon: "layui-icon-list", text: "龙虎榜", title: "龙虎榜 / 机构榜 / 游资榜" },
      { action: "dragon_watch", icon: "layui-icon-flag", text: "龙虎观察", title: "龙虎榜机构与游资观察" },
      { action: "flow_topology", icon: "layui-icon-flow", text: "资金拓扑", title: "龙虎榜资金流向拓扑台" },
      { action: "industry_matrix", icon: "layui-icon-table", text: "行业矩阵", title: "行业强度作战矩阵" },
      { action: "monitor", icon: "layui-icon-eye", text: "监控", title: "严重异动与重点监控" },
      { action: "auction", icon: "layui-icon-time", text: "竞价", title: "集合竞价个股机会" },
    ];
    var html =
      '<div id="ths-floating-bar">' +
      '<span class="ths-fab-brand">同花顺</span>' +
      buttons
        .map(function (button) {
          return (
            '<button type="button" class="ths-fab" data-action="' +
            button.action +
            '" title="' +
            esc(button.title) +
            '"><i class="layui-icon ' +
            button.icon +
            '"></i><span>' +
            esc(button.text) +
            "</span></button>"
          );
        })
        .join("") +
      "</div>";
    $("body").append(html);

    $("#ths-floating-bar").on("click", ".ths-fab", function () {
      var action = $(this).data("action");
      if (action === "limit") {
        open_limit_pool();
      } else if (action === "pulse") {
        open_market_pulse();
      } else if (action === "ladder") {
        open_ladder();
      } else if (action === "hot") {
        open_hot();
      } else if (action === "anomaly") {
        open_anomaly();
      } else if (action === "dragon") {
        open_dragon_tiger();
      } else if (action === "dragon_watch") {
        open_dragon_tiger_watch();
      } else if (action === "flow_topology") {
        open_flow_topology();
      } else if (action === "industry_matrix") {
        open_industry_matrix();
      } else if (action === "monitor") {
        open_abnormal_monitor();
      } else if (action === "auction") {
        open_auction();
      }
    });
  }

  function init() {
    if (!is_a_market()) {
      return;
    }
    sync_theme_class();
    init_toolbar();

    $(document)
      .off("click.thsStock")
      .on("click.thsStock", ".ths-stock", function (event) {
        event.preventDefault();
        event.stopPropagation();
        switch_stock($(this).data("code"), $(this).data("name"));
      })
      .off("click.thsGoto")
      .on("click.thsGoto", ".ths-goto", function (event) {
        event.preventDefault();
        event.stopPropagation();
        switch_stock($(this).data("code"), $(this).data("name"));
      })
      .off("click.thsDept")
      .on("click.thsDept", ".ths-dept", function (event) {
        event.preventDefault();
        event.stopPropagation();
        open_department($(this).data("id"), $(this).data("name"));
      });
  }

  return {
    init: init,
    open_limit_pool: open_limit_pool,
    open_ladder: open_ladder,
    open_hot: open_hot,
    open_anomaly: open_anomaly,
    open_dragon_tiger: open_dragon_tiger,
    open_abnormal_monitor: open_abnormal_monitor,
    open_auction: open_auction,
    open_stock_anomaly: open_stock_anomaly,
    open_institution: open_institution,
    open_hot_rank: open_hot_rank,
    open_financial_health: open_financial_health,
    open_market_pulse: open_market_pulse,
    open_dragon_tiger_watch: open_dragon_tiger_watch,
    open_flow_topology: open_flow_topology,
    open_industry_matrix: open_industry_matrix,
  };
})();
