// 板块概念 JS 封装
var BKGN = (function () {
  var bkgnList = [];
  var xmSelectIns = null;

  // 初始化板块概念下拉和表格
  function init_bkgn_opts() {
    get_bkgn_list();
  }

  // 获取板块概念列表
  function get_bkgn_list() {
    $.get("/a/bkgn_list", function (res) {
      if (res.code === 0) {
        bkgnList = res.data || [];
        render_bkgn_xm_select();
      } else {
        layer.msg("获取板块概念失败");
      }
    });
  }

  // 渲染 xm-select 下拉
  function render_bkgn_xm_select() {
    var options = bkgnList.map(function (item) {
      return {
        name: item.bkgn_name,
        value: item.type + "|" + item.bkgn_code,
      };
    });
    if (xmSelectIns) {
      xmSelectIns.update({ data: options });
      return;
    }
    xmSelectIns = xmSelect.render({
      el: "#bkgn_xm_select",
      name: "bkgn_xm_select",
      filterable: true,
      radio: true,
      clickClose: true,
      height: "300px",
      model: { label: { type: "text" } },
      data: options,
      on: function (data) {
        if (data.arr && data.arr.length > 0) {
          var val = data.arr[0].value;
          var arr = val.split("|");
          if (arr.length === 2) {
            get_bkgn_codes(arr[0], arr[1]);
          }
        } else {
          $("#bkgn_table").hide();
        }
      },
      tips: "请选择板块/概念",
      theme: {
        color: "#c00",
      },
    });
  }

  // 获取板块概念下的股票
  function get_bkgn_codes(type, code) {
    layer.load(1);
    $.post(
      "/a/bkgn_codes",
      { bkgn_type: type, bkgn_code: code },
      function (res) {
        layer.closeAll("loading");
        if (res.code === 0) {
          render_bkgn_table(res.data || {});
        } else {
          layer.msg("获取股票列表失败");
        }
      }
    );
  }

  // 渲染股票表格
  function render_bkgn_table(stocks) {
    var data = [];
    for (var code in stocks) {
      if (stocks.hasOwnProperty(code)) {
        var s = stocks[code];
        data.push({
          code: code,
          name: s["name"] || "",
        });
      }
    }
    layui.table.render({
      elem: "#bkgn_table",
      data: data,
      cols: [
        [
          { field: "code", title: "代码", width: "48%" },
          { field: "name", title: "名称", width: "48%" },
        ],
      ],
      page: true,
      limit: 20,
      skin: "row",
      even: true,
    });
    // 行单击事件( 双击事件为: rowDouble )
    layui.table.on("row(bkgn_table)", function (obj) {
      const data = obj.data; // 获取当前行数据
      const code = data.code;
      change_chart_ticker(Utils.get_market(), code);
      $("#ai_code").val(code);
      table.setRowChecked("bkgn_table", {
        index: "all", // 所有行
        checked: false,
      });
      table.setRowChecked("bkgn_table", {
        index: obj.index, // 选中行的下标。 0 表示第一行
      });
    });
    $("#bkgn_table").show();
  }

  return {
    init_bkgn_opts: init_bkgn_opts,
  };
})();
