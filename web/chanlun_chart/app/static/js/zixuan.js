var ZiXuan = (function () {
  return {
    render_zixuan_opts: function () {
      $.ajax({
        type: "GET",
        url:
          "/get_stock_zixuan/" +
          Utils.get_market() +
          "/" +
          Utils.get_code().replace("/", "__"),
        dataType: "json",
        success: function (res) {
          let data = [];
          layui.each(res, function (i, e) {
            if (e["exists"] === 0) {
              templet =
                '<span><input type="checkbox" /> ' + e["zx_name"] + "</span>";
            } else {
              templet =
                '<span><input type="checkbox" checked /> ' +
                e["zx_name"] +
                "</span>";
            }
            data.push({
              title: e["zx_name"],
              id: i,
              templet: templet,
              exists: e["exists"],
              code: e["code"],
            });
          });
          // 再重新请求一遍自选列表，刷新
          $("#zixuan_groups").change();
          layui.dropdown.reloadData("add_zixuan", {
            data: data,
          });
        },
      });
    },
    stocks_update_rate: function () {
      // 更新展示的股票列表涨跌幅
      let codes = [];
      $(".code_rate").each(function () {
        codes.push($(this).data("code"));
      });
      if (codes.length === 0) {
        return true;
      }
      $.ajax({
        type: "POST",
        url: "/ticks",
        data: { market: Utils.get_market(), codes: JSON.stringify(codes) },
        dataType: "json",
        success: function (ticks) {
          for (let i = 0; i < ticks["ticks"].length; i++) {
            let tick = ticks["ticks"][i];
            let color = tick["rate"] > 0 ? "#ff5722" : "#16baaa";
            let obj_span_rate = $(
              '.code_rate[data-code="' + tick["code"] + '"]'
            );
            obj_span_rate.html(tick["rate"] + "%");
            obj_span_rate.css("color", color);
          }
          let now_trading = ticks["now_trading"];
          if (now_trading !== true) {
            clearInterval(interval_update_rates);
          }
        },
      });
    },
    render_zixuan_stocks: function () {
      // 自选列表渲染与操作
      layui.use(["table", "dropdown", "util"], function () {
        let table = layui.table;
        let dropdown = layui.dropdown;
        // 创建自选列表渲染实例
        table.render({
          elem: "#table_zixuan_list",
          defaultContextmenu: false,
          url: "/get_zixuan_stocks/" + Utils.get_market() + "/" + zx_group,
          page: false,
          className: "layui-font-12",
          size: "sm",
          cols: [
            [
              {
                field: "code",
                title: "代码",
                sort: false,
                templet: function (d) {
                  if (d.color !== undefined && d.color !== "") {
                    return (
                      '<span style="color: ' +
                      d.color +
                      '">' +
                      d.code +
                      "</span>"
                    );
                  }
                  return d.code;
                },
              },
              {
                field: "name",
                title: "名称",
                sort: false,
                templet: function (d) {
                  if (d.color !== undefined && d.color !== "") {
                    return (
                      '<span style="color: ' +
                      d.color +
                      '">' +
                      d.name +
                      "</span>"
                    );
                  }
                  return d.name;
                },
              },
              {
                field: "zf",
                title: "涨跌幅",
                sort: false,
                width: 70,
                templet: function (d) {
                  return (
                    '<span class="code_rate" data-code="' +
                    d.code +
                    '">--</span>'
                  );
                },
              },
            ],
          ],
          done: function () {
            ZiXuan.stocks_update_rate();
          },
        });
        // 行单击事件( 双击事件为: rowDouble )
        table.on("row(table_zixuan_list)", function (obj) {
          const data = obj.data; // 获取当前行数据
          const code = data.code;
          change_chart_ticker(Utils.get_market(), code);
          $("#ai_code").val(code);
          table.setRowChecked("table_zixuan_list", {
            index: "all", // 所有行
            checked: false,
          });
          table.setRowChecked("table_zixuan_list", {
            index: obj.index, // 选中行的下标。 0 表示第一行
          });
        });
        // 右键菜单
        table.on("rowContextmenu(table_zixuan_list)", function (obj) {
          let data = obj.data; // 获取当前行数据
          // 右键操作
          let menu_data = [
            { title: "删除", id: "del" },
            { title: "置顶", id: "sort_1", direction: "top" },
            { title: "置底", id: "sort_2", direction: "bottom" },
            {
              title: "色彩",
              id: "color_1",
              color: "#ff5722",
              templet: function () {
                return '<div class="layui-bg-red">红色</div>';
              },
            },
            {
              title: "色彩",
              id: "color_2",
              color: "#ffb800",
              templet: function () {
                return '<div class="layui-bg-orange">橙色</div>';
              },
            },
            {
              title: "色彩",
              id: "color_3",
              color: "#16baaa",
              templet: function () {
                return '<div class="layui-bg-green">绿色</div>';
              },
            },
            {
              title: "色彩",
              id: "color_4",
              color: "#1e9fff",
              templet: function () {
                return '<div class="layui-bg-blue">蓝色</div>';
              },
            },
            {
              title: "色彩",
              id: "color_5",
              color: "#a233c6",
              templet: function () {
                return '<div class="layui-bg-purple">紫色</div>';
              },
            },
            {
              title: "色彩",
              id: "color_6",
              color: "",
              templet: function () {
                return '<div class="layui-bg-gray">清除颜色</div>';
              },
            },
          ];
          if (Utils.get_market() === "a") {
            menu_data.splice(3, 0, { title: "操盘必读", id: "dfcf" });
          }
          dropdown.render({
            trigger: "contextmenu",
            show: true,
            data: menu_data,
            click: function (menuData, othis) {
              if (menuData["id"] === "del") {
                $.ajax({
                  type: "POST",
                  url: "/set_stock_zixuan",
                  data: {
                    opt: "DEL",
                    market: Utils.get_market(),
                    group_name: zx_group,
                    code: data.code,
                    color: "",
                    direction: "",
                  },
                  dataType: "json",
                  success: function (res) {
                    if (res["ok"]) {
                      layer.msg("删除成功");
                      obj.del();
                    } else {
                      layer.msg("删除失败");
                    }
                  },
                });
              } else if (menuData["title"] === "色彩") {
                $.ajax({
                  type: "POST",
                  url: "/set_stock_zixuan",
                  data: {
                    opt: "COLOR",
                    market: Utils.get_market(),
                    group_name: zx_group,
                    code: data.code,
                    color: menuData["color"],
                    direction: "",
                  },
                  dataType: "json",
                  success: function (res) {
                    obj.update(
                      {
                        color: menuData["color"],
                      },
                      true
                    );
                  },
                });
              } else if (
                menuData["id"] === "sort_1" ||
                menuData["id"] === "sort_2"
              ) {
                $.ajax({
                  type: "POST",
                  url: "/set_stock_zixuan",
                  data: {
                    opt: "SORT",
                    market: Utils.get_market(),
                    group_name: zx_group,
                    code: data.code,
                    color: "",
                    direction: menuData["direction"],
                  },
                  dataType: "json",
                  success: function (res) {
                    // 再重新请求一遍自选列表，刷新
                    ZiXuan.render_zixuan_stocks();
                  },
                });
              } else if (menuData["id"] === "dfcf") {
                window.open(
                  "https://emweb.securities.eastmoney.com/pc_hsf10/pages/index.html?type=web&code=" +
                    data.code.replace(".", "")
                );
              }
            },
          });
        });
      });
    },
  };
})();
