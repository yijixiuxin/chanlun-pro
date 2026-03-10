var ZiXuan = (function () {
  var zx_group = "我的关注";
  // 定义一个模块级变量来存储定时器ID
  var interval_update_rates = null;

  // 停止定时器的辅助函数
  function stop_timer() {
    if (interval_update_rates) {
      clearInterval(interval_update_rates);
      interval_update_rates = null;
    }
  }

  return {
    render_zixuan_opts: function () {
      $.ajax({
        type: "GET",
        url: "/get_stock_zixuan/" + Utils.get_market() + "/" + Utils.get_code().replace("/", "__"),
        dataType: "json",
        success: function (res) {
          let data = [];
          layui.each(res, function (i, e) {
            // 修复：添加 let 防止全局变量污染
            let templet = "";
            if (e["exists"] === 0) {
              templet = '<span><input type="checkbox" /> ' + e["zx_name"] + "</span>";
            } else {
              templet = '<span><input type="checkbox" checked /> ' + e["zx_name"] + "</span>";
            }
            data.push({
              title: e["zx_name"],
              id: i,
              templet: templet,
              exists: e["exists"],
              code: e["code"],
            });
          });

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
      // 仅获取当前DOM中存在的代码，避免请求无用数据
      $(".code_rate").each(function () {
        codes.push($(this).data("code"));
      });

      if (codes.length === 0) {
        return true;
      }

      layui.use(["laytpl"], function () {
        var laytpl = layui.laytpl;
        // 预编译模版，提高循环渲染性能
        var rate_show_tpl =
          "<div style='color:{{= d.color }}' class='code_rate' data-code='{{= d.code }}'>" +
          "<div style='color:{{= d.color }}' class='layui-font-14'>{{= d.rate }}%</div>" +
          "<div class='layui-font-12'>{{= d.price }}</div>" +
          "<div>";

        $.ajax({
          type: "POST",
          url: "/ticks",
          data: { market: Utils.get_market(), codes: JSON.stringify(codes) },
          dataType: "json",
          success: function (ticks) {
            // 遍历更新 DOM
            for (let i = 0; i < ticks["ticks"].length; i++) {
              let tick = ticks["ticks"][i];
              let color = "#1e9fff"; // 默认平盘色（或灰色）
              if (tick["rate"] > 0) color = "#ff5722"; // 涨
              else if (tick["rate"] < 0) color = "#16baaa"; // 跌

              // 找到对应的 DOM 元素
              let obj_span_rate = $('.code_rate[data-code="' + tick["code"] + '"]');

              // 使用 laytpl 渲染
              laytpl(rate_show_tpl).render({
                code: tick["code"],
                price: tick["price"],
                rate: tick["rate"],
                color: color,
              }, function(html){
                  obj_span_rate.html(html); // 替换内容
              });
            }

            // 如果后端返回当前非交易时间，停止轮询以节省资源
            let now_trading = ticks["now_trading"];
            if (now_trading !== true) {
              console.log("非交易时间，停止自动刷新");
              stop_timer();
            }
          },
        });
      });
    },

    render_zixuan_stocks: function () {
      // 每次重新渲染表格前，先停止旧的定时器，防止重复
      stop_timer();

      layui.use(["table", "dropdown", "util", "laytpl"], function () {
        var laytpl = layui.laytpl;
        let table = layui.table;
        let dropdown = layui.dropdown;

        var code_show_tpl = laytpl(
          "<div style='color:{{= d.color }}' class='layui-font-14'>{{= d.name }}</div><div class='layui-font-12 layui-font-gray'>{{= d.code }}</div>"
        );
        // 初始渲染的占位模版
        var rate_show_tpl = laytpl(
          "<div class='code_rate' data-code='{{= d.code }}'><div class='layui-font-14'>- %</div><div class='layui-font-12'>-</div><div>"
        );

        table.render({
          elem: "#table_zixuan_list",
          defaultContextmenu: false,
          url: "/get_zixuan_stocks/" + Utils.get_market() + "/" + ZiXuan.zx_group,
          page: false,
          className: "layui-font-12",
          size: "sm",
          lineStyle: "height: 52px;",
          loading: true,
          cols: [
            [
              {
                field: "code",
                title: "标的",
                sort: false,
                templet: function (d) {
                  return code_show_tpl.render({
                    color: d.color,
                    name: d.name,
                    code: d.code,
                  });
                },
              },
              {
                field: "zf",
                title: "涨跌幅",
                sort: false,
                width: 70,
                templet: function (d) {
                  return rate_show_tpl.render({
                    code: d.code,
                  });
                },
              },
            ],
          ],
          done: function () {
            // 1. 表格加载完成后，立即执行一次更新
            ZiXuan.stocks_update_rate();

            // 2. 启动定时器，每 3000 毫秒（3秒）刷新一次
            // 注意：这里赋值给模块顶部的 interval_update_rates 变量
            interval_update_rates = setInterval(function() {
                ZiXuan.stocks_update_rate();
            }, 3000);
          },
        });

        // 行单击事件
        table.on("row(table_zixuan_list)", function (obj) {
          const data = obj.data;
          const code = data.code;
          change_chart_ticker(Utils.get_market(), code);
          $("#ai_code").val(code);
          // 清除其他行选中样式
          table.setRowChecked("table_zixuan_list", {
            index: "all",
            checked: false,
          });
          // 选中当前行
          table.setRowChecked("table_zixuan_list", {
            index: obj.index,
          });
        });

        // 右键菜单 (保持原有逻辑不变)
        table.on("rowContextmenu(table_zixuan_list)", function (obj) {
          let data = obj.data;
          let menu_data = [
            { title: "删除", id: "del" },
            { title: "置顶", id: "sort_1", direction: "top" },
            { title: "置底", id: "sort_2", direction: "bottom" },
            {
                title: "色彩",
                id: "color_1",
                color: "#ff5722",
                templet: function () { return '<div class="layui-bg-red">红色</div>'; },
            },
            {
                title: "色彩",
                id: "color_2",
                color: "#ffb800",
                templet: function () { return '<div class="layui-bg-orange">橙色</div>'; },
            },
            {
                title: "色彩",
                id: "color_3",
                color: "#16baaa",
                templet: function () { return '<div class="layui-bg-green">绿色</div>'; },
            },
            {
                title: "色彩",
                id: "color_4",
                color: "#1e9fff",
                templet: function () { return '<div class="layui-bg-blue">蓝色</div>'; },
            },
            {
                title: "色彩",
                id: "color_5",
                color: "#a233c6",
                templet: function () { return '<div class="layui-bg-purple">紫色</div>'; },
            },
            {
                title: "色彩",
                id: "color_6",
                color: "",
                templet: function () { return '<div class="layui-bg-gray">清除颜色</div>'; },
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
                // 删除逻辑
                $.ajax({
                  type: "POST",
                  url: "/set_stock_zixuan",
                  data: {
                    opt: "DEL",
                    market: Utils.get_market(),
                    group_name: ZiXuan.zx_group,
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
                 // 颜色设置逻辑
                 $.ajax({
                    type: "POST",
                    url: "/set_stock_zixuan",
                    data: {
                      opt: "COLOR",
                      market: Utils.get_market(),
                      group_name: ZiXuan.zx_group,
                      code: data.code,
                      color: menuData["color"],
                      direction: "",
                    },
                    dataType: "json",
                    success: function (res) {
                      obj.update({ color: menuData["color"] }, true);
                    },
                  });
              } else if (
                menuData["id"] === "sort_1" ||
                menuData["id"] === "sort_2"
              ) {
                // 排序逻辑
                $.ajax({
                    type: "POST",
                    url: "/set_stock_zixuan",
                    data: {
                      opt: "SORT",
                      market: Utils.get_market(),
                      group_name: ZiXuan.zx_group,
                      code: data.code,
                      color: "",
                      direction: menuData["direction"],
                    },
                    dataType: "json",
                    success: function (res) {
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

    // ... init_zixuan_opts 保持不变 ...
    init_zixuan_opts: function () {
        // (这部分代码没有逻辑错误，为了节省篇幅，这里可以直接使用你原有的代码，或者复制过来)
        // 建议：确保里面的 Utils.get_market() 都能正常获取
        layui.use(function () {
           var layer = layui.layer;
           var dropdown = layui.dropdown;
           var form = layui.form;

           // 获取自选组
           $.ajax({
             type: "GET",
             url: "/get_zixuan_groups/" + Utils.get_market(),
             dataType: "json",
             success: function (res) {
               let zixuan_groups = $("#zixuan_groups");
               $(zixuan_groups).html();
               layui.each(res, function (i, r) {
                 $(zixuan_groups).append(
                   "<option value='" + r.name + "'>" + r.name + "</option>"
                 );
               });
               layui.form.render($(zixuan_groups));
               // 触发第一个点击
               $(zixuan_groups).siblings("div.layui-form-select").find("dl").find("dd")[0].click();
             },
           });

           // 后续的 dropdown.render, form.on, xmSelect.render 等逻辑保持原样即可
           // ...

            dropdown.render({
                elem: "#add_zixuan",
                data: [],
                click: function (data, othis) {
                    let opt = "ADD";
                    if (data["exists"] === 1) {
                        opt = "DEL";
                    }
                    $.ajax({
                        type: "POST",
                        url: "/set_stock_zixuan",
                        data: {
                            opt: opt,
                            market: Utils.get_market(),
                            group_name: data["title"],
                            code: data["code"],
                            color: "",
                            direction: "",
                        },
                        dataType: "json",
                        success: function (res) {
                            if (data["title"] == ZiXuan.zx_group) {
                                ZiXuan.render_zixuan_opts();
                                ZiXuan.render_zixuan_stocks();
                            }
                        },
                    });
                    return false;
                },
            });

            form.on("select(select_zx_group)", function (data) {
                ZiXuan.zx_group = data.value;
                ZiXuan.render_zixuan_stocks();
            });

            $("#refresh_zixuan").click(function () {
                ZiXuan.render_zixuan_stocks();
            });

             // 代码搜索逻辑(保持不变)...
             const searchSelect = xmSelect.render({
                el: "#code_search",
                filterable: true,
                remoteSearch: true,
                radio: true,
                clickClose: true,
                tips: "商品代码搜索",
                empty: "没有搜索商品",
                theme: { color: "#e54d42" },
                delay: 1000,
                remoteMethod: function (val, cb, show) {
                    if (val) {
                        $.ajax({
                            type: "GET",
                            url: "/tv/search?limit=30&type=&query=" + val + "&exchange=" + Utils.get_market(),
                            dataType: "json",
                            success: function (res) {
                                let lst = [];
                                layui.each(res, function (i, r) {
                                    lst.push({
                                        name: r["symbol"] + ":" + r["description"],
                                        value: r["symbol"],
                                    });
                                });
                                cb(lst);
                            },
                        });
                    } else {
                        let storedItems = JSON.parse(localStorage.getItem(Utils.get_market() + "_selectedItems")) || [];
                        cb(storedItems);
                    }
                },
                show: function () {
                    let storedItems = JSON.parse(localStorage.getItem(Utils.get_market() + "_selectedItems")) || [];
                    searchSelect.update({ data: storedItems });
                },
                on: function (data) {
                    if (data.arr.length > 0) {
                        change_chart_ticker(Utils.get_market(), data.arr[0]["value"]);
                        Utils.add_to_cache(data);
                    }
                },
                data: [],
            });
        });
    }
  };
})();