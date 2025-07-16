var Alert = (function () {
  return {
    init: function () {
      layui.use(["table", "form"], function () {
        let form = layui.form;

        // 获取提醒任务列表并填充到select中
        $.get("/alert_list/" + Utils.get_market(), function (res) {
          if (res.code == 0) {
            let task_name_select = $("#task_name_select");
            task_name_select.empty();
            task_name_select.append("<option value=''>全部</option>");
            $.each(res.data, function (index, item) {
              task_name_select.append(
                `<option value='${item.task_name}'>${item.task_name}</option>`
              );
            });
            form.render("select");
          }
        });

        // 监听select选择器，选择后刷新列表
        form.on("select(task_name_select)", function (data) {
          Alert.get_alert_records();
        });
      });
    },

    get_alert_records: function () {
      layui.use(["table", "form"], function () {
        let table = layui.table;

        table.render({
          elem: "#table_alert_reocrds",
          defaultContextmenu: false,
          url:
            "/alert_records/" +
            Utils.get_market() +
            "?task_name=" +
            $("#task_name_select").val(),
          page: false,
          className: "layui-font-12",
          size: "sm",
          maxHeight: 550,
          lineStyle: "height: auto;",
          cols: [
            [
              {
                field: "custom",
                title: "",
                templet: function (d) {
                  return `
                    <div class="alert-record-row">
                      <div style="font-weight: bold; font-size: 14px;">
                        ${d.name || ""} <span style="color: #888;">${
                    d.code || ""
                  }</span> <span style="color: #16baaa;">${
                    d.frequency || ""
                  }</span> <span style="color: #b37feb;">${
                    d.line_type || ""
                  }</span>
                      </div>
                      <div style="font-size: 16px;">${d.msg || ""}</div>
                      <div style="color: #888; font-size: 12px;">
                        ${
                          d.datetime_str || ""
                        } <span style="margin-left: 10px; color:rgb(203, 243, 183);">${
                    d.task_name || ""
                  }</span>
                      </div>
                    </div>
                  `;
                },
              },
            ],
          ],
        });
        // 单击警报内容列表
        table.on("row(table_alert_reocrds)", function (obj) {
          let data = obj.data; // 获取当前行数据
          change_chart_ticker(Utils.get_market(), data.code);
        });
      });
    },

    refresh_alerts_table: function () {
      layui.use(["table", "dropdown", "util"], function () {
        let table = layui.table;
        let dropdown = layui.dropdown;
        table.render({
          elem: "#table_alerts",
          defaultContextmenu: false,
          url: "/alert_list/" + Utils.get_market(),
          page: false,
          className: "layui-font-12",
          size: "sm",
          cols: [
            [
              { field: "task_name", title: "监控名称" },
              {
                field: "zx_group",
                title: "自选组",
                templet: function (d) {
                  return d.zx_group;
                },
              },
              {
                filed: "frequency",
                title: "周期",
                templet: function (d) {
                  return d.frequency;
                },
              },
              {
                filed: "interval_minutes",
                title: "运行间隔(分钟)",
                sort: true,
                templet: function (d) {
                  return d.interval_minutes;
                },
              },
              {
                filed: "check_bi_type",
                title: "笔方向",
                templet: function (d) {
                  return d.check_bi_type;
                },
              },
              {
                filed: "check_bi_beichi",
                title: "笔背驰",
                templet: function (d) {
                  return d.check_bi_beichi;
                },
              },
              {
                filed: "check_bi_mmd",
                title: "笔买卖点",
                templet: function (d) {
                  return d.check_bi_mmd;
                },
              },
              {
                filed: "check_xd_type",
                title: "线段方向",
                templet: function (d) {
                  return d.check_xd_type;
                },
              },
              {
                filed: "check_xd_beichi",
                title: "线段背驰",
                templet: function (d) {
                  return d.check_xd_beichi;
                },
              },
              {
                filed: "check_xd_mmd",
                title: "线段买卖点",
                templet: function (d) {
                  return d.check_xd_mmd;
                },
              },
              {
                filed: "is_send_msg",
                title: "发送消息",
                sort: true,
                templet: function (d) {
                  if (d.is_send_msg === 1) {
                    return "发送";
                  } else {
                    return "不发";
                  }
                },
              },
              {
                filed: "is_run",
                title: "启用",
                sort: true,
                templet: function (d) {
                  if (d.is_run === 1) {
                    return "启用";
                  } else {
                    return "禁用";
                  }
                },
              },
            ],
          ],
        });
        // 行双击事件( 双击事件为: rowDouble )
        table.on("row(table_alerts)", function (obj) {
          let data = obj.data; // 获取当前行数据
          layer.open({
            type: 2,
            title: "修改警报提醒",
            area: ["1000px", "90vh"],
            content: "/alert_edit/" + Utils.get_market() + "/" + data.id,
            anim: 1,
            fixed: true, // 不固定
            shadeClose: true,
          });
        });
        // 右键菜单
        table.on("rowContextmenu(table_alerts)", function (obj) {
          let data = obj.data; // 获取当前行数据
          // 右键操作
          dropdown.render({
            trigger: "contextmenu",
            show: true,
            data: [{ title: "删除", id: "del" }],
            click: function (menuData, othis) {
              if (menuData["id"] === "del") {
                $.ajax({
                  type: "GET",
                  url: "/alert_del/" + data.id,
                  dataType: "json",
                  success: function (res) {
                    if (res["ok"]) {
                      layer.msg("删除成功");
                    } else {
                      layer.msg("删除失败");
                    }
                    Alert.refresh_alerts_table();
                  },
                });
              }
            },
          });
        });
      });
    },
  };
})();
