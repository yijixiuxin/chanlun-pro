var Alert = (function () {
  return {
    get_alert_records: function () {
      layui.use(["table"], function () {
        let table = layui.table;

        table.render({
          elem: "#table_alert_reocrds",
          defaultContextmenu: false,
          url: "/alert_records/" + Utils.get_market(),
          page: false,
          className: "layui-font-12",
          size: "sm",
          maxHeight: 550,
          cols: [
            [
              { field: "jh_type", title: "触发", sort: false, width: 230 },
              {
                field: "datetime_str",
                title: "时间",
                sort: false,
                width: 160,
              },
              { field: "code", title: "代码", sort: false, width: 80 },
              { field: "name", title: "名称", sort: false, width: 80 },
              { field: "frequency", title: "周期", sort: false, width: 60 },
              {
                field: "task_name",
                title: "监控名称",
                sort: false,
                width: 80,
                templet: function (d) {
                  return d.task_name;
                },
              },
              { field: "is_done", title: "是否完成", sort: false, width: 80 },
              { field: "is_td", title: "是否停顿", sort: false, width: 80 },
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
  };
})();
