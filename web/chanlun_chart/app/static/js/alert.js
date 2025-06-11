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
  };
})();
