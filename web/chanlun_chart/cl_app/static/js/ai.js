var AI = (function () {
  var tableInstanceId = "table_ai_analysis"; // 表格实例 ID（与配置中的 id 保持一致）
  var isTableRendered = false; // 标记表格是否已渲染

  return {
    get_ai_analyse_records: function () {
      function stripMarkdownCodeBlock(md) {
        // 匹配 ```markdown ... ``` 或 ``` ... ```，支持只有开头没有结尾的情况
        const startMatch = md.match(/^```(?:markdown)?\s*/i);
        const endMatch = md.match(/\s*```\s*$/);
        if (startMatch && endMatch) {
          // 有开头和结尾
          return md
            .replace(/^```(?:markdown)?\s*/i, "")
            .replace(/\s*```\s*$/, "");
        } else if (startMatch) {
          // 只有开头
          return md.replace(/^```(?:markdown)?\s*/i, "");
        }
        return md;
      }
      layui.use(["table"], function () {
        let table = layui.table;
        var element = layui.element;

        // 如果表格已渲染，则重载数据；否则创建新实例
        if (isTableRendered) {
          table.reload(tableInstanceId, {
            url: "/ai/analyse_records/" + Utils.get_market(),
            page: {
              curr: 1, // 重新从第 1 页开始
            },
          });
          return;
        }

        // 创建AI分析列表渲染实例
        table.render({
          elem: "#table_ai_analysis",
          id: tableInstanceId, // 设置表格实例 ID
          defaultContextmenu: false,
          url: "/ai/analyse_records/" + Utils.get_market(),
          page: true, // 开启分页
          limit: 10, // 每页显示数量
          limits: [10, 20, 30, 50, 100], // 每页条数的选择项
          className: "layui-font-12",
          size: "sm",
          maxHeight: 750,
          cols: [
            [
              { field: "stock_name", title: "名称", sort: false, width: 100 },
              { field: "stock_code", title: "代码", sort: false, width: 80 },
              { field: "frequency", title: "周期", sort: false, width: 60 },
              { field: "dt", title: "时间", sort: false, width: 160 },
            ],
          ],
        });
        isTableRendered = true; // 标记表格已渲染
        // 点击AI分析结果，弹框展示内容
        table.on("row(table_ai_analysis)", function (obj) {
          let data = obj.data; // 获取当前行数据
          var title =
            "AI分析 " +
            data.stock_code +
            " " +
            data.stock_name +
            " " +
            data.frequency +
            data.dt +
            " 模型 " +
            data.model;
          var show_html =
            '<div class="layui-collapse ai-analyse-div" lay-filter="collapse-ais"><div class="layui-colla-item"><div class="layui-colla-title">缠论状态提示词</div><div class="layui-colla-content">' +
            marked.parse(stripMarkdownCodeBlock(data.prompt)) +
            "</div></div>" +
            '<div class="layui-colla-item"><div class="layui-colla-title">AI分析结果</div><div class="layui-colla-content layui-show">' +
            marked.parse(stripMarkdownCodeBlock(data.msg)) +
            "</div></div></div>";
          layer.open({
            type: 1,
            title: title,
            content: show_html,
            area: ["720px", "650px"],
            // maxHeight: 950,
            anim: "slideLeft",
            shade: 0,
          });
          element.render("collapse", "collapse-ais");

          // 切换到该股票
          change_chart_ticker(Utils.get_market(), data.stock_code);
          $("#ai_code").val(data.stock_code);
        });
      });
    },
    init_ai_opts: function () {
      let ai_frequencys = $("#ai_frequencys");
      $(ai_frequencys).html();
      layui.each(market_frequencys[Utils.get_market()], function (i, f) {
        $(ai_frequencys).append("<option value='" + f + "'>" + f + "</option>");
      });
      layui.form.render($(ai_frequencys));
      $(ai_frequencys)
        .siblings("div.layui-form-select")
        .find("dl")
        .find("dd[lay-value=d]")
        .click();

      $("#ai_code").val(Utils.get_code());
      $("#ai_analyse_btn").click(function () {
        // 将 btn 置灰
        $("#ai_analyse_btn")
          .addClass("layui-btn-disabled")
          .attr("disabled", true);
        $("#ai_analyse_btn").html("分析中...");
        $.ajax({
          type: "POST",
          url: "/ai/analyse",
          data: {
            market: Utils.get_market(),
            code: $("#ai_code").val(),
            frequency: $("#ai_frequencys").val(),
          },
          dataType: "json",
          success: function (res) {
            if (res["ok"] === true) {
              layer.msg("分析成功");
              // 重载表格数据
              AI.get_ai_analyse_records();
            } else {
              layer.msg(res["msg"]);
            }
            $("#ai_analyse_btn")
              .removeClass("layui-btn-disabled")
              .attr("disabled", false);
            $("#ai_analyse_btn").html("分析");
          },
          error: function (res) {
            layer.msg("分析失败，查看控制台，查找错误问题");
            $("#ai_analyse_btn")
              .removeClass("layui-btn-disabled")
              .attr("disabled", false);
            $("#ai_analyse_btn").html("分析");
          },
        });
      });
    },
  };
})();
