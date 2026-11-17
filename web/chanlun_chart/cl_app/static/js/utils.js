var Utils = (function () {
  var THS_BAR_VISIBLE_KEY = "ths_bar_visible";

  // 同花顺底部条默认显示
  function ths_bar_visible() {
    var value = Utils.get_local_data(THS_BAR_VISIBLE_KEY);
    return value === undefined ? true : !!value;
  }

  function apply_ths_bar_visible() {
    $("#ths-floating-bar").toggle(ths_bar_visible());
  }

  function sync_ths_bar_button() {
    var $btn = $('.layui-fixbar li[lay-type="ths_bar"]');
    if (!$btn.length) {
      return;
    }
    var visible = ths_bar_visible();
    $btn
      .toggleClass("layui-icon-eye", visible)
      .toggleClass("layui-icon-eye-invisible", !visible)
      .attr("title", visible ? "隐藏同花顺底部条" : "显示同花顺底部条");
  }

  function toggle_ths_bar() {
    Utils.set_local_data(THS_BAR_VISIBLE_KEY, !ths_bar_visible());
    apply_ths_bar_visible();
    sync_ths_bar_button();
  }

  return {
    apply_ths_bar_visible: apply_ths_bar_visible,
    get_local_data: function (key) {
      if (layui.data("tv_chart")) {
        let val = layui.data("tv_chart")[key];
        if (val === undefined) {
          return default_vals[key];
        }
        return val;
      }
      return default_vals[key];
    },
    set_local_data: function (key, val) {
      layui.data("tv_chart", {
        key: key,
        value: val,
      });
    },
    add_to_cache: function (data) {
      // 获取之前的列表
      let selectedItems =
        JSON.parse(
          localStorage.getItem(Utils.get_market() + "_selectedItems")
        ) || [];

      // 将当前选择的项目添加到列表的最前面
      selectedItems.unshift({
        name: data.arr[0].name,
        value: data.arr[0].value,
      });

      // 只保留最近的30个
      selectedItems = selectedItems.slice(0, 30);

      // 在最后放到缓存之前去重，保留最近的项
      const uniqueItems = [];
      const seenValues = new Set();

      for (const item of selectedItems) {
        if (!seenValues.has(item.value)) {
          seenValues.add(item.value);
          uniqueItems.push(item);
        }
      }

      // 更新 localStorage
      localStorage.setItem(
        Utils.get_market() + "_selectedItems",
        JSON.stringify(uniqueItems)
      );
    },
    get_market: function () {
      return Utils.get_local_data("market");
    },
    get_code: function () {
      return Utils.get_local_data(Utils.get_market() + "_code");
    },
    render_fixbar: function () {
      // 渲染底部工具栏
      // 固定条，显示与隐藏菜单栏
      layui.use(function () {
        var util = layui.util;
        var bars = [
          {
            // 定义可显示的 bar 列表信息 -- v2.8.0 新增
            type: "hide_menu",
            icon: "layui-icon-spread-left", // layui-icon-shrink-right
          },
        ];
        // 同花顺底部条仅在 A 股显示
        if (Utils.get_market() === "a") {
          bars.push({ type: "ths_bar", icon: "layui-icon-eye" });
        }
        util.fixbar({
          bars: bars,
          default: false,
          on: {
            // 任意事件 --  v2.8.0 新增
          },
          // 点击事件
          click: function (type) {
            if (type === "hide_menu") {
              var fixed_li = $('.layui-fixbar li[lay-type="hide_menu"]');
              if (fixed_li.attr("class").includes("layui-icon-spread-left")) {
                fixed_li
                  .removeClass("layui-icon-spread-left")
                  .addClass("layui-icon-shrink-right");
                $("#chart_menu").hide();
                $("#chart_container")
                  .removeClass("layui-col-xs10 layui-col-sm10 layui-col-md10")
                  .addClass("layui-col-xs12 layui-col-sm12 layui-col-md12");
              } else {
                fixed_li
                  .removeClass("layui-icon-shrink-right")
                  .addClass("layui-icon-spread-left");
                $("#chart_menu").show();
                $("#chart_container")
                  .removeClass("layui-col-xs12 layui-col-sm12 layui-col-md12")
                  .addClass("layui-col-xs10 layui-col-sm10 layui-col-md10");
              }
            } else if (type === "ths_bar") {
              toggle_ths_bar();
            }
          },
        });
        sync_ths_bar_button();
      });
    },
  };
})();
