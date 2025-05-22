var Utils = (function () {
  return {
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
        JSON.parse(localStorage.getItem(Utils.get_market() + "_selectedItems")) || [];

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
  };
})();
