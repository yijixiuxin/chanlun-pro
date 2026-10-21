/**
 * BacktestLocalSaveLoadAdapter —— 回测学习图表的本地存储 SaveLoad 适配器。
 *
 * 背景：回测学习页面有两张固定标的的图表（日线 small / 周线 high），仪表盘布局只在
 * 浏览器本地保存，**不调用 /tv/storage 服务端接口**（与正式图表页的
 * `TvStorage.TradingViewSaveLoadAdapter` 区分开）。
 *
 * 与官方 LocalStorageSaveLoadAdapter 示例的差异（按需求）：
 * - 不保存用户画的图（LineTool 绘图）：布局 content 落盘前会被 `stripLineToolsFromContent`
 *   过滤掉 panes[].sources 中 `type` 以 `LineTool` 开头的条目，并清空 `lineToolsGroups`；
 * - 不实现 `saveLineToolsAndGroups` / `loadLineToolsAndGroups`（本版本库未启用
 *   `saveload_separate_drawings_storage`，不会调用这两个方法）；
 * - 保留图表布局、指标设置、指标模板、画图模板、图表模板等“设置类”数据。
 *
 * 本文件是**经典脚本**（无构建、无 import/export），IIFE 挂到
 * `window.BacktestSaveLoad`（Node 单测时挂 `globalThis.BacktestSaveLoad`）。
 * 脚本引入顺序：在 `backtest.js` 之前用 <script> 引入即可。
 *
 * 导出符号（`BacktestSaveLoad.*`）：
 * - `BacktestLocalSaveLoadAdapter`                 适配器类（IExternalSaveLoadAdapter 子集）
 * - `createBacktestLocalSaveLoadAdapter(options)`  工厂：`{namespace?, storage?, onWarn?}`
 * - `stripLineToolsFromContent(content)`           纯函数：从布局 content 中移除用户绘图
 */
(function (root) {
  "use strict";

  var BacktestSaveLoad = root.BacktestSaveLoad || (root.BacktestSaveLoad = {});

  /** localStorage key 前缀；namespace 用于隔离 small / high 两张图表（两者周期固定不同）。 */
  var STORAGE_KEY_PREFIX = "chanlun_bt_save_load_";
  /** 默认布局名（库在保存对话框里通常已经让用户填名字，这里只做兜底）。 */
  var DEFAULT_CHART_NAME = "未命名布局";

  // ------------------------------------------------------------------ 工具函数

  function safeJsonStringify(value) {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return null;
    }
  }

  function deepClone(value) {
    if (typeof structuredClone === "function") {
      try {
        return structuredClone(value);
      } catch (error) {
        /* 继续尝试 JSON 克隆 */
      }
    }
    var json = safeJsonStringify(value);
    if (json === null) return value;
    try {
      return JSON.parse(json);
    } catch (error) {
      return value;
    }
  }

  function defaultWarn(message, error) {
    if (typeof console !== "undefined" && typeof console.warn === "function") {
      if (error === undefined) console.warn("[BacktestLocalSaveLoadAdapter] " + message);
      else console.warn("[BacktestLocalSaveLoadAdapter] " + message, error);
    }
  }

  function normalizeNamespace(namespace) {
    var value = namespace === undefined || namespace === null ? "" : String(namespace);
    value = value.replace(/[^a-zA-Z0-9_-]/g, "_");
    return value || "default";
  }

  function buildStorageKeys(namespace) {
    var base = STORAGE_KEY_PREFIX + namespace;
    return {
      charts: base + "_charts",
      studyTemplates: base + "_study_templates",
      drawingTemplates: base + "_drawing_templates",
      chartTemplates: base + "_chart_templates",
    };
  }

  function getDefaultStorage() {
    try {
      if (typeof window !== "undefined" && window.localStorage) {
        return window.localStorage;
      }
    } catch (error) {
      /* 隐私模式/禁用 localStorage 时退回内存存储 */
    }
    return null;
  }

  /** localStorage 不可用时的内存兜底，保证页面不因保存直接报错。 */
  function createMemoryStorage() {
    var data = Object.create(null);
    return {
      getItem: function (key) {
        return Object.prototype.hasOwnProperty.call(data, key) ? data[key] : null;
      },
      setItem: function (key, value) {
        data[key] = String(value);
      },
      removeItem: function (key) {
        delete data[key];
      },
    };
  }

  function generateUniqueChartId(charts) {
    var existing = Object.create(null);
    (charts || []).forEach(function (chart) {
      if (chart && chart.id !== undefined && chart.id !== null) {
        existing[String(chart.id)] = true;
      }
    });
    for (var attempt = 0; attempt < 100; attempt += 1) {
      var uid = Math.random().toString(16).slice(2);
      if (!existing[uid]) return uid;
    }
    return String(Date.now());
  }

  // -------------------------------------------------------------- 绘图过滤（纯函数）

  function isLineToolSource(source) {
    return !!(
      source &&
      typeof source.type === "string" &&
      source.type.indexOf("LineTool") === 0
    );
  }

  function stripChartLineTools(chart) {
    if (!chart || typeof chart !== "object") return;
    if (Array.isArray(chart.panes)) {
      chart.panes.forEach(function (pane) {
        if (!pane || !Array.isArray(pane.sources)) return;
        pane.sources = pane.sources.filter(function (source) {
          return !isLineToolSource(source);
        });
      });
    }
    // 绘图分组只描述 LineTool 的层级关系；绘图被移除后分组一并清空。
    if (Object.prototype.hasOwnProperty.call(chart, "lineToolsGroups")) {
      chart.lineToolsGroups = { groups: [] };
    }
  }

  /**
   * 递归处理库保存的 content 结构。
   *
   * 实测本版本库传给 `saveChart` 的 content 是**外层包装**：
   * `{"resolution":"1D","symbol_type":"stock",...,"content":"<layout JSON 字符串>"}`，
   * 真正的布局对象在 `content`（字符串/对象）里，结构为
   * `{"charts":[{"panes":[{"sources":[...]}]}]}`。这里同时兼容：
   * - 直接传入布局对象（含 `charts` 或 `panes`）；
   * - 外层包装的 `content` 字段（字符串或对象）；
   * - 低层 API 的 `{json: {...}}` 形态。
   */
  function stripLayoutPayload(payload) {
    if (!payload || typeof payload !== "object") return;

    if (Array.isArray(payload.charts)) {
      payload.charts.forEach(stripChartLineTools);
    } else if (Array.isArray(payload.panes)) {
      stripChartLineTools(payload);
    }

    if (Object.prototype.hasOwnProperty.call(payload, "content")) {
      if (typeof payload.content === "string") {
        try {
          var inner = JSON.parse(payload.content);
          if (inner && typeof inner === "object") {
            stripLayoutPayload(inner);
            payload.content = safeJsonStringify(inner) || payload.content;
          }
        } catch (error) {
          /* 内层不是合法 JSON 时保持原样 */
        }
      } else if (payload.content && typeof payload.content === "object") {
        stripLayoutPayload(payload.content);
      }
    }

    if (
      Object.prototype.hasOwnProperty.call(payload, "json") &&
      payload.json &&
      typeof payload.json === "object" &&
      payload.json !== payload
    ) {
      stripLayoutPayload(payload.json);
    }
  }

  /**
   * 从图表布局 content 中移除用户画的图（LineTool 绘图），只保留指标/图表设置。
   *
   * content 是库传进来的 `ChartData.content`（JSON 字符串，可能带外层包装，
   * 见 `stripLayoutPayload`）。解析失败时原样返回，避免因为个别字段异常导致整个保存流程失败。
   *
   * @param {string|Object} content
   * @returns {string}
   */
  function stripLineToolsFromContent(content) {
    var parsed = null;
    if (typeof content === "string") {
      try {
        parsed = JSON.parse(content);
      } catch (error) {
        return content;
      }
    } else if (content && typeof content === "object") {
      parsed = deepClone(content);
    } else {
      return content === undefined || content === null ? "" : String(content);
    }

    if (!parsed || typeof parsed !== "object") {
      return safeJsonStringify(parsed) || "";
    }

    stripLayoutPayload(parsed);

    return safeJsonStringify(parsed) || "";
  }

  // ------------------------------------------------------------------ 适配器

  class BacktestLocalSaveLoadAdapter {
    /**
     * @param {{namespace?: string, storage?: Storage, onWarn?: Function}} [options]
     *   namespace —— 图表作用域（如 "small"/"high"），不同 namespace 使用不同 localStorage key；
     *   storage   —— 可注入的 Storage 实现（Node 单测用内存实现）；缺省 window.localStorage；
     *   onWarn    —— 告警回调，缺省 console.warn。
     */
    constructor(options) {
      options = options || {};
      this._namespace = normalizeNamespace(options.namespace);
      this._keys = buildStorageKeys(this._namespace);
      this._storage =
        options.storage && typeof options.storage.getItem === "function"
          ? options.storage
          : getDefaultStorage() || createMemoryStorage();
      this._warnFn = typeof options.onWarn === "function" ? options.onWarn : defaultWarn;

      this._charts = this._readArray(this._keys.charts);
      this._studyTemplates = this._readArray(this._keys.studyTemplates);
      this._drawingTemplates = this._readArray(this._keys.drawingTemplates);
      this._chartTemplates = this._readArray(this._keys.chartTemplates);
    }

    // -------------------------------------------------------- 布局 charts

    async getAllCharts() {
      return this._charts.map(function (chart) {
        return {
          id: chart.id,
          name: chart.name === undefined || chart.name === null ? "" : String(chart.name),
          symbol: chart.symbol === undefined || chart.symbol === null ? "" : String(chart.symbol),
          resolution:
            chart.resolution === undefined || chart.resolution === null
              ? ""
              : String(chart.resolution),
          timestamp: typeof chart.timestamp === "number" ? chart.timestamp : 0,
        };
      });
    }

    async saveChart(chartData) {
      chartData = chartData || {};
      var id =
        chartData.id === undefined || chartData.id === null || chartData.id === ""
          ? generateUniqueChartId(this._charts)
          : String(chartData.id);

      var savedChart = {
        id: id,
        name:
          chartData.name === undefined || chartData.name === null || String(chartData.name) === ""
            ? DEFAULT_CHART_NAME
            : String(chartData.name),
        symbol: chartData.symbol === undefined || chartData.symbol === null ? "" : String(chartData.symbol),
        resolution:
          chartData.resolution === undefined || chartData.resolution === null
            ? ""
            : String(chartData.resolution),
        content: stripLineToolsFromContent(chartData.content),
        timestamp: Math.round(Date.now() / 1000),
      };

      var index = this._indexOfChart(id);
      if (index === -1) {
        this._charts.push(savedChart);
      } else {
        this._charts[index] = savedChart;
      }
      this._writeArray(this._keys.charts, this._charts);
      return id;
    }

    async getChartContent(id) {
      var index = this._indexOfChart(id);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 布局不存在: " + String(id));
      }
      return this._charts[index].content;
    }

    async removeChart(id) {
      var index = this._indexOfChart(id);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 布局不存在: " + String(id));
      }
      this._charts.splice(index, 1);
      this._writeArray(this._keys.charts, this._charts);
    }

    // -------------------------------------------------------- 指标模板 study templates

    async getAllStudyTemplates() {
      return this._studyTemplates.map(function (template) {
        return {
          name: template && template.name !== undefined && template.name !== null ? String(template.name) : "",
        };
      });
    }

    async saveStudyTemplate(studyTemplateData) {
      studyTemplateData = studyTemplateData || {};
      var name = String(studyTemplateData.name === undefined || studyTemplateData.name === null ? "" : studyTemplateData.name);
      var template = { name: name, content: studyTemplateData.content };
      var index = this._indexByName(this._studyTemplates, name);
      if (index === -1) this._studyTemplates.push(template);
      else this._studyTemplates[index] = template;
      this._writeArray(this._keys.studyTemplates, this._studyTemplates);
    }

    async getStudyTemplateContent(studyTemplateInfo) {
      var name = String(
        studyTemplateInfo && studyTemplateInfo.name !== undefined && studyTemplateInfo.name !== null
          ? studyTemplateInfo.name
          : ""
      );
      var index = this._indexByName(this._studyTemplates, name);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 指标模板不存在: " + name);
      }
      return this._studyTemplates[index].content;
    }

    async removeStudyTemplate(studyTemplateInfo) {
      var name = String(
        studyTemplateInfo && studyTemplateInfo.name !== undefined && studyTemplateInfo.name !== null
          ? studyTemplateInfo.name
          : ""
      );
      var index = this._indexByName(this._studyTemplates, name);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 指标模板不存在: " + name);
      }
      this._studyTemplates.splice(index, 1);
      this._writeArray(this._keys.studyTemplates, this._studyTemplates);
    }

    // -------------------------------------------------------- 画图模板 drawing templates

    async getDrawingTemplates(toolName) {
      var tool = String(toolName === undefined || toolName === null ? "" : toolName);
      var names = [];
      this._drawingTemplates.forEach(function (template) {
        if (template && String(template.toolName) === tool) {
          names.push(String(template.name));
        }
      });
      return names;
    }

    async loadDrawingTemplate(toolName, templateName) {
      var index = this._indexOfDrawingTemplate(toolName, templateName);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 画图模板不存在: " + String(templateName));
      }
      return this._drawingTemplates[index].content;
    }

    async saveDrawingTemplate(toolName, templateName, content) {
      var tool = String(toolName === undefined || toolName === null ? "" : toolName);
      var name = String(templateName === undefined || templateName === null ? "" : templateName);
      var template = { toolName: tool, name: name, content: content };
      var index = this._indexOfDrawingTemplate(tool, name);
      if (index === -1) this._drawingTemplates.push(template);
      else this._drawingTemplates[index] = template;
      this._writeArray(this._keys.drawingTemplates, this._drawingTemplates);
    }

    async removeDrawingTemplate(toolName, templateName) {
      var index = this._indexOfDrawingTemplate(toolName, templateName);
      if (index === -1) return;
      this._drawingTemplates.splice(index, 1);
      this._writeArray(this._keys.drawingTemplates, this._drawingTemplates);
    }

    // -------------------------------------------------------- 图表模板 chart templates

    async getAllChartTemplates() {
      return this._chartTemplates.map(function (template) {
        return String(template && template.name !== undefined && template.name !== null ? template.name : "");
      });
    }

    async saveChartTemplate(templateName, content) {
      var name = String(templateName === undefined || templateName === null ? "" : templateName);
      var template = { name: name, content: deepClone(content) };
      var index = this._indexByName(this._chartTemplates, name);
      if (index === -1) this._chartTemplates.push(template);
      else this._chartTemplates[index] = template;
      this._writeArray(this._keys.chartTemplates, this._chartTemplates);
    }

    async getChartTemplateContent(templateName) {
      var name = String(templateName === undefined || templateName === null ? "" : templateName);
      var index = this._indexByName(this._chartTemplates, name);
      if (index === -1) {
        throw new Error("[BacktestLocalSaveLoadAdapter] 图表模板不存在: " + name);
      }
      return { content: deepClone(this._chartTemplates[index].content) };
    }

    async removeChartTemplate(templateName) {
      var name = String(templateName === undefined || templateName === null ? "" : templateName);
      var index = this._indexByName(this._chartTemplates, name);
      if (index === -1) return;
      this._chartTemplates.splice(index, 1);
      this._writeArray(this._keys.chartTemplates, this._chartTemplates);
    }

    // -------------------------------------------------------- internals

    _indexOfChart(id) {
      var key = String(id);
      for (var i = 0; i < this._charts.length; i += 1) {
        if (String(this._charts[i].id) === key) return i;
      }
      return -1;
    }

    _indexByName(list, name) {
      var key = String(name);
      for (var i = 0; i < list.length; i += 1) {
        if (String(list[i].name) === key) return i;
      }
      return -1;
    }

    _indexOfDrawingTemplate(toolName, templateName) {
      var tool = String(toolName === undefined || toolName === null ? "" : toolName);
      var name = String(templateName === undefined || templateName === null ? "" : templateName);
      for (var i = 0; i < this._drawingTemplates.length; i += 1) {
        var template = this._drawingTemplates[i] || {};
        if (String(template.toolName) === tool && String(template.name) === name) return i;
      }
      return -1;
    }

    _readArray(key) {
      try {
        var raw = this._storage.getItem(key);
        if (!raw) return [];
        var parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
      } catch (error) {
        this._warn("读取本地存储失败，按空数据处理: " + key, error);
        return [];
      }
    }

    _writeArray(key, value) {
      try {
        this._storage.setItem(key, JSON.stringify(value));
      } catch (error) {
        this._warn("写入本地存储失败（可能超出 localStorage 容量）: " + key, error);
        throw error;
      }
    }

    _warn(message, error) {
      try {
        this._warnFn(message, error);
      } catch (warnError) {
        /* 忽略告警回调自身的异常 */
      }
    }
  }

  function createBacktestLocalSaveLoadAdapter(options) {
    return new BacktestLocalSaveLoadAdapter(options);
  }

  // 重复加载（测试脚本多次 eval）时只覆盖同名导出，不报错。
  BacktestSaveLoad.BacktestLocalSaveLoadAdapter = BacktestLocalSaveLoadAdapter;
  BacktestSaveLoad.createBacktestLocalSaveLoadAdapter = createBacktestLocalSaveLoadAdapter;
  BacktestSaveLoad.stripLineToolsFromContent = stripLineToolsFromContent;
  /** 供测试/调试确认存储 key 生成规则。 */
  BacktestSaveLoad.buildStorageKeys = buildStorageKeys;
})(typeof window !== "undefined" ? window : globalThis);
