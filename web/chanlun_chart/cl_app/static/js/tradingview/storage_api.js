/**
 * TvStorageApi —— TradingView 图表存储后端（`/tv/storage/*`）的薄 fetch 封装。
 *
 * 契约：docs/tv_storage_refactor.md 第 4.1 节（唯一契约，已冻结）。
 *
 * 本文件是**经典脚本**（无构建、无 import/export），用 IIFE 把导出挂到
 * `window.TvStorage`（Node 测试时挂 `globalThis.TvStorage`），因此：
 * - 浏览器里必须在 `charts.js` / `save_load_adapter.js` 之前用 <script> 引入；
 * - Node 24 可直接 `require()` 做单测（导出的构造函数挂在全局 `TvStorage` 上）。
 *
 * 约定（见契约第 3/4 节）：
 * - 布局 / 迁移 / 绘图接口统一带 `?client=encodeURIComponent(clientId)`；
 *   指标模板 / 画图模板 / 图表模板接口**不带** client（只按 user_id 过滤）。
 * - `layout_id` 一律以字符串发送，`chart_id` 以数字发送，`all_symbols` 以布尔发送。
 * - 请求 `credentials: "same-origin"`（flask_login 同源 Cookie）。
 * - 响应统一信封 `{status: "ok", data}`；`status !== "ok"` 时抛 `Error`
 *   （错误文案优先用 `message`/`detail`，并附 HTTP 状态）。
 * - 依赖注入：默认用全局 `fetch`，测试可传 `fetchImpl`；`baseUrl` 默认 `""`（同源）。
 */
(function (root) {
  "use strict";

  var TvStorage = root.TvStorage || (root.TvStorage = {});

  /** 请求体/查询串里把任意值归一化为字符串（undefined/null 由调用方决定是否跳过）。 */
  function toQueryString(value) {
    return encodeURIComponent(String(value));
  }

  /** chart_id / sharing_mode 等数值参数：非数字兜底 0。 */
  function toNumber(value) {
    var numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  /** 模板列表返回 `[{name}]`，这里统一收窄成 `string[]`。 */
  function toNameList(list) {
    if (!Array.isArray(list)) return [];
    var names = [];
    list.forEach(function (item) {
      if (item === null || item === undefined) return;
      var name = typeof item === "object" ? item.name : item;
      names.push(name === undefined || name === null ? "" : String(name));
    });
    return names;
  }

  /** 安全 JSON 化（循环引用等极端情况兜底成 String）。 */
  function safeJson(value) {
    try {
      return JSON.stringify(value);
    } catch (error) {
      return String(value);
    }
  }

  class TvStorageApi {
    /**
     * @param {{clientId?: string, baseUrl?: string, fetchImpl?: Function}} [options]
     *   clientId —— 图表实例作用域，取值 `chanlun_pro_<market>_<chartId>`（与旧数据一致）；
     *   baseUrl  —— 服务端前缀，默认 `""`（浏览器同源）；
     *   fetchImpl —— 注入的 fetch 实现，默认全局 fetch。
     */
    constructor(options) {
      options = options || {};
      this.clientId = options.clientId === undefined || options.clientId === null ? "" : String(options.clientId);
      this.baseUrl = options.baseUrl === undefined || options.baseUrl === null ? "" : String(options.baseUrl);
      this.fetchImpl = typeof options.fetchImpl === "function" ? options.fetchImpl : null;
    }

    // ------------------------------------------------------------------ 布局 charts

    async getAllCharts() {
      var charts = await this._call("GET", "/tv/storage/charts", {
        query: { client: this.clientId },
      });
      return Array.isArray(charts) ? charts : [];
    }

    async saveChart(data) {
      data = data || {};
      var result = await this._call("POST", "/tv/storage/charts", {
        query: { client: this.clientId },
        body: {
          id: data.id === undefined ? null : data.id,
          name: data.name,
          symbol: data.symbol,
          resolution: data.resolution,
          content: data.content,
          timestamp: data.timestamp,
        },
      });
      if (result && typeof result === "object" && result.id !== undefined && result.id !== null) {
        return result.id;
      }
      throw new Error("[TvStorageApi] saveChart: 响应缺少 data.id");
    }

    async getChartContent(id) {
      var result = await this._call(
        "GET",
        "/tv/storage/charts/" + encodeURIComponent(String(id)) + "/content",
        { query: { client: this.clientId } }
      );
      return result && typeof result.content === "string" ? result.content : "";
    }

    async removeChart(id) {
      await this._call("DELETE", "/tv/storage/charts/" + encodeURIComponent(String(id)), {
        query: { client: this.clientId },
      });
    }

    // ------------------------------------------------------------- 指标模板 study templates

    async getAllStudyTemplates() {
      var templates = await this._call("GET", "/tv/storage/study_templates");
      if (!Array.isArray(templates)) return [];
      return templates.map(function (template) {
        return { name: template && template.name !== undefined ? String(template.name) : "" };
      });
    }

    async getStudyTemplateContent(name) {
      var result = await this._call("GET", "/tv/storage/study_templates/content", {
        query: { name: name === undefined || name === null ? "" : name },
      });
      return result && typeof result.content === "string" ? result.content : "";
    }

    async saveStudyTemplate(data) {
      data = data || {};
      await this._call("POST", "/tv/storage/study_templates", {
        body: { name: data.name, content: data.content },
      });
    }

    async removeStudyTemplate(name) {
      await this._call("DELETE", "/tv/storage/study_templates", {
        query: { name: name === undefined || name === null ? "" : name },
      });
    }

    // ------------------------------------------------------------ 画图模板 drawing templates

    async getDrawingTemplates(tool) {
      var templates = await this._call("GET", "/tv/storage/drawing_templates", {
        query: { tool: tool === undefined || tool === null ? "" : tool },
      });
      return toNameList(templates);
    }

    async loadDrawingTemplate(tool, name) {
      var result = await this._call("GET", "/tv/storage/drawing_templates/content", {
        query: {
          tool: tool === undefined || tool === null ? "" : tool,
          name: name === undefined || name === null ? "" : name,
        },
      });
      return result && typeof result.content === "string" ? result.content : "";
    }

    async saveDrawingTemplate(tool, name, content) {
      await this._call("POST", "/tv/storage/drawing_templates", {
        body: { tool: tool, name: name, content: content },
      });
    }

    async removeDrawingTemplate(tool, name) {
      await this._call("DELETE", "/tv/storage/drawing_templates", {
        query: {
          tool: tool === undefined || tool === null ? "" : tool,
          name: name === undefined || name === null ? "" : name,
        },
      });
    }

    // -------------------------------------------------------------- 图表模板 chart templates

    async getAllChartTemplates() {
      var templates = await this._call("GET", "/tv/storage/chart_templates");
      return toNameList(templates);
    }

    async getChartTemplateContent(name) {
      var result = await this._call("GET", "/tv/storage/chart_templates/content", {
        query: { name: name === undefined || name === null ? "" : name },
      });
      return result ? result.content : undefined;
    }

    async saveChartTemplate(name, content) {
      await this._call("POST", "/tv/storage/chart_templates", {
        body: { name: name, content: content },
      });
    }

    async removeChartTemplate(name) {
      await this._call("DELETE", "/tv/storage/chart_templates", {
        query: { name: name === undefined || name === null ? "" : name },
      });
    }

    // ------------------------------------------------------------------- 绘图 line tools

    /**
     * @param {{layoutId: string, chartId: string|number, symbol?: string, allSymbols?: boolean}} query
     * @returns {Promise<{sources: Object, groups: Object, symbol?: string}>}
     */
    async loadLineTools(query) {
      query = query || {};
      var result = await this._call("GET", "/tv/storage/line_tools", {
        query: {
          client: this.clientId,
          layout_id: query.layoutId === undefined || query.layoutId === null ? "" : String(query.layoutId),
          chart_id: toNumber(query.chartId),
          symbol: query.symbol === undefined || query.symbol === null ? "" : String(query.symbol),
          all_symbols: query.allSymbols === true,
        },
      });
      return {
        sources: result && result.sources && typeof result.sources === "object" ? result.sources : {},
        groups: result && result.groups && typeof result.groups === "object" ? result.groups : {},
        symbol: result ? result.symbol : undefined,
      };
    }

    /**
     * 整包一次 POST；`sources`/`groups` 里的 `null` 表示删除该条（墓碑）。
     * @param {{layoutId: string|number, chartId: string|number, sharingMode: number,
     *          symbol?: string, sources: Object, groups: Object}} payload
     * @returns {Promise<{saved: number, deleted: number}>}
     */
    async saveLineTools(payload) {
      payload = payload || {};
      var result = await this._call("POST", "/tv/storage/line_tools", {
        query: { client: this.clientId },
        body: {
          layout_id: payload.layoutId === undefined || payload.layoutId === null ? "" : String(payload.layoutId),
          chart_id: toNumber(payload.chartId),
          sharing_mode: toNumber(payload.sharingMode),
          symbol: payload.symbol === undefined || payload.symbol === null ? "" : String(payload.symbol),
          sources: payload.sources || {},
          groups: payload.groups || {},
        },
      });
      return {
        saved: result && Number.isFinite(Number(result.saved)) ? Number(result.saved) : 0,
        deleted: result && Number.isFinite(Number(result.deleted)) ? Number(result.deleted) : 0,
      };
    }

    // --------------------------------------------------------------------- internals

    /** `${baseUrl}${path}?k=v...`；undefined/null 的查询值跳过，其余统一 encodeURIComponent。 */
    _buildUrl(path, query) {
      var parts = [];
      if (query) {
        Object.keys(query).forEach(function (key) {
          var value = query[key];
          if (value === undefined || value === null) return;
          parts.push(toQueryString(key) + "=" + toQueryString(value));
        });
      }
      var search = parts.length > 0 ? "?" + parts.join("&") : "";
      return this.baseUrl + path + search;
    }

    _getFetch() {
      if (this.fetchImpl) return this.fetchImpl;
      if (typeof root.fetch === "function") return root.fetch.bind(root);
      if (typeof fetch === "function") return fetch.bind(globalThis);
      throw new Error(
        "[TvStorageApi] 当前环境没有可用的 fetch，请通过 {fetchImpl} 注入。"
      );
    }

    async _call(method, path, options) {
      options = options || {};
      var url = this._buildUrl(path, options.query);
      var headers = { Accept: "application/json" };
      var init = { method: method, headers: headers, credentials: "same-origin" };
      if (options.body !== undefined) {
        headers["Content-Type"] = "application/json";
        init.body = JSON.stringify(options.body);
      }

      var response = await this._getFetch()(url, init);

      var payload = null;
      if (response && typeof response.json === "function") {
        try {
          payload = await response.json();
        } catch (error) {
          payload = null;
        }
      }

      if (!payload || payload.status !== "ok") {
        throw new Error(describeError(method, path, response, payload));
      }
      return payload.data;
    }
  }

  /** 失败文案：优先 `message`/`detail`（后端 Flask 信封），其次 HTTP 状态。 */
  function describeError(method, path, response, payload) {
    var detail;
    if (payload && payload.message !== undefined && payload.message !== null && payload.message !== "") {
      detail = payload.message;
    } else if (payload && payload.detail !== undefined && payload.detail !== null && payload.detail !== "") {
      detail = payload.detail;
    }

    var reason = "";
    if (typeof detail === "string" && detail) {
      reason = detail;
    } else if (detail !== undefined && detail !== null) {
      reason = safeJson(detail);
    }

    var statusText = "";
    if (response && typeof response.status === "number" && response.status > 0) {
      statusText = "HTTP " + response.status + (response.statusText ? " " + response.statusText : "");
    }

    if (reason && statusText) reason = reason + " (" + statusText + ")";
    else if (!reason) reason = statusText || "未知错误";

    return "[TvStorageApi] " + method + " " + path + " 失败: " + reason;
  }

  /** 工厂函数（契约导出之一）。 */
  function createTvStorageApi(options) {
    return new TvStorageApi(options || {});
  }

  // 重复加载（示例：测试脚本多次 eval）时只覆盖同名导出，不报错。
  TvStorage.TvStorageApi = TvStorageApi;
  TvStorage.createTvStorageApi = createTvStorageApi;
})(typeof window !== "undefined" ? window : globalThis);
