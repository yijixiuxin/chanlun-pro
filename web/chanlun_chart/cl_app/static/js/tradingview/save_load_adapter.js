/**
 * TradingViewSaveLoadAdapter —— 官方 `IExternalSaveLoadAdapter` 的结构实现（布局与绘图分离存储模式）。
 *
 * 契约：docs/tv_storage_refactor.md 第 4.2 节（唯一契约，已冻结）。
 *
 * 本文件是**经典脚本**（无构建、无 import/export），与 `storage_api.js` 同属 `TvStorage` 命名空间，
 * IIFE 挂到 `window.TvStorage`（Node 测试时挂 `globalThis.TvStorage`）。
 * 脚本引入顺序：先 `storage_api.js`，再本文件（本文件在真正用到时才查找 `TvStorage.TvStorageApi`，
 * 所以顺序反了也不会在加载时报错，但首次调用会给出明确错误）。
 *
 * 导出符号（`TvStorage.*`）：
 * - `TradingViewSaveLoadAdapter`              适配器类（18 个官方方法 + 迁移辅助）
 * - `createTradingViewSaveLoadAdapter(options)` 工厂：`{api?, clientId, onWarn?, onSaveNotice?}`
 * - `setupLayoutMigration(widget, adapter)`   订阅加载事件 + `chartReady()` 兜底，触发历史布局懒迁移
 * - `filterLineToolsByRequest(...)`           纯函数：按加载请求类型过滤绘图（含墓碑语义）
 * - `drawingCacheKey(...)`                    纯函数：绘图缓存 key
 *
 * 库通过**方法名反射**调用适配器，18 个方法名/签名必须与 `charting_library.d.ts`
 * 的 `IExternalSaveLoadAdapter` 逐字一致，不要改参数名以外的东西。
 *
 * 与参考 TS 实现（chanlun-open-webui）的差异：
 * - 存储客户端需要 `clientId`（本项目同一用户有多个并列图表实例，布局/绘图按 user+client 隔离）；
 * - 契约要求迁移触发的那次布局保存 notice 带 `migration: true`（UI 据此不弹「布局已保存」）；
 * - 全部导出挂在 `TvStorage` 命名空间下，供无构建的原生 <script> 使用。
 */
(function (root) {
  "use strict";

  var TvStorage = root.TvStorage || (root.TvStorage = {});

  /** 迁移时兜底布局名（契约 4.2 第 5 条）。 */
  var FALLBACK_LAYOUT_NAME = "默认布局";

  /** 全标的（布局维度）缓存桶的 symbol 占位符。 */
  var ALL_SYMBOLS_KEY = "*";

  /** `SharingMode.GloballyShared`：本版本运行时是数字 2（契约第 2 节）。 */
  var SHARING_MODE_GLOBALLY_SHARED = 2;

  /** 官方 SharingMode 枚举 —— 新版文档用字符串，本版本库实际传数字，两者都归一化。 */
  var SHARING_MODE_BY_NAME = {
    NotShared: 0,
    SharedInLayout: 1,
    GloballyShared: 2,
  };

  // ------------------------------------------------------------------ 纯函数工具

  /**
   * 绘图缓存 key：`layoutId/chartId/(symbol|"*")`。
   * 主标的用 symbol 维度（服务端按 symbol 过滤，省流量），其余请求用 `"*"` 布局维度（本地再过滤）。
   * `layoutId`/`chartId` 做 String() 归一化；调用方传的是**归一化后**的小写 symbol，
   * 保证同一标的的大小写差异落到同一个缓存桶。
   *
   * @param {string|number|null|undefined} layoutId
   * @param {string|number|null|undefined} chartId
   * @param {string|null|undefined} [symbol]
   * @returns {string}
   */
  function drawingCacheKey(layoutId, chartId, symbol) {
    var layoutPart = layoutId === null || layoutId === undefined ? "" : String(layoutId);
    var chartPart = chartId === null || chartId === undefined ? "0" : String(chartId);
    var symbolPart = symbol ? String(symbol) : ALL_SYMBOLS_KEY;
    return layoutPart + "/" + chartPart + "/" + symbolPart;
  }

  /**
   * 按官方文档规则本地过滤绘图（契约 4.2 第 7 条，参考官方 FilterLineToolsByRequest）：
   * - 墓碑 `null` 在所有 requestType 下原样保留（库据此移除本地已删除的绘图）；
   * - `allLineTools`           → 全部；
   * - `mainSeriesLineTools`    → `source.symbol` 与 `requestContext.symbol` trim+小写后相等（空 symbol 不匹配）
   *                              且（无 `seriesSourceId` 或 `ownerSource === seriesSourceId`）；
   * - `lineToolsWithoutSymbol` → 无 symbol 且（无 seriesSourceId 或 ownerSource 匹配）；
   * - `studiesLineTools`       → 无 seriesSourceId 或 `ownerSource !== seriesSourceId`；
   * - groups 只保留被过滤后存活 sources 的 `groupId` 引用到的项；
   * - 未知 requestType（库升级）宽松返回全部，避免绘图凭空消失。
   *
   * 本函数不返回 null；「过滤后没有活绘图 → null」由 `loadLineToolsAndGroups` 判断。
   *
   * @param {string} requestType
   * @param {Map<string, Object|null>|null|undefined} sources
   * @param {{symbol?: string, seriesSourceId?: string, sharingMode?: number|string}|null|undefined} requestContext
   * @param {Map<string, Object|null>|null|undefined} [groups]
   * @returns {{sources: Map<string, Object|null>, groups: Map<string, Object|null>}}
   */
  function filterLineToolsByRequest(requestType, sources, requestContext, groups) {
    requestContext = requestContext || {};
    var symbol = requestContext.symbol;
    var seriesSourceId = requestContext.seriesSourceId;
    var requestSymbol = normalizeSymbolForMatch(symbol);

    var filteredSources = new Map();
    if (sources) {
      sources.forEach(function (source, id) {
        // 墓碑（null）无法判断归属，原样保留给库处理删除。
        if (source === null || source === undefined) {
          filteredSources.set(id, null);
          return;
        }
        if (matchesRequest(source, requestType, requestSymbol, seriesSourceId)) {
          filteredSources.set(id, source);
        }
      });
    }

    // 组只跟随存活 sources 的 groupId 引用（墓碑不带 groupId）。
    var referencedGroupIds = new Set();
    filteredSources.forEach(function (source) {
      if (source && source.groupId) referencedGroupIds.add(String(source.groupId));
    });

    var filteredGroups = new Map();
    if (groups) {
      groups.forEach(function (group, groupId) {
        if (referencedGroupIds.has(String(groupId))) filteredGroups.set(groupId, group);
      });
    }

    return { sources: filteredSources, groups: filteredGroups };
  }

  /** 单条绘图是否属于本次请求（symbol 已小写归一化）。 */
  function matchesRequest(source, requestType, requestSymbol, seriesSourceId) {
    var ownedBySeries = !seriesSourceId || source.ownerSource === seriesSourceId;
    switch (requestType) {
      case "allLineTools":
        return true;
      case "mainSeriesLineTools": {
        // 大小写不敏感比较（库保存时写 "A:SH.000001"、加载请求给 "a:SH.000001"）；
        // 无 symbol 的绘图不属于主图序列，不匹配。
        var sourceSymbol = normalizeSymbolForMatch(source.symbol);
        return sourceSymbol !== "" && sourceSymbol === requestSymbol && ownedBySeries;
      }
      case "lineToolsWithoutSymbol":
        return !source.symbol && ownedBySeries;
      case "studiesLineTools":
        return !seriesSourceId || source.ownerSource !== seriesSourceId;
      default:
        return true;
    }
  }

  /**
   * 标的归一化（**只用于本地比较与缓存键，不改发往服务端的值**）：
   * 库保存绘图时 symbol 可能是大写交易所前缀（`A:SH.000001`），加载请求可能是小写（`a:SH.000001`）。
   * 只做大小写 + 首尾空白归一，刻意不做前缀剥离 / 包含匹配，避免不同标的串味。
   */
  function normalizeSymbolForMatch(value) {
    return value === null || value === undefined ? "" : String(value).trim().toLowerCase();
  }

  /**
   * sharingMode 归一化为数字（缺省/非法 → 0 = NotShared）：
   * 兼容数字 `0/1/2`（本版本库运行时值）、数字字符串以及
   * `"NotShared"/"SharedInLayout"/"GloballyShared"` 字符串枚举。
   */
  function normalizeSharingMode(value) {
    if (value === null || value === undefined) return 0;
    if (typeof value === "string") {
      var trimmed = value.trim();
      if (trimmed === "") return 0;
      if (Object.prototype.hasOwnProperty.call(SHARING_MODE_BY_NAME, trimmed)) {
        return SHARING_MODE_BY_NAME[trimmed];
      }
      var numericFromString = Number(trimmed);
      return Number.isFinite(numericFromString) ? numericFromString : 0;
    }
    var numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  /** `chart_id` 一律以数字发往后端；非数字兜底 0。 */
  function toChartId(chartId) {
    var numeric = Number(chartId);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  /** `Map<id, T|null>` → 普通对象；`undefined` 归一化为 `null`（墓碑语义）。 */
  function mapToRecord(map) {
    var record = {};
    if (!map) return record;
    if (typeof map.forEach === "function") {
      map.forEach(function (value, key) {
        record[String(key)] = value === undefined ? null : value;
      });
      return record;
    }
    // 兼容普通对象（防御性：官方类型是 Map，这里不额外依赖 instanceof）。
    Object.keys(map).forEach(function (key) {
      var value = map[key];
      record[String(key)] = value === undefined ? null : value;
    });
    return record;
  }

  /** 服务端返回的普通对象 → `Map<id, T|null>`；忽略非法条目，`null` 视为墓碑。 */
  function recordToMap(record) {
    var map = new Map();
    if (!record || typeof record !== "object") return map;
    Object.keys(record).forEach(function (key) {
      var value = record[key];
      if (value === null || value === undefined) {
        map.set(key, null);
        return;
      }
      if (typeof value !== "object") return;
      map.set(key, value);
    });
    return map;
  }

  /** 模板 content 归一化：后端可能返回字符串（JSON 文本）或对象。非法内容告警后忽略。 */
  function normalizeChartTemplateContent(raw, warn) {
    if (raw === undefined || raw === null) return undefined;
    if (typeof raw === "string") {
      if (!raw) return undefined;
      try {
        var parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : undefined;
      } catch (error) {
        warn("[TradingViewSaveLoadAdapter] getChartTemplateContent: content 不是合法 JSON，已忽略。", error);
        return undefined;
      }
    }
    return typeof raw === "object" ? raw : undefined;
  }

  /** 默认告警出口（console.warn 不存在时静默）。 */
  function defaultWarn(message, error) {
    if (typeof console !== "undefined" && typeof console.warn === "function") {
      if (error === undefined) console.warn(message);
      else console.warn(message, error);
    }
  }

  // --------------------------------------------------------------------- 适配器

  class TradingViewSaveLoadAdapter {
    /**
     * @param {{api?: Object, clientId?: string, onWarn?: Function, onSaveNotice?: Function}} [options]
     *   api          —— 注入存储客户端（Node 单测传内存假 API）；缺省内部 new TvStorageApi({clientId});
     *   clientId     —— 图表实例作用域 `chanlun_pro_<market>_<chartId>`；
     *   onWarn       —— 告警回调，缺省 console.warn；
     *   onSaveNotice —— 保存结果通知（UI 层用），回调异常会被吞掉并告警。
     */
    constructor(options) {
      options = options || {};
      this._injectedApi = options.api || null;
      this._clientId = options.clientId === undefined || options.clientId === null ? "" : String(options.clientId);
      this._warnFn = typeof options.onWarn === "function" ? options.onWarn : defaultWarn;
      this._onSaveNotice = typeof options.onSaveNotice === "function" ? options.onSaveNotice : null;
      this._lazyApi = null;

      /** 绘图缓存：key = layoutId/chartId/(symbol|"*")，值是在途或已完成的 Promise（并发去重）。 */
      this._lineToolsCache = new Map();
      /** 布局 id（String(id)）→ 服务端 drawings_migrated。 */
      this._drawingsMigratedById = new Map();
      /** 布局 id（String(id)）→ 布局名，用于迁移时兜底 chartName。 */
      this._layoutNamesById = new Map();

      this._currentLayoutId = null;
      this._currentLayoutName = null;
      /** 当前布局是否需要重存一次（旧布局首次加载）。 */
      this._pendingMigration = false;
      /** chart_load_requested 是否到达（仅记录，不作为迁移触发条件）。 */
      this._loadRequested = false;
      /** 迁移触发的布局保存进行中（用于给 notice 打 migration 标记）。 */
      this._migrationSavePending = false;
    }

    // ------------------------------------------ IExternalSaveLoadAdapter（18 个方法）

    async getAllCharts() {
      var charts = await this._resolveApi().getAllCharts();
      var result = [];
      (Array.isArray(charts) ? charts : []).forEach(function (chart) {
        if (!chart || chart.id === undefined || chart.id === null) return;
        var id = chart.id; // 原样透传（当前后端是 number），不做 String() 转换
        var key = String(id);
        if (typeof chart.drawings_migrated === "boolean") {
          this._drawingsMigratedById.set(key, chart.drawings_migrated);
        }
        if (typeof chart.name === "string" && chart.name) {
          this._layoutNamesById.set(key, chart.name);
        }
        result.push({
          id: id,
          name: chart.name === undefined || chart.name === null ? "" : chart.name,
          symbol: chart.symbol === undefined || chart.symbol === null ? "" : chart.symbol,
          resolution: chart.resolution === undefined || chart.resolution === null ? "" : chart.resolution,
          timestamp: typeof chart.timestamp === "number" ? chart.timestamp : 0,
          // 库会忽略多余字段；带上便于排查/测试，与参考实现一致。
          drawings_migrated: chart.drawings_migrated,
        });
      }, this);
      return result;
    }

    async removeChart(id) {
      await this._resolveApi().removeChart(id);
      var key = String(id);
      this._drawingsMigratedById.delete(key);
      this._layoutNamesById.delete(key);
      // 删除布局会连带删除该布局保存过的 GloballyShared 条目（全局桶对所有布局可见），
      // 所以必须清空整个缓存，只失效自身布局前缀不够。
      this._lineToolsCache.clear();
      if (this._currentLayoutId === key) {
        this._currentLayoutId = null;
        this._currentLayoutName = null;
        this._pendingMigration = false;
      }
    }

    async saveChart(chartData) {
      chartData = chartData || {};
      // 迁移触发的那一次布局保存：notice 带 migration:true（UI 不弹「布局已保存」）。
      var migration = this._migrationSavePending === true;
      this._migrationSavePending = false;
      var id;
      try {
        id = await this._resolveApi().saveChart({
          id: chartData.id,
          name: chartData.name,
          symbol: chartData.symbol,
          resolution: chartData.resolution,
          content: chartData.content,
          timestamp: chartData.timestamp,
        });
      } catch (error) {
        this._notifySave({ kind: "chart", ok: false, name: chartData.name || "", error: error, migration: migration });
        throw error;
      }
      if (id !== undefined && id !== null) {
        var key = String(id);
        // 布局一旦经过本适配器保存，服务端即为分离存储（契约 3.3），本地无需再迁移。
        this._drawingsMigratedById.set(key, true);
        if (this._currentLayoutId === key) this._pendingMigration = false;
        if (chartData.name) this._layoutNamesById.set(key, chartData.name);
        this._notifySave({ kind: "chart", ok: true, name: chartData.name || "", migration: migration });
      }
      return id;
    }

    async getChartContent(id) {
      var content = await this._resolveApi().getChartContent(id);
      // 内容取回之后才记录当前布局并标记待迁移：此刻布局内容已经交给库，
      // 配合 layout_loaded / chartReady 触发，避免「内容还没到就重存」。
      // 未知 id（未在 getAllCharts 出现）也按未迁移处理，保证历史布局一定会被重存。
      var key = String(id);
      this._currentLayoutId = key;
      var recordedName = this._layoutNamesById.get(key);
      this._currentLayoutName = recordedName === undefined ? null : recordedName;
      this._pendingMigration = this._drawingsMigratedById.get(key) !== true;
      return content;
    }

    async getAllStudyTemplates() {
      var templates = await this._resolveApi().getAllStudyTemplates();
      return (Array.isArray(templates) ? templates : []).map(function (template) {
        return { name: template && template.name !== undefined && template.name !== null ? template.name : "" };
      });
    }

    async removeStudyTemplate(studyTemplateInfo) {
      await this._resolveApi().removeStudyTemplate(studyTemplateInfo && studyTemplateInfo.name !== undefined ? studyTemplateInfo.name : "");
    }

    async saveStudyTemplate(studyTemplateData) {
      studyTemplateData = studyTemplateData || {};
      try {
        await this._resolveApi().saveStudyTemplate({
          name: studyTemplateData.name,
          content: studyTemplateData.content,
        });
      } catch (error) {
        this._notifySave({ kind: "studyTemplate", ok: false, name: studyTemplateData.name || "", error: error });
        throw error;
      }
      this._notifySave({ kind: "studyTemplate", ok: true, name: studyTemplateData.name || "" });
    }

    async getStudyTemplateContent(studyTemplateInfo) {
      return await this._resolveApi().getStudyTemplateContent(
        studyTemplateInfo && studyTemplateInfo.name !== undefined ? studyTemplateInfo.name : ""
      );
    }

    async getDrawingTemplates(toolName) {
      var names = await this._resolveApi().getDrawingTemplates(toolName);
      return (Array.isArray(names) ? names : []).map(function (name) {
        return String(name);
      });
    }

    async loadDrawingTemplate(toolName, templateName) {
      return await this._resolveApi().loadDrawingTemplate(toolName, templateName);
    }

    async saveDrawingTemplate(toolName, templateName, content) {
      try {
        await this._resolveApi().saveDrawingTemplate(toolName, templateName, content);
      } catch (error) {
        this._notifySave({ kind: "drawingTemplate", ok: false, name: templateName || "", error: error });
        throw error;
      }
      this._notifySave({ kind: "drawingTemplate", ok: true, name: templateName || "" });
    }

    async removeDrawingTemplate(toolName, templateName) {
      await this._resolveApi().removeDrawingTemplate(toolName, templateName);
    }

    async getAllChartTemplates() {
      var names = await this._resolveApi().getAllChartTemplates();
      return (Array.isArray(names) ? names : []).map(function (name) {
        return String(name);
      });
    }

    async saveChartTemplate(newName, theme) {
      try {
        await this._resolveApi().saveChartTemplate(newName, theme);
      } catch (error) {
        this._notifySave({ kind: "chartTemplate", ok: false, name: newName || "", error: error });
        throw error;
      }
      this._notifySave({ kind: "chartTemplate", ok: true, name: newName || "" });
    }

    async removeChartTemplate(templateName) {
      await this._resolveApi().removeChartTemplate(templateName);
    }

    async getChartTemplateContent(templateName) {
      var self = this;
      var raw = await this._resolveApi().getChartTemplateContent(templateName);
      var content = normalizeChartTemplateContent(raw, function (message, error) {
        self._warn(message, error);
      });
      return content === undefined ? {} : { content: content };
    }

    /**
     * 保存绘图/绘图组：`state.sources/groups` 是 `Map`，这里序列化为普通对象（值保持 `null` 墓碑语义），
     * 整包只发一次 POST。`layoutId` 为空直接抛错且不发请求（库也不允许无布局保存）。
     */
    async saveLineToolsAndGroups(layoutId, chartId, state, context) {
      if (layoutId === undefined || layoutId === null || String(layoutId).length === 0) {
        throw new Error(
          "[TradingViewSaveLoadAdapter] saveLineToolsAndGroups: layoutId 为空，拒绝保存绘图（库不允许无布局保存）。"
        );
      }
      state = state || {};
      context = context || {};
      var layoutKey = String(layoutId);
      // 归一化后与发往后端的数值共用同一个变量，避免「失效口径」与「发送口径」不一致。
      var sharingMode = normalizeSharingMode(context.sharingMode);
      var symbol = context.symbol !== undefined && context.symbol !== null ? context.symbol : state.symbol;
      var sources = mapToRecord(state.sources);
      var groups = mapToRecord(state.groups);

      // 统计本次实际写入/删除的条目数（null = 墓碑 = 删除），用于 UI 提示与「空保存不打扰」。
      var savedCount = 0;
      var deletedCount = 0;
      Object.keys(sources).forEach(function (key) {
        if (sources[key] === null) deletedCount += 1;
        else savedCount += 1;
      });
      Object.keys(groups).forEach(function (key) {
        if (groups[key] === null) deletedCount += 1;
        else savedCount += 1;
      });

      try {
        await this._resolveApi().saveLineTools({
          layoutId: layoutKey,
          chartId: toChartId(chartId),
          sharingMode: sharingMode,
          symbol: symbol ? String(symbol) : undefined,
          sources: sources,
          groups: groups,
        });
      } catch (error) {
        this._notifySave({
          kind: "lineTools",
          ok: false,
          saved: savedCount,
          deleted: deletedCount,
          error: error,
        });
        throw error;
      }

      // 刚写入的数据不能再用旧缓存（下一次 load 会重新取数）。
      this._invalidateLineToolsCacheAfterWrite(sharingMode, layoutKey);

      // 库在保存布局时也会顺带 flush 一次绘图（可能是空包），空包不提示，避免刷屏。
      if (savedCount + deletedCount > 0) {
        this._notifySave({
          kind: "lineTools",
          ok: true,
          saved: savedCount,
          deleted: deletedCount,
        });
      }
    }

    /**
     * 加载绘图/绘图组：
     * - `layoutId` 为空 → `null`；
     * - 缓存 key = layoutId/chartId/(symbol|"*")，值存 Promise 做在途去重；
     * - `mainSeriesLineTools` 且有 symbol → 按 symbol 维度取数；其余 → `allSymbols=true` 布局维度取数后本地过滤；
     * - 本地过滤按契约 4.2 第 7 条；过滤后没有任何「活」source → `null`。
     */
    async loadLineToolsAndGroups(layoutId, chartId, requestType, requestContext) {
      if (layoutId === undefined || layoutId === null || String(layoutId).length === 0) return null;

      requestContext = requestContext || {};
      var layoutKey = String(layoutId);
      var numericChartId = toChartId(chartId);
      var symbol = requestContext.symbol;
      // 缓存键与「是否需要 symbol 维度取数」都用归一化后的 symbol：同一标的的大小写差异
      // （库保存用 "A:SH.000001"、加载请求用 "a:SH.000001"）必须落到同一个缓存桶。
      // 发往服务端的 symbol 仍用库给的原始值（大小写匹配由后端负责）。
      var symbolKey = normalizeSymbolForMatch(symbol);
      var useSymbolScope = requestType === "mainSeriesLineTools" && symbolKey !== "";

      var cacheKey = drawingCacheKey(layoutKey, numericChartId, useSymbolScope ? symbolKey : null);
      var cached = await this._loadRawLineTools(
        cacheKey,
        layoutKey,
        numericChartId,
        useSymbolScope ? symbol : undefined,
        !useSymbolScope
      );

      var filtered = filterLineToolsByRequest(requestType, cached.sources, requestContext, cached.groups);
      // 空结果 → null：过滤后没有任何「活」绘图时返回 null（只有墓碑、或什么都不剩）。
      var hasLiveSource = false;
      filtered.sources.forEach(function (source) {
        if (source !== null && source !== undefined) hasLiveSource = true;
      });
      if (!hasLiveSource) return null;

      return { sources: filtered.sources, groups: filtered.groups };
    }

    // ------------------------------------------------------------- 迁移辅助（契约 4.2 第 8/9 条）

    /** `chart_load_requested` 事件：仅记录（该事件只覆盖 Load layout 对话框路径，不作为触发门禁）。 */
    noteChartLoadRequested() {
      this._loadRequested = true;
    }

    /**
     * 三条加载路径共用的迁移判定：仅当「当前已知布局 id」+「该布局未迁移」时返回 true，
     * 并立即消费（防重入：同一次加载只保存一次）。刻意不要求 `_loadRequested`：
     * `load_last_chart` 启动路径不会 emit `chart_load_requested`，靠它当门禁会导致启动路径永不迁移。
     */
    shouldRunMigrationSave() {
      if (!this._pendingMigration || this._currentLayoutId === null) return false;
      this._loadRequested = false;
      this._pendingMigration = false;
      return true;
    }

    /** 迁移保存结果记账；失败时恢复 pending，使下次加载可重试（告警由调用方负责）。 */
    markMigrationResult(id, ok) {
      var key = id === null || id === undefined ? null : String(id);
      if (ok) {
        if (key !== null) {
          this._drawingsMigratedById.set(key, true);
          this._pendingMigration = false;
        }
        return;
      }
      if (key !== null && key === this._currentLayoutId) {
        this._pendingMigration = true;
      }
    }

    /** 当前布局 id（String(id)），未加载布局时为 null。 */
    getCurrentLayoutId() {
      return this._currentLayoutId;
    }

    /** 当前布局名（来自 getAllCharts / saveChart），未知时为 null。 */
    getCurrentLayoutName() {
      return this._currentLayoutName;
    }

    /** 供 `setupLayoutMigration` 等外部代码复用同一套告警出口。 */
    notifyWarning(message, error) {
      this._warn(message, error);
    }

    /** 迁移保存开始（内部使用）：本次布局保存的 notice 会带 `migration: true`。 */
    beginMigrationSave() {
      this._migrationSavePending = true;
    }

    /** 迁移保存结束（内部使用，兜底清标记，防止 saveChartToServer 未触发保存时污染后续提示）。 */
    endMigrationSave() {
      this._migrationSavePending = false;
    }

    // -------------------------------------------------------------------- internals

    _resolveApi() {
      if (this._injectedApi) return this._injectedApi;
      if (!this._lazyApi) {
        var ApiCtor = TvStorage.TvStorageApi;
        if (typeof ApiCtor !== "function") {
          throw new Error(
            "[TradingViewSaveLoadAdapter] 未找到 TvStorage.TvStorageApi，请先加载 js/tradingview/storage_api.js 或注入 api。"
          );
        }
        this._lazyApi = new ApiCtor({ clientId: this._clientId });
      }
      return this._lazyApi;
    }

    /** 通知 UI 层保存结果；回调是「尽力而为」的，异常只告警，绝不影响保存流程。 */
    _notifySave(notice) {
      if (!this._onSaveNotice) return;
      try {
        this._onSaveNotice(notice);
      } catch (error) {
        this._warn("[TradingViewSaveLoadAdapter] onSaveNotice 回调异常（已忽略）", error);
      }
    }

    /** 告警出口；onWarn 回调自身异常也必须吞掉。 */
    _warn(message, error) {
      try {
        this._warnFn(message, error);
      } catch (error) {
        /* 忽略告警回调异常 */
      }
    }

    /**
     * 取原始绘图（带**在途 Promise 去重**）。
     * 库对同一个 key 会并发发起多次加载（sharingMode 轮询、studies / withoutSymbol 并发），
     * 所以这里**同步**先把在途 Promise 放进 map 再 await，并发的第二次调用直接复用同一个 Promise。
     * 失败不缓存：reject 时把该 key 从 map 删掉（只删仍指向本次 Promise 的条目），下次可重试。
     */
    _loadRawLineTools(cacheKey, layoutId, chartId, symbol, allSymbols) {
      var self = this;
      var inFlight = this._lineToolsCache.get(cacheKey);
      if (inFlight) return inFlight;

      // 用 async IIFE 让「发起请求」与「登记 Promise」在同一个同步段内完成（await 之前 map 里已有条目）。
      var pending = (async function () {
        var data = await self._resolveApi().loadLineTools({
          layoutId: layoutId,
          chartId: chartId,
          symbol: symbol,
          allSymbols: allSymbols,
        });
        return {
          sources: recordToMap(data ? data.sources : null),
          groups: recordToMap(data ? data.groups : null),
        };
      })();

      this._lineToolsCache.set(cacheKey, pending);

      // 失败清理（同时保证 map 里的 Promise 有 rejection handler，不会有 unhandled rejection）。
      pending.catch(function () {
        if (self._lineToolsCache.get(cacheKey) === pending) self._lineToolsCache.delete(cacheKey);
      });

      return pending;
    }

    /**
     * 绘图写入后的缓存失效（按 sharingMode 区分可见范围）：
     * - `sharingMode === 2`（GloballyShared）：对所有布局可见，且服务端保存全局条目时会跨布局删除
     *   同 item_id 的脏副本 → 必须清空整个缓存；
     * - `sharingMode === 0/1`：只影响自身布局 → 按 layoutId/ 前缀失效即可。
     */
    _invalidateLineToolsCacheAfterWrite(sharingMode, layoutId) {
      if (sharingMode === SHARING_MODE_GLOBALLY_SHARED) {
        this._lineToolsCache.clear();
        return;
      }
      var prefix = layoutId + "/";
      Array.from(this._lineToolsCache.keys()).forEach(function (key) {
        if (key.indexOf(prefix) === 0) this._lineToolsCache.delete(key);
      }, this);
    }
  }

  function createTradingViewSaveLoadAdapter(options) {
    return new TradingViewSaveLoadAdapter(options || {});
  }

  /**
   * 订阅懒迁移流程（契约 4.2 第 9 条）。
   *
   * 库有三条加载布局的路径，各自 emit 的事件不同，这里用**同一个 attempt** 覆盖
   * （attempt 内部靠 `shouldRunMigrationSave()` 消费一次去重，保证同一次加载只保存一次）：
   * 1. 「Load layout 对话框」：`chart_load_requested`（仅记录）→ `layout_loaded` / `chart_loaded`；
   * 2. 公开 API 路径：`chart_loaded`；
   * 3. `load_last_chart` 启动路径（本项目默认）：三个事件都不发，只能靠
   *    `widget.chartReady().then(...)` 兜底（真实 CL v33.0.0 浏览器 E2E 已确认）。
   *
   * 触发后用**回调签名** `widget.saveChartToServer(onDone, onFail, {chartName, defaultChartName})`
   * （本版本库只实现回调签名，传单对象会把对象当回调调用而报错）。
   * 失败只告警 + `markMigrationResult(id, false)`，下次加载自动重试。
   */
  function setupLayoutMigration(widget, adapter) {
    if (!widget || typeof widget.subscribe !== "function") {
      adapter.notifyWarning("[TradingViewSaveLoadAdapter] setupLayoutMigration: widget.subscribe 不可用，跳过迁移订阅。");
      return;
    }

    var attemptMigrationSave = function () {
      if (!adapter.shouldRunMigrationSave()) return;

      var layoutId = adapter.getCurrentLayoutId();
      if (typeof widget.saveChartToServer !== "function") {
        adapter.notifyWarning("[TradingViewSaveLoadAdapter] widget.saveChartToServer 不可用，迁移保存跳过。");
        adapter.markMigrationResult(layoutId, false);
        return;
      }

      var chartName = resolveMigrationChartName(widget, adapter);
      try {
        adapter.beginMigrationSave();
        widget.saveChartToServer(
          function () {
            adapter.endMigrationSave();
            adapter.markMigrationResult(layoutId, true);
          },
          function (error) {
            adapter.endMigrationSave();
            adapter.notifyWarning(
              "[TradingViewSaveLoadAdapter] 布局 " + (layoutId === null ? "(unknown)" : layoutId) + " 迁移保存失败，将在下次加载时重试。",
              error
            );
            adapter.markMigrationResult(layoutId, false);
          },
          { chartName: chartName, defaultChartName: chartName }
        );
      } catch (error) {
        adapter.endMigrationSave();
        adapter.notifyWarning(
          "[TradingViewSaveLoadAdapter] 布局 " + (layoutId === null ? "(unknown)" : layoutId) + " 调用 saveChartToServer 异常，将在下次加载时重试。",
          error
        );
        adapter.markMigrationResult(layoutId, false);
      }
    };

    safeSubscribe(widget, adapter, "chart_load_requested", function () {
      adapter.noteChartLoadRequested();
    });
    safeSubscribe(widget, adapter, "chart_loaded", attemptMigrationSave);
    // `layout_loaded` 未收录在本版 charting_library.d.ts 的事件表里；真实 CL v33.0.0 E2E 确认
    // `load_last_chart` 启动路径连它也不 emit（chart_load_requested / chart_loaded 同样不发），
    // 启动路径实际靠下面的 `chartReady()` 兜底。仍保留订阅：对话框 / loadChart API 路径会 emit。
    var layoutLoadedSubscribed = safeSubscribe(widget, adapter, "layout_loaded", attemptMigrationSave);

    // 启动路径兜底：用 `chartReady()`（返回 Promise；已 ready 时立即 resolve）替代已废弃的 onChartReady()。
    if (typeof widget.chartReady === "function") {
      try {
        var ready = widget.chartReady();
        if (ready && typeof ready.then === "function") {
          ready.then(
            function () {
              attemptMigrationSave();
            },
            function (error) {
              adapter.notifyWarning(
                "[TradingViewSaveLoadAdapter] widget.chartReady() 失败，启动路径仅靠 layout_loaded 兜底。",
                error
              );
            }
          );
        } else {
          // 兼容返回非 Promise 的 facade 实现：立即尝试一次。
          attemptMigrationSave();
        }
      } catch (error) {
        adapter.notifyWarning(
          "[TradingViewSaveLoadAdapter] widget.chartReady() 调用异常，启动路径仅靠 layout_loaded 兜底。",
          error
        );
      }
    } else if (!layoutLoadedSubscribed) {
      adapter.notifyWarning(
        "[TradingViewSaveLoadAdapter] 既无 widget.chartReady 也无 layout_loaded 订阅，load_last_chart 启动路径的懒迁移无法触发（对话框/API 路径不受影响）。"
      );
    }
  }

  /** 订阅库事件；事件名未知/已废弃或 facade 不接受时只告警、不抛出。返回是否订阅成功。 */
  function safeSubscribe(widget, adapter, event, handler) {
    try {
      widget.subscribe(event, handler);
      return true;
    } catch (error) {
      adapter.notifyWarning("[TradingViewSaveLoadAdapter] 订阅 " + event + " 事件失败。", error);
      return false;
    }
  }

  /** 迁移保存用的布局名：activeChart().chartName?.() → 适配器记录名 → "默认布局"。 */
  function resolveMigrationChartName(widget, adapter) {
    try {
      var chart = typeof widget.activeChart === "function" ? widget.activeChart() : undefined;
      if (chart && typeof chart.chartName === "function") {
        var name = chart.chartName();
        if (typeof name === "string" && name.length > 0) return name;
      }
    } catch (error) {
      // activeChart 在某些生命周期阶段可能抛错，忽略并走兜底名。
    }
    var recorded = adapter.getCurrentLayoutName();
    return recorded && recorded.length > 0 ? recorded : FALLBACK_LAYOUT_NAME;
  }

  // 重复加载（示例：测试脚本多次 eval）时只覆盖同名导出，不报错。
  TvStorage.TradingViewSaveLoadAdapter = TradingViewSaveLoadAdapter;
  TvStorage.createTradingViewSaveLoadAdapter = createTradingViewSaveLoadAdapter;
  TvStorage.setupLayoutMigration = setupLayoutMigration;
  TvStorage.filterLineToolsByRequest = filterLineToolsByRequest;
  TvStorage.drawingCacheKey = drawingCacheKey;
})(typeof window !== "undefined" ? window : globalThis);
