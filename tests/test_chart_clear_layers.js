const assert = require("assert");
const fs = require("fs");
const vm = require("vm");

global.window = global;

vm.runInThisContext(
  fs.readFileSync("web/chanlun_chart/cl_app/static/js/charts.js", "utf8") +
    "\nglobalThis.ChartManager = ChartManager; globalThis.CHART_CONFIG = CHART_CONFIG;"
);

function resolvedId(id) {
  return { id: Promise.resolve(id), time: 100, key: id };
}

async function flushPromises() {
  await new Promise((resolve) => setImmediate(resolve));
}

async function testFullClearKeepsAiPredictionLayer() {
  const removed = [];
  const manager = new ChartManager("test");
  manager.chart = {
    removeEntity(id) {
      removed.push(id);
    },
  };
  manager.obj_charts = {
    "a:SH.000001_1": {
      fxs: [resolvedId("chanlun-fx")],
      bis: [],
      xds: [],
      zsds: [],
      bi_zss: [],
      xd_zss: [],
      zsd_zss: [],
      bcs: [],
      mmds: [],
      ai_pred_bis: [resolvedId("ai-line")],
      ai_pred_labels: [resolvedId("ai-label")],
      ai_pred_levels: [resolvedId("ai-level")],
    },
  };

  manager.clear_draw_chanlun();
  await flushPromises();

  assert.deepStrictEqual(removed, ["chanlun-fx"]);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].fxs.length, 0);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].ai_pred_bis.length, 1);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].ai_pred_labels.length, 1);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].ai_pred_levels.length, 1);
}

async function testLastClearKeepsAiPredictionLayer() {
  const removed = [];
  const manager = new ChartManager("test");
  manager.chart = {
    removeEntity(id) {
      removed.push(id);
    },
  };
  manager.obj_charts = {
    "a:SH.000001_1": {
      fxs: [resolvedId("chanlun-fx")],
      bis: [],
      xds: [],
      zsds: [],
      bi_zss: [],
      xd_zss: [],
      zsd_zss: [],
      bcs: [],
      mmds: [],
      ai_pred_bis: [{ ...resolvedId("ai-line"), time: 200 }],
    },
  };

  manager.clear_draw_chanlun("last");
  await flushPromises();

  assert.deepStrictEqual(removed, ["chanlun-fx"]);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].fxs.length, 0);
  assert.strictEqual(manager.obj_charts["a:SH.000001_1"].ai_pred_bis.length, 1);
}

(async function run() {
  await testFullClearKeepsAiPredictionLayer();
  await testLastClearKeepsAiPredictionLayer();
})();
