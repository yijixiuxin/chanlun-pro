const assert = require("assert");
const fs = require("fs");
const vm = require("vm");

const createdShapes = [];
const removedShapes = [];

global.window = global;
global.console = console;
global.CHART_CONFIG = { COLORS: { AI_PRED: "#8E44AD" } };

class FakeElement {
  constructor(tagName) {
    this.tagName = tagName;
    this.children = [];
    this.parentNode = null;
    this.style = {};
    this.attributes = {};
    this.className = "";
    this.textContent = "";
    this.innerHTML = "";
    this.id = "";
  }

  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }

  remove() {
    if (!this.parentNode) return;
    this.parentNode.children = this.parentNode.children.filter(
      (child) => child !== this
    );
    this.parentNode = null;
  }

  setAttribute(name, value) {
    this.attributes[name] = value;
    if (name === "id") this.id = value;
  }

  querySelector(selector) {
    if (selector.startsWith(".")) {
      const className = selector.slice(1);
      return findElement(this, function (element) {
        return String(element.className || "")
          .split(/\s+/)
          .includes(className);
      });
    }
    return null;
  }
}

function findElement(root, predicate) {
  for (const child of root.children) {
    if (predicate(child)) return child;
    const found = findElement(child, predicate);
    if (found) return found;
  }
  return null;
}

const chartRoot = new FakeElement("div");
chartRoot.id = "tv_chart_container_test";
chartRoot.style.position = "static";
const elementsById = { tv_chart_container_test: chartRoot };
global.document = {
  createElement(tagName) {
    return new FakeElement(tagName);
  },
  getElementById(id) {
    return elementsById[id] || null;
  },
};
global.getComputedStyle = function (element) {
  return { position: element.style.position || "static" };
};
global.ChartUtils = {
  createLineShape(chart, line, options) {
    const id = `line-${createdShapes.length + 1}`;
    createdShapes.push({ id, kind: "line", line, options });
    return Promise.resolve(id);
  },
  createShape(chart, point, options) {
    const id = `shape-${createdShapes.length + 1}`;
    createdShapes.push({ id, kind: "shape", point, options });
    return Promise.resolve(id);
  },
};

vm.runInThisContext(
  fs.readFileSync("web/chanlun_chart/cl_app/static/js/ai_prediction.js", "utf8")
);

const manager = {
  id: "test",
  chart: {
    removeEntity(id) {
      removedShapes.push(id);
    },
  },
  obj_charts: {},
  widget: {
    symbolInterval() {
      return { symbol: "a:SH.000001", interval: "1" };
    },
  },
  getChartData() {
    return { symbolKey: "a_SH.000001_1" };
  },
  initChartContainer(symbolKey) {
    this.obj_charts[symbolKey] = this.obj_charts[symbolKey] || {};
    return this.obj_charts[symbolKey];
  },
};

window.AIPrediction.draw(manager, {
  summary: "中枢震荡，后续演化三类。",
  current_structure: "线段向下未完成，价格位于中枢内。",
  classes: [
    {
      key: "up_break",
      name: "向上突破",
      direction: "up",
      probability: 0.45,
      trigger: "放量站上中枢上沿",
      boundary: "ZG=3310",
      action: "三买确认后跟随",
      basis: "离开中枢不背驰",
      bis: [
        {
          points: [
            { time: 1767231060, price: 3280 },
            { time: 1767231360, price: 3360 },
          ],
        },
      ],
      levels: [{ price: 3310, type: "trigger", text: "触发" }],
    },
    {
      key: "down_break",
      name: "向下突破",
      direction: "down",
      probability: 0.3,
      trigger: "跌破ZD形成三卖",
      boundary: "不回到ZD上方",
      action: "三卖确认后做空",
      basis: "向下离开中枢",
      bis: [],
      levels: [],
    },
  ],
});

assert.strictEqual(createdShapes.filter((shape) => shape.kind === "line").length, 2);
assert.strictEqual(
  createdShapes.some(
    (shape) => shape.options.text && shape.options.text.includes("向上突破")
  ),
  true
);
assert.strictEqual(
  createdShapes.some(
    (shape) => shape.options.text && shape.options.text.includes("触发")
  ),
  true
);
const panel = chartRoot.querySelector(".ai-prediction-panel");
assert.notStrictEqual(panel, null);
assert.strictEqual(panel.style.position, "absolute");
assert.strictEqual(panel.style.top, "12px");
assert.strictEqual(panel.style.right, "12px");
assert.strictEqual(panel.innerHTML.includes("AI完全分类"), true);
assert.strictEqual(panel.innerHTML.includes("中枢震荡，后续演化三类。"), true);
assert.strictEqual(panel.innerHTML.includes("线段向下未完成"), true);
assert.strictEqual(panel.innerHTML.includes("放量站上中枢上沿"), true);
assert.strictEqual(panel.innerHTML.includes("ZG=3310"), true);
assert.strictEqual(panel.innerHTML.includes("三买确认后跟随"), true);
assert.strictEqual(panel.innerHTML.includes("离开中枢不背驰"), true);
assert.strictEqual(panel.innerHTML.includes("向下突破"), true);

createdShapes.length = 0;
window.AIPrediction.draw(manager, [
  {
    name: "旧预测",
    probability: 0.5,
    bis: [
      {
        points: [
          { time: 1767231060, price: 3280 },
          { time: 1767231360, price: 3360 },
        ],
      },
    ],
  },
]);
assert.strictEqual(createdShapes.length, 0);
window.AIPrediction.clear(manager);
assert.strictEqual(chartRoot.querySelector(".ai-prediction-panel"), null);
