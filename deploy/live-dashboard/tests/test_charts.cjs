const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const { test } = require("node:test");
const { runInNewContext } = require("node:vm");

const prepare = runInNewContext(
  readFileSync(join(__dirname, "../static/charts.js"), "utf8") +
    "\nchartSeriesData;",
);
const data = (...args) => JSON.parse(JSON.stringify(prepare(...args)));

test("preserves real zeroes and missing series independently", () => {
  assert.deepEqual(
    data(
      [
        { t: 10, rx: 0, tx: 7 },
        { t: 25, rx: 2, tx: null },
      ],
      ["rx", "tx"],
      0,
      30,
    ),
    [
      [10, 25],
      [0, 2],
      [7, null],
    ],
  );
});

test("an outage breaks every series without inventing a value", () => {
  const result = data(
    [
      { t: 10, rx: 2, tx: 7 },
      { t: 100, rx: 3, tx: 9 },
    ],
    ["rx", "tx"],
    0,
    120,
  );
  assert.equal(result[0].length, 3);
  assert.ok(result[0][1] > 10 && result[0][1] < 100);
  assert.deepEqual(result.slice(1), [
    [2, null, 3],
    [7, null, 9],
  ]);
});

test("ordinary intervals remain connected through the 45 second limit", () => {
  assert.deepEqual(
    data(
      [
        { t: 10, v: 2 },
        { t: 55, v: 3 },
      ],
      ["v"],
      0,
      100,
    ),
    [
      [10, 55],
      [2, 3],
    ],
  );
});

test("range filtering neither extends samples nor mutates the history", () => {
  const rows = [
    { t: 10, v: 1 },
    { t: 20, v: 2 },
    { t: 30, v: 3 },
  ];
  const before = JSON.stringify(rows);
  assert.deepEqual(data(rows, ["v"], 20, 25), [[20], [2]]);
  assert.equal(JSON.stringify(rows), before);
});

test("isolated samples remain available to the processing scatter chart", () => {
  assert.deepEqual(data([{ t: 10, v: 4 }], ["v"], 0, 20), [[10], [4]]);
  assert.deepEqual(data([{ t: 10 }], ["v"], 0, 20), [[10], [null]]);
});

const hitTest = runInNewContext(readFileSync(join(__dirname, "../static/charts.js"), "utf8") + "\nchartPointHits;");
test("point hover ignores empty space and retains every overlapping value", () => {
  const plot = { data: [[10, 10, 11, 50], [20, 20, 21, null], [null, null, 22, 80]], valToPos: v => v };
  assert.equal(hitTest(plot, 30, 40).length, 0);
  assert.equal(hitTest(plot, -1, 20).length, 0);
  const hits = hitTest(plot, 10, 20);
  assert.equal(hits.length, 4);
  assert.equal(hits.filter(p => p.value === 20).length, 2);
  assert.equal(hits.filter(p => p.series === 2).length, 1);
  assert.equal(hitTest(plot, 11, 22)[0].value, 22);
});
