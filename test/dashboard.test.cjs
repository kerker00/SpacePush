const test = require("node:test");
const assert = require("node:assert/strict");
const {requests, polling} = require("../priv/stats/dashboard.js");

test("request totals count endpoint requests once, including widget/other subsets", () => {
  assert.equal(requests({"requests.directory": 100, "requests.space": 40,
    "requests.register": 5, "requests.widget": 60, "requests.other": 20,
    "rate_limited.read": 3, "date": "2026-10-28"}), 145);
  assert.equal(requests({date: "2026-10-28"}), 0);
});

test("polling uses weighted durations and the maximum instead of adding daily statistics", () => {
  assert.deepEqual(polling([
    {"poll.rounds": 1, "poll.ms_sum": 100, "poll.ms_avg": 100, "poll.ms_max": 100,
      "spaces.fetch_ok": 5, "mainframe.fetch_ok": 2, "changes.total": 3},
    {"poll.rounds": 9, "poll.ms_sum": 4500, "poll.ms_avg": 500, "poll.ms_max": 800,
      "spaces.fetch_failed": 2, "mainframe.fetch_failed": 1, "stats.dropped": 4}
  ]), {rounds: 10, average: 460, maximum: 800, fetched: 7, failed: 3, changes: 3, dropped: 4});
  assert.equal(polling([]).average, null);
  assert.equal(polling([]).maximum, 0);
});
