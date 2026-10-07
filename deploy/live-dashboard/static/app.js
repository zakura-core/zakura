"use strict";

const $ = (id) => document.getElementById(id);
const valid = (v) => typeof v === "number" && Number.isFinite(v);
const fmt = (v, digits = 0) =>
  valid(v) ? v.toLocaleString("en-US", { maximumFractionDigits: digits }) : "—";
const esc = (v) =>
  String(v ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const set = (id, value) => {
  $(id).textContent = value;
};
const WINDOWS = { "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400 };
// Consensus limit from zakura-chain/src/block/serialize.rs (bytes, not MiB).
const MAX_BLOCK_BYTES = 2_000_000;
const COLORS = ["#bcf6b8", "#c1b6ef", "#f1bf75"];
let state = null;
let history = [];
let cryptoBatches = null;
let range = "15m";
let paused = false;
let fetching = false;
let lastHistory = 0;
let disconnected = false;
let selectedBlock = null;
let blockEvents = null;
let nativeDetails = null;
let period = null;
let periodBlocks = [];
let historyRequest = 0;

function bytes(v, perSecond = false) {
  if (!valid(v)) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let n = Math.max(0, v),
    i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${fmt(n, i ? 1 : 0)} ${units[i]}${perSecond ? "/s" : ""}`;
}
function ms(v) {
  if (!valid(v)) return "—";
  return v >= 1000 ? `${fmt(v / 1000, 2)} s` : `${fmt(v, v < 1 ? 3 : 2)} ms`;
}
function age(seconds) {
  if (!valid(seconds)) return "—";
  if (seconds < -5) return "future timestamp";
  const s = Math.max(0, seconds);
  return s < 60
    ? `${Math.floor(s)}s`
    : s < 3600
      ? `${Math.floor(s / 60)}m`
      : s < 86400
        ? `${Math.floor(s / 3600)}h`
        : `${Math.floor(s / 86400)}d`;
}
function clock(t, full = false) {
  return valid(t)
    ? new Date(t * 1000).toLocaleString(
        "en-US",
        full
          ? {
              timeZone: "UTC",
              month: "short",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
              hour12: false,
            }
          : { hour: "2-digit", minute: "2-digit", hour12: false },
      )
    : "—";
}
function sourceFresh(name) {
  const source = state?.sources?.[name];
  return Boolean(
    source?.fresh &&
      (paused ? state.generated_at : Date.now() / 1000) - source.at <
        (name === "chain" ? 25 : 120),
  );
}
function freshLabel(id, name) {
  set(id, sourceFresh(name) ? "" : "STALE");
}
function metric(key) {
  return sourceFresh("metrics") ? state.metrics[key] : null;
}

const CHAIN_FIELDS = new Set(["height", "lag", "headers", "finalized"]);
const ACTIVITY_FIELDS = new Set([
  "tps",
  "user_tps",
  "block_interval",
  "mean_block_bytes",
]);
const HOST_FIELDS = new Set([
  "cpu_percent",
  "iowait_percent",
  "cpu_cores",
  "rss_bytes",
  "mem_available_bytes",
  "disk_free_bytes",
  "host_rx_bps",
  "host_tx_bps",
  "host_drops_ps",
  "host_errors_ps",
]);
function value(key) {
  if (CHAIN_FIELDS.has(key))
    return sourceFresh("chain") ? state.chain[key] : null;
  if (ACTIVITY_FIELDS.has(key))
    return sourceFresh("chain") && sourceFresh("blocks")
      ? state.chain_activity?.[key]
      : null;
  if (HOST_FIELDS.has(key)) return sourceFresh("host") ? state.host[key] : null;
  if (key === "peer_total")
    return sourceFresh("peers") ? state.peer_summary?.total : null;
  if (key === "peer_p50_ms" || key === "peer_p95_ms")
    return sourceFresh("peers")
      ? state.peer_latency?.[key.replace("peer_", "")]
      : null;
  return metric(key);
}
function format(key, v) {
  if (!valid(v)) return "—";
  if (key.endsWith("_bps")) return bytes(v, true);
  if (key.endsWith("_bytes")) return bytes(v);
  if (key.endsWith("_ms")) return ms(v);
  if (key.endsWith("_percent")) return `${fmt(v, 1)}%`;
  if (key === "block_interval") return `${fmt(v, 1)} s`;
  if (key.endsWith("_ps") || key.endsWith("tps") || key === "rpc_rps")
    return fmt(v, v > 0 && v < 0.01 ? 4 : 3);
  return fmt(v);
}
const rows = (id, data) => {
  $(id).innerHTML = data
    .map(
      ([name, val]) => `<div><dt>${esc(name)}</dt><dd>${esc(val)}</dd></div>`,
    )
    .join("");
};
const cell = (v) => `<td>${esc(v)}</td>`;
function table(id, data, columns) {
  $(id).innerHTML = data.length
    ? data.map((r) => `<tr>${r.map(cell).join("")}</tr>`).join("")
    : `<tr><td colspan="${columns}" class="empty">No current measurements</td></tr>`;
}
function render() {
  if (!state) return;
  const c = state.chain || {},
    h = sourceFresh("host") ? state.host : {};
  document.querySelectorAll("[data-value]").forEach((el) => {
    el.textContent = format(el.dataset.value, value(el.dataset.value));
  });
  set("version", state.version ? `v${state.version}` : "Version unavailable");
  set("chain-badge", sourceFresh("chain") ? "VERIFIED" : "STALE");
  set(
    "chain-status",
    !sourceFresh("chain")
      ? "Chain data stale"
      : c.resource_stalled
        ? "Resource pressure"
        : c.body_unavailable
          ? "Waiting for body"
          : c.lag
            ? "Catching up"
            : "Following known tip",
  );
  const tip = state.blocks.find((b) => b.hash === c.hash);
  const now = paused ? state.generated_at : Date.now() / 1000;
  set("tip-age", tip ? `${age(now - tip.time)} old` : "—");
  const activity = state.chain_activity;
  set(
    "tps-window",
    activity
      ? `${activity.blocks} block intervals · ${age(activity.seconds)}`
      : "Collecting linked blocks",
  );
  document.querySelectorAll("[data-event]").forEach((el) => {
    el.textContent = fmt(eventCount(el.dataset.event));
  });
  document.querySelectorAll("[data-period]").forEach((el) => {
    el.textContent = `Observed in ${range}`;
  });
  document.querySelectorAll("[data-period-short]").forEach((el) => {
    el.textContent = range;
  });
  const coverage = Math.max(
    0,
    ...Object.entries(period?.coverage || {})
      .filter(([key]) => !key.startsWith("host_"))
      .map(([, item]) => item.seconds),
  );
  set(
    "event-coverage",
    period
      ? `Recorded events: up to ${age(coverage)} of ${range}, through ${clock(period.end)}. Missing intervals are excluded.`
      : `Loading observations for ${range}…`,
  );
  renderBlocks(now);
  renderFlow();
  renderPipeline();
  renderNetwork();
  renderSystem(h);
  renderStatus();
  renderCharts();
  if (selectedBlock && $("block-dialog").open) renderBlockDetail(selectedBlock);
  $("source-details").innerHTML =
    Object.entries(state.sources)
      .map(
        ([name, s]) =>
          `<div>${esc(name)} · ${sourceFresh(name) ? "fresh" : "stale"} · ${esc(clock(s.at, true))} UTC</div>`,
      )
      .join("") +
    `<div>Build ${esc(state.build.slice(0, 12))} · ${esc(state.node)}</div>`;
}
function eventCount(key) {
  return period?.totals[key] ?? null;
}
function renderFlow() {
  const stages = [
    ["Queued", "tx_queued_ps"],
    ["Downloaded", "tx_downloaded_ps"],
    ["Pushed directly", "tx_pushed_ps"],
    ["Verified", "tx_verified_ps"],
    ["Advertised", "tx_relayed_ps"],
    ["Failed tasks", "tx_failed_ps"],
    ["Oversize rejection", "tx_policy_rejected_ps"],
  ];
  const max = Math.max(1, ...stages.map(([, key]) => eventCount(key) || 0));
  $("transaction-flow").innerHTML = stages
    .map(([name, key], i) => {
      const n = eventCount(key);
      return `<div class="flow-stage${i >= 5 ? " failure" : ""}"><span class="stage-number">${i >= 5 ? "OUTCOME" : `0${i + 1}`}</span><h3>${name}</h3><strong>${fmt(n)}</strong><div class="flow-track"><span data-width="${valid(n) ? Math.min(100, (n / max) * 100) : 0}"></span></div></div>`;
    })
    .join("");
  applyWidths($("transaction-flow"));
}

function renderPipeline() {
  const end = period?.end || state.generated_at;
  const peak = (key) =>
    history.reduce(
      (max, row) =>
        row.t >= end - WINDOWS[range] && row.t <= end && valid(row[key])
          ? max === null
            ? row[key]
            : Math.max(max, row[key])
          : max,
      null,
    );
  table(
    "sync-buffers",
    [
      ["Reserved block budget", "reserved_bytes", bytes],
      ["Reorder buffer", "reorder_bytes", bytes],
      ["Attributed pipeline memory", "pipeline_memory_bytes", bytes],
      ["Header chunks owned", "header_budget_used", fmt],
    ].map(([name, key, formatValue]) => [
      name,
      formatValue(value(key)),
      formatValue(peak(key)),
    ]),
    3,
  );
  rows("pipeline-resources", [
    ["Header chunk capacity", fmt(value("header_budget_capacity"))],
    [
      "Resource alarm",
      sourceFresh("chain")
        ? state.chain.resource_stalled
          ? "Stalled"
          : "Clear"
        : "—",
    ],
  ]);
}
function renderNetwork() {
  const nativeNow = paused ? state.generated_at : Date.now() / 1000;
  const connections = (nativeDetails?.connections || []).filter(
    (row) =>
      nativeNow - row.received_at <= 15 && nativeNow - row.received_at >= -5,
  );
  $("native-health").hidden =
    !connections.length && !valid(value("native_rx_bps"));
  set(
    "native-health-coverage",
    `${connections.length} recent session observations / ${fmt(value("native_peers"))} active sessions`,
  );
  table(
    "native-peer-details",
    connections
      .slice()
      .sort((a, b) => (b.rtt_ms ?? -1) - (a.rtt_ms ?? -1))
      .slice(0, 10)
      .map((row) => [
        `Session ${row.connection}`,
        ms(row.rtt_ms),
        bytes(row.rx_bytes_ps, true),
        bytes(row.tx_bytes_ps, true),
        fmt(row.lost_packets),
      ]),
    5,
  );
  rows("native-stats", [
    ["Active sessions now", fmt(value("native_peers"))],
    ["Dials started", fmt(eventCount("dial_started_ps"))],
    ["Dials succeeded", fmt(eventCount("dial_succeeded_ps"))],
    ["Dials failed", fmt(eventCount("dial_failed_ps"))],
    ["Sessions accepted", fmt(eventCount("native_accepted_ps"))],
    ["Neutral closes", fmt(eventCount("native_closed_ps"))],
  ]);
  const peer = sourceFresh("peers") ? state.peer_summary || {} : {};
  rows("legacy-stats", [
    [
      "Ready / unready now",
      `${fmt(value("ready_peers"))} / ${fmt(value("legacy_unready"))}`,
    ],
    ["Handshakes in flight", fmt(value("handshakes"))],
    ["Handshake failures", fmt(eventCount("legacy_handshake_failed_ps"))],
    ["RPC inbound / outbound", `${fmt(peer.inbound)} / ${fmt(peer.outbound)}`],
    ["Median RTT", ms(value("peer_p50_ms"))],
    ["p95 RTT", ms(value("peer_p95_ms"))],
  ]);
  const commands = new Set((state.messages || []).map((m) => m.name));
  for (const key of Object.keys(period?.totals || {}))
    if (key.startsWith("message.")) commands.add(key.split(".")[2]);
  table(
    "messages",
    [...commands]
      .sort()
      .map((name) => [
        name,
        fmt(eventCount(`message.in.${name}`)),
        fmt(eventCount(`message.out.${name}`)),
      ]),
    3,
  );
  table(
    "streams",
    (state.streams || []).map((s) => [
      s.name.replaceAll("_", " "),
      fmt(eventCount(`stream.${s.name}`)),
      sourceFresh("metrics") ? fmt(s.last_depth) : "—",
    ]),
    3,
  );
  rows("interface-stats", [
    ["Host interface drops", fmt(eventCount("host_drops"))],
    ["Host interface errors", fmt(eventCount("host_errors"))],
  ]);
  table(
    "peer-table",
    sourceFresh("peers")
      ? (state.peer_details || []).map((p) => [
          p.agent.replace(/^\//, "").replace(/\/$/, ""),
          p.inbound ? "Inbound" : "Outbound",
          fmt(p.version),
          ms(p.ping_ms),
          ms(p.ping_wait_ms),
        ])
      : [],
    5,
  );

  const lat = state.peer_latency || {};
  set(
    "peer-coverage",
    sourceFresh("peers")
      ? `${fmt(lat.measured)} RTT measurements · ${fmt(lat.unknown)} unknown · addresses omitted`
      : "Peer data unavailable",
  );
}
function renderSystem(h) {
  const c = sourceFresh("chain") ? state.chain : {};
  const used =
    h.disk_total_bytes > 0 && valid(h.disk_free_bytes)
      ? 100 * (1 - h.disk_free_bytes / h.disk_total_bytes)
      : null;
  $("disk-used").style.width = valid(used)
    ? `${Math.max(0, Math.min(100, used))}%`
    : "0%";
  rows("storage-stats", [
    ["Volume used", valid(used) ? `${fmt(used, 1)}%` : "—"],
    ["Database on disk", bytes(value("db_bytes"))],
    ["Live database data", bytes(value("db_live_bytes"))],
    ["RocksDB memory", bytes(value("db_memory_bytes"))],
    ["Block cache", bytes(value("cache_bytes"))],
    ["Compaction backlog", bytes(value("compaction_pending_bytes"))],
    ["Running compactions", fmt(value("compactions"))],
  ]);
  rows("host-stats", [
    [
      "Node service",
      sourceFresh("host") ? state.node_service || "Unavailable" : "—",
    ],
    ["Automatic restarts", fmt(h.restart_count)],
    ["Host uptime", age(h.uptime_seconds)],
    [
      "Load · 1 / 5 / 15 min",
      [h.load1, h.load5, h.load15].map((n) => fmt(n, 2)).join(" / "),
    ],
    ["Host memory total", bytes(h.mem_total_bytes)],
    ["Host memory available", bytes(h.mem_available_bytes)],
  ]);
  rows("chain-stats", [
    ["Upgrade", c.upgrade || "—"],
    [
      "Storage mode",
      c.pruned === true ? "Pruned" : c.pruned === false ? "Archive" : "—",
    ],
    ["Pruned below", fmt(c.prune_height)],
    ["Finalized storage height", fmt(c.finalized)],
    ["Support height", fmt(value("support_height"))],
    ["Blocks until support ends", fmt(value("support_blocks"))],
  ]);
  table(
    "rpc-methods",
    sourceFresh("metrics")
      ? [...state.rpc_methods]
          .sort(
            (a, b) =>
              (eventCount(`rpc.${b.name}`) || 0) -
              (eventCount(`rpc.${a.name}`) || 0),
          )
          .slice(0, 12)
          .map((m) => [m.name, fmt(eventCount(`rpc.${m.name}`)), ms(m.p95_ms)])
      : [],
    3,
  );
  const pools = c.pools || [],
    max = Math.max(1, ...pools.map((p) => p.zec || 0));
  $("pools").innerHTML = pools
    .map(
      (p) =>
        `<div class="pool-row"><span>${esc(p.name)}</span><div class="meter"><div data-width="${Math.max(0, Math.min(100, ((p.zec || 0) / max) * 100))}"></div></div><b>${fmt(p.zec)}</b></div>`,
    )
    .join("");
  applyWidths($("pools"));
}
function applyWidths(container) {
  container.querySelectorAll("[data-width]").forEach((el) => {
    el.style.width = `${el.dataset.width}%`;
  });
}
function renderStatus() {
  const stale = ["chain", "metrics", "peers", "host", "blocks"].filter(
    (s) => !sourceFresh(s),
  );
  $("live-status").classList.toggle(
    "stale",
    stale.length > 0 || disconnected || paused,
  );
  $("live-status").innerHTML =
    `<i class="dot"></i>${paused ? "Paused" : disconnected ? "Reconnecting" : stale.length ? "Partial data" : "Live"}`;
  const message = paused
    ? "View paused. Collection continues. Resume for the latest observations."
    : disconnected
      ? "Connection lost. Last received observations are shown while reconnecting."
      : stale.length
        ? `Unavailable or stale: ${stale.join(", ")}. Missing measurements appear as —.`
        : "";
  $("notice").hidden = !message;
  set("notice", message);
  set(
    "updated",
    state
      ? `${paused ? "Paused" : "Snapshot"} ${clock(state.generated_at)} · local time`
      : "Connecting",
  );
}
function renderBlocks(now) {
  const blocks = state.blocks || [];
  const focusHash = document.activeElement?.dataset?.hash;
  $("blocks").innerHTML = blocks.length
    ? blocks
        .map((b) => {
          const fullness = valid(b.size)
            ? Math.max(0, Math.min(100, (b.size / MAX_BLOCK_BYTES) * 100))
            : null;
          const sizeLabel = valid(fullness)
            ? `${bytes(b.size)} · ${fmt(fullness, 2)}% of 2 MB limit`
            : "Block size unavailable";
          return `<button class="block-row${b.hash === state.chain.hash ? " latest" : ""}${b.canonical === false ? " orphan" : ""}" data-hash="${esc(b.hash)}" aria-label="View block ${fmt(b.height)}, ${esc(sizeLabel)}" title="${esc(sizeLabel)}"><span class="block-fill" aria-hidden="true" data-width="${fullness ?? 0}"></span><span class="row-main"><b>${fmt(b.height)}</b><time>${esc(age(now - b.time))}</time></span><span class="row-sub"><span>${fmt(b.transactions)} tx${b.canonical === false ? " · off chain" : ""}</span><span>${bytes(b.size)}</span></span></button>`;
        })
        .join("")
    : '<p class="empty">Waiting for blocks…</p>';
  applyWidths($("blocks"));
  if (focusHash)
    $("blocks")
      .querySelector(`[data-hash="${CSS.escape(focusHash)}"]`)
      ?.focus({ preventScroll: true });
  const linked = periodBlocks.slice().sort((a, b) => a.height - b.height);
  const atEnd =
    $("block-volume").scrollLeft + $("block-volume").clientWidth >=
    $("block-volume").scrollWidth - 4;
  set("period-block-count", fmt(linked.length));
  set(
    "period-block-size",
    linked.length
      ? bytes(linked.reduce((sum, b) => sum + (b.size || 0), 0) / linked.length)
      : "—",
  );

  const max = Math.max(1, ...linked.map((b) => b.transactions));
  $("block-volume").innerHTML =
    linked
      .map(
        (b) =>
          `<button class="volume-column" data-hash="${esc(b.hash)}" title="Block ${fmt(b.height)} · ${fmt(b.transactions)} tx · ${bytes(b.size)} · ${esc(clock(b.time))}" aria-label="Block ${fmt(b.height)}, ${fmt(b.transactions)} transactions"><span data-height="${Math.max(2, (b.transactions / max) * 100)}"></span></button>`,
      )
      .join("") ||
    `<p class="empty">${period ? "No available blocks in this period" : "Loading blocks…"}</p>`;
  if (atEnd) $("block-volume").scrollLeft = $("block-volume").scrollWidth;
  $("block-volume")
    .querySelectorAll("[data-height]")
    .forEach((el) => {
      el.style.height = `${el.dataset.height}%`;
    });
}
async function loadBlockEvents(hash) {
  try {
    const events = await getJSON(`api/block/${encodeURIComponent(hash)}`);
    if (selectedBlock === hash) blockEvents = events;
  } catch {
    if (selectedBlock === hash)
      blockEvents = { error: "Event history unavailable" };
  }
  if (selectedBlock === hash && $("block-dialog").open) renderBlockDetail(hash);
}
function renderBlockEvents() {
  const events = blockEvents;
  if (!events) return "<p>Loading measured block events…</p>";
  if (events.error) return `<p>${esc(events.error)}</p>`;
  if (!events.attempts?.length && !events.arrival?.length && !events.stages?.length)
    return `<p>${
      events.status?.enabled
        ? "No measured events for this block in the retained history. It may predate collection or have missing events."
        : "Per-block event collection is not enabled on this node yet."
    }</p>`;
  const arrival = (events.arrival || [])
    .slice(-1)
    .map((row) => {
      const entries = [];
      if (valid(row.inventory_at))
        entries.push([
          "First recorded inventory · UTC",
          clock(row.inventory_at, true),
        ]);
      if (valid(row.body_at)) {
        entries.push(
          ["First recorded complete body · UTC", clock(row.body_at, true)],
          [
            "Body transport",
            row.body_transport === "zakura" ? "Native" : row.body_transport,
          ],
          ["Duplicate bodies observed", fmt(row.duplicate_bodies)],
          ["Inventory → complete body", ms(row.inventory_to_body_ms)],
          ["Request queue → complete body", ms(row.request_queue_to_body_ms)],
          ["Complete body → committed", ms(row.body_to_commit_ms)],
        );
      }
      if (row.relay_observed)
        entries.push([
          "Relay calls succeeded / failed",
          `${fmt(row.relay_successes)} / ${fmt(row.relay_failures)}`,
        ]);
      return entries.length
        ? `<h3>Arrival and relay · latest observed node run</h3><dl class="stat-list detail-stats">${entries.map(([key, val]) => `<div><dt>${esc(key)}</dt><dd>${esc(val)}</dd></div>`).join("")}</dl><p>Inventory-to-body includes scheduling and fetching. Request timing includes the local send queue and earlier bodies in the same range. It is unavailable for unsolicited bodies or transports without a matching request measurement. Relay success means the local broadcast service completed, not that every peer received it.</p>`
        : "";
    })
    .join("");
  const stages = (events.stages || []).slice(-24);
  const stageRows = stages.length ? `<h3>Measured state stages</h3>
    <table><thead><tr><th>Stage</th><th>Started · UTC</th><th>Duration</th><th>Outcome</th></tr></thead>
    <tbody>${stages.map((row) => `<tr><td>${esc(row.stage.replaceAll("_", " "))}</td><td>${valid(row.started_at) ? clock(row.started_at, true) : "—"}</td><td>${ms(row.duration_ms)}</td><td>${row.complete ? (row.success ? "Succeeded" : "Failed") : "Incomplete"}</td></tr>`).join("")}</tbody></table>
    <p>Each row is one stage occurrence. Contextual validation includes the stages beneath it. Do not add overlapping durations. Showing up to 24 retained occurrences, including retries.</p>` : "";
  return `${arrival}${stageRows}${events.attempts.length ? "<h3>Measured processing attempts</h3>" : ""}${events.attempts
    .slice(-3)
    .map(
      (attempt) =>
        `<dl class="stat-list detail-stats"><div><dt>Outcome</dt><dd>${esc(attempt.result.replaceAll("_", " "))}</dd></div>
    <div><dt>Submission queue</dt><dd>${ms(attempt.queue_ms)}</dd></div>
    <div><dt>Verification + state commit</dt><dd>${ms(attempt.verify_and_commit_ms)}</dd></div></dl>`,
    )
    .join("")}
    <p>Durations join events from the same node run and processing attempt. — means a boundary is missing. Verification + state commit includes both operations, not just disk writing.${events.limited || events.attempts.length > 3 ? " Showing the latest retained attempts." : ""}</p>`;
}
function renderBlockDetail(hash) {
  const b = [...state.blocks, ...periodBlocks].find(
    (block) => block.hash === hash,
  );
  if (!b) return;
  set("block-title", `Block ${fmt(b.height)}`);
  const data = [
    ["Transactions · including coinbase", fmt(b.transactions)],
    ["Serialized size", bytes(b.size)],
    ["Miner timestamp · UTC", clock(b.time, true)],
    [
      "Dashboard observation · UTC",
      b.observed_at ? clock(b.observed_at, true) : "Backfilled block",
    ],
    ...Object.entries(b.trees || {}).map(([name, n]) => [
      `${name} tree leaves`,
      fmt(n),
    ]),
  ];
  $("block-detail").innerHTML =
    `<span class="block-state">${b.canonical === true ? "On the observed best chain" : b.canonical === false ? "Off the observed best chain" : "Chain membership not yet checked"}</span><div class="block-hash">${esc(b.hash)}</div><dl class="stat-list detail-stats">${data.map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join("")}</dl><p>The observation time is when the dashboard polled this block. It is not its receive time or processing duration.</p>${renderBlockEvents()}`;
}
const charts = new Map();
function chart(id, keys, format, points = false, rows = history) {
  const container = $(id);
  if (!container) return;
  if (!charts.has(id))
    charts.set(
      id,
      new HistoryChart(container, keys, format, points, COLORS, clock),
    );
  const end = period?.end || state?.generated_at || Date.now() / 1000;
  charts.get(id).update(rows, end - WINDOWS[range], end);
}

function renderCharts() {
  const stages = [
    "Submit queue",
    "Writer queue",
    "Contextual validation",
    "Initial checks",
    "Transparent spends",
    "Shielded anchors",
    "Parallel state update",
    "RocksDB write",
  ];
  for (const label of stages) {
    const name = label.toLowerCase().replaceAll(" ", "_");
    const id = `stage-${name}-chart`;
    if (!$(id)) {
      const section = document.createElement("section");
      section.innerHTML = `<div class="panel-heading"><h3>${label}</h3></div><div id="${id}" class="chart short" role="img" aria-label="${label} timing history"></div>`;
      $("stage-history").append(section);
    }
    chart(id, [`stage_${name}_p50_ms`, `stage_${name}_p95_ms`], ms, true);
  }
  const verifiers = {
    halo2: "Halo 2",
    groth16_sapling: "Sapling / Groth16",
    ed25519: "Ed25519",
    redpallas: "RedPallas",
    redjubjub: "RedJubjub",
  };
  for (const [name, label] of Object.entries(verifiers)) {
    const keys = [`crypto_${name}_p50_ms`, `crypto_${name}_p95_ms`];
    const id = `crypto-${name}-chart`;
    const measured = (cryptoBatches?.samples || []).filter((row) => row.verifier === name);
    const available = !measured.length && history.some((row) =>
      keys.some((key) => valid(row[key])),
    );
    const measuredId = `batch-${name}`;
    if (measured.length && !$(measuredId)) {
      const section = document.createElement("section");
      section.id = measuredId;
      section.innerHTML = `<div class="panel-heading"><h3>${label} measured batches</h3></div>
        <p id="${measuredId}-summary" class="panel-note"></p>
        <div class="chart-legend"><span><i class="dot"></i>Execution</span><span><i class="dot violet"></i>Scheduling</span><span><i class="dot amber"></i>In-batch wait</span></div>
        <div id="${measuredId}-time" class="chart short" role="img" aria-label="${label} individual batch durations"></div>
        <div class="chart-legend"><span><i class="dot"></i>Items</span><span><i class="dot violet"></i>Work units</span></div>
        <div id="${measuredId}-size" class="chart short" role="img" aria-label="${label} batch sizes"></div>`;
      $("crypto-history").append(section);
    }
    if ($(measuredId)) {
      $(measuredId).hidden = !measured.length;
      if (measured.length) {
        const rows = measured.map((row) => ({ ...row, t: row.at }));
        const failures = measured.filter((row) => !row.success).length;
        const fallback = measured.filter((row) => row.mode === "fallback").length;
        const unit = measured[0].unit.replaceAll("_", " ");
        set(`${measuredId}-summary`, `${fmt(measured.length)} recorded completions · ${fmt(failures)} failed · ${fmt(fallback)} fallback · work units: ${unit}${cryptoBatches.limited ? " · limited history" : ""}`);
        chart(`${measuredId}-time`, ["execution_ms", "scheduling_ms", "in_batch_wait_ms"], ms, true, rows);
        chart(`${measuredId}-size`, ["items", "work_units"], fmt, true, rows);
      }
    }
    if (!$(id) && available) {
      const section = document.createElement("section");
      section.innerHTML = `<div class="panel-heading"><h3>${label}</h3></div><div id="${id}" class="chart short" role="img" aria-label="${label} batch timing history"></div>`;
      $("crypto-history").append(section);
    }
    if ($(id)) {
      $(id).parentElement.hidden = !available;
      chart(id, keys, ms, true);
    }
  }
  $("crypto-empty").hidden = Boolean(cryptoBatches?.samples?.length) || history.some((row) =>
    Object.keys(verifiers).some((name) => valid(row[`crypto_${name}_p95_ms`])),
  );
  chart("tps-chart", ["tps", "user_tps"], (v) => format("tps", v));
  chart("proof-chart", ["count_halo2_ps", "count_sapling_ps"], fmt);
  chart("latency-chart", ["peer_p50_ms"], ms);
  chart("processing-chart", ["contextual_ms", "write_ms"], ms, true);
  chart("tx-chart", ["count_tx_verified_ps", "count_tx_failed_ps"], fmt);
  if (!$("native-health").hidden)
    chart("native-traffic-chart", ["native_rx_bps", "native_tx_bps"], (v) =>
      bytes(v, true),
    );
  chart("traffic-chart", ["legacy_in_bps", "legacy_out_bps"], (v) =>
    bytes(v, true),
  );
  chart("host-network-chart", ["host_rx_bps", "host_tx_bps"], (v) =>
    bytes(v, true),
  );
  chart(
    "source-chart",
    ["count_zakura_first_ps", "count_legacy_first_ps"],
    fmt,
  );
  chart("cpu-chart", ["cpu_percent", "iowait_percent"], (v) =>
    format("cpu_percent", v),
  );
  chart("memory-chart", ["rss_bytes"], bytes);
  const first = history.find((p) => valid(p.t));
  set(
    "history-note",
    first
      ? `Observed since ${clock(first.t)} · 15s samples · chart range ${range}`
      : "New series appear as observations arrive · 15s samples",
  );
}
async function getJSON(path) {
  const r = await fetch(path, {
    cache: "no-store",
    signal: AbortSignal.timeout(8000),
  });
  if (!r.ok) throw new Error("Data unavailable");
  return r.json();
}
async function loadHistory() {
  const requested = range;
  const request = ++historyRequest;
  const end = state?.generated_at || Date.now() / 1000;
  const data = await getJSON(
    `api/history?window=${encodeURIComponent(requested)}&end=${end}`,
  );
  if (request === historyRequest && requested === range) {
    history = data.samples;
    cryptoBatches = data.crypto || null;
    period = data.activity;
    periodBlocks = data.blocks;
    lastHistory = Date.now();
    if (state) render();
  }
}
async function poll() {
  if (paused || fetching || document.hidden) return;
  fetching = true;
  try {
    const next = await getJSON("api/overview");
    if (paused) return;
    let nextNative = null;
    try {
      nextNative = await getJSON("api/native");
    } catch {}
    if (paused) return;
    state = next;
    nativeDetails = nextNative;
    disconnected = false;
    render();
    if (selectedBlock && $("block-dialog").open)
      await loadBlockEvents(selectedBlock);
    if (Date.now() - lastHistory > 15000) {
      try {
        await loadHistory();
      } catch {
        set("history-note", "History temporarily unavailable");
      }
    }
  } catch {
    if (!paused) {
      disconnected = true;
      if (state) render();
      else {
        $("notice").hidden = false;
        set("notice", "Connecting to the node feed. Retrying automatically.");
      }
    }
  } finally {
    fetching = false;
  }
}
$("pause").addEventListener("click", () => {
  paused = !paused;
  $("pause").setAttribute("aria-pressed", String(paused));
  set("pause", paused ? "Resume" : "Pause");
  if (state) render();
  if (!paused) poll();
});
document.querySelectorAll("[data-window]").forEach((button) =>
  button.addEventListener("click", async () => {
    range = button.dataset.window;
    document.querySelectorAll("[data-window]").forEach((b) => {
      b.classList.toggle("selected", b === button);
      b.setAttribute("aria-pressed", String(b === button));
    });
    history = [];
    period = null;
    periodBlocks = [];
    if (state) render();
    try {
      await loadHistory();
    } catch {
      set("history-note", "History temporarily unavailable");
    }
  }),
);
for (const id of ["blocks", "block-volume"])
  $(id).addEventListener("click", (event) => {
    const b = event.target.closest("[data-hash]");
    if (b) {
      selectedBlock = b.dataset.hash;
      blockEvents = null;
      loadBlockEvents(selectedBlock);
      renderBlockDetail(selectedBlock);
      $("block-dialog").showModal();
    }
  });
for (const id of ["about", "sources-button"])
  $(id).addEventListener("click", () => $("about-dialog").showModal());
document
  .querySelectorAll(".close-dialog")
  .forEach((button) =>
    button.addEventListener("click", () => button.closest("dialog").close()),
  );
window.addEventListener("resize", renderCharts);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) poll();
});
setInterval(poll, 5000);
poll();
