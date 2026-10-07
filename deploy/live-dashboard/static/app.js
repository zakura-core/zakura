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
const COLORS = ["#bcf6b8", "#c1b6ef", "#f1bf75"];
let state = null;
let history = [];
let range = "15m";
let paused = false;
let fetching = false;
let lastHistory = 0;
let disconnected = false;
let selectedBlock = null;
let flowMode = "rate";

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
function renderFlow() {
  const stages = sourceFresh("metrics") ? state.transaction_flow || [] : [];
  const titles = [
    "Queued",
    "Downloaded",
    "Pushed directly",
    "Verified",
    "Advertised",
    "Failed tasks",
    "Oversize rejection",
  ];
  const max = Math.max(1e-9, ...stages.map((s) => s[flowMode] || 0));
  $("transaction-flow").innerHTML = titles
    .map((name, i) => {
      const n = stages.find((s) => s.name === name)?.[flowMode];
      return `<div class="flow-stage${i >= 5 ? " failure" : ""}"><span class="stage-number">${i >= 5 ? "OUTCOME" : `0${i + 1}`}</span><h3>${name}</h3><strong>${flowMode === "rate" ? format("tx_verified_ps", n) : fmt(n)}</strong><div class="flow-track"><span data-width="${valid(n) ? Math.min(100, (n / max) * 100) : 0}"></span></div></div>`;
    })
    .join("");
  applyWidths($("transaction-flow"));
}
function renderPipeline() {
  const now = paused ? state.generated_at : Date.now() / 1000;
  const retained = (kind) =>
    (state.last_processing?.[kind] || []).filter(
      (r) =>
        valid(r.observed_at) &&
        now >= r.observed_at &&
        now - r.observed_at < 86400,
    );
  const observationAge = (row) =>
    valid(row?.observed_at)
      ? `Seen ${age(now - row.observed_at)} ago`
      : "Not observed yet";
  const current = sourceFresh("chain") && sourceFresh("metrics");
  const queues = ["applying", "outstanding", "unsubmitted", "missing"].map(
    value,
  );
  const idle =
    current &&
    queues.every((n) => n === 0) &&
    state.chain.lag === 0 &&
    !state.chain.resource_stalled &&
    !state.chain.body_unavailable;
  const busy = current && queues.some((n) => valid(n) && n > 0);
  set(
    "pipeline-status",
    !current
      ? "Live telemetry unavailable"
      : state.chain.resource_stalled
        ? "Resource pressure"
        : idle
          ? "Between blocks · queues empty"
          : busy
            ? "Processing blocks"
            : "Waiting on chain progress",
  );
  $("pipeline-state").dataset.mode = !current
    ? "unavailable"
    : idle
      ? "idle"
      : "active";
  const tip = state.blocks.find((b) => b.hash === state.chain.hash);
  set(
    "pipeline-tip",
    tip
      ? `Last observed tip ${fmt(tip.height)}${tip.observed_at ? ` · seen ${age(now - tip.observed_at)} ago` : ""}`
      : "Waiting for a tip observation",
  );
  const stages = [
    [
      "01 / Headers",
      "headers",
      "best known height",
      `${fmt(value("missing"))} bodies missing`,
    ],
    [
      "02 / Download",
      "outstanding",
      "outstanding blocks",
      `${bytes(value("download_bps"), true)} native payload`,
    ],
    [
      "03 / Apply",
      "applying",
      "blocks applying",
      `${fmt(value("unsubmitted"))} not yet submitted`,
    ],
    [
      "04 / Verify",
      "blocks_verified_ps",
      "blocks / second",
      `${format("tx_verified_ps", value("tx_verified_ps"))} mempool tx verified /s`,
    ],
    [
      "05 / State",
      "finalized",
      "finalized storage height",
      `${fmt(value("compactions"))} compactions running`,
    ],
  ];
  $("block-pipeline").innerHTML = stages
    .map(
      ([name, key, unit, detail]) =>
        `<div class="pipeline-stage"><h3>${name}</h3><strong>${format(key, value(key))}</strong><small>${unit}</small><p>${esc(detail)}</p></div>`,
    )
    .join("");
  const savedStages = retained("stages");
  const stageNames = [
    ...new Set([
      ...(state.stage_timings || []).map((r) => r.name),
      ...savedStages.map((r) => r.name),
    ]),
  ];
  const timings = stageNames.map(
    (name) => savedStages.find((r) => r.name === name) || { name },
  );
  const max = Math.max(0.001, ...timings.map((t) => t.p95_ms || 0));
  $("stage-timings").innerHTML = timings.length
    ? timings
        .map(
          (t) =>
            `<div class="timing-row" title="${esc(t.name)}: p50 ${ms(t.p50_ms)}, p95 ${ms(t.p95_ms)}. ${valid(t.observed_at) ? `Observed ${esc(clock(t.observed_at, true))} UTC` : "Not observed yet"}"><span class="timing-name">${esc(t.name)}<small>${observationAge(t)}</small></span><div class="timing-track"><span data-width="${Math.min(100, ((t.p95_ms || 0) / max) * 100)}"></span><b data-width="${Math.min(100, ((t.p50_ms || 0) / max) * 100)}"></b></div><span class="timing-value">${ms(t.p95_ms)}</span></div>`,
        )
        .join("")
    : '<p class="empty">Waiting for the first processing sample. Readings stay visible between blocks.</p>';
  applyWidths($("stage-timings"));
  const names = {
    halo2: "Halo 2",
    groth16_sapling: "Sapling / Groth16",
    ed25519: "Ed25519",
    redpallas: "RedPallas",
    redjubjub: "RedJubjub",
  };
  table(
    "verifiers",
    retained("verifiers").map((v) => [
      names[v.name] || v.name,
      ms(v.p50_ms),
      ms(v.p95_ms),
      observationAge(v),
    ]),
    4,
  );
  rows("pipeline-resources", [
    ["Reserved block budget", bytes(value("reserved_bytes"))],
    ["Reorder buffer", bytes(value("reorder_bytes"))],
    ["Attributed pipeline memory", bytes(value("pipeline_memory_bytes"))],
    [
      "Header chunks owned / capacity",
      `${fmt(value("header_budget_used"))} / ${fmt(value("header_budget_capacity"))}`,
    ],
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
  rows("native-stats", [
    ["Active sessions", fmt(value("native_peers"))],
    ["Dials started /s", format("dial_started_ps", value("dial_started_ps"))],
    [
      "Dials succeeded /s",
      format("dial_succeeded_ps", value("dial_succeeded_ps")),
    ],
    ["Dials failed /s", format("dial_failed_ps", value("dial_failed_ps"))],
    [
      "Sessions accepted /s",
      format("native_accepted_ps", value("native_accepted_ps")),
    ],
    [
      "Neutral closes /s",
      format("native_closed_ps", value("native_closed_ps")),
    ],
  ]);
  const peer = sourceFresh("peers") ? state.peer_summary || {} : {};
  rows("legacy-stats", [
    [
      "Ready / unready",
      `${fmt(value("ready_peers"))} / ${fmt(value("legacy_unready"))}`,
    ],
    ["Handshakes in flight", fmt(value("handshakes"))],
    [
      "Handshake failures /s",
      format("legacy_handshake_failed_ps", value("legacy_handshake_failed_ps")),
    ],
    ["RPC inbound / outbound", `${fmt(peer.inbound)} / ${fmt(peer.outbound)}`],
    ["Median RTT", ms(value("peer_p50_ms"))],
    ["p95 RTT", ms(value("peer_p95_ms"))],
  ]);
  table(
    "messages",
    sourceFresh("metrics")
      ? (state.messages || []).map((m) => [
          m.name,
          format("messages_in_ps", m.in_ps),
          format("messages_out_ps", m.out_ps),
        ])
      : [],
    3,
  );
  table(
    "streams",
    sourceFresh("metrics")
      ? (state.streams || []).map((s) => [
          s.name.replaceAll("_", " "),
          format("stream_ps", s.accepted_ps),
          fmt(s.last_depth),
        ])
      : [],
    3,
  );
  rows("interface-stats", [
    [
      "Host interface drops /s",
      format("host_drops_ps", value("host_drops_ps")),
    ],
    [
      "Host interface errors /s",
      format("host_errors_ps", value("host_errors_ps")),
    ],
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
          .sort((a, b) => (b.rps || 0) - (a.rps || 0))
          .slice(0, 12)
          .map((m) => [m.name, format("rpc_rps", m.rps), ms(m.p95_ms)])
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
        .map(
          (b) =>
            `<button class="block-row${b.hash === state.chain.hash ? " latest" : ""}${b.canonical === false ? " orphan" : ""}" data-hash="${esc(b.hash)}" aria-label="View block ${fmt(b.height)}"><span class="row-main"><b>${fmt(b.height)}</b><time>${esc(age(now - b.time))}</time></span><span class="row-sub"><span>${fmt(b.transactions)} tx${b.canonical === false ? " · off chain" : ""}</span><span>${bytes(b.size)}</span></span></button>`,
        )
        .join("")
    : '<p class="empty">Waiting for blocks…</p>';
  if (focusHash)
    $("blocks")
      .querySelector(`[data-hash="${CSS.escape(focusHash)}"]`)
      ?.focus({ preventScroll: true });
  const linked = blocks
    .filter((b) => b.canonical === true)
    .slice(0, 30)
    .reverse();
  const max = Math.max(1, ...linked.map((b) => b.transactions));
  $("block-volume").innerHTML = linked
    .map(
      (b) =>
        `<button class="volume-column" data-hash="${esc(b.hash)}" title="Block ${fmt(b.height)} · ${fmt(b.transactions)} tx · ${bytes(b.size)}" aria-label="Block ${fmt(b.height)}, ${fmt(b.transactions)} transactions"><span data-height="${Math.max(2, (b.transactions / max) * 100)}"></span></button>`,
    )
    .join("");
  $("block-volume")
    .querySelectorAll("[data-height]")
    .forEach((el) => {
      el.style.height = `${el.dataset.height}%`;
    });
}
function renderBlockDetail(hash) {
  const b = state.blocks.find((block) => block.hash === hash);
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
    `<span class="block-state">${b.canonical === true ? "On the observed best chain" : b.canonical === false ? "Off the observed best chain" : "Chain membership not yet checked"}</span><div class="block-hash">${esc(b.hash)}</div><dl class="stat-list detail-stats">${data.map(([k, v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join("")}</dl><p>The observation time is when the dashboard polled this block. It is not its receive time or processing duration.</p>`;
}
const charts = new Map();
function chart(id, keys, format, points = false) {
  const container = $(id);
  if (!container) return;
  if (!charts.has(id))
    charts.set(
      id,
      new HistoryChart(container, keys, format, points, COLORS, clock),
    );
  const end = state?.generated_at || Date.now() / 1000;
  charts.get(id).update(history, end - WINDOWS[range], end);
}

function renderCharts() {
  chart("tps-chart", ["tps", "user_tps"], (v) => format("tps", v));
  chart("proof-chart", ["halo2_ps", "sapling_ps"], (v) =>
    format("halo2_ps", v),
  );
  chart("queue-chart", ["outstanding", "applying"], (v) => fmt(v));
  chart("latency-chart", ["peer_p50_ms"], ms);
  chart("processing-chart", ["contextual_ms", "write_ms"], ms, true);
  chart("tx-chart", ["tx_verified_ps", "tx_failed_ps"], (v) =>
    format("tx_verified_ps", v),
  );
  chart("traffic-chart", ["legacy_in_bps", "legacy_out_bps"], (v) =>
    bytes(v, true),
  );
  chart("host-network-chart", ["host_rx_bps", "host_tx_bps"], (v) =>
    bytes(v, true),
  );
  chart("source-chart", ["zakura_first_ps", "legacy_first_ps"], (v) =>
    format("zakura_first_ps", v),
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
  const data = await getJSON(
    `api/history?window=${encodeURIComponent(requested)}`,
  );
  if (requested === range) {
    history = data.samples;
    lastHistory = Date.now();
    renderCharts();
  }
}
async function poll() {
  if (paused || fetching || document.hidden) return;
  fetching = true;
  try {
    const next = await getJSON("api/overview");
    if (paused) return;
    state = next;
    disconnected = false;
    render();
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
document.querySelectorAll("[data-flow]").forEach((button) =>
  button.addEventListener("click", () => {
    flowMode = button.dataset.flow;
    document.querySelectorAll("[data-flow]").forEach((b) => {
      b.classList.toggle("selected", b === button);
      b.setAttribute("aria-pressed", String(b === button));
    });
    if (state) renderFlow();
  }),
);
document.querySelectorAll("[data-window]").forEach((button) =>
  button.addEventListener("click", async () => {
    range = button.dataset.window;
    document.querySelectorAll("[data-window]").forEach((b) => {
      b.classList.toggle("selected", b === button);
      b.setAttribute("aria-pressed", String(b === button));
    });
    renderCharts();
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
