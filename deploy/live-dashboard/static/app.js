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
      Date.now() / 1000 - source.at < (name === "chain" ? 25 : 120),
  );
}
function freshLabel(id, name) {
  set(id, sourceFresh(name) ? "" : "STALE");
}
function metric(key) {
  return sourceFresh("metrics") ? state.metrics[key] : null;
}

function render() {
  if (!state) return;
  const c = state.chain || {},
    m = state.metrics || {},
    h = state.host || {};
  const now = paused ? state.generated_at : Date.now() / 1000;
  const chainFresh = sourceFresh("chain");
  const peersFresh = sourceFresh("peers");
  const metricsFresh = sourceFresh("metrics");
  const hostFresh = sourceFresh("host");
  const tip = state.blocks.find((b) => b.hash === c.hash);
  set("node-name", state.node);
  set(
    "node-version",
    state.version ? `Zakura ${state.version}` : "Version unavailable",
  );
  set("upgrade", c.upgrade || "Zcash");
  set("height", fmt(c.height));
  set("lag", fmt(c.lag));
  set("headers", fmt(c.headers));
  set(
    "chain-status",
    !chainFresh
      ? "Chain data is stale"
      : c.resource_stalled
        ? "Resource pressure"
        : c.body_unavailable
          ? "Waiting for block body"
          : c.lag
            ? "Catching up"
            : "Following known tip",
  );
  set(
    "tip-age",
    tip ? `Block age ${age(now - tip.time)}` : "Loading recent blocks",
  );
  set("mempool-count", fmt(metric("mempool_count")));
  set("mempool-bytes", bytes(metric("mempool_bytes")));
  set("mempool-queue", `${fmt(metric("mempool_queued"))} queued`);
  set("peer-count", fmt(peersFresh ? state.peer_summary?.total : null));
  set("peer-in", `${fmt(peersFresh ? state.peer_summary?.inbound : null)} in`);
  set(
    "peer-out",
    `${fmt(peersFresh ? state.peer_summary?.outbound : null)} out`,
  );
  freshLabel("chain-fresh", "chain");
  freshLabel("metrics-fresh", "metrics");
  freshLabel("peers-fresh", "peers");
  freshLabel("host-fresh", "host");

  set("pipe-headers", fmt(c.headers));
  set("pipe-missing", fmt(metric("missing")));
  set("pipe-download", fmt(metric("outstanding")));
  set("pipe-payload", bytes(metric("download_bps"), true));
  set("pipe-apply", fmt(metric("applying")));
  set("pipe-waiting", fmt(metric("unsubmitted")));
  set("pipe-write", ms(metric("write_ms")));
  set("pipe-writer", ms(metric("writer_queue_ms")));
  set("pipe-state", bytes(metric("db_bytes")));
  set("pipe-compactions", fmt(metric("compactions")));
  set("finalized", fmt(c.finalized));
  set(
    "pipeline-note",
    !metricsFresh
      ? "Telemetry is stale"
      : c.resource_stalled
        ? "Node reports resource pressure"
        : c.lag === 0 && m.applying === 0 && m.outstanding === 0
          ? "Caught up · waiting for the next block"
          : "Live queue and latency observations",
  );
  set("traffic-in", bytes(metric("legacy_in_bps"), true));
  set("traffic-out", bytes(metric("legacy_out_bps"), true));
  set("halo2-rate", fmt(metric("halo2_ps"), 2));
  set("sapling-rate", fmt(metric("sapling_ps"), 2));
  set("accepted-rate", fmt(metric("tx_accepted_ps"), 2));
  set("rejected-rate", fmt(metric("tx_rejected_ps"), 2));

  const verifierNames = {
    halo2: "Halo 2",
    groth16_sapling: "Sapling · Groth16",
    ed25519: "Ed25519",
    redpallas: "RedPallas",
    redjubjub: "RedJubjub",
  };
  $("verifiers").innerHTML =
    metricsFresh && state.verifiers.length
      ? state.verifiers
          .map(
            (v) =>
              `<tr><td>${esc(verifierNames[v.name] || v.name)}</td><td>${ms(v.p50_ms)}</td><td>${ms(v.p95_ms)}</td></tr>`,
          )
          .join("")
      : '<tr><td colspan="3" class="empty">Verifier metrics unavailable</td></tr>';
  set("native-peers", fmt(metric("native_peers")));
  set("ready-peers", fmt(metric("ready_peers")));
  const agents = peersFresh ? state.peers.slice(0, 4) : [];
  $("peer-agents").innerHTML = agents.length
    ? agents
        .map(
          (p) =>
            `<div class="bar-item"><div class="bar-label"><span title="${esc(p.agent)}">${esc(p.agent.replace(/^\//, "").replace(/\/$/, ""))}</span><b>${fmt(p.count)}</b></div><div class="bar-track"><div class="bar-fill" data-width="${Math.min(100, (p.count / (state.peer_summary?.total || 1)) * 100)}"></div></div></div>`,
        )
        .join("")
    : '<p class="empty">Peer mix unavailable</p>';

  const diskFree = hostFresh ? h.disk_free_bytes : null;
  const freePercent =
    hostFresh && h.disk_total_bytes > 0 && valid(diskFree)
      ? (diskFree / h.disk_total_bytes) * 100
      : null;
  const diskLow = valid(freePercent) && freePercent < 10;
  set("disk-free", bytes(diskFree));
  set(
    "disk-percent",
    valid(freePercent) ? `${fmt(freePercent, 1)}% free` : "Unavailable",
  );
  $("disk-meter").style.width = valid(freePercent)
    ? `${Math.max(0, Math.min(100, 100 - freePercent))}%`
    : "0%";
  document.querySelector(".host-panel").classList.toggle("warning", diskLow);
  set(
    "disk-warning",
    !hostFresh
      ? "Host data unavailable"
      : diskLow
        ? "Less than 10% free on the node’s data volume"
        : "Used capacity on the node’s data volume",
  );
  set("rss", bytes(hostFresh ? h.rss_bytes : null));
  set("memory-free", bytes(hostFresh ? h.mem_available_bytes : null));
  set(
    "host-load",
    hostFresh
      ? [h.load1, h.load5, h.load15].map((v) => fmt(v, 2)).join(" / ")
      : "—",
  );
  set("db-cache", bytes(metric("cache_bytes")));
  set("uptime", hostFresh ? age(h.uptime_seconds) : "—");

  set("rpc-rate", fmt(metric("rpc_rps"), 2));
  set("rpc-errors", `${fmt(metric("rpc_errors_ps"), 2)} errors / sec`);
  const methods = metricsFresh
    ? [...state.rpc_methods]
        .filter((v) => valid(v.p95_ms) && v.p95_ms > 0)
        .sort((a, b) => (b.rps || 0) - (a.rps || 0))
        .slice(0, 5)
    : [];
  $("rpc-methods").innerHTML = methods.length
    ? methods
        .map(
          (v) =>
            `<tr><td>${esc(v.name)}</td><td>${fmt(v.rps, 2)}</td><td>${ms(v.p95_ms)}</td></tr>`,
        )
        .join("")
    : '<tr><td colspan="3" class="empty">RPC metrics unavailable</td></tr>';
  const poolOrder = [
    "transparent",
    "sprout",
    "sapling",
    "orchard",
    "ironwood",
    "lockbox",
  ];
  const pools = [...(c.pools || [])].sort(
    (a, b) => poolOrder.indexOf(a.name) - poolOrder.indexOf(b.name),
  );
  const maxPool = Math.max(1, ...pools.map((p) => p.zec || 0));
  $("pools").innerHTML = pools.length
    ? pools
        .map(
          (p) =>
            `<div class="pool-row"><span class="pool-name">${esc(p.name)}</span><div class="bar-track"><div class="bar-fill" data-width="${Math.min(100, ((p.zec || 0) / maxPool) * 100)}"></div></div><strong>${fmt(p.zec, 0)}</strong></div>`,
        )
        .join("")
    : '<p class="empty">Pool totals unavailable</p>';
  document.querySelectorAll("[data-width]").forEach((el) => {
    el.style.width = `${el.dataset.width}%`;
  });
  set("context-upgrade", c.upgrade || "—");
  set(
    "pruning",
    c.pruned === false ? "Full archive" : c.pruned === true ? "Pruned" : "—",
  );
  const fleet = state.fleet || {};
  set(
    "fleet-status",
    !hostFresh || !fleet.hash
      ? "Unavailable"
      : fleet.hash === c.hash
        ? "Tip matches majority"
        : fleet.height === c.height
          ? "Different tip at same height"
          : `${fmt(Math.abs(c.height - fleet.height))} block${Math.abs(c.height - fleet.height) === 1 ? "" : "s"} ${c.height > fleet.height ? "ahead" : "behind"}`,
  );
  set("support-height", fmt(metric("support_height")));
  set("support-blocks", fmt(metric("support_blocks")));
  $("reorgs").innerHTML = state.reorgs.length
    ? state.reorgs
        .slice(0, 2)
        .map(
          (r) =>
            `<div class="reorg-row"><span>${fmt(r.from_height)} → ${fmt(r.to_height)}</span><span>${esc(valid(r.at) ? age(now - r.at) + " ago" : "Observed")}${valid(r.depth) ? ` · depth ${fmt(r.depth)}` : ""}</span></div>`,
        )
        .join("")
    : '<p class="empty">No recent tip switches reported</p>';

  renderBlocks(now);
  if (selectedBlock && $("block-dialog").open) renderBlockDetail(selectedBlock);
  renderStatus();
  renderSourceDetails();
  renderCharts();
}

function renderStatus() {
  const bad = !sourceFresh("chain") || disconnected;
  $("live-status").classList.toggle("stale", bad || paused);
  $("live-status").innerHTML =
    `<i></i>${paused ? "Paused" : disconnected ? "Reconnecting" : bad ? "Stale data" : "Live"}`;
  const stale = ["chain", "metrics", "peers", "host", "blocks"].filter(
    (key) => !sourceFresh(key),
  );
  const text = paused
    ? "Your view is paused. Collection continues in the background. Resume to see the latest observations."
    : disconnected
      ? "Connection lost. Showing the last received data while the dashboard reconnects."
      : stale.length
        ? `Some data is unavailable or stale: ${stale.join(", ")}. Missing metrics are shown as —.`
        : "";
  $("notice").hidden = !text;
  set("notice", text);
  set(
    "updated",
    state
      ? `${paused ? "Paused at" : "Snapshot"} ${clock(state.generated_at)} · local time`
      : "Waiting for first observation",
  );
}

function renderBlocks(now) {
  const blocks = state.blocks || [];
  if (!blocks.length) {
    $("blocks").innerHTML = '<p class="empty">Waiting for block details…</p>';
    return;
  }
  const focusHash = document.activeElement?.dataset?.hash;
  $("blocks").innerHTML = blocks
    .map(
      (b) =>
        `<button class="block-row${b.hash === state.chain.hash ? " latest" : ""}${b.canonical === false ? " orphan" : ""}" data-hash="${esc(b.hash)}" aria-label="View block ${fmt(b.height)}${b.canonical === false ? ", off the current chain" : ""}"><span class="row-main"><b>${fmt(b.height)}</b><time>${esc(age(now - b.time))}</time></span><span class="row-sub"><span>${fmt(b.transactions)} tx${b.canonical === false ? " · off chain" : ""}</span><span class="tx-ticks" aria-hidden="true">${"▏".repeat(Math.min(12, Math.max(1, b.transactions)))}</span><span>${bytes(b.size)}</span></span></button>`,
    )
    .join("");
  if (focusHash)
    $("blocks")
      .querySelector(`[data-hash="${CSS.escape(focusHash)}"]`)
      ?.focus({ preventScroll: true });
}

function renderBlockDetail(hash) {
  const block = state.blocks.find((b) => b.hash === hash);
  if (!block) return;
  const status =
    block.canonical === true
      ? "On the observed best chain"
      : block.canonical === false
        ? "Off the observed best chain"
        : "Chain membership not yet checked";
  set("block-title", `Block ${fmt(block.height)}`);
  const row = (label, value) =>
    `<div><dt>${esc(label)}</dt><dd>${esc(value)}</dd></div>`;
  $("block-detail").innerHTML =
    `<span class="block-state${block.canonical !== true ? " orphan" : ""}">${status}</span><div class="block-hash">${esc(block.hash)}</div><dl class="stat-list detail-stats">${row("Transactions, including coinbase", fmt(block.transactions))}${row("Serialized size", bytes(block.size))}${row("Miner timestamp · UTC", clock(block.time, true))}${row("First seen by dashboard · UTC", block.observed_at ? clock(block.observed_at, true) : "Backfilled block")}${Object.entries(
      block.trees || {},
    )
      .map(([name, size]) =>
        row(`${name[0].toUpperCase() + name.slice(1)} tree leaves`, fmt(size)),
      )
      .join(
        "",
      )}</dl><p class="block-note">Times are not per-block processing durations. First seen is the dashboard’s polling observation. Shielded tree sizes are public commitment counts.</p>`;
}

function chart(id, keys, format) {
  const container = $(id);
  const end = state?.generated_at || Date.now() / 1000;
  const start = end - WINDOWS[range];
  const rows = history.filter((p) => p.t >= start && p.t <= end);
  const values = rows.flatMap((p) => keys.map((k) => p[k]).filter(valid));
  if (rows.length < 2 || !values.length) {
    container.innerHTML =
      '<div class="chart-empty">Collecting live samples<br>Charts appear as data arrives</div>';
    return;
  }
  // Keep real timestamps and missing samples. Never interpolate across an outage.
  const w = Math.max(160, container.clientWidth - 32),
    height = Math.max(100, container.clientHeight - 15),
    left = 3,
    right = 3,
    top = 19,
    bottom = 26;
  const plotH = height - top - bottom,
    plotW = w - left - right;
  const max = (Math.max(...values) || 1) * 1.12;
  const x = (t) => left + ((t - start) / (end - start)) * plotW;
  const y = (v) => top + plotH * (1 - Math.max(0, v) / max);
  const linePaths = keys
    .map((key, k) => {
      let path = "",
        last = null;
      for (const p of rows) {
        if (!valid(p[key])) {
          last = null;
          continue;
        }
        path += `${last !== null && p.t - last <= 45 ? "L" : "M"}${x(p.t).toFixed(1)},${y(p[key]).toFixed(1)} `;
        last = p.t;
      }
      return `<path d="${path}" fill="none" stroke="${COLORS[k]}" stroke-width="1.7" vector-effect="non-scaling-stroke"/>`;
    })
    .join("");
  container.innerHTML = `<svg viewBox="0 0 ${w} ${height}" preserveAspectRatio="none" aria-hidden="true"><line class="grid-line" x1="0" y1="${top}" x2="${w}" y2="${top}"/><line class="grid-line" x1="0" y1="${top + plotH / 2}" x2="${w}" y2="${top + plotH / 2}"/><line class="grid-line" x1="0" y1="${top + plotH}" x2="${w}" y2="${top + plotH}"/><text x="3" y="11">${esc(format(max))}</text><text x="3" y="${height - 4}">${esc(clock(start))}</text><text x="${w / 2}" y="${height - 4}" text-anchor="middle">${esc(clock((start + end) / 2))}</text><text x="${w - 3}" y="${height - 4}" text-anchor="end">${esc(clock(end))}</text>${linePaths}</svg>`;
  container.onpointermove = (event) => {
    const rect = container.getBoundingClientRect();
    const t =
      start +
      Math.max(
        0,
        Math.min(1, (event.clientX - rect.left - 16) / (rect.width - 32)),
      ) *
        (end - start);
    const p = rows.reduce(
      (best, point) =>
        Math.abs(point.t - t) < Math.abs(best.t - t) ? point : best,
      rows[0],
    );
    let tooltip = container.querySelector(".chart-tooltip");
    if (!tooltip) {
      tooltip = document.createElement("div");
      tooltip.className = "chart-tooltip";
      container.append(tooltip);
    }
    tooltip.textContent = `${clock(p.t)} · ${keys.map((key) => format(p[key])).join(" / ")}`;
  };
  container.onpointerleave = () =>
    container.querySelector(".chart-tooltip")?.remove();
}

function renderCharts() {
  chart("traffic-chart", ["legacy_in_bps", "legacy_out_bps"], (v) =>
    bytes(v, true),
  );
  chart("proof-chart", ["halo2_ps", "sapling_ps"], (v) =>
    valid(v) ? `${fmt(v, 2)} /s` : "—",
  );
  chart("mempool-chart", ["mempool_count"], (v) => `${fmt(v)} tx`);
  const first = history.find((p) => valid(p.t));
  set(
    "history-note",
    first
      ? `Observed since ${clock(first.t)} · local time · 15s samples`
      : "History builds as this dashboard runs",
  );
}

function renderSourceDetails() {
  if (!state) return;
  $("source-details").innerHTML =
    Object.entries(state.sources)
      .map(
        ([name, source]) =>
          `<div>${esc(name)} · ${sourceFresh(name) ? "fresh" : "stale"} · last success ${esc(clock(source.at, true))} UTC</div>`,
      )
      .join("") +
    `<div>Dashboard build · ${esc(state.build.slice(0, 12))}</div>`;
}

async function getJSON(path) {
  const response = await fetch(path, {
    cache: "no-store",
    signal: AbortSignal.timeout(8000),
  });
  if (!response.ok) throw new Error("Data unavailable");
  return response.json();
}
async function loadHistory() {
  const requestedRange = range;
  const data = await getJSON(
    `api/history?window=${encodeURIComponent(requestedRange)}`,
  );
  if (requestedRange === range) {
    history = data.samples;
    lastHistory = Date.now();
    renderCharts();
  }
}
async function poll() {
  if (paused || fetching || document.hidden) return;
  fetching = true;
  try {
    const nextState = await getJSON("api/overview");
    if (paused) return;
    state = nextState;
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
    if (paused) return;
    disconnected = true;
    if (state) render();
    else {
      $("notice").hidden = false;
      set(
        "notice",
        "The node feed is temporarily unavailable. Retrying automatically.",
      );
      $("live-status").innerHTML = "<i></i>Reconnecting";
      $("live-status").classList.add("stale");
    }
  } finally {
    fetching = false;
  }
}

$("pause").addEventListener("click", () => {
  paused = !paused;
  $("pause").setAttribute("aria-pressed", String(paused));
  set("pause", paused ? "Resume" : "Pause");
  renderStatus();
  if (!paused) poll();
});
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
$("blocks").addEventListener("click", (event) => {
  const button = event.target.closest("[data-hash]");
  if (button) {
    selectedBlock = button.dataset.hash;
    renderBlockDetail(selectedBlock);
    $("block-dialog").showModal();
  }
});
const showAbout = () => {
  renderSourceDetails();
  $("about-dialog").showModal();
};
$("about-button").addEventListener("click", showAbout);
$("sources-button").addEventListener("click", showAbout);
document
  .querySelectorAll(".close-dialog")
  .forEach((button) =>
    button.addEventListener("click", () => button.closest("dialog").close()),
  );
document.querySelectorAll("dialog").forEach((dialog) =>
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) {
      const r = dialog.getBoundingClientRect();
      if (
        event.clientX < r.left ||
        event.clientX > r.right ||
        event.clientY < r.top ||
        event.clientY > r.bottom
      )
        dialog.close();
    }
  }),
);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) poll();
});
new ResizeObserver(() => {
  if (state) renderCharts();
}).observe($("traffic-chart"));
poll();
setInterval(poll, 5000);
