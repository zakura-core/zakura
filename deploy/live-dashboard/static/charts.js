"use strict";

function chartSeriesData(rows, keys, start, end) {
  const data = Array.from({ length: keys.length + 1 }, () => []);
  let previous = null;
  for (const row of rows) {
    if (row.t < start || row.t > end) continue;
    // A missing scrape interval must break every line, even without a null row.
    if (previous !== null && row.t - previous > 45) {
      data[0].push((previous + row.t) / 2);
      for (const series of data.slice(1)) series.push(null);
    }
    data[0].push(row.t);
    keys.forEach((key, index) =>
      data[index + 1].push(Number.isFinite(row[key]) ? row[key] : null),
    );
    previous = row.t;
  }
  return data;
}

function chartSeriesLabel(key) {
  if (key.endsWith("_p50_ms")) return "p50";
  if (key.endsWith("_p95_ms")) return "p95";
  const labels = {
    duration_ms: "Duration", body_wait_ms: "Queue → body ready", verification_ms: "Verification",
    relay_ms: "Local relay", admission_ms: "Admission checks",
    mined_ms: "Mined", expired_ms: "Expired", evicted_ms: "Evicted",
    execution_ms: "Execution", scheduling_ms: "Scheduling", in_batch_wait_ms: "In-batch wait",
    bundles: "Bundles", actions: "Actions", spends_outputs: "Spends + outputs", signatures: "Signatures", contextual_ms: "Contextual checks", write_ms: "RocksDB write",
    tps: "Total TPS", user_tps: "Excluding coinbase", count_halo2_ps: "Halo 2",
    count_sapling_ps: "Sapling", count_tx_verified_ps: "Verified", count_tx_failed_ps: "Failed tasks",
    native_rx_bps: "Received", native_tx_bps: "Sent", legacy_in_bps: "Received", legacy_out_bps: "Sent",
    host_rx_bps: "Received", host_tx_bps: "Sent", cpu_percent: "CPU busy", iowait_percent: "I/O wait",
    rss_bytes: "Node RSS", outstanding: "Outstanding", applying: "Applying",
  };
  return labels[key] || key.replaceAll("_", " ");
}

function chartPointHits(plot, left, top, radius = 18) {
  if (left < 0 || top < 0) return [];
  const points = [];
  let closest = null, distance = radius * radius;
  for (let i = 0; i < plot.data[0].length; i++) {
    const x = plot.valToPos(plot.data[0][i], "x");
    if (Math.abs(x - left) > radius + 6) continue;
    for (let series = 1; series < plot.data.length; series++) {
      const value = plot.data[series][i];
      if (!Number.isFinite(value)) continue;
      const y = plot.valToPos(value, "y");
      const point = { i, series, value, t: plot.data[0][i], x, y };
      points.push(point);
      const d = (x - left) ** 2 + (y - top) ** 2;
      if (d <= distance) { closest = point; distance = d; }
    }
  }
  return closest ? points.filter(p => (p.x - closest.x) ** 2 + (p.y - closest.y) ** 2 <= 36)
    .sort((a, b) => ((a.x - left) ** 2 + (a.y - top) ** 2) - ((b.x - left) ** 2 + (b.y - top) ** 2)) : [];
}

class HistoryChart {
  constructor(container, keys, format, points, colors, clock) {
    Object.assign(this, { container, keys, format, points, colors, clock });
    this.plot = null;
    this.hoverEntries = [];
    this.pinnedEntries = null;
    this.readout = document.createElement("div");
    this.readout.className = "chart-readout";
    this.readout.setAttribute("aria-label", "Chart inspection");
    const legend = container.previousElementSibling;
    (legend?.classList.contains("chart-legend") ? legend : container).before(this.readout);
    // uPlot consumes bubbling clicks. Capture clicks to pin a stable chart snapshot.
    container.addEventListener("click", (event) => {
      if (!this.plot) return;
      const rect = this.plot.over.getBoundingClientRect();
      const entries = this.entriesAt(this.plot, event.clientX - rect.left, event.clientY - rect.top);
      if (!entries.length) { this.unpin(); return; }
      this.pinnedEntries = entries;
      this.hoverEntries = entries;
      this.renderReadout(entries, true);
      this.plot.setCursor({ left: entries[0].x, top: entries[0].y });
    }, true);
    this.readout.addEventListener("keydown", event => {
      if (event.key === "Escape") this.unpin();
    });
  }

  entriesAt(plot, left, top) {
    if (left < 0 || top < 0 || left > plot.over.clientWidth || top > plot.over.clientHeight) return [];
    if (this.points) return chartPointHits(plot, left, top);
    const i = plot.posToIdx(left);
    return this.keys.flatMap((_key, index) => {
      const value = plot.data[index + 1][i], t = plot.data[0][i];
      return Number.isFinite(value) ? [{ i, series: index + 1, value, t,
        x: plot.valToPos(t, "x"), y: plot.valToPos(value, "y") }] : [];
    });
  }

  unpin() {
    this.pinnedEntries = null;
    this.hoverEntries = [];
    this.readout.replaceChildren();
    this.plot?.setCursor({ left: -10, top: -10 });
    if (this.pendingUpdate) {
      const pending = this.pendingUpdate;
      this.pendingUpdate = null;
      this.update(...pending);
    }
  }

  update(rows, start, end) {
    // Keep the selected points still while the user reads or opens their details.
    if (this.pinnedEntries) { this.pendingUpdate = [rows, start, end]; return; }
    const data = chartSeriesData(rows, this.keys, start, end);
    if (!data.slice(1).some((series) => series.some(Number.isFinite))) {
      this.plot?.destroy();
      this.plot = null;
      this.readout.replaceChildren();
      this.container.innerHTML =
        '<div class="chart-empty">Collecting live samples<br>Charts appear as data arrives</div>';
      return;
    }
    this.window = { min: start, max: end };
    const size = {
      width: Math.max(160, this.container.clientWidth - 30),
      height: Math.max(60, this.container.clientHeight - 10),
    };
    if (!this.plot) {
      this.container.replaceChildren();
      this.plot = new uPlot(this.options(size), data, this.container);
    } else {
      this.plot.batch(() => {
        if (this.plot.width !== size.width || this.plot.height !== size.height)
          this.plot.setSize(size);
        this.plot.setData(data);
        this.plot.setScale("x", this.window);
      });
    }
  }

  options(size) {
    const axis = {
      stroke: "#8096a3",
      font: "10px ui-monospace, monospace",
      ticks: { show: false },
      border: { show: false },
    };
    return {
      ...size,
      padding: [10, 18, 0, 0],
      legend: { show: false },
      select: { show: false },
      cursor: {
        x: true,
        y: false,
        drag: { x: false, y: false, setScale: false },
        points: { size: 7, width: 1, stroke: "#0c1117" },
        dataIdx: (_plot, series, index) => this.pinnedEntries || this.points
          ? ((this.pinnedEntries || this.hoverEntries).find(p => p.series === series)?.i ?? null) : index,
        move: (plot, left, top) => {
          if (this.pinnedEntries) return [this.pinnedEntries[0].x, this.pinnedEntries[0].y];
          if (this.points) {
            this.hoverEntries = chartPointHits(plot, left, top);
            const hit = this.hoverEntries[0];
            return hit ? [hit.x, hit.y] : [-10, -10];
          }
          return left < 0 ? [left, top] : [plot.valToPos(plot.data[0][plot.posToIdx(left)], "x"), top];
        },
      },
      scales: {
        x: { time: true, range: () => [this.window.min, this.window.max] },
        y: { range: (_plot, _min, max) => [0, (max || 1) * 1.12] },
      },
      axes: [
        {
          ...axis,
          size: 22,
          space: 75,
          grid: { show: false },
          values: (_plot, ticks) => ticks.map((t) => this.clock(t)),
        },
        {
          ...axis,
          size: 54,
          space: 40,
          ...(this.keys.every(
            (key) =>
              key.startsWith("count_") ||
              key === "outstanding" ||
              key === "applying",
          )
            ? {
                incrs: Array.from({ length: 10 }, (_, i) =>
                  [1, 2, 5].map((n) => n * 10 ** i),
                ).flat(),
              }
            : {}),
          grid: { stroke: "#2b3842", dash: [2, 4], width: 1 },
          values: (_plot, ticks) => ticks.map((v) => this.format(v)),
        },
      ],
      series: [
        {},
        ...this.keys.map((key, index) => ({
          label: chartSeriesLabel(key),
          stroke: this.colors[index],
          width: 1.7,
          spanGaps: false,
          ...(this.points
            ? { paths: () => null }
            : key.startsWith("count_")
              ? { paths: uPlot.paths.stepped({ align: 1 }) }
              : {}),
          points: {
            show: this.points,
            size: 6,
            width: 0,
            fill: this.colors[index],
          },
        })),
      ],
      hooks: { setCursor: [(plot) => this.showTooltip(plot)] },
    };
  }

  eventTime(t) {
    return new Date(t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3 });
  }

  showObservations() {
    if (!this.points || this.hoverEntries.length < 2) return;
    const dialog = document.createElement("dialog");
    dialog.className = "event-detail-dialog";
    const heading = document.createElement("h2");
    heading.textContent = `${this.hoverEntries.length} overlapping points`;
    const close = document.createElement("button");
    close.textContent = "Close observations";
    close.addEventListener("click", () => dialog.close());
    const table = document.createElement("table");
    const head = table.createTHead().insertRow();
    for (const label of ["Recorded time", "Series", "Value"]) {
      const cell = document.createElement("th"); cell.textContent = label; head.append(cell);
    }
    const body = table.createTBody();
    for (const entry of [...this.hoverEntries].sort((a, b) => a.t - b.t || a.series - b.series || a.value - b.value)) {
      const row = body.insertRow();
      for (const value of [this.eventTime(entry.t), chartSeriesLabel(this.keys[entry.series - 1]), this.format(entry.value)]) row.insertCell().textContent = value;
    }
    dialog.append(heading, close, table);
    dialog.addEventListener("close", () => dialog.remove());
    document.body.append(dialog);
    dialog.showModal();
  }

  showTooltip(plot) {
    if (this.pinnedEntries) return;
    const { idx, left, top } = plot.cursor;
    if (idx == null || left < 0 || top < 0) { this.readout.replaceChildren(); return; }
    const entries = this.points ? this.hoverEntries : this.entriesAt(plot, left, top);
    if (!entries.length) { this.readout.replaceChildren(); return; }
    this.renderReadout(entries, false);
  }

  renderReadout(entries, pinned) {
    this.readout.replaceChildren();
    const main = document.createElement("div");
    main.className = "chart-readout-main";
    const time = document.createElement("div");
    time.className = "chart-readout-time";
    time.textContent = `${pinned ? "Pinned · " : ""}${this.eventTime(entries[0].t)}${this.points && entries.length > 1 ? ` · ${entries.length} points` : ""}`;
    const values = document.createElement("div");
    values.className = "chart-readout-values";
    for (let series = 1; series <= this.keys.length; series++) {
      const group = entries.filter(entry => entry.series === series);
      if (!group.length) continue;
      const row = document.createElement("span");
      row.className = "chart-readout-value";
      const swatch = document.createElement("i");
      swatch.style.backgroundColor = this.colors[series - 1];
      const label = document.createElement("span");
      label.textContent = chartSeriesLabel(this.keys[series - 1]);
      const value = document.createElement("strong");
      const min = Math.min(...group.map(p => p.value)), max = Math.max(...group.map(p => p.value));
      value.textContent = min === max ? this.format(min) : `${this.format(min)} – ${this.format(max)}`;
      row.append(swatch, label, value);
      values.append(row);
    }
    main.append(time, values);
    this.readout.append(main);
    const controls = document.createElement("div");
    controls.className = "chart-readout-controls";
    if (pinned) {
      if (this.points && entries.length > 1) {
        const inspect = document.createElement("button");
        inspect.textContent = `Inspect ${entries.length} points`;
        inspect.addEventListener("click", () => { this.hoverEntries = this.pinnedEntries; this.showObservations(); });
        controls.append(inspect);
      }
      const clear = document.createElement("button");
      clear.textContent = "Unpin";
      clear.addEventListener("click", () => this.unpin());
      controls.append(clear);
    } else {
      const hint = document.createElement("span");
      hint.textContent = this.points ? "Click point to pin" : "Click to pin";
      controls.append(hint);
    }
    this.readout.append(controls);
  }
}
