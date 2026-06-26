#!/usr/bin/env python3
"""Zebra sync observability dashboard + bottleneck classifier.

Three roles, one zero-dependency stdlib script:

  1. Live sidecar   — scrape a running node's /metrics, serve live charts.
  2. Recorder       — scrape + persist a per-run time series to disk (jsonl).
  3. Archive server — serve live AND replay past recorded runs (always-on).
  4. Classifier     — read a recorded run and emit a 3-way bottleneck verdict
                      (commit / download / verify) as markdown + JSON.

The classifier is the CI outcome: it decides whether a sync run was
commit-starved (the writer is the limiter), download-bound (block supply is
the limiter), or verify-bound (block verification is the limiter), from where
the backlog sits in the download -> verify -> commit pipeline.

Usage:
  # live dashboard (auto-detect the running node's metrics port)
  python3 zebra-metrics-dashboard.py
  python3 zebra-metrics-dashboard.py --target 127.0.0.1:19980

  # always-on archive server (live + every recorded run under DIR)
  python3 zebra-metrics-dashboard.py --archive /opt/zebra-bench/dashboard/runs

  # headless recorder (CI sidecar): scrape a node, write samples, no web server
  python3 zebra-metrics-dashboard.py --no-serve --record DIR --target 127.0.0.1:19999 \
      --label v5.0.0 --ckpt-limit 1500 --dl-limit 150

  # classify a recorded run -> markdown on stdout, JSON to --verdict-out
  python3 zebra-metrics-dashboard.py --classify DIR/samples.jsonl \
      --verdict-out verdict.json --label v5.0.0

Only the Python stdlib is used; Chart.js is loaded from a CDN by your browser.
"""
import argparse, json, re, os, glob, threading, time, urllib.request, urllib.parse
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)\s*$')

def parse(text):
    out = {}
    for ln in text.splitlines():
        if not ln or ln[0] == '#':
            continue
        m = LINE.match(ln)
        if not m:
            continue
        name, labels, val = m.group(1), m.group(2) or '', m.group(3)
        try:
            out.setdefault(name, []).append((labels, float(val)))
        except ValueError:
            pass
    return out

def bare(m, name):
    for lbl, v in m.get(name, []):
        if lbl == '':
            return v
    vs = m.get(name, [])
    return vs[0][1] if vs else None

def total(m, name):
    return sum(v for _, v in m.get(name, [])) if name in m else None

def quantile(m, name, q):
    needle = f'quantile="{q}"'
    for lbl, v in m.get(name, []):
        if needle in lbl:
            return v
    return None

# group, key, label, unit, kind
PANELS = [
    ("Throughput", "blocks_per_s",   "Blocks / sec",               "blk/s", "rate"),
    ("Throughput", "height",         "Finalized height",           "",      "gauge"),
    ("Commit",     "committer_util", "Committer utilization",       "%",     "gauge"),
    ("Commit",     "committer_ms",   "Committer ms / block",        "ms",    "gauge"),
    ("Commit",     "queue_depth",    "Committer queue depth",       "",      "gauge"),
    ("Commit",     "p_note_tree",    "  note_tree",                 "ms",    "gauge"),
    ("Commit",     "p_write_block",  "  write_block",               "ms",    "gauge"),
    ("Commit",     "p_batch_commit", "  rocksdb batch_commit",      "ms",    "gauge"),
    ("Commit",     "p_install_ovh",  "  install overhead",          "ms",    "gauge"),
    ("Verify",     "verify_queued",  "Checkpoint queued slots",     "",      "gauge"),
    ("Verify",     "verify_mem_q",   "Semantic queued blocks",      "",      "gauge"),
    ("Verify",     "verify_util",    "Verify queue utilization",    "%",     "gauge"),
    ("Verify",     "verify_height",  "Verified height (checkpoint)","",      "gauge"),
    ("Verify",     "verified_per_s", "Verified blocks / s",         "/s",    "rate"),
    ("Download",   "dl_p50_ms",      "Block download p50",          "ms",    "gauge"),
    ("Download",   "dl_p90_ms",      "Block download p90",          "ms",    "gauge"),
    ("Download",   "dl_util",        "Download slot utilization",   "%",     "gauge"),
    ("Download",   "in_flight",      "Downloads in flight (legacy)","",      "gauge"),
    ("Download",   "outstanding",    "Blocks outstanding (Zakura)", "",      "gauge"),
    ("Download",   "missing_bodies", "Header bodies missing",       "",      "gauge"),
    ("VCT path",   "vct_fast_s",     "VCT fast commits / s",        "/s",    "rate"),
    ("VCT path",   "vct_legacy_s",   "VCT legacy commits / s",      "/s",    "rate"),
    ("Network",    "net_in_mbps",    "Network in",                  "MB/s",  "rate"),
    ("Network",    "net_out_mbps",   "Network out",                 "MB/s",  "rate"),
    ("Network",    "peers",          "Legacy peers",                "",      "gauge"),
    ("Network",    "zakura_peers",   "Zakura peers (active)",       "",      "gauge"),
]
PANEL_KEYS = [k for _, k, *_ in PANELS]

def panels_meta():
    return [{"group": g, "key": k, "label": l, "unit": u} for g, k, l, u, _ in PANELS]

# ── bottleneck classifier ─────────────────────────────────────────────────────
# A sync run is limited by exactly one pipeline stage: download -> verify -> commit.
# The limiter is the stage running at full utilization while work backs up at its
# input and starves everything downstream. We read three utilization signals,
# checked downstream-first so a queue that is full only because the stage *after*
# it is slow is attributed to that downstream stage, not to itself.
STALL_BPS      = 1.0    # below this, the node is making no commit progress
COMMIT_UTIL_HI = 80.0   # committer busy >= this fraction of wall time -> commit-bound
VERIFY_FRAC_HI = 0.50   # verify input queue >= this fraction of the verify limit
DL_FRAC_HI     = 0.80   # download slots >= this fraction of the concurrency limit

def _median(xs):
    xs = sorted(v for v in xs if v is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0

def steady_window(samples):
    """Drop cold-start: keep samples from the first one that committed a block
    onward, then trim the leading 10% as warm-up. Returns the steady slice."""
    started = [i for i, s in enumerate(samples)
               if (s.get("blocks_per_s") or 0) >= STALL_BPS]
    if not started:
        return samples
    lo = started[0]
    tail = samples[lo:]
    trim = len(tail) // 10
    return tail[trim:] if len(tail) - trim >= 3 else tail

def classify(samples, ckpt_limit=None, dl_limit=None):
    """Return {verdict, label, confidence, scores, detail, bps} over the steady
    window. scores are per-stage utilizations in [0,1]; the verdict names the
    limiting stage. Robust to missing metrics (legacy vs Zakura paths)."""
    win = steady_window(samples)
    if not win:
        return {"verdict": "idle", "label": "No data", "confidence": "low",
                "scores": {}, "detail": "no samples recorded", "bps": None}

    def med(k):
        return _median([s.get(k) for s in win])

    bps  = med("blocks_per_s")
    cutil = med("committer_util")                 # % busy
    vq   = med("verify_queued")                   # checkpoint verifier backlog
    vmq  = med("verify_mem_q")                    # semantic verifier backlog
    infl = med("in_flight")                       # legacy download slots
    outs = med("outstanding")                     # Zakura outstanding blocks

    # per-stage utilization in [0,1]
    commit_u = (cutil / 100.0) if cutil is not None else None
    verify_back = max([x for x in (vq, vmq) if x is not None], default=None)
    verify_u = (min(1.0, verify_back / ckpt_limit)
                if (verify_back is not None and ckpt_limit) else None)
    dl_inflight = infl if infl is not None else outs
    dl_u = (min(1.0, dl_inflight / dl_limit)
            if (dl_inflight is not None and dl_limit) else None)

    scores = {}
    if commit_u is not None: scores["commit"] = round(commit_u, 3)
    if verify_u is not None: scores["verify"] = round(verify_u, 3)
    if dl_u     is not None: scores["download"] = round(dl_u, 3)

    # No (or near-zero) finalized throughput. The finalized-height gauge is only emitted
    # once the committer commits a block, so a node that downloads/verifies but never
    # finalizes reports bps=None — that is a STALL, not idle. Distinguish a genuinely
    # quiet node from one that is clearly working, and attribute the stall to the stage
    # that is saturated (downloads piling up = head-of-line; verifier backlog; else commit).
    active = any(med(k) is not None for k in
                 ("in_flight", "outstanding", "verify_queued", "verify_mem_q"))
    vps = med("verified_per_s")
    if bps is None or bps < STALL_BPS:
        if not active and bps is None:
            return {"verdict": "idle", "label": "No data", "confidence": "low",
                    "scores": scores, "detail": "node not reporting activity", "bps": bps}
        prog = f"{bps:.2f}" if bps is not None else "0"
        vnote = (f"; checkpoint verification still advancing (+{vps:.0f} blk/s) — finalization is the wall"
                 if (vps and vps > 0) else "")
        if dl_u is not None and dl_u >= DL_FRAC_HI:
            return {"verdict": "stalled", "label": "STALLED — download head-of-line",
                    "confidence": "high", "scores": scores, "bps": bps,
                    "detail": (f"finalized output stalled ({prog} blk/s) while downloads pile up "
                               f"({dl_inflight:.0f} in flight vs limit {dl_limit:g}){vnote}")}
        if verify_u is not None and verify_u >= VERIFY_FRAC_HI:
            return {"verdict": "stalled", "label": "STALLED — verify backlog",
                    "confidence": "high", "scores": scores, "bps": bps,
                    "detail": (f"finalized output stalled ({prog} blk/s) with verifier backlog "
                               f"{verify_back:.0f} ({verify_u*100:.0f}% of limit {ckpt_limit:g}){vnote}")}
        return {"verdict": "stalled", "label": "STALLED — not finalizing",
                "confidence": "medium", "scores": scores, "bps": bps,
                "detail": f"finalized output stalled ({prog} blk/s); no stage clearly saturated{vnote}"}

    # downstream-first decision tree
    if commit_u is not None and cutil >= COMMIT_UTIL_HI:
        phases = {k: med(k) for k in ("p_note_tree", "p_write_block",
                  "p_batch_commit", "p_install_ovh")}
        phases = {k: v for k, v in phases.items() if v}
        top = max(phases, key=phases.get) if phases else None
        detail = f"writer saturated ({cutil:.0f}% util, {bps:.0f} blk/s)"
        if top:
            detail += f"; dominant phase: {top} ({phases[top]:.1f} ms)"
        return {"verdict": "commit_starved", "label": "COMMIT-STARVED",
                "confidence": _conf(scores, "commit"), "scores": scores,
                "detail": detail, "bps": round(bps, 2)}

    if verify_u is not None and verify_u >= VERIFY_FRAC_HI:
        return {"verdict": "verify_bound", "label": "VERIFY-BOUND",
                "confidence": _conf(scores, "verify"), "scores": scores,
                "detail": (f"verifier backlog {verify_back:.0f} "
                           f"({verify_u*100:.0f}% of limit {ckpt_limit}), "
                           f"committer {cutil:.0f}% util" if cutil is not None
                           else f"verifier backlog {verify_back:.0f}"),
                "bps": round(bps, 2)}

    if dl_u is not None and dl_u >= DL_FRAC_HI:
        return {"verdict": "download_bound", "label": "DOWNLOAD-BOUND",
                "confidence": _conf(scores, "download"), "scores": scores,
                "detail": (f"download slots {dl_inflight:.0f}/{dl_limit} "
                           f"({dl_u*100:.0f}%) saturated, downstream queues idle"),
                "bps": round(bps, 2)}

    lean = max(scores, key=scores.get) if scores else "?"
    return {"verdict": "mixed", "label": "MIXED / UNSATURATED",
            "confidence": "low", "scores": scores, "bps": round(bps, 2),
            "detail": (f"no stage saturated at {bps:.0f} blk/s "
                       f"(leaning {lean}); likely coordination / head-of-line")}

def _conf(scores, winner):
    """high if the winning stage clearly leads the runner-up, else medium."""
    if winner not in scores:
        return "low"
    others = [v for k, v in scores.items() if k != winner]
    if not others:
        return "medium"
    return "high" if scores[winner] - max(others) >= 0.25 else "medium"

# verdict level -> live-banner CSS class
LEVEL = {"commit_starved": "warn", "verify_bound": "warn", "download_bound": "info",
         "mixed": "info", "stalled": "bad", "idle": "idle"}

def render_markdown(v, label=None):
    title = f" — {label}" if label else ""
    lines = [f"## Bottleneck verdict{title}", "",
             f"**{v['label']}** (confidence {v['confidence']})", "",
             v.get("detail", ""), "",
             "| stage | utilization |", "|-------|------------:|"]
    names = {"commit": "commit (writer)", "verify": "verify (checkpoint)",
             "download": "download (peers)"}
    win = {"commit_starved": "commit", "verify_bound": "verify",
           "download_bound": "download"}.get(v["verdict"])
    for k in ("download", "verify", "commit"):
        if k in v.get("scores", {}):
            mark = " ⟵ limiter" if k == win else ""
            lines.append(f"| {names[k]} | {v['scores'][k]*100:.0f}%{mark} |")
    if v.get("bps") is not None:
        lines += ["", f"Throughput: **{v['bps']:.1f} blk/s** (steady-state median)."]
    return "\n".join(lines) + "\n"

# ── live / recording collector ────────────────────────────────────────────────
def autodetect_target():
    cands = []
    for pid in glob.glob('/proc/[0-9]*'):
        try:
            exe = os.readlink(pid + '/exe')
        except OSError:
            continue
        if 'zebrad' not in exe:
            continue
        try:
            cmd = open(pid + '/cmdline', 'rb').read().split(b'\0')
        except OSError:
            continue
        for i, a in enumerate(cmd):
            if a == b'-c' and i + 1 < len(cmd):
                try:
                    for ln in open(cmd[i + 1].decode()):
                        mo = re.search(r'endpoint_addr\s*=\s*"[^:]*:(\d+)"', ln)
                        if mo:
                            cands.append(int(mo.group(1)))
                except OSError:
                    pass
    cands += list(range(19980, 20000))
    for port in dict.fromkeys(cands):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=1) as r:
                head = r.read(8000)
            if b'state_' in head or b'zebra' in head:
                return f"127.0.0.1:{port}"
        except Exception:
            continue
    return None

class Collector:
    def __init__(self, target, interval, window, record_dir=None, meta=None):
        self.target = target
        self.interval = interval
        self.series = {k: deque(maxlen=window) for k in PANEL_KEYS}
        self.ts = deque(maxlen=window)
        self.prev = None
        self.prev_t = None
        self.lock = threading.Lock()
        self.status = "starting"
        self.verdict = {"text": "—", "level": "idle"}
        self.ckpt_limit = (meta or {}).get("ckpt_limit")
        self.dl_limit = (meta or {}).get("dl_limit")
        self.samples = deque(maxlen=window)   # raw dicts for the classifier
        self.record_dir = record_dir
        self._rec = None
        if record_dir:
            os.makedirs(record_dir, exist_ok=True)
            with open(os.path.join(record_dir, "meta.json"), "w") as f:
                json.dump({"target": target, "interval": interval,
                           "start": int(time.time()), **(meta or {})}, f)
            self._rec = open(os.path.join(record_dir, "samples.jsonl"), "a", buffering=1)

    def loop(self):
        while True:
            try:
                self._tick()
            except Exception as e:
                self.status = f"scrape error: {e}"
            time.sleep(self.interval)

    def _tick(self):
        with urllib.request.urlopen(f"http://{self.target}/metrics", timeout=4) as r:
            m = parse(r.read().decode('utf-8', 'replace'))
        now = time.time()
        d = {}
        d["height"]         = bare(m, "state_finalized_block_height")
        d["queue_depth"]    = bare(m, "zebra_committer_input_queue_depth")
        d["verify_queued"]  = bare(m, "checkpoint_queued_slots")
        d["verify_mem_q"]   = bare(m, "state_memory_queued_block_count")
        d["verify_height"]  = bare(m, "checkpoint_verified_height")
        d["in_flight"]      = bare(m, "sync_downloads_in_flight")
        d["outstanding"]    = bare(m, "sync_block_outstanding")
        d["missing_bodies"] = bare(m, "sync_header_missing_bodies")
        d["peers"]          = bare(m, "zcash_net_peers")
        d["zakura_peers"]   = bare(m, "zakura_p2p_conn_active")
        dl50 = quantile(m, "sync_block_download_duration_seconds", "0.5")
        dl90 = quantile(m, "sync_block_download_duration_seconds", "0.9")
        d["dl_p50_ms"] = dl50 * 1000 if dl50 is not None else None
        d["dl_p90_ms"] = dl90 * 1000 if dl90 is not None else None
        if self.ckpt_limit and d["verify_queued"] is not None:
            vb = max(d["verify_queued"], d["verify_mem_q"] or 0)
            d["verify_util"] = min(100.0, 100.0 * vb / self.ckpt_limit)
        if self.dl_limit:
            di = d["in_flight"] if d["in_flight"] is not None else d["outstanding"]
            if di is not None:
                d["dl_util"] = min(100.0, 100.0 * di / self.dl_limit)

        cur = {
            "h":   bare(m, "state_finalized_block_height"),
            "vf":  bare(m, "state_vct_fast_block_count"),
            "vl":  bare(m, "state_vct_legacy_block_count"),
            "vh":  bare(m, "checkpoint_verified_height"),
            "net_in": total(m, "zcash_net_in_bytes_total"),
            "net_out": total(m, "zcash_net_out_bytes_total"),
            "cm_s":bare(m, "zebra_committer_commit_duration_seconds_sum"),
            "cm_c":bare(m, "zebra_committer_commit_duration_seconds_count"),
            "nt_s":bare(m, "zebra_state_write_update_trees_duration_seconds_sum"),
            "nt_c":bare(m, "zebra_state_write_update_trees_duration_seconds_count"),
            "wb_s":bare(m, "zebra_state_write_write_block_total_duration_seconds_sum"),
            "wb_c":bare(m, "zebra_state_write_write_block_total_duration_seconds_count"),
            "bc_s":bare(m, "zebra_state_rocksdb_batch_commit_duration_seconds_sum"),
            "bc_c":bare(m, "zebra_state_rocksdb_batch_commit_duration_seconds_count"),
            "cc_s":bare(m, "zebra_state_write_commitment_check_duration_seconds_sum"),
            "cc_c":bare(m, "zebra_state_write_commitment_check_duration_seconds_count"),
            "wi_s":bare(m, "zebra_state_commit_write_block_install_duration_seconds_sum"),
            "wi_c":bare(m, "zebra_state_commit_write_block_install_duration_seconds_count"),
        }
        def rate(a, b, dt):
            return (b - a) / dt if (a is not None and b is not None and dt > 0) else None
        def avg(s0, s1, c0, c1, scale=1000.0):
            if None in (s0, s1, c0, c1) or (c1 - c0) <= 0:
                return None
            return scale * (s1 - s0) / (c1 - c0)
        if self.prev is not None:
            dt = now - self.prev_t
            p = self.prev
            d["blocks_per_s"]  = rate(p["h"],  cur["h"],  dt)
            d["verified_per_s"]= rate(p["vh"], cur["vh"], dt)
            d["vct_fast_s"]    = rate(p["vf"], cur["vf"], dt)
            d["vct_legacy_s"]  = rate(p["vl"], cur["vl"], dt)
            ni = rate(p["net_in"], cur["net_in"], dt);  d["net_in_mbps"]  = ni/1e6 if ni is not None else None
            no = rate(p["net_out"], cur["net_out"], dt);  d["net_out_mbps"] = no/1e6 if no is not None else None
            d["p_note_tree"]    = avg(p["nt_s"], cur["nt_s"], p["nt_c"], cur["nt_c"])
            d["p_write_block"]  = avg(p["wb_s"], cur["wb_s"], p["wb_c"], cur["wb_c"])
            d["p_batch_commit"] = avg(p["bc_s"], cur["bc_s"], p["bc_c"], cur["bc_c"])
            p_cc                = avg(p["cc_s"], cur["cc_s"], p["cc_c"], cur["cc_c"])
            wi = avg(p["wi_s"], cur["wi_s"], p["wi_c"], cur["wi_c"])
            d["p_install_ovh"] = (wi - d["p_write_block"]) if (wi is not None and d.get("p_write_block") is not None) else None
            # committer busy ms/block — fall back across what the build exports:
            # full committer loop (perf branch) -> write_block_total (perf branch) ->
            # sum of the dominant serial commit phases (ironwood-main, commit-metrics feature).
            ct  = avg(p["cm_s"], cur["cm_s"], p["cm_c"], cur["cm_c"])
            wbt = d["p_write_block"]
            if ct is not None:
                d["committer_ms"] = ct
            elif wbt is not None:
                d["committer_ms"] = wbt + (d["p_note_tree"] or 0.0)
            else:
                parts = [x for x in (d["p_note_tree"], d["p_batch_commit"], p_cc) if x is not None]
                d["committer_ms"] = sum(parts) if parts else None
            if d["committer_ms"] is not None and d.get("blocks_per_s") is not None:
                d["committer_util"] = min(100.0, max(0.0, d["committer_ms"] * d["blocks_per_s"] / 10.0))
            else:
                d["committer_util"] = None
        self.prev, self.prev_t = cur, now
        with self.lock:
            self.ts.append(int(now * 1000))
            for k in PANEL_KEYS:
                self.series[k].append(d.get(k))
            self.samples.append(d)
            self.verdict = self._verdict()
        if self._rec is not None:
            row = {"t": int(now * 1000)}
            row.update({k: d.get(k) for k in PANEL_KEYS})
            self._rec.write(json.dumps(row) + "\n")
        h = d.get('height')
        self.status = f"ok {time.strftime('%H:%M:%S')} | {self.target} | height {int(h) if h else '—'}"

    def _verdict(self):
        v = classify(list(self.samples), self.ckpt_limit, self.dl_limit)
        return {"text": f"{v['label']} — {v.get('detail','')}", "level": LEVEL.get(v["verdict"], "idle")}

    def snapshot(self):
        with self.lock:
            return {
                "t": list(self.ts),
                "series": {k: list(self.series[k]) for k in PANEL_KEYS},
                "panels": panels_meta(),
                "status": self.status, "verdict": self.verdict, "target": self.target,
            }

# ── recorded-run archive (replay) ─────────────────────────────────────────────
def load_run(run_dir):
    """Reconstruct a snapshot-shaped dict from a recorded run directory."""
    meta = {}
    mp = os.path.join(run_dir, "meta.json")
    if os.path.exists(mp):
        try:
            meta = json.load(open(mp))
        except Exception:
            pass
    ts, series, samples = [], {k: [] for k in PANEL_KEYS}, []
    sp = os.path.join(run_dir, "samples.jsonl")
    if os.path.exists(sp):
        for ln in open(sp):
            ln = ln.strip()
            if not ln:
                continue
            try:
                row = json.loads(ln)
            except ValueError:
                continue
            ts.append(row.get("t"))
            d = {k: row.get(k) for k in PANEL_KEYS}
            samples.append(d)
            for k in PANEL_KEYS:
                series[k].append(row.get(k))
    v = classify(samples, meta.get("ckpt_limit"), meta.get("dl_limit"))
    label = meta.get("label") or os.path.basename(run_dir.rstrip("/"))
    return {
        "t": ts, "series": series, "panels": panels_meta(),
        "status": f"replay · {label} · {len(samples)} samples",
        "verdict": {"text": f"{v['label']} — {v.get('detail','')}",
                    "level": LEVEL.get(v["verdict"], "idle")},
        "target": label,
    }

def list_runs(archive):
    out = []
    for d in sorted(glob.glob(os.path.join(archive, "*")), reverse=True):
        if not os.path.isdir(d):
            continue
        meta = {}
        mp = os.path.join(d, "meta.json")
        if os.path.exists(mp):
            try:
                meta = json.load(open(mp))
            except Exception:
                pass
        out.append({"id": os.path.basename(d),
                    "label": meta.get("label") or os.path.basename(d),
                    "start": meta.get("start")})
    return out

COLLECTOR = None
ARCHIVE = None

PAGE = r"""<!doctype html><html><head><meta charset=utf-8><title>Zebra metrics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2"></script>
<style>
 body{font-family:system-ui,sans-serif;margin:0;background:#0e1116;color:#d7dde5}
 header{padding:10px 16px;background:#161b22;border-bottom:1px solid #30363d}
 h1{font-size:15px;margin:0 0 4px;font-weight:600} #status{font-size:12px;color:#8b949e}
 #verdict{margin-top:6px;font-size:13px;font-weight:600;padding:4px 8px;border-radius:6px;display:inline-block}
 .bad{background:#3d1417;color:#ff7b72} .warn{background:#3a2d12;color:#e3b341}
 .info{background:#11233a;color:#6cb6ff} .idle{background:#21262d;color:#8b949e}
 h3{margin:14px 12px 0;font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:#6e7681}
 .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:10px;padding:6px 10px}
 .card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px 10px}
 .card h2{font-size:12px;margin:0 0 2px;color:#8b949e;font-weight:500;display:flex;justify-content:space-between}
 .val{font-size:17px;font-weight:600;color:#e6edf3} canvas{max-height:130px}
 .toolbar{margin-top:8px;display:flex;align-items:center;gap:6px;font-size:12px}
 .toolbar .grp{display:inline-flex;border:1px solid #30363d;border-radius:6px;overflow:hidden}
 .toolbar button{background:#161b22;color:#8b949e;border:0;padding:3px 10px;font-size:12px;cursor:pointer}
 .toolbar .grp button+button{border-left:1px solid #30363d}
 .toolbar button:hover{color:#d7dde5}
 .toolbar button.on{background:#1f6feb;color:#fff}
 select{background:#161b22;color:#d7dde5;border:1px solid #30363d;border-radius:6px;padding:3px 8px;font-size:12px}
 .reset{background:#161b22;color:#8b949e;border:1px solid #30363d;border-radius:6px;padding:3px 10px;font-size:12px;cursor:pointer}
 .reset:hover{color:#d7dde5}
 .hint{color:#586069}
 .expand{background:none;border:0;color:#586069;cursor:pointer;font-size:13px;line-height:1;padding:0 2px}
 .expand:hover{color:#d7dde5}
 #overlay{position:fixed;inset:0;background:#0e1116;z-index:50;display:none;flex-direction:column;padding:10px 14px;box-sizing:border-box}
 #overlay.show{display:flex}
 #ovbar{display:flex;align-items:center;gap:10px}
 #ovtitle{font-size:14px;font-weight:600;color:#e6edf3} #ovval{font-size:14px;color:#8b949e}
 #ovbar .sp{margin-left:auto}
 #ovwrap{flex:1;min-height:0;margin-top:8px}
 #ovcanvas{width:100%!important;height:100%!important;max-height:none}
</style></head><body>
<header><h1>Zebra observability</h1><div id=status>connecting…</div>
<div id=verdict class=idle>—</div>
<div class=toolbar>
 <span class=grp><button id=x_time class=on onclick="setXMode('time')">Time</button><button id=x_height onclick="setXMode('height')">Height</button></span>
 <span id=runwrap style="display:none">run <select id=run onchange="setRun(this.value)"></select></span>
 <span class=hint>x-axis · click ⤢ on a chart to expand &amp; zoom it</span>
</div></header>
<div id=board></div>
<div id=overlay>
 <div id=ovbar>
  <span id=ovtitle></span><span id=ovval></span>
  <span class=sp></span>
  <span class=hint>drag = zoom to range · wheel = zoom · ctrl+drag = pan</span>
  <button class=reset onclick="ovReset()">Reset zoom</button>
  <button class=reset onclick="closeOverlay()">✕ Close</button>
 </div>
 <div id=ovwrap><canvas id=ovcanvas></canvas></div>
</div>
<script>
const charts={}; let built=false, last_r=null, curRun='live';
let xMode='time';                 // 'time' (wall clock) | 'height' (block height)
let ovChart=null, ovKey=null;     // the single expanded/fullscreen chart, if open
const _zp=window.ChartZoom||window['chartjs-plugin-zoom']||window.chartjsPluginZoom;
if(_zp&&Chart.registry&&!Chart.registry.plugins.get('zoom')){try{Chart.register(_zp)}catch(e){}}

function fmt(v,u){if(v==null)return '—';let n=Math.abs(v)>=100?v.toFixed(0):Math.abs(v)>=1?v.toFixed(1):v.toFixed(3);return n+(u?(' '+u):'')}
function xType(){return xMode==='time'?'time':'linear'}
function lastOf(S){return [...S].reverse().find(x=>x!=null)}

function pointsFor(r,key){
 const H=r.series.height,S=r.series[key],T=r.t,pts=[];
 for(let i=0;i<T.length;i++){
  const x=(xMode==='time')?T[i]:H[i];
  if(x==null)continue;
  pts.push({x:x,y:S[i]});
 }
 return pts;
}

function setXMode(mode){
 if(mode===xMode)return;
 xMode=mode;
 document.getElementById('x_time').classList.toggle('on',mode==='time');
 document.getElementById('x_height').classList.toggle('on',mode==='height');
 for(const k in charts)charts[k].options.scales.x.type=xType();
 if(ovChart){ovChart.options.scales.x.type=xType();if(ovChart.resetZoom)try{ovChart.resetZoom('none')}catch(e){}}
 if(last_r)redraw(last_r);
}
function setRun(v){curRun=v;tick();}

async function loadRuns(){
 let runs;try{runs=await (await fetch('runs')).json()}catch(e){return}
 if(!runs||!runs.length)return;
 const sel=document.getElementById('run');document.getElementById('runwrap').style.display='';
 sel.innerHTML='<option value=live>● live</option>'+runs.map(r=>`<option value="${r.id}">${r.label}</option>`).join('');
}

function panelMeta(key){return (last_r?last_r.panels:[]).find(p=>p.key===key)}
function expand(key){
 ovKey=key;
 document.getElementById('ovtitle').textContent=(panelMeta(key)||{label:key}).label;
 if(!ovChart){
  ovChart=new Chart(document.getElementById('ovcanvas'),{type:'line',
   data:{datasets:[{data:[],borderColor:'#3fb950',borderWidth:1.5,pointRadius:0,fill:true,
     backgroundColor:'rgba(63,185,80,.08)',tension:.25,spanGaps:true}]},
   options:{animation:false,maintainAspectRatio:false,plugins:{legend:{display:false},
     zoom:{zoom:{wheel:{enabled:true},drag:{enabled:true},mode:'x'},
           pan:{enabled:true,mode:'x',modifierKey:'ctrl'}}},
    scales:{x:{type:xType(),ticks:{color:'#586069'},grid:{color:'#21262d'}},
            y:{ticks:{color:'#586069'},grid:{color:'#21262d'},beginAtZero:true}}}});
 }
 ovChart.options.scales.x.type=xType();
 if(ovChart.resetZoom)try{ovChart.resetZoom('none')}catch(e){}
 document.getElementById('overlay').classList.add('show');
 if(last_r)refreshOv(last_r);
}
function refreshOv(r){
 if(!ovChart||ovKey==null)return;
 ovChart.data.datasets[0].data=pointsFor(r,ovKey);
 ovChart.update('none');
 document.getElementById('ovval').textContent=fmt(lastOf(r.series[ovKey]),(panelMeta(ovKey)||{}).unit||'');
}
function ovReset(){if(ovChart&&ovChart.resetZoom)try{ovChart.resetZoom()}catch(e){}}
function closeOverlay(){document.getElementById('overlay').classList.remove('show');ovKey=null;}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeOverlay()});

function build(panels){
 const board=document.getElementById('board'); const groups={};
 for(const p of panels){(groups[p.group]=groups[p.group]||[]).push(p)}
 for(const g in groups){
  const h=document.createElement('h3');h.textContent=g;board.appendChild(h);
  const grid=document.createElement('div');grid.className='grid';board.appendChild(grid);
  for(const p of groups[g]){
   const card=document.createElement('div');card.className='card';
   card.innerHTML=`<h2><span>${p.label}</span><span><button class=expand title=Expand onclick="expand('${p.key}')">⤢</button> <span class=val id=v_${p.key}></span></span></h2><canvas id=c_${p.key}></canvas>`;
   grid.appendChild(card);
   charts[p.key]=new Chart(document.getElementById('c_'+p.key),{type:'line',
    data:{datasets:[{data:[],borderColor:'#3fb950',borderWidth:1.5,pointRadius:0,fill:true,
      backgroundColor:'rgba(63,185,80,.08)',tension:.25,spanGaps:true}]},
    options:{animation:false,plugins:{legend:{display:false}},scales:{
      x:{type:xType(),ticks:{color:'#586069',maxTicksLimit:5},grid:{color:'#21262d'}},
      y:{ticks:{color:'#586069'},grid:{color:'#21262d'},beginAtZero:true}}}});
  }
 }
 built=true;
}

function redraw(r){
 for(const p of r.panels){
  const c=charts[p.key]; if(!c)continue;
  c.data.datasets[0].data=pointsFor(r,p.key);
  c.update('none');
  document.getElementById('v_'+p.key).textContent=fmt(lastOf(r.series[p.key]),p.unit);
 }
 if(ovKey!=null)refreshOv(r);
}

async function tick(){
 const url=curRun==='live'?'data':('data?run='+encodeURIComponent(curRun));
 let r;try{r=await (await fetch(url)).json()}catch(e){document.getElementById('status').textContent='dashboard unreachable';return}
 document.getElementById('status').textContent=r.status||'';
 const vd=document.getElementById('verdict');vd.textContent=r.verdict.text;vd.className=r.verdict.level;
 if(!built)build(r.panels);
 last_r=r;
 redraw(r);
}
loadRuns();tick();setInterval(tick,2500);
</script></body></html>"""

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, body, ct):
        self.send_response(200); self.send_header('Content-Type', ct)
        self.send_header('Content-Length', str(len(body))); self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path.startswith('/runs'):
            body = json.dumps(list_runs(ARCHIVE) if ARCHIVE else []).encode()
            return self._send(body, 'application/json')
        if self.path.startswith('/data'):
            run = None
            mo = re.search(r'[?&]run=([^&]+)', self.path)
            if mo:
                run = urllib.parse.unquote(mo.group(1))
            if run and run != 'live' and ARCHIVE:
                # path-traversal guard: only serve a direct child of the archive
                rd = os.path.join(ARCHIVE, os.path.basename(run))
                snap = load_run(rd) if os.path.isdir(rd) else {
                    "t": [], "series": {k: [] for k in PANEL_KEYS}, "panels": panels_meta(),
                    "status": "run not found", "verdict": {"text": "—", "level": "idle"}, "target": run}
            elif COLLECTOR is not None:
                snap = COLLECTOR.snapshot()
            else:
                snap = {"t": [], "series": {k: [] for k in PANEL_KEYS}, "panels": panels_meta(),
                        "status": "no live node — pick a recorded run", "verdict": {"text": "—", "level": "idle"},
                        "target": "archive"}
            return self._send(json.dumps(snap).encode(), 'application/json')
        return self._send(PAGE.encode(), 'text/html; charset=utf-8')

def do_classify(path, label, ckpt_limit, dl_limit, verdict_out):
    samples = []
    meta = {}
    if os.path.isdir(path):
        run_dir = path
        mp = os.path.join(path, "meta.json")
        if os.path.exists(mp):
            meta = json.load(open(mp))
        path = os.path.join(path, "samples.jsonl")
    if os.path.exists(path):
        for ln in open(path):
            ln = ln.strip()
            if ln:
                try:
                    samples.append(json.loads(ln))
                except ValueError:
                    pass
    ckpt_limit = ckpt_limit or meta.get("ckpt_limit")
    dl_limit = dl_limit or meta.get("dl_limit")
    label = label or meta.get("label")
    v = classify(samples, ckpt_limit, dl_limit)
    out = {"label": label, **v, "samples": len(samples),
           "ckpt_limit": ckpt_limit, "dl_limit": dl_limit}
    if verdict_out:
        with open(verdict_out, "w") as f:
            json.dump(out, f, indent=2)
    print(render_markdown(v, label))
    return v

def main():
    global COLLECTOR, ARCHIVE
    ap = argparse.ArgumentParser()
    ap.add_argument('--target', default=None, help='node metrics host:port (default: auto-detect)')
    ap.add_argument('--port', type=int, default=8090)
    ap.add_argument('--host', default='0.0.0.0', help='dashboard bind host (default 0.0.0.0 = reachable by IP)')
    ap.add_argument('--interval', type=float, default=2.0)
    ap.add_argument('--window', type=int, default=4000)
    ap.add_argument('--record', default=None, metavar='DIR', help='persist samples to DIR/samples.jsonl')
    ap.add_argument('--no-serve', action='store_true', help='record only, no web server (CI sidecar)')
    ap.add_argument('--archive', default=None, metavar='DIR', help='serve recorded runs under DIR (+live)')
    ap.add_argument('--label', default=None, help='run label (recorded into meta / used by classify)')
    ap.add_argument('--ckpt-limit', type=float, default=None, help='checkpoint_verify_concurrency_limit')
    ap.add_argument('--dl-limit', type=float, default=None, help='download_concurrency_limit')
    ap.add_argument('--classify', default=None, metavar='PATH', help='classify a recorded run dir or samples.jsonl and exit')
    ap.add_argument('--verdict-out', default=None, help='write the classifier verdict JSON here')
    a = ap.parse_args()

    if a.classify:
        do_classify(a.classify, a.label, a.ckpt_limit, a.dl_limit, a.verdict_out)
        return

    ARCHIVE = a.archive
    meta = {"label": a.label, "ckpt_limit": a.ckpt_limit, "dl_limit": a.dl_limit}

    # a live/recording collector needs a target; an archive-only server does not
    need_target = (a.record is not None) or (not a.archive) or (not a.no_serve and a.target)
    target = a.target
    if need_target and not target:
        target = autodetect_target()
    if (a.record or a.no_serve) and not target:
        raise SystemExit("no running zebrad metrics endpoint found; pass --target host:port")
    if target:
        COLLECTOR = Collector(target, a.interval, a.window, record_dir=a.record, meta=meta)
        threading.Thread(target=COLLECTOR.loop, daemon=True).start()
        print(f"scraping http://{target}/metrics every {a.interval}s"
              + (f"; recording to {a.record}" if a.record else ""))

    if a.no_serve:
        # headless recorder: keep the scrape loop alive in the foreground
        while True:
            time.sleep(3600)
    print(f"dashboard bound on {a.host}:{a.port}"
          + (f"; archive {a.archive}" if a.archive else ""))
    ThreadingHTTPServer((a.host, a.port), H).serve_forever()

if __name__ == '__main__':
    main()
