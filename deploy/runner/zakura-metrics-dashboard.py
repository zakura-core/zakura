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
  python3 zakura-metrics-dashboard.py
  python3 zakura-metrics-dashboard.py --target 127.0.0.1:19980

  # always-on archive server (live + every recorded run under DIR)
  python3 zakura-metrics-dashboard.py --archive /opt/zakura-bench/dashboard/runs

  # headless recorder (CI sidecar): scrape a node, write samples, no web server
  python3 zakura-metrics-dashboard.py --no-serve --record DIR --target 127.0.0.1:19999 \
      --label v5.0.0 --ckpt-limit 1500 --dl-limit 150

  # classify a recorded run -> markdown on stdout, JSON to --verdict-out
  python3 zakura-metrics-dashboard.py --classify DIR/samples.jsonl \
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

def lbval(m, name, want):
    """Value of a labeled series whose label string contains `want` (e.g. a
    `state="outstanding"` floor-gap tick counter)."""
    for lbl, v in m.get(name, []):
        if want in lbl:
            return v
    return None

def first_present(m, *names):
    for name in names:
        value = bare(m, name)
        if value is not None:
            return value
    return None

def quantile(m, name, q):
    needle = f'quantile="{q}"'
    for lbl, v in m.get(name, []):
        if needle in lbl:
            return v
    return None

# group, key, label, unit, kind
# The note-commitment commit pipeline (the single-writer bottleneck) plus Zakura
# supply. Legacy TCP sync / verifier / committer-task metrics are not emitted on
# the Zakura v2 path, so they are not shown.
PANELS = [
    ("Throughput", "blocks_per_s",   "Blocks / sec (20s avg)",      "blk/s", "rate"),
    ("Throughput", "height",         "Finalized height",            "",      "gauge"),
    ("Commit",     "commit_ms",      "Commit busy / block",         "ms",    "gauge"),
    ("Commit",     "commit_util",    "Commit utilization",          "%",     "gauge"),
    ("Commit CPU", "p_checkpoint",   "checkpoint_compute",          "ms",    "gauge"),
    ("Commit CPU", "p_commit_check", "  commitment_check",          "ms",    "gauge"),
    ("Commit CPU", "p_note_tree",    "  note_tree (update_trees)",  "ms",    "gauge"),
    ("Commit CPU", "p_history_push", "  history_push",              "ms",    "gauge"),
    ("Commit DB",  "p_spent_reads",  "spent_utxo_reads",            "ms",    "gauge"),
    ("Commit DB",  "p_addr_reads",   "address_reads",               "ms",    "gauge"),
    ("Commit DB",  "p_batch_prep",   "batch_prep",                  "ms",    "gauge"),
    ("Commit DB",  "p_rocksdb",      "rocksdb_write",               "ms",    "gauge"),
    ("Commit DB",  "commit_mb",      "Committed MB / block",        "MB",    "gauge"),
    ("Commit DB",  "write_mbps",     "Write throughput",            "MB/s",  "rate"),
    ("VCT path",   "vct_fast_s",     "VCT fast commits / s",        "/s",    "rate"),
    ("VCT path",   "vct_legacy_s",   "VCT legacy commits / s",      "/s",    "rate"),
    ("Zakura",     "zk_peers",       "Cohort peers (active)",       "",      "gauge"),
    ("Zakura",     "zk_qdepth",      "Zakura queue depth",          "",      "gauge"),
    ("Zakura",     "zk_block_sync",  "block_sync streams",          "",      "gauge"),
    # Apply-queue depth + floor-gap attribution: separates HOL download stalls
    # from the sequencer→committer handoff ("glue") from peer-supply starvation.
    ("Apply queue","applying",       "Applying (contiguous, cap 400)","",    "gauge"),
    ("Apply queue","reorder",        "Reorder (out-of-order buffered)","",   "gauge"),
    ("Apply queue","unsub_applying", "Unsubmitted applying",        "",      "gauge"),
    ("Floor gap",  "body_lead",      "Body lead (floor−finalized)", "",      "gauge"),
    ("Floor gap",  "commit_gap",     "Commit gap (floor−verified)", "",      "gauge"),
    ("Floor gap",  "commit_stall_s", "Commit frontier stall",       "s",     "gauge"),
    ("Floor gap",  "outstanding",    "Outstanding floor requests",  "",      "gauge"),
    ("Floor gap",  "fg_slow_s",      "floor: slow-download /s",     "/s",    "rate"),
    ("Floor gap",  "fg_starve_s",    "floor: peer/slot starve /s",  "/s",    "rate"),
    ("Floor gap",  "fg_glue_s",      "floor: buffered-unrequested /s","/s",  "rate"),
]
PANEL_KEYS = [k for _, k, *_ in PANELS]

def panels_meta():
    return [{"group": g, "key": k, "label": l, "unit": u} for g, k, l, u, _ in PANELS]

# ── bottleneck classifier ─────────────────────────────────────────────────────
# The single-writer commit pipeline gates throughput. If the writer is busy most
# of the wall (high commit utilization) the run is COMMIT-BOUND on its heaviest
# phase; if the writer is mostly idle the Zakura cohort isn't supplying blocks
# fast enough (SUPPLY-BOUND).
STALL_BPS      = 1.0    # below this, no commit progress
COMMIT_UTIL_HI = 70.0   # writer busy >= this % of wall -> commit-bound
COMMIT_UTIL_LO = 45.0   # writer busy <= this % of wall -> supply-bound

# (key, label) for the sequential per-block commit phases (cc ∥ ut + hp are
# inside checkpoint_compute; reads + batch build + write are sequential after it).
COMMIT_PHASES = [
    ("p_checkpoint",  "checkpoint_compute (note_tree+history)"),
    ("p_spent_reads", "spent_utxo_reads"),
    ("p_addr_reads",  "address_reads"),
    ("p_batch_prep",  "batch_prep"),
    ("p_rocksdb",     "rocksdb_write"),
]

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

APPLY_QUEUE_CAP = 400   # MAX_CHECKPOINT_HEIGHT_GAP: contiguous applying-queue ceiling
APPLY_FULL      = 360    # "near cap" => bodies downloaded & queued, handoff not draining

def _floor_reason(applying, reorder, outstanding, slow_s, starve_s, glue_s):
    """Attribute a non-advancing commit floor from apply-queue depth (Evan's test),
    corroborated by the floor-gap reason rates. Returns (tag, human text).

    apply queue pinned near 400  -> glue/commit-bound (NOT block-sync): blocks are
      downloaded and contiguously queued, the sequencer→committer handoff isn't draining.
    apply queue low + reorder high -> head-of-line download stall: the floor body is
      missing while out-of-order successors pile up in reorder.
    apply queue low + reorder low  -> supply starvation: the cohort isn't delivering."""
    a = applying if applying is not None else 0
    if applying is not None and a >= APPLY_FULL:
        return ("glue", f"apply queue FULL ({a:.0f}/{APPLY_QUEUE_CAP}) — bodies downloaded & "
                        f"queued, commit handoff not draining (glue/commit-bound, NOT block-sync)")
    if reorder is not None and reorder > max(a, 50):
        return ("hol", f"apply queue LOW ({a:.0f}/{APPLY_QUEUE_CAP}) + reorder {reorder:.0f} "
                       f"out-of-order — floor body missing, successors stacked (head-of-line)")
    bits = [b for b in (
        (f"slow-dl {slow_s:.2f}/s"      if slow_s   else None),
        (f"peer/slot-starve {starve_s:.2f}/s" if starve_s else None),
        (f"buffered-unreq {glue_s:.2f}/s" if glue_s else None),
    ) if b]
    rtxt = ("; floor-gap " + ", ".join(bits)) if bits else ""
    return ("supply", f"apply queue LOW ({a:.0f}/{APPLY_QUEUE_CAP}), outstanding="
                      f"{outstanding if outstanding is not None else '—'} — cohort not supplying the floor{rtxt}")

def classify(samples, ckpt_limit=None, dl_limit=None):
    """Return {verdict, label, confidence, scores, detail, bps} over the steady
    window: COMMIT-BOUND (writer saturated, names the heaviest phase) vs
    SUPPLY-BOUND (writer idle, the cohort isn't feeding blocks) vs BALANCED."""
    win = steady_window(samples)
    if not win:
        return {"verdict": "idle", "label": "No data", "confidence": "low",
                "scores": {}, "detail": "no samples recorded", "bps": None}

    def med(k):
        return _median([s.get(k) for s in win])

    bps   = med("blocks_per_s")
    cutil = med("commit_util")                     # writer % busy of wall
    commit_u = (cutil / 100.0) if cutil is not None else None
    phases = {k: med(k) for k, _ in COMMIT_PHASES}
    phases = {k: v for k, v in phases.items() if v}
    scores = {"commit": round(commit_u, 3)} if commit_u is not None else {}

    # Apply-queue attribution: HOL vs glue/commit vs supply (Evan's 400 test).
    fr_tag, fr_text = _floor_reason(med("applying"), med("reorder"), med("outstanding"),
                                    med("fg_slow_s"), med("fg_starve_s"), med("fg_glue_s"))

    if bps is None or bps < STALL_BPS:
        return {"verdict": "stalled", "label": "STALLED / STARVED", "confidence": "high",
                "scores": scores, "bps": (round(bps, 2) if bps is not None else None),
                "detail": f"no commit progress ({(bps or 0):.2f} blk/s) — {fr_text}"}

    top = max(phases, key=phases.get) if phases else None
    topname = dict(COMMIT_PHASES).get(top, top)

    if commit_u is not None and cutil >= COMMIT_UTIL_HI:
        detail = f"writer saturated ({cutil:.0f}% util, {bps:.0f} blk/s)"
        if top:
            detail += f"; dominant phase: {topname} ({phases[top]:.1f} ms/blk)"
        return {"verdict": "commit_bound", "label": "COMMIT-BOUND",
                "confidence": _conf(scores, "commit"), "scores": scores,
                "detail": detail, "bps": round(bps, 2)}

    if commit_u is not None and cutil <= COMMIT_UTIL_LO:
        # Writer idle is necessary but NOT sufficient for "supply-bound": a full
        # apply queue means bodies ARE downloaded and the handoff is the limiter.
        # Use the apply-queue depth to tell glue/commit from genuine supply/HOL.
        label = {"glue": "GLUE / HANDOFF-BOUND", "hol": "HEAD-OF-LINE (download)",
                 "supply": "SUPPLY-BOUND (Zakura)"}.get(fr_tag, "SUPPLY-BOUND (Zakura)")
        verdict = "commit_bound" if fr_tag == "glue" else "supply_bound"
        return {"verdict": verdict, "label": label,
                "confidence": "medium", "scores": scores, "bps": round(bps, 2),
                "detail": f"writer only {cutil:.0f}% busy at {bps:.0f} blk/s — {fr_text}"}

    detail = f"writer {cutil:.0f}% busy at {bps:.0f} blk/s" if cutil is not None else f"{bps:.0f} blk/s"
    if top:
        detail += f"; heaviest phase {topname} ({phases[top]:.1f} ms/blk)"
    return {"verdict": "balanced", "label": "BALANCED", "confidence": "low",
            "scores": scores, "bps": round(bps, 2), "detail": detail}

def _conf(scores, winner):
    """high if the winning stage clearly leads the runner-up, else medium."""
    if winner not in scores:
        return "low"
    others = [v for k, v in scores.items() if k != winner]
    if not others:
        return "medium"
    return "high" if scores[winner] - max(others) >= 0.25 else "medium"

# verdict level -> live-banner CSS class
LEVEL = {"commit_bound": "warn", "supply_bound": "info", "balanced": "info",
         "stalled": "bad", "idle": "idle"}

def render_markdown(v, label=None):
    title = f" — {label}" if label else ""
    lines = [f"## Bottleneck verdict{title}", "",
             f"**{v['label']}** (confidence {v['confidence']})", "",
             v.get("detail", ""), ""]
    if "commit" in v.get("scores", {}):
        mark = " ⟵ limiter" if v["verdict"] == "commit_bound" else ""
        lines += ["| stage | utilization |", "|-------|------------:|",
                  f"| commit (single writer) | {v['scores']['commit']*100:.0f}%{mark} |"]
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
        if 'zakurad' not in exe:
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
    def __init__(self, target, interval, window, record_dir=None, meta=None,
                 smooth_secs=20.0):
        self.target = target
        self.interval = interval
        self.series = {k: deque(maxlen=window) for k in PANEL_KEYS}
        self.ts = deque(maxlen=window)
        # Trailing (time, height) samples for SMOOTHED throughput. Checkpoint
        # sync commits in batches, so a single-interval Δheight aliases into
        # spike/zero; averaging over smooth_secs gives the honest block rate.
        self.smooth_secs = smooth_secs
        self.h_window = deque()
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
        d["height"]        = bare(m, "state_finalized_block_height")
        d["zk_peers"]      = bare(m, "zakura_p2p_conn_active")
        d["zk_qdepth"]     = bare(m, "zakura_p2p_queue_depth")
        d["zk_block_sync"] = next((v for lbl, v in m.get("zakura_p2p_stream_accepted", [])
                                   if "block_sync" in lbl), None)
        # Apply-queue depth + floor-gap frontiers (instantaneous gauges).
        d["applying"]       = bare(m, "sync_block_applying")
        d["reorder"]        = first_present(m, "sync_block_reorder",
                                            "sync_block_reorder_buffered_blocks")
        d["unsub_applying"] = bare(m, "sync_block_unsubmitted_applying")
        d["commit_gap"]     = bare(m, "sync_block_commit_gap_height")
        d["commit_stall_s"] = bare(m, "sync_block_commit_frontier_stall_seconds")
        d["outstanding"]    = bare(m, "sync_block_outstanding")
        # Body lead/backlog: contiguous downloaded/queued bodies ahead of finalized
        # state = body floor (download_floor) − finalized tip.
        _dlf = first_present(m, "sync_block_download_floor_height",
                             "sync_block_body_download_floor_height")
        _fin = bare(m, "state_finalized_block_height")
        d["body_lead"] = (_dlf - _fin) if (_dlf is not None and _fin is not None and _dlf > 0) else None

        cur = {
            "h":    bare(m, "state_finalized_block_height"),
            # Floor-gap state-tick counters (cumulative; rate() ⇒ time-fraction).
            "fg_slow":   lbval(m, "sync_block_floor_gap_state_ticks", 'state="outstanding"'),
            "fg_q":      lbval(m, "sync_block_floor_gap_state_ticks", 'state="queued"'),
            "fg_ns":     lbval(m, "sync_block_floor_gap_state_ticks", 'state="needed_unscheduled"'),
            "fg_glue":   lbval(m, "sync_block_floor_gap_state_ticks", 'state="in_flight_without_outstanding"'),
            "vf":   bare(m, "state_vct_fast_block_count"),
            "vl":   bare(m, "state_vct_legacy_block_count"),
            "ckc_s":bare(m, "zakura_state_write_checkpoint_compute_duration_seconds_sum"),
            "ckc_c":bare(m, "zakura_state_write_checkpoint_compute_duration_seconds_count"),
            "cc_s": bare(m, "zakura_state_write_commitment_check_duration_seconds_sum"),
            "cc_c": bare(m, "zakura_state_write_commitment_check_duration_seconds_count"),
            "ut_s": bare(m, "zakura_state_write_update_trees_duration_seconds_sum"),
            "ut_c": bare(m, "zakura_state_write_update_trees_duration_seconds_count"),
            "hp_s": bare(m, "zakura_state_commit_history_push_duration_seconds_sum"),
            "hp_c": bare(m, "zakura_state_commit_history_push_duration_seconds_count"),
            "sur_s":bare(m, "zakura_state_write_spent_utxo_reads_duration_seconds_sum"),
            "sur_c":bare(m, "zakura_state_write_spent_utxo_reads_duration_seconds_count"),
            "ar_s": bare(m, "zakura_state_write_address_reads_duration_seconds_sum"),
            "ar_c": bare(m, "zakura_state_write_address_reads_duration_seconds_count"),
            "bp_s": bare(m, "zakura_state_write_batch_prep_duration_seconds_sum"),
            "bp_c": bare(m, "zakura_state_write_batch_prep_duration_seconds_count"),
            "bc_s": bare(m, "zakura_state_rocksdb_batch_commit_duration_seconds_sum"),
            "bc_c": bare(m, "zakura_state_rocksdb_batch_commit_duration_seconds_count"),
            "bb_s": bare(m, "zakura_state_write_batch_bytes_sum"),
            "bb_c": bare(m, "zakura_state_write_batch_bytes_count"),
        }
        # Maintain the trailing height window for smoothed throughput.
        if cur["h"] is not None:
            self.h_window.append((now, cur["h"]))
            cutoff = now - self.smooth_secs
            while len(self.h_window) > 2 and self.h_window[0][0] < cutoff:
                self.h_window.popleft()
        def rate(a, b, dt):
            return (b - a) / dt if (a is not None and b is not None and dt > 0) else None
        def smoothed_bps():
            # blk/s over the trailing window spread (>=2 samples), else single-tick.
            if len(self.h_window) >= 2:
                t0, h0 = self.h_window[0]; t1, h1 = self.h_window[-1]
                if t1 > t0 and h0 is not None and h1 is not None:
                    return (h1 - h0) / (t1 - t0)
            return None
        def avg(s0, s1, c0, c1, scale=1000.0):
            if None in (s0, s1, c0, c1) or (c1 - c0) <= 0:
                return None
            return scale * (s1 - s0) / (c1 - c0)
        if self.prev is not None:
            dt = now - self.prev_t
            p = self.prev
            d["blocks_per_s"]   = smoothed_bps()
            if d["blocks_per_s"] is None:
                d["blocks_per_s"] = rate(p["h"], cur["h"], dt)
            d["vct_fast_s"]     = rate(p["vf"], cur["vf"], dt)
            d["vct_legacy_s"]   = rate(p["vl"], cur["vl"], dt)
            # Floor-gap reason rates (ticks/s ≈ fraction of stall time in each reason).
            d["fg_slow_s"]      = rate(p.get("fg_slow"), cur["fg_slow"], dt)
            d["fg_glue_s"]      = rate(p.get("fg_glue"), cur["fg_glue"], dt)
            _q  = rate(p.get("fg_q"),  cur["fg_q"],  dt)
            _ns = rate(p.get("fg_ns"), cur["fg_ns"], dt)
            d["fg_starve_s"]    = ((_q or 0) + (_ns or 0)) if (_q is not None or _ns is not None) else None
            d["p_checkpoint"]   = avg(p["ckc_s"], cur["ckc_s"], p["ckc_c"], cur["ckc_c"])
            d["p_commit_check"] = avg(p["cc_s"],  cur["cc_s"],  p["cc_c"],  cur["cc_c"])
            d["p_note_tree"]    = avg(p["ut_s"],  cur["ut_s"],  p["ut_c"],  cur["ut_c"])
            d["p_history_push"] = avg(p["hp_s"],  cur["hp_s"],  p["hp_c"],  cur["hp_c"])
            d["p_spent_reads"]  = avg(p["sur_s"], cur["sur_s"], p["sur_c"], cur["sur_c"])
            d["p_addr_reads"]   = avg(p["ar_s"],  cur["ar_s"],  p["ar_c"],  cur["ar_c"])
            d["p_batch_prep"]   = avg(p["bp_s"],  cur["bp_s"],  p["bp_c"],  cur["bp_c"])
            # rocksdb write is recorded once per DiskWriteBatch, so bc_c counts
            # BATCHES not blocks when batch_commit_max>1. Normalize by a per-block
            # count (bp_c, recorded once per block) so commit_ms/commit_util stay
            # per-block instead of inflating ~K× and pinning util at 100%. For K=1
            # bc_c == bp_c, so this is unchanged.
            d["p_rocksdb"]      = avg(p["bc_s"],  cur["bc_s"],  p["bp_c"],  cur["bp_c"])
            bpb = avg(p["bb_s"], cur["bb_s"], p["bb_c"], cur["bb_c"], scale=1.0)  # bytes/block
            d["commit_mb"]      = (bpb/1e6) if bpb is not None else None
            wb = rate(p["bb_s"], cur["bb_s"], dt)                                 # bytes/s
            d["write_mbps"]     = (wb/1e6) if wb is not None else None
            # commit wall/block = the sequential phases (commitment_check ∥ note_tree
            # + history_push are inside checkpoint_compute, so don't double-count them).
            parts = [d.get(k) for k in ("p_checkpoint", "p_spent_reads",
                     "p_addr_reads", "p_batch_prep", "p_rocksdb")]
            d["commit_ms"] = (sum(x for x in parts if x is not None)
                              if any(x is not None for x in parts) else None)
            if d["commit_ms"] is not None and d.get("blocks_per_s") is not None:
                d["commit_util"] = min(100.0, max(0.0, d["commit_ms"] * d["blocks_per_s"] / 10.0))
            else:
                d["commit_util"] = None
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

PAGE = r"""<!doctype html><html><head><meta charset=utf-8><title>Zakura metrics</title>
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
<header><h1>Zakura observability</h1><div id=status>connecting…</div>
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
    ap.add_argument('--smooth', type=float, default=20.0,
                    help='throughput smoothing window in seconds (default 20; '
                         'averages over batched checkpoint commits)')
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
        raise SystemExit("no running zakurad metrics endpoint found; pass --target host:port")
    if target:
        COLLECTOR = Collector(target, a.interval, a.window, record_dir=a.record, meta=meta,
                              smooth_secs=a.smooth)
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
