# Agentic Perf Loop (Phases 0–1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the tooling + playbook that lets an orchestrator agent run unattended perf experiments against Zakura: ephemeral DO bench droplets, A/B sync benchmarks via `scripts/checkpoint-sync-bench.sh`, deterministic verdicts, and a committed ledger.

**Architecture:** Small single-purpose shell/python primitives under `perf-lab/` that the orchestrator (a Claude session following `.agents/skills/perf-lab/SKILL.md`) composes. Droplets boot from the `zakura-pr-node-*` golden image and are tag-scoped (`zakura-perf-lab`); the bench script runs on-droplet from a pinned control clone; artifacts come back to the Mac; `verdict.py` turns them into WIN/NEUTRAL/LOSS.

**Tech Stack:** bash + doctl 1.120 + ssh/scp, python3 stdlib (no deps), existing `scripts/checkpoint-sync-bench.sh` on `origin/main`, GitHub CLI for the fallback lane.

**Spec:** `docs/superpowers/2026-07-20-agentic-perf-workflow-design.md` (decisions D1–D7 resolved).

---

## File structure

```
perf-lab/
  README.md          — operating guide (what, how to run, cost)
  config.env         — all knobs (region/size/tag/paths/budget); sourced by every script
  droplet.sh         — provision | ip | ssh | destroy | reap | list  (doctl wrapper, tag-guarded)
  bench.sh           — start | status | collect  (remote bench lifecycle + artifact pull)
  gates.sh           — l0 (fmt/clippy/tests) and micro-mockbs pre-filter
  verdict.py         — parse bench summary.md → WIN/NEUTRAL/LOSS JSON (stdlib only)
  tests/
    test_verdict.py  — unit tests + summary.md fixture
    doctl_stub.sh    — records doctl argv for droplet.sh guard tests
  BACKLOG.md         — seeded experiment backlog
  LEDGER.md          — append-only experiment record (the reporting channel, per D5)
  REPORT.md          — regenerated morning summary
  state.json         — loop state (current experiment, droplet ids, batch counters)
.agents/skills/perf-lab/SKILL.md — the orchestrator playbook (state machine + safety rails)
.claude/settings.local.json      — permission allowlist (NOT committed; gitignored)
~/zakura-perf-lab/               — outside repo: runs/<label>/ artifacts, shared cargo target
```

Everything under `perf-lab/` + the skill is committed on `adam/zakura-agentic-perf-5667cc`. Nothing touches `main`.

---

### Task 1: Rebase onto origin/main and scaffold perf-lab/

**Files:**
- Create: `perf-lab/config.env`, `perf-lab/BACKLOG.md`, `perf-lab/LEDGER.md`, `perf-lab/REPORT.md`, `perf-lab/state.json`, `perf-lab/README.md`

- [ ] **Step 1: Rebase the orchestration branch onto the real mainline**

```bash
cd /Users/czar/Documents/zakura/.claude/worktrees/zakura-sighash-optimization-5328f4
git fetch origin main
git rebase --onto origin/main 2998771df adam/zakura-agentic-perf-5667cc
```

The `--onto` form deliberately **drops** `2998771df` ("ci: align Zakura workflows with main"): that commit was inherited from the stale local `main` tip, is not an ancestor of `origin/main` (the shared ancestor is `1c34ceaa7`), and conflicts with — and would partially revive — workflow files `origin/main` has since rewritten or deleted. Only the `docs(superpowers)` commits are ours; they replay cleanly (docs/superpowers/ does not exist on origin/main). `ls` now shows `zakura-*` crates. If this still conflicts, stop and report — do not force anything.

- [ ] **Step 2: Sanity-build check (warm the shared target dir)**

```bash
mkdir -p ~/zakura-perf-lab/runs
CARGO_TARGET_DIR=~/zakura-perf-lab/target cargo check -p zakura 2>&1 | tail -3
```

Expected: `Finished` (cold run takes ~5–15 min on the M4 Max; that's normal). This validates the renamed workspace builds on macOS before anything else depends on it.

- [ ] **Step 3: Write `perf-lab/config.env`**

```bash
# perf-lab configuration — sourced by droplet.sh / bench.sh / gates.sh.
# Shell-safe key=value only; no side effects.

# --- DigitalOcean ---
DO_REGION="${DO_REGION:-nyc3}"                 # matches pr-node bake default
DO_SIZE="${DO_SIZE:-c-16}"                     # dedicated CPU; disk must be >= golden image disk
DO_FALLBACK_IMAGE="${DO_FALLBACK_IMAGE:-ubuntu-24-04-x64}"
GOLDEN_IMAGE_PREFIX="${GOLDEN_IMAGE_PREFIX:-zakura-pr-node-}"
PERF_TAG="zakura-perf-lab"                     # every perf-lab resource carries this tag
NAME_PREFIX="perf-lab"                         # ...and this name prefix
SSH_KEY_NAME="perf-lab-claude"
SSH_KEY_FILE="$HOME/.ssh/perf-lab-claude"
MAX_DROPLETS=2
REAP_MAX_AGE_HOURS=24

# --- remote layout ---
BENCH_HOME_REMOTE="/opt/zakura-bench"          # checkpoint-sync-bench.sh cache root
CTL_CLONE_REMOTE="/root/zakura-ctl"            # pinned control clone the script RUNS from
GOLDEN_CLONE_REMOTE="/root/zakura"             # golden image's warm clone (BUILD_SRC target)
GOLDEN_TARGET_REMOTE="/root/cargo-target"      # golden image's warm cargo target
BENCH_OUT_REMOTE="/root/bench-out"

# --- local layout ---
ARTIFACT_ROOT="$HOME/zakura-perf-lab"

# --- measurement ---
BATCH_SIZE=8                                   # D3/D6: bench runs per batch
WIN_THRESHOLD_PCT="3.0"                        # floor; effective = max(this, 2*NOISE_BAND_PCT)
NOISE_BAND_PCT=""                              # set by Task 7 A/A calibration
```

- [ ] **Step 4: Write `perf-lab/BACKLOG.md`** (seeded from spec §6; EV/cost are the orchestrator's running estimates)

```markdown
# perf-lab backlog

Ranked queue. The campaign-target memo (SKILL.md step 0) re-ranks this list
against measured attribution before experiment 001. Statuses:
READY | BLOCKED(<why>) | DONE(EXP-NNN) | DROPPED(<why>).

## Tuning-class (green)

- B-01 READY — Sweep `CKPT_LIMIT` (checkpoint_verify_concurrency_limit).
  Hypothesis: default 1500 was hand-picked; the knee may sit elsewhere on
  dedicated CPU. Lane: bench env var only (no code change). Cost: 1 bench
  run per point, 3 points (500/1500/3000).
- B-02 READY — Sweep `DL_LIMIT` (download_concurrency_limit) 50/150/400.
  Same shape as B-01.
- B-03 READY — Block-sync knob sweep: `max_blocks_per_response`, request
  timeout, in-flight cap. Hypothesis: one manual pass tuned these; a
  mock-blocksync-pre-filtered sweep finds a better operating point.
  Lane: code-default change per point; mock-blocksync L1 pre-filter.
- B-04 READY — Body-commit batch size knee (DiskWriteBatch batching).
  Hypothesis: batch-size metrics show batching active; the knee is unmeasured.
  Lane: code-default change + L2.
- B-05 READY — RocksDB bulk-load read-side options during checkpoint sync
  (block cache size, memtable count/size, compaction style). Guarded by the
  rocksdb batch-commit histogram. Lane: code-default change + L2.
- B-06 READY — Verifier batch sizes/windows for redpallas/halo2/groth16
  batched verification. Criterion L1 pre-filter; consensus subsystem has never
  had a perf pass (risk: green while only limits/windows change, red if
  verification logic changes).
- B-07 READY — Rayon pool sizing: global verifier pool vs dedicated commit
  pool vs core count. Lane: code-default change + L2.
- B-08 READY — Tokio worker-thread count + channel capacities on the split
  sequencer channels / writer input queue. Lane: code-default change + L2.

## Structural-class (yellow)

- B-09 READY — `FromDisk` TODO at
  zakura-state/src/service/finalized_state/disk_format/block.rs:296 —
  skip redundant crypto checks when deserializing transactions from trusted
  storage, or parallelize across transactions. Extra gates per spec §5.
- B-10 BLOCKED(profile-first) — Allocation/clone hotspots in
  download→verify→commit. Needs samply/perf evidence naming the site.
- B-11 BLOCKED(coordinate PR 228) — Tracing/metrics overhead in hot loops.
- B-12 BLOCKED(attribution) — Writer idle / commit pacing; only if verdicts
  show commit-bound.

## Exclusions (refresh from `gh pr list` at session start; as of 2026-07-20)

sighash/ZIP-244 (merged caching + PR 288), block-template isolation (PR 292),
VCT artifact generation (PR 249), retained-memory accounting (PR 217/225),
lazy trace events (PR 228), block-sync peer accountability/reconnect
(PR 209/166), header-sync alignment (PR 313 + active main-tip work),
consensus/state-integrity fixes branch (PR 165).
```

- [ ] **Step 5: Write `perf-lab/LEDGER.md`**

```markdown
# perf-lab ledger

Append-only. One `## EXP-NNN` entry per experiment; one `## SESSION` header
per orchestrator session; one `## BATCH` summary every BATCH_SIZE bench runs.
This file is the sole reporting channel (design D5). Entry template:

    ## EXP-NNN <slug>
    - date / session: ...
    - backlog id / hypothesis: ...
    - risk class: green|yellow|red-proposal
    - branch: adam/perf-exp/NNN-<slug>   patch: ~/zakura-perf-lab/runs/<label>/exp.patch
    - diff summary: 2–4 lines
    - gates: L0 pass|fail, L1 <numbers or n/a>
    - bench: label(s), droplet, baseline vs candidate post-commit blk/s,
      delta %, noise band %, threshold %
    - verdict: WIN | PROMISING | NEUTRAL | LOSS | BROKEN | PROPOSAL
    - attribution: dominant bottleneck class from verdict-*.json
    - simplicity: 1–5 (1 = config constant, 5 = pipeline restructure)
    - follow-ups: ...
```

- [ ] **Step 6: Write `perf-lab/REPORT.md`**

```markdown
# perf-lab report

(Regenerated by the orchestrator after every verdict — see SKILL.md. Sections:
Baseline, Confirmed wins ranked by delta × simplicity, Promising queue,
Red-class proposals, Incidents, Spend.)

No sessions run yet.
```

- [ ] **Step 7: Write `perf-lab/state.json`**

```json
{
  "schema": 1,
  "session": null,
  "batch_runs_used": 0,
  "next_exp_id": 1,
  "droplets": {},
  "in_flight": {},
  "noise_band_pct": null
}
```

- [ ] **Step 8: Write `perf-lab/README.md`**

```markdown
# perf-lab

Tooling for the agentic sync-perf loop. Design + decisions:
`docs/superpowers/2026-07-20-agentic-perf-workflow-design.md`.
Operating playbook: `.agents/skills/perf-lab/SKILL.md` (invoke the `perf-lab`
skill to start/resume a session).

- `config.env` — every knob. `NOISE_BAND_PCT` is written by A/A calibration.
- `droplet.sh` — `provision|ip|ssh|destroy|reap|list`. Only touches DO
  resources named `perf-lab-*` AND tagged `zakura-perf-lab`.
- `bench.sh` — `start|status|collect` one A/B bench on the droplet.
- `gates.sh` — local L0 gates and the mock-blocksync L1 pre-filter.
- `verdict.py` — bench artifacts → verdict JSON.
- Artifacts land in `~/zakura-perf-lab/runs/<label>/`.

Cost: one c-16 droplet ≈ $0.5/h; a 12 h session ≈ $6. Every create/destroy is
recorded in LEDGER.md.
```

- [ ] **Step 9: Commit**

```bash
git add perf-lab/
git commit -m "feat(perf-lab): scaffold config, backlog, ledger, and state"
```

---

### Task 2: verdict.py (TDD)

**Files:**
- Create: `perf-lab/verdict.py`
- Test: `perf-lab/tests/test_verdict.py`

The bench writes `summary.md` (writer at `scripts/checkpoint-sync-bench.sh` on `origin/main`, ~lines 668–706): a bullet list, then a table with header `| binary | p2p_stack | end height | blocks covered | time taken | blocks/s | post-commit blk/s |`, rows suffixed `(baseline)` / `(primary)`, then `**Speedup:** B → R blocks/s = **X.XX×**`. The parser keys on header names + row suffixes, not column positions. **Note:** the fixture below is derived from that writer; Task 7 validates it against a real run and updates the fixture if reality differs.

- [ ] **Step 1: Write the failing test**

```python
# perf-lab/tests/test_verdict.py
import json, subprocess, sys, unittest
from pathlib import Path

HERE = Path(__file__).parent
VERDICT = HERE.parent / "verdict.py"

FIXTURE = """## Checkpoint-sync benchmark

- binary source: build `adam/perf-exp/001-ckpt-limit`
- snapshot start height: **1707210**, stop height: **1737210**, feed: `167.99.162.47:8233` (peerset=1)
- sync knobs: checkpoint_verify=1500, download=150
- P2P mode: target p2p_stack=zakura, baseline p2p_stack=zakura

| binary | p2p_stack | end height | blocks covered | time taken | blocks/s | post-commit blk/s |
|--------|----------:|-----------:|---------------:|-----------:|---------:|------------------:|
| main (baseline) | zakura | 1737210 | 30000 | 1250s | 24.00 | 26.00 |
| adam/perf-exp/001-ckpt-limit (primary) | zakura | 1737210 | 30000 | 1190s | 25.21 | 27.30 |

**Speedup:** 24.00 → 25.21 blocks/s = **1.05×**
"""

def run(args, summary_text):
    d = HERE / "_tmp"; d.mkdir(exist_ok=True)
    f = d / "summary.md"; f.write_text(summary_text)
    p = subprocess.run([sys.executable, str(VERDICT), str(f), *args],
                       capture_output=True, text=True)
    return p, (json.loads(p.stdout) if p.returncode == 0 else None)

class TestVerdict(unittest.TestCase):
    def test_parses_gating_metric(self):
        p, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], FIXTURE)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertAlmostEqual(out["baseline_pc_bps"], 26.00)
        self.assertAlmostEqual(out["primary_pc_bps"], 27.30)
        self.assertAlmostEqual(out["delta_pct"], 5.0, places=1)

    def test_win_when_above_effective_threshold(self):
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], FIXTURE)
        self.assertEqual(out["verdict"], "WIN_CANDIDATE")
        self.assertAlmostEqual(out["effective_threshold_pct"], 3.0)  # max(3.0, 2*0.8)

    def test_noise_band_can_raise_threshold(self):
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "4.0"], FIXTURE)
        self.assertEqual(out["effective_threshold_pct"], 8.0)
        self.assertEqual(out["verdict"], "NEUTRAL")

    def test_loss_detected(self):
        worse = FIXTURE.replace("| 1190s | 25.21 | 27.30 |", "| 1400s | 21.43 | 22.90 |")
        _, out = run(["--threshold-pct", "3.0", "--noise-band-pct", "0.8"], worse)
        self.assertEqual(out["verdict"], "LOSS")

    def test_aa_mode_reports_observed_noise(self):
        _, out = run(["--aa"], FIXTURE)
        self.assertAlmostEqual(out["observed_noise_pct"], 5.0, places=1)

    def test_missing_baseline_row_errors(self):
        p, _ = run([], FIXTURE.replace("(baseline)", "(nope)"))
        self.assertNotEqual(p.returncode, 0)

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run to verify it fails**

Run: `python3 perf-lab/tests/test_verdict.py -v`
Expected: errors — `verdict.py` does not exist.

- [ ] **Step 3: Implement `perf-lab/verdict.py`**

```python
#!/usr/bin/env python3
"""Turn a checkpoint-sync-bench summary.md into a machine verdict.

Gating metric (design §4): post-first-commit blocks/s. Verdicts:
WIN_CANDIDATE (delta >= effective threshold; needs a confirmation run to
become WIN), NEUTRAL (within +/- threshold), LOSS (below -threshold).
--aa prints the observed |delta| as the noise sample instead.

Usage:
  verdict.py SUMMARY_MD [--threshold-pct F] [--noise-band-pct F] [--aa]
Exit 0 with JSON on stdout; exit 2 on parse failure (missing rows/columns).
"""
import argparse, json, re, sys


def parse_table(text):
    """Return {role: {col_name: cell}} for the (baseline)/(primary) rows."""
    lines = [l.strip() for l in text.splitlines() if l.strip().startswith("|")]
    if len(lines) < 3:
        sys.exit(2)
    header = [c.strip().lower() for c in lines[0].strip("|").split("|")]
    rows = {}
    for line in lines[2:]:  # skip header + separator
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != len(header):
            continue
        binary = cells[0]
        m = re.search(r"\((baseline|primary)\)", binary)
        if m:
            rows[m.group(1)] = dict(zip(header, cells))
    return rows


def fnum(row, col):
    try:
        return float(row[col])
    except (KeyError, ValueError):
        print(f"verdict.py: cannot read column '{col}' from row {row}", file=sys.stderr)
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("summary")
    ap.add_argument("--threshold-pct", type=float, default=3.0)
    ap.add_argument("--noise-band-pct", type=float, default=0.0)
    ap.add_argument("--aa", action="store_true")
    args = ap.parse_args()

    rows = parse_table(open(args.summary).read())
    if "baseline" not in rows or "primary" not in rows:
        print("verdict.py: need both (baseline) and (primary) rows", file=sys.stderr)
        sys.exit(2)

    col = "post-commit blk/s"
    b, p = fnum(rows["baseline"], col), fnum(rows["primary"], col)
    if b <= 0:
        print("verdict.py: baseline gating metric is zero", file=sys.stderr)
        sys.exit(2)
    delta = (p - b) / b * 100.0

    if args.aa:
        print(json.dumps({"mode": "aa", "observed_noise_pct": round(abs(delta), 3),
                          "baseline_pc_bps": b, "primary_pc_bps": p}))
        return

    eff = max(args.threshold_pct, 2.0 * args.noise_band_pct)
    verdict = ("WIN_CANDIDATE" if delta >= eff
               else "LOSS" if delta <= -eff else "NEUTRAL")
    print(json.dumps({
        "mode": "ab", "baseline_pc_bps": b, "primary_pc_bps": p,
        "delta_pct": round(delta, 3), "threshold_pct": args.threshold_pct,
        "noise_band_pct": args.noise_band_pct,
        "effective_threshold_pct": eff, "verdict": verdict,
        "baseline_bps": fnum(rows["baseline"], "blocks/s"),
        "primary_bps": fnum(rows["primary"], "blocks/s"),
    }))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 perf-lab/tests/test_verdict.py -v`
Expected: `OK` (6 tests).

- [ ] **Step 5: Commit**

```bash
git add perf-lab/verdict.py perf-lab/tests/test_verdict.py
git commit -m "feat(perf-lab): verdict.py turns bench summaries into verdicts"
```

---

### Task 3: droplet.sh (tag-guarded doctl wrapper)

**Files:**
- Create: `perf-lab/droplet.sh`
- Test: `perf-lab/tests/doctl_stub.sh`

Conventions mirrored from `.github/workflows/zakura-pr-node.yml` (golden-image pick), `zakura-pr-node-bake.yml` (create flags, SSH wait), and `zakura-pr-node-reaper.yml` (age via JSON `created_at`; `doctl ... --format` has **no** creation column).

- [ ] **Step 1: Write the guard test (stub doctl)**

```bash
#!/usr/bin/env bash
# perf-lab/tests/doctl_stub.sh — records argv; fakes minimal JSON output.
echo "$@" >> "${DOCTL_LOG:?}"
case "$*" in
  *"droplet get"*)
    # simulate an UNTAGGED droplet named like ours
    echo '[{"id":123,"name":"perf-lab-x","tags":[]}]' ;;
  *"droplet list"*) echo '[]' ;;
  *) echo '[]' ;;
esac
```

Test (append to a new shell test, run inline — no framework):

```bash
chmod +x perf-lab/tests/doctl_stub.sh
DOCTL_LOG=$(mktemp)
if DOCTL_BIN=perf-lab/tests/doctl_stub.sh DOCTL_LOG=$DOCTL_LOG \
   bash perf-lab/droplet.sh destroy perf-lab-x; then
  echo "FAIL: destroy of untagged droplet must be refused"; exit 1
else
  echo "PASS: untagged destroy refused"
fi
```

Expected now: `bash: perf-lab/droplet.sh: No such file or directory` (fails).

- [ ] **Step 2: Implement `perf-lab/droplet.sh`**

```bash
#!/usr/bin/env bash
# perf-lab droplet lifecycle. Subcommands:
#   provision [suffix]   create+prepare a bench droplet (golden image if found)
#   ip NAME | ssh NAME [cmd...] | destroy NAME | reap | list
# Safety: destroy/reap only act on droplets named ${NAME_PREFIX}-* AND tagged
# ${PERF_TAG}. DRYRUN=1 prints mutating commands instead of running them.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
DOCTL="${DOCTL_BIN:-doctl}"
SSH="${SSH_BIN:-ssh}"
SSH_OPTS=(-i "$SSH_KEY_FILE" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)

die() { echo "droplet.sh: $*" >&2; exit 1; }
run() { if [ -n "${DRYRUN:-}" ]; then echo "DRYRUN: $*"; else "$@"; fi; }

ensure_key() {
  if [ ! -f "$SSH_KEY_FILE" ]; then
    run ssh-keygen -t ed25519 -N "" -C "$SSH_KEY_NAME" -f "$SSH_KEY_FILE"
  fi
  if ! $DOCTL compute ssh-key list --format Name --no-header | grep -qx "$SSH_KEY_NAME"; then
    run $DOCTL compute ssh-key import "$SSH_KEY_NAME" --public-key-file "$SSH_KEY_FILE.pub"
  fi
  # In real mode the run-wrapped keygen above guarantees the pubkey exists; in
  # DRYRUN it may not, so substitute a placeholder instead of failing.
  if [ -f "$SSH_KEY_FILE.pub" ]; then
    FP=$(ssh-keygen -lf "$SSH_KEY_FILE.pub" -E md5 | awk '{print $2}' | sed 's/^MD5://')
  elif [ -n "${DRYRUN:-}" ]; then
    FP="dryrun-fp-placeholder"
  else
    die "ssh public key missing after keygen: $SSH_KEY_FILE.pub"
  fi
}

golden_image() {  # newest zakura-pr-node-* image id, empty if none (pr-node recipe)
  $DOCTL compute image list-user --format ID,Name --no-header \
    | awk -v p="$GOLDEN_IMAGE_PREFIX" '$2 ~ "^"p {print $2, $1}' | sort | tail -1 | awk '{print $2}'
}

droplet_json() { $DOCTL compute droplet list --tag-name "$PERF_TAG" --output json; }
droplet_ip()  {
  droplet_json | python3 -c '
import json,sys
name=sys.argv[1]
for d in json.load(sys.stdin) or []:
    if d["name"]==name:
        print(next(n["ip_address"] for n in d["networks"]["v4"] if n["type"]=="public")); break
' "$1"
}

cmd_provision() {
  local name="${NAME_PREFIX}-${1:-$(date +%m%d%H%M)}"
  # hard rule (design §5): concurrent perf-lab droplets <= MAX_DROPLETS.
  # Checked before ensure_key so a refused provision has zero side effects.
  local count; count="$(droplet_json | python3 -c 'import json,sys; print(len(json.load(sys.stdin) or []))')"
  [ "$count" -lt "$MAX_DROPLETS" ] || die "refusing: $count perf-lab droplet(s) exist (MAX_DROPLETS=$MAX_DROPLETS)"
  ensure_key
  local image; image="$(golden_image)"
  if [ -n "$image" ]; then echo "using golden image $image"
  else image="$DO_FALLBACK_IMAGE"; echo "WARN: no ${GOLDEN_IMAGE_PREFIX}* image; falling back to $image (slow bootstrap)"; fi
  run $DOCTL compute droplet create "$name" \
    --region "$DO_REGION" --size "$DO_SIZE" --image "$image" \
    --ssh-keys "$FP" --tag-name "$PERF_TAG" \
    --wait --format ID,PublicIPv4 --no-header
  [ -n "${DRYRUN:-}" ] && return 0
  local ip; ip="$(droplet_ip "$name")"; [ -n "$ip" ] || die "no ip for $name"
  echo "waiting for ssh on $ip ..."
  for _ in $(seq 1 30); do
    $SSH "${SSH_OPTS[@]}" "root@$ip" true 2>/dev/null && break; sleep 10
  done
  $SSH "${SSH_OPTS[@]}" "root@$ip" true || die "ssh never came up on $ip"
  prepare_remote "$ip"
  echo "$name ready at $ip"
}

prepare_remote() {  # idempotent post-boot prep (golden image or fallback)
  local ip="$1"
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
# a fresh droplet runs apt at boot; wait for the lock (pr-node-bake gotcha)
for _ in \$(seq 1 120); do pgrep -x apt-get >/dev/null || break; sleep 5; done
if ! command -v cargo >/dev/null 2>&1; then   # fallback-image path only
  apt-get -o DPkg::Lock::Timeout=600 update -qq
  apt-get -o DPkg::Lock::Timeout=600 install -y -qq \
    build-essential clang pkg-config libssl-dev protobuf-compiler \
    git curl zstd jq python3
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
  ln -sf /root/.cargo/bin/cargo /root/.cargo/bin/rustc /root/.cargo/bin/rustup /usr/local/bin/
fi
# bench cache root + warm-cache symlinks onto the golden clone/target
mkdir -p ${BENCH_HOME_REMOTE}
[ -d ${GOLDEN_CLONE_REMOTE} ]  && ln -sfn ${GOLDEN_CLONE_REMOTE}  ${BENCH_HOME_REMOTE}/src
[ -d ${GOLDEN_TARGET_REMOTE} ] && ln -sfn ${GOLDEN_TARGET_REMOTE} ${BENCH_HOME_REMOTE}/build-target
[ -d /root/.cargo ]            && ln -sfn /root/.cargo            ${BENCH_HOME_REMOTE}/cargo-home
# pinned control clone the bench script RUNS from (BUILD_SRC gets checked out
# per-ref mid-run, so the script must not execute from BUILD_SRC itself)
if [ ! -d ${CTL_CLONE_REMOTE} ]; then
  git clone --depth 1 https://github.com/zakura-core/zakura.git ${CTL_CLONE_REMOTE}
else
  git -C ${CTL_CLONE_REMOTE} fetch --depth 1 origin main && git -C ${CTL_CLONE_REMOTE} checkout -f origin/main
fi
mkdir -p ${BENCH_OUT_REMOTE}
echo "remote prep done"
REMOTE
}

assert_ours() {  # refuse to touch anything not name-prefixed AND tagged
  local name="$1"
  case "$name" in "${NAME_PREFIX}"-*) ;; *) die "refusing: '$name' lacks prefix ${NAME_PREFIX}-";; esac
  droplet_json | python3 -c '
import json,sys
name=sys.argv[1]
ok=any(d["name"]==name for d in json.load(sys.stdin) or [])
sys.exit(0 if ok else 1)
' "$name" || die "refusing: '$name' is not tagged $PERF_TAG"
}

cmd_destroy() {
  local name="${1:?usage: droplet.sh destroy NAME}"
  assert_ours "$name"
  run $DOCTL compute droplet delete "$name" -f
  echo "destroyed $name"
}

cmd_reap() {  # delete tagged droplets older than REAP_MAX_AGE_HOURS (reaper recipe)
  local max=$((REAP_MAX_AGE_HOURS * 3600))
  droplet_json | python3 -c '
import json,sys,datetime
max_age=int(sys.argv[1]); now=datetime.datetime.now(datetime.timezone.utc)
for d in json.load(sys.stdin) or []:
    created=datetime.datetime.fromisoformat(d["created_at"].replace("Z","+00:00"))
    if (now-created).total_seconds() > max_age: print(d["name"])
' "$max" | while read -r name; do
    echo "reaping stale droplet $name"; cmd_destroy "$name"
  done
}

cmd_list() { droplet_json | python3 -c '
import json,sys
for d in json.load(sys.stdin) or []:
    ip=next((n["ip_address"] for n in d["networks"]["v4"] if n["type"]=="public"),"?")
    print(d["name"], ip, d["created_at"])'; }

cmd_ssh() { local name="${1:?}"; shift; local ip; ip="$(droplet_ip "$name")"
  [ -n "$ip" ] || die "no perf-lab droplet named $name"
  exec $SSH "${SSH_OPTS[@]}" "root@$ip" "$@"; }

case "${1:-}" in
  provision) shift; cmd_provision "$@";;
  ip)        shift; droplet_ip "${1:?}";;
  ssh)       shift; cmd_ssh "$@";;
  destroy)   shift; cmd_destroy "$@";;
  reap)      cmd_reap;;
  list)      cmd_list;;
  *) die "usage: droplet.sh provision|ip|ssh|destroy|reap|list";;
esac
```

- [ ] **Step 3: Run the guard test + shellcheck**

```bash
chmod +x perf-lab/droplet.sh perf-lab/tests/doctl_stub.sh
DOCTL_LOG=$(mktemp) DOCTL_BIN=perf-lab/tests/doctl_stub.sh bash perf-lab/droplet.sh destroy perf-lab-x \
  && echo "FAIL" || echo "PASS: untagged destroy refused"
DOCTL_LOG=$(mktemp) DOCTL_BIN=perf-lab/tests/doctl_stub.sh bash perf-lab/droplet.sh destroy other-droplet \
  && echo "FAIL" || echo "PASS: bad prefix refused"
DOCTL_LOG=$(mktemp) DOCTL_BIN=perf-lab/tests/doctl_stub.sh MAX_DROPLETS=0 \
  bash perf-lab/droplet.sh provision capped \
  && echo "FAIL" || echo "PASS: MAX_DROPLETS cap refused provision"
shellcheck perf-lab/droplet.sh perf-lab/tests/doctl_stub.sh
```

Expected: all three `PASS` lines (the stub's `droplet list` returns `[]`, so the tag check fails closed and, with `MAX_DROPLETS=0`, the cap check refuses); shellcheck clean (annotate any deliberate ignores inline).

- [ ] **Step 4: DRYRUN provision prints, creates nothing**

```bash
DRYRUN=1 DOCTL_BIN=perf-lab/tests/doctl_stub.sh DOCTL_LOG=$(mktemp) bash perf-lab/droplet.sh provision test 2>&1 | grep "DRYRUN: .*droplet create perf-lab-test"
```

Expected: exactly the create line, e.g. `DRYRUN: perf-lab/tests/doctl_stub.sh compute droplet create perf-lab-test --region nyc3 --size c-16 --image ubuntu-24-04-x64 --ssh-keys dryrun-fp-placeholder --tag-name zakura-perf-lab --wait --format ID,PublicIPv4 --no-header` (fallback image, since the stub reports no golden images; the placeholder fingerprint appears when no real key exists).

- [ ] **Step 5: Commit**

```bash
git add perf-lab/droplet.sh perf-lab/tests/doctl_stub.sh
git commit -m "feat(perf-lab): tag-guarded droplet lifecycle wrapper"
```

---### Task 4: bench.sh (start / status / collect)

**Files:**
- Create: `perf-lab/bench.sh`

Key facts encoded here: the bench script self-installs its apt deps, needs cargo, keeps caches in `BENCH_HOME`, and **must not be executed from `BENCH_HOME/src`** (it checks that clone out per-ref mid-run) — it runs from the pinned control clone instead. Both refs must run the **same** `p2p_stack` (the script's default baseline stack is `legacy`, which would measure the wrong thing for our A/B).

- [ ] **Step 1: Implement `perf-lab/bench.sh`**

```bash
#!/usr/bin/env bash
# One A/B bench on a perf-lab droplet, asynchronously:
#   bench.sh start   NAME LABEL BUILD_REF [BASELINE_REF=main] [EXTRA_ENV...]
#   bench.sh status  NAME LABEL          -> RUNNING | DONE:<exit> | ABSENT
#   bench.sh collect NAME LABEL          -> pulls artifacts, prints verdict JSON path
# EXTRA_ENV: KEY=VAL pairs passed to checkpoint-sync-bench.sh (e.g. CKPT_LIMIT=3000).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
SSH="${SSH_BIN:-ssh}"; SCP="${SCP_BIN:-scp}"
SSH_OPTS=(-i "$SSH_KEY_FILE" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
die() { echo "bench.sh: $*" >&2; exit 1; }

ip_of() { bash "$DIR/droplet.sh" ip "$1"; }

cmd_start() {
  local name="${1:?}" label="${2:?}" build_ref="${3:?}" baseline_ref="${4:-main}"; shift 4 || shift $#
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  local extra_env=("$@")
  # shellcheck disable=SC2087  # client-side expansion of label/refs is intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
# fresh per-label output: the bench script APPENDS to summary.md, so a stale
# same-label dir would leave two tables in one file
rm -rf ${BENCH_OUT_REMOTE}/${label} ${BENCH_OUT_REMOTE}/${label}.log ${BENCH_OUT_REMOTE}/${label}.pid
mkdir -p ${BENCH_OUT_REMOTE}/${label}
cd ${CTL_CLONE_REMOTE}
nohup env \
  BUILD_REF='${build_ref}' BASELINE_REF='${baseline_ref}' \
  TARGET_P2P_STACK=zakura BASELINE_P2P_STACK=zakura \
  BENCH_HOME='${BENCH_HOME_REMOTE}' \
  OUT_DIR='${BENCH_OUT_REMOTE}/${label}' DASHBOARD=1 ${extra_env[@]+${extra_env[@]}} \
  bash scripts/checkpoint-sync-bench.sh \
  > ${BENCH_OUT_REMOTE}/${label}.log 2>&1 < /dev/null &
echo \$! > ${BENCH_OUT_REMOTE}/${label}.pid
disown
REMOTE
  echo "started bench '$label' on $name (BUILD_REF=$build_ref vs BASELINE_REF=$baseline_ref)"
}

cmd_status() {
  local name="${1:?}" label="${2:?}"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
if [ ! -f ${BENCH_OUT_REMOTE}/${label}.pid ]; then echo ABSENT; exit 0; fi
pid=\$(cat ${BENCH_OUT_REMOTE}/${label}.pid)
if kill -0 "\$pid" 2>/dev/null; then echo RUNNING; else
  if [ -f ${BENCH_OUT_REMOTE}/${label}/summary.md ]; then echo DONE:0; else echo DONE:1; fi
fi
REMOTE
}

cmd_collect() {
  local name="${1:?}" label="${2:?}"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no droplet $name"
  local st; st="$(cmd_status "$name" "$label")"
  [ "${st#DONE}" != "$st" ] || die "bench '$label' is $st, not DONE"
  local dest="$ARTIFACT_ROOT/runs/$label"; mkdir -p "$dest"
  $SCP "${SSH_OPTS[@]}" -r "root@$ip:${BENCH_OUT_REMOTE}/${label}/." "$dest/"
  $SCP "${SSH_OPTS[@]}" "root@$ip:${BENCH_OUT_REMOTE}/${label}.log" "$dest/bench.log" || true
  [ -f "$dest/summary.md" ] || die "no summary.md in $dest — see $dest/bench.log ($st)"
  local band="${NOISE_BAND_PCT:-0}"
  python3 "$DIR/verdict.py" "$dest/summary.md" \
    --threshold-pct "$WIN_THRESHOLD_PCT" --noise-band-pct "${band:-0}" \
    > "$dest/verdict.json"
  echo "$dest/verdict.json"
  cat "$dest/verdict.json"
}

case "${1:-}" in
  start)   shift; cmd_start "$@";;
  status)  shift; cmd_status "$@";;
  collect) shift; cmd_collect "$@";;
  *) die "usage: bench.sh start|status|collect ...";;
esac
```

- [ ] **Step 2: shellcheck + stub-SSH smoke**

```bash
chmod +x perf-lab/bench.sh
shellcheck perf-lab/bench.sh
cat > /tmp/ssh_stub.sh <<'EOF'
#!/usr/bin/env bash
echo "SSH-CALL: $*" >&2; cat >/dev/null; echo RUNNING
EOF
chmod +x /tmp/ssh_stub.sh
cat > /tmp/doctl_ip_stub.sh <<'EOF'
#!/usr/bin/env bash
case "$*" in *"droplet list"*) echo '[{"id":1,"name":"perf-lab-t","tags":["zakura-perf-lab"],"created_at":"2026-07-20T00:00:00Z","networks":{"v4":[{"type":"public","ip_address":"203.0.113.9"}]}}]' ;; *) echo '[]';; esac
EOF
chmod +x /tmp/doctl_ip_stub.sh
DOCTL_BIN=/tmp/doctl_ip_stub.sh SSH_BIN=/tmp/ssh_stub.sh \
  bash perf-lab/bench.sh status perf-lab-t lab1
```

Expected: `RUNNING` (and an `SSH-CALL: … root@203.0.113.9 …` line on stderr). Shellcheck clean.

- [ ] **Step 3: Commit**

```bash
git add perf-lab/bench.sh
git commit -m "feat(perf-lab): async A/B bench driver over ssh"
```

---

### Task 5: gates.sh + permission allowlist

**Files:**
- Create: `perf-lab/gates.sh`
- Create: `.claude/settings.local.json` (in the worktree root; **not** committed — verify it is gitignored)

- [ ] **Step 1: Implement `perf-lab/gates.sh`**

```bash
#!/usr/bin/env bash
# Local gates for an experiment worktree.
#   gates.sh l0 WORKTREE_DIR CRATE [CRATE...]   fmt + clippy + targeted tests
#   gates.sh micro-mockbs WORKTREE_DIR [RUNS]   mock-blocksync throughput samples
# Uses a shared CARGO_TARGET_DIR so experiment worktrees build incrementally.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
export CARGO_TARGET_DIR="$ARTIFACT_ROOT/target"
die() { echo "gates.sh: $*" >&2; exit 1; }

cmd_l0() {
  local wt="${1:?}"; shift; [ $# -ge 1 ] || die "l0 needs at least one crate"
  cd "$wt"
  cargo fmt --all -- --check
  for c in "$@"; do cargo clippy -p "$c" --all-targets -- -D warnings; done
  if command -v cargo-nextest >/dev/null 2>&1; then
    for c in "$@"; do cargo nextest run -p "$c" --no-fail-fast; done
  else
    for c in "$@"; do cargo test -p "$c"; done
  fi
  echo "L0 PASS ($*)"
}

cmd_micro_mockbs() {
  local wt="${1:?}" runs="${2:-3}"
  cd "$wt"
  for i in $(seq 1 "$runs"); do
    ZAKURA_MOCK_BS_RUN=1 cargo test -p zakura-network --release \
      zakura_mock_blocksync_throughput -- --ignored --nocapture 2>/dev/null \
      | grep -E "^throughput:" | sed "s/^/run $i /"
  done
}

case "${1:-}" in
  l0)           shift; cmd_l0 "$@";;
  micro-mockbs) shift; cmd_micro_mockbs "$@";;
  *) die "usage: gates.sh l0|micro-mockbs ...";;
esac
```

- [ ] **Step 2: shellcheck + run L0 against the clean worktree**

```bash
chmod +x perf-lab/gates.sh
shellcheck perf-lab/gates.sh
bash perf-lab/gates.sh l0 "$PWD" zakura-chain 2>&1 | tail -3
```

Expected: ends with `L0 PASS (zakura-chain)` (first clippy build is slow; later runs are incremental). If `cargo fmt` fails on a clean origin/main worktree, report it — that means upstream is unformatted and the L0 gate must scope fmt to changed files instead (`cargo fmt -p <crate> -- --check`); adjust and note in README.

- [ ] **Step 3: Write `.claude/settings.local.json`** (allowlist so overnight runs never stall on prompts)

```json
{
  "permissions": {
    "allow": [
      "Bash(perf-lab/droplet.sh:*)",
      "Bash(bash perf-lab/droplet.sh:*)",
      "Bash(perf-lab/bench.sh:*)",
      "Bash(bash perf-lab/bench.sh:*)",
      "Bash(perf-lab/gates.sh:*)",
      "Bash(bash perf-lab/gates.sh:*)",
      "Bash(python3 perf-lab/verdict.py:*)",
      "Bash(python3 perf-lab/tests/test_verdict.py:*)",
      "Bash(doctl compute droplet list:*)",
      "Bash(doctl compute image list-user:*)",
      "Bash(doctl compute ssh-key list:*)",
      "Bash(ssh -i ~/.ssh/perf-lab-claude:*)",
      "Bash(scp -i ~/.ssh/perf-lab-claude:*)",
      "Bash(git fetch:*)",
      "Bash(git worktree:*)",
      "Bash(git push origin adam/perf-exp/:*)",
      "Bash(git push origin adam/zakura-agentic-perf-5667cc:*)",
      "Bash(git push origin --delete adam/perf-exp/:*)",
      "Bash(gh pr list:*)",
      "Bash(gh run list:*)",
      "Bash(gh run view:*)",
      "Bash(gh run download:*)",
      "Bash(gh workflow run zakura-e2e.yml:*)",
      "Bash(gh workflow run checkpoint-sync-bench.yml:*)",
      "Bash(cargo fmt:*)",
      "Bash(cargo clippy:*)",
      "Bash(cargo check:*)",
      "Bash(cargo build:*)",
      "Bash(cargo test:*)",
      "Bash(cargo nextest:*)",
      "Bash(cargo bench:*)",
      "Bash(codex exec:*)",
      "Bash(shellcheck:*)"
    ],
    "deny": [
      "Bash(doctl compute droplet delete:*)",
      "Bash(git push origin main:*)",
      "Bash(git push --force:*)"
    ]
  }
}
```

Note: raw `doctl … delete` is denied on purpose — deletion goes through `droplet.sh destroy`, which enforces the tag+prefix guard. Verify the file is ignored: `git check-ignore .claude/settings.local.json` prints the path (if it does not, add it to `.git/info/exclude`, not the repo `.gitignore`).

- [ ] **Step 4: Capture the Mac mock-blocksync baseline** (spec Phase 0 item; criterion needs no standing baseline — L1 compares base-vs-head per experiment via critcmp)

```bash
bash perf-lab/gates.sh micro-mockbs "$PWD" 3
```

Expected: three `run N throughput: X blocks/sec, Y MiB/sec …` lines (~2–5 min each after the first build). Record the three numbers in `perf-lab/README.md` under `## Measured timings` as the Mac mock-blocksync baseline.

- [ ] **Step 5: Commit (gates.sh + README only)**

```bash
git add perf-lab/gates.sh perf-lab/README.md
git commit -m "feat(perf-lab): local L0 gates and mock-blocksync pre-filter"
```

---

### Task 6: Live droplet smoke (provision → verify → destroy)

Real cloud actions; cost ≈ $0.50. Uses the golden image if present.

- [ ] **Step 1: Reap check + provision**

```bash
bash perf-lab/droplet.sh reap          # expect: no output (nothing stale)
bash perf-lab/droplet.sh provision smoke
```

Expected: `using golden image zakura-pr-node-…` (or the WARN + fallback path), a droplet ID/IP line, `waiting for ssh…`, `remote prep done`, `perf-lab-smoke ready at <ip>`. Record wall time in README later.

- [ ] **Step 2: Verify the warm cache actually took**

```bash
bash perf-lab/droplet.sh ssh perf-lab-smoke \
  'command -v cargo && ls -ld /opt/zakura-bench/src /opt/zakura-bench/build-target /root/zakura-ctl && df -B1G --output=avail /opt/zakura-bench | tail -1'
```

Expected: a cargo path; two symlinks pointing at `/root/zakura` and `/root/cargo-target` (golden image) or real dirs absent + WARN noted (fallback); the control clone present; ≥ 45 GiB free (the bench script's own floor). If free space < 45, bump `DO_SIZE` in config.env and redo this task.

- [ ] **Step 3: Destroy + confirm gone**

```bash
bash perf-lab/droplet.sh destroy perf-lab-smoke
bash perf-lab/droplet.sh list          # expect: empty
```

- [ ] **Step 4: Record timings in `perf-lab/README.md`** (append a `## Measured timings` section with provision→ready wall time and image used) and commit:

```bash
git add perf-lab/README.md
git commit -m "docs(perf-lab): record droplet smoke timings"
```

---

### Task 7: A/A calibration (noise band)

Real bench runs: 1 droplet ≈ 2–3 h total. Both runs build the same `main` SHA (second resolve hits the per-SHA binary cache; both syncs still execute).

- [ ] **Step 1: Provision + start the A/A pass**

```bash
bash perf-lab/droplet.sh provision cal
bash perf-lab/bench.sh start perf-lab-cal aa1 main main
```

Expected: `started bench 'aa1' …`. First run downloads the ~30 GiB snapshot into `BENCH_HOME` (one-time per droplet) and builds `main` (incremental if golden image). Poll every ~10 min:

```bash
bash perf-lab/bench.sh status perf-lab-cal aa1     # RUNNING … then DONE:0
```

- [ ] **Step 2: Collect + validate the fixture against reality**

```bash
bash perf-lab/bench.sh collect perf-lab-cal aa1
diff <(head -12 ~/zakura-perf-lab/runs/aa1/summary.md) /dev/null || true
```

Expected: verdict JSON prints with `"mode": "ab"`. Open `~/zakura-perf-lab/runs/aa1/summary.md`; if its table header/rows differ from the Task 2 fixture, update the fixture + parser now and re-run `python3 perf-lab/tests/test_verdict.py`.

- [ ] **Step 3: Second A/A pass (sturdier band), then compute the band**

```bash
bash perf-lab/bench.sh start perf-lab-cal aa2 main main
# … poll status; when DONE:
bash perf-lab/bench.sh collect perf-lab-cal aa2
python3 perf-lab/verdict.py ~/zakura-perf-lab/runs/aa1/summary.md --aa
python3 perf-lab/verdict.py ~/zakura-perf-lab/runs/aa2/summary.md --aa
```

Take `NOISE_BAND_PCT = max(observed_noise_pct of aa1, aa2)`, write it into `perf-lab/config.env` and `state.json.noise_band_pct`.

- [ ] **Step 4: Destroy the droplet, ledger the session, commit**

```bash
bash perf-lab/droplet.sh destroy perf-lab-cal
```

Append to `perf-lab/LEDGER.md`: a `## SESSION 0 — calibration` entry with both A/A deltas, the chosen band, snapshot/build timings, and droplet cost. Then:

```bash
git add perf-lab/config.env perf-lab/state.json perf-lab/LEDGER.md
git commit -m "feat(perf-lab): A/A noise-band calibration"
```

---

### Task 8: The orchestrator playbook (SKILL.md)

**Files:**
- Create: `.agents/skills/perf-lab/SKILL.md`

- [ ] **Step 1: Write the skill**

```markdown
---
name: perf-lab
description: Run or resume the unattended Zakura perf-experiment loop — provision perf-lab droplets, pick experiments from perf-lab/BACKLOG.md, gate + A/B-bench them, and record verdicts in perf-lab/LEDGER.md. Use when asked to "run the perf lab", "continue the perf loop", or "find perf wins".
---

# perf-lab orchestrator

Design + resolved decisions: `docs/superpowers/2026-07-20-agentic-perf-workflow-design.md`.
All primitives live in `perf-lab/` (see its README). You are the state machine;
the scripts are deliberately dumb.

## Session start (every time, in order)

1. `git fetch origin main` — never trust local refs (they run ~hundreds of
   commits stale in this clone).
2. `bash perf-lab/droplet.sh reap` then `list` — kill stale droplets, note any
   reaped in the ledger as an incident.
3. Refresh exclusions: `gh pr list --limit 50` → update BACKLOG.md's
   Exclusions section; drop/block any backlog item that now collides.
4. Read `perf-lab/state.json`, `LEDGER.md` tail, `BACKLOG.md`. Resume any
   `in_flight` bench first (status → collect → verdict → ledger).
5. Append `## SESSION N` to LEDGER.md (date, origin/main SHA, plan for the
   session). Commit ledger updates as you go:
   `git add perf-lab && git commit -m "perf-lab: session N ledger"` and
   `git push origin adam/zakura-agentic-perf-5667cc`.
6. `bash perf-lab/droplet.sh provision s<N>` (one droplet; a second only when
   two L2-ready experiments are queued and MAX_DROPLETS allows).

## Campaign-target memo (once, before EXP-001)

Run a baseline bench (`bench.sh start <droplet> base main main` reuses A/A
artifacts if fresh), read `verdict-*.json` + summary, skim
`analysis/zakura_trace_analysis/` output if deeper attribution is needed, and
write `## CAMPAIGN` in the ledger: dominant bottleneck class, chosen target
metric (default: checkpoint-zone post-commit blk/s), re-ranked top-5 backlog.

## Per experiment (state machine)

1. **Pick** the top READY backlog item compatible with the exclusions.
   Allocate `EXP-NNN` from state.json; risk-class it (spec §5). Red → write a
   PROPOSAL ledger entry, mark backlog DROPPED(red-proposal), next item.
2. **Branch**: `git worktree add /tmp/perf-exp-NNN origin/main -b adam/perf-exp/NNN-<slug>`.
3. **Implement** the minimal diff. Mechanical + fully specced → delegate to
   codex per ~/.claude/CLAUDE.md's delegation table; consensus-adjacent stays
   here. Archive the diff before L2:
   `mkdir -p ~/zakura-perf-lab/runs/expNNN && git -C /tmp/perf-exp-NNN diff origin/main > ~/zakura-perf-lab/runs/expNNN/exp.patch`.
4. **L0**: `bash perf-lab/gates.sh l0 /tmp/perf-exp-NNN <touched crates>`.
   Fail twice → BROKEN ledger entry, delete worktree+branch, next.
5. **L1** (only if a micro lane applies): `gates.sh micro-mockbs` for
   block-sync-layer diffs (3 runs each side; kill on clear regression) or
   `cargo bench -p <crate>` + critcmp for crypto/serialization diffs.
6. **L2**: `git push origin adam/perf-exp/NNN-<slug>`, then
   `bash perf-lab/bench.sh start <droplet> expNNN adam/perf-exp/NNN-<slug> main`
   (env-var experiments skip the branch: pass `CKPT_LIMIT=… `-style args and
   bench `main main`). While it runs (~60–90 min), implement the next
   experiment. Poll `bench.sh status` on wakeups. If the droplet lane is
   broken (provision or bench fails twice), fall back to the shared runner:
   `gh workflow run checkpoint-sync-bench.yml -f build_ref=<branch> -f baseline_ref=main`,
   poll `gh run list --workflow=checkpoint-sync-bench.yml`, then
   `gh run download <id>`. Caveat: inside Actions the script writes its table
   to `GITHUB_STEP_SUMMARY`, so the artifact may lack `summary.md` — if so,
   derive post-commit blk/s from each `samples-*.csv` (height delta ÷ elapsed
   after the first height increase) and record the verdict as PROMISING at
   most; confirm on a recovered droplet before calling any fallback result a
   WIN.
7. **Verdict**: `bench.sh collect` → verdict.json.
   - WIN_CANDIDATE → one confirmation run (same refs). Two above-threshold
     runs = **WIN**: run full workspace tests in the worktree
     (`cargo nextest run --profile all-tests` if feasible, else targeted +
     build), keep the branch, ledger with simplicity score. Yellow-class wins
     additionally: `gh workflow run zakura-e2e.yml --ref adam/perf-exp/NNN-…`
     and require green before final WIN.
   - NEUTRAL/LOSS → ledger, `git push origin --delete adam/perf-exp/NNN-…`,
     remove worktree (patch already archived).
8. **Report**: regenerate REPORT.md (baseline, wins ranked by delta ×
   simplicity, promising, proposals, incidents, spend). Commit + push the
   orchestration branch.

## state.json protocol (crash recovery depends on this)

Write `perf-lab/state.json` at every transition and commit it with the ledger:

- session start: set `session` = N; `droplets` gains
  `{"<name>": {"ip": "...", "created_at": "..."}}` on every provision, entry
  removed on destroy/reap.
- `bench.sh start` fired: `in_flight["<label>"] = {"droplet": "...",
  "build_ref": "...", "baseline_ref": "...", "exp": "EXP-NNN",
  "started_at": "..."}`.
- `bench.sh collect` done (or the run abandoned): delete `in_flight["<label>"]`
  and increment `batch_runs_used` (reset to 0 at each batch boundary).
- experiment id allocated: increment `next_exp_id`.
- calibration: `noise_band_pct` mirrors config.env.

A fresh session must be able to reconstruct everything it needs from
state.json + LEDGER.md alone.

## Budget & halts (D3/D6)

- `BATCH_SIZE=8` bench runs per batch; at each boundary write `## BATCH`
  (runs, wins, spend) and continue automatically.
- Halt (destroy droplets, final REPORT) when: a full batch has zero
  WIN_CANDIDATEs AND no READY backlog item's expected value clears the
  threshold; or on harness breakage twice in a row; or when Adam says stop.
- Never leave a droplet up while no bench is running or imminent.

## Safety rails (hard)

- Only `droplet.sh` touches DO, only on `perf-lab-*` + tag `zakura-perf-lab`.
- Never push to main/feat/release; never open PRs; never dispatch deploy or
  release workflows; never edit `deploy/`, `.github/workflows/`, checkpoint
  files, or dependency versions inside an experiment.
- Both bench refs always run `p2p_stack=zakura` (bench.sh enforces).
- Ledger is the only reporting channel. No Slack, no notifications, no PRs.
```

- [ ] **Step 2: Commit**

```bash
git add .agents/skills/perf-lab/SKILL.md
git commit -m "feat(perf-lab): orchestrator playbook skill"
```

---

### Task 9: EXP-000 end-to-end dry run

Exercises the full state machine with a no-op diff (comment-only change), expecting NEUTRAL. Real droplet + 1 bench run (~1.5 h, ~$1).

- [ ] **Step 1: Create the no-op experiment branch**

```bash
git worktree add /tmp/perf-exp-000 origin/main -b adam/perf-exp/000-noop-dry-run
cd /tmp/perf-exp-000
printf '\n// perf-lab EXP-000 dry-run marker (no functional change)\n' >> zakura-utils/src/lib.rs
git add -A && git commit -m "test(perf-lab): EXP-000 no-op dry-run marker"
```

- [ ] **Step 2: Gates + push + bench**

```bash
cd /Users/czar/Documents/zakura/.claude/worktrees/zakura-sighash-optimization-5328f4
bash perf-lab/gates.sh l0 /tmp/perf-exp-000 zakura-utils
git -C /tmp/perf-exp-000 push origin adam/perf-exp/000-noop-dry-run
bash perf-lab/droplet.sh provision dry
bash perf-lab/bench.sh start perf-lab-dry exp000 adam/perf-exp/000-noop-dry-run main
# poll: bash perf-lab/bench.sh status perf-lab-dry exp000  → DONE:0
bash perf-lab/bench.sh collect perf-lab-dry exp000
```

Expected: verdict `NEUTRAL` with `|delta_pct|` ≤ the calibrated band (a no-op diff cannot be a real win; if it reports WIN_CANDIDATE, the band is too small — recalibrate Task 7 with a third A/A run).

- [ ] **Step 3: Ledger + cleanup**

Append `## EXP-000 noop-dry-run` to LEDGER.md following the template (verdict NEUTRAL, note "state machine validated"). Then:

```bash
git push origin --delete adam/perf-exp/000-noop-dry-run
git worktree remove /tmp/perf-exp-000 --force
git branch -D adam/perf-exp/000-noop-dry-run
bash perf-lab/droplet.sh destroy perf-lab-dry
git add perf-lab/LEDGER.md && git commit -m "perf-lab: EXP-000 dry-run verdict (state machine validated)"
```

---

### Task 10: Final review + operating notes

- [ ] **Step 1: Self-review the tooling against the spec** — walk spec §3–§7 and confirm each requirement maps to a script/skill feature (droplet lifecycle §3 ↔ droplet.sh; measurement §4 ↔ bench.sh+verdict.py+calibration; safety §5 ↔ guards+allowlist+SKILL rails; ideas §6 ↔ BACKLOG; reporting §7 ↔ LEDGER/REPORT+SKILL step 8). Fix gaps inline.

- [ ] **Step 2: Update `perf-lab/README.md`** with the "start a session" one-liner (invoke the `perf-lab` skill), measured timings from Tasks 6–9, the calibrated noise band, and one-line bullets for `state.json`, `BACKLOG.md`, `LEDGER.md`, and `REPORT.md` (the scaffold README omits them).

- [ ] **Step 3: Commit + push the orchestration branch**

```bash
git add perf-lab/ docs/superpowers/
git commit -m "docs(perf-lab): operating notes and measured timings"
git push origin adam/zakura-agentic-perf-5667cc
```

Phase 1 exit criterion (spec §8): the next overnight session — started by invoking the `perf-lab` skill — produces ≥3 completed verdicts in the morning REPORT.md.
