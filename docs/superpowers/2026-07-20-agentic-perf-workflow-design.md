# Agentic sync-performance workflow — design

Date: 2026-07-20
Status: accepted 2026-07-20 (decisions D1–D7 resolved by Adam; bench lane
amended per D4 to self-provisioned DigitalOcean droplets)
Branch: `adam/zakura-agentic-perf-5667cc`

## 1. Goal

An autonomous loop that Adam can start and leave for hours. It proposes low-risk
performance changes to Zakura, implements each one on its own branch, validates
correctness, measures real sync throughput A/B against baseline, and reports
findings incrementally. Humans review everything before anything merges.

Non-goals:

- No autonomous merges, PRs, deploys, or fleet operations of any kind.
- No autonomous changes to consensus rules, cryptography, checkpoint lists,
  chain parameters, DB formats, or wire formats (it may *propose* these as
  written analyses, never implement them unsupervised).
- Not a replacement for the nightly e2e, continuous-sync fleet, or human-driven
  deep optimization work — it is a screening harness that finds and validates
  candidates cheaply.

## 2. What already exists (the loop orchestrates, it does not rebuild)

Measurement and validation infrastructure already on `origin/main`, confirmed
2026-07-20:

| Asset | What it gives the loop |
| --- | --- |
| `scripts/checkpoint-sync-bench.sh` | The authoritative macro benchmark, self-contained ("can be run by hand on any Linux box"). Env-driven: `BUILD_REF`/`BASELINE_REF` build both refs on the host (cached by commit SHA), each syncs a hard-link-forked ~1.7M-height snapshot forward (`STOP_HEIGHT` default 1,737,210 = +30k, `WALL_CAP` 2000 s), **pinned to a single feed peer by default** (`FEED_PEER` preset, `PEERSET_SIZE=1`). Emits `summary.md` (blocks/s, post-first-commit blocks/s, A/B speedup), `samples-*.csv`, and — with `DASHBOARD=1` (default) — a per-run bottleneck **verdict JSON** (commit/download/verify classification). |
| `.github/workflows/checkpoint-sync-bench.yml` | The same script as a `workflow_dispatch` on the shared self-hosted `zakura-bench` runner. Kept as the **fallback lane** when the droplet lane is unavailable. |
| `zakura-pr-node-bake.yml` / `zakura-pr-node.yml` / `zakura-pr-node-reaper.yml` + `.github/workflows/scripts/pr-node-*.sh` | The repo's established **ephemeral DigitalOcean lifecycle pattern**: doctl-created droplets/volumes, baked snapshots, reaper cleanup. The perf-lab droplet tooling extends this pattern rather than inventing one. |
| `zakura_mock_blocksync_throughput` (`zakura-network/src/zakura/testkit/mock_blocksync.rs`) | macOS-native local throughput harness: 4 seed nodes + 1 leecher over real loopback QUIC, synthetic corpus (default 100k blocks), prints blocks/s + MiB/s. Env-tunable (`ZAKURA_MOCK_BS_*`). Fast pre-filter for network/block-sync-layer changes. |
| Criterion benches + `.github/workflows/benchmarks.yml` | Micro A/B for crypto verifiers (`groth16`, `halo2`, `sapling`, `redpallas`) and serialization (`block`, `transaction`); `critcmp` comparison exists in CI (PR label `C-benchmark`) and locally. |
| `.github/workflows/zakura-e2e.yml` | Correctness at the protocol level: `pr-gate` mode (~6 min, auto on PRs touching sync paths) and dispatchable long modes (`checkpoint-long`, `no-checkpoint-long`, `restart-matrix`) with the JSONL `trace_oracle.py` (includes commit-latency ceiling and slowdown-trend checks). |
| `tests-unit.yml` lane (`cargo nextest run --profile all-tests --run-ignored=all`) | The unit/property correctness bar, reproducible locally. |
| the `zakura-trace-plots` skill (`.agents/skills/zakura-trace-plots/scripts/plot_zakura_traces.py`) | Post-hoc bottleneck attribution from JSONL traces (blocking-class classification: `hol_gap`, `commit_backpressure`, `download_starved`, …) — deeper drill-down behind the bench's verdict JSON. |
| `deploy/runner/` cohort harness (`make perf-*`) | Deterministic isolated-cohort bench with `commit-metrics` per-stage attribution; host-side state lives on a shared bench host. Superseded as a lane by the droplet decision (D4); its frozen-cohort determinism pattern can be ported to perf-lab droplets later if live-peer variance ever becomes the limiting factor. |

Environment facts: `doctl` is authenticated on this Mac (Valargroup team,
droplet limit 100) and agent-created SSH keys are established practice in the
account. Known gaps the loop must respect: no deterministic full-verification
replay bench exists; Zakura v2 sync speed is JSONL-only (not Prometheus); the
Mac has no synced state and Docker-on-Mac e2e is unfaithful (debug via CI
artifacts, never a local docker stack).

## 3. Architecture

Three candidate shapes were considered; the resolved design is A with C's
lanes embedded, and with A's measurement substrate being self-provisioned
droplets per D4:

- **A. Orchestrator + remote bench (chosen).** Orchestrator session on the
  Mac; fast gates run locally (fmt/clippy/unit tests, criterion,
  mock-blocksync); the authoritative A/B verdict comes from running
  `checkpoint-sync-bench.sh` on **ephemeral perf-lab DigitalOcean droplets**
  the loop provisions and destroys itself (doctl from the Mac, SSH to drive
  the script, artifacts pulled back). The `checkpoint-sync-bench.yml` dispatch
  on the shared runner is the fallback when the droplet lane is down.
- **B. Shared bench-host loop (rejected per D4).** Driving the cohort harness
  or the `zakura-bench` runner exclusively means contending with humans on
  shared hardware. Replaced by droplets we own; the GH dispatch survives only
  as fallback.
- **C. Mac-only loop (insufficient alone).** Criterion + mock-blocksync +
  local regtest only. macOS ≠ production Linux and there is no local
  full-pipeline macro signal, so it cannot produce trustworthy verdicts alone.
  Its lanes are the inner pre-filters of A.

### The loop, per experiment

```text
pick next idea from BACKLOG (ranked by expected-value × confidence ÷ cost)
  → new worktree, branch adam/perf-exp/NNN-slug cut from freshly fetched
    origin/main (never local refs — see §10)
  → implement minimal diff (delegate mechanical diffs to codex; consensus-
    adjacent stays with the orchestrator model)
  → L0: cargo fmt --check, clippy -D warnings, targeted crate tests   [minutes]
  → L1: relevant micro lane, only when one applies:
        criterion A/B via critcmp, or mock-blocksync A/B (3 runs each) [minutes]
        — kill experiments that regress micro signals; pass-through those
          without a relevant micro lane
  → push branch; on the perf-lab droplet run
      BUILD_REF=<branch> BASELINE_REF=main checkpoint-sync-bench.sh
    over SSH; pull bench-out/ (summary.md, verdict-*.json, CSVs)     [~1–1.5 h]
  → verdict (see §4) → record in LEDGER; update REPORT
  → WIN: keep branch, save patch + artifacts, full workspace test lane.
         Yellow-class wins also dispatch zakura-e2e.yml (checkpoint-long)
         on the branch before being called validated.
  → LOSS/NEUTRAL: record numbers, delete branch (patch preserved in ledger).
  → BROKEN (build/test failure twice): park with error snippet, move on.
```

While an L2 bench run is in flight (the long pole), the orchestrator
implements and L0/L1-gates the *next* experiment — the pipeline stays full.
Up to two droplets may run L2 benches concurrently (each run is a
self-contained A/B on one droplet, so cross-droplet variance never enters a
comparison).

### Perf-lab droplet lifecycle

- **Provision per session, not per run**: at loop start, create one droplet
  (dedicated-CPU class, size/region fixed in `perf-lab/config.env`). Droplets
  boot from the newest `zakura-pr-node-*` **golden image** (baked by
  `zakura-pr-node-bake.yml`: build deps, rustup on the default PATH, repo
  clone at `/root/zakura`, warm release cargo cache), falling back to
  `ubuntu-24-04-x64` + a bootstrap script when no image exists. Provisioning
  symlinks the bench script's `BENCH_HOME` build dirs onto the golden
  clone/cache so even the first experiment build is incremental; the state
  snapshot downloads once per droplet into `BENCH_HOME`. The droplet is
  reused across the whole session's experiments. A second droplet is added
  only when two L2-ready experiments are queued.
- **Naming/tagging**: every loop-created resource is named `perf-lab-*` and
  tagged `zakura-perf-lab`. A dedicated SSH key (`perf-lab-claude`) is created
  once in Phase 0.
- **Teardown**: droplets are destroyed at session end; a session-start sweep
  destroys any `zakura-perf-lab`-tagged resource older than 24 h (same reaper
  idea as `zakura-pr-node-reaper.yml`).
- **Cost**: one dedicated-CPU droplet ≈ $0.5–1/h → a 12 h overnight session
  ≈ $6–12/droplet. Recorded per session in the ledger.

### Session mechanics

- The orchestrator is a Claude Code session on the Mac using scheduled wakeups
  while benches run remotely; long-running local commands run as background
  tasks. Nothing depends on the session staying "awake" — all state lives on
  disk.
- **Resumable state**: `perf-lab/state.json` (current experiment, phase,
  droplet IDs, in-flight bench labels), `perf-lab/BACKLOG.md`,
  `perf-lab/LEDGER.md`, `perf-lab/REPORT.md`, all committed on the
  orchestration branch (`adam/zakura-agentic-perf-5667cc`). Any fresh session
  resumes by reading these. Bulk artifacts (CSVs, logs, verdicts) go to
  `~/zakura-perf-lab/` outside the repo.
- **Permissions**: the loop needs pre-approved allowlist entries for the exact
  command shapes it uses (`doctl compute … `, `ssh`/`scp` to perf-lab
  droplets, `gh workflow run/list/view/download` for the fallback and e2e
  lanes, `git push origin adam/perf-exp/*`, cargo build/test/bench, codex
  exec) so it never stalls on a prompt overnight. Set up once in Phase 0.
- **Model split** (per Adam's delegation table): orchestration, risk
  classification, and consensus-adjacent diffs → fable-5; mechanical
  implementation with a written spec → codex gpt-5.5; review of winning diffs
  → the `zcash-reviewer` skill before Adam sees them in the ledger.

## 4. Measurement protocol

- **Noise floor first.** Phase 0 runs an A/A pass (`BUILD_REF = BASELINE_REF =
  main`) on the perf-lab droplet. The observed A/A delta defines the noise
  band; the win threshold is `max(3%, 2 × observed A/A delta)`. The gating
  metric is the harness's **post-first-commit blocks/s** (it excludes
  startup/restore cost); plain whole-run blocks/s is recorded as a secondary
  signal. A/A re-runs when the droplet config changes and weekly otherwise.
- **Determinism defaults.** Driving the script directly uses its own defaults:
  pinned single feed peer (`PEERSET_SIZE=1`), fixed stop height, same host for
  both refs in one invocation — shared conditions by construction.
- **Confirmation.** A candidate above threshold gets one repeat run before
  being declared a WIN (2 runs = 4 syncs). Single-run results are recorded as
  PROMISING, never WIN.
- **Secondary signals**, recorded but not gating: whole-run blocks/s, CSV
  height-over-time shape (no stall cliffs), peak memory if present in logs,
  micro-lane deltas.
- **Attribution.** Every run's `verdict-*.json` (commit/download/verify
  bottleneck classification) is logged in the ledger; JSONL traces go through
  the existing analyzers when deeper drill-down is needed. The dominant
  blocking class ranks the next ideas (e.g. commit-bound → prioritize state/
  commit experiments over network ones).

## 5. Safety model

Risk classes decide what the loop may do autonomously:

- **Green — implement freely.** Config/tuning constants (channel capacities,
  buffer/batch sizes, concurrency limits, timeouts), allocation/clone
  reduction in non-consensus paths, RocksDB options that don't change the
  format, metrics/tracing overhead, lock scoping, `spawn_blocking` placement,
  tokio runtime tuning.
- **Yellow — implement with extra gates.** Restructuring inside the sync
  reactor, checkpoint pipeline, or state-write path that preserves semantics.
  Extra gates: full workspace test lane locally + `zakura-integration` nextest
  profile + a dispatched `zakura-e2e.yml` long mode must pass on the branch
  before a WIN is final.
- **Red — propose only.** Anything touching consensus semantics, cryptographic
  verification internals, ZIP-244/sighash logic, checkpoint lists, chain
  params, DB format/migrations, wire formats. The loop writes a ledger entry
  with analysis and expected win, and stops. Adam can hand one back as an
  explicitly scoped task later.

Hard rules regardless of class:

- Never push to `main`/`feat/**`/`release/**`; never open PRs (D2); never
  dispatch deploy/release workflows; never touch fleet nodes, PR-node
  resources, or any DO resource not tagged `zakura-perf-lab`.
- DO actions are restricted to: create/destroy/list droplets and volumes
  named `perf-lab-*` + tagged `zakura-perf-lab`, and the one-time
  `perf-lab-claude` key creation. Concurrent perf-lab droplets ≤ 3 (raised
  from 2 by Adam, 2026-07-22, to admit the B-15 frozen-cohort port: two
  seeded-then-frozen serving droplets plus one bench).
- Never modify `deploy/`, `.github/workflows/`, checkpoint files, or bump
  dependencies in `Cargo.lock` as part of an experiment.
- Bench budget runs in batches of 8 L2 runs (D6): at each batch boundary the
  loop writes a batch summary to the ledger and continues with the next batch
  automatically. It halts instead if the win rate is 0 for a full batch and
  the backlog holds nothing above expected-value threshold — that's a signal
  to stop burning droplet-hours and wait for Adam.

Repo-policy compliance: nothing the loop produces is a PR until Adam reviews;
any PR he opens from a winning branch carries the standard AI disclosure and
test evidence (the ledger entry is the test evidence).

## 6. Idea generation and first-campaign targeting

Per D7, choosing what to attack is the loop's job, evidence-first:

- **Campaign-target memo (Phase 1, before experiment 001).** Run the baseline
  bench with `DASHBOARD=1`, read the verdict JSON + trace attribution, and
  cross-check against a profiling pass and the coverage map below. The memo —
  first entry in the ledger — names the dominant bottleneck class, the
  subsystems worth attacking first, and the expected ceiling of each. Working
  hypothesis going in: checkpoint-zone sync throughput is the right first
  campaign (checkpoints extend near the tip, so the checkpoint zone dominates
  full-sync wall time, and it is exactly what the bench measures) — but the
  memo must confirm or overturn this with data, and gets refreshed whenever
  the dominant blocking class changes.

Ongoing sources, in priority order:

1. **Bench attribution** (§4): target whatever blocking class currently
   dominates. The only source that self-refreshes.
2. **Seeded backlog** (below).
3. **Profiling lane**: `cargo build --profile profiling` + `samply` on
   targeted workloads on the Mac; `perf` on the perf-lab droplet for
   Linux-side flamegraphs of real bench runs. Hotspots become backlog entries.
4. **Fresh-eyes sweeps**: periodic subagent review of one hot file/subsystem
   per night (sync reactor, checkpoint verifier, state write path) hunting
   redundant work, clones, and lock contention — filed as backlog entries with
   file:line evidence, not implemented directly.
5. **Upstream delta**: perf-relevant commits in upstream zebra not yet in the
   fork — filed as proposals; each ported diff is risk-classified on its own
   merits, defaulting to red when it touches verification code.

### Already covered — do not re-optimize

The merged perf lineage on `origin/main` (verified 2026-07-20) has already been
through: the Zakura block-sync reactor (multi-stage rewrite, BBR-lite
congestion control, tuned native P2P defaults, fill-loop trims), the
finalized-state writer (parallel note-commitment append, dedicated commit
rayon pool, parallel per-block serialization, parallel/deduped UTXO+address
reads, precomputed checkpoint auth-data roots, WAL growth limiting),
txid/ZIP-244 hashing (native computation without librustzcash round-trips,
shared txid+auth-digest conversion, Sinsemilla domain cache, deferred Sapling
cv/epk decompression), transparent sighash caching (merged 2026-07-20), the
legacy syncer's refill loop, and process-wide Sapling-prover reuse in the RPC
template path. New ideas in these areas need profiling evidence that a hotspot
*remains*, not first-principles guesses.

Subsystems with **no perf pass ever** (verified via commit history):
`zakura-consensus` batch proof/signature verification internals, the mempool,
general RPC serving, and `zakura-script` FFI. One live, well-scoped TODO on
the read hot path:
`zakura-state/src/service/finalized_state/disk_format/block.rs:296` — "skip
cryptography verification during transaction deserialization from storage, or
do it in a rayon thread (ideally in parallel with other transactions)".

### Seeded backlog (initial, to be re-ranked by the campaign-target memo)

Tuning-class (green):

- Systematic sweeps of block-sync knobs (`max_blocks_per_response`, request
  timeouts, in-flight limits, fanout): defaults got one manual tuning pass;
  a mock-blocksync-pre-filtered sweep with L2 confirmation is exactly the kind
  of patient search an unattended loop does well.
- Checkpoint & download concurrency limits (`CKPT_LIMIT`/`DL_LIMIT` are
  already bench-script env vars — sweepable without code changes).
- Body-commit batch size knee-finding (batched `DiskWriteBatch` exists and is
  instrumented via the batch-size metrics).
- RocksDB read-side/bulk-load options: block-cache size, memtable count/size,
  compaction style during checkpoint sync (WAL growth is already handled).
- Verifier batch sizes/windows for redpallas/halo2/groth16 batched
  verification — criterion lanes pre-filter, and this sits inside the one
  consensus subsystem that has never had a perf pass.
- Rayon pool sizing (global verifier pool vs the dedicated commit pool) and
  tokio worker-thread count.
- Channel capacities on the split sequencer channels and writer input queue.

Structural-class (yellow):

- The `FromDisk` TODO above: skip redundant crypto checks when deserializing
  transactions from trusted local storage, or parallelize across transactions.
- Remaining allocation/clone hotspots in download→verify→commit — much has
  landed already, so act only on measured profile evidence.
- Tracing/metrics overhead in hot loops (coordinate with the lazy-trace-events
  draft PR).
- Writer idle / commit-pacing improvements if attribution still shows
  commit-bound verdicts (the commit-metrics probe metrics were built for
  exactly this).

Coordination rule: before implementing any idea, the loop checks the ledger's
exclusion list — areas covered by open PRs or in-flight branches (as of
2026-07-20: empty ZIP-244 bundle-hash caching [PR 288] and the whole
sighash/ZIP-244 area freshly covered by the just-merged caching work;
block-template isolation [PR 292]; VCT artifact generation [PR 249];
retained-memory accounting [PRs 217/225]; lazy trace events [PR 228];
block-sync peer accountability/reconnect [PRs 209/166]; header-sync alignment,
actively iterated by others [PR 313 and the current `origin/main` tip];
the broad consensus/state-integrity/sync-liveness fixes branch [PR 165]) —
and skips or reformulates to avoid conflicts. The exclusion list is refreshed
from `gh pr list` at session start.

## 7. Reporting (ledger-only, per D5)

- **LEDGER.md** — append-only, one entry per experiment: id, hypothesis, risk
  class, diff summary + patch path, gate results, bench labels and numbers
  (baseline vs candidate post-first-commit blocks/s, delta, noise band),
  verdict, attribution notes, follow-ups. Batch summaries at every 8-run
  boundary and a session header with droplet cost. The ledger is the sole
  reporting channel — no PRs, no Slack, no push notifications.
- **REPORT.md** — regenerated after every verdict: current baseline
  throughput, ranked confirmed wins (each with delta size and a
  change-simplicity note, since that's what Adam triages on per D2), the
  PROMISING queue, parked red-class proposals, infra incidents. Written to be
  read in the morning.
- **Winning branches** stay on origin, one clean commit each; Adam opens PRs
  himself after reading the ledger. Losers' branches are deleted after their
  patch files are archived.

## 8. Delivery phases

- **Phase 0 — calibrate (half a day, mostly automated).** Reset this
  orchestration branch onto a freshly fetched `origin/main` (local refs trail
  origin by ~380 commits); permission allowlist; `perf-lab/` scaffolding +
  seeded backlog; `perf-lab-claude` SSH key; droplet provisioning/teardown/
  reaper script (extending the pr-node pattern); first droplet up, snapshot
  cached, A/A noise pass; criterion + mock-blocksync baselines on the Mac.
  Exit: measured noise band and a green dry-run of the full experiment state
  machine with a no-op diff, droplet destroyed cleanly afterward.
- **Phase 1 — MVP loop (1–2 days).** Campaign-target memo, then the
  per-experiment state machine over the seeded backlog, green-class only,
  ledger/report, batch-of-8 budget with automatic continuation, resumability.
  Exit: an unattended 8-hour overnight run that produces a morning report
  with ≥3 completed verdicts.
- **Phase 2 — self-directed (2–3 days).** Attribution-driven idea ranking,
  profiling lanes (Mac samply + droplet perf), fresh-eyes sweeps, yellow-class
  gates (e2e dispatch), codex delegation for mechanical diffs, adaptive repeat
  counts, second droplet for parallel L2 runs.
- **Phase 3 — optional.** Frozen-cohort determinism ported to perf-lab
  droplets (only if live-peer variance proves limiting), near-tip
  full-verification campaign (needs a higher snapshot), a perf-lab-specific
  bake (golden image plus a pre-extracted bench snapshot) if the per-droplet
  snapshot download ever matters.

Phases 0–1 are one PR-sized unit of tooling each (scripts + docs on this
branch), per the lean-PR convention; nothing lands on `main` unless Adam wants
the tooling upstreamed.

## 9. Decisions (resolved by Adam, 2026-07-20)

- **D1 — Execution lane**: approved. The loop may push `adam/perf-exp/*`
  branches and execute workflows.
- **D2 — PR handling**: branches + ledger only, never PRs. Adam triages the
  ledger on win size and change simplicity.
- **D3/D6 — Budget**: batches of 8 bench runs, continuing automatically batch
  after batch (with the zero-win halt condition in §5).
- **D4 — Bench substrate**: ephemeral DigitalOcean droplets the loop spins up
  and kills itself; tooling to do so is part of this work. Shared hosts are
  fallback only.
- **D5 — Reporting**: ledger only.
- **D7 — Targeting**: the loop's job — scan the codebase/measurements and pick
  what matters most (§6 campaign-target memo).

## 10. Risks

- **Bench noise swamps small wins.** Mitigated by A/A calibration, same-host
  same-invocation A/B, confirmation runs, and preferring ideas with expected
  wins ≥ the threshold. Residual: genuinely small (<3%) wins are invisible —
  acceptable for a screening harness.
- **Live-peer variance** (each run feeds from the pinned peer over the real
  network). Mitigated by `PEERSET_SIZE=1` + same-invocation A/B; residual
  variance lands in the noise band. Porting the frozen-cohort pattern to
  perf-lab droplets (Phase 3) removes it entirely if needed.
- **Stale local refs.** The local clone's `main` and its worktrees trail
  `origin/main` by hundreds of commits (the `zebra-*`→`zakura-*` rename and
  most of the perf lineage exist only remotely), and the main checkout's
  working tree carries large uncommitted changes. The loop therefore fetches
  `origin/main` at every session start, cuts branches only from it, and never
  reads baseline facts from local refs or the main checkout's tree.
- **Droplet leaks.** Tag-scoped naming, session-end teardown, and the
  session-start reaper sweep bound the blast radius to hours; the ledger
  records every create/destroy with IDs.
- **Feed-peer dependency.** The default pinned peer is shared infrastructure;
  if it's down or slow the A/A band catches it and the loop pauses L2 runs
  rather than recording garbage.
- **Agent-induced regressions sneaking through.** Every change is a small
  reviewed-diff branch, gated by clippy/tests/e2e-as-needed, and nothing
  merges without human review. The trace oracle's slowdown-trend check also
  runs in yellow-class validation.
- **Cost.** Droplet-hours ≈ $6–12 per overnight session per droplet, recorded
  in the ledger; token cost is dominated by the orchestrator, with mechanical
  work delegated to codex (effectively free per Adam's table).

## 11. Remaining open item

- Droplet size/region defaults: chosen empirically in Phase 0 (region matched
  to the pinned feed peer; smallest dedicated-CPU size whose A/A noise band is
  ≤ the shared runner's), recorded in `perf-lab/config`.
