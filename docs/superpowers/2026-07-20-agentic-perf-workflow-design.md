# Agentic sync-performance workflow — design

Date: 2026-07-20
Status: draft, awaiting Adam's review
Branch: `adam/zakura-agentic-perf-5667cc`

## 1. Goal

An autonomous loop that Adam can start and leave for hours. It proposes low-risk
performance changes to Zakura, implements each one on its own branch, validates
correctness, measures real sync throughput A/B against baseline, and reports
findings incrementally. Humans review everything before anything merges.

Non-goals:

- No autonomous merges, deploys, or fleet operations of any kind.
- No autonomous changes to consensus rules, cryptography, checkpoint lists,
  chain parameters, DB formats, or wire formats (it may *propose* these as
  written analyses, never implement them unsupervised).
- Not a replacement for the nightly e2e, continuous-sync fleet, or human-driven
  deep optimization work — it is a screening harness that finds and validates
  candidates cheaply.

## 2. What already exists (the loop orchestrates, it does not rebuild)

Measurement and validation infrastructure already in the repo, confirmed
2026-07-20:

| Asset | What it gives the loop |
| --- | --- |
| `.github/workflows/checkpoint-sync-bench.yml` + `scripts/checkpoint-sync-bench.sh` | The authoritative macro benchmark. `workflow_dispatch` with `build_ref`/`baseline_ref` inputs; runs both refs on the same self-hosted `zakura-bench` runner from a hard-link-forked ~1.7M-height snapshot to `stop_height` (default 1,730,000, `wall_cap_seconds` 2400); emits `summary.md` (blocks/s, post-first-commit blocks/s, A/B speedup) + `samples-*.csv` + logs as artifacts. |
| `zakura_mock_blocksync_throughput` (`zakura-network/src/zakura/testkit/mock_blocksync.rs`) | macOS-native local throughput harness: 4 seed nodes + 1 leecher over real loopback QUIC, synthetic corpus (default 100k blocks), prints blocks/s + MiB/s. Env-tunable (`ZAKURA_MOCK_BS_*`). Fast pre-filter for network/block-sync-layer changes. |
| Criterion benches + `.github/workflows/benchmarks.yml` | Micro A/B for crypto verifiers (`groth16`, `halo2`, `sapling`, `redpallas`) and serialization (`block`, `transaction`); `critcmp` comparison exists in CI (PR label `C-benchmark`) and locally. |
| `.github/workflows/zakura-e2e.yml` | Correctness at the protocol level: `pr-gate` mode (~6 min, auto on PRs touching sync paths) and dispatchable long modes (`checkpoint-long`, `no-checkpoint-long`, `restart-matrix`) with the JSONL `trace_oracle.py` (includes commit-latency ceiling and slowdown-trend checks). |
| `tests-unit.yml` lane (`cargo nextest run --profile all-tests --run-ignored=all`) | The unit/property correctness bar, reproducible locally. |
| `analysis/zakura_trace_analysis/analyze_zakura_sync.py` + `.agents/skills/zakura-trace-plots` | Post-hoc bottleneck attribution from JSONL traces (blocking-class classification: `hol_gap`, `commit_backpressure`, `download_starved`, …) — turns bench output into the *next* experiment idea. |
| `deploy/runner/` cohort harness (`make perf-*`) | Deterministic isolated-cohort bench with `commit-metrics` per-stage attribution. Tracked on `origin/main`; the host-side pieces (`cohort.env`, snapshot, frozen serving nodes) live on the bench host. Richest signal; not yet dispatchable from CI. Phase-3 candidate, gated on Adam (see decision D4). |

Known gaps the loop must respect: no deterministic full-verification replay
bench exists; Zakura v2 sync speed is JSONL-only (not Prometheus); the Mac has
no synced state and Docker-on-Mac e2e is unfaithful (debug via CI artifacts,
never a local docker stack).

## 3. Architecture

Three candidate shapes were considered:

- **A. CI-dispatch loop (recommended).** Orchestrator session on the Mac; fast
  gates run locally (fmt/clippy/unit tests, criterion, mock-blocksync); the
  authoritative A/B verdict comes from dispatching `checkpoint-sync-bench.yml`
  on experiment branches via `gh`. Uses only existing, already-authorized
  execution lanes; survives hours unattended; single new moving part (the
  orchestrator itself).
- **B. Bench-host SSH loop.** Drive the `deploy/runner` cohort harness over SSH
  for deterministic measurements with per-stage attribution. Better signal, and
  the tooling is tracked on `origin/main` — but the host-side state
  (`cohort.env`, snapshot, frozen cohort) lives on a bench host that appears to
  be shared/personal, and no SSH config exists on this Mac. Not the MVP;
  optional Phase-3 upgrade behind decision D4.
- **C. Mac-only loop.** Criterion + mock-blocksync + local regtest only. No
  external dependencies, but macOS ≠ production Linux and there is no local
  full-pipeline macro signal, so it cannot produce trustworthy verdicts alone.
  Its lanes are embedded in A as pre-filters.

**Recommendation: A**, with C's lanes as the inner filter and B as a later
optional lane.

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
  → push branch; dispatch checkpoint-sync-bench (build_ref=branch,
    baseline_ref=main); poll run; pull summary.md + CSVs              [~1–2 h]
  → verdict (see §4) → record in LEDGER; update REPORT
  → WIN: keep branch, save patch + artifacts, full workspace test lane,
         notify Adam. Yellow-class wins also dispatch zakura-e2e.yml
         (checkpoint-long) on the branch before being called validated.
  → LOSS/NEUTRAL: record numbers, delete branch (patch preserved in ledger).
  → BROKEN (build/test failure twice): park with error snippet, move on.
```

While an L2 bench dispatch is in flight (the long pole), the orchestrator
implements and L0/L1-gates the *next* experiment — the pipeline stays full
without ever running two L2 benches concurrently.

### Session mechanics

- The orchestrator is a Claude Code session on the Mac using scheduled wakeups
  while benches run remotely; long-running local commands run as background
  tasks. Nothing depends on the session staying "awake" — all state lives on
  disk.
- **Resumable state**: `perf-lab/state.json` (current experiment, phase,
  in-flight GitHub run IDs), `perf-lab/BACKLOG.md`, `perf-lab/LEDGER.md`,
  `perf-lab/REPORT.md`, all committed on the orchestration branch
  (`adam/zakura-agentic-perf-5667cc`). Any fresh session resumes by reading
  these. Bulk artifacts (CSVs, logs, staged binaries) go to
  `~/zakura-perf-lab/` outside the repo.
- **Permissions**: the loop needs pre-approved allowlist entries for the exact
  command shapes it uses (`gh workflow run/list/view/download` on the two
  named workflows, `git push origin adam/perf-exp/*`, cargo build/test/bench,
  codex exec) so it never stalls on a prompt overnight. Set up once in Phase 0.
- **Model split** (per Adam's delegation table): orchestration, risk
  classification, and consensus-adjacent diffs → fable-5; mechanical
  implementation with a written spec → codex gpt-5.5; review of winning diffs
  → the `zcash-reviewer` skill before anything is proposed as a PR.

## 4. Measurement protocol

- **Noise floor first.** Phase 0 runs an A/A dispatch (`build_ref =
  baseline_ref = main`). The observed A/A delta defines the noise band; the
  win threshold is `max(3%, 2 × observed A/A delta)`. The gating metric is the
  harness's **post-first-commit blocks/s** (it excludes startup/restore cost);
  plain whole-run blocks/s is recorded as a secondary signal. A/A re-runs
  weekly (or after runner changes) re-calibrate.
- **Same-dispatch A/B.** Both refs build and run inside one dispatch on one
  host — shared conditions by construction, and the harness already computes
  the speedup.
- **Confirmation.** A candidate above threshold gets one repeat dispatch before
  being declared a WIN (2 dispatches = 4 syncs). Single-dispatch results are
  recorded as PROMISING, never WIN.
- **Secondary signals**, recorded but not gating: whole-run blocks/s, CSV
  height-over-time shape (no stall cliffs), peak memory if present in logs,
  micro-lane deltas.
- **Attribution.** After each L2 run, feed traces/CSVs through the existing
  analyzers; the dominant blocking class is logged in the ledger and used to
  rank the next ideas (e.g. `commit_backpressure` dominant → prioritize state/
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

Hard rules regardless of class: never push to `main` or `feat/**`/`release/**`;
never open PRs autonomously (see D2); never dispatch deploy/release workflows
or touch fleet nodes; never modify `deploy/`, `.github/workflows/`, checkpoint
files, or `Cargo.lock` dependency bumps as part of an experiment; at most one
L2 dispatch in flight, and yield if the bench runner's queue is busy with a
human-triggered run (check before dispatch, D3); stop dispatching after the
nightly cap (D6) and fall back to local-only lanes.

Repo-policy compliance: nothing the loop produces is a PR until Adam reviews;
any PR he does open from a winning branch carries the standard AI disclosure
and test evidence (the ledger entry is the test evidence).

## 6. Idea generation

Sources, in priority order:

1. **Bench attribution** (§4): target whatever blocking class currently
   dominates the baseline run. This is the only source that self-refreshes.
2. **Seeded backlog** (below).
3. **Profiling lane**: `cargo build --profile profiling` + `samply` on targeted
   workloads (mock-blocksync, criterion drivers) on the Mac; hotspots become
   backlog entries. (Linux-side flamegraphs only if/when D4 opens the bench
   host.)
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

### Seeded backlog (initial, to be re-ranked by first attribution run)

Tuning-class (green):

- Systematic sweeps of block-sync knobs (`max_blocks_per_response`, request
  timeouts, in-flight limits, fanout): defaults got one manual tuning pass;
  a mock-blocksync-pre-filtered sweep with L2 confirmation is exactly the kind
  of patient search an unattended loop does well.
- Checkpoint & download concurrency limits (already parameterized as bench
  workflow inputs — sweepable without code changes).
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
  `commit_backpressure` (the feed-run probe metrics were built for exactly
  this).

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

## 7. Reporting

- **LEDGER.md** — append-only, one entry per experiment: id, hypothesis, risk
  class, diff summary + patch path, gate results, bench run IDs and numbers
  (baseline vs candidate blocks/s, delta, noise band), verdict, attribution
  notes, follow-ups. The ledger is the artifact of record.
- **REPORT.md** — regenerated after every verdict: current baseline throughput,
  ranked confirmed wins with expected stacked impact, PROMISING queue, parked
  proposals (red-class), infra incidents. Written to be read in the morning.
- **Notifications** (push notification to Adam): confirmed WIN, harness
  breakage that halts the loop, budget/cap reached, end-of-session digest.
  Everything else is ledger-only. Slack posting only if D5 says yes.
- **Winning branches** stay on origin, one clean commit each, ready for
  `gh pr create --draft` the next morning; losers' branches are deleted after
  their patch files are archived.

## 8. Delivery phases

- **Phase 0 — calibrate (half a day, mostly automated).** Reset this
  orchestration branch onto a freshly fetched `origin/main` (local refs trail
  origin by ~380 commits); permission allowlist; `perf-lab/` scaffolding +
  seeded backlog; A/A noise dispatch (pinned `feed_peer`, `peerset_size=1`);
  local warm builds; criterion + mock-blocksync baselines on the Mac; verify
  artifact download and summary parsing end-to-end. Exit: measured noise band
  and a green dry-run of the full experiment state machine with a no-op diff.
- **Phase 1 — MVP loop (1–2 days).** The per-experiment state machine over the
  seeded backlog, green-class only, ledger/report/notifications, budget caps,
  resumability. Exit: an unattended 8-hour overnight run that produces a
  morning report with ≥3 completed verdicts.
- **Phase 2 — self-directed (2–3 days).** Attribution-driven idea ranking,
  profiling lane, fresh-eyes sweeps, yellow-class gates (e2e dispatch),
  codex delegation for mechanical diffs, adaptive repeat counts.
- **Phase 3 — optional lanes.** Dispatchable wrapper for the cohort harness on
  the bench host (D4), auto-draft-PR mode (D2), Slack digests (D5), second
  bench runner if dispatch queueing becomes the bottleneck.

Phases 0–1 are one PR-sized unit of tooling each (scripts + docs on this
branch), per the lean-PR convention; nothing lands on `main` unless Adam wants
the tooling upstreamed.

## 9. Decisions Adam must make (defaults proposed)

- **D1 — Execution lane.** Authorize the loop to push `adam/perf-exp/*`
  branches and dispatch `checkpoint-sync-bench.yml` / `zakura-e2e.yml` via
  `gh`. *Proposed: yes — this is the core of design A.*
- **D2 — PR handling.** Auto-open draft PRs for confirmed wins, or
  branches+ledger only? *Proposed: branches only; you open PRs after review.*
- **D3 — Bench-runner etiquette.** Cap of one in-flight dispatch, yield to
  human runs, any time-of-day restrictions? *Proposed: cap 1, always yield,
  no time restriction.*
- **D4 — Cohort harness / bench host.** Is the `deploy/runner` host fair game
  for a Phase-3 dispatchable lane, or is that setup personal/shared (it looks
  like Roman's box)? *Proposed: defer; ask Roman before Phase 3.*
- **D5 — Slack.** Post digests/wins to `#zakura-alerts` (or another channel)?
  *Proposed: no Slack initially; push notifications + morning report.*
- **D6 — Budget.** Max L2 dispatches per night and max experiment count?
  *Proposed: 8 dispatches/night, no experiment cap (local-only lanes continue
  when the dispatch budget is spent).*
- **D7 — Scope of first campaign.** Start with the tuning-class backlog
  (§6) targeting checkpoint-zone sync throughput, or aim at a different
  metric first (e.g. near-tip full-verification throughput)? *Proposed:
  checkpoint-zone throughput — it is what the existing bench measures.*

## 10. Risks

- **Bench noise swamps small wins.** Mitigated by A/A calibration, same-
  dispatch A/B, confirmation dispatches, and preferring ideas with expected
  wins ≥ the threshold. Residual: genuinely small (<3%) wins are invisible —
  acceptable for a screening harness.
- **Live-peer variance** (the bench feeds from a real peer). Mitigated by the
  pinned `feed_peer` input and same-dispatch A/B; residual variance lands in
  the noise band. The Phase-3 cohort lane removes it entirely.
- **Stale local refs.** The local clone's `main` and its worktrees trail
  `origin/main` by hundreds of commits (the `zebra-*`→`zakura-*` rename and
  most of the perf lineage exist only remotely), and the main checkout's
  working tree carries large uncommitted changes. The loop therefore fetches
  `origin/main` at every session start, cuts branches only from it, and never
  reads baseline facts from local refs or the main checkout's tree.
- **Runner contention / breakage.** Detected by dispatch-queue checks and run
  failures; the loop parks L2 work, continues local lanes, and notifies.
- **Agent-induced regressions sneaking through.** Every change is a small
  reviewed-diff branch, gated by clippy/tests/e2e-as-needed, and nothing
  merges without human review. The trace oracle's slowdown-trend check also
  runs in yellow-class validation.
- **Cost.** Bench runs consume the self-hosted runner (already provisioned)
  and CI minutes for e2e dispatches only on yellow wins. Token cost is
  dominated by the orchestrator; mechanical work is delegated to codex
  (effectively free per Adam's table).

## 11. Open questions

- Does the `zakura-bench` runner have spare capacity overnight, and who else
  dispatches it? (Affects D3/D6 defaults.)
- Is there appetite to add a tiny `perf-experiment.yml` wrapper workflow
  (dispatch → bench → artifact upload with a machine-readable JSON verdict)?
  Not required — the loop can parse `summary.md` — but it would make verdict
  parsing robust to script changes.
- Should confirmed wins also get a criterion `C-benchmark` compare run for the
  record when they touch code covered by micro benches? (Cheap, adds a second
  independent number.)
