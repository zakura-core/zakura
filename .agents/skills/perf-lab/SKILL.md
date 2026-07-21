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
artifacts if fresh), read `verdict-*.json` + summary, run the `zakura-trace-plots` skill (`.agents/skills/zakura-trace-plots/`) over the traces if deeper attribution is needed, and
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
   For low-level-crate diffs (zakura-chain, zakura-state) also pass their main
   downstream crates so a cross-crate break surfaces here instead of wasting a
   droplet build. Fail twice → BROKEN ledger entry, delete worktree+branch,
   next.
5. **L1** (only if a micro lane applies): for block-sync-layer diffs run
   `gates.sh micro-mockbs` with **4 runs per side and compare the median of
   runs 2–4** (run 1 is a consistent ~30% cold-start outlier on the Mac
   baseline). A nonzero exit means discard every sample from that invocation.
   Kill the experiment only on a clear regression — candidate median ≥10%
   below base median (warm noise is ~1–2%; anything smaller passes through to
   L2 for the authoritative verdict). For crypto/serialization diffs use
   `cargo bench -p <crate>` + critcmp instead.
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

Status semantics: only a SUCCESSFUL `bench.sh status` call yields
ABSENT/RUNNING/DONE — an ssh transport failure (nonzero exit, empty output)
means UNKNOWN, retry later. Enforce a wall-clock cap from
`in_flight[label].started_at` (3 h default): past it, treat the run as failed
regardless of reported status, collect whatever exists, and destroy/
re-provision as needed.

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
