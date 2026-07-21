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
2. Read `perf-lab/state.json`, `LEDGER.md` tail, `BACKLOG.md` FIRST, and
   resume any `in_flight` bench (status → collect → verdict → ledger) before
   any reaping, so a live bench's droplet is never destroyed under it.
3. `bash perf-lab/droplet.sh list` then `reap` — never reap a droplet named in
   `state.json.in_flight`; note anything reaped in the ledger as an incident.
4. Refresh exclusions: `gh pr list --limit 50` → update BACKLOG.md's
   Exclusions section; drop/block any backlog item that now collides.
5. Noise-band gate: `config.env`'s `NOISE_BAND_PCT` must be non-empty. If it
   is empty, or the droplet size/region/image changed since it was measured,
   or its ledger date is >7 days old, run an A/A first
   (`bench.sh start <droplet> aa-recal main main`, collect, then
   `python3 perf-lab/verdict.py <summary.md> --aa`) and write the observed
   value into config.env and state.json before trusting any verdict.
6. Append `## SESSION N` to LEDGER.md (date, origin/main SHA, plan for the
   session). Commit ledger updates as you go:
   `git add perf-lab && git commit -m "perf-lab: session N ledger"` and
   `git push origin adam/zakura-agentic-perf-5667cc`.
7. Droplet: reuse a healthy droplet already in `state.json.droplets` if one
   exists; otherwise `bash perf-lab/droplet.sh provision s<N>`. `<droplet>`
   below always means the full `perf-lab-…` name from `list`/state.json. Add
   a second droplet only when two L2-ready experiments are queued and
   MAX_DROPLETS allows.

## Campaign-target memo (once, before EXP-001)

If fresh A/A artifacts already exist from calibration or session-start step 5,
reuse their summary/verdicts instead of running again; otherwise run a
baseline bench (`bench.sh start <droplet> base main main`, collect). All
calibration/campaign/confirmation collects count toward the batch budget.
Read `verdict-*.json` + summary, run the `zakura-trace-plots` skill
(`.agents/skills/zakura-trace-plots/`) over the traces if deeper attribution
is needed, and
write `## CAMPAIGN` in the ledger: dominant bottleneck class, chosen target
metric (default: checkpoint-zone post-commit blk/s), re-ranked top-5 backlog.

## Per experiment (state machine)

1. **Pick** the top READY backlog item compatible with the exclusions.
   Allocate `EXP-NNN` from state.json; risk-class it (spec §5). Red → write a
   PROPOSAL ledger entry, mark backlog DROPPED(red-proposal), next item.
2. **Branch**: `git worktree add /tmp/perf-exp-NNN origin/main -b adam/perf-exp/NNN-<slug>`.
3. **Implement** the minimal diff. Mechanical + fully specced → delegate to
   codex per ~/.claude/CLAUDE.md's delegation table; consensus-adjacent stays
   here. Commit the diff as ONE clean commit in the worktree
   (`git -C /tmp/perf-exp-NNN add -A && git -C /tmp/perf-exp-NNN commit -m "perf(<area>): <slug> (EXP-NNN)"`)
   — an uncommitted worktree pushes an empty branch and benches main-vs-main.
   Then archive the diff:
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
   **Env-knob sweeps (B-01/B-02 style) are NOT within-invocation A/Bs**: the
   harness applies `CKPT_LIMIT`/`DL_LIMIT` to BOTH rows (only `P2P_STACK` has
   a per-side split), so the within-run delta of a knob run is pure noise.
   Sweep procedure (no branch, no commit): for each point run
   `bench.sh start <droplet> expNNN-pX main main CKPT_LIMIT=X`, collect, and
   IGNORE the within-run verdict except as a validity check (if its
   |delta_pct| exceeds the noise band, the run was noisy — rerun the point).
   Compare each point's `primary_pc_bps` ABSOLUTELY against the campaign
   baseline's `primary_pc_bps`: PROMISING at ≥ max(5%, 3× noise band) above
   baseline; confirm with a repeat run before WIN (cross-invocation
   comparisons need the wider margin). B-13 upgrades sweeps to true per-side
   A/Bs later. While any bench runs (~60–90 min), implement the next
   experiment. Poll `bench.sh status` on wakeups. If the droplet lane is
   broken (provision or bench fails twice), fall back to the shared runner:
   `gh workflow run checkpoint-sync-bench.yml -f build_ref=<branch> -f baseline_ref=main -f skip_baseline=false`
   (skip_baseline defaults to TRUE and would silently drop the baseline row),
   poll `gh run list --workflow=checkpoint-sync-bench.yml`, then
   `gh run download <id>`. Caveat: inside Actions the script writes its table
   to `GITHUB_STEP_SUMMARY`, so the artifact may lack `summary.md` — if so,
   derive post-commit blk/s from each `samples-*.csv` (height delta ÷ elapsed
   after the first height increase) and record the verdict as PROMISING at
   most; confirm on a recovered droplet before calling any fallback result a
   WIN.
7. **Verdict**: `bench.sh collect` → verdict.json (the A/B decision from
   verdict.py; distinct from the harness's `verdict-*.json` bottleneck
   classifier).
   - WIN_CANDIDATE → one confirmation run under label `expNNN-c` (NEVER reuse
     the original label — `start` clears same-label artifacts); record both
     deltas. Two above-threshold runs = **WIN**: run full workspace tests in
     the worktree (`cargo nextest run --profile all-tests` if feasible, else
     targeted + build) — if the /tmp worktree is gone after a crash, recreate
     it from the pushed branch (`git worktree add /tmp/perf-exp-NNN adam/perf-exp/NNN-<slug>`)
     — keep the branch, ledger with simplicity score. Yellow-class wins
     additionally: the `zakura-integration` nextest profile locally, plus
     `gh workflow run zakura-e2e.yml --ref adam/perf-exp/NNN-…`
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

- `BATCH_SIZE=8` bench collects per batch (calibration, campaign, sweep
  points, and confirmations all count). When the counter reaches 8, finish
  the in-progress experiment first, then write `## BATCH` (runs, wins,
  spend), reset the counter, and continue automatically.
- Halt (destroy droplets, final REPORT) when: a full batch produces zero runs
  at or above the effective threshold AND no READY backlog item's expected
  value clears it; or on harness breakage twice in a row; or when Adam says
  stop.
- Never leave a droplet up while no bench is running or imminent.

## Safety rails (hard)

- Only `droplet.sh` touches DO, only on `perf-lab-*` + tag `zakura-perf-lab`.
- Never push to main/feat/release; never open PRs; never dispatch deploy or
  release workflows; never edit `deploy/`, `.github/workflows/`, checkpoint
  files, or dependency versions inside an experiment.
- Both bench refs always run `p2p_stack=zakura` (bench.sh enforces).
- Ledger is the only reporting channel. No Slack, no notifications, no PRs.
