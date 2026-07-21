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
