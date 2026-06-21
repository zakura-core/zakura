# Upstream Triage

This directory tracks upstream Zebra pull requests that the Valar fork has
evaluated for `ironwood-main`.

The automation is intentionally conservative:

- it runs hourly and can also be started manually;
- it triages up to 25 upstream PRs per run;
- it opens at most one downstream PR per run;
- it pauses while any generated upstream sync PR is open;
- it triages before attempting any import;
- Codex runs without GitHub write permissions;
- only important production bug fixes should open draft PRs;
- uncertain relevant changes open zero-diff draft PRs for human review;
- closed generated PRs are treated as intentional human skips on later runs; and
- reverting the implementation PR removes the automation.

Most upstream PRs should be skipped. Features, test-only fixes, docs, CI,
formatting, release metadata, and routine refactors do not meet the import bar
unless they carry an important production bug fix for this fork.

Skipped and already-present triage decisions are recorded on the
`upstream-sync/state` branch in `.github/upstream-sync/triage-ledger.jsonl`.
This avoids churn on `ironwood-main` just to remember skipped upstream PRs.
`needs_human` decisions open a draft PR with an empty commit and no file
changes, so reviewers have a visible place to decide whether to close it, add a
manual fix, or keep investigating.

The state branch tracks terminal decisions per upstream PR instead of a single
"last upstream PR seen" pointer. A single pointer would be unsafe because
intentionally skipped upstream PRs remain absent from the fork, and upstream PR
numbers do not perfectly describe commit order. The per-PR record lets each run
skip reviewed PRs and continue to the next oldest missing upstream change. State
records are scoped to their source repository, and `already_present` records
only apply to the target revision that was inspected.

When a run sees a backlog, it triages candidates in upstream order. Quiet
decisions are recorded in one batch. If it reaches an important fix or a
`needs_human` blocker, it records the earlier quiet decisions, opens one draft
PR, and stops. While that draft PR is open, later runs pause.

## Statuses

- `pending`: discovered but not evaluated.
- `candidate`: selected for a manual workflow run.
- `imported`: behavior was brought into the fork. Record the downstream PR or
  commit.
- `skipped`: intentionally not relevant to this fork.
- `already_present`: already covered by the fork.
- `superseded`: already covered by a fork-specific change. Record evidence.
- `needs_human`: likely relevant, but needs human conflict resolution or a
  broader design decision. The workflow opens a draft PR with no file changes.

## Pilot

The first missing upstream PR is defined by upstream history, not by the lowest
PR number or easiest patch. At the time this automation was added, that first
candidate was upstream PR 10676.
