# Upstream Sync

This directory tracks upstream Zebra pull requests that the Valar fork has
evaluated for `ironwood-main`.

The automation is intentionally conservative:

- it is manual-only in v1;
- it processes exactly one upstream PR per run;
- Codex runs without GitHub write permissions;
- only a later job without the Codex API key may open a draft PR; and
- reverting the implementation PR removes the automation.

## Statuses

- `pending`: discovered but not evaluated.
- `candidate`: selected for a manual workflow run.
- `imported`: behavior was brought into the fork. Record the downstream PR or
  commit.
- `skipped`: intentionally not relevant to this fork.
- `superseded`: already covered by a fork-specific change. Record evidence.
- `blocked`: relevant, but needs human conflict resolution or a broader design
  decision.

## Pilot

The first missing upstream PR is defined by upstream history, not by the lowest
PR number or easiest patch. At the time this automation was added, that first
candidate was upstream PR 10676.
