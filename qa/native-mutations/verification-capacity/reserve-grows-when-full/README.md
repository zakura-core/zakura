# A slot reservation adds a slot when none is free

`SlotBudget::reserve` adds a permit when the budget has none free, so the
budget grows with demand and bounds nothing. This shared toolkit patch
breaks every `SlotBudget` user. Discovery's verification slots are this
entry's only instance.

The witness holds the state lock once the readers fill the single slot.
Each later reader adds a slot and starts its worker, so more workers run
than the fixture's one slot allows.

## Witness

`qa/planted/discovery/verification-capacity/release-before-install.toml`, the work list's witness for `discovery/verification-capacity`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/verification-capacity --out <report dir>
```

Required failure: the run ends at `discovery/verification-capacity`.
