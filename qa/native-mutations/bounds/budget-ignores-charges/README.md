# The byte budget forgets its charges

`ByteBudget::available` and `ByteBudget::try_reserve` (`transport/guard.rs`)
both read the budget's capacity as its whole maximum `M`, not `M` minus the
bytes already reserved. Admission sizes every take from `available`, and
`reserve_owned` admits any reservation of at most `M`, so the byte budget
counts reservations far past `M` without any floor overdraft.

The patch changes the shared toolkit. Of the bounds instances, only
`block-sync/reserved-byte-bound` reads the byte budget. The retained,
look-ahead and decoded-input bounds read the sequencer's holders, the work
queue's estimates and the decoded counters, which this patch leaves alone,
so it has no case for them.

## Guards on the rule's path

1. Admission's sizing (`admission.rs`, `admit`) caps each take at
   `budget_available`. The patch makes `available` return `M`.
2. `try_reserve`'s capacity check refuses a reservation past `M` minus
   the reserved bytes. The patch compares against `M` alone.

Breaking only the second guard is close to equivalent: admission then sees
no room and refuses, except for one first item per take.

## Witness

`paused_handoffs_pass_m_plus_2b_within_the_handoff_allowance` in
`crates/zakura-sim/tests/block_resource_properties.rs` runs the committed
input `block-sync-paused-handoffs.bin` with no witness judgement. The planted
case `above-floor-charge` can't witness this patch. Its trigger history needs
a floor charge, and the patch reserves past `M` without one.
Four mirrors serve sixteen bodies of unknown size, so each request reserves
`B`, against `M = 1.5·B`. Four routines' requests then reserve past
`M + 2·B + h·B` with no floor charge.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/reserved-byte-bound --out <report dir>
```

Required failure: the run ends at `block-sync/reserved-byte-bound`.
