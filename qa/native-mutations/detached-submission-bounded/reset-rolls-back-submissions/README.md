# A destructive reset rolls back dispatched submissions

`reset-rolls-back-submissions.patch` is a mutation-loop patch for the `detached-submission-bounded` work-list
entry. Fault class: resource-lifetime.

## What it breaks

Before a destructive reset, `SequencerState::destructive_reset_to`
rolls back every submitted `applying` body above the reset tip through
`Sequencer::unsubmit`, the path a refused dispatch takes. The patch's
reasoning is that the reset retires that work. The rollback removes each
submission's holder and frees its slot and decoded bytes while storage
still holds the submission. `Sequencer::reset_to` then finds nothing left
to detach.

## Witness

The witness is `qa/planted/block-sync/detached-submission-bounded/forget-on-reset.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-stalled-reset.bin`.

The input's storage never answers a submission, and a reset to a fork
from height 2 arrives once the first is submitted. The reset rolls that
submission back, so storage holds it across the reset with no holder.

## Remaining guards

The detach in `release_all_applying_for_reset` stays intact but has
nothing left to detach. Unlike `forget-on-reset`, which removes only the
map entry and leaks the slot, this rollback releases the slot and bytes
too.

## Reproduce

```sh
git apply qa/native-mutations/detached-submission-bounded/reset-rolls-back-submissions/reset-rolls-back-submissions.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/detached-submission-bounded
```

Required failure: the run ends at `block-sync/detached-submission-bounded` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
