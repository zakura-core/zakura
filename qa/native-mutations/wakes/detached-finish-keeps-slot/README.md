# A detached submission's verdict keeps its slot

`detached-finish-keeps-slot.patch` is a mutation-loop patch for the `wakes` work-list
entry. Fault class: boundary-accounting.

## What it breaks

`Sequencer::finish_submission` frees a detached submission's holder,
bytes and decoded charge, but no longer decrements
`in_flight_submission_count`. The patch's reasoning is that the detach
already moved the submission out of the window. Every detached
submission's verdict therefore leaks one slot of `submitted_apply_limit`,
and `submittable_heights` sees no room once the window has filled.

## Witness

The witness is `qa/planted/block-sync/capacity-release-wakes/stale-completion-no-wake.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-capacity-frontier-first.bin`.

The input fills the 401-submission window, then resets to a fork from
height 1, which detaches the whole window. Storage commits height 1;
block sync handles the tip first and the stale reply then frees the
detached submission's holder. The sequencer wakes the waiting fork body,
but the submission pass finds the window still full, so no body goes into
the freed room before quiescence.

## Remaining guards

The wake itself still runs: `finish_apply`'s stale branch returns its
refill request. Only the count that `submittable_heights` reads stays
wrong.

## Reproduce

```sh
git apply qa/native-mutations/wakes/detached-finish-keeps-slot/detached-finish-keeps-slot.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/capacity-release-wakes
```

Required failure: the run ends at `block-sync/capacity-release-wakes` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
