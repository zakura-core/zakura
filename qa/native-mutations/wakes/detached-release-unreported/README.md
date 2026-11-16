# Releasing a detached submission reports no release

`detached-release-unreported.patch` is a mutation-loop patch for the `wakes` work-list
entry. Fault class: wakeup.

## What it breaks

`Sequencer::finish_submission` releases a detached submission in full,
but returns `false` for it, as if nothing matched. The patch's reasoning is
that only an attached submission held a slot that waiting work can use.
Every caller in `finish_apply` gates its refill on that result, so a
detached submission's verdict frees room and wakes nothing.

## Witness

The witness is `qa/planted/block-sync/capacity-release-wakes/stale-completion-no-wake.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-capacity-frontier-first.bin`.

The input fills the 401-submission window, then resets to a fork from
height 1, which detaches it. Storage commits height 1, block sync handles
the tip first, and the stale reply then frees the detached submission's
slot. The stale branch reads the release result, sees none, and returns no
refill, so the waiting fork body stays unsubmitted before quiescence.

## Remaining guards

The release itself is intact; only the result that the wake reads
changes.

## Reproduce

```sh
git apply qa/native-mutations/wakes/detached-release-unreported/detached-release-unreported.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/capacity-release-wakes
```

Required failure: the run ends at `block-sync/capacity-release-wakes` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
