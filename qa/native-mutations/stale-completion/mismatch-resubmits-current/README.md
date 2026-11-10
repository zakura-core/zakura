# Mismatched completion rolls back the current submission

`mismatch-resubmits-current.patch` is a mutation-loop patch for the `stale-completion` work-list
entry. Fault class: ownership.

## What it breaks

When a completion reaches `finish_apply` for a height whose `applying`
entry carries another token, the sequencer releases the completion's own
exact submission, as before. The patch then rolls back the current
`applying` entry's submission through `Sequencer::unsubmit`, the path a
refused dispatch takes, so the refill that the branch already requests
dispatches the body again under a fresh token. Storage still holds the
rolled-back submission, but no holder charges it any more.

## Witness

The witness is `qa/planted/block-sync/stale-completion-inert/release-by-height.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-stale-beside-current.bin`.

The input resets the chain to a fork from height 2 while storage holds
the first submission at height 2. The detached submission's retryable
verdict arrives at 5 s while the fork's height-2 body waits in storage.
The verdict reaches the mismatch branch, which now releases the fork's
submission beside the pending stale completion and resubmits its body.

## Remaining guards

`Sequencer::unsubmit` checks only that the token it gets is the
`applying` entry's, which the patch passes; nothing else stands between
the stale completion and the current submission.

## Reproduce

```sh
git apply qa/native-mutations/stale-completion/mismatch-resubmits-current/mismatch-resubmits-current.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/stale-completion-inert
```

Required failure: the run ends at `block-sync/stale-completion-inert` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
