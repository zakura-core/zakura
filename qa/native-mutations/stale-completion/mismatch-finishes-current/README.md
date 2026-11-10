# Mismatched completion finishes the current submission

`mismatch-finishes-current.patch` is a mutation-loop patch for the `stale-completion` work-list
entry. Fault class: ownership.

## What it breaks

When a completion reaches `finish_apply` for a height whose `applying`
entry carries another token, the sequencer releases the completion's own
exact submission, as before. The patch then also calls
`finish_attached_submission` with the current `applying` entry's identity.
That finishes the current submission, which storage still holds: its holder
leaves `in_flight_submissions` while its body stays attached and marked
submitted. The patch's reasoning is that the older verifier call made the
current one redundant.

## Witness

The witness is `qa/planted/block-sync/stale-completion-inert/release-by-height.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-stale-beside-current.bin`.

The input resets the chain to a fork from height 2 while storage holds
the first submission at height 2. The reset detaches that submission. The
fork's height-2 body then waits in storage under a new token. The detached
submission's retryable verdict arrives at 5 s and reaches the mismatch
branch, because the fork's body is `applying` at height 2. The patched
branch then releases the fork's submission beside the pending stale
completion.

## Remaining guards

The completion's exact identity check in `Sequencer::finish_submission`
still holds; the patch releases the current submission through
`finish_attached_submission`, which checks the current identity it is
handed.

## Reproduce

```sh
git apply qa/native-mutations/stale-completion/mismatch-finishes-current/mismatch-finishes-current.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/stale-completion-inert
```

Required failure: the run ends at `block-sync/stale-completion-inert` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
