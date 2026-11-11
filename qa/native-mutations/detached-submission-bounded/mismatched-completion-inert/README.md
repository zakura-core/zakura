# A mismatched completion releases nothing

`mismatched-completion-inert.patch` is a mutation-loop patch for the `detached-submission-bounded` work-list
entry. Fault class: resource-lifetime.

## What it breaks

`finish_apply`'s mismatch branch, for a completion whose height another
submission now holds, no longer releases the completion's own exact
submission. The patch treats the completion as fully stale: it changes
nothing. A detached submission whose verdict arrives while a newer body is
`applying` at its height keeps its holder, slot and decoded bytes for good.

## Witness

The witness is `qa/planted/block-sync/detached-submission-bounded/keep-detached-after-verdict.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-stale-beside-current.bin`.

The input resets the chain while storage holds a submission at height 2,
which the reset detaches. Its retryable verdict reaches block sync at 5 s
while the fork's height-2 body is `applying`, so it takes the mismatch
branch. The detached holder is never released before quiescence.

## Remaining guards

The witness picks `keep-detached-after-verdict.toml` over the work
list's `forget-on-reset.toml`: storage never answers in the stalled-reset
input, so no verdict reaches the mismatch branch there.

## Reproduce

```sh
git apply qa/native-mutations/detached-submission-bounded/mismatched-completion-inert/mismatched-completion-inert.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/detached-submission-bounded
```

Required failure: the run ends at `block-sync/detached-submission-bounded` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
