# The reactor drops completions the gate finds stale

`reactor-drops-stale-verdict.patch` is a mutation-loop patch for the `detached-submission-bounded` work-list
entry. Fault class: resource-lifetime.

## What it breaks

The reactor stops forwarding a body completion to the sequencer when
`Gate::check_body` finds it stale. The patch reads the gate's "must have no
effects" literally. A stale verdict for a detached submission then never
reaches the sequencer, so the detached holder, its slot and its decoded
bytes stay charged for good.

## Witness

The witness is `qa/planted/block-sync/detached-submission-bounded/keep-detached-after-verdict.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-stale-beside-current.bin`.

The input resets the chain while storage holds a submission at height 2,
which the reset detaches. Its late retryable verdict is stale under the new
body-work epoch. The reactor drops it, and the detached holder is never
released before quiescence.

## Remaining guards

The witness picks `keep-detached-after-verdict.toml` over the work
list's `forget-on-reset.toml`: storage never answers in the stalled-reset
input, so no stale verdict reaches the reactor there.

## Reproduce

```sh
git apply qa/native-mutations/detached-submission-bounded/reactor-drops-stale-verdict/reactor-drops-stale-verdict.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/detached-submission-bounded
```

Required failure: the run ends at `block-sync/detached-submission-bounded` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
