# A stale request's body is credited to the height's current owner

`stale-body-credits-current-owner.patch` is a mutation-loop patch for the `submit-only-requested` work-list
entry. Fault class: ownership.

## What it breaks

When a body matches an outstanding request that no longer owns its
height in the work queue, the routine drops the request. The patch instead
credits the body to the request that holds the height now, as
first-completion-wins does for an unmatched body, but without the hash
check that path makes. `begin_receipt_for_owner` ends the current owner's
reservation, and the body reaches the sequencer under the current owner.

## Witness

The witness is `qa/planted/block-sync/submit-only-requested/height-only.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-reset-mid-flight.bin`.

The input resets the chain to a fork from height 8 once the node submits
its third body, while two mirrors serve. Old-epoch bodies still on the wire
match their old requests after the reset. A body at or above the fork
height then carries the old chain's hash under a new-epoch owner whose
request asked for the fork's hash. The sequencer accepts it, since its
owner's epoch is current, and submits it. No needed-blocks answer of the
new epoch listed that hash, and the body crossed in the old request's
exchange.

## Remaining guards

The sequencer's epoch check in `offer_body` still holds, but the
re-owned body carries the current epoch. The transport's authorization
checks the body against the old request it answers, which it does.

## Reproduce

```sh
git apply qa/native-mutations/submit-only-requested/stale-body-credits-current-owner/stale-body-credits-current-owner.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/submit-only-requested
```

Required failure: the run ends at `block-sync/submit-only-requested` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
