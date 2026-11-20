# A stale request's body is credited to the height's current owner

`stale-body-credits-current-owner.patch` is a mutation-loop patch for the `block-sync-table` work-list
entry. Fault class: ownership.

## What it breaks

When a body matches an outstanding request that no longer owns its
height in the work queue, the routine drops the request. The patch instead
credits the body to the request that holds the height now, as
first-completion-wins does for an unmatched body, but without the hash
check that path makes. `begin_receipt_for_owner` ends the current owner's
reservation, and the body reaches the sequencer under the current owner.

## Witness

The witness is the `block_table` test
`a_stale_request_body_holds_receiver_work_authorized`. It searches
generated Plans of the reset-mid-flight scenario, which resets the chain to
a fork while two mirrors serve, for a run in which one routine judges an
old-chain body after the other routine's fork request reserved that
body's height, while the first routine's old request is still local. The
patch credits that body to the fork request.

`block-sync/s6v3/block/receiver-work-authorized` judges the body when it
enters the sequencer's input queue. The credited request's connection
carried no frame for the body, so the rule fails there.
`block-sync/submit-only-requested` judges only the later submission, so it
never sees the body first.

`qa/planted/block-sync/submit-only-requested/height-only.toml`
(`block-sync-reset-mid-flight.bin`) never drives this order: every
old-epoch body crosses before the reset.

## Remaining guards

The sequencer's epoch check in `offer_body` still holds, but the
re-owned body carries the current epoch.

## Reproduce

```sh
git apply qa/native-mutations/submit-only-requested/stale-body-credits-current-owner/stale-body-credits-current-owner.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/s6v3/block/receiver-work-authorized
```

Required failure: the run ends at `block-sync/s6v3/block/receiver-work-authorized` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
