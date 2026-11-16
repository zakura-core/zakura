# First-completion-wins credits any hash

`competing-receipt-any-hash.patch` is a mutation-loop patch for the `block-sync-table` work-list
entry. Fault class: request-authorization.

## What it breaks

First-completion-wins credits an unmatched body to whichever published
request reserves its height. Two hash checks stand in sequence on that
path: `accept_unmatched_queued_body` compares the body's hash with the
queue's hash for the height, and `end_competing_reservation_at_receipt`
compares it again under the queue lock. The patch removes both, so a body
of another chain at a reserved height ends that request's reservation and
reaches the sequencer under its owner.

## Witness

The witness is the `block_table` test
`old_chain_bodies_after_a_reset_hold_receiver_work_authorized`. It runs
the reset-mid-flight scenario under the plan of
`crates/zakura-sim/tests/fixtures/block-sync-reset-mid-flight.bin`, but
resets once the mirrors have completed 14 range reads. Old-chain bodies of
heights past the fork are then still on the wire. One reaches its routine
after the routine dropped its request, at a height that a fork request on
the other connection reserves. The patch credits that body to the fork
request.

The input `block-sync-reset-mid-flight.bin` itself never drives this path:
every old-epoch body crosses before its reset.

`block-sync/s6v3/block/receiver-work-authorized` judges the body when it
enters the sequencer's input queue. The credited request's connection
carried no frame for the body, so the rule fails there.
`block-sync/submit-only-requested` judges only the later submission, so it
never sees the body first.

## Remaining guards

Both hash guards are broken. The sequencer's epoch check passes because
the credited owner is current.

## Reproduce

```sh
git apply qa/native-mutations/submit-only-requested/competing-receipt-any-hash/competing-receipt-any-hash.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/s6v3/block/receiver-work-authorized
```

Required failure: the run ends at `block-sync/s6v3/block/receiver-work-authorized` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
