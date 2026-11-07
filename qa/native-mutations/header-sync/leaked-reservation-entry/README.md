# Leaked reservation entry

`leaked-reservation-entry.patch` drops the reservation removal from
`PeerWorkQueue::release_capacity`, so a peer's reservation entry outlives the
work it backs.

The witness is `a_round_retires_with_headers_still_reserved` in
`crates/zakura-sim/tests/header_node_reservations.rs`. It closes `mirror-0`'s
link right after the node reserves for it, so a round retires with headers
still reserved. A round that reaches its target short of its reservation
retires the same way. The reactor tap's ownership ledger fails the run with
`LeakedReservation: the queue released round 1 with its peer still holding
reservation 2`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply qa/native-mutations/header-sync/leaked-reservation-entry/leaked-reservation-entry.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply -R qa/native-mutations/header-sync/leaked-reservation-entry/leaked-reservation-entry.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `LeakedReservation` harness failure counts
as detection. A build error, a timeout, a panic or another property's failure
doesn't.
