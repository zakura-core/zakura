# Skipped round release

`skipped-round-release.patch` drops the `release_capacity` call from
`PeerWorkQueue::remove`, so a removed round keeps its peer's reservation and
the queue's own lease clones. The queue still reports the removal, since it
reports it from `remove` itself, after the release, however that went.

The witness is `a_round_retires_with_headers_still_reserved` in
`crates/zakura-sim/tests/header_node_reservations.rs`. It closes `mirror-0`'s
link right after the node reserves for it, so a round retires with headers
still reserved. The reactor tap's ownership ledger fails the run with
`LeakedReservation: the queue released round 1 with its peer still holding
reservation 2`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply qa/native-mutations/header-sync/skipped-round-release/skipped-round-release.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply -R qa/native-mutations/header-sync/skipped-round-release/skipped-round-release.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `LeakedReservation` harness failure counts
as detection. A build error, a timeout, a panic or another property's failure
doesn't.
