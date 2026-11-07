# Secondary reservation

`secondary-reservation.patch` takes a second reservation from the budget for a
peer that just reserved, and forgets it outside `request_reservations`, so the
queue's accounting never ends it. The budget reports every reservation it
makes, so the patch adds no report of its own. `uncapped-secondary-reservation`
reruns it unreported, with the budget's own capacity checks bypassed, where
the resource probe's read fails `BudgetOverCapacity`.

The witness is `a_round_retires_with_headers_still_reserved` in
`crates/zakura-sim/tests/header_node_reservations.rs`. The node's first
reservation for `mirror-0` is enough. The reactor tap's ownership ledger fails
the run with `SecondReservation: reservation 2 opened for a peer that still
holds reservation 1`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply qa/native-mutations/header-sync/secondary-reservation/secondary-reservation.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply -R qa/native-mutations/header-sync/secondary-reservation/secondary-reservation.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `SecondReservation` harness failure counts
as detection. A build error, a timeout, a panic or another property's failure
doesn't.
