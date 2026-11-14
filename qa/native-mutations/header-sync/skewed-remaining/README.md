# Skewed remaining

`skewed-remaining.patch` has `HeaderCountReservation::consume` store one
header fewer than a page left in the reservation. The budget's counts stay
right, so only the reservation's own balance is wrong. The staging report
reads that balance from the queue's own read of it, so it carries the skew
while the reservation is still live.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages a page from its reservation. The reactor tap's ownership ledger fails
the run with `OverReservation: a page of 5 headers spent reservation 1, which
held ... and reports ... left`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/skewed-remaining/skewed-remaining.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/skewed-remaining/skewed-remaining.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `OverReservation` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
