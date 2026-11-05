# Raw budget reservation

`raw-budget-reservation.patch` takes one more header from the budget each
time the queue reserves for a peer, with no observer, and forgets it. Nothing
reports the reservation and nothing ends it, so the queue's accounting never
sees it and every owner event stays clean.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. The first quiet
boundary after the node's first reservation reads one header more reserved
than the ledger holds. The reactor tap fails the run with `ReservedMismatch:
at a quiet boundary the header budget holds 1 reserved, and the queue's
accounting 0`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/raw-budget-reservation/raw-budget-reservation.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/raw-budget-reservation/raw-budget-reservation.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `ReservedMismatch` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
