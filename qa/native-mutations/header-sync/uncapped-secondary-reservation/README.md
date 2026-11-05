# Uncapped secondary reservation

`uncapped-secondary-reservation.patch` reruns `secondary-reservation` with
the budget's own capacity checks bypassed: `HeaderChunkUsage::reserve` no
longer refuses a count above what's unclaimed, and `HeaderChunkBudget::publish`
drops its debug assertion. The second reservation takes the whole 4,000-header
budget on top of the peer's own.

Unlike `secondary-reservation`, it has no observer, so nothing reports it.
A reported one fails the ledger's `SecondReservation` rule first, before any
read. Unreported, it also leaves the queue's accounting short, but every read
checks the capacity bound first.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. The read after the
node's first reservation finds the budget over capacity. The reactor tap
fails the run with `BudgetOverCapacity: the header budget holds 4250 reserved
and 0 owned, over its 4000 headers`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/uncapped-secondary-reservation/uncapped-secondary-reservation.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/uncapped-secondary-reservation/uncapped-secondary-reservation.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `BudgetOverCapacity` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
