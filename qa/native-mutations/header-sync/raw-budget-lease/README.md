# Raw budget lease

`raw-budget-lease.patch` turns one more header into owned capacity each time
the queue stages a page, through a raw reservation with no observer whose
lease it forgets. The raw reservation is spent at once, so the budget's
reserved count stays right and only its owned count drifts. Nothing reports
the lease and nothing returns it.

The plan's mutation here was a kept clone of a staged lease. That clone also
holds back the lease's `LeaseReturned`, and the ownership ledger already
fails it as `LeakedLease` when the queue releases the round, as
`kept-queue-lease` shows. An untagged lease is the owned-side stray that only
the probe can see.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. The first quiet
boundary after the node stages a page reads one header more owned than the
ledger holds. The reactor tap fails the run with `OwnedMismatch: at a quiet
boundary the header budget holds 251 owned, and the queue's accounting 250`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/raw-budget-lease/raw-budget-lease.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/raw-budget-lease/raw-budget-lease.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `OwnedMismatch` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
