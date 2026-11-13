# Forgotten locator permit

`forgotten-locator-permit.patch` forgets a clone of each locator permit the
reactor takes, so the permit never returns to the reactor's locator
semaphore. The kept clone also keeps the semaphore alive.
`consumed-locator-permit` covers a permit that lets go of it. The reactor
reports no permits, and the ownership ledger doesn't count them, so every
owner event stays clean.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. The node takes two
locator reads before its shutdown, and the teardown read once its reactor
is gone finds both permits still out. The reactor tap fails the run with
`TeardownLocatorPermits: the stopped reactor is gone with 14 of 16 locator
permits free, and storage holds 0`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/forgotten-locator-permit/forgotten-locator-permit.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/forgotten-locator-permit/forgotten-locator-permit.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `TeardownLocatorPermits` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
