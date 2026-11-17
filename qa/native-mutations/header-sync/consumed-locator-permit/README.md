# Consumed locator permit

`consumed-locator-permit.patch` takes a second permit from the reactor's
locator semaphore each time dispatch takes one, outside dispatch's own
accounting, and consumes it with `OwnedSemaphorePermit::forget`. That never
returns the permit and lets go of the semaphore, so once the reactor drops
the semaphore no stray permit keeps it alive. The resource probe holds the
semaphore itself, so the teardown read still sees the permits missing. The
reactor reports no permits, and the ownership ledger doesn't count them.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. The node takes two
locator reads before its shutdown.
The reactor tap finds `TeardownLocatorPermits: the reactor is gone
with 14 of 16 locator permits free, and storage holds 0`, which violates
`header-sync/teardown-returns-baseline` at node settlement. It stays a native
mutation, since no committed input can shut the node down until the storage
grammar has a shutdown step.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/consumed-locator-permit/consumed-locator-permit.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/consumed-locator-permit/consumed-locator-permit.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only a `header-sync/teardown-returns-baseline`
violation naming `TeardownLocatorPermits` counts as detection. A build error,
a timeout, a panic, a harness failure or another property's failure doesn't.
