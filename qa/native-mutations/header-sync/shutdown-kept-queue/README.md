# Shutdown kept queue

`shutdown-kept-queue.patch` makes the reactor's shutdown forget its work
queue instead of retiring each peer's work. The queue's reservations and
leases then outlive the reactor, and no round is released, so only node
settlement can see them. The queue also keeps a clone of the reactor's
observer, which is why node settlement keys on the reactor's task ending
rather than on its observer.

The witness is `the_probe_matches_the_accounting_through_close_relink_and_shutdown`
in `crates/zakura-sim/tests/header_node_capacity.rs`. Once the stopped
reactor's task ends, the teardown read finds `TeardownReserved: the stopped
reactor is gone with 200 headers still reserved`, which violates
`header-sync/teardown-returns-baseline` at node settlement. It stays a native
mutation, since no committed input can shut the node down until the storage
grammar has a shutdown step.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply qa/native-mutations/header-sync/shutdown-kept-queue/shutdown-kept-queue.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_capacity \
  the_probe_matches_the_accounting_through_close_relink_and_shutdown
git apply -R qa/native-mutations/header-sync/shutdown-kept-queue/shutdown-kept-queue.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only a `header-sync/teardown-returns-baseline`
violation at node settlement counts as detection. A build error, a timeout, a
panic, a harness failure such as `ShutdownIncomplete`, or another property's
failure doesn't.
