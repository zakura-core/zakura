# Kept queue lease

`kept-queue-lease.patch` drops the lease removal from
`PeerWorkQueue::release_capacity`, so the queue keeps its own clones of a
removed round's leases. A storage call may still hold the same leases, so
`LeaseReturned` alone can't show it. The queue's release report counts the
headers its clones still hold, and that must be 0.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages, so its removal finds the queue's clones. The reactor tap's
ownership ledger fails the run with `LeakedLease: the queue released round 1
still holding leases of 5 headers for its peer`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/kept-queue-lease/kept-queue-lease.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/kept-queue-lease/kept-queue-lease.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `LeakedLease` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
