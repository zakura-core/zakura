# Foreign leases

`foreign-leases.patch` has `retain_headers` give a call every peer's staged
leases, not just its own round's. When two mirrors' rounds overlap, a call
holds another round's leases.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Two mirrors on
different forks stage pages at the same time under some plans. The reactor
tap's ownership ledger fails the run with `ForeignLease: call ... holds round
... with a lease staged for round ...`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/foreign-leases/foreign-leases.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/foreign-leases/foreign-leases.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `ForeignLease` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
