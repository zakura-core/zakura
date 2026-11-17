# Swapped payload

`swapped-payload.patch` has `begin_prepare` swap the first and last of the
round's staged headers in the prepare it sends. The prepare keeps its owner,
its target, its count and all its leases, so only the headers it actually
sends differ.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages and prepares them. The reactor tap's ownership ledger fails the
run with `WrongPayload: call 3 sends 5 headers ending at Some(...) toward ...
for round 1, which holds 5 staged through ...`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/swapped-payload/swapped-payload.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/swapped-payload/swapped-payload.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `WrongPayload` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
