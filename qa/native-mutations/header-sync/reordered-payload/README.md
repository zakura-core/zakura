# Reordered payload

`reordered-payload.patch` has `begin_prepare` swap the first two of the
round's staged headers in the prepare it sends. Its count, its target and its
last header all hold, so only the sequence differs.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages and prepares them. The reactor tap's ownership ledger fails the
run with `WrongPayload: call 3 sends 5 headers toward ... for round 1, which
holds 5 staged through ..., and they aren't its staged headers in order`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/reordered-payload/reordered-payload.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/reordered-payload/reordered-payload.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `WrongPayload` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
