# Short payload

`short-payload.patch` has `begin_prepare` leave the round's last staged
header out of the prepare it sends. The prepare still names its round's owner
and target, and holds all of the round's leases, so only its payload is
wrong.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages and prepares them. The reactor tap's ownership ledger fails the
run with `WrongPayload: call 3 sends 4 headers ... for round 1, which holds 5
staged through ...`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/short-payload/short-payload.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/short-payload/short-payload.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `WrongPayload` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
