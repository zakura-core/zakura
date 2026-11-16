# Skipped retention

`skipped-retention.patch` has `retain_for_call` give a call that takes no
handoff none of its round's headers, so a prepare runs without holding the
capacity its round's leases charge.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
prepares its pages. The reactor tap's ownership ledger fails the run with
`UnheldCall: call 3 ended without ever holding its round's headers`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/skipped-retention/skipped-retention.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/skipped-retention/skipped-retention.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `UnheldCall` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
