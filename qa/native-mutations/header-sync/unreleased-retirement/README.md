# Unreleased retirement

`unreleased-retirement.patch` has `retire_peer_work` report a round's
retirement while the queue keeps the round, so neither its capacity release
nor the queue's release report happens.

The witness is `a_round_retires_with_headers_still_reserved` in
`crates/zakura-sim/tests/header_node_reservations.rs`. It closes `mirror-0`'s
link right after the node reserves for it, so the node retires the round. The
reactor tap's ownership ledger fails the run with `UnreleasedRound: round 1
retired while the queue still holds it`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply qa/native-mutations/header-sync/unreleased-retirement/unreleased-retirement.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  a_round_retires_with_headers_still_reserved
git apply -R qa/native-mutations/header-sync/unreleased-retirement/unreleased-retirement.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `UnreleasedRound` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
