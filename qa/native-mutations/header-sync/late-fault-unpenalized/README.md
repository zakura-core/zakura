# Late header fault left unpenalized

`late-fault-unpenalized.patch` reverts #2001's early-return fix. When a
prepare or apply fails after its round retired, for example because the peer
disconnected while the node was preparing its headers, the reactor returns
before it classifies the failure. A header-peer fault that settles late then
reports no `Misbehavior`.

Witness, in `crates/zakura-sim/tests/header_wire_prepare_fault.rs` on
`header-sync/wire`: `a_disconnect_mid_prepare_is_still_penalized`, where the
peer's page passes the wire checks, the peer disconnects while the node's
storage holds that page's preparation, and the preparation then fails with an
`InvalidHeader` that blames the peer.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_wire_prepare_fault
git apply qa/native-mutations/header-sync/late-fault-unpenalized/late-fault-unpenalized.patch
cargo test --locked --profile planted -p zakura-sim --test header_wire_prepare_fault
git apply -R qa/native-mutations/header-sync/late-fault-unpenalized/late-fault-unpenalized.patch
```

Required failure: the penalty rule breaks with `UnpenalizedFault`, so
`a_disconnect_mid_prepare_is_still_penalized` fails. No registered property
fails, and `an_invalid_page_is_judged_at_prepare_and_penalized`, where the
session is still current when the preparation fails, still passes.
