# Readmission cooldown installed short

`short-cooldown.patch` makes `start_readmission_cooldown` install half the
configured cooldown. The reactor still starts a cooldown after each
header-peer fault, but the peer gets back in after 30 s instead of 60 s.
`CooldownStarted` reports the entry the reactor installed, so it says 30 s.

Witness, in `crates/zakura-sim/tests/header_wire_cooldown.rs` on
`header-sync/wire`: `a_late_fault_cools_down_the_peer_and_refuses_its_redial`.
The peer's page passes the wire checks, the peer disconnects while the node's
storage holds that page's preparation, and the preparation then fails with an
`InvalidHeader` that blames the peer.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_wire_cooldown \
  a_late_fault_cools_down_the_peer_and_refuses_its_redial
git apply qa/native-mutations/header-sync/short-cooldown/short-cooldown.patch
cargo test --locked --profile planted -p zakura-sim --test header_wire_cooldown \
  a_late_fault_cools_down_the_peer_and_refuses_its_redial
git apply -R qa/native-mutations/header-sync/short-cooldown/short-cooldown.patch
```

Required failure: the run ends at `header-sync/header-fault-penalized` with
`UncooledFault: prepare operation <n> failed with a header-peer fault of
<peer> in session <s>, and the reactor installed no 60s readmission cooldown
for it after its Misbehavior`, at the step after the reactor handled the
failed preparation. No reconnect after the 30 s mark is needed to see it. A
build error, a timeout, a panic or another property's failure doesn't count.
The unit witness `a_late_fault_cools_down_the_peer_and_refuses_its_reconnect`
in `zakura-network` fails too, since its `CooldownStarted` says 30 s.
