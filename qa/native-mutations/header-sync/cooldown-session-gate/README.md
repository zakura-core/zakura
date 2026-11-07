# Cooldown gated on the current session

`cooldown-session-gate.patch` restores the gate that #2001 removed from
`dispatch_misbehavior`: the reactor starts a readmission cooldown only when
the faulted work's session is still current. A fault that settles after its
session ended, for example because the peer disconnected while the node
prepared its headers, still reports `Misbehavior`, but installs no cooldown
and reports no `CooldownStarted`.

Witness, in `crates/zakura-sim/tests/header_wire_cooldown.rs` on
`header-sync/wire`: `a_late_fault_cools_down_the_peer_and_refuses_its_redial`.
The peer's page passes the wire checks, the peer disconnects while the node's
storage holds that page's preparation, and the preparation then fails with an
`InvalidHeader` that blames the peer.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_wire_cooldown \
  a_late_fault_cools_down_the_peer_and_refuses_its_redial
git apply qa/native-mutations/header-sync/cooldown-session-gate/cooldown-session-gate.patch
cargo test --locked --profile planted -p zakura-sim --test header_wire_cooldown \
  a_late_fault_cools_down_the_peer_and_refuses_its_redial
git apply -R qa/native-mutations/header-sync/cooldown-session-gate/cooldown-session-gate.patch
```

Required failure: the run ends at `header-sync/header-fault-penalized` with
`UncooledFault: prepare operation <n> failed with a header-peer fault of
<peer> in session <s>, and the reactor installed no 60s readmission cooldown
for it after its Misbehavior`, at the step after the reactor handled the
failed preparation. A build error, a timeout, a panic or another property's
failure doesn't count. The unit witness
`a_late_fault_cools_down_the_peer_and_refuses_its_reconnect` in
`zakura-network` fails too, since it sees no `CooldownStarted`.
