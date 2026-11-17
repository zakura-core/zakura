# Readmission not checked

`readmission-unchecked.patch` removes the readmission check at the start of
`handle_peer_connected`. The reactor then admits a session from a peer whose
readmission cooldown still holds, and reports `SessionAdmitted` where it
should report `SessionRefused`.

Witness, in `zakura-network`'s reactor tests:
`a_late_fault_cools_down_the_peer_and_refuses_its_reconnect` in
`crates/zakura-network/src/zakura/header_sync/reactor/tests/observer.rs`. A
session disconnects while the node prepares its invalid headers, the prepare
then fails, and the peer reconnects. The test hands the reconnect straight to
the reactor.

```sh
cargo test --locked -p zakura-network --lib \
  header_sync::reactor::tests::observer::a_late_fault_cools_down_the_peer_and_refuses_its_reconnect
git apply qa/native-mutations/header-sync/readmission-unchecked/readmission-unchecked.patch
cargo test --locked -p zakura-network --lib \
  header_sync::reactor::tests::observer::a_late_fault_cools_down_the_peer_and_refuses_its_reconnect
git apply -R qa/native-mutations/header-sync/readmission-unchecked/readmission-unchecked.patch
```

Required failure: the test's event assertion for the 60 s cooldown, which
sees `SessionAdmitted` for session 2 after `CooldownStarted` instead of
`SessionRefused`. A build error, a timeout or a panic elsewhere doesn't count.

The simulator can't reach this check. In `header-sync/wire`, the header
service reads the back-off the reactor publishes with each cooldown and parks
the redialed session before handing it over, so
`a_late_fault_cools_down_the_peer_and_refuses_its_redial` still passes with
this patch. `header-sync/storage` hands sessions straight to the reactor, but
nothing there makes a header-peer fault. If a run did reach the reactor, the
admission would break `header-sync/header-fault-penalized` as
`ReadmittedInCooldown`.
