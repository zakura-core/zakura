# Native own-9 join mutations

`reuse-first-session-id.patch` makes the handler hand a reopened header
session the session ID of the generation that exited before it, while the
transport still reports the reopened session's own ID and generation. The
witness is `a_reopen_joins_a_second_generation_on_one_connection` in
`crates/zakura-sim/tests/header_wire_sessions.rs`. The peer finishes its
header stream and the node reopens it on the same connection. With the
patch, the second admission's origin names the new QUIC stream, but its
session ID is the first one's, so the session view fails the run with
`Own9Join: the node admitted session <first> from peer-0 on connection 1's
stream 8, but its Session row has session <second>`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_wire_sessions \
  a_reopen_joins_a_second_generation_on_one_connection
git apply qa/native-mutations/header-sync/own9-join/reuse-first-session-id.patch
cargo test --locked --profile planted -p zakura-sim --test header_wire_sessions \
  a_reopen_joins_a_second_generation_on_one_connection
git apply -R qa/native-mutations/header-sync/own9-join/reuse-first-session-id.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `Own9Join` harness failure naming the
Session row's session counts as detection. A build error, a timeout, a panic
or another property's failure doesn't.
