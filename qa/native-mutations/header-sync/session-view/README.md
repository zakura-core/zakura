# Native session-view mutations

`keep-replaced-session-work.patch` drops the `retire_peer_work` call in
`handle_peer_connected`, so a replacement leaves the old session's round
active. The witness is
`a_relink_replaces_the_session_and_its_held_prepare_stays_inert` in
`crates/zakura-sim/tests/header_node_sessions.rs`. It holds the old session's
first prepare until the replacement is admitted. With the patch, the released
prepare's completion dispatches an apply under the replaced session, and the
reactor tap's session view fails the run with
`DepartedSessionWork: call 3 applies mirror-0's session 1, after it left the
reactor's view`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_sessions \
  a_relink_replaces_the_session_and_its_held_prepare_stays_inert
git apply qa/native-mutations/header-sync/session-view/keep-replaced-session-work.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_sessions \
  a_relink_replaces_the_session_and_its_held_prepare_stays_inert
git apply -R qa/native-mutations/header-sync/session-view/keep-replaced-session-work.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `DepartedSessionWork` harness failure counts
as detection. A build error, a timeout, a panic or another property's failure
doesn't.
