# Dropped owner check

`dropped-owner-check.patch` drops the owner and call checks from the prepare
handler's `settles_round`, so a stale prepare settles whatever round its peer
is preparing. That's `header-sync/stale-completion-inert`'s negative control
in the hdr-6–8 plan.

The witness is `prepare_completed_after_replacement_is_inert` in
`crates/zakura-sim/tests/header_node_late.rs`. Session 1's prepare is held
until `mirror-0` relinks as session 2 and session 2's own prepare is
submitted, and session 2's prepare is held until the old one is handled. So
when the old prepare completes, session 2's round is still preparing, and the
mutant settles it under the old call. Its handler then returns session 2's
reservation, which violates `header-sync/stale-completion-inert`.

It stays a native mutation, since the storage grammar can't encode a hold
with a deadline, which the witness's holds need. The planted cases in
`qa/planted/header-sync/stale-completion-inert` cover the stale handler's
effects on a committed input.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_late \
  prepare_completed_after_replacement_is_inert
git apply qa/native-mutations/header-sync/dropped-owner-check/dropped-owner-check.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_late \
  prepare_completed_after_replacement_is_inert
git apply -R qa/native-mutations/header-sync/dropped-owner-check/dropped-owner-check.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only a `header-sync/stale-completion-inert`
violation counts as detection. A build error, a timeout, a panic, a harness
failure, or another property's failure doesn't.
