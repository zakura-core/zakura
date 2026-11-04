# Tip watch not sent after startup

`unsent-tip.patch` removes the reactor's tip-watch send when an adopted
snapshot changes the selected tip. Only the watch's initial value is ever
published.

Witness: `every_adopted_tip_is_published` in
`crates/zakura-sim/tests/header_node_adoption.rs`, on the `grow_witness`
scenario in `header-sync/storage`.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply qa/native-mutations/header-sync/unsent-tip/unsent-tip.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply -R qa/native-mutations/header-sync/unsent-tip/unsent-tip.patch
```

Required failure: the tip rule breaks with `UnpublishedTip`, so
`every_adopted_tip_is_published` fails. The generation and status tests
still pass. The mirrors run the same patched reactor, so the witness no longer
has the shape `the_grow_witness_holds_a_read_across_the_growth` checks, and
that test fails too. It checks no rule.
