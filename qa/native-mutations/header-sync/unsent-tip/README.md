# Tip watch not sent after startup

`unsent-tip.patch` removes the reactor's tip-watch send when an adopted
snapshot changes the selected tip. Only the watch's initial value is ever
published.

Witnesses, in `crates/zakura-sim/tests/header_node_adoption.rs` on
`header-sync/storage`: `every_adopted_tip_is_published`, on the
`grow_witness` scenario, where the next adoption shows the earlier tip
unpublished, and `a_single_tip_change_is_published_by_a_settled_end`, where
only the run's settled end does.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply qa/native-mutations/header-sync/unsent-tip/unsent-tip.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply -R qa/native-mutations/header-sync/unsent-tip/unsent-tip.patch
```

Required failure: the tip rule breaks with `UnpublishedTip`, so
`every_adopted_tip_is_published` and
`a_single_tip_change_is_published_by_a_settled_end` fail. The generation and status tests
still pass. The mirrors run the same patched reactor, so the witness no longer
has the shape `the_grow_witness_holds_a_locator_across_the_growth` checks, and
that test fails too. It checks no rule.
