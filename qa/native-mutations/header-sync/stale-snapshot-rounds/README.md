# New rounds from a stale snapshot copy

`stale-snapshot-rounds.patch` gives the reactor a copy of the snapshot it was
built with and stages every new offer under that copy's header authority,
instead of the snapshot it adopted last. Rounds offered after the first
adoption read their locator under the starting generation.

Witness: `new_work_carries_the_adopted_generation` in
`crates/zakura-sim/tests/header_node_adoption.rs`, on the `grow_witness`
scenario in `header-sync/storage`. Its second growth is offered after the
node adopted the first.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply qa/native-mutations/header-sync/stale-snapshot-rounds/stale-snapshot-rounds.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply -R qa/native-mutations/header-sync/stale-snapshot-rounds/stale-snapshot-rounds.patch
```

Required failure: the generation rule breaks with `StaleGeneration`, so
`new_work_carries_the_adopted_generation` fails. The status and tip tests
still pass. The mirrors run the same patched reactor, so the witness no longer
has the shape `the_grow_witness_holds_a_read_across_the_growth` checks, and
that test fails too. It checks no rule.
