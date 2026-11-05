# A stale locator read starts a round

`stale-locator-round.patch` drops the header-generation comparison from
`AdvertisedHeaderTarget::is_current`. An adoption that changes the generation
then no longer retires a target still awaiting its locator read, and the read,
once it settles, starts a round under the generation it was taken in.

Witness: `new_work_carries_the_adopted_generation` in
`crates/zakura-sim/tests/header_node_adoption.rs`, on the `grow_witness`
scenario in `header-sync/storage`. The scenario holds the node's first locator
read, for `mirror-1`'s target, until the node has applied `mirror-0`'s growth
under a new generation. Without that hold the read settles before any
adoption, and this patch goes undetected.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply qa/native-mutations/header-sync/stale-locator-round/stale-locator-round.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply -R qa/native-mutations/header-sync/stale-locator-round/stale-locator-round.patch
```

Required failure: the generation rule breaks with `StaleGeneration` for a
started request, so `new_work_carries_the_adopted_generation` fails. Every
other test, including the witness's shape test, still passes.
