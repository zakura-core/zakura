# Stale `Status` cache

`stale-status-cache.patch` stops the reactor updating an existing session's
status publisher when it adopts a new snapshot. A session admitted earlier
keeps advertising the state it was admitted under.

Witness: `every_live_session_gets_the_adopted_status` in
`crates/zakura-sim/tests/header_node_adoption.rs`, on the `grow_witness`
scenario in `header-sync/storage`.

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply qa/native-mutations/header-sync/stale-status-cache/stale-status-cache.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_adoption
git apply -R qa/native-mutations/header-sync/stale-status-cache/stale-status-cache.patch
```

Required failure: the status rule breaks with `StatusOverdue` or
`StaleStatus`, so `every_live_session_gets_the_adopted_status` fails. The
generation and tip tests still pass. The mirrors run the same patched reactor,
so the witness no longer has the shape
`the_grow_witness_holds_a_read_across_the_growth` checks, and that test fails
too. It checks no rule.
