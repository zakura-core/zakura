# A new authority keeps the old authority's query

`ReactorState::observe_committed_view` no longer clears the query state
when the body-work authority changes. The pending query, its retry gate and
its owed force outlive the authority that dispatched them.

The witness's history commits authority 1 while query 3 of authority 0 is
pending (step 8). The model retires the query, and the patched reactor keeps
it.

## Witness

`qa/planted/block/reactor-policy/refusal-consumes-query-id.toml`, the work list's witness for `block/reactor-policy`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block/reactor-policy --out <report dir>
```

Required failure: the run ends at `block/reactor-policy`.
