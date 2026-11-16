# A destructive reset keeps the pending query

`ReactorState::observe_view` no longer clears the query state when the
Sequencer's reset epoch advances. The pending query's bounds predate the
reset, and a reply to it can still complete.

The witness's history delivers a view with a reset while query 2 is pending
(step 7). The model retires the query, and the patched reactor keeps it, so
the check before the reaction's refill fails.

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
