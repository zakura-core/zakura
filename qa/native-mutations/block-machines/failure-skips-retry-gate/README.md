# A failed query retries without its gate

`QueryState::fail` retires the pending query but arms no retry gate. The
next refill dispatches the same query at once, so a failing state service
gets a query on every reactor wake.

The witness's history fails the pending query under authority 1 (step 11).
The model arms the retry gate, and the patched reactor leaves it unarmed, so
the state check fails.

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
