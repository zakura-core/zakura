# Eviction keeps the relay's attribution

`ZakuraDiscoveryBook::remove_entry` removes the entry but not its relay
attribution. Every eviction leaves its relay charged, so a relay's quota
fills with entries the book no longer holds. The retained planted patch
`keep-old-relay-attribution` breaks the update path. This patch breaks the
removal path.

The witness's history fills relay 0's quota with two lapsed records, then
imports a third record through relay 0 (step 13). The book evicts the two
lapsed records. The model's relay 0 then holds one entry, and the patched
book counts three.

## Witness

`qa/planted/discovery/record-book/skip-book-eviction.toml`, the work list's witness for `discovery/record-book`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/record-book --out <report dir>
```

Required failure: the run ends at `discovery/record-book`.
