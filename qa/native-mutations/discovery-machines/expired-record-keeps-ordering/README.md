# An expired record still orders its node's records

`update_existing_entry` treats every stored record as current. An expired
record with a higher sequence then refuses every lower sequence, so a node
that reset its sequence never recovers in this book.

The witness's history imports author 0's sequence 2 over its expired
sequence 3 (step 11). The model replaces the record, and the patched book
ignores the import as older.

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
