# A replayed record inherits confirmation

`update_existing_entry` keeps the confirmed and dialed addresses when a
record that replaces an expired one has no higher sequence. A relay that
replays an old record of a confirmed node gets it disclosed in peer samples
without a dial.

The witness's history has relay 0 supply author 0's sequence 2 over its
expired, dial-confirmed sequence 3 (step 11). The model drops the
confirmation, and the patched book keeps it.

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
