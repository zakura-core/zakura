# A reset releases the ledger but not its funding

`WorkQueue::reset_above` drops the reset heights' charges from the
logical ledger but never settles their bytes from the pooled funding. The
pool keeps budget that no request owns, and admission loses that capacity
until the queue drops.

The witness's history resets above floor 0 while the published request's
second height is reserved (step 8). The model releases its 100 bytes from
the pool, and the patched queue keeps them, so the pooled funding check
fails.

## Witness

`qa/planted/block/work-funding/lost-reservation-release.toml`, the work list's witness for `block/work-funding`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block/work-funding --out <report dir>
```

Required failure: the run ends at `block/work-funding`.
