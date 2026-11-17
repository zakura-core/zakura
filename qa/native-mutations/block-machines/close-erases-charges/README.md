# Closing a pool erases its charges

`DecodedBlockPool::close` resets the pool's decoded-byte counter. Blocks
that their final holders still retain no longer count, so a closed
generation loses its final-holder settlement.

The witness's history closes pool 0 while it holds three blocks (step 20).
The model keeps their bytes charged, and the patched pool reports none.

## Witness

`qa/planted/block/decoded-retention/missing-admission.toml`, the work list's witness for `block/decoded-retention`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block/decoded-retention --out <report dir>
```

Required failure: the run ends at `block/decoded-retention`.
