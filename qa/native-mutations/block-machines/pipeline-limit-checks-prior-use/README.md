# The pipeline limit checks the bytes already held

`DecodedBlockPool::charge_pipeline` compares the pipeline bytes held
before the admission with the limit, not the total after it. A pool at its
limit admits one more block of any size.

The witness's history fills pool 0's pipeline with three blocks and admits
a fourth (step 18). The model refuses it for capacity, and the patched pool
admits it.

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
