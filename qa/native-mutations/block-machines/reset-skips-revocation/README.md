# A reset drops a reserved height without a revocation

`WorkQueueState::reset_above` reports a revocation only for an answered
height, not for a reserved one. The routine that owns the dropped request
never learns that its work left it.

The witness's history resets above floor 0 while the first claim's
published height is in flight and reserved (step 4). The model expects a
revocation, and the patched owner reports none.

## Witness

`qa/planted/block/work-ownership/missing-published-charge.toml`, the work list's witness for `block/work-ownership`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block/work-ownership --out <report dir>
```

Required failure: the run ends at `block/work-ownership`.
