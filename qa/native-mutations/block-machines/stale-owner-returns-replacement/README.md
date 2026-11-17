# A stale owner returns its replacement's height

`WorkQueueState::return_owned` skips a height only when no owner holds it,
not when another owner does. A request that a reset retired can then return
the height its replacement took and published, and release that charge.

The witness's history resets the first claim's height, takes and publishes
it again under a second request, then returns the first claim (step 8). The
model counts the height as missing for the stale owner. The patched owner
moves it back to `pending` and releases the second request's charge, so the
step's outcome check fails.

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
