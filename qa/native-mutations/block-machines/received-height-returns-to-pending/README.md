# A received height returns to pending work

`WorkQueueState::return_owned` no longer leaves a received height in
place. A request that returns its heights after its body arrived puts that
height back in `pending`, so another request downloads it again.

The witness's history receives the second request's height and then returns
the request (steps 9 and 10). The model counts the height as received. The
patched owner moves it to `pending`.

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
