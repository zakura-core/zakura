# Publication releases the admitted funding

`UnpublishedRequest::publish` merges the admitted funding into a
temporary owner instead of the queue's pool. The ledger charges the
published request, and the budget releases its bytes when the temporary
owner drops, so admission can pass the budget limit.

The witness's history publishes the admitted first request (step 4). The
model keeps its 200 bytes in the pool, and the patched queue releases them.

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
