# A receipt leaves its charge in the pool

`WorkQueue::split_receipt_funding` moves only the retired request's bytes
into the body handoff, not the receipt's own charge. The receipt's funding
stays in the queue's pool, so the handoff holds no budget for the body it
carries.

The witness's history receives the published request's first height
(step 7). The model moves its 100 bytes into the receipt. The patched queue
moves none, so the pooled funding and receipt checks fail.

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
