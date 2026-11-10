# Requests near the floor count as floor requests

`request_priority` (`admission.rs`) gives floor priority to a request that
starts up to four heights above the download floor, not only at the floor
or the next height. Floor priority lets `TakenRequest::admit` charge past
`M` when the reservation fails, so more than two requests can overdraft
the byte budget at once.

The existing planted case `above-floor-charge` lets every above-floor
request charge. This patch keeps the charge gate and widens which requests
it counts as floor requests.

## Guards on the rule's path

1. `admit` (`admission.rs`) gives a floor grant at least one byte. The
   patch reaches it through the priority it computes.
2. `TakenRequest::admit` (`work_queue/request_write.rs`) charges past `M`
   only for floor priority, which it computes with the same
   `request_priority`.

## Witness

`qa/planted/block-sync/reserved-byte-bound/above-floor-charge.toml`: the committed input `block-sync-paused-handoffs.bin`.
Its `M` is `1.5·B` and every estimate is `B`, so each request past the
first refuses its reservation. Four mirrors ask for heights just above the
floor, and each such request now charges `B` past `M`.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/reserved-byte-bound --out <report dir>
```

Required failure: the run ends at `block-sync/reserved-byte-bound`.
