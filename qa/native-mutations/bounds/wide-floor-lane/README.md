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

The committed input `crates/zakura-sim/tests/fixtures/block-sync-floor-lane-race.bin`
(paused handoffs), which the test
`block_resource_properties::the_floor_lane_race_input_holds_reserved_byte_bound`
also runs. A seeded random search on the remote host found it. In its run,
one routine probes the floor's next height, another routine takes that
height first, and the first routine's floor take then starts above it. The
unpatched node refuses that take for capacity. The patch counts it as a
floor request, so it charges past `M` and the run ends at the reserved
bound.

`above-floor-charge.toml`'s input never drives that race. The patch
survived on it and on `every_worker_count_holds_the_resource_bounds` and
`the_floor_churn_holds_the_reserved_bound`.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/reserved-byte-bound --out <report dir>
```

Required failure: the run ends at `block-sync/reserved-byte-bound`.
