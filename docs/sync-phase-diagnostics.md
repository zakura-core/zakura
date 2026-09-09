# Sync phase diagnostics

Enable the `sync_phase` tracing target at debug level on a disposable diagnostic node:

```toml
[tracing]
filter = "info,sync_phase=debug"
```

This diagnostic branch adds observations to the unchanged sync baseline. It does not alter request limits, verification, admission, or queue policy. Keep logging and system sampling identical on both sides of a comparison, and measure instrumentation overhead before interpreting throughput.

| Phase | Observation |
|---|---|
| `body_frame_seen` | A body frame reaches the per-peer service, before the decode permit. This is after the transport channel; it is not socket arrival. |
| `body_decoded` | Decoding completes. The record includes height, peer, decode duration, and total time since the service saw the frame. |
| `checkpoint_call` | The checkpoint verifier receives a block, before its per-block checks. |
| `checkpoint_queued` | Per-block checks have completed and the block is queued for its checkpoint range. |
| `checkpoint_released` | Range verification releases a successful block result. |
| `checkpoint_ready` | The waiting task observes that successful result. The difference from release includes scheduling delay. |
| `checkpoint_state_request` | Auth-data-root preparation is complete and the task is about to submit to state. |
| `state_queue_enter` | The state service accepts the block into its ordered queue. |
| `state_writer_send` | The state service attempts to send an ordered block to the writer. |
| `state_writer_received` | The writer takes a new or retryable block; VCT prerequisite checks follow. |
| `state_commit_begin` | VCT prerequisites are available and the finalized commit function is about to run. |
| `state_commit_end` | That function returns, with success recorded. It can return an error that causes retry. |

Use block height and phase order to pair events. Retries can produce multiple writer/commit observations for one height. Ordinary tracing timestamps align with the system sampler's wall clock; the existing JSONL process epoch provides the bridge to its relative timestamps. Logs are bounded by the disposable workload and disabled unless this target is enabled. `body_decoded` durations use a monotonic clock and are measured only when the target is enabled.

The state commit function includes its preparation, validation, header-state update and database write. Use its existing narrower timers and system CPU/I/O counters to separate those operations; do not describe its entire duration as disk time. Pair service frame observations with serving-node traces to distinguish transport/service queuing from remote preparation. The receiving node alone cannot establish the remote portion.
