# Sync phase diagnostics

The ordinary release feature set compiles out debug events. Build the diagnostic binary explicitly without that static cap:

```sh
cargo build --release --locked -p zakura --no-default-features --features prometheus,commit-metrics,progress-bar,sentry,opentelemetry,release_max_level_debug,zakura-network/internal-bench
```

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

Use block height and phase order to pair events. Retries can produce multiple writer/commit observations for one height. Ordinary tracing timestamps align with the system sampler's wall clock. JSONL timestamps are relative to their emitter's monotonic origin; `process_trace_id` is only a correlation label, not an exact wall-clock origin. Compare durations within each clock domain, or calibrate a bridge using matched events before combining text and JSONL timestamps. Logs are bounded by the disposable workload and disabled unless this target is enabled. `body_decoded` durations use a monotonic clock and are measured only when the target is enabled.

The state commit function includes its preparation, validation, header-state update and database write. Use its existing narrower timers and system CPU/I/O counters to separate those operations; do not describe its entire duration as disk time. Pair service frame observations with serving-node traces to distinguish transport/service queuing from remote preparation. The receiving node alone cannot establish the remote portion.

## Controlled header serving

The `zakura-network/internal-bench` feature also includes a private serving hook. On the isolated feeder only, set `SYNC_BENCH_HEADER_SCHEDULE=2:2500` to release the first two page completions without a delay and hold each subsequent page completion for 2,500 ms. The hook runs after the state read and before delivery to the header reactor. It neither changes block serving nor holds a database transaction during its timer. It logs page-ready and page-release observations under `sync_fixture`.

Restart the feeder before each replay: the page counter is process-wide. Connect exactly one test client, keep the feeder's state and selected tip fixed, and validate the observed request/page sequence. Do not set the variable during public-network preparation or on the client. `0:0` provides the same serving instrumentation without delay. The hook is absent from ordinary builds, and does nothing when its variable is absent. This fixture is for diagnosing controlled schedules; its artificial latency must not be described as public-network throughput.
