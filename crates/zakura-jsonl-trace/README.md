# Zakura tracing utilities

The existing JSONL tracing API is unchanged. The `block_profile` module adds optional, bounded block timelines for a local collector.

## Enable block recording

Set the dedicated node's configuration explicitly. Omitting `socket` leaves recording disabled.

```toml
[block_profile]
socket = "/run/zakura-profile/node.sock"
node = "mainnet-profile-01"
session = "daily-canary"
```

Only one recorder can be installed per process. Use one node/network per profiling process. The node logs an initialization failure and continues without a new recorder. A missing collector never blocks verification. Keep the socket in a private runtime directory writable by the collector and accessible to the node.

The root measures router entry through the caller's result. It excludes caller readiness, the outer router buffer, ingress, and networking. A successful semantic request may be followed by additional writer/finalization work, which retains the same attempt context and can end after the root. Checkpoint requests initially have root timing only. The protocol and storage formats of the chain do not change.

## Context and bounds

`Context::wrap` enters a context only while polling an async future. `Context::in_scope` carries an explicitly captured context into synchronous work. Never hold the synchronous `Entered` guard across an await. Dropping a span emits a complete elapsed interval. Shared crypto-batch work has no exclusive block owner and must not be attributed by timestamp alone.

An attempt owns no block payload or consensus state. At most 1,024 attempt contexts, 131,072 detail events, and 2,048 summary events are retained by the recorder. Each attempt permits 65,536 spans, reserving 128 for state phases after transaction/worker detail. The fixed event queues use less than 64 MiB. Queue writes are nonblocking. Only the exporter thread serializes and sends datagrams, each at most 8,192 bytes. It sends repeated run metadata and health counters to recover collector restarts.

If the socket temporarily fills or a send is interrupted, the exporter retains the serialized datagram and retries every 2 ms for up to one second. Its transport sequence stays unchanged. Only this background thread waits, and the existing bounded queues absorb new events. Shutdown interrupts retries. Permanent send errors, an exhausted retry deadline, or sustained queue pressure still cause counted loss. Successful retries do not increment transport-drop counters.

Transaction contexts carry their zero-based position in the block. Descendant spans inherit that index across async polls and worker handoffs, and their parent IDs identify the owning transaction span. Transaction envelope spans also record the already-computed mined transaction ID as `transaction_hash`, in internal byte order. Child spans do not repeat the hash. Both transaction fields are optional for compatibility with older recordings. The collector must be upgraded before the node to preserve these fields and accept span IDs above the previous limit of 256. The larger budget preserves ordinary block detail while retaining bounded loss under extreme fanout or collection pressure.

## Wire format

Every JSON datagram has `schema: 1` and a `type` of `run`, `event`, or `health`. Events also carry `run_id` and an increasing transport sequence. Within a run, attempts have distinct IDs even when block hashes are equal.

- `start` records identity and the monotonic start offset.
- `span` records its ID, parent, stage, start/end offsets, and thread at completion. That thread is not proof of CPU ownership throughout an async interval.
- `finish` contains its own identity, start, end, outcome, and drop count. It remains useful without a received `start` event.
- `seal` is emitted when the last context owner finishes. It records expected spans and final drops, including work that outlives the caller.

Completion and seal records have a queue separate from span detail. That queue is also bounded and can lose records. Health counters and sequence gaps expose loss. The exporter gives summaries priority, so collectors must handle a finish or seal arriving before detail. A root dropped without a result records `abandoned`. This does not imply that already-submitted state work was cancelled.

Intervals use monotonic microseconds since the run epoch. UTC is a display anchor. Linux also includes a `CLOCK_MONOTONIC` anchor and acquisition uncertainty for timestamped perf samples. Hash arrays are in internal byte order. Reverse their bytes for the conventional block explorer display. No transaction payloads, peer addresses, credentials, or arbitrary configuration are exported.

## Validation

```sh
cargo test --locked -p zakura-jsonl-trace
cargo run --locked -p zakura-jsonl-trace --example block_profile -- /path/to/collector.sock
```

The example labels all records synthetic. Tests cover scope restoration during polling and unwinding, independent retries, cancellation, bounded active workers, queue overflow, and reservation of writer-stage capacity. Real performance overhead still requires a Linux canary on the intended node workload.
