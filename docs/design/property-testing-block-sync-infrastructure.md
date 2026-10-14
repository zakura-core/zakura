# GetBlocks regulation tests

> **Status: first draft.** This plan covers regulated block-sync version 3 under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise GetBlocks through its Block responses and one BlocksDone or RangeUnavailable terminal.
Use production framing, decode, reservations, serving, and cleanup. Add only the seams needed to
control capacity, storage completion, and response delivery.

This work introduces no Work bucket, refund, mandatory Delay verdict, or serving query-result timer.
It does not require a generic framework or exhaustive explorer. Header subscriptions have separate
tests. Version 3 retains height-range correlation and forbids overlapping live ranges per connection.
Version 2 is retired, so every block-sync session is regulated.

## Existing infrastructure

| Capability | Existing path | Use |
| --- | --- | --- |
| Production in-memory peers | [SyntheticBlockSyncPeers](../../crates/zakura-network/src/zakura/testkit/block_sync_peer.rs) | Real stream-6 encoding, decoding, and peer routines |
| Multi-peer configuration | [Scenario and PeerSpec](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/scenario.rs) | Peer, timing, range, queue, and corpus helpers |
| Adversarial behavior | [Synthetic serve loop](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/peer.rs) | Withhold, reorder, stall, and disconnect |
| Reactor harness | [run_scenario](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/mod.rs) | Work queue, byte budget, peer routines, and sequencer |
| Trace-derived checks | [invariants.rs](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/invariants.rs) | Request, byte, and progress bounds |
| Monotonic test time | [TestClock](../../crates/zakura-network/src/zakura/testkit/clock.rs) | Existing clock-dependent checks |

Reuse workspace Proptest for legal values and short sequences. These helpers do not establish real
QUIC backpressure or control every runtime interleaving. Use real transport for flow-control claims.

## Frame and decode

Check the 9-byte GetBlocks payload cap, frame/payload discriminator agreement, exact consumption,
count of 1 through 128, and checked end-height arithmetic. The decoded request has no variable-length
collection. Peer-controlled values must not cause decode allocation.

Generate legal start/count values and check production encode/decode round trips. Include explicit
minimum/maximum counts, maximum legal end height, overflow, zero count, truncation, trailing bytes,
and mismatched discriminators. Generated cases supplement deterministic boundaries.

Check response count and body-byte limits independently. A storage result or decoded representation
may consume more memory than its encoded bodies.

## Reservations and terminals

Create authorization before sending GetBlocks. Exercise:

1. A legal range with ascending blocks and matching BlocksDone.
2. Partial completion whose returned count equals consumed blocks, followed by requeueing missing heights.
3. RangeUnavailable with exact start/count before any block.
4. Unsolicited blocks, wrong hashes, duplicates, wrong terminal counts, and duplicate terminals.
5. Overlapping live ranges on one connection.
6. Local reassignment, finality change, or a competing response before the original response arrives.
7. Connection closure with unconsumed authorization.

Local scheduling must not remove authorization. Terminals consume a range once. Known headers do
not validate arbitrary bodies; retain body-commitment and downstream consensus checks.

Use production transitions. A small expected-state model is optional when it clarifies a race.
Do not build a generic model merely to mirror every handler.

## Capacity and ownership

Configure more legal inflight commitments than workers. Verify that executing operations stay
within the worker limit and retained commitments remain bounded.

Saturate output and node-wide workers independently. New response work must wait for required
resources. Releasing capacity must resume processing without a serving-rate timer.

Hold a database operation open with a controlled dependency. Disconnect its peer and drop its
waiter. Verify that the operation keeps its permit until completion. Reconnect repeatedly while
the operation remains blocked. Connection churn must not multiply running operations beyond the
node-wide bound.

Exercise ordinary storage and encoding failures. Finished resources release once, retries return
to scheduling, and local failures do not count as peer violations. Universal panic recovery is not
required for these cleanup checks.

Bound retained results before encoding and queueing. Cover large and small blocks. A bounded
sequential producer may perform several reads; assert memory and execution bounds rather than one
database call per range.

## Bidirectional backpressure

Run endpoints that request and serve concurrently. Saturate output and admission on both sides.
Demonstrate progress without a timeout breaking a cycle between request intake and response receipt.

A paused reader must not trap a response or terminal needed to finish active work. Check another
peer and required service/control streams on the connection. State finite bounds and scheduling
assumptions.

Repeat the relevant case through real QUIC. Stop application request reads and verify bounded
receive buffering, exhaustion of existing stream credit, and required connection-credit headroom.
Resume after capacity returns. In-memory queues alone cannot establish these properties.

## Load and reporting

Run continuous useful requests, repeated unavailable ranges, maximum ranges, tiny responses,
non-reading requesters, and connection churn. Empty responses can cause lookup work without filling
output buffers. Assert bounded execution and opportunities for other runnable peers to progress.

Report peer limits, worker limits, memory/output bounds, and the exercised transport arrangement.
Assert aggregate bounds. Keep diagnostics bounded and save minimal regressions in existing tests
or small fixtures.

Run focused regressions on pull requests and broader generated/load cases on a schedule.
Do not claim exhaustive coverage. A future explorer must state model bounds, production
correspondence, and completion status.

## Activation measurements

The serving limit advertised to version 3 peers comes from a 512 MiB budget for queued serving
bookkeeping. `serving_max_inflight_requests` in `block_sync/regulated/session.rs` documents the
derivation; the default 512 session slots give 2,816 requests per peer. Local download sizing keeps
the configured 32,000.

These tests run automatically:

- `compact_session_lifecycle_peak` follows one session's allocations across reader and encoding
  workers in an isolated process: admission at twice the limit, completion, idle retention,
  cancellation and an overlapping replacement. Its asserted ceilings are the regression budget.
- `concurrent_sessions_share_bounded_capacity_without_refill_waits` runs 16 sessions with useful
  and empty responses on a paused clock and checks the per-peer and node execution limits,
  progress for every session, capacity release and bounded allocations.
- `regulated_serving_stops_at_the_output_bound_and_resumes_on_read` and
  `regulated_status_bursts_keep_the_connection_serving` exercise serving backpressure and Status
  cadence over native QUIC.

| Lifecycle checkpoint at 5,632 requests | Allocator-requested bytes |
| --- | --- |
| Fixed session setup | 10,540 |
| All requests admitted | 684,300 |
| All endings sent, no further admission | 462,401 |
| Session dropped before old dispatch cleanup | 246,529 |
| Full replacement queued before old cleanup | 930,265 |
| Both sessions and runtime dropped | 40 |

These figures use empty storage results and an in-memory framed transport. They exclude allocator
metadata, storage results, output and QUIC buffers, and requester state, which have separate
bounds. Idle retention is intentional: completed ranges stay until the next admission or session
teardown, within the session's peak allowance.

Manual probes reproduce the host evidence. They are ignored and gated by environment variables:

- `maximum_sessions_continuous_serving_probe` (`ZAKURA_MAX_PEER_PROBE=1`) serves every default
  session slot in process and reports throughput, CPU, per-session fairness and resident memory.
  `ZAKURA_PROBE_SESSIONS`, `ZAKURA_PROBE_WINDOW`, `ZAKURA_PROBE_BLOCKS`,
  `ZAKURA_PROBE_READ_DELAY_MS` and `ZAKURA_PROBE_SHAPE` (`useful`, `unavailable` or `stalled`)
  select the workload. `stalled` with a window of 5,632 measures queued bookkeeping at twice the
  limit across all sessions.
- `regulated_getblocks_throughput_probe` (`ZAKURA_RTT_PROBE=1`) pipelines GetBlocks over native
  QUIC for small and large bodies at several in-flight depths. Add delay with netem and pass the
  RTT in `ZAKURA_PROBE_RTT_MS`.
- `getblocks_storage_read_probe` in `zakura-state` (`ZAKURA_STORAGE_PROBE_CACHE_DIR`) times the
  production block-range read against a synced database at concurrency from 1 to 76.
  `ZAKURA_STORAGE_PROBE_ENCODE=1` adds the serving encode on a blocking thread after each read.
  `ZAKURA_PROBE_DROP_CACHES=1` starts each case with a cold page cache.

Run them with the `zakura-testkit` feature and a release build for representative figures.

The node runs at most 24 read-and-encode jobs, 12 for one peer (`NODE_EXECUTION` in
`block_sync/regulated/session.rs`). The storage probe chose these on a 16-vCPU host against
cloned, pruned PR-node state (Mainnet height 3,514,052 and Testnet height 4,473,088), with one
block per response, warm cache, and the encode enabled:

| Jobs | Mainnet reads/s | Mainnet p99 | Cores | Testnet reads/s |
| --- | --- | --- | --- | --- |
| 16 | 23,400 | 2.9 ms | 8.1 | 78,900 |
| 24 | 28,600 | 3.8 ms | 10.3 | 76,300 |
| 32 | 30,400 | 4.9 ms | 11.4 | 72,100 |
| 38 | 30,200 | 5.9 ms | 11.5 | 71,300 |
| 76 | 29,000 | 11.1 ms | 11.6 | 70,600 |

At 24 jobs Mainnet reaches 94% of its peak and Testnet 97%. More jobs add latency and CPU that
validation needs. Encoding costs about a quarter of read-only throughput (Mainnet reads alone peak
near 38,000/s). 16-block responses level off between 24 and 48 jobs, and unavailable heights
saturate by 4. The limits do not depend on the response configuration.

The advertisement covers requests outstanding until their endings reach the requester. Size it
from storage latency, RTT, response bytes and transport buffering, and check twice that
advertisement against the node budget. Execution concurrency alone does not determine it. With the
default of one block per response, per-connection throughput for small bodies is capped near
`limit × body bytes / RTT`; at 500 ms the 2,816 limit gives about 7 MB/s for 1.6 KB bodies, below
the QUIC window. Raising it needs a larger bookkeeping budget or response batching. Any dynamic
advertisement must keep capacity for its highest outstanding promise, because lowering a Status
value does not release old commitments.

The stream-6 conformance adapter covers the production layout over QUIC, including simultaneous
serving and exhausted credit. Requester tests cover
local incompatibility, supplier cooldowns across reconnects, and endings that survive local read
pauses.
