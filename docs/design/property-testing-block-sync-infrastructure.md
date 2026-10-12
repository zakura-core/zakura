# GetBlocks regulation tests

> **Status: first draft.** This plan covers regulated block-sync version 3 and version 2 compatibility under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise GetBlocks through its Block responses and one BlocksDone or RangeUnavailable terminal.
Use production framing, decode, reservations, serving, and cleanup. Add only the seams needed to
control capacity, storage completion, and response delivery.

This work introduces no Work bucket, refund, mandatory Delay verdict, or serving query-result timer.
It does not require a generic framework or exhaustive explorer. Header subscriptions have separate
tests. Version 3 retains height-range correlation and forbids overlapping live ranges per connection.
Version 2 keeps the legacy behavior for peers that have not negotiated regulation.

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

On October 4, 2026, the ARM64 Mac debug build retained about 44.1 MB for
64,000 waiting GetBlocks commitments in one session. The maximum-session probe retained
22,567,691,400 allocator-requested bytes for 512 sessions. That implementation kept a watch
channel and a completion future for each range.

On October 5, production switched to one completion queue per session, with unique admission
identities and a weak completion lease per job. Publication and completion draining share a lock.
Records are reserved on admission, so completion does not allocate on an encoding worker.
Cancellation and a late producer drop cannot release a replacement admission. The GetBlocks
range index drains completed identities before testing overlap, without per-range futures.

The real `Serving::session` admission path now measures the following on the same host and build.
Workers do not run during these synchronous allocation probes. Each session retains 10,332 bytes
of fixed bookkeeping before admission; the table lists additional waiting-request bytes.

| Advertised requests per peer | Waiting commitments | Additional retained bytes |
| --- | --- | --- |
| 32 | 64 | 5,696 |
| 128 | 256 | 27,360 |
| 512 | 1,024 | 114,240 |
| 1,500 | 3,000 | 353,696 |
| 32,000 | 64,000 | 7,230,240 |

At the default limit this is about 113 bytes per request plus the fixed session cost, an 84%
reduction from the previous session measurement. Extrapolating the combined 7,240,572 bytes to
512 sessions gives 3,707,172,864 bytes, about 3.7 GB. This is bookkeeping, not a total node bound:
storage results, output and QUIC buffers, allocator overhead, requester state, and overlapping
session cleanup need separate allowance. The regression ceilings are 16 KiB per session plus
160 bytes per waiting request. They detect growth; they do not establish an acceptable budget.

The shared queue alone retains 2,622,688 bytes for 64,000 untracked requests and 3,671,328 bytes
with completion tracking. The complete GetBlocks range index and queue account for the table's
7,230,240 additional bytes. These are allocator-requested bytes. The earlier 66-byte candidate
container excluded lifecycle handling and has been replaced by these production-path probes.

The automatic `compact_session_lifecycle_peak` regression runs the production serving session
in an isolated test process. One allocation observation follows ownership across the reader and
encoding workers, without resetting between transitions. It queues twice the regulated serving
limit, completes all of them, leaves the session idle, reuses all the heights, then cancels and
queues a full replacement before the old dispatch cleans up. An allocator test separately verifies
that storage allocated on one thread and freed on another leaves the observation.

| Lifecycle checkpoint | 64,000 requests (October 5) | 5,632 requests (current default) |
| --- | --- | --- |
| Fixed session setup | 10,540 | 10,540 |
| All requests admitted | 7,240,780 | 684,300 |
| All endings sent, no further admission | 4,625,793 | 462,401 |
| Session dropped, old dispatch has not cleaned up | 2,639,617 | 246,529 |
| Full replacement queued before old cleanup | 9,879,833 | 930,265 |
| Both sessions and runtime dropped | 40 | 40 |

The measured lifecycle peak was 9,881,145 bytes at 64,000 requests and 931,577 bytes at the
current 5,632. Small runtime allocations are included here,
so fixed setup differs slightly from the synchronous admission probe. Allocator bookkeeping,
fragmentation and reserved virtual memory are excluded. This workload uses empty storage results
and an in-memory framed transport. It does not establish live-storage or QUIC buffer peaks.

The test asserts ceilings at each checkpoint: 16 KiB for fixed setup, 160 bytes per request for
admission, 96 bytes per request for idle retention, and 64 bytes per request for retiring jobs.
Execution has an additional 128 KiB worker allowance. Replacement overlap allows two fixed session
costs, worker overhead, and 224 bytes per request (160 for the replacement plus 64 for retiring
jobs). Final cleanup must retain at most 16 KiB. These ceilings detect regressions on the tested
lifecycle. They do not establish how many retiring sessions can overlap across a node.

Idle retention is intentional: the completion buffer and completed range index remain until the
next admission or session teardown. Admission drains before checking overlap, so an idle session
can reuse completed heights. This retention fits within the session's peak allowance and is not
added to that peak a second time. A node budget must separately allow for replacement cleanup,
requester state, storage results, transport buffering and allocator overhead.

The lifecycle test runs automatically with the network unit tests. The ignored maximum-session
probe still requires `ZAKURA_LARGE_LOAD_TEST=1` and a dedicated host because it retains several GB.
No advertised capacity default changed.

The advertisement covers requests outstanding until their endings arrive at the requester.
The server releases a commitment when it queues the ending, while output grants remain charged
through the transport write. Long round trips can therefore require a large advertisement even
with few active storage jobs. Size it from measured storage latency, RTT, response bytes, and
transport buffering, then check adversarial waiting entries at twice that advertisement against
the node budget. Execution concurrency alone does not determine this limit.

Requester reservations have a separate cost. The probe includes the authorization maps, writer
fences, and their retained copies of expected hashes, but excludes scheduler and decoded-block
state. Its input hash vector is allocated before measurement.

| Reservation entries | Expected hashes per entry | Retained bookkeeping bytes |
| --- | --- | --- |
| 16,384 | 1 | 6,632,988 |
| 16,384 | 128 | 73,217,564 |
| 32,768 | 1 | 13,268,828 |
| 32,768 | 128 | 146,437,980 |

Run the ignored `serving_capacity_candidates`, `waiting_commitment_allocation_breakdown`,
and `requester_reservation_allocations` tests with
`ZAKURA_CAPACITY_PROBE=1` and the `zakura-testkit` feature to reproduce this evidence. The candidate runs completed 31,804–42,944 unavailable
responses per second, 30,092–36,487 single-small-block responses per second, and 421–436 configured
128-block responses per second. The latter workload explicitly raises the default one-block
response limit. The probe checks the number of bodies served. These fixtures use an in-process
queue and synthetic storage, without QUIC backpressure or an injected network RTT.

Adding a simulated 500 ms storage delay to each single-block response reduced every candidate
to 36.2–36.3 responses per second, about 0.48 Mbps for these small blocks. The current per-peer
execution limit is 19, derived using the largest response rather than the actual response size.
Nineteen concurrent reads at that latency permit at most 38 responses per second. Increasing the
advertisement from 32 to 32,000 therefore did not improve this workload. This is a controlled
latency experiment, not a measurement of production storage latency.

### Serving bookkeeping budget

Version 3 peers are now advertised and held to a limit derived from a 512 MiB budget for queued
serving bookkeeping. Session reservations outlive queued jobs, so the inbound plus outbound
session slots bound every session that can hold this state, including replacements that are
still cleaning up. Each slot is charged 144 KiB plus 160 bytes for each of twice the limit. The
default 512 slots give 2,816 requests per peer and fill the budget exactly. Fewer slots keep the
configured 32,000, and more slots shrink the limit. Above 3,633 slots even one request per
session exceeds the budget, so the node warns at startup and advertises one. Version 2 peers and
local download sizing keep the configured value.

These measurements do not validate the 32,000 local default or the 2,816 serving limit. A single small-block response
in this fixture averages about 1,644 wire bytes. At that size, the design's twice-bandwidth-delay
allowance at 10 Gbps and 500 ms needs about 760,341 commitments, exceeding the protocol's 32,768
ceiling. The existing 32 MiB QUIC send window also limits transport to about 537 Mbps at that RTT,
before overhead. A lower request count alone cannot establish the target.

Keep activation in draft until representative production-storage and QUIC measurements justify
the capacity default and reconcile the throughput target with response batching and the transport
window. Use the production completion measurements when evaluating range batching above the download
floor and budgeted per-session advertisements. Any dynamic advertisement must retain capacity for
its highest outstanding promise. Lowering a Status value does not release old commitments.
Measure fixed costs and allocator variation before selecting a memory target, and include the
requester pool separately. Do not silently change those policies or size request counts and
execution slots using the maximum response alone when the default serves one block.

The stream-6 conformance adapter exercises the production layout over QUIC, including simultaneous
serving and exhausted credit. It uses the shared serving harness, so the production GetBlocks
peer-routine QUIC test remains a separate check. Existing synthetic tests without a range source
still exercise the old serving path and do not establish regulated-path coverage.

Version 3 selects the entire regulated contract through its own capability. A real-QUIC test uses
five old peers and one upgraded peer, downloads from an old peer without endings, forces an
overlapping timeout retry, serves actual bodies to every old peer, completes a v3 serving
exchange before the old supplier finishes, and checks prompt Status corrections and replacement. Separate tests preserve v2
replacement admission at its direction cap and its one-second range-correction allowance.
Unexpected header hashes retire locally and retain authorization until cleanup. A short supplier
cooldown survives reconnect without replacing the sequencer's retry episode. Buffered endings
survive local read pauses, while actively withheld endings still retire the connection.
The decoder and shared serving prerequisites do not activate these policies.
