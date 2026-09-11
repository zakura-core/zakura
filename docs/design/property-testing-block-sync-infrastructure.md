# GetBlocks regulation tests

> **Status: first draft.** This plan covers block-sync version 2 under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise GetBlocks through its Block responses and one BlocksDone or RangeUnavailable terminal.
Use production framing, decode, reservations, serving, and cleanup. Add only the seams needed to
control capacity, storage completion, and response delivery.

This work introduces no Work bucket, refund, mandatory Delay verdict, or serving query-result timer.
It does not require a generic framework or exhaustive explorer. Header subscriptions have separate
tests. Version 2 retains height-range correlation and forbids overlapping live ranges per connection.

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
