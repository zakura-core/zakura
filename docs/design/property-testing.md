# Property testing

> **Status: first draft.** This document describes focused tests for the
> [design](peer-message-regulation.md) and [specification](../specs/peer-message-regulation.md).
> The [GetBlocks plan](property-testing-block-sync-infrastructure.md) identifies the initial paths.

## Scope and claim strength

Test observable invariants with existing Proptest, codec tests, synthetic peers, Tokio test time,
and real transport checks. A new declaration builder, custom scheduler, exhaustive model explorer,
and universal panic-recovery suite are not prerequisites.

Generated tests search for counterexamples. Passing samples do not prove all executions correct.
Deterministic and transport tests establish behavior only under their exercised conditions.

Defer exhaustive exploration until a measured gap justifies it. Any future explorer must state its
finite bounds, production correspondence, and whether it completed or stopped at a resource limit.

## Message checks

Every supported message kind needs deterministic legal boundaries and applicable invalid-input
cases. Random generation cannot decide whether a kind or rule gets tested.

Call production code at the boundary appropriate to the claim. Check:

- legal encodings fit their payload caps;
- decoding an encoding returns the original value;
- canonical payloads re-encode to identical bytes;
- invalid ranges and trailing bytes fail;
- variable-length fields obey allocation bounds before allocation;
- bounded arbitrary payloads return a result without panicking.

A payload cap is not an allocation bound. Check requested allocation and retained decoded state.
Keep expected bounds reviewable against the specification without requiring a new declaration API.

Adding a message requires updating its codec, handling, bounds, boundary cases, and applicable
protocol tests. Reuse exhaustive production dispatch where available. Do not require a new
reference-model arm for every wire variant.

## Stateful checks

Use short sequences that deliver messages, advance time, complete storage work, release output
capacity, reassign work, and close connections. Exercise production transitions. A test starting
from a decoded message makes no framing or allocation claim.

A small independent reference model is optional when it clarifies a race. It must not call the
production transition under test. Compare relevant observations after each action:

- validation result and reservation consumption;
- subscription identity and credit;
- worker ownership and actual operation completion;
- retained-result and queued-output bytes;
- handler completion, connection state, and local-failure cleanup.

No observation needs a response-byte charge, refund, or serving-rate refill event.

Separate conformant sequences from explicit protocol violations. Conformant sequences must not
produce a peer violation. Keep adversarial violations identifiable while shrinking. Save minimized
failures as ordinary deterministic tests or small fixtures. Reuse existing shrinking and storage.

## Required scenarios

| Area | Checks |
| --- | --- |
| Cadence | Initial sends, unchanged messages, legal updates, sender coalescing, and floods |
| Discovery requests | Periodic refresh, configured intervals, one outstanding request per type, and summary renewal |
| Buffered arrivals | Transport stalls and local read pauses followed by compliant bursts; no false peer violation |
| Discovery state | Known Hello completes initial progress; sequence/expiry policy; empty Services clears state; equal values renew validity |
| Reservations | Unsolicited, duplicate, mismatched, and reordered responses; exact count/byte bounds |
| Authorization lifetime | Reassignment, competing peers, finality, and local-interest changes preserve reservations |
| Subscriptions | Open/Grant/Close, credit exhaustion, bounded cursor history, crossed updates/outcomes, and idle subscriptions |
| Capacity ownership | Saturation, ordinary failures, blocked storage, connection churn, and actual operation completion |
| Control work | Repeated grants and empty responses yield shared execution; Close progresses while data output is blocked |
| Block terminals | Partial completion, missing heights, wrong counts, duplicate terminals, and unavailable ranges |

A cancelled waiter is not a finished operation. A blocking query keeps its permit after connection
closure until it finishes. A timer must not manufacture capacity by releasing a still-running job.

For cadence, test arrival batching explicitly. Sender intervals and refill margins do not alone
prove that an honest buffered burst fits the bucket. Validate timing and buffering assumptions
before enforcing exhaustion as a peer violation.

## Transport and load

Synthetic peers exercise framing and service dispatch. Real transport tests cover the QUIC boundary
that in-memory queues cannot establish.

Fill workers or output buffers. Verify that request reads stop, bounded receive buffers fill, and
existing stream credit eventually exhausts. Account for connection credit and authorized bytes.
Release capacity and verify that eligible processing resumes.

Run simultaneous requests in both directions on one connection. Verify that a paused reader cannot
trap a response or control message needed to finish active work. Check another peer and required
independent service streams too.

Load cases include maximum requests, continuous block serving, unavailable ranges, tiny responses,
non-reading peers, and connection churn. Assert aggregate execution, memory, buffered-byte, and
protocol-state bounds. Available capacity must not wait for a serving-rate refill timer.

State finite progress bounds and scheduling assumptions. Identify the runnable participant and the
event demonstrating progress. Do not require progress from a blocked dependency without releasing it.

## Execution and diagnostics

Keep diagnostics bounded. Optional detailed traces can expose the first divergence. Production
need not write every decision to a dedicated JSONL file.

Run deterministic regressions and a practical generated sample on pull requests. Broader generated
and load runs can execute on a schedule. Choose counts from measured runtime and report incomplete
coverage honestly.

Start with existing codec and reservation tests. Add GetBlocks ownership and bidirectional cases,
then subscription tests. Expand the harness only when a named property exposes a gap.
