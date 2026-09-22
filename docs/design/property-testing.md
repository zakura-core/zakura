# Message regulation and property testing

> **Status: implemented for block sync, legacy gossip, header-sync request IDs, and discovery.**
> This document describes how Zakura regulates peer messages by role, and how one test suite per
> piece covers every message that uses it. It implements the
> [design](peer-message-regulation.md) and [specification](../specs/peer-message-regulation.md).
> The [GetBlocks plan](property-testing-block-sync-infrastructure.md) covers the GetBlocks paths.

## The problem

Every peer message needs bounds before allocation, a check that a response was requested, bounded
work while serving a request, and tests for each of those properties. Written per message, this is
a lot of code. The GetBlocks stack (#945 and #973–#981) spends about 12,000 lines on one message
family. About 2,400 production lines and 6,500 test lines of that rebuild request identity and
serving ownership for GetBlocks alone. Almost none of that code transfers to the next message.

The cost comes from organizing the work around messages. Most of the rules are the same for every
message with the same role:

- every announcement needs frame, allocation, and cadence bounds;
- every request needs bounded execution and output;
- every response needs a reservation that the receiver created.

Zakura therefore writes each rule once per role. A message joins by declaring data and supplying a
codec, not by adding machinery. The target is 15 to 150 lines per message, tests included.

## Roles and the check pipeline

The specification gives each message one of three roles and each check one of four results.

| Role | What the receiver checks |
| --- | --- |
| Announcement | Frame and allocation bounds; cadence where declared |
| Request | Frame and allocation bounds; bounded execution and output while serving |
| Response | Frame and allocation bounds; a live reservation created by the receiver's request |

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot change accepted state. |
| `Disconnect` | The sender violated a protocol obligation. |
| `LocalFault` | The receiver failed to complete accepted work; the peer is not at fault. |

Checks run before the work that they bound:

```text
frame -> cadence -> reservation precheck -> bounded decode -> reservation match -> verify -> handler
```

Each stage maps to one shared piece:

| Stage | Piece |
| --- | --- |
| frame | [`MessageRule` table](#1-message-rule-table) checked by the transport |
| bounded decode | [`WireMessage` codec](#2-bounded-wire-codec) |
| reservation precheck and match | [`Reservations`](#4-reservations) |
| serving a request | [`Serve` loop](#3-serving-loop) |
| result | [`Verdict`](#5-verdict) |

## The five pieces

Each piece has at least two production users. Each piece has one test suite that runs against
every user.

### 1. Message rule table

A service returns one static table per stream from `Service::message_rules`:

```rust
pub const BLOCK_SYNC_MESSAGE_RULES: [MessageRule; 5] = [
    MessageRule::announcement(MSG_BS_STATUS as u16, PayloadLen::exact(STATUS_PAYLOAD_BYTES)),
    MessageRule::request(MSG_BS_GET_BLOCKS as u16, PayloadLen::exact(RANGE_PAYLOAD_BYTES)),
    MessageRule::response(MSG_BS_BLOCK as u16, PayloadLen::between(MIN_BLOCK, MAX_BLOCK)),
    // ...
];
```

The transport reads the 8-byte frame header, then checks the message type, the flags, and the
payload length against the table. It rejects a failure before it allocates or reads the payload.
A request-stream reader admits only request rows; a response reader admits only response rows. A
rule's maximum can only tighten the stream's frame cap.

A service that returns no table keeps the old behavior: any type, any flags, and the stream cap.

**Invariant:** no payload byte is allocated for a frame whose type, flags, or length the table
rejects.

**Users:** block sync (stream 6) and both legacy gossip streams (2 and 3).

### 2. Bounded wire codec

A message family implements `WireMessage` once:

```rust
impl WireMessage for LegacyRequestFrame {
    type Error = LegacyGossipError;
    const RULES: &'static [MessageRule] = LEGACY_REQUEST_MESSAGE_RULES.split_at(7).0;

    fn message_type(&self) -> u16 { /* one match */ }
    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), Self::Error> { /* one match */ }
    fn decode_payload(kind: u16, reader: &mut BoundedReader<'_>) -> Result<Self, Self::Error> {
        Ok(match kind {
            MSG_REQUEST_BLOCKS_BY_HASH => Self::BlocksByHash(INVENTORY_HASHES.decode(reader)?),
            // ...
        })
    }
}
```

`RULES` is the same table the service returns to the transport, so the header check and the
decoder read one source of truth. The shared `encode_frame` and `decode_frame` repeat the header
checks, because tests and in-process channels hand frames to the codec directly.

Fields use `Wire` items with static length bounds, such as `HeightLe`, `LeU32`, `HashItem`, and
the `Zcash<T, MIN, MAX>` adapter. Lists use `BoundedVec` descriptors:

```rust
const INVENTORY_HASHES: BoundedVec<HashItem> =
    BoundedVec::new("inventory hashes", 0, MAX_INVENTORY_ITEMS);
```

A descriptor is a constant, not a container, so messages keep plain `Vec` fields. Decoding reads
the count, checks it against the descriptor and against the bytes that remain, and only then
allocates. Rule rows derive their bounds from the same descriptors, so a table row cannot drift
from its codec.

**Invariant:** a list allocation never exceeds `min(count, protocol limit, remaining / min_item)`
items.

**Users:** block sync, legacy gossip announcements, and legacy requests.

### 3. Serving loop

A service implements `Serve` for each request kind: a response bound and one `produce` step.

```rust
impl Serve for GetPeersServe {
    type Request = GetPeersRequest;
    fn response_cap(&self, _: &GetPeersRequest) -> u32 { MAX_DISCOVERY_RESPONSE_FRAME }
    async fn produce(&self, request: GetPeersRequest, lease: WorkLease, sink: ResponseSink)
        -> Result<Responded, ServeEnd>
    {
        let records = self.handle.sample_peers(/* ... */).await;
        sink.respond(discovery_frame(DiscoveryMessage::Peers { records })?)
    }
}
```

`ServeSession::serve` owns everything else, in this order:

1. wait for a per-peer execution slot;
2. wait for a per-peer output grant of `response_cap` bytes;
3. wait for a node execution slot;
4. spawn the work, which runs `produce` to completion;
5. release both execution slots, then queue the response while holding only the output grant.

Every wait races the stream's cancellation. The transport releases the output grant after it
writes the frame. `ResponseSink::respond` is the only way to build `Responded`, so a successful
`produce` sends exactly one response.

**Invariants:**

- A peer that stops reading stops at its output bound and holds no node slot.
- Cancellation never releases capacity that running work still uses. It marks the `WorkLease`
  cancelled; work that moved a lease clone into a blocking task keeps its slots until that task
  ends.
- A reconnect shares the peer's live budgets instead of getting new ones.

**Users:** discovery `GetPeers` and `GetServices`.

### 4. Reservations

`Reservations<K, C>` is the only way a response is admitted:

```rust
reservations.reserve(request_id, max_payload_bytes, credit)?; // before sending
// ...
let credit = reservations.claim(&request_id, frame.payload.len())?; // before decoding
```

The claim returns the credit that the requester stored, such as a decode context or a record
limit. An unsolicited, duplicate, or oversized response fails to claim and disconnects the peer.

There is no expiry. A local timeout or a retired request leaves the reservation in place until the
peer answers or the session ends, because an honest peer may still answer. The session owns the
map and drops it on close. A map holds at most `cap` entries; a full map is a local condition.

**Invariant:** every admitted response matches exactly one earlier local request, and no local
timer turns an honest late answer into a violation.

**Users:** header-sync request IDs (at most four live per session) and discovery `Peers` and
`Services` (one of each).

### 5. Verdict

`Verdict` names the four results and maps them onto a stream sink's result: `Continue` and `Drop`
keep the stream open, and a drop is counted; `Disconnect` is a protocol reject; `LocalFault` is a
local reject. A refused claim converts to `Disconnect`.

**Users:** the header-sync pipe, the discovery sink, and the serving task.

## How the pieces meet the specification

| Specification requirement | Where it holds |
| --- | --- |
| Frame failures disconnect before allocation | `FrameFilter::check_header` in the transport reader |
| Decode allocation is bounded by count, protocol limit, and remaining bytes | `BoundedReader::check_count` inside `BoundedVec::decode` |
| Responses need a receiver-created reservation | `Reservations::claim` before decode |
| Local timeouts do not create violations | `Reservations` has no expiry API |
| Execution and output capacity bound serving | `ServeSession::serve` acquisition order |
| Local capacity exhaustion is not a violation | Full reservation maps and serving waits are local |
| Cancelled work keeps capacity until it ends | `WorkLease` clones in `produce` |
| One response per request | `ResponseSink::respond` consumes the sink |

## Test suites

Each piece has one suite. A message joins a suite by supplying data or a small adapter.

| Suite | Checks | Per-message input |
| --- | --- | --- |
| Frame suite (`transport/message_rule/tests.rs`) | Every native table: min and max accepted, one past each bound rejected, each of the 16 flag bits, cross-role rejection, every undeclared type, and `None` tables | One line per new stream |
| Codec conformance (`wire_codec/conformance.rs`) | Closed coverage, tight bounds, round trips, truncations, trailing bytes, flag bits, one past the maximum, undeclared types, family violations, and metered allocation; plus property tests for round trips and total, idempotent decoding | A `WireSample` implementation |
| Reservation suite (`regulation/reservations/tests.rs`) | Unsolicited, duplicate, and oversized claims; no expiry after 24 hours; capacity refusal and recovery; session close; a property test against a ghost key set | None |
| Serving ownership kit (`regulation/serve/tests/kit.rs`) | Cancellation keeps execution until the work ends; reconnects share capacity; blocked output stops at its bound with no node slot; failure releases everything; non-reading peers do not starve an honest peer; one response per request | A `ServingUnderTest` adapter |

Two rules keep the suites honest.

**Tests must not validate themselves.** A table row derived from codec constants cannot be checked
against those same constants. The conformance suite instead encodes real samples and requires the
shortest and longest sample of each bounded row to equal the row's bounds exactly. A test family
with a loose row proves that the suite catches the mismatch.

**Allocation is measured, not assumed.** A test-only counting allocator meters heap bytes on the
decoding thread. The suite requires each decode to stay within the family's declared bound plus a
small slack for error values.

Per-message scripts cover what the suites cannot: the header-sync pipe routes a late response to
the reactor and disconnects a duplicate, and the discovery scripts run over real connections.

## Adding a message

1. Add a rule row. Derive its bounds from the codec's items and descriptors.
2. Add encode and decode arms to the family's `WireMessage` match. Use `BoundedVec` for lists.
3. Add samples that reach the row's bounds, and any family-specific violations, to `WireSample`.
4. For a request, implement `Serve` and add a `ServingUnderTest` adapter.
5. For a response, reserve before sending and claim before decoding.
6. Keep the handler a plain `match` arm.

The migrations in this change measured as follows (`git diff --numstat`):

| Change | Lines changed | Test adapter lines |
| --- | --- | --- |
| Block sync codec (5 messages) | +211 / −223 | +175 |
| Legacy gossip codecs (2 families, 9 messages) | +234 / −372 | +245 for both families |
| Header-sync request IDs onto reservations (code and pipe tests) | +212 / −330 | none |
| Discovery responses onto reservations (code and test updates) | +151 / −18 | none |
| Discovery `GetPeers` and `GetServices` onto `Serve` | +253 / −89 | +129 for both kinds |

The first rows exclude the removed GetBlocks prototype (415 lines). Each rule table added 18 to 95
lines in the first commit.

The shared pieces cost about 1,400 production lines and 1,300 test lines, once. The GetBlocks port
from #945 would add a `Serve` implementation of about 20 lines and a kit adapter.

## Guardrails

- **Two users per abstraction.** A piece exists only when two production paths use it.
- **No self-validating tests.** Derived constants are checked against encoded samples.
- **Plain handlers.** Tables and codecs are data and traits; there is no dispatch framework.
- **No second reference model.** Stateful tests compare counters and delivered frames.
- **One requirement-to-test map.** The table above names where each requirement holds; the suite
  table names where it is tested.

## Known gaps and follow-ups

- **Header sync and discovery tables.** Header sync v8 has a stateful, context-dependent codec, and
  discovery uses one frame type for every message. Both still rely on their codecs for type and
  length checks.
- **Response rate-bucket exemption.** The table records each row's role, but responses still spend
  the connection's message bucket. Block sync silently drops some unmatched responses after a full
  decode, so exempting responses first needs a pre-decode reservation precheck there.
- **Status clamps.** `BlockSyncStatus` decoding clamps out-of-range limits instead of rejecting
  them. The specification does not yet say which is required.
- **Rows above the stream cap.** Legacy rows 2, 4, and 13 declare maxima above the 1 MiB stream
  cap. The frame suite pins them so the gap stays visible.
- **Block-sync serving and ranges.** GetBlocks serving and outstanding ranges move onto `Serve` and
  `Reservations` after #945 lands, to avoid conflicting with it.
- **Serving property test.** The kit covers six fixed cases; an operation-sequence property test
  over its counters is not written yet.

## Scope and claim strength

Test observable invariants with existing Proptest, codec tests, synthetic peers, Tokio test time,
and real transport checks. A declaration builder, custom scheduler, exhaustive model explorer, and
universal panic-recovery suite are not prerequisites.

Generated tests search for counterexamples. Passing samples do not prove all executions correct.
Deterministic and transport tests establish behavior only under their exercised conditions.

Defer exhaustive exploration until a measured gap justifies it. Any future explorer must state its
finite bounds, production correspondence, and whether it completed or stopped at a resource limit.

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

Keep diagnostics bounded. The transport counts header rejections in
`zakura.p2p.ratelimit.frame.rejected` by reason and traces them as `frame.rejected`. Serving waits
count in `zakura.p2p.serve.delayed` by bound, and drops count in `zakura.p2p.message.dropped` by
reason. Optional detailed traces can expose the first divergence. Production need not write every
decision to a dedicated JSONL file.

Run deterministic regressions and a practical generated sample on pull requests. Keep suite tests
out of the cluster and block-sync fuzz modules, which the pull-request profile skips. Broader
generated and load runs can execute on a schedule. Choose counts from measured runtime and report
incomplete coverage honestly.
