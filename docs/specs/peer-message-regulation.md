# Zakura peer message regulation: specification

> **Status: first draft.** This specification defines the rules introduced by
> the [peer message regulation design](../design/peer-message-regulation.md). It covers the 14 native
> application messages in discovery, header sync, and block sync.

## Filter model

Each message has one role. The role selects the bound on the work caused by that message.

| Role | Work bound | Budget verdict |
| --- | --- | --- |
| Announcement | A cadence declared by the protocol | `Disconnect` when the cadence budget is empty |
| Request | The upper-bound cost of the response | `Delay` when the work budget is empty |
| Response | A one-shot, range, or subscription reservation created by the receiver | `Disconnect` when no matching reservation or credit exists |

Each message declaration selects concrete filters from four categories:

| Category | Filters | Purpose |
| --- | --- | --- |
| **Safe** | Frame, Decode, Verify | Bound parsing, allocation, and verification before they occur |
| **Authorized** | Reservation | Bind responses and subscription updates to receiver-created bounds |
| **Useful** | Relevant | Remove obsolete work without blaming the peer for a race |
| **Budgeted** | Cadence, Work | Bound message frequency or response work |

The admission path returns one of five results:

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot help now. |
| `Delay` | The request waits at the admission boundary for Work. |
| `Disconnect` | A conformant sender cannot produce the message. |
| `LocalFault` | The receiver accepted work and then failed to complete it. |

After `LocalFault`, the receiver MUST keep the connection open unless a caught panic leaves
connection state indeterminate. The receiver MUST NOT restore any reservation consumed by the
failed message. It MUST return the affected work to its scheduler. The implementation MUST classify
the failure by its cause without inferring sender intent.

The implementation MUST apply filters before the work that they bound. Applicable filters MUST run
in this order:

```text
frame
  -> cadence
  -> reservation precheck
  -> bounded decode
  -> reservation match
  -> stateless verification
  -> relevance
  -> work charge
  -> handler
```

The order MAY skip filters that the message declaration does not select. The Reservation filter MAY
split into a precheck and an exact match. The precheck MUST establish decode bounds before any
allocation. The exact match MUST run before Verify. A fixed-prefix read used by either step MUST NOT
allocate from a peer-declared value.

### Common requirements

1. Each message type MUST have exactly one declaration and one handler. Each handler MUST have a
   declaration. Each declaration MUST select exactly one role and one work bound. It MUST declare
   the payload cap and an allocation bound for each variable-length decoded field. An allocation
   bound MAY depend on a checked fixed prefix, a protocol limit, and a live reservation. The wire
   message, declaration, handler, and reference model MUST use one closed message-kind inventory.
   Their mappings MUST use exhaustive matches without wildcard arms.
2. Each filter MUST cap its per-peer state at a receiver-configured capacity. Peer-provided
   messages, keys, and counts MAY consume that capacity but MUST NOT increase it. The filter MUST
   define its behavior at capacity. The receiver MUST size each capacity so its aggregate across the
   maximum peer count fits the corresponding resource budget.
3. Every enforced inbound rule MUST have a matching outbound obligation. Local scheduling,
   finality, reorganization, and work reassignment MUST NOT produce a peer violation.
4. Every non-`Continue` result MUST record the service, message type, filter, direction, peer, key,
   result, and reason in `regulation.jsonl`. A full-trace mode SHOULD also record `Continue` results.
   The implementation MUST rotate the file and bound its total size.
5. Each peer MUST have an independent processing path. Admitted work from one peer MUST NOT starve
   another peer's path.
6. Each peer response path MUST bound queued unsent bytes. Reaching that bound MUST stop new
   response work for that peer without blocking another peer or service stream.

This specification bounds complete messages. Transport frame progress and stream layout remain out
of scope. The transport MUST enforce an independent deadline for an incomplete frame.

### Panic isolation

A panic in a decoder, verifier, handler, reactor port operation, or per-peer worker is a receiver
defect. The implementation MUST catch the panic at the affected peer's boundary, release reserved
resources, and return affected work to the scheduler. The process and other peers' processing paths
MUST continue. The implementation MUST report `LocalFault` and MAY close the affected connection.

## Safe filters

### Frame

Frame validates `(stream_kind, stream_version, message_type, flags, payload_len)` without reading
the payload. A payload cap excludes the frame header, stream framing, and transport encryption
overhead.

- The Frame filter MUST require `flags == 0`.
- The Frame filter MUST require `message_type` to appear in the allowlist for the stream kind and
  version.
- The Frame filter MUST reject `payload_len` above the applicable payload cap before allocating a
  payload buffer. Each message's absolute cap MUST equal the codec's maximum encoded payload size
  for that message and network.
- The frame header MUST carry the message discriminator. A payload copy MAY exist only when the
  codec verifies that both copies match.
- A Frame failure MUST return `Disconnect`.

### Decode

Decode converts a bounded payload into one canonical message.

- Decode MUST return a result without panicking for every bounded payload and consume every valid
  payload exactly.
- Decode MUST reject trailing bytes, unknown flags, reserved bits, non-canonical values, and values
  outside their declared ranges. It MUST NOT clamp invalid values.
- Decode MUST bound every allocation before it occurs. A collection allocation MUST NOT exceed the
  smallest of its declared count, protocol limit, and
  `remaining_bytes / minimum_item_size`.
- The decoder MUST expose requested-allocation and retained decoded-state bounds to tests. A payload
  cap MUST NOT serve as an allocation bound.
- A live reservation MUST supply request-selected response bounds.
- A Decode failure MUST return `Disconnect`.

### Verify

Verify performs checks that need no chain state or mutable service state.

- Verify MUST run before the handler without I/O, locks, or shared mutable state.
- Each data type MUST have one verifier and one ingress call site. A message declaration MUST name
  that verifier instead of repeating its checks.
- A Verify failure MUST return `Disconnect`.
- The handler MUST perform contextual checks and return `Disconnect` for a message-caused failure.
  It MUST return `LocalFault` or retry for a failure caused by local state or capacity.

## Authorized filter

### Reservation

A reservation is the requester's local authorization for an expected response. It keeps that
response admissible even when the scheduler no longer wants the work.

- The requester MUST create the reservation before sending the request. The reservation MUST
  identify and bound the authorized response.
- Each response MUST match and consume one live reservation or one unconsumed part of a bounded
  range reservation. A missing, duplicate, or mismatched reservation MUST return `Disconnect`.
- A local work deadline MAY reassign the work. It MUST NOT change or remove the reservation. A
  separate protocol deadline MAY close a connection that produces no terminal response. The
  reservation MUST remain live until the exchange completes or the connection ends.
- Reservation state MUST remain within the requester's inflight limit.

A subscription turns one request into a bounded response stream. The publisher can push follow-on
work without a new request for each response. The subscriber controls the stream with object and
byte credit. Each response spends that credit. An acknowledgement advances the accepted cursor. A
later grant renews the credit from that cursor. Closing the subscription stops future responses.
The subscriber still admits responses that it already authorized.

- The subscriber MUST add each credit grant to its local reservation before sending the update that
  carries the grant. Each response MUST consume its object and byte credit before the subscriber's
  handler starts.
- The subscriber MUST acknowledge only progress accepted by its handler. The publisher MUST retain
  bounded sent-response state. It MUST validate each update sequence and acknowledgement against
  that state before it applies added credit.
- A subscriber that stops wanting the work MUST stop granting credit. It MAY close the subscription.
  It MUST NOT revoke existing credit. After a close update, the publisher MUST stop producing new
  responses. The subscriber MUST keep its local reservation live through the terminal response.
- A protocol deadline MAY cover required initial progress or close completion. An idle subscription
  with no push obligation MUST NOT require a terminal response.

A matched response MUST reach its handler despite work reassignment, a competing response, or a
change in local interest. The receiver MAY stop issuing requests or granting credit for future work.

## Useful filter

### Relevant

Relevant decides whether a legal message can affect a receiver decision.

- The predicate MUST use the decoded message and a cheap, bounded snapshot without taking a lock.
- The predicate MAY test whether message content can change receiver state. A changed field alone
  MUST NOT establish relevance, and the predicate MUST NOT reconsider local interest in a matched
  response.
- A false predicate MUST return `Drop`. Relevant MUST NOT return `Delay` or `Disconnect`.
- The sender MUST provide enough information to distinguish a protocol violation from a local race.
- Cadence, Reservation, or a declared drop budget MUST bound a message before Relevant returns
  `Drop`.

## Budgeted filters

### Cadence

Cadence uses one monotonic token bucket per `(peer, message_type)`.

```text
Cadence {
  capacity: messages,
  refill: messages / second,
  on_empty: Disconnect,
}
```

- Capacity MUST absorb the allowed connect-time send, jitter, and message coalescing.
- The protocol MUST state a matching sender cadence no greater than half the receiver refill rate.
- Cadence exhaustion MAY return `Disconnect` only when its configuration prevents a conformant
  sender from exhausting it.

### Work

Work bounds how much response work one peer can start. Each `(peer, request_type)` has one token
bucket and one concurrency bound. The token bucket grants response bandwidth. The concurrency bound
caps active requests. Work reserves the worst-case charge before dispatch and later refunds unused
charge.

```text
charge = upper_bound_response_bytes + REQUEST_OVERHEAD
refund = upper_bound_response_bytes - actual_response_bytes
```

- `REQUEST_OVERHEAD` MUST equal 64 KiB. The bucket capacity MUST cover the largest accepted request.
  Its refill MUST state the granted per-peer response bandwidth.
- Work MUST run after every other selected filter. Immediately before the handler starts, it MUST
  acquire one concurrency slot and apply the computed charge once. The request MUST hold the slot
  until its terminal response is queued, `LocalFault` occurs, or the connection ends. A subscription
  update releases its slot when the handler atomically commits the update. Work MUST apply the
  computed refund after the terminal response is queued. The refund MUST return unused response
  capacity without returning `REQUEST_OVERHEAD`. Work MUST release the slot and refund unused
  response capacity after `LocalFault`.
- Work MUST return `Disconnect` when a request exceeds the concurrency bound. The sender MUST stay
  within the advertised concurrency and inflight limits. Token exhaustion alone MUST NOT create a
  peer violation.
- When Work is unavailable, the peer routine MUST return `Delay` and leave the request at the
  admission boundary. It MUST stop reading that peer's ordered stream until Work becomes available.
  The existing bounded application and QUIC queues MUST provide backpressure. The implementation
  MUST NOT add a delayed-request queue or scheduler.
- A delayed request MUST hold no shared lock, stream writer, or handler permit. All buffering MUST
  fit the per-peer resource bound. The delay MUST NOT block another peer or service stream. Later
  messages on the same ordered stream MAY wait. A refund or refill MUST wake the peer routine. The
  peer routine MUST NOT rerun preceding filters or charge the request again.

## Message declarations

- [Discovery — stream 4, version 2](native-p2p/discovery.md)
- [Header sync — stream 5, version 9](native-p2p/header-sync.md)
- [Block sync — stream 6, version 2](native-p2p/block-sync.md)

## Parameters to validate

The wire caps and reservation identities follow from the message encodings. The policy parameters
below remain candidate values while this specification has first-draft status:

| Parameters | Evidence required before implementation |
| --- | --- |
| Cadence capacities and refill rates | Honest-node traces with connect bursts and scheduling jitter, plus a flood test that reaches `Disconnect` within bounded work |
| `REQUEST_OVERHEAD`, Work capacities, and Work refill rates | CPU, lock, storage, and egress measurements at the maximum peer count; a liveness test for the largest legal request |
| Header credit, cursor-ring size, and `HS_PUSH_DEADLINE` | Model traces for open, grant, close, crossing updates, reorganization, and slow but conformant links |
| Incomplete-frame deadline and queued-response byte bound | Transport buffer accounting and partial-frame and non-reading-peer tests |

## Conformance tests

The implementation MUST provide these checks:

1. A declaration closure check MUST derive the current message kinds from the same closed inventory
   used by production dispatch. It MUST check that every kind has one declaration, one handler arm,
   one independent reference-model arm, legal minimum and maximum values, allocation bounds, and an
   explicit state effect. It MUST NOT use a fixed expected message count or a separately maintained
   message-kind list.
2. A cap test MUST check that every maximal legal encoding fits its declared cap and that no
   computed maximum exceeds that cap.
3. Property tests MUST generate legal messages and gate-event sequences whose sender preconditions
   hold. After every production transition, they MUST check reservation, budget, and state-size
   invariants. A conformant sequence MUST never return `Disconnect`. A deterministic coverage check
   MUST execute every current message kind, legal boundary value, and declared adversarial rule at
   least once. It MUST execute the cadence properties for every announcement, the Work and terminal
   refund properties for every request, and the reservation properties for every response. Random
   generation MUST NOT provide the only coverage for any kind, boundary, or rule.
4. Model-based reservation tests MUST generate subscription opens, grants, closes, exhausted credit,
   crossed and spontaneous terminal responses, work reassignment, finality changes, response
   reordering, duplicate responses, and connection closure. Each pushed response MUST consume the
   exact header and byte credit from one live subscription.
5. Fuzz tests MUST search arbitrary frames for decoder panics, inexact consumption, and allocation-
   bound violations. Property tests MUST check the corresponding claims over generated bounded
   payloads.
6. Honest-node regtest traces MUST contain no `Disconnect` result.
7. Load tests MUST drive one peer at the maximum conformant rate and show that CPU, memory, and
   filter state stay within their declared bounds. They MUST drive a non-conformant flood and show
   that the receiver reaches `Disconnect` within bounded work.
8. Panic-isolation tests MUST inject a panic in a decoder, in a handler, and in a reactor port
   operation. Each test MUST show that the process survives, that other peers' processing paths
   continue, that the affected work returns to the scheduler, and that the result reaches neither
   peer-set ban policy nor the peer-violation count.
9. Backpressure tests MUST exhaust one request type's Work bucket and show that its handler does not
   start. They MUST show that application and QUIC buffering stays within the declared bounds, that
   the peer routine resumes after a refund or refill, and that another peer and service stream make
   progress within the test's declared scheduling and progress bounds.
10. Bounded model exploration MUST visit every reachable state in the finite model declared by the
    [`GetBlocks` property-testing infrastructure](../design/property-testing-block-sync-infrastructure.md).
    It MUST check reservation, Work, slot, queue, isolation, cleanup, and bounded-progress invariants.
    If a resource limit stops exploration before its frontier is empty, the check MUST report an
    incomplete result instead of an exhaustive result.

Peer-slot selection, message priority, and stream layout are outside this specification. Peer-slot
selection must remain separate because a conformant peer can waste a slot without violating a
message rule.

## Reference implementations

Every link below pins commit
[`f892b9074002a04a678ef2365ec7658795796572`](https://github.com/zakura-core/zakura/tree/f892b9074002a04a678ef2365ec7658795796572)
on `main`.

[record-verify]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L286
[record-import]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3882
[record-bounds]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3917
[query-fields]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3939
[get-services]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3968
[services-validate]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3972
[summary-envelope]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3985
[services-peer-binding]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L1875
[service-id]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L4042
[hs-decode]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/header_sync/wire.rs#L675
[hs-payload-size]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/header_sync/wire.rs#L999
[hs-driver]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakurad/src/commands/start/zakura/header_sync_driver.rs#L635
[prepare-headers]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-header-chain/src/validation/prepare/pipeline.rs#L85
[context-free]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-header-chain/src/validation/context_free/mod.rs
[check-block]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/checkpoint.rs#L651
[block-check]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/block/check.rs
[bs-decode]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116
[bs-count]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230
[bs-block-len]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L253
[bs-expected-hash]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/peer_routine.rs#L1437
[bs-window]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/state.rs#L333
[bs-config]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/config.rs
