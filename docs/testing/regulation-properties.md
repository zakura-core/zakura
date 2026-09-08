# Regulation property tests

These tests check who owns admitted work and when capacity returns. They exercise
production admission, response encoding, queueing, and write ownership against an
independent model. They do not establish complete conformance to the peer-message
regulation draft or whole-node overload protection.

## Running the tests

The ordinary unit-test lanes include these tests. The dedicated profile allows
longer local exploration without changing production limits:

```sh
cargo nextest run --locked --profile regulation-properties
```

For a focused network run:

```sh
cargo test --locked -p zakura-network --lib serving_regulation
cargo test --locked -p zakura-network --lib regulation_properties
cargo test --locked -p zakura-network --lib regulation::properties
```

Increase `PROPTEST_CASES` for additional generated histories. Failures print a
concrete JSON scenario and retain Proptest's normal seed-based reproduction.

## Test boundaries

| Layer | Production path exercised | Observation |
| --- | --- | --- |
| Primitive | Concurrency slots | Owned permit count |
| Shared requests | Finite-request admission, rollback, execution and response owners | Independent per-session and node ownership model |
| Serving | Admission, query lifecycle, encoder, transport queue | Peer and node slot counts, plus ownership by original session |
| Reactor | Peer routine and response queue | Response prefix, terminal, producer lifetime |
| Driver | Query claim, timeout, cancellation, result handoff | Underlying query and result retain capacity |
| State | Range response limits | Actual returned body bytes stay within the cap |

The model tracks ledger, query, and frame owners independently. It does not use
production counters or release helpers to calculate expected state. Production
observations are compared after every action, including failed admission.

Transport witnesses use the same `QueuedFrame::write_with` boundary as the real
writer. Dequeue alone must not release a producer. Controlled writes exercise
completion, failure, and cancellation. The base PR also tests a real QUIC write
blocked by stream credit while another stream makes progress.

A header-only QUIC test checks that an oversized GetBlocks request is rejected
before payload reading begins. It verifies the service's declared cap, tighter
stream caps, and valid GetBlocks and Block messages on the same stream. The
profile also checks the existing negotiated message-size limit before allocation.

A GetBlocks decoding property mixes structured requests with arbitrary short
payloads, flags, and message tags. An independent wire-rule check requires exact
length and valid fields, checks the decoded values, and catches both invalid
acceptance and valid rejection. Decoder panics fail and shrink the generated case.

## Shared request histories

The shared `RequestAdmission` layer is exercised with a test-only GetPeers policy
that calls the production discovery codec. Generated histories use two sessions,
one or two node slots, and at most two execution leases per request. An independent
model tracks provisional admission, response ownership, execution claims,
cancellation, and frame ownership after every action. Action availability comes
from the model. Failures include the concrete action history and the Proptest seed.

The shared model checks ownership with frame guards directly. A separate witness
encodes a real Peers response and retains ownership through the production queued
write boundary. It also checks partial-admission rollback and that a fair waiter's
permit cannot be reused for another peer's capacity pool. These tests do not
enable discovery regulation or claim its complete protocol conformance.

## Generated GetBlocks histories

The ownership model uses two peer identities, at most eight session generations,
four request slots, and a one-frame output queue per session.
Requests contain one committed mainnet block fixture or an empty terminal. The
independently calculated maximum response payload is 2,000,010 bytes; this is a
wire bound, not an outstanding-byte budget.

Actions cover provisional admission, commit, query claim and cloning, ledger
closure, frame queueing, pending writes, write completion/failure/cancellation,
reconnects, and time advancement. Only reference-model-enabled
actions are generated. A concrete replay rejects an inapplicable action instead
of silently skipping it. Cleanup uses the same actions to finish every owner.

Deterministic witnesses ensure these boundaries are reached independently of
random coverage:

- **Admission and rollback:** both peer and node producer limits block, then
  recover when ownership ends. Failed admission returns any earlier slot.
- **One execution:** cloned query leases cannot claim a second state read.
  Ledger closure prevents a queued read from starting.
- **Write backpressure:** a queued or writing response keeps its peer producer
  occupied after the ledger and query owners drop. Failed queue admission changes
  no ownership and can be retried after capacity becomes available.
- **Reconnects:** old query and frame owners remain counted under the old session.
  Replacing the session shares the same peer limit until those owners finish.
  Another peer can still start work while the replacement waits.
- **Response boundaries:** separate properties vary legal counts and response
  caps; real reactor histories cover empty, partial, and complete responses with
  queues of depth one through three.
- **Checker sensitivity:** deliberately missing write ownership, wrong-session
  attribution, and duplicate releases fail comparison. Shrinking removes
  irrelevant time advances while preserving a concrete failing observation.

The real reactor's existing timeout, shutdown, full-queue, stale-session, and
same-stream download tests complement the generated histories. The generator
is not claimed to cover every reactor terminal path.

## Production defaults

The default advertisement permits one block per response. One maximum-size body
plus its discriminator and terminal is 2,000,010 payload bytes. There is no fixed
request price, serving byte-rate allowance, or outstanding-byte balance.

| Boundary | Default witness | Recovery |
| --- | --- | --- |
| Session producer | One query/result/response shared by all its owners | Last owner finishes |
| Node producers | 64 sessions retaining responses | One producer finishes |

Time advancement never frees owned capacity. A separate witness completes 4,096
responses without advancing time, proving admission does not wait for a refill.
These tests exercise resource counts without allocating maximum-size block bodies;
real response encoding and write tests cover the framing boundary separately.

Generated service cases vary the advertised waiting limit, request burst, and
channel depth. They hold the node producer and fill the outbound queue, then
require a matched download response behind the waiting requests to reach the
reactor. Releasing capacity must start the oldest waiting request. Another
property sends one request beyond the waiting limit and checks that only the
block-sync stream closes, with no connection penalty or leaked producer.

The base PR also tests both peers serving large responses over real QUIC, beyond
the receive windows and application queues. Separate tests check queue admission
before encoding, encoding failure, cancellation, and delayed terminal delivery.
Waiting to serve does not extend download deadlines because reads continue.
A paused-reader transport witness checks sibling-stream credit and recovery;
cancellation during a blocked write must finish the current frame before closing.

## Replay and reuse

JSON version 4 records one active response per authenticated peer, including old
work that survives a reconnect. Earlier versions are rejected because they describe
different admission contracts. The committed reconnect scenario is a human-readable
example. Replay checks peer capacity and attributes each retained request to its
original session, so matching node totals cannot hide an ownership error.

For another message family:

1. Declare its actual rules and work unit; do not assume it needs byte accounting.
2. Identify admission and every owner that can outlive the handler, including
   state work, results, and transport writes where applicable.
3. Write an independent ownership model and concrete replay actions.
4. Add deterministic witnesses for each limit, release path, and legal retry.
5. Exercise the real handler, encoder, and transport boundaries against those
   expectations, including cancellation and session replacement.

For finite requests, reuse `RequestPolicy`, `RequestAdmission`, and the shared
execution/response ownership properties. Supply the actual codec and response
bound, then exercise the message's handler and transport integration separately.
Keep announcements, response reservations, and subscription semantics explicit;
they do not all have the lifetime of a finite request. GetBlocks' range, terminal,
state-driver, reconnect, and JSON replay properties remain independent coverage of
its integration.

These are finite generated histories, not exhaustive state exploration. They do
not measure RocksDB capacity, consensus scheduling, or full sync performance.
The local real-Iroh comparison and its limitations are documented in
[GetBlocks serving regulation](../design/getblocks-regulation.md).
