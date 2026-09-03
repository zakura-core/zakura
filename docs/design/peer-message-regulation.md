# Native P2P message regulation: design

> **Status: proposal.** The
> [message-regulation specification](../specs/peer-message-regulation.md)
> defines the required behavior. This document explains why the controls are
> separate and how they compose. The
> [property-testing design](property-testing.md) defines how that behavior is
> tested.

Message regulation is traffic control for Zakura's native P2P services. It
decides how much peer-caused work may enter the node, how long that work owns
capacity, and what happens at a limit. Property testing is one way to verify
those decisions; it is not the regulation itself.

## Problem

Per-message validation prevents malformed input, but valid messages can still
consume too much CPU, memory, disk, state work, or outbound bandwidth. A
per-peer limit alone is not sufficient because one operator can create many
peer identities. A node-wide limit alone allows one peer to consume the whole
node.

Every regulated exchange therefore needs:

- a message-specific frame cap enforced before payload allocation;
- peer and node admission limits;
- an upper-bound charge computed from local policy;
- ownership that survives asynchronous handoffs;
- bounded waiting and outbound buffering;
- one cleanup path for completion, failure, cancellation, and replacement; and
- a declared overload outcome that does not blame a peer for local scheduling.

Peer selection remains separate. A conformant peer can waste a connection slot
without violating any message rule.

## Exchange ownership

Regulation follows protocol exchanges rather than treating every wire variant
as an independent traffic source:

| Role | Regulation owner |
| --- | --- |
| Standalone announcement | The announcement's cadence and state bounds |
| Request | The request and all response work it causes |
| Response | The reservation created when Zakura sent the request |

A request declaration names its possible response messages and reserves their
worst-case cost. Each response frame spends bytes from that reservation. This
prevents counting `GetBlocks`, `Block`, and `BlocksDone` as three unrelated
load regulators when they are one serving exchange.

Response messages still have independent wire and receiving-side contracts.
Exchange ownership changes resource accounting, not the wire inventory.

## Admission path

The exact checks depend on the role, but a serving request follows this shape:

```text
frame header and per-message cap
  → bounded decode and wire validation
  → peer/session and protocol prerequisites
  → provisional resource admission
  → reactor or service handoff
  → committed handler work
  → leased response frames
  → settlement
```

The frame header identifies the message before Zakura allocates its payload.
The transport rejects a payload above that message's cap at that point. A
stream-wide maximum remains a final ceiling, not a substitute for the
message-specific cap.

Validation that needs no shared state runs before expensive work. Local
availability and lifecycle checks must distinguish peer behavior from races
created by replacement, finality, or scheduling. Zakura disconnects only when a
conformant sender could not have produced the input.

## Separate resource controls

Rate and outstanding work are different resources:

| Control | What releases it | Purpose |
| --- | --- | --- |
| Rate bucket | Time and permitted refunds | Bounds bursts and sustained throughput |
| Outstanding budget | Work completion or cancellation | Bounds admitted work that remains resident |
| Response backlog | Frame handoff or drop | Bounds response bytes owned by one session |
| Concurrency ledger | Terminal request settlement | Bounds active request count |
| Pending-input bound | Admission progress or session end | Bounds decoded requests waiting to be admitted |

A refilling rate bucket cannot bound stalled work. If unfinished requests stay
resident while tokens refill, outstanding work can grow forever. Conversely,
an outstanding budget alone does not bound sustained throughput. Regulated
requests therefore use both.

The same distinction applies at two scopes:

- **Peer controls** isolate identities and sessions.
- **Node controls** cap aggregate work even when an attacker uses many peers.

The node controls are the security boundary. Peer controls provide isolation
and fairness within it. Every configured per-peer collection must also have an
aggregate bound at the maximum connection count.

## Linear ownership

Resource ownership follows the work instead of relying on matching refund calls:

```text
admission attempt → committed request permit → frame leases → released
```

An admission attempt holds provisional peer and node charges. Dropping it
before commit restores all of them. Commit occurs only after the receiving
service confirms that the originating session still owns the peer.

The committed permit is bound to the service's request identity. Its fixed
request overhead remains spent. Unused response capacity is refundable.
Completion, rejection, channel failure, cancellation, disconnect, and
replacement all settle the same owned permit exactly once.

When the handler queues a response, capacity transfers from the permit into a
lease carried with that frame. Reserving the queue slot happens before the
transfer, so a failed enqueue moves no accounting. The lease ends when the
transport accepts the write or drops the frame.

QUIC may retain bytes after the application write completes. Its send-window
envelope is a separate transport bound and must be included in slow-reader
tests.

## Waiting and full-duplex streams

When a limit blocks a request, that request must not reach the handler and must
not be charged twice when retried. The peer routine waits only on the resource
that blocked it.

Stopping all reads is the smallest backpressure mechanism on a one-way stream.
On a bidirectional ordered stream it can also trap responses to requests Zakura
sent. A contract may therefore use bounded demultiplexing while one admission
waits, provided it defines:

- which message classes may continue;
- the per-session and aggregate pending-input bound;
- the outcome when that bound is full; and
- a full-duplex progress property.

The GetBlocks contract uses this option so block responses can pass delayed
serving requests. Its advertised in-flight limit bounds queued request tuples,
and requests beyond that queue are dropped without a peer score. The aggregate
memory implied by that choice must be validated at the maximum connection
count before the regulated-load layer is marked complete.

Waiting for one peer must not hold shared locks, writers, or handler permits.
Other peer routines remain independently runnable. Strong fairness or latency
claims require an explicit scheduler policy; otherwise native tests report the
observed bound under a named topology.

## Identity and reconnects

Session-owned resources, including response backlog and queued frames, end with
the session. A rate bucket may instead follow an authenticated identity across
reconnects so reconnecting does not grant a fresh burst.

Any inactive identity cache must be bounded. It may discard a fully refilled
entry because recreating it grants no additional work. An eviction policy must
state the maximum allowance a prematurely evicted identity can regain, and it
must not evict an active or permit-referenced account.

## Configuration and observability

All charge arithmetic uses checked operations and local configuration. Startup
validation ensures that the largest legal request fits every applicable burst,
outstanding, and backlog capacity, so a valid request cannot wait forever
because it can never fit.

Regulation records:

- the exchange and peer;
- the bound that delayed or rejected work;
- requested, reserved, transferred, used, and refunded units; and
- the terminal reason.

Metrics use bounded labels. Peer identities belong in trace rows, not metric
labels. A shared regulation log is unnecessary until more than one service
needs it.

## Adoption

The first exchange should validate generic primitives without forcing every
message through a speculative framework:

1. Specify the exchange and its load properties.
2. Build generic rate, outstanding, and frame-lease primitives.
3. Compose message-specific attempt and permit types beside the service.
4. Validate logical accounting, native load, slow readers, and honest progress.
5. Compare the second regulated exchange with the first before extracting a
   shared declaration API.

Cadence, response reservations, and verification filters should move into a
common facade only when a second implementation demonstrates the same shape.
The production design must remain understandable without the property-test
model.

Message priority, connection-slot policy, Sybil-resistant peer selection, and
stream layout are outside this design.
