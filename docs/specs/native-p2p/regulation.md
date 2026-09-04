# Native P2P message regulation: specification

> **Status: proposal.** This specification defines traffic-control requirements
> for Zakura's native P2P exchanges. The
> [design](../../design/peer-message-regulation.md) explains the choices. The
> [property-testing standard](../../design/property-testing.md) defines the
> required evidence.

## Scope

Message regulation bounds work caused by complete native P2P messages. It
covers framing allocation, admission, response ownership, pending state,
cleanup, and outbound response bytes.

It does not choose peers, prioritize services, change stream layout, or prevent
a protocol-conformant peer from wasting a connection slot. Those require
separate policies.

The keywords **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

Shared requirements use the prefix `P2P-RG`, meaning native P2P regulation.
An exchange contract must map every applicable `P2P-RG` ID to one or more of
its own ID-named tests. A shared transport test may satisfy the mapping when the
exchange uses that exact transport path. An inapplicable requirement needs a
reason; an applicable requirement may not be omitted.

## Contract inventories

**P2P-RG-01 — Closed inventories.**

A completed multi-exchange implementation MUST derive a closed wire-kind
inventory from production code. For the first exchange, the catalog and
machine-checked test manifests are the temporary review gate. Compiler-derived
closure is required before a second exchange is called a reusable
implementation of this standard.

Every wire kind MUST have:

- one codec and exhaustive dispatch arm;
- a payload cap known before payload allocation;
- bounds for variable-length decoded state;
- a wire-format contract;
- the sender obligation for any rule that can disconnect; and
- an explicit state effect or no-state-effect marker.

Each regulated exchange root MUST have:

- its role: announcement, request, or reserved response;
- the response message kinds it can cause;
- peer and node resource bounds;
- checked charge and refund arithmetic;
- its concurrency and pending-input bounds;
- outcomes for each unavailable resource;
- its session and request ownership keys; and
- all terminal settlement paths.

Wire kinds and regulated exchange roots are different inventories. A request
owns regulation for the response frames it causes; those responses remain
separate wire kinds.

Implementations SHOULD derive both inventories from production enums or a
shared declaration. They MUST NOT treat a fixed message count as closure.

## Admission outcomes

**P2P-RG-02 — Declared outcomes and sender obligations.**

Contracts use these semantic outcomes even when production names them
differently:

| Outcome | Meaning |
| --- | --- |
| Admit | The message may enter its handler. |
| Drop | Ignore a bounded message without blaming the peer. |
| Wait | Keep one bounded admission pending until a local resource changes. |
| Reject | Decline a valid request with its declared protocol response. |
| Disconnect | End the peer or stream for behavior a conformant sender cannot produce. |
| Local fault | Release owned resources and report an implementation or dependency failure. |

Local scheduling, finality, replacement, cancellation, and capacity exhaustion
MUST NOT become peer violations. A contract MAY drop or reject excess valid
requests when replying would itself violate a resource bound.

Every inbound rule that can disconnect a peer MUST identify the matching
obligation on a conformant sender. Local configuration, scheduling, finality,
replacement, or work reassignment MUST NOT cause a conformant sender to violate
that obligation.

## Processing order

**P2P-RG-03 — Checks precede the work they bound.**

Checks MUST run before the work they bound. A serving request uses this order:

1. Read the fixed frame header.
2. Validate stream version, message kind, flags, and the message-specific
   payload length.
3. Allocate and decode no more than that payload cap.
4. Validate canonical wire values and exact consumption.
5. Check peer, session, and protocol prerequisites.
6. Reserve all applicable peer and node resources provisionally.
7. Hand the request to the owning service.
8. Confirm session ownership and commit the admission.
9. Run the handler and transfer response bytes into frame leases.
10. Settle on every terminal path.

An announcement replaces resource admission with its declared cadence check. A
response also checks the reservation created by the request Zakura sent.

## Message validity and harmless ignores

**P2P-RG-04 — Invalid and harmless messages remain distinct.**

**Message validity** asks whether the peer sent a valid message. These checks
use the decoded message, the authenticated peer identity, and fixed network
rules. They run before consulting mutable chain or service state. A failure
follows the contract's declared invalid-message outcome.

**Ignore without penalty** applies to a valid message that is stale or cannot
affect the receiver's current state. The decision MUST use a cheap, bounded
snapshot without waiting for I/O or a shared-state lock. It MUST NOT disconnect
the peer, record misbehavior, or discard a response that still matches a live
reservation merely because local interest changed. Cadence, reservation, or
another declared limit MUST bound repeated ignored messages.

## Framing and decoding

**P2P-RG-05 — Message-specific caps precede allocation.**

The fixed frame header MUST identify the message kind before its payload buffer
is allocated. The transport MUST reject a payload longer than that kind's
declared cap before allocation. A stream-wide frame maximum MUST remain as a
defense in depth limit but MUST NOT replace the per-message cap.

**P2P-RG-06 — Incomplete frames are bounded.** The transport MUST separately
bound partial-frame state and enforce a read
deadline for an incomplete frame. Complete-message admission limits do not
prevent a peer from starting a frame and then withholding the remaining bytes.

**P2P-RG-07 — Decoding is total, canonical, and independently bounded.** For
every payload within the cap, decoding MUST return a result without
panicking. Accepted payloads MUST be canonical and consumed exactly. Decoding
MUST reject unknown flags, reserved bits, trailing bytes, and values outside
their declared wire ranges.

Every variable-length field MUST have an allocation bound independent of the
payload buffer's cap. A checked fixed prefix, protocol limit, and live
reservation MAY further reduce that bound before allocation.

The contract MUST distinguish invalid encoding from a valid request whose
requested application data is unavailable.

## Resource bounds

**P2P-RG-08 — Charges and internal results are locally bounded.**

A request that can cause response work MUST declare a worst-case charge from
local inputs:

```text
charge = response_cap + request_overhead
refund = response_cap - response_bytes_used
```

`response_cap` MUST include every response payload the request can cause,
including a terminal response. Multiplication and addition MUST use checked
arithmetic. Peer-provided configuration MUST NOT increase the local charge
limit.

An internal query that can produce several response items MUST receive the
applicable local byte limit and enforce it while constructing its result.
Charging only the outbound prefix does not bound a larger intermediate result
that is materialized and discarded later.

**P2P-RG-09 — Peer and node controls bound aggregate state.** Each regulated
serving exchange MUST use the controls that apply:

| Control | Required behavior |
| --- | --- |
| Peer rate | Bounds one authenticated identity's burst and sustained work. |
| Node rate | Bounds aggregate burst and sustained work across identities. |
| Peer backlog | Bounds reserved, queued, and application-owned unwritten response bytes for one session. |
| Node outstanding | Bounds all admitted response bytes not yet handed to transport. It does not refill with time. |
| Concurrency ledger | Bounds committed requests and associates each with one owner. |
| Pending-input bound | Bounds decoded requests waiting behind admission, per session and in aggregate. |

Per-peer controls MUST NOT be the only aggregate defense. Each capacity and
collection MUST fit its node-wide resource budget at the configured maximum
connection count.

Startup validation MUST ensure the largest legal request fits every applicable
rate capacity, outstanding budget, and backlog. A legal request MUST NOT wait
forever because it can never fit a configured bound.

## Ownership and settlement

**P2P-RG-10 — Accounting has one session-gated ownership chain.**

Resource accounting MUST follow one linear ownership chain:

```text
provisional attempt → committed permit → zero or more frame leases → release
```

A provisional attempt MUST roll back every charge and reservation when dropped.
The service MUST commit it only after confirming that its originating session
still owns the peer. A stale attempt MUST produce no query, response, or peer
violation.

After commit:

- the fixed request overhead remains spent;
- unused response capacity remains refundable;
- the permit is bound to the service request identity; and
- every completion, rejection, channel failure, cancellation, disconnect, and
  replacement settles it exactly once.

**P2P-RG-11 — Queues, leases, and transport buffers remain bounded.** Enqueueing
a leased frame MUST reserve a queue slot before ownership moves. A
failed reservation MUST move no accounting. A successful enqueue transfers the
frame's accounted payload bytes from the request permit into one non-cloneable
lease.

The lease MUST remain live until the application transport accepts the write or
drops the frame. Bytes retained later by QUIC MUST fit declared per-connection
and node-wide send-window caps. Startup validation MUST ensure the sum of all
connection windows at the configured connection limit fits the node-wide QUIC
cap. Slow-reader evidence MUST check the application budget, the QUIC budget,
and their combined envelope.

Dropping a session MUST settle its permits without transferring them to a
replacement session. Already queued frame leases remain responsible for their
bytes until the transport releases them. An unknown, repeated, stale, or
mismatched completion MUST NOT release another request's resources.

## Waiting and overload

**P2P-RG-12 — Waiting and overload remain bounded and explicit.**

When rate or outstanding capacity is unavailable, the contract MUST state
whether the request waits, is rejected, or is dropped. It MUST also state the
observable response and peer-score behavior.

A waiting request:

- MUST emit no handler work before admission;
- MUST own no shared lock, stream writer, or handler permit;
- MUST wait only on the bound that blocked it;
- MUST retry without rerunning completed filters or charging twice; and
- MUST end cleanly when its session or handoff channel closes.

If a routine continues reading a bidirectional stream while admission waits, it
MUST bound decoded pending requests per session and across the maximum peer
count. It MUST let required response traffic reach its receiving path, define
what happens when the pending bound is full, and test that a peer Zakura is
downloading from can still make progress.

A full internal handoff channel MUST retain a provisional attempt until the
channel accepts it or closes. A committed request MUST not disappear because an
action or output channel is full. It MUST settle through its declared rejection
or local-fault path.

## Responses and reservations

**P2P-RG-13 — Inbound responses consume live reservations.**

Before Zakura sends a request, it MUST create a bounded reservation for the
responses that request authorizes. The reservation identifies the peer,
session, request, response kinds, and count or byte limits.

Each received response MUST match one live reservation or unconsumed range
part. A missing, duplicate, or mismatched reservation follows the exchange's
declared violation outcome. Local work reassignment MUST NOT silently make an
already-authorized response invalid.

Reservation state MUST remain bounded by the outbound request limit and must be
removed on terminal completion or connection end.

## Announcements

**P2P-RG-14 — Announcements have bounded state and cadence.**

An unsolicited announcement MUST have a fixed payload and state bound. If it
can arrive repeatedly, its contract MUST define a peer cadence with enough
burst allowance for connection setup and scheduling jitter.

Cadence exhaustion MAY disconnect only when the matching sender obligation
guarantees that a conformant sender cannot exhaust it. A stale but valid
announcement SHOULD be dropped rather than scored.

## Identity and cache bounds

**P2P-RG-15 — Identity-owned state has bounded retention.**

The contract MUST say whether each resource belongs to a session or an
authenticated identity. Identity-owned rate state MAY survive reconnects to
prevent burst resets.

An inactive identity cache MUST have a fixed capacity and eviction rule. It
MUST NOT evict active or permit-referenced state. Its contract MUST bound the
allowance an early eviction can restore.

## Faults and observability

**P2P-RG-16 — Local faults release ownership and expose bounded evidence.**

A local panic or dependency failure MUST release all owned resources. It MUST
NOT be recorded as peer misbehavior. Failure isolation MUST keep unrelated peer
paths running whenever the surrounding service remains sound.

Each delayed, rejected, disconnected, or locally failed admission MUST expose:

- service and exchange;
- peer and session when available;
- the bound or rule responsible;
- reserved, transferred, used, and refunded units when applicable; and
- the terminal reason.

Metrics MUST avoid peer identity labels. Trace storage and label cardinality
MUST remain bounded.

## Required evidence

A regulation layer is implemented only when:

1. Its contract maps every applicable `P2P-RG` ID to exchange-specific,
   ID-named tests and gives a reason for every inapplicable ID.
2. Fast properties check charge arithmetic, conservation, rollback,
   settlement, session ownership, and configured bounds.
3. Generated histories vary peers and every applicable resource boundary.
4. Under-budget histories preserve the pre-regulation behavior.
5. Native traffic checks reading floods, stopped readers, transport buffering,
   cleanup, and useful peer progress under a named topology.
6. Transport tests bound partial-frame state and expire incomplete frames under
   the configured read deadline.
7. Configuration tests reject any value set that cannot admit the largest legal
   request or cannot bound aggregate state.
8. Sensitivity checks demonstrate that each observed production channel can
   make the suite fail.

CPU, resident memory, and throughput are diagnostics unless the contract names
the build, topology, and deadline that make one an acceptance gate.

The first concrete declaration is the
[block-range exchange](block-range.md).
