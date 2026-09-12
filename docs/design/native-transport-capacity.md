# Native transport capacity

This document defines the transport milestone for message regulation. The policy
is shared by all native messages. It includes streams before application admission
and retains capacity while transport state is closing.

## Stream progress

The native endpoint allows 16 remotely initiated and 17 locally initiated
bidirectional streams. Unidirectional application streams are disabled. The local
limit includes control setup, compatibility requests and retired streams whose
receive final offset is still unknown. A stream returns its transport slot only
when both halves are freed. The application handshake advertises 16 open streams.

Each receive stream has 256 KiB of credit. Connection credit is 9.5 MiB, or 38
stream windows. Thirty-two paused siblings can retain 8 MiB, leaving 1.5 MiB for
the remaining stream. The pinned transport batches MAX_DATA updates until one
eighth of connection credit has been consumed. That threshold is 1.1875 MiB.
The remaining stream therefore has enough credit to reach successive updates.

For a stream window W, a connection window C, and S paused streams, the required
inequality is C - S*W > C/8. The policy uses S=32 and C=38*W, so the available
6*W exceeds the update threshold of 4.75*W. Increasing a stream count or window
requires recomputing this inequality before changing production defaults.

Verification must fill every paused stream with acknowledged, unread bytes,
exercise both stream directions, deliver more than one connection window through
the remaining stream, then drain every original stream without reconnecting.
The original one- and two-sibling T02 witnesses must also pass.

## Allocation inventory

Flow-control credit is not an allocation budget. These bounds are inputs to the
node allowance and must be verified separately.

| Resource | Policy | Lifetime |
| --- | --- | --- |
| Receive payload credit | 9.5 MiB per connection | Until consumed or discarded |
| Receive fragment records | 1,024 per receive stream | Includes retained backing capacity |
| Send payload | 32 MiB per connection | Includes acknowledged tails behind a missing prefix |
| Send backing | Owned 64 KiB blocks | Payload plus at most two blocks per buffered stream |
| Send range records | 4,096 per set, two sets per send stream | Reject before growth; release empty arrays |
| Packet history | Span of 4,096 per path and encryption space | Includes sent and lost records and gaps |
| Multipath | Existing eight-path limit | Includes closing paths and unused granted IDs |
| Driver datagram queue | 256 packets per connection | Slot retained through protocol processing |
| Raw incoming attempts | 32 attempts, 2 MiB additional packets | Before connection admission |
| Connection admission | Shared inbound/outbound pool | Owner released after transport storage destruction |

With finite fragment limits, received stream fragments own their allocation.
Compaction removes duplicate bytes and copies each contiguous run into a separate
allocation. For ordered native readers, the retained backing after an insert is
at most 2.5*W + 32 KiB per stream, where W is its receive window. A subsequent
insert adds at most one datagram before compaction. During compaction the old
backing, a temporary buffer of at most W and new runs totaling at most W can
coexist. A conservative peak allowance is therefore 4.5*W + 32 KiB + D, with D
the maximum admitted datagram size. Fragment heap capacity and allocator overhead
are separate. Application-owned copies after a read use the application budget.

The regression tests check release of a 1 MiB packet backing after admitting one
byte, independent ownership of compacted runs, and actual backing capacity through
generated insert, read, overlap and compaction histories. These checks establish
stream backing ownership, not a process RSS ceiling or an unordered-history bound.

The node-wide byte allowance is not established by this table. Receive batching,
pending control retransmissions, packet-frame metadata, endpoint tables, and
allocator overhead still require explicit bounds and allocation evidence. A
connection charge must cover their sum before constructing transport state.
The maximum connection count remains a separate ceiling. Application objects,
verification and state caches retain their own budgets outside this transport
allowance.

## Endpoint lifetime

Connection reservations alone cannot fund the current endpoint implementation.
`Tasks::start_remote_state_actor` creates an endpoint-ID mapping when a remote
actor starts. `AddrMap::get` inserts both forward and reverse entries, and the map
has no removal operation. `RemoteMap::remove_or_restart_actor` removes an idle
actor's sender but leaves those mapping entries behind. Sequential connections
with new identities can therefore grow endpoint storage after each connection's
transport reservation is released. This is established by source inspection.

Remote actors also retain their own state through a 60-second idle timeout.
That state needs an endpoint allocation owner independent of a connection's
owner. A complete node model needs bounded admission for remote actors and their
mapping entries, funding retained capacity through cleanup and concurrent reuse.
Closing a connection or expiring an actor must not leave unfunded map capacity.
The same owner must cover incoming peers and outgoing resolution attempts.

Before selecting a numeric connection charge, finish the pending-control and
packet-metadata bounds, separate fixed endpoint/receive-batch storage, and bound
these endpoint lifetimes. Then reserve from one configurable node pool before
creating each owner. Verify sequential identity churn as well as simultaneous
connection saturation. A count semaphore or an RSS sample alone cannot establish
this bound. The proposed 4 GiB starting budget remains provisional until those
charges establish how many connections it can actually fund.

## Qualification

On September 12, combined property revision `d594451d2`, including compliance
`13bd5044f`, passes 104 compliance witnesses and 473 fixed regressions. All 49
property assertions pass with 2,048 cases and seed 896, with two unresolved
nextest output-handle closure flags. The full-occupancy witness is included in
the ordinary regression selection. The transport dependency passes 447 protocol
tests. These results precede optimized throughput and complete node allocation
qualification.

The milestone is complete only when the enabled policy passes T02 and the full
occupancy witness, its allocation inventory has a funded node-wide bound, and
tests cover denial, handshake failure, retirement and reuse at capacity. RSS
measurements supplement allocation and ownership evidence. They do not replace
the bounds. Optimized throughput must retain the existing 90 percent threshold
against the baseline on both lossless and impaired links, with five samples each.

The copied transport dependencies contain unpublished APIs. Publishing those
changes and updating registry requirements remains a release prerequisite.
