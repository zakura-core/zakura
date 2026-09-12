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

The node-wide byte allowance is not established by this table. Receive backing
after compaction, receive batching, packet-frame metadata, endpoint tables, and
allocator overhead still require explicit bounds and allocation evidence. A
connection charge must cover their sum before constructing transport state.
The maximum connection count remains a separate ceiling. Application objects,
verification and state caches retain their own budgets outside this transport
allowance.

## Qualification

The milestone is complete only when the enabled policy passes T02 and the full
occupancy witness, its allocation inventory has a funded node-wide bound, and
tests cover denial, handshake failure, retirement and reuse at capacity. RSS
measurements supplement allocation and ownership evidence. They do not replace
the bounds. Optimized throughput must retain the existing 90 percent threshold
against the baseline on both lossless and impaired links, with five samples each.

The copied transport dependencies contain unpublished APIs. Publishing those
changes and updating registry requirements remains a release prerequisite.
