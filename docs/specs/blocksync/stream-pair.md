# Block-sync stream pair

Native block sync uses two persistent bidirectional QUIC streams on one
authenticated connection. The [serving design](../../design/getblocks-regulation.md)
describes resource ownership, local policy, and the required transport qualification.

## Negotiation

Both peers must select capability bit 6 (`1 << 6`) and both stream roles:

| Role | Kind | Version | Allowed frame message types | Maximum frame size |
| --- | --- | --- | --- | --- |
| Data and control | 6 | 3 | Status (1), Block (3), BlocksDone (4), RangeUnavailable (5) | 3 MiB + 8 bytes |
| Requests | 7 | 1 | GetBlocks (2) | 17 bytes |

The previous single-stream layout used kind 6, version 2, and capability bit 3.
Its encoding does not contain a pair identifier. Never interpret that stream as
one role of this layout. If negotiation cannot select both roles of a pair, it
must select neither.

## Setup

The opener writes the ordinary 13-byte ordered-stream prelude on each role,
followed immediately by an eight-byte pair identifier. Multi-byte integers use
little-endian encoding.

| Field | Bytes | Value |
| --- | --- | --- |
| Magic | 4 | `ZKST` |
| Stream kind | 2 | Role's kind above |
| Stream version | 2 | Role's version above |
| Request-ID presence | 1 | 0; these are persistent streams |
| Receive frame cap | 4 | Opener's accepted frame limit |
| Pair identifier | 8 | Same nonzero value on both roles |

Scope the pair identifier to the connection and opener. It is separate from
the generic prelude's optional request ID. Apply the existing simultaneous-open
selection rule to the whole pair. Reject duplicate roles, mismatched identifiers,
and unsupported declarations.

Neither role reaches block sync until both are ready. At most one incomplete
pair per block-sync service and connection is retained. The second role must
arrive within the configured prelude deadline, three seconds by default.
Setup holds the service's pending and directional session permits. Admission
releases the pending permit; transport tasks and application senders retain the
session permit through teardown.
The pending allowance is shared across directions, with one slot protected from
inbound setup whenever outbound sessions are enabled. At the default limit of
32, at most 31 incomplete inbound pairs can reserve setup capacity. Outbound
setup can use the remaining slot, and all setup together stays capped at 32.
With a limit of one, only outbound setup is possible unless outbound sessions
are disabled.
The demand check for a complete incoming pair reuses that reservation, so the
last available slot can admit a session. It still honors parks and useful-work
policy; opening another pair requires a new reservation.

## Messages

Message encodings are unchanged. Every frame has the ordinary eight-byte header
and a payload beginning with the block-sync discriminator. Reject a frame's
message type for the wrong role before allocating its payload. Status is bounded
to 53 payload bytes; GetBlocks and both ending messages to nine. Block frames
are capped at 2,000,001 payload bytes, including their discriminator. Reject
nonzero flags from the frame header before reading any payload. Block decoding
retains its existing maximum block-size check.

Status travels on the data stream. A request can arrive first on the other
stream: retain at most one decoded request while awaiting valid Status, for at
most ten seconds. Arrival order alone is not a protocol error.

One serving task handles requests sequentially. It acquires the authenticated
peer's response permit before node capacity, reads the bounded range, and sends
the available prefix followed by its ending message on the captured data sender.
Storage jobs, encodes, and queued or writing frames retain their permits until
completion or discard. Status can interleave between complete response frames.

## Writes and retirement

Both roles share one session identity, cancellation token, and message-rate
budget. Ending either role retires both; a replacement uses a new identity.
Teardown waits for both workers and their readers before reporting session exit.

Claim each outgoing request atomically against expiry and reset before its first
byte is written. Skip invalidated unwritten requests. A started request finishes
while its session remains valid, even if its request deadline expires. Cancelling
an unfinished write resets the pair; never append another frame after an
abandoned partial frame.

Request writes have no independent write timeout. The data stream has a bounded
32-second write deadline, including control and ending messages. This allows
shared-credit waits on slow links while other services are paused. Cancellation
can still interrupt a data write. Stream resets during a payload remain local to
the stream pair. A data-write timeout also resets only the pair, preserving
unrelated services on the connection. If downloads remain unanswered, teardown
applies the existing no-progress cooldown and repeated-stall disconnect before
readmission. A timeout with no download obligation does not penalize the peer.
A graceful end with a truncated frame payload remains invalid.

## Flow control

Keep the existing 16 MiB receive window per stream, 32 MiB shared connection
receive ceiling, and 32 MiB connection send window. Opening a stream does not
reserve dedicated connection credit. Other services can consume the shared
allowance; bounded application queues do not prevent that.

Temporary pauses can clear naturally. Sustained stalls use the existing write,
setup, and download deadlines. Cleanup returns unreceived work for retry and
retains resources still owned by running jobs. Reopening remains subject to
cooldown and backoff. A usable peer must still be able to complete the download
when another connection is saturated.
