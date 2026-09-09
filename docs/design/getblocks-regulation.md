# GetBlocks serving

Native block sync uses two persistent QUIC streams on one connection. Requests
travel on one stream; Status, blocks, and response endings travel on the other.
One sequential task serves each session. Waiting for serving capacity pauses
request intake while the data reader continues processing downloads.

The [stream specification](../specs/blocksync/stream-pair.md) defines negotiation,
framing, and retirement. The [execution results](getblocks-refactor-results.md)
record the transport gate, including its memory and throughput limits.

## Request flow

**Example:** A is downloading block 200 from B while A's 64 serving slots are
occupied. B also asks A for block 100. A holds that request and waits for a
serving slot. Block 200 can still arrive on A's separate data stream.

The serving task:

1. Decodes one GetBlocks request. If the initial Status has not arrived on the
   data stream, waits for it under a ten-second setup deadline.
2. Acquires the authenticated peer's response slot, then the node's slot. Both
   waits are cancellable and use FIFO admission. Waiting for an earlier response
   from this identity does not consume another node slot.
3. Rechecks the session and snapshots the committed serving range and response
   limits. An unavailable range gets RangeUnavailable without a storage read.
4. Dispatches one bounded storage read through the node's state adapter.
5. Reserves an output-queue slot before encoding each response frame. Queue
   pressure waits for space; it does not truncate the available response.
6. Queues the available contiguous prefix followed by BlocksDone. An empty read
   or storage failure produces RangeUnavailable. All output uses the original
   session's data sender.

A request may arrive before Status because the streams are independent. That
ordering alone is not a peer fault. Wrong-role messages, malformed ranges, and
oversized headers are rejected before expensive work.

## Ownership

A response slot remains held by every resource that can outlive its caller:
its producer, database job, retained result, encode, and queued or writing frames.
The slot returns only after all of those owners finish or are discarded.

**Example:** B disconnects while A is reading block 100 for it. A cancels delivery,
but the running database job still owns its slot. B's replacement session waits
for that same identity's slot instead of starting another read alongside it.

The state adapter claims execution once, then moves the lease into the blocking
job and its returned result. Cancellation is checked before and between lookups.
Aborting the async caller does not release resources still owned by a real read.
There is no serving-query timeout. A database call that never ends keeps its slot.

Each queued frame has a guard that retains the response's slots through its
application write or discard. Taking a frame out of the queue does not release
those slots. QUIC can retain bytes after accepting the write; the application
permit does not wait for the remote peer to read them.

The producer drops its ownership after queuing the ending message. The next
request can wait for the final frame guard to release the peer's slot. It must
not hold the old producer while waiting for all old owners to disappear.

## Bounds and cancellation

| Resource or wait | Limit or behavior |
| --- | --- |
| Decoded waiting requests | One per admitted session |
| Raw request queues | One inbound and one outbound frame; a reader or writer can also hold a frame |
| Data queues | Configured block-sync inbound and outbound queue depths |
| Active responses | One per authenticated identity, including reconnects; 64 per node by default |
| Storage and encoding | One storage dispatch and at most one active encode per response |
| Request writes | Finish once claimed while the session is valid; cancellation resets the pair |
| Data writes | 32 seconds, including Status and ending messages |
| Initial Status | Ten seconds |
| Incomplete stream pair | Prelude deadline, three seconds by default |

Outgoing requests reserve queue space before publishing outstanding work. The
same ownership lock orders publication, reset, enqueue failure, and the writer's
initial claim. If expiry wins before that claim, no bytes are written. If the
writer wins, it finishes the frame while its session remains valid. Cancellation
of a partial write resets both streams before another frame can be sent.
Received blocks and replacement requests keep their exact ownership during
cleanup; an obsolete request cannot return their reservations.

Full buffers alone do not cause a disconnect. Existing request expiry,
block-progress liveness, and bounded data writes handle sustained stalls.
Cancellation returns unreceived work for retry and keeps running jobs charged.
Pair reopening uses the existing cooldown and backoff. Other services remain
usable unless a connection-wide failure or repeated-stall policy closes the
connection.

Active responses, admission waiters, and the oldest response age are reported
under `sync.block.serving.*`. Observations follow the last database/result/frame
owner and do not retain that work themselves. `sync.block.sessions.reserved`
counts establishing and retiring sessions as well as current ones;
`sync.block.sessions.pending` counts incomplete setup. Labels contain no peer IDs.

## Transport and memory

The transport uses a 16 MiB receive allowance per stream, a 32 MiB shared receive
allowance per connection, and a 32 MiB connection send window. It needs no
per-stream window extension.
Opening two streams does not allocate or reserve that memory in advance. Paused
services can consume the shared allowance and delay otherwise-ready streams.

The default advertised burst of 32,000 requests is 544,000 framed bytes. It fits
alongside one paused 16 MiB sibling in the measured workload. Excessive traffic
can fill two stream windows and consume all shared receive credit. Tests require
natural recovery from a temporary pause and bounded cleanup followed by a
completed retry for sustained saturation.

For each admitted response, the payload bound is
`min(count * MAX_BLOCK_BYTES, advertised_max_response_bytes) + count + 9`.
The extra count allows a tag per block; nine bytes cover the ending. Output queue
capacity is reserved before serialization, and all frames share the original
response slot.

This bound does not measure decoded blocks, encoding temporaries, total RSS, or
QUIC buffers. Connection limits and existing decode bounds still matter. The
512 MiB gate envelope applies to the documented local fixture, not every possible
configuration or node workload.

## Shared policy and scope

`GetBlocksPolicy` supplies decoding and response-size rules to the shared finite
request admission code. GetBlocks uses its own node slot pool. A GetPeers test
checks reuse of the generic mechanism; production GetPeers regulation is outside
this change.

This implements serving ownership and progress under the declared workloads,
not all requirements of the broader regulation draft. Overlapping live ranges
are still handled by the requester's existing reassignment and late-response
rules; serial serving does not reject them. Message prioritization, download-index
optimization, and regulation of other services remain separate work.
