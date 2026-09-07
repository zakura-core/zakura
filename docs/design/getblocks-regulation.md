# GetBlocks serving regulation

GetBlocks admits bounded state work and holds its response producer until the
query, result, and application writes finish. QUIC flow and congestion control
provide transport backpressure.

## Message rules

The GetBlocks declaration in the peer-message regulation draft selects Frame,
Decode, and Work. The four categories organize message rules; they do not require
every message to select every filter.

| Category | Current GetBlocks path |
| --- | --- |
| Safe | The codec bounds the request count and height range. Serving enforces the advertised response count and body-byte cap, including the encoded framing allowance. |
| Authorized | Serving uses the authenticated session after its initial Status. Reservation checks belong to the Block and terminal responses on the requesting side. |
| Useful | GetBlocks has no Relevant predicate in the draft. Stale session work is cancelled before dispatch. Completed requests may be legitimate retries; the server does not infer what the requester has stored. |
| Budgeted | A session owns one response producer. Shared concurrency permits bound state queries, retained results, and writes; each routine holds at most one waiting request. |

This implements serving ownership, not complete conformance to the draft. The
complete filter inventory and its full reservation rules are not introduced here. In particular, the draft prohibits overlapping live GetBlocks
ranges, while the current sender has reassignment and late-response behavior
that needs a coordinated requester/responder change before that rule can be
enforced. Serial response production does not reject overlapping requests.

## Shared request admission

Admission means deciding whether a request can start work now. A slot is room for
one active response.

For GetBlocks, the steps are:

1. Check the request's fields and that the session has sent its initial `Status`.
2. Take a slot from both the session and the node's GetBlocks pool. If either is
   full, return any slot already taken and pause reading this stream until room
   opens.
3. Check that the session is still current, then start reading the blocks.
4. Keep the slots held until the block query ends and the response is written to
   the transport or discarded. Then another request can use them.

If the request is cancelled before its query starts, the query won't run. If the
query has already started, it keeps its slots until it finishes.

The shared code handles taking, holding, and returning slots. Each message
supplies its own rules for reading the request and limiting its response.
GetBlocks supplies those rules through `GetBlocksPolicy`; block sync still reads
the blocks and sends them. Each message's setup chooses its slot pool. GetBlocks
currently has its own node pool.

This shared code supports requests whose responses end. A GetPeers test checks
that a second message can use the same code. GetPeers regulation is only enabled
in that test.

## Backpressure and ownership

Backpressure means making the sender wait when we have no room for more work.
Ownership means keeping a request's slots held until its work is done.

The request, block query, returned blocks, and queued messages share the same
slots. Taking a message out of the send queue does not free those slots: its write
may still be waiting for QUIC. The slots return when the request, query, and all
writes have finished or been discarded. QUIC may still hold bytes after accepting
a write; we don't wait for the peer to confirm it has read them.

For example, if one response is stuck waiting to be written, the next GetBlocks
request on that session waits for its slot. We stop reading that stream. As its
buffers fill, QUIC makes the sender wait too. Messages behind the waiting request,
including responses to our own downloads, also wait. Outgoing writes keep running
so the first response can finish and free its slots.

A peer may send requests ahead of time within the advertised request limit. A
request waiting for room is not a peer fault. If we cannot take both required
slots, we return any slot already taken before waiting. Once the wait gives us a
slot, we use that same slot when trying again.

| Limit | Default | What happens at the limit |
| --- | --- | --- |
| Waiting requests | 1 per session | Pause reading that stream |
| Active responses | 1 per session, 64 per node | Wait for the previous work and writes to release their slots |
| Waiting for a query result | 8 seconds | Stop waiting for the result; the query keeps its slots until it ends |
| Waiting to queue the ending message | `request_timeout`, 8 seconds | Close the session without scoring the peer for misconduct |
| Waiting to admit a request | `request_timeout`, 8 seconds | Close the session without scoring the peer for misconduct |

If we cancel a request before its query starts, the query won't run. Starting the
query and checking cancellation happen together. A query that has already started
keeps its slots until it ends, even after a timeout or disconnect. A query that
never ends keeps those slots; the timeout cannot stop the storage work. Reconnecting
does not free slots still held by the old session.

If the send queue fills partway through a response, we send only the blocks already
queued, followed by an ending message. That ending message waits for queue space
without blocking other reactor work. It keeps the response's slots held and stays
tied to the original session. Cancellation, shutdown, a closed queue, or the time
limit ends the wait. Once queued, the message keeps holding the slots through its
write.

We extend download deadlines by the time spent waiting at admission, up to
`request_timeout` in total between accepted blocks. A longer wait closes the session
without blaming the peer. Download speed measurements still include the wait.
Each QUIC stream has a 16 MiB receive window within the connection's 32 MiB window,
leaving room for another service when one stream pauses.

Each response also has a size limit. For the block count we allow in that response,
its maximum payload size is
`min(count * MAX_BLOCK_BYTES, advertised_max_response_bytes) + count + 9`.
The extra `count` allows one message tag per block; the final 9 bytes allow the
ending message. All messages in a response share its slots rather than taking a
new slot for each message.

## Resource boundary

The response wire bound does not measure decoded block memory or total process
RSS. State decoding, serialization temporaries, and a block fetched while finding
the range boundary can add memory. At defaults, each response contains at most
one 2,000,000-byte block plus framing; larger configured ranges remain capped by
32 MiB of bodies. The transport separately allows a 32 MiB send window per
connection. Connection limits and existing decode bounds remain relevant.

Message prioritization is outside this change. These controls do not establish
consensus progress under every combined CPU, storage, and network workload.
