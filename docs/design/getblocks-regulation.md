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

A request acquires a producer before dispatching its state query. The ledger,
state worker, returned result, and queued frames share the same permit. Dequeuing
a frame does not release it: the transport retains the frame's guard through
`write_ordered_frame`. A pending QUIC write therefore keeps the producer occupied
and prevents the next query for that session. This does not wait for a remote
application acknowledgement; QUIC may retain bytes after accepting a write.

There is at most one response being produced or written per live session. Its
payload cap is
`min(count * MAX_BLOCK_BYTES, advertised_max_response_bytes) + count + 9`.
Each queued frame shares the producer without acquiring additional capacity.
The final owner releases capacity immediately.

| Resource | Default | Owner and release point |
| --- | --- | --- |
| Waiting request | 1 per session | Held at admission; further stream reads pause |
| Response producers | 1 per session, 64 per node | Ledger, query, result, and transport frames; released when the last owner drops |
| Query response deadline | 8 seconds | Ends response delivery; underlying state work retains its producer until completion |
| Terminal queue deadline | `request_timeout`, 8 seconds | Retains ownership while waiting; expiry closes the original session without a misconduct score |
| Admission delay deadline | `request_timeout`, 8 seconds | Closes the locally backpressured session without a misconduct score |

The advertised inflight window still permits pipelined requests. One producer
serializes their execution; it does not turn a waiting request into a protocol
violation. Admission rolls back partial reservations before waiting. A slot
waiter uses the permit assigned to it on its next admission attempt.

When admission waits, the routine holds the current request and stops reading
further frames. Existing application queues and QUIC receive buffers provide
backpressure. Later responses on this ordered stream wait too.
Outbound writes run independently of inbound
forwarding so they can release serving capacity while reads are paused.

Download deadlines exclude local admission pauses. The total grace between
accepted blocks is at most `request_timeout`; a longer pause closes the local
session without penalizing the peer. Delivery-rate samples still include the
pause. QUIC uses a 16 MiB stream receive window within the existing 32 MiB
connection window, leaving credit for another service when one stream pauses.

Ledger closure and the one-time query claim share synchronized state. Closure
before the claim prevents the read. A claimed read drains after timeout or
disconnect because dropping its awaiter does not stop blocking state work. A read
that never completes retains capacity; the timeout cannot terminate the storage
operation. Old session owners remain counted until they finish even after a
replacement session connects.

A full outbound queue can truncate a response to the prefix already queued. Its
terminal response waits independently of other reactor work, tied to the original
session and retaining its producer. Cancellation, queue closure, shutdown, or the
local deadline ends that wait. Successful enqueue retains ownership through the
application write.

## Resource boundary

The response wire bound does not measure decoded block memory or total process
RSS. State decoding, serialization temporaries, and a block fetched while finding
the range boundary can add memory. At defaults, each response contains at most
one 2,000,000-byte block plus framing; larger configured ranges remain capped by
32 MiB of bodies. The transport separately allows a 32 MiB send window per
connection. Connection limits and existing decode bounds remain relevant.

Message prioritization is outside this change. These controls do not establish
consensus progress under every combined CPU, storage, and network workload.
