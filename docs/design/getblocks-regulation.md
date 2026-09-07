# GetBlocks serving regulation

GetBlocks admits bounded state work and holds its response producer until the
query, result, and application writes finish. QUIC flow and congestion control
provide transport backpressure. There is no serving byte-rate bucket or separate
peer/node outstanding-byte budget.

## Message rules

The GetBlocks declaration in the peer-message regulation draft selects Frame,
Decode, and Work. The four categories organize message rules; they do not require
every message to select every filter.

| Category | Current GetBlocks path |
| --- | --- |
| Safe | The codec bounds the request count and height range. Serving enforces the advertised response count and body-byte cap, including the encoded framing allowance. |
| Authorized | Serving uses the authenticated session after its initial Status. Reservation checks belong to the Block and terminal responses on the requesting side. |
| Useful | GetBlocks has no Relevant predicate in the draft. Stale session work is cancelled before dispatch. Completed requests may be legitimate retries; the server does not infer what the requester has stored. |
| Budgeted | A session owns one response producer. Shared concurrency permits bound state queries, retained results, and writes; pending request state is bounded separately. |

This implements serving ownership, not complete conformance to the draft. The
common message-declaration framework and its full reservation rules are not
introduced here. In particular, the draft prohibits overlapping live GetBlocks
ranges, while the current sender has reassignment and late-response behavior
that needs a coordinated requester/responder change before that rule can be
enforced. Serial response production does not reject overlapping queued requests.

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
This is the response's wire bound, not a shared byte balance. Each queued frame
shares the producer without acquiring additional capacity. The final owner
releases capacity immediately, without a refill timer.

| Resource | Default | Owner and release point |
| --- | --- | --- |
| Pending requests | 64 per session, 1,024 per node | Queue entry or admission task; released on admission or cancellation |
| Response producers | 1 per session, 64 per node | Ledger, query, result, and transport frames; released when the last owner drops |
| Query response deadline | 8 seconds | Ends response delivery; underlying state work retains its producer until completion |
| Terminal queue deadline | `request_timeout`, 8 seconds | Retains ownership while waiting; expiry closes the original session without a misconduct score |
| Full pending queue deadline | `request_timeout`, 8 seconds | Closes the locally backpressured session without a misconduct score |

The advertised inflight window still permits pipelined requests. One producer
serializes their execution; it does not turn a waiting request into a protocol
violation. Admission rolls back partial reservations before waiting. A slot
waiter uses the permit assigned to it on its next admission attempt.

Bounded request staging lets block responses received on the same ordered stream
make progress while serving waits for a query or write. Once staging fills, the
routine holds one additional decoded request and pauses reads. Thus there may be
one blocked input per live session in addition to the 1,024 node queue slots.
Completion handling, cancellation, and the queue deadline remain live. This
staging is an implementation difference from the draft's instruction not to add a
delayed-request queue; removing it requires preserving same-stream download
progress, not merely removing a rate limiter.

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

## Local comparison

The ignored `getblocks_serving_comparison` test uses real local Iroh connections,
a single server, and one or four downloading nodes. Each downloads 128 synthetic
64,000-byte blocks through the production block-sync path. The existing harness
uses mock storage and apply operations, a 64-request window, and bounded transport
queues. This measures serving and transport behavior, not RocksDB throughput or
full mainnet sync time. Run the identical test on main and the candidate:

```sh
ZAKURA_COMPARISON_READERS=1 cargo test --locked -p zakura-network --lib getblocks_serving_comparison -- --ignored --nocapture
ZAKURA_COMPARISON_READERS=4 cargo test --locked -p zakura-network --lib getblocks_serving_comparison -- --ignored --nocapture
```
