# GetBlocks serving regulation

GetBlocks uses the shared regulation primitives to reserve work before a state
read starts. These accounts cover GetBlocks across the node. Other message
families do not yet share these accounts.

## Ownership and defaults

| Resource | Default | Owner and release point |
| --- | --- | --- |
| Pending requests | 64 per session, 1,024 per node | Queue entry or admission task; released on admission or cancellation |
| Active requests | 64 per node; per-session ceiling follows advertised inflight requests | Ledger, state worker, returned result, and pending terminal share ownership; released when their last owner drops |
| Outstanding response payload | 64 MiB per session, 256 MiB per node | Reserved before the query; actual queued bytes transfer into frame leases, released after the application write completes or drops |
| Peer work rate | 16 MiB/s; burst 32 MiB + 128 bytes + 9 bytes + 64 KiB | Identity account survives reconnects while retained; fixed work and queued payload consume tokens, unused response allowance is refunded |
| Node work rate | 64 MiB/s; burst 128 MiB | Shared GetBlocks account with the same settlement rules |
| Fixed request work | 64 KiB of byte-equivalent work | Committed when the reactor accepts the request, even for an empty response |
| Query response deadline | 8 seconds | Ends response delivery; underlying state work keeps its charge until completion |
| Terminal queue deadline | `request_timeout`, 8 seconds by default | Retains the terminal and request ownership while waiting; expiry closes the original session without a misconduct score |
| Full pending queue deadline | `request_timeout`, 8 seconds by default | Closes the locally backpressured session without a peer misconduct score |

For a request clamped to `count` blocks, the reserved payload is
`min(count * MAX_BLOCK_BYTES, advertised_max_response_bytes) + count + 9`.
The extra bytes cover each Block discriminator and the terminal response.
The rate reservation adds the fixed request work. The default advertisement
serves one block per response; the 32 MiB range byte ceiling also supports larger
configured ranges. Configuration validation requires each capacity to fit the
largest request allowed by that configuration.

Admission rolls back partial reservations before waiting. A slot waiter retains
the permit assigned to it and uses that permit in its next admission attempt.
There is no strict fairness guarantee across the combined rate, slot, and byte
accounts.

When the pending queue fills, the peer routine holds one additional decoded
request and pauses stream reads. Completion processing, cancellation, and the
local queue deadline remain live. Thus the 1,024 node slots bound fully retained
queue entries; there can also be one blocked input per live session. That input
may own a session slot while waiting for a node slot.

The query lease follows the dispatched action, the underlying state future, and
the returned blocks. Dropping request ownership cancels delivery. Ledger closure and the one-time
query claim share a synchronized state. Closure before the claim prevents the
read; a successful claim retains capacity even if closure immediately follows.
A claimed read drains even after timeout or disconnect,
because dropping its awaiter would not stop blocking state work. Each lease
authorizes at most one query. A read that never completes keeps its capacity;
the timeout cannot promise to terminate the underlying storage operation.

A full outbound queue can truncate a block response to the prefix already
queued, but its `BlocksDone` or `RangeUnavailable` is retained until queue space
is available. Each terminal wait owns the request's active slot and remaining
reservations, so at most the configured active-request limit can wait. These
waits run independently of other reactor work and stay tied to the original
session. Cancellation, queue closure, reactor shutdown, or the local deadline
ends the wait. Successful enqueue transfers terminal bytes to the transport;
those bytes remain charged until the application write completes or drops.

## Memory boundary

The outstanding-byte limit counts serialized response payload reservations,
not total process resident memory. Decoded blocks, temporary serialization
buffers, and a block fetched while discovering the range byte boundary can add
memory outside that count. Queued frames retain their payload leases through
the application write, but QUIC can retain data after that handoff. Its separate
send window is 32 MiB per connection, not a node-wide GetBlocks memory budget.
Peer/session limits therefore remain part of the resource envelope.

## Fixed local measurement

An ignored measurement exercises admission, serialization, frame leases, and
immediate draining of an in-memory transport. It reuses committed mainnet
fixtures plus an existing fixed-shape near-limit serialization fixture. The
synthetic fixture is not a consensus-valid chain. Each case sends 32 one-block
responses per peer, round-robin, with and without regulation.

```sh
cargo test --locked -p zakura-network --lib serving_fixed_workload_measurement -- --ignored --nocapture
```

Observed on the dedicated Mac Studio on 2026-09-04 with a debug build:

| Serialized body bytes | Peers | Total responses | Unregulated ms | Regulated ms |
| --- | --- | --- | --- | --- |
| 1,617 | 1 | 32 | 0.617 | 0.624 |
| 1,617 | 4 | 128 | 0.454 | 0.903 |
| 73,079 | 1 | 32 | 4.616 | 4.079 |
| 73,079 | 4 | 128 | 16.935 | 16.974 |
| 1,998,926 | 1 | 32 | 455.385 | 1,962.776 |
| 1,998,926 | 4 | 128 | 1,809.659 | 2,002.162 |

All cases returned outstanding bytes and active slots to zero. Near-limit cases
show rate pacing after the initial burst. Small timing differences are noisy;
these are single local samples, not production throughput estimates.

The existing fixed healthy downloader scenarios `fuzz_steady` and
`fuzz_steady_bytes_unit` also passed, each downloading 300 fixture blocks from
three peers. The existing native transport test
`service_owned_source_sends_multiple_ordered_frames` passed. These establish
basic download progress and native ordered delivery, not sustained native
GetBlocks performance.

Defaults remain provisional. This measurement excludes disk reads, a real QUIC
GetBlocks exchange, and process memory under sustained pressure. Production
calibration still needs representative block eras, concurrent serving and
downloading, constrained readers, and measurements of throughput, queue delay,
state concurrency, and resident memory. Generated property/load histories remain
deferred until the production policy is settled.
