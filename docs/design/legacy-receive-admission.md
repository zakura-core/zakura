# Legacy receive admission

Legacy streams use the same transport frame reader as native services. Their message types, reserved flags, and fixed or bounded payload lengths are now checked before payload allocation. These checks retain the existing wire limits and exact-budget EOF behavior.

## Ownership

At most eight request bodies per authenticated peer and 64 per node may be received or retained for processing. Admission happens before reading the payload. A cloneable receive lease follows work into the inbound service queue, so timing out the transport waiter cannot release capacity while queued work still owns the body. Local exhaustion resets that exchange without blaming or disconnecting the peer.

Outbound legacy exchanges reserve separate response capacity, with one response per peer and four per node. The response retains its reservation through synchronous decoding. Dropped results and failed exchanges release the reservation. Native control and block-sync streams do not use this pool.

Each response has a 32 MiB retained-memory envelope in addition to its unchanged 16 MiB wire-payload budget. The estimate includes frame descriptors, vector growth and reallocation, allocator overhead, and two maximum-message buffers for assembly and decoding. A stream of tiny chunks can exhaust this local envelope before exhausting its wire allowance. This is local capacity exhaustion, not proof of a protocol violation.

Gossip ingress is separately bounded to eight queued frames per stream. The request/response reservations are not a complete process RSS limit. QUIC buffers, gossip queues, decoded objects, allocator retention, and unrelated node state remain separate consumers.

## Validation

The partial-payload test uses actual QUIC connections with default flow-control windows. Eight identities each hold eight almost-complete 1 MiB transaction frames. A ninth stream per identity and a ninth identity are refused without cancelling their connections. A separate Ping/Pong exchange progresses while all 64 inbound slots are occupied. Cancellation releases the slots.

Additional tests cover impossible header declarations, tiny response chunks, lease cloning, decode lifetime, and dropping undelivered results. The network library suite covers ordinary legacy responses, timeout handling, and native sync. Three unrelated TCP tests require extra loopback addresses unavailable on this macOS host and are excluded from the local suite.
