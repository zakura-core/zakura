# Zakura peer message regulation: specification

> **Status: first draft.** This specification defines the rules introduced by
> [`final_draft_design_doc.md`](./final_draft_design_doc.md). It covers the 14 native application
> messages in discovery, header sync, and block sync.

`MUST` and `MUST NOT` identify requirements needed for security or interoperability. `SHOULD` and
`SHOULD NOT` identify defaults that an implementation may change only when it preserves every
`MUST` requirement. `MAY` identifies optional behavior.

## Filter model

Each message has one role. The role selects the bound on the work caused by that message.

| Role | Work bound | Budget verdict |
| --- | --- | --- |
| Announcement | A cadence declared by the protocol | `Disconnect` when the cadence budget is empty |
| Request | The upper-bound cost of the response | `Delay` when the work budget is empty |
| Response | A reservation created by the receiver's request | `Disconnect` when no matching reservation exists |

Each message declaration selects concrete filters from four categories:

| Category | Filters | Purpose |
| --- | --- | --- |
| **Safe** | Frame, Decode, Verify | Bound parsing, allocation, and verification before they occur |
| **Authorized** | Reservation | Accept only responses caused and bounded by a request we sent |
| **Useful** | Unique, Relevant | Remove repeated or obsolete work without blaming the peer for a race |
| **Budgeted** | Cadence, Work | Bound message frequency or response work |

The admission path returns one of five results:

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot help now. |
| `Delay` | The request is legal, but its work budget is empty. |
| `Disconnect` | A conformant sender cannot produce the message. |
| `LocalFault` | The receiver accepted work and then failed to complete it. |

The implementation MUST apply filters before the work that they bound. Applicable filters MUST run
in this order:

```text
frame
  -> cadence
  -> reservation precheck
  -> bounded decode
  -> reservation match
  -> stateless verification
  -> uniqueness and relevance
  -> work charge
  -> handler
```

The order MAY skip filters that the message declaration does not select. The Reservation filter MAY
split into a precheck and an exact match. The precheck MUST establish decode bounds before any
allocation. The exact match MUST run before Verify. A fixed-prefix read used by either step MUST NOT
allocate from a peer-declared value.

### Requirements common to every filter

1. Every message type MUST have one declaration and one handler. Every handler MUST have one
   declaration.
2. Every declaration MUST select exactly one role and one work bound.
3. Every filter MUST hold O(1) state per peer. The receiver MUST choose every state bound.
4. A filter MUST NOT grow a map or queue from peer-chosen keys or counts.
5. Every enforced inbound rule MUST have a matching outbound obligation.
6. An outbound cadence MUST not exceed half of the cadence enforced inbound.
7. Local scheduling, finality, reorganization, and work reassignment MUST NOT produce a peer
   violation.
8. `Delay` MUST stall only the responsible peer and message class. It MUST NOT block an inbound
   response on the same connection.
9. Every non-`Continue` result MUST emit the service, message type, filter, direction, peer, key,
   result, and reason to `regulation.jsonl`. A full-trace mode SHOULD emit `Continue` results.

## Safe filters

### Frame

The Frame filter takes `(stream_kind, stream_version, message_type, flags, payload_len)` and does not
read the payload.

- The Frame filter MUST require `flags == 0`.
- The Frame filter MUST require `message_type` to appear in the allowlist for the stream kind and
  version.
- The Frame filter MUST reject `payload_len` above the message cap before allocating a payload
  buffer.
- Each message cap MUST equal the maximum encoded payload size for that message and network.
- A test MUST compare each declared cap with the codec's computed maximum.
- The frame header MUST carry the message discriminator. A payload copy MAY exist only when the
  codec verifies that both copies match.
- A Frame failure MUST return `Disconnect`.

### Decode

The Decode filter converts a bounded frame into one canonical message.

- Decode MUST be total and MUST consume the payload exactly.
- Decode MUST reject trailing bytes, unknown flags, reserved bits, non-canonical values, and values
  outside their declared ranges.
- Decode MUST reject an invalid field. It MUST NOT clamp that field into range.
- Decode MUST bound every allocation before it occurs.
- A collection allocation MUST not exceed the smallest of its declared count, its protocol limit,
  and `remaining_bytes / minimum_item_size`.
- A live reservation MUST supply every response bound chosen by the request.
- A Decode failure MUST return `Disconnect`.

### Verify

The Verify filter performs every check that does not need chain state or mutable service state.

- Verify MUST perform no I/O.
- Verify MUST hold no lock and access no shared mutable state.
- Each data type MUST have one verifier and one ingress call site.
- Verify MUST run before the handler and before any shared-state lock.
- A Verify failure MUST return `Disconnect`.
- A contextual check MUST run in the handler. A contextual failure caused by the message MUST
  return `Disconnect`.
- A handler failure caused by local state or local capacity MUST return `LocalFault` or retry
  internally. It MUST NOT return `Disconnect`.

## Authorized filter

### Reservation

A reservation represents one protocol exchange. It is not the scheduler's current interest in the
work.

- The sender MUST create a reservation before sending a request.
- The reservation MUST record the peer, response kind, correlation identity, decode bounds, and
  expected object identity.
- Each response MUST match and consume exactly one live reservation or one unconsumed part of a
  bounded range reservation.
- A missing, duplicate, or mismatched reservation MUST return `Disconnect`.
- A reservation MUST remain live until the complete response arrives or the connection ends.
- A local work deadline MAY reassign work. It MUST NOT remove or change the reservation.
- A protocol deadline MAY close a connection when the peer produces no terminal response. The
  protocol deadline MUST be separate from every work deadline.
- Reservation state MUST not exceed the inflight request limit chosen by the sender.

After a response consumes its reservation, the Relevant filter MAY drop the response because the
receiver no longer needs the work. That drop is a local scheduling result, not a peer violation.

## Useful filters

### Unique

The Unique filter stores recent keys in a fixed-size per-peer ring.

- A declaration MUST state the key, ring capacity, window, and repeat result.
- A key MUST identify the work. It MUST NOT use only a peer-chosen request identifier.
- An announcement repeat MUST return `Drop`.
- A request repeat MAY return `Disconnect` only when the first request was fulfilled and the key
  pins an immutable answer for the entire window.
- A request whose answer can change MUST omit this filter or return `Drop`.
- The sender MUST NOT repeat a fulfilled request within the declared window.

### Relevant

The Relevant filter decides whether a legal message can change receiver state.

- The predicate MUST use the decoded message and a cheap, bounded snapshot.
- The predicate MUST take no lock.
- A false predicate MUST return `Drop`.
- The Relevant filter MUST NOT return `Delay` or `Disconnect`.
- The sender MUST include enough information for the receiver to distinguish a peer violation from
  a local race.

A dropped announcement has already consumed a Cadence token. A dropped response has already
consumed reservation capacity. A declaration that has neither bound MUST declare a separate drop
budget.

## Budgeted filters

### Cadence

Cadence uses one monotonic token bucket per `(peer, message_type)`.

```text
Cadence {
  capacity: messages,
  refill: messages / second,
  on_empty: Disconnect,
}
```

- Cadence MUST run before Decode or Verify when either operation has material cost.
- Capacity MUST absorb the allowed connect-time send, jitter, and message coalescing.
- The protocol MUST state the matching sender cadence.
- The sender cadence MUST not exceed half of the receiver refill rate.
- Cadence exhaustion MAY return `Disconnect` only when a conformant sender cannot exhaust it.

### Work

Work uses one token bucket and one concurrency bound per `(peer, request_type)`.

```text
charge = upper_bound_response_bytes + REQUEST_OVERHEAD
refund = charge - actual_response_bytes
```

- `REQUEST_OVERHEAD` MUST equal 64 KiB.
- Work MUST charge the upper bound before the handler starts.
- Work SHOULD refund unused response bytes after the terminal response is queued.
- Work exhaustion MUST return `Delay(deficit / refill)`.
- Work MUST NOT discard an accepted request.
- The bucket capacity MUST be at least the largest request that the receiver accepts.
- The refill MUST state the per-peer response bandwidth that the receiver grants.
- The concurrency bound MUST not exceed the inflight limit advertised by the receiver.
- A service MUST isolate delayed requests before it enables `Delay`.

## Message declarations

All byte caps below cover payload bytes. The transport frame header is not part of the cap. Only
the filters listed in a declaration apply to that message.

### Discovery — stream 4, version 2

Discovery MUST carry discriminators `1..=5` in the frame header. It MUST remove the payload
discriminator used by version 1. The following limits apply to every discovery message:

```text
MAX_DIRECT_ADDRS             = 8
MAX_SERVICES_PER_RECORD      = 8
MAX_SERVICE_ID_BYTES         = 32
MAX_DISCOVERY_RECORDS        = 32
MAX_EXCLUDED_NODE_IDS        = 256
MAX_SERVICE_SUMMARIES        = 8
MAX_SERVICE_SUMMARY_BYTES    = 256
NODE_RECORD_MAX              = 648 bytes
SERVICE_ENVELOPE_MAX         = 294 bytes
DISCOVERY_WORK_CAPACITY      = 4 MiB
DISCOVERY_WORK_REFILL        = 1 MiB/s
```

#### `Hello` — Announcement, discriminator 1

```text
Frame        cap = 648 bytes
Decode       addresses <= 8; services <= 8; service ID length = 1..=32 ASCII bytes;
             protocol_min <= protocol_max; record body <= 580 bytes; exact consumption
Verify       record signature; network and chain IDs; protocol overlap;
             record author == authenticated peer
Relevant     sequence > peer-local stored sequence and expiry is acceptable under local policy
Cadence      capacity = 4; refill = 1 message / 7 seconds; on_empty = Disconnect
```

The sender MUST send at most one `Hello` every 15 seconds. Verify MUST run before the discovery
book lock.

#### `GetPeers` — Request, discriminator 2

```text
Frame        cap = 8,470 bytes
Decode       limit = 1..=32; wanted services <= 8; excluded node IDs <= 256;
             service IDs are 1..=32 ASCII bytes and unique;
             excluded node IDs are sorted and unique
Work         charge = 2 bytes + limit * 648 bytes + 64 KiB; capacity = 4 MiB;
             refill = 1 MiB/s; concurrency = 1; on_empty = Delay
```

The handler MUST apply `wanted_services` when it samples the discovery book. The handler MUST send
exactly one `Peers` response for every admitted request.

#### `Peers` — Response, discriminator 3

```text
Frame        absolute cap = 20,738 bytes; reservation cap = 2 + reserved_limit * 648 bytes
Decode       count = 0..=reserved_limit; records <= 32; node IDs unique; exact consumption
Verify       verify each record signature, bounds, network ID, chain ID, and protocol range before
             taking the discovery book lock
Reservation  one outstanding GetPeers on this stream; bounds count and payload bytes;
             consumed by this message
```

A malformed record, invalid signature, wrong network, wrong chain, or incompatible protocol range
MUST disconnect the relaying peer. The handler MUST discard an otherwise valid record when local
staleness, expiry, address, or storage policy rejects it.

#### `GetServices` — Request, discriminator 4

```text
Frame        cap = 1,090 bytes
Decode       wanted services <= 32; service IDs are 1..=32 ASCII bytes and unique;
             exact consumption
Work         response_cap = 42 bytes + min(matching local services, 8) * 294 bytes;
             charge = response_cap + 64 KiB; capacity = 4 MiB;
             refill = 1 MiB/s; concurrency = 1; on_empty = Delay
```

The handler MUST apply `wanted_services`. An empty list means all supported services. The handler
MUST send exactly one `Services` response for every admitted request.

#### `Services` — Response, discriminator 5

```text
Frame        absolute cap = 2,394 bytes;
             reservation cap = 42 + reserved_summary_count * 294 bytes
Decode       summaries <= 8; service ID length = 1..=32 ASCII bytes;
             summaries contain only reserved service IDs; summary length <= 256 bytes;
             service IDs unique; exact consumption
Verify       envelope tag matches the service ID; each known summary decodes strictly;
             node_id == authenticated peer
Reservation  one outstanding GetServices on this stream; supplies allowed service IDs,
             summary count, and payload cap; an empty request reserves all service IDs and
             eight summaries; consumed by this message
Relevant     expiry is not in the past and at least one summary differs from stored live state
```

An empty summary list remains legal and clears the peer's live service state.

### Header sync — stream 5, version 8

Header sync MUST allow discriminators `1..=4`. Let `H` equal 1,487 bytes on Mainnet and Testnet and
177 bytes on Regtest. Let `A` equal 156 bytes when the request selects tree auxiliary schema V1 and
zero otherwise.

#### `Status` — Announcement, discriminator 1

```text
Frame        cap = 123 bytes
Decode       work_anchor_height <= selected_tip_height; oldest_retained_height <= selected_tip_height;
             max_headers_per_response = 1..=MAX_HS_RANGE; max_inflight_requests >= 1;
             max_message_bytes = HEADERS_RESPONSE_FIXED_BYTES + H + 4 ..= 2 MiB;
             tree_aux_schema_mask contains only known bits; exact consumption
Relevant     decoded Status != stored Status for this peer
Cadence      capacity = 4; refill = 2 messages/s; on_empty = Disconnect
```

The sender MUST coalesce changes to at most one `Status` per second.

#### `GetHeaders` — Request, discriminator 2

```text
Frame        cap = 463 bytes
Decode       request_id != 0 and increases within the session; locator count = 1..=13;
             locator hashes are unique; max_header_count = 1..=MAX_HS_RANGE;
             tree_aux_schema is known and was advertised; exact consumption
Unique       key = (target_tip_hash, locator_hashes, max_header_count, tree_aux_schema);
             capacity = 16; window = 30 seconds; on_repeat = Disconnect;
             record only after a Headers response is queued
Work         response_cap = min(local_max_message_bytes,
             83 + max_header_count * (H + 4 + A)); charge = response_cap + 64 KiB;
             capacity = 4 MiB; refill = 1 MiB/s; concurrency = 1; on_empty = Delay
```

A `HeadersOutcome` response MUST NOT record the request in the Unique ring. The sender MAY retry
that request after the terminal outcome and another work charge.

#### `Headers` — Response, discriminator 3

```text
Frame        absolute cap = computed_max_encoded_size(Headers, network) <= 2 MiB;
             reservation cap = min(reserved_message_bytes,
             83 + reserved_count * (H + 4 + A))
Decode       request_id != 0; header_count <= reserved_count; response schema admitted by request;
             complete is 0 or 1; body_size <= 2,000,000; canonical network solution size;
             exact consumption
Verify       valid Equihash; hash <= target(nBits); nBits well-formed; page linkage;
             complete page ends at target_tip_hash; empty only for a complete response at target;
             auxiliary height alignment and activation defaults
Reservation  identity = (request_id, work scope, target_tip_hash, sent locator entries,
             max_header_count, requested schema, reserved_message_bytes); consumed by this message
Relevant     reservation work scope is still current
```

The handler MUST verify contextual difficulty, time, chain connection, and auxiliary roots. The
handler MUST disconnect the peer when one of those checks fails.

#### `HeadersOutcome` — Response, discriminator 4

```text
Frame        cap = 42 bytes
Decode       request_id != 0; outcome is TargetNotRetained, NoLocatorIntersection,
             or HistoryPruned; exact consumption
Reservation  same identity and reservation as Headers; consumed by this message
Relevant     reservation work scope is still current
```

`Busy` is not a protocol outcome. Local capacity exhaustion MUST delay the request before
admission. A local failure after admission MUST return `LocalFault`.

### Block sync — stream 6, version 2

Block sync MUST allow discriminators `1..=5` in the frame header. It MUST remove the duplicate
payload discriminator. The following limits apply:

```text
MAX_BLOCKS_PER_RESPONSE  = 128
MAX_BLOCK_BYTES          = 2,000,000 bytes
MAX_BS_RESPONSE_BYTES    = 33,554,432 bytes
MAX_BS_INFLIGHT_REQUESTS = 32,768
BLOCK_WORK_CAPACITY      >= 4 bytes + min(local_max_blocks_per_response * MAX_BLOCK_BYTES,
                           local_max_response_bytes) + 64 KiB
BLOCK_WORK_REFILL        = configured per-peer upload bandwidth
```

The receiver MUST advertise its actual block count, response byte, inflight, and work-refill
limits. It MUST NOT inherit the header-sync work budget.

#### `Status` — Announcement, discriminator 1

```text
Frame        cap = 20 bytes
Decode       servable_low <= servable_high; max_blocks_per_response = 1..=128;
             max_inflight_requests = 1..=32,768;
             max_response_bytes = 1..=33,554,432; exact consumption
Relevant     decoded range and limits differ from stored values for this peer
Cadence      capacity = 4; refill = 1 message / 15 seconds; on_empty = Disconnect
```

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens.

#### `GetBlocks` — Request, discriminator 2

```text
Frame        cap = 8 bytes
Decode       start_height <= Height::MAX; count = 1..=128; exact consumption
Work         response_cap = 4 bytes + min(min(count, local_max_blocks_per_response) * 2,000,000 bytes,
             local_max_response_bytes);
             charge = response_cap + 64 KiB;
             capacity = BLOCK_WORK_CAPACITY; refill = BLOCK_WORK_REFILL;
             concurrency = local_max_inflight_requests; on_empty = Delay
```

The handler MUST perform at most one contiguous read. It MUST send no more than one `Block` for
each requested height. It MUST finish with exactly one `BlocksDone` or `RangeUnavailable`.

#### `Block` — Response, discriminator 3

```text
Frame        cap = 2,000,000 bytes
Decode       one complete block; exact consumption
Verify       block parses; at least one transaction; coinbase is first and carries a height;
             merkle root recomputes; valid Equihash;
             hash <= target(nBits)
Reservation  one live GetBlocks range containing this height; expected identity is the committed
             header hash; consumes the height part of the reservation
Unique       key = height; scope = reservation; capacity = reserved count;
             window = reservation lifetime; on_repeat = Drop
Relevant     height and expected block are still needed
```

The handler MUST perform contextual block validation before commit. A message-caused consensus
failure MUST disconnect the peer. A local timeout MUST return `LocalFault` or retry internally.

#### `BlocksDone` — Response, discriminator 4

```text
Frame        cap = 4 bytes
Decode       start_height <= Height::MAX; exact consumption
Reservation  live GetBlocks range with this start_height; consumes the terminal part and closes
             the reservation
```

The handler MUST return every unreceived height to the work queue.

#### `RangeUnavailable` — Response, discriminator 5

```text
Frame        cap = 4 bytes
Decode       start_height <= Height::MAX; exact consumption
Reservation  live GetBlocks range with this start_height; consumes the terminal part and closes
             the reservation
```

The handler MUST requeue the range. A retry policy MAY avoid this peer for the immediate retry.

## Conformance tests

The implementation MUST provide these checks:

1. A declaration test MUST prove that the 14 declarations and 14 handler branches match exactly.
2. A cap test MUST prove that every maximal legal encoding fits its declared cap and that no
   computed maximum exceeds that cap.
3. Property tests MUST generate legal messages and gate-event sequences. They MUST preserve
   reservation, budget, and state-size invariants. A conformant sequence MUST never return
   `Disconnect`.
4. Model-based reservation tests MUST generate work reassignment, finality changes, response
   reordering, duplicate responses, and connection closure. Each response MUST consume exactly one
   reservation part.
5. Fuzz tests MUST prove that each decoder is total, exact, and allocation-bounded for arbitrary
   frames.
6. Honest-node regtest traces MUST contain no `Disconnect` result.

Peer-slot selection, message priority, and stream layout are outside this specification. Peer-slot
selection must remain separate because a conformant peer can waste a slot without violating a
message rule.
