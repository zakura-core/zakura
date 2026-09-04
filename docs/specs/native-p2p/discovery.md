# Discovery — stream 4, version 2

Discovery MUST carry discriminators `1..=5` in the frame header. It MUST remove the payload
discriminator used by version 1. The remaining field order and integer encodings stay unchanged.
The following limits apply to every discovery message:

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

## `Hello` — Announcement, discriminator 1

- **Frame**
  - payload cap = 648 bytes
- **Decode** — [`validate_record_body_bounds`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3917), [`validate_service_id`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L4042)
  - addresses <= 8
  - services <= 8
  - service ID length = 1..=32 ASCII bytes
  - `protocol_min <= protocol_max`
  - record body <= 580 bytes
  - exact consumption
- **Verify** — [`ZakuraNodeRecord::verify`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L286), with time-varying import policy split
  from [`validate_record_body_for_import`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3882)
  - record signature
  - network and chain IDs
  - protocol overlap
  - record author == authenticated peer
- **Relevant**
  - sequence > peer-local stored sequence
  - expiry is acceptable under local policy
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Hello` every 15 seconds. Verify MUST run before the discovery
book lock. Expiry and sequence checks MUST return `Drop` because clock passage and local record
state can make an otherwise valid record obsolete. The handler MUST NOT store an address that is
not globally routable unless local policy allows that address class. A stored record's sequence
comparison ends when the stored record expires, so a peer that reset its sequence recovers after
expiry.

## `GetPeers` — Request, discriminator 2

- **Frame**
  - payload cap = 8,470 bytes
- **Decode** — [`validate_query_fields`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3939)
  - limit = 1..=32
  - wanted services <= 8
  - excluded node IDs <= 256
  - service IDs are 1..=32 ASCII bytes and unique
  - excluded node IDs are sorted and unique
  - exact consumption
- **Work**
  - charge = 2 bytes + limit * 648 bytes + 64 KiB
  - capacity = 4 MiB
  - refill = 1 MiB/s
  - concurrency = 1
  - on_empty = `Delay`

The handler MUST apply `wanted_services` when it samples the discovery book. It MUST sample
qualifying records at random. It MAY apply the excluded node IDs on a best-effort basis: exclusion
improves sampling efficiency and does not entitle the sender to enumerate the book. The handler
MUST send exactly one `Peers` response for every admitted request.

## `Peers` — Response, discriminator 3

- **Frame**
  - absolute payload cap = 20,738 bytes
  - reservation payload cap = 2 + reserved_limit * 648 bytes
- **Decode** — [`validate_record_body_bounds`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3917)
  - count = 0..=reserved_limit
  - records <= 32
  - node IDs unique
  - exact consumption
- **Reservation**
  - one outstanding `GetPeers` on this stream
  - bounds count and payload bytes
  - consumed by this message
- **Verify** — the signature and immutable import checks from
  [`ZakuraNodeRecord::verify`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L286) for each record, before the discovery book lock
  - record signature
  - record body bounds
  - network ID and chain ID
  - protocol range

A malformed record, invalid signature, wrong network, wrong chain, or incompatible protocol range
MUST disconnect the relaying peer. Expiry MUST remain a handler policy check because a record can
expire in transit. The handler MUST discard an otherwise valid record when local staleness, expiry,
address, or storage policy rejects it. The handler MUST NOT store an address that is not globally
routable unless local policy allows that address class. Storage policy MUST bound the number of
stored records attributed to each source peer.

## `GetServices` — Request, discriminator 4

- **Frame**
  - payload cap = 1,090 bytes
- **Decode** — [`validate_get_services`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3968), [`validate_service_id`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L4042)
  - wanted services <= 32
  - service IDs are 1..=32 ASCII bytes and unique
  - exact consumption
- **Work**
  - response_cap = 42 bytes + min(requested service IDs, 8) * 294 bytes
  - an empty request uses eight requested service IDs for this calculation
  - charge = response_cap + 64 KiB
  - capacity = 4 MiB
  - refill = 1 MiB/s
  - concurrency = 1
  - on_empty = `Delay`

The handler MUST apply `wanted_services`. An empty list means all supported services. The handler
MUST send exactly one `Services` response for every admitted request.

## `Services` — Response, discriminator 5

- **Frame**
  - absolute payload cap = 2,394 bytes
  - reservation payload cap = 42 + reserved_summary_count * 294 bytes
- **Decode** — [`validate_services`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3972)
  - summaries <= 8
  - service ID length = 1..=32 ASCII bytes
  - summaries contain only reserved service IDs
  - summary length <= 256 bytes
  - service IDs unique
  - exact consumption
- **Reservation**
  - one outstanding `GetServices` on this stream
  - supplies allowed service IDs, summary count, and payload cap
  - an empty request reserves all service IDs and eight summaries
  - consumed by this message
- **Verify** — [`validate_summary_envelope`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3985) for the envelope, and
  [`import_connected_peer_services`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L1875) for the peer binding
  - each known envelope tag matches the service ID
  - each known summary decodes strictly
  - an unknown summary stays length-bounded and is ignored
  - `node_id` == authenticated peer
- **Relevant**
  - expiry is not in the past
  - at least one summary differs from stored live state

An empty summary list remains legal and clears the peer's live service state.
