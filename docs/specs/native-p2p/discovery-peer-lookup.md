# Discovery peer-lookup exchange (`GetPeers` and `Peers`)

> **Status: draft successor proposal.** This preserves the stream-4 version 2
> design from the original regulation proposal. It is not the current protocol
> contract and has not been reconciled with production or executable evidence.
>
> Normative keywords below apply only to the proposed successor. Before this
> becomes **Specified**, its requirements need stable IDs and implementation
> evidence under the [contract standard](README.md).

## Proposed stream version and shared limits

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

## `GetPeers` — Request, discriminator 2

- **Frame**
  - payload cap = 8,470 bytes
- **Decode** — `validate_query_fields`
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
- **Decode** — `validate_record_body_bounds`
  - count = 0..=reserved_limit
  - records <= 32
  - node IDs unique
  - exact consumption
- **Reservation**
  - one outstanding `GetPeers` on this stream
  - bounds count and payload bytes
  - consumed by this message
- **Verify** — the signature and immutable import checks from
  `ZakuraNodeRecord::verify` for each record, before the discovery book lock
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
