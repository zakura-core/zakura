# Discovery service-lookup exchange (`GetServices` and `Services`)

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

## `GetServices` — Request, discriminator 4

- **Frame**
  - payload cap = 1,090 bytes
- **Decode** — `validate_get_services`, `validate_service_id`
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
- **Decode** — `validate_services`
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
- **Verify** — `validate_summary_envelope` for the envelope, and
  `import_connected_peer_services` for the peer binding
  - each known envelope tag matches the service ID
  - each known summary decodes strictly
  - an unknown summary stays length-bounded and is ignored
  - `node_id` == authenticated peer
- **Relevant**
  - expiry is not in the past
  - at least one summary differs from stored live state

An empty summary list remains legal and clears the peer's live service state.
