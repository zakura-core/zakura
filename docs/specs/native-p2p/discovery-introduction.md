# Discovery introduction exchange (`Hello`)

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

## `Hello` — Announcement, discriminator 1

- **Frame**
  - payload cap = 648 bytes
- **Decode** — `validate_record_body_bounds`, `validate_service_id`
  - addresses <= 8
  - services <= 8
  - service ID length = 1..=32 ASCII bytes
  - `protocol_min <= protocol_max`
  - record body <= 580 bytes
  - exact consumption
- **Message validity** — `ZakuraNodeRecord::verify`, with time-varying import policy split
  from `validate_record_body_for_import`
  - record signature
  - network and chain IDs
  - protocol overlap
  - record author == authenticated peer
- **Ignore without penalty**
  - ignore when the sequence does not advance the peer-local stored record
  - ignore when local expiry policy rejects the record
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Hello` every 15 seconds. The message validity checks MUST run
before the discovery book lock. Expiry and sequence checks MUST return `Drop` because clock passage
and local record state can make an otherwise valid record obsolete. The handler MUST NOT store an
address that is not globally routable unless local policy allows that address class. A stored
record's sequence comparison ends when the stored record expires, so a peer that reset its sequence
recovers after expiry.
