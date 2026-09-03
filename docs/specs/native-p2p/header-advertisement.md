# Header-sync advertisement exchange (`Status`)

> **Status: draft successor proposal.** This preserves the stream-5 version 9
> design from the original regulation proposal. It is not the current protocol
> contract and has not been reconciled with production or executable evidence.
>
> Normative keywords below apply only to the proposed successor. Before this
> becomes **Specified**, its requirements need stable IDs and implementation
> evidence under the [contract standard](README.md).

## Proposed stream version and shared limits

Header sync version 9 MUST allow discriminators `1..=4` in the frame header. It MUST remove the
duplicate payload discriminator used by version 8. Let `H` equal 1,487 bytes on Mainnet and Testnet
and 177 bytes on Regtest. For a selected auxiliary schema, let `A` equal 156 bytes for V1 and zero
otherwise. The following subscription limits apply:

```text
MAX_HS_PUSH_CREDIT_HEADERS   = 4,000
MAX_HS_PUSH_CREDIT_BYTES     = 8 MiB
MAX_HS_SUBSCRIPTIONS         = 1 live or closing subscription per peer
MAX_HS_RANGE                 = 4,000 headers per response
HEADERS_RESPONSE_FIXED_BYTES = 82 bytes
HEADERS_OUTCOME_BYTES        = 41 bytes
HS_SENT_CURSOR_RING          = 4,096 sent cursors per subscription
HS_PUSH_DEADLINE             = 30 seconds
HS_WORK_CAPACITY             = MAX_HS_PUSH_CREDIT_BYTES + HEADERS_OUTCOME_BYTES + 64 KiB
HS_WORK_REFILL               = 1 MiB/s
```

The cap test pins `HEADERS_RESPONSE_FIXED_BYTES` to the codec. The frame cap already has an
implementation in `HeaderSyncMessage::check_payload_size`.

`Status` retains the version 8 fields in the same order: work-anchor height and hash, selected-tip
height and hash, 32-byte cumulative work, oldest-retained height, maximum headers per response,
maximum subscriptions, maximum message bytes, and the auxiliary-schema mask. Removing the payload
discriminator makes its encoded size 122 bytes.

## `Status` — Announcement, discriminator 1

- **Frame**
  - payload cap = 122 bytes
- **Decode** — `HeaderSyncMessage::decode`
  - `work_anchor_height <= selected_tip_height`
  - `oldest_retained_height <= selected_tip_height`
  - `max_headers_per_response` = 1..=`MAX_HS_RANGE`
  - `max_subscriptions` = 1
  - `max_message_bytes` = `HEADERS_RESPONSE_FIXED_BYTES + H + 4 + A` ..= 2 MiB for every advertised
    auxiliary schema
  - `tree_aux_schema_mask` contains only known bits
  - exact consumption
- **Ignore without penalty**
  - ignore when neither the advertised target nor a serving-limit change can affect target selection,
    failover, or a future credit grant
- **Cadence**
  - capacity = 4
  - refill = 2 messages/s
  - on_empty = `Disconnect`

The sender MUST coalesce changes to at most one `Status` per second. `work_anchor_height` is the
height of the sender's finality anchor. `oldest_retained_height` is the lowest height for which
the sender retains headers.

Before this draft becomes **Specified**, honest connection and update traces MUST show that the
candidate cadence accepts startup bursts and scheduling jitter. A flood test MUST show that a sender
which exceeds its stated obligation reaches `Disconnect` within bounded work.
