# Block-sync advertisement exchange (`Status`)

> **Status: draft.** This preserves the block-sync advertisement declaration
> from the original regulation proposal. It has not yet been converted into
> stable requirement IDs or linked to executable evidence. GetBlocks serving
> has its own [block-range contract](block-range.md).

## Preserved block-sync context and limits

Version 2 requests bodies by height range. The requester still reserves the expected header hash for
each height, but the wire request does not identify that hash. Competing branches can occupy the
same height, so version 2 cannot safely overlap live ranges on one connection. A future version must
make request and body correlation explicit.

Block sync MUST allow discriminators `1..=5` in the frame header. Version 2 also carries the same
one-byte discriminator at the start of each payload. The decoder MUST require both copies to match.
Removing the payload copy requires a new stream version. The following limits apply:

```text
MAX_BLOCKS_PER_RESPONSE  = 128
MAX_BLOCK_BYTES          = 2,000,000 bytes
MAX_BS_RESPONSE_BYTES    = 33,554,432 bytes
MAX_BS_INFLIGHT_REQUESTS = 32,768
```

The receiver MUST advertise its actual block count, response body-byte limit,
and inflight limit.

## `Status` — Announcement, discriminator 1

- **Frame**
  - payload cap = 53 bytes
- **Decode** — `BlockSyncMessage::decode`
  - `servable_low <= servable_high`
  - `max_blocks_per_response` = 1..=128
  - `max_inflight_requests` = 1..=32,768
  - `max_response_bytes` = 1..=33,554,432
  - exact consumption
- **Ignore without penalty**
  - ignore when none of the range, tip hash, or serving-limit changes can affect candidate selection,
    pending demand, failover, or an open request
- **Cadence**
  - capacity = 4
  - refill = 1 message / 15 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens.
