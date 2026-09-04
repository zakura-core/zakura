# Block sync — stream 6, version 2

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
N                        = local_max_blocks_per_response
BLOCK_WORK_CAPACITY      >= 9 bytes + N + min(N * MAX_BLOCK_BYTES,
                           local_max_response_bytes) + 64 KiB
BLOCK_WORK_REFILL        = local per-peer serving rate, bytes/second
```

The receiver MUST advertise its actual block count, response body-byte limit, and inflight limit. It
MUST NOT inherit the header-sync work budget. `BLOCK_WORK_REFILL` is local policy and is not
advertised.

Block sync already regulates its rate on the requesting side. Each sender sizes its outstanding
`GetBlocks` work with a per-peer BBR window ([`DownloadWindow`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/state.rs#L333)), clamped by the inflight
limit the receiver advertises and operating at the measured bandwidth-delay product, which is
normally far below that clamp. That window is the outbound obligation matching this inbound rule.

The two sides meet through `Delay`. A receiver whose Work bucket is empty stops reading further
frames from that peer's ordered stream until Work becomes available. Its bounded queues apply QUIC
flow control instead of adding another request scheduler. The delay lengthens the sender's
round-trip samples, the sender's delay gradient shrinks its window, and the sender settles below the
rate the receiver serves.

`BLOCK_WORK_REFILL` therefore binds only a sender that ignores its own controller. The receiver MUST
set the rate from local policy. It MUST NOT derive the rate from a peer-supplied or peer-influenced
measurement, because a peer able to move that measurement would set its own budget. The receiver
MUST size the rate so that the rate multiplied by the maximum peer count fits its serving egress
budget. No configuration key sets this rate today: [`block_sync::config`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/config.rs) bounds the
requesting side (inflight requests, inflight block bytes, look-ahead bytes) and has no serving-rate
setting. An implementation of this specification MUST add one.

`MAX_BLOCKS_PER_RESPONSE` and `MAX_BS_RESPONSE_BYTES` both apply to one range response, and the
smaller one stops it. `MAX_BS_RESPONSE_BYTES` counts encoded block bodies and excludes message
discriminators and the terminal response. The count binds for small blocks. The byte total binds for
large ones: at `MAX_BLOCK_BYTES` the byte total admits about 16 blocks, so the count never engages
there.

## `Status` — Announcement, discriminator 1

- **Frame**
  - payload cap = 53 bytes
- **Decode** — [`BlockSyncMessage::decode`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116)
  - `servable_low <= servable_high`
  - `max_blocks_per_response` = 1..=128
  - `max_inflight_requests` = 1..=32,768
  - `max_response_bytes` = 1..=33,554,432
  - exact consumption
- **Relevant**
  - the range, tip hash, or a serving-limit change can affect candidate selection, pending demand,
    failover, or an open request
- **Cadence**
  - capacity = 4
  - refill = 1 message / 15 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens.

## `GetBlocks` — Request, discriminator 2

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116), [`validate_block_count`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230)
  - count = 1..=128
  - `start_height + count - 1 <= Height::MAX`
  - exact consumption
- **Work**
  - `N` = min(count, local_max_blocks_per_response)
  - response_cap = 9 bytes + `N` discriminator bytes + min(`N` * 2,000,000 bytes,
    local_max_response_bytes)
  - charge = response_cap + 64 KiB
  - capacity = `BLOCK_WORK_CAPACITY`
  - refill = `BLOCK_WORK_REFILL`
  - concurrency = local_max_inflight_requests
  - on_empty = `Delay`

The handler MUST perform at most one contiguous read. It MUST send no more than one `Block` for
each requested height. It MUST finish with exactly one `BlocksDone` or `RangeUnavailable`.

The requester MUST NOT send overlapping live ranges on one connection. A `GetBlocks` that overlaps
a live admitted range from the same peer MUST return `Disconnect`. This rule makes every `Block`
and terminal response match exactly one range despite version 2's missing request ID.

## `Block` — Response, discriminator 3

- **Frame**
  - payload cap = 2,000,001 bytes
- **Decode** — [`BlockSyncMessage::decode`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116),
  [`validate_encoded_block_len`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L253)
  - one complete block
  - exact consumption
- **Reservation** — [`BlockRangeRequest::expected_hash`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/peer_routine.rs#L1437)
  - one live `GetBlocks` range whose next unconsumed height expects this header hash
  - consumes that hash's part of the reservation
- **Verify** — [`CheckpointVerifier::check_block`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/checkpoint.rs#L651), the existing stateless block
  check. It establishes the encoding version and hash, the coinbase height, the compact target,
  and the Equihash solution, then recomputes the Merkle root. The individual rules live in
  [`block::check`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/block/check.rs).

The receiver matches a `Block` by hashing its header and comparing that hash with the committed
header hashes expected by live ranges. A block that does not match the next expected hash of exactly
one live range MUST return `Disconnect`. The publisher MUST send the blocks of a range in ascending
height order. The reservation identity commits to a header that header sync already validated, so
Verify re-checks Equihash and the target only as defense in depth. An implementation MAY skip both
checks when the header bytes hash to the expected identity. Block sync takes that option today: it
matches the hash at [`peer_routine`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/peer_routine.rs#L1437) and leaves
[`CheckpointVerifier::check_block`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/checkpoint.rs#L651) to run downstream.

## `BlocksDone` — Response, discriminator 4

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116), [`validate_block_count`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230)
  - `start_height <= Height::MAX`
  - returned = 1..=128
  - exact consumption
- **Reservation**
  - live `GetBlocks` range with this `start_height`
  - `returned` equals the number of blocks consumed from the range and does not exceed its requested
    count
  - consumes the terminal part and closes the reservation

[`validate_block_count`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230) rejects zero, so `BlocksDone` reports at least one block. A peer
that serves none of a range MUST send `RangeUnavailable` instead.

The handler MUST return every unreceived height to the work queue. A retry policy SHOULD avoid a
peer that serves no blocks for heights inside its advertised servable range.

## `RangeUnavailable` — Response, discriminator 5

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116), [`validate_block_count`](https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230)
  - `start_height <= Height::MAX`
  - count = 1..=128
  - exact consumption
- **Reservation**
  - live `GetBlocks` range with this `start_height` and requested count
  - no block has been consumed from the range
  - `count` equals the requested count
  - consumes the terminal part and closes the reservation

The handler MUST requeue the range. A retry policy MAY avoid this peer for the immediate retry.

### Block sync successor (planned)

A successor version should identify each request with a receiver-chosen nonzero request ID and name
each requested body by header hash. Every body and terminal response must echo the request ID. Those
fields would remove version 2's overlap restriction and bind each body to the header chain that the
requester selected.

This section is non-normative. The successor message set, encoding, caps, reservation rules, and
work bounds remain unspecified. Implementations MUST support only version 2 until a separate change
defines that complete wire contract.
