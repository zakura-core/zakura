# Block-range exchange (`GetBlocks`)

> **Status: specified.** Implementation PRs must add evidence before changing
> any layer to implemented.

This contract covers the stream-6 block-range serving exchange initiated by
`GetBlocks`. It specifies the request wire format, the server's state and
lifecycle behavior, and regulation for the response work it causes.

`Block`, `BlocksDone`, and `RangeUnavailable` are specified here as responses
Zakura sends. Their earlier standalone wire and receiving-side proposal is
preserved as a draft below. Block-sync `Status` has a separate draft.

The contract follows the
[native P2P contract catalog](README.md). `GB-WF` means
GetBlocks wire format, `GB-SM` means GetBlocks serving model, and `GB-RL` means
GetBlocks regulated load.

## Production path

Wire-format properties start at the production frame reader or codec. Serving
properties use this path:

1. A peer connects and sends `Status` and `GetBlocks` on framed stream 6.
2. The real block-sync service, peer routine, and reactor handle the request.
3. The reactor emits `QueryBlocksByHeightRange` to the state driver.
4. A controlled driver returns a valid or deliberately invalid result.
5. The test observes response frames, request ownership, and session state.

The test controls peer inputs, connection ordering, and the driver result. It
does not replace production framing, decoding, service dispatch, peer routines,
the reactor, or request identity allocation.

## Wire format contract

| ID | Requirement |
| --- | --- |
| GB-WF-01 | The outer frame type and payload discriminator are both `2`. |
| GB-WF-02 | The canonical payload is nine bytes: discriminator, little-endian start height, and little-endian count. |
| GB-WF-03 | The start height is in `0..=0x7fff_ffff`. |
| GB-WF-04 | The count is in `1..=128`. |
| GB-WF-05 | The decoder consumes the payload exactly and rejects trailing bytes. |
| GB-WF-06 | Accepted frames have zero flags. |
| GB-WF-07 | Every accepted request re-encodes to the same canonical payload. |
| GB-WF-08 | Start and count are independently valid. A request beginning at `Height::MAX` with count 128 is valid; serving safely clamps it to the representable and available prefix. |
| GB-WF-09 | The frame reader rejects a `GetBlocks` payload longer than nine bytes before allocating its payload buffer. |
| GB-WF-10 | Decoding the fixed payload performs no allocation sized from peer-provided fields. |

Deterministic cases cover:

- minimum and maximum start heights;
- counts 1 and 128;
- count 0 and 129;
- a start above `0x7fff_ffff`;
- `Height::MAX` with count 128;
- truncated and trailing payloads;
- mismatched outer and payload discriminators;
- nonzero flags; and
- a declared `GetBlocks` frame longer than nine bytes.

A malformed frame or payload is a protocol error and closes the affected peer
or stream according to the surrounding transport policy. A valid request for
unavailable blocks is not malformed; it follows the serving contract.

## Serving model contract

Input classes identify who can create each event:

- **Peer:** real frames and connection lifecycle changes.
- **Driver:** state results returned through the production action interface.
- **Internal:** forged or unreachable completions used to test fail-safe
  behavior.
- **All:** invariants checked after each settled step.

| ID | Class | Requirement |
| --- | --- | --- |
| GB-SM-01 | Peer | A replacement connection cancels the preceding session for the same peer. |
| GB-SM-02 | Peer | A stale disconnect does not close or mutate the current session. |
| GB-SM-03 | Peer | A peer without retained valid `Status` cannot start a request; the attempt is recorded as `GetBlocksSpam`. |
| GB-SM-04 | All | Each peer has an independent request ledger bounded by the configured local in-flight cap. |
| GB-SM-05 | Peer | A cap-rejected request emits no state query and receives `RangeUnavailable` while output capacity is available. |
| GB-SM-06 | Peer | A request starting above the servable tip emits no state query and receives `RangeUnavailable`. |
| GB-SM-07 | Peer | An accepted query count is clamped by the wire count, local count limit, representable heights, and available range. |
| GB-SM-08 | Driver | Request identities are nonzero and are not reused during one replay. |
| GB-SM-09 | Driver | A matching ready response sends the largest contiguous prefix within the byte cap followed by exactly one appropriate terminal frame. |
| GB-SM-10 | Internal | Unknown, retired, mismatched, repeated, or orphaned completion identities have no serving effect. |
| GB-SM-11 | Internal | Repeating a completed response does not release another live request slot. |
| GB-SM-12 | Peer | Disconnecting or replacing a session orphans its queries; later results never reach the replacement. |
| GB-SM-13 | Peer | Saturating one peer does not consume another peer's request ledger. |
| GB-SM-14 | All | Every `Block` or terminal frame is attributable to the live session and request that owns it. |
| GB-SM-15 | Peer | A delayed older `PeerConnected` event cannot replace a newer reactor session for the same peer. |
| GB-SM-16 | Peer | A peer routine does not process frames until the reactor admits or rejects its session. |
| GB-SM-17 | Peer | A request decoded by a superseded routine produces no state query, reply, or misbehavior record for its replacement session. |
| GB-SM-18 | Driver | A matching zero-result state completion sends `RangeUnavailable`, retires the request, and releases its slot. |
| GB-SM-19 | Peer | Inbound sessions serve `GetBlocks` through the same path and use the inbound peer cap independently of the outbound cap. |

Serving `Status` survives an overlapping replacement for the same authenticated
peer, but not a fully settled disconnect. Changing that policy requires a
contract change.

### Generated scenarios

A generated scenario is created by the property test, not recorded from a live
network. The test applies the same scenario to an independent reference model
and the production path, then compares their observations after each step.

Each case varies the block corpus, connection direction, peer limit, in-flight
limit, request size, and response-byte limit. It contains:

1. A successful exchange proving the full path works.
2. One focused boundary or lifecycle scenario.
3. Generated steps that search interactions among the requirements.

| Operation | Effect |
| --- | --- |
| `Connect` | Connect or replace a logical peer. |
| `Disconnect` | Remove its current or an older connection. |
| `Cancel` | Cancel the current peer session. |
| `Status` | Send a valid or invalid status frame. |
| `GetBlocks` | Send a boundary-biased request. |
| `Complete` | Return a result for a live, completed, orphaned, unknown, or mismatched query. |

A step may issue several operations before settling only when they share a
defined FIFO order or happens-before relationship. Normal settled steps use an
explicit production barrier. Fixed sleeps or yield counts are not a settlement
contract.

Focused scenarios cover every model-checkable `GB-SM` occurrence before random
histories run. Invariants report their successful comparison count. Forced
regressions cover task orderings the normal runner cannot reliably place.

## Regulated load contract

The request owns admission and response accounting for the exchange. For
request count `C`:

```text
N            = min(C, local GetBlocks count limit)
response_cap = 9 + N + min(N × MAX_BLOCK_BYTES, local response-byte limit)
charge       = response_cap + 64 KiB
```

The nine bytes cover the terminal response. `N` covers one discriminator byte
per `Block` payload. The remaining term covers encoded block bodies.

Admission reserves the worst case. The 64 KiB request overhead remains spent
after commit. Unused response capacity is refunded. Response bytes remain
reserved until their frames are accepted by QUIC or dropped; QUIC's
unacknowledged send window is bounded separately.

### Initial parameters

These are implementation candidates until native load evidence validates them:

| Bound | Initial value | Scope |
| --- | --- | --- |
| Peer rate | 16 MiB/s | One authenticated identity |
| Peer rate capacity | 32 MiB response cap + 128 discriminators + 9 terminal bytes + 64 KiB overhead | One authenticated identity, retained while depleted |
| Peer backlog | 64 MiB | One session's reserved and application-owned response bytes |
| Node rate | 64 MiB/s | All inbound `GetBlocks` serving |
| Node rate capacity | 128 MiB | All inbound `GetBlocks` serving |
| Node outstanding | 256 MiB | Admitted response bytes not yet handed to QUIC |

Startup validation requires the largest legal request to fit every applicable
capacity. Rate balances refill with time; outstanding and backlog capacity
return only when ownership is released.

One admission may wait while the routine continues decoding the bidirectional
stream so responses to Zakura's own block requests can pass. Later serving
requests are bounded by the advertised in-flight limit. Requests beyond that
bound are dropped without a query, response, or peer score. The implementation
must also account for the aggregate pending-request memory implied by that
limit across the maximum connection count.

### Requirements

| ID | Requirement |
| --- | --- |
| GB-RL-01 | The admission charge matches the declared formula for generated request counts and local limits. |
| GB-RL-02 | A blocked request emits no work or frame, pending requests stay within the declared queue bound, excess is handled as specified, and each queued request is admitted at most once after capacity returns. |
| GB-RL-03 | A pre-commit drop refunds everything; post-commit settlement keeps overhead and refunds only unused response capacity. |
| GB-RL-04 | Every rejection settles once and accounts for its terminal frame only when that frame queues. |
| GB-RL-05 | One peer cannot consume another peer's rate bucket, backlog, or request ledger. |
| GB-RL-06 | Reserved and application-owned unwritten response bytes never exceed the peer backlog; draining resumes admission. |
| GB-RL-07 | Time refills rate tokens but never outstanding-byte capacity. |
| GB-RL-08 | A full handoff channel retains its attempt; closure rolls it back; action, driver, and output failures settle through the declared outcome. |
| GB-RL-09 | Session end settles permits without moving them to a replacement; frame leases survive until their frames leave the application transport. |
| GB-RL-10a | Generated hostile histories vary peer count and every configured bound without exceeding peer or node accounting. |
| GB-RL-10b | Fifteen reading flood peers do not push an honest tiny- or full-block response beyond the existing eight-second request timeout in the named native topology. |
| GB-RL-10c | Stopped readers remain within application and QUIC envelopes, their writes release all leases after failure or timeout, and honest service recovers within the write timeout plus stated slack. |
| GB-RL-11 | Responses to Zakura's downloads continue within the request timeout behind admission-delayed serving requests on the same stream. |
| GB-RL-12 | Supported configurations use checked arithmetic, fit the largest legal request, and reject insufficient capacities. |
| GB-RL-13 | Under-budget histories produce the same queries, frames, and ownership state as the unregulated serving reference model. |
| GB-RL-14 | Reconnects retain a depleted identity bucket; inactive retention is bounded and early eviction restores no more than the evicted deficit. |
| GB-RL-15 | Rejecting a superseded routine at the session gate rolls back all provisional regulation ownership. |
| GB-RL-16 | Pending serving-request state stays within its per-session bound and its derived aggregate bound at the configured maximum connection count. |

The fast lane uses small capacities to reach every boundary deterministically.
The native lane uses real stream-6 frames, the production peer routine and
reactor, a controlled state driver, the ordered transport worker, and loopback
QUIC.

The first native topology uses:

- fifteen reading flood peers plus one honest peer for GB-RL-10b;
- nine stopped readers plus one honest peer for GB-RL-10c; and
- three stopped readers plus one full-duplex peer for GB-RL-11.

These are reproducible experiments, not network-wide proofs. CPU, RSS, UDP
traffic, and throughput are diagnostics. The request timeout, write timeout,
application budgets, and configured QUIC envelope are contract gates.

## Draft response-receiving contract

> **Status: draft.** The serving rules above cover responses Zakura sends.
> This section preserves the original proposal for responses Zakura receives.
> It is not part of the currently specified GetBlocks layers. Stable response
> wire and receiving-side IDs still need to be assigned.

The requester must not send overlapping live ranges on one connection. The
original proposal treated an overlapping request as a protocol violation so
each response could match one range despite version 2 having no wire request
ID. That policy remains deferred until the receiving-side contract is written.

### `Block` — Response, discriminator 3

- **Frame**
  - payload cap = 2,000,001 bytes
- **Decode** — `BlockSyncMessage::decode`,
  `validate_encoded_block_len`
  - one complete block
  - exact consumption
- **Reservation** — `BlockRangeRequest::expected_hash`
  - one live `GetBlocks` range whose next unconsumed height expects this header hash
  - consumes that hash's part of the reservation
- **Verify** — `CheckpointVerifier::check_block`, the existing stateless block
  check. It establishes the encoding version and hash, the coinbase height, the compact target,
  and the Equihash solution, then recomputes the Merkle root. The individual rules live in
  `block::check`.

The receiver matches a `Block` by hashing its header and comparing that hash with the committed
header hashes expected by live ranges. A block that does not match the next expected hash of exactly
one live range MUST return `Disconnect`. The publisher MUST send the blocks of a range in ascending
height order. The reservation identity commits to a header that header sync already validated, so
Verify re-checks Equihash and the target only as defense in depth. An implementation MAY skip both
checks when the header bytes hash to the expected identity. Block sync takes that option today: it
matches the hash at `peer_routine` and leaves
`CheckpointVerifier::check_block` to run downstream.

### `BlocksDone` — Response, discriminator 4

- **Frame**
  - payload cap = 9 bytes
- **Decode** — `BlockSyncMessage::decode`, `validate_block_count`
  - `start_height <= Height::MAX`
  - returned = 1..=128
  - exact consumption
- **Reservation**
  - live `GetBlocks` range with this `start_height`
  - `returned` equals the number of blocks consumed from the range and does not exceed its requested
    count
  - consumes the terminal part and closes the reservation

`validate_block_count` rejects zero, so `BlocksDone` reports at least one block. A peer
that serves none of a range MUST send `RangeUnavailable` instead.

The handler MUST return every unreceived height to the work queue. A retry policy SHOULD avoid a
peer that serves no blocks for heights inside its advertised servable range.

### `RangeUnavailable` — Response, discriminator 5

- **Frame**
  - payload cap = 9 bytes
- **Decode** — `BlockSyncMessage::decode`, `validate_block_count`
  - `start_height <= Height::MAX`
  - count = 1..=128
  - exact consumption
- **Reservation**
  - live `GetBlocks` range with this `start_height` and requested count
  - no block has been consumed from the range
  - `count` equals the requested count
  - consumes the terminal part and closes the reservation

The handler MUST requeue the range. A retry policy MAY avoid this peer for the immediate retry.

### Successor stream version (planned)

A successor version should identify each request with a receiver-chosen nonzero request ID and name
each requested body by header hash. Every body and terminal response must echo the request ID. Those
fields would remove version 2's overlap restriction and bind each body to the header chain that the
requester selected.

This section is non-normative. The successor message set, encoding, caps, reservation rules, and
work bounds remain unspecified. Implementations MUST support only version 2 until a separate change
defines that complete wire contract.

## Deferred behavior

The first implementation deliberately leaves these policies separate:

- Block-sync `Status` cadence has its own future contract.
- Overlapping outbound range reservations remain receiving-side work.
- Exceeding the serving ledger is rejected rather than treated as a
  disconnect-worthy peer violation.
- Universal fair-admission latency requires an explicit fair scheduler. The
  initial native case gates on the existing request timeout and reports
  observed admission order.
- A successor stream version may add a wire request ID and block hashes to make
  response ownership explicit.

## Implementation evidence

The implementation PR for each layer must add:

- the ID-named Rust test for every requirement;
- a machine-checked ID-to-test manifest;
- run and replay commands;
- generated case and successful comparison counts;
- focused regressions for forced schedules;
- sensitivity results for historical defects and observation channels; and
- peer-reachability or native-load evidence for operational claims.

Until that evidence exists, the catalog must continue to show the layer as
**Specified**, not **Implemented**.
