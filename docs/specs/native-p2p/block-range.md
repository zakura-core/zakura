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
| GB-WF-11 | Once any frame byte arrives, the transport bounds partial-frame state and expires an incomplete `GetBlocks` frame at the configured read deadline. |

Deterministic cases cover:

- minimum and maximum start heights;
- counts 1 and 128;
- count 0 and 129;
- a start above `0x7fff_ffff`;
- `Height::MAX` with count 128;
- truncated and trailing payloads;
- mismatched outer and payload discriminators;
- nonzero flags; and
- a declared `GetBlocks` frame longer than nine bytes; and
- an incomplete header and payload held through the read deadline.

A malformed frame or payload is a protocol error and closes the affected peer
or stream according to the surrounding transport policy. A valid request for
unavailable blocks is not malformed; it follows the serving contract.

### Status prerequisite for serving

GB-SM-03 uses a narrow prerequisite from the otherwise draft block-sync
`Status` exchange. A Status becomes retained for GetBlocks serving only when:

- it decodes as the current `BlockSyncStatus` wire type;
- `servable_low` is not above `servable_high`; and
- the peer routine accepts it under the existing Status cadence or
  servable-range-growth gate.

Acceptance sets the routine's `received_status` state and publishes the range
and locally clamped serving limits. The generated model uses one valid class
whose range covers its block corpus and one invalid class whose range is
inverted. It does not claim coverage of the remaining Status policy.

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
| GB-SM-04 | All | Each peer has an independent committed-request ledger bounded by the configured local in-flight cap. |
| GB-SM-05 | Peer | A request rejected by the full committed-request ledger emits no state query and receives `RangeUnavailable` echoing its original wire count while output capacity is available. |
| GB-SM-06 | Peer | A request starting above the servable tip emits no state query and receives `RangeUnavailable` echoing its original wire count. |
| GB-SM-07 | Peer | An accepted query count is clamped by the wire count, local count limit, representable heights, and available range. |
| GB-SM-08 | Driver | Request identities are nonzero and are not reused during one replay. |
| GB-SM-09 | Driver | While the output path remains available, a matching ready response sends the largest contiguous prefix within the byte cap followed by exactly one appropriate terminal frame; output failure follows the regulated-load failure policy. |
| GB-SM-10 | Internal | Unknown, retired, mismatched, repeated, or orphaned completion identities have no serving effect. |
| GB-SM-11 | Internal | Repeating a completed response does not release another live request slot. |
| GB-SM-12 | Peer | Disconnecting or replacing a session orphans its queries; later results never reach the replacement. |
| GB-SM-13 | Peer | Saturating one peer does not consume another peer's request ledger. |
| GB-SM-14 | All | Every `Block` or terminal frame is attributable to the live session and request that owns it. |
| GB-SM-15 | Peer | A delayed older `PeerConnected` event cannot replace a newer reactor session for the same peer. |
| GB-SM-16 | Peer | A peer routine does not process frames until the reactor admits or rejects its session. |
| GB-SM-17 | Peer | A request decoded by a superseded routine produces no state query, reply, or misbehavior record for its replacement session. |
| GB-SM-18 | Driver | A matching zero-result state completion sends `RangeUnavailable` echoing the original wire count, retires the request, and releases its slot. |
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
| `Status` | Send one of the valid or inverted-range prerequisite classes defined above. |
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

The state query receives the local response-body byte limit and returns only
the largest contiguous prefix whose encoded block sizes fit that limit. It must
enforce the limit while constructing the result; materializing all `N` blocks
and truncating them afterward does not satisfy this contract. Inspecting the
next candidate may temporarily materialize one additional block, bounded by
`MAX_BLOCK_BYTES`, but that block must not remain in the returned result.

The local response-body byte limit must be at least `MAX_BLOCK_BYTES`, so every
valid block can be served by itself. This is a local configuration requirement,
not a stricter wire range for limits advertised by a remote peer.

Admission reserves the worst case. The 64 KiB request overhead remains spent
after commit. Unused response capacity is refunded. Response bytes remain
reserved until their frames are accepted by QUIC or dropped. QUIC may then
retain them under both the per-connection window and the node-wide transport
envelope below.

### Initial parameters

These are implementation candidates until native load evidence validates them:

| Bound | Initial value | Scope |
| --- | --- | --- |
| Response-body limit | 32 MiB; minimum `MAX_BLOCK_BYTES` | One state query and response |
| Peer rate | 16 MiB/s | One authenticated identity |
| Peer rate capacity | 32 MiB response cap + 128 discriminators + 9 terminal bytes + 64 KiB overhead | One authenticated identity, retained while depleted |
| Inactive identity buckets | Configured maximum connection count | Depleted peer-rate buckets without an active session or permit |
| Peer backlog | 64 MiB | One session's reserved and application-owned response bytes |
| Node rate | 64 MiB/s | All inbound `GetBlocks` serving |
| Node rate capacity | 128 MiB | All inbound `GetBlocks` serving |
| Node outstanding | 256 MiB | Admitted response bytes not yet handed to QUIC |
| Session pending inputs | Advertised in-flight limit + 1 | Decoded requests waiting before reactor processing in one session |
| Node pending inputs | 32,001 requests | Decoded requests waiting before reactor processing across live and draining sessions |
| QUIC send window | At most 32 MiB and no more than node QUIC envelope / configured connections | One connection |
| Node QUIC envelope | 512 MiB | Sum of send windows at the configured connection limit |
| Stopped-reader recovery deadline | 12 seconds: 10-second transport write timeout + 2-second scheduling slack | Honest request admission after saturation |

Startup validation requires the largest legal request to fit every applicable
byte capacity and the node pending-input capacity to fit one configured session
window. Rate balances refill with time; outstanding and backlog capacity return
only when ownership is released.

A depleted peer-rate bucket survives reconnects under its authenticated
identity. An inactive bucket remains cached until it refills, for at most
`ceil(deficit / peer rate)` seconds, unless the inactive cache reaches the
configured maximum connection count. The cache then evicts the inactive bucket
with the smallest deficit. It never evicts an active or permit-referenced
bucket. One eviction can restore at most that bucket's deficit, which is no
greater than the peer-rate capacity; the node-rate bucket still bounds
aggregate work across identities.

One admission may wait while the routine continues decoding the bidirectional
stream so responses to Zakura's own block requests can pass. Each session may
retain one admission plus its advertised in-flight count behind it. The node
has a separate configured capacity that does not grow with the connection
limit; the initial value fits one complete default session window. A request
beyond either capacity is dropped without a query, response, or peer score.
This is separate from the committed-request ledger: once admitted, a request
rejected by that full ledger follows GB-SM-05.

### Failure outcomes

| Failure point | Required outcome |
| --- | --- |
| Routine-to-reactor handoff is full | Keep the provisional attempt and wait for that channel only. |
| Handoff closes or the session ends before commit | Roll back the attempt and end that admission with no query, response, or peer score. |
| State-action channel is full or closed after commit | Retire the ledger entry and queue `RangeUnavailable` with the original wire count if output remains available, with no peer score. |
| State driver fails, times out, or returns the wrong response | Retire the ledger entry and queue `RangeUnavailable` with the original wire count if output remains available, with no peer score. |
| Output queue is full after commit | Drop the unsent response or terminal frame, settle its permit exactly once, keep the session connected, and assign no peer score. Existing frame leases remain until transport releases them. No terminal frame is required while the queue is full. |
| Output queue is closed or otherwise fails after commit | End the affected session without a peer score and settle its permit exactly once. If the session remains registered when the failure is observed, cancel it. Existing frame leases remain until transport releases them. No terminal frame is required when its output path is unavailable. |

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
| GB-RL-08 | Handoff, action, driver, and output failures follow the failure-outcome table and settle each attempt or permit exactly once. |
| GB-RL-09 | Session end settles permits without moving them to a replacement; frame leases survive until their frames leave the application transport. |
| GB-RL-10a | Generated hostile histories vary peer count and every configured bound without exceeding peer or node accounting. |
| GB-RL-10b | Fifteen reading flood peers do not push an honest tiny- or full-block response beyond the existing eight-second request timeout in the named native topology. |
| GB-RL-10c | Stopped readers remain within the application budgets and per-connection QUIC windows; the sum of configured windows fits the node QUIC envelope; the combined application and QUIC envelope is reported; writes release every lease after failure or timeout; and honest admission recovers within the 12-second deadline above. |
| GB-RL-11 | Responses to Zakura's downloads continue within the request timeout behind admission-delayed serving requests on the same stream. |
| GB-RL-12 | Supported configurations use checked arithmetic, fit the largest legal request, one maximum-size block, and one session's pending-input window, and reject insufficient limits or capacities. |
| GB-RL-13 | Under-budget histories produce the same queries, frames, and ownership state as the unregulated serving reference model. |
| GB-RL-14 | Reconnects retain a depleted identity bucket; the inactive cache follows the capacity, retention, and smallest-deficit eviction policy above; active and permit-referenced buckets survive churn; and one eviction restores no more than the evicted deficit. |
| GB-RL-15 | Rejecting a superseded routine at the session gate rolls back all provisional regulation ownership. |
| GB-RL-16 | Pending serving-request state stays within its per-session bound and an independently configured node-wide count; exhausting either bound drops the excess request with no work, response, or peer score. |
| GB-RL-17 | The state query receives the local response-body byte limit and never returns block bodies whose total encoded size exceeds it. |
| GB-RL-18 | A panic while holding a provisional attempt, committed permit, or frame lease releases that ownership, records no peer violation, and leaves unrelated peer admission usable. |

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
- **Message validity** — `CheckpointVerifier::check_block`, the existing block
  check that uses the block and fixed network rules rather than current chain state. It establishes
  the encoding version and hash, the coinbase height, the compact target, and the Equihash solution,
  then recomputes the Merkle root. The individual checks live in `block::check`.

The receiver matches a `Block` by hashing its header and comparing that hash with the committed
header hashes expected by live ranges. A block that does not match the next expected hash of exactly
one live range MUST return `Disconnect`. The publisher MUST send the blocks of a range in ascending
height order. The reservation identity commits to a header that header sync already validated, so
message validation re-checks Equihash and the target only as defense in depth. An implementation
MAY skip both checks when the header bytes hash to the expected identity. Block sync takes that
option today: it matches the hash at `peer_routine` and leaves
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
  - `count` equals the original wire-request count; local state-query and
    serving clamps do not change this echoed value
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

- The full block-sync `Status` contract, including cadence policy, remains
  separate. This contract defines only the prerequisite used by GetBlocks
  serving.
- Overlapping outbound range reservations remain receiving-side work.
- Exceeding the serving ledger is rejected rather than treated as a
  disconnect-worthy peer violation.
- Universal fair-admission latency requires an explicit fair scheduler. The
  initial native case gates on the existing request timeout and reports
  observed admission order.
- Versioned scenario replay is deferred. Seeds reproduce cases only on the
  same revision and generator, so important failures must become focused
  regressions. Before claiming replay across generator or backend changes, add
  schema-versioned scenarios, direct replay, and repeat-run comparison.
- A successor stream version may add a wire request ID and block hashes to make
  response ownership explicit.

## Implementation evidence

### Shared regulation coverage

The serving exchange maps the shared regulation requirements as follows. A
mapping names the message-specific evidence that must exist before this layer
can be marked implemented.

| Shared ID | GetBlocks evidence |
| --- | --- |
| P2P-RG-01 | The catalog plus GB-WF-01 through GB-WF-11, GB-SM-09, and GB-RL-01 close the serving request and its response kinds. |
| P2P-RG-02 | GB-WF-01 through GB-WF-06, GB-SM-03, GB-SM-05, GB-SM-06, GB-RL-02, and GB-RL-08 cover declared outcomes and sender obligations. |
| P2P-RG-03 | GB-WF-01 through GB-WF-10, GB-SM-03, and GB-RL-15 enforce the processing order. |
| P2P-RG-04 | GB-SM-03, GB-SM-06, GB-SM-10, GB-SM-12, GB-SM-17, and GB-SM-18 distinguish invalid, stale, and unavailable work. |
| P2P-RG-05 | GB-WF-01, GB-WF-02, GB-WF-09, and GB-WF-10 cover allocation caps. |
| P2P-RG-06 | GB-WF-11 covers partial-frame state and the read deadline. |
| P2P-RG-07 | GB-WF-01 through GB-WF-08 and GB-WF-10 cover total and canonical decoding. |
| P2P-RG-08 | GB-RL-01, GB-RL-12, and GB-RL-17 cover checked charges and bounded state results. |
| P2P-RG-09 | GB-SM-04, GB-SM-13, GB-RL-05 through GB-RL-07, GB-RL-10a through GB-RL-10c, GB-RL-12, and GB-RL-16 cover peer and node bounds. |
| P2P-RG-10 | GB-SM-08 through GB-SM-12, GB-SM-14, GB-SM-17, GB-SM-18, GB-RL-03, GB-RL-04, GB-RL-08, GB-RL-09, and GB-RL-15 cover ownership and settlement. |
| P2P-RG-11 | GB-RL-06, GB-RL-08 through GB-RL-10c, and GB-RL-12 cover application and transport buffering. |
| P2P-RG-12 | GB-RL-02, GB-RL-08, GB-RL-11, and GB-RL-16 cover waiting, pending input, and overload. |
| P2P-RG-13 | Not applicable to serving responses. The receiving direction remains draft below. |
| P2P-RG-14 | Not applicable because `GetBlocks` is a request, not an announcement. |
| P2P-RG-15 | GB-RL-05 and GB-RL-14 cover session- and identity-owned state. |
| P2P-RG-16 | GB-RL-08, the native GB-RL-10 cases, and GB-RL-18 cover local faults, panic cleanup, isolation, and bounded evidence. |

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
