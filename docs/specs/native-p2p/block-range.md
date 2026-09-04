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
[native P2P contract catalog](README.md). `GB-WF` means block-range exchange
wire format, `GB-SM` means GetBlocks serving model, and `GB-RL` means GetBlocks
regulated load.

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
| GB-WF-12 | Decoding every `GetBlocks` payload from zero through nine bytes returns a result without panicking; every accepted payload re-encodes canonically. |
| GB-WF-13 | `Block`, `BlocksDone`, and `RangeUnavailable` use outer and payload discriminators `3`, `4`, and `5` respectively, and every accepted response frame has zero flags. |
| GB-WF-14 | A `Block` payload is its discriminator followed by exactly one canonical block encoding of at most `MAX_BLOCK_BYTES`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-15 | The frame reader rejects a `Block` payload longer than `1 + MAX_BLOCK_BYTES` before allocating its payload buffer. |
| GB-WF-16 | A `BlocksDone` payload is nine bytes: its discriminator, little-endian start height, and little-endian returned count in `1..=128`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-17 | The frame reader rejects a `BlocksDone` payload longer than nine bytes before allocating its payload buffer. |
| GB-WF-18 | A `RangeUnavailable` payload is nine bytes: its discriminator, little-endian start height, and little-endian original request count in `1..=128`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-19 | The frame reader rejects a `RangeUnavailable` payload longer than nine bytes before allocating its payload buffer. |

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
- an incomplete header and payload held through the read deadline; and
- arbitrary payload bytes at every length through the nine-byte cap;
- minimum, maximum, truncated, trailing, and arbitrary fixed response payloads;
  and
- a declared response frame one byte above each response kind's allocation cap.

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

This layer defines what GetBlocks regulation must protect. It deliberately does
not choose production limits or prescribe a benchmark schedule. The
implementation records those values and the measurements used to validate
them.

For request count `C`, the maximum charge is:

```text
N            = min(C, local GetBlocks count limit)
response_cap = 9 + N + min(N × MAX_BLOCK_BYTES, local response-byte limit)
charge       = response_cap + local request-overhead charge
```

The nine bytes cover the terminal response. `N` covers one discriminator byte
per `Block` payload. The remaining term covers encoded block bodies, and the
request-overhead term bounds request-processing work. The implementation must
justify its chosen value.

The state query receives the local response-body byte limit and returns only
the largest contiguous prefix whose encoded block sizes fit that limit. It must
enforce the limit while constructing the result; materializing all `N` blocks
and truncating them afterward does not satisfy this contract. Inspecting the
next candidate may temporarily materialize one additional block, bounded by
`MAX_BLOCK_BYTES`, but that block must not remain in the returned result.

Regulation applies independent peer and node rate limits, outstanding-work
limits, per-session outbound backlog limits, and per-session and node pending
request limits. These are separate from the serving ledger in GB-SM-04 because
they bound different resources. Startup validation must reject configurations
that cannot fit the largest legal request or one maximum-size block.

An admission owns its charges until it is rejected, completed, cancelled, or
its session ends. Committing binds that ownership to the originating session
and request. Queued response bytes remain owned until the transport accepts or
drops their frames. Every exit releases ownership exactly once, and a
replacement session never inherits the preceding session's work. Reconnecting
also must not provide an unbounded fresh peer-rate burst.

Invalid requests and requests without the Status prerequisite are rejected
before regulation. When a valid request cannot be admitted, retained pending
work remains bounded and no state query begins. Local congestion or failure is
not peer misbehavior. If output remains available, a committed request that
cannot finish may receive `RangeUnavailable`; otherwise its response may be
dropped while its accounting is still settled.

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
| GB-RL-08 | Handoff, action, driver, and output failures settle each attempt or permit exactly once and never blame the peer for local failure. |
| GB-RL-09 | Session end settles permits without moving them to a replacement; frame leases survive until their frames leave the application transport. |
| GB-RL-10a | Generated hostile histories vary peer count and every configured bound without exceeding peer or node accounting. |
| GB-RL-10b | A named native reading-flood workload preserves useful service for an honest peer within the declared local deadline. |
| GB-RL-10c | A named native stopped-reader workload stays within the declared application and transport envelopes, releases ownership after failure or timeout, and restores honest service. |
| GB-RL-11 | Responses to Zakura's downloads continue within the request timeout behind admission-delayed serving requests on the same stream. |
| GB-RL-12 | Supported configurations use checked arithmetic, fit the largest legal request, one maximum-size block, and one session's pending-input window, and reject insufficient limits or capacities. |
| GB-RL-13 | The frozen reference model and focused scenarios first pass against the unregulated production path. With every regulation bound made nonbinding, the regulated path then produces the same queries, frames, and ownership state without changing the model transitions or expected observations. |
| GB-RL-14 | Reconnects do not reset depleted peer-rate state, and retention of inactive peer state remains bounded. |
| GB-RL-15 | Rejecting a superseded routine at the session gate rolls back all provisional regulation ownership. |
| GB-RL-16 | Pending serving-request state stays within its per-session bound and an independently configured node-wide count; exhausting either bound drops the excess request with no work, response, or peer score. |
| GB-RL-17 | The state query receives the local response-body byte limit and never returns block bodies whose total encoded size exceeds it. |
| GB-RL-18 | A panic while holding a provisional attempt, committed permit, or frame lease releases that ownership, records no peer violation, and leaves unrelated peer admission usable. |
| GB-RL-19 | Frame and canonical-value validation plus the valid-Status prerequisite complete before any peer or node regulation ownership is acquired. A malformed request or request without retained Status leaves every regulation balance unchanged. |

The fast lane uses small capacities to reach every boundary deterministically.
The native lane uses real stream-6 frames and loopback QUIC to check that
declared limits protect an honest peer under reading floods, stopped readers,
and same-stream full-duplex traffic. The implementation PR owns the concrete
limits, workloads, deadlines, and measurements. Those experiments provide
local evidence, not network-wide performance guarantees.

## Draft response-receiving model

> **Status: draft.** GB-WF-13 through GB-WF-19 specify the response wire
> formats. The serving rules specify responses Zakura sends. This section
> preserves only the unfinished reservation and state rules for responses
> Zakura receives.

When Zakura is the requester, it must not send overlapping live ranges on one
connection. Its receiver otherwise cannot assign an incoming `Block` to
exactly one range because version 2 has no wire request ID. This outbound
scheduler obligation remains deferred until the receiving-side contract is
written.

The restriction does not make overlapping requests from a peer ambiguous to
Zakura's serving path. Each inbound request has a distinct reactor request ID,
ledger entry, and regulation permit. The first serving implementation may
therefore process two bounded overlapping peer requests independently; the
peer is responsible for matching the responses it requested. Detecting or
rejecting peer overlap is not a serving safety requirement.

### `Block` — Response, discriminator 3

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
- Preventing overlapping ranges sent by Zakura remains receiving-side work.
  Overlapping requests received from a peer stay independently owned and
  bounded by the serving ledger and regulation limits.
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

The GetBlocks properties above specialize the shared requirements in
[the regulation specification](regulation.md). The implementation's
machine-checked test manifest is the authoritative mapping from requirement IDs
to tests.

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
