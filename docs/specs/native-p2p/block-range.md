# Block-range exchange (`GetBlocks`)

> **Status: partially implemented.** The wire-format and serving-model layers
> are implemented. Regulated load is specified here and gains implementation
> evidence in the regulation layer of the stack.

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

| ID | Test | Requirement |
| --- | --- | --- |
| GB-WF-01 | `gb_wf_01_payload_and_frame_type_are_two` | The outer frame type and payload discriminator are both `2`. |
| GB-WF-02 | `gb_wf_02_payload_uses_canonical_nine_byte_layout` | The canonical payload is nine bytes: discriminator, little-endian start height, and little-endian count. |
| GB-WF-03 | `gb_wf_03_start_height_is_bounded` | The start height is in `0..=0x7fff_ffff`. |
| GB-WF-04 | `gb_wf_04_count_is_between_one_and_128` | The count is in `1..=128`. |
| GB-WF-05 | `gb_wf_05_decoder_rejects_trailing_bytes` | The decoder consumes the payload exactly and rejects trailing bytes. |
| GB-WF-06 | `gb_wf_06_frames_require_zero_flags` | Accepted frames have zero flags. |
| GB-WF-07 | `gb_wf_07_accepted_messages_reencode_canonically` | Every accepted request re-encodes to the same canonical payload. |
| GB-WF-08 | `gb_wf_08_maximum_start_and_count_are_safe_to_serve` | Start and count are independently valid. A request beginning at `Height::MAX` with count 128 is valid; serving safely clamps it to the representable and available prefix. |
| GB-WF-09 | `gb_wf_09_get_blocks_payload_cap_precedes_allocation` | The frame reader rejects a payload longer than nine bytes before allocation when either its outer frame type or payload discriminator identifies `GetBlocks`. |
| GB-WF-10 | `gb_wf_10_fixed_fields_do_not_size_decode_allocation` | Decoding the fixed payload performs no allocation sized from peer-provided fields. |
| GB-WF-11 | `gb_wf_11_incomplete_get_blocks_frame_expires_at_read_deadline` | Once any frame byte arrives, the transport bounds partial-frame state and expires an incomplete `GetBlocks` frame at the configured read deadline. |
| GB-WF-12 | `gb_wf_12_arbitrary_get_blocks_payloads_never_panic` | Decoding every `GetBlocks` payload from zero through nine bytes returns a result without panicking; every accepted payload re-encodes canonically. |
| GB-WF-13 | `gb_wf_13_response_discriminators_and_flags_match_wire_kinds` | `Block`, `BlocksDone`, and `RangeUnavailable` use outer and payload discriminators `3`, `4`, and `5` respectively, and every accepted response frame has zero flags. |
| GB-WF-14 | `gb_wf_14_block_payload_is_one_canonical_bounded_block` | A `Block` payload is its discriminator followed by exactly one canonical block encoding of at most `MAX_BLOCK_BYTES`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-15 | `gb_wf_15_block_payload_cap_precedes_allocation` | The frame reader rejects a `Block` payload longer than `1 + MAX_BLOCK_BYTES` before allocating its payload buffer. |
| GB-WF-16 | `gb_wf_16_blocks_done_uses_canonical_nine_byte_layout` | A `BlocksDone` payload is nine bytes: its discriminator, little-endian start height, and little-endian returned count in `1..=128`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-17 | `gb_wf_17_blocks_done_payload_cap_precedes_allocation` | The frame reader rejects a `BlocksDone` payload longer than nine bytes before allocating its payload buffer. |
| GB-WF-18 | `gb_wf_18_range_unavailable_uses_canonical_nine_byte_layout` | A `RangeUnavailable` payload is nine bytes: its discriminator, little-endian start height, and little-endian original request count in `1..=128`; decoding consumes it exactly and canonical re-encoding is stable. |
| GB-WF-19 | `gb_wf_19_range_unavailable_payload_cap_precedes_allocation` | The frame reader rejects a `RangeUnavailable` payload longer than nine bytes before allocating its payload buffer. |

Deterministic cases cover:

- minimum and maximum start heights;
- counts 1 and 128;
- count 0 and 129;
- a start above `0x7fff_ffff`;
- `Height::MAX` with count 128;
- truncated and trailing payloads;
- mismatched outer and payload discriminators;
- nonzero flags;
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

| ID | Test | Class | Requirement |
| --- | --- | --- | --- |
| GB-SM-01 | `gb_sm_01_replacement_cancels_previous_session` | Peer | A replacement connection cancels the preceding session for the same peer. |
| GB-SM-02 | `gb_sm_02_stale_disconnect_preserves_current_session` | Peer | A stale disconnect does not close or mutate the current session. |
| GB-SM-03 | `gb_sm_03_missing_status_is_rejected_as_spam` | Peer | A peer without retained valid `Status` cannot start a request; the attempt is recorded as `GetBlocksSpam`. |
| GB-SM-04 | `gb_sm_04_peer_ledgers_are_independent_and_bounded` | All | Each peer has an independent committed-request ledger bounded by the configured local in-flight cap. |
| GB-SM-05 | `gb_sm_05_saturated_ledger_rejects_without_state_query` | Peer | A request rejected by the full committed-request ledger emits no state query and receives `RangeUnavailable` echoing its original wire count while output capacity is available. |
| GB-SM-06 | `gb_sm_06_above_tip_request_is_unavailable_without_state_query` | Peer | A request starting above the servable tip emits no state query. While output capacity is available, the peer receives `RangeUnavailable` echoing its original wire count. |
| GB-SM-07 | `gb_sm_07_accepted_query_count_respects_all_bounds` | Peer | An accepted query count is clamped by the wire count, local count limit, representable heights, and available range. |
| GB-SM-08 | `gb_sm_08_request_ids_are_nonzero_and_unique` | Driver | Request identities are nonzero and are not reused during one replay. |
| GB-SM-09 | `gb_sm_09_ready_response_sends_largest_valid_prefix_and_one_terminal` | Driver | While the output path remains available, a matching ready response sends the largest contiguous prefix within the byte cap followed by exactly one appropriate terminal frame; output failure follows the regulated-load failure policy. |
| GB-SM-10 | `gb_sm_10_invalid_completion_has_no_serving_effect` | Internal | Unknown, retired, mismatched, repeated, or orphaned completion identities have no serving effect. |
| GB-SM-11 | `gb_sm_11_repeated_completion_does_not_release_live_slot` | Internal | Repeating a completed response does not release another live request slot. |
| GB-SM-12 | `gb_sm_12_ended_session_responses_do_not_reach_replacement` | Peer | Disconnecting or replacing a session orphans its queries; later results never reach the replacement. |
| GB-SM-13 | `gb_sm_13_saturated_peer_does_not_block_other_peers` | Peer | Saturating one peer does not consume another peer's request ledger. |
| GB-SM-14 | `gb_sm_14_frames_are_attributable_to_live_request_owner` | All | Every `Block` or terminal frame is attributable to the live session and request that owns it. |
| GB-SM-15 | `gb_sm_15_delayed_older_connect_cannot_replace_newer_session` | Peer | A delayed older `PeerConnected` event cannot replace a newer reactor session for the same peer. |
| GB-SM-16 | `gb_sm_16_peer_frames_wait_for_reactor_admission` | Peer | A peer routine does not process frames until the reactor admits or rejects its session. |
| GB-SM-17 | `gb_sm_17_superseded_routine_request_cannot_reach_replacement_session` | Peer | A request decoded by a superseded routine produces no state query, reply, or misbehavior record for its replacement session. |
| GB-SM-18 | `gb_sm_18_live_unavailable_completion_sends_terminal_and_releases_slot` | Driver | A matching zero-result state completion retires the request and releases its slot. While output capacity is available, it also sends `RangeUnavailable` echoing the original wire count. |
| GB-SM-19 | `gb_sm_19_inbound_sessions_serve_and_use_inbound_cap` | Peer | Inbound sessions serve `GetBlocks` through the same path and use the inbound peer cap independently of the outbound cap. |

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
3. Between 8 and 32 generated steps that search interactions among the
   requirements.

| Operation | Effect |
| --- | --- |
| `Connect` | Connect or replace a logical peer. |
| `Disconnect` | Remove its current or an older connection. |
| `Cancel` | Cancel the current peer session. |
| `Status` | Send one of the valid or inverted-range prerequisite classes defined above. |
| `GetBlocks` | Send a boundary-biased request. |
| `Complete` | Return a result for a live, completed, orphaned, unknown, or mismatched query. |

A step may issue several operations before settling only when they share a
defined FIFO order or happens-before relationship. Focused scenarios cover
every model-checkable `GB-SM` occurrence before random histories run, and
forced regressions cover task orderings the normal runner cannot reliably
place.

The runner settles each step with explicit acknowledgements from the reactor
and every live peer routine. It rejects invalid case-count or seed overrides
and prints the effective values for rerunning the unchanged generator.

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

## Peer reachability evidence

The contract records eight failure scenarios in three fix areas. Several
scenarios share one ownership or ordering fix, so there are three production
fixes rather than eight.

- **Natural** means peer traffic triggered the behavior without timing
  controls.
- **Controlled schedule** means a delay made a production-valid ordering
  repeatable.
- **Internal only** means a peer cannot create the event.

| Fix area | Scenario exercised | Reachability |
| --- | --- | --- |
| Admission ordering | Peer reads begin before reactor admission | Natural |
| Admission ordering | A valid first request is treated as spam | Controlled schedule |
| Connection lifecycle | An older connection replaces the current session | Controlled schedule |
| Connection lifecycle | A stale disconnect removes the replacement | Controlled schedule |
| Request ownership | An old response reaches a replacement session | Controlled schedule |
| Request ownership | A stale completion releases a live request slot | Controlled schedule |
| Request ownership | A forged response identity changes serving state | Internal only |
| Request ownership | A superseded routine's request reaches its replacement session | Controlled schedule |

The three production fixes are:

1. **Admission ordering:** prevent peer reads until reactor admission finishes.
2. **Connection lifecycle:** tie connect and disconnect events to the correct
   connection generation.
3. **Request ownership:** bind serving requests to the originating session and
   responses and capacity to the exact live request.

### Confirmation results

Temporary local-network probes compared complete source snapshots before and
after each fix. The probes were removed after recording the results. The
superseded-routine case remains as a forced-ordering regression because its
exact handoff must be intercepted deterministically.

| Fix area | Before-fix snapshot | Fixed snapshot | Result |
| --- | --- | --- | --- |
| Admission ordering | `5f1e12367` | `cdc769f19` | Under simultaneous peer load, `Status` preceded admission in 90 of 96 sessions before the fix and 0 of 96 after it. Reproducing the dropped-as-spam consequence required delaying the real `Connected` event. |
| Connection lifecycle | `5f1e12367` | `28b4aef9e` | Delaying real lifecycle events displaced the current session 6 of 6 times before the fix. A stronger wire replay failed 3 of 3 times before the fix. Neither failed afterward. |
| Request ownership | `38538d460` | `6d6ab411f` | Before the fix, a delayed state result produced three `Block`/`BlocksDone` sequences and no rejection. After the fix, Zakura produced one valid sequence and rejected the stale request with `RangeUnavailable`. |
| Request session ownership | `6d6ab411f` | `070247cbe` | Two real peer routines decoded requests around a replacement. Delaying the older routine's request made the pre-fix reactor query its range through the replacement; the fixed reactor ignored it and queried only the replacement's range. |

See [reachability claims](../../design/property-testing.md#reachability-claims) for
the evidence required by each label.

## Running and replaying

Run the current wire-format and serving-model evidence:

```sh
cargo test -p zakura-network --lib message_contracts -- --nocapture --test-threads=1
cargo test -p zakura-network --lib gb_wf_09 -- --nocapture
```

Run more generated serving scenarios before changing block-sync lifecycle or
serving logic:

```sh
ZAKURA_SERVING_MODEL_CASES=1000 \
  cargo test -p zakura-network --lib message_contracts::serving_model \
  -- --nocapture --test-threads=1
```

Every generated run prints its effective case count and seed. Set both values
to rerun the same cases on the same revision and generator, and preserve
important failures as focused regressions:

```sh
ZAKURA_SERVING_MODEL_CASES=1000 \
ZAKURA_SERVING_MODEL_SEED=<printed-seed> \
  cargo test -p zakura-network --lib message_contracts::serving_model \
  -- --nocapture --test-threads=1
```
