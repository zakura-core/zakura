# Block-range exchange (`GetBlocks`)

> **Status: implemented.** Every wire-format, serving-model, and regulated-load
> requirement below names passing evidence.

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

| ID | Test | Class | Requirement |
| --- | --- | --- | --- |
| GB-SM-01 | `gb_sm_01_replacement_cancels_previous_session` | Peer | A replacement connection cancels the preceding session for the same peer. |
| GB-SM-02 | `gb_sm_02_stale_disconnect_preserves_current_session` | Peer | A stale disconnect does not close or mutate the current session. |
| GB-SM-03 | `gb_sm_03_missing_status_is_rejected_as_spam` | Peer | A peer without retained valid `Status` cannot start a request; the attempt is recorded as `GetBlocksSpam`. |
| GB-SM-04 | `gb_sm_04_peer_ledgers_are_independent_and_bounded` | All | Each peer has an independent committed-request ledger bounded by the configured local in-flight cap. |
| GB-SM-05 | `gb_sm_05_saturated_ledger_rejects_without_state_query` | Peer | A request rejected by the full committed-request ledger emits no state query and receives `RangeUnavailable` echoing its original wire count while output capacity is available. |
| GB-SM-06 | `gb_sm_06_above_tip_request_is_unavailable_without_state_query` | Peer | A request starting above the servable tip emits no state query and receives `RangeUnavailable` echoing its original wire count. |
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
| GB-SM-18 | `gb_sm_18_live_unavailable_completion_sends_terminal_and_releases_slot` | Driver | A matching zero-result state completion sends `RangeUnavailable` echoing the original wire count, retires the request, and releases its slot. |
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

### Default parameters

The native load evidence below validates these implementation defaults in the
named local topology:

| Bound | Default | Scope |
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

| ID | Test | Requirement |
| --- | --- | --- |
| GB-RL-01 | `gb_rl_01_charge_matches_declared_formula` | The admission charge matches the declared formula for generated request counts and local limits. |
| GB-RL-02 | `gb_rl_02_blocked_request_bounds_queue_and_is_admitted_once_after_release` | A blocked request emits no work or frame, pending requests stay within the declared queue bound, excess is handled as specified, and each queued request is admitted at most once after capacity returns. |
| GB-RL-03 | `gb_rl_03_attempt_rolls_back_and_commit_keeps_overhead` | A pre-commit drop refunds everything; post-commit settlement keeps overhead and refunds only unused response capacity. |
| GB-RL-04 | `gb_rl_04_rejections_settle_once_and_account_their_terminal_frame` | Every rejection settles once and accounts for its terminal frame only when that frame queues. |
| GB-RL-05 | `gb_rl_05_peer_rate_backlog_and_ledger_are_isolated` | One peer cannot consume another peer's rate bucket, backlog, or request ledger. |
| GB-RL-06 | `gb_rl_06_backlog_never_overshoots_and_draining_resumes_work` | Reserved and application-owned unwritten response bytes never exceed the peer backlog; draining resumes admission. |
| GB-RL-07 | `gb_rl_07_stalled_outstanding_bytes_do_not_refill_with_time` | Time refills rate tokens but never outstanding-byte capacity. |
| GB-RL-08 | `gb_rl_08_handoff_failures_hold_rollback_or_settle_exactly_once`<br>`gb_rl_08_output_queue_full_drops_but_closed_ends_session` | Handoff, action, driver, and output failures follow the failure-outcome table and settle each attempt or permit exactly once. |
| GB-RL-09 | `gb_rl_09_session_end_settles_permit_but_frame_leases_survive_until_drop` | Session end settles permits without moving them to a replacement; frame leases survive until their frames leave the application transport. |
| GB-RL-10a | `gb_rl_10a_generated_hostile_flood_stays_within_all_declared_bounds` | Generated hostile histories vary peer count and every configured bound without exceeding peer or node accounting. |
| GB-RL-10b | `gb_rl_10b_native_reading_flood_preserves_honest_tiny_and_full_service` | Fifteen reading flood peers do not push an honest tiny- or full-block response beyond the existing eight-second request timeout in the named native topology. |
| GB-RL-10c | `gb_rl_10c_native_stopped_readers_stay_bounded_reclaim_and_restore_service`<br>`gb_rl_10c_quic_send_windows_fit_node_transport_envelope` | Stopped readers remain within the application budgets and per-connection QUIC windows; the sum of configured windows fits the node QUIC envelope; the combined application and QUIC envelope is reported; writes release every lease after failure or timeout; and honest admission recovers within the 12-second deadline above. |
| GB-RL-11 | `gb_rl_11_pipelined_serving_requests_keep_same_stream_download_live` | Responses to Zakura's downloads continue within the request timeout behind admission-delayed serving requests on the same stream. |
| GB-RL-12 | `gb_rl_12_supported_configuration_covers_largest_request` | Supported configurations use checked arithmetic, fit the largest legal request, one maximum-size block, and one session's pending-input window, and reject insufficient limits or capacities. |
| GB-RL-13 | `gb_rl_13_under_budget_histories_match_pre_regulation_reference_model` | Under-budget histories produce the same queries, frames, and ownership state as the unregulated serving reference model. |
| GB-RL-14 | `gb_rl_14_reconnect_retains_rate_bucket_and_bounds_inactive_cache` | Reconnects retain a depleted identity bucket; the inactive cache follows the capacity, retention, and smallest-deficit eviction policy above; active and permit-referenced buckets survive churn; and one eviction restores no more than the evicted deficit. |
| GB-RL-15 | `gb_rl_15_stale_session_gate_rolls_back_regulation_ownership` | Rejecting a superseded routine at the session gate rolls back all provisional regulation ownership. |
| GB-RL-16 | `gb_rl_16_pending_requests_stay_within_session_and_node_bounds` | Pending serving-request state stays within its per-session bound and an independently configured node-wide count; exhausting either bound drops the excess request with no work, response, or peer score. |
| GB-RL-17 | `gb_rl_17_state_query_receives_local_response_byte_limit`<br>`gb_rl_17_state_query_result_never_exceeds_response_byte_limit` | The state query receives the local response-body byte limit and never returns block bodies whose total encoded size exceeds it. |
| GB-RL-18 | `gb_rl_18_panics_release_owned_resources_and_preserve_other_peers` | A panic while holding a provisional attempt, committed permit, or frame lease releases that ownership, records no peer violation, and leaves unrelated peer admission usable. |

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

The manifest maps `GB-RL-01` through `GB-RL-18` to one or more test names and
checks those names against Rust's registered test inventory. A missing ID or
test fails explicitly.

The current local measurements support the defaults in this topology; they are
not portable performance promises:

- Across two optimized runs with fifteen reading flood peers, the honest tiny
  response completed within 11 ms and the honest full response within 5.14 s,
  both below 8 s.
- Nine stopped readers held 264,781,497 application bytes, below 256 MiB, and
  honest service resumed in 5.86 s.
- In the same-stream case, three downloaded blocks progressed in about 45 ms
  behind two admission-delayed requests, which later served 60,135,213 payload
  bytes.

After a lease ends, QUIC may still own unacknowledged bytes. Zakura caps this
with a 512 MiB node send-window envelope divided across configured connections,
up to 32 MiB each. The default 256-connection limit therefore uses 2 MiB per
connection, enough for one maximum-size block frame and therefore for the
default one-block response. A static test checks both facts and the aggregate
bound. The stopped-reader lane records UDP traffic and RSS as diagnostics
rather than treating either as QUIC memory occupancy. Operators that increase
`max_blocks_per_response` should validate WAN throughput with their connection
limit; larger multi-block responses can require additional flow-control turns.

Cancelling a partially written ordered stream uses a dedicated QUIC reset code
so an upgraded peer closes only that stream. Older peers treat the reset as a
connection error, so during a mixed-version rollout they may reconnect instead
of preserving sibling streams; message framing remains compatible.

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

Run the fast contract. The native pressure tests are registered but ignored:

```sh
cargo test -p zakura-network --lib gb_ -- --nocapture --test-threads=1
```

Run only the fast regulated-load contract:

```sh
cargo test -p zakura-network --lib gb_rl_ -- --nocapture --test-threads=1
```

Complete the contract by running the fast command above, then the three native
QUIC pressure cases serially in an optimized build:

```sh
cargo test --release -p zakura-network --lib gb_rl_ \
  -- --ignored --nocapture --test-threads=1
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

For `GB-RL-10a`, every run prints the effective case count, seed, and exact
replay environment. Increase or reproduce it with:

```sh
ZAKURA_REGULATED_LOAD_CASES=1000 \
ZAKURA_REGULATED_LOAD_SEED=<printed-seed> \
  cargo test -p zakura-network --lib gb_rl_10a_ \
  -- --nocapture --test-threads=1
```

Invalid, non-UTF-8, or zero overrides fail instead of silently using defaults.
