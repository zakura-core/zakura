# Zakura peer message regulation: specification

> **Status: first draft.** This specification defines the rules introduced by
> the [peer message regulation design](../design/peer-message-regulation.md). It covers the 14 native
> application messages in discovery, header sync, and block sync.

## Regulation model

### Throughput and scope

Message regulation MUST preserve high throughput. Implementations SHOULD rely on QUIC flow control,
congestion control, and the protections in this specification. Implementations MUST bound message
size, decoded allocations, active work, retained results, buffered bytes, and response authorization.
They MUST apply node-wide resource bounds as well as per-peer bounds.

This specification does not require application-level byte-rate buckets, response-byte charges,
refunds, or serving-rate refill timers. A future rate limit requires a measured resource cost that
the existing bounds cannot control. Cadence remains required for announcements and discovery
requests because those messages serve infrequent metadata exchanges.

Message prioritization and peer-slot selection remain separate work. A conformant peer can request
blocks continuously. Per-peer limits cannot prevent an attacker from creating more identities.
Connection admission MUST bound aggregate connection state. Resource admission MUST bound aggregate
active work and memory without depending on future prioritization. Prioritization can later choose
which eligible peers receive service within those bounds.

### Roles and results

| Role | Required bounds |
| --- | --- |
| Announcement | Frame and allocation bounds; protocol cadence |
| Request | Bounded execution and output capacity; message-specific limits and cadence where declared |
| Response | Frame and allocation bounds; a receiver-created one-shot, range, or subscription reservation |

The message rules below describe observable behavior. They do not require a declaration builder,
one handler function per message, or a particular filter API.

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot change accepted state. |
| `Disconnect` | The sender violated a protocol obligation. |
| `LocalFault` | The receiver failed to complete accepted work. |

Capacity waiting belongs to the receive or serving loop. It need not produce an admission verdict.
Local capacity exhaustion MUST NOT constitute a peer violation. After an ordinary local failure,
the receiver MUST release resources whose work has ended and return affected work to its scheduler.
It MUST NOT restore a consumed response reservation. It MUST keep the connection open if its
protocol state remains usable. A receiver defect MUST NOT count as a peer violation or ban reason.
Universal panic recovery is separate runtime work.

The implementation MUST apply checks before the work that they bound:

```text
frame -> cadence where declared -> reservation precheck -> bounded decode
      -> reservation match -> required stateless verification -> handler policy
```

A reservation precheck MUST establish request-selected allocation bounds before allocation.
A fixed-prefix read MUST NOT allocate from an unchecked peer-declared value. Exact reservation
matching MUST precede expensive verification. Execution and buffer capacity MUST bound decoding,
verification, and response production before each resource commitment occurs.

### Common requirements

1. Every supported message kind MUST have explicit payload and decoded-allocation bounds,
   validation, handling, and tests. Bounds MAY depend on a checked fixed prefix, a protocol limit,
   and a live reservation. Tests MUST cover all supported kinds without requiring a new common
   declaration or reference-model framework.
2. Each peer's protocol state MUST have a receiver-configured capacity and defined behavior at
   capacity. Peer-provided keys and counts MUST NOT increase that capacity. Aggregate state across
   admitted connections MUST fit node-wide resource bounds.
3. Every enforced inbound rule MUST have a matching outbound obligation. Local scheduling,
   finality, reorganization, work reassignment, and read pauses MUST NOT create a peer violation.
4. Diagnostics MUST identify protocol violations and local failures while bounding logging work
   and retained output. Implementations MAY sample or aggregate diagnostics. Complete decision
   traces MAY be enabled for tests and debugging; no particular file or trace schema is required.
5. One peer's blocked work MUST NOT hold a shared lock or writer that prevents another peer from
   progressing. Implementations MUST bound the work performed before yielding shared execution.
6. Each response path MUST bound queued unsent bytes. Reaching that bound MUST stop new response
   production for that peer. Decoded objects, storage results, application queues, and transport
   buffers MUST all fit their resource bounds.

Transport framing MUST bound incomplete-frame retention. A transport progress deadline MUST NOT
classify the receiver's intentional read pause as sender misconduct. The concrete stream layout
is separate design work, but it MUST satisfy the progress requirements under Capacity admission.

## Safe filters

### Frame

Frame validates `(stream_kind, stream_version, message_type, flags, payload_len)` without reading
the payload. A payload cap excludes the frame header, stream framing, and transport encryption
overhead.

- The Frame filter MUST require `flags == 0`.
- The Frame filter MUST require `message_type` to appear in the allowlist for the stream kind and
  version.
- The Frame filter MUST reject `payload_len` above the applicable payload cap before allocating a
  payload buffer. Each message's absolute cap MUST equal the codec's maximum encoded payload size
  for that message and network.
- The frame header MUST carry the message discriminator. A payload copy MAY exist only when the
  codec verifies that both copies match.
- A Frame failure MUST return `Disconnect`.

### Decode

Decode converts a bounded payload into one canonical message.

- Decode MUST return a result without panicking for every bounded payload and consume every valid
  payload exactly.
- Decode MUST reject trailing bytes, unknown flags, reserved bits, non-canonical values, and values
  outside their declared ranges. It MUST NOT clamp invalid values.
- Decode MUST bound every allocation before it occurs. A collection allocation MUST NOT exceed the
  smallest of its declared count, protocol limit, and
  `remaining_bytes / minimum_item_size`.
- The decoder MUST expose requested-allocation and retained decoded-state bounds to tests. A payload
  cap MUST NOT serve as an allocation bound.
- A live reservation MUST supply request-selected response bounds.
- A Decode failure MUST return `Disconnect`.

### Verify

Verify performs checks that need no chain state or mutable service state.

- Verify MUST run before the handler without I/O, locks, or shared mutable state.
- Implementations SHOULD reuse existing validators. Every ingress path MUST perform the required
  checks before the state or resource commitment they protect. This does not require one call site.
- A Verify failure MUST return `Disconnect`.
- The handler MUST perform contextual checks and return `Disconnect` for a message-caused failure.
  It MUST return `LocalFault` or retry for a failure caused by local state or capacity.

## Authorized filter

### Reservation

A reservation is the requester's local authorization for an expected response. It keeps that
response admissible even when the scheduler no longer wants the work.

- The requester MUST create the reservation before sending the request. The reservation MUST
  identify and bound the authorized response.
- Each response MUST match and consume one live reservation or one unconsumed part of a bounded
  range reservation. A missing, duplicate, or mismatched reservation MUST return `Disconnect`.
- A local work deadline MAY reassign the work. It MUST NOT change or remove the reservation. The
  requester MAY close an unproductive connection under local liveness policy without treating slow
  progress alone as a protocol violation. The reservation MUST remain live until the exchange
  completes or the connection ends.
- Reservation state MUST remain within the requester's inflight limit.

A subscription turns one request into a bounded response stream. The publisher can push follow-on
work without a new request for each response. The subscriber controls the stream with object and
byte credit. Each response spends that credit. An acknowledgement advances the accepted cursor. A
later grant renews the credit from that cursor. Closing the subscription stops future responses.
The subscriber still admits responses that it already authorized.

- The subscriber MUST add each credit grant to its local reservation before sending the update that
  carries the grant. Each response MUST consume its object and byte credit before the subscriber's
  handler starts.
- The subscriber MUST acknowledge only progress accepted by its handler. The publisher MUST retain
  bounded sent-response state. It MUST validate each update sequence and acknowledgement against
  that state before it applies added credit.
- A subscriber that stops wanting the work MUST stop granting credit. It MAY close the subscription.
  It MUST NOT revoke existing credit. After a close update, the publisher MUST stop producing new
  responses. The subscriber MUST keep its local reservation live through the terminal response.
- A subscriber MAY use local progress tracking to select another peer or close an unproductive
  connection. This specification imposes no mandatory push deadline. An idle subscription MUST NOT
  require a terminal response merely because no new header is available.

A matched response MUST reach its handler despite work reassignment, a competing response, or a
change in local interest. The receiver MAY stop issuing requests or granting credit for future work.

## Handler policy

Handlers MUST preserve sequence, expiry, empty-response, and chain-selection semantics. They MAY
discard obsolete data or suppress redundant scheduling after the applicable cadence or reservation
check. An unchanged or expired legal message MUST NOT become a protocol violation. A matched
response MUST still consume its reservation and reach its handler.

Implementations SHOULD keep cheap no-op checks where they avoid work. They need not maintain a
separate lock-free relevance snapshot. A changed metadata field alone does not require expensive
target selection or scheduling. Cadence limits frequency independently of usefulness.

## Cadence

Cadence uses a bounded monotonic message-count bucket per `(peer, message_type)` where declared.
The message rules state candidate burst capacities, refill rates, and matching sender intervals.
Cadence MUST run before expensive verification, sampling, or handler work. Stale, expired, and
unchanged messages MUST count toward cadence.

- The sender MUST obey the declared minimum interval, including across configurable refreshes.
  It MAY send one initial message where the message rules permit it.
- Receiver capacity MUST tolerate the allowed initial send, jitter, coalescing, and buffered
  arrivals after transport stalls or intentional read pauses.
- A sender rate below half the receiver refill rate does not by itself prove burst tolerance.
  Before enforcing exhaustion as `Disconnect`, the implementation MUST establish that its buffer
  and timing policy cannot reject a conformant sender. An ambiguous buffered burst MUST NOT count
  as a peer violation. Capacity admission MUST still bound its processing.
- Implementations MUST validate candidate cadence values with the tests under Parameters to
  validate. Reopening a stream MUST NOT permit unbounded resets of initial-message allowances
  within an admitted connection.

## Capacity admission

The receiver MUST start response work only when worker capacity and bounded output capacity are
available. When capacity is unavailable, it MUST stop draining the affected request stream.
Capacity release MUST resume eligible processing. Local capacity exhaustion is not a peer violation.

- Execution slots MUST bound actual running work. Protocol inflight limits bound accepted
  commitments and MAY exceed execution slots. A sender MUST obey advertised inflight limits.
  Exceeding a protocol limit is distinct from finding all local workers occupied.
- The implementation MUST bound per-peer and node-wide execution, read-ahead, retained storage
  results, decoded data, and unsent output. It MUST acquire capacity before producing data that
  consumes it. It MUST NOT drain QUIC into an unbounded application queue.
- A job MUST retain its execution slot until the underlying operation finishes. Cancellation,
  connection closure, or dropping a waiting future MUST NOT release capacity still used by an
  operation. No separate serving query-result timer is required. Finished work and discarded
  output MUST release their owned resources exactly once.
- Pausing reads MUST propagate to the QUIC receive buffer. The transport MUST stop extending stream
  credit as that buffer fills. The accounting MUST include bytes already authorized by existing
  credit. Connection credit MUST leave room for required independent streams to progress.
- A paused request reader MUST NOT trap responses or control messages needed to finish active
  work. This includes simultaneous requests in both directions on one connection. The stream
  arrangement and bounded queues MUST demonstrate this property before deployment.
- Waiting work MUST NOT hold shared locks or writers. Capacity waiting MUST NOT require artificial
  byte-rate tokens, refill timers, or a second delayed-request scheduler. A resumed message MUST
  NOT consume cadence or reservation state twice.
- Empty responses and control updates still consume CPU or storage work. Their processing MUST
  obey bounded execution and yield to other runnable work even when output backpressure is weak.

## Message rules

### Discovery — stream 4, version 2

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
```

#### `Hello` — Announcement, discriminator 1

- **Frame**
  - payload cap = 648 bytes
- **Decode** — [`validate_record_body_bounds`][record-bounds], [`validate_service_id`][service-id]
  - addresses <= 8
  - services <= 8
  - service ID length = 1..=32 ASCII bytes
  - `protocol_min <= protocol_max`
  - record body <= 580 bytes
  - exact consumption
- **Verify** — [`ZakuraNodeRecord::verify`][record-verify], with time-varying import policy split
  from [`validate_record_body_for_import`][record-import]
  - record signature
  - network and chain IDs
  - protocol overlap
  - record author == authenticated peer
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`

The sender MAY send one initial `Hello`. It MUST send later `Hello` messages at least 15 seconds
apart. A long-lived shared connection MAY carry periodic `Hello` messages, including an unchanged
signed record. A discovery-only connection can finish after one exchange; this does not impose a
lifetime limit of one or two messages on shared connections. The handler MUST apply sequence and
expiry policy. It need not maintain a separate relevance snapshot. A repeated valid record MUST
still satisfy initial-exchange progress when the receiver already knows that record.
Verify MUST run before the discovery book lock. Expiry and sequence checks MUST return `Drop`
for obsolete records because clock passage and local record
state can make an otherwise valid record obsolete. The handler MUST NOT store an address that is
not globally routable unless local policy allows that address class. A stored record's sequence
comparison ends when the stored record expires, so a peer that reset its sequence recovers after
expiry.

#### `GetPeers` — Request, discriminator 2

- **Frame**
  - payload cap = 8,470 bytes
- **Decode** — [`validate_query_fields`][query-fields]
  - limit = 1..=32
  - wanted services <= 8
  - excluded node IDs <= 256
  - service IDs are 1..=32 ASCII bytes and unique
  - excluded node IDs are sorted and unique
  - exact consumption
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`, subject to the common burst-tolerance requirement
- **Capacity**
  - one outstanding `GetPeers` per peer stream
  - bounded sampling work and queued response bytes

The sender MAY send one initial `GetPeers`. It MUST send later requests at least 15 seconds apart
and wait for the previous response before sending the next request. Configurable refresh intervals
MUST obey this minimum. The handler MUST bound sampling work independently of discovery-book size.
It MUST apply `wanted_services` when it samples the discovery book. It MUST sample
qualifying records at random. It MAY apply the excluded node IDs on a best-effort basis: exclusion
improves sampling efficiency and does not entitle the sender to enumerate the book. The handler
MUST send exactly one `Peers` response for every admitted request.

#### `Peers` — Response, discriminator 3

- **Frame**
  - absolute payload cap = 20,738 bytes
  - reservation payload cap = 2 + reserved_limit * 648 bytes
- **Decode** — [`validate_record_body_bounds`][record-bounds]
  - count = 0..=reserved_limit
  - records <= 32
  - node IDs unique
  - exact consumption
- **Reservation**
  - one outstanding `GetPeers` on this stream
  - bounds count and payload bytes
  - consumed by this message
- **Verify** — the signature and immutable import checks from
  [`ZakuraNodeRecord::verify`][record-verify] for each record, before the discovery book lock
  - record signature
  - record body bounds
  - network ID and chain ID
  - structurally valid protocol range

A malformed record, invalid signature, wrong network, or wrong chain MUST disconnect the relaying
peer. The handler MUST discard an otherwise valid record whose protocol range does not overlap
local support. A relaying peer can legitimately know nodes with different protocol compatibility.
Expiry MUST remain a handler policy check because a record can expire in transit.
The handler MUST discard an otherwise valid record when local staleness, expiry,
address, or storage policy rejects it. The handler MUST NOT store an address that is not globally
routable unless local policy allows that address class. Storage policy MUST bound the number of
stored records attributed to each source peer.

#### `GetServices` — Request, discriminator 4

- **Frame**
  - payload cap = 1,090 bytes
- **Decode** — [`validate_get_services`][get-services], [`validate_service_id`][service-id]
  - wanted services <= 32
  - service IDs are 1..=32 ASCII bytes and unique
  - exact consumption
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`, subject to the common burst-tolerance requirement
- **Capacity**
  - one outstanding `GetServices` per peer stream
  - bounded summary generation and queued response bytes

The sender MAY send one initial `GetServices`. It MUST send later requests at least 15 seconds
apart and wait for the previous response before sending the next request. Configurable refreshes
MUST obey this minimum. The receiver MUST acquire output capacity before generating summaries.

The handler MUST apply `wanted_services`. An empty list means all supported services. The handler
MUST send exactly one `Services` response for every admitted request.

#### `Services` — Response, discriminator 5

- **Frame**
  - absolute payload cap = 2,394 bytes
  - reservation payload cap = 42 + reserved_summary_count * 294 bytes
- **Decode** — [`validate_services`][services-validate]
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
- **Verify** — [`validate_summary_envelope`][summary-envelope] for the envelope, and
  [`import_connected_peer_services`][services-peer-binding] for the peer binding
  - each known envelope tag matches the service ID
  - each known summary decodes strictly
  - an unknown summary stays length-bounded and is ignored
  - `node_id` == authenticated peer

The handler MUST apply expiry and state-update policy after reservation consumption. An empty
summary list remains legal and clears the peer's live service state. Unchanged summary contents MAY
renew validity. The handler MUST NOT suppress renewal merely because service values match.

### Header sync — stream 5, version 9

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
```

The cap test pins `HEADERS_RESPONSE_FIXED_BYTES` to the codec. The frame cap already has an
implementation in [`HeaderSyncMessage::check_payload_size`][hs-payload-size].

`Status` retains the version 8 fields in the same order: work-anchor height and hash, selected-tip
height and hash, 32-byte cumulative work, oldest-retained height, maximum headers per response,
maximum subscriptions, maximum message bytes, and the auxiliary-schema mask. Removing the payload
discriminator makes its encoded size 122 bytes.

A subscription has **reached its initial target** when its receive cursor equals
`initial_target_tip_hash`. Until then the publisher serves the path to that target. After that the
publisher pushes direct descendants as its selected chain grows.

`SubscribeHeaders` replaces `GetHeaders`. A subscription binds its initial pages to one advertised
target. After those pages reach the target, the subscription authorizes direct descendants of its
accepted cursor. It allows at most the declared outstanding header and byte credit. Credit is
renewable, so the publisher can keep the link full without accepting an unbounded push.

`SubscribeHeaders` encodes an operation byte, a `u64` subscription ID, a `u32` update sequence, a
32-byte target hash, a `u32` acknowledged height, a 32-byte acknowledged hash, two `u64`
acknowledged counts, a locator-count byte, up to 13 locator hashes, two `u32` credit grants, and a
schema byte. Its maximum encoded size is 523 bytes.

`Headers` retains the version 8 response fields and order. It renames `request_id` to
`subscription_id` and `complete` to `reaches_initial_target`. The fixed fields occupy 82 bytes.
`reaches_initial_target` is true exactly on the page whose last header is the initial target and is
false on every other page. `HeadersOutcome` encodes the `u64` subscription ID, the 32-byte initial
target hash, and a one-byte outcome.

#### `Status` — Announcement, discriminator 1

- **Frame**
  - payload cap = 122 bytes
- **Decode** — [`HeaderSyncMessage::decode`][hs-decode]
  - `work_anchor_height <= selected_tip_height`
  - `oldest_retained_height <= selected_tip_height`
  - `max_headers_per_response` = 1..=`MAX_HS_RANGE`
  - `max_subscriptions` = 1
  - `max_message_bytes` = `HEADERS_RESPONSE_FIXED_BYTES + H + 4 + A` ..= 2 MiB for every advertised
    auxiliary schema
  - `tree_aux_schema_mask` contains only known bits
  - exact consumption
- **Cadence**
  - capacity = 4
  - refill = 2 messages/s
  - on_empty = `Disconnect`

The sender MUST coalesce changes to at most one `Status` per second. The handler MUST retain
bounded latest-status state and SHOULD suppress redundant target-selection work. It need not
maintain a separate relevance snapshot. `work_anchor_height` is the
height of the sender's finality anchor. `oldest_retained_height` is the lowest height for which
the sender retains headers.

#### `SubscribeHeaders` — Request, discriminator 2

- **Frame**
  - payload cap = 523 bytes
- **Decode** — [`HeaderSyncMessage::decode`][hs-decode]
  - operation is `Open`, `Grant`, or `Close`
  - `subscription_id != 0`
  - `Open` has `update_sequence = 0` and 1..=13 unique locator hashes
  - `Grant` and `Close` have `update_sequence > 0` and no locator hashes
  - `added_header_credit <= 4,000`
  - `added_byte_credit <= 8 MiB`
  - exact consumption
- **Reservation**
  - `Open` requires a free publisher slot and creates bounded send state
  - `Grant` and `Close` match one live subscription or the terminal tombstone
  - `update_sequence` increases by exactly one
  - `initial_target_tip_hash` and `tree_aux_schema` remain fixed
  - the acknowledged cursor and counters equal the current acknowledgement or advance to a prefix
    in the sent-cursor ring (capacity = `HS_SENT_CURSOR_RING`)
  - remaining header credit <= 4,000
  - remaining byte credit <= 8 MiB
  - terminal tombstone capacity = 1
- **Capacity**
  - one live or closing subscription per peer
  - bounded control processing and response production
  - `Close` processing MUST NOT wait for serving worker or output capacity

The publisher MUST process updates atomically in stream order. It MUST bound control-processing
work before yielding to other runnable work. Grant processing MUST NOT allocate an unbounded queue
of response jobs. Credit authorizes future responses; it does not allocate executing workers.
The implementation MUST preserve control progress while response output is blocked.

`Open` MUST carry `1..=13` unique locator hashes. Its acknowledged cursor MUST equal the first
locator. Its acknowledged header and byte counts MUST equal zero. It MUST add nonzero header and byte
credit. The byte credit MUST fit the fixed response fields and one entry under the selected schema.
The subscriber MUST select the target from a received `Status`. It MUST create the local
subscription reservation before it sends `Open`.

`Grant` MUST carry no locator hashes. It MUST acknowledge a cursor accepted from this subscription.
It MUST add nonzero header or byte credit. The subscriber MUST add the credit to its local
subscription reservation before it sends `Grant`. Until the subscription reaches its initial
target, the resulting header and byte credit MUST fit at least one legal nonempty page. A smaller
grant stalls the subscription: the publisher cannot legally send a page, and the subscriber waits
for one. After the subscription reaches its target, the publisher may have nothing to send. The
subscriber does not need to hold credit for a full page in that state. The subscriber SHOULD keep
header and byte credit for at least one page outstanding on a live subscription.

`Close` MUST carry no locator hashes or added credit. The publisher MUST stop producing new pages
when it receives `Close`. It MUST send `HeadersOutcome(SubscriptionClosed)` after every page already
queued unless it already queued a terminal outcome. After a `Close` matches a live subscription, a
further `Grant` on that subscription MUST return `Disconnect`.

Receiving `Close` or queueing a terminal outcome frees the publisher slot. The publisher MUST
retain a terminal tombstone until it receives a crossing `Close` or the next `Open`. The tombstone
prevents a conformant update that crossed the terminal outcome from causing a violation. A
tombstone match validates only the subscription ID. A crossing `Grant` that matches the tombstone
MUST return `Drop` and MUST NOT consume the tombstone. A crossing `Close`
consumes the tombstone. An `Open` that finds no free publisher slot MUST return `Disconnect`,
because the subscriber knows its own live subscription count.

The subscriber MUST receive the terminal outcome before it sends the next `Open`. The next `Open`
clears the tombstone and MUST use a different subscription ID. This rule bounds each side to one
live or closing subscription and prevents terminal reservations from accumulating. The terminal
outcome consumes no header or byte credit. The publisher MUST reserve bounded terminal-output
capacity so exhausted data credit cannot prevent closure.

The publisher MUST split output into frames that satisfy its advertised per-response count and byte
limits. It MAY send several frames without another `Grant` while credit remains. It MUST NOT treat
bytes sent on the ordered stream as new credit. The sent-cursor ring MUST hold at least the
maximum number of unacknowledged pages. The header credit bound limits that number to 4,000
one-header pages, so `HS_SENT_CURSOR_RING` always suffices.

#### `Headers` — Response, discriminator 3

- **Frame**
  - absolute payload cap = 2 MiB
  - reservation payload cap = min(publisher_advertised_message_bytes,
    remaining_subscription_byte_credit)
- **Decode** — [`HeaderSyncMessage::decode`][hs-decode]
  - `subscription_id != 0`
  - `header_count` <= remaining header credit
  - encoded payload bytes <= remaining byte credit
  - response schema matches the subscription
  - `header_count >= 1`
  - `reaches_initial_target` is 0 or 1
  - `body_size <= 2,000,000`
  - canonical network solution size
  - exact consumption
- **Reservation**
  - identity = (`subscription_id`, work scope, `initial_target_tip_hash`, sent locator entries,
    requested schema)
  - the first parent is a sent locator; each later parent equals the reservation receive cursor
  - consume `header_count` and the encoded payload bytes
  - advance the receive cursor
  - require `reaches_initial_target` exactly when the final header hash equals the initial target
  - record when the initial target is reached and reject a second such marker
  - remain live
- **Verify** — [`prepare_headers`][prepare-headers], the context-free header validator, over the
  decoded page. It establishes the supported encoding version, the locally computed hash, the
  inferred height, the commitment interpretation, the canonical compact target, the hash-to-target
  filter, the Equihash solution under the network proof-of-work policy, and the per-block work.
  Header sync already calls it on this path in
  [`header_sync_driver`][hs-driver]. The individual rules live in
  [`validation::context_free`][context-free].

Page linkage is a reservation rule, not a context-free one. It holds against the sent locator and
receive cursor in the local reservation, so [`prepare_headers`][prepare-headers] cannot decide it.

The first page MUST extend the locator intersection selected by the publisher. The publisher MUST
reach the initial target before it pushes a descendant beyond that target. Each later page MUST
extend the preceding page on the ordered stream. After reaching the initial target, the publisher
MAY push a new header immediately when its selected chain extends the subscription cursor and credit
remains. If its selected chain no longer extends that cursor, it MUST stop pushing and MUST queue
`HeadersOutcome(SubscriptionSuperseded)`. The subscriber MUST acknowledge only a cursor that passes
contextual difficulty, time, chain-connection, and auxiliary-root validation.

The publisher SHOULD produce an eligible page when credit, chain data, and execution/output
capacity are available. This specification imposes no mandatory push deadline. The subscriber MAY
track progress and select another peer under local policy. Slow progress alone is not a protocol
violation, and local failover MUST NOT revoke outstanding authorization.

The handler MUST verify contextual difficulty, time, chain connection, and auxiliary roots. The
transition planner performs those checks; [`prepare_headers`][prepare-headers] documents the split.
The handler MUST disconnect the peer when one of those checks fails.

#### `HeadersOutcome` — Response, discriminator 4

- **Frame**
  - payload cap = 41 bytes
- **Decode** — [`HeaderSyncMessage::decode`][hs-decode]
  - `subscription_id != 0`
  - `initial_target_tip_hash` matches the subscription
  - outcome is `TargetNotRetained`, `NoLocatorIntersection`, `TargetNotSelected`, `HistoryPruned`,
    `SubscriptionSuperseded`, or `SubscriptionClosed`
  - exact consumption
- **Reservation**
  - same identity and reservation as `Headers`
  - consumes no header or byte credit
  - releases unused subscription credit
  - closes this subscription

Each outcome is legal only in its window:

| Outcome | Legal window |
| --- | --- |
| `NoLocatorIntersection` | Before the first page |
| `TargetNotRetained` | Before the first page |
| `TargetNotSelected` | Before the first page |
| `HistoryPruned` | Before the subscription reaches the initial target |
| `SubscriptionSuperseded` | Any time |
| `SubscriptionClosed` | After the publisher receives `Close` |

An outcome outside its window MUST return `Disconnect`.

`Busy` is not a protocol outcome. Local capacity exhaustion MUST pause response-work admission
under Capacity admission. A local failure after admission MUST return `LocalFault`.
`TargetNotSelected` reports
that the publisher changed its selected chain between `Status` and `Open`. `SubscriptionSuperseded`
reports that the publisher's selected chain stopped extending the subscription cursor. Neither is
a peer violation.

### Block sync — stream 6, version 2

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

The receiver MUST advertise its actual block count, response body-byte limit, and protocol
inflight limit. The inflight limit bounds outstanding commitments; it does not specify the number
of executing storage jobs. Worker and memory capacity MUST remain bounded independently.

The requester sizes outstanding work through its existing per-peer download window
([`DownloadWindow`][bs-window]) and MUST obey the receiver's advertised inflight limit. Receiver
safety MUST NOT depend on requester adaptation. Continuous serving is legal while resources are
available. This specification adds no per-peer serving-rate setting or artificial refill delay.

A stream layout that pauses request intake MUST preserve response and control progress in both
directions. Capacity admission defines that requirement independently of the concrete layout.

`MAX_BLOCKS_PER_RESPONSE` and `MAX_BS_RESPONSE_BYTES` both apply to one range response, and the
smaller one stops it. `MAX_BS_RESPONSE_BYTES` counts encoded block bodies and excludes message
discriminators and the terminal response. The count binds for small blocks. The byte total binds for
large ones: at `MAX_BLOCK_BYTES` the byte total admits about 16 blocks, so the count never engages
there.

#### `Status` — Announcement, discriminator 1

- **Frame**
  - payload cap = 53 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode]
  - `servable_low <= servable_high`
  - `max_blocks_per_response` = 1..=128
  - `max_inflight_requests` = 1..=32,768
  - `max_response_bytes` = 1..=33,554,432
  - exact consumption
- **Cadence**
  - capacity = 4
  - refill = 1 message / 15 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens. The handler MUST retain bounded latest-status state and SHOULD suppress
redundant candidate-selection work without requiring a separate relevance snapshot.

#### `GetBlocks` — Request, discriminator 2

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode], [`validate_block_count`][bs-count]
  - count = 1..=128
  - `start_height + count - 1 <= Height::MAX`
  - exact consumption
- **Capacity**
  - outstanding commitments <= advertised inflight limit
  - bounded per-peer and node-wide storage execution, retained results, and queued output

The handler MUST bound storage work and retained results for each range. It MAY use bounded
sequential reads rather than retaining the whole response from one contiguous read. It MUST send
no more than one `Block` for each requested height. It MUST finish with exactly one `BlocksDone`
or `RangeUnavailable`.

Repeated unavailable-range requests still consume lookup work. The receiver MUST bound that work
even when responses are small and output backpressure does not engage. Continuous requests MUST
yield shared execution so another runnable peer can progress. Prioritization policy remains
separate from these resource bounds.

The requester MUST NOT send overlapping live ranges on one connection. A `GetBlocks` that overlaps
a live admitted range from the same peer MUST return `Disconnect`. This rule makes every `Block`
and terminal response match exactly one range despite version 2's missing request ID.

#### `Block` — Response, discriminator 3

- **Frame**
  - payload cap = 2,000,001 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode],
  [`validate_encoded_block_len`][bs-block-len]
  - one complete block
  - exact consumption
- **Reservation** — [`BlockRangeRequest::expected_hash`][bs-expected-hash]
  - one live `GetBlocks` range whose next unconsumed height expects this header hash
  - consumes that hash's part of the reservation
- **Verify** — [`CheckpointVerifier::check_block`][check-block], the existing stateless block
  check. It establishes the encoding version and hash, the coinbase height, the compact target,
  and the Equihash solution, then recomputes the Merkle root. The individual rules live in
  [`block::check`][block-check].

The receiver matches a `Block` by hashing its header and comparing that hash with the committed
header hashes expected by live ranges. A block that does not match the next expected hash of exactly
one live range MUST return `Disconnect`. The publisher MUST send the blocks of a range in ascending
height order. The reservation identity commits to a header that header sync already validated, so
Verify re-checks Equihash and the target only as defense in depth. An implementation MAY skip both
checks when the header bytes hash to the expected identity. Block sync takes that option today: it
matches the hash at [`peer_routine`][bs-expected-hash] and leaves
[`CheckpointVerifier::check_block`][check-block] to run downstream. Matching a known header does not
validate its supplied body. The implementation MUST retain body-commitment checks and required
downstream consensus validation before accepting
the block. It SHOULD reuse those validation paths rather than add duplicate ingress checks.

#### `BlocksDone` — Response, discriminator 4

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode], [`validate_block_count`][bs-count]
  - `start_height <= Height::MAX`
  - returned = 1..=128
  - exact consumption
- **Reservation**
  - live `GetBlocks` range with this `start_height`
  - `returned` equals the number of blocks consumed from the range and does not exceed its requested
    count
  - consumes the terminal part and closes the reservation

[`validate_block_count`][bs-count] rejects zero, so `BlocksDone` reports at least one block. A peer
that serves none of a range MUST send `RangeUnavailable` instead.

The handler MUST return every unreceived height to the work queue. A retry policy SHOULD avoid a
peer that serves no blocks for heights inside its advertised servable range.

#### `RangeUnavailable` — Response, discriminator 5

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode], [`validate_block_count`][bs-count]
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

## Parameters to validate

Wire caps and reservation identities follow from message encodings. Policy values remain candidates
while this specification has first-draft status. Implementations MUST obtain the following evidence
before enforcing those values:

| Parameters | Required evidence |
| --- | --- |
| Cadence capacities, refill rates, and discovery request intervals | Initial and periodic exchanges, summary renewal before expiry, configurable refreshes, and buffered arrivals after pauses; flood rejection without rejecting conformant bursts |
| Execution slots, inflight commitments, retained-result and output bounds | CPU, lock, storage, and memory measurements at the maximum admitted peer count; continuous serving and repeated unavailable-range requests |
| Header credit and cursor-ring size | Open, grant, close, crossing updates, reorganization, credit exhaustion, and control floods while data output is blocked |
| Incomplete-frame retention and transport windows | Partial-frame tests, intentional read pauses, non-reading peers, existing-credit accounting, and independent stream progress |

## Conformance tests

The implementation MUST provide focused checks for the following properties. It MAY use existing
unit, property, synthetic-peer, and transport test infrastructure. A new declaration framework,
universal panic-recovery suite, and exhaustive model explorer are not prerequisites.

1. Every supported message kind has legal boundary cases, canonical round trips, exact decoding,
   payload caps, and decoded-allocation checks. Deterministic cases MUST cover every kind and rule;
   random generation MUST NOT provide the only coverage.
2. Cadence checks cover announcements and discovery requests, including initial sends, periodic
   refreshes, unchanged messages, configured sender intervals, floods, and buffered bursts after
   transport stalls or local pauses. Conformant sequences MUST NOT produce `Disconnect`.
3. Reservation tests cover unsolicited, duplicate, mismatched, and reordered responses; local work
   reassignment; finality changes; completion; and connection closure. Local scheduler actions MUST
   NOT revoke response authorization.
4. Subscription tests cover open, grant, close, exhausted credit, crossing updates and terminal
   outcomes, bounded cursor history, and control processing while output is blocked. Each page
   MUST consume the exact header and byte credit of its subscription.
5. Fuzz and property tests check decoder panics, trailing-byte acceptance, and allocation-bound
   violations over bounded payloads. These checks do not require universal runtime panic recovery.
6. Capacity tests saturate workers and output buffers and verify that new response work stops.
   They check resource release, ordinary local failures, blocked storage operations, and connection
   churn. A cancelled waiter MUST NOT release a slot still used by its underlying operation.
7. Real-transport tests show that paused reads fill bounded QUIC receive buffers and stop further
   data after existing credit is exhausted. Capacity release resumes processing. Tests MUST cover
   simultaneous bidirectional serving and required control progress, including on one connection.
8. Load tests check aggregate CPU, memory, execution, and protocol-state bounds across admitted
   peers. They cover continuous useful serving and repeated empty responses. Available capacity
   MUST NOT wait for a serving-rate refill timer.
9. Discovery tests cover repeated valid `Hello` records, expiry, sequence checks, empty `Services`
   clearing live state, and unchanged summaries renewing validity. Honest regtest exchanges MUST
   produce no protocol-violation result.

Tests MUST state scheduling assumptions and finite progress bounds where they assert progress.
Optional traces MUST bound their logging and storage costs. Generated tests search for failures;
they do not prove all executions correct. The [testing design](../design/property-testing.md) and
[`GetBlocks` checks](../design/property-testing-block-sync-infrastructure.md) describe the initial
implementation scope.

## Reference implementations

Every link below pins commit
[`f892b9074002a04a678ef2365ec7658795796572`](https://github.com/zakura-core/zakura/tree/f892b9074002a04a678ef2365ec7658795796572)
on `main`.

[record-verify]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L286
[record-import]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3882
[record-bounds]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3917
[query-fields]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3939
[get-services]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3968
[services-validate]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3972
[summary-envelope]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L3985
[services-peer-binding]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L1875
[service-id]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/discovery/protocol.rs#L4042
[hs-decode]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/header_sync/wire.rs#L675
[hs-payload-size]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/header_sync/wire.rs#L999
[hs-driver]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakurad/src/commands/start/zakura/header_sync_driver.rs#L635
[prepare-headers]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-header-chain/src/validation/prepare/pipeline.rs#L85
[context-free]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-header-chain/src/validation/context_free/mod.rs
[check-block]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/checkpoint.rs#L651
[block-check]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-consensus/src/block/check.rs
[bs-decode]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L116
[bs-count]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L230
[bs-block-len]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/wire.rs#L253
[bs-expected-hash]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/peer_routine.rs#L1437
[bs-window]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/state.rs#L333
[bs-config]: https://github.com/zakura-core/zakura/blob/f892b9074002a04a678ef2365ec7658795796572/crates/zakura-network/src/zakura/block_sync/config.rs
