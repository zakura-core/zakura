# Zakura peer message regulation: specification

> **Status: first draft.** This specification defines the rules introduced by
> the [peer message regulation design](../design/peer-message-regulation.md). It covers the 14 native
> application messages in discovery, header sync, and block sync.

## Filter model

Each message has one role. The role selects the bound on the work caused by that message.

| Role | Work bound | Budget verdict |
| --- | --- | --- |
| Announcement | A cadence declared by the protocol | `Disconnect` when the cadence budget is empty |
| Request | The upper-bound cost of the response | `Delay` when the work budget is empty |
| Response | A one-shot, range, or subscription reservation created by the receiver | `Disconnect` when no matching reservation or credit exists |

Each message declaration selects concrete filters from four categories:

| Category | Filters | Purpose |
| --- | --- | --- |
| **Safe** | Frame, Decode, Verify | Bound parsing, allocation, and verification before they occur |
| **Authorized** | Reservation | Bind responses and subscription updates to receiver-created bounds |
| **Useful** | Relevant | Remove obsolete work without blaming the peer for a race |
| **Budgeted** | Cadence, Work | Bound message frequency or response work |

The admission path returns one of five results:

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot help now. |
| `Delay` | The request waits at the admission boundary for Work. |
| `Disconnect` | A conformant sender cannot produce the message. |
| `LocalFault` | The receiver accepted work and then failed to complete it. |

After `LocalFault`, the receiver MUST keep the connection open unless a caught panic leaves
connection state indeterminate. The receiver MUST NOT restore any reservation consumed by the
failed message. It MUST return the affected work to its scheduler. The implementation MUST classify
the failure by its cause without inferring sender intent.

The implementation MUST apply filters before the work that they bound. Applicable filters MUST run
in this order:

```text
frame
  -> cadence
  -> reservation precheck
  -> bounded decode
  -> reservation match
  -> stateless verification
  -> relevance
  -> work charge
  -> handler
```

The order MAY skip filters that the message declaration does not select. The Reservation filter MAY
split into a precheck and an exact match. The precheck MUST establish decode bounds before any
allocation. The exact match MUST run before Verify. A fixed-prefix read used by either step MUST NOT
allocate from a peer-declared value.

### Common requirements

1. Each message type MUST have exactly one declaration and one handler. Each handler MUST have a
   declaration. Each declaration MUST select exactly one role and one work bound. It MUST declare
   the payload cap and an allocation bound for each variable-length decoded field. An allocation
   bound MAY depend on a checked fixed prefix, a protocol limit, and a live reservation. The wire
   message, declaration, handler, and reference model MUST use one closed message-kind inventory.
   Their mappings MUST use exhaustive matches without wildcard arms.
2. Each filter MUST cap its per-peer state at a receiver-configured capacity. Peer-provided
   messages, keys, and counts MAY consume that capacity but MUST NOT increase it. The filter MUST
   define its behavior at capacity. The receiver MUST size each capacity so its aggregate across the
   maximum peer count fits the corresponding resource budget.
3. Every enforced inbound rule MUST have a matching outbound obligation. Local scheduling,
   finality, reorganization, and work reassignment MUST NOT produce a peer violation.
4. Every non-`Continue` result MUST record the service, message type, filter, direction, peer, key,
   result, and reason in `regulation.jsonl`. A full-trace mode SHOULD also record `Continue` results.
   The implementation MUST rotate the file and bound its total size.
5. Each peer MUST have an independent processing path. Admitted work from one peer MUST NOT starve
   another peer's path.
6. Each peer response path MUST bound queued unsent bytes. Reaching that bound MUST stop new
   response work for that peer without blocking another peer or service stream.

This specification bounds complete messages. Transport frame progress and stream layout remain out
of scope. The transport MUST enforce an independent deadline for an incomplete frame.

### Panic isolation

A panic in a decoder, verifier, handler, reactor port operation, or per-peer worker is a receiver
defect. The implementation MUST catch the panic at the affected peer's boundary, release reserved
resources, and return affected work to the scheduler. The process and other peers' processing paths
MUST continue. The implementation MUST report `LocalFault` and MAY close the affected connection.

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
- Each data type MUST have one verifier and one ingress call site. A message declaration MUST name
  that verifier instead of repeating its checks.
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
- A local work deadline MAY reassign the work. It MUST NOT change or remove the reservation. A
  separate protocol deadline MAY close a connection that produces no terminal response. The
  reservation MUST remain live until the exchange completes or the connection ends.
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
- A protocol deadline MAY cover required initial progress or close completion. An idle subscription
  with no push obligation MUST NOT require a terminal response.

A matched response MUST reach its handler despite work reassignment, a competing response, or a
change in local interest. The receiver MAY stop issuing requests or granting credit for future work.

## Useful filter

### Relevant

Relevant decides whether a legal message can affect a receiver decision.

- The predicate MUST use the decoded message and a cheap, bounded snapshot without taking a lock.
- The predicate MAY test whether message content can change receiver state. A changed field alone
  MUST NOT establish relevance, and the predicate MUST NOT reconsider local interest in a matched
  response.
- A false predicate MUST return `Drop`. Relevant MUST NOT return `Delay` or `Disconnect`.
- The sender MUST provide enough information to distinguish a protocol violation from a local race.
- Cadence, Reservation, or a declared drop budget MUST bound a message before Relevant returns
  `Drop`.

## Budgeted filters

### Cadence

Cadence uses one monotonic token bucket per `(peer, message_type)`.

```text
Cadence {
  capacity: messages,
  refill: messages / second,
  on_empty: Disconnect,
}
```

- Capacity MUST absorb the allowed connect-time send, jitter, and message coalescing.
- The protocol MUST state a matching sender cadence no greater than half the receiver refill rate.
- Cadence exhaustion MAY return `Disconnect` only when its configuration prevents a conformant
  sender from exhausting it.

### Work

Work bounds how much response work one peer can start. Each `(peer, request_type)` has one token
bucket and one concurrency bound. The token bucket grants response bandwidth. The concurrency bound
caps active requests. Work reserves the worst-case charge before dispatch and later refunds unused
charge.

```text
charge = upper_bound_response_bytes + REQUEST_OVERHEAD
refund = upper_bound_response_bytes - actual_response_bytes
```

- `REQUEST_OVERHEAD` MUST equal 64 KiB. The bucket capacity MUST cover the largest accepted request.
  Its refill MUST state the granted per-peer response bandwidth.
- Work MUST run after every other selected filter. Immediately before the handler starts, it MUST
  acquire one concurrency slot and apply the computed charge once. The request MUST hold the slot
  until its terminal response is queued, `LocalFault` occurs, or the connection ends. A subscription
  update releases its slot when the handler atomically commits the update. Work MUST apply the
  computed refund after the terminal response is queued. The refund MUST return unused response
  capacity without returning `REQUEST_OVERHEAD`. Work MUST release the slot and refund unused
  response capacity after `LocalFault`.
- Work MUST return `Disconnect` when a request exceeds the concurrency bound. The sender MUST stay
  within the advertised concurrency and inflight limits. Token exhaustion alone MUST NOT create a
  peer violation.
- When Work is unavailable, the peer routine MUST return `Delay` and leave the request at the
  admission boundary. It MUST stop reading that peer's ordered stream until Work becomes available.
  The existing bounded application and QUIC queues MUST provide backpressure. The implementation
  MUST NOT add a delayed-request queue or scheduler.
- A delayed request MUST hold no shared lock, stream writer, or handler permit. All buffering MUST
  fit the per-peer resource bound. The delay MUST NOT block another peer or service stream. Later
  messages on the same ordered stream MAY wait. A refund or refill MUST wake the peer routine. The
  peer routine MUST NOT rerun preceding filters or charge the request again.

## Message declarations

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
DISCOVERY_WORK_CAPACITY      = 4 MiB
DISCOVERY_WORK_REFILL        = 1 MiB/s
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
- **Relevant**
  - sequence > peer-local stored sequence
  - expiry is acceptable under local policy
- **Cadence**
  - capacity = 4
  - refill = 1 message / 7 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Hello` every 15 seconds. Verify MUST run before the discovery
book lock. Expiry and sequence checks MUST return `Drop` because clock passage and local record
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
- **Work**
  - charge = 2 bytes + limit * 648 bytes + 64 KiB
  - capacity = 4 MiB
  - refill = 1 MiB/s
  - concurrency = 1
  - on_empty = `Delay`

The handler MUST apply `wanted_services` when it samples the discovery book. It MUST sample
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
  - protocol range

A malformed record, invalid signature, wrong network, wrong chain, or incompatible protocol range
MUST disconnect the relaying peer. Expiry MUST remain a handler policy check because a record can
expire in transit. The handler MUST discard an otherwise valid record when local staleness, expiry,
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
- **Work**
  - response_cap = 42 bytes + min(requested service IDs, 8) * 294 bytes
  - an empty request uses eight requested service IDs for this calculation
  - charge = response_cap + 64 KiB
  - capacity = 4 MiB
  - refill = 1 MiB/s
  - concurrency = 1
  - on_empty = `Delay`

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
- **Relevant**
  - expiry is not in the past
  - at least one summary differs from stored live state

An empty summary list remains legal and clears the peer's live service state.

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
HS_PUSH_DEADLINE             = 30 seconds
HS_WORK_CAPACITY             = MAX_HS_PUSH_CREDIT_BYTES + HEADERS_OUTCOME_BYTES + 64 KiB
HS_WORK_REFILL               = 1 MiB/s
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
- **Relevant**
  - the advertised target or a serving-limit change can affect target selection, failover, or a
    future credit grant
- **Cadence**
  - capacity = 4
  - refill = 2 messages/s
  - on_empty = `Disconnect`

The sender MUST coalesce changes to at most one `Status` per second. `work_anchor_height` is the
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
  - `Open` requires a free publisher slot and creates send state after the Work charge
  - `Grant` and `Close` match one live subscription or the terminal tombstone
  - `update_sequence` increases by exactly one
  - `initial_target_tip_hash` and `tree_aux_schema` remain fixed
  - the acknowledged cursor and counters equal the current acknowledgement or advance to a prefix
    in the sent-cursor ring (capacity = `HS_SENT_CURSOR_RING`)
  - remaining header credit <= 4,000
  - remaining byte credit <= 8 MiB
  - terminal tombstone capacity = 1
- **Work**
  - `Open` charge = `added_byte_credit` + `HEADERS_OUTCOME_BYTES` + 64 KiB
  - `Grant` charge = `added_byte_credit` + 64 KiB
  - `Close` charge = 0 and cannot `Delay`
  - capacity = `HS_WORK_CAPACITY`
  - refill = `HS_WORK_REFILL`
  - concurrency = 1
  - on_empty = `Delay`

`Open` MUST carry `1..=13` unique locator hashes. Its acknowledged cursor MUST equal the first
locator. Its acknowledged header and byte counts MUST equal zero. It MUST add nonzero header and byte
credit. The byte credit MUST fit the fixed response fields and one entry under the selected schema.
The subscriber MUST select the target from a relevant `Status`. It MUST create the local
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
MUST return `Drop`, MUST charge no work, and MUST NOT consume the tombstone. A crossing `Close`
consumes the tombstone. An `Open` that finds no free publisher slot MUST return `Disconnect`,
because the subscriber knows its own live subscription count.

The subscriber MUST receive the terminal outcome before it sends the next `Open`. The next `Open`
clears the tombstone and MUST use a different subscription ID. This rule bounds each side to one
live or closing subscription and prevents terminal reservations from accumulating. The current
subscription work charge covers the terminal outcome, which consumes no header or byte credit.

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

A push obligation is outstanding while the subscription holds credit for at least one nonempty
page and the publisher's latest `Status` advertises a selected tip that extends the subscription
cursor. The publisher MUST queue a page or a terminal outcome within `HS_PUSH_DEADLINE` of the
obligation arising. The subscriber MAY treat the obligation as violated only after twice
`HS_PUSH_DEADLINE`.

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
  - releases unused credit and refunds its unspent response-capacity charge
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

`Busy` is not a protocol outcome. Local capacity exhaustion MUST delay the request before
admission. A local failure after admission MUST return `LocalFault`. `TargetNotSelected` reports
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
N                        = local_max_blocks_per_response
BLOCK_WORK_CAPACITY      >= 9 bytes + N + min(N * MAX_BLOCK_BYTES,
                           local_max_response_bytes) + 64 KiB
BLOCK_WORK_REFILL        = local per-peer serving rate, bytes/second
```

The receiver MUST advertise its actual block count, response body-byte limit, and inflight limit. It
MUST NOT inherit the header-sync work budget. `BLOCK_WORK_REFILL` is local policy and is not
advertised.

Block sync already regulates its rate on the requesting side. Each sender sizes its outstanding
`GetBlocks` work with a per-peer BBR window ([`DownloadWindow`][bs-window]), clamped by the inflight
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
budget. No configuration key sets this rate today: [`block_sync::config`][bs-config] bounds the
requesting side (inflight requests, inflight block bytes, look-ahead bytes) and has no serving-rate
setting. An implementation of this specification MUST add one.

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
- **Relevant**
  - the range, tip hash, or a serving-limit change can affect candidate selection, pending demand,
    failover, or an open request
- **Cadence**
  - capacity = 4
  - refill = 1 message / 15 seconds
  - on_empty = `Disconnect`

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens.

#### `GetBlocks` — Request, discriminator 2

- **Frame**
  - payload cap = 9 bytes
- **Decode** — [`BlockSyncMessage::decode`][bs-decode], [`validate_block_count`][bs-count]
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
[`CheckpointVerifier::check_block`][check-block] to run downstream.

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

The wire caps and reservation identities follow from the message encodings. The policy parameters
below remain candidate values while this specification has first-draft status:

| Parameters | Evidence required before implementation |
| --- | --- |
| Cadence capacities and refill rates | Honest-node traces with connect bursts and scheduling jitter, plus a flood test that reaches `Disconnect` within bounded work |
| `REQUEST_OVERHEAD`, Work capacities, and Work refill rates | CPU, lock, storage, and egress measurements at the maximum peer count; a liveness test for the largest legal request |
| Header credit, cursor-ring size, and `HS_PUSH_DEADLINE` | Model traces for open, grant, close, crossing updates, reorganization, and slow but conformant links |
| Incomplete-frame deadline and queued-response byte bound | Transport buffer accounting and partial-frame and non-reading-peer tests |

## Conformance tests

The implementation MUST provide these checks:

1. A declaration closure check MUST derive the current message kinds from the same closed inventory
   used by production dispatch. It MUST check that every kind has one declaration, one handler arm,
   one independent reference-model arm, legal minimum and maximum values, allocation bounds, and an
   explicit state effect. It MUST NOT use a fixed expected message count or a separately maintained
   message-kind list.
2. A cap test MUST check that every maximal legal encoding fits its declared cap and that no
   computed maximum exceeds that cap.
3. Property tests MUST generate legal messages and gate-event sequences whose sender preconditions
   hold. After every production transition, they MUST check reservation, budget, and state-size
   invariants. A conformant sequence MUST never return `Disconnect`. A deterministic coverage check
   MUST execute every current message kind, legal boundary value, and declared adversarial rule at
   least once. It MUST execute the cadence properties for every announcement, the Work and terminal
   refund properties for every request, and the reservation properties for every response. Random
   generation MUST NOT provide the only coverage for any kind, boundary, or rule.
4. Model-based reservation tests MUST generate subscription opens, grants, closes, exhausted credit,
   crossed and spontaneous terminal responses, work reassignment, finality changes, response
   reordering, duplicate responses, and connection closure. Each pushed response MUST consume the
   exact header and byte credit from one live subscription.
5. Fuzz tests MUST search arbitrary frames for decoder panics, inexact consumption, and allocation-
   bound violations. Property tests MUST check the corresponding claims over generated bounded
   payloads.
6. Honest-node regtest traces MUST contain no `Disconnect` result.
7. Load tests MUST drive one peer at the maximum conformant rate and show that CPU, memory, and
   filter state stay within their declared bounds. They MUST drive a non-conformant flood and show
   that the receiver reaches `Disconnect` within bounded work.
8. Panic-isolation tests MUST inject a panic in a decoder, in a handler, and in a reactor port
   operation. Each test MUST show that the process survives, that other peers' processing paths
   continue, that the affected work returns to the scheduler, and that the result reaches neither
   peer-set ban policy nor the peer-violation count.
9. Backpressure tests MUST exhaust one request type's Work bucket and show that its handler does not
   start. They MUST show that application and QUIC buffering stays within the declared bounds, that
   the peer routine resumes after a refund or refill, and that another peer and service stream make
   progress within the test's declared scheduling and progress bounds.
10. Bounded model exploration MUST visit every reachable state in the finite model declared by the
    [first block-sync infrastructure](../design/property-testing-block-sync-infrastructure.md). It
    MUST check reservation, Work, slot, queue, isolation, cleanup, and bounded-progress invariants.
    If a resource limit stops exploration before its frontier is empty, the check MUST report an
    incomplete result instead of an exhaustive result.

Peer-slot selection, message priority, and stream layout are outside this specification. Peer-slot
selection must remain separate because a conformant peer can waste a slot without violating a
message rule.

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
