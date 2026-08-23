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
| **Useful** | Unique, Relevant | Remove repeated or obsolete work without blaming the peer for a race |
| **Budgeted** | Cadence, Work | Bound message frequency or response work |

The admission path returns one of five results:

| Result | Meaning |
| --- | --- |
| `Continue` | The handler may process the message. |
| `Drop` | The message is legal but cannot help now. |
| `Delay` | The request is legal, but its work budget is empty. |
| `Disconnect` | A conformant sender cannot produce the message. |
| `LocalFault` | The receiver accepted work and then failed to complete it. |

The implementation MUST apply filters before the work that they bound. Applicable filters MUST run
in this order:

```text
frame
  -> cadence
  -> reservation precheck
  -> bounded decode
  -> reservation match
  -> stateless verification
  -> uniqueness and relevance
  -> work charge
  -> handler
```

The order MAY skip filters that the message declaration does not select. The Reservation filter MAY
split into a precheck and an exact match. The precheck MUST establish decode bounds before any
allocation. The exact match MUST run before Verify. A fixed-prefix read used by either step MUST NOT
allocate from a peer-declared value.

### Requirements common to every filter

1. Every message type MUST have one declaration and one handler. Every handler MUST have one
   declaration.
2. Every declaration MUST select exactly one role and one work bound.
3. Every filter MUST hold O(1) state per peer. The receiver MUST choose every state bound.
4. A filter MUST NOT grow a map or queue from peer-chosen keys or counts.
5. Every enforced inbound rule MUST have a matching outbound obligation.
6. An outbound cadence MUST NOT exceed half of the cadence enforced inbound.
7. Local scheduling, finality, reorganization, and work reassignment MUST NOT produce a peer
   violation.
8. `Delay` MUST stall only the responsible peer and message class. It MUST NOT block an inbound
   response on the same connection.
9. Every non-`Continue` result MUST emit the service, message type, filter, direction, peer, key,
   result, and reason to `regulation.jsonl`. A full-trace mode SHOULD emit `Continue` results.
   The implementation MUST rotate `regulation.jsonl` and bound its total size.
10. Each peer MUST have an independent processing path. Admitted work from one peer MUST NOT
    starve another peer's path.
11. The receiver MUST size every per-peer bound so that the bound multiplied by the maximum peer
    count fits its resource budget.

In this specification, a peer is one authenticated connection. Filter state and budgets end when
the connection ends. Reconnect churn, dial policy, and bans after a `Disconnect` are peer-set
policy. Peer-set policy MUST receive every `Disconnect` verdict.

`LocalFault` keeps the connection open and does not blame the peer. A caught panic is the one
exception, described under Panic isolation. The reservation part consumed
by the failed message stays consumed. The receiver MUST return the affected work to its scheduler.
`LocalFault` classifies the cause of the failure, not the sender's intent. The implementation MUST
NOT infer intent. If a sender deliberately uses a conformant message to expose a receiver defect,
the implementation MUST still return `LocalFault`. The implementation MUST classify a
message-caused protocol or contextual failure as a peer violation even when the sender acts
accidentally.

This specification bounds complete messages. The transport MUST bound the time a frame may remain
incomplete. The stream layout specification states that bound.

### Panic isolation

A panic inside a decoder, verifier, handler, or reactor port operation is a receiver defect, not
peer behavior. The implementation MUST catch it at a boundary. It MUST NOT let a panic end the
process or another peer's processing path.

- Peer-controlled decoding MUST run inside an unwind boundary. "Decode MUST be total" states the
  requirement; this boundary bounds the damage when an implementation fails to meet it.
- Each reactor port operation and each spawned per-peer worker MUST run inside an unwind boundary.
  The boundary MUST return the affected work to the scheduler and MUST release every resource the
  operation reserved.
- A caught panic MUST NOT reach peer-set ban policy, and MUST NOT count as a peer violation.
- A caught panic MAY close the connection whose boundary caught it. A panic leaves parser, session,
  and handler state indeterminate, so continuing on that connection is unsafe. The close is a local
  fault, not a `Disconnect` verdict.
- Every boundary MUST increment a metric that names the boundary.

## Safe filters

### Frame

The Frame filter takes `(stream_kind, stream_version, message_type, flags, payload_len)` and does not
read the payload.

Every cap in this specification bounds `payload_len` alone. A cap excludes the frame header, the
stream framing, and the transport's encryption overhead, so it states the maximum encoded body of
one message. A message whose body is a single 4-byte height therefore has a 4-byte payload cap.

- The Frame filter MUST require `flags == 0`.
- The Frame filter MUST require `message_type` to appear in the allowlist for the stream kind and
  version.
- The Frame filter MUST reject `payload_len` above the message cap before allocating a payload
  buffer.
- Each message cap MUST equal the maximum encoded payload size for that message and network.
- A test MUST compare each declared cap with the codec's computed maximum.
- The frame header MUST carry the message discriminator. A payload copy MAY exist only when the
  codec verifies that both copies match.
- A Frame failure MUST return `Disconnect`.

### Decode

The Decode filter converts a bounded frame into one canonical message.

- Decode MUST be total and MUST consume the payload exactly.
- Decode MUST reject trailing bytes, unknown flags, reserved bits, non-canonical values, and values
  outside their declared ranges.
- Decode MUST reject an invalid field. It MUST NOT clamp that field into range.
- Decode MUST bound every allocation before it occurs.
- A collection allocation MUST NOT exceed the smallest of its declared count, its protocol limit,
  and `remaining_bytes / minimum_item_size`.
- A live reservation MUST supply every response bound chosen by the request.
- A Decode failure MUST return `Disconnect`.

### Verify

The Verify filter performs every check that does not need chain state or mutable service state.

- Verify MUST perform no I/O.
- Verify MUST hold no lock and access no shared mutable state.
- Each data type MUST have one verifier and one ingress call site.
- Verify MUST run before the handler and before any shared-state lock.
- A Verify failure MUST return `Disconnect`.
- A contextual check MUST run in the handler. A contextual failure caused by the message MUST
  return `Disconnect`.
- A handler failure caused by local state or local capacity MUST return `LocalFault` or retry
  internally. It MUST NOT return `Disconnect`.

## Authorized filter

### Reservation

A reservation represents one protocol exchange. It is not the scheduler's current interest in the
work.

- The sender MUST create a reservation before sending a request.
- The reservation MUST record the peer, response kind, correlation identity, decode bounds, and
  expected object identity. Where an identity names a work scope, it means the scope recorded when
  the reservation was created.
- Each response MUST match and consume exactly one live reservation or one unconsumed part of a
  bounded range reservation.
- A missing, duplicate, or mismatched reservation MUST return `Disconnect`.
- A reservation MUST remain live until the complete response arrives or the connection ends.
- A local work deadline MAY reassign work. It MUST NOT remove or change the reservation.
- A protocol deadline MAY close a connection when the peer produces no terminal response. The
  protocol deadline MUST be separate from every work deadline.
- Reservation state MUST NOT exceed the inflight request limit chosen by the sender.

A subscription is a reservation with renewable credit.

- The subscriber MUST create or add credit to its reservation before sending the corresponding
  subscription message.
- The subscription MUST record its initial target, schema, receive cursor, remaining object credit,
  remaining byte credit, and update sequence.
- The first pushed response MUST extend one initial locator. Each later response MUST extend the
  receive cursor. Every response MUST consume its object count and encoded bytes before the handler
  starts.
- The publisher MUST keep sent cursors in a bounded ring until the subscriber acknowledges them.
- The subscriber MUST acknowledge only a cursor that its handler accepted.
- The publisher MUST add credit only after it validates the acknowledged cursor and update sequence.
- A close update MUST stop new responses. The reservation MUST remain live until every response
  already sent and the terminal response arrives.
- A local decision MUST NOT revoke outstanding credit. It MUST stop new credit and MAY close the
  subscription.
- A protocol deadline MAY cover the initial catch-up or a requested close. It MUST NOT require a
  terminal response while a live subscription waits for a new direct descendant and no push
  obligation is outstanding.

A response that matches a live reservation is relevant. The receiver asked for it, spent bandwidth
on it, and cannot re-derive it for free. Work reassignment, a competing request for the same object,
or another peer answering first MUST NOT drop a matched response. A receiver that stops wanting the
work stops issuing requests and stops granting credit. It does not discard what it already ordered.

## Useful filters

### Unique

The Unique filter stores recent keys in a fixed-size per-peer ring.

- A declaration MUST state the key, ring capacity, window, and repeat result.
- A key MUST identify the work. It MUST NOT use only a peer-chosen request identifier.
- An announcement repeat MUST return `Drop`.
- A request repeat MAY return `Disconnect` only when the first request was fulfilled and the key
  pins an immutable answer for the entire window.
- A request whose answer can change MUST omit this filter or return `Drop`.
- The sender MUST NOT repeat a fulfilled request within the declared window.

### Relevant

The Relevant filter decides whether a legal message can affect a receiver decision.

- The predicate MUST use the decoded message and a cheap, bounded snapshot.
- The predicate MUST take no lock.
- A false predicate MUST return `Drop`.
- The Relevant filter MUST NOT return `Delay` or `Disconnect`.
- The predicate MUST NOT test whether the receiver still wants the work. A response that matches a
  live reservation is wanted by construction. The predicate MAY test whether the message content can
  change receiver state.
- The sender MUST include enough information for the receiver to distinguish a peer violation from
  a local race.
- A difference from the last accepted message MUST NOT establish relevance unless the changed field
  can affect a receiver decision.
- A publisher MUST send a pushed payload only under a live subscription. The first payload MUST
  extend an initial locator. Each later payload MUST extend the subscribed cursor. Every payload
  MUST consume subscriber-issued credit.
- A subscriber MUST grant credit only for a selected branch and accepted base.
- A subscriber that stops wanting a branch MUST stop granting credit and MAY send `Close`. Those are
  its only levers. Credit it already granted stays valid.
- A peer MAY send bounded announcement metadata without a subscription only when a strict Cadence
  rule applies. It MUST NOT send headers, blocks, or other application objects under that exception.

A conformant pushed payload can arrive after the subscriber stops wanting the branch. Another peer,
a reorganization, or finality can cause this race. The payload MUST consume outstanding credit and
MUST reach its handler. It MUST NOT return `Disconnect`.

A dropped announcement has already consumed a Cadence token. A dropped response has already
consumed reservation capacity. A declaration that has neither bound MUST declare a separate drop
budget.

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

- Cadence MUST run before Decode or Verify when either operation has material cost.
- Capacity MUST absorb the allowed connect-time send, jitter, and message coalescing.
- The protocol MUST state the matching sender cadence.
- The sender cadence MUST NOT exceed half of the receiver refill rate.
- Cadence exhaustion MAY return `Disconnect` only when a conformant sender cannot exhaust it.

### Work

Work uses one token bucket and one concurrency bound per `(peer, request_type)`.

```text
charge = upper_bound_response_bytes + REQUEST_OVERHEAD
refund = charge - actual_response_bytes
```

- `REQUEST_OVERHEAD` MUST equal 64 KiB.
- Work MUST charge the upper bound before the handler starts.
- Work SHOULD refund unused response bytes after the terminal response is queued.
- A close operation MAY charge zero only when it adds no work and the existing reservation charge
  covers its terminal response.
- Work exhaustion MUST return `Delay(deficit / refill)`.
- Work MUST NOT discard an accepted request.
- Work MUST refund the unused charge when the handler returns `LocalFault`.
- The bucket capacity MUST be at least the largest request that the receiver accepts.
- The refill MUST state the per-peer response bandwidth that the receiver grants.
- The concurrency bound MUST NOT exceed the inflight limit advertised by the receiver.
- A request that exceeds the advertised concurrency bound MUST return `Disconnect`.

#### Delayed request isolation

For Work, a message class is one `request_type`. Each `(peer, request_type)` MUST own one FIFO
request lane. The request lane MUST have a fixed capacity equal to that request type's advertised
concurrency bound. A request occupies one concurrency slot after it passes every filter before Work.
It MUST keep that slot until it reaches the completion point declared for that request, its handler
returns `LocalFault`, or the connection ends. A one-shot or range request completes when its terminal
response is queued. A subscription update completes when the handler atomically commits the update
to bounded send state. Work charge and refund accounting MAY outlive the concurrency slot.

The Work filter MUST apply these steps in order:

1. If the request lane has no free concurrency slot, return `Disconnect`.
2. Compute the request's full charge and acquire one concurrency slot.
3. If the bucket has enough tokens and no earlier request waits in the request lane, subtract the
   charge and dispatch the request to its handler.
4. Otherwise, append the request to the request lane and return `Delay`.

A zero-charge operation declared as unable to `Delay` MUST bypass delayed charged requests. It MUST
NOT consume a Work concurrency slot. It MUST use separately bounded control state when it changes
protocol state that an earlier delayed request reserved or snapshotted.

The implementation MUST retain each delayed request as a bounded decoded message plus the bounded
admission state needed by its handler. It MUST NOT repeat Frame, Decode, Verify, or another charged
filter when the request becomes eligible. It MUST reserve or snapshot mutable protocol state checked
before Work so that a later local state change cannot turn the delayed request into a peer violation.

For a delayed request, `deficit` is the request's charge plus the charges of every earlier delayed
request in its request lane, minus the bucket's available tokens. The implementation MUST clamp a
negative deficit to zero. `Delay(deficit / refill)` is the request's eligibility estimate when no
earlier request receives a refund. A token refund MUST prompt the request lane to recompute the head
request's eligibility. The request lane MUST dispatch eligible requests in FIFO order and subtract
each charge exactly once before its handler starts.

The request lane MUST arm a monotonic timer for the head request's eligibility estimate. The timer
and every Work refund MUST wake the request lane. A timer wait MUST NOT hold a shared lock, a
transport reader, a stream writer, or a handler execution permit. The request lane MUST recompute
the bucket and the head request's deficit after every wake-up.

The transport reader MUST continue reading complete frames while a request waits in a request lane.
It MUST dispatch an inbound response without waiting for a delayed request on the same connection or
ordered stream. It MUST also dispatch every other message class through its own processing path. The
implementation MUST NOT implement `Delay` by sleeping in the transport reader, by stopping reads
from the ordered stream, or by filling a transport ingress queue. QUIC flow-control backpressure is
not the Work delay mechanism.

The request lane is application state in addition to the transport's receive buffers and ingress
queues. Its capacity and maximum retained bytes MUST satisfy the per-peer resource requirements in
this specification. When the connection ends, the implementation MUST discard its delayed requests
and release their concurrency slots. A delayed request has not consumed Work tokens, so connection
closure refunds no Work tokens for that request.

The refund and refill regenerate admission tokens, not delivery. Three rules bound the outbound
queue behind them:

- The receiver MUST bound the unsent response bytes queued per peer.
- When a peer reaches its unsent-byte bound, response production MUST block only that peer's
  processing path.
- A drain deadline MAY disconnect a peer that stops reading its responses.

The sender-side obligations for Work:

- The sender MUST size its protocol deadlines using the receiver's advertised refill, concurrency,
  and inflight limits.
- The sender SHOULD NOT hold outstanding requests whose combined charge exceeds what the advertised
  refill sustains within those deadlines.

## Message declarations

All byte caps below cover payload bytes. The transport frame header is not part of the cap. Only
the filters listed in a declaration apply to that message.

### Discovery — stream 4, version 2

Discovery MUST carry discriminators `1..=5` in the frame header. It MUST remove the payload
discriminator used by version 1. The following limits apply to every discovery message:

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

```text
Frame        payload cap = 648 bytes
Decode       addresses <= 8; services <= 8; service ID length = 1..=32 ASCII bytes;
             protocol_min <= protocol_max; record body <= 580 bytes; exact consumption
Verify       record signature; network and chain IDs; protocol overlap;
             record author == authenticated peer
Relevant     sequence > peer-local stored sequence and expiry is acceptable under local policy
Cadence      capacity = 4; refill = 1 message / 7 seconds; on_empty = Disconnect
```

The sender MUST send at most one `Hello` every 15 seconds. Verify MUST run before the discovery
book lock. The handler MUST NOT store an address that is not globally routable unless local policy
allows that address class. A stored record's sequence comparison ends when the stored record
expires, so a peer that reset its sequence recovers after expiry.

#### `GetPeers` — Request, discriminator 2

```text
Frame        payload cap = 8,470 bytes
Decode       limit = 1..=32; wanted services <= 8; excluded node IDs <= 256;
             service IDs are 1..=32 ASCII bytes and unique;
             excluded node IDs are sorted and unique
Work         charge = 2 bytes + limit * 648 bytes + 64 KiB; capacity = 4 MiB;
             refill = 1 MiB/s; concurrency = 1; on_empty = Delay
```

The handler MUST apply `wanted_services` when it samples the discovery book. It MUST sample
qualifying records at random. It MAY apply the excluded node IDs on a best-effort basis: exclusion
improves sampling efficiency and does not entitle the sender to enumerate the book. The handler
MUST send exactly one `Peers` response for every admitted request.

#### `Peers` — Response, discriminator 3

```text
Frame        absolute payload cap = 20,738 bytes; reservation payload cap = 2 + reserved_limit * 648 bytes
Decode       count = 0..=reserved_limit; records <= 32; node IDs unique; exact consumption
Verify       verify each record signature, bounds, network ID, chain ID, and protocol range before
             taking the discovery book lock
Reservation  one outstanding GetPeers on this stream; bounds count and payload bytes;
             consumed by this message
```

A malformed record, invalid signature, wrong network, wrong chain, or incompatible protocol range
MUST disconnect the relaying peer. The handler MUST discard an otherwise valid record when local
staleness, expiry, address, or storage policy rejects it. The handler MUST NOT store an address
that is not globally routable unless local policy allows that address class. Storage policy MUST
bound the number of stored records attributed to each source peer.

#### `GetServices` — Request, discriminator 4

```text
Frame        payload cap = 1,090 bytes
Decode       wanted services <= 32; service IDs are 1..=32 ASCII bytes and unique;
             exact consumption
Work         response_cap = 42 bytes + min(matching local services, 8) * 294 bytes;
             charge = response_cap + 64 KiB; capacity = 4 MiB;
             refill = 1 MiB/s; concurrency = 1; on_empty = Delay
```

The handler MUST apply `wanted_services`. An empty list means all supported services. The handler
MUST send exactly one `Services` response for every admitted request.

#### `Services` — Response, discriminator 5

```text
Frame        absolute payload cap = 2,394 bytes;
             reservation payload cap = 42 + reserved_summary_count * 294 bytes
Decode       summaries <= 8; service ID length = 1..=32 ASCII bytes;
             summaries contain only reserved service IDs; summary length <= 256 bytes;
             service IDs unique; exact consumption
Verify       envelope tag matches the service ID; each known summary decodes strictly;
             node_id == authenticated peer
Reservation  one outstanding GetServices on this stream; supplies allowed service IDs,
             summary count, and payload cap; an empty request reserves all service IDs and
             eight summaries; consumed by this message
Relevant     expiry is not in the past and at least one summary differs from stored live state
```

An empty summary list remains legal and clears the peer's live service state.

### Header sync — stream 5, version 9

Header sync version 9 MUST allow discriminators `1..=4`. Let `H` equal 1,487 bytes on Mainnet and Testnet and
177 bytes on Regtest. Let `A` equal 156 bytes when the request selects tree auxiliary schema V1 and
zero otherwise. The following subscription limits apply:

```text
MAX_HS_PUSH_CREDIT_HEADERS   = 4,000
MAX_HS_PUSH_CREDIT_BYTES     = 8 MiB
MAX_HS_SUBSCRIPTIONS         = 1 per peer
MAX_HS_RANGE                 = 4,000 headers per response
HEADERS_RESPONSE_FIXED_BYTES = encoded size of a Headers response with zero entries
HS_SENT_CURSOR_RING          = 4,096 sent cursors per subscription
HS_PUSH_DEADLINE             = 30 seconds
HS_WORK_CAPACITY             = MAX_HS_PUSH_CREDIT_BYTES + 64 KiB
HS_WORK_REFILL               = 1 MiB/s
```

The cap test pins `HEADERS_RESPONSE_FIXED_BYTES` to the codec.

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

#### `Status` — Announcement, discriminator 1

```text
Frame        payload cap = 123 bytes
Decode       work_anchor_height <= selected_tip_height; oldest_retained_height <= selected_tip_height;
             max_headers_per_response = 1..=MAX_HS_RANGE; max_subscriptions = 1;
             max_message_bytes = HEADERS_RESPONSE_FIXED_BYTES + H + 4 ..= 2 MiB;
             tree_aux_schema_mask contains only known bits; exact consumption
Relevant     the advertised target or a serving-limit change can affect target selection,
             failover, or a future credit grant
Cadence      capacity = 4; refill = 2 messages/s; on_empty = Disconnect
```

The sender MUST coalesce changes to at most one `Status` per second. `work_anchor_height` is the
height of the sender's finality anchor. `oldest_retained_height` is the lowest height for which
the sender retains headers.

#### `SubscribeHeaders` — Request, discriminator 2

```text
Frame        payload cap = 523 bytes
Decode       operation is Open, Grant, or Close; subscription_id != 0;
             Open has update_sequence = 0 and 1..=13 unique locator hashes;
             Grant and Close have update_sequence > 0 and no locator hashes;
             added_header_credit <= 4,000; added_byte_credit <= 8 MiB; exact consumption
Reservation Open requires a free publisher slot and creates send state after the Work charge;
             Grant and Close match one live subscription or the terminal tombstone;
             update_sequence increases by exactly one; initial_target_tip_hash and tree_aux_schema
             remain fixed; acknowledged cursor and counters equal the current acknowledgement or
             advance to a prefix in the sent-cursor ring (capacity = HS_SENT_CURSOR_RING);
             remaining header credit <= 4,000; remaining byte credit <= 8 MiB;
             terminal tombstone capacity = 1
Work         Open or Grant charge = added_byte_credit + 64 KiB;
             Close charge = 0 and cannot Delay; capacity = HS_WORK_CAPACITY;
             refill = HS_WORK_REFILL; concurrency = 1; on_empty = Delay
```

`Open` MUST carry `1..=13` unique locator hashes. Its acknowledged cursor MUST equal the first
locator. Its acknowledged header and byte counts MUST equal zero. It MUST add nonzero header and byte
credit. The byte credit MUST fit the fixed response fields and one entry under the selected schema.
The subscriber MUST select the target from a relevant `Status`. It MUST create the inbound
subscription reservation before it sends `Open`.

`Grant` MUST carry no locator hashes. It MUST acknowledge a cursor accepted from this subscription.
It MUST add nonzero header or byte credit. The subscriber MUST add the credit to its inbound
reservation before it sends `Grant`. Until the subscription reaches its initial target, the
resulting header and byte credit MUST fit at least one legal nonempty page. A smaller grant stalls
the subscription: the publisher cannot legally send a page, and the subscriber waits for one. After
the subscription reaches its target the publisher may have nothing to send, so the subscriber need
not hold credit for a full page. The subscriber SHOULD keep header and byte
credit for at least one page outstanding on a live subscription.

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

The next `Open` clears the tombstone. It MUST use a subscription ID that differs from the retained
tombstone's ID, so a crossing update never matches two subscriptions. The subscriber MUST retain
the old reservation until its terminal outcome arrives. It MAY open a new subscription while it
waits for that outcome. The current subscription work charge covers the terminal outcome, which
consumes no header or byte credit.

The publisher MUST split output into frames that satisfy its advertised per-response count and byte
limits. It MAY send several frames without another `Grant` while credit remains. It MUST NOT treat
bytes sent on the ordered stream as new credit. The sent-cursor ring MUST hold at least the
maximum number of unacknowledged pages. The header credit bound limits that number to 4,000
one-header pages, so `HS_SENT_CURSOR_RING` always suffices.

#### `Headers` — Response, discriminator 3

```text
Frame        absolute payload cap = computed_max_encoded_size(Headers, network) <= 2 MiB;
             reservation payload cap = min(publisher_advertised_message_bytes,
             remaining_subscription_byte_credit)
Decode       subscription_id != 0; header_count <= remaining header credit;
             encoded payload bytes <= remaining byte credit; response schema matches subscription;
             header_count >= 1; body_size <= 2,000,000; canonical network solution size;
             exact consumption
Verify       valid Equihash; hash <= target(nBits); nBits well-formed; page linkage;
             first parent is a sent locator; each later parent equals the reservation receive cursor;
             auxiliary height alignment and activation defaults
Reservation  identity = (subscription_id, work scope, initial_target_tip_hash, sent locator entries,
             requested schema); consume header_count and encoded payload bytes; advance the receive
             cursor; record when the initial target is reached; remain live
```

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
`HS_PUSH_DEADLINE`. This rule makes a quiet subscription either honestly idle or a violation: a
publisher cannot advertise work and then withhold it.

The handler MUST verify contextual difficulty, time, chain connection, and auxiliary roots. The
handler MUST disconnect the peer when one of those checks fails.

#### `HeadersOutcome` — Response, discriminator 4

```text
Frame        payload cap = 42 bytes
Decode       subscription_id != 0; outcome is TargetNotRetained, NoLocatorIntersection,
             TargetNotSelected, HistoryPruned, SubscriptionSuperseded, or SubscriptionClosed;
             exact consumption
Reservation  same identity and reservation as Headers; consumes no header or byte credit;
             closes this subscription
```

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

Version 2 requests bodies by height range. That suits the checkpoint range, where the heights are
contiguous and no validated header identity exists yet. Above the checkpoint a height does not name
a block: competing branches occupy the same height, and a height range cannot say which branch the
requester wants. Version 3 addresses that. Version 2 keeps heights.

Block sync MUST allow discriminators `1..=5` in the frame header. It MUST remove the duplicate
payload discriminator. The following limits apply:

```text
MAX_BLOCKS_PER_RESPONSE  = 128
MAX_BLOCK_BYTES          = 2,000,000 bytes
MAX_BS_RESPONSE_BYTES    = 33,554,432 bytes
MAX_BS_INFLIGHT_REQUESTS = 32,768
BLOCK_WORK_CAPACITY      >= 4 bytes + min(local_max_blocks_per_response * MAX_BLOCK_BYTES,
                           local_max_response_bytes) + 64 KiB
BLOCK_WORK_REFILL        = local per-peer serving rate, bytes/second
```

The receiver MUST advertise its actual block count, response byte, inflight, and work-refill
limits. It MUST NOT inherit the header-sync work budget.

Block sync already regulates its rate on the requesting side. Each sender sizes its outstanding
`GetBlocks` work with a per-peer BBR window, clamped by the inflight limit the receiver advertises
and operating at the measured bandwidth-delay product, which is normally far below that clamp. That
window is the outbound obligation matching this inbound rule.

The two sides meet through `Delay`. A receiver whose Work bucket is empty delays the request instead
of answering it. The delay lengthens the sender's round-trip samples, the sender's delay gradient
shrinks its window, and the sender settles below the rate the receiver serves. Neither side needs to
advertise a rate.

`BLOCK_WORK_REFILL` therefore binds only a sender that ignores its own controller. The receiver MUST
set the rate from local policy. It MUST NOT derive the rate from a peer-supplied or peer-influenced
measurement, because a peer able to move that measurement would set its own budget. The receiver
MUST size the rate so that the rate multiplied by the maximum peer count fits its serving egress
budget.

`MAX_BLOCKS_PER_RESPONSE` and `MAX_BS_RESPONSE_BYTES` both apply to one range response, and the
smaller one stops it. The count binds for small blocks. The byte total binds for large ones: at
`MAX_BLOCK_BYTES` the byte total admits about 16 blocks, so the count never engages there.

#### `Status` — Announcement, discriminator 1

```text
Frame        payload cap = 20 bytes
Decode       servable_low <= servable_high; max_blocks_per_response = 1..=128;
             max_inflight_requests = 1..=32,768;
             max_response_bytes = 1..=33,554,432; exact consumption
Relevant     the range or a serving-limit change can affect candidate selection, pending demand,
             failover, or an open request
Cadence      capacity = 4; refill = 1 message / 15 seconds; on_empty = Disconnect
```

The sender MUST send at most one `Status` every 30 seconds. It MAY send one immediate `Status` when
the connection opens.

#### `GetBlocks` — Request, discriminator 2

```text
Frame        payload cap = 8 bytes
Decode       count = 1..=128; start_height + count - 1 <= Height::MAX; exact consumption
Work         response_cap = 4 bytes + min(min(count, local_max_blocks_per_response) * 2,000,000 bytes,
             local_max_response_bytes);
             charge = response_cap + 64 KiB;
             capacity = BLOCK_WORK_CAPACITY; refill = BLOCK_WORK_REFILL;
             concurrency = local_max_inflight_requests; on_empty = Delay
```

The handler MUST perform at most one contiguous read. It MUST send no more than one `Block` for
each requested height. It MUST finish with exactly one `BlocksDone` or `RangeUnavailable`.

The sender MUST NOT hold two live ranges with the same `start_height` on one connection. A
`GetBlocks` whose `start_height` equals a live admitted range from the same peer MUST return
`Disconnect`, so every terminal response matches exactly one range.

#### `Block` — Response, discriminator 3

```text
Frame        payload cap = 2,000,000 bytes
Decode       one complete block; exact consumption
Verify       block parses; at least one transaction; coinbase is first and carries a height;
             merkle root recomputes; valid Equihash;
             hash <= target(nBits)
Reservation  one live GetBlocks range expecting this header hash; consumes that hash's part of
             the reservation
Unique       key = expected header hash; scope = reservation; capacity = reserved count;
             window = reservation lifetime; on_repeat = Drop
```

The receiver matches a `Block` by hashing its header and comparing that hash with the committed
header hashes expected by live ranges. A block whose hash matches no live expectation MUST return
`Disconnect`. The publisher MUST send the blocks of a range in ascending height order. The
reservation identity commits to a header that header sync already validated, so Verify re-checks
Equihash and the target only as defense in depth; an implementation MAY skip both checks when the
header bytes hash to the expected identity.

The handler MUST perform contextual block validation before commit. A message-caused consensus
failure MUST disconnect the peer. A local timeout MUST return `LocalFault` or retry internally.

#### `BlocksDone` — Response, discriminator 4

```text
Frame        payload cap = 4 bytes
Decode       start_height <= Height::MAX; exact consumption
Reservation  live GetBlocks range with this start_height; consumes the terminal part and closes
             the reservation
```

The handler MUST return every unreceived height to the work queue. A retry policy SHOULD avoid a
peer that returned no blocks for heights inside its advertised servable range.

#### `RangeUnavailable` — Response, discriminator 5

```text
Frame        payload cap = 4 bytes
Decode       start_height <= Height::MAX; exact consumption
Reservation  live GetBlocks range with this start_height; consumes the terminal part and closes
             the reservation
```

The handler MUST requeue the range. A retry policy MAY avoid this peer for the immediate retry.

### Block sync — stream 6, version 3 (planned)

Version 3 names bodies by header hash, the same identity header sync already uses. Each requested
item carries an optional hash beside its height. When a request supplies a hash, the returned body
MUST hash to that value; a body that does not MUST return `Disconnect`. When a request omits the
hash, height alone identifies the item and version 2 behavior applies.

The hash removes version 2's restriction against two live ranges over the same heights. Two ranges
on one connection MAY cover the same heights when each supplies a distinct hash, because the hash
correlates every body and every terminal response to exactly one range. Two live ranges that cover
the same heights without hashes remain a violation, because nothing separates their terminal
responses.

This section is a placeholder. The message set, its caps, and its budgets are not yet specified.

## Conformance tests

The implementation MUST provide these checks:

1. A declaration test MUST prove that the 14 declarations and 14 handler branches match exactly.
2. A cap test MUST prove that every maximal legal encoding fits its declared cap and that no
   computed maximum exceeds that cap.
3. Property tests MUST generate legal messages and gate-event sequences. They MUST preserve
   reservation, budget, and state-size invariants. A conformant sequence MUST never return
   `Disconnect`.
4. Model-based reservation tests MUST generate subscription opens, grants, closes, exhausted credit,
   crossed and spontaneous terminal responses, work reassignment, finality changes, response
   reordering, duplicate responses, and connection closure. Each pushed response MUST consume the
   exact header and byte credit from one live subscription.
5. Fuzz tests MUST prove that each decoder is total, exact, and allocation-bounded for arbitrary
   frames.
6. Honest-node regtest traces MUST contain no `Disconnect` result.
7. Load tests MUST drive one peer at the maximum conformant rate and show that CPU, memory, and
   filter state stay within their declared bounds. They MUST drive a non-conformant flood and show
   that the receiver reaches `Disconnect` within bounded work.
8. Panic-isolation tests MUST inject a panic in a decoder, in a handler, and in a reactor port
   operation. Each test MUST show that the process survives, that other peers' processing paths
   continue, that the affected work returns to the scheduler, and that the result reaches neither
   peer-set ban policy nor the peer-violation count.
9. Delay-isolation tests MUST exhaust one request type's Work bucket and place a request in its
   request lane. A later response and a message of another class on the same ordered stream MUST
   reach their handlers before the delayed request becomes eligible. The tests MUST also prove FIFO
   dispatch, prompt wake-up after a refund, and `Disconnect` at the concurrency bound.

Peer-slot selection, message priority, and stream layout are outside this specification. Peer-slot
selection must remain separate because a conformant peer can waste a slot without violating a
message rule.
