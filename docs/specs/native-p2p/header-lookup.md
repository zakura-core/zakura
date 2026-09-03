# Header-lookup successor exchange (`SubscribeHeaders`)

> **Status: draft successor proposal.** This preserves the stream-5 version 9
> design from the original regulation proposal. It is not the current protocol
> contract and has not been reconciled with production or executable evidence.
>
> Normative keywords below apply only to the proposed successor. Before this
> becomes **Specified**, its requirements need stable IDs and implementation
> evidence under the [contract standard](README.md).

## Proposed stream version and shared limits

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
implementation in `HeaderSyncMessage::check_payload_size`.

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

## `SubscribeHeaders` — Request, discriminator 2

- **Frame**
  - payload cap = 523 bytes
- **Decode** — `HeaderSyncMessage::decode`
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
The subscriber MUST select the target from an applicable `Status`. It MUST create the local
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

## `Headers` — Response, discriminator 3

- **Frame**
  - absolute payload cap = 2 MiB
  - reservation payload cap = min(publisher_advertised_message_bytes,
    remaining_subscription_byte_credit)
- **Decode** — `HeaderSyncMessage::decode`
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
- **Message validity** — `prepare_headers`, which checks the decoded page before
  consulting mutable chain state. It establishes the supported encoding version, the locally
  computed hash, the inferred height, the commitment interpretation, the canonical compact target,
  the hash-to-target filter, the Equihash solution under the network proof-of-work policy, and the
  per-block work. Header sync already calls it on this path in `header_sync_driver`. The individual
  checks live in `validation::context_free`.

Page linkage is a reservation rule, not a message validity check. It depends on the sent locator and
receive cursor in the local reservation, so `prepare_headers` cannot decide it from the page alone.

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
transition planner performs those checks; `prepare_headers` documents the split.
The handler MUST disconnect the peer when one of those checks fails.

## `HeadersOutcome` — Response, discriminator 4

- **Frame**
  - payload cap = 41 bytes
- **Decode** — `HeaderSyncMessage::decode`
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

## Evidence required before promotion

The proposed limits remain candidate values until the following evidence exists:

| Parameters | Required evidence |
| --- | --- |
| 4,000-header credit, 8 MiB byte credit, and 4,096-entry cursor ring | Generated histories covering open, grant, close, crossing updates, one-header pages, exhausted credit, and reorganization; the ring must retain every cursor that can still be acknowledged. |
| 30-second push deadline | Native tests over slow but conformant links and target changes; a conformant publisher must queue within 30 seconds, and the subscriber must wait 60 seconds before treating the obligation as violated. |
| Work capacity and refill | CPU, state, and egress measurements at the maximum peer count, plus a liveness test for the largest legal grant. |
