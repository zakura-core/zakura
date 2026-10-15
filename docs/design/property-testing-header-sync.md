# Header sync regulation tests

> **Status: first draft.** This plan covers header sync version 9 under the
> [testing design](property-testing.md). The [specification](../specs/peer-message-regulation.md)
> defines behavior, and the [GetBlocks plan](property-testing-block-sync-infrastructure.md) covers
> block sync.

## Scope

Exercise `SubscribeHeaders` through its `Headers` pages and one `HeadersOutcome` terminal, plus
`Status`. Use production framing, decode, the subscription tools, serving capacity, and cleanup.
Add only the seams needed to control chain growth, header validation, storage completion,
capacity, and delivery.

This work introduces no Work bucket, refund, mandatory push deadline, or serving query-result
timer. It reuses the subscription tools and their tests rather than a new model. Version 9
replaces version 8 without a compatibility path, so version 8 tests retire at activation.

The checks land in stages after this plan:

1. Conformance suite support for subscription-only layouts.
2. A dormant version 9 codec and message table.
3. The publisher module.
4. The subscriber module.
5. Atomic activation of version 9.
6. Disconnects on contextual header failures.
7. Parameter evidence and close-out.

## Existing infrastructure

| Capability | Existing path | Use |
| --- | --- | --- |
| Subscription tools | [subscription.rs](../../crates/zakura-network/src/zakura/regulation/subscription.rs) | Credit window, slots, tombstones, and the crossed-update proptest |
| Message suite | [message_suite.rs](../../crates/zakura-network/src/zakura/wire_codec/message_suite.rs) | Coverage, tight bounds, round trips, mutations, hostile payloads, and allocation |
| Frame suite | [frame_suite.rs](../../crates/zakura-network/src/zakura/transport/message_rule/frame_suite.rs) | Header checks for every row and reader of a layout |
| Stream conformance suite | [stream_conformance](../../crates/zakura-network/src/zakura/testkit/stream_conformance/mod.rs) | Properties P1 through P9 over real QUIC |
| Cadence buckets | [cadence.rs](../../crates/zakura-network/src/zakura/regulation/cadence.rs) | `Status` observation, outage bursts, and local pause credit |
| Version 8 reactor tests | [reactor tests](../../crates/zakura-network/src/zakura/header_sync/reactor/tests/mod.rs) | Serving, admission, peer faults, timeouts, and VCT repair |
| Header pursuit replay | [fuzz.rs](../../crates/zakura-network/src/zakura/header_sync/fuzz.rs) | Scheduler model replay |
| Codec fuzzing | [header_codec](../../qa/fuzz/header-chain/fuzz_targets/header_codec.rs) | Decode and re-encode of arbitrary payloads |
| Multi-node harness | [cluster.rs](../../crates/zakura-network/src/zakura/testkit/cluster.rs) | Honest multi-node sync |
| Compatibility matrix | [matrix.rs](../../crates/zakura-network/src/zakura/testkit/matrix.rs) | Pinned peer profiles |
| Monotonic test time | [TestClock](../../crates/zakura-network/src/zakura/testkit/clock.rs) | Paused-clock cadence and progress checks |

The subscription tools already test their own window, slots, tombstones, and crossings with
integer cursors. Header sync tests add real header chains, the four-field cursor, the outcome
windows, page linkage, and the stream layout. Use real QUIC for flow-control claims.

## Conformance map

| Spec item | Planned checks | Lands with |
| --- | --- | --- |
| 1. Message checks | Message and frame suites over the version 9 family and layout, plus fixed fixtures for every kind and rule | Codec and message table |
| 2. Cadence | `Status` at the one-second floor, the initial send, the outage burst, and floods that are recorded and forwarded | Codec and message table, activation |
| 3. Reservations | Unsolicited, foreign, and misordered pages and outcomes. Local failover keeps authorization | Subscriber |
| 4. Subscriptions | `Open`, `Grant`, `Close`, crossings, credit exhaustion, the cursor ring, and outcome windows | Publisher, subscriber |
| 5. Fuzz and property | Hostile payloads, allocation bounds, and the codec fuzz target on version 9 | Codec and message table |
| 6. Capacity | Saturated serving, `Close` progress while output is blocked, blocked header reads, and churn | Publisher |
| 7. Real transport | The stream conformance suite on stream 5, and both directions subscribed on one connection | Conformance suite support, activation |
| 8. Load | Many subscribed peers, grant and `Status` floods, aggregate bounds, and parameter evidence | Parameter evidence |

Item 9 covers discovery and does not apply.

## Frame and decode

The message table declares four rows. `Status` is an announcement of exactly 122 bytes with
capacity 602, a 500 ms refill, and a one-second sender. `SubscribeHeaders` is a subscription row of
107 to 523 bytes with `max_live = 1`, credit of 4,000 headers and 8 MiB, a cursor history of 4,096,
and no cadence. `Headers` is a page row whose 263-byte minimum holds one Regtest header, and
`HeadersOutcome` is an ending row of exactly 41 bytes. `Stream::validate_layout` checks the table in a `const` item. A
test checks the `Status` capacity against `Cadence::min_capacity`.

The `Headers` row maximum must be a page that the codec can produce, because the message suite
requires each row's longest sample to encode to exactly its maximum. Pages take discrete sizes, so
2 MiB is not one. The largest page within 2 MiB holds 1,273 Mainnet headers under the V1 schema
and encodes to 2,096,713 bytes. The codec change must settle this with the specification's 2 MiB
absolute cap.

The message suite needs samples at each row's bounds:

- `Status` at 122 bytes.
- `Open` with one and with 13 locators, at 139 and 523 bytes.
- `Grant` and `Close` at 107 bytes.
- `Headers` with one Regtest header and with the largest page.
- `HeadersOutcome` for each of the six outcomes.

Its domain violations cover every Decode bullet in the specification. These include a zero
subscription ID, an `Open` with a nonzero sequence, no locators, 14 locators, or duplicates, and a
`Grant` or `Close` with sequence zero or locators. They also include a `Close` with credit, credit
above 4,000 headers or 8 MiB, and a `Grant` that adds no headers and `P - 1` bytes, while one
header or exactly `P` bytes passes. Run the minimum on every network. Add a fixed-fixture test that
covers every kind and rule, as block sync's `stream_six_fixed_fixtures_cover_every_kind_and_rule`
does. Run `check_frame_filter` over the stream 5 layout. Move the `header_codec` fuzz target to the
version 9 codec.

## Status cadence

On a paused clock, check that a sender at exactly the one-second floor never exhausts the bucket.
Admit the initial message plus the 600 messages that a ten-minute outage can buffer. Flood the
bucket and check that the reader records exhaustion and forwards every message without a
`Disconnect`. Unchanged messages count toward the bucket. A local read pause adds tokens.

Check that the sender coalesces tip changes within one second into one `Status` that carries the
latest value. Over real QUIC, repeat block sync's `regulated_status_bursts_keep_the_connection_serving`
for stream 5.

## Subscriptions and pages

Use production header chains and the header sync publisher and subscriber modules. Keep the
tools' integer-cursor tests and add these header sync cases.

**Open.** The acknowledged cursor must be the start cursor, and any other cursor faults. Credit
that cannot fit one entry under the selected schema faults. `NoLocatorIntersection`,
`TargetNotRetained`, and `TargetNotSelected` end the subscription before its first page. An `Open`
while one slot is held is served and increments `zakura.p2p.subscription.over_limit`. One more
disconnects, as does a reused subscription ID.

**Grant.** Check the window at exactly 4,000 headers and 8 MiB, and one more of each. A refused
grant changes nothing. Change each of the four cursor fields by one and expect `Disconnect`. The
current acknowledgement passes. A ring entry passes and acknowledges every earlier page. A cursor
that the publisher already passed faults. Sequence gaps and repeats fault.

**Close.** `Close` stops new pages. Pages already queued arrive before `SubscriptionClosed`. A
`Grant` or second `Close` while the publisher still holds the subscription faults.

**Crossings.** Extend `crossed_updates_never_fault_a_conformant_peer` with header sync's outcomes.
Interleave `Grant` and `Close` with `SubscriptionSuperseded` at any time and `HistoryPruned` before
the target, through two ordered queues. Conformant interleavings never fault, and both ledgers
agree. A crossing grant within consumed credit plus the window is dropped. One beyond it faults. The
next `Open` clears every tombstone.

**Credit exhaustion.** The publisher stops when unspent credit cannot fit a page and resumes on the
next grant. Each page spends its exact header count and payload bytes. The subscriber's precheck
refuses a page whose header exceeds the remaining byte credit before it reads the payload. A header
count above the remaining header credit faults.

**Cursor ring.** Send 4,000 one-header pages without an acknowledgement and check that the ring
stays within 4,096 entries. A slow acknowledger is held by the window and never reaches
`HistoryFull`. Assert the ring's memory at two subscriptions per peer.

**Pages.** The first page's parent is a sent locator, and each later page extends the receive
cursor. `reaches_initial_target` is set exactly on the page that ends at the target. A second
marker, a descendant past the target before the target, or a schema mismatch faults.

**Outcome windows.** Drive each of the six outcomes through each phase: before the first page,
before the target, after the target, and after `Close`. Each lands as the specification's window
table says, and every other placement faults.

**Authorization lifetime.** Local failover, target changes, work reassignment, and finality keep
the subscription live. Pages within credit still reach the handler and count as unwanted. A page or
outcome with no live subscription disconnects before decode.

**Idle and reorganization.** After the target, a subscription with no new header needs no page or
outcome and holds no execution. When the publisher's selected chain stops extending the cursor, it
sends `SubscriptionSuperseded`, and the subscriber reopens from the next `Status`.

**Contextual failures.** A page that fails contextual difficulty, time, chain connection, or
auxiliary roots disconnects its sender, and the subscriber never acknowledges its cursor. A local
state failure returns `LocalFault` and keeps the connection.

## Capacity and Close progress

Hold every execution slot and the whole output allowance. `Close` still produces
`SubscriptionClosed` and frees the slot, and grants still apply. The tools test this alone in
`close_needs_no_credit_and_no_execution` and
`pages_share_the_response_allowance_but_close_can_still_queue`. Repeat it through the header sync
publisher.

A burst of updates yields shared execution to another peer's work. Hold a header storage read,
disconnect its peer, and check that the read keeps its permit until it finishes. Reconnect
repeatedly while it stays blocked and check that churn does not multiply running reads. Retiring a
session with a live subscription closes the connection.

## Bidirectional transport

Once the conformance suite supports layouts without request rows, run it on stream 5:

- P1. An unsolicited page or outcome disconnects before decode.
- P2. The `Open` slot margin replaces the request margin.
- P3. Sustained subscriptions never fault.
- P4. Each peer subscribes to the other, and both reach their targets.
- P5 through P8. Retirement, paused siblings, unnegotiated versions, and partial frames behave as
  for block sync.
- P9. `Close` progresses while pages sit unread and execution is held.

Then run two real nodes on one connection, each publishing to and subscribing from the other on
stream 5. Stall one node's header validation so its accepted cursor stops. Its peer fills the
remaining credit and stops at the window. The stalled node must still apply the peer's grants and
`Close` for its own publication, so a paused handler cannot trap control behind pages. Release
validation and check that both subscriptions reach their targets. Buffered pages stay within one
credit window per direction plus transport buffers.

## Load and parameter evidence

Run the default maximum of peers, each holding a subscription while following the tip, with some
peers served full pages from an empty start. Assert aggregate execution, sent-cursor ring memory,
buffered page bytes, and granted credit against the node-wide pool. Flood grants at the minimum
size until the bound and flood `Status` past its bucket. Report the CPU cost per update.

The specification's parameter table needs this evidence:

- **Header and byte credit.** At a 500 ms round trip, 4,000 headers of credit carry at most 8,000
  headers per second from one peer. Header credit binds before 8 MiB on Mainnet. Measure with
  netem at the design target.
- **Cursor ring.** Memory at two subscriptions of 4,096 cursors per peer.
- **Grant minimum.** Grants per header during initial sync and tip following, and acknowledgement
  delay.
- **`Status` interval.** Recovery time after `SubscriptionSuperseded` at the one-second floor and at
  any longer candidate.

Keep manual probes ignored and gated by environment variables, as block sync does. Report the
host, network, round trip, and peer count with each figure.

## Compatibility and activation

Version 9 replaces version 8 without a compatibility path. Activation switches the stream
declaration, the capability, and the reactor together. Check that:

- Header sync declares one stream, version 9 on capability bit 9. This replaces
  `declares_only_capability_bit_five_at_stream_version_eight`.
- A node never advertises bit 5, and bit 5 joins the retired bits listed in `zakura.rs`.
- A peer that offers only version 8 negotiates no header sync stream, and the connection stays
  usable for other services. The matrix shows this for released Zakura.
- A stream 5 frame at version 8 is refused like any unnegotiated version, as in P7.

Before activation, the codec and module tests run against dormant version 9 code. Activation
removes the version 8 codec, reactor paths, and their tests. After it, an honest multi-node Regtest
sync on version 9 produces no protocol-violation result.
