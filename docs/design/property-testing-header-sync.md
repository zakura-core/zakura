# Header sync regulation tests

> **Status: first draft.** This plan covers header sync version 9 under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise SubscribeHeaders through its Headers pages and one HeadersOutcome terminal, and Status
through its cadence. Use production framing, decode, subscription tools, serving, and cleanup. Add
only the seams needed to control chain growth, header validation, capacity, and page delivery.

Version 9 replaced version 8 without a compatibility path, so the version 8 tests retired with
it.

## Existing infrastructure

| Capability | Existing path | Use |
| --- | --- | --- |
| Subscription tools | [Subscriptions and Publications](../../crates/zakura-network/src/zakura/regulation/subscription.rs) | Credit window, slots, tombstones with their own credit window, and crossed-update sequences |
| Generated codec checks | [Message suite](../../crates/zakura-network/src/zakura/wire_codec/message_suite.rs) | Row coverage, tight bounds, round trips, mutations, and allocation |
| Header checks | [Frame suite](../../crates/zakura-network/src/zakura/transport/message_rule/frame_suite.rs) | Every row against every reader of a layout |
| Real QUIC properties | [Stream conformance suite](../../crates/zakura-network/src/zakura/testkit/stream_conformance/mod.rs) | Unsolicited responses, pages beyond their credit, the subscription margin, retirement, paused siblings, partial frames, and Close progress |
| Cadence buckets | [CadenceBuckets](../../crates/zakura-network/src/zakura/regulation/cadence.rs) | Status observation, outage bursts, and read-pause credit |
| Reactor tests | [Header sync reactor tests](../../crates/zakura-network/src/zakura/header_sync/reactor/tests/mod.rs) | Admission, completion, peer faults, repair, and timeouts over version 9 |
| Codec fuzzing | [header_codec_v9](../../qa/fuzz/header-chain/fuzz_targets/header_codec_v9.rs) | Arbitrary payload decode and re-encode |
| Multi-node harness | [Cluster](../../crates/zakura-network/src/zakura/testkit/cluster.rs) | Honest multi-node sync |
| Monotonic test time | [TestClock](../../crates/zakura-network/src/zakura/testkit/clock.rs) | Existing clock-dependent checks |
| Parameter evidence and load | [Publisher load tests](../../crates/zakura-network/src/zakura/header_sync/v9/publisher/tests/load.rs) | Production credit and ring, Grant floods, throughput, and many sessions |

The subscription tools test their own counters with integer cursors.

## Frame and decode

Check the 122-byte Status cap, the SubscribeHeaders range of 107 to 523 bytes, the 41-byte
HeadersOutcome cap, and the Headers page range. Each row's maximum must be an encoding the codec
can produce. Check the removed payload discriminator, exact consumption, 1 through 13 unique
locators, nonzero subscription IDs, and credit limits of 4,000 headers and 8 MiB. Locator and
header counts must not cause allocation beyond the checked prefix and the remaining credit.

Generate legal updates, pages, and outcomes and check production encode/decode round trips.
Include minimum/maximum locator counts, a Grant of exactly one header or one smallest page of
bytes and one below, maximum credit and one above, an Open with a nonzero sequence, a Grant or
Close with locators or sequence zero, a Close with credit, a zero header count, an unknown outcome,
truncation, and trailing bytes. Run each case on every network, because header sizes differ.

Status is the only row with a cadence. Check its capacity against the one-second sender's outage
derivation. On a paused clock, a sender at the floor never exhausts the bucket, and the initial
message plus a ten-minute outage burst fits. A flood exhausts it, and the reader records the
exhaustion and forwards every message. Unchanged messages count toward the bucket. Local read
pauses add tokens. Check that each side sends Status immediately on open, within two seconds of
each committed change, and at least once per refresh interval while idle, and that two idle
peers on a new connection both open subscriptions.

## Subscriptions and outcomes

Record the local subscription and its credit before sending Open or Grant. Exercise:

1. An Open whose pages start at the highest sent locator that is the target or its ancestor and
   reach the initial target, with the target marker on exactly the last of them, followed by
   pushed descendants.
2. An Open whose first or a later locator is the target, with no path page and no target marker,
   followed by pushes that extend the target, and a HistoryPruned that faults.
3. Grants at the window limit and one beyond, each cursor field changed, a cursor the publisher
   already passed, and sequence gaps.
4. Credit exhaustion and the next Grant, with each page spending its exact header and byte credit,
   and 4,000 unacknowledged one-header pages within the cursor history.
5. Close with pages queued, followed by SubscriptionClosed, and a Grant or second Close after it,
   both before and after the subscription ends.
6. Grant and Close crossing SubscriptionSuperseded or HistoryPruned, repeated crossing Grants up to
   and beyond the tombstone window, and the next Open.
7. A second Open while one slot is held, a third, and a reused subscription ID.
8. Unsolicited pages and outcomes, a first page that extends no sent locator or, when a locator is
   the target, does not extend the target, broken linkage, a second target marker, a schema
   mismatch, and each outcome outside its window.
9. Local failover, target change, finality change, or reassignment before pages arrive.
10. Connection closure with a live subscription.

Local scheduling must not revoke credit. Conformant crossings never fault, and both ledgers agree.
A terminal outcome ends a subscription once. An idle subscription at the tip needs no page or
outcome. Contextual difficulty, time, chain-connection, and auxiliary-root failures disconnect the
sender, and the subscriber never acknowledges that cursor. Local state failures do not.

## Capacity and ownership

Hold every serving slot and the output allowance. Close must still produce SubscriptionClosed and
free its slot, and Grants must still apply.

Bound control work. A burst of updates must yield shared execution to another peer. Credit
authorizes future pages and must not allocate a queue of page jobs.

Hold a header read open with a controlled dependency. Disconnect its peer and drop its waiter.
Verify that the read keeps its permit until completion. Reconnect repeatedly while the read remains
blocked. Connection churn must not multiply running reads beyond the node-wide bound.

Exercise ordinary storage failures. Finished resources release once, and local failures do not
count as peer violations. Publisher state, credit, and cursor history stay within two
subscriptions per peer.

## Bidirectional backpressure

Run two endpoints that publish to and subscribe from each other on one connection. Stall one
side's header validation so its accepted cursor stops. Its peer must stop at the credit window, and
the stalled side must still apply the peer's Grants and Close. A paused handler must not trap
control behind pages. State finite bounds and scheduling assumptions.

The stream conformance properties run over real QUIC for stream 5. A peer that offers only
version 8 negotiates no header sync stream, and the connection stays usable.

## Load and reporting

The publisher load test runs 40 sessions on each pull request and 512, every default session slot,
in the full-coverage unit test runs on pushes to main, the merge queue, and the nightly schedule,
which include ignored tests. A fifth of the sessions each follow the
tip, read full pages from an empty start, flood one-header grants, never read, or churn. Reads first
wait on a closed gate, so they stop at the node and peer execution limits of 8 and 2. After every
scheduler pass the test checks that cursor history stays within the window and output within its
bound. Every reading session must then finish. A non-reading session queues pages in every
transport slot but the one kept for control, and its next page waits without spending credit.
The pages carry Regtest headers, about 181 KB for 1,000 headers against roughly 1.7 MB on Mainnet
with records, so the test checks counts and slots, not output bytes near production limits.

Each of these tests runs on a paused clock in one thread. A progress bound counts scheduler passes,
at most one million, so a wait on a timer fails instead of advancing time. The port and transport
are in memory, so the tests exclude storage reads, header validation, and QUIC buffering.
Generated tests search for failures and don't prove every execution correct. The nightly run
raises the publisher, subscriber, and crossed-update properties to 4,096 cases through
`HEADER_SYNC_PROPTEST_CASES`.

### Measured evidence

These figures come from a release build on an Apple M3 Ultra. Rates on the paused clock are exact.

| Specification row | Evidence |
| --- | --- |
| Header credit | Even one-header pages with records spend at most 6.9 MB of the 8 MiB byte window per full header window on Mainnet, so header credit binds first on every network. Over a fixed one-way delay and no bandwidth limit, the real subscriber and publisher move one 4,000-header window per round trip: 8,000 headers per second at the 500 ms target and 40,000 at 100 ms. At 500 ms that is about 13 MB/s of Mainnet pages, below the byte window's 16.8 MB/s and the 67 MB/s that QUIC's 32 MiB send window allows. Validation time adds to each round trip, because the subscriber acknowledges a page only after validation. |
| Cursor-ring size | 4,000 unacknowledged one-header pages fit the 4,096-entry ring. The publisher stops at the header credit, and a one-header `Grant` renews it. The generated publisher, subscriber, and crossed-update properties cover `Open`, `Grant`, `Close`, crossing updates, reorganization, storage failures, and credit exhaustion. |
| Grant minimum | A subscriber that never acknowledges can send 35,894 minimum updates on Regtest, or 9,331 on Mainnet, before its window is full. That is one fewer than the specification's bounds of 35,895 and 9,332, because `Open` must add both a header and `P` bytes. All of them apply while page output is blocked, at 60 ns each under the publisher's lock, with no allocation. The publisher spends credit only on the page its transport slot holds. |
| `Status` interval | The specification lets a peer send its new `Status` up to two seconds after the commit. This node's `StatusPublisher` sends it within its one-second floor, which `initial_refresh_and_coalesced_change_deadlines_are_bounded` checks. The subscriber starts the new target as soon as it has both that `Status` and `SubscriptionSuperseded`, in either order, without a timer wait. So recovery takes at most two seconds plus the round trips against any conformant peer, and at most one second plus the round trips against this node. Before this evidence, a `Status` that arrived first was spent while the subscription was still active, and recovery waited for the peer's 30 s refresh. No longer floor is proposed. |
| Load | 512 sessions finish in 1.4 s and 202 scheduler passes, and 40 in 0.23 s. Running reads reach the node limit of 8, and no peer holds more than its 2 slots. |

`Status` floods start no repeated work. A 602-message burst of repeated statuses from each of 16
peers starts one locator read and costs 7.3 µs per status. From each of 255 peers it costs 40 µs per
status. About 30 µs of that is the pass over cached statuses that the reactor runs after every
event, so the cost of each status grows with the number of connected peers. The 602-message burst
is the cadence capacity, not a ceiling. Cadence exhaustion records and forwards, so one peer can
keep sending. At 40 µs per status with 255 peers connected, about 25,000 statuses per second
saturate the reactor. Sizing that as a denial-of-service bound is follow-up work.
