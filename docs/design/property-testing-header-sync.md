# Header sync regulation tests

> **Status: first draft.** This plan covers header sync version 9 under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise SubscribeHeaders through its Headers pages and one HeadersOutcome terminal, and Status
through its cadence. Use production framing, decode, subscription tools, serving, and cleanup. Add
only the seams needed to control chain growth, header validation, capacity, and page delivery.

Version 9 replaces version 8 without a compatibility path, so version 8 tests retire when
version 9 activates.

## Existing infrastructure

| Capability | Existing path | Use |
| --- | --- | --- |
| Subscription tools | [Subscriptions and Publications](../../crates/zakura-network/src/zakura/regulation/subscription.rs) | Credit window, slots, tombstones, and crossed-update sequences |
| Generated codec checks | [Message suite](../../crates/zakura-network/src/zakura/wire_codec/message_suite.rs) | Row coverage, tight bounds, round trips, mutations, and allocation |
| Header checks | [Frame suite](../../crates/zakura-network/src/zakura/transport/message_rule/frame_suite.rs) | Every row against every reader of a layout |
| Real QUIC properties | [Stream conformance suite](../../crates/zakura-network/src/zakura/testkit/stream_conformance/mod.rs) | Unsolicited responses, retirement, paused siblings, partial frames, and Close progress |
| Cadence buckets | [CadenceBuckets](../../crates/zakura-network/src/zakura/regulation/cadence.rs) | Status observation, outage bursts, and read-pause credit |
| Reactor tests | [Header sync reactor tests](../../crates/zakura-network/src/zakura/header_sync/reactor/tests/mod.rs) | Version 8 serving, admission, peer faults, and timeouts to port |
| Codec fuzzing | [header_codec](../../qa/fuzz/header-chain/fuzz_targets/header_codec.rs) | Arbitrary payload decode and re-encode |
| Multi-node harness | [Cluster](../../crates/zakura-network/src/zakura/testkit/cluster.rs) | Honest multi-node sync |
| Monotonic test time | [TestClock](../../crates/zakura-network/src/zakura/testkit/clock.rs) | Existing clock-dependent checks |

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

Repeat the stream conformance properties over real QUIC for stream 5. A peer that offers only
version 8 negotiates no header sync stream, and the connection stays usable.

## Load and reporting

Run many peers following the tip, full pages from an empty start, Grant floods at the minimum
size, Status floods, non-reading subscribers, and connection churn. Assert bounded execution,
cursor history, and buffered pages, and opportunities for other runnable peers to progress.

Gather the evidence that the specification's parameter table lists.
