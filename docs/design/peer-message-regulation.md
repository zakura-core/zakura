# Zakura peer message regulation: design

> **Status: first draft.** The [specification](../specs/peer-message-regulation.md) states the
> rules for discovery, header sync, and block sync. This document explains their implementation scope.

## Resource bounds and service policy

Zakura must use available p2p capacity while bounding peer-controlled work. Regulation bounds frame
sizes, decoded allocations, verification, active storage work, retained results, buffered output,
and response authorization. Node-wide bounds limit aggregate commitments across admitted peers.

Announcements and discovery requests exchange metadata at a limited cadence. Keep that restriction
even though block requests intentionally support continuous traffic. A relevance check cannot
replace frequency control: a peer can continually change fields without providing useful data.

A conformant block requester can consume service indefinitely. Per-peer byte-rate buckets cannot
prevent an attacker from creating more identities. Prioritization and peer-slot selection will
decide which peers receive service. This work bounds resources independently of those future
policies. Connection admission must bound aggregate connection state.

This design adds no response-byte charges, fixed request overhead, refunds, or serving-rate refill
timers. A future rate limit needs a measured resource cost that existing bounds cannot control.
Repeated unavailable-range lookups and subscription control updates need measurement because small
responses can still cause CPU or storage work.

## Acting on violations

The regulation tools act against a peer (`Disconnect`, a ban score, or any penalty) only on an
unambiguous violation: an event that no conformant peer could cause. An event that a conformant
peer could cause is traced, never acted on, so the protocol can be tuned with evidence. A local
bug is no reason to stay passive; bugs get fixed.

Whether this node still wants a response is local scheduling. It never decides whether the peer
broke the protocol.

Limits are conservative enough that exceeding them is unambiguous. A limit holds, with margin, for
every conformant peer after the worst transport stall and at full link speed. A count limit acts
only above the largest count a conformant peer can reach on this node's tally, under any
congestion, reordering between streams, or release timing. Counts between the advertised limit and
that margin are served and traced.

Limits never cost throughput. Every capacity default derives from one target, 10 Gbps per
connection at 500 ms round-trip time, and a test checks each default against its derivation. A
local capacity limit makes the node wait; it never faults a peer. QUIC flow control binds first
today: a 32 MiB connection send window carries about 540 Mbps at 500 ms.

## Message checks and handler policy

The implementation may use existing codecs, handlers, and validators. It need not introduce a
declaration builder, universal filter framework, or one ingress call site per data type.

Frame checks precede allocation. Cadence checks precede expensive metadata handling. Reservation
prechecks supply request-selected decode bounds. Exact response matching precedes expensive
verification. Worker and buffer admission precede the work that consumes those resources.

Handlers own sequence, expiry, empty-response, and target-selection policy. Keep cheap no-op checks
where they save work. A separate lock-free relevance snapshot is not required. Local staleness must
not turn a legal message into a protocol violation. Matched responses still consume authorization
and reach their handler.

Ordinary local failures release resources whose work has ended and return affected work to the
scheduler. They do not count as peer violations. Universal panic recovery belongs to separate
runtime work. Bounded decoder tests still check that untrusted payloads cannot cause a panic.

## Message tables

A stream can declare a message table: one row for each message type it carries.
A row states the message's role, its payload bounds, and the limits its role
needs. An announcement declares its cadence. A request declares its in-flight
limit and an optional cadence. A response names the request it answers and
whether it ends the exchange. Rows hold values only. A bound that depends on a
message's contents stays in the codec or the reactor.

The table is the single source for three checks. The reader checks each frame
header against it before it reads the payload. The message family's codec
checks each payload against the same rows. Generated test suites read the rows,
so a new message needs a row and a codec arm, not new bound tests.

A layout is one request/response stream, or the persistent streams of one
service session. A response row may answer a request row on another stream of
its layout. A stream pair is a layout whose request rows sit on their own
stream. It needs no pair-specific code.

`Stream::validate_layout` checks a layout's tables in a `const` item, so a bad
table fails the build. The registry repeats the check at startup. These checks
cover the tables' consistency only. The stream arrangement must still
demonstrate progress under paused reads with the transport tests below.

A row with a cadence declares both sides of it: the sender's minimum interval,
and the receiver's bucket capacity and refill interval. The sender's tool and
the receiver's tool read the same row, so they cannot drift apart. The layout
validator proves that a conformant sender never empties the bucket: the bucket
refills faster than the sender sends, and its capacity holds every message a
sender can queue during the longest outage a connection survives, plus two. The
receiver credits its own read pauses as they happen. An empty bucket is
therefore a violation, and the reader disconnects the peer. Buckets that fall
below a quarter of their capacity are traced, as evidence for tuning the values.

On a stream with a table, only rows with a cadence charge a bucket. Commitments
bound requests without one, and reservations bound responses. A service can
attach its reservations to the stream as a response precheck: the reader then
checks each response header against the live reservations before it allocates
the payload.

A stream without a table keeps the legacy behavior: its reader admits any
message type and any flags up to the stream's frame cap, and charges its
per-kind message-rate bucket. Reactors adopt tables one at a time.

## Capacity admission and QUIC backpressure

The receiver starts response work only when worker capacity and bounded output capacity are
available. When capacity is unavailable, it stops draining the affected request stream. Capacity
release resumes eligible processing. Local capacity exhaustion is not a peer violation.

The serving task owns this wait, not the reader. The reader admits each request as a commitment
without waiting, so no layout can trap responses or control messages behind waiting requests. The
serving task takes the peer's and the node's output bytes for the whole response cap, then a peer
and a node execution slot, and only then starts the work. The response therefore never waits for
the peer: its frames queue against output that is already granted, and the work gives back its
execution slots as soon as it returns. This meets "stop draining the affected request stream"
because accepted commitments are bounded and every capacity wait runs off the reader. It needs no
admission verdict, second delayed-request scheduler, or byte-refill timer.

A commitment is released when its ending enters the session's ordered output, before transmission.
A conformant peer sends its next request only after it receives an ending, so it never exceeds the
advertised limit on this node's tally. The toolkit still acts only above twice the limit: even if
the release happened after the write, the tally would include at most one further request per
ending in transit. Requests between the limit and twice the limit are served and traced.
Commitments are counted per session, so a retired session's running work never counts against the
peer's next session. The enforced limit is the highest one advertised on the connection.

Every admitted request ends exactly once. If the work fails locally, the reactor supplies a legal
ending for what it already sent, and the connection stays open. The response cap reserves room for
the largest ending, so the failure ending always fits.

Stopping application reads must stop draining the QUIC receive buffer. Once the peer consumes its
existing stream credit, it cannot send more data on that stream. Already authorized bytes still
count toward the resource bound. Account for both stream and connection credit.
See [QUIC flow control](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.1).

Pausing request intake must not trap responses or control messages needed to finish active work.
A mixed ordered stream can create that dependency even when every queue is bounded. The concrete
stream layout must demonstrate simultaneous bidirectional serving and control progress before
deployment. Connection credit must preserve room for required independent streams.

Bound application read-ahead, decoded objects, retained storage results, verification workers,
and queued output. QUIC cannot bound resources after the application removes bytes from its receive
buffer. Reserve output capacity before generating retained results or encoded frames.

Protocol inflight limits count outstanding commitments. Execution slots count running operations.
A node can advertise more inflight requests than workers if it bounds the retained commitments.

Each operation owns its execution slot until it actually finishes. Disconnecting a peer or dropping
a waiter does not stop an underlying blocking database read. Keep the slot until that read ends.
A separate serving query-result timer is unnecessary for this ownership rule. Cancel future work
where possible and release finished resources exactly once.

Bound the control or empty-response work performed before yielding shared execution. A tiny response
may never fill the output buffer. This path still needs bounded execution and progress opportunities
for other runnable work.

## Discovery cadence and state

A discovery-only connection can complete one exchange and close. A connection shared with another
service can remain open and repeat exchanges. Hello is not limited to one or two messages over
the lifetime of every connection.

The reviewed [discovery source][discovery-source] sends Hello, GetPeers, and GetServices in each
exchange. Its default interval is 15 seconds because service summaries have a 30-second default
validity period. The source can reuse an unchanged signed Hello record until it needs renewal.

Keep Hello cadence. Replace discovery request byte accounting with explicit GetPeers and GetServices
cadence. The draft permits one initial message and later messages at least 15 seconds apart.
A requester also waits for its previous response before sending another request of that type.
Configurable refreshes must obey the protocol minimum. Validate renewal under scheduling jitter
before enforcing candidate values.

An unchanged Hello still counts toward cadence. It can satisfy initial-exchange progress when the
receiver already knows the record. The import handler applies sequence and expiry policy.

Peers and Services each consume a one-shot reservation. They need no separate response cadence.
The requester controls how often it creates those reservations. Keep validation and bounded import
state. An otherwise valid relayed record with an incompatible protocol range is an import-policy
rejection. A malformed range remains invalid.

Equal service values can renew validity. An empty Services list clears live service state.
A generic relevance predicate must not discard either effect.

Receiver cadence must tolerate buffered arrivals after stalls and intentional read pauses.
A rate margin alone does not prove burst tolerance. Establish that the timing and buffering policy
cannot disconnect a conformant sender. Ambiguous bursts still consume bounded execution capacity.

## Reservations and header push

The requester creates a reservation before sending a request. A response consumes exactly one
reservation or one unconsumed range part. Local work reassignment does not revoke authorization.

A claim either succeeds, and the frame reaches its handler, or is refused, and the peer is
disconnected. Every refusal is unambiguous: no live reservation, a row that answers another
request, an ending claimed as a frame or the reverse, or frames or bytes over the reservation's
budget. Only the response's own ending or the connection's end removes a reservation. The same
response cap bounds the responder's output and the requester's reservation, so a conformant
responder can never exceed it.

Want is local. A requester that no longer wants the work abandons the reservation: it stays live,
its frames still reach the handler, which may skip their work, and they are counted as unwanted.
Nothing about want reaches the peer's record.

Every requester map draws its entries from one node-wide pool, sized to keep twice the target's
bandwidth-delay product reserved. A full pool makes the node wait before its next request; it
never acts against a peer.

Header push remains in scope. SubscribeHeaders grants bounded header and byte credit. The
subscription identifies the initial target, locator, schema, cursor, and authorized descendants.
QUIC byte credit cannot replace that identity or object authorization.

Keep bounded acknowledgement history, update ordering, and terminal tombstones. These rules handle
crossing messages independently of worker admission. The subscriber records credit before sending
Open or Grant. Both sides consume the corresponding credit when they send or admit a page.

Close stops new pages and follows queued pages with a terminal outcome. Close processing must not
wait for a serving worker or data-output capacity. Reserve bounded terminal-output capacity
independently of header and byte credit. Control updates remain bounded and yield shared execution.

Remove the mandatory push deadline. The publisher produces eligible pages when credit, chain data,
and capacity are available. The subscriber can track progress and select another peer under local
policy. Slow progress alone is not a protocol violation. Idle subscription state stays bounded.

Block sync retains version-2 correlation rules. Live ranges cannot overlap on one connection because
the wire format lacks a request ID. BlocksDone and RangeUnavailable close the matching range once.
A separate successor protocol can improve correlation later.

## Tests and diagnostics

Use existing tests for frame/allocation bounds, cadence, reservations, subscription races,
capacity ownership, and transport progress. Test initial and periodic discovery, buffered bursts,
blocked storage, connection churn, continuous serving, and repeated unavailable-range lookups.

Diagnostics identify protocol violations and local failures with bounded logging and storage cost.
Sampling or aggregation is allowed. Complete per-decision traces are optional test/debug output.
No particular file name or schema is required.

A new exhaustive model explorer and universal panic-recovery suite are deferred. The [testing design](property-testing.md) and
[GetBlocks plan](property-testing-block-sync-infrastructure.md) describe the initial checks.

## Adoption order

1. Check frame, allocation, validation, and response-correlation bounds for every supported message.
2. Preserve reservations across reassignment and remove unmatched-response exceptions.
3. Implement bounded execution and output ownership with transport backpressure and bidirectional
   progress tests. Keep the underlying operation's permit through cancellation.
4. Add discovery request cadence alongside announcement cadence. Validate sender configuration
   and buffered-arrival tolerance before enforcing candidate receiver thresholds.
5. Add header subscriptions with bounded authorization, credit, control processing, and closure.
   Apply capacity admission without a byte-rate bucket or mandatory push deadline.

Prioritization, peer-slot policy, exhaustive model exploration, and universal panic recovery remain
separate work. Stream layout can have its own design, but admission must demonstrate the progress
requirements above.

[discovery-source]: https://github.com/zakura-core/zakura/blob/fbf466b07dbc0e86c3ad1e8ae33c2f9e94f8a647/crates/zakura-network/src/zakura/discovery/service.rs#L851
