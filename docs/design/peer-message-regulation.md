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

## Response lifetime and session replacement

The shared requester primitives separate response credit from session lifetime.
`ResponseCredit` counts consumed objects and actual bytes. `ResponseScope` fences
publication and first writes for one receiver incarnation. Each exchange has one
`ResponseAuthorization` owner, retained until its validated ending. The writer
holds a separate permission and cannot complete the response by finishing a write.
Identity, ordering, legal endings, and useful local work remain message policy.

Session admission retires the predecessor scope before publishing a replacement.
Retirement waits for a publication already in progress and prevents further
publications or first writes. A queued request that never started can be skipped.
If any exchange started and still lacks its ending, retirement closes its
connection locally before admitting another receiver on that connection.
Dropping that exchange's owner has the same close behavior. This is not a peer
protocol fault. A different connection can proceed independently.

For GetBlocks, the work lock is acquired before the response scope lock. Terminal
handling releases the scope lock before returning local work. Session admission
holds its session-table lock while retiring the old scope, without taking the
work lock. This preserves request publication atomicity without an inverse lock
order.

The endpoint creates one response metadata pool with a 128 MiB node limit and a
16 MiB limit per connection. All service sessions on that connection receive the
same context, including later escalation and replacement. Authorization records
reserve their allocation before creation and retain the charge through the last
writer handle, even after an ending or connection close. Exhaustion pauses new
requests locally, and a release wakes affected waiters.

An adapter can include its allocation plan in that reservation. Every planned
allocation must remain owned by the authorization or one of its writer handles.
GetBlocks includes expected hashes, the taken-work vector, and writer and status
allocations. The work vector is moved into the writer without cloning it. Status
readers retain the authorization's memory charge after the writer and response
owner exit. If the preferred batch cannot fit, the requester tries progressively
smaller batches before waiting for capacity.

First-use scope and cancellation locks can allocate on some platforms and still
need funding. Retained window and registry capacities also need charges before
these limits bound all protocol metadata. These allowances are separate from
body storage, decoding, and execution budgets.

## Capacity admission and QUIC backpressure

The receiver starts response work only when worker capacity and bounded output capacity are
available. When capacity is unavailable, it stops draining the affected request stream. Capacity
release resumes eligible processing. Local capacity exhaustion is not a peer violation.

The receive or serving loop owns this wait. It needs no mandatory admission verdict, second
delayed-request scheduler, or byte-refill timer. Bound any retained request prefix or decoded request.

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

A new exhaustive model explorer, compiler-enforced declaration framework, and universal
panic-recovery suite are deferred. The [testing design](property-testing.md) and
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
