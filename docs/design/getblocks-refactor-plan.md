# GetBlocks refactor plan

Simplify #892's serving path while preserving response reads when requests to serve other peers wait for capacity. Use two streams per block-sync session and one sequential serving task. Keep the current limits: one active response per authenticated peer, including across reconnects, and 64 per node by default.

A **session** is one active block-sync relationship with a peer. A **permit** reserves capacity for one response. Storage work and outgoing data retain that permit until they finish or are discarded.

This plan replaces the shared-stream implementation before #892 ships. It describes the remaining implementation and the tests required before activation.

Execution has started. The initial [transport results](getblocks-refactor-results.md)
show progress with the advertised request volume and a paused sibling service,
but a stall when two paused streams fill the connection's receive allowance.
Supported workloads must complete normally. Excessive traffic that exhausts
the shared allowance must trigger bounded cleanup and successful recovery;
the original download attempt may be retried. Paired transport, atomic request
publication, the sequential serving task, and the node's owned storage adapter
are implemented. Initial matched downloads and cancellation tests pass.
Current sessions now use a table and watch notification, and block sync's
configured queue depths bound its actual transport queues.
The new block-sync version remains disabled until the full acceptance gate
passes. The existing serving driver remains active in production during this
validation.

## Before and after

**Overload example:** A is downloading block 200 from B, and A's 64 serving slots are occupied. The test peer B also sends a request for block 100 to put pressure on A's serving path. Only A has a download that must complete; B's extra request is test traffic.

**Today:** Requests and responses share one stream. A queues B's request so it can keep reading block 200. The current #892 implements this progress fix.

**After:** A holds one decoded request for block 100 and pauses request intake. It continues reading block 200 from the separate data stream. A dedicated serving task handles the request when capacity becomes available.

| Stream | Messages | When reading can pause |
| --- | --- | --- |
| Requests | `GetBlocks` | While waiting for serving capacity or the previous response to finish |
| Data and control | `Status`, blocks, ending messages | During ordinary bounded download processing |

The benefit is simpler request handling and ownership. Serving waits become local to the serving task, rather than work coordinated through the shared block-sync event loop, called the **reactor**.

## Completed work

These changes are complete in the inspected #892 source. Preserve their behavior and adapt their tests during the refactor.

- [x] Kept response reads progressing during serving admission and outbound congestion.
- [x] Removed admission-grace timers and compensating download-deadline extensions.
- [x] Represented the active serving request with `Option<ServingBlockRequest>`.
- [x] Derived the received-block count from the exact bitmap with `count_ones()`.
- [x] Reserved output-queue capacity before encoding responses.
- [x] Added production-QUIC serving-progress regressions and separate matched-download coverage.

## Proposal adjustments

Keep the [proposal's](https://github.com/zakura-core/zakura/pull/892#issuecomment-5590691911) core design, with these adjustments:

1. **Allow request writes to wait under serving pressure.** Today's generic write timeout closes the connection after ten seconds. In the example above, that could disconnect B while useful block data is arriving. Once a write starts, finish the frame while the session remains valid; cancellation of that write resets the pair. Retain deadlines for data writes, session establishment, and ordinary downloads.
2. **Allow a slow peer to establish and maintain delivery progress.** The loss test also fails on the original PR: its only cold probe expires under the short floor deadline. Give an unmeasured peer the normal bounded request deadline. Include transfer time for the requested body and earlier unreceived responses on the ordered data stream, using the measured rate with the existing 256 KiB/s lower bound. Keep the probe cap, ownership checks, and block-progress timeout. This policy fix is included by the user's explicit decision.
3. **Allow data writes to wait for shared connection credit.** With a paused sibling and packet loss, a healthy block-data write can take about 14 seconds while earlier blocks are still arriving. The existing ten-second deadline closes that connection. Use a bounded 32-second deadline for the paired data stream, including its control and ending messages. Cancellation still interrupts the write, and request expiry and block-progress liveness still apply. The user approved this amendment after a prototype completed the combined workload.
4. **Move download-index optimization into a separate effort.** Preserve the current matching, retry, and ownership bookkeeping here. Optimizing it alongside the stream migration adds correctness risk and is not needed to complete this refactor.

## Implementation order

### 1. Prove transport and storage behavior

Start from a refreshed #892 head, preserving its transport prerequisite. Record baseline test results, memory use, and useful block throughput for comparison.

#### Transport limits

QUIC already gives each stream independent flow control. A can stop reading requests while continuing to read blocks, provided the connection has capacity. The two-stream design does not inherently require a QUIC fork change.

A **receive window** limits how far a sender can get ahead of the application reading its data. Reading replenishes the allowance.

**Selected design:** Keep two streams with the existing window sizes and QUIC dependency. Any upstream work on individual stream-window controls is a separate future effort.

| Allowance | Selected setting |
| --- | --- |
| Request stream | 16 MiB |
| Data stream | 16 MiB |
| All streams combined | 32 MiB shared receive ceiling |
| Shared send window | 32 MiB |

If A stops reading B's requests, unread requests may occupy up to 16 MiB in QUIC. The connection's 32 MiB receive ceiling is shared; it is not memory allocated in advance. The two block-sync streams fit within it, but other services and incomplete setup also consume capacity. Enforce stream-count limits and account for every stream that can pause. A small application queue alone does not bound QUIC buffers.

#### Transport acceptance gate

Before measuring the prototype, record each workload, connection/stream counts, permitted peak memory, completion deadline, and useful-throughput floor in the execution results. Compare against the refreshed #892 baseline on the same hardware and link conditions. Thresholds must not be chosen after seeing the new implementation's results.

Use an ordinary download from B to A: A must receive its requested blocks and the ending message, with responses matched to its actual outstanding request. Exercise other paused services, delayed acknowledgments/loss, and reset/reopen cycles, including combined conditions. Starting queries or accepting bytes into a send queue is insufficient.

Separately, inject extra `GetBlocks` requests from test peer B while A's serving capacity is occupied. Within the declared supported workload, A's download must still complete and the extra requests must remain bounded. This checks resilience to incoming request pressure. Remove the earlier requirement that both peers complete matched downloads from each other simultaneously.

Full buffers are ordinary backpressure, not a reason to disconnect. Let paused consumers resume and release transport allowance naturally. If expected block progress remains absent until the existing block-progress deadline, retire the affected stream pair and apply the existing cooldown and repeated-stall policy. Request expiry can return missing work earlier. Reuse these timers rather than adding a separate fullness timer.

For sustained saturation, require bounded cleanup and recovery instead of completion of the original attempt. Return unreceived work for retry, preserve received blocks, and keep running reads and encodes charged until they end. If other streams prevent recovery, close the peer connection. Repeated saturation must not create an endless immediate reopen loop or prevent use of another available peer. Test natural recovery before the deadline, recovery on a fresh session, and escape from a repeatedly saturating peer. Stream replacement alone is not evidence that syncing recovered.

Passing the completion, memory, and throughput criteria is a prerequisite for enabling the new service version. An unmeasured case is not a pass. If these default settings fail the gate, keep the new version disabled and record the failed condition before revisiting the design.

The user approved a specific revision after raw QUIC measurements showed that
the default transport cannot meet the original loss threshold. Lossy workloads
must complete within 240 seconds and retain at least 90% of the original serving
path's median throughput with the same download-policy fix. Loss-free workloads
retain the 30-second deadline. See the execution results for the failed baseline,
the revision, and subsequent measurements.

#### Storage ownership

**Before:** The async node driver holds a `BlockRangeQueryLease`, which retains response capacity. After normal cancellation or a query timeout, it keeps waiting for the state read to finish. That protection depends on the driver task staying alive.

**After:** Put the ownership inside the blocking database job itself. Aborting the async caller must leave capacity charged until the actual read and any retained result finish.

**Example:** A is reading block 100 for B when B disconnects. The active read must keep its serving slot even if the task waiting for its result is aborted. Otherwise A could admit replacement work while the old read still runs.

Add a narrow state API that accepts caller-owned resources and checks cancellation before the first lookup and between lookups. It must retain those resources through a running read and any returned blocks. Use at most one result per job; failed delivery drops both the result and its ownership. Test caller cancellation, caller abort, and panic unwinding.

Define the serving interface in the network crate, perform reads in the state crate, and connect them through an adapter in `zakurad`. Reuse the existing range-read logic and state readiness/failure checks. Keep this single-execution job out of the cloneable `ReadRequest` enum. Proceed once both prototypes demonstrate the required behavior.

### 2. Treat both streams as one session

**Before:** A and B use one persistent block-sync stream inside their QUIC connection. Requests, blocks, ending messages, and `Status` share that stream. There is one stream to select, track, and replace.

**After:** A and B still use one QUIC connection. Block sync uses two persistent ordered streams: requests on one, blocks and control messages on the other. Treat them as one pair with one session identity. Poll each stream's reader and writer independently so waiting on requests cannot stop response processing.

#### Stream setup

Splitting the stream creates setup and ordering cases that need explicit rules:

| Situation | Before | After |
| --- | --- | --- |
| Both peers open block sync | The existing selection rule chooses one stream. | Apply that rule to the whole pair; reject duplicate roles and mismatched pairs. |
| Setup is incomplete | One stream must be accepted. | Activate only after both roles are accepted; abandon incomplete pairs after a bounded deadline. |
| A stream ends normally | Retire that stream's session. | Retire both streams and reopen with a new pair identity; leave unrelated services usable. |
| B sends `Status`, then a request | The ordered stream delivers `Status` first. | The request can arrive first on its separate stream. Hold at most one decoded request until valid `Status` arrives, under the setup deadline. Arrival order alone must not penalize B. |

Negotiate a new service version before using this layout. Choose unused identifiers and document the bounded setup encoding. Scope the pair identity to its connection and opener, separately from the generic prelude's `request_id`. The existing `RequestResponse` mode opens a stream per request; use persistent ordered streams here.

#### Current session tracking

**Before:** The service maintains an `active_peers` admission map and queues individual connection/disconnection events for the reactor.

**After:** Make that map the authoritative table of current sessions. Replace the event history with a watch notification meaning "the table changed; check its current state."

**Example:** B's session 7 is replaced by session 8 while the reactor is busy. Both designs cancel session 7 directly. Today the reactor processes queued lifecycle events; after the change, it reads session 8 from the current table and reconciles its state. Repeated replacements must not accumulate a queue of obsolete events.

Preserve readiness signals, download cleanup, and peer counts, including changes arriving during reconciliation. Keep the existing session-identity checks: delayed requests, results, or cleanup from session 7 must never affect session 8.

#### Resource limits

Adding a stream must not double the peer's allowance or let reconnects accumulate unfinished work:

- Enforce small bounded request queues and bounded data queues. The `ServicePeerLimits` queue-depth and pending-escalation fields currently do not enforce limits; wire the actual queues and admission checks. Count a reader-held frame separately from the raw queue and the serving task's decoded request.
- Count sessions being established and tasks being retired against admission limits until they finish. Replacing a table entry does not mean its old work has ended.
- Keep storage workers and outgoing frames charged to response permits across session replacement. Retain the weak per-identity permit registry and prune expired entries so reconnects cannot bypass the peer limit.
- Share the existing block-sync message budget across both streams. Check message roles and payload limits before allocation, preserving malformed-message checks and the nine-byte `GetBlocks` payload limit: 17 bytes with framing. Account for pair setup separately.

Bound paired data writes to 32 seconds. Request writes follow the cancellation-aware policy under Proposal adjustments. Session setup and unrelated services retain their existing deadlines.

### 3. Move serving into a sequential task

**Before:** A's peer routine admits B's request for block 100 and forwards it to the reactor. The reactor asks the node driver to read storage, then handles the result and queues the response. Serving state is coordinated across these components.

**After:** A's serving task waits for capacity, calls the storage adapter, and queues the response on B's data stream. The reactor no longer coordinates those serving steps. In the overload example, waiting to serve B's extra request does not stop A's data reader from receiving block 200.

#### Request flow

For each request, the session's serving task:

1. Validates the request and session readiness.
2. Acquires the peer permit, then waits for the node permit. This prevents an old response from causing its peer to hold extra node slots. Keep FIFO waits and make both waits cancellable.
3. Rechecks the session, captures its exact sender and cancellation handle, and snapshots the serving range and advertised count/byte limits.
4. Dispatches the bounded storage operation exactly once through the injected adapter.
5. Reserves an output slot before encoding each block. Run at most one encode at a time per response, outside the reactor on the bounded blocking path. An encode retains its permit even if its async caller disappears.
6. Sends the available bounded prefix and its ending message in order on the captured session. Wait for queue capacity instead of truncating the response because the queue is full. Preserve existing storage-error and unavailable-range behavior.
7. Drops the task's ownership after queuing the ending message. Each queued or partly written frame keeps a `FrameGuard`, which retains the permit through the actual application write or discard. The next request waits for that peer's permit to become available.

Do not wait for every ownership reference to disappear while the task still holds one itself. Do not find a replacement sender by peer ID after an asynchronous wait. `Status` may interleave only where the existing protocol allows it.

#### Cancellation and deadlines

**Before:** `query_timeout` ends the response when the storage deadline expires, even if the database is still reading. The driver retains its lease while waiting for that read to finish.

**After:** Remove this serving-query timeout and its generated completion. A slow read can finish normally while the session remains active. Session cancellation stops delivery and can prevent later lookups, but a running database call keeps its permit until it exits. Ordinary download deadlines, including floor rescue, still apply.

Remove the obsolete timeout configuration and tests. If the field has shipped, provide an explicit configuration migration.

#### Production wiring

Wire the adapter through real node startup: `zakurad` constructs it, network initialization carries it, and `handler.rs::spawn_zakura_endpoint_inner` passes it into block sync. Updating test constructors alone would miss production.

### 4. Connect the outgoing request queue

**Before:** When A requests block 200 from B, it reserves the download work, tries to enqueue `GetBlocks` on the shared stream, and records the queued request as outstanding. A failed enqueue returns the reservation still owned by that attempt.

**After:** Use the dedicated request queue and reserve its capacity first. If it is full, A creates no new outstanding request and continues reading responses on the data stream. `Status` and responses use the data sender.

#### Queue admission

1. Reserve a request-queue slot without waiting.
2. Validate the current work authority and session, then publish the outstanding request and its exact ownership and byte reservation.
3. Transfer the request into the reserved slot under the same ownership protocol.

Avoiding an asynchronous wait is not atomicity: another task can reset the work concurrently. Reuse the existing synchronization where possible and document where publication takes effect relative to reset. The ownership argument must cover reset before/during publication, enqueue failure, writer startup, and response arrival. Each reservation settles exactly once, and a stale attempt cannot release work now owned by another request. Do not hold a lock while waiting on network I/O.

Before writing the first byte, the writer must atomically claim the queued request against expiry, reset, and session replacement. If invalidation wins, discard it unwritten. If the writer wins, it owns completion of that frame under the rule below. A separate check followed by an unprotected write is insufficient.

On enqueue failure, return only work and bytes still owned by that attempt. Preserve the existing exact bitmap, expected hashes, late-response handling, reset accounting, and duplicate-issuance protection. The change is where and when requests enter the transport; their response-matching rules stay intact.

#### Request expiry

Keep download deadlines measured from queue admission. Because request writes can wait under serving pressure, the writer must distinguish these cases:

| Request state when it expires | Required action |
| --- | --- |
| Still queued; writer has not claimed it | Atomically invalidate it against writer startup, then discard it using its exact owner and session. |
| Writer has claimed it, including a partial frame | Finish the frame while its session remains valid. Deadline expiry alone does not abandon a started frame. If the write is cancelled, reset both streams before another request can be sent. |
| Fully written | Apply existing timeout and late-response settlement rules. |

Session cancellation or a reset that invalidates an active write cancels that write and resets the pair. Never drop an unfinished frame and append another request: the peer would interpret the new bytes as part of the old request.

### 5. Remove the old path and finish integration

The new task replaces the following serving machinery. Remove each component only after its last consumer has moved:

| Old component | Replacement |
| --- | --- |
| Routine serving queue, pending admission, and `ServeGetBlocks` forwarding | One decoded request waiting in the session's serving task |
| `QueryBlocksByHeightRange`, `serve_block_range`, and serving result/completion events | Storage adapter returns an owned result directly to the serving task |
| Reactor serving record and `pending_serving_terminals` | Response permit owned by the task, plus the ordered data queue |
| `BlockRangeQueryLease` and its execution mutex | Database job that owns the permit and guarantees one dispatch |

Keep download IDs, response guards, shared regulation policy, and unrelated apply/verification work.

#### Activation and compatibility

Activate the new capability only when the requester, responder, and pair teardown work together and the transport acceptance gate has recorded passing results. Keep it disabled while any required result is missing or failing. Remove temporary migration adapters afterward.

Test old-only, new-only, and mixed peers through negotiation and existing fallback. The new layout must never be interpreted as the old version. If older native block sync must remain supported, that requires a separate version and overload policy.

#### Documentation and tests

Update the stream specification, GetBlocks design, parameter ledger, configuration examples, public API docs, and existing `docs/changelog/unreleased/892.md` entry. Adapt #896's model, production tests, trace/replay format, regression seeds, and test filters together. Preserve the meaning of old failing histories through an explicit replay version or conversion. Update #747 to describe pausing request intake while response processing continues.

Use the one-way download and request-pressure cases below when adapting the existing serving regressions. Preserve coverage of admission bounds, output congestion, ownership, and cleanup.

## Resource limits

Every resource below must remain bounded during overload, cancellation, and reconnects.

| Resource | Required bound |
| --- | --- |
| Session records and session tasks | Admission covers current, establishing, and retiring sessions |
| Waiting serving requests | One decoded request per admitted session; separately bounded raw queue and transport-reader-held frame |
| Active response ownership | One per authenticated identity and 64 per node by default, including obsolete-session work |
| Storage and encoding | One storage dispatch, one retained result, and at most one active encode per response |
| Outgoing frames | Bounded queues plus writer-held frames; response count/byte limits and guards remain in force |
| Identity budgets | Current sessions plus identities retaining response ownership; prune expired weak entries |
| Transport buffers | Explicit stream, connection, and connection-count limits, including setup and other services |

One decoded waiter does not mean only one request's bytes are retained: the raw queue and QUIC buffers also count. Likewise, 64 response permits are not a process-memory limit.

**Memory example:** At an illustrative 32 MiB per response, 64 responses represent 2 GiB before decoded objects, encoding temporaries, transport buffers, and other node work. This response budget is separate from QUIC's connection window. Measure the supported memory envelope.

The limits contain load; they cannot guarantee service under unlimited identities or make a hung database return. Track active/waiting work, queue depths, retiring sessions, and the oldest response age. Keep metrics limited to live bounded state and avoid peer-ID labels.

## Validation

Run these checks through the production transport and serving path, with controlled storage where necessary:

| Scenario | Required result |
| --- | --- |
| A downloads blocks from B | Requested blocks and the ending message complete and settle A's actual outstanding request |
| A's serving capacity is occupied; test peer B sends extra requests while supplying A's download | A's download completes and B's extra requests remain bounded |
| Request writes remain blocked beyond ten seconds | Useful responses continue without a timeout disconnect breaking the wait; data-write stalls still follow their deadline |
| Other services pause; connections experience loss or reopening | Ready response readers and writers make progress within declared transport bounds |
| All 64 response permits are held | Extra requests stay bounded and dispatch no extra storage or encoding work |
| A read lasts beyond eight seconds or its caller is aborted | No query-timeout completion; the real worker retains its permit until it exits |
| The output queue is full | Encoding waits for a reserved slot; the bounded response and its ending message stay ordered |
| Cancellation at admission, read, encode, queue, or partial-write boundaries | Resources settle once; reconnects cannot bypass the peer limit or receive old-session output |
| Pair setup is reordered, duplicated, incomplete, or replaced | One complete pair is selected; setup and teardown remain bounded |
| Session replacement while reconciliation is paused | Records and tasks plateau at their limits, then converge to current state |
| Request expiry, send failure, reset, rejection, or late response | No unqueued outstanding request, duplicate issuance, or double release |
| Expiry/reset races the writer's initial claim | Invalidation wins and no bytes are written, or the writer wins and finishes the frame unless cancellation resets the pair |
| Reset races publication, enqueue failure, or response arrival on another task | Exactly one settlement per reservation; old owners cannot release replacement work |
| Invalid roles, oversized headers, malformed ranges, response-cap edges, and tighter transport caps | Reject before expensive work; preserve valid empty/partial response behavior |
| Old/new peers and configuration migration | Explicit version selection and working fallback |

Keep tests that deliberately release a guard early or accept stale ownership and verify that the suite catches the mistake. Start stream workers before sending large bursts in the new harness. Require completed downloads; the existing serving reproduction consumes blocks as stale and counts queries, so it is not evidence of matched-download completion.

### Performance comparison

Compare baseline and final behavior under the same workloads: normal sync, downloading from B while serving other peers, slow readers/storage, large blocks, reconnects, and delayed/lossy links. Measure the synthetic request-pressure case separately from ordinary sync. Record completed requests, useful bytes, latency, CPU, and peak memory against the transport gate's thresholds, fixed before measuring the prototype.

### Required checks

During implementation, run focused network regulation, block-sync, transport, state, and node-driver suites. At the combined head, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo nextest list --profile regulation-properties --locked
cargo nextest run --profile regulation-properties --locked
cargo nextest run --profile zakura-integration --locked
```

The `regulation-properties` profile comes from #896 and must be adapted before use. Confirm the intended tests actually run. Preserve #933's transport fixes and the current QUIC crate-family pin and lockfile. Run the relevant documentation/configuration checks too.

## Code map and baseline

Network paths below are relative to `crates/zakura-network/src/zakura/`.

| Area | Main targets |
| --- | --- |
| Stream pair, queues, and write policy | `handler.rs`, `handshake.rs`, `transport/` |
| Serving task and permits | New `block_sync/serving.rs`, existing `block_sync/serving_regulation.rs`, `regulation/request.rs`, `regulation/slots.rs` |
| Sessions, downloads, and old serving removal | `block_sync/service.rs`, `block_sync/peer_routine.rs`, `block_sync/reactor.rs`, and their state/event modules |
| State API | `crates/zakura-state/src/service.rs` and a focused serving module if needed |
| Production adapter and wiring | `crates/zakurad/src/commands/start.rs`, `crates/zakurad/src/commands/start/zakura/`, and `crates/zakura-network/src/peer_set/initialize.rs` |
| Tests and wire contract | `handler/tests/serving_progress.rs`, block-sync/transport tests and testkit, #896, and a proposed `docs/specs/blocksync/stream-pair.md` |

The source baseline is [#892 at `5d5c8abc8f5b`](https://github.com/zakura-core/zakura/tree/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c), inspected September 8, 2026. Its base is [the transport fixes at `53ccc72b5dcb`](https://github.com/zakura-core/zakura/tree/53ccc72b5dcb65713ee4af17216432d1fcfdb134), with the QUIC family pinned to `1dcc7a43488fecd199d343d47e93e9ed8319fcaa`. Refresh these before implementation.

Useful source anchors:

- [Current GetBlocks design](https://github.com/zakura-core/zakura/blob/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c/docs/design/getblocks-regulation.md).
- [QUIC flow control](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.1), [Zakura's window defaults](https://github.com/zakura-core/zakura/blob/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c/crates/zakura-network/src/zakura/handler.rs#L151-L157), and [the pinned window configuration](https://github.com/zakura-core/iroh-quinn/blob/1dcc7a43488fecd199d343d47e93e9ed8319fcaa/quinn-proto/src/config/transport.rs#L97-L118).
- [Actual blocking state dispatch](https://github.com/zakura-core/zakura/blob/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c/crates/zakura-state/src/service.rs#L3523-L3529).
- [Ordered-write timeout](https://github.com/zakura-core/zakura/blob/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c/crates/zakura-network/src/zakura/handler.rs#L4508-L4537) and [unenforced queue fields](https://github.com/zakura-core/zakura/blob/5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c/crates/zakura-network/src/zakura.rs#L74-L115).

Download-index optimization, stricter overlap/response validation, regulation of other services, and broader fairness or process isolation remain separate work. This refactor preserves existing download semantics unless a concrete migration dependency requires an explicit design decision.
