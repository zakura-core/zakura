# Transparent UTXO resolution

## Status

The immediate change overlaps up to 64 state lookups per block transaction.
It preserves input order, known block outputs, error translation, and serial
mempool lookup behavior. Futures are created only as the window advances.
The existing per-request timeout starts after service readiness. Overlap can
make missing dependencies time out together during out-of-order verification;
recovery still relies on the existing sync restart.

The architecture below is proposed follow-up work. The window removes a serial
latency dependency; it does not reduce the number of database reads or establish
a node-wide resource bound. It is not a complete denial-of-service defense.

## Current path

`Verifier::spent_utxos` in `crates/zakura-consensus/src/transaction.rs` resolves
block-internal outputs from `known_utxos`, then sends `AwaitUtxo` for other inputs.
The corresponding handler in `crates/zakura-state/src/service.rs` checks queued
and recently sent blocks before using `ReadRequest::AnyChainUtxo`.

The read service schedules a blocking job for each such request. Its
`read::any_utxo` checks non-finalized state, then `ZakuraDb::utxo` resolves a
transaction hash to a location and reads the output at that location.
Consequently, many inputs from one source transaction repeat the transaction
location lookup as well as service dispatch and blocking-job overhead.

The block verifier already polls transaction checks concurrently. Multiplying
that concurrency by a per-transaction window does not create an aggregate bound.
Tower buffer capacity bounds queued dispatch, not the lifetime of work after
it leaves the buffer.

## State-owned resolver

Introduce a bounded batch UTXO request owned by `zakura-state`. Consensus
provides the outpoints it still needs and keeps their original input indexes.
State resolves a bounded chunk in one scheduled job, returning a keyed result
that consensus maps back into input order. Keep existing duplicate-spend checks
before resolution; internal deduplication must not make duplicate inputs valid.

Resolve queued and non-finalized outputs first. For remaining finalized-state
reads, group by transaction hash, obtain each transaction location once, then
batch the output-location reads. Add typed batch reads to the existing column
family abstraction; a loop of individual `get` calls only amortizes scheduling,
whereas a storage batch can also reduce database overhead. Keep database errors
distinct from absent outputs.

Use bounded chunks with limits on keys and retained result bytes, including
scripts. A maximum count of outpoints alone is insufficient. Schedule another
chunk only after admission, and account for accumulated results retained by
transaction verification as well as the active chunk.

Start with transaction-scoped batches. Move resolution to a block-wide prefetch
only if measurements show meaningful further savings; it must stream bounded
chunks instead of creating another full-block copy of all outputs.

## Resource ownership

One shared admission controller owns lookup capacity across all callers and
service clones. It accounts separately for queued keys and bytes, active disk
jobs, retained result bytes, and missing-output subscriptions. The state-owned
controller is the enforcement point even when work arrived through RPC, legacy
networking, or native Zakura networking.

Use resource classes for chain progress, mempool admission, and serving. Reserve
capacity for chain progress and schedule bounded chunks fairly within each
class. Peer and source-group limits can improve fairness but must share the
same global ceilings; creating new identities must not create more capacity.
Local backlog also needs accounting before it becomes a state request.

Disk-work permits live until the blocking job actually exits. Dropping an async
receiver does not necessarily stop an already running blocking job. Result
memory remains charged while a consumer retains it. Release unused reservations
on cancellation, error, or timeout without refunding completed work.

Missing dependencies must release disk execution slots before waiting. Track
those waits under a separate bounded subscription budget and deadline. Preserve
capacity for ancestor verification and commit notifications, so descendants
waiting for outputs cannot hold the resources their ancestors need to progress.
Admission failure is a retryable scheduling outcome, not evidence that a block
is invalid. These limits must delay valid work rather than introduce new
transaction or block consensus limits.

## Chain semantics

Keep block and mempool resolution as distinct request modes. Block verification
can obtain output data from any non-finalized chain, with contextual spend and
ordering checks at commit. Mempool verification requires best-chain unspentness
and can subsequently depend on outputs from its mempool. A batch implementation
must preserve these contracts rather than substitute one for the other.

For coherent batch reads, pair the non-finalized view with the finalized database
snapshot and its boundary. Check that they describe a compatible state, or retry
if finalization moved that boundary. Taking two independent snapshots does not
by itself establish coherence. Carry a chain generation with mempool results
and use the existing tip-change re-verification path when that view changes.

Register missing-output subscriptions before checking availability, then
recheck under the state service's serialization rules, to avoid losing a commit
notification between a failed read and registration. Cancellation removes the
subscriber and eventually the last unused entry without an unbounded cleanup
backlog. Coalesce identical reads only within compatible request modes and
views; the subscriber list itself must also have a bound.

## Caching

Batch-local reuse of transaction locations is the first cache to add. A bounded
positive cache may later retain immutable output contents, but it cannot answer
whether an output is currently unspent or belongs to the relevant chain.
Negative results must not survive a state transition that could create the
output. Reorganization and finalization semantics need explicit tests.

Transparent script-result caching is separate work. It can save repeated CPU
verification, but it cannot eliminate current UTXO membership and contextual
checks. Any reuse must bind the exact authorizing data, applicable verification
rules, and trusted prevout context, and cache only successful checks.

## Delivery and validation

1. Ship the bounded overlap change with deterministic mock-state tests for the
   window, output ordering, known-output bypass, errors, and timeout behavior.
2. Add transaction-scoped state batches and coherent storage reads, comparing
   their results against scalar lookups across queued, finalized, non-finalized,
   and mempool-dependent cases.
3. Add shared resource accounting and fair scheduling before increasing overall
   verification concurrency. Test permit conservation, canceled blocking jobs,
   dependency progress, and bounded queues with deterministic synthetic jobs.
4. Measure realistic consolidation workloads on an isolated database fixture.
   Compare warm and cold reads, one transaction and many concurrent transactions,
   and mixed sync/mempool/serving work. Select batch sizes and global budgets
   from memory, tail latency, and chain-progress measurements rather than copying
   the immediate patch's value of 64 into a global default.

Record queue delay, active jobs, lookup keys, retained bytes, pending
subscriptions, cache hits, and completion latency under `state.utxo.*` metrics.
Acceptance requires bounded aggregate usage and continued ancestor/commit
progress under saturation, unchanged validation decisions and input ordering,
and no material normal-load latency regression. A latency improvement in one
transaction is not sufficient evidence for the architecture.
