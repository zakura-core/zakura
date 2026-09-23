# Verification waterfall plan

Status: approved with high confidence by three independent reviewers (measurement, recording/storage, and UI/operations). All three reviewers also approved the integrated implementation with high confidence after cross-review. Deployment remains gated on the validation below. Keep main base d4997d9dd43f269008b85542d708344b9acbb54e, profiling branch only, CPU sampling off, existing 100 GB retention. No replay or rewriting historical timings.

## Goal

Explain which verification components and waits make a block slow, with enough workload and cache context to choose an optimization. Instrument elapsed intervals without changing verification decisions, scheduling, batching thresholds, fallback behavior, caches, or cryptographic algorithms.

## Measured boundaries

New recordings declare a verification-detail version. Each Sapling, Orchard, and Ironwood request records pool and workload (Sapling spends/outputs, Orchard/Ironwood actions, request counts). Record cache hit/miss/unknown explicitly. A cache hit has no fabricated verification duration.

Carry an explicit request capture with the verifier Item across service/worker boundaries, preserved by fallback clones. Fix the existing request wrapper to poll the verification future inside its request span context. Ambient worker thread-local context is not an ownership mechanism.

Measure cache lookup; waiting for inner service readiness; admission wait from service submission until BatchControl::Item handling; synchronous bundle preparation/check_bundle/queue; formation wait from preparation completion until the flush request; flush scheduling from that request until actual worker submission; dispatch-to-worker-start; combined cryptographic execution; completion/publication and caller-result delivery. Use exact measured boundaries. Result publication and delivery use explicit timestamps shared with waiters, not a subtraction guessed from the outer envelope. Admission wait may include channel backlog and batch-concurrency limits, so do not call it only batch formation. Include the normal and Drop-flush paths and individual fallback. Record cancellation/error separately from success; dropping a timer is not a success signal.

Key initialization, where present inside a measured worker, is a separate setup phase. Other setup not covered by a phase remains explicitly uninstrumented. Fallback means individual retry after a primary batch/service failure, not necessarily an invalid proof. Cache identities, outcomes, ordering, and verification inputs are unchanged.

## Shared batch representation

Approved implementation: a run-unique batch ID and one projected batch record per participating block attempt, rather than a new global retention catalog. Projected copies identify the same actual execution and are never summed across blocks or charged entirely to one transaction. They include full batch workload, profiled/unprofiled participant counts, phases, status and coverage. Absence of a block context means unprofiled, not necessarily mempool.

Per-request detail references its primary and optional fallback batch IDs. Deduplicate each block's batch row by ID. Copy no per-member identity array into the wire. Collect at most one retained context per participating attempt, with a hard bounded collection and explicit omitted-link/coverage reporting. Reserving the projected span before completion keeps the existing attempt seal truthful, including work continuing after caller cancellation. Batch-only mempool work does not become an independent stored session.

Keep projection records in the existing attempt-owned immutable chunks so existing quota, expiry, crash recovery, and lookup bounds apply. Add optional fixed-size metadata to span records (or an equivalently bounded representation), preserving old JSON. Each new phase consumes the fine-span budget, never the state/finalization reserve. Use run-unique IDs that never get reused after omission. Missing projected batch or incomplete phase evidence stays explicitly unknown/partial, even if ordinary request spans are complete.

Batch captures must be bounded independently of attacker-controlled request/action counts. Cap retained attempt owners at 64 per batch, live batch captures at 256, and live request captures at 8192. Overflow cannot block or alter verification and must mark affected request coverage partial. Disabled instrumentation must avoid request/batch allocations, clocks and ID generation. Measure Event size and maximum JSON/chunk size. Preserve <64 MiB event queue storage and 8192-byte datagrams. Use 2048 events per chunk to preserve the existing 4 MiB decoded chunk bound; do not raise query memory limits silently.

## Explorer

Add an expandable Shielded verification section above the transaction list. Keep transactions collapsed and default to transaction-number sorting with longest-first available and explorer links. Show cache hit/miss counts with unknown counts, pool/workload, primary vs individual retry, and one row per batch. Cached-only blocks clearly say no fresh execution was recorded rather than zero proof cost.

Use one visibly aligned time axis across nested rows. Labels may indent, plot origins and widths may not. Distinguish preparation/setup, waiting, execution, and delivery using text and colors. Use microseconds below one millisecond. Full batch intervals may begin before block entry; extend the axis and label the block-entry marker rather than pretending that work began at zero. Do not change block timing totals by including earlier/shared work as exclusive block work. Existing verifier response and recorded-after-response semantics remain intact; shared batch projections are excluded from exclusive timing aggregates, including the server-derived recorded_end and after-response totals, not only frontend sums.

Show request-to-batch links and which shielded requests finished last. Do not claim a proven critical path, exclusive CPU time, savings from summing overlaps, or a per-transaction share of a shared batch. Missing timings, old uninstrumented profiles, and empty workloads are separate states. For legacy profiles, preserve existing rows and explicitly state that the new breakdown was not recorded.

## Deliberate exclusions

Pure proof-vs-signature timing requires additive hooks in the Sapling/Orchard libraries; their public validators currently combine both. Do not copy, split, or reimplement cryptographic verification to obtain timers. This version distinguishes setup, preparation, waiting, combined crypto execution and delivery. A full critical-path dependency graph, historical replay, CPU sampling, cache disabling and cross-version benchmark campaign are outside this rollout.

## Validation and rollout gates

Tests must cover: real successful cache hit and fresh verification; miss and service-readiness failure; multiple requests and two block attempts sharing one batch plus an unprofiled member; fallback after primary failure; cancellation before/after dispatch; Drop flush; lost/capped projection evidence; disabled instrumentation; old JSON/chunk compatibility; exact seal/span accounting; bounded event/chunk bytes; no double counting; pre-block intervals; microsecond display; unchanged transaction sorting and links.

Use deterministic existing proof fixtures to validate new fresh-execution phases. Do not depend on a live uncached block appearing, and do not flush the live cache. Test the production verification path against the same accepted/rejected fixtures and preserve results. Run focused recorder/explorer/consensus checks, frontend tests, formatting and Clippy. Review implementation against this plan before rollout. Any material design change returns to the plan reviewers.

Deploy with the existing update command and --keep-base only after all three reviewers approve the final plan with high confidence and required validation passes. Build first, stop node/drain collector, update collector/web before node. Verify new run capability/source fields, unchanged main base, disabled CPU sampler, new sealed block data, complete retained request/batch evidence, zero transport/collector errors and the public waterfall. Historical blocks may have no new fields. Do not open PRs or touch chain DB directly.

## Review outcome

Measurement, recording/storage, and UI/operations reviewers independently approved the plan and cross-reviewed the implementation. Review corrections exclude idle empty-batch lifetime, preserve separate flush and dispatch timestamps, avoid publication timestamps on abandoned work, retain partially prepared Sapling batch evidence, and mark cancelled requests partial. The implementation keeps verification algorithms, keys, cache identities, acceptance results, batch thresholds and worker scheduling unchanged.

The additions are one coordinated branch rollout because the node, collector, and explorer share an additive wire contract. They remain separated into recording, consensus, and explorer changes for review. No PR is opened for this trial.
