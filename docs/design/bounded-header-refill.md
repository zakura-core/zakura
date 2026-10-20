# Keep the body backlog supplied with bounded header refills

## Goal and evidence

Keep ordinary checkpoint body downloads supplied near the existing 4,000-header window as execution becomes faster.
Preserve all memory and ownership bounds.
Report full body backlog separately from the 401 submitted verifier operations.

The completed combined-stack trace at heights 1.4M–1.5M averaged 2,685 contiguous apply bodies and 48 reorder bodies.
Header lead averaged 2,856.
Thus about 1,144 slots below 4,000 had no admitted header, while only 124 admitted headers lacked downloaded bodies.
The 2,000-block header refill threshold caused most of this ordinary-workload deficit.
At heights 1.8M–1.9M, header lead still averaged 2,856 but downloaded backlog averaged only 217.
That large-body delivery deficit requires separate congestion and bandwidth work.

A full submission window does not establish that the full backlog stays near capacity.
A fuller backlog also does not establish a throughput gain.

## Memory findings

Active headers are not exclusively lazy disk reads.
`HeaderChainRuntime` holds an `Arc<Mutex<HeaderChainEngine>>`.
The engine loads the retained graph into `MemHeaderStore`, which holds header nodes and graph indexes in memory.
Each `HeaderNode` retains an `Arc<block::Header>` and associated metadata.
Requester pages also occupy memory while receiving, preparing, and applying.

Keep these bounds:

- Admitted header lead plus outstanding/staged header claims: the existing 4,000-header integrated window.
- Aggregate requester reservations and owned entries: the existing 4,000-header chunk budget.
- Durable non-finalized graph: the existing 65,536-node limit, including competing paths.
- Per-response wire/byte limits and per-peer negotiated limits.
- Existing body resident-memory, reorder, and reservation budgets.

Do not increase header lookahead without measuring retained graph and auxiliary-state memory.
Count limits do not prove a fixed process RSS bound.
The experiment must measure RSS, body memory, and graph growth.
This change raises average utilization inside existing bounds; it does not enlarge those bounds.

## Implementation

1. Reopen ordinary refill when the integrated window has room for one maximum checkpoint range plus its successor: currently 401 headers.
   With a 4,000-header window, the refill threshold becomes a lead of 3,599 rather than 2,000.
   Coalesce smaller credit returns to avoid one-header durable transitions during bulk sync.
   The bound applies to the window rather than to the grant.
   Outstanding claims can still leave less than a batch free, and the smaller top-up keeps the backlog supplied.
   Allow the existing final partial-target exception near the tip.
2. Publish a normal selected-chain extension once it contains that checkpoint-sized batch.
   Do not wait for the body backlog to drain or chase credits freed during receipt.
   A wire page can take the batch past the minimum; existing reservations still bound the result.
   Preserve full preparation and durable admission before exposing headers to bodies.
3. Reconsider cached peer targets when a committed snapshot reopens refill capacity.
   Verified-body progress must wake refill even without a new header generation, finality anchor, or peer status.
   Trigger on closed-to-open capacity transitions rather than polling or scheduling a locator after every commit.
4. Preserve existing exact branch, repair, durable-headroom, and shared-budget checks.
   Forks and auxiliary repair do not use the selected-extension early-publication shortcut.

The refill threshold bounds intentional slack.
It cannot eliminate network, proof-validation, or durable-writer latency.
After capacity reopens, expected additional depletion is approximately body consumption rate multiplied by refill latency.

## Tests

- Capacity boundaries: full window, 400 free slots, 401 free slots, and overfull snapshots.
- Shared claims: reservations and staged entries reduce headroom without overflow.
- Final partial target: a one-header suffix remains reachable.
- Moving body progress: a completed batch stays publishable as commits free more headroom.
- Action path: two 250-header pages publish a 500-header prefix while thousands of bodies remain ahead.
- Event path: verified progress reopens cached work without a status refresh or reanchor; further progress does not duplicate locator work.
- Existing durable graph, cancellation, fork, repair, and negotiated continuation tests.

## Stack placement

Stack this change on #1032, which preserves negotiated continuation capacity.
This change supersedes #1016's 2,000-header selected-refill publication threshold.
During integration, keep the bounded early-refill policy and preserve #1016's moving-headroom regression intent.
Do not stack both policies as independent gates.
All other combined-stack performance and liveness changes remain independent of this PR.
The current genesis baseline keeps its existing binary.

## Experiment plan

After the current genesis baseline completes:

1. Integrate this change with the verified combined stack in a separate test branch.
   Resolve the #1016 overlap explicitly.
   Run the relevant header tests on that integrated source.
2. Compare matched ordinary-block ranges on the same node and storage.
   Keep suppliers, release features, UDP buffers, tracing, and body/apply parameters the same.
   Preserve both binaries, manifests, configuration files, and traces.
   Do not change a running comparison arm midway through its interval.
3. Record process-clock timestamps for snapshot publication, locator query, header request/response, preparation, durable admission, body-work extension, and first body send.
   Use existing events where they cover a stage.
   Add only missing stage timing before attributing residual latency.
4. Record time-weighted apply queue, unsubmitted contiguous bodies, reorder bodies, header lead, header claims, and header-to-body gap.
   Record NIC traffic, UDP/socket drops, request windows, CPU, RSS, retained body bytes, and header graph size.
   Distinguish admitted headers from response pages that remain staged.
5. Report checkpoint-only throughput separately from semantic verification and readiness holds.
   Include the full backlog p05/p50/p95 and time above 3,600 downloaded bodies.
   Break out large-body intervals where memory or delivery limits prevent a full backlog.

### Acceptance gates

- Every request remains within available shared claims, durable headroom, and wire limits.
- No missing body/commit terminal coverage, new steady-state errors, or unbounded retained graph growth.
- In ordinary steady-state ranges with healthy delivery, header lead has p50 at least 3,700 and p05 at least 3,400.
- Downloaded backlog rises materially toward that lead; unexplained header-to-body gaps block acceptance.
- RSS remains within the existing 8 GiB experiment guard and stabilizes across repeated refills.
- Report header transitions per block and CPU cost explicitly.
  Smaller refills can increase durable transition frequency by roughly fivefold.
- A checkpoint throughput regression above 5% in repeated comparable ranges blocks acceptance pending diagnosis.
  Do not claim success solely because backlog occupancy rises.

### If the queue still drains too far

Attribute each missing interval to one stage before changing another limit:

- No admitted header and no request: inspect credit-return notification and target scheduling.
- Request active: measure supplier/network response latency and negotiated page limits.
- Pages received but not published: inspect prefix completion, proof preparation, and durable-writer delay.
- Header admitted but no body request: inspect body producer queries and requester admission.
- Bodies requested but absent: inspect congestion windows, UDP loss, suppliers, and bandwidth.

If durable header transitions become the bottleneck, optimize or overlap bounded preparation/admission before expanding lookahead.
Do not assume that arbitrarily early headers are free because they also exist on disk.

## Locator operation ownership

Ordinary target slots and locator reads have separate lifetimes. Each reactor has
16 locator permits, using `MAX_STAGED_TARGETS_V1`. Dispatch acquires a permit
before creating a locator future. Eviction, disconnect, session replacement and
header authority changes can retire a target without releasing its read's permit.
A repeated offer from the same peer coalesces with its existing query.

The port contract transfers a clone of that permit into the state read request.
The blocking state closure owns it through completion or unwinding. Reactor and
adapter timeouts drop their callers, but cannot release the clone held by accepted
state work. Thus the sum of pending locator operations and orphaned accepted reads
is at most 16 within a reactor's capacity domain, even under identity churn.
Shutdown drops unaccepted futures; accepted closures retain their permits until
exit. This is a per-reactor bound, not a process-wide bound across independently
constructed reactors.

At exhaustion, useful offers remain unspent. The reactor waits on the semaphore
and immediately schedules cached offers when the last owner releases a permit.
A permanently stuck accepted operation permanently occupies its permit; a timeout
cannot safely manufacture replacement capacity. Read failures still consume their
ordinary attempt, as required by the status-offer policy.

## Cached-offer scheduling opportunities

The end-of-event pass first compares five constant-size inputs: the root policy
revision, queue ownership revision, claimed header count, available locator permits,
and committed resource-stall state. Unchanged inputs skip peer traversal, offer
clones, sorting and duplicate/headroom checks. The existing deterministic priority
order and immediate end-of-event scheduling remain unchanged.

Every Status is saved and considered synchronously for its sender, including a
fresh attempt or changes to target, eligibility and priority. A still-blocked
sender's changed offer does not itself create an opportunity for another peer;
it therefore does not invalidate the global pass. This rule does not depend on
whole-Status equality or an allowlist of ignorable fields.

The root revision changes for session membership, explicit offer deferral,
committed snapshots (including body-only advancement), every terminal subscription
outcome, a pruned schema 1 round that re-arms its sender's status, and changes to
an existing locator's effective virtual header claim. Header sync v9 holds an offer
whose session still has a live subscription until that subscription ends, so the
terminal outcome is an opportunity. The latter uses the same negotiated,
fair-share-capped count as ordinary headroom. Changing a limit above that cap,
nonzero slot counts or other metadata does not change the claim. The queue revision
changes on insertion, removal, target-hash replacement or an awaiting request
becoming active. Updated metadata and priority are still retained, but do not
invalidate other peers' offers by themselves. No-op removals do not change it.

Capacity counters are read directly because the last RAII lease can be dropped
outside a queue mutation. A watch notification wakes the reactor on final
header-capacity release; locator permits use their semaphore wakeup.

The cached key is captured **before** scanning. A release during the scan must
remain visible to the next pass; caching the post-scan counters could swallow that
wakeup. A pass that changes its own queue can cause one further scan, after which
unchanged blocked offers are quiescent again. Real shared opportunity changes
retain the existing bounded sorting cost. This does not optimize serving
notifications or other independent per-peer reactor work.

The deterministic work test uses 16, 64, 256 and 512 admitted peers, covering the
configured 256 inbound plus 256 outbound slots. It holds duplicate-target or
zero-headroom offers across 300 end-of-event passes, including repeated blocked
and active-owner Status messages. It asserts zero additional cached-offer peer
visits after the initial pass. This measures eliminated scheduler work, not
production throughput or a saturation threshold.

The test also clears only the scheduling cache before each pass as a control,
replaying the previous unconditional scan with identical offers and scheduling
logic. Each full pass writes a `header_cached_offer_pass` trace row with the peers
it walked, and the test counts visits from those rows. Expected additional peer
visits over 300 passes are:

| Admitted peers | Unconditional control | Opportunity tracking |
| --- | ---: | ---: |
| 16 | 4,800 | 0 |
| 64 | 19,200 | 0 |
| 256 | 76,800 | 0 |
| 512 | 153,600 | 0 |

Both the duplicate-target and zero-headroom workloads assert these counts.

Additional metadata-churn regressions hold 65 peers across 64 Status updates.
Changing only `oldest_retained_height` previously caused 4,160 extra peer visits
for each of duplicate blocking, zero headroom and an awaiting-locator owner.
Each now asserts zero extra visits, and also checks changed nonzero slot counts,
priority and limits that leave the effective claim unchanged. Separate tests
check that reducing a pending claim wakes an alternate, retargeting releases the
old target, and a newly eligible Status starts its sender immediately.

## Cached-pass budget

The opportunity key cannot tell a real opportunity from one peer churning its
own unstarted target. Flipping that target between two hashes changes the queue
revision on every `Status`, through a retarget or a removal and a fresh
insertion. Each change would buy a full pass, which clones and sorts every idle
offer.

So full passes draw from a token bucket of 32 that refills one pass per
millisecond (`CACHED_PASS_BURST`, `CACHED_PASS_REFILL`). The refill clock
restarts when a pass ends, so time spent scanning earns no refill. Otherwise a
pass that took a whole interval would pay for the next one, and passes could run
back to back. Past the burst, a pass of cost P takes at most P / (P + 1 ms) of
the reactor. A changed key whose
pass finds the bucket empty stays uncached, and the refill joins the
maintenance deadlines. The deferred opportunity is therefore scheduled at the
refill without a new `Status`. The sender's own offer is still considered
synchronously, so only cross-peer opportunities wait, and only while something
changes the key more than 1,000 times a second. Unchanged keys spend nothing.
