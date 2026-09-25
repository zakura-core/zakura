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
