# Dogwood experiment report

The September 8, 2026 local experiments estimate subscription allocation under
congestion and compare parity with duplicate routes. They support testing
ordinary-delivery feedback alongside the challenge baseline. They do not
establish production congestion control, proposer seeding latency, or overlay
convergence. The [design](dogwood.md) explains the tradeoffs; the
[spec](../specs/dogwood.md#parameter-registry) owns parameter definitions.

The experiment source and raw results remain on the local branch
`local/dogwood-experiments-20260908`. PR #901 contains documentation only.
The local worktree is `zakura.dogwood-experiments`, alongside the docs worktree.
Its `docs/experiments/dogwood` directory retains the September 5 experiments
and adds the scripts and result directories named below.

## Congestion baseline

The model releases 400 synthetic 2 MiB bodies at 20.48 ms intervals, equivalent
to 50,000 transactions/s at 2 KiB each. Each body has 32 data parts and eight
parity parts. Four suppliers offer 800/400/200/100 Mbps of usable upload; the
receiver has 1,600 Mbps ingress. These are scenario inputs, not peer measurements.
Balanced suppliers each offer 375 Mbps. The 1 Gbps case changes only ingress.
The capacity-drop case reduces the 800 Mbps supplier to 50 Mbps halfway through
the run, leaving only 750 Mbps aggregate upload. The source-delay case adds
150 ms to upstream part availability without changing link capacity.

The half-load case uses 40.96 ms releases. The larger-body case uses 100 bodies
of 8 MiB at 81.92 ms intervals to preserve the offered byte rate. Every case
runs seeds 0–5 with all five policies. The model varies supplier availability
and part mapping with the seed. These finite traces are not a distribution of
real peer bandwidth or a reliable estimate of rare network tails.

The policies are static equal allocation, paired races, the existing budgeted
race controller, a candidate delivery-rate allocator, and an informed reference.
The candidate starts each connection at a 100 Mbps estimate. Every 100 ms it
uses at least four delivered parts to estimate bytes divided by the larger of
sender and receiver time spans. It excludes the first part's bytes and weights
the new sample by 0.5. Different sender clocks have constant offsets, which
cancel in span differences. The experiment assumes honest timestamps at the
start of idealized link service. Application transport-submission timestamps
may provide a weaker signal.

The candidate assigns each new distinct part to the smallest estimated
completion time: outstanding assigned bytes plus the new part, divided by the
estimated delivery rate. All blocks share that outstanding-byte count. Its
exact block assignments incur the modeled control delay. The informed policy
uses the same allocator with current supplier capacities, including the drop.
It knows information unavailable to the receiver; it is a comparison, not a
proven optimum. Neither allocator implements the complete spec controller.

Each link serializes parts and serves block queues in round-robin order.
The model bounds queues at 256 parts and models receiver ingress separately.
It charges a 384-byte proof/framing allowance per part, including the candidate's
eight-byte timestamp. It models 20 ms control delay and cancellation tails.
At 400 ms, repair requests can add at most `2k` copies. At 1,200 ms, an unfinished
block counts as fallback, not reconstruction. Queue drops discard modeled work;
the model does not simulate reliable-transport retransmissions.

The first comparison requests one copy of each encoded index outside challenges
and repair. It disables extra failure-coverage routes for every policy. Four
suppliers cannot satisfy the single-supplier-loss target with 25% parity and
no duplicates. This comparison isolates allocation and is not a conformant
coverage configuration. The parity comparison below restores that requirement.

### Results

The table reports means across six seeds. Body Mbps counts only reconstructed
bodies and divides by elapsed time through the last terminal block outcome.
Startup, drain time, and failed blocks therefore affect this metric. p95 is the
mean of each run's completed-block p95; misses and fallback include all blocks.
Wire/body includes delivered parity, duplicates, and late parts through drain,
relative to all offered bodies. It excludes separately recorded control bytes.

| Scenario | Policy | Body Mbps | p95 ms | Miss % | Fallback % | Wire/body |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| heterogeneous | equal | 792.0 | 422.0 | 53.2 | 0.0 | 1.117 |
| heterogeneous | races | 793.4 | 396.2 | 41.9 | 0.0 | 1.098 |
| heterogeneous | budgeted | 791.9 | 422.0 | 49.9 | 0.0 | 1.113 |
| heterogeneous | delivery_rate | 816.2 | 52.9 | 0.0 | 0.0 | 1.255 |
| heterogeneous | informed | 816.2 | 51.7 | 0.0 | 0.0 | 1.257 |
| balanced | equal | 816.2 | 51.6 | 0.0 | 0.0 | 1.257 |
| balanced | races | 816.2 | 51.6 | 0.0 | 0.0 | 1.259 |
| balanced | budgeted | 816.2 | 51.6 | 0.0 | 0.0 | 1.258 |
| balanced | delivery_rate | 816.2 | 52.0 | 0.0 | 0.0 | 1.257 |
| balanced | informed | 816.2 | 52.0 | 0.0 | 0.0 | 1.257 |
| ingress_1gbps | equal | 791.9 | 424.1 | 53.8 | 0.0 | 1.119 |
| ingress_1gbps | races | 792.0 | 424.0 | 51.8 | 0.0 | 1.117 |
| ingress_1gbps | budgeted | 792.0 | 424.0 | 51.8 | 0.0 | 1.117 |
| ingress_1gbps | delivery_rate | 799.5 | 273.4 | 0.3 | 0.0 | 1.238 |
| ingress_1gbps | informed | 803.4 | 265.2 | 0.0 | 0.0 | 1.240 |
| capacity_drop | equal | 505.4 | 1057.9 | 55.2 | 31.9 | 1.027 |
| capacity_drop | races | 508.6 | 1058.0 | 52.4 | 31.4 | 1.028 |
| capacity_drop | budgeted | 506.9 | 1057.9 | 52.5 | 31.6 | 1.027 |
| capacity_drop | delivery_rate | 575.1 | 1046.0 | 42.2 | 21.1 | 1.150 |
| capacity_drop | informed | 570.0 | 1034.1 | 40.5 | 21.7 | 1.155 |
| upstream_stall | equal | 786.6 | 423.7 | 88.0 | 0.0 | 1.202 |
| upstream_stall | races | 786.6 | 423.7 | 88.1 | 0.0 | 1.202 |
| upstream_stall | budgeted | 786.6 | 423.7 | 88.1 | 0.0 | 1.202 |
| upstream_stall | delivery_rate | 801.3 | 239.0 | 0.0 | 0.0 | 1.237 |
| upstream_stall | informed | 801.6 | 201.5 | 0.0 | 0.0 | 1.257 |
| half_load | equal | 409.1 | 63.6 | 0.0 | 0.0 | 1.187 |
| half_load | races | 409.3 | 62.1 | 0.0 | 0.0 | 1.262 |
| half_load | budgeted | 409.3 | 62.1 | 0.0 | 0.0 | 1.262 |
| half_load | delivery_rate | 409.3 | 52.9 | 0.0 | 0.0 | 1.257 |
| half_load | informed | 409.4 | 51.8 | 0.0 | 0.0 | 1.257 |
| larger_blocks | equal | 796.6 | 423.3 | 81.8 | 0.0 | 1.165 |
| larger_blocks | races | 796.6 | 423.3 | 75.3 | 0.0 | 1.162 |
| larger_blocks | budgeted | 797.2 | 423.2 | 69.8 | 0.0 | 1.161 |
| larger_blocks | delivery_rate | 819.0 | 85.4 | 0.0 | 0.0 | 1.251 |
| larger_blocks | informed | 819.0 | 84.6 | 0.0 | 0.0 | 1.257 |

The candidate improves allocation on unequal links in these traces. Equal
allocation already performs well on balanced links. The candidate also spends
more bytes by delivering more parity before cancellation. The 1 Gbps receiver
case accumulates queues and drops work even when reconstruction succeeds.
The capacity-drop case exceeds available service and produces substantial
fallback under every policy. A low completed-only latency cannot hide those
failures. These results do not select production parameters.

### Do sender timestamps explain the improvement?

A paired follow-up keeps the candidate allocator and all 42 case/seed inputs
fixed, but estimates rate from receiver arrival spans alone. It retains the
same per-part framing allowance to isolate the measurement change.

| Scenario | Sender-span candidate p95 ms | Receiver-only p95 ms |
| --- | ---: | ---: |
| Balanced | 52.0 | 52.0 |
| Heterogeneous | 52.9 | 53.1 |
| 1 Gbps ingress | 273.4 | 271.6 |
| Capacity drop | 1,046.0 | 1,024.1 |
| Upstream delay | 239.0 | 239.9 |
| Half load | 52.9 | 53.1 |
| Larger bodies | 85.4 | 85.2 |

Receiver-only fallback under the capacity drop is 21.4%, compared with 21.1%
for the sender-span candidate. Every other receiver-only case has zero fallback.
This test does not establish a timestamp benefit. The allocation rule explains
most of the observed improvement over equal assignment in this model. The
separate receive-compression test confirms that a sender span can guard against
an inflated sample, but the network traces do not establish its deployment value.

## Parity versus duplicate subscriptions

This comparison uses the same heterogeneous supplier rates and body workload.
It runs six seeds for each parity ratio and allocator. Every allocation must
retain at least `k` distinct parts after removing any one supplier's assignments.
The allocator adds duplicate routes where parity and assignment diversity do
not satisfy that test. Both policies use the same failure model and repair
deadline. The test does not model an actual supplier failure or independent
physical paths; it checks assignment coverage before measuring normal delivery.

Proposer seed time below is an analytic lower bound for uploading the entire
encoded codeword once over a 1 Gbps link. It excludes encoding, proofs, framing,
and direct duplicate requests. The simulation starts with exogenous availability
at suppliers and does not model a proposer. These are separate measurements;
their times must not be added as if this experiment measured a complete path.

| Parity/data | Policy | Proposer seed ms ≥ | p95 ms | Miss % | Wire/body | Duplicate/body |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 12.5% | equal | 18.9 | 445.9 | 90.8 | 1.413 | 0.351 |
| 12.5% | delivery_rate | 18.9 | 137.5 | 0.0 | 1.834 | 0.730 |
| 25% | equal | 21.0 | 421.8 | 44.3 | 1.248 | 0.164 |
| 25% | delivery_rate | 21.0 | 105.2 | 0.0 | 1.825 | 0.622 |
| 50% | equal | 25.2 | 97.1 | 0.0 | 1.123 | 0.000 |
| 50% | delivery_rate | 25.2 | 84.8 | 0.0 | 1.821 | 0.428 |
| 100% | equal | 33.6 | 75.9 | 0.0 | 1.360 | 0.000 |
| 100% | delivery_rate | 33.6 | 71.5 | 0.0 | 1.828 | 0.103 |

Duplicate/body counts delivered duplicate payload relative to offered body
bytes. More parity raises the proposer seeding lower bound. Fewer duplicates
can reduce receiver traffic, but concentration on fast suppliers can require
duplicates even with substantial parity. Cancellation also means delivered
wire/body can be below the full codeword ratio. No row establishes a network-wide
optimum. The next comparison must include a proposer with limited upload,
relay forwarding during seeding, direct proposer subscriptions, and concurrent
receivers. Relays can regenerate parity after reconstruction, but relying on
that path changes when parity becomes available.

## Proposer seeding checks

`seeding.py` separates first-hop scheduling from downstream reachability.
It uses a 2 MiB body, 40 encoded parts, the same 384-byte framing allowance,
and a 1 Gbps proposer upload limit. All parts are ready at time zero. Peer
rates are fixed independent capacities and every peer grants any-index credit
for up to 40 parts. No ordinary demand, competing blocks, propagation delay,
CPU work, or transport startup consumes the modeled capacity.

| Peers' usable Mbps | Optimal part counts | Optimal seed ms | Equal-assignment seed ms |
| --- | --- | ---: | ---: |
| 1,000 | 40 | 21.1 | 21.1 |
| 250/250/250/250 | 10/10/10/10 | 21.1 | 21.1 |
| 800/400/200/100/20/5 | 22/11/5/2/0/0 | 21.1 | 632.8 |

For `N` parts of `w` bytes, proposer byte rate `U`, peer byte rates `c[p]`,
and credits `g[p]`, choose the `N` earliest slots `j*w/c[p]` with
`1<=j<=g[p]`. The resulting counts minimize `max_p(a[p]*w/c[p])`: any earlier
completion threshold contains fewer than `N` eligible service slots. The
shared-upload lower bound is `N*w/U`. Pacing each connection at
`a[p]*w/max(N*w/U, max_p(a[p]*w/c[p]))` meets both rate constraints, so the
larger bound is attainable in this model. The checker verifies that construction
and compares the result with exhaustive count allocations in 240 small cases,
including credit caps. It also rejects insufficient total credit.

This objective seeds every chosen part once; it is not the earliest time a
receiver can reconstruct from `k` parts. It does not justify trusting advertised
capacity or omitting the only path to a downstream group.

For reachability, the checker enumerates all connected labeled undirected
graphs with two through five nodes and uses node zero as proposer. The proposer
seeds five distinct parts disjointly among its direct peers for `k=4`.
Every non-proposer edge subscribes to every index. A receiver forwards verified
parts and regenerates the codeword after collecting four distinct indices.

| Nodes | Connected graphs checked | Incomplete without repair | Complete with header-tree repair |
| --- | ---: | ---: | ---: |
| 2 | 1 | 0 | 1 |
| 3 | 4 | 1 | 4 |
| 4 | 38 | 10 | 38 |
| 5 | 728 | 158 | 728 |

For all 771 graphs, the result matches the component condition: remove the
proposer, then each remaining component reconstructs exactly when it receives
at least `k` distinct seeds. With fewer than `k`, forwarding cannot create
enough independent information for an arbitrary body. With at least `k`,
all-index forwarding eventually brings those seeds to every node in the
component. This argument assumes honest forwarding, adequate credit, retained
data, no prior body information, and fair service.

A separate counterexample uses a star with four leaves, `k=4`, and 100% parity.
Each leaf gets one distinct data part and one distinct parity part. None can
decode. Repair supplies two more parts per leaf, raising proposer upload from
eight parts to sixteen. That equals the cut lower bound of four bodies' worth
of independent information. Some parity at every peer is therefore insufficient.

The header-tree repair closure succeeds in all enumerated graphs. Its parent
relation reaches the source, so a parent can eventually serve a child's deficit
after reconstructing. The checker has no clocks, failed parents, credit
exhaustion, retention expiry, or repair-byte cap. This is a conditional liveness
check, not a bounded-latency guarantee or a proof for sparse part subscriptions.
The [design TODOs](dogwood.md#open-problems-and-todos) track those missing cases
and the proposed seeding-grant semantics.

## On-arrival Reed–Solomon

The runnable `online_rs.cpp` example uses the benchmark's eager GF(2^16)
Reed–Solomon decoder. It receives parity part 4, data part 1, duplicate part 4,
data part 3, and data part 0 for a four-data/one-parity codeword. The ranks are
1, 2, 2, 3, and 4. It recovers missing data part 2 and re-encodes the matching
root. Its data includes nonzero high bytes. The decoder verifies membership
before each insertion and ignores duplicate indices.

A fresh 2 MiB, 25%-parity, parity-first codec run used one warm-up and three
retained repetitions. At a 1 ms synthetic arrival gap, median remaining work
was 24.8 ms for batch decoding, 19.3 ms for forward-only incremental decoding,
and 8.0 ms for eager incremental decoding. The eager run moved about 5.9 ms of
elimination before the last required arrival. Re-encoding and root checking
still cost about 7.8 ms in the eager run.

These are measured CPU-task durations replayed on one serial worker, not a
wall-clock network run or a production codec benchmark. The host had no CPU
reservation and other experiments ran concurrently. Three repetitions do not
establish stable performance. The result verifies the intended schedule and
keeps the final root check visible; it does not demonstrate post-Tachyon CPU
capacity or feasible decoding for multi-gigabyte bodies.

## Reproduction and remaining work

Run these commands in the preserved worktree's `docs/experiments/dogwood`
directory. Choose new output directories; the scripts preserve prior runs.

```sh
python3 congestion.py results/my-congestion
python3 parity_frontier.py results/my-parity
python3 timestamp_ablation.py results/my-timestamps
python3 seeding.py results/my-seeding
python3 -m unittest -v test_sim.py test_congestion.py
g++ -std=c++20 -O3 -Wall -Wextra -Werror online_rs.cpp -lcrypto -o build/online_rs
build/online_rs
g++ -std=c++20 -O3 -Wall -Wextra -Werror codec.cpp -lcrypto -o build/codec
build/codec --test
build/codec 32 0.25 rs parity_first 1 200 4
```

The final result directories are `results/2026-09-08-congestion-final`,
`results/2026-09-08-parity-final`, `results/2026-09-08-timestamps`, and
`results/2026-09-08-seeding-final`. The timestamp comparison reads the saved
final congestion cases as its paired reference. The codec CSV and example output are in
`results/2026-09-08-congestion`. Result directories include source snapshots
or hashes. The tests passed 19 Python cases and 121 small Reed–Solomon subsets
under both incremental schedules. Tests check constant clock offsets, receive
compression, idle samples, exact-assignment control delay, and failure coverage.

The earlier September 5 experiments tested arrival ambiguity, coding schedules,
and sparse subscription reachability. They found closed subscription cycles
without a source and controllers that did not consistently beat equal shares.
The new single-receiver results do not resolve those overlay limitations.

Before selecting a controller, test real transport pacing, RTT/loss/ECN signals,
application-limited samples, misleading timestamps, shared physical bottlenecks,
sender queue residence, bounded rate increases, and multiple adapting receivers.
Before selecting parity, measure proposer encoding and upload together with
relay duplication, receiver bytes, correlated failures, and repair latency.
