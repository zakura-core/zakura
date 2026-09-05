# Dogwood experiments

Dogwood needs to reconstruct blocks quickly without assuming a fixed entry
point or uniform bandwidth. These experiments isolate four questions: when to
decode, how much redundancy to request, what arrival measurements establish,
and whether sparse subscriptions can carry a block from its source.

The results support keeping systematic Reed–Solomon and testing an eager
incremental decoder. They do not establish a tuned subscription controller.
The reduced controller fails to beat static allocation in several cases.
Random sparse subscriptions also fail to reach the whole network in some
connected graphs. Recovery remains part of the protocol, not an exceptional
path that parity removes.

The [design](../../design/dogwood.md) explains the protocol.
The [specification](../../specs/dogwood.md) remains authoritative.
The [recorded results](results/2026-09-05/SUMMARY.md) include every baseline,
seed ranges, failed reconstructions, and links to this methodology.

## Goals and constraints

We want to minimize time from admitted metadata to a checked body. We must
also bound bytes, queued work, and recovery time. A peer can receive data late
upstream even when its connection has spare capacity. A proposer can publish
too rarely to support rapid learning from repeated blocks.

The experiments use a 400 ms reconstruction target and a 1,200 ms failure
cutoff for routing comparisons. These are test targets, not proposed network
deadlines. A candidate must improve latency without hiding fallback or buying
the improvement through unreported duplicate traffic. We retain static equal
shares as a baseline. We do not select a controller from its best scenario.

We distinguish three quantities:

- **Committed parity:** parts the proposer encodes and includes in the root.
- **Subscribed redundancy:** distinct parts and extra copies the receiver requests.
- **Delivered redundancy:** bytes that arrive, including cancellation tails.

Increasing the first quantity does not necessarily increase the other two.
All delivered parity still consumes transport capacity. FEC does not replace
transport congestion control. [RFC 9265](https://www.rfc-editor.org/rfc/rfc9265.html)
discusses this separation.

## Codecs

### Options

The reference Reed–Solomon kernel implements the draft's systematic
Vandermonde matrix over GF(2^16). The construction guarantees recovery from
any `k` distinct encoded parts. [RFC 5510, section 8](https://www.rfc-editor.org/rfc/rfc5510.html#section-8)
defines the construction.

The RLNC kernel uses the same field and payload operations. Its systematic
parts remain unchanged. A seeded generator chooses dense random parity rows.
This is a finite, precommitted RLNC codeword, not a rateless or recoding
protocol. Keeping the arithmetic identical isolates the coding and scheduling
choices. It does not compare the fastest available libraries.

Both kernels can eliminate equations as parts arrive. RLNC's sliding-window
advantages matter when the source produces a stream over time.
[RFC 8681](https://www.rfc-editor.org/rfc/rfc8681.html#section-6.2) describes
such a decoder. Dogwood already requires a complete committed codeword before
`HeaderMeta`. Switching to sliding-window production would change that
authentication model. A node also cannot attach the original Merkle proof to
a newly recoded combination.

Random parity introduces rank risk. For `u` missing systematic parts and `u`
independent uniform repair rows over a field of size `q`, the full-rank
probability is `product(1 - q^-j, j=1..u)`. This follows by counting vectors
outside the span of earlier rows. At GF(2^16), the failure probability approaches
0.00153%; at GF(2^8), it approaches 0.392%. These are calculated probabilities,
not observed failure rates. Neither gives Reed–Solomon's any-`k` guarantee.

### Prototype

[codec.cpp](codec.cpp) measures matrix construction, encoding, SHA-256 tree
construction, proof checks, elimination, back-substitution, and re-encoding
with a root check. It tests systematic-first, random, parity-first, withheld,
insufficient, and committed-invalid-codeword inputs.

The decoder has two kernels and three schedules:

| Schedule | Work on arrival | Work after the last required part |
| --- | --- | --- |
| Batch | Verify membership | Forward elimination, back-substitution, codeword check |
| Incremental | Verify and forward-eliminate | Back-substitution, codeword check |
| Eager incremental | Verify and reduce existing rows with each new pivot | Codeword check |

The benchmark measures kernel task durations, then replays those durations on
one serial worker with 0, 0.1, and 1 ms arrival gaps. Batch and incremental
forward elimination reuse identical measured costs. Eager elimination has its
own measurements. The replay cannot invent parallel CPU capacity. It includes
proof work and any backlog before the last required arrival.

This is a measured-task scheduling experiment, not a wall-clock network
benchmark. It does not measure batch-specific cache effects. Four assemblies
have independent decoder states and share one worker. They receive identical
payloads, which can favor cache reuse. The timing results use three retained
repetitions after one warm-up repetition on one unreserved host.

Bodies contain 0.5, 2, or 8 MiB of synthetic bytes. The 8 MiB case tests scaling;
it does not assert a consensus block-size allowance. All payloads use 64 KiB
parts. Separate sweeps encode 12.5%, 25%, 50%, and 100% parity. The experiment
records explicit array-storage bounds, process peak RSS, and whole-process
CPU time. RSS includes fixtures and crypto allocations; it is not decoder-only
memory. The benchmark-only tree format is not a proposed wire profile.

### Result

For a 2 MiB body with 25% parity arriving first, the 1 ms-gap replay leaves
approximately 18 ms after the last required part with batch Reed–Solomon.
Eager incremental Reed–Solomon leaves approximately 6 ms. RLNC is similar.
With only systematic arrivals, decoding is already cheap; the codeword check
dominates. Four concurrent assemblies increase the remaining serial work.

Keep Reed–Solomon. Prototype eager elimination before changing the codec.
Do not assume that accepting incremental input means the implementation has
moved substantial work out of the completion tail. Keep root re-verification
in the latency budget, even for systematic-only reception.

The proposer pays encoding plus tree construction after mining if its cached
body changed. A reusable body avoids that work after mining. The experiment
measures both components but does not measure signatures, mining, canonical
transaction parsing, or consensus validation. We still need an optimized
implementation benchmark before choosing production CPU and memory limits.

## Parity and coverage

Suppose the receiver subscribes to `m` distinct parts with one supplier per
part. Let `a[p]` be the number assigned exclusively to peer `p`. Surviving any
one supplier loss without repair requires:

```text
m - max(a[p]) >= k
```

With `d` equally loaded suppliers, this becomes `m - ceil(m/d) >= k`.
Ignoring integer rounding, the required subscribed overhead is at least
`1/(d-1)`. This gives 100% with two suppliers, 50% with three, about 33% with
four, and 25% with five. This is a coverage bound, not a latency prediction.

For `k=32`, committing 40 parts does not let four suppliers meet the bound
without duplicates. Five suppliers can each carry eight parts. Committing
64 parts but subscribing to only 40 leaves the same coverage constraint.
Concentrating most parts on one fast connection requires substantial coverage
elsewhere, regardless of that connection's bandwidth.

The [parity calculation](observations.py) enumerates these bounds.
The routing sweep varies committed parity and subscribed distinct parts
separately. Requesting only `k` distinct parts forces a backup copy of every
requested part under the single-supplier-loss model. Requesting more distinct
parts can therefore reduce duplicates. Subscribing to all of a 100%-parity
codeword can instead waste bandwidth after reconstruction.

Keep 25% as the draft's experimental encoding schedule, not a measured optimum.
Do not make parity an unconstrained sender choice. A future negotiated profile
could commit more parity and let receivers subscribe to a subset. That option
adds encoding, root-verification, and storage work even when receivers request
few parity parts. The prototype does not yet price those costs together with
a complete overlay. Choose the failure model and acceptable repair latency
before tuning this ratio.

## What arrivals tell us

Different-part arrival times do not identify the better route without further
assumptions. Consider two possible worlds. The receiver requests part 0 from A
and part 1 from B. It observes 100 ms and 30 ms in both worlds:

| World | A, part 0 | A, part 1 | B, part 0 | B, part 1 | Faster for either part |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | **100** | 20 | 110 | **30** | A |
| 2 | **100** | 120 | 10 | **30** | B |

No estimator can distinguish these worlds from those observations alone.
Comparing the same part supplies the missing comparison. It establishes arrival
order at the offered load, not physical bandwidth or performance under more load.

[observations.py](observations.py) tests independent availability, fixed part
skew, randomized assignment, unequal opportunities, and completion censoring.
Across 400 seeds, fixed skew makes the different-part mean choose the wrong
peer in every trial, even with 64 blocks. Randomizing assignments removes that
systematic error in this model. More blocks then improve both estimators.
Raw delivery counts remain sensitive to how many parts each peer received.
Short paired trials also make mistakes; pairing does not remove measurement noise.

Use ordinary arrivals to rank candidates and detect changes. Existing duplicate
subscriptions can provide paired evidence without extra traffic. A randomized
different-part experiment can estimate average route performance if it controls
assignment, load, and censoring. Merely assuming that the original distribution
was random does not establish those conditions.

Keep same-block, same-part comparisons for the baseline promotion rule.
Do not add a sender timestamp. An unauthenticated timestamp adds a manipulable
measurement and still does not reveal upstream waiting or unused capacity.
One block supplies one correlated vote, not one independent vote per part.

## Routing and challenges

### Prototype

[sim.py](sim.py) models four suppliers and one or two receivers. Supplier
egress and receiver ingress links serialize whole parts. Each supplier uses
round-robin service across receiver/block queues. Block streams, proposers,
backups, and trials share those links. Queue limits drop work explicitly.
Completion cancels queued sends after a control delay; in-flight parts remain.

Primary masks stand before the block. The simulator resolves additional
coverage subscriptions after metadata and charges control delay. It starts
from equal primary shares plus coverage, not the draft's two-copy cold-start
phase. Every supplier's part availability follows an explicit exogenous trace.
The model does not simulate recursive upstream route changes or TCP/QUIC.
It therefore tests local allocation under shared load, not overlay stability.

The six policies are equal shares, random assignment, a global best observed
peer, a proposer-specific best observed peer, repeated paired races, and races
with a shared byte-budget gate. The `budgeted` policy is a reduced experiment,
not an implementation of every rule in spec section 7. Its learned budget gates
exploration and promotion; it does not fully rebalance standing demand above
that budget. It omits migration settling, persistent grant replay, bounded
proposer-history eviction, and separate local verification queues.

Fifteen scenarios vary proposer locality, entry-point changes, bandwidth loss
and recovery, part skew, block-size jumps, bursts, a shared receiver bottleneck,
upstream stalls, correlated withholding, competing receivers, overload, sparse
proposers, and unfamiliar keys. Each policy runs 12 seeds and 80 blocks.
The 244 configuration sweeps run four seeds each. Raw records include queues,
assigned bytes, copies, cancellation tails, recovery, challenge expenditure,
and movement times. A movement is not proof of successful adaptation.

The model assumes valid parts and zero decode/verification time. It uses a
384-byte wire allowance and fixed control-message sizes, not wire serialization.
At 1,200 ms it records a failed reconstruction. It does not simulate the
fallback download or count that event as success. Completed-only percentiles
must therefore accompany fallback rates. A p99 over 80 blocks is descriptive,
not a reliable estimate of a rare tail.

### Results

The paired controller does not consistently improve latency over equal shares.
In the balanced case it adds traffic without a useful latency improvement.
In the burst case it produces no promotions and misses most 400 ms deadlines.
The proposer-specific passive baseline performs much better in that case, but
uses more bytes. In other cases that same passive policy performs worse.
Neither policy wins the latency/bandwidth tradeoff uniformly.

The shared receiver and overload cases expose a capacity limit, not a missing
peer-ranking formula. The source-stall case makes every route miss its deadline
without reducing link capacity. Lowering a delivery budget in that case does
not diagnose congestion. The experiment does not justify treating the AIMD
assignment budget as a capacity estimator or fixing its production parameters.

Challenge lifetime matters more than timer frequency when proposer observations
are scarce. With four alternating proposers and a 1.2 s block interval, each
proposer appears every 4.8 s. A five-second trial cannot collect warming plus
three later votes. Those sweep configurations produce no promotions.
Longer lifetimes reduce inconclusive expiry but retain trial state longer.
Cold keys never repeat, so per-key learning cannot converge.

For a duplicated fraction `f` and exploration fraction `rho`, one paid
observation costs roughly `f/rho` completed blocks of funding. With a 16-bit
mask, one bit selects about 1/16 of a block. At `rho=1/32`, that is about two
blocks of funding per observation, before rounding, warming, and control bytes.
Duplicating more parts within one block does not supply more block votes.
Use the narrowest selection that provides useful signal. Widen it only to
test additional load or reduce ambiguous within-block comparisons.

## Reachability and the first-principles check

[graph.py](graph.py) separates reachability from timing. It builds connected
undirected peer graphs and independent per-part subscriptions. One node starts
with the codeword. Nodes push subscribed parts and may regenerate missing
parts after reconstruction.

One random supplier per part fails to bootstrap these sampled networks, even
with 100% parity. Two suppliers improve reachability but do not guarantee it.
A closed subscription cycle also fails in the deterministic tests. Local
coverage counts cannot establish a path from the source.

The prototype then adds deficit-sized repair subscriptions toward each node's
first-header parent. Equal header delays produce a rooted discovery tree in
this model. An honest parent can supply requested parts after it reconstructs.
These dependencies reach the source instead of forming an isolated cycle.
This repairs the sampled graphs, but assumes connected honest peers, retained
data, sufficient credit, and eventual service. The closure has no clocks;
it provides no repair-latency bound and tests no failed parent.

This suggests a small use of existing metadata: retain the first authenticated
header supplier as a repair candidate. It is not proof of part availability.
Peers reporting `FullBlock`, alternative suppliers, and bounded fallback remain
necessary. We do not need another message to prototype this path.

The minimum rules still make sense:

1. Authenticate the header and commitment before allocating part work.
2. Forward verified parts without waiting for reconstruction.
3. Count distinct surviving parts, not subscription count or committed parity.
4. Keep proposer-specific routes and shared connection resource limits.
5. Test bounded route changes under load; preserve coverage while changing them.
6. Recover on a deadline without waiting for statistically convincing route learning.

The performance claim must be narrower: learned standing routes avoid request
latency. Unfamiliar entry points and stale routes may pay discovery or repair
latency. The present prototypes do not establish that the proposed controller
learns those routes quickly enough. They support the codec and measurement
choices, not deployment of the whole protocol.

## Initial parameters and remaining choices

These values define reproducible starting experiments. They are not tuned
defaults. Keep wire/profile choices separate from receiver-local policy.

| Parameter | Starting value | Reason and exposure |
| --- | --- | --- |
| Part bytes | 64 KiB | Retain the draft profile; smaller parts trade scheduling granularity for proof/control work. |
| Parity/data | 25% | Retain the draft while sweeping 12.5–100%; profile-fixed, not an arbitrary sender knob. |
| Subscribed distinct parts | `k + ceil(k/4)` | Expose separately from committed parity; reduce only while checking coverage. |
| Part-mask width | 16 | Small experimental state; one bit moves about 1/16 of a block. Larger masks offer smaller moves but more state. |
| Reconstruction / failure cutoff | 400 / 1,200 ms | Explicit test targets; production policy must account for size and supported network conditions. |
| Control delay | 20 ms | A scenario input, not a measured RTT or controller estimate. Sweep against part serialization time. |
| Exploration fraction | 1/32 | A small byte tax; sweep 1/128–1/8 against selection width and proposer rate. |
| Initial / capped exploration funds | 2 / 4 reference bodies | Avoid a zero-credit bootstrap in the experiment; production needs node-wide bounds and deduplication. |
| Challenge width | One mask bit | Several comparable parts in a 40-part block; widening does not multiply independent evidence. |
| Start interval / jitter | 250 ms / up to 25% | Limit churn, not observation supply; sweep 0, 250, and 2,000 ms as controls. Zero is not a conformant profile. |
| Trial block / age caps | 12 blocks / 20 s | Finite retention; five seconds fails the alternating-proposer test. Expose both caps; derive age from expected proposer opportunities. |
| Minimum votes / switch threshold | 3 / 2/3 | Minimal repeated evidence, not statistical significance. Keep configurable for noise and load trials. |
| Race epsilon | 1 ms | Ignore small differences in this model; calibrate to local timing and verification noise. |
| Initial `W` / increase / decrease | 20 parts / 1 part / 0.75 | A test gate, not measured capacity; sweep initial 8–64 and decrease 0.5–0.9. No production choice follows. |
| Migration cap | Four parts | Bound a test move; sweep 1–16. A fixed part count grows in bytes when the profile changes. |
| Queue / repair bounds | 256 parts per link / `2k` repair copies per block | Bound this finite experiment. Production also needs aggregate node-wide and concurrent-assembly reserves. |

The implementation also caps `W` at 256 parts, floors it at two parts, uses
80% loaded utilization, and adjusts it at most once per 250 ms. These constants
belong to the reduced gate experiment. They do not complete the spec's cohort,
settling, or fairness rules.

For throughput-oriented operation, test smaller subscribed redundancy and accept
more repair latency. For latency-oriented operation, test more independent
coverage and pay its bytes. For a nearby high-bandwidth proposer, test gradual
concentration while retaining enough coverage elsewhere. Do not infer that one
fast small trial permits a whole large block on that connection.

Before promoting a controller to the default, implement the missing settling
and grant rules in a multi-hop simulator with feedback-driven upstream routes.
Measure time to sustained improvement in both wall time and proposer blocks.
Include failed header parents, synchronized receiver changes, and part deadlines
that compete with proof and decode work. Then combine the codec costs with
the parity sweep. This is the next validation gate, not evidence already supplied
by these prototypes.

## Reproduce

Requirements: Python 3.10 or newer, a C++20 compiler named `g++`, OpenSSL headers
and `libcrypto`, `pkg-config`, and Linux `lscpu`. No node build, chain data,
Python packages, or network access is required.

From this directory:

```sh
python3 run.py --out results/my-run
python3 summarize.py results/my-run
python3 check_results.py results/my-run
```

The runner refuses a nonempty output directory. It keeps the binary in the
ignored `build/` directory. Use `--quick` for tests, one codec case, and two
routing scenarios. Run `python3 -m unittest -v test_sim.py` for deterministic
model tests alone. Run `python3 sim.py --scenario burst --policy budgeted --seed 1`
to replay a routing case.

[environment.json](results/2026-09-05/environment.json) records the command,
compiler, host, and source hashes. The result directory contains raw CSV/JSON,
test output, and checksums. The kernels are reference prototypes, not code for
untrusted network input. None of these files changes the node implementation.
