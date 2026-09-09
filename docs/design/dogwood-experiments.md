# Dogwood experiment report

The September 8–9, 2026 experiments test subscription allocation, proposer
seeding, connected relay graphs, bounded fallback, and TCP delivery feedback.
They do not establish production congestion control or sustained 50,000 TPS.
The [design](dogwood.md) explains the tradeoffs; the
[spec](../specs/dogwood.md#parameter-registry) owns parameter definitions.

The experiment source and raw results remain on the local branch
`local/dogwood-experiments-20260908`. PR #901 contains documentation only.
The local worktree is `zakura.dogwood-experiments`, alongside the docs worktree.
Its `docs/experiments/dogwood` directory retains the September 5 experiments
and adds the scripts and result directories named below.

## W1 payload conformance

The W1 follow-up fixes candidate payload bytes, tagged SHA-256 commitments,
balanced 16-bit selection, and the metadata signature transcript. It preserves
the whole-body codec. The profile separates seed permission from ordinary
demand and gives each class its own inheritance action. Default scope cannot
inherit. Both classes retain shared connection budgets and send-once records.

`wire_profile.py` emits six frame fixtures and checks their round trips. Tests
cover all five message families, all selection classes, frame and field bounds,
range canonicalization, proof shape, body counts, byte credit, and malformed
input. The largest part frame is 66,102 bytes. The codec's two-data-part vector
has a fixed Merkle root in the spec. These checks do not implement the service
state machine or production allocation controls.

`ed25519_profile.py` uses OpenSSL 3.6.3 to reproduce the published
[RFC 8032 test 2](https://www.rfc-editor.org/rfc/rfc8032.html#section-7.1)
signature. It checks canonical nonidentity prime-order points before signature
verification. Tests reject small-order points, mixed torsion, noncanonical
points/scalars, and malformed lengths. The probe signs the 171-byte W1
transcript and rejects changes to the chain identifier, admitted block
identifier, and part root. This is not a cryptographic performance benchmark.

The complete local Python suite passes **78 tests**. Reviewed results live in
`results/2026-09-09-wire-w1-reviewed` and
`results/2026-09-09-ed25519-w1-reviewed`, with source snapshots and hashes.
Earlier W1 directories preserve intermediate experiments. Header and coinbase
fields in these frame fixtures are structural placeholders. They are not
consensus-valid blocks or evidence of successful header/key-binding admission.
The production chain adapter, transport negotiation, and aggregate resource
bounds remain open. No result establishes sustained 50,000 TPS.

## Concurrent bodies and key binding

The concurrent follow-up ran **128 streams of 120 synthetic 2 MiB bodies**.
The 20.48 ms release interval supplies 819.2 Mbps of body bytes. It is a load
generator, not a selected consensus block interval. Each stream starts with
routes and seed placement learned from one separate reference body.

`concurrent_push.py` shares each node's upload, ingress, and CPU service across
all bodies. It serves one part per body turn, then rotates peers within that
body. Each node can queue at most 256 parts for upload. The source has 1.25 Gbps
upload; relays have 2.5 Gbps upload and ingress. Encoding/root work costs an
assumed 6.38 ms per body, using the earlier local reference cost. Per-part proof
work costs 0.02 ms and reconstruction costs 8 ms. These CPU costs drive shared
queues; the simulation does not execute the codec. Metadata dissemination
remains idealized. Every body keeps the 400 ms fallback and 1,200 ms final
deadline, with at most `2k` extra requests per receiver.

The first 80 streams compare two-supplier startup routes with retained reference
paths. They cover steady service, heterogeneous upload, a temporary relay-link
slowdown, a source slowdown, and a CPU slowdown. All steady and heterogeneous
cases complete before fallback. Four route seeds produce these steady means:

| Relays | Routes | Mean per-run p95 completion | Network part bytes / receiver body bytes | Largest relay upload / body bytes |
| --- | --- | --- | --- | --- |
| 16 | Two suppliers | 93.0 ms | 2.37 | 3.00 |
| 16 | Retained reference paths | 74.7 ms | 1.15 | 1.89 |
| 64 | Two suppliers | 239.3 ms | 2.21 | 2.92 |
| 64 | Retained reference paths | 207.1 ms | 1.11 | 2.04 |

The largest-relay column averages each run's maximum node upload. The network
ratio includes source and relay part upload, including the 384-byte proof/frame
allowance. It excludes transport and control overhead. Some receivers cancel
parts after obtaining enough distinct indices, so delivered traffic can fall
below a complete codeword per receiver. The source still seeds at most one
codeword per body. These short traces do not establish long-run queue stability
or end-to-end 50,000 TPS.

Capacity planning must account for the largest relay, not only the network
average. A relay serving three body copies at this workload needs about
2.46 Gbps before additional overhead or utilization headroom. The retained-path
candidate lowers that cost in the steady cases, but does not protect a deadline
when service changes.

The other 48 streams isolate upload changes. Four relays halve their upload or
reduce it eightfold from 1,000 to 2,000 ms; ingress remains 2.5 Gbps. A third
policy restores a receiver's startup suppliers for future bodies after a miss,
paying 20 ms control delay. It does not restore routes solely because enough
parts are waiting on local CPU work. It does not prune again during the run.

| Relays | Upload change | Two suppliers: normal completions | Retained paths | Retained paths, then restore |
| --- | --- | --- | --- | --- |
| 16 | Half | 100% | 100% | 100% |
| 16 | One eighth | 78.0% | 68.6% | 71.5% |
| 64 | Half | 90.2% | 94.9% | 94.8% |
| 64 | One eighth | 59.1% | 52.5% | 51.5% |

These fractions include every receiver/body observation. Restoration does not
consistently improve normal completion and sometimes adds traffic to a busy
network. For the 64-relay eightfold slowdown, mean direct fallback traffic is
225.7 MiB with two suppliers and 448.9 MiB with retained paths. Eventual
completion is 95.9% and 95.5%, respectively. Fallback neither hides the missed
normal deadline nor guarantees recovery within its caps. Do not select this
restoration rule as a complete congestion controller.

The source and CPU slowdowns also cause misses. Some disturbances reduce a
node below the workload's required service rate. A routing policy cannot repair
that capacity deficit. The model enforces upload queue and repair caps, but
ingress and CPU queues are service-delay models without transport backpressure
or production memory admission. Retained paths remain a candidate under fixed
mapping and seed placement, not a deployed stripe profile.

### Coinbase key commitment probe

`coinbase_binding.py` constructs a canonical transparent-only V5 coinbase and
checks a proposer key in one zero-value output. Its input-script mutation keeps
the txid unchanged; its output-key mutation changes the txid and fails the old
Merkle proof. This matches the split between transaction effects and authorizing
data in [ZIP 244](https://zips.z.cash/zip-0244).

The candidate script is `OP_RETURN`, a direct 40-byte push, `DOGWOOD`, byte
`01`, and a 32-byte key. The proof must establish coinbase position zero against
the admitted header. A txid proof cannot authenticate a key carried only in
the V5 input script. The probe rejects duplicate matching outputs, nonzero
commitment value, wrong position, stale roots, truncation, and noncanonical
counts. Its example coinbase is 119 bytes. It does not check proof of work,
metadata signatures, rewards, shielded coinbases, or post-Tachyon transaction
formats. The chain adapter remains an implementation gate.

The current local suite passes **60 tests**. Results and source snapshots are
in `2026-09-09-concurrent-push`, `2026-09-09-concurrent-upload-change`, and
`2026-09-09-coinbase-binding` under the local experiment results directory.

## Connected-network and transport follow-up

The normal throughput target assumes that honest relays remain connected after
removing the proposer. `FullBlock`-triggered requests for missing parts are
bounded fallback. These experiments report completion before fallback separately
from eventual completion. The planning workload is **50,000 TPS at 2 KiB per
transaction after Tachyon**: 819.2 Mbps of body bytes, or 1.024 Gbps with 25%
parity before proof and transport overhead.

### Connected push and route pruning

`push_overlay.py` ran 656 single-block cases on rings with additional local
edges. It used 16/64 relays, degree 4/8, one/four source neighbors, and one/two
suppliers per mask bit. Every relay graph remains connected without the source
and one failed relay. The 2 MiB body has 32 data parts and eight parity parts.
The source seeds at most one codeword. The model compares spread seeds with
seeding a decodable subset to one neighbor first.

Source upload is 1 Gbps. Relay upload and ingress are 1.6/2 Gbps, either equal
or divided by 1/2/4/8 across peers. The model serializes upload, ingress, and
verification queues. Proof verification costs 0.02 ms per part; reconstruction
costs 8 ms. These CPU values are assumptions. Metadata is preinstalled, parts
pay 5 ms propagation, and control messages pay 20 ms. Fallback starts at
400 ms, requests at most `2k` extra copies per receiver, and ends at 1,200 ms.
Only completion advertisements authorize pull attempts. Failed peers remain
silent; a separate case sends false completion advertisements.

The following healthy, equal-rate cases use degree eight and four source
neighbors. Values average eight distinct route seeds. The 16-relay cases also
appear in the failure sweep; those repeated configurations add no independent
evidence.

| Relays | Suppliers per bit | Seed policy | Receivers complete before fallback | Last completion, mean |
| --- | --- | --- | --- | --- |
| 16 | 1 | Spread | 0/16 | 542.6 ms, with fallback |
| 16 | 1 | Decodable first | 1/16 | 516.9 ms, with fallback |
| 16 | 2 | Spread | 16/16 | 65.5 ms |
| 64 | 1 | Spread | 0/64 | 889.4 ms, with fallback |
| 64 | 2 | Spread | 64/64 | 167.2 ms |

Two suppliers establish useful startup delivery in these cases, but consume
bandwidth. Total source plus relay part upload divided by receiver count and
body size is 2.42 with 16 relays and 2.30 with 64 relays. These network averages
include the 384-byte per-part proof/framing allowance. They exclude transport
overhead and do not bound an individual relay's upload. A 5% allowance cannot
cover this duplicate traffic.

`prune_routes.py` tests 72 sequences of 24 blocks. Local majority wins and a
one-supplier-loss coverage check do not preserve global delivery when several
nodes prune routes. `seed_offer_routes.py` adds 192 sequences. Its source sends
each seed only to an eligible neighbor with outgoing demand for that mask bit.
Every static two-supplier configuration then completes normally in the tested
six route seeds. Coverage-preserving pruning still causes fallback in three
of eight configuration groups. Physical connectivity, local coverage, and
past wins therefore do not establish a safe pruning rule.

`witness_pruning.py` tests a separate stripe candidate. Each node retains every
incoming supplier that delivered a distinct part before its reconstruction of
one shared reference stripe. Subsequent equal-shape stripes retain the mask
mapping and source seed recipients. This preserves the reference's causal
delivery paths under honest peers, unchanged availability, adequate credit,
retention, and fair service. The argument establishes eventual delivery under
those assumptions; it does not establish a deadline or failure tolerance.

All 48 paired configurations complete before fallback with and without this
pruning. Retained routes reduce mean relay upload by 45.8–52.5% across groups.
The 24 sequential stripes repeat fixed service and routes; they are not 24
independent observations or a concurrent stream. This candidate requires a new
striped codec profile. The present whole-body codeword cannot apply its result
directly. Changing the mapping, seed placement, codeword shape, or availability
invalidates the reference argument. The earlier witness directory varied the
mapping and does not support that argument.

These simulations omit transport backpressure and real coding work. Direct
pull responses have separate byte counters; later forwarding of repaired parts
still counts as relay forwarding. Normal success always requires completion
before the fallback timer. The models do not implement every grant and
controller rule.

### Real TCP feedback

`run_tcp_feedback.py` ran 45 isolated TCP experiments: five scenarios, three
allocation policies, and three repetitions. Four suppliers use application
pacing at 80/40/20/10 Mbps. They share a 100 Mbps loopback qdisc with 5 ms delay.
Each run releases 120 bodies at 25 ms intervals, with four required 64 KiB parts
and five available parts. The capacity-drop case reduces the fastest supplier
to 5 Mbps after 1.5 seconds. The application-limited case inserts 40 ms stalls.
The loss and ECN cases configure 0.2% netem loss; ECN marks eligible packets.

The allocator compares equal assignment, receiver arrival-rate feedback, and
feedback using the larger of sender and receiver spans. It assigns exact parts
after release. This is a transport experiment, not Dogwood standing push or a
complete controller. It hashes synthetic payloads and does not run Reed–Solomon.

| Scenario | Equal: complete within 800 ms | Receiver-rate feedback | Sender-span feedback |
| --- | --- | --- | --- |
| Baseline | 198/360 | 360/360 | 358/360 |
| Capacity drop | 148/360 | 272/360 | 270/360 |
| Application limited | 137/360 | 319/360 | 244/360 |
| Loss | 199/360 | 358/360 | 358/360 |
| ECN | 198/360 | 358/360 | 358/360 |

Baseline mean per-run p95 completion falls from 1,011.8 ms with equal assignment
to 82.3 ms with receiver-rate feedback. That p95 includes completed bodies only;
the table includes every released body. Sender spans provide no consistent
advantage and perform worse during application stalls. Keep sender timestamps
out of the baseline wire profile. Continue testing receiver-local feedback.
TCP counters confirm ECN marks, but retransmission counts are small. These
short loopback runs do not establish WAN loss behavior, full controller
stability, or throughput at the post-Tachyon target.

### Grants and remaining decisions

`grant_model.py` exhausts 380 states and 1,270 transitions for a finite
two-index grant model. It checks queueing, cancellation, crossed `FullBlock`,
retirement, and send-once accounting. Separate tests cover exploration credit,
loaded cohorts, failure precedence, migration overlap, and final coverage.
That checkpoint passed 45 tests. This is bounded state exploration, not a proof
of the full protocol or multi-connection controller.

Retain two-supplier startup coverage where budgets allow it. Do not select
majority-based pruning as a demonstrated path to one-copy throughput. Keep
`FullBlock` pull strictly as fallback. Keep 25% parity and one-part scheduling
portions in the current draft; the earlier small-block results remain candidate
profile evidence. A complete implementation still needs a joint controller,
codec, and transport test with concurrent bodies and changing routes.

The remaining profile choices depend on the block interval, propagation
deadline, supported peak body size, and post-Tachyon chain binding. At this
workload, the current single-codeword limit is reached after about 33.55 seconds
of transactions. A stripe profile needs authenticated stripe identifiers,
commitments, completion semantics, resource limits, and failure tests before
adoption. These experiments do not complete that profile.

The local result directories are `2026-09-09-connected-push`,
`2026-09-09-route-pruning-final`, `2026-09-09-seed-offer-routes`,
`2026-09-09-witness-pruning-final`, `2026-09-09-tcp-feedback-run`, and
`2026-09-09-grants` under `docs/experiments/dogwood/results`.
They retain source snapshots and provenance. The local README records commands.

## Bounded-recovery follow-up

The follow-up ran **2,448 deterministic single-block simulations** and eight
reference codec configurations. The simulations replace the earlier timeless,
all-index relay closure with sparse per-index subscriptions and serialized
upload. They advance the bootstrap and small-block TODOs; transport feedback
and controller convergence remain untested here.

### Method

The recovery sweep covers one receiver and three eight-receiver topologies:
a star, two branches with bridge peers, and a mesh with alternate paths.
The proposer seeds each encoded index once, spread evenly across its neighbors.
Each relay index selects one random non-source neighbor. Seeds 0–11 select
those subscriptions. The proposer has 1 Gbps upload; relays have either
200 Mbps each or repeated 800/400/200/100 Mbps upload rates.
These rates constrain aggregate node upload, not independently measured links.
The one-copy comparison does not implement the draft's startup coverage policy.

We compare no repair, header-parent repair, and repair that tries alternatives
after the first attempt. A failed peer remains silent from the start; metadata
and the parent tree are preinstalled. Repairs start at 100/300/600 ms and pay
20 ms control delay. Each receiver can add at most `2k` requests. Source credit
allows either `n` parts or `n + nodes*k` parts, including initial seeds.
The experiment ends at 1,200 ms and counts unfinished receivers as failures.

The model forwards a complete part after upload plus 5 ms propagation.
It assumes unlimited ingress, instant verification and regeneration, and one
valid block. It charges 384 bytes per part for proof/framing, includes sends
to failed peers, and allows a 20 ms cancellation tail after reconstruction.
It does not simulate full-block fallback, transport loss, competing blocks,
negotiated grants, or adaptive routes. Completion establishes reconstruction
in this model, not verified end-to-end production delivery.

### Recovery result

These rows use 2 MiB bodies, 25% parity, and equal relay rates. Completion time
is the mean time when the last healthy receiver finishes, over successful runs.
Source MiB includes seeds, repair, and cancellation tails.

| Topology and failure | Source budget / repair | All healthy receivers finish | Completion ms | Source MiB |
| --- | --- | --- | --- | --- |
| Single receiver | One codeword / none | 12/12 | 21.9 | 2.515 |
| Eight-leaf star | One codeword / alternatives | 0/12 | — | 2.515 |
| Eight-leaf star | Reserve / alternatives | 12/12 | 238.9 | 16.094 |
| Mesh, healthy | One codeword / alternatives | 12/12 | 385.8 | 2.515 |
| Mesh, healthy | Reserve / parent | 12/12 | 205.6 | 5.270 |
| Mesh, failed parent | Reserve / parent | 0/12 | — | 5.501 |
| Mesh, failed parent | Reserve / alternatives | 12/12 | 355.8 | 5.501 |
| Bridge cut, failed bridge | Reserve / alternatives | 0/12 | — | 3.269 |

The star requires eight body copies across eight separate source cuts.
Its 16.094 MiB result matches that payload lower bound plus framing.
The healthy mesh can trade repair delay for lower source upload.
Alternatives recover the connected mesh after parent failure. They cannot
recover the three honest receivers disconnected by the failed bridge.
These cases do not support an unconditional one-codeword bootstrap guarantee.

### Small blocks, parity, and portions

The mesh sweep varies `k=1/2/4/8/32`, 16/64 KiB parts, 25%/100% parity,
one/two/four-part service portions, and systematic-first/parity-first seeding.
All 1,440 runs finish with the repair reserve. At `k=1`, parity rounding makes
both ratios identical. The table uses 64 KiB parts, one-part portions, and
systematic-first seeding.
Times include repair; byte totals cover the whole network's source or relays.

| Body | Parity | Completion ms | Source MiB | Relay MiB |
| --- | --- | --- | --- | --- |
| 256 KiB | 25% | 125.7 | 0.545 | 2.635 |
| 256 KiB | 100% | 66.6 | 0.545 | 3.641 |
| 512 KiB | 25% | 154.5 | 1.425 | 5.564 |
| 512 KiB | 100% | 71.3 | 1.058 | 6.470 |

More parity can reduce total source bytes by avoiding repair copies, while
increasing relay bytes. Smaller parts also change that tradeoff: at 512 KiB
and 100% parity, 16 KiB parts finish in 56.9 ms with 7.024 MiB relay upload.
A threshold stated only as `k<=8` changes its body-size meaning when `S` changes.

Across the equally weighted sweep, mean completion is 93.3/93.7/94.1 ms for
one/two/four-part portions with systematic-first seeding. Parity-first gives
92.3/92.5/93.0 ms. These small differences do not select a larger portion or
an ordering rule. Portions change local service fairness here; they add no
wire aggregation or measured CPU savings.

The codec run uses the existing 64 KiB reference kernel, one warm-up, and three
retained repetitions per configuration on an unreserved host. At 512 KiB,
the sum of median encoding and root times rises from 0.71 ms at 25% parity to
1.77 ms at 100%. At 2 MiB it rises from 6.38 to 21.70 ms. These CPU measurements
are separate from the network simulation; we have not tested their queueing
interaction. They argue against extrapolating the small-block result to all
block sizes.

### Decisions and next gates

- **Source budget:** retain an initial seed budget plus a bounded repair reserve.
  Decide which source-cut fanout and repair latency the supported topology must
  accommodate. A global one-codeword cap cannot support the star case.
- **Recovery:** retain alternative suppliers after a parent stalls. The spec now
  makes the shared deadline and non-resetting credit rules explicit.
- **Small-block profile:** keep 100% parity as a candidate, with a threshold in
  body bytes and an explicit part size. Keep the draft's 25% rule until a joint
  CPU/network run includes correlated failures and competing blocks.
- **Portions:** retain one part as the reference service quantum. No wire portion
  message follows from this sweep.
- **Next implementation work:** build the complete grant/controller state model,
  then test real transport feedback. Wire negotiation, PoW key binding, resource
  caps, and the target block interval still require profile decisions.

Run `python3 bounded_overlay.py results/my-bounded-overlay` and
`python3 summarize_bounded.py results/my-bounded-overlay` in the local experiment
directory. Final raw runs, summaries, source snapshots, and environment records
are in `results/2026-09-08-bounded-overlay-final`; codec CSVs and exact commands
are in `results/2026-09-08-small-codec`. All 25 Python tests pass, including
analytic serialization and source-cut checks, replay, credit, and deadline tests.

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
