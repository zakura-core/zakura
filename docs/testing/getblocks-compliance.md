# GetBlocks compliance tests

These tests execute the requirements identified in the #896 coverage audit of
[#747's regulation specification][spec] and [GetBlocks testing plan][plan]. They
run against #971, the compliance branch above #945. Failures identify differences from the
intended requirements. They do not silently redefine the specification to match
the implementation.

**Example:** A requests heights 100–102 and receives only block 100. B sends
`BlocksDone(100, 3)`. `r06_done_count_must_equal_consumed_prefix` requires a protocol
rejection because only one body was consumed. Accepting the ending or recording
misbehavior without rejection both fail this test.

The current negotiated layout is data stream 6 version 3 and request stream 7
version 1. Tests retain this accepted stack decision rather than restoring #747's
older layout text.

## Current scope and results

The September 12 dependency extraction restores published `zakura-iroh`
1.1.0-rc.1 and `noq`/`noq-proto` 1.2.0. There are no transport package overrides
in the Zakura workspace. Shared application admission, requester authorization
and metadata accounting remain enabled across messages.

The published transport settings allow 16 remotely initiated bidirectional
streams, 256 KiB receive credit per stream and 9.5 MiB per connection. These
settings support the fixed independent-progress scenarios. They do not bound
arbitrary local stream churn or all transport allocations.

At combined property revision `30e842a05`, including compliance `676a711ed`,
local Rust 1.97.0 checks report:

| Profile | Result |
| --- | --- |
| Compliance, 64 cases and four load rounds | 104 passed |
| Fixed regressions, 64 cases | 468 passed |
| Properties, 2,048 cases and seed 896 | 49 passed |
| Dependency-blocked transport witnesses | 5 excluded from compilation |

Both T02 witnesses and the 32-paused-sibling regression pass on published
packages. The latter transfers 19 MiB before draining the original streams.
No retries or timeout-policy changes were used. This run has no nextest
output-handle closure flags. The profile filters exclude unrelated tests. The
five dependency-blocked witnesses are separate from those filter counts.
Network all-target Clippy with warnings denied, formatting, Markdown and
changelog checks pass. Passing selected cases does not establish complete message
compliance or a node-wide transport memory bound. The longer load and optimized
throughput gates remain open.

## Deferred dependency requirements

Five transport ownership tests retain their complete assertions and individual
ignore reasons. Their module is also excluded from compilation because ignored
Rust tests still type-check calls to unavailable dependency APIs. They are not
included in passing totals and cannot run with `--ignored` alone.

The [capacity guide][capacity] lists each test and its re-enable requirements:
owned incoming/outgoing construction, admission before the handshake, one
inbound/outbound pool held through final cleanup, and limits on locally opened
and retiring stream state. Restore the native integration and publish the
corresponding dependency APIs before enabling these witnesses.

The separate transport patches also address retained send/receive storage,
datagram queues and protocol metadata. None of those package changes are enabled
by this stack. Endpoint identity retention, idle actors, aggregate allocation
funding, optimized five-sample throughput comparisons, 64-round load and remaining
subscription publisher contracts are still outstanding. The extracted patch
alone does not complete that inventory.

[capacity]: ../design/native-transport-capacity.md

## Historical results

The following results precede the dependency extraction. They describe earlier
revisions and must not be used as passing evidence for the current stack.

### Earlier September 11 baseline

On 2026-09-11, property code revision `6f1ba00ad` includes compliance
implementation `af5fa24a2` from [#971](https://github.com/zakura-core/zakura/pull/971).
Local Rust 1.97.0 runs use seed 896. Compliance and fixed regressions use 64
generated cases and four load rounds. The property profile uses 2,048 cases.

| Profile | Result |
| --- | --- |
| Compliance | 103 passed, 1 failed across 104 tests |
| Property profile, 2,048 cases | 40 passed across 40 tests |
| Fixed regressions | 466 passed, 1 failed across 467 tests |

All 40 property tests pass in one run. The receiver-history property takes 78
seconds with the five-minute outer limit added in `1ae81b3a6`. Its two-second
per-event waits and input bounds are unchanged. The earlier 60-second timeout
and process leak classifications remain recorded in the execution evidence.

A follow-up moves the registry fixture helpers into test sources in `da97e5e55`,
imported by `936e9cb54`. Its final 73-test registry, receiver, and allocation
selection passes, including 2,048 cases for the generated allocation check.
Clippy passes with warnings denied. The full profile results above precede that
test-helper move.

At that earlier revision, the failing witness in compliance and fixed regressions
was T02 with two paused sibling streams. Their occupied receive windows prevented the independent service
frame from arriving before resumption. T01 now completes both directions and
consumes all endings after worker and output pressure. Real checkpoint-verifier,
storage ownership, allocation, and current bounded-load controls pass.

The receiver now retains original response credit through local expiry, finality,
reset, and reassignment. It checks the next authorized header before handler
capacity or full Block decoding, consumes actual object and byte counts, and
keeps protocol slots until a valid ending. Locally undrainable authorization
requires connection closure without a peer fault. Numeric Status ceilings remain
fixed within the connection. Availability updates preserve existing credit.

Five additional receiver tests cover current ownership after reassignment,
queued-only cancellation, first and repeated withheld-ending stalls, availability
changes, and both directions of all three numeric ceiling changes. Fixtures share
node work and byte budgets with distinct session generations. T01 keeps the same
numeric limits that its connection initially advertised.

The shared response scope now fences publication and first writes before session
replacement or removal. Fixed tests cover prepared and queued writes, partial and
complete request writes with unfinished responses, validated completion, owner
Drop, and replacement on a new connection. The real GetBlocks request queue is
used for the service checks. A separate independent requester model passed 2,048
generated histories with seed 896, retaining stale writer handles across receiver
generations. The longer runs cover the shared lifecycle and budget models and
the request metadata allocation property, not the full qualification suite.

The shared metadata pool now follows each connection through service fanout,
escalation, and replacement. Fixed checks cover exhaustion and wakeup without a
peer fault. Generated ownership histories compare live allocation owners with
the node and connection counters. The allocation probe found that publication
created another mutex allocation. Inline atomic phase storage now keeps those
transitions inside the funded record and that witness passes. GetBlocks now adds
expected hashes, the taken-work vector, and writer/state allocations to the plan
before taking work. A constrained pool reduces the batch before waiting. The
work vector is moved into the writer, and retry filtering does not allocate
temporary vectors. Status handles retain funding after the writer and response
owner exit. Generated allocation measurements cover request sizes 1–128, with
fixed examples at the capacity boundaries.

The first-use allocation witness now passes. Production includes fixed charges
for pool accounting (512 bytes), each connection context (4 KiB), and each receiver
scope (512 bytes). The probe measures each cold constructor, including tokens and
first-use locks, before measuring authorization. Denied connection, scope, and
request admission must allocate nothing. A separate cold lifecycle probe includes
unfinished cleanup. The node's notification storage remains funded with its pool,
including after the endpoint handle exits.

Connection funding precedes registration, and receiver funding precedes retiring
the prior receiver or publishing registry admission. A fixed service test fills
the pool, attempts replacement, and requires the previous receiver to remain
usable. Retained window capacity now has a separate permit admitted atomically
with the request. Both buffers are charged during growth, and removing entries
keeps backing capacity charged. The receiver witness verifies this before taking
work and after consuming the ending. A shared collection property compares
contents, capacity, allocation peaks, and counters with an independent model
after each generated action. The registry now uses funded response ranges and a
sorted height index. Admission prepares both the routine's scratch snapshot and
the registry's published copy. Publication filters current owners, sorts in place,
and swaps the buffers without allocating. Probes check this while the pool is full
and after each body. Denied growth preserves the current snapshot and allocates
nothing. Replacement releases the old published buffers and fences the old
producer, while that producer's scratch storage keeps its funding until Drop.
Other metadata collections, transport setup, and cancellation children still
require aggregate accounting, so this is not yet a complete metadata bound.

The fixed regression migration preserves its test functions and original work
invariants with legal exchanges. Reordering uses separate requests, retries finish
their old exchange or use another connection, and rejected duplicates must leave
accepted work intact. Two bounded fork/reset replays also found and now guard
scheduling errors: preferring a server for the wrong height, or preferring a
server whose retained authorization forbids requesting that height again.

Network/test all-target Clippy with warnings denied passes. Formatting, Markdown
lint, whitespace, and changelog checks pass. No tests were retried or marked as
expected failures. These three full profile runs have no leak classifications.
An earlier 35-test allocation selection classified
`allocation_plans_are_admitted_as_one_reservation` and
`retained_growth_and_request_admission_are_atomic` as leaky despite passing
assertions. [Nextest reports this](https://www.nexte.st/docs/features/leaky-tests/)
when output handles remain open past its timeout,
not from a heap measurement. Those observations and older classifications remain
unresolved and recorded in the execution logs. No leak timeout or result policy
was changed. The known macOS linker unwind-size warning also remains.

This is not full compliance or activation qualification. Complete authorization
metadata bounds, real discovery and future subscription response adapters, and
funded transport headroom remain open. The current property profile has completed
2,048 cases, but qualification also requires the missing adapters and bounds,
64-round load, and long transport gates.
The earlier decoder stage separately passed 159 chain regressions and six
serialization doc tests.

## Initial results

On 2026-09-10, the compliance profile ran 91 tests with 64 generated cases, seed
896, and four load rounds on Apple Silicon with Rust 1.97.0. It reported **48
passed and 43 failed** against stack base `6cdee100ff129978f75d7ca279aee334cbafe4eb`.
The dependency versions come from this branch's `Cargo.lock`. Several failing
tests exercise the same underlying gap.

| Failing area | Observed difference from the intended rule |
| --- | --- |
| Early framing and decode, F01–F04 | Block uses the broader stream cap. Nonzero flags can wait for payload. Missing transaction bytes can still trigger an 8 KiB vector allocation. Wrong tags or absent authorization can allocate a Block before rejection. |
| Outbound terminal codec, F02 | An out-of-range start height can be encoded even though decoding rejects it. |
| Response identity and rejection, R03–R07 | Some unsolicited, duplicate, out-of-order, wrong-hash, or incorrectly counted responses return success from the receiver instead of a protocol rejection. |
| Authorization lifetime, R02 and R08–R11 | Body completion, deadlines, and local work changes can retire the exchange before its terminal. A local deadline can permit an overlapping retry. |
| Response byte contract, R12 | The receiver does not reject the tested one-byte cumulative excess. A small local size estimate can also cause a peer fault for a response within its advertised cap. |
| Duplex terminal consumption, T01 | Both peers receive all 32 useful bodies, but each records 32 published requests and zero consumed endings. The original connection remains open. |
| Connection headroom, T02 | One paused sibling permits independent service traffic. Two occupied sibling windows block it until their consumers resume. |

All three real checkpoint-verifier controls pass. The new storage ownership,
encoder cancellation, serving boundary, default-traffic, and bounded aggregate
load tests pass in this run. These results are a starting compliance backlog.
They do not qualify the transport for activation.

The expanded property profile reports 26 passed and three failed across 29 tests.
All 22 pre-existing properties pass. Its failures are the new generated terminal
and authorization histories. The inherited regression profile reports 112 passed
and five failed across 117 tests. All five failures are completed-download cases
whose ending assertion now observes actual terminal consumption. Formatting,
targeted network/test/node Clippy, Markdown lint, spelling, and changelog checks
pass. The long activation gate and 64-round scheduled workload were not rerun.

## Requirement map

Identifiers match the research checklist. Test names contain these identifiers.
Each row has a deterministic witness. Generated tests supplement those witnesses.
The shared #896 admission and writer models remain in place.

| ID | Executable observation | Test module |
| --- | --- | --- |
| F01 | Production message caps, tighter negotiated cap, nonzero flags rejected before absent payload waits | `handler::tests::compliance_frames` |
| F02 | Terminals in both directions, legal/invalid heights and counts, truncations, tags, canonical consumption, exact 2 MB Block and excess | `block_sync::wire::compliance` |
| F03 | Actual allocations, missing outer/nested collection bytes, independent element minima, capacity growth edges, complete transaction compatibility, retained bytes distinct from wire bytes | `block_sync::wire::compliance` |
| F04 | Wrong discriminator before Block allocation, absent authorization before decode, invalid identity before handler capacity | Wire and receiver `compliance` modules |
| R01 | Exact hash authorization before first write, immediate response, generated legal prefixes | `block_sync::peer_routine::compliance`, existing request-write regressions |
| R02 | No overlapping retry on the same connection, buffered overlap shapes admitted only after the old terminal write | Receiver and serving `compliance` modules |
| R03 | Needed, servable, obsolete, or another peer's work cannot authorize a body | `block_sync::peer_routine::compliance` |
| R04 | Exact next hash and height, out-of-order and excess bodies rejected before delivery | `block_sync::peer_routine::compliance` |
| R05 | Duplicate parts before/after ending or finality, separately authorized peers as legal control | `block_sync::peer_routine::compliance` |
| R06 | Done start/count equal the consumed nonempty prefix, suffix requeued on legal partial completion | `block_sync::peer_routine::compliance` |
| R07 | Unavailable has the original start/count and zero bodies, retries honor the local floor | `block_sync::peer_routine::compliance` |
| R08 | Last body leaves a terminal obligation, exactly one terminal, no duplicate ending | Receiver and paired QUIC `compliance` modules |
| R09 | Written authorization survives deadline/reassignment and another peer's delivery | `block_sync::peer_routine::compliance` |
| R10 | Finality, reset, and lost local interest preserve original network authorization | `block_sync::peer_routine::compliance` |
| R11 | Cleanup across queued, written, partial, and terminal-pending phases, terminal uses inflight capacity | Receiver `compliance`, existing generation/write regressions |
| R12 | Cumulative actual body bytes, exact cap excluding tags/endings, estimates do not create peer obligations | `block_sync::peer_routine::compliance` |
| R13 | Authorized header with changed body fails real checkpoint verification before commit, valid/local-fault controls | Node `block_sync_driver::tests::compliance` |
| C01 | Actual database operation starts and completions with more commitments than workers | `block_sync::serving::tests::compliance` |
| C02 | Real output/unfinished write stops read-ahead, cancellation releases the dependency | `block_sync::serving::tests::compliance` |
| C03 | Blocked operations stay charged across same/distinct identity reconnects | `block_sync::serving::tests::compliance` |
| C04 | Actual encoder and completed-but-unobserved result remain charged after caller cancellation | `block_sync::serving::tests::compliance` |
| C05 | Separate decoded objects, storage/encoder allocation peaks, bounded frame capacity and retained write ownership | Serving `compliance`, `compliance::load` |
| C06 | Readiness/read failure, encoding error after a prefix, output/handler closure, local verifier-state failure | Serving, receiver, and node `compliance` modules |
| C07 | All counts 1–128, mandatory maximal count/byte examples, gaps, exact fits, changed waiting limits | `block_sync::serving::tests::compliance` |
| T01 | Both peers request and serve useful bodies with workers/output held, independent traffic, exact endings | `paired_block_sync::compliance` |
| T02 | One/two sibling windows occupied, independent service arrival while still paused, original connection resumes | `paired_block_sync::compliance` and existing ordered-worker tests |
| T03 | Every request header/payload split, FIN/reset truncation, finite partial-frame deadline, local-pause resumption | `compliance_frames`, `default_traffic`, existing ordered-worker tests |
| T04 | Another peer completes a useful exchange while a peer holds storage on the same serving node | `paired_block_sync::compliance::default_traffic` |
| L01 | Maximum configured inbound peers, small/large/mixed maximal requests, CPU, storage, registry locks and decoded memory | `serving::tests::compliance::load` |
| L02 | Sequential outside-window/missing-inside-window/tiny responses let a runnable peer progress | `serving::tests::compliance::load` |
| L03 | Retained output plus incomplete setup, same/distinct identity churn, cleanup and useful recovery | `serving::tests::compliance::load` |
| L04 | Sequential useful/empty exchanges and disjoint buffered requests with default QUIC/message-rate settings | `paired_block_sync::compliance::default_traffic` |

R02 observes admission, not arrival of raw bytes. A request buffered behind the
previous response can become legal once that response's ending has been written.
This tests identical, containing, contained, endpoint-overlap, adjacent, and
disjoint ranges without labelling local backpressure a peer violation.

## Shared observations

`zakura-test::allocations` measures allocations during a synchronous operation on
the calling thread. Its allocator delegates to the system allocator. Observation
bookkeeping is excluded, and the observer resets after a panic. It records the
largest request, peak live bytes, and bytes retained at return separately.

`zakura-test::execution` pauses real blocking work at entry or before returning
its result. Its start/finish counts are independent of production permits. A drop
guard unblocks jobs if an assertion fails. `zakura-test::resources` records process
CPU/peak RSS and actual mutex acquisition/hold intervals. These helpers contain no
GetBlocks message logic and can serve other message adapters.

The network binary installs the allocator only under `cfg(test)`. Optional decode
and encode probes, registry ending counts, and lock observations are test-only.
They do not change validation, admission, or ownership in production builds.

The terminal observation increments only when the production receiver consumes
a matched ending. It does not count the last body, bytes arriving on QUIC, a
queued terminal, or a local deadline as completion. It is bounded by live registry
entries and resets with the session generation.

## Workload bounds

The aggregate serving fixture admits eight inbound peers, the configured inbound
maximum, with two workers, one output queue slot per peer, at most 128 requested
blocks, and a 32 MiB response cap. Small results contain 128 distinct bodies.
Large/mixed fixtures contain 20 bodies, sufficient to exercise the byte cap.
Large bodies are independently decoded from stored bytes, with a 1.9 MB script.

The decoded-result ceiling is two times the largest allowed response's attributed
decoded size. Storage allocation measurements include a bounded lookahead body.
Encoder tests observe actual temporary allocations and retained vector capacity.
Queue/write tests hold the actual owner while cancellation and replacement run.
Immutable fixture storage is separate from transient response memory.

Each default load test runs four rounds. Scheduled/manual jobs set
`ZAKURA_REGULATION_LOAD_ROUNDS=64`. The supported range is 1–256. Empty/tiny and
default-transport cases execute 64 sequential exchanges per round. Repeating a
range happens only after its preceding ending. Buffered exchanges use disjoint
ranges within the advertised concurrent-request limit.

The duplex fixture transfers 32 large bodies in each direction. Its serving cache
is populated independently of both verification pipelines, which begin at genesis.
It holds real worker and output capacity, observes independent service traffic,
then releases the dependencies. Storage and final verification are controlled
fixtures there. R13 separately exercises the real checkpoint verifier.

The headroom tests use the native 256 KiB stream and 9.5 MiB connection receive
windows. They require an independent service's frame to arrive within three
seconds while sibling consumers remain stopped. They then resume those consumers
and require useful body completion on the original connection. The fixed
full-occupancy regression pauses 32 sibling consumers and transfers 19 MiB through
another stream before draining the originals. The separate recovery fixture
explicitly selects the former 16/32 MiB windows to reproduce a connection stall.
A reset or timeout cannot stand in for independent progress.

Process CPU and high-water RSS include other tests in the same process. They are
reported evidence, not universal CPU/RSS limits. Run the load profile alone for
comparable measurements. Lock times observe the real admission registry mutex,
not RocksDB locks. Controlled storage tests and real QUIC tests establish their
respective boundaries. They do not certify a combined production database/fleet
workload, all operating systems, every workload duration, or activation readiness.

## Execution

Run every compliance witness, including the real node verifier composition:

```sh
PROPTEST_CASES=64 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network -p zakura -p zakura-test --lib \
  --profile regulation-compliance
```

Run longer workloads independently:

```sh
ZAKURA_REGULATION_LOAD_ROUNDS=64 cargo nextest run --locked \
  -p zakura-network --lib --profile regulation-load
```

The profiles disable retries and fail-fast. PR CI runs the compliance profile
even after an ordinary unit-test failure. Scheduled/manual CI also runs expanded
generated histories and the load profile. Failed assertions remain failed checks.
No new test uses `ignore`, `should_panic`, or expected-failure status.

The existing serving generator now waits for the preceding ending before reusing
its range. The inherited 32,000 overlapping request burst remains explicitly a
hostile pressure fixture. Only that fixture raises the message-rate limit. Legal
traffic uses defaults. Existing short JSON replays and Proptest regression inputs
remain available through the [property guide](regulation-properties.md).

Record the tested head and base, pinned #747 revision, Rust/dependency revisions,
seed, case count, configured workload bounds, and full test results with each run.
A passing sample is evidence for that sample, not proof that every property holds.

[spec]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/specs/peer-message-regulation.md
[plan]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/design/property-testing-block-sync-infrastructure.md
