# GetBlocks compliance tests

These tests check the pinned [regulation specification][spec] and
[GetBlocks testing plan][plan]. Each contract lives beside its enforcement.
This guide maps the complete requirement set and defines the combined workloads.
A passing run is evidence for its selected cases, not complete message compliance
or transport activation readiness.

For example, A requests heights 100–102 and receives only block 100. B sends
`BlocksDone(100, 3)`. The receiver must reject the ending because only one body
was consumed. Returning success while recording a fault does not satisfy the test.

The accepted negotiated layout is data stream 6 version 3 and request stream 7
version 1. The tests preserve this decision from the implementation stack.

## Review layers

| Contract | Guide |
| --- | --- |
| Bounded decoding | [Decoder bounds](bounded-decoding.md) |
| Frame validation | [Frame enforcement](frame-enforcement.md) |
| Serving ownership | [Serving ownership](serving-ownership.md) |
| Response identity and lifetime | [Response lifetime](response-lifetime.md) |
| Session replacement | [Session fencing](response-session-fencing.md) |
| Shared metadata | [Response memory](response-memory.md) |
| Request allocation plans | [Allocation planning](request-allocation-planning.md) |
| Transport and composed workloads | Requirement map and workload bounds below |

## Continuous traffic

The 64-round L04 workload attempts 4,096 sequential legal exchanges with the
default limits. It exposed a shared rate limiter charging requested responses.
After enough fast answers, the requester disconnected its serving peer. The
failure also occurred with the earlier transport windows, so changing receive
credit did not cause it.

The shared admission policy now uses capacity and authorization for GetBlocks,
Block, BlocksDone and RangeUnavailable. Status and unconfigured services retain
their existing frequency checks. The L04 request sequence, workload and assertion
remain unchanged. Ordered-reader diagnostics record `ordered_rate_limited` before
teardown can replace the rejection with a generic read error.

`handler::tests::message_admission` checks generated mixtures of message types,
payload sizes, bursts and refills against an independent count. It runs the same
oracle with production GetBlocks declarations and a discovery test adapter. Fixed
controls check that responses preserve metadata credit and continue after its
exhaustion. A real ordered worker still rejects excess Status traffic and records
the specific cause. These admission tests supplement response authorization and
resource-ownership tests in the earlier layers.

## Stream limits

The hello, handshake acknowledgement and application admission use the same
16-stream ceiling as QUIC. Lower configured limits still apply. For example,
configuring 32 advertises 16, so a service layout requiring 17 streams is rejected
before it can wait for transport capacity that will never arrive. The ceiling
keeps the qualified receive-window budget valid.

`handler::tests::stream_limits` checks generated local and remote limits and fixed
boundaries. Real QUIC peers open every advertised stream in both directions and
verify that the next open waits while those streams remain live.

## Deferred requirements

The stack uses published transport packages without overrides. The receive policy
allows 16 remotely initiated bidirectional streams, 256 KiB per stream and 9.5 MiB
per connection. Fixed progress tests exercise these settings. They do not bound
arbitrary local stream churn or all transport allocations.

Five witnesses preserve their complete assertions and individual ignore reasons.
Their module is also excluded from compilation because the published packages
lack APIs those bodies call. They cannot run with `--ignored` alone and never
count as passing tests. The [transport capacity guide](../design/native-transport-capacity.md)
lists each witness, its dependency and integration requirements, and the remaining
endpoint, transport storage and aggregate allocation work.

Full qualification also requires sustained load and optimized five-sample
throughput comparisons. The window comparison experiment reports ratios but does
not assert the 90 percent threshold. Review its measurements separately from the
activation gates. Test-only discovery and subscription adapters demonstrate shared
contracts. They do not migrate production discovery or complete subscription
publisher cursor and crossing Grant/Close coverage.

## Requirement map

Identifiers match the research checklist. Test names contain these identifiers.
Each row has a deterministic witness. Generated tests supplement those witnesses.
Shared admission and writer models supplement the message-specific witnesses.

| ID | Executable observation | Test module |
| --- | --- | --- |
| F01 | Production message caps, tighter negotiated cap, nonzero flags rejected before absent payload waits | `handler::tests::frame_policy` |
| F02 | Terminals in both directions, legal/invalid heights and counts, truncations, tags, canonical consumption, exact 2 MB Block and excess | `block_sync::wire::{frame_codec,bounded_decoding}` |
| F03 | Actual allocations, missing outer/nested collection bytes, independent element minima, capacity growth edges, complete transaction compatibility, retained bytes distinct from wire bytes | `block_sync::wire::{frame_codec,bounded_decoding}` |
| F04 | Wrong discriminator before Block allocation, absent authorization before decode, invalid identity before handler capacity | `wire::frame_codec` and receiver `response_contract` |
| R01 | Exact hash authorization before first write, immediate response, generated legal prefixes | `block_sync::peer_routine::response_contract`, existing request-write regressions |
| R02 | No overlapping retry on the same connection, buffered overlap shapes admitted only after the old terminal write | Receiver `response_contract` and serving `serving_contract` |
| R03 | Needed, servable, obsolete, or another peer's work cannot authorize a body | `block_sync::peer_routine::response_contract` |
| R04 | Exact next hash and height, out-of-order and excess bodies rejected before delivery | `block_sync::peer_routine::response_contract` |
| R05 | Duplicate parts before/after ending or finality, separately authorized peers as legal control | `block_sync::peer_routine::response_contract` |
| R06 | Done start/count equal the consumed nonempty prefix, suffix requeued on legal partial completion | `block_sync::peer_routine::response_contract` |
| R07 | Unavailable has the original start/count and zero bodies, retries honor the local floor | `block_sync::peer_routine::response_contract` |
| R08 | Last body leaves a terminal obligation, exactly one terminal, no duplicate ending | Receiver `response_contract` and paired QUIC `qualification` |
| R09 | Written authorization survives deadline/reassignment and another peer's delivery | `block_sync::peer_routine::response_contract` |
| R10 | Finality, reset, and lost local interest preserve original network authorization | `block_sync::peer_routine::response_contract` |
| R11 | Cleanup across queued, written, partial, and terminal-pending phases, terminal uses inflight capacity | Receiver `response_contract`, existing generation/write regressions |
| R12 | Cumulative actual body bytes, exact cap excluding tags/endings, estimates do not create peer obligations | `block_sync::peer_routine::response_contract` |
| R13 | Authorized header with changed body fails real checkpoint verification before commit, valid/local-fault controls | Node `block_sync_driver::tests::verification_contract` |
| C01 | Actual database operation starts and completions with more commitments than workers | `block_sync::serving::tests::serving_contract` |
| C02 | Real output/unfinished write stops read-ahead, cancellation releases the dependency | `block_sync::serving::tests::serving_contract` |
| C03 | Blocked operations stay charged across same/distinct identity reconnects | `block_sync::serving::tests::serving_contract` |
| C04 | Actual encoder and completed-but-unobserved result remain charged after caller cancellation | `block_sync::serving::tests::serving_contract` |
| C05 | Separate decoded objects, storage/encoder allocation peaks, bounded frame capacity and retained write ownership | Serving `serving_contract`, `serving_contract::load` |
| C06 | Readiness/read failure, encoding error after a prefix, output/handler closure, local verifier-state failure | Serving `serving_contract`, receiver `response_contract` and node `verification_contract` |
| C07 | All counts 1–128, mandatory maximal count/byte examples, gaps, exact fits, changed waiting limits | `block_sync::serving::tests::serving_contract` |
| T01 | Both peers request and serve useful bodies with workers/output held, independent traffic, exact endings | `paired_block_sync::qualification` |
| T02 | One/two sibling windows occupied, independent service arrival while still paused, original connection resumes | `paired_block_sync::qualification` and existing ordered-worker tests |
| T03 | Every request header/payload split, FIN/reset truncation, finite partial-frame deadline, local-pause resumption | `frame_policy`, `default_traffic`, existing ordered-worker tests |
| T04 | Another peer completes a useful exchange while a peer holds storage on the same serving node | `paired_block_sync::qualification::default_traffic` |
| L01 | Maximum configured inbound peers, small/large/mixed maximal requests, CPU, storage, registry locks and decoded memory | `serving::tests::serving_contract::load` |
| L02 | Sequential outside-window/missing-inside-window/tiny responses let a runnable peer progress | `serving::tests::serving_contract::load` |
| L03 | Retained output plus incomplete setup, same/distinct identity churn, cleanup and useful recovery | `serving::tests::serving_contract::load` |
| L04 | Sequential useful/empty exchanges and disjoint buffered requests with default QUIC/message-rate settings | `paired_block_sync::qualification::default_traffic` |

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

Each default load test runs four rounds. Longer local runs set
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

## Local execution

No profile adds a CI trigger. Select the complete requirement suite locally:

```sh
PROPTEST_CASES=64 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network -p zakura -p zakura-test --lib \
  --profile regulation-compliance
```

Run longer workloads in isolation for comparable measurements:

```sh
ZAKURA_REGULATION_LOAD_ROUNDS=64 cargo nextest run --locked \
  -p zakura-network --lib --profile regulation-load
```

The profiles disable retries and fail-fast. The property and replay commands are
in the [shared property guide](regulation-properties.md). Fixed regressions use
`blocksync-regression`. Optimized transport experiments use
`blocksync-transport-gate` with `--run-ignored=all` and require a separate deliberate
run. Keep their ignored status distinct from dependency-excluded witnesses.

Legal repeated ranges wait for the preceding ending. Buffered requests use disjoint
ranges. Only the explicitly hostile 32,000-request pressure fixture raises the
message-rate limit. Other traffic uses defaults.

Record the tested head and base, pinned specification revision, toolchain,
dependency versions, seed, case count, workload bounds and full results. Do not
reuse historical passing totals as evidence for a new revision.

[spec]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/specs/peer-message-regulation.md
[plan]: https://github.com/zakura-core/zakura/blob/d4f2fad598294d391b475f9c22e63352557ba5ff/docs/design/property-testing-block-sync-infrastructure.md
