# GetBlocks refactor results

The earlier transport acceptance measurements used the previous QUIC dependency.
They do not establish readiness for the separate Iroh upgrade in #935. The planned
deployment combines the activation draft with that upgrade and requires validation
of the combined code and dependencies.
Historical measurements and their exact dependency context are retained below.

## Iroh integration

On September 9, 2026, an isolated local checkout combined the corrected activation
code at `0ccedba6069e978c53c6819ca3ad748f3b85df3e` with #935 at
`39663a59ef8b872c8a97f56b45c4445d45f78b51`. Local integration commit
`52de2864c` resolves the merge and migrates the newly added transport fixtures to
Iroh 1.1. The separately owned upgrade branch was not changed.

The final refresh also integrates #935 at
`b3a6a3fcb6419fad43f57627c0af0a8687e4463f` in local commit `56fcfffc7`.
That refresh changes only CI compatibility, decision documentation, and audit
configuration. Its Rust sources, Cargo manifests, and lock file are identical
to the measured local tree at `122eaced2`.

- All 87 ordinary handler tests passed, including the raw QUIC transfer test.
- All 345 selected block-sync, guarded-transport, and request-regulation tests passed.
- All 22 ordinary cluster integration tests passed.
- Network and node Clippy passed with all targets and warnings denied.

These 454 tests ran without retries. The standalone transport gates and one
ignored cluster test were excluded from those counts. The full workspace runtime
suite and a new five-run throughput comparison were not run.

The first combined loss-and-paused-service probe was rejected: Iroh discovered
the other endpoint's real loopback address and routed around the impaired proxy.
Local test-only commit `122eaced2` keeps each endpoint on its initial proxy path,
using Iroh's path-selector API only through a development dependency. Production
transport settings and flow-control windows are unchanged. The failed probe is
not a successful impairment measurement.

The following targeted measurements passed at `122eaced2`, without retries:

| Workload | Matched download result | Whole-test wall time | Peak RSS |
| --- | --- | --- | --- |
| Request pressure, one paused sibling, 50 ms RTT, and 1% loss | All 32 blocks and endings in 203.305 s; proxy path and loss verified | 268.76 s | 174.4 MiB |
| Sustained saturation followed by a fresh peer | Returned work completes in 1.553 s; serving and session slots return | 22.06 s | 214.0 MiB |
| Twenty pair reopenings under request pressure | Every round completes; 35.992 s of useful transfer in total | 178.60 s | 130.7 MiB |

Whole-test time includes fixture setup, pressure injection, reopen backoff, and
cleanup. The download deadline starts when that round's work is submitted.
Each measured process includes both endpoints and remains below the 512 MiB cap.
Network all-target Clippy also passed after the test-only path-selector change.

### Bounded setup and recovery pass

The scoped fixes landed in #943 at `72d5f96d1` and were merged through #944
and #945. They preserve the paired-stream design: one incoming setup future
keeps the connection loop responsive; a complete pair identity precedes service
reservation; an expired incomplete offer gets one setup interval of retry
backoff. A replacement that races old workers is reset locally and can retry
without disconnecting sibling services. Retiring owners keep their permits.

Four real QUIC regressions fail on the preceding #943 revision `5873a6ef0` and
pass with the fixes. The transport chunk passes all 80 handler tests and 20
additional immediate-replacement runs. All-target network Clippy passes.

Local integration `5a315614a` combines the final production changes with #935 at
`b3a6a3fcb`, using Iroh compatibility revision
`9e6cbf251d2edd6304c2d9799fc29bf39788d305`. Its test-only Iroh API and proxy-path
adaptations are retained. All 458 focused handler, block-sync, guarded-transport,
request-regulation, and ordinary cluster tests pass without retries. Network and
node all-target Clippy pass. Seven ignored tests are excluded from that count.

The following existing transport gates ran as standalone processes on this
combined revision. Loss and recovery workloads ran concurrently; these are
completion, resource-release, and memory checks, not throughput comparisons.

| Workload | Result | Whole-test wall time | Peak RSS |
| --- | --- | --- | --- |
| Request pressure, 50 ms RTT, 1% loss | 32 blocks and endings; proxy path and loss verified | 211.46 s | 141.2 MiB |
| Request pressure, paused sibling, 50 ms RTT, 1% loss | 32 blocks and endings; proxy path and loss verified | 264.68 s | 172.3 MiB |
| Twenty saturation and fresh-peer recovery cycles | 20/20 retries; serving and session slots return | 442.65 s | 240.9 MiB |
| Twenty pair reopenings under request pressure | 20/20 matched downloads on the same connection | 181.04 s | 158.1 MiB |

The bounded pass did not expand into legacy Ping allocation, general stream-open
rate charging, or existing frame-deadline policy. The remaining activation
qualification is the twenty-round lossy reopening gate and the five-run throughput
comparison on the final Iroh dependency set. #935's package-build and publish-graph
CI failures also remain separate requirements. These results do not clear those
gates or authorize a production deployment.

## Baseline and criteria

The baseline is #892 at `5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c`, including
base `53ccc72b5dcb65713ee4af17216432d1fcfdb134`. Both refs were refreshed on
September 8, 2026. The QUIC family stays pinned to
`1dcc7a43488fecd199d343d47e93e9ed8319fcaa`.

Measurements use Rust 1.97.0 debug test executables on the same Mac Studio.
Each matched download uses two real QUIC endpoints in one process. A receives
32 blocks of about 1.9 MB each from B, matching every block and ending to its
actual outstanding request. Storage and consensus verification are fixtures;
the blocks have consistent commitments but are not consensus-valid chain blocks.

These local engineering thresholds were fixed before measuring each comparison.
They are not a production memory guarantee.

| Workload | Required result |
| --- | --- |
| Ordinary matched download | All blocks and endings within 30 seconds |
| Incoming request pressure | Same completion while A's serving slots are occupied and B sends 32,000 synthetic requests |
| Paused sibling service | Same completion with one sibling retaining a full 16 MiB window |
| 50 ms RTT and 1% loss, including pressure and paused sibling | All blocks and endings within 240 seconds |
| Throughput comparison | Five-run median useful throughput at least 90% of the original serving path with the same policy fixes |
| Process memory | Peak RSS at most 512 MiB, including both endpoints |
| Reopening | Twenty replacements on the same connection, including loss; each new session completes within its applicable deadline |
| Temporary saturation | Resuming a paused consumer before liveness expires lets the original pair complete without a reset |
| Sustained saturation | Bounded cleanup returns unreceived work; a usable peer completes the retry within 30 seconds; twenty cycles stay within memory and session bounds |

Only A must complete a matched download. B's extra requests are synthetic serving
pressure, not a second matched download requirement.

## Completed comparisons

Each row contains five runs of each implementation. Every run completed all
blocks and endings without replacing its session. The baseline includes the same
download-policy fix. Paused fixture streams use a one-frame application queue on
both paths; the original block-sync queues are otherwise unchanged.

| Workload | Original median | Paired median | Paired useful throughput versus original | Highest RSS across both paths |
| --- | --- | --- | --- | --- |
| Ordinary download | 1.802 s | 1.781 s | 101.2% | 182.4 MiB |
| 32,000 reverse requests | 1.830 s | 1.827 s | 100.1% | 186.3 MiB |
| Reverse requests and a paused 16 MiB sibling | 1.781 s | 1.803 s | 98.8% | 210.2 MiB |
| Reverse requests, 50 ms RTT, and 1% loss | 203.164 s | 205.934 s | 98.7% | 150.0 MiB |
| Reverse requests, paused sibling, 50 ms RTT, and 1% loss | 210.156 s | 210.811 s | 99.7% | 179.7 MiB |

All 50 runs pass. Both implementations use the approved 32-second block-sync
data-write deadline in the combined-condition row. The first four rows used the
previous ten-second deadline; it did not fire. QUIC and its windows are identical
throughout.

After removing the activation override, the combined-condition test completed
again through normal service selection: **213.178 seconds**, every block and
ending, and **183.5 MiB peak RSS**. With the final ownership observations and
session metrics, it passed again in **211.588 seconds** at **170.1 MiB peak RSS**.

| Recovery test | Result |
| --- | --- |
| Twenty loss-free replacements under request pressure | Every round completes; 35.236 seconds of useful transfer in total; real reopen backoff retained |
| Twenty replacements with loss | 640 matched blocks and endings; every round within 240 seconds; 4,129.461 seconds of useful transfer in total; 171.3 MiB peak RSS |
| Two full paused siblings resume after one second | Original pair completes in 2.782 seconds without a reset |
| Twenty sustained-saturation and fresh-peer retries with the approved data deadline | Every retry completes from returned work without a new work submission; serving and session slots return to initial counts; 257.6 MiB peak RSS |

In the sustained-saturation fixture, an unrelated sibling's unchanged ten-second
write deadline closes the blocked connection first. That test proves recovery
and resource ownership; it does not measure the exact 32-second block-sync timer.

## Decisions supported by failed probes

### Shared connection credit

Two paused streams can consume all 32 MiB of connection receive allowance.
Reading an empty data stream cannot release bytes held by those siblings.
Reducing the application queues to one frame does not reduce QUIC's windows.

| Raw transport probe | Result before releasing paused traffic | Peak RSS |
| --- | --- | --- |
| No paused stream | 64 MiB in 0.750 s | 150.9 MiB |
| One full 16 MiB paused stream | 64 MiB in 0.719 s | 187.6 MiB |
| Two full 16 MiB paused streams | Zero data bytes in 30 s | 187.8 MiB |
| Two full windows behind bounded production frame workers | Zero data bytes in 30 s | 189.5 MiB |
| Advertised 32,000 requests plus one full paused sibling | 64 MiB in 0.767 s | 196.5 MiB |

The full-window request burst is about 986,895 frames. The advertised 32,000
requests occupy only 544,000 framed bytes. Releasing one paused sibling restored
the original stalled transfer, which completed about 0.77 seconds later.

The user selected bounded cleanup and a completed retry for sustained excessive
traffic, rather than requiring the original saturated attempt to complete.
Fullness alone does not trigger a new timer. Temporary pressure can clear;
existing write, request, and block-progress deadlines handle sustained stalls.
The recovery tests above establish this behavior through real session management.

### Download deadlines

The original PR and paired prototype both failed the original 30-second loss
gate. The original completed zero matched blocks because its only cold probe
expired. A raw 4 MiB transfer without block-sync policy took 12.706 seconds under
the same 50 ms RTT and 1% loss. The default transport could not meet the original
absolute throughput requirement.

The user approved including the download-policy fix and, before measuring the
complete comparisons, revising lossy completion to 240 seconds plus the 90%
relative throughput floor. Loss-free deadlines and the memory envelope stayed
unchanged. The failed original measurements remain failures.

The policy fix gives an unmeasured peer the normal bounded request deadline.
Measured peers include transfer time for the requested body and earlier
unreceived responses on the ordered data stream, with the existing 256 KiB/s
minimum estimated rate. Cold-probe limits, exact ownership, and block-progress
liveness remain in force.

### Data writes

Combining loss with a paused sibling exposed a separate limit. The original
serving path completed 16 of 32 blocks before a write timeout; paired diagnostic
runs reached 16 and 17 blocks. Healthy writes took 13.8–14.1 seconds while earlier
blocks were still arriving, exceeding the generic ten-second deadline.

A 32-second prototype completed the combined workload in 208.527 seconds at
171.9 MiB peak RSS. The user approved that deadline for paired data writes,
including Status and ending messages. The completed comparison gives both paths
the same deadline. Request writes remain cancellation-aware; setup and unrelated
services retain their existing deadlines. No QUIC change was needed.

## Correctness coverage

The sequential serving tests cover requests arriving before Status, missing
Status, bounded and unavailable ranges, storage failures, and output congestion.
Eighteen storage/queue combinations preserve the exact available prefix and
ending. A read lasting beyond the former eight-second query timeout can finish
normally. Cancellation and stale-session tests keep the actual job charged and
prevent old output from reaching a replacement.

Eleven owned-state-read tests exercise the byte cap, retained results, cancellation
before and between lookups, a dropped waiter, an aborted caller, panic unwinding,
and the public API against empty and populated databases. The concurrency tests
wait for a real blocking job, terminate its caller, verify retained ownership,
then release the job and observe exactly one resource release.

The request ownership tests cover expiry versus the writer's first claim and
reset on another thread during publication. Publication, ledger transfer,
enqueue, and reset share an explicit lock. Only the exact owner can return
unreceived reservations; received blocks and replacement work survive cleanup.
An unfinished write cancels its pair.

Pair tests cover incomplete and mismatched setup, duplicate roles, request
backpressure beyond ten seconds while data continues, and repeated reopening.
The independent ownership model retains negative controls and uses replay format
version 4. Long transport gates have a separate profile and are excluded from
routine CI, including profiles that run ignored tests.

## Reproduction and final checks

Build the network tests with
`cargo +1.97.0 test -p zakura-network --lib --locked --no-run`.
Run the emitted executable with `/usr/bin/time -l`, the fully qualified test
name, and `--exact --nocapture` to measure RSS without compilation.

Matched downloads are in `handler/tests/paired_block_sync.rs`; long comparisons
and reopen repetitions are under its `gate` module. The earlier raw probes were
removed after the paired tests covered completion, pauses, saturation, and retry.
Their [source remains in history](https://github.com/zakura-core/zakura/blob/7cf0d46d0c0b0a3680f032935384a397d0c324d9/crates/zakura-network/src/zakura/handler/tests/quic_progress.rs).
At the measured head, `quic_progress.rs` retained the QUIC backport regression test. That test and the backport were subsequently removed from the replacement stack when #941 was closed in favor of the separate Iroh upgrade in #935. The `blocksync-transport-gate` nextest profile
selects those explicit long-running measurements. Local measurement logs and
median JSON files are retained under `target/getblocks-gate/`.

A passing diagnostic stall test confirms the expected stall and recovery;
it is not a successful completion measurement. The matched results above are
the activation evidence.

Checks before the cleanup, at `c7481f0cd`:

- Workspace Clippy with all targets and warnings denied passes.
- The regulation profile lists and runs 102 tests; all pass without retries,
  including the final observation guards and nine owned-state-read tests.
- All 22 Zakura integration tests pass, including old and mixed capability
  advertisements retaining other negotiated services after rejecting the old
  block-sync layout.
- The final network run passes 1,236 tests. Ten remain ignored, and the three
  unavailable loopback tests listed below are excluded.
- The workspace run passes all 402 node library tests, 572 state library tests,
  and 1,234 network library tests. Other workspace library and doc-test targets
  pass. Six tests fail for the conditions listed below. This broad run precedes
  the final metrics additions; the focused checks above include them.
- Formatting, Markdown lint, and changelog validation pass.

The three network failures are
`listener_bans_zcashd_compat_peer_before_reserved_slot`,
`listener_reserves_one_zcashd_compat_inbound_slot`, and
`listener_zcashd_compat_reconnect_bypasses_recent_ip_limit`. Their additional
loopback source addresses are unavailable on this Mac (OS error 49).

The three node acceptance failures are `activate_mempool_mainnet`,
`restart_stop_at_height`, and `sync_one_checkpoint_mainnet`. They run the legacy
P2P stack, fail to establish usable public peer connections, and expire waiting
for sync progress. These are not paired-transport runs. Host networking and
legacy peer policy were left unchanged. The macOS compact-unwind linker warning
also remains; it does not prevent linking or execution.

### Cleanup validation against main

The file-by-file cleanup removes unused send APIs, duplicate test admission and
lifecycle adapters, and superseded raw transport probes. Tests now use the real
admission future and session table. The owned state-read API retains its byte
cap; the existing unowned API keeps its original signature.

With Rust 1.97.0 on September 9, 2026:

- All 1,230 selected network tests and 11 owned-state-read tests pass.
- The regulation profile lists and passes 103 tests without retries.
- All 22 integration tests pass without retries.
- Workspace Clippy with all targets and warnings denied, formatting, Markdown
  lint, and changelog validation pass.

The first network run exposed a test timing assumption: connection setup took
3.27 seconds after starting a three-second cooldown. The test now keeps its
cooldown longer than its connection deadline and asserts that the park is still
live before checking admission. No production cooldown changed.

The three unavailable loopback-address tests above remain excluded. The long
loss, reopening, and repeated-saturation gates retain their earlier measurements;
they were not rerun for this cleanup. Their transport settings and serving
algorithms are unchanged, and the routine matched-download and recovery tests
pass again. The full workspace run was not repeated.

### Test consolidation

At `2fd9cac34`, duplicate admission checks are consolidated in the shared request
tests. Separate regressions still cover dropping a read waiter, aborting its
caller, and each transport-write outcome; they now share their setup. Paired QUIC
fixtures also share native connection negotiation. The storage outcome matrix
keeps all four distinct outcomes at three queue depths, removing six repeated
failure cases.

The independent ownership models, generated histories, and JSON replay move
to #896. Fixed regressions for one block per response, 64 active responses, execution
claims, reconnects, and cancellation remain in #892. This changes test placement
and fixtures, with no change to production behavior or transport settings.

The `blocksync-regression` profile passes all 84 tests without retries, and the
integration profile passes all 22 tests. Workspace all-target Clippy passes.
The earlier long transport measurements above remain the validation evidence for
loss, repeated reopening, and sustained saturation; those gates were not rerun
for this test consolidation.
