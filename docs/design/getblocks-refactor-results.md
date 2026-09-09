# GetBlocks refactor execution

## Baseline

Execution starts from #892 at `5d5c8abc8f5b6c00f3a2e7adc03256b55cb18c3c`, with
base `53ccc72b5dcb65713ee4af17216432d1fcfdb134` already included. Both refs were
refreshed on September 8, 2026. The QUIC family remains pinned to
`1dcc7a43488fecd199d343d47e93e9ed8319fcaa`.

Use Rust 1.97.0 on this Mac Studio. The installed stable toolchain lacks its
standard-library archive. Use the same compiler and build profile for baseline
and prototype measurements.

## Criteria fixed before measurement

These are local engineering acceptance thresholds, not a production memory
guarantee. A missing result leaves the new service disabled.

| Workload | Topology and load | Completion and throughput | Memory |
| --- | --- | --- | --- |
| Supported raw transport | Two loopback endpoints in one process; one connection; one active data stream; up to the advertised 32,000 request frames and one sibling service retaining a full 16 MiB window; transfer 64 MiB in one direction | Receive and verify all 64 MiB within 30 seconds, at least 2.13 MiB/s, without reading the paused streams | Peak process RSS at most 512 MiB, measured on the test executable |
| Matched block download | Same endpoints and default windows; A downloads 32 near-maximum-size blocks from B, respecting the advertised count and byte caps; each actual outstanding request matched through its ending message | Complete within 30 seconds; median useful throughput over five runs at least 90% of the current #892 baseline under the same conditions | Peak process RSS at most 512 MiB |
| Incoming request pressure | Same matched download; A's serving slots occupied; B also supplies up to the advertised 32,000 synthetic requests | A's matched download meets the same deadline and throughput floor; request queues plateau at their configured bounds | Same envelope, including retained transport bytes |
| Paused services and impaired link | Matched download and pressure cases with a paused sibling service, then 50 ms RTT and 1% packet loss, then both together | Same completion deadline; at least 90% of the corresponding baseline's useful throughput | Same envelope |
| Reopening | Twenty sequential session replacements on the same connection, also under pressure and link impairment | Every new session completes its matched download within 30 seconds; no old-session delivery | Same peak envelope; session/task counts return to the configured steady-state bound |
| Sustained saturation | Two paused streams each consume 16 MiB; include a case where sibling services retain all credit and a peer that repeats saturation | Cleanup finishes within 10 seconds after the existing block-progress deadline expires; a usable replacement session or peer completes the retry within 30 seconds; preserve existing cooldown and exponential reopen backoff | Same envelope over twenty repetitions; pending/retiring tasks return to their steady-state bounds and unfinished workers keep their permits |
| Transient saturation | The same full buffers, but a paused consumer resumes before the block-progress deadline | The original download completes without a reset or disconnect, within the same 30-second transfer deadline after the consumer resumes | Same envelope |

### Approved loss-gate revision

The original 30-second loss gate failed on both the original PR and the paired
prototype. The original PR completed zero matched blocks because its only cold
probe expired. A separate raw QUIC transfer, without block-sync policy, moved
4 MiB in 12.706 seconds at 50 ms RTT and 1% loss. The existing transport therefore
falls below the original absolute throughput requirement.

On September 8, the user approved including the download-policy fix and keeping
the transport defaults. Before measuring complete downloads with that fix, the
loss gate is revised to **240 seconds**, with a five-run median useful throughput
of at least **90% of the original serving path with the same policy fix**. Apply
this deadline to all impaired-link combinations and reopen rounds. The memory
envelope and all loss-free thresholds above remain unchanged. Keep the original
failed measurements as evidence; they are not passes under the revised gate.

The raw prerequisite diagnoses shared receive credit. It does not exercise
message framing, serving admission, outstanding-request matching, or the new
stream-pair implementation, and cannot establish that the full gate passes.
Use one-way traffic throughout the new probes. The existing generic QUIC
bidirectional byte-transfer regression remains separate transport coverage.

## Transport results

The original full-saturation completion criterion fails with default windows.
The user subsequently selected bounded cleanup and recovery for excessive
traffic. That policy amendment does not turn the diagnostic stall into a
successful recovery test. The generic paired-stream implementation is tested below. No new block-sync capability is enabled.
The checks below use one connection and an
already-established, continuously polled data reader. They transfer bytes in
one direction, from B to A.

Individual measurements on the debug test executable:

| Paused traffic | Data received before releasing paused traffic | Peak RSS | Outcome |
| --- | --- | --- | --- |
| None | 64 MiB in 0.750 s, 85.4 MiB/s | 150.9 MiB | Raw prerequisite passes |
| One unread 16 MiB stream | 64 MiB in 0.719 s, 89.0 MiB/s | 187.6 MiB | Raw prerequisite passes |
| Two unread 16 MiB streams | Zero bytes in 30 s | 187.8 MiB | Completion and throughput fail |
| Production frame workers: request and header queues each hold one frame, with another frame waiting to enter; each stream receives 16 MiB | Zero bytes in 30 s | 189.5 MiB | Completion and throughput fail despite bounded application queues |
| Production frame workers: the advertised 32,000 requests, plus a sibling header stream receiving 16 MiB | 64 MiB in 0.767 s, 83.5 MiB/s | 196.5 MiB | This smaller traffic case passes the raw deadline and memory thresholds |

These are individual diagnostic runs, not five-run matched-download medians.
RSS includes both endpoints and the eventual recovery transfer. All measured
cases fit the declared local memory envelope; the failure is progress.

In both stalled cases, releasing just one paused sibling restored the same
data transfer, which finished about 0.77 seconds later. The data stream and
connection were neither reset nor reopened. In the raw test the application
drained a sibling; in the frame-worker test it stopped the header worker.

### Cause

Two 16 MiB streams can consume the connection's entire 32 MiB receive
allowance. Reading the otherwise-empty data stream cannot release bytes held
by its siblings. The default stream-count allowance is 1,024, so it does not
exclude this three-stream topology.

The production frame reader waits when its bounded application queue is full.
At that point it also stops checking later messages against the message-rate
budget. Reducing that queue to one frame does not prevent QUIC from receiving
the rest of its stream window.

The failed frame-worker case deliberately supplies about 986,895 GetBlocks
frames, well above the default advertised ceiling of 32,000. The advertised
ceiling corresponds to only 544,000 encoded request bytes. That smaller burst
leaves enough connection credit even with one full paused header stream, as
the last measurement demonstrates.

### Scope and decision

The frame-worker probe uses real QUIC, frame parsing, per-message payload caps,
rate admission, and bounded queues. It deliberately pauses the application
consumers and bypasses service negotiation and reactors. It proves what happens
if those consumers remain paused; it does not establish that ordinary peers
produce the full-saturation workload. The sender finishes the paused stream's
send half so acknowledgment confirms receipt; the blocked reader has not
reached that end marker. The full-window frame burst ends inside a frame that
also remains unread.

The selected contract requires completed downloads within the supported
request/service workload and bounded teardown and recovery under excessive
traffic. Full buffers alone do not trigger cancellation. Let temporary
backpressure clear naturally, and use the existing request-expiry and
block-progress deadlines for sustained stalls. Keep the default windows and
QUIC dependency. The original attempt
may fail; its unreceived work must become retryable without losing completed
work or releasing resources still owned by running jobs. If resetting the
block-sync pair does not clear the condition, close the peer connection.
Repeated saturation must leave another usable peer able to make progress.

The saturation rows above record recovery thresholds before measuring
that behavior. Recovery still needs testing through real session management.
The original failed completion measurements remain above as diagnostic evidence.

QUIC dependencies and window settings are unchanged. Paired transport support and
request ownership are implemented below. Production block sync still uses the
existing layout and serving driver while the remaining work is tested.
Initial matched downloads are recorded below. Impaired links, repeated matched
downloads after reopening, baseline medians, and combined conditions are being
measured. The full acceptance gate has not passed.

## Sequential serving and matched downloads

The paired service now reads requests in one sequential task and calls the
storage adapter directly. It waits for peer capacity before node capacity,
reserves output space before each blocking encode, and retains ownership in
database jobs, encoded results, and queued frames. The node constructs the real
state adapter at startup. The production version switch remains off.

Five serving tests pass, including request arrival before Status, bounded Status
setup, a queued frame retaining the peer's slot, and aborting a running database
job while a replacement session waits for the same peer capacity. The full
node compiles with the adapter. The session table replaces queued lifecycle
history, and transport queues now use block sync's configured depths. The
network regression run after these changes passed 934 tests, with three
diagnostic tests ignored.

The first matched-download fixture uses two real QUIC endpoints, normal service
negotiation, peer routines, serving, and the download sequencer. Storage is an
in-memory bounded source; consensus verification is a fixture. A downloads 32
blocks of about 1.9 MB each and consumes every ending message without replacing
either session. These blocks have internally consistent commitments but are not
consensus-valid test-chain blocks.

| Case | Matched completion | Peak process RSS |
| --- | --- | --- |
| Existing serving path in the working tree | 1.789 s | Not measured |
| Paired serving | 1.744 s | Not measured |
| Paired serving; A's 64 serving slots held; B sends 32,000 extra requests | 1.760 s | 148.1 MiB in a separate 1.761 s run |

These are initial single runs. The existing path measurement uses the working
tree, including the request-ownership and queue-limit changes; it is not the
required comparison against the original PR head. The tests establish matched
completion and the pressure run fits the predeclared memory envelope. They do
not complete the full activation gate.

### Download-policy fix and recovery

The original PR at the baseline commit also fails the 30-second impaired test:
zero of 32 blocks complete, after its only cold probe expires. The raw 4 MiB
measurement above separates the transport's throughput limit from that policy
failure.

The shared download-policy fix gives an unmeasured peer the normal request
deadline. Once measured, floor requests retain their shorter base timeout, with
a 256 KiB/s minimum transfer rate. Each deadline includes earlier unreceived
responses because those bytes must pass through the same ordered data stream.
Probe counts, exact ownership checks, and the block-progress timeout remain.

The cold-probe regression verifies delivery after the short rescue deadline and
rejection after the normal deadline. The slow-peer scenario still requires full
completion, no rejection or park, and a smaller final congestion window; it now
allows reliability to remain perfect when the corrected deadlines avoid every
timeout.

| New measurement | Outcome |
| --- | --- |
| Paired download with 32,000 reverse requests, 50 ms RTT, and 1% loss | All 32 blocks and endings in 205.002 s; no session replacement |
| Paired download with reverse requests and one paused sibling service | All blocks and endings in 1.809 s |
| Two paused siblings; consumers resume after one second | No complete body before resume; original pair completes in 2.782 s |
| Two siblings remain paused | Existing data-write timeout closes the connection; session and serving slots return to their initial counts; a fresh peer completes returned work in 1.473 s |
| Twenty pair replacements with reverse request pressure, without loss | Every round completes; summed transfer time 35.236 s; real reopen backoff retained |

The saturation test originally assumed the 32-second block-progress timeout
would act first. The existing ten-second data-write timeout actually closes this
fully blocked connection first, about 9.6 seconds after download timing starts.
This is an existing deadline, not a new fullness timer. Both temporary and
sustained cases keep the default windows.

The full network-library run passed 1,230 tests. Three listener tests cannot bind
their additional loopback source addresses on this Mac. A fourth test required a
reliability dip even when no request expired; after adapting that assertion, all
947 Zakura tests passed, with 11 standalone gates ignored. The node also compiles.
Five-run baseline comparisons and impaired reopen rounds remain in progress.

## Paired transport and request ownership

The transport can now admit two declared ordered streams as one session. Each
prelude is followed by the same nonzero eight-byte little-endian pair ID, scoped
to that connection and opener. Neither role reaches the service alone. A missing
role expires under the setup deadline; mismatched or duplicate roles are rejected.
Both workers share cancellation and the service message budget. Retirement waits
for both workers, including their readers, before reporting a single session exit.
Request writes can wait beyond the generic ten-second timeout. Data writes retain
their deadline. An interrupted partial write resets the pair.

Three real-QUIC tests passed: repeated pair reopening on the same connection,
request backpressure beyond ten seconds while data continues, and bounded cleanup
of incomplete or mismatched setup. These exchange test frames; they are not yet
matched block downloads through the new serving task.

Outgoing requests now reserve a queue slot first. The work queue gives each take
an exact provisional owner. Outstanding-request publication, byte-ledger transfer,
and enqueue happen under the same lock as reset and the writer's initial claim.
Expiry skips an unwritten frame but lets a started frame finish. Dropping an
unfinished write cancels its session. Only the matching work owner can return
unreceived reservations; received blocks and replacement attempts survive cleanup.
A queue that closes after slot reservation triggers explicit settlement.

Eight focused ownership tests passed, including a reset on another thread while
publication holds the lock. The broader network regression run passed 929 tests
with three ignored tests in 52.75 seconds, including version-selection coverage.
Network library/test Clippy also passed.
The new service version remains disabled: full matched-download, loss, memory,
throughput, and saturation-recovery gates are still outstanding.

## Storage prototype

Added `ReadStateService::read_owned_block_range` and `OwnedBlockRange<R>`.
The API moves the caller's resources into one blocking database job and then
into the returned block prefix. It reuses state readiness checks and the
existing bounded range-read helper. The single-execution operation is separate
from the cloneable `ReadRequest` enum.

Cancellation is checked before the first lookup and between lookups. It stops
further reads; a lookup already running keeps its resources until it exits.
Dropping the async future or aborting its task does not release those resources.
An undelivered result drops its blocks and resources when the job finishes.

Nine tests passed, covering the byte cap and retained result,
cancellation before/between lookups, a dropped waiter, an aborted caller,
panic unwinding, and the public API against empty and populated databases,
including failure of state readiness checks. The concurrency
tests wait until a real blocking job has started, terminate its async owner,
assert that capacity is still charged, then release the job and verify exactly
one resource release.

The network interface and node adapter are wired into the paired serving task.
The old production driver still uses `BlockRangeQueryLease` and
`ReadRequest::BlocksByHeightRange` until the paired version is enabled.

## Reproduction and checks

The probes live in `handler/tests/quic_progress.rs` in the network crate.
Build with `cargo +1.97.0 test -p zakura-network --lib --locked --no-run`.
Use the emitted test-executable path with `/usr/bin/time -l`, a fully qualified
test name, and `--exact --nocapture` to measure a single test's peak RSS without
including compilation. The test prints bytes received and transfer duration.

`two_paused_streams_exhaust_default_connection_credit` and
`paused_frame_workers_exhaust_default_connection_credit` succeed as regression
tests when they reproduce the stall and recovery. A passing test therefore
records a failed transport acceptance criterion, not successful activation.

Validation completed for this checkpoint:

- All six `zakura::handler::tests::quic_progress` tests passed in 69.37 seconds,
  including the existing transport regression. Peak RSS for this sequential
  combined run was 284.3 MiB.
- `cargo +1.97.0 fmt --all -- --check` passed.
- All nine `service::block_range::tests` state tests passed in 1.19 seconds.
- `cargo +1.97.0 clippy -p zakura-state --lib --tests --locked -- -D warnings`
  passed.
- `cargo +1.97.0 clippy -p zakura-network --lib --tests --locked -- -D warnings`
  passed. The two diagnostic functions explicitly allow stderr output so
  standalone measurements remain visible regardless of tracing filters.
- Both design documents passed the repository's Markdown lint configuration;
  `git diff --check` passed.

The test link emitted the pre-existing macOS compact-unwind size warning. It
did not prevent linking or execution. Full workspace and node-driver validation
is pending until production serving moves to the new path.

The original worktree disappeared during execution. Its uncommitted edits were
recovered from this task's recorded changes into
`/Users/czstudio/Documents/zakura-worktrees/getblocks-two-stream-refactor`.
The original task path links to that checkout. The 929-test run and Clippy results
above were repeated against the recovered source; Markdown and changelog checks
also passed.
