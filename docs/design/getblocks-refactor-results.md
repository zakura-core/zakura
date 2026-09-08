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
| Raw transport prerequisite | Two loopback endpoints in one process; one connection; one active data stream; zero, one, or two other streams each retaining a full 16 MiB window; transfer 64 MiB in one direction | Receive and verify all 64 MiB within 30 seconds, at least 2.13 MiB/s, without reading the paused streams | Peak process RSS at most 512 MiB, measured on the test executable |
| Matched block download | Same endpoints and default windows; A downloads 32 near-maximum-size blocks from B, respecting the advertised count and byte caps; each actual outstanding request matched through its ending message | Complete within 30 seconds; median useful throughput over five runs at least 90% of the current #892 baseline under the same conditions | Peak process RSS at most 512 MiB |
| Incoming request pressure | Same matched download; A's serving slots occupied; B also supplies synthetic requests up to the transport's receive allowance | A's matched download meets the same deadline and throughput floor; request queues plateau at their configured bounds | Same envelope, including retained transport bytes |
| Paused services and impaired link | Matched download and pressure cases with a paused sibling service, then 50 ms RTT and 1% packet loss, then both together | Same completion deadline; at least 90% of the corresponding baseline's useful throughput | Same envelope |
| Reopening | Twenty sequential session replacements on the same connection, also under pressure and link impairment | Every new session completes its matched download within 30 seconds; no old-session delivery | Same peak envelope; session/task counts return to the configured steady-state bound |

The raw prerequisite diagnoses shared receive credit. It does not exercise
message framing, serving admission, outstanding-request matching, or the new
stream-pair implementation, and cannot establish that the full gate passes.
Use one-way traffic throughout the new probes. The existing generic QUIC
bidirectional byte-transfer regression remains separate transport coverage.

## Transport results

The full-saturation prerequisite fails with default windows. No new capability
has been implemented or enabled. The checks below use one connection and an
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

Keep the failed criterion recorded. There are two different contracts to
choose between before proceeding past this gate:

- Preserve completed downloads even when multiple other streams consume their
  entire allowances. This requires an enforceable source of receive credit for
  data, such as isolation or a revised window and stream-admission policy.
  Two streams and small application queues alone do not provide it.
- Guarantee progress within an explicit supported request/service workload;
  require bounded teardown and successful later reopening under excessive
  traffic. This could retain the selected default windows, but changes the
  full-saturation completion criterion. That recovery path still needs testing
  through real session management.

No QUIC dependency, window setting, stream layout, or overload policy has been
changed. The independent storage prototype below has passed its initial focused
tests; the transport and serving migration has not started.
Matched downloads, impaired links, repeated reopening, production baseline
comparisons, and their combined conditions remain unmeasured. The full
acceptance gate has not passed.

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

The network interface and node adapter are not wired yet. The existing driver
still uses `BlockRangeQueryLease` and `ReadRequest::BlocksByHeightRange`; this
prototype does not change its production behavior.

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
is pending because the serving migration has not begun.
