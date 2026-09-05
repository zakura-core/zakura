# GetBlocks workload capture

Workload capture starts where the peer routine decodes a message. Transport and
pending-input backpressure can delay this boundary, so its timestamps describe
decoded demand. Client send times require a separate client observation.

With native JSONL tracing enabled, the block-sync table includes session start
and finish rows and a sequence on each decoded message. GetBlocks rows also
carry the requested height and count. The sequence advances before emission,
including when the bounded trace queue rejects a row. Together with peer and
session identity, it distinguishes repeated requests for the same height.

Import a closed process's table with:

```sh
python3 scripts/import-getblocks-arrivals.py block_sync.jsonl arrivals.json
python3 -m unittest scripts/tests/test_import_getblocks_arrivals.py
```

The importer rejects missing boundaries, missing or duplicate messages, changed
process identity, backwards session timestamps, and unsupported capture
versions. It preserves timestamp ties and file order. Peer labels become local
integers, with the same peer retained across reconnects and distinct session
indices. The artifact records the source file's SHA-256.

## Evidence boundary

This first import contains decoded arrivals. The artifact explicitly sets
`service_lifecycles_complete` and `capture_loss_verified` to false. Per-session
continuity cannot detect a session whose entire trace disappeared. Process-wide
event totals and a durable final loss report are needed to close that gap.
The `sync.block.capture.sessions_started`, `sessions_finished`, and
`decoded_messages` counters count attempts independently of trace emission;
the latter is labeled by message kind. The `process_info` gauge binds these
totals to the JSONL process identity. After all observed clients disconnect,
keep new clients disconnected, wait for both session counters to agree, save
the server's local Prometheus scrape, then stop the server and preserve its
closed trace. Check those totals with:

```sh
python3 scripts/import-getblocks-arrivals.py block_sync.jsonl arrivals.json \
  --final-metrics final-metrics.prom
```

This also rejects an entirely missing session, a different process's scrape,
or unequal totals by message kind. The artifact records the scrape hash and
sets `decode_totals_reconciled` to true. It leaves `capture_loss_verified` false:
matching decode totals cannot prove coverage after the scrape or completeness
of service events. The capture controller must establish the final observation
boundary and retain that evidence separately.

A resource replay additionally needs correlated admission, actual state-query
completion, frame ownership through write completion or drop, and final
settlement. Keep the effective configuration, binary hashes, initial state,
capture timing, and native client progress with the run manifest. Arrival
artifacts alone cannot establish accounting, capacity recovery, or predicted
sync throughput. The existing ownership-scenario JSON remains a separate test
format.

## Serving correlation

The `get_blocks_serving` rows carry the decoded session and message sequence
through input retention, provisional admission, commitment, and reactor request
ID assignment. They use the decode emitter's clock. Repeated ranges remain
distinct, and the observation metadata retains no resource owners. The
`sync.block.capture.serving_events` counter counts emission attempts by phase.

The commit-state table adds `get_blocks_query` rows keyed by reactor request ID.
These distinguish the driver read future's start and completion from delivery
timeout or cancellation. Read completion after an expired delivery remains
visible. This timing includes the read service's readiness and internal queues;
it is not a measurement of physical disk service time alone. The native startup
passes the endpoint's cloned trace emitter to this driver.

The `get_blocks_frame` rows follow each leased response into the transport queue,
through write polling, and through byte-lease release. They include request and
frame sequences, message type, payload bytes, and write state. `write_returned`
means the write future returned, possibly with an error; it does not prove peer
receipt. A queued drop has no write start, and a cancelled write has no return.
The `sync.block.capture.frame_events` counters count emission attempts by phase.

Release-start and release-finish bracket the actual byte release. Other tasks
can wake during that interval, so neither timestamp alone is an atomic global
accounting transition. Observers hold diagnostic identity only. Tests check
that the final observation sees returned capacity after queue drop, write
cancellation, and successful or failed write return.

The `get_blocks_settlement` rows bracket final request-resource release after
the last ledger, query, or response owner drops. They report fixed request work,
response capacity, bytes transferred to transport, and unused response capacity.
The release-finish guard follows every resource field in Rust's drop order.
Transferred frames can remain outstanding after this request release finishes;
their separate frame rows describe that ownership. The independent
`sync.block.capture.settlement_events` counters count attempts by phase.

The `get_blocks_ownership` rows bracket release of retained pending input or
rollback of a fully reserved provisional admission. Rust drops paired guards
before and after the resource fields; commit disarms the rollback guards.
`input_consumed` means the reactor extracted the request, not that it accepted
or committed the work. Dropped input has release rows without consumption.
The `sync.block.capture.ownership_events` counters include stage and phase.

The `get_blocks_wait` rows identify each pending-input, admission, or reactor
queue wait with a per-request sequence. They end as ready, closed, or cancelled,
including task aborts and waits dropped before their first poll. Readiness can
still be followed by another blocked admission attempt. `initial_bound` names
the original reason for waiting; pending retention can wait on both session and
node capacity. The `sync.block.capture.wait_events` counters count attempts by
stage and phase.

The arrival importer does not yet join these rows or validate service lifetimes.
These observations do not individually record temporary reservations inside a
failed synchronous admission attempt or partial pending-slot acquisition. They
must not be treated as a complete trace of instantaneous global balances. The
workload adapter must exercise the actual regulator and preserve uncertainty
inside acquisition and release intervals.

## Completed application lifetimes

For a closed run with both trace tables, final metrics and the controller's
boundary evidence, import a completed workload profile with:

```sh
python3 scripts/import-getblocks-workload.py closed-run workload.json
python3 -m unittest discover -s scripts/tests -p 'test*getblocks*.py'
```

The run directory must contain `traces/block_sync.jsonl`,
`traces/commit_state.jsonl`, `final-metrics.prom`, `capture-boundary.json` and
`clients-stopped.json`. The controller evidence declares the owned clients
stopped and the population closed. Its boundary binds that declaration and the
final scrape by SHA-256 and declares quiescent counters verified. The importer
checks those bindings, validates decode continuity, and reconciles all six
lifecycle counter families against the closed files before joining requests.
This declaration remains an operational prerequisite; the importer cannot
independently observe stopped processes on remote machines.

This profile accepts committed requests with completed reads and returned
writes. It checks identities, phase order, response accounting, frame and wait
sequences, and ownership release intervals. A missing event or unsupported
outcome rejects the entire episode, even if other requests completed. Cancelled
reads, rolled-back admissions, dropped writes and closed waits need a later
profile extension; they are never silently omitted. A returned write can still
be an error and does not establish peer receipt.

The output retains decoded arrival times, admission and query timing, frame
sizes, waits and release intervals. A frame may outlive the request that queued
it. Timestamp ties remain explicit, and peer identities become local integers.
The original startup configuration and binary provenance remain required run
metadata. This artifact does not itself predict throughput;
the resource adapter below exercises the production regulator using an explicit
policy and relative service durations. These observations are not atomic
changes to global resource balances.

## Resource replay

The test-only `serving_regulation::workloads` module replays an imported episode
against the production GetBlocks regulator. It uses explicit captured and
candidate policies and a paused Tokio clock. All serving cost inputs must be
present; missing fields cannot inherit current defaults. Candidate response
count/byte shapes must still match the recorded responses. The runner does not
model changes to peer connection admission.

Keep the imported JSON and a policy file alongside the original run evidence.
The policy file has a `values` object containing every field read by serving
admission; the Rust `Policy` type defines the required fields. Preserve the
startup configuration witness and binary hashes with it. Run the local corpus
entry point with:

```sh
GETBLOCKS_WORKLOAD=/absolute/path/workload.json \
GETBLOCKS_CAPTURE_POLICY=/absolute/path/captured-policy.json \
GETBLOCKS_CANDIDATE_POLICY=/absolute/path/candidate-policy.json \
GETBLOCKS_REPLAY_OUTPUT=/absolute/path/new-report.json \
cargo test --locked -p zakura-network captured_workload_local_corpus \
  -- --ignored --nocapture
```

The report includes both explicit policies and four scheduling scenarios. They
use each release interval's start or finish and each initial session polling
order. These are sensitivity checks, not exhaustive best/worst bounds. A
single-peer capture has no cross-session ordering variation. Source trace
integrity is checked by the Python importer; the Rust reader validates the
resource fields and dependency ordering needed to execute the profile.

The runner preserves offered decoded arrival times and session order. When
pending capacity fills, one input per session uses the real pending waiter;
additional demand stays in an external harness queue. The report exposes that
backlog and input delay. It exercises actual fair active-slot waits, admission,
commitment, query claims, frame transfers and resource drops. Dependencies move
with the new admission time, including the captured provisional interval,
query duration, frame lifetime and request settlement. Original finish times
cannot release a delayed request early. Sessions remain alive until their
conditional work drains, with any extension beyond the observed end reported.

Rate eligibility comes from the real budget, rounded up to a microsecond. The
runner supplies captured service durations and does not simulate executor
latency, storage contention, transport FIFO scheduling, actual writes, reader
pausing, retries or cancellation. It reports long input delays and reads beyond
the candidate query deadline as partial validity checks. An input delay over
eight seconds is a signal to require native feedback validation, not an exact
reconstruction of the peer routine's pending-wait deadline. A clean report does
not establish every protocol deadline or native sync throughput.

The runner asserts resource bounds throughout and requires complete ownership
recovery at the end. Its results complement the independent ownership model
and native integration tests. Capture policy replay must first be compared to
observed admission timings; held-out native comparisons remain necessary before
using a changed policy's results to choose production defaults. The full corpus
stays with its provenance outside the repository; small deterministic witnesses
cover frame retention, delayed completion, rate refunds and fair slot waits in
ordinary tests.

## Closing a capture

The capture controller must stop the owned clients, verify their process state,
and keep new clients disconnected until the server has stopped. Preserve the
observed state in `clients-stopped.json` in the run directory. Its schema is:

```json
{
  "schema_version": 1,
  "no_new_clients": true,
  "clients": [
    {"host": "owned-downloader", "unit": "native-sync.service", "MainPID": 0, "ActiveState": "inactive"}
  ]
}
```

Populate this declaration from actual observations, with host identities and
observation times retained in the run manifest. Then, on the serving node, run:

```sh
python3 scripts/finalize-getblocks-capture.py /absolute/path/run
```

By default it reads the local exporter on port 19999 and waits up to 240 seconds
for two equal, drained capture-counter samples at least two seconds apart. An
abrupt client exit can leave its server session alive until the 150-second
QUIC idle timeout expires, so the capture wait includes time for that cleanup. It
rejects unknown counter labels and preserves existing boundary files. A failure
leaves the capture unfinalized; retain its diagnostics instead of manufacturing
a successful boundary. Success writes `final-metrics.prom` and
`capture-boundary.json`, binding the scrape and client declaration by SHA-256.

Keep clients disconnected, stop the server, and preserve the closed trace files
before running the workload importer. The finalizer checks application-owner
counts and the declared controller boundary. It does not establish trace
completeness; only the subsequent closed-file reconciliation can verify the
observed lifecycle rows.
