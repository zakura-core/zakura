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

The arrival importer does not yet join these rows or validate service lifetimes.
Admission waits, cancellation outcomes, and final request settlement still need
complete observations before this becomes an accounting replay input.
