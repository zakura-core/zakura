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

A resource replay additionally needs correlated admission, actual state-query
completion, frame ownership through write completion or drop, and final
settlement. Keep the effective configuration, binary hashes, initial state,
capture timing, and native client progress with the run manifest. Arrival
artifacts alone cannot establish accounting, capacity recovery, or predicted
sync throughput. The existing ownership-scenario JSON remains a separate test
format.
