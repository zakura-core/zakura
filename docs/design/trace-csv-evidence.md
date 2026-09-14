PR #965 changes trace storage and the evidence consumed by the regtest oracle.
The [original audit](https://github.com/zakura-core/zakura/pull/965#issuecomment-5627995452)
reported seventeen findings. The review of head `79c7b5576` reproduced incorrect
cross-process commit matching, reused finishes, false handoff timeouts, missing
table coverage, and rotation-invalidated cursors.

The root problem was an implicit completeness assumption. Production tracing can
drop events when its queue fills. Rotation can delete earlier records. The oracle
could not infer complete evidence from the remaining CSV files.

The implementation keeps production tracing asynchronous and bounded. Validation
captures use a fresh run ID, disable rotation, and publish writer counts and loss
status. The harness seals each capture before a planned restart or final analysis.
Each writer stops accepting events, drains reserved and queued events, syncs its
files, and acknowledges the seal. The oracle requires those acknowledgements and
compares loaded row counts with writer counts. An abrupt exit cannot supply a
sealed capture. Debug-stop traces remain separate from the ordinary oracle input.

All emitters now share one process-wide monotonic origin. The CSV envelope includes
`trace_version=2`, so readers reject captures with older clock semantics. The
oracle partitions process-local checks by `process_trace_id`. The benchmark digest
uses the latest process generation for latency calculations. Wall time remains
available for cross-process correlation.

The oracle and flush command share one ordered commit matcher. It scopes keys by
process and source, consumes each finish once, and rejects unmatched or duplicate
events. Required node/table coverage is separate from commit balance. The schema
defines required fields for events consumed by the checks.

| Audit findings | Root correction |
| --- | --- |
| F-273501 | Versioned process-wide clock; process-scoped invariant evaluation. |
| F-273502, F-273541 | Explicit commit and sync evidence requirements; sealed writer counts. |
| F-273519, F-273520, F-273549 | Bounded JSON decoding, typed envelopes, and required event fields. JSONL input was removed earlier in this PR. |
| F-273524 | Preserve machine identities; document text-only spreadsheet import. |
| F-273530 | Inspect nested candidates even when root files exist; bound discovery. |
| F-273534 | Fresh run directory, expected run ID, and sealed process acknowledgements. |
| F-273535, F-273536, F-273548 | No-follow regular-file access, snapshot locks, bounded lock acquisition, and guarded-shutdown timeout. |
| F-273540, F-273545 | Capture-wide input limits, cached diagnostics, bounded diagnostic rows, and capped failure reports. |
| F-273542, F-273544 | Shared Python decoder and cross-language fixtures for duplicate keys, schema headers, and extra-field collisions. |
| F-273546 | One parsed, process-scoped commit matcher for both validation and flush checks. |

The Docker verification also exposed a configuration conflict: the node's generic
environment loader treated trace-runtime variables as unknown TOML fields. The
config loader now excludes the exact variables owned by the trace runtime.
The existing rotation-size variable and the new capture-run variable both use
this boundary. Other unknown configuration variables still fail validation.

The run also exposed two shapes sharing the `block_sync_state` event name.
Full snapshots include request diagnostics. Pipeline-only snapshots omit those
fields to avoid registry scans on each commit. The latter now use
`block_sync_pipeline_state`, so validation can require complete leak counters
without inventing values for omitted fields.

Readers acquire a directory snapshot lock to avoid observing append or rotation
halfway through an update. The Rust test reader runs after its writers stop.
Row-count cursors reject rotated input. Complete validation captures never rotate.
Production captures without completeness evidence support partial analysis, but
cannot produce a complete oracle PASS.

The [trace README](../../crates/zakura-jsonl-trace/README.md) documents the commands,
limits, platform requirements, and shutdown behavior. A timeout does not cancel an
operating-system filesystem call that has already started. The file-type checks
exclude FIFO/device blocking, while failing storage remains an operational limit.

Regression tests cover commit-matcher agreement, process-isolated activity,
required coverage, sealed counts, dropped records, stale runs, malformed fields,
symlinks/FIFOs, import budgets, cached diagnostics, and retention-unsafe cursors.
Rust and Python consume the same valid/invalid CSV fixtures. The node config tests
cover runtime environment variables alongside normal TOML environment overrides.
