# Structured CSV traces

The node writes all twelve structured trace tables as CSV.
`JsonlTraceTable::csv` defines a table with an explicit column list.
The crate retains its published name, but no longer supports JSONL output.

CSV files start with `ts,wall_ts,node,process_trace_id,trace_version`, followed by the declared
columns and an `extra` column. The writer writes the header once and appends rows
when a process restarts. `wall_ts` records UTC time with millisecond precision.
`ts` records elapsed microseconds from one process-wide monotonic origin.
Every emitter uses that origin. Readers compare `ts` only within the same
`process_trace_id`. `trace_version=2` identifies this clock contract.
Readers reject older CSV envelopes instead of mixing clock semantics.

The writer quotes commas, quotes, and line breaks. Absent and null fields become
empty fields. Arrays remain JSON values inside CSV fields. The writer flattens
nested objects into dotted column names. Fields outside the declared columns
remain in the `extra` JSON object, so readers must inspect that column when an
event adds fields. Identity fields such as hashes and node labels remain strings.

The Rust test reader, regtest oracle, and benchmark digest read CSV segments
from oldest to newest. External readers must use a CSV parser.
`schema.json` lists every table's columns and value types.

When a node restarts, the writer checks each existing CSV header against the
current schema. A mismatched or incomplete header disables that table and emits
a warning. Use a new trace directory after changing a CSV schema.

The writer rotates at 128 MiB by default and retains two older segments.
`ZAKURA_TRACE_FILE_BYTES` sets the size limit. Each segment contains one header.
The writer locks the table during append and rotation. It syncs each segment
before rotation and syncs current files on the periodic timer and guarded shutdown.
External tools must not truncate trace files.

Validation captures use `ZAKURA_TRACE_CAPTURE_RUN` to name the harness run.
This mode disables rotation and publishes one `capture-*.json` status per writer.
The status records accepted events, dropped events, failed tables, and persisted
row counts. The writer syncs rows before publishing status. Production tracing
keeps its bounded queue and rotation defaults.

The regtest harness requires a fresh directory for each run. Before a planned
restart or final validation, it requests a seal with the oracle's
`--seal-capture RUN_ID` command. Each writer stops accepting events, drains its
queue, and publishes a sealed status. Events after that boundary lie outside the
capture. A new process starts a new capture in the same run directory.
`--check-capture RUN_ID` waits for the acknowledgements. The final oracle checks
`--capture-run-id RUN_ID`, required node/table/event coverage, and exact persisted
row counts. Missing acknowledgements, dropped events, write failures, or mismatched
counts prevent PASS. An unsealed process exit leaves incomplete evidence.

Python readers share `scripts/zakura_trace.py`. The Rust reader and Python readers
check the same CSV fixtures. The schema defines required fields for events that
feed the sync invariants. Full `block_sync_state` records include peer/request counters.
Cheaper `block_sync_pipeline_state` records contain only pipeline counters.
The oracle requires full snapshots for leak checks.
The oracle and flush guard share one commit matcher.
It consumes each finish once and scopes identities by process and source.

Python import tools require POSIX file descriptors. Readers reject symlinks and
special files. The writer uses no-follow regular-file opens and bounded locks.
Directory locks protect reader snapshots against append and rotation. The Rust
test reader consumes captures after writers stop. Imported captures have limits
of 256 MiB, 500,000 rows, and 256 files. Fields are limited to 64 KiB, records to
1 MiB, and embedded JSON to 64 nesting levels. Budget failures prevent validation.
The oracle caches diagnostics and limits reported failures. The benchmark digest
uses only the latest process generation for its monotonic latency series.

Production segments can support partial analysis. Without complete writer status,
the oracle reports INCOMPLETE even when the available checks pass. Row-count
cursors require unrotated captures. Preserve `capture-*.json` files when sharing a
validation capture; CSV files alone cannot establish completeness.

Raw CSV preserves text such as `=1+1` in node labels. Import raw fields as text in
spreadsheet software. CSV quoting protects delimiters; it does not disable formula
evaluation. Machine readers must preserve the original identity strings.

A guarded shutdown waits up to five seconds for the writer task. The timeout does
not cancel an operating-system filesystem call already running on a blocking
thread. Regular-file checks exclude FIFO/device waits; they do not guarantee
that a failing storage device will respond.
