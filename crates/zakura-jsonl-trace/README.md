# Structured trace formats

The shared writer supports JSONL and CSV. `JsonlTraceTable::new` defines a JSONL
table. `JsonlTraceTable::csv` defines a CSV table with an explicit column list.

The node writes `commit_state.csv` and `legacy_peer_request.csv`. These tables
record repeated commit and peer request fields during sync. Other tables retain
JSONL, including the sparse block-sync and header-sync event tables.

CSV files start with `ts,wall_ts,node,process_trace_id`, followed by the declared
columns and an `extra` column. The writer writes the header once and appends rows
when a process restarts. `wall_ts` records UTC time with millisecond precision in
both formats. `ts` retains its elapsed-microsecond meaning.

The writer quotes commas, quotes, and line breaks. Absent and null fields become
empty fields. Arrays remain JSON values inside CSV fields. The writer flattens
nested objects into dotted column names. Fields outside the declared columns
remain in the `extra` JSON object, so readers must inspect that column when an
event adds fields. Identity fields such as hashes and node labels remain strings.

The Rust test reader, regtest oracle, and benchmark digest accept the migrated
CSV files and historical JSONL files. External scripts that read the migrated
tables must use a CSV parser and the new file extensions.
