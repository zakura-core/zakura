# Structured CSV traces

The node writes all twelve structured trace tables as CSV.
`JsonlTraceTable::csv` defines a table with an explicit column list.
The crate retains its published name, but no longer supports JSONL output.

CSV files start with `ts,wall_ts,node,process_trace_id`, followed by the declared
columns and an `extra` column. The writer writes the header once and appends rows
when a process restarts. `wall_ts` records UTC time with millisecond precision.
`ts` records elapsed microseconds.

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
