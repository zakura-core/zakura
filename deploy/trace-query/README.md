# Trace query client

`trace_query.py` is the collecting side of the raw trace endpoint
([`docs/raw-trace-endpoint.md`](../../docs/raw-trace-endpoint.md)). The node serves
its JSONL trace tables over HTTP with standard byte ranges and ETags. This client follows those tables
on one or more nodes, mirrors their rows into SQLite, and runs arbitrary SQL over
the mirror. It uses only the Python standard library.

## Node configuration

```toml
[network.zakura]
trace_dir = "/var/lib/zakura/traces"

[tracing]
raw_endpoint_addr = "127.0.0.1:9009"
```

## Commands

Mirror two tables once, or keep following them every 10 s:

```sh
python3 trace_query.py pull --db traces.sqlite \
  --node local=http://127.0.0.1:9009 \
  --table legacy_peer_request --table commit_state \
  --follow 10 --retain-hours 48
```

Without `--table`, `pull` follows every table in each node's `GET /traces` index.
`--event NAME` (repeatable) stores only rows with those `event` values.
`--node` is repeatable, so one mirror can hold several nodes' rows.

Query the mirror. Each trace table is a view of the same name over
`trace_rows`. The raw JSON row is in `row`:

```sh
python3 trace_query.py sql --db traces.sqlite "
  SELECT json_extract(row, '$.result') AS result, count(*)
  FROM legacy_peer_request
  WHERE event = 'block_request_finish'
  GROUP BY result"
```

`--format json` prints one JSON object per row. Queries open the mirror read-only.

List a node's tables, or stream one for `jq` (the final line may be incomplete):

```sh
python3 trace_query.py tables --url http://127.0.0.1:9009
python3 trace_query.py dump --url http://127.0.0.1:9009 --table commit_state | jq .
```

## Columns

| Column | Meaning |
| --- | --- |
| `node` | The `--node` name. |
| `file_id` | The trace file's `ETag`. A replaced file gets a new value. |
| `offset` | The row's byte offset in that file. With `node`, `tbl` and `file_id` it is the row's key. |
| `event`, `ts`, `process_trace_id` | Copied from the row. `ts` is microseconds since that node process started, not wall time. |
| `fetched_unix` | The collector's wall clock when it stored the row. |
| `row` | The row's JSON. |

## Remote nodes

The endpoint binds loopback only. Reach a remote node through an
operator-managed SSH tunnel and pass the node's bound address as the Host header:

```sh
ssh -N -L 19009:127.0.0.1:9009 node-host &
python3 trace_query.py pull --db traces.sqlite \
  --node remote=http://127.0.0.1:19009 --host-header remote=127.0.0.1:9009 \
  --table block_sync
```

## Limits

The client inherits the endpoint's consistency model: it assumes append-only
files. External copytruncate or in-place edits can mix bytes without an error.
A row the writer dropped or buffered is not in the mirror. The cursor only
advances past complete lines, so the last unfinished line is read on the next
poll. Each pass reads up to 64 MiB per table, plus enough bytes to complete its
first line. Lines above 8 MiB fail the poll without advancing the cursor.
`pull` without `--follow` performs one pass. Use `--follow` to drain a backlog;
each pass gives every node and table a turn. `dump` streams the table in chunks.
SQL results are materialized in memory; use `LIMIT` for large result sets.
The mirror uses WAL mode, so `sql` queries run while `pull` writes. A table whose
store finds the mirror locked by another writer for 30 s keeps its cursor, and
the next pass retries it. Any other HTTP status than 200, 206, 404 or 416 fails
that table's poll for the pass.

Changing `--event` does not replay rows before a saved cursor. Use a new mirror
to collect events that a previous filter excluded. Retention deletes rows but
does not shrink the database file; SQLite reuses those pages for later rows.
