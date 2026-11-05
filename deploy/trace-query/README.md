# Trace query client

`trace_query.py` is the collecting side of the raw trace endpoint
([`docs/raw-trace-endpoint.md`](../../docs/raw-trace-endpoint.md)). The node only
streams byte ranges of its JSONL trace tables. This client follows those tables
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

Print a fresh prefix of one table for `jq`:

```sh
python3 trace_query.py dump --url http://127.0.0.1:9009 --table commit_state | jq .
```

## Columns

| Column | Meaning |
| --- | --- |
| `node` | The `--node` name. |
| `file_id` | The trace file's `device:inode`. A replaced file gets a new value. |
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
poll. Each poll reads at most 64 MiB per table and continues until it catches up.
