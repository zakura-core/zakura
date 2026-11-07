# Raw trace endpoint

The optional endpoint serves the node's structured JSONL trace files over local
HTTP, so that collectors can copy and follow them while the node appends. It
serves the existing files only. It does not enable more trace events, and it
does not export application logs or OpenTelemetry spans.

```toml
[network.zakura]
trace_dir = "/var/lib/zakura/traces"

[tracing]
raw_endpoint_addr = "127.0.0.1:9009"
```

Both settings are required. Without `raw_endpoint_addr`, the node opens no
listener. A non-loopback address, a missing `trace_dir` or a bind error fails
node startup. The endpoint requires Unix.

## Requests

| Request | Response |
| --- | --- |
| `GET /traces` | `application/json` index of the tables. |
| `GET /traces/<table>` | `application/x-ndjson`: the table's file, `<trace_dir>/<table>.jsonl`. |
| `HEAD` on either path | The same headers, with no body. |

A table name is 1 to 64 characters from `a-z`, `0-9` and `_`. The endpoint
serves every regular file in `trace_dir` whose name is a table name followed by
`.jsonl`. It never follows a symlink.

The index lists each table's name, its length in bytes and its `ETag`:

```json
{"tables":[{"name":"block_sync","bytes":1048576,"etag":"\"803-1a2b-17f0c0ffee\""}]}
```

```sh
curl --fail http://127.0.0.1:9009/traces
curl --fail http://127.0.0.1:9009/traces/block_sync -o block_sync.jsonl
```

A response ends at the file's length when the node opened it. Rows appended
after that point go to later requests. Responses are exact bytes, so the last
line can be incomplete; collectors should hold back a partial line until a
later request completes it.

### Byte ranges

The endpoint implements HTTP range requests from RFC 9110 for one range:
`bytes=<first>-`, `bytes=<first>-<last>` or `bytes=-<suffix>`. A satisfiable
range returns 206 with `Content-Range: bytes <first>-<last>/<length>`. A range
that starts at or past the end returns 416 with `Content-Range: bytes */<length>`.
A malformed range or a multi-range request returns 416. A `Range` header with
another unit is ignored. Ranges have no size limit.

### File identity

Every response for a table carries a strong `ETag` that names the file, not
its contents: the device, the inode and, where the filesystem records it, the
birth time. The trace writer only appends, so bytes already read under one
`ETag` never change. A replaced file has a new `ETag`.

- `If-Range: <etag>` with a range sends the range when the `ETag` matches, and
  the whole current file with 200 when it does not.
- `If-Match: <etag>` returns 412 when the `ETag` does not match.

### Following a table

Store the `ETag` and the number of bytes read, then poll:

```sh
curl -H 'Range: bytes=<bytes read>-' -H 'If-Range: <etag>' \
  http://127.0.0.1:9009/traces/block_sync
```

| Status | Meaning | Next step |
| --- | --- | --- |
| 206 | New bytes after the cursor. | Append them and advance the cursor. |
| 416, length equals the cursor | Nothing new. | Poll again later. |
| 416, length below the cursor | The file was truncated. | Restart from byte 0. |
| 200 | The file was replaced. | Discard the old rows and store the new `ETag`. |

One request per poll is enough. A collector that follows many tables can poll
`GET /traces` and request only the tables whose length or `ETag` changed. A
replacement can be the same size as the old file or shorter, so a length check
alone misses it. `deploy/trace-query/trace_query.py` follows tables with these
requests and mirrors their rows into SQLite for arbitrary queries.

## Status codes

| Status | Cause |
| --- | --- |
| 400 | A query string, or a non-ASCII `Range`, `If-Range` or `If-Match`. |
| 403 | `Host` differs from the bound address, the request has an `Origin` header or an absolute URI, or the table's path is not a regular file. |
| 404 | An unknown path, an invalid table name, or a missing file. |
| 405 | A method other than `GET` and `HEAD`. |
| 412 | `If-Match` failed. |
| 416 | The range is unsatisfiable, malformed or multiple. |
| 503 | Every filesystem thread is busy or stalled, or the file cannot be read. |

Refusals carry a one-line `text/plain` reason.

## Consistency

The endpoint is correct for append-only files, which is how the node's trace
writer works: it opens each file with `create(true).append(true)`, keeps the
handle and never truncates or rotates it. An external tool that rewrites a file
in place, truncates it and lets it regrow past a collector's cursor, or reuses
its inode can mix old and new bytes without an error. **Do not run
copytruncate log rotation on collected files.** The continuous-sync
controller's logrotate template uses copytruncate today.

The trace writer buffers rows and can drop them under load, so an export is
not complete evidence. There is no ordering across tables.

## Access

The listener accepts only loopback addresses. A request must send the literal
bound address and port as `Host`, and must not send `Origin` or an absolute URI.
Responses set `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`
and grant no CORS access. These checks stop browsers and DNS rebinding. They do
not authenticate local processes: **every local user that can reach the port
can read the traces**, which include peer addresses, block timing and node
identity.

For remote collection, use an authenticated SSH tunnel or an authenticating TLS
proxy that forwards the bound address as `Host`. Do not forward the port
publicly, and do not treat a loopback bind inside a container as access control.

Keep `trace_dir` and its parent directories writable only by the node operator.
The endpoint refuses symlinks and non-regular files but trusts the directory.

## Resource limits

- At most 4 connections are served at once; further clients wait in the listen
  backlog. Each connection serves one request.
- A connection is dropped when request headers, the client's reading or the
  file read stall for 30 seconds. A transfer that keeps making progress has no
  deadline.
- Each response reads the file in 256 KiB chunks and holds at most three.
- Each request runs its file reads on its own thread, at most 4 at once in
  the whole process. A read that stalls keeps its thread until the read
  returns, and further requests get 503 while every thread is taken. The
  threads are not Tokio's blocking pool, so a stalled disk cannot block the
  node's other blocking work or its shutdown.
- On shutdown the endpoint closes its listener and connections. Each file
  thread exits when its current read returns. A host that embeds the node and
  runs it repeatedly in one process shares the 4 threads across runs, so
  threads stranded by stalled storage cannot accumulate.
