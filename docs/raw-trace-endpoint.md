# Raw trace endpoint

The optional endpoint serves structured JSONL traces for diagnostics and
future collection tools. It reuses existing capture; it does not enable more
trace events, aggregate nodes, or export application logs or OpenTelemetry spans.

```toml
[network.zakura]
trace_dir = "/var/lib/zakura/traces"

[tracing]
raw_endpoint_addr = "127.0.0.1:9009"
```

Both settings are required. Without `raw_endpoint_addr` no listener is opened.
Invalid addresses, missing `trace_dir`, and bind errors fail node startup.
The endpoint currently requires Unix for no-follow file opens.

```sh
curl --fail http://127.0.0.1:9009/traces/block_sync
```

`GET /traces/<table>` returns `application/x-ndjson`. Allowed table names are
`handshake`, `conn`, `stream`, `discovery`, `ratelimit`, `header_sync`,
`legacy_request`, `block_sync`, `commit_state`, `quic_conn`, `queue_send`,
`legacy_peer_request`, and `legacy_sync`. Missing files return 404; unavailable
or unsafe files return 503. Other paths, methods and query parameters are rejected.

A request without `Range` streams a fixed prefix of the current file, stopping at
the length observed when it was opened. Appends beyond that length are excluded.
Memory is bounded to 64 KiB chunks; the connection has a 30-second deadline,
so large or slow transfers may end early. Empty files return an empty 200 response.

Use a single inclusive byte range to select a portion:

```sh
curl --fail -D headers.txt -H 'Range: bytes=0-1048575' \
  http://127.0.0.1:9009/traces/block_sync -o part.jsonl
```

A range returns 206 with `Content-Range`, `Content-Length`, and `X-Trace-File-Id`.
Each range is limited to 1 MiB. `bytes=0-` selects up to that bound; explicit ends
are clipped to the observed file length. Suffix and multiple ranges are rejected.
Invalid, oversized and beyond-EOF ranges return 416. Query parameters are rejected.

Resume at the byte after the previous response's end, passing its file identity:

```sh
curl --fail -H 'Range: bytes=1048576-' \
  -H 'X-Trace-File-Id: <value from previous response>' \
  http://127.0.0.1:9009/traces/block_sync -o next-part.jsonl
```

Nonzero offsets require this header (428 otherwise); malformed tokens return 400.
The token contains the file's device, inode and captured length. Every resumed
range is interpreted against that original length, including `Content-Range`'s
total. Appends do not change the token or extend the response. Replacement or an
observed length below the captured bound returns 409. During streaming an observed
shrink or premature EOF aborts the body. Start a new collection on 409; never
concatenate portions from different files.

After an interrupted transfer, resume after the last byte actually received with
the same token. Responses preserve exact bytes, including partial JSONL lines;
join portions before parsing lines. At the original end, further ranges return
416 even if the source has grown. Omit the token to capture a fresh prefix and
see newer rows. There is no wait for future rows or live-tail subscription.

To follow a growing file without re-reading it, capture a fresh token with
`Range: bytes=0-0`. When its device and inode match the earlier token's, resume
at the earlier end with the fresh token. Portions read under two tokens with the
same device and inode come from the same append-only file. A different device or
inode means the file was replaced; start again at byte 0.
`deploy/trace-query/trace_query.py` implements this and mirrors rows into SQLite
for arbitrary queries.

## Consistency model: append-only source

This is a fixed-prefix view of an **append-only** file, not an immutable filesystem
snapshot or content-hash validator. The current `zakura-jsonl-trace` writer opens
files with `create(true).append(true)`, retains its handles, and appends buffered
rows; it has no built-in truncate or rotation path. That producer can keep writing
while an earlier prefix is streamed or resumed. Tests exercise the actual writer
and appends before/between body reads and range requests.

**Disable external copytruncate and in-place editing for files being collected.**
The continuous-sync controller in `deploy/continuous-sync/continuous-sync.py`
and its logrotate template currently use copytruncate; their default rotation
setup is incompatible with this endpoint's resume contract. This PR does not
change that controller or silently disable its retention policy.

Device/inode and a minimum length detect replacement and observed truncation.
They cannot detect truncate-and-regrow above the captured length between checks,
same-inode overwrites, or inode reuse. These unsupported mutations can mix bytes
without an error; the token must not be treated as proof against them. There is
no retained file history, copy, content hashing or per-client cursor state.
Use a trusted local filesystem and an operator-controlled append-only directory,
and start a new capture after node restarts or operator file changes.
Rename/replacement between requests returns 409. An already-open stream remains
on the old descriptor; it never switches to the replacement pathname. However,
the current writer also keeps its old descriptor, so renaming files is not a
supported way to redirect its future output. Do not assume consistency across
tables or that a captured last line is complete.

Reading does not flush writers or wait for future rows. Existing buffering,
dropped rows and rotation still apply. There is no acknowledgement, retention
guarantee, backpressure on capture, or historical segment export. Collectors must
not interpret exports as complete evidence. CSV export and table selection in
PRs #965 and #1549 remain separate work; this endpoint exports main's current
JSONL format only.

## Access and resource limits

Only literal loopback bind addresses are accepted. The request Host must match
the bound address and port exactly. Requests with an Origin header or an absolute
URI are rejected, and responses disable caching and MIME sniffing. No CORS
permission is supplied. These controls limit browser and DNS-rebinding access;
they do not authenticate local processes.

Every local user able to reach the listener can read traces. Traces may contain
peer addresses, block timing, node identity, and other sensitive operational data.
Use only on hosts with trusted local users. For remote collection, use an
operator-managed authenticated SSH tunnel or a TLS proxy with authentication and
access controls. Preserve the endpoint's literal Host header. Do not forward the
port publicly or treat a loopback bind inside a container as remote authentication.

Keep the trace directory and its parent directories writable only by the node
operator. The endpoint rejects symlinked files and nonregular files, but assumes
trusted directory ownership; it does not sandbox an attacker who can replace
parent directories or plant hard links. It never accepts a filesystem path from
a request and exports only the fixed table allowlist.

The server processes one connection at a time, disables keep-alive, bounds HTTP
headers, limits range responses to 1 MiB, and closes connections after 30 seconds.
Slow local clients can delay other readers but cannot create unbounded concurrent
request tasks. Accept errors retry after a one-second, shutdown-cancellable backoff.

One dedicated thread performs all trace filesystem operations, including open,
metadata, seek and read. It does not use the node's shared Tokio blocking pool.
Admission belongs to the filesystem job until it exits, not to the HTTP request:
a timeout or disconnect cannot release the slot while a syscall is still running.
Further exports return 503 while the worker is occupied. A one-slot response
channel applies backpressure, allowing at most one queued and one worker-owned
64 KiB chunk, plus the chunk being sent by HTTP and the fixed worker thread stack.
There is no unbounded job queue or per-request worker thread.

Shutdown closes the listener and response channel. The worker exits after its
current job returns. An operating-system filesystem syscall cannot be forcibly
cancelled; permanently stalled storage can strand this single dedicated thread
and its file handle until process exit, but cannot accumulate workers or occupy
the node's shared blocking pool. These limits do not remove the existing disk
and CPU cost of enabling trace capture.
