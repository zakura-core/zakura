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

A request without `Range` streams the whole current file, stopping at the length
observed when it was opened. Appends after that point are excluded. Memory is
bounded to 64 KiB chunks; the connection has a 30-second deadline, so large or
slow transfers may end early. Empty files return an empty 200 response.

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

Nonzero offsets require this header (428 otherwise). A changed file identity
returns 409; start a new capture rather than concatenating different generations.
After an interrupted transfer, resume after the last byte actually received.
Responses preserve exact bytes, including partial JSONL lines; collectors must
join byte portions before parsing lines. No record boundary is silently dropped.

The identity uses the opened file's device and inode. It detects rename-based
rotation but is not a content hash or durable cursor: inode reuse and same-inode
copy-truncate/refill cannot reliably be detected. An observed premature EOF
aborts the body. For resumable collection, use an operator-controlled directory
without copy-truncate or in-place edits, and a fresh directory per node run.
Do not assume consistency across tables or across concurrent file modification.

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
request tasks. It stops when the node shuts down. These limits do not remove the
existing disk and CPU cost of enabling trace capture.
