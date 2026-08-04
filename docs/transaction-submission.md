# Public transaction submission

Zakura starts a dedicated public HTTP listener that exposes one JSON-RPC
method, `sendrawtransaction`. The full RPC listener remains disabled by
default and is not reachable through this endpoint.

The default addresses are `[::]:8237` on Mainnet and `[::]:18237` on Testnet
or Regtest. For example:

```console
curl --fail-with-body \
  --header 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"sendrawtransaction","params":["SIGNED_TRANSACTION_HEX"]}' \
  http://127.0.0.1:8237/
```

The listener also serves `GET /healthz` as a cheap liveness check. It does not
report sync readiness. Use Zakura's separate `/ready` health endpoint when a
load balancer must exclude nodes that are behind the chain tip.

## Configuration

The listener is enabled without any RPC configuration. Operators can override
its production defaults independently of the full RPC server:

```toml
[rpc.transaction_submission]
enabled = true
listen_addr = "[::]:8237"
requests_per_second = 10
request_burst = 20
requests_per_minute_per_ip = 60
request_burst_per_ip = 4
max_in_flight = 16
max_in_flight_per_ip = 4
max_connections = 100
max_connections_per_ip = 20
trusted_proxies = []
```

Set `enabled = false` to disable the listener. Environment variable overrides
use the same nesting, for example
`ZAKURA_RPC__TRANSACTION_SUBMISSION__REQUESTS_PER_SECOND=20`. Separate trusted
proxy networks with commas in
`ZAKURA_RPC__TRANSACTION_SUBMISSION__TRUSTED_PROXIES`.

Zakura rejects zero limits, per-IP limits above their global limits, more than
500 global in-flight submissions, more than 100,000 global connections, more
than 256 trusted proxy networks, and trusted catch-all networks at startup.

The maximum HTTP body size is derived from the configured mempool transaction
size limit. Requests must use `POST /`, `Content-Type: application/json`, and a
bounded `Content-Length`; compressed, chunked, batch, WebSocket, and HTTP/2
requests are not accepted.

## Reverse proxies and TLS

By default, Zakura ignores `X-Forwarded-For`. If a reverse proxy overwrites
that header with the connecting client's address, list only the proxy's
network ranges:

```toml
[rpc.transaction_submission]
trusted_proxies = ["127.0.0.1/32", "2001:db8:1234::/48"]
```

Never trust a catch-all range. Zakura rejects `0.0.0.0/0` and `::/0` at
startup. It also bounds forwarded-header size and hop count and falls back to
the directly connected proxy address if parsing fails.

TLS can be terminated at a reverse proxy or configured on this listener:

```toml
[rpc.transaction_submission.tls]
cert_file = "/etc/zakura/tls/fullchain.pem"
key_file = "/etc/zakura/tls/private-key.pem"
```

## Resource and retry behavior

Global and per-client token buckets reject excess HTTP requests with HTTP 429
before routing, including liveness and unsupported requests. IPv4 addresses are
limited individually and IPv6 addresses share one identity per /64. Separate
global and per-client in-flight limits bound transaction verification, while a
connection limit and header/body deadlines bound slow clients. Client IPs and
raw request bodies are not written to normal logs, and metric labels use only
fixed result categories.

The listener makes one immediate mempool submission per request. Unlike the
full RPC method, rejected public submissions are not added to Zakura's legacy
multi-block retry queue. If a client disconnects, verification continues while
holding its admission permits, so disconnecting cannot create unaccounted
background work.

A timeout or lost connection can be ambiguous: the transaction may have been
accepted even though the client did not receive the response. Relays and
clients should not immediately retry an ambiguous POST against another
backend; first check for the transaction or wait before rebroadcasting.
