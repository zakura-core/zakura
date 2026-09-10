# Zakura public edge gateway

TLS front doors, allowlisted proxies, and related edge services for Zakura
fleets. Network-specific config lives under `mainnet/` and `testnet/`.

## Layout

```text
deploy/gateway/
  broadcast.py            # sendrawtransaction-only JSON-RPC proxy
  test_broadcast.py
  README.md
  mainnet/
    backends.toml
    Caddyfile             # us-east-0 edge (status + broadcast)
    broadcast.service
  testnet/
    backends.toml
    Caddyfile             # zakura-testnet-1 edge (ironwood + status + broadcast)
    broadcast.service
```

## Broadcast service

Submit-only JSON-RPC reverse proxy for Vizor and other clients that need
`sendrawtransaction` without exposing full node RPC.

| Item | Value |
| --- | --- |
| Method | JSON-RPC 2.0 `sendrawtransaction` only |
| Auth | none |
| Success | HTTP 2xx; `result` = 64-char hex txid |
| Redirects | none (HTTP returns `400 HTTPS required`) |
| Body logging | never |
| Rate limit | 30 req/min/client IP |
| Concurrency | 64 in-flight requests total, 8 per client IP |
| Read timeouts | headers 10 s, body 30 s (enforced by both Caddy and the gateway) |

### Mainnet

- Public URL: `https://zakura-broadcast.valargroup.dev/`
- Origin host: `us-east-0` (`127.0.0.1:8092`)
- Install dir: `/opt/zakura-gateway-mainnet`
- Service: `zakura-broadcast-mainnet.service`
- Installed by `.github/workflows/zakura-mainnet-deploy.yml`

```bash
python3 deploy/gateway/test_broadcast.py

curl -fsS --max-redirs 0 https://zakura-broadcast.valargroup.dev/healthz
```

```text
VIZOR_ZCASH_TRANSACTION_RELAY_URL_MAIN=https://zakura-broadcast.valargroup.dev/
```

### Testnet

- Public URL: `https://zakura-broadcast.testnet.valargroup.dev/`
- Origin host: `zakura-testnet-1` (`127.0.0.1:8092`)
- Install dir: `/opt/zakura-gateway-testnet`
- Service: `zakura-broadcast-testnet.service`
- Installed by `.github/workflows/zakura-testnet-deploy.yml`

```bash
curl -fsS --max-redirs 0 https://zakura-broadcast.testnet.valargroup.dev/healthz
```

```text
VIZOR_ZCASH_TRANSACTION_RELAY_URL_TEST=https://zakura-broadcast.testnet.valargroup.dev/
```
