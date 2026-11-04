# NU7 mainnet developer dashboard

One Zakura mainnet node with NU7 instrumentation and a public developer
dashboard at <https://nu7-mainnet.valargroup.dev>, built to watch the Network
Sustainability Mechanism (NSM), fee flows, difficulty and block time through
the NU7 activation at height 3,543,405.

The page follows the layout of the public <https://zakura.com/nu7/> page and
extends it with time series for developers. Everything it shows is observed
from this one node; reorg counts are tip replacements seen here, not a
network-wide rate.

## What runs on the host

| Unit | Role |
| --- | --- |
| `zakurad.service` | The node, built from this branch so it exports the `zcash_chain_tip_*`, `zcash_chain_block_*`, `zcash_chain_nsm_*` and `zcash_chain_value_pool_*` metrics. RPC (`127.0.0.1:8232`), metrics (`127.0.0.1:9999`) and health (`127.0.0.1:8080`) stay on loopback; only P2P port 8233 is open. Config: `zakura.toml`. |
| `zakura-nu7-mainnet-dashboard.service` | `collector.py` with `nu7_chain.py`, `nu7_daa.py`, `nu7_miners.py`, `nu7_store.py` and `page.html` under `/opt/zakura-nu7-mainnet`. Stores blocks, samples and events in `/var/lib/zakura-nu7-mainnet/dashboard.sqlite` and serves `/`, `/v1/dashboard` (window 1h, 6h, 24h or 7d) and `/healthz` on `127.0.0.1:8095`. Unit: `collector.service`. |
| `caddy.service` | Publishes those three routes at the hostname with an automatic certificate. Config: `Caddyfile`. |

The collector works against a stock release too: the per-block fee and NSM
columns stay empty until the instrumented node runs, and the page says so.

## How the data is gathered

- Every 2 s (stretched when the exposition is slow) the collector scrapes the
  node's metrics. The per-block `zcash_chain_block_*_total` counters are
  attributed to blocks by delta; when two blocks land between scrapes the tip
  keeps the latest-block gauges and the block before it carries the remainder,
  marked `merged_from`, so totals stay exact.
- Every stored block also gets its miner (the coinbase tag, or the largest
  transparent payout that is not a funding stream; `nu7_miners.py`) and the
  difficulty consensus required for it, recomputed from the stored headers by
  `nu7_daa.py`, a bit-exact Python copy of the node's adjustment rule. A block
  whose `bits` differ is a `daa_mismatch` event. The first start backfills
  2016 blocks (about 42 h before NU7) as the miner baseline. The miners table
  marks each miner `active` (a block within the last hour), `inactive` (a
  block within 7 days but none for over an hour) or `gone` (nothing in 7
  days), and separately checks its recent block count against its baseline
  share.
- After every new tip the collector estimates the hashrate (work over time,
  the same arithmetic as `getnetworksolps`), the next block's difficulty, the
  expected block time at the tip, and projects difficulty and block time 1500
  blocks ahead from the real chain. The first tip it sees also freezes a
  3000-block projection from the activation height at the hashrate of the
  last 1008 blocks (`meta.projection_fixed`), with a 300-run Monte Carlo band
  computed once in the background. The page draws live difficulty against
  both projections; the frozen one is the prediction made before activation.
- Every 3 s it reads `getblockchaininfo`, re-checks the last stored hash to
  detect a reorg (rolling back the rows above the fork point), and stores each
  new block from `getblock` with `getblocksubsidy` and the tip's NSM balance.
- Every 10 s it samples the mempool (`getmempoolinfo`, `getrawmempool true`
  while the mempool is small), peers, verification and commit latency
  quantiles, the node's RSS and restart count.

## Deploying

Host: DigitalOcean droplet `zakura-nu7-mainnet-1` (sfo2, `s-8vcpu-16gb`, from
the developer image), DNS-only A record `nu7-mainnet.valargroup.dev` in
Cloudflare.

```sh
# Node: build this branch on the host, then install.
cargo build --release --locked -p zakura --bin zakurad
sudo install -m 755 target/release/zakurad /usr/local/bin/zakurad
sudo install -m 644 deploy/nu7-mainnet/zakura.toml /etc/zakura/zakura.toml
sudo install -m 644 deploy/nu7-mainnet/zakurad.service /etc/systemd/system/zakurad.service

# State: restore the latest pruned mainnet snapshot into /var/lib/zakura
# (https://zakura.valargroup.dev/mainnet-pruned/latest.json: verify the sha256,
# then `zstd -dc | tar -x`), owned by the `zakura` system user.

# Collector and Caddy.
sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin nu7-dashboard
sudo install -d /opt/zakura-nu7-mainnet
sudo install -m 644 deploy/nu7-mainnet/{collector.py,nu7_chain.py,nu7_daa.py,nu7_miners.py,nu7_store.py,page.html} /opt/zakura-nu7-mainnet/
sudo install -m 644 deploy/nu7-mainnet/collector.service /etc/systemd/system/zakura-nu7-mainnet-dashboard.service
sudo install -m 644 deploy/nu7-mainnet/Caddyfile /etc/caddy/Caddyfile
sudo systemctl daemon-reload
sudo systemctl enable --now zakurad zakura-nu7-mainnet-dashboard
sudo caddy validate --config /etc/caddy/Caddyfile && sudo systemctl reload caddy
```

Redeploying the node restarts its counters; the collector records one
`counter_reset` event and continues.

## Checking it

```sh
curl -s 127.0.0.1:9999/metrics | grep -c '^zcash_chain_'
curl -s 127.0.0.1:8095/healthz
curl -s '127.0.0.1:8095/v1/dashboard?window=1h' | jq '.activation, .chain.intervals, .nsm'
curl -s '127.0.0.1:8095/v1/dashboard?window=1h' | jq '.chain.expected, .projections.fixed.summary, .miners.table'
python3 -m unittest discover -s deploy/nu7-mainnet -p 'test_*.py'
```
