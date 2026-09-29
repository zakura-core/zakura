# Zakura fork monitor

A standalone observatory for Zcash testnet orphans, reorgs and network splits. It follows the chain from several vantage points at once (our Zakura RPC nodes, TazMiner, and every reachable P2P peer), keeps its own block tree, and serves a dashboard plus a JSON API. It is a stdlib-only Python 3.11+ package with SQLite storage; nothing here runs inside `zakurad`.

## What it measures and why

Testnet runs a permanent difficulty sawtooth. When a block comes more than 6 × 75 s after its parent, the minimum-difficulty rule lets it be mined at the PoW limit. Zakura's `getblocktemplate` switches to such a template 150 s early and dates it `parent + 451`, so these reset blocks are future-dated (see `min_difficulty_min_time` in [`difficulty.rs`](../../crates/zakura-state/src/service/read/difficulty.rs)). After a reset, blocks come every few seconds until the 17-block averaging window catches up. Most orphans happen in this fast phase, and many of them are a miner racing itself.

Equal-work races are resolved differently by the implementations on the network:

- Zakura (`ChainScore` in [`frontier.rs`](../../crates/zakura-header-chain/src/graph/frontier.rs)) and Zebra 6.3 pick the greater raw tip hash.
- Zebra 6.4 picks the block it received first.
- Zebra serves `getdata` only from its best chain, while Zakura serves any retained chain.

Together these can leave groups of nodes on different branches. The monitor records:

- **Orphans and fork events:** fork point, winner, losers, depth in blocks and work, miners, and self-race vs race between miners. It also records whether the winner was seen first or had the greater raw hash.
- **Reorgs per vantage point:** every tip change of every RPC endpoint and P2P peer, with blocks disconnected and connected.
- **Network splits:** vantage points are grouped by implementation and minor version (`zakura 1.5`, `zebra 6.3`, `zebra 6.4`, ...). A split is two or more groups on conflicting branches, each at least 2 blocks past the fork point, for more than 30 s. In the fast phase peers often sit 30–70 s on a one-block orphan before they reorg; that is lag, not a split. Nodes stuck on dead forks are listed separately: tip off the best chain for over an hour, or more than 1000 blocks behind.
- **Per-miner attribution:** from the coinbase tag, the template marker (🌸 Zakura, 🦓 Zebra) and the payout address. It gives stale rate, self-orphans, and races won and lost.
- **Sawtooth correlation:** each block's min-difficulty flag, blocks since reset `k`, difficulty relative to the pre-reset level, and fast or slow phase. Orphan rates are bucketed by these. Each reset is listed with its miner, template, gap and next-block `dt`.
- **Propagation:** first sighting of each block per peer (`inv`) and per RPC endpoint, with p50 and p90 spread per implementation.
- **Availability:** `getdata` probes after announcements, recording `block`, `notfound` or `timeout` per implementation. An `inv` from a peer followed by `notfound` from the same peer is flagged as an incident.
- **CipherScan cross-check (optional):** CipherScan's orphan list compared with ours.

## Architecture

```
            one asyncio event loop (main thread)
  +-----------------------------------------------------------------+
  | RpcCollector x N ---+                                           |
  |  tip, walk-back,    |                                           |
  |  getchaintips,      +--> Monitor (service.py, single writer)    |
  |  getpeerinfo        |      |-- Chain: in-memory block tree,     |
  | P2PObserver --------+      |   canonical chain, forks, phases   |
  |  <=300 peers, polls,|      |-- Store: SQLite (WAL), batched     |
  |  inv, getdata probes|      |   commits every <=1 s              |
  | CipherscanImporter -+      +-- snapshot dict, rebuilt every 2 s |
  |                            body sweep and prune loops           |
  +-----------------------------------------------------------------+
                ^ chain queries hop onto the loop
                |
  web thread: ThreadingHTTPServer  /  /healthz  /api/*
              reads the snapshot and its own read-only SQLite connection
```

| Module | Role |
|---|---|
| `consensus.py` | Difficulty math, block, header and coinbase parsing, miner labels |
| `wire.py` | P2P message codecs |
| `store.py` | SQLite schema and data access |
| `chain.py` | Block tree, tie-breaks, fork events, sawtooth phases |
| `rpc.py` | RPC client, collector and backfill |
| `p2p.py` | Outbound-only peer observer |
| `cipherscan.py` | CipherScan importer |
| `analysis.py` | Everything the API and `report` return |
| `web.py` + `page.html` | Dashboard and API |
| `service.py` | Wiring, split tracking and lifecycle |

Header-only blocks near the tip (for example the middle of a fast-phase burst learned from P2P `headers`) get their bodies fetched for miner attribution. Canonical ones come over RPC; side-branch ones and missing parents come over P2P. A body from a P2P peer outside the fleet is untrusted (see Limitations): such blocks are fetched again the same way, and a body from RPC or a fleet peer replaces their attribution.

## Running locally

Python 3.11 or newer is required; macOS `/usr/bin/python3` is too old.

```bash
cd deploy/fork-monitor
python3 -m zakura_fork_monitor run --config fork-monitor.testnet.toml \
  --db /tmp/fork-monitor/monitor.sqlite3 --host 127.0.0.1 --port 8093 --backfill-blocks 3000
# open http://127.0.0.1:8093/
```

The first start backfills the last `backfill_blocks` canonical blocks over RPC (3000 take about 4 s), then starts the collectors. `/healthz` and the page report "starting" until the first snapshot. The P2P observer reaches about 45 peers within a minute. After a restart it can take about two minutes, because peers refuse a reconnect from the same IP for a while. Stop the service with Ctrl-C or SIGTERM.

Other commands take the same `--config`, `--db` and `--log-level` options:

| Command | What it does |
|---|---|
| `backfill [--blocks N]` | Load the last N canonical blocks over RPC and exit. |
| `report [--forks N]` | Print a text summary of the database to stdout. It is read-only, so it is safe while `run` is active. |
| `probe-peers --once [--limit N] [--json]` | Refresh the tip and the peer list over RPC, dial up to N peers once, and print the implementation groups and each peer's tip. It writes what it learns, so use another `--db` while `run` is active. |

`run` also takes these overrides: `--host`, `--port`, `--no-p2p`, `--no-cipherscan` and `--backfill-blocks N`.

Tests: `python3 -m unittest discover -s deploy/fork-monitor/tests -t deploy/fork-monitor` from the repo root. They need no network access, and CI runs them in the Lint workflow.

## Configuration

A TOML file; [`fork-monitor.testnet.toml`](fork-monitor.testnet.toml) is the testnet fleet example. Unknown keys and out-of-range values stop the program with a message naming the key.

| Key | Default | Meaning |
|---|---|---|
| `network` | `"testnet"` | `testnet` or `mainnet` (consensus parameters, magic, DNS seeds) |
| `db` | `/var/lib/zakura-fork-monitor/monitor.sqlite3` | SQLite path; `--db` overrides |
| `[http] host`, `port` | `127.0.0.1`, `8093` | Dashboard listen address; `--host` and `--port` override |
| `[chain] backfill_blocks` | `20000` | Canonical blocks backfilled at startup. After longer downtime the range is widened to reach the stored tip, up to `memory_window`. |
| `[chain] memory_window` | `30000` | Heights kept in the in-memory tree; statistics cover this window, minus heights not watched live (see Limitations) |
| `[chain] settle_depth` | `3` | A stale block counts as an orphan this many blocks below the tip |
| `[[rpc]] name`, `url` | required | Vantage point `rpc:<name>`. `user:pass@` in the URL becomes basic auth. |
| `[[rpc]] kind` | `"zakura"` | `zakura`, `zebra`, `zcashd` or `other` (grouping before `getnetworkinfo` answers) |
| `[[rpc]] interval` | `1.0` | Seconds between `getbestblockhash` polls |
| `[[rpc]] chaintips_interval` | `3.0` | Seconds between `getchaintips` polls; the step turns itself off where the method is missing (Zebra) |
| `[[rpc]] peerinfo_interval` | `120.0` | Seconds between `getpeerinfo` polls, which feed P2P candidates |
| `[[rpc]] timeout` | `10.0` | HTTP timeout in seconds |
| `[[rpc]] fleet` | `false` | Our own node: its host is dialed over P2P and preferred for fetches and backfill |
| `[[rpc]] backfill` | `true` | May be used for the startup backfill |
| `[p2p] enabled` | `true` | Run the P2P observer; `--no-p2p` overrides |
| `[p2p] max_peers` | `300` | Concurrent outbound connections |
| `[p2p] connect_rate` | `5.0` | New dials per second |
| `[p2p] poll_interval` | `15.0` | Seconds between tip polls (`getheaders` + `ping`) per peer; an `inv` triggers an extra poll |
| `[p2p] probe_sample` | `0.2` | Share of new blocks that also get a sampled availability probe to each Zebra minor version that announced them |
| `[p2p] dns_seeds` | zcashd seeds for the network | Seed hostnames |
| `[p2p] static_peers` | `[]` | Extra `host[:port]` peers |
| `[cipherscan] enabled` | `true` on testnet | Import CipherScan orphans; `--no-cipherscan` overrides |
| `[cipherscan] base_url` | `https://api.testnet.cipherscan.app` | API origin |
| `[cipherscan] interval` | `60.0` | Seconds between polls; at most 2 requests per second |
| `[cipherscan] backfill_pages` | `0` | Pages of 200 orphans fetched at startup |
| `[retention] days` | `30` | Sightings, probes, tip changes and chaintips older than this are pruned a minute after start, then hourly; blocks are kept |

## Data model

All times are unix seconds (UTC) and hashes are display hex. The schema is in [`store.py`](zakura_fork_monitor/store.py).

| Table | Contents |
|---|---|
| `blocks` | Every block seen: header fields, work, min-difficulty flag, height. From a body: size, tx count, miner label, tag, template, payouts, extranonce, and `body_trusted` (0 for a body from a P2P peer outside the fleet). `first_seen_at` and `first_seen_source` hold the earliest timed sighting; they are NULL for a block known only from the backfill. |
| `sightings` | First time each source saw each block, with its kind (see below) |
| `sources` | Vantage points `rpc:<name>` and `p2p:<ip>:<port>`: implementation, version, user agent, start height, tip, status, last error |
| `tip_changes` | Every tip transition per source: fork point, blocks disconnected and connected, work, and `is_reorg` |
| `chaintips` | `getchaintips` entries per RPC source, as a union over time |
| `probes` | `getdata` probes. Reason is `announce`, `fetch` or `reprobe`; result is `block`, `notfound`, `timeout` or `error`. It also records latency and whether the same peer announced the block. |
| `split_events` | Network splits: start, end, fork point, and a JSON summary with depth, `max_depth`, `closed_by` and the last split candidate (sides and groups) |
| `external_orphans` | CipherScan orphan records |

Sighting kinds:

- These time a block's arrival:
  - `inv`: pushed announcement
  - `rpc_tip`: 1 s tip poll
- These say a source holds the block, but not when it arrived:
  - `headers`
  - `chaintip`
  - `getdata`
  - `rpc_walk`: ancestors fetched behind a new tip
  - `rpc_body`: body sweep
  - `backfill`

## API

Every route answers GET and HEAD. The API routes return JSON with `Cache-Control: no-store`. Bad parameters give 400 and unknown paths give 404. `since` takes unix seconds, or a negative number meaning seconds before now. Response shapes are documented on the matching functions in [`analysis.py`](zakura_fork_monitor/analysis.py).

| Route | Returns |
|---|---|
| `/` | The dashboard (single page, no external requests) |
| `/healthz` | `ok`, `starting` or `stale` (503 unless `ok`), plus snapshot age and tip height |
| `/api/snapshot[?full=1]` | Live view: tip, phase, implementation groups and branches, split candidate and `split_event`, stuck nodes, collector health and recent reorgs. `full=1` adds per-source rows. |
| `/api/summary` | Orphans, forks, reorgs and resets for the last 1 h, 24 h and 7 d, plus source health |
| `/api/forks?limit=&since=` | Fork events with winners, losers, tie-break analysis, probes and which sources adopted which branch |
| `/api/orphans/stats` | Orphan rate by hour, by day, by `k` bucket, by D/D_pre bucket, and fast vs slow |
| `/api/miners?since=` | Per-miner canonical, stale, self-orphan, race and reset counts |
| `/api/sawtooth?n=` | Per canonical block: height, time, dt, difficulty, k, min-difficulty flag and orphans; plus resets |
| `/api/resets?limit=&since=` | Resets with gap, next dt, fast-phase length and cycle length |
| `/api/propagation?since=` | First-seen spread per implementation group |
| `/api/probes?since=` | Availability probe outcomes per implementation and version: announce probes in `by_group`, sampled reprobes in `reprobe_by_group`, body fetches only in `by_reason`. Plus "announced then notfound" incidents. |
| `/api/peers` | Every vantage point with its relation to the best tip |
| `/api/crosscheck` | CipherScan orphans against ours, per day |

Rates count only heights watched live (see Limitations). `/api/summary` periods, `/api/orphans/stats` totals, `/api/miners` and each `/api/resets` row report the settled canonical blocks left out as `unobserved`; a period with any is `partial`, and a reset cycle never watched has `orphans: null`. `/api/crosscheck` counts CipherScan orphans at such heights as `unwatched` rather than `only_theirs`. In `/api/forks`, a block counts as seen first only when the other block's earliest `inv` or `rpc_tip` sighting came more than 1 s later.

## Deploying on a new host

**The P2P observer must run from an IP that no fleet node uses.** Zakura and Zebra keep at most one connection per remote IP and silently drop a second one. An observer on a fleet host would lose almost every peer right after the handshake, and those nodes' own peering would suffer too.

The RPC collectors and the dashboard can run anywhere that can reach the nodes' RPC ports. That includes a fleet host, with `--no-p2p` or `[p2p] enabled = false`.

The host needs:

- Python 3.11 or newer (Ubuntu 24.04 ships 3.12; 22.04's 3.10 is too old).
- Outbound TCP to peers on 18233 and to the RPC endpoints.
- HTTPS to CipherScan, and DNS.
- No inbound port except Caddy's. The observer only dials out.

With about 45 peers, expect 100 to 200 MB RSS and a few percent of one core. At that peer count the database grows by about 29 MB per 1000 blocks. Each connected peer adds one sighting and usually one tip change per block, about 0.65 KB with their indexes, so growth scales with the peer count. Testnet makes about 4,500 to 7,000 blocks a day, so 30 days of retention come to about 4 to 6 GB. Blocks are never pruned and add about 0.66 MB per 1000 blocks on top, 1 to 2 GB a year.

```bash
sudo useradd --system --home-dir /var/lib/zakura-fork-monitor --shell /usr/sbin/nologin zakura-fork-monitor
sudo mkdir -p /opt/zakura-fork-monitor /etc/zakura-fork-monitor
sudo cp -r deploy/fork-monitor/zakura_fork_monitor /opt/zakura-fork-monitor/
sudo cp deploy/fork-monitor/fork-monitor.testnet.toml /etc/zakura-fork-monitor/config.toml
sudo cp deploy/fork-monitor/zakura-fork-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now zakura-fork-monitor
journalctl -u zakura-fork-monitor -f   # a status line every 5 min, reorgs and splits as they happen
```

The unit ([`zakura-fork-monitor.service`](zakura-fork-monitor.service)) runs as a dedicated user under `ProtectSystem=strict`, with the database in its `StateDirectory`. It is a template: no deploy workflow installs it yet. The dashboard listens on `127.0.0.1:8093`. Put Caddy in front of it for TLS, either as a site block in `/etc/caddy/Caddyfile` or as a file in `/etc/caddy/conf.d/` on hosts whose Caddyfile imports that directory:

```caddy
fork-monitor.testnet.example.com {
	reverse_proxy 127.0.0.1:8093
}
```

The page and API are read-only and serve only public chain and peer data. Add Caddy `basic_auth` if the peer list should not be public.

To upgrade, copy the new `zakura_fork_monitor/` directory and run `sudo systemctl restart zakura-fork-monitor`. A restart keeps all history and backfills only what is missing. An open split event is closed at shutdown, or at the next start after a crash.

## Limitations

- **Reachable peers only.** The observer sees only peers that accept inbound connections. In the live sweep about two thirds of Zebra nodes did not: they are behind NAT or not listening. Their tips are invisible except through our nodes' `getpeerinfo`, which carries no heights.
- **Partial announcements.** Nodes announce each block to a random subset of peers, so one observer sees a fraction of each node's announcements. Propagation figures are from a single vantage point and assume a disciplined clock (NTP) on the host.
- **Statistics start when watching starts.** Orphan, fork and miner rates count only heights whose canonical block was first seen within 600 s of its header time. The backfill fetches only canonical blocks, so the competitors at backfilled heights were never observable, and after a restart the rates cover only the time the monitor has been watching.
- **Fetched blocks have no arrival time.** Backfilled blocks have no first-seen time, so forward-dating and "seen first" show as unknown until a live sighting, which can itself be late (for example the first RPC tip poll after a restart). Bodies fetched later over P2P or RPC are timed at the fetch.
- **Side-branch bodies are best effort.** Zebra serves only its best chain, so a side block is fetched from the peer that just showed it to us, or from Zakura nodes, which serve retained side chains (at most 10 per node). Live, some tips the fleet lists as `valid-fork` in `getchaintips` still came back `notfound` over P2P, and some of TazMiner's `valid-fork` tips reached no peer at all. Such blocks stay header-only or unknown, and unattributed.
- **Forks below the window.** A peer on a fork below the in-memory window is shown as stuck from its version-message height; its fork point is unknown.
- **Untrusted coinbases.** P2P headers and bodies are checked for their target, the Equihash (200, 9) solution, the expected nBits when the 28 ancestors are in the window, and a time at most 2 h ahead; a body's coinbase height must match its place in the chain. Nothing else ties a body to its header. The merkle root is not checked, and checking it would not help: v5 and later txids leave out the coinbase scriptSig, which holds the tag and the template marker. So a body from a P2P peer outside the fleet counts as untrusted until RPC or a fleet peer serves that block. Side blocks that only other peers served keep their untrusted attribution.
- **Cheap testnet orphans.** A min-difficulty block costs about 32 Equihash solutions, so anyone can mine consensus-valid side blocks. The monitor counts every such block it is shown as an orphan, even one relayed only to it.
- **Single process.** One SQLite file with no high availability.
