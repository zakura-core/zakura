# zcashd-compat Mode (`zebrad start --zcashd-compat`)

zcashd-compat mode is for operators — typically exchanges and custodial
services — that want to migrate to Zebra while keeping the `zcashd` wallet and
RPC surface their integration already depends on. Zebra faces the Zcash P2P
network and is the consensus node; `zcashd` runs as a **P2P sidecar** that
makes a single outbound peer connection to the local Zebra node and listens
for nothing. zcashd never touches the public network directly.

Your systems keep talking to `zcashd` exactly as before:

| Provided by `zcashd`, unchanged          | Moved to Zebra                              |
|------------------------------------------|---------------------------------------------|
| Wallet behavior and wallet RPC methods   | Public P2P networking and peer selection    |
| Local block files, chainstate, indexes   | Network-facing block and transaction relay  |
| ZMQ notifications                        | Block templates for miners                  |
| Local RPC response semantics             | DNS seeding and peer discovery              |

```text
Zcash network ═P2P (8233)═▶ zebrad ◀═P2P, internal only═ zcashd ◀─wallet RPC, ZMQ─ your systems
                            (front)   connect=zebra:8233   (sidecar)
```

The whole topology is two lines of zcashd configuration:

```text
connect=<zebra-host>:8233   # one outbound peer: Zebra
listen=0                    # no inbound P2P
```

`-connect` makes zcashd peer with the given address and *only* that address:
zcashd itself then disables DNS seeding, inbound listening, and peer
discovery. zcashd syncs blocks and relays transactions over the standard
Zcash P2P protocol, with Zebra as its entire network.

There are two ways to run the pair:

- **Externally managed (default):** you run `zcashd` yourself with
  `connect=`/`listen=0` pointed at Zebra's P2P listener.
- **Supervised:** Zebra spawns and manages `zcashd` itself when
  `[zcashd_compat].manage_zcashd = true`, passing the peer-pinning arguments
  automatically.

## The sidecar zcashd build

Use the sidecar `zcashd` build from
[valargroup/zcashd](https://github.com/valargroup/zcashd) (branch
`feat/p2p-sidecar`). It differs from stock `zcash/zcash` in three ways:

1. **Miner RPCs are removed.** `getblocktemplate`, `submitblock`,
   `getgenerate`, `setgenerate`, and `generate` are not registered and return
   JSON-RPC `Method not found` (-32601). Zebra is the canonical source of
   block templates (see [Mining](#mining-zebra-is-canonical)). Read-only
   mining info RPCs (`getmininginfo`, `getnetworksolps`, `getblocksubsidy`,
   `prioritisetransaction`) remain.
2. **The upstream end-of-support halt is disabled.** Stock zcashd shuts
   itself down at its deprecation height; the sidecar build logs a warning
   and keeps serving its wallet/RPC surface. Consensus safety comes from
   Zebra, which fully validates every block before relaying it to zcashd.
3. **`-regtestacceptunvalidatedpow` (regtest only)** lets zcashd follow a
   Zebra regtest chain, whose mined blocks carry null Equihash solutions.
   It is rejected on any other network.

Everything else — wallet, chainstate format, RPC semantics, ZMQ — is stock
zcashd.

## Quick start (supervised)

```console
zebrad start --zcashd-compat
```

with a config like:

```toml
[zcashd_compat]
enabled = true
manage_zcashd = true
zcashd_source = "path"
zcashd_path = "/usr/local/bin/zcashd"
zcashd_datadir = "/var/lib/zcashd"
zcashd_extra_args = ["-rpcbind=127.0.0.1", "-rpcallowip=127.0.0.1"]
```

On start, Zebra:

1. runs Linux hardware and filesystem preflight checks (see
   [Hardware preflight](#hardware-preflight-linux); `--unsafe-low-specs`
   skips the minimums for test rigs);
2. bootstraps the zcashd datadir and a minimal `zcash.conf` (including the
   zcashd deprecation acknowledgement) if none exists;
3. spawns `zcashd` pinned to Zebra's own P2P listener:

   ```text
   zcashd -datadir=... -printtoconsole <your extra args> \
          -connect=<zebra p2p addr> -listen=0 -dnsseed=0 -listenonion=0 -discover=0
   ```

4. supervises it: restarts on unexpected exit with capped exponential
   backoff, and shuts it down gracefully (SIGTERM, then a configurable grace
   period) when Zebra stops.

The forced peer-pinning arguments are placed *after* `zcashd_extra_args`
because zcashd takes the last occurrence of a single-valued command-line
argument. Peer-selection options (`-connect`, `-addnode`, `-seednode`) in
`zcashd_extra_args` are rejected at startup: the sidecar must peer with Zebra
alone.

By default the supervisor derives the `-connect` address from Zebra's own
bound legacy P2P listener (`network.listen_addr`), substituting `127.0.0.1`
when Zebra listens on an unspecified address. Set
`zcashd_compat.p2p_connect_addr` when zcashd must reach Zebra through a
different address (for example across containers).

zcashd-compat mode requires `network.legacy_p2p = true` (the default):
zcashd speaks the legacy Zcash P2P protocol. Do not enable state pruning on
the fronting Zebra — a pruned node does not advertise `NODE_NETWORK` and
zcashd will not sync from it.

## Quick start (externally managed)

Run Zebra normally (with `zcashd_compat.enabled = true` if you want the
dedicated compat RPC listener and preflight checks), then run zcashd yourself:

```console
zcashd -datadir=/var/lib/zcashd \
       -connect=127.0.0.1:8233 -listen=0 -dnsseed=0 -listenonion=0 -discover=0 \
       -printtoconsole
```

Or put the equivalent in `zcash.conf`:

```text
connect=127.0.0.1:8233
listen=0
i-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet-in-2025=1
```

`make compat-zcashd-start-standalone` (see `make/zcashd-compat.mk`) wraps
this command.

### Verify the integration

Confirm zcashd is talking only to Zebra and exposes no P2P or mining surface:

```console
$ zcash-cli getpeerinfo
# -> exactly ONE peer: the Zebra node ("subver": "/Zebra:.../", "inbound": false)

$ zcash-cli getconnectioncount
1

$ zcash-cli getblocktemplate
error code: -32601  # Method not found: miners must use Zebra

$ ss -tlnp | grep 8233
# -> only zebrad listening; zcashd has no P2P listener
```

Then confirm the tips converge: heights track each other and
`getbestblockhash` matches on both nodes once the drift reaches zero.

`deploy/zcashd-compat/sync-check.sh` and `make compat-status-sync` automate
the process/peer-pinning/height-drift checks, and the deploy watchdog's
`zcashd_compat_sync` check mirrors them for continuous monitoring.

The shield (single peer, no listener, no miner RPCs) is in effect immediately
on startup; you do not need a fully synced chain to verify it.

## Mining: Zebra is canonical

Miners and pools must request block templates from **Zebra's** RPC, not
zcashd's. Enable Zebra's RPC listener and set a miner address:

```toml
[rpc]
listen_addr = "127.0.0.1:8232"

[mining]
miner_address = "t1YourTransparentOrShieldedAddress"
```

Zebra's `getblocktemplate` and `submitblock` are always compiled in; no
special build is needed. See [Mining Zcash with
Zebra](https://zebra.zfnd.org/user/mining.html) for details. The sidecar
zcashd returns `Method not found` for all template and submission RPCs, so a
misconfigured miner fails loudly instead of building on a lagging view.

## Initial sync and existing datadirs

The sidecar syncs the whole chain through its single Zebra peer. That works,
but initial block download through one peer is slow — for production
migrations, **bring your existing synced zcashd datadir** and let the sidecar
continue from its current height. The chainstate and block files are the
stock zcashd format; no conversion is needed.

zcashd still performs its own full validation of every block Zebra relays —
the sidecar removes zcashd's *network exposure*, not its consensus checks.

## Configuration reference

```toml
[zcashd_compat]
# Master switch; also enabled by the `--zcashd-compat` CLI flag.
enabled = true

# Spawn and supervise zcashd (true) or run it yourself (false, default).
manage_zcashd = true

# "path" (use zcashd_path) or "managed" (SHA256-pinned download).
zcashd_source = "path"
zcashd_path = "/usr/local/bin/zcashd"

# zcashd datadir; defaults to a subdirectory of state.cache_dir.
zcashd_datadir = "/var/lib/zcashd"

# Extra zcashd arguments. Peer-selection options are rejected.
zcashd_extra_args = ["-rpcbind=127.0.0.1", "-rpcallowip=127.0.0.1"]

# Zebra P2P address zcashd connects to. Defaults to Zebra's own bound
# legacy listener (loopback-substituted). Set for cross-container setups.
# p2p_connect_addr = "10.0.0.2:8233"

# Supervision lifecycle.
startup_delay = "1s"
restart_backoff = "2s"
restart_backoff_max = "5m"
restart_reset_after = "1h"
shutdown_grace_period = "5m"
```

All values can also be set through environment variables, e.g.
`ZEBRA_ZCASHD_COMPAT__ZCASHD_PATH=/usr/local/bin/zcashd`.

> [!WARNING]
> Until the managed-download manifest is updated for the sidecar build,
> `zcashd_source = "managed"` downloads the previous RPC-ingest zcashd, which
> still contains the upstream end-of-support halt and miner RPCs. Use
> `zcashd_source = "path"` with a sidecar build for now.

### Legacy dedicated RPC listener (deprecated)

Earlier zcashd-compat versions ingested chain data from Zebra over a
dedicated, cookie-authenticated Zebra RPC listener (default
`127.0.0.1:28232`, configured by `listen_addr`, `cookie_dir`,
`cookie_file_name`, `enable_cookie_auth`, `tls_cert_file`, `tls_key_file`,
`unsafe_allow_remote_http`). The P2P sidecar does not use it. The listener is
still started for operator tooling that queries Zebra through it, and the
config keys still parse, but they are deprecated and will be removed. The
`tls_ca_file` key is no longer used at all.

## Hardware preflight (Linux)

When `zcashd_compat.enabled` is set, Zebra checks at startup that the host
meets the minimum hardware requirements for running both nodes (CPU cores,
memory, and free disk per mount). Startup fails below the minimums;
`--unsafe-low-specs` overrides the failure for test environments. Warnings
(not failures) are printed between the minimum and recommended tiers.

## Monitoring and lifecycle

- The supervisor exports `zcashd_compat.supervisor.active` / `.disabled` /
  `.exhausted` gauges.
- zcashd's stdout/stderr are forwarded into Zebra's logs under the
  `zcashd_compat.zcashd` target.
- On Zebra shutdown the supervisor sends zcashd SIGTERM and waits
  `shutdown_grace_period` before force-killing, so zcashd can flush its
  chainstate and wallet.
- `make compat-status-sync` / `deploy/zcashd-compat/sync-check.sh` check both
  processes, peer pinning (`getconnectioncount == 1`), and height drift.

## Testing

The integration suite runs zebrad + a supervised regtest zcashd end to end:

```console
make compat-test-regtest TEST_ZCASHD_PATH=/path/to/sidecar/zcashd
```

It covers startup, tip following over P2P, deep reorgs, restarts,
transaction flow from zcashd's wallet through Zebra's mempool, and the
miner-RPC removal. `make compat-test-mainnet` / `compat-test-testnet` run the
read-only subset against a live deployment.

## Upstream network upgrades

The sidecar zcashd must keep up with Zcash network upgrades: when a network
upgrade activates, zebrad requires peers to advertise that upgrade's minimum
protocol version, and an out-of-date zcashd will be disconnected. Plan to
deploy the updated sidecar build before each activation height.
