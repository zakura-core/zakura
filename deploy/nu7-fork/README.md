# NU7 fork testnet

A separate network that forks the public Testnet and activates NU7 at a chosen
height, so NU7 consensus can be exercised end to end. The current fork is
open for public participation.

The fork is an ordinary configured testnet. `testnet::Parameters::build()`
already _is_ the public Testnet — genesis hash, magic, every activation height
through NU6.3, funding streams, the NU6.1 lockbox disbursements, the Orchard
soft-fork height and the checkpoint list. The fork overrides three things: its
name, its network magic, and the NU7 activation height. Everything else is
inherited, which is why the tooling here is small.

Its chain state is seeded from a real Testnet cache, so it carries genuine
pre-NU7 history and the measured NSM value balance rather than starting empty.

**Current public fork:** the running nodes use consensus branch ID `77190ad9`
and source revision [`ff0e0f0442d857b395b6c721cc177e7dcd550e78`](https://github.com/zakura-core/zakura/tree/ff0e0f0442d857b395b6c721cc177e7dcd550e78).
Build that pinned revision to join the existing chain. `main` uses the same
branch ID but also carries later post-NU7 consensus changes and the full
100-block coinbase maturity, so a `main` build starts a new fork rather than
joining the running one.

## Prerequisites

The deployed ref **must** contain the ZIP 259 NU7 consensus branch ID
(`0x77190ad9`), which landed on `main` in PR #1144. Before that, the NU7 branch
entry was gated behind `cfg(any(test, feature = "zakura-test"))`, so a stock
release binary had no NU7 branch at all and the fork could not activate.

`provision` additionally needs `doctl` on PATH and a DigitalOcean token. The
other subcommands only need SSH access to the host.

## Quick start

```sh
cd deploy/nu7-fork

./fork.py provision            # droplet + a clone of the newest Testnet state snapshot
$EDITOR fork.toml              # set host.ssh_string to the new droplet
./fork.py plan                 # what heights would this fork use?
./fork.py up                   # seed, render, deploy
./fork.py status               # height and NU7 status

# Produce blocks. Until NU7 activates this mines one block per ~7.5 minutes.
cargo run --release -p zakura-fork-miner -- --rpc 127.0.0.1:18232
```

## How the activation height is chosen

`activation_offset` in `fork.toml` is a number of blocks **above the seeded
tip**, not a wall-clock time. On an isolated fork we mine every one of those
blocks ourselves, so the offset sets the schedule.

Proof of work stays enabled, and the pace comes from the Testnet
minimum-difficulty rule: when a block arrives more than `target spacing * 6`
after its parent, difficulty resets to the network's PoW limit. That gap is

| | target spacing | minimum-difficulty gap |
| --- | --- | --- |
| before NU7 | 75s | **450s** (7.5 min) |
| after NU7 | 25s | **150s** (2.5 min) |

So an offset of 10 is about 75 minutes to activation, and 100 would be most of a
day. `./fork.py plan` prints the estimate before you commit to it.

## Continuous mining on the primary node

After NU7 activates, use the primary service unit in `miner/` to mine
continuously against the primary RPC. The local observer remains a validator
without a miner. The miner polls the tip once per second and refreshes its
block template every 15 seconds. It cancels work when the tip changes,
including a same-height reorganization. A refreshed template can pick up the
Testnet minimum-difficulty rule after a long gap; the miner does not
deliberately wait for that gap.

Build the miner from the same source revision as the running fork nodes and
install its unit on the fork host:

```sh
CARGO_TARGET_DIR=/root/cargo-target cargo build --release --locked -p zakura-fork-miner
sudo install -m 644 deploy/nu7-fork/miner/zakura-fork-miner.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart zakura-fork-miner.service
```

Keep node RPC bound to localhost. Watch accepted blocks, template refreshes, CPU use,
the observed block intervals, and tip replacements during the trial. The
25-second protocol target is an average; the dashboard's median is a different
statistic. Observe at least one 102-block DAA window before judging whether
this host can sustain the target without the Testnet minimum-difficulty fallback.

## Geographic miners on the running fork

Three additional validating nodes mine in SFO3, AMS3, and SGP1. Each is a
DigitalOcean `s-1vcpu-2gb` Droplet (1 shared vCPU, 2 GiB RAM, 50 GiB disk;
$12/month), assigned to the `zakura-testnet` project. One original miner
remains on the NYC1 fork host. The remote nodes use the same pinned live source
revision (`ff0e0f044`), network magic, activation heights, and NSM seed, with
different P2P peers and solver IDs 3, 4, and 5. Their RPC servers bind only to
localhost. The miner service has a 70% CPU quota to leave capacity for node
validation on a shared CPU plan. Do not deploy a `main` build to these hosts;
it would diverge from the running fork after NU7.

The remote configs are derived from the live node config with
`miner/render-remote-config.py`. All three currently mine to the existing
primary miner address, so their matured rewards remain available to the faucet
without distributing its spending key. The dashboard attributes **accepted
submissions**, not canonical blocks, from each miner's journal; a reorg may
displace an accepted block. Each remote miner has its own full node and
`miner/remote-status.py` reports service health, tip, NU7 branch ID, and accepted
submissions over 24 hours. The primary collector polls those endpoints using
`miner/remote-miners.json`, requires a fresh report on the same chain within two
blocks of the primary, and exposes the result in `/v1/status`. The DigitalOcean
firewall allows the status port only from the primary host; P2P port 18233 is
public. No GitHub SSH key is stored on the remote hosts.

For a replacement host, stop a local observer briefly to archive its `state`
and `non_finalized_state` directories consistently. Verify the archive hash
after transfer before extraction. Install the pinned live node and miner
binaries, the rendered config, and the `zakurad.service`,
`zakura-nu7-remote-miner@.service`, and
`zakura-nu7-miner-status@.service` units, and apply
`miner/99-zakura-nu7.conf` for prompt block propagation. Start the matching
miner and status instances, then confirm its reported hash agrees with the
primary at the same height. Update `miner/remote-miners.json` if the replacement
IP changes.

To move an existing remote miner between DigitalOcean regions, stop and
disable its node, miner, and status services before powering it off and taking
a disk snapshot. Transfer the snapshot image to the destination region, create
the replacement Droplet with the operator's SSH key in the `zakura-testnet`
project, and attach the geo-miner firewall. Start the node first; compare its
tip hash and NU7 branch ID with the primary before enabling the miner. Update
the other remote nodes' seed lists, the collector URL, and the downloadable
join config. Remove the old Droplet and temporary snapshot after the new node
is healthy.

The DigitalOcean Testnet state snapshot is refreshed weekly (Monday 04:00 UTC by
`zakura-pr-node-bake.yml`), so the seeded tip can lag the live chain by up to a
week. That is harmless for a fork — the activation height is relative to the
seed, not to the public chain.

## The state volume

`fork.py provision` attaches a clone of the Testnet state snapshot but nothing
mounts it, because in CI that is `pr-node-run.sh`'s job. `fork.py seed` mounts it
at `host.snapshot_mount` using DigitalOcean's `/dev/disk/by-id/scsi-0DO_Volume_*`
convention, then copies `state/v<db-format>/testnet` out of it.

That snapshot is taken in `tip` mode, which is a **pruned** database, so
`host.storage_mode` defaults to `pruned` to match. Describing a pruned seed as an
archive node would misreport what the node actually holds.

## Producing fees

A fork whose every block is coinbase-only has no fees, so NU7 fee recycling
never engages: the 60% that should reach the NSM balance and the 40% the miner
should claim are both zero. `txload/` creates those fees.

```sh
cargo run --release -p zakura-fork-txload -- \
  --address <the fork's miner address> --secret-key <its key, hex> --fee 10000000
```

Each transaction shields one matured coinbase output into the **Ironwood** pool
and leaves the fee behind as the gap between the input value and the note
value. Two consensus rules force that shape:

- a transaction spending transparent coinbase **must have no transparent
  outputs**, so the value has to leave the transparent pool entirely, and
- the Orchard pool is closed to new value after NU6.3, so Ironwood is where it
  can go.

The only spendable value on a fork is its own coinbase, and transparent
coinbase needs 100 confirmations, so a freshly seeded fork cannot produce a fee
for its first ~100 blocks.

## Public faucet

`faucet.py` accepts Testnet Unified Addresses with an Orchard receiver and uses
`zakura-fork-txload` to send **0.1 testnet ZEC in Ironwood** from mature miner
coinbase. It allows one claim per address and two per client IP every 24 hours,
at most 100 claims (10 ZEC) per UTC day. Claims are queued persistently in
SQLite, spaced at least 30 seconds apart, and in-flight claims become
`review` after a restart so a broadcast is never repeated automatically.
Only `/v1/faucet/*` is public; the node RPC and the Python listener stay local.

For the existing public fork, build the sender with
`./build-live-faucet.sh /path/to/fresh-build-dir`. The script combines the
faucet sender from this PR with the pinned live node source and patches the
pinned `zakura-protocol` dependency to encode the running chain's branch ID.
It prints the resulting binary path. This patch is specific to the current
fork; a new fork built from `main` needs no patch.

Install the release build as `/usr/local/bin/zakura-fork-txload`, install
`faucet.py` under `/opt/zakura-nu7-faucet`, and install `faucet.service` as
`zakura-nu7-faucet.service`. The service uses systemd `LoadCredential` to give
the existing miner key file to an isolated worker. Edit the service's miner
address and key path together if mining moves to another host. Add the faucet
route in `dashboard.Caddyfile`, validate Caddy, then start the service.

The output beyond the exact payout and the 100,000-zatoshi fee goes to a
deterministic Ironwood change address derived from the miner key. The current
sender does not spend those change notes; retain the miner key for future
recovery tooling. This limits the faucet to fresh mature coinbase outputs until
shielded change spending is implemented.

## Running multiple miners

Set `peer.miner_address` on an additional node to let it mine. Leaving it empty
keeps that node a pure validator: without a miner address a node refuses
`getblocktemplate`, so it can only accept blocks another node produced.

One node that both mines and validates cannot catch a block it builds wrong and
accepts wrong in the same way. Two competing miners additionally exercise
losing a race and re-templating on a tip someone else mined, including across
the activation boundary.

## Reconfiguring

```sh
$EDITOR fork.toml              # new network_name and/or activation_offset
./fork.py reconfigure
```

`zakurad` stores chain state under `state/v<db-format>/<network name
lowercased>`, so **renaming the network gives the next run a clean cache**. That
is what makes repeated reconfiguration cheap: `host.pristine_cache_dir` keeps
the untouched Testnet seed, and each run copies it into a fresh fork directory.
Never point the node at the pristine copy directly.

## Why each setting is the way it is

**Distinct `network_magic`.** Without it the fork dials real Testnet peers,
rejects their blocks once NU7 activates, and bans them. The magic is what makes
the fork a separate network from the first block.

**`initial_testnet_peers = []`.** Mandatory, not cosmetic. `zakurad` refuses to
load a config that pairs the default public DNS seeds with testnet parameters
incompatible with the public Testnet, and adding NU7 makes them incompatible.
`fork.py` always emits this.

**`checkpoints = true`.** The serde default is genesis-only checkpoints, and
`build_configured_testnet` applies it unconditionally. Omitting it would make the
node fully verify four million blocks it already trusts.

**The complete activation-height list.** `with_activation_heights` discards every
configured height at or above `Height(1)` before applying the new set, so a
partial list silently disables Sapling through NU6.3. `fork.py` parses the real
heights out of `crates/zakura-chain/src/parameters/constants.rs` rather than
restating them, so they cannot drift.

**`initial_nsm_value_balance`.** Set to the measured Testnet constant
(`55_768_414_957`). The builder default is zero, which is only correct for a
chain with no pre-NU7 history; a fork of Testnet has four million blocks of it.

## What happens at activation

Fee recycling starts at NU7 unconditionally: 60% of aggregate block fees go to
NSM and the miner claims the subsidy plus the remaining 40%.

ZIP 234 NSM reissuance is separate, and stays unscheduled on the fork:
`DTestnetParameters` exposes only `initial_nsm_value_balance`, with no
configurable reissuance start height on this base. See
`docs/design/reissuance-accounting.md`.

Seeding a fork changes the header-chain network policy digest, because that
digest binds the full activation list. On the current database format this is
handled as a `RecoveryRepair::NetworkPolicyConfiguration` and rebound in place
during startup recovery — it is not a failure. Only the legacy v1–v3 migration
path rejects a mismatch outright.

## Public dashboard status feed

`dashboard.py` is a read-only collector for the public NU7 page. It polls the two
local RPC servers and the three regional miner status endpoints, verifies that
the nodes report the configured NU7 activation, and
serves a small JSON response on `127.0.0.1:8093/v1/status`. The accompanying
systemd unit and Caddyfile publish `/v1/status`, `/v1/block/<height-or-hash>`,
`/v1/tx/<txid>`, and `/healthz` at `api.nu7.valargroup.dev`; node RPC remains
bound to localhost. Explorer responses include only selected public block and
transaction fields; older records may be unavailable from pruned storage.

The response includes current tip, header timestamps, recent intervals,
difficulty, external peer count, local node agreement, regional miner health,
and the live NSM
balance when the deployed node reports `nsmValueBalanceZat`. The reorg count
includes only tip replacements observed while the collector is running. That
measurement comes from the primary host, so it is not a network-wide orphan
rate.

Install from the repository root on the fork host:

```sh
sudo install -d -m 755 /opt/zakura-nu7-dashboard
sudo install -m 755 deploy/nu7-fork/dashboard.py /opt/zakura-nu7-dashboard/dashboard.py
sudo install -m 644 deploy/nu7-fork/dashboard.service /etc/systemd/system/zakura-nu7-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable --now zakura-nu7-dashboard.service
```

Create unproxied A records for `seed.nu7.valargroup.dev`,
`api.nu7.valargroup.dev`, and `nu7.valargroup.dev` pointing at the fork host;
P2P cannot use an ordinary HTTP proxy. Install Caddy, copy
`dashboard.Caddyfile` to `/etc/caddy/Caddyfile`, validate it with
`caddy validate --config /etc/caddy/Caddyfile`, and restart Caddy. Caddy serves
the public status API and proxies the dashboard to its Sites publication. The
page proxy allows link preview clients to read its metadata without the Sites
edge's bot challenge. Keep the fork's source revision and participant config in
sync before advertising a build as join-ready.

## Layout

| Path | Purpose |
| --- | --- |
| `fork.toml` | Every fork parameter, safe to edit between runs |
| `fork.py` | Provision, seed, plan, render, deploy, status, reconfigure |
| `miner/` | The external miner (`zakura-fork-miner`) |
| `txload/` | Drives fee-bearing transactions (`zakura-fork-txload`) |
| `nodes.generated.toml` | Generated `deploy.py` fleet config; not committed |

`fork.py` renders a fleet config for `deploy/deployer/deploy.py` rather than
deploying by itself, so the fork node is built, shipped and supervised by exactly
the same path as every other managed node.
