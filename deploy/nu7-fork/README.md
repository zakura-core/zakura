# NU7 fork testnet

A separate network that forks the public Testnet and activates NU7 at a chosen
height, so NU7 consensus can be exercised end to end. The current fork is
open for public participation.

The fork is an ordinary configured testnet. `testnet::Parameters::build()`
already _is_ the public Testnet — genesis hash, magic, every activation height
through NU6.3, funding streams, the NU6.1 lockbox disbursements, the Orchard
soft-fork height and the checkpoint list. The fork overrides its name, network
magic, and NU7 activation height, and sets the initial NSM balance from the seed.
Earlier consensus parameters are inherited.

Its chain state is seeded from a real Testnet cache, so it carries genuine
pre-NU7 history and the measured NSM value balance rather than starting empty.

**Current public fork:** use the [network manifest](https://api.nu7.valargroup.dev/v1/network)
for the exact node revision, activation height, network identity, participant
configuration, and matching snapshot/checksum. The manifest is generated from the
running node's configuration and checked against its RPC upgrade list; these
values are not maintained separately in the website. V2 balances and
transactions do not carry over; use a fresh cache.

## `Nu7StagingV3` facts

The September 30, 2026 reset launched the current network. These facts are its
identity; a later redeploy must preserve them, and a new fork must change the name,
magic and snapshot URL.

| Fact | Value |
| --- | --- |
| Network name / magic | `Nu7StagingV3` / `7a6b7539` (`[122, 107, 117, 57]`) |
| NU7 consensus branch ID | `77190ad9` |
| Seed (caught-up public Testnet tip) | 4,420,648, `0002d46ac1fe7f29f6a8f76396eac63895f595f1811b54060e7b46030e6583d0` |
| NU7 activation | 4,420,652, `000b1414a4fa968bd8e21853d1b9b07aba73ff2d8ad83b929c68850091b8a9e0` |
| Bootstrap snapshot | immutable; see [One-time bootstrap snapshot](#one-time-bootstrap-snapshot) |

Read the [network manifest](https://api.nu7.valargroup.dev/v1/network) for the
node revision, participant configuration digest, and bootstrap snapshot metadata.

**Compatibility.** Main's PR #1209 requires a gap of more than 450 seconds, from
the configured NU7 activation, before a Testnet block may use minimum difficulty.
`Nu7StagingV2` activated at 4,398,756 (hash
`035144c0ff852897608979e863e0210809ee08c3fe9bf35cfab4a9c09649e08d`) with a
minimum-difficulty block 151 seconds after its parent, so its history is invalid
under current rules. It must not be upgraded in place: use a separately
identified fork from a preserved pre-NU7 seed, or first design and review an
explicit consensus migration. V3 was launched under the 450-second rule.

## One-time bootstrap snapshot

The current seed is served directly by Caddy on `zakura-nu7-fork-1`
(`api.nu7.valargroup.dev`), from
`/mnt/snapshots/nu7-public/snapshots/nu7-v3-seed-4420648.tar.zst`.
It is the fixed, pruned seed published before fork mining. Read its checksum,
size, database version, and original publication time from the manifest's
`snapshot` object. The V2 archive remains unchanged at its distinct URL.
There is no daily refresh job. The website presents this artifact separately
from Mainnet snapshots.

The manifest's `snapshot` object includes `url`, `sha256`, `height`, `sizeBytes`,
`publishedAt` (Unix seconds, UTC), `storageMode` (`pruned`), and `dbVersion`
(the database's three-component version). Publication time describes the artifact,
not a later manifest refresh. The publisher requires the snapshot height to match
`seed.height`; this publication flow is for the original pre-activation seed.

For a manual publication, verify the entire archive with `sha256sum`, inspect its
members, and restore it into an isolated cache using the manifest's pinned binary.
Only `state` and `non_finalized_state` with the matching fork name belong in the
archive. Read `dbVersion` from its `state/v*/<network>/version` file. Record the
compressed file size and original publication time in the snapshot metadata JSON.
Pass that file to `publish_network.py --snapshot` with the existing config, seed,
revision and peers. It validates metadata before atomically replacing the manifest.
Keep a copy of the previous manifest outside the public document root for rollback.

Keep the existing archive URL immutable. Caddy's `/snapshots/*` file server already
supports HTTPS GET, HEAD, and byte ranges for resumed downloads. No proxy, bucket,
or extra service is needed. Verify these responses and `/v1/network` CORS after
publication; the website should consume metadata only after the archive is verified.
A future fork reset needs a distinct artifact URL and a matching manifest/config.

## Prerequisites

The deployed ref **must** contain the ZIP 259 NU7 consensus branch ID
(`0x77190ad9`), which landed on `main` in PR #1144. Before that, the NU7 branch
entry was gated behind `cfg(any(test, feature = "zakura-test"))`, so a stock
release binary had no NU7 branch at all and the fork could not activate.

`provision` additionally needs `doctl` on PATH, a DigitalOcean token, and
`droplet.ssh_fingerprint` in `fork.toml` set to a DigitalOcean SSH key's
fingerprint. The other subcommands only need SSH access to the host.

`plan` and `up` read the seed's tip with the host's own `zakurad tip-height`, so
the host needs a `zakurad` at `/usr/local/bin/zakurad` and a config at
`/etc/zakura/zakura.toml` naming `storage_mode = "pruned"`. A freshly provisioned
droplet has neither; install them before the first `up`.

`deploy` and `up` build `zakurad` on the machine running `fork.py`, through
`deploy/deployer/deploy.py build`, and ship that binary. Run them from a Linux
x86_64 host, not a Mac.

## Quick start

```sh
cd deploy/nu7-fork

# The committed fork.toml is a template, not the live V3 config.
cp fork.toml fork.local.toml   # ignored by Git
$EDITOR fork.local.toml        # a new network_name and network_magic for this fork
fork="./fork.py --config fork.local.toml"

$fork provision                # droplet + a clone of the newest Testnet state snapshot
$EDITOR fork.local.toml        # set host.ssh_string to the new droplet
$fork catch-up                 # sync the seed to the public Testnet tip
$fork plan                     # what heights would this fork use?
$fork up                       # seed, render, deploy; the primary starts mining
$fork status                   # height and NU7 status
```

Until NU7 activates, the fork mines about one block per 7.5 minutes.

## How the activation height is chosen

`activation_offset` in `fork.toml` is a number of blocks **above the seeded
tip**, not a wall-clock time. On an isolated fork we mine every one of those
blocks ourselves, so the offset sets the schedule.

Proof of work stays enabled, and the pace comes from the Testnet
minimum-difficulty rule: when a block arrives more than the consensus gap
after its parent, difficulty resets to the network's PoW limit. The multiplier
is six target spacings before NU7 and eighteen afterwards (PR #1209). That gap is

| | target spacing | minimum-difficulty gap |
| --- | --- | --- |
| before NU7 | 75s | **450s** (7.5 min) |
| after NU7 | 25s | **450s** (7.5 min) |

So an offset of 10 is about 75 minutes to activation, and 100 would be most of a
day. `./fork.py plan` prints the estimate before you commit to it.

## Mining

Every mining node runs `zakurad`'s own internal miner: there is no separate
miner process or unit. `fork.py` renders `build_features = ["internal-miner"]`,
so the deployer builds one `zakurad --features internal-miner` binary per commit,
and sets `[mining] internal_miner = true` on the primary. The local observer
uses the same binary and remains a pure validator with the default empty
`peer.miner_address`. Supplying that address enables its internal miner with a
distinct coinbase tag. A stock workspace build does not compile the Equihash solver.

The internal miner long-polls `getblocktemplate` inside the node. Its solver is
cancelled when the tip changes, including a same-height reorganization, and when
a template is withdrawn. Testnet long polling hands out minimum-difficulty work
once the 450-second gap has passed, so no miner waits for the gap deliberately.

The internal miner runs **one solver thread per node**, at the lowest thread
priority, so validation on the same host takes precedence. Adding mining capacity
means adding mining nodes. The rendered fleet config is the source of truth for
the participating nodes. Miners sharing an address must render distinct
`extra_coinbase_data`. Each solver starts from the same nonce, so only a
different coinbase transaction keeps two nodes from repeating each other's work.
`fork.py` tags each node with its name.

Keep node RPC bound to localhost. Watch accepted blocks, CPU use, the observed
block intervals, and tip replacements. The 25-second protocol target is an
average; the dashboard's median is a different statistic. Observe at least one
102-block DAA window before judging whether the miners sustain the target without
the Testnet minimum-difficulty fallback.

## Remote miners

Each `[[remote]]` entry renders another validating node with the internal miner,
the primary's network parameters and miner address, and distinct P2P peers and
coinbase tags. `deploy.py` installs the binary and configuration but does not
copy chain state. A new host must restore a consistent copy of the matching
fork's `state` and `non_finalized_state` before its first deployment: pruned
peers cannot supply the inherited history to an empty node.

For a replacement host, stop an observer while archiving those directories,
verify the archive hash after transfer, and restore it before running `fork.py
render` and `fork.py deploy`. Update the peer lists, collector URL and published
join config if its address changes. Confirm matching hashes at a common height
before enabling mining; the spending key stays off the remote host.

Install `miner/zakura-nu7-miner-status.service` with the
`miner/collector-allowlist.conf` drop-in, and apply `miner/99-zakura-nu7.conf` for
prompt block propagation. The status endpoint reports health, tip, branch ID and
accepted submissions over 24 hours. A reorganized accepted block still counts;
this is not a count of canonical blocks. Observations survive log rotation while
the status process runs and expire after a day or at the generation boundary.

The health service runs unprivileged without journal access, caps concurrency
at four requests, caches samples for five seconds, and times out stalled clients.
Allow port 8094 only from the collector host in both the DigitalOcean firewall
and the unit's IP allowlist. On systems without systemd `+BPF_FRAMEWORK`, use an
nftables rule instead and verify that other hosts cannot connect. P2P is public;
node RPC stays on localhost.

## The state volume

`fork.py provision` attaches a clone of the Testnet state snapshot but nothing
mounts it, because in CI that is `pr-node-run.sh`'s job. `fork.py seed` mounts it
at `host.snapshot_mount` using DigitalOcean's `/dev/disk/by-id/scsi-0DO_Volume_*`
convention, then copies `state/v<db-format>/testnet` out of it into each fork
node's own cache. The peer gets its own copy: the fork nodes are pruned, so a
node that started empty could not sync the inherited history from the other.

### Catching the seed up to the public tip

A snapshot's tip is hours to days old, and a fork cannot be mined on a tip that
old: every block time is capped at the median time of the previous blocks plus
90 minutes, so the miner's blocks never get far enough past their parents to use
the Testnet minimum-difficulty rule, and difficulty ratchets up until the fork
stalls.

`fork.py catch-up` fixes that before seeding. It starts a temporary public-Testnet
node over `host.pristine_cache_dir`, on its own ports (`[catch_up]` in
`fork.toml`), and waits until the tip block is at most 20 minutes old. Then it
stops the node and reopens the cache with P2P seeds and peer caching disabled.
Both runs use synchronous non-finalized backups. Only the tip restored from disk
is recorded in `seed-tip.json`, and it must still pass the age limit. A failed
stop, restore or age check leaves no seed record.

`zakurad` keeps the last thousand or so blocks outside the finalized database, and
`tip-height` reads only the finalized part. After `catch-up`, `seed` also copies
the non-finalized backup, and the activation height is computed from the recorded
tip. Without `catch-up`, only the finalized database is seeded, because non-finalized
blocks above the tip `tip-height` reports would put NU7 below the loaded chain.

That snapshot is taken in `tip` mode, which is a **pruned** database, so
`host.storage_mode` defaults to `pruned` to match. Describing a pruned seed as an
archive node would misreport what the node actually holds.

## Public faucet

`faucet.py` accepts Testnet Unified Addresses with an Orchard receiver and
queues **0.1 testnet ZEC Ironwood** payouts through a separately installed sender
that spends mature coinbase at the faucet's own address. It allows one claim per address and two per client IP every 24 hours,
at most 100 claims (10 ZEC) per UTC day. Claims are queued persistently in
SQLite, spaced at least 30 seconds apart, and in-flight claims become
`review` after a restart so a broadcast is never repeated automatically.
Only `/v1/faucet/*` is public; the node RPC and the Python listener stay local.
The inline form on `https://zakura.com/nu7/` uses this API directly. Browser
requests allow only the exact origins `https://zakura.com` and
`https://nu7.valargroup.dev`; errors carry the same CORS headers so rate limits
and address validation remain readable. Command-line requests without an Origin
header remain supported. Local previews use a loopback proxy to the real staging
faucet; production does not allow localhost origins.

The website reads `GET /v1/faucet/status`, submits JSON `{"address":"utest1…"}`
to `POST /v1/faucet/claim`, and polls `GET /v1/faucet/claim/<claimId>`.
A 202 response reserves a queued claim; only a `sent` receipt with a transaction
ID indicates broadcast. `review` requires operator attention and must not be
resent automatically. Keep the existing claim limits and persistent database
when deploying UI/CORS changes; restart the worker only when no claim is processing.

The repository does not provide a transaction sender. Install an executable that
supports this fork's configured network and Ironwood transactions, and set its
absolute path as `FAUCET_SENDER` in `/etc/zakura-nu7-faucet/faucet.env`. The wrapper
refuses to start if the executable is missing or not executable. Retain an
existing deployed sender until a replacement has been independently validated.

Install `faucet.py` under `/opt/zakura-nu7-faucet` and `faucet.service` as
`zakura-nu7-faucet.service`. Preserve the old claims database for audit after a
reset, and wait for coinbase maturity before reopening the faucet.

The sender must accept `--rpc`, `--config`, `--address`, `--secret-key-file`,
`--recipient`, `--amount-zat`, `--fee`, and `--count 1`. It must validate that the
key controls the source address, create a valid Ironwood payout with no transparent
outputs when spending coinbase, and exit successfully with
`FAUCET_TXID=<64-character transaction hash>` on stdout only after broadcast.
The wrapper passes a payout of 10,000,000 zatoshis and a fee of 100,000 zatoshis.
Any failed, timed-out or ambiguous submission becomes `review`; it is never
retried automatically.

**The faucet has its own key.** It never holds the operator mining key. The
fork config's `faucet.address` is a separate transparent address, and the primary
node mines to it, while the remote miners keep mining to `miner.address`, whose
key stays off every fork host. The faucet therefore spends only the primary's
coinbase, and a leaked faucet host exposes only that balance. Coinbase cannot be
moved to another transparent address (it must have no transparent outputs), so
mining to the faucet address is how it is funded. `fork.py` refuses a faucet
address equal to the miner address. Key-to-address validation is also required
of the external sender.

The service loads `/etc/zakura-nu7-faucet/faucet-key.hex` (root-owned, mode
0600) through systemd `LoadCredential`, and reads `FAUCET_ADDRESS`, `FAUCET_SENDER`, and
optionally an existing `FAUCET_DB`, from `/etc/zakura-nu7-faucet/faucet.env`,
which holds no secret. Add the faucet route in `dashboard.Caddyfile`, validate
Caddy, then start the service. After switching the primary to a new faucet
address, the faucet reports itself unavailable until that address has mature
coinbase, about 100 primary-mined blocks.

Retain the faucet key and any sender-specific wallet state for recovery of
shielded change. Change-note management belongs to the external sender.

## Reconfiguring

`seed`, `up` and `reconfigure` refuse configurations containing remote miners
before any host command. These commands reseed only the primary and local
observer; automatic fleet-wide restoration is not implemented. For a coordinated
reset, stop every node and the faucet, preserve the old generation for audit,
choose a new network name and magic, restore the same stopped seed on every host,
and then render and deploy the fleet. Compare hashes at the seed height before
mining, publish a distinct immutable snapshot and matching manifest, and reopen
the faucet only after coinbase maturity.

For a local-only fork:

```sh
$EDITOR fork.toml              # new network_name and/or activation_offset
./fork.py reconfigure
```

`zakurad` stores chain state under `state/v<db-format>/<network name
lowercased>`, so **renaming the network gives the next run a clean cache**. That
is what makes repeated reconfiguration cheap: `host.pristine_cache_dir` keeps
the untouched Testnet seed, and each run copies it into a fresh fork directory.
Never point the node at the pristine copy directly.

`reconfigure` stops both fork nodes before it deletes their state, then removes
the fork's state under both the seed's and the code's database versions, because
`zakurad` moves a previous-version seed forward on first start. It also removes
the fork's `non_finalized_state` backup, so the previous run's blocks are not
reloaded. `fork.py` refuses a `network_name` of `Mainnet`, `Testnet` or
`Regtest`, and a cache directory that overlaps the pristine seed.

`fork.py deploy` hands the whole rendered fleet to one `deploy.py deploy`, which
deploys in parallel. Each node is staged in its own `mktemp -d` directory on its
host and that directory is removed afterwards, so the two nodes on the primary
host never install each other's files.

## Tearing down

The fork droplet carries the `zakura-nu7-fork` tag, which the PR-node reaper
deliberately skips, so nothing deletes it automatically. When a fork run is
finished, delete the droplet and then its state volume. The volume is named
`droplet.volume_name` plus the region it landed in, for example
`zakura-pr-nu7-fork-state-nyc1`:

```sh
doctl compute droplet list --tag-name zakura-nu7-fork
doctl compute droplet delete <droplet-id>
doctl compute volume list | grep zakura-pr-nu7-fork-state
doctl compute volume delete <volume-id>
```

Delete the droplet first: it detaches the volume, and the reaper deletes a
detached `zakura-pr-*` volume on its own. Deleting the volume explicitly avoids
waiting for that sweep.

## Network configuration

The deployer writes `network` as an inline parameter table rather than
`"Testnet"`. The rendered fixture at
`crates/zakura-network/src/config/tests/data/nu7-fork-node.toml` is loaded with
the node's actual config type in `rendered_nu7_fork_config_loads`.

| Setting | Reason |
| --- | --- |
| Distinct name and `network_magic` | Separate the fork's state and wire protocol from public Testnet |
| `initial_testnet_peers = []` | Prevent dialing incompatible public DNS seeds |
| `checkpoints = true` | Preserve public Testnet checkpoints for the inherited history |
| `inherit_activation_heights = true` | Overlay NU7 on the public upgrade list instead of disabling earlier upgrades |
| `initial_nsm_value_balance = 55_768_414_957` | Carry the measured pre-NU7 Testnet balance instead of the zero default |

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

The public NU7 page reads `/v1/status` from the existing fleet collector,
`deploy/runner/zakura-cluster-status.py`, run on the primary host with
`--nu7-config`. It reads the same rendered fleet config `deploy.py` deploys, so
validator membership is listed once. Nodes with `monitor = { local = true }` (the
primary and the local observer) are probed on the host itself without SSH; the
remote miners are read from their `monitor.status_url` health reports, so
the primary holds no SSH key for them. The collector serves a small JSON response
on `127.0.0.1:8093/v1/status` with the zakura.com CORS allowlist, and the
Caddyfile publishes only `/v1/status` and `/healthz` at `api.nu7.valargroup.dev`.
Its fleet page stays on localhost; reach it through an SSH tunnel. The unit runs
as a transient unprivileged user whose only extra permission is the
`systemd-journal` group, which the node probe uses for zakurad's commit and node
ID and for kernel OOM counts;
node RPC remains bound to localhost. The API has no block or transaction explorer
routes: the website reads only `/v1/status`, `/v1/network`, and `/v1/faucet/*`.

The response keeps schema version 1: current tip, header timestamps, recent
intervals, difficulty, external peer count, local node agreement, regional miner
health, and the live NSM balance when the deployed node reports
`nsmValueBalanceZat`. `status` is `live` only while all configured validators are on
the primary's chain within two blocks; `observation.validatorsAgree`,
`validatorsAgreeing` and `validatorsConfigured` report that agreement. The reorg
count includes only tip replacements observed while the collector is running,
and never observations from before `--nu7-generation-start`. That measurement
comes from the primary host, so it is not a network-wide orphan rate.

The status feed computes mean and median header-time intervals over the latest
301 post-NU7 headers (300 intervals). Before that many blocks exist, it uses
only the available post-activation intervals and reports the actual count in
`chain.intervalSampleBlocks`. Headers are cached between polls; normal tip
advancement only fetches newly mined headers. The recent-block list stays at
eight entries. Existing website clients display the returned sample count
without a website update.

Install from the repository root on the fork host, with the rendered fleet config:

```sh
sudo install -d -m 755 /opt/zakura-nu7-status
sudo install -m 755 deploy/runner/zakura-cluster-status.py /opt/zakura-nu7-status/
sudo install -m 644 deploy/nu7-fork/nodes.generated.toml /etc/zakura/nu7-nodes.toml
sudo install -m 644 deploy/nu7-fork/dashboard.service /etc/systemd/system/zakura-nu7-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable --now zakura-nu7-dashboard.service
```

Create unproxied A records for `seed.nu7.valargroup.dev`,
`api.nu7.valargroup.dev`, and `nu7.valargroup.dev` pointing at the fork host;
P2P cannot use an ordinary HTTP proxy. Install Caddy, copy
`dashboard.Caddyfile` to `/etc/caddy/Caddyfile`, validate it with
`caddy validate --config /etc/caddy/Caddyfile`, and restart Caddy. Caddy serves
the public status API, and redirects every `nu7.valargroup.dev` request to the
canonical page at `https://zakura.com/nu7/`; it proxies no third-party site. Keep the fork's source revision and participant config in
sync before advertising a build as join-ready.

## Tests

The fork's Python tests run on every pull request in `lint.yml`, beside the
deployer's and the status collector's. That is kept deliberately while this
tooling exists: they use only the standard library, run in a few seconds, and
cover what Rust tests cannot.

| Suite | Covers |
| --- | --- |
| `deploy/nu7-fork/test_fork.py` | fleet rendering, the activation overlay, remote miners, faucet address separation, seeding and catch-up |
| `deploy/nu7-fork/test_faucet.py` | claim limits, queueing, CORS, and that the sender uses only the faucet key |
| `deploy/nu7-fork/test_remote_status.py` | mined-block counts, generation boundaries, endpoint bounds, unprivileged units |
| `deploy/nu7-fork/test_publish_network.py` | the participant manifest, including the published V3 config digest |
| `deploy/runner/test_zakura_cluster_status.py` | the `/v1/status` schema version 1 contract and configured-validator agreement |
| `deploy/deployer/test_deploy.py` | nested config round trips and parallel same-host staging |

The Rust side is `cargo test -p zakura-network --lib config::tests` for the
rendered fixture and the activation overlay.
As the tooling shrinks, so does this CI cost.

## Layout

| Path | Purpose |
| --- | --- |
| `fork.toml` | Template parameters for a fresh fork, not the live V3 configuration |
| `fork.py` | Provision, seed, plan, render, deploy, status, reconfigure |
| `miner/` | Remote mining node health endpoint and its unit |
| `nodes.generated.toml` | Generated `deploy.py` fleet config; not committed |

`fork.py` renders a fleet config for `deploy/deployer/deploy.py` rather than
deploying by itself, so the fork node is built, shipped and supervised by exactly
the same path as every other managed node.
