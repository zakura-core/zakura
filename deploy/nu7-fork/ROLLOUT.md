# Redeploying `Nu7StagingV3` with the reviewed tooling

This guide moves the running `Nu7StagingV3` fork from the original sidecar
tooling to the layout adopted after review of PR #1097. It is a redeploy, not a
reset: chain state, network identity, the bootstrap snapshot, the public manifest,
faucet claims and existing credentials are preserved. Nothing here creates or
publishes a secret.

The network's identity is in [README.md](README.md#nu7stagingv3-facts). Every
check below compares against those values.

Commands marked "operator" run from the repository root of a checkout of the PR
head on the operator's Linux machine; commands marked "host" run as root on the
named host, with files copied there first.

## What changes

| Area | Before | After |
| --- | --- | --- |
| Mining | `zakura-fork-miner.service` and the inactive `zakura-fork-miner2.service` on the primary; `zakura-nu7-remote-miner@3`, `@4` and `@5` on US, EU and AP | `zakurad`'s internal miner, one solver thread, on the primary, US, EU and AP; no miner process or unit |
| Node binary | stock `zakurad` | one `zakurad` built with `--features internal-miner` for all five validators and the isolated participant |
| Node config | full activation-height list | `inherit_activation_heights = true, activation_heights = { NU7 = 4420652 }`, the same network; `[mining] internal_miner = true` and a per-node `extra_coinbase_data` on the four miners |
| Remote node unit | hand-installed copy of `miner/zakurad.service` | `deploy/deployer/templates/zakurad.service`, installed by `deploy.py` |
| Primary status feed | `dashboard.py` in `zakura-nu7-dashboard.service`, as root | `deploy/runner/zakura-cluster-status.py --nu7-config` in the same unit name, as a transient unprivileged user |
| Remote health | `zakura-nu7-miner-status@<solver>.service`, as root, open on `0.0.0.0:8094` | `zakura-nu7-miner-status.service`, unprivileged, bounded, allowlisted to the primary |
| Faucet | spends with the mining key `/root/fork-miner-key.hex` | spends with its own key `/etc/zakura-nu7-faucet/faucet-key.hex`; the primary mines to the faucet address |
| Caddy | `/v1/block/*` and `/v1/tx/*` routes; Caddy-added CORS on `/v1/status`; `nu7.valargroup.dev` proxied a third-party page | no explorer routes; the collector answers CORS; `nu7.valargroup.dev` redirects to `https://zakura.com/nu7/` |

Unchanged: `/v1/network`, `/v1/config`, `/snapshots/*`, `/v1/faucet/*`, the
`/v1/status` schema version 1 fields the website reads, P2P port 18233, node RPC
on localhost, and the remote miners' payout address.

## Hosts and units

| Node | Host | Unit | Config | Mines |
| --- | --- | --- | --- | --- |
| `zakura-nu7-fork-1` (primary) | `167.99.146.155` | `zakurad.service` | `/etc/zakura/zakura.toml` | yes, to the faucet address |
| `zakura-nu7-fork-2` (local observer) | `167.99.146.155` | `zakurad-fork2.service` | `/etc/zakura/zakura-fork2.toml` | no |
| US | `134.199.239.83` | `zakurad.service` | `/etc/zakura/zakura.toml` | yes, to the miner address |
| EU | `157.245.69.251` | `zakurad.service` | `/etc/zakura/zakura.toml` | yes, to the miner address |
| AP | `165.22.255.181` | `zakurad.service` | `/etc/zakura/zakura.toml` | yes, to the miner address |
| isolated participant | `167.99.146.155` (RPC `127.0.0.1:18262`) | `zakura-v3-participant.service` | `/root/nu7-v3-ops/participant.toml`, binary `/root/nu7-v3-ops/bin/zakurad` | no |

The primary ran one active sidecar miner (`zakura-fork-miner2` was inactive), so
one solver on each of the primary, US, EU and AP keeps the four active solvers;
no thread or node is added. No host or paid resource is added.

Six validators run in total. The isolated participant is the sixth: it is not
managed by `deploy.py` or counted by the status feed, which reports agreement
across the five fleet validators above. The release owner updates it by hand with
the same `zakurad` binary in step 5, and the identity checks in step 8 cover it.

## 1. Fleet config

Operator. Copy the template and set the V3 values. The file is ignored by Git.

```sh
cp deploy/nu7-fork/fork.toml deploy/nu7-fork/fork.local.toml
```

Set, in `fork.local.toml`:

```toml
[fork]
network_name = "Nu7StagingV3"
network_magic = [0x7A, 0x6B, 0x75, 0x39]
activation_offset = 4                 # rendered with --tip 4420648 -> NU7 4420652

[host]
ssh_string = "root@167.99.146.155"
commit = "<PR #1097 head SHA>"

[miner]
address = "<the current V3 miner address>"   # the remote miners keep paying it

[faucet]
address = "<the new faucet address, from step 3>"   # leave empty until step 3

[[remote]]
name = "zakura-nu7-miner-us"
id = "us"
region = "San Francisco, US"
ssh_string = "root@134.199.239.83"
initial_testnet_peers = ["seed.nu7.valargroup.dev:18233", "157.245.69.251:18233", "165.22.255.181:18233"]

[[remote]]
name = "zakura-nu7-miner-eu"
id = "eu"
region = "Amsterdam, NL"
ssh_string = "root@157.245.69.251"
initial_testnet_peers = ["seed.nu7.valargroup.dev:18233", "134.199.239.83:18233", "165.22.255.181:18233"]

[[remote]]
name = "zakura-nu7-miner-ap"
id = "ap"
region = "Singapore, SG"
ssh_string = "root@165.22.255.181"
initial_testnet_peers = ["seed.nu7.valargroup.dev:18233", "134.199.239.83:18233", "157.245.69.251:18233"]
```

The current V3 miner address is the `--miner-address` in the running
`zakura-nu7-faucet.service` and the `[mining] miner_address` on the remote hosts.
Keep every `[host]` and `[peer]` path, port and name as the template has them
unless step 4 shows the live value differs.

Render the fleet (operator):

```sh
deploy/nu7-fork/fork.py --config deploy/nu7-fork/fork.local.toml \
    --out deploy/nu7-fork/nodes.v3.toml render --tip 4420648
head -3 deploy/nu7-fork/nodes.v3.toml   # must say: NU7 activates at 4420652
```

## 2. Build

Operator, on Linux x86_64:

```sh
python3 deploy/deployer/deploy.py build --config deploy/nu7-fork/nodes.v3.toml
cargo build --release --locked -p zakura-fork-txload
sha256sum deploy/deployer/.build-cache/zakurad-<sha>-internal-miner target/release/zakura-fork-txload
```

Compare the checksums with the ones recorded in the PR description. Install the
sender on the primary as `/usr/local/bin/zakura-fork-txload` (mode 0755); the
running faucet keeps working with it, since it signs only for the address its key
controls. The build depends only on the commit and features, not on step 3.

## 3. Faucet key

The faucet's dedicated key already exists on the primary as
`/root/nu7-v3-faucet-key.hex` (mode 0600), with public address
`tmDCiNGTbRz1Y1eYWyrPBFCaH61JSffwzSr`, and that address holds mature prefunded
coinbase. Its source of truth is the team's Infisical project, from which the
release owner installed it; nothing here generates, prints or copies a key, and a
replacement key is created in Infisical, never on a host.

Host, on the primary: install it where the faucet unit loads it, and confirm it
controls the expected address with the new sender:

```sh
install -d -m 700 /etc/zakura-nu7-faucet
install -m 600 /root/nu7-v3-faucet-key.hex /etc/zakura-nu7-faucet/faucet-key.hex
/usr/local/bin/zakura-fork-txload --config /etc/zakura/zakura.toml \
    --secret-key-file /etc/zakura-nu7-faucet/faucet-key.hex --print-address
# must print tmDCiNGTbRz1Y1eYWyrPBFCaH61JSffwzSr
```

Set `faucet.address = "tmDCiNGTbRz1Y1eYWyrPBFCaH61JSffwzSr"` in `fork.local.toml`
and render again (operator). On the primary, write
`/etc/zakura-nu7-faucet/faucet.env` (no secret):

```sh
FAUCET_ADDRESS=tmDCiNGTbRz1Y1eYWyrPBFCaH61JSffwzSr
FAUCET_DB=<the --db path in the running zakura-nu7-faucet.service>
```

`fork.py` refuses a faucet address equal to the miner address. The committed
`fork.toml` keeps `faucet.address` empty; the V3 address belongs only in the
ignored `fork.local.toml`.

## 4. Pre-flight, read only

Operator. Render each node's config and compare it with the live file before
anything is installed:

```sh
mkdir -p /tmp/nu7-render
PYTHONPATH=deploy/deployer python3 - <<'EOF'
import pathlib, deploy
for node in deploy.load_nodes(pathlib.Path("deploy/nu7-fork/nodes.v3.toml"), None):
    pathlib.Path(f"/tmp/nu7-render/{node.name}.toml").write_text(deploy.render_node_config(node))
EOF
ssh root@167.99.146.155 cat /etc/zakura/zakura.toml | diff - /tmp/nu7-render/zakura-nu7-fork-1.toml
ssh root@167.99.146.155 cat /etc/zakura/zakura-fork2.toml | diff - /tmp/nu7-render/zakura-nu7-fork-2.toml
ssh root@134.199.239.83 cat /etc/zakura/zakura.toml | diff - /tmp/nu7-render/zakura-nu7-miner-us.toml
ssh root@157.245.69.251 cat /etc/zakura/zakura.toml | diff - /tmp/nu7-render/zakura-nu7-miner-eu.toml
ssh root@165.22.255.181 cat /etc/zakura/zakura.toml | diff - /tmp/nu7-render/zakura-nu7-miner-ap.toml
```

Acceptable differences are only: the `network = { ... }` line in overlay form,
`[mining]` gaining `internal_miner` and `extra_coinbase_data`, the primary's
miner address becoming the faucet address, and comments. **Stop** if
`[state] cache_dir`, `storage_mode`, `[network] cache_dir`, `identity_dir`,
`listen_addr`, `[rpc] listen_addr`, `[tracing] log_file` or the peer lists differ:
correct `fork.local.toml` and render again. A different state path would start a
node from empty state.

The overlay names the same network as the explicit list; zakura-network's
`activation_height_overlay_preserves_public_testnet_history` test pins this for
the exact V3 parameters.

Back up what `deploy.py` overwrites, including the binaries, on every host.
`deploy.py` also writes `<bin_path>.bak`, but a repeated deploy replaces that copy,
so roll back from this directory instead:

```sh
ssh root@<host> 'mkdir -p /root/nu7-v3-review-rollback && cp -a /etc/zakura /usr/local/bin/zakurad* /usr/local/bin/zakura-fork-* /etc/systemd/system/zakurad*.service /etc/systemd/system/zakura-* /root/nu7-v3-review-rollback/ 2>/dev/null; ls /root/nu7-v3-review-rollback'
ssh root@167.99.146.155 'cp -a /etc/caddy/Caddyfile /opt/zakura-nu7-dashboard /opt/zakura-nu7-faucet /root/nu7-v3-review-rollback/'
```

Record the baseline on the primary:

```sh
curl -s https://api.nu7.valargroup.dev/v1/network | sha256sum
curl -s https://api.nu7.valargroup.dev/v1/status > /tmp/nu7-status-before.json
```

## 5. Deploy the nodes, one at a time

Restart one validator at a time so four of five stay up and the status feed stays
fresh. Each deploy starts that node's internal miner; stop the host's sidecar
miner right after, so block production never pauses. Before the next node,
confirm the redeployed one is back at the primary's tip (the identity and
agreement checks in step 8).

Operator:

```sh
cd deploy/deployer
python3 deploy.py deploy --config ../nu7-fork/nodes.v3.toml --node zakura-nu7-fork-2
python3 deploy.py deploy --config ../nu7-fork/nodes.v3.toml --node zakura-nu7-miner-us
ssh root@134.199.239.83 'systemctl disable --now zakura-nu7-remote-miner@3.service'
python3 deploy.py deploy --config ../nu7-fork/nodes.v3.toml --node zakura-nu7-miner-eu
ssh root@157.245.69.251 'systemctl disable --now zakura-nu7-remote-miner@4.service'
python3 deploy.py deploy --config ../nu7-fork/nodes.v3.toml --node zakura-nu7-miner-ap
ssh root@165.22.255.181 'systemctl disable --now zakura-nu7-remote-miner@5.service'
python3 deploy.py deploy --config ../nu7-fork/nodes.v3.toml --node zakura-nu7-fork-1
ssh root@167.99.146.155 'systemctl disable --now zakura-fork-miner.service zakura-fork-miner2.service'
cd ../..
```

Host, on the primary, for the isolated participant: back up
`/root/nu7-v3-ops/bin/zakurad`, install the step 2 `zakurad` there, and restart
`zakura-v3-participant.service`. `/root/nu7-v3-ops/participant.toml` is unchanged:
it does not mine, and its full activation list names the same network.

Each run stages its files in its own `/tmp/zakurad-deploy.XXXXXXXX` directory and
removes it afterwards. The remote hosts' `/etc/systemd/system/zakurad.service` is
replaced by the deployer's template with the same `ExecStart`. Between a node's
restart and its sidecar's stop, both mine on that node with different nonces and
coinbase data, so no work is duplicated.

Then remove the retired unit files and reload:

```sh
ssh root@167.99.146.155 'rm -f /etc/systemd/system/zakura-fork-miner.service /etc/systemd/system/zakura-fork-miner2.service; systemctl daemon-reload'
ssh root@<each remote> 'rm -f /etc/systemd/system/zakura-nu7-remote-miner@.service; systemctl daemon-reload'
```

## 6. Remote health endpoints

Copy `deploy/nu7-fork/miner/remote-status.py`, `zakura-nu7-miner-status.service`
and `collector-allowlist.conf` to each remote host. There, as root, replace the
per-solver instance (solver 3 on US, 4 on EU, 5 on AP):

```sh
systemctl cat zakura-nu7-miner-status@<solver>.service | grep -o -- '--since [0-9]*'   # keep this value; use 0 if none
install -m 644 remote-status.py /opt/zakura-nu7-miner/remote-status.py
install -m 644 zakura-nu7-miner-status.service /etc/systemd/system/
install -d /etc/systemd/system/zakura-nu7-miner-status.service.d
install -m 644 collector-allowlist.conf /etc/systemd/system/zakura-nu7-miner-status.service.d/
echo 'NU7_GENERATION_START=<the --since value>' > /etc/zakura/nu7-generation.env
systemctl disable --now zakura-nu7-miner-status@<solver>.service
rm -f /etc/systemd/system/zakura-nu7-miner-status@.service
systemctl daemon-reload
systemctl enable --now zakura-nu7-miner-status.service
```

`NU7_GENERATION_START` must be a number; an empty value makes the unit fail to
start. Unlike the old instance, which counted accepted blocks with `journalctl`,
the new service reads the node's log file and needs no `systemd-journal` group.
Its startup line, and any loss or recovery of node RPC or the log, appear in
`journalctl -u zakura-nu7-miner-status`. The service reads `/etc/zakura/zakura.toml` and the node's log file as an
unprivileged user; both must stay world-readable (`stat -c %a` shows `644`).

## 7. Primary status feed, Caddy and faucet

Copy `deploy/runner/zakura-cluster-status.py`, `deploy/nu7-fork/nodes.v3.toml`,
`dashboard.service`, `dashboard.Caddyfile`, `faucet.py` and `faucet.service` to
the primary, keeping their repository paths. There, as root:

```sh
install -d -m 755 /opt/zakura-nu7-status
install -m 755 deploy/runner/zakura-cluster-status.py /opt/zakura-nu7-status/
install -m 644 deploy/nu7-fork/nodes.v3.toml /etc/zakura/nu7-nodes.toml
install -m 644 deploy/nu7-fork/dashboard.service /etc/systemd/system/zakura-nu7-dashboard.service
echo 'NU7_GENERATION_START=<the same --since value, or 0>' > /etc/zakura/nu7-generation.env
systemctl daemon-reload
systemctl restart zakura-nu7-dashboard.service

install -m 644 deploy/nu7-fork/dashboard.Caddyfile /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile
systemctl reload caddy
```

`/etc/zakura/nu7-nodes.toml` holds no secret. The old `/opt/zakura-nu7-dashboard`
stays in the rollback copy.

The faucet address is already prefunded, so the faucet can switch right after
the primary is deployed and stay ready; the primary's mining then keeps it
funded. Confirm the mature outputs first:

```sh
curl -s -X POST http://127.0.0.1:18232/ -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getaddressutxos","params":[{"addresses":["<faucet address>"]}]}'
```

It must list outputs at least 102 blocks below the tip, each worth at least the
0.1 ZEC payout plus the fee (the sender keeps two confirmations beyond the
100-block coinbase maturity rule). When no claim is `processing`:

```sh
sqlite3 "<FAUCET_DB>" "SELECT count(*) FROM claims WHERE status = 'processing'"   # must be 0
install -m 644 deploy/nu7-fork/faucet.py /opt/zakura-nu7-faucet/faucet.py
install -m 644 deploy/nu7-fork/faucet.service /etc/systemd/system/zakura-nu7-faucet.service
systemctl daemon-reload
systemctl restart zakura-nu7-faucet.service
```

Then move `/root/fork-miner-key.hex` off the host. It controls the miner address
the remote miners pay, and existing V3 rewards; the faucet no longer needs it.

## 8. Acceptance checks

Run each on the primary unless noted. All must pass.

**Binaries.** On all six validators, `sha256sum` of the running binary
(`/proc/$(systemctl show -p MainPID --value zakurad)/exe`, and
`zakurad-fork2` on the observer), and of the participant's
`/root/nu7-v3-ops/bin/zakurad`, matches step 2. The checksum is the
authoritative check; the version string carries at most an abbreviated commit.

**Identity and history**, on all six validators:

```sh
rpc() { curl -s -X POST http://127.0.0.1:18232/ -H 'content-type: application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}"; }
rpc getblockchaininfo '[]' | python3 -c 'import json,sys; i=json.load(sys.stdin)["result"]; print(i["chain"], {k: v["activationheight"] for k, v in i["upgrades"].items() if v["name"] == "NU7"})'
rpc getblockhash '[4420648]'   # 0002d46ac1fe7f29f6a8f76396eac63895f595f1811b54060e7b46030e6583d0
rpc getblockhash '[4420652]'   # 000b1414a4fa968bd8e21853d1b9b07aba73ff2d8ad83b929c68850091b8a9e0
```

Expected: `test {'77190ad9': 4420652}`, and both hashes as shown. Use port 18242
for the observer and 18262 for the participant, and confirm the participant's tip
hash equals the primary's at the same height.

**Agreement and mining**, from the public feed:

```sh
curl -s https://api.nu7.valargroup.dev/v1/status | python3 -c 'import json,sys; s=json.load(sys.stdin); o=s["observation"]; m=s["mining"]; print(s["schemaVersion"], s["status"], o["validatorsAgreeing"], o["validatorsConfigured"], m["operatorMinersActive"], m["operatorMinersConfigured"], s["network"]["name"], s["network"]["magic"])'
```

Expected: `1 live 5 5 4 4 Nu7StagingV3 7a6b7539`. On each mining host,
`grep 'successfully mined a new block.*success=Accepted' /var/log/zakura/zakura-fork.log | tail -1`
shows a recent block within the first hour.

**Status contract.**

```sh
curl -s -D - -o /dev/null -H 'Origin: https://zakura.com' https://api.nu7.valargroup.dev/v1/status | grep -ci '^access-control-allow-origin: https://zakura.com'   # 1
curl -s -o /dev/null -w '%{http_code}\n' -X OPTIONS -H 'Origin: https://zakura.com' https://api.nu7.valargroup.dev/v1/status   # 204
```

The payload has `chain.height`, `chain.hash`, `chain.blockTime`,
`chain.difficulty`, `chain.medianIntervalSeconds`, `chain.intervalSampleBlocks`
(up to 300), `nsm.balanceZat`, `observation.reorgs24h` and `recentBlocks`, the
fields the website parses.

**Unchanged publication.**

```sh
curl -s https://api.nu7.valargroup.dev/v1/network | sha256sum   # equals the step 4 baseline
curl -s https://api.nu7.valargroup.dev/v1/network | python3 -c 'import json,sys; print(json.load(sys.stdin)["configSha256"])'   # 12c94fe8...
curl -sI https://api.nu7.valargroup.dev/snapshots/nu7-v3-seed-4420648.tar.zst | grep -i '^content-length'   # 9945546169
```

**Removed and redirected routes.**

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://api.nu7.valargroup.dev/v1/block/4420652   # 404
curl -s -o /dev/null -w '%{http_code} %{redirect_url}\n' https://nu7.valargroup.dev/block/1   # 302 https://zakura.com/nu7/
```

**Restricted collector access.** From the primary,
`curl -s --max-time 5 http://134.199.239.83:8094/v1/miner` returns JSON; from any
other host it must fail, for each of the three remote hosts. The unit's
`IPAddressDeny`/`IPAddressAllow` only work when `systemctl --version` lists
`+BPF_FRAMEWORK`; with `-BPF_FRAMEWORK` systemd ignores them. Treat the allowlist as
unverified until that negative check passes. Where it fails, keep (or add) a host
nftables rule that admits port 8094 only from the primary, persisted in the host's
nftables configuration, for example:

```sh
nft add table inet zakura_nu7
nft add chain inet zakura_nu7 input '{ type filter hook input priority 0; policy accept; }'
nft add rule inet zakura_nu7 input tcp dport 8094 ip saddr != 167.99.146.155 drop
nft add rule inet zakura_nu7 input tcp dport 8094 meta nfproto ipv6 drop
```

Then repeat the negative check. Never widen the listener instead. On each host,
`systemctl show -p DynamicUser -p User zakura-nu7-miner-status.service` and, on the
primary, the same for `zakura-nu7-dashboard.service` show `DynamicUser=yes` and no
`root` user. `ss -ltn sport = :8093` shows only `127.0.0.1`.

**Faucet**, after step 7's switch:
`systemctl show -p LoadCredential zakura-nu7-faucet.service` names only
`faucet-key.hex`; `curl -s https://api.nu7.valargroup.dev/v1/faucet/status` reports
`"ready": true`; a test claim reaches `sent` with a transaction ID; and
`/root/fork-miner-key.hex` is gone from the host.

**Retired units.**
`systemctl list-unit-files 'zakura-fork-miner*' 'zakura-nu7-remote-miner@*' 'zakura-nu7-miner-status@*'`
lists nothing on any host.

The website itself is checked in a browser by the release owner.

## Rollback

If an acceptance check fails, stop and restore the previous generation as a whole:

1. Restore each node's binary, config and unit from
   `/root/nu7-v3-review-rollback` (and the participant's binary from its backup), then `systemctl daemon-reload` and restart the
   node. Chain state is untouched by this redeploy.
2. Re-enable the sidecar miner units from the rollback copy, using the binaries
   that remain in their original paths.
3. Restore `/opt/zakura-nu7-dashboard`, the old dashboard unit, the old remote
   status instances, and `/etc/caddy/Caddyfile`, then reload Caddy.
4. Keep the faucet on its previous unit and key if it was not yet switched.

Never publish a mixed manifest. The V3 snapshot URL stays immutable in every case.
