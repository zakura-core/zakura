# Pool canaries

Tools and a runbook for running the pool core and `tx-relay` on a few of our own
nodes, next to neighbours on today's pool, and showing that block relay, pools
and templates hold up.

`canary.py` has four subcommands. Each grade prints a JSON report and exits 0
for pass, 1 for fail, 3 for inconclusive and 2 for a usage or runtime error.
Too few samples or a missing row is inconclusive, never a pass.

## Pool overlap and templates

`sample` reads every node at once, every `--interval` seconds, for
`--duration` seconds. For each node it reads the tip, then the pool, then the
tip again, and drops the pool if the tip moved. Every `--templates-every`
rounds it also reads `getblocktemplate`. Rows are appended to `--out`, one per
node per round.

```sh
python3 canary.py sample --duration 86400 --out samples.jsonl \
  --node canary=http://10.0.0.1:8232/ \
  --node n1=http://10.0.0.2:8232/ \
  --node n2=http://10.0.0.3:8232/
```

RPC must answer without a cookie, as it does on a private address with
`enable_cookie_auth = false`. A node without a miner address serves no
templates, so give each compared node one before the stage starts.

`overlap` pairs two nodes' reads only when both saw the same stable tip and
their pool reads were within `--max-skew` seconds, counting how long each
read took. A pair's score is the share of the two pools they have in
common. Pairs where both pools are empty are counted apart. It passes when
every pair with the canary scores at least the lowest pair of neighbours,
less `--tolerance`.

Both `overlap` and `templates` fail a canary that answered a smaller share
of rounds than every neighbour, by more than `--max-missed` (default 0.01).
Their scores use only rounds the canary answered, so this is what catches a
canary that stops answering partway through a window.

```sh
python3 canary.py overlap --samples samples.jsonl --canary canary --min-pairs 1000
```

`templates` pairs the canary's template with each neighbour's when both build
on the same block and were read at most `--max-skew` seconds apart, counts
pairs of empty templates apart, and passes when the canary's median fee total
is at least `1 - --tolerance` of each neighbour's. `sample` reads every
node's template at once, after all the pool reads, so the reads line up.

```sh
python3 canary.py templates --samples samples.jsonl --canary canary --min-pairs 100
```

## Tip lag

Tip lag is the gap between two nodes publishing the same block as their tip.
It reads `tip_published` rows from a `deploy/trace-query` mirror, so traces
must be on and pulled from every measured node (see below). The block list
comes from one node's RPC, for heights `--from-height` to `--to-height`, and
every node must have a row for every block in it, or the window is
inconclusive.

```sh
python3 canary.py tip-lag --db mirror.sqlite --rpc http://10.0.0.2:8232/ \
  --node n1 --node canary --from-height 3600000 --to-height 3600200 \
  --clock-bound-ms 12 --margin-ms 250
```

The first `--node` is the reference unless `--reference` names another. A lag
within `--clock-bound-ms`, the hosts' clock offset bound from
`deploy/trace-query/README.md`, counts as none. Without `--margin-ms` the lags
are reported and the window is inconclusive.

## Runbook

Every Mainnet restart in this runbook waits for an explicit go.

### Before a stage

1. Read each node's effective settings from its metrics and write them down:
   `mempool_implementation{implementation=...}`,
   `zakura_p2p_tx_relay_registered` and `zakura_p2p_tx_relay_sessions`.
   `curl -s http://127.0.0.1:9999/metrics | grep -E 'mempool_implementation|tx_relay_(registered|sessions)'`
2. Check that `getblocktemplate` succeeds on the canary and on each
   neighbour it is compared with.
3. Check that traces are on (below) and that a fresh `tip_published` row has
   arrived from every measured node.

### Save the config that matches the binary

An older binary refuses config keys it does not know, so a binary rollback
needs the config that went with it. Before changing a Mainnet node:

```sh
systemctl cat zakurad | grep ExecStart   # the config is the path after -c
/usr/local/bin/zakurad --version         # e.g. zakurad 1.8.9
cp /etc/zakura/zakura.toml /etc/zakura/zakura.toml.1.8.9
```

Keep that copy until the node runs a binary that has been on the fleet for a
release.

### Make a Mainnet node a canary

The Mainnet deploy only swaps binaries, so the switch is a hand edit.

1. Save the config, as above.
2. Add the pool setting, and for a dual node `tx-relay`:

   ```toml
   [mempool]
   implementation = "core"

   [network.zakura.tx_relay]
   enabled = true
   ```

   `tx-relay` needs `p2p_stack = "dual"` or `"zakura"` as well. Without the
   core pool zakurad logs a warning and leaves it off.
3. `systemctl restart zakurad`, then check the metrics from "Before a stage".
   For `tx-relay` also check that `zakura_p2p_tx_relay_sessions` is above zero
   once a peer with it is connected.

On Testnet, `zakura-testnet-deploy.yml` does the same for one named node with
its `mempool_implementation` and `tx_relay` inputs.

### Turn a canary back

1. Remove the `[mempool] implementation` and `[network.zakura.tx_relay]` lines.
2. `systemctl restart zakurad` and check that `mempool_implementation` reads the
   default pool and `zakura_p2p_tx_relay_registered` reads 0.

If the binary misbehaves too, restore the saved config first, then run
`zakura-mainnet-rollback.yml`, then check that the node started.

### Traces

Turn traces on with a trace directory and the loopback raw endpoint, then
restart:

```toml
[network.zakura]
trace_dir = "/mnt/data/traces/canary"

[tracing]
raw_endpoint_addr = "127.0.0.1:9009"
```

Pull them from a collector through an SSH tunnel per node, as
`deploy/trace-query/README.md` describes:

```sh
ssh -N -L 19009:127.0.0.1:9009 root@canary &
python3 ../trace-query/trace_query.py pull --db mirror.sqlite \
  --node canary=http://127.0.0.1:19009 --host-header canary=127.0.0.1:9009 \
  --table chain_tip --follow 10
```

Record each host's clock offset before and after the window with
`timedatectl timesync-status`. Turn traces off again when the stage ends.
