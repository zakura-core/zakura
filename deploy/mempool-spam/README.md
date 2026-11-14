# Mempool spam harness

This harness submits zecd self-sends through alternating Zakura and Zebra
upstreams, measures propagation across the selected environment, waits for
mining, and writes machine- and human-readable reports.

It uses a spend-capable wallet already installed on the controller. Wallet
material is not stored in this directory and must never be copied into CI
artifacts.

## Layout

- `scripts/round_robin_selfsend.py` — transaction driver and grader
- `scripts/matrix_poller.py` — read-only baseline / propagation poller
- `scripts/spam_report.py` — rebuild reports from a driver JSONL log
- `envs/testnet/config.json` — live testnet controller and observation set
- `envs/mainnet/config.example.json` — mainnet schema example, not a live config

An environment config contains:

```json
{
  "network": "test",
  "controller": {
    "ssh_host": "167.99.103.111",
    "spam_root": "/root/ironwood-spam",
    "zecd_rpc": "http://127.0.0.1:18888"
  },
  "nodes": [
    {
      "name": "node-name",
      "impl": "zakura",
      "rpc_url": "http://node-ip:18232"
    }
  ]
}
```

`network` must be `test` or `main`. Every observation node must provide
`name`, `impl`, and `rpc_url`.

## Run on the testnet controller

The checked-out or synced harness must be under
`/root/ironwood-spam/harness`, alongside the existing `bin/`, `wallet/`, and
`logs/` directories:

```bash
python3 /root/ironwood-spam/harness/scripts/round_robin_selfsend.py \
  --environment testnet \
  --duration-minutes 10 \
  --amount 0.0002 \
  --require-all-seen \
  --require-mined
```

The driver:

1. Restarts one zecd process against each submit node in round-robin order.
2. Self-sends using normal zecd coin selection (Ironwood and Orchard allowed).
3. Counts a transaction as seen when a node's pool holds it: in
   `getrawmempool`, or in `getrawtransaction` with no confirmations. A node
   that first shows it already mined is recorded under `seen_after_mined`,
   and one whose last answer did not settle it under `unobserved`. Either
   makes that round inconclusive rather than a pass or a miss. A miss needs
   a definite answer: the pool listing without the transaction, and
   `getrawtransaction`'s "not found" error. Logs from before this split
   prove neither, so their rounds are inconclusive.
4. After submission ends, drains for up to 10 minutes and records mining.
5. Restores zecd to the controller's loopback Zakura RPC.

Outputs default to the environment's `<spam_root>/logs/`:

- `round-robin.jsonl`
- `round-robin-status.json`
- `report.json`
- `report.md`

Use `--config /path/to/config.json` for an ad-hoc environment, or
`--out-dir /safe/path` for separate output. `--max-rounds` can replace a timed
run. Grading is strict by default: the process exits 2 for runtime
failures, propagation misses, or unconfirmed transactions, and 3 for a run
with no miss but a round not seen in every pool, so the run is repeated
rather than passed. Node names must be unique. `--no-require-all-seen` and `--no-require-mined` turn either check
off.

To restart an untimed manual testnet run in the background:

```bash
bash /root/ironwood-spam/harness/envs/testnet/restart.sh
```

## Read-only matrix

```bash
python3 deploy/mempool-spam/scripts/matrix_poller.py \
  --environment testnet \
  --duration-secs 60 \
  --record-peer-graph \
  --out-dir /tmp/mempool-matrix
```

Add `--watch-txids txids.txt` to stop early once every listed transaction is
present on every node.

## Rebuild a report

```bash
python3 deploy/mempool-spam/scripts/spam_report.py \
  /root/ironwood-spam/logs/round-robin.jsonl
```

## GitHub Actions

Run the `Zakura mempool gossip` workflow manually. Its inputs select the
environment, submission duration (1–60 minutes), and amount. The workflow
syncs this directory to the configured controller, runs the strict propagation
and mining checks, and uploads only the JSONL and reports.

Testnet is ready to dispatch. Mainnet intentionally has only
`config.example.json`; a reviewed `envs/mainnet/config.json` and a
spend-capable controller wallet are required before mainnet dispatch can run.
