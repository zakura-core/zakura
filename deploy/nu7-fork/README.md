# NU7 fork testnet

A private network that forks the public Testnet and activates NU7 at a height we
choose, so NU7 consensus can be exercised end to end before any public
deployment.

The fork is an ordinary configured testnet. `testnet::Parameters::build()`
already *is* the public Testnet — genesis hash, magic, every activation height
through NU6.3, funding streams, the NU6.1 lockbox disbursements, the Orchard
soft-fork height and the checkpoint list. The fork overrides three things: its
name, its network magic, and the NU7 activation height. Everything else is
inherited, which is why the tooling here is small.

Its chain state is seeded from a real Testnet cache, so it carries genuine
pre-NU7 history and the measured NSM value balance rather than starting empty.

## Prerequisites

The deployed ref **must** contain the finalized NU7 consensus branch ID
(`0x77190ad8`). It landed in PR #1093; before that, the NU7 branch entry was
gated behind `cfg(any(test, feature = "zakura-test"))`, so a stock release
binary had no NU7 branch at all and the fork could not activate.

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
|---|---|---|
| before NU7 | 75s | **450s** (7.5 min) |
| after NU7 | 25s | **150s** (2.5 min) |

So an offset of 10 is about 75 minutes to activation, and 100 would be most of a
day. `./fork.py plan` prints the estimate before you commit to it.

The DigitalOcean Testnet state snapshot is refreshed weekly (Monday 04:00 UTC by
`zakura-pr-node-bake.yml`), so the seeded tip can lag the live chain by up to a
week. That is harmless for a fork — the activation height is relative to the
seed, not to the public chain.

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
NSM and the miner claims the subsidy plus the remaining 40%. NSM reissuance is
separate and only starts if `nsm_reissuance_offset` is set; it is clamped to at
least the NU7 height. See `docs/design/reissuance-accounting.md`.

Seeding a fork changes the header-chain network policy digest, because that
digest binds the full activation list. On the current database format this is
handled as a `RecoveryRepair::NetworkPolicyConfiguration` and rebound in place
during startup recovery — it is not a failure. Only the legacy v1–v3 migration
path rejects a mismatch outright.

## Layout

| Path | Purpose |
|---|---|
| `fork.toml` | Every fork parameter, safe to edit between runs |
| `fork.py` | Provision, seed, plan, render, deploy, status, reconfigure |
| `miner/` | The external miner (`zakura-fork-miner`) |
| `nodes.generated.toml` | Generated `deploy.py` fleet config; not committed |

`fork.py` renders a fleet config for `deploy/deployer/deploy.py` rather than
deploying by itself, so the fork node is built, shipped and supervised by exactly
the same path as every other managed node.
