# Repeatable custom Testnet

This deployment restores a fixed pre-NU7 Testnet seed and activates NU7 and PQ
transparent together in its first new block. Mining starts five minutes after
both validators have restored and verified their seed tip. Proof of work remains
enabled, so the block can arrive later than the countdown.

The controller and node services run on one dedicated host. Caddy serves the
standalone dashboard and immutable seed. No part of the existing NU7 deployment
or zakura.com needs to change.

## Reset or inspect

On the deployment host:

```sh
sudo /usr/local/bin/zakura-testnet reset --activation-delay 5m
sudo /usr/local/bin/zakura-testnet status
```

Reset stops this deployment's services, replaces only its two working node
directories, and restores the preserved archive. It creates a new name, network
magic, node identity, participant configuration, and run identifier each time.
The countdown starts after both nodes agree with the recorded seed hash. A
controller restart resumes the recorded countdown; status queries never enable
mining. Concurrent resets are refused. A failed reset leaves mining stopped.
Previous fork balances and transactions do not carry into the new run.

The five-minute countdown is deliberate: the historical seed can immediately
qualify for Testnet minimum difficulty. Waiting for eight newly mined blocks is
neither necessary nor a five-minute schedule.

## Configuration and services

`/etc/zakura-custom-testnet.json` pins the deployment root, service prefix, binary,
source revision, miner receiving address, public peer, and seed URL provenance,
full and finalized tip heights/hashes, checksum, and original network directory.
It contains no signing keys. Preserve the seed archive outside the disposable
node directories.

`install.py` installs the controller, the two node units, Caddy configuration,
and the operator command from a checked-out source revision. It requires an
already installed binary, mounted storage, and verified archive. It does not
provision resources or launch mining; call reset explicitly afterward.

The configured `max_block_time_start_height` is seed height plus seven. This
allows the historical tip to resume at the current clock, then restores the
normal median-time-plus-90-minute bound once six fresh blocks supply a majority
of the preceding eleven timestamps. The lower timestamp bound and proof of work
remain enforced. This is a custom-network consensus parameter shared by every
participant; it does not change public Testnet or Mainnet.

The seed database format may be upgraded only by the node's supported upgrade
path. Do not edit its version file or rewind a pruned post-NU7 snapshot to
fake a pre-NU7 seed. Snapshot restoration and node startup must pass before mining.

The last configured checkpoint is the seed’s finalized tip, not its full tip.
A checkpoint above the finalized tip prevents restoration of the non-finalized
backup, leaving both nodes below the expected seed height.

## Join a run

Use `/v1/network` and `/v1/config` from the dashboard origin together. Build the
manifest's revision with the `pq` feature, download and checksum the seed,
extract into the participant cache directory, and rename its network directory
under `state/v*/` and directly under `non_finalized_state/` to the new
lowercase network name. Keep JSON-RPC on localhost. A participant must refresh
its configuration
and state after every operator reset.

`/v1/status` reports the current generation, node heights/hashes and agreement,
activation settings, countdown, and observed activation block. An unavailable
node or mismatched parameters produce an unhealthy response. Configuration and
activation checks do not prove a signed PQ spend has been exercised; report that
validation separately.

## Checks

```sh
python3 -m unittest discover -s deploy/custom-testnet -p 'test_*.py'
```

The focused Rust tests cover isolated PQ configuration and historical mining
timestamps. Live acceptance additionally requires a matching restored seed,
independent validator agreement, shared NU7/PQ activation, and two consecutive
successful reset cycles.
