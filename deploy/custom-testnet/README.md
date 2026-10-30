# Repeatable archive Testnet

Each reset refreshes an archive seed to the public Testnet tip, then creates an
isolated fork with PQ transparent activation in its first newly mined block.
NU7 retains its public Testnet activation at 4,465,026. Mining starts ten minutes
after both archive validators restore and verify the refreshed seed. Proof of
work remains enabled, so activation can occur later than the countdown.

The controller, preparation node, and two validators use one dedicated host.
Caddy serves the standalone dashboard and immutable seed files. The preparation
node runs only while refreshing a seed and never mines.

## Reset or inspect

On the deployment host:

```sh
sudo systemctl start --no-block zakura-pq-reset.service
sudo journalctl -fu zakura-pq-reset.service
sudo /usr/local/bin/zakura-testnet status
```

For a different countdown, run the foreground command instead:

```sh
sudo /usr/local/bin/zakura-testnet reset --activation-delay 10m
```

Reset stops this deployment's validators and refreshes the preserved archive on
public Testnet. It requires two agreeing, fresh public validator observations,
then checks that the local archive matches their canonical tip twice. An
isolated probe with fresh peer state records the exact stopped seed tip without
importing more public blocks. The resulting snapshot contains finalized state
and non-finalized backups, without network identities or credentials.

Reset creates a new name, network magic, node identities, participant
configuration, and run identifier. Both validators must report archive storage,
serve historical block bodies, and agree with the pinned seed before the
countdown starts. The final checkpoint is the seed's finalized tip, allowing
non-finalized backup replay. No historical timestamp exception is needed.

A controller restart resumes the recorded countdown; status queries never enable
mining. At the deadline, the controller waits for the restarted miner, then
restarts its observer to reconnect. Concurrent resets are refused. Failed
preparation or validation leaves fork mining stopped. Previous fork balances and
transactions do not carry into the new run.

## Archive provenance and services

`/etc/zakura-custom-testnet.json` pins the deployment root, service prefix,
operator source revision, binary source revision and checksum, source snapshot
URL, archive checksum and size, full and finalized seed tips, original network
directory, miner receiving address, public peer, and trusted public-status URL.
The seed must include public NU7 history and declare archive storage. Pruned
state cannot be converted to an archive by changing a setting.

`archive_seed.py` captures a new source snapshot by stopping a source archive
node only while copying its state. It restarts that service even if copying
fails, and compresses the independent copy after restart. Two other agreeing
public validators must remain online. Checksum-verify the download before
installing it as the initial seed. Subsequent resets reuse the refreshed local
archive as their catch-up base.

`install.py` installs the controller, preparation, validator, and reset service
units, Caddy configuration, and operator command. It requires the existing
binary, mounted storage, and a verified archive; it does not provision resources
or launch mining. The reset service records progress in the journal and the
dashboard reports preparation stages. A failed reset requires operator action;
it never silently starts another run.

Archive files live outside disposable node directories. Snapshot URLs name an
immutable file; `/snapshots/seed.tar.zst` redirects to the current pinned file.
Retain a participant's matching snapshot and manifest together. Archive space
checks use the measured uncompressed state size. No paid resources are created
by installation or reset.

The active state format is pinned separately so obsolete repair databases in a
source cache are excluded during restore. Locally retain the current and previous
seed; a successful refresh retires only the older archive recorded in deployment
configuration. It never scans for unrelated files to delete.
Archive bytes never change at a URL, but older download URLs expire after two
generations. The deployment commits the new seed record before retiring an older
archive and its matching metadata.

Set `miner_threads` in the deployment JSON to choose the primary's solver count.
The deployed value is six; the observer does not mine. The node caps solvers at
available CPUs and 256 distinct nonce ranges.

## Join a run

Use `/v1/network` and `/v1/config` from the dashboard origin together. Build the
manifest's binary source revision with `pq`, download the pinned archive, verify
its checksum, and extract into a separate cache directory. Rename its network
directory under `state/v*/` and directly under `non_finalized_state/` to the new
lowercase network name. Use `storage_mode = "archive"`, keep JSON-RPC on
localhost, and refresh state and configuration after every reset.

`/v1/status` reports archive preparation, node agreement, public NU7 and custom
PQ activation heights, countdown, seed provenance, and the observed PQ block.
Activation and archive checks do not establish that a signed PQ spend has been
exercised; report that validation separately.

## Checks

```sh
python3 -m unittest discover -s deploy/custom-testnet -p 'test_*.py'
```

Live acceptance requires historical block bodies on both archive validators,
seed hash preservation, observer agreement on the PQ activation block, and two
consecutive successful reset cycles.
