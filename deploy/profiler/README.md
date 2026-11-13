# Continuous CPU profiler

`zakura_profiler.py` runs beside a `zakurad` node, samples where the node spends
its CPU time all day, and serves flame graphs of it. The cluster status
dashboard links to it: a node with a profiler shows a "profiled" badge that
opens `/profile/<node>`.

It runs on every mainnet node that has a `profiler_url` in the deploy workflow's
node list: the regional nodes and `archive-vct-off`.

## What it measures

**CPU time, as a flame graph.** Every 60 seconds `perf` samples the stacks of
the node's threads while they are on a CPU. Nothing is added to `zakurad`: the
release binary already has the symbols and unwind tables `perf` needs. The page
shows the result for the last 15 minutes up to 7 days, or for a stretch you
drag across the timeline. You can:

- zoom into any function, or show one function everywhere it is called from;
- switch to one subsystem (block verification, block propagation, block
  arrival and sync, RPC and mining, mempool, other networking, state commit),
  which merges that subsystem's work across every thread and task it ran on;
- keep only the samples taken while a block lands, from 250 ms before to
  100 ms after each block announcement in the node log.

Time a thread spends waiting, on the network, a lock or a disk read, is not CPU
time and is not in the flame graph.

**A block's path through the node, on the clock.** When the node writes its
trace tables (`trace_dir` under `[network.zakura]` in its config), the profiler
also reads how long each block spent in each stage: download, verification,
the wait for the state writer, the commit, and telling Zakura peers about the
new tip. This includes waiting. Blocks that arrive over the legacy Zcash
network have no header stages.

**A block's path between nodes.** For each block the profiler also keeps the
wall-clock time at which the node first heard of it, asked for it, had its
bytes, finished verifying it, made it the tip and told its peers, and what it
exchanged with each peer it can tell apart: a Zakura peer by the label the
trace rows carry for its node id, a legacy peer by the name the node was given
for its address (`peer-names` below). It serves these records at
`/api/propagation`, with the node's clock offset as chrony or
systemd-timesyncd reports it. The
status dashboard reads them from every node and joins them into the block
propagation page (`deploy/runner/README.md`). That request is for the
dashboard only and is not among those it passes through to the public.

**Who served each block.** Each record also lists the node's attempts to
download the block, up to 16: the network stack, how long the attempt took,
how it ended, and the peer's software as its handshake gave it. Legacy attempts
include failures; Zakura attempts currently include only received bodies. A
whole-range unavailable event is not evidence that every body in it failed.
When the node writes peer addresses (`expose_peer_addresses`, below) an attempt also has the
peer's IP address; otherwise the address is absent. These addresses are every
peer's, with nothing left out, which is why `/api/propagation` is not passed
through: the dashboard drops the private ones when it joins the nodes.

## Staying out of the node's way

- Parsing, storage and serving run at idle scheduling priority and idle I/O
  priority, so they use only CPU nothing else wants. Only `perf record` itself
  runs at normal priority, because it has to keep up with the samples.
- The systemd unit caps the whole service at 30% of one core and 1 GiB of
  memory.
- The sampling rate adapts: up to 199 samples a second while the node is
  quiet, falling as low as 19 when it is busy, so one window never holds more
  than about 3,000 samples.
- The page reports the profiler's own CPU cost and any windows with lost
  samples.

To switch CPU sampling and trace retention off: `systemctl disable --now
zakura-profiler`. First remove the node's `trace_dir` setting and restart the node,
or arrange separate retention: stopping the profiler does not stop trace writes.

## Files and storage

| Path on the node | Contents |
| --- | --- |
| `/opt/zakura-profiler/` | `zakura_profiler.py`, `stacks.py`, `blocks.py`, `peer_addresses.py`, `page.html` |
| `/etc/systemd/system/zakura-profiler.service` | the unit, with the resource limits |
| `/etc/systemd/system/zakura-profiler.service.d/node.conf` | this node's name, log file and trace directory |
| `/mnt/data/zakura-profiler/` | one file per minute for 48 hours, one per hour for 14 days, capped at 2 GiB |
| `/run/zakura-profiler/` | scratch space in memory for sampled windows, 512 MiB at most in all |

The service listens on `127.0.0.1:8097`. The dashboard reaches it through the
node's `monitor.profiler_url` setting and serves it publicly, rate limited, at
`/profile/<node>`.

The node never rotates its trace tables. Given `--trace-dir`, the profiler
renames any table in that directory that grows past `--trace-table-cap-mb`
(200 MB by default) to `<table>.jsonl.1`, replacing the one renamed before it.
The node notices within about a second and starts a new `<table>.jsonl`, so no
row is lost: the older rows are in `<table>.jsonl.1` and the newest in
`<table>.jsonl`. Reads stay bounded per poll; if the renamed file takes several
polls to drain, the profiler retains it even while the node has not created its
replacement, and postpones the next rotation.
Each table normally uses about twice the cap, but can temporarily exceed it
while the profiler catches up. A node build
from before the node did this keeps writing to the renamed file; the profiler
empties that file once it reaches twice the cap and its observed backlog has
been read. This can lose rows appended between the read and the emptying. If
the profiler cannot catch up, the file can grow beyond twice the cap. Leave
`--trace-dir` off if something else needs the complete tables. A cap of 0 disables rotation.
Trace reading and rotation run in their own worker every five seconds, including
when `perf` cannot start or produces no samples. The status endpoint reports the
configured trace directory, cap, and whether that worker has completed a recent
poll without a file-rotation error. A table it could not rotate makes it
not ready and is reported as the profiler's error.

## Installing

`fleet.py` installs the profiler from the deployer's node list. A node takes
part when its entry says where the dashboard reaches its profiler:

```toml
[[nodes]]
name       = "us-0"
ssh_string = "root@203.0.113.10"
monitor    = { profiler_url = "http://127.0.0.1:8100" }
```

The mainnet deploy workflow prepares each node before deploying its binary and
installs the tunnels afterwards. The commands are safe to repeat; the three node
commands accept `--node NAME` to act on one node.

```bash
# Before the node restarts: install retention, then enable trace writes.
python3 deploy/profiler/fleet.py ensure-trace-config --config nodes.toml
# Before the node restarts: tell it the other nodes' names, except private ones.
sudo -n cat /etc/zakura-mainnet-dashboard/private/addresses.json \
  | python3 deploy/profiler/fleet.py peer-names --config nodes.toml \
      --private-addresses -
# To refresh the profiler alone, without changing the node's config:
python3 deploy/profiler/fleet.py install --config nodes.toml
# On the dashboard host: one SSH tunnel per node on another host.
python3 deploy/profiler/fleet.py tunnels --config nodes.toml
```

**`ensure-trace-config`** reads the config file named on the node's service
command line, installs the profiler, and verifies its trace-retention worker is
running with the expected directory and cap. Only then does it add
`trace_dir = "/mnt/data/zakura-traces"` under
`[network.zakura]` when the config sets no `trace_dir`, and
`expose_peer_addresses = true` under `[network]` when the config does not set
it. It changes nothing else,
keeps the first original as `<config>.before-trace-dir`, and does not restart
the node: the settings take effect when the node next starts. A config that
already names a trace directory keeps it, and one that sets
`expose_peer_addresses = false` keeps that and is reported.
The node replaces its config only after checking that the new text arrived
whole, so a connection that drops mid-write leaves the config as it was.
An installation or readiness failure leaves a node without tracing unchanged.

When adding the setting, the installer creates a new trace directory containing
`.zakura-profiler-managed`, its explicit permission to rotate files there on later
runs. An existing directory without that marker is refused rather than adopted.

`us-east-0` wrote trace files to the default directory before this installer
existed, and its profiler kept them under the cap. To keep that, create the
marker there once by hand, before the first deploy that runs this installer:

```bash
touch /mnt/data/zakura-traces/.zakura-profiler-managed
```

Without it the profiler on `us-east-0` reads that directory with a cap of 0 and
its trace files grow without limit.

With `expose_peer_addresses` on, the node writes its peers' real addresses,
where it wrote `v4redacted` before, in two places on its own disk: its log
file and its trace files. Both are readable by root only. Nothing else on the
node changes, and no address leaves the node except through the profiler's
`/api/propagation`, which only the dashboard host can reach.

**`peer-names`** writes a systemd drop-in for the node's own service
(`<service>.service.d/trace-peer-names.conf`) that sets
`ZAKURA_TRACE_PEER_NAMES` to `name=ip` for every other node in the list, with
the address each is reached at over SSH. The node then writes that name, and
never the address, in the trace rows for a legacy peer connecting from it. It
does not restart the node: the names take effect when the node next starts. A
node build that does not know the variable ignores it. Anyone on the node can
read the drop-in, so a node whose address is on the private list
(`--private-addresses`, the dashboard host's list) is named to no node, and
nothing is written when that list cannot be read.

**`install`** needs `perf` on the node (the `linux-tools` package) and installs
`binutils` when `c++filt` is missing. It writes the node's own name, log file
and trace directory into a systemd drop-in
(`zakura-profiler.service.d/node.conf`), so the unit file is the same on every
node. A trace directory that `ensure-trace-config` did not set is read with a
cap of 0, so the profiler never empties files someone else chose to keep, even if
their directory is `/mnt/data/zakura-traces`. Only a directory carrying the
installer's marker gets the 200 MB per-file cap.

**`tunnels`** is for the dashboard host. A profiler listens only on its own
host's loopback address, so the dashboard host keeps an SSH tunnel to each
node on another host: an instance of `zakura-profiler-tunnel@.service` per
node, forwarding the port in the node's `profiler_url` to that profiler. The
node on the dashboard host itself is listed with the profiler's own port, 8097,
and needs no tunnel. The tunnels use the SSH access the dashboard already has
to every node, so nothing new is opened on a profiled host.

To install by hand on one host instead:

```bash
install -d /opt/zakura-profiler
install -m 644 zakura_profiler.py stacks.py blocks.py peer_addresses.py page.html /opt/zakura-profiler/
install -m 644 zakura-profiler.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now zakura-profiler
curl -s 127.0.0.1:8097/api/status
```

The unit's node name, log path and trace directory are those of the regional
fleet nodes. On a host where they differ, override `ExecStart` in a drop-in
(`systemctl edit zakura-profiler`); leave `--trace-dir` out where the node does
not write trace tables.

## Peer names in gossip traces

Set the IP-to-name mapping in the node configuration to label peers you operate:

```toml
[network.zakura]
trace_peer_names = { "192.0.2.1" = "us-east-0", "2001:db8::9" = "asia-0" }
```

This only labels gossip trace rows; it does not change peer selection or trust.
Unlisted and proxied peers remain unnamed. Without `trace_dir`, no gossip rows
are written. Invalid IP addresses are rejected when the configuration loads.

`ensure-trace-config` fills in a missing mapping from the fleet inventory,
excluding the node itself. It preserves an explicit mapping, including an empty
one. After retention is ready and the configuration write has been verified, it
removes the obsolete `trace-peer-names.conf` systemd override and reloads systemd.
The node is not restarted by this step. Peer names are read from configuration,
not from `ZAKURA_TRACE_PEER_NAMES`.

An older binary that does not recognize `trace_peer_names` cannot read this
configuration. Remove that setting before rolling back to such a binary.

## Tests

```bash
python3 deploy/profiler/test_zakura_profiler.py
python3 deploy/profiler/test_fleet.py
```
