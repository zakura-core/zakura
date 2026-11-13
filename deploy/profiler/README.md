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

To switch it off: `systemctl disable --now zakura-profiler`. The node is not
affected.

## Files and storage

| Path on the node | Contents |
| --- | --- |
| `/opt/zakura-profiler/` | `zakura_profiler.py`, `stacks.py`, `blocks.py`, `page.html` |
| `/etc/systemd/system/zakura-profiler.service` | the unit, with the resource limits |
| `/etc/systemd/system/zakura-profiler.service.d/node.conf` | this node's name, log file and trace directory |
| `/mnt/data/zakura-profiler/` | one file per minute for 48 hours, one per hour for 14 days, capped at 2 GiB |
| `/run/zakura-profiler/` | scratch space in memory for the window being sampled |

The service listens on `127.0.0.1:8097`. The dashboard reaches it through the
node's `monitor.profiler_url` setting and serves it publicly, rate limited, at
`/profile/<node>`.

The node never rotates its trace tables. Given `--trace-dir`, the profiler
empties any table in that directory that grows past `--trace-table-cap-mb`
(200 MB by default). With a cap of 0 it never empties one: use that, or leave
`--trace-dir` off, where something else needs the complete tables.

## Installing

`fleet.py` installs the profiler from the deployer's node list. A node takes
part when its entry says where the dashboard reaches its profiler:

```toml
[[nodes]]
name       = "us-0"
ssh_string = "root@203.0.113.10"
monitor    = { profiler_url = "http://127.0.0.1:8100" }
```

The mainnet deploy workflow runs the four steps below on every deploy. Each is
safe to repeat, and each but `tunnels` takes `--node NAME` to act on one node.

```bash
# Before the node restarts: make it write trace tables.
python3 deploy/profiler/fleet.py ensure-trace-config --config nodes.toml
# Before the node restarts: tell it the names of the other nodes.
python3 deploy/profiler/fleet.py peer-names --config nodes.toml
# Copy the profiler to each node and start it.
python3 deploy/profiler/fleet.py install --config nodes.toml
# On the dashboard host: one SSH tunnel per node on another host.
python3 deploy/profiler/fleet.py tunnels --config nodes.toml
```

**`ensure-trace-config`** reads the config file named on the node's service
command line and adds `trace_dir = "/mnt/data/zakura-traces"` under
`[network.zakura]` when the config sets no `trace_dir`. It changes nothing else,
keeps the first original as `<config>.before-trace-dir`, and does not restart
the node: the setting takes effect when the node next starts. A config that
already names a trace directory is left as it is.

**`peer-names`** writes a systemd drop-in for the node's own service
(`<service>.service.d/trace-peer-names.conf`) that sets
`ZAKURA_TRACE_PEER_NAMES` to `name=ip` for every other node in the list, with
the address each is reached at over SSH. The node then writes that name, and
never the address, in the trace rows for a legacy peer connecting from it. It
does not restart the node: the names take effect when the node next starts. A
node build that does not know the variable ignores it.

**`install`** needs `perf` on the node (the `linux-tools` package) and installs
`binutils` when `c++filt` is missing. It writes the node's own name, log file
and trace directory into a systemd drop-in
(`zakura-profiler.service.d/node.conf`), so the unit file is the same on every
node. A trace directory that `ensure-trace-config` did not set is read with a
cap of 0, so the profiler never empties files someone else chose to keep.

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
install -m 644 zakura_profiler.py stacks.py blocks.py page.html /opt/zakura-profiler/
install -m 644 zakura-profiler.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now zakura-profiler
curl -s 127.0.0.1:8097/api/status
```

The unit's node name, log path and trace directory are those of the regional
fleet nodes. On a host where they differ, override `ExecStart` in a drop-in
(`systemctl edit zakura-profiler`); leave `--trace-dir` out where the node does
not write trace tables.

## Tests

```bash
python3 deploy/profiler/test_zakura_profiler.py
python3 deploy/profiler/test_fleet.py
```
