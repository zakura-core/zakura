# Continuous CPU profiler

`zakura_profiler.py` runs beside a `zakurad` node, samples where the node spends
its CPU time all day, and serves flame graphs of it. The cluster status
dashboard links to it: a node with a profiler shows a "profiled" badge that
opens `/profile/<node>`.

It is a proof of concept on two mainnet nodes: `us-east-0`, which runs both
peer-to-peer stacks, and `archive-vct-off`, which runs only the legacy one.

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
network have no header stages, and there is no trace point between the new tip
and the hand-off to legacy gossip.

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
| `/mnt/data/zakura-profiler/` | one file per minute for 48 hours, one per hour for 14 days, capped at 2 GiB |
| `/run/zakura-profiler/` | scratch space in memory for the window being sampled |

The service listens on `127.0.0.1:8097`. The dashboard reaches it through the
node's `monitor.profiler_url` setting and serves it publicly, rate limited, at
`/profile/<node>`.

The node never rotates its trace tables. Given `--trace-dir`, the profiler
empties any table in that directory that grows past `--trace-table-cap-mb`
(200 MB by default). Leave `--trace-dir` off if something else needs the
complete tables.

## Installing

The mainnet deploy workflow installs the profiler on the runner host and adds
its address to the dashboard's node list. By hand, on a host with `perf` and
`c++filt` (the `linux-tools` and `binutils` packages):

```bash
install -d /opt/zakura-profiler
install -m 644 zakura_profiler.py stacks.py blocks.py page.html /opt/zakura-profiler/
install -m 644 zakura-profiler.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now zakura-profiler
curl -s 127.0.0.1:8097/api/status
```

Then add `monitor = { profiler_url = "http://127.0.0.1:8097" }` to that node's
entry in the dashboard's `nodes.toml` and restart the dashboard.

The unit's node name, log path and trace directory are those of the regional
fleet nodes. On a host where they differ, override `ExecStart` in a drop-in
(`systemctl edit zakura-profiler`); leave `--trace-dir` out where the node does
not write trace tables.

### A profiler on another host

The profiler listens only on its own host's loopback address. For a node that
is not the dashboard host, the dashboard host keeps an SSH tunnel to it, and
`profiler_url` names the tunnel's local port.
`zakura-profiler-tunnel-archive-vct-off.service` is that tunnel for
`archive-vct-off` (local port 8098); the deploy workflow installs it. It uses
the SSH access the dashboard already has to every node, so nothing new is
opened on the profiled host.

## Tests

```bash
python3 deploy/profiler/test_zakura_profiler.py
```
