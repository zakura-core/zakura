# Native sync observations

The recorder captures a test node's chain progress, process statistics, Linux
cgroup counters, host pressure and local metrics. The pressure report separates
reclaimable file cache from the time spent waiting for memory. These observations
complement the [GetBlocks ownership replay](getblocks-workloads.md); neither
establishes native sync success or a supported hardware envelope by itself.

## Record an existing test node

Run on the Linux test host after its node service starts. Use Python 3.11 or later
and a cgroup v2 system. The recorder only reads the service and local endpoints;
it does not start, stop or reconfigure the node.

```sh
python3 scripts/record-native-sync.py \
  --unit native-sync-test.service \
  --out /var/tmp/native-sync-samples.jsonl.gz \
  --seconds 900
```

The default exporter is `http://127.0.0.1:19999/metrics`, and the default RPC is
`http://127.0.0.1:18232`. Override them with `--metrics-url` and `--rpc-url` for
the test configuration. Only literal loopback HTTP endpoints without URL
credentials are accepted; proxies and redirects are disabled. The test node
must expose the read-only chain RPC without authentication on that local port.

The observer aims for one sample every two seconds. It brackets all reads with
same-host monotonic timestamps and also records wall time. RPC/exporter reads
have two-second timeouts and bounded responses. Missing files and endpoint
failures remain error objects rather than zero values. A missing systemd unit
fails the recording instead of resembling a successful stopped node.

Recording stops when the unit is inactive or failed, or the duration expires.
An existing output is never replaced. Preserve a failed or interrupted recording;
it can still be useful as explicitly incomplete diagnostic evidence.

## Read pressure without loading the whole recording

Use the measured workload phase from the experiment controller. Replace
`START_NS` and `END_NS` below with its inclusive Unix-nanosecond boundaries.
Those boundaries need recorded clock uncertainty when derived across hosts.

```sh
python3 scripts/report-native-sync-pressure.py \
  /var/tmp/native-sync-samples.jsonl.gz pressure.json \
  --start-utc-ns START_NS --end-utc-ns END_NS
python3 -m unittest discover -s scripts/tests -p 'test_native_sync_pressure.py'
```

The report streams through the entire gzip recording, including its footer and
rows outside the selected phase, and records its SHA-256. Memory use does not
grow with the number of samples. The output is created exclusively.

`recording_complete` means the recording ended with a stopped unit. A failed
unit can have a complete recording. `unit_success_observed` separately reports
whether the final systemd properties show success; it is null for older
recordings that did not include the unit result. Neither field proves the node
reached the intended chain hash. Keep the controller's exact-target and cleanup
evidence with this report.

An active final unit is rejected by default. `--allow-incomplete` produces a
diagnostic report with `recording_complete=false`; it does not accept a corrupt
or truncated gzip stream. A recording with no selected active-node samples has
null pressure and available-memory observations, not a zero-pressure pass.

## Interpret the measurements

PSI totals count microseconds of stalls. The analyzer excludes counter resets,
long sampling gaps and cgroup identity changes. It retains sample-read uncertainty
in the covered-time bounds and maximum interval percentage. These percentages
are bounds on observed intervals, not exact instantaneous utilization. Missing
counters remain null and excluded intervals are counted.

The host's estimated available memory includes reclaimable cache. Cgroup file
memory may be charged to the group that first created those pages, including a
fixture-preparation group. Neither high cache occupancy nor high estimated
availability alone establishes safe memory headroom. Inspect both host and node
pressure, memory-limit/OOM events, process memory and actual committed progress.
The recorder includes the underlying counters for those separate checks.

Do not align timestamps from different native trace emitters merely because
their process identities match. Explain received-body gaps with events from the
same block-sync emitter or an independently synchronized observer.
