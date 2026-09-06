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

The separate `whole_recording` section includes all recorded active-node samples,
including startup and the period after the last client finishes. Keep it beside
the selected workload results: a server can reach a memory limit during capture
drain or shutdown even when its sync-phase counters were zero. This summary uses
the same streaming analysis and does not change the chosen phase boundaries.

## Interpret the measurements

PSI totals count microseconds of stalls. The analyzer excludes counter resets,
long sampling gaps and cgroup identity changes. It retains sample-read uncertainty
in the covered-time bounds and maximum interval percentage. These percentages
are bounds on observed intervals, not exact instantaneous utilization. Missing
counters remain null and excluded intervals are counted.

`host_pressure` separately summarizes the recorded CPU and I/O PSI counters,
using the same interval rules as memory pressure. These cover the entire host,
including activity outside the node's cgroup. They can identify contention worth
investigating, but do not attribute a node stall to a particular operation or
measure CPU utilization, disk latency or disk throughput. CPU, I/O and memory
stall intervals can overlap; their totals must not be added into one stall time.
Only CPU `some` is reported. System-wide CPU `full` is undefined even when the
kernel exports zero for compatibility; see the
[Linux PSI documentation](https://docs.kernel.org/accounting/psi.html).

The host's estimated available memory includes reclaimable cache. Cgroup file
memory may be charged to the group that first created those pages, including a
fixture-preparation group. Neither high cache occupancy nor high estimated
availability alone establishes safe memory headroom. Inspect both host and node
pressure, memory-limit/OOM events, process memory and actual committed progress.
The recorder includes the underlying counters for those separate checks.

The `cgroup_memory` section retains the memory composition at the highest sampled
total usage and, separately, at the highest sampled anonymous usage. Each is one
bracketed read, not an atomic snapshot or a sum of independent peaks. File, LRU,
dirty-page and kernel subcategories overlap. The report preserves their values
without treating them as guaranteed reclaimable memory or calculating a headroom
pass from subtraction.

`value_observations` reports numeric ranges and counts for usage, swap usage,
and the applied high and hard memory limits across all selected active-node
samples. It counts unlimited (`max`) and unavailable reads separately. A limit
seen only at peak usage cannot hide a different or missing limit in other
samples. Matching limits throughout the recording still do not prove the value
between samples, and zero observed swap usage requires numeric observations;
missing swap counters cannot establish it.

The recorded `memory.peak` counter also preserves kernel-observed spikes between
usage samples. It is a cgroup lifetime maximum, so a value read during the workload
may include an earlier startup peak; it is not an exact peak for that phase.
Keep it separate from the largest sampled `memory.current` value. A missing peak
counter cannot rule out spikes, and the recording cannot capture a later peak
after its final successful read.

Limit-event changes are differences between valid adjacent samples of the same
unit and boot. Initial counter values, missing reads, resets and sampling gaps
do not contribute; excluded intervals remain explicit. A null change means no
valid interval was observed, while zero means valid intervals showed no change.
These changes are not the unit's absolute lifetime event counts. Keep the raw
recording and the controller's full-run outcome alongside the selected-phase
report, particularly when the node reaches a memory limit.

`event_observations` separately preserves the maximum absolute counter value
read, including the first sample. Numeric and unavailable sample counts keep a
missing counter distinct from observed zero. The maximum is not a sum across
counter resets or a count of events after the final successful read.

Do not align timestamps from different native trace emitters merely because
their process identities match. Explain received-body gaps with events from the
same block-sync emitter or an independently synchronized observer.

## Compare a planned series

After the native experiment controller writes its outcome audits, compare the
whole frozen plan, including trials whose evidence is not yet available:

```sh
python3 scripts/report-native-sync-series.py series-plan.json series-report.json
python3 -m unittest discover -s scripts/tests -p 'test_native_sync_series.py'
```

This reporter consumes the native lab's existing schema-1 plan and evidence
layout. The plan lists `first_pair` and indexed `pairs`, with a filename and
SHA-256 for each baseline and candidate specification. Each completed run needs
its controller, resources, outcome audit, and per-host archives with their
audited extracted metadata alongside the plan. It does not launch experiments
or recreate missing audits. Output is created exclusively.

The reporter checks specification, configuration, resource and archive digests,
audit/outcome agreement, and matching runtime helpers and host environments.
Within a pair, only the serving binary, its regulation overrides and capture
enablement may differ. Across repetitions, each side's conditions must remain
the same. Companion binaries, targets, resource limits and other settings remain
part of that comparison. Finite-pause recovery trials are rejected as ordinary
timing trials. These checks bind existing audited evidence; they do not repeat
the raw trace audit or independently attest what ran on the remote hosts.

Every planned pair remains in the report. A missing completion audit is
`unavailable`, which can mean an active run or a failure before an audit was
written. A recorded supervision failure or audited unsuccessful outcome is
`failed`. Neither contributes a
completion timing. Changed evidence fails reporting instead of silently dropping
the trial. `all_pairs_complete` requires every planned pair to be complete.

A preparation failure may later have a completed outcome only with a bound
resumption record showing that no native trial had started and no process needed
stopping. The failure and its resumption remain visible in the completed run,
and `runs_with_supervision_failures` still counts it. Repeating a timed trial
under the same run identifier is not accepted by this exception.

Individual client changes, medians and ranges describe the completed pairs.
Clients share a server and are not independent repetitions. A partial series,
or five completed pairs, does not establish p95 performance or production
readiness. Keep resource pressure, recovery and exact-target evidence with the
timing report when deciding whether the serving policy is acceptable.
