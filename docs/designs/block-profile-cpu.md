# Live block CPU profiles

## Scope

Capture sampled user-space CPU stacks from the running profiling node for new blocks. Keep the current main base and block timing instrumentation. No replay, chain database changes, or shielded batch instrumentation is required. Existing block pages remain usable when no CPU capture exists.

## Viewer

A CPU profile button opens `/cpu/RUN/ATTEMPT`, pinned to the exact verification request. A full-page, self-hosted, pinned Speedscope viewer provides zoom, search, thread selection, and aggregated flame graphs. The page identifies the block and the selected recorded-work interval, including finalization. The verifier-response interval is an optional narrower selection.

Frame identity retains the full symbol and module. Compact labels affect rendering only. Inspection and search retain full function names. Downloads preserve original symbols. Source links are only appropriate when a real file and line resolve at a verified revision.

New captures retain the `cpu-clock:u` period in nanoseconds for every sample. Flame widths use those periods as estimated CPU milliseconds. Speedscope exports include both millisecond and sample-count profiles, with one thread or all threads. Hover shows Self and Total weights, percentages, and sample counts. Historical captures without periods, and intervals mixing weighted and unweighted samples, retain count-only views. The canonical API supplies each `cpu_period_ns`, weight source, explicit units, and the retained period sum. Lost samples are not extrapolated. Sample order is not elapsed time. The canonical API retains actual monotonic timestamps and thread IDs separately. Do not invent durations between samples or convert idle gaps into CPU work. Captures cover process activity during the selected block interval, which can include other blocks and background work. User-space CPU sampling does not measure waiting or kernel execution.

## Capture

Use one continuously running `perf record` process with rotated output and an independent bounded decoder. Ordinary rotation must not stop sampling while symbols are decoded. Use 999 Hz and native timed rotation every second (ten seconds at 19/49/99 Hz) to keep raw segments bounded even across sixteen busy CPUs, with size and backlog limits. The installed perf version busy-polls after monitored worker threads exit. Capture therefore uses per-CPU events filtered to the node's exact systemd control group, verified against `/proc/PID/cgroup`. The decoder still filters the exact node PID. This preserves newly created workers without per-thread event lifetimes. An oversized active file stops capture visibly before its hard file limit. Verify the installed perf version's rotation and closed-file behavior in a bounded live canary before enabling the service.

Only immutable files no longer open by the producer can be decoded. Preserve process start identity, run ID, executable SHA-256, and build identity. Bound active files, sealed backlog, decoder memory and CPU, output size, and retained raw data. Never remove active or decoder-owned files. Overload must produce explicit partial coverage or stop capture visibly, without blocking the verifier. Keep all new data under the existing profiler quota.

The service follows the supervised node identity across restarts. Updater operations must drain capture before replacing the executable and restore it against the new PID. Historical captured data remains bound to the executable that produced it.

## API and coverage

A separate bounded CPU query retains interned full frames and stacks plus timestamp, thread, and stack ID per sample. Ordinary block detail should not decode all stacks. Calculate the recorded end before selecting CPU data, so finalization is included.

Distinguish pending, partial, unavailable, and retained coverage. Report acquisition uncertainty, decoder errors, lost samples, omitted samples or frames, and query limits. Empty captures are distinct from absent captures. Capture process lifetime or first/last sample alone does not prove exact coverage. Sparse samples remain useful but cannot prove absence of a short function. Do not silently retain only the hottest stacks.

## Deployment and validation

Three independent plan reviews covered capture lifecycle, measurement correctness, and UI/operations. They approved this approach with the boundaries above. Validate parser losses, bounded capture decoding, timestamp selection, finalization inclusion, exact recording links, and viewer interaction. Test the actual Linux perf rotation behavior and observe decoder throughput, retained sample quality, collection drops, CPU, memory, and disk growth during a bounded canary.

Deploy collector and viewer changes before enabling continuous capture. Preserve the live TLS/domain configuration when extending the proxy's explicit read-only routes. Assets remain local, with any viewer-specific content-security policy scoped to the viewer. Verify new live blocks and historical block pages. Rollback stops the sampler and restores the previous explorer executable without changing chain state.

## 999 Hz validation, September 25, 2026

The Linux host's perf 6.8.12 emits `cpu-clock:u` periods of 1,001,001 ns at
999 Hz. The decoder preserves these periods, rather than assigning wall-clock
intervals between samples. Tests cover nonuniform periods, concurrent threads,
idle gaps, mixed historical/new samples, invalid periods, import/query/export,
and count preservation in the viewer's grouped and caller/callee views.

A controlled OpenSSL SHA-256 throughput check ran three rounds of disabled,
99 Hz, and 999 Hz sampling in rotating order. Each workload used
`openssl speed -elapsed -seconds 3 -bytes 16384 -evp sha256`. Sampling used
`perf record -e cpu-clock:u -F RATE --call-graph dwarf,8192`. Median throughput
was 425,519.79, 425,831.08, and 424,591.36 kB/s respectively. The 999 Hz median
was 0.22% below disabled. It captured about 3,000 samples per three-second run,
compared with 297 at 99 Hz. Decode time was about 0.10 seconds versus 0.03 seconds.
This is a sampler overhead check, not a matched block-verification benchmark.
It does not establish a block latency percentile overhead bound.

The live rollout kept the node PID, current main base, run ID, and retained
history. It restarted the sampler, collector, and web service. The first new
blocks, 3,495,776 and 3,495,777, retained 56 and 46 samples, with period sums of
56.056056 and 46.046046 CPU ms. Their recorded elapsed times were 42.158 and
35.460 ms. Both had zero reported sample loss, decode errors, or query omissions.
The initial live observation had no sampler restarts or dropped segments and
zero collector errors. Capture coverage remains explicitly approximate and
unresolved stack frames remain visible. Historical block 3,495,775 still uses
its original 99 Hz count-only profile.

The live check also exposed an import throughput limit: the collector originally
accepted only eight segments per ten-second maintenance pass. With one-second
rotation, decoded segments accumulated in its inbox even though decoding kept
up. The collector now accepts up to 32 segments per pass, allowing steady
collection plus bounded backlog recovery. Existing queued captures import
normally without replaying blocks. A regression test covers ten new segments
plus ten accumulated during a collector restart.
