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
