# Block profile explorer

A local collector and read-only explorer for `zakurad` block timelines. The homepage shows the latest ten accepted blocks, twenty slow outliers from the last 24 hours, and failed or unfinished attempts. The active profiling run supplies the home page. Block pages show their main base commit for comparisons.

```sh
cargo build --locked -p zakura-profile-explorer
mkdir -p /tmp/block-profiles
target/debug/zakura-profile-explorer collect --store /tmp/block-profiles --socket /tmp/block-profiles/node.sock
# Another terminal:
target/debug/zakura-profile-explorer serve --store /tmp/block-profiles
```

Open `http://127.0.0.1:8787`. Set the node’s `block_profile.socket` field to `"/tmp/block-profiles/node.sock"` in its TOML configuration, or run the synthetic `zakura-jsonl-trace` example. The collector must own a private store and socket directory. The web server binds to localhost. Use SSH forwarding for a remote viewer.

A block detail page shows overlapping elapsed spans, their evidence completeness, and JSON/Perfetto exports. The expandable Shielded verification section distinguishes cached requests, preparation, waiting, combined proof and signature execution, and result delivery. Each shared batch appears once per block, with links from participating transaction requests. Its workload includes all members, including unprofiled requests. Shared batch durations are not attributed exclusively to a block or transaction, and do not extend the block's total recorded time.

All bars share one time axis relative to block entry. A batch already in progress can start at a negative offset. Submillisecond durations use microseconds. The last completed shielded requests help locate late work but do not establish a critical path. Older profiles explicitly lack the new instrumentation, and partial batch evidence is distinct from complete ordinary spans.

Continuous CPU sampling stays off. Explicitly retained timestamped CPU captures can add a process flamegraph through `?cpu=1`. It includes other blocks and background work and never claims exclusive CPU ownership from elapsed intervals.

The collector defaults to a 100 GB byte budget. It retains compressed chunks and indexed summaries, prioritizes recent outliers during pruning, and records detail expiry separately from summaries. A separate filesystem quota is required for a hard limit covering samples, temporary files, and logs. The deployment runbook supplies Linux services, CPU capture, daily reports, and the quota setup.

```sh
cargo test --locked -p zakura-jsonl-trace -p zakura-profile-explorer
cargo build --locked -p zakura-jsonl-trace --example block_profile
python3 scripts/block-profile-smoke.py
```

The smoke test checks real datagram transport, latest/outlier queries, detail after the caller response, exports, collector crash recovery, and socket ownership using synthetic blocks. Linux overhead, real perf output, and near-capacity retention need a dedicated canary before broader use.
