# Block profile explorer

A local collector and read-only explorer for `zakurad` block timelines. The homepage shows the latest ten accepted blocks, twenty slow outliers from the last 24 hours, and failed or unfinished attempts. Select a process run and verification mode before comparing timings.

```sh
cargo build --locked -p zakura-profile-explorer
mkdir -p /tmp/block-profiles
target/debug/zakura-profile-explorer collect --store /tmp/block-profiles --socket /tmp/block-profiles/node.sock
# Another terminal:
target/debug/zakura-profile-explorer serve --store /tmp/block-profiles
```

Open `http://127.0.0.1:8787`. Set the node’s `block_profile.socket` field to `"/tmp/block-profiles/node.sock"` in its TOML configuration, or run the synthetic `zakura-jsonl-trace` example. The collector must own a private store and socket directory. The web server binds to localhost. Use SSH forwarding for a remote viewer.

A block detail page shows overlapping elapsed spans, their evidence completeness, and JSON/Perfetto exports. Retained timestamped CPU captures add a process flamegraph. It includes other blocks and background work and never claims exclusive CPU ownership from elapsed intervals.

The collector defaults to a 100 GB byte budget. It retains compressed chunks and indexed summaries, prioritizes recent outliers during pruning, and records detail expiry separately from summaries. A separate filesystem quota is required for a hard limit covering samples, temporary files, and logs. The deployment runbook supplies Linux services, CPU capture, daily reports, and the quota setup.

```sh
cargo test --locked -p zakura-jsonl-trace -p zakura-profile-explorer
cargo build --locked -p zakura-jsonl-trace --example block_profile
python3 scripts/block-profile-smoke.py
```

The smoke test checks real datagram transport, latest/outlier queries, detail after the caller response, exports, collector crash recovery, and socket ownership using synthetic blocks. Linux overhead, real perf output, and near-capacity retention need a dedicated canary before broader use.
