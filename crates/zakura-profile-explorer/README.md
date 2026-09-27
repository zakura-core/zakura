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


### Completion recovery

The node exporter retains the latest 512 Finish/Seal events in memory and replays
one every 50ms while the collector is reachable. Replays have fresh sequence IDs
and update the same attempt without duplicating spans or changing its measured
end time. This repairs transient transport loss and collector restarts, including
messages received before a collector transaction was saved. The cache lives only
in the exporter. Block verification never waits for collection. Recovery remains
bounded: node restarts, producer queue overflow, and events evicted during a long
outage can still leave an incomplete profile. Detailed spans are not replayed.

A missing result is shown as an incomplete profile, not proof of a failed block.
After independently checking an entry, an operator can remove it from the home
page with `zakura-profile-explorer dismiss --store PATH --run RUN --attempt ID
--reason REASON`. The command retains the raw profile and review note, and never
invents a completion time. It is not exposed through the public HTTP service.
