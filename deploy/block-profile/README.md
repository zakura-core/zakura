# Block profile explorer

This is an opt-in recorder, a local collector, and a read-only explorer. The homepage shows the latest ten accepted blocks and the twenty slowest successful requests of at least 500 ms from the last 24 hours. The user-facing home view automatically follows the newest recording run using full block verification (the semantic route). It has no session or route selectors and re-resolves the current run on each refresh, including after a node restart. Operators can still select other runs and routes through the read-only API and CLI reports. Each process run and verification route remains separate in stored data. Session lists and latency percentiles use only the newest request per block hash within that session and route. Search shows only the newest recording per block hash across sessions and routes, with the request counter breaking ties within a run. Different block hashes at the same height remain separate. Errors and unfinished requests have their own list. Older recordings remain available through saved links and raw exports until normal pruning.

The home page starts with the latest blocks and search, with slow blocks and failed requests below. Request counts, completeness, and storage totals remain available in the daily JSON report linked from the footer. Aggregate producer-drop and transport-loss counters stay in JSON reports instead of a homepage banner. Individual profiles still label omitted detail, missing spans, and known measurement interference. Search opens a separate breakdown page, with one stable `/block/RUN_ID/REQUEST_ID` URL per recording. Block links use normal navigation, so browser Back, refresh, and opening in another tab work. Old hash links redirect to the corresponding page. A height with multiple block hashes opens a choice page. Empty searches and missing recordings keep the search form available. The breakdown page keeps block metadata and downloads collapsed under “Block details & downloads”. Expanded metadata has labeled fields for the hash, result, timing coverage, network, storage mode, build, profile ID, and timestamp with its local time zone. Perfetto and profile JSON downloads appear as buttons in that section. When updating a public trial, install the matching `public-nginx.conf` so the new read-only page routes are reachable.

The block header shows total recorded time from router entry through the last retained span, alongside verifier response time and work after that response. These are elapsed intervals, not sums of overlapping spans or CPU time. Missing or expired spans can understate the total. The internal attempt number is a process-local counter of verification requests, not a retry count for a block.

The timeline answers where elapsed time went. Linux CPU sampling adds a zoomable flamegraph of process activity during that interval. Parallel spans overlap. Shared batches and other blocks can appear in process samples, so neither view claims an exact allocation of CPU milliseconds to one block.

## Build and local check

```sh
cargo build --release --locked -p zakura -p zakura-profile-explorer
cargo test --locked -p zakura-jsonl-trace -p zakura-profile-explorer
python3 -m unittest discover -s deploy/block-profile -p 'test_*.py'
```

The frontend is bundled JavaScript and CSS. It needs no external assets, npm install, or public service. For a synthetic integration check, start the collector and explorer, then run the recorder example.

```sh
mkdir -p /tmp/block-profiles
target/release/zakura-profile-explorer collect \
  --store /tmp/block-profiles --socket /tmp/block-profiles/node.sock
# In another terminal:
target/release/zakura-profile-explorer serve --store /tmp/block-profiles
cargo run --locked -p zakura-jsonl-trace --example block_profile -- /tmp/block-profiles/node.sock
```

Open `http://127.0.0.1:8787`. The example is labelled synthetic. Its measurements exercise the transport and UI and say nothing about real block performance.

## Node configuration

Add this to the dedicated profiling node's configuration. An omitted socket disables recording. Keep the node's ordinary network, P2P, and consensus settings.

```toml
[block_profile]
socket = "/run/zakura-profile/node.sock"
node = "mainnet-profile-01"
session = "2026-09-22-canary"

[state]
storage_mode = "pruned"
```

Use a fresh or compatible pruned database. Do not restore a benchmark snapshot over a serving node. Archive mode is useful for archive-specific work, but retained profiles do not require historical block bodies. Never switch a pruned database back to archive.

Each restart creates a new run ID. Metadata includes network, node/session label, node build, PID, storage settings, and UTC/monotonic clock anchors. The sampler also records the executable SHA-256 and perf build IDs. No transactions, peer addresses, credentials, or arbitrary node logs enter the profile schema.

## Timing and coverage

The root interval starts at the block verifier router's `call` and ends at its returned result. Caller readiness, the outer router buffer, body download, and network admission are outside this interval. Compare it with existing metrics only when their boundaries match. Hashing and any work without a span remain uncovered elapsed time.

Semantic profiles include known-block lookup, block checks, the transaction envelope, bounded transaction/UTXO/Sapling/Halo2 request detail, state readiness/response, writer queue, contextual state phases, publication, and finalization. The writer queue begins when the state service dispatches to the writer. Earlier parent waits remain inside the larger state-response envelope. Finalization may extend beyond the root response. Checkpoint mode currently records the root interval, not every checkpoint worker stage.

New semantic recordings split finalization into in-memory chain/fork updates, finalized-block preparation, output indexes, parallel spent-output reads and transaction serialization, address reads, database batch preparation, pruning, commit, and state publication. Batch detail separates block/transaction data, nullifiers, trees, transparent indexes, and value pools. The RocksDB write call is nested inside commit. These spans describe older blocks being finalized by the current writer request, not a second verification of the displayed block. Parent rows include child time, and parallel rows overlap. Older recordings keep their original coarse timing. Upgrade the collector before the node so it understands the additional stage names.

The node holds at most 1,024 active attempt contexts, 16,384 detail events, and 2,048 summary events. Each attempt admits at most 256 spans, with transaction/worker detail limited to 128 so it cannot consume the entire phase budget. Producers use nonblocking queues and never serialize or write files. An exporter thread sends bounded Unix datagrams. A missing collector loses evidence without delaying verification.

Root completion is self-contained and separate from detail. A seal is emitted when the last worker/context owner finishes. The collector compares the seal's expected count with retained spans. Queue drops, transport gaps, interrupted attempts, truncation, expired chunks, and collector failures stay visible. A context handoff that cannot acquire the existing transaction registry immediately is omitted and counted as run-level loss.

## Known measurement interference

Do not reject recordings just because they are slow. When operator logs prove a capture was contaminated, such as a deliberate process pause, annotate the exact recording locally with a reason. The collector must have opened the store with this version first to install the additive catalog table.

```sh
sudo -u zakura-profile zakura-profile-explorer exclude \
  --store /srv/zakura-profile/data --run RUN_ID --attempt REQUEST_ID \
  --reason "Operator paused the node during this recording"
```

This command can run while collection continues. It preserves every measured interval. Excluded recordings stay findable with an explicit warning and raw times, but never enter slowest-block rankings or latency percentiles. A newer excluded recording does not cause an older measurement to reappear. Capture counts still describe raw requests, while daily JSON reports expose `timing_blocks` and `excluded_timings` separately. Profile JSON and Perfetto exports carry the reason. Annotations are pruned with their summaries. The public explorer remains read-only.

## Storage

All profiler data belongs under `/srv/zakura-profile/data` on a separate filesystem. Chain state and the operating system are outside the 100 GB profile budget. The supplied installer requires ext4 mounted with group quotas. It never formats a disk. Set up the new profiler volume deliberately, mount it with `grpquota`, initialize its quota files, and enable group quota enforcement before installation.

The `zakura-profile` group has a hard quota of 97,656,250 KiB, exactly 100,000,000,000 bytes. The data directory uses that group and the setgid bit. This covers collector data, SQLite and WAL, raw captures, import files, reports, symbols, and profiler file logs. Confirm the quota is actually on and perform a disposable quota-exhaustion test before a real session. The application byte budget alone is not a hard filesystem limit.

Pruning starts at 85% and targets 75%. Optional detail stops at 90%. CPU captures have a 20% retained allocation, while the sampler keeps raw captures below 8 GB with space reserved for its active files. Raw stacks are private and excluded from default exports. Each raw/decoded file has a 256 MiB limit. Daily reports retain at most 90 files. Summary retention targets two million attempts and prunes in bounded batches, with additional pruning under disk pressure. These are byte/resource limits, not promised retention days.

The collector drains at most 256 queued datagrams into each SQLite transaction, sharing one durable disk sync across that bounded batch. Invalid events roll back independently. It writes compressed chunks through a temporary file, fsyncs and renames, then commits their SQLite references. Deletion marks a chunk, removes the payload, and then removes its catalog references. Restart recovery removes orphaned files and completes interrupted deletion. Payload sizes record allocated filesystem blocks in the catalog, so routine pruning does not scan every retained payload. Readers have bounded queries and decompression, and report expiry if a chunk disappears during a read. WAL checkpointing is periodic. A failed or full profiler store can stop the collector without stopping the node.

The initial collector uses one indexed SQLite catalog with bounded summary retention. Hourly summary shards, long-lived rollups, and manual pinning from the broader design are not enabled in this first implementation. Do not promise full 24-hour history at catch-up rates until the canary establishes the observed retention window.

## Linux installation

On the dedicated host, install `perf` for the running kernel, Python 3.11 or later, and the quota tools. Ubuntu minimal images may also need `linux-modules-extra-$(uname -r)` for `quota_v2`. Verify quota enforcement before installing the services. Use the normal deployment build and preserve debugging information for useful Rust symbols. The profiling process never changes `perf_event_paranoid`.

```sh
sudo deploy/block-profile/install.sh target/release/zakura-profile-explorer zakura
sudo systemctl start zakura-profile-collector.service zakura-profile-web.service
sudo systemctl enable --now zakura-profile-report.timer
```

Start the dedicated node with the configured socket after updating its supplementary group membership. Set `/etc/zakura-profile-sampler.env` to the actual supervised node PID, its exact executable path, and a session limit of at most 86,400 seconds.

```text
NODE_PID=12345
NODE_EXECUTABLE=/usr/local/bin/zakurad
SESSION_SECONDS=86400
```

Then start `zakura-profile-sampler.service`. The sampler verifies `/proc/PID/exe`, process start ticks, and the fresh recorder run before capture. A node restart requires a refreshed PID and a new sampler invocation. No PID-name matching is used. Capabilities are restricted to the sampler unit. An unsupported kernel or denied perf access stops sampling and leaves timelines available. Decoding keeps symbolized stack frames without expanding compiler-inlined calls, which avoids unbounded `addr2line` memory use.

All profiler services share a one-core CPU cap and a 1 GiB memory cap through `zakura-profile.slice`. The collector and viewer each have a 512 MiB cap. Kernel sample buffers and node recorder memory are additional and must be measured in the canary. Each HTTP query uses a read-only connection with a four-second SQLite work deadline and at most two concurrent readers.

The explorer binds to IPv4 localhost and requires a localhost Host header. Reach it through SSH forwarding.

```sh
ssh -N -L 8787:127.0.0.1:8787 operator@PROFILE_HOST
```

The web API has no capture, delete, or pin controls. Reports are stored locally and never sent to Slack or email.

### Optional public trial

When the operator explicitly requests public access, `public-nginx.conf` exposes the read-only explorer on HTTP port 80 of a dedicated trial host. Anyone with its address can view and download retained profiles, including node/build metadata and decoded CPU stacks. This configuration has no authentication or TLS. Keep SSH forwarding for private sessions, and configure a domain and TLS for a longer-lived public service.

Install Ubuntu's `nginx` package, then use `public-nginx.conf` as `/etc/nginx/nginx.conf` only on the dedicated host. Install `public-nginx.service.conf` as `/etc/systemd/system/nginx.service.d/profile.conf`, run `nginx -t`, reload systemd, and start nginx. Keep the explorer on localhost. The proxy permits only its known GET/HEAD endpoints, rewrites the upstream Host header, and bounds requests, connections, and resources. It cannot serve the profile filesystem, node RPC, or metrics. Stop nginx at the beginning of the session's orderly shutdown.

## DigitalOcean sessions

Use a dedicated pruned-node image and a retained volume named `zakura-profile-store-*`. The existing PR-node reaper only deletes detached volumes with the `zakura-pr-` prefix, so the profile volume must never use that prefix. Keep the profile volume separate from the node's chain state.

The controller requires authenticated `doctl`, the approved image ID, retained volume ID, and SSH key fingerprint. Defaults follow the existing benchmark's `c-16` size and `nyc1` region. Confirm image disk requirements and cost for the actual account before creating the canary.

```sh
python3 deploy/block-profile/session.py create \
  --image APPROVED_PRUNED_IMAGE_ID --volume-id RETAINED_PROFILE_VOLUME_ID \
  --ssh-key APPROVED_KEY_FINGERPRINT --name zakura-profile-canary --hours 24
```

The image must contain the dedicated node setup and the profiler volume's persistent mount configuration, or an operator must complete installation before starting the session. This controller does not clone or rewrite an existing serving database.

Run `session.py reap` hourly on an authenticated operator/controller host with verified SSH host keys. The expiry tag is a deadline, not a DigitalOcean TTL. Without that external controller the droplet continues to incur charges after expiry. Do not start an unattended campaign without configuring and testing the reaper.

For an orderly stop:

```sh
python3 deploy/block-profile/session.py stop \
  --droplet-id EXACT_SESSION_DROPLET_ID --volume-id RETAINED_PROFILE_VOLUME_ID
```

The stop path verifies resource identity and ownership, stops sampling and the dedicated node, seals/checkpoints the collector, writes a final report, unmounts the profile filesystem, detaches and verifies the retained volume, and deletes only the disposable droplet. Failed SSH, flush, unmount, or detach leaves the droplet intact for inspection. There is no volume deletion command. Later sessions can attach the same volume and reopen retained profiles without the original blockchain database.

## Daily benchmarks

The report timer summarizes actual recorded sessions. For repeatable historical semantic benchmarks, `campaign.py` dispatches the existing `zakura-perf-bench.yml` workflow once per UTC date during a campaign lasting at most seven dates. Run it daily from the authenticated operator host with a persistent state file. The example campaign is intentionally expired outside its named date.

```sh
python3 deploy/block-profile/campaign.py \
  --config /etc/zakura-profile-campaign.json \
  --state /var/lib/zakura-profile-controller/campaign.json
```

The controller reserves the date before dispatch. If dispatch fails or its result is uncertain, inspect GitHub before clearing that reservation. It does not replace the weekly schedule. Existing benchmark outputs remain in the existing GitHub artifact retention policy and are outside the persistent installation's 100 GB quota. This is still the existing network-fed historical corpus, not a deterministic replay of a recent slow block.

## Canary acceptance

Before broader use, run one disposable pruned Linux node and record its exact executable checksum, configuration, hardware, chain height, and observation times. Validate real perf output and symbol quality. Compare identical workloads with profiling off, timelines enabled, and CPU sampling enabled. Targets are at most 1% disabled-path throughput change, 2% timeline cost, 5% combined cost, and a p95 latency increase below the larger of 10 ms or 5%.

Exercise collector kill/restart, sampler failure, quota exhaustion, pruning during reads, node restart, and clean detach/reattach. Test near-capacity query latency and retention under catch-up load. Local unit tests and synthetic examples do not establish these performance or Linux deployment results. Leave the feature opt-in until the canary meets the targets.

Transaction detail starts collapsed in each block timeline. Click the Transactions row to inspect it, or click Finalization to expand its nested elapsed-time breakdown. A link to a specific attempt selects its recording session, so replay results remain distinct from earlier observations of the same block.
