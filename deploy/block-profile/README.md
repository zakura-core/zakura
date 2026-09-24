# Block profile explorer

This is an opt-in recorder, a local collector, and a read-only explorer. The homepage shows the latest ten accepted blocks and the twenty slowest successful requests of at least 120 ms from the last 24 hours. The user-facing home view automatically follows the newest recording run using full block verification (the semantic route). It has no session or route selectors and re-resolves the current run on each refresh, including after a node restart. Operators can still select other runs and routes through the read-only API and CLI reports. Each process run and verification route remains separate in stored data. Session lists and latency percentiles use only the newest request per block hash within that session and route. Search shows only the newest recording per block hash across sessions and routes, with the request counter breaking ties within a run. Different block hashes at the same height remain separate. Errors and unfinished requests have their own list. Older recordings remain available through the recording-specific JSON and trace APIs until normal pruning. Public block pages always resolve the latest recording.

The home page starts with the latest blocks and search, with slow blocks and failed requests below. Request counts, completeness, and storage totals remain available in the daily JSON report linked from the footer. Aggregate producer-drop and transport-loss counters stay in JSON reports instead of a homepage banner. Individual profiles still label omitted detail, missing spans, and known measurement interference. Search opens a separate breakdown page at `/block/HEIGHT`, which resolves the newest retained recording whenever opened or refreshed. Hash searches and fork choices use `/block/FULL_BLOCK_HASH` to identify the exact block. Block links use normal navigation, so browser Back, refresh, and opening in another tab work. Old `/block/RUN_ID/REQUEST_ID` and hash-fragment links resolve the same block to its newest retained recording and replace the browser URL. A height with multiple block hashes opens a choice page. Empty searches and missing recordings keep the search form available. The breakdown page keeps block metadata and downloads collapsed under “Block details & downloads”. Expanded metadata has labeled fields for the hash, result, timing coverage, network, storage mode, build, profile ID, and timestamp with its local time zone. Perfetto and profile JSON downloads appear as buttons in that section. When updating a public trial, install the matching `public-nginx.conf` so the new read-only page routes are reachable.

The block header shows total recorded time from router entry through the last retained span, alongside verifier response time and work after that response. These are elapsed intervals, not sums of overlapping spans or CPU time. Missing or expired spans can understate the total. The internal attempt number is a process-local counter of verification requests, not a retry count for a block.

Magnifying glass links beside the block heading and transaction rows open CipherScan in a new tab. They use the exact block hash and mined transaction ID, with separate Mainnet and Testnet hosts. Other network names have no links. Transaction links require a retained `transaction_hash` on the transaction envelope, so older profiles still have block links but no transaction links. The explorer does not fetch data from CipherScan in the background.

The timeline answers where elapsed time went. When live CPU capture covers a block, its CPU profile button opens a dedicated full-page interactive viewer pinned to that exact request. New sampling does not fill in historical blocks. The default interval includes finalization after the verifier response. Flame widths represent sample counts, not elapsed milliseconds. Actual timestamps and thread IDs remain in the CPU JSON. Samples show user-space process activity during the block, including any concurrent background work. Waiting and kernel execution are outside this measurement. Parallel spans overlap, and neither view claims an exact allocation of CPU milliseconds to one block. See [the live CPU design](../../docs/designs/block-profile-cpu.md) for capture and coverage guarantees.

## Build and local check

```sh
cargo build --release --locked -p zakura -p zakura-profile-explorer
cargo test --locked -p zakura-jsonl-trace -p zakura-profile-explorer
node --test crates/zakura-profile-explorer/web/*.test.cjs
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

Each restart creates a new run ID. Metadata includes network, node/session label, the main base commit, the exact instrumented commit, node build, PID, storage settings, and UTC/monotonic clock anchors. The block heading links to the main base. The full instrumented build stays in collapsed details. This identifies the code included in that recording, not whichever revision main points to when the page is opened. Unknown historical provenance is labeled “Not recorded”. The sampler also records the executable SHA-256 and perf build IDs. Public block and transaction hashes are retained. No transaction payloads, peer addresses, credentials, or arbitrary node logs enter the profile schema.

## Updating the main base

Keep the profiling additions on `adam/block-profile-operations`. From its clean local checkout, run:

```sh
python3 deploy/block-profile/update.py --config deploy/block-profile/update.example.json
```

The example config names the dedicated profiling host. Copy it to an operator location for another installation and set its SSH alias, expected hostname, repository, and shared Cargo target directory. This requires an authenticated Git remote, SSH access as root, local Node.js for the frontend tests, and the existing Linux profiler installation with Rust and Python 3.11+. It does not create infrastructure, change DNS, touch chain state directly, or enable CPU sampling when it was stopped. An already running sampler is drained before executable replacement and restarted against the new supervised node PID after verification.

The command fetches main and the profiling branch, includes other operators' changes by fast-forward, merges the selected main commit, and pushes normally. It never rebases or force-pushes. A merge conflict aborts the new merge and stops before deployment. Use `--base MAIN_SHA` to select a particular main revision, `--keep-base` to deploy only profiling changes, or `--prepare-only` to merge and push without deploying. A branch that already includes newer main code cannot be downgraded with this command.

A persistent systemd job builds the exact pushed revision on the dedicated host with four build jobs, a four-core CPU limit, and a 20 GiB memory limit. The current services continue running during tests and compilation. Only after success does it stop the node, drain the collector, install both executables, and start the collector, web service, and node. It verifies the new run's main and instrumented commits through the local API. New blocks may take longer to arrive, and normal startup timing exclusions still apply. The command leaves the proxy, quotas, service configuration, and lifecycle timers alone.

Build-time provenance is `git merge-base --all HEAD refs/remotes/origin/main` plus the full `HEAD` SHA. Builds without a single verifiable base omit provenance. Cargo tracks the Git references, including shared references in worktrees. The updater fetches main before building and checks that the computed base matches the requested base. Historical clean builds with a resolvable Git revision in the profiling history are backfilled from their own merge base, never from the new deployment's base.

Logs, previous executables, and `result.json` are retained under `/var/lib/zakura-profile-updates/INSTRUMENTED_SHA/`. Check `systemctl status zakura-profile-update` after an interrupted SSH connection. The remote job continues independently. A build failure leaves the running services untouched. A failure during activation is reported with its stage and requires inspection. There is no automatic binary downgrade because newer main code may have changed the chain database format.

For a separately verified legacy recording, provenance can also be attached explicitly. Existing provenance cannot be changed, and the original build string must match:

```sh
sudo -u zakura-profile zakura-profile-explorer source \
  --store /srv/zakura-profile/data --run RUN_ID --expected-build 'EXACT_RECORDED_BUILD' \
  --base-commit VERIFIED_MAIN_SHA --commit VERIFIED_INSTRUMENTED_SHA
```

## Timing and coverage

The root interval starts at the block verifier router's `call` and ends at its returned result. Caller readiness, the outer router buffer, body download, and network admission are outside this interval. Compare it with existing metrics only when their boundaries match. Hashing and any work without a span remain uncovered elapsed time.

Semantic profiles include known-block lookup, block checks, the transaction envelope, bounded transaction/UTXO/Sapling/Halo2 request detail, state readiness/response, writer queue, contextual state phases, publication, and finalization. The writer queue begins when the state service dispatches to the writer. Earlier parent waits remain inside the larger state-response envelope. Finalization may extend beyond the root response. Checkpoint mode currently records the root interval, not every checkpoint worker stage.

New semantic recordings split finalization into in-memory chain/fork updates, finalized-block preparation, output indexes, parallel spent-output reads and transaction serialization, address reads, database batch preparation, pruning, commit, and state publication. Batch detail separates block/transaction data, nullifiers, trees, transparent indexes, and value pools. The RocksDB write call is nested inside commit. These spans describe older blocks being finalized by the current writer request, not a second verification of the displayed block. Parent rows include child time, and parallel rows overlap. Older recordings keep their original coarse timing. Upgrade the collector before the node so it understands the additional stage names.

The node holds at most 1,024 active attempt contexts, 131,072 detail events, and 2,048 summary events. Each attempt admits up to 65,536 spans, reserving 128 slots for state phases after transaction/worker detail. This replaces the original 128-span transaction limit. The fixed event queues use less than 64 MiB. Producers use nonblocking queues and never serialize or write files. An exporter thread sends bounded Unix datagrams. A missing collector loses evidence without delaying verification. The larger budget aims to preserve full detail for ordinary blocks, while an extreme block or collection overload can still lose detail and is labeled accordingly. Profile retention continues to use the same 100 GB quota and pruning policy.

New transaction spans carry their zero-based position in the block and their parent span. The transaction envelope also records its already-computed mined transaction ID for explorer links. The explorer labels these as Transaction 1, Transaction 2, and so on, using block order. Expanded transaction lists default to transaction number. A sort control switches to longest elapsed time first without changing labels or closing expanded rows. Sorting uses the same retained interval shown by each row, with transaction number breaking ties. Each row expands into its own input waits, checks, proof requests, and worker timings. The browser creates check rows only when expanded. Older profiles have no transaction association, so their transaction numbers use recorded start order and unassigned checks stay separate instead of guessing ownership from overlapping times. Omitted historical spans require a replay to recover. Upgrade the collector before the node so it accepts the larger span IDs and preserves transaction indexes and hashes.

Root completion is self-contained and separate from detail. A seal is emitted when the last worker/context owner finishes. The collector compares the seal's expected count with retained spans. Queue drops, transport gaps, interrupted attempts, truncation, expired chunks, and collector failures stay visible. A context handoff that cannot acquire the existing transaction registry immediately is omitted and counted as run-level loss.

The background exporter retries temporary socket-buffer pressure and interrupted sends every 2 ms for up to one second per datagram. It preserves the payload and sequence during retries, while verification continues to use nonblocking bounded queues. Shutdown interrupts retries. Permanent errors, deadline expiry, or a prolonged collection outage can still lose detail and remain visible in the counters. This prevents ordinary bursts from being discarded immediately when the collector is busy committing a batch. Historical missing spans require a new profiling run to recover.

## Startup readiness

The dedicated profiling node preloads Sapling parameters, the Orchard verifier for its starting chain tip, and the Sprout verifying key before accepting verification work. Reporting stays in startup until fresh sync checks confirm catch-up has finished and no profiled block work has run for 60 seconds. Retained attempt contexts include finalization after the caller response. A missed sync observation for more than 30 seconds or failed sync check restarts the settling period. Legacy sync requires a drained round with no discovered blocks. Native sync additionally checks the committed tip against the header tip. Both paths request headers after the tip's parent and require a peer to return exactly our current tip. This gives a positive confirmation even when peers suppress empty replies. The query has a 10-second timeout.

Readiness is recorded once per process run. Later slow blocks, queue delays, and sync interruptions remain eligible. The boundary is repeated in health frames so collector restarts and lost datagrams cannot turn startup into valid timing. Blocks are classified by router-entry time, including requests that finish after readiness. Before the boundary arrives, every block in a gated run is conservatively classified as startup.

Startup profiles remain in latest blocks and search with a Startup label. Their raw timings and downloads stay intact, but they are excluded from slow blocks and daily latency statistics. Historical recordings without readiness metadata keep their prior behavior. After checking operator logs, backfill a proven boundary for an old run with:

```sh
sudo -u zakura-profile zakura-profile-explorer startup-boundary \
  --store /srv/zakura-profile/data --run RUN_ID --ready-us MICROSECONDS_FROM_RUN_START
```

The boundary cannot be moved once set. Upgrade and restart the collector before the web service and node. This installs the additive catalog table before readers need it. Nodes without profiling enabled do not preload keys or perform extra readiness checks.

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

Start the dedicated node with the configured socket after updating its supplementary group membership. CPU capture is opt-in. After a bounded canary verifies symbol quality, decoding throughput, resource use and collection loss, enable continuous sampling on the dedicated profiler:

```sh
sudo systemctl enable --now zakura-profile-sampler.service
```

The sampler resolves the exact supervised `zakurad` PID, verifies its executable and process start identity, and matches a fresh profiling run. It verifies the exact systemd control group against `/proc/PID/cgroup` and uses per-CPU events filtered to that group, with an additional exact PID filter during decoding. This avoids an exited-thread polling storm in the host's perf version while including newly created workers. It rotates the continuously running recorder about every ten seconds and decodes closed segments in a separate process. A node restart starts a new capture session automatically. No PID-name matching is used. Stop and disable the sampler to return to timeline-only collection.

For a bounded operator canary, invoke `sample.py --store STORE --pid PID --executable EXACT_PATH --frequency 99 --duration-seconds 120` in the same restricted service environment. A zero duration means continuous collection. The service no longer uses the old fixed-PID environment file or a one-day expiry. Capabilities remain restricted to the sampler unit. An unsupported kernel or denied perf access leaves timelines available. Decoding keeps symbolized stack frames without expanding compiler-inlined calls, avoiding expensive `addr2line` expansion.

The raw spool, symbol cache, import inbox, and decoded captures stay under the profiler's filesystem quota. Backlog and query limits are explicit in CPU coverage. Short blocks can have few or zero statistical samples. New profiles normally become available after rotation, decoding, and the collector's next import pass. Downloads preserve full symbols and exact monotonic sample timestamps, even though the interactive flame graph uses sample counts.

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

Transaction detail starts collapsed in each block timeline. Expand Transactions, then an individual transaction to inspect its checks, or expand Finalization to inspect its nested elapsed-time breakdown. Public links resolve the newest retained profile for the block.

The experimental shielded verification breakdown has been withdrawn. New recordings use the previous stage instrumentation. The collector retains read compatibility for already stored experimental spans, and the restored timeline hides those additional rows. Historical profile downloads remain intact.
