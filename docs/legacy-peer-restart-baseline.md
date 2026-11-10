# Legacy peer restart diagnostics

Set `network.p2p_stack = "legacy"` and `network.zakura.trace_dir` to enable
`legacy_peers.jsonl`. This records candidates from seeds and the disk cache,
startup attempt positions, outbound TCP/handshake outcomes, completed connections,
connection lifetimes, measured RTTs, cache eligibility and membership, and
single-block inventory announcements. It does not change selection or persistence.

The existing `legacy_sync.jsonl` and state traces record block download and
processing. Join block events by hash. Events share a process identifier and a
monotonic microsecond clock; `network_start.wall_time_ms` anchors each process to
wall time for comparisons across restarts and nodes. Queue saturation can drop
trace events; inspect the JSONL tracer's dropped-event metrics before interpreting
missing events as missing activity.

Peer and IP identifiers are stable pseudonyms by default, not anonymization.
`network.expose_peer_addresses = true` exposes endpoints, allowing joins with
existing download diagnostics and RPC snapshots. Keep those artifacts private.
An IP match after restart is only suggestive of a returning inbound peer: multiple
nodes can share an IP, and inbound source ports can change.

Dial cancellation includes outer timeouts and shutdown; it does not prove a remote
refusal. Connection teardown events do not yet identify the precise disconnect
cause. A cache snapshot describes the file read after an update, including an old
file retained when no peers are eligible. Candidate enumeration is unordered on
main; `startup_dial.position` records actual scheduled order. Announcement times
are observations at the inventory handler, not packet timestamps or miner times.

`scripts/peer_restart_baseline.py` runs on a development worker with
`zakura-state` and a task-owned `CARGO_TARGET_DIR`. It checks and builds the node,
creates two private pruned snapshot copies, and uses independent caches and
identities. After both nodes have advanced and agree near their estimated tips,
it records 30 minutes, restarts only the experiment node with the same binary,
then records another 30 minutes. Both processes stop on completion or failure.
It never attaches to existing node processes or refreshes snapshot masters.

RPC snapshots and metrics are sampled every ten seconds; event traces provide
finer timing. Compare connected counts and RTT distributions by direction,
pre-restart peer retention, cached/selected/attempted/connected endpoints, and
first announcement/download/commit times by block hash. The reference node stays
running throughout, but shares host resources: this is a restart diagnostic,
not an isolated throughput benchmark. Inbound conclusions require observed
inbound connections; listening on a public address alone does not prove reachability.

The run writes private artifacts under the managed workspace's
`unpublished/legacy-restart-baseline/`, including the exact source SHA, snapshot
provenance, restart marker, samples, traces, and per-process logs. It stops on
validation failure or if tip acquisition exceeds two hours. No numeric RTT is
added to the peer cache, and no RTT preference change is included.
