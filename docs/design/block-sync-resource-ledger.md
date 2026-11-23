# Block-sync resource ledger

> **Status:** conformance evidence for regulated block-sync version 3 (stream 6) under the
> [regulation specification](../specs/peer-message-regulation.md). The
> [GetBlocks test plan](property-testing-block-sync-infrastructure.md) lists the tests and
> probes this ledger cites.

## Principle

The block-sync reactor decides what to ask for. Its congestion control, its pool-leased request
bytes and its benefit-capped look-ahead size the work to the node's progress. Memory follows from
those requests. A peer that sends more than its live reservations owe, or a body that disagrees
with its own header, breaks the protocol, and the node disconnects it.

While the node's `[memory] budget` covers the work's demand, the node adds no cap below the
benefit signals: the link's congestion window and the commit rate. When the budget is smaller than
demand, the shared memory pool holds bulk requests, look-ahead, serving output and send windows
within it. That can cost a peer throughput, and it never faults or penalizes a peer. The reserved
`Consensus` consumer funds commit-window blocks (`height ≤ verified_tip + 401`), so commit progress
never waits behind bulk work.

## Node budget

Every term counts one physical allocation once, under one node-wide memory budget. `[memory]
budget` sets it, and its default `"auto"` reads the tightest of host RAM and the cgroup limit. The
node derives the P2P budget `B = (budget − base − sidecar) / ρ` at startup.
`[network.zakura] peer_memory_budget_bytes` is removed. `B` bounds every byte in this ledger.
`base` and `ρ` are measured peaks, not bounds, so the process's total RSS is a target.

`[network.zakura] max_connections` and each reactor's `peer_limits` are unset by default. Then 256
connections is a ceiling, and the `Sessions` consumer's share of the pool decides how many
connections and sessions the node funds. The `Sessions` floor funds 19 full connections on
Mainnet. The `Consensus` reserve holds the network's worst 401-block checkpoint window at
admission's charge per block, so the chain sets the smallest host that can run. With the draft
values in `memory/draft.rs` and `p2p_stack = "zakura"`, that host has about 12.8 GiB on Mainnet.
A smaller host refuses to start with default settings.

A pending handshake holds 128 KiB of windows (`CONTROL_RECEIVE_BYTES` and
`HANDSHAKE_SEND_WINDOW_BYTES`). Admission leases each connection's transport base `t` and its
reactors' bases from `Sessions`. Closed connections release their stream buffers and drain in a
separate allowance (ADM-13). Each request leases its receive credit above the base (API-9). A
request that ends short keeps its unspent credit leased until the peer can no longer spend it. A
send window grows from its base toward `min(2 × cwnd, serving_per_peer)` while output waits, and
the `Serving` consumer leases the growth (`PeerSubPool` per peer). The base send window holds one
maximal block-sync frame, so one block always fits. The base keeps no room free in the window:
stream priority (CTRL-40 in the `zakura-quic` spec) decides which stream gets the room that
acknowledgments free.

| Term | Pool consumer | Bound comes from |
| --- | --- | --- |
| QUIC receive buffers, base | `Sessions` (`t` plus each negotiated reactor's receive part) | The control share and each negotiated reactor's unsolicited receive bytes, including one maximal frame. An idle peer holds only this base. |
| QUIC receive buffers, requests | `BlockSync`, `Consensus` or `Tx` request lease | The node raises the window (API-9) before each request goes out. Reads move bytes onto the same lease (`CreditHold`, API-10). Unspent credit drains once the peer can no longer spend it. |
| QUIC send buffers | Base in `Sessions` (`t`), growth in `Serving` | A connection ticks every 50 ms only while its window is above the base or output waits. On each tick, a window with output waiting grows toward `min(2 × cwnd, serving_per_peer)`. After four idle ticks it shrinks to the base, and the shrunk lease drains once the connection accepts a byte. |
| Serving bookkeeping | `Sessions` (the block-sync session part) | `BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES`, 1 MiB per admitted session, funds 2,816 requests at 160 B each plus 144 KiB of setup. |
| State reads | Fixed (`state_reads`) | The state's read limit, which the plan derives from read concurrency: one largest response per serving `NODE_EXECUTION` slot, one legacy `getdata` response in `Dual` mode and one decoded block for RPC. The state's parser allowance adds to it. A read beyond the limit is refused. |
| Serving encoded output | `Serving` (`PeerSubPool` per peer) | The floor is one largest served response. A peer's cap is `serving_per_peer`: 96 MiB by default, and `[memory.cap]` can change it. `Serve` shrinks both grants to the encoded bytes at the response's ending. |
| Requester bookkeeping | Fixed | `MAX_BS_INFLIGHT_REQUESTS`: 32,768 ranges and at most 128 MiB of expected hashes. |
| Raw stream-6 frames awaiting the routine | The request's lease (`CreditHold`) | `BLOCK_SYNC_INBOUND_QUEUE_DEPTH` (8 frames). `Reservations::admit` disconnects a peer that sends more than it owes. |
| Look-ahead above the commit window | `BlockSync` | The benefit cap: the peers' BBR windows plus `2 × commit_rate × p99_range_fetch_time`, at most the in-flight cap plus `max_reorder_lookahead_bytes` or `[memory.cap] look_ahead`. |
| Commit-window bodies (`≤ verified_tip + 401`) | `Consensus` (reserved) | Reserved in full for the worst checkpoint window at admission's charge, `wire + max(4 × wire, decoded)` per block. The draft value is `401 × 5 × MAX_BLOCK_BYTES` (3.73 GiB) until a scan of the chain measures it. Each height holds at most one `Consensus` lease. It never waits on bulk. |
| Control state | Fixed | 8,192 supplier cooldowns. `Status` keeps the latest value. |

Startup checks every term against `B`, and the pool enforces the leases while the node runs. A
raised operator setting whose worst case exceeds `B` starts the node with one warning per setting.
While the node runs, it warns about an overrun at most once every 10 minutes.

## Serving

### Bookkeeping

Each admitted block-sync session leases its own `BLOCK_SYNC_SESSION_BOOKKEEPING_BYTES` (1 MiB) in
its session base. That funds `144 KiB + 2 × 2,816 × 160 B`. It replaces one fixed 512 MiB table, which capped the
node at 3,633 sessions. `compact_session_lifecycle_peak` measures the real retained
bookkeeping at 142 to 153 B per request, so 160 B is an upper bound. Each session funds its 2,816
advertised requests, and the `Sessions` share caps how many sessions are open at once.

### Storage work

Execution slots are 24 per node and 12 per peer. A slot is held until the blocking read and
encode actually return, including after the session is cancelled, so connection churn cannot
multiply running reads. Each job retains the blocks it read. These are decoded blocks from
the node's own consensus-valid state. One job holds `blocks_per_response` blocks (always 1):
at most about 53 MB decoded using the conservative pre-boxing bound, about 7 MB for sampled Mainnet
blocks (≤ 3.45× wire). The encoder serializes one block at a time and the handler drops each
frame right after encoding, so the transient copy is one block.

### Output

Unsent encoded responses are bounded at 64 MiB per peer (twice the send window) and 1.25 GB
per node (twice the 10 Gbps × 500 ms bandwidth-delay product). One response slot per 4 KiB
bounds the per-response bookkeeping (about 2.5 KiB each). That term becomes the binding one
only if an operator sets a tiny `max_response_bytes`; the ledger records it but no change was
needed.

## Requesting

### Requester bookkeeping

The node-wide request pool holds 32,768 live ranges. One session holds at most an eighth of
them, 4,096 (`SESSION_RESERVATIONS`), so two sessions take at most a quarter of the pool. Normal
sync stays below that: a default Zakura server advertises 2,816 in-flight requests, and the header
lead keeps missing heights within 4,000. Abandoned exchanges after GC or a reorg, or a larger
custom advertisement, can briefly reach it, and the session then waits for endings. A session also
stops issuing while 64 fully received ranges still owe an ending (`MAX_PENDING_ENDINGS`). Each
range keeps at most 128 expected hashes, so hashes total at most 128 MiB. Range maps release their
excess capacity after a peak (`shrink_after_peak`), so a burst does not pin its high-water
allocation.

### Raw frames

The transport precheck admits a stream-6 frame only while the connection's live reservations
still owe it: frames left plus one ending per range, and bytes left under each range's
response cap. A frame beyond that is `FrameRejection::AboveOwed`, which closes the connection.
`queued_frames_never_exceed_what_the_live_reservations_owe` and
`the_transport_queue_holds_only_what_the_live_ranges_owe` pin the rule. Before this change,
the queue could hold up to 4,096 frames per connection regardless of what the node asked for.

A range's response cap is `count × MAX_BLOCK_BYTES`, at most the peer's advertised
`max_response_bytes`. The node requests by estimated size, so a body larger than its estimate
is not a violation (`an_authorized_body_above_its_size_estimate_is_not_a_peer_violation`).
The owed bound alone therefore grows with the request window, by about 2 MB per live one-block
range, and reached the 4,096-frame transport depth (about 8 GB) for a large window. Pre-NU5
bodies cannot grow: their txids cover every byte and the Merkle check below rejects any change.
Post-NU5 transparent signatures are outside the txid, so a peer can pad a body up to its
range's cap, and the padding survives until consensus checks the auth-data commitment.

Stream 6 therefore also declares an inbound queue depth of 8 frames
(`BLOCK_SYNC_INBOUND_QUEUE_DEPTH`). A session holds at most those 8, the one the reader is
forwarding and the one the routine keeps while it waits for decode capacity. Its raw frames
therefore stay under 10 × 2 MB, about 20 MB, whatever the window. Each of those frames arrived
under a request's receive credit, so the request's lease counts it. Later frames wait in the 16 MiB QUIC stream receive
window the transport already budgets. The owed check still disconnects a peer that sends more
than it was asked for. `block_sync_queues_a_fixed_number_of_bodies_whatever_the_reservations_owe`
pins the depth with a precheck that owes an unlimited window.

The other candidate charged queued raw bytes against the request byte budget, with tolerance
for honest bodies above their estimate. It was not chosen. That budget is node-wide, so one
connection could still hold all 6 GiB of it. Its estimates may be untrusted size hints from
header sync, so a tight tolerance would disconnect conformant peers, and a loose one gives no
useful bound. A fixed depth needs neither. It slows a conformant peer only when the routine is
already the bottleneck, and the [queue depth measurements](#queue-depth) show no throughput
change at depths 2, 8 and 32. Depth 8 keeps headroom over the two slots regulation needs for a
response beside requests and control.

### Decoded bodies

A decoded block's heap is not a fixed multiple of its wire size. Boxing Sprout, Sapling,
and Orchard bundles reduces the measured payloads on a 64-bit target:

| Body | Wire | Decoded before | Decoded now |
| --- | --- | --- | --- |
| 26,646 small transparent V5 transactions | 2 MB | 56.3 MB | 9.4 MB (4.7×) |
| Junk body: 37,037 empty V1 transactions | 372 KB | 74.9 MB | 9.7 MB (26×) |

`size_of::<Transaction>()` drops from 2,008 B to 248 B. Ironwood remains inline, so
transparent and Ironwood-only transactions need no additional bundle allocation. The V5
example still exceeds `DESERIALIZED_MEM_FACTOR` = 4; the factor remains an average, not a
bound. The pre-boxing Mainnet samples decoded at up to 3.45×, about 2× in aggregate.

Two changes tie decoded memory to the blocks the node asked for:

1. The peer routine checks each authorized body's transactions against its header before the
   body enters the pipeline: the transaction Merkle root must equal `header.merkle_root` and no
   txid may repeat (CVE-2012-2459). A mismatch reports `InvalidBlock` and disconnects. Before
   this change a junk body reached the verifier and the record-only misbehavior left the peer
   connected. With the check, the decoded transactions are the real block's, up to post-NU5
   authorization data.
2. Admission charges `max(4 × wire, measured decoded bytes)` for the sequencer input, reorder
   buffer and apply window. The measured figure comes from the decoded-size attribution the
   pipeline already traces. The maximum never loosens the gate relative to the old estimate,
   so throughput on typical blocks is unchanged.

Resident block-sync memory is then bounded by

```text
B + R + L × W × (1 + r) + min(P, L) × J
```

- `B` = look-ahead budget (1.5 GiB)
- `R` = one in-window response beyond the gate (≤ 32 MiB)
- `L` = submission window (401), `W` = 2 MB maximum block, `r` = the chain's decoded ratio
- `J` = one transient junk decode per connection before its rejection (≤ about 80 MB)

At `r` ≈ 4 this is about 5.6 GB, of which about 3.2 GB is the decoded window. A deliberately
built, proof-of-work-paid chain under the conservative pre-boxing estimate at `r` ≈ 26
raises the window term to about 21 GB. The boxed V5 example at `r` ≈ 4.7 instead gives about
3.8 GB; admission still charges the measured size and stops requesting ahead. The check costs one txid pass per
body, about 11% of decode time on real blocks, on the per-peer routine tasks.

Above the last checkpoint the sequencer also gates new decoded submissions on
`BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES` (512 MiB). Past the cap, bodies stay raw until earlier
submissions finish, so in steady sync the window term there is 512 MiB plus about one block of
estimate error. Submissions detached by a failure or reset stay charged until their exact
completions, and they block the next submission like attached ones. Only an empty window admits
a body past the cap, so one oversized block still progresses. Repeated resets therefore can't
push the window past 512 MiB. While storage never completes the detached submissions,
submission stops once they fill the cap. That rules out the pre-boxing 21 GB case for steady
sync, because below the last checkpoint the window holds fixed Mainnet blocks of about 7 MB
decoded at most, about 2.8 GB in total.

The Merkle check departs from the Block section's SHOULD to reuse downstream validation
rather than add a duplicate ingress check. The downstream check runs only after the junk body
is decoded into the pipeline, and its misbehavior report does not disconnect. Common
requirement 2 needs the decoded size tied to the requested block before the body is retained,
which only an ingress check provides. Consensus still runs its own Merkle check.

A node-wide decoded-byte semaphore was rejected because it limits throughput. Moving the body's raw-backlog demotion into the routine was
also rejected: the routine's floor view can be stale, which would push re-decodes onto the
single sequencer task.

### Retry and cooldown state

Supplier retry deferrals share one cap: `MAX_SUPPLIER_RETRIES` = 8,192, including deferrals
for unavailable bodies (`sync.block.body.unavailable_cooldown_skipped` counts skips). The
sequencer's `BodyRetryQueue` has no explicit cap. Its episodes exist only for submitted blocks
that failed retryably; the sequencer applies in order, so they stay within the submission
window per branch, and a header generation change retires them.

The `body_unavailable` gauges keep hash and height labels because the header-chain spec
LC-AVAIL-02 requires them. Each alarm episode adds four series.

## Obligation audit

| Spec requirement | Enforcement | Evidence | Result |
| --- | --- | --- | --- |
| Capacity admission: an advertised limit must be fundable | Served-limit validation before reactor, service or endpoint start | `an_unfundable_config_fails_before_the_reactor_or_service_starts`, `an_unfundable_block_sync_config_fails_before_the_endpoint_binds`, `loaded_block_sync_session_slots_must_fund_one_request_each` | Closed |
| Common requirement 6: receive buffering bounded by what was requested | `Reservations::admit` owed bound and the stream-6 inbound depth | `queued_frames_never_exceed_what_the_live_reservations_owe`, `the_transport_queue_holds_only_what_the_live_ranges_owe`, `block_sync_queues_a_fixed_number_of_bodies_whatever_the_reservations_owe` | Closed |
| Frame and decode: every kind has boundary and allocation evidence | Message rows, bounded decoder | `stream_six_fixed_fixtures_cover_every_kind_and_rule` | Closed |
| Reservation: unsolicited or overrunning data disconnects | Block precheck and `claim_block` byte check | `an_unsolicited_or_overrunning_block_disconnects_before_claim` | Closed |
| Reservation: overlapping live ranges disconnect, adjacent ranges are legal | Session range admission | `adjacent_ranges_are_admitted_and_any_overlap_disconnects` | Closed |
| Reservation: a shortened Unavailable requeues the original range | Requester ending handling | `a_shortened_unavailable_requeues_the_whole_original_range` | Closed |
| Reservation: a local failure keeps the consumed claim | Claim precedes the decode permit | `a_local_failure_after_a_claim_keeps_the_reservation_consumed` | Closed |
| Verify: a body must match its authorizing header | Coinbase height and transaction Merkle checks in the routine | `authorized_block_with_another_coinbase_height_is_a_protocol_violation`, `a_body_whose_transactions_miss_its_header_disconnects_before_sequencing` | Closed |
| Cadence: Status cannot postpone a missing ending | Loop-top request liveness check | `a_status_flood_cannot_postpone_a_withheld_ending` | Closed |
| Serving stops at a noncontiguous or excess source block | `encode_response` contiguity check | `serving_stops_at_the_first_noncontiguous_or_excess_source_block` | Closed |
| Common requirement 4: maps return to baseline after churn | `remove_peer` cleanup | `reconnect_churn_returns_block_sync_maps_to_baseline` | Closed |
| Common requirement 2: decoded memory bounded | Merkle check plus measured decoded charge | Derivation above | Closed, with the post-NU5 padding caveat |
| Parameters to validate at maximum load | Probes | [Measurements](#measurements) | See below |

Mutation checks confirmed that 16 of 17 targeted mutations fail their test. The survivor drops
the `returned == max_blocks` check in `encode_response`; the response sink's frame cap refuses
the extra block and the ending is identical on the wire, so the check is redundant.

## Measurements

Release builds of base `e33dc4308` and this branch ran alternately on one host: AMD Ryzen AI 9
HX 370 (24 threads, Zen 5 and Zen 5c cores), 93 GiB RAM, Linux 7.2, loopback, no netem. Other
work shared the host, so each figure is a range over repeated runs. Every probe ran with
`cargo +1.98.0 nextest run --release -p zakura-network --features zakura-testkit
--run-ignored only` under a 32 GiB cgroup memory cap.

The candidate passed if each range overlapped the base range. A gap beyond the run-to-run
spread counted as a regression to investigate.

**Serving over native QUIC** (`regulated_getblocks_throughput_probe`, `ZAKURA_RTT_PROBE=1`,
`ZAKURA_PROBE_SECS=10`, 6 runs each). Body MB/s:

| Body | Blocks per request | Depth | Base | Branch |
| --- | --- | --- | --- | --- |
| 1,617 B | 1 | 64 | 158–170 | 157–183 |
| 1,617 B | 1 | 512 | 158–192 | 171–186 |
| 1,617 B | 1 | 2,816 (advertised) | 171–193 | 160–194 |
| 1,617 B | 16 | 2,816 | 355–389 | 353–373 |
| 73,079 B | 1 | 64 | 862–875 | 842–898 |
| 73,079 B | 1 | 512 | 844–882 | 840–860 |
| 73,079 B | 1 | 2,816 | 845–873 | 841–874 |

Loopback CPU limits these figures, not the protocol: each case used 32–47 CPU-seconds in
10 seconds.

**Serving at every default session slot** (`maximum_sessions_continuous_serving_probe`,
`ZAKURA_MAX_PEER_PROBE=1`, 512 sessions, 2 runs each):

| Shape | Base responses/s | Branch responses/s | RSS after admission |
| --- | --- | --- | --- |
| `useful` | 236,095–238,688 | 240,811–242,188 | about 190 MB on both |
| `unavailable` | 268,179–269,002 | 264,909–271,131 | about 189 MB on both |
| `stalled`, window 5,632 (twice the limit) | — | — | 353 MB on both |

Per-session completions stayed within 0.4% of each other in every run, so no session starved.
The stalled shape queues twice the advertised limit in all 512 sessions. Its 343 MB above
baseline fits the 512 sessions' bookkeeping, 1 MiB each.

**Requesting end to end** (`zakura_mock_blocksync_throughput`, `ZAKURA_MOCK_BS_RUN=1`, 4 seeds,
100,000 synthetic blocks of 1–16 transactions, 3 runs each, alternating): base 1,434–1,584
blocks/s, branch 1,474–1,565 blocks/s. This harness saturates about one core, so it is
sensitive to codegen in the peer routine's loop. An earlier revision traced the ending timeout
inside the per-iteration liveness check and ran at 940–1,060 blocks/s. Moving the trace to the
routine's exit restored parity.

**Routine bookkeeping** (`request_bookkeeping_scaling`, 5 runs each): both builds fill at
2.3–5.1 µs per request and settle at 8.8–17.0 µs per response, nearly flat from 1,000 to 16,000
requests.
The two modes in both builds track which core type ran the test.

In the mock harness, measured decoded bytes exceeded the 4× estimate on almost every admission
check. Synthetic transactions are tiny, so they decode at about 8× their wire size. The
look-ahead gate never closed, so the larger charge did not reduce throughput.

### Queue depth

The stream-6 inbound depth was measured on a second host: Apple M3 Ultra (28 cores),
96 GiB RAM, macOS, loopback, no netem, release builds with `cargo +1.98.0`. Other work shared
the host, so single runs dipped when it was busy. The same pass criterion applies: each
branch range must overlap the base range.

A sweep first ran the base build against one build whose stream-6 depth came from an
environment variable, at depths 2, 8 and 32, 4 rounds each, alternating. No depth was
consistently slower on any case below. Then base `c542425f6` and this branch (depth 8) ran 6
alternating rounds of each probe.

**Serving over native QUIC** (`regulated_getblocks_throughput_probe`, `ZAKURA_RTT_PROBE=1`,
`ZAKURA_PROBE_SECS=10`). Body MB/s:

| Body | Blocks per request | Depth | Base | Branch |
| --- | --- | --- | --- | --- |
| 1,617 B | 1 | 64 | 48–93 | 91–94 |
| 1,617 B | 1 | 512 | 104–108 | 105–110 |
| 1,617 B | 1 | 2,816 (advertised) | 109–114 | 107–114 |
| 1,617 B | 16 | 2,816 | 115–124 | 119–126 |
| 73,079 B | 1 | 64 | 111–144 | 128–145 |
| 73,079 B | 1 | 512 | 90–140 | 137–150 |
| 73,079 B | 1 | 2,816 | 115–142 | 132–147 |

This probe measures the serving side. The victim's stream-6 queue carries the peer's
`GetBlocks` requests, which now share the 8 slots with control frames.

**Requesting end to end** (`zakura_mock_blocksync_throughput`, `ZAKURA_MOCK_BS_RUN=1`, 4 seeds).
This harness sends real bodies into the depth-limited queue:

| Corpus | Base blocks/s | Branch blocks/s |
| --- | --- | --- |
| 100,000 synthetic blocks of 1–16 transactions (default) | 858–1,442 | 707–2,559 |
| 4,000 synthetic blocks of about 1 MB (`ZAKURA_MOCK_BS_TARGET_BLOCK_BYTES=1000000`) | 133–154 | 138–159 |

The default corpus saturates one core on this host and swings with load, so its wide ranges
reflect the host rather than either build. The 1 MB corpus puts the queue under the most byte
pressure and stays within a few percent.

## Throughput requirement and the 10 Gbps target

The specification requires that regulation not limit a conformant peer's throughput below
what the transport and the node's own processing allow. The 10 Gbps per connection at 500 ms
RTT figure in `regulation/sizing.rs` is the design target that sizes the serving output and
execution bounds. It is not a spec threshold, and these measurements do not claim it: they
run on loopback on one host, where CPU, storage reads and verification bind first.

## Open items

- **Storage probe.** `getblocks_storage_read_probe` needs a synced database. Its code is
  unchanged by this work, so it was not re-run.
- **Fleet benchmark.** Loopback probes cannot show the multi-host operating envelope.
- **Duplicate body classification.** A duplicate body after a count-1 range is fully claimed is
  classed as an incompatible header (local, supplier cooldown) rather than as over-sending.
  The connection closes either way.
