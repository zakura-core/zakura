# Block-sync resource ledger

> **Status:** conformance evidence for regulated block-sync version 3 (stream 6) under the
> [regulation specification](../specs/peer-message-regulation.md). The
> [GetBlocks test plan](property-testing-block-sync-infrastructure.md) lists the tests and
> probes this ledger cites.

## Principle

The block-sync reactor decides what to ask for. Its congestion control, request byte budget
and look-ahead gate size the work to the node's progress. Memory follows from those requests.
A peer that sends more than its live reservations owe, or a body that disagrees with its own
header, breaks the protocol, and the node disconnects it.

The node adds no independent rate or memory cap on a conformant peer's data path. A cap that
can stall a fast, conformant supplier would trade throughput for a bound the reactor already
provides. Where a bound must exist without a request behind it (serving, transport windows),
it is sized to the throughput target, not below it.

## Node budget

Every term counts one physical allocation once. Defaults are shown. "Peer" means one
connection; the node admits at most `zakura.max_connections` = 256 of them.

| Term | Per peer | Node | Bound comes from |
| --- | --- | --- | --- |
| QUIC receive buffers | 32 MiB connection window (16 MiB per stream) | 8 GiB | `DEFAULT_ZAKURA_RECEIVE_WINDOW`, `DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW` in `handler.rs`, enforced by QUIC flow control |
| QUIC send buffers | 32 MiB | 8 GiB | `DEFAULT_ZAKURA_SEND_WINDOW`; serving output below sits inside it |
| Serving bookkeeping | 1 MiB (144 KiB + 2 × 2,816 × 160 B) | 512 MiB | `BOOKKEEPING_BUDGET_BYTES` in `regulated/session.rs`; config validation refuses a config that cannot fund one request per slot |
| Serving storage results | 12 jobs | 24 jobs × one read (≤ `blocks_per_response` decoded blocks) | `NODE_EXECUTION` / `PEER_EXECUTION`; a lease lives until the blocking read and encode return |
| Serving encoded output | 64 MiB | 1.25 GB, 1 response slot per 4 KiB | `regulation/sizing.rs` (`peer_output_bytes`, `node_output_bytes`) |
| Requester bookkeeping | ≤ live ranges | 32,768 ranges; ≤ 128 MiB of expected hashes | `MAX_BS_INFLIGHT_REQUESTS` request pool in `reactor.rs` |
| Raw stream-6 frames awaiting the routine | ≤ what live reservations owe (frames + one ending each, remaining response-cap bytes); ≤ 4,096 frames | see [raw frames](#raw-frames) | `Reservations::admit` in `regulation/reservations.rs`; over-sending disconnects |
| Look-ahead backlog (raw) | — | `max_reorder_lookahead_bytes` = 1.5 GiB plus one range | `admission.rs` byte gate, re-checked by the sequencer on every body |
| Decoded bodies | one transient junk decode before rejection | 401 submissions × measured decoded size | `max_submitted_block_applies`; admission charges `max(4 × wire, measured decoded)` |
| Control state | bounded maps | 8,192 supplier cooldowns; Status keeps the latest value | `MAX_SUPPLIER_RETRIES` in `peer_registry.rs` |

At defaults, block-sync application memory sums to about 8.8 GB in the worst case: 0.5 GiB
serving bookkeeping, 1.25 GB serving output, 1.3 GB of worst-case serving reads (about 170 MB
for real blocks), 0.13 GiB requester hashes and 5.6 GB of requested bodies (derived below).
The raw-frame term is the exception noted under [raw frames](#raw-frames). The QUIC windows
add 8 + 8 GiB. They are shared with every other service on the connection and stay unchanged:
256 connections × 32 MiB is the transport budget, not a block-sync choice.

## Serving

### Bookkeeping

`S × (144 KiB + 2 × A × 160 B) ≤ 512 MiB` holds with `S` = 512 session slots and `A` = 2,816
advertised requests. `compact_session_lifecycle_peak` measures the real retained bookkeeping
at 142 to 153 B per request, so 160 B is an upper bound. The 144 KiB per-session allowance
overcharges the measured fixed setup (about 10 KB).

Before this change, a config whose slots could not fund one request logged a warning and
advertised one request anyway. Now every production constructor validates the served limits
first: `spawn_block_sync_reactor`, `BlockSyncService::new*` and the endpoint refuse such a
config with `BlockSyncStartError::InvalidConfig` before binding or negotiating. 3,633 slots
is the largest fundable count; 3,634 slots fail.

### Storage work

Execution slots are 24 per node and 12 per peer. A slot is held until the blocking read and
encode actually return, including after the session is cancelled, so connection churn cannot
multiply running reads. Each job retains the blocks it read. These are decoded blocks from
the node's own consensus-valid state. One job holds `blocks_per_response` blocks (default 1):
at most about 53 MB decoded for the worst consensus-valid block, about 7 MB for real Mainnet
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

The node-wide request pool holds 32,768 live ranges. Each range keeps at most 128 expected
hashes, so hashes total at most 128 MiB. Range maps release their excess capacity after a
peak (`shrink_after_peak`), so a burst does not pin its high-water allocation.

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
The raw queue's worst case is therefore the sum of the live ranges' caps, which can exceed the
6 GiB request byte budget when bodies run far above their estimates. Pre-NU5 bodies cannot
grow: their txids cover every byte and the Merkle check below rejects any change. Post-NU5
transparent signatures are outside the txid, so padding them survives until consensus checks
the auth-data commitment. This is the loosest term in the ledger; see [open items](#open-items).

### Decoded bodies

A decoded block's heap is not a fixed multiple of its wire size:

| Body | Wire | Decoded | Ratio |
| --- | --- | --- | --- |
| Real Mainnet blocks | — | — | ≤ 3.45×, about 2× in aggregate |
| Worst consensus-valid block (smallest valid transactions) | 2 MB | about 53 MB | about 26× |
| Junk body: 37,037 empty V1 transactions | 372 KB | 74.9 MB | about 200× |

`size_of::<Transaction>()` is 2,008 B, so per-transaction overhead dominates small
transactions. `DESERIALIZED_MEM_FACTOR` = 4 is an average, not a bound.

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
built, proof-of-work-paid chain at `r` ≈ 26 raises the window term to about 21 GB; admission
then charges the measured size and stops requesting ahead. The check costs one txid pass per
body, about 11% of decode time on real blocks, on the per-peer routine tasks.

The Merkle check departs from the Block section's SHOULD to reuse downstream validation
rather than add a duplicate ingress check. The downstream check runs only after the junk body
is decoded into the pipeline, and its misbehavior report does not disconnect. Common
requirement 2 needs the decoded size tied to the requested block before the body is retained,
which only an ingress check provides. Consensus still runs its own Merkle check.

Two options were rejected because they limit throughput: a node-wide decoded-byte semaphore,
and a stream-6 queue depth of 2. Moving the body's raw-backlog demotion into the routine was
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
| Common requirement 6: receive buffering bounded by what was requested | `Reservations::admit` owed bound | `queued_frames_never_exceed_what_the_live_reservations_owe`, `the_transport_queue_holds_only_what_the_live_ranges_owe` | Closed |
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
baseline fits the 512 MiB bookkeeping budget.

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

## Throughput requirement and the 10 Gbps target

The specification requires that regulation not limit a conformant peer's throughput below
what the transport and the node's own processing allow. The 10 Gbps per connection at 500 ms
RTT figure in `regulation/sizing.rs` is the design target that sizes the serving output and
execution bounds. It is not a spec threshold, and these measurements do not claim it: they
run on loopback on one host, where CPU, storage reads and verification bind first.

## Open items

- **Raw frames above their estimates.** The owed bound caps each connection at its live
  ranges' response caps. A post-NU5 peer can pad transparent signatures up to that cap
  before consensus rejects the block. Charging queued raw bytes against the request byte
  budget, or checking the auth-data commitment in the routine, would close it.
- **Storage probe.** `getblocks_storage_read_probe` needs a synced database. Its code is
  unchanged by this work, so it was not re-run.
- **Fleet benchmark.** Loopback probes cannot show the multi-host operating envelope.
- **Duplicate body classification.** A duplicate body after a count-1 range is fully claimed is
  classed as an incompatible header (local, supplier cooldown) rather than as over-sending.
  The connection closes either way.
