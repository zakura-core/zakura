# Zakura block-sync download scheduling: lanes, look-ahead, and the memory budget

Developer notes for the admission/backpressure design in this module — the floor vs
above-floor request lanes, the commit-window exemption, the resident-memory look-ahead
budget, and the liveness rules that hold them together. Code anchors:
[`admission.rs`](admission.rs) (the pure decision logic), [`peer_routine.rs`](peer_routine.rs)
(the per-peer fill loop that consumes it), [`config.rs`](config.rs) (budgets, floors,
clamps).

## Pipeline overview

Header sync runs far ahead of block bodies. Body download is driven by one **fill loop
per connected peer** (`PeerRoutine::try_fill`), all pulling from a shared `WorkQueue` of
needed heights. A downloaded body flows through these pools until the verifier commits
it:

```text
WorkQueue (pending heights)
  → in-flight request (reserved bytes, outstanding)
    → sequencer input channel (wire payload, plus the peer-decoded block while the decoded cap has room)
      → reorder buffer (out-of-order arrivals, wire-retained)
        → applying pool (contiguous; backlog wire-retained)
          → bounded submission window (decoded Arc<Block>)
            → checkpoint/semantic verifier → commit (verified tip advances)
```

Two heights anchor every scheduling decision:

- **`download_floor`** — the lowest height not yet downloaded. Advances on every
  download, so it can escalate far ahead of commit.
- **`verified_block_tip`** — the commit tip. Advances only when the verifier commits,
  which during checkpoint sync happens one whole checkpoint range at a time.

The distinction matters: anything anchored to the download floor is self-propelling
(downloading moves the floor, which permits more downloading),
while anything anchored to the verified tip is pinned until real progress commits.

## Retained serving range

Native block-sync status advertises the range of retained, verified block bodies.
The node initializes its lower bound from the persisted pruning marker. Storage
publishes each new floor after the durable write and before the corresponding tip,
so even a tip notification handled first uses the current floor. Archive nodes
advertise from height zero. Requests for pruned heights return `RangeUnavailable`
without querying storage. Genesis remains servable separately, and a request starting
there stops before the pruned gap.

Incoming status changes allow one extra range update per rate-limit window.
Further range changes are coalesced and applied when the window expires, even
if no new message arrives. Status replies remain rate limited.
Each peer tracks the last status queued on its ordered stream. Changes that raise
the lower bound or reduce the upper bound use a separate window of at most one
second, including when pruning and tip growth happen together. All other changes
use the configured refresh interval, so pruning does not wait behind tip-only growth.
Further changes coalesce into the latest range. Full queues back off for at most
100 ms without consuming a range-change allowance. One deadline calculation schedules
range changes, queue retries, and incomplete status exchanges.

Checkpoint retention can put the lower bound above the verified tip. Until retained
bodies reach that height, the node advertises only genesis, which is always retained.
This storage boundary does not advance the node's download floor or discard blocks
it still needs from other peers. Embedders using pruned state should pass its retained
height watch in `ZakuraHeaderSyncDriverStartup`, which every production endpoint
initializer forwards. Direct reactor users attach it with
`BlockSyncStartup::with_retention`.

Advertisements describe current availability and do not reserve block bodies.
A request already in flight can overlap pruning before its database read. That
pre-existing race can still return `RangeUnavailable`; preventing it requires a
separate serving/read reservation contract.

## The two lanes: Floor vs AboveFloor

`RequestPriority` classifies a request by its start height
(`admission::request_priority`):

| | **Floor** | **AboveFloor** (speculative) |
| --- | --- | --- |
| Start height | `≤ download_floor + 1` (`floor_rescue_high`) | anything higher |
| Purpose | unblock the contiguous prefix so the committer keeps moving | fill the look-ahead buffer so the bursty committer never starves |
| Candidate selection | first pending in `[servable_low, min(servable_high, floor_high)]`, only if this peer is the preferred floor carrier | first pending in the peer's servable range, only when the floor arm produced nothing |
| cwnd slots | may borrow up to `floor_bypass_slots` (default 2) beyond a saturated cwnd — bypass slots fund the floor **only** | normal cwnd slots only |
| Byte funding | Same as above-floor: the memory pool leases the request, and a refusal makes the routine wait | `admit_for_connection` leases from `Consumer::Consensus` inside the commit window and from `Consumer::BlockSync` above it; a refusal makes the routine wait |
| Request deadline | short fixed leash (`floor_rescue_timeout`, default 2 s); on expiry the height is rescued to a faster carrier, the peer is retry-avoided but **not** disconnected | `request_timeout` (default 8 s) + expected transfer time (`estimated_bytes / measured BtlBw`, rate floored at 256 KiB/s) — patient, since it never gates the floor |

**Floor carrier preference:** the floor rides the fastest servable peer. Before taking
floor work, a routine asks the shared registry
(`floor_has_preferred_unsaturated_server`) whether another peer should take it instead —
outside bypass, only a strictly faster (lower RTprop) unsaturated peer causes deferral;
inside bypass an equal-RTprop peer is preferred too, so scarce bypass slots are spent
only when nobody better can move the floor. If every servable peer is saturated, nobody
defers and the floor still moves.

### Accounting closure

The request budget permanently closes its generation when an accounting operation
fails. The queue, unpublished requests, and writers consult the same closure flag.
Closure rejects normal and floor admission, publication, and new writer starts.
Existing owners retain their charges and can settle them. A frontier reset cannot
reopen the budget or erase unmatched funding.

The first closure emits an error with its cause, current charge, and budget limit.
A funding audit also includes the logical and funded totals in that cause.
A lock acquisition that finds a poisoned queue closes the same budget generation.
Cleanup recovers the guard so request owners can settle during unwind. The queue
notifies closure waiters after unlocking. Retained receipts and unrelated
reservations remain charged until their own owners finish.

The sequencer publishes `accounting_closed` in its view and block-state trace.
Fill passes report `accounting_closed` separately from capacity exhaustion.
Typed admission refusals also distinguish cancellation and stale claims.

The queue guard notifies closure waiters after unlocking, including during unwind.
Counter release also publishes closure if its arithmetic fails. The driver reads
this signal independently of action-queue capacity and sequencer-view publication.
It stops starting verification work, discards queued work, and drains its in-flight
verification futures. Its exit triggers the existing critical-driver supervisor,
which starts node shutdown. Recovery requires a new process. The old budget never
reopens, and the node creates no replacement subsystem in the same process.

## Look-ahead rules: `admit()` and the commit window

Request admission decisions go through one pure function, `admission::admit(config,
snapshot, start_height, servable_high, response_byte_cap)`. It is the single authority
for the commit-window exemption, the resident-memory gate, and request sizing — the
fill loop feeds its grant verbatim to the work queue and may not substitute its own
sizing.

### The commit window

Heights in `(verified_tip, verified_tip + 401]` are the **commit window**
(`COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS` = `MAX_CHECKPOINT_HEIGHT_GAP + 1`). The checkpoint
verifier resolves a range only once the _whole_ range (up to 401 blocks) is submitted,
while the verified tip stays pinned to the previous checkpoint. So every block of the
active range must stay fundable even when the look-ahead budget is full — otherwise the
range can never assemble and sync wedges. The span is deliberately a constant, not
`config.submitted_apply_limit()`: that knob has no ceiling, and a huge configured
submit window would widen the exemption until the memory gate is disabled.

Because the window is anchored to the **verified tip**, it advances only on commit. The
download floor moves on every download and can run far ahead of commit, but any such
height is just another gated height.

### Decision table

| Condition | Outcome | `take_high` | `max_request_bytes` |
| --- | --- | --- | --- |
| `start ≤ verified_tip + 401` (in-window) | always `Admit` | `min(servable_high, window_top)` | `min(budget_available, response_byte_cap)`; Floor priority floors it at 1 |
| `start > window` and gate full (bytes **or** blocks), or wire headroom rounds to 0 | `LookaheadAtCap` | — | — |
| `start > window`, gate open | `Admit` | `servable_high` | `min(budget_available, remaining_wire, response_byte_cap)` |
| any admitted sizing that still comes to 0 bytes (in-flight budget spent, non-floor) | `InflightBudgetEmpty` | — | — |

where `remaining_wire = effective_budget − estimated_resident`. Admitted bodies remain
serialized until they enter the bounded submission window, so one byte of remaining
headroom funds one wire byte. The peer routine keeps its decoded copy in the handoff only
while the input queue's and `applying`'s decoded bytes leave room for it under
`BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES` (#1522). Otherwise it hands over the wire payload
alone. The decoded copy appears in the next resident snapshot.

**Takes never span the window boundary.** An exempt (in-window) grant is clamped at the
window top, so a single multi-block request can never carry both in-window blocks, which
lease from Consensus, and above-window blocks, which lease from block sync. A contiguous run straddling the boundary becomes two requests (the
above-window half is planned on the next fill iteration with headroom sizing). Requests
carry one block, so this split never occurs in practice.

## The memory budget

The node-wide memory pool (`crate::zakura::memory`) bounds block sync. Block sync
has no byte gate of its own.

- **Requests.** A request inside the commit window leases its estimated wire bytes
  from `Consumer::Consensus`, a reserved consumer that never waits behind bulk work.
  A request above the window leases from `Consumer::BlockSync`, a bulk consumer.
  The QUIC receive credit for the request follows its lease.
- **Bodies.** A received body keeps its request's lease, sized to its wire bytes. A
  body above its estimate grows its lease. If the pool refuses, block sync drops the
  body and returns the height at its measured size, so the next request leases the
  body whole and waits.
- **Decode.** At decode the lease grows to `wire + decoded`. A commit-window body on
  a block-sync lease moves to Consensus there.
- **Drains.** Credit a peer didn't deliver stays leased until the peer can no longer
  spend it. A Consensus drain moves to block sync, so a re-issued request takes the
  height's only Consensus lease (mem-15). Only a full pool leaves a drain on
  Consensus; `sync.block.consensus.drain_unmoved_bytes` counts it.
- **Benefit cap.** The sequencer sets block sync's cap once a second to the peers'
  BBR windows plus `2 × commit rate × p99 range fetch time`. The commit rate is a
  decaying peak, so a commit stall shrinks the cap gradually.
- **Block cap.** `LOOKAHEAD_BLOCK_HARD_CAP` (262,144 blocks) bounds the retained
  block count above the window, for tiny bodies whose bookkeeping dominates.

`ByteBudget` remains an unlimited settlement ledger: it records each owner's
charge, and the simulator compares it with the work queue. It never refuses.

## Liveness rules (why sync cannot wedge)

1. **Commit-window heights lease from the Consensus reserve.** Consensus holds one
   lease per height, and the reserve covers the window at admission's per-block
   charge, so a pinned checkpoint range can always assemble. A Consensus refusal for
   a commit-window body breaks that invariant: block sync counts it in
   `sync.block.consensus.lease_refused`, and the simulator asserts it.
2. **Floor grants never size below one byte**, and `take_request` always takes its
   first item regardless of the byte cap, so the floor block reaches the pool and
   waits for capacity.

## Observability

Every fill pass ends with a typed stop reason (`FillStop`), emitted as the
`sync.block.fill_stop{reason}` counter and the fill-stop trace event:

| reason | meaning |
| --- | --- |
| `no_status` | peer has not sent its servable status yet |
| `cwnd_saturated` | no slots (not even a floor bypass slot), or bypass pass with no floor take |
| `no_work` | nothing pending in the peer's servable range (or floor deferred to a preferred carrier) |
| `lookahead_cap` | the block cap refused an above-window take (either lane) |
| `inflight_budget` | the pool consumer for the take has no room |
| `retry_avoid` | the whole take was heights this routine recently failed |
| `budget` / `internal` / `outbound_full` / `send_error` | funding/bookkeeping/transport stops |

The `sync.block.backlog.at_cap` gauge is latched to 1 whenever a pass stops on
`lookahead_cap` (from any lane, including bypass) and reset to 0 on a successful
above-floor grant. `sync.block.request.floor_bypass` counts floor requests that
borrowed a bypass slot.

### Byte gauges

Resource-bound tests read these gauges, so their names are a test contract.
Each resource gauge is written where its owner changes the value, including
release and drop paths, so a reader between any two steps sees the current
value:

| Gauge | Measures | Written |
| --- | --- | --- |
| `sync.block.reorder.buffered_bytes` | Wire bytes of bodies in the reorder buffer | When the reorder buffer inserts or drops a body |
| `sync.block.applying.buffered_bytes` | Wire bytes of bodies in `applying`; a detached submission no longer counts | When the sequencer adds or removes an `applying` body |
| `sync.block.sequencer_input.queued_bytes` | Wire bytes of bodies in the sequencer input queue | When a body enters the queue, leaves it, or drops |
| `sync.block.submission.in_flight_bytes` | Wire bytes of decoded submissions the driver can still retain, including detached ones | When the sequencer submits or settles a body |
| `sync.block.work.reserved_bytes` | Request-estimate bytes that the work queue's ledger reserves | Under the work-queue lock, when the ledger changes |
| `sync.block.budget.reserved_bytes` | Bytes charged to the shared in-flight request budget | When the budget charges or releases bytes |
| `sync.block.work.reserved_above_floor_bytes` | Request-estimate bytes that in-flight work above the download floor reserves | At each admission snapshot |

The above-floor gauge records an admission input, not a resource, so it keeps
the value the latest admission read. It counts every reservation above the
current download floor, including commit-window work, so it is a diagnostic.
The budget charges a request before the
work queue does and releases it after, so the budget gauge can exceed the work
gauge while a request is in transition. The two gauges are written separately,
so only a reader that sees no request in transition can compare them.

## Key config knobs (defaults)

| Knob | Default | Notes |
| --- | --- | --- |
| `max_reorder_lookahead_bytes` | 1.5 GiB | sizes the decoded pipeline bound; the pool, not this knob, bounds retained bodies |
| `max_inflight_block_bytes` | unset: `x × C` (92 × 32 MiB = 2,944 MiB) | only validated; the pool, not this knob, bounds requests |
| `max_blocks_per_response` | 1 (fixed) | deprecated and ignored; requests always carry one block, and serving answers a larger request with a one-block prefix |
| `floor_bypass_slots` | 2 | extra slots past a saturated cwnd, floor lane only |
| `request_timeout` / `floor_rescue_timeout` | 8 s / 2 s | above-floor base deadline / floor rescue leash |
| `max_submitted_block_applies` | 401 | sequencer submit window (floored at one checkpoint range; no ceiling — which is why the exemption span is a constant) |

## Known limitations and follow-ups

- **Decoded-memory factor.** `DESERIALIZED_MEM_FACTOR` is an average, not a bound:
  even with Sprout, Sapling, and Orchard bundles boxed, a block of small transparent V5
  transactions decodes at about 4.7× its wire size. Admission charges the larger of the factor's estimate and
  the measured decoded heap. The factor remains a floor so the gate never loosens on
  typical blocks. `estimated_resident_pipeline_bytes` is the single calibration point.
- **Block gate does not count the sequencer input channel** (its bytes are charged, its
  count is not); the channel is bounded by the submit window (401 by default), which is
  noise against the 262,144 default cap.

## Size corrections

A later known hint for the same header and auxiliary payload can fill an unknown size
once, but never replaces a known one. State keeps the fill in a sparse
`header_aux_body_size_v1` row beside the original delivery evidence and removes it
when that delivery is pruned. Committed block sizes still take precedence.

Committed corrections publish bounded batches of affected hashes. The queue updates
matching pending items and saves newer hints for retries without changing issued
reservations. A local measured size remains a minimum for a pressure retry. If a
consumer misses the retained update history, it refreshes its existing queued window
in bounded state queries. Accurate first deliveries add no correction writes or
refresh queries. Size-only changes leave verification observations, header generations,
and body-work epochs intact.
