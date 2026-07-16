# Zakura block-sync download scheduling: lanes, look-ahead, and the memory budget

Developer notes for the admission/backpressure design in this module — the floor vs
above-floor request lanes, the commit-window exemption, the resident-memory look-ahead
budget, and the liveness rules that hold them together. Code anchors:
[`admission.rs`](admission.rs) (the pure decision logic), [`peer_routine.rs`](peer_routine.rs)
(the per-peer fill loop that consumes it), and [`config.rs`](config.rs) (budgets and floors).

## Pipeline overview

Header sync runs far ahead of block bodies. Body download is driven by one **fill loop
per connected peer** (`PeerRoutine::try_fill`), all pulling from a shared `WorkQueue` of
needed heights. A downloaded body flows through these pools until the verifier commits
it:

```text
WorkQueue (pending heights)
  → in-flight request (reserved bytes, outstanding)
    → sequencer input channel (wire payload plus transient peer-decoded block)
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

## The two lanes: Floor vs AboveFloor

`RequestPriority` classifies a request by its start height
(`admission::request_priority`):

| | **Floor** | **AboveFloor** (speculative) |
| --- | --- | --- |
| Start height | `≤ download_floor + 1` (`floor_rescue_high`) | anything higher |
| Purpose | unblock the contiguous prefix so the committer keeps moving | fill the look-ahead buffer so the bursty committer never starves |
| Candidate selection | first pending in `[servable_low, min(servable_high, floor_high)]`, only if this peer is the preferred floor carrier | first pending in the peer's servable range, only when the floor arm produced nothing |
| cwnd slots | may borrow up to `floor_bypass_slots` (default 2) beyond a saturated cwnd — bypass slots fund the floor **only** | normal cwnd slots only |
| Byte funding | never refused: `reserve_request_budget`'s floor path overdrafts the in-flight budget by at most one request when `try_reserve` fails — reachable even at zero in-flight budget | non-blocking `try_reserve`; refused if the in-flight budget is spent |
| Request deadline | short fixed leash (`floor_rescue_timeout`, default 2 s); on expiry the height is rescued to a faster carrier, the peer is retry-avoided but **not** disconnected | `request_timeout` (default 8 s) + expected transfer time (`estimated_bytes / measured BtlBw`, rate floored at 256 KiB/s) — patient, since it never gates the floor |

**Floor carrier preference:** the floor rides the fastest servable peer. Before taking
floor work, a routine asks the shared registry
(`floor_has_preferred_unsaturated_server`) whether another peer should take it instead —
outside bypass, only a strictly faster (lower RTprop) unsaturated peer causes deferral;
inside bypass an equal-RTprop peer is preferred too, so scarce bypass slots are spent
only when nobody better can move the floor. If every servable peer is saturated, nobody
defers and the floor still moves.

## Look-ahead rules: `admit()` and the commit window

All admission decisions go through one pure function, `admission::admit(config,
snapshot, start_height, servable_high, response_byte_cap)`. It is the single authority
for the commit-window exemption, the resident-memory gate, and request sizing — the
fill loop feeds its grant verbatim to the work queue and may not substitute its own
sizing.

The one deliberate exception is `admission::admit_received_body`, the retention-only
gate for a body that is already downloaded (the unmatched-fallthrough path). A received
body consumes no request budget, so it applies the same commit-window exemption and
resident gate but never consults `budget_available` — a wire budget saturated by
outstanding requests must not force an already-paid-for body to be dropped and
re-downloaded.

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
| `start > window` and gate full (bytes **or** blocks) | `LookaheadAtCap` | — | — |
| `start > window`, gate open | `Admit` | `servable_high` | `min(budget_available, response_byte_cap, remaining_retained_headroom)` |
| any admitted sizing that still comes to 0 bytes (in-flight budget spent, non-floor) | `InflightBudgetEmpty` | — | — |

**Takes never span the window boundary.** An exempt (in-window) grant is clamped at the
window top, so a single multi-block request can never carry both exempt in-window blocks
and gated above-window blocks — every above-window height must pass the resident check
in the gated arm. A contiguous run straddling the boundary becomes two requests (the
above-window half is planned on the next fill iteration with headroom sizing). At the
production default of one block per response this split never occurs.

## The memory budget

### Two independent byte budgets

Block sync limits two different resources:

- `max_inflight_block_bytes` (default 6 GiB, tracked by `ByteBudget`) bounds
  estimated serialized bytes requested from peers but not received yet. It is
  a wire-pacing and denial-of-service bound.
- `max_reorder_lookahead_bytes` (default 1.5 GiB, tracked by
  `RetainedBodyMemoryTracker`) is the admission target for the sum of retained
  body memory and estimated memory headroom promised to outstanding
  above-window requests.

These budgets cannot be merged: their limits, units, and lifetimes differ. The
wire reservation ends when a body arrives; its memory reservation becomes the
body's retained charge and remains until the pipeline drops its final owner.

### Retained accounting and ownership

`RetainedBodyMemoryTracker` is the memory-admission authority. Its single atomic
`used` total includes both:

1. `InFlightMemoryReservation`s for above-window requests; and
2. exact `RetainedCharge`s owned by bodies already in the pipeline.

Consequently, concurrent peer routines cannot observe and spend the same free
headroom. Every accepted body owns an RAII charge that follows it through input,
reorder, applying, and detached submission ownership:

| Representation | Charge |
| --- | --- |
| raw payload | retained `Arc<[u8]>` length |
| decoded block | `Block::attributed_memory_size_bytes()` |
| decoded block plus raw payload | both charges |

Changing representation resizes the same charge. Dropping the final owner releases it,
notifies routines waiting for capacity, and makes the headroom immediately reusable.
Error, duplicate, reset, and shutdown paths therefore cannot leak retained accounting.

### Request reservation lifecycle

For each above-window request:

1. `admit` performs a cheap snapshot check and sizes the take by the smallest of
   wire-budget availability, peer response capacity, and remaining retained
   headroom.
2. After selecting concrete heights, `try_reserve_many` atomically reserves the
   entire request's memory estimates. It is all-or-none, closing races between
   peer routines that used the same snapshot.
3. `WorkQueue::mark_issued` commits the request-byte ledger and per-height memory
   reservations under one lock. Reset, watchdog, and competing-response paths
   therefore see either the whole issued request or none of it.
4. On receipt, `WorkQueue` transfers the height's memory reservation to
   `BufferedBlockBody`. `reconcile_exact` resizes it to the measured raw payload
   allocation plus `Block::attributed_memory_size_bytes()`. Growth is added to
   the global total before it is published locally, so admission never observes
   a transient undercount.
5. Timeout, short response, send failure, watchdog, reset, floor GC, and
   disconnect remove unreceived reservations from `WorkQueue`. RAII releases
   their headroom exactly once.

The request estimate is not claimed to be the exact decoded size. For reservations
that arrive together, reconciliation changes the tracked total by
`sum(exact retained bytes) - sum(reserved estimate bytes)`, where each exact
charge is the raw allocation plus attributed decoded memory. The default 200%
size tolerance limits an accepted serialized body to twice its estimate, but
there is no enforced maximum decoded-to-serialized expansion ratio. Consequently,
the 1.5 GiB setting has no fixed worst-case overshoot multiplier. Once growth
makes the total reach or exceed the target, subsequent above-window reservation
attempts fail until retained charges are released.

This is still stronger than accounting only against the separate 6 GiB wire
budget: every outstanding speculative request consumes estimated retained
headroom before issuance, and its exact charge remains visible after receipt.

Two gates, checked together in `lookahead_over_budget`:

- **Byte gate:** retained bodies plus outstanding above-window memory
  reservations `≥ effective_max_reorder_lookahead_bytes`
  (= `max_reorder_lookahead_bytes`).
- **Block gate (defense in depth):** reorder + applying + reserved block counts
  `≥ LOOKAHEAD_BLOCK_HARD_CAP` (a fixed 262,144 — it binds before the byte gate
  only for tiny bodies averaging under ~6.1 KB wire, where per-entry bookkeeping
  overhead dominates; never needed operator tuning, so it is a constant, not a
  config knob).

### The bound

The configured value is a soft admission target, not a hard memory ceiling.
Retained body memory consists of the target plus these overshoot sources:

- estimate reconciliation, whose aggregate growth is exact retained bytes minus
  reserved estimate bytes but has no fixed multiplier while decoded expansion
  remains uncapped;
- commit-window bodies, which remain exempt for checkpoint liveness;
- raw bodies entering the configured verifier submission window and gaining a
  decoded representation.

The commit window and submission decode window are independently count-bounded;
estimate reconciliation is not numerically bounded beyond the decoded block
representations that can be produced from protocol-valid bodies. This is
deterministic Rust object-graph accounting, not process RSS. Allocator metadata,
fragmentation, runtime overhead, and verifier allocations remain outside the
charge.

## Liveness rules (why sync cannot wedge)

1. **Commit-window heights are always fundable**, on both lanes, regardless of the
   look-ahead gates — a pinned checkpoint range can always assemble.
2. **Floor grants never size below one byte**, so a full wire-request budget
   cannot prevent the floor from reaching the request-budget reservation path.
3. **The floor may overdraft only the wire-request budget.** When
   `ByteBudget::try_reserve` fails, `reserve_request_budget` charges at most one
   floor request past the limit. Normal receipt or cancellation repays it.
4. **The floor does not bypass retained-memory admission above the commit
   window.** If an above-window floor take cannot reserve memory headroom, it is
   returned to the queue until retained capacity is released. Commit-window
   progress remains exempt and advances the verified-tip-anchored window.

## Observability

Every fill pass ends with a typed stop reason (`FillStop`), emitted as the
`sync.block.fill_stop{reason}` counter and the fill-stop trace event:

| reason | meaning |
| --- | --- |
| `no_status` | peer has not sent its servable status yet |
| `cwnd_saturated` | no slots (not even a floor bypass slot), or bypass pass with no floor take |
| `no_work` | nothing pending in the peer's servable range (or floor deferred to a preferred carrier) |
| `lookahead_cap` | the resident look-ahead gate refused an above-window take (either lane) |
| `inflight_budget` | gate has headroom but the in-flight byte budget funds zero bytes |
| `retry_avoid` | the whole take was heights this routine recently failed |
| `budget` / `internal` / `outbound_full` / `send_error` | funding/bookkeeping/transport stops |

The `sync.block.backlog.at_cap` gauge is latched to 1 whenever a pass stops on
`lookahead_cap` (from any lane, including bypass) and reset to 0 on a successful
above-floor grant. `sync.block.request.floor_bypass` counts floor requests that
borrowed a bypass slot. Trace and metric fields named `retained_memory_bytes`
report the authoritative admission total: retained body charges plus outstanding
above-window memory reservations.

## Key config knobs (defaults)

| Knob | Default | Notes |
| --- | --- | --- |
| `max_reorder_lookahead_bytes` | 1.5 GiB | soft cap on retained body charges plus above-window memory reservations |
| `max_inflight_block_bytes` | 6 GiB | outstanding-request wire budget, released at receipt (separate from the resident gate) |
| `max_blocks_per_response` | 1 | count cap per request (effective = min of both sides' advertisements, hard max 128) |
| `floor_bypass_slots` | 2 | extra slots past a saturated cwnd, floor lane only |
| `request_timeout` / `floor_rescue_timeout` | 8 s / 2 s | above-floor base deadline / floor rescue leash |
| `max_submitted_block_applies` | 401 | sequencer submit window (floored at one checkpoint range; no ceiling — which is why the exemption span is a constant) |

## Known limitations and follow-ups

- **Attributed memory is not RSS.** It excludes allocator and runtime overhead, and
  shared allocations are attributed according to the block sizing contract.
- **Window-boundary split.** With `max_blocks_per_response > 1`, a run straddling the
  commit window costs one extra request per crossing (the price of the never-span
  rule). Irrelevant at the default of 1.
- **Block gate does not count the sequencer input channel** (its bytes are charged, its
  count is not); the channel is bounded by the submit window (401 by default), which is
  noise against the 262,144 default cap.
