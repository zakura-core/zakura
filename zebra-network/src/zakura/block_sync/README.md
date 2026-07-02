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
    → reorder buffer (out-of-order arrivals, wire-retained)
      → applying pool (contiguous prefix, decoded Arc<Block>)
        → sequencer input channel (decoded, awaiting submit)
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
| Byte funding | may **block**: `reserve_request_budget`'s floor path can shed an above-floor reorder body to fund the floor (`FundFloorReservation`) — reachable even at zero in-flight budget | non-blocking `try_reserve`; refused if the in-flight budget is spent |
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

where `remaining_wire = (effective_budget − estimated_resident) / DESERIALIZED_MEM_FACTOR`
— the remaining memory headroom expressed back in wire bytes, so one admitted response
cannot push resident memory past the budget once it lands and decodes.

**Takes never span the window boundary.** An exempt (in-window) grant is clamped at the
window top, so a single multi-block request can never carry both exempt in-window blocks
and gated above-window blocks — every above-window height must pass the resident check
in the gated arm. A contiguous run straddling the boundary becomes two requests (the
above-window half is planned on the next fill iteration with headroom sizing). At the
production default of one block per response this split never occurs.

## The memory budget

### Resident accounting

The look-ahead budget bounds **estimated resident memory**, not wire bytes. Decoded
bodies occupy roughly 3.3–4× their serialized size, so every pool is charged at its
_eventual_ decoded cost, `wire_bytes × DESERIALIZED_MEM_FACTOR` (= 4):

| Pool (snapshot field) | Actual retention today | Charged at ×4 because… |
| --- | --- | --- |
| `reorder_buffered_bytes` | wire-retained | the reorder→applying drain decodes the whole contiguous prefix unconditionally the moment a gap fills, with no admission re-gate |
| `applying_buffered_bytes` | decoded `Arc<Block>` | already resident at the decoded multiple |
| `sequencer_input_queued_bytes` | decoded | already resident |
| `reserved_above_floor_bytes` | not received yet | in-flight bodies land and decode the same way; charging them 0× makes the landing wave invisible until it is already resident |

Two gates, checked together in `lookahead_over_budget`:

- **Byte gate:** `estimated_resident ≥ effective_max_reorder_lookahead_bytes`, where
  `effective = min(max_reorder_lookahead_bytes, max_inflight_block_bytes × 4)`.
- **Block gate (defense in depth):** reorder + applying + reserved block counts
  `≥ LOOKAHEAD_BLOCK_HARD_CAP` (a fixed 262,144 — it binds before the byte gate
  only for tiny bodies averaging under ~6.1 KB wire, where per-entry bookkeeping
  overhead dominates; never needed operator tuning, so it is a constant, not a
  config knob).

This is separate from the **in-flight wire budget** (`max_inflight_block_bytes`,
default 6 GiB, tracked by `ByteBudget`): that bounds bytes concurrently on the wire;
the look-ahead gate bounds bytes _retained_ by the pipeline.

### Config clamps

At config load (`clamp_reorder_lookahead_to_floor`, serde path only), sub-range budgets
are raised to one worst-case checkpoint range — `BS_CHECKPOINT_RANGE_BYTE_FLOOR × 4`
(401 × 2 MB × 4 ≈ 3.208 GB) bytes and 401 blocks — with a warning. The clamps are
defense-in-depth _sizing_ only: liveness is guaranteed by the commit-window exemption,
not by budget size. Zero config values are rejected.

### The bound

Resident memory plateaus near
**`effective budget + one worst-case commit window`** (401 × `MAX_BLOCK_BYTES` × 4
≈ 3.2 GB), plus bounded transients:

- the floor's first-item progress margin (≤ one block per request — see liveness below);
- a single in-window response can exceed the byte gate by up to
  `response_byte_cap × 4` (in-window sizing is by the in-flight budget, for liveness);
- concurrent peer routines admit against per-iteration snapshots, so a simultaneous
  wake can transiently over-admit by roughly one response per racing runtime worker
  before the reservations land in the next iteration's snapshot.

Measured (mainnet 100K-height runs, 3 GB configured budget clamped to 3.208 GB): peak
retained wire 807.7–807.9 MB → ×4 = 3.231 GB, +0.7% over budget.

## Liveness rules (why sync cannot wedge)

1. **Commit-window heights are always fundable**, on both lanes, regardless of the
   look-ahead gates — a pinned checkpoint range can always assemble.
2. **Floor grants never size below one byte**, and `take_in_range_budgeted` always
   takes its first item regardless of the byte cap — so the floor block is taken even
   when the in-flight budget is exactly full, reaching the floor funding path…
3. **…which can shed to fund.** `reserve_request_budget`'s Floor path awaits
   `FundFloorReservation`: an above-floor reorder body is shed (returned to pending)
   to free bytes for the floor block. The floor is therefore never starved by
   speculative work, and this path is deliberately independent of the in-flight byte
   budget.
4. **In-window floor liveness never depends on budget size** — the clamps only stop
   sub-range configs from thrashing the speculative lane.

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
borrowed a bypass slot.

## Key config knobs (defaults)

| Knob | Default | Notes |
| --- | --- | --- |
| `max_reorder_lookahead_bytes` | ~6.4 GB | **resident-denominated** (compared against wire × 4); effective value capped at `max_inflight_block_bytes × 4`; clamped up to ~3.208 GB |
| `max_inflight_block_bytes` | 6 GiB | in-flight wire budget (separate from the resident gate) |
| `max_blocks_per_response` | 1 | count cap per request (effective = min of both sides' advertisements, hard max 128) |
| `floor_bypass_slots` | 2 | extra slots past a saturated cwnd, floor lane only |
| `request_timeout` / `floor_rescue_timeout` | 8 s / 2 s | above-floor base deadline / floor rescue leash |
| `max_submitted_block_applies` | 401 | sequencer submit window (floored at one checkpoint range; no ceiling — which is why the exemption span is a constant) |

## Known limitations and follow-ups

- **Flat ×4 factor.** `DESERIALIZED_MEM_FACTOR` is a calibrated approximation of the
  measured ~3.3–4× wire→decoded ratio, not a per-block heap measure. Replacing it with
  a precise per-block resident estimate is tracked as
  [ZCA-750](https://linear.app/zcale/issue/ZCA-750).
- **Conservative memory accounting reduces look-ahead.** Reorder blocks kept in
  wire form, and reservations for blocks we have not received yet, are charged at
  the decoded-memory estimate (`wire bytes × 4`). During reservation-heavy bursts,
  that means the look-ahead budget admits about a quarter of its nominal size in
  wire bytes. A better follow-up is **decode-at-submit**: keep bodies in wire form
  until they enter the submit window, then decode them there. Once each pool has a
  fixed representation, we can safely charge serialized pools near `×1` and
  decoded/applying pools near `×4`.
  `estimated_resident_pipeline_bytes` is the single edit point.
- **Window-boundary split.** With `max_blocks_per_response > 1`, a run straddling the
  commit window costs one extra request per crossing (the price of the never-span
  rule). Irrelevant at the default of 1.
- **Block gate does not count the sequencer input channel** (its bytes are charged, its
  count is not); the channel is bounded by the submit window (401 by default), which is
  noise against the 262,144 default cap.
