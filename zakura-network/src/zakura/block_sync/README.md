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
| `start > window` and gate full (bytes **or** blocks), or wire headroom rounds to 0 | `LookaheadAtCap` | — | — |
| `start > window`, gate open | `Admit` | `servable_high` | `min(budget_available, remaining_wire, response_byte_cap)` |
| any admitted sizing that still comes to 0 bytes (in-flight budget spent, non-floor) | `InflightBudgetEmpty` | — | — |

where `remaining_wire = effective_budget − estimated_resident`. Admitted bodies remain
serialized until they enter the bounded submission window, so one byte of remaining
headroom funds one wire byte. The transient decoded input copy is bounded by the
sequencer channel and appears in the next resident snapshot.

**Takes never span the window boundary.** An exempt (in-window) grant is clamped at the
window top, so a single multi-block request can never carry both exempt in-window blocks
and gated above-window blocks — every above-window height must pass the resident check
in the gated arm. A contiguous run straddling the boundary becomes two requests (the
above-window half is planned on the next fill iteration with headroom sizing). At the
production default of one block per response this split never occurs.

## The memory budget

### Resident accounting

The look-ahead budget bounds **estimated resident memory**. Backlog bodies remain in
their serialized wire form and are decoded only inside the bounded verifier submission
window. Accounting therefore follows each pool's actual representation:

| Pool (snapshot field) | Representation | Charge |
| --- | --- | --- |
| `reorder_buffered_bytes` | serialized | wire bytes |
| unsubmitted `applying_buffered_bytes` | serialized | wire bytes |
| `sequencer_input_queued_bytes` | serialized payload plus decoded block | wire bytes plus `wire × DESERIALIZED_MEM_FACTOR` |
| `in_flight_submission_bytes` | submitted decoded block, including detached submissions awaiting completion | `wire × DESERIALIZED_MEM_FACTOR`, plus the applying wire charge while attached |
| `reserved_above_floor_bytes` | not received yet | estimated wire bytes |

Two gates, checked together in `lookahead_over_budget`:

- **Byte gate:** `estimated_resident ≥ effective_max_reorder_lookahead_bytes`
  (= `max_reorder_lookahead_bytes`; the request budget no longer caps it, since
  the request cap does not imply retention).
- **Block gate (defense in depth):** reorder + applying + reserved block counts
  `≥ LOOKAHEAD_BLOCK_HARD_CAP` (a fixed 262,144 — it binds before the byte gate
  only for tiny bodies averaging under ~6.1 KB wire, where per-entry bookkeeping
  overhead dominates; never needed operator tuning, so it is a constant, not a
  config knob).

This is separate from the **in-flight request budget** (`max_inflight_block_bytes`,
default 6 GiB, tracked by `ByteBudget`): that bounds outstanding request
reservations — charged at issuance, released at receipt (or timeout/watchdog/
reset/floor GC) — while the look-ahead gate is the single authority over bytes
_retained_ by the pipeline.

### Config clamps

At config load (`clamp_reorder_lookahead_to_floor`, serde path only), sub-range budgets
are raised to one worst-case serialized checkpoint range —
`BS_CHECKPOINT_RANGE_BYTE_FLOOR` (401 × 2 MB ≈ 802 MB) — with a warning. The clamp is
defense-in-depth _sizing_ only: liveness is guaranteed by the commit-window exemption,
not by budget size. Zero config values are rejected.

### The bound

Resident memory plateaus near **`effective budget + the bounded decode window`**, plus
process overhead and bounded transients. At the default 401-block submission window,
the worst-case decoded contribution is approximately
401 × `MAX_BLOCK_BYTES` × `DESERIALIZED_MEM_FACTOR` ≈ 3.2 GB:

- the floor's first-item progress margin (≤ one block per request — see liveness below);
- a single in-window response can exceed the byte gate by up to its decoded resident
  cost (in-window sizing is by the in-flight budget, for liveness);
- concurrent peer routines admit against per-iteration snapshots, so a simultaneous
  wake can transiently over-admit by roughly one response per racing runtime worker
  before the reservations land in the next iteration's snapshot.

## Liveness rules (why sync cannot wedge)

1. **Commit-window heights are always fundable**, on both lanes, regardless of the
   look-ahead gates — a pinned checkpoint range can always assemble.
2. **Floor grants never size below one byte**, and `take_in_range_budgeted` always
   takes its first item regardless of the byte cap — so the floor block is taken even
   when the in-flight budget is exactly full, reaching the floor reservation path…
3. **…which overdrafts instead of waiting.** When `try_reserve` fails,
   `reserve_request_budget`'s Floor path charges the reservation past the max — a
   bounded overshoot of at most one request (the WorkQueue single-owner invariant
   permits one floor reservation globally), repaid through the normal release
   discipline. The floor is therefore never starved by speculative work, with no
   cross-task funding round trip.
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
| `max_reorder_lookahead_bytes` | 1.5 GiB | resident-memory target; serialized pools are charged at wire size and decoded windows at the calibrated multiple; clamped up to ~802 MB |
| `max_inflight_block_bytes` | 6 GiB | outstanding-request wire budget, released at receipt (separate from the resident gate) |
| `max_blocks_per_response` | 1 | count cap per request (effective = min of both sides' advertisements, hard max 128) |
| `floor_bypass_slots` | 2 | extra slots past a saturated cwnd, floor lane only |
| `request_timeout` / `floor_rescue_timeout` | 8 s / 2 s | above-floor base deadline / floor rescue leash |
| `max_submitted_block_applies` | 401 | sequencer submit window (floored at one checkpoint range; no ceiling — which is why the exemption span is a constant) |

## Known limitations and follow-ups

- **Decoded-memory factor.** `DESERIALIZED_MEM_FACTOR` is a calibrated approximation of the
  measured ~3.3–4× wire→decoded ratio, not a per-block heap measure. Replacing it with
  a precise per-block resident estimate is tracked as
  [ZCA-750](https://linear.app/zcale/issue/ZCA-750).
- **Decoded accounting remains approximate.** Serialized pools are charged exactly
  at wire size, but the bounded input and submission windows still use the calibrated
  factor rather than a precise heap measurement.
  `estimated_resident_pipeline_bytes` is the single calibration point.
- **Window-boundary split.** With `max_blocks_per_response > 1`, a run straddling the
  commit window costs one extra request per crossing (the price of the never-span
  rule). Irrelevant at the default of 1.
- **Block gate does not count the sequencer input channel** (its bytes are charged, its
  count is not); the channel is bounded by the submit window (401 by default), which is
  noise against the 262,144 default cap.
