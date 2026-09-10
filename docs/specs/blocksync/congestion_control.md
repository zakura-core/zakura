# Block-Sync BBR Congestion Control — Specification

## Overview

The controller runs **per peer** and is **byte-denominated**: from a peer's body
delivery rate (bytes/s) and round-trip time it sizes an **in-flight window** — the body
bytes that peer may have outstanding. One request fetches one body, so request count =
`window ÷ body size`.

It balances three goals: **throughput** (keep each link full), **responsiveness** (keep
queues shallow, so a block rescued from a slow carrier lands fast), and **bounded
memory** (cap outstanding data regardless of peer behaviour).

Throughput and responsiveness don't conflict: the smallest window that keeps a link full
is one **BDP** (delivery-rate × base round-trip) — pipe saturated, nothing queued. The
window tracks `BDP × gain` and never grows past it; ProbeRtt periodically drains to
re-measure the true round-trip, and the delay gradient trims the window as soon as a
queue forms. Memory is bounded separately by the global outstanding-request budget, the
resident look-ahead gate, and per-peer caps.

## Glossary

Plain term (code identifier):

- **In-flight window** (`cwnd`) — max outstanding body bytes per peer.
- **Base round-trip** (`RTprop`) — minimum recent round-trip (queue-free latency).
- **Byte delivery rate / BDR** (`BtlBw`) — maximum recent body-delivery rate (bytes/s).
- **BDP** = `BDR × base round-trip` — bytes that exactly fill the pipe; the window's
  target. Less idles the link, more only queues.
- **Gain** — multiplier > 1 on the BDP so the window probes past measured capacity.
- **ProbeBw / ProbeRtt** — steady state (window ≈ BDP) / brief drain to re-measure RTprop.
- **Delay gradient** — round-trip risen above base by more than a set ratio; a forming queue.
- **Floor** — lowest height still needed (commit can't pass it); a slow floor carrier
  gets the height _rescued_ to a faster peer.

## Measured signals (per peer)

Windowed min/max signals MUST be filtered against the clock at _read_ time, not just
pruned on insert — else a peer that went fast then stalled keeps advertising a stale-low
round-trip / stale-high BDR and still looks like a fast floor server.

- **Base round-trip** — windowed _min_ of raw request round-trip (`bbr_rtprop_window`,
  10 s); never zero.
- **BDR** — windowed _max_ of per-response delivery rate (`bbr_delivery_rate_window`, 10 s).
- **BDP** = `BDR × base round-trip`; window target =
  `max(min window, BDP × gain × reliability_factor)`, `gain = 300%`.
- **Delay gradient** — smoothed round-trip vs a size-aware baseline
  (`base round-trip + bytes/BDR`).
- **Reliability** — per-peer EWMA of _goodput_: the fraction of issued requests that
  deliver a body (α = 0.1, starts at 1.0). It falls on every non-delivery — a timeout
  _and_ the missing heights of a short response (`BlocksDone`/`RangeUnavailable`) — so a
  peer can't serve one body per request to reset its liveness while dropping the rest of
  each range. A _late_ body (arriving after its request timed out) credits reliability
  back. Min/max filters see only completed requests; reliability is the drop signal.

Admission compares a peer's **reserved body bytes** against its window.

## Control law

**MUST:**

- Outstanding reserved bytes MUST NOT exceed the window (plus the floor bypass below).
- **ProbeBw**: the window tracks `BDP × gain`, clamped above by the delay-gradient
  ceiling and below by the minimum window, then scaled by `reliability_factor`.
- **ProbeRtt**: every `bbr_probe_rtt_interval` (10 s) the window MUST drain to the
  minimum for `bbr_probe_rtt_duration` (200 ms) to sample a fresh base round-trip;
  otherwise a sustained queue inflates the round-trip min and the window never collapses.

**SHOULD:**

- A real request timeout SHOULD dip the window once (`×0.85`, floored) and pull the delay
  ceiling to match — congestion evidence, not hard backoff.
- Round-trip above `base × delay_gradient` (150%) SHOULD ratchet the ceiling down
  (`×0.9`); with headroom it SHOULD relax up (~12% per response) to re-probe bandwidth.
- **Reliability discount**: scale the window by
  `reliability_factor = 1 − weight × (1 − reliability)`
  (`weight = bbr_reliability_weight_percent ÷ 100`; default 100% ⇒ factor = reliability,
  `0` = plain BBR). A dropped request is expensive — it can stall the floor for a whole
  timeout — so a carrier delivering `r` of its requests holds `r ×` the window. Applied
  _after_ the minimum-window floor, it MAY ramp the window to zero — the fast seal on a
  peer that stops delivering (see [wedged vs slow](#wedged-vs-slow)). Being an EWMA it
  self-heals as the peer recovers.

## Edge cases and bounds

**Fast peer's base round-trip → 0, voiding the BDP.** On a fast link a body's transfer
time is nearly the whole round-trip.

- The BDP MUST use the **raw** round-trip minimum, never a transmission-stripped residual
  (the residual feeds only the delay gate).
- The BDP-derived window MUST be floored at `bbr_min_cwnd_bytes` (≈2.5 MB — one max block
  plus headroom, the primary concurrency lever): a proven peer rides its measured BDP up
  via the gain rather than teleporting to a multi-MB burst. This floors only the BDP term;
  the reliability discount is applied after it and MAY go below the floor to zero.

**A buffered-body burst inflates the BDR.** The delivery-rate interval MUST be floored at
the previous base round-trip, so one tick's burst can't inflate the BDR max.

**A slow peer holds the contiguous floor.** The lowest missing height gates commit; one
slow carrier must not pin it.

- A floor request MUST carry a short leash (`floor_rescue_timeout`, 2 s); on expiry the
  height MUST return to the queue and the peer be retry-avoided — rescued, not
  disconnected (record-only).
- The floor MAY borrow up to `floor_bypass_slots` (2) bodies beyond a saturated window
  (within the request-count cap, reserving real budget). The borrow MUST scale by the
  peer's reliability, so a sealed peer earns **no** bypass; if every servable carrier is
  sealed, the floor waits for a fresh one.
- Above-floor speculation SHOULD use a size-aware deadline (`request_timeout + bytes ÷
  BDR`) and MUST NOT gate the floor.

**Unbounded memory under attacker-controlled bodies or stalls.**

- Outstanding request reservations MUST stay within `max_inflight_block_bytes`
  (6 GiB), apart from the single bounded floor overdraft; each reservation MUST be
  released when its body arrives or the request otherwise terminates.
- The resident look-ahead gate MUST be the sole authority for speculative bodies
  retained by, or reserved to enter, the reorder, applying, and sequencer-input pools.
  Serialized pools MUST be charged at wire size, while bounded decoded pools MUST also
  pay the calibrated decoded-memory charge. Commit-window work remains exempt so a
  checkpoint range can always assemble.
- Every size estimate MUST be clamped to `[floor, MAX_BLOCK_BYTES]`; untrusted header
  hints MUST NOT exceed the per-block worst case.
- The request-count cap (≤ `MAX_BS_INFLIGHT_REQUESTS = 32 768`) MUST bind even with byte
  headroom, so tiny bodies can't buy an unbounded request count.
- A reservation MUST be bounded by _remaining_ window bytes (window − reserved, plus the
  floor bypass), so a small window can't fund a large multi-body request — except the
  single always-taken item that guarantees floor progress.
- The reorder look-ahead and the serving-request heap MUST be bounded.

**An unbounded wait wedges a peer.** Every outbound request MUST have a network deadline —
the only sanctioned timer. When BDR is near zero the above-floor deadline assumes a
minimum delivery rate, so it stays finite (~16 s worst case).

**A peer accepts requests but never delivers bodies (probe-first).** Admission in front
of the window MUST enforce a no-progress policy:

- An **unproven** peer MUST get at most `initial_block_probe_requests` (1) before its
  first accepted body, so the cold-start window isn't spent as one burst.
- Once proven, `max_requests_without_block_progress` (64) caps requests without an
  accepted body before the liveness deadline disconnects it (then parked for
  `no_progress_peer_cooldown`, 180 s).
- The streak resets on any accepted body; only genuine silence is penalised:
  - a body accepted through the late/unmatched path MUST count as progress;
  - a destructive view reset MUST clear the streak so an unproven peer can re-probe;
  - a would-be disconnect attributable to **local** outbound backpressure MAY extend the
    deadline, but only while the outbound queue has been full for less than
    `request_timeout`; beyond that the peer MUST be disconnected regardless (an unbounded
    extend would let a non-reading peer dodge the timer until the transport idle timeout).

**<a id="wedged-vs-slow"></a>Distinguishing a wedged peer from a merely-slow one.** Both
miss deadlines; only the wedged one is disconnected, with no slowness-based disconnect:

- **Wedged**: completions cease → reliability collapses → the window and its
  reliability-scaled floor bypass ramp to zero → no work, not even the floor. No accepted
  progress ⇒ the liveness deadline (`request_timeout × BLOCK_PROGRESS_TIMEOUT_REQUESTS`)
  disconnects it, even if it stopped reading and holds our outbound full (bounded escape
  above). The seal is the fast reaction; the liveness timer is the authority.
- **Slow but delivering**: bodies still arrive, late. Completions and late-body credits
  keep reliability up, so it's not sealed — its BDP shrinks (window adapts down, peer
  kept) and late bodies keep resetting the liveness deadline.

**Observability (SHOULD).** Each peer SHOULD emit a periodic `block_peer_bbr` heartbeat
(~10 s) with full controller state (window, base round-trip, BDR, phase, delay ceiling,
reliability, no-progress streak) **even while idle**, so a trace distinguishes a settled
controller (window stable, reliability ≈ 1.0) from an oscillating one.

**Numeric safety (MUST).** Arithmetic over untrusted values MUST saturate or be checked;
rates and BDP products MUST be clamped to finite, non-negative values before sizing a
window.

## Defaults

| Knob | Default | Meaning |
| --- | --- | --- |
| `bbr_cwnd_unit` | `bytes` | window budgets header-hinted body bytes |
| `bbr_cwnd_gain_percent` | 300 | window target = 3 × BDP (faster ramp) |
| `bbr_min_cwnd_bytes` | ≈2.5 MB | window floor / cold-start = one max block + headroom (primary lever) |
| `bbr_min_cwnd` | 4 | window floor in blocks (A/B baseline unit) |
| `bbr_reliability_weight_percent` | 100 | goodput discount strength (0 = plain BBR) |
| `bbr_rtprop_window` | 10 s | base-round-trip measurement horizon |
| `bbr_delivery_rate_window` | 10 s | BDR measurement horizon |
| `bbr_probe_rtt_interval` | 10 s | ProbeRtt cadence |
| `bbr_probe_rtt_duration` | 200 ms | drained hold to refresh base round-trip |
| `bbr_delay_gradient_percent` | 150 | queue-building round-trip ratio |
| `initial_block_probe_requests` | 1 | unproven-peer probe budget before first body |
| `max_requests_without_block_progress` | 64 | proven-peer no-progress hard cap |
| `no_progress_peer_cooldown` | 180 s | park after a no-progress disconnect |
| `floor_rescue_timeout` | 2 s | floor leash before rescue |
| `floor_bypass_slots` | 2 | floor borrow beyond the window |
| `max_inflight_block_bytes` | 6 GiB | global in-flight byte ceiling |
| timeout dip / delay-cap down / delay EWMA α / reliability EWMA α | ×0.85 / ×0.9 / 0.25 / 0.1 | (constants) |
| `block_peer_bbr` heartbeat | 10 s | per-peer controller-state trace cadence |
