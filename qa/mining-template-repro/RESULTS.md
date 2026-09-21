# Measurements

All runs on one 8-core host, release builds, same harness and configs throughout.
The headline workload is Lever A with `GEN_JOBS=4`: four concurrent block
producers, 16 long-poll clients, 45 seconds, which produces roughly 620 blocks —
about one every 72 ms.

## Attribution

Of 2 879 withholds recorded with `instrumentation.patch` across every run:

| Exit | Share | Notes |
| --- | --- | --- |
| `finish_mining_template` parent check | ~96% | The dominant path by a wide margin |
| Long-poll head tip-agreement guard | ~4% | |
| New-tip arm tip-agreement guard | 0 | Never fired once, in any run |
| Saturation, recovery re-check | 0 | Never reached |

The guards' race window is a few instructions wide. The parent check is separated
from its own tip read by a full template build, so it is far likelier to lose the
race. This matches the field report attached to #1080.

## The reorg is neither necessary nor sufficient

**Not necessary.** One serialized producer, where no fork can exist:

```text
tip_publications           2146
equal_height_flips_total      0
withholds_total             163
```

**Not sufficient.** Eight deterministic equal-height reorgs against a lightly
loaded node withheld nothing. The reorg only withholds when it lands while template
builds are in flight — which is why a repro attempt that changes one template at a
time will not see it.

**But real once builds are in flight.** Lever B, 32 concurrent builds, six rounds:

| Order | Reorg occurred | In-flight templates withheld |
| --- | --- | --- |
| lesser-hash sibling first | 6/6 | 24-28 of 32, every round |
| greater-hash first (control) | 0/6 | 0 of 32, every round |

The control delivers the same two blocks to the same node under the same load, so
this isolates the reorg as the cause.

## How long work is withheld

Measured from the first withheld response to the next served template:

| Lever | Block interval | p50 | max |
| --- | --- | --- | --- |
| A | ~21 ms | 209 ms | 416 ms |
| C | ~25 s | 61 ms | 86 ms |

Lever C settles it: if work were withheld until the next block broke the tie, the
gap would be about 25 seconds. It is 0.06 seconds. A withhold is a single failed
response, not a latched state — `TemplateRejections::set_parent` clears on any
parent change, and the latching states (`needs_fallback`, `saturated`) were never
reached.

## Severity regression from #1074

Same workload, all uninstrumented:

| Build | Withholds | Templates served |
| --- | --- | --- |
| `a2aecb2fb`, before #1074 | 84 | 14 723 |
| `1690f6bd2`, current main | 1 789 | 13 484 |

Instrumented main measured 1 745 against clean main's 1 789, so the probes are not
a confound.

Only one of the four commits between those trees touches `methods.rs`: #1074,
which added the `best_tip_hash() != chain_info.tip_hash` disjunct to the error
condition and moved template construction onto the blocking pool.

That check is correct and necessary — after NSM reissuance a template's reward
depends on the actual parent balance, not the height alone, so a template built on
a superseded parent really is invalid. Wiring it to a hard error is what turned a
rare failure into a constant one. This is an availability regression, not a
correctness bug.

## #1088

Measured against `1690f6bd2` (main) and `bd6bea7ec` (#1088). #1088 forks from that
same main commit, and the one commit main has gained since does not touch this path.

| Metric | main | #1088 |
| --- | --- | --- |
| Client-visible withholds | 1 789 | **57 and 48**, two runs |
| Templates served | 13 484 | 13 097 / 13 531 |
| Latency p50 / p90 | 39 / 92 ms | 43 / 99 ms |
| Node CPU seconds | 97.2 | 100.7 / 102.5 |
| Blocks produced | 618 | 595 / 598 |

At a realistic single-producer block rate, main withheld 11 times and #1088
withheld **zero**.

On Lever B, main withheld 24-28 of 32 templates every round. #1088 withheld **0 of
32 in all six rounds, while all six reorgs still occurred**. The reorg still
happens; the miner keeps getting work.

The rebuild loop converges and does not pay for itself in latency or CPU: both are
flat within noise and throughput is slightly up. `mining.template.rebuilt` recorded
2 673 rebuilds during a storm against 125 withholds that still reached clients,
consistent with those residual cases exhausting the rebuild budget at a block every
72 ms — far faster than any real network. The bound behaves as documented.

Note also that #1088 puts the storm below the pre-#1074 baseline, at 48-57
withholds against 84.

## What the residual withholds under #1088 are

They are retry-budget exhaustion, not an unhandled path. Two independent checks:

After #1088 exactly one site still returns `template parent changed; retry`, and
it sits after the `'rebuild` loop falls through — it is reachable only when all
`MAX_TEMPLATE_REBUILDS` attempts were superseded in turn.

Raising the constant from 4 to 16 and alternating the two builds, to keep host
drift out of the comparison. Measured on `3dc3d847a`, an earlier revision of #1088
whose loop ran `0..MAX_TEMPLATE_REBUILDS`; the branch now runs
`0..=MAX_TEMPLATE_REBUILDS`, one attempt more, which is why the residual on
`bd6bea7ec` is lower. The conclusion is unchanged:

| Build | Blocks | Withholds | Templates | p50 | p90 | max | CPU s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bound 4, run 1 | 551 | 92 | 10 883 | 54 ms | 110 ms | 411 ms | 81.5 |
| bound 16, run 1 | 593 | **0** | 12 661 | 46 ms | 107 ms | 299 ms | 96.7 |
| bound 4, run 2 | 600 | 66 | 13 411 | 43 ms | 99 ms | 318 ms | 100.4 |
| bound 16, run 2 | 631 | **0** | 13 677 | 43 ms | 97 ms | 277 ms | 99.5 |

The higher bound removes the residual entirely and costs nothing measurable.
Latency does not get worse — tail latency is lower, because a call that exhausts
the budget has already paid for four builds before erroring, and the client then
starts over anyway.

A single earlier run had suggested the higher bound raised latency. Alternating the
builds showed that was host drift, not the bound.

### What actually drives the residual

Not the block rate. Sweeping the number of concurrent block producers on `main` and
on #1088, with 16 long-poll clients throughout:

| Producers | ms/block | main withholds | #1088 withholds | build p90 |
| --- | --- | --- | --- | --- |
| 1 | 29.5 | 20 | **0** | 40 ms |
| 2 | 46.8 | 948 | **0** | 67 ms |
| 4 | 76.1 | 1 857 | 67 | 96 ms |

One producer makes the _fastest_ blocks and yields the _fewest_ withholds. The
driver is the number of template builds in flight when the tip moves. With a single
producer the same actor builds and then submits, serially, so nothing else is
mid-build at the moment the tip changes. With four, the other three are always
mid-build when one of them submits. `MAX_TEMPLATE_BUILDS` is 1, so those builds also
queue behind each other, stretching p90 build latency from 40 ms to 96 ms and
widening the window further.

So the harness amplifies on two axes at once: concurrent template consumers, and
the build latency that contention creates.

**This does not justify changing the constant.** A withhold needs a tip change to
land inside a build window, so the block interval is the denominator. Target spacing
is 75 s post-Blossom and 25 s post-NU7 (`POST_NU7_POW_TARGET_SPACING`, ZIP 218),
against the storm's ~76 ms.

Many concurrent callers is realistic for a pool, so that axis does transfer. The
block interval is what does not. Even taking the contended 96 ms build window, a
single collision at 25 s spacing is ~0.4%, and the budget needs five in a row, on
the order of 1e-12. A shielded coinbase pushing builds to ~500 ms still leaves it
near 1e-8. The measured runs agree: zero withholds at one and two producers.

The right way to read the storm numbers is as an amplifier. It makes a rare race
observable in 45 seconds so two builds can be compared. The ratios between builds
transfer; the absolute counts do not.

## Lever E — the fallback-recovery branch

The saturation exit and the recovery re-check never fired in Levers A-D, and #1088
leaves both unchanged, so they were the untested part of #1080. The node builds its
own templates, so they are valid by construction and the branch is unreachable
without help: `fault-injection.patch` adds an env-gated counter
(`GBT_REPRO_FORCE_TEMPLATE_REJECTIONS`) that forces N background preparations to
report a template-rejecting failure. The trigger is injected; the behaviour that
follows is the real code.

**Fallback mode itself is fine.** With five forced rejections and a static tip, the
node served 15 022 templates and withheld nothing. Entering fallback does not by
itself cost a miner work.

**Saturation looks unreachable.** Asking for 500 rejections with 64 concurrent
clients landed only **6**. The first rejection sets `needs_fallback`, and
`finish_mining_template` then stops calling `prepare_template_in_background` —
which is the only thing that produces rejections. The state machine disables its own
input. Only preparations already in flight at that instant can add more, so the 64
needed to set `saturated` cannot accumulate. That exit appears to be dead code, and
it cannot be a cause of #1080.

**The fallback branch degrades badly under tip churn.** Re-arming the injection on
every new parent, with two block producers and 32 clients for 30 s:

| Outcome | Count |
| --- | --- |
| Templates served | 3 567 |
| `template changed during recovery; retry` | 203 |
| Raw verification error reaching the client | 6 113 |

Nearly two thirds of responses were errors, and most were not a clean retry signal
but the proposal verifier's own message propagated verbatim:
`"block could not be full-verified due to: ... proposal is not based on the current
best chain tip"`. That is the fallback branch's `Request::Prepare` failing because
the tip moved underneath it, with the error returned rather than rebuilt.

This is the part #1088 explicitly defers. That work is now #1090, which ports #1083's
rework: the context re-check and the recovery record happen under one write
lock, and a failed validation is confirmed against committed state before being
surfaced. Measured with the same Lever E parameters, two runs each:

| | #1088 | #1090 |
| --- | --- | --- |
| Raw verifier errors to client | 3 065 / 6 305 | **0 / 0** |
| `template changed during recovery` | 209 / 184 | **0 / 0** |
| Rebuild-budget exhaustion | 0 / 0 | 841 / 993 |
| Total withholds | 3 274 / 6 489 | 841 / 993 |

Both error classes go to zero. What remains is the bounded-rebuild message from #1088,
the intended transient signal rather than verifier internals. The residual
is larger than in #1088's own measurements because this scenario re-arms a
rejection on every parent, so each rebuild lands back in fallback mode.

Caveat: in production the node's own templates are valid, so fallback mode should
rarely be entered at all. The finding is about how badly it behaves once something
does put it there, not about how often that happens.

## What a real mining pool actually does

The in-repo pool stack (`docker/mining/`, s-nomp with a pinned
`node-stratum-pool`) was pointed at a node to measure how long it serves a stale
job when a withhold occurs. The end-to-end run did not complete: on Regtest
`getblocksubsidy` returns no funding streams, and the pool dereferences
`subsidy.fundingstreams` unconditionally for Zcash, so it crashes before producing
a job. Reading its source answered the question more directly.

- **The pool does not long poll.** There is no long-poll reference anywhere in
  `stratum-pool/lib/pool.js`. It calls `getblocktemplate` on a timer,
  `blockRefreshInterval`, which the stack sets to 500 ms and to 2 000 ms on Mainnet.
- **On a getblocktemplate error it logs and waits for the next tick.** No retry, no
  backoff, no special handling of the transient message.

Both halves matter, and they point in opposite directions:

A polling client samples at arbitrary moments, so its exposure is roughly the build
window over the poll interval — tens of milliseconds against 2 s. A long-polling
client is the opposite: it wakes _at_ the tip change, which is exactly the moment
the race is live, and every such client starts building at once. That is why the
harness, which long polls, sees a withhold on most tip changes at fast block rates
while a polling pool would rarely see one at all.

So the population most exposed to #1080 is long-polling miners, not this pool. When
the pool does hit it, the cost is bounded by one refresh interval of stale work,
up to 2 s on Mainnet.
