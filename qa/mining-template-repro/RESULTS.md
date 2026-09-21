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
drift out of the comparison. Measured on `3dc3d847a`, an earlier revision of
#1088 whose loop ran `0..MAX_TEMPLATE_REBUILDS`; the branch now runs
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

**This does not justify changing the constant.** The storm runs a block every
~72 ms. Target spacing is 75 s post-Blossom and 25 s post-NU7
(`POST_NU7_POW_TARGET_SPACING`, ZIP 218), so the storm is roughly 350x faster than
the network this code targets.

A withhold needs a tip change to land inside a template build. At ~40 ms builds and
25 s spacing a single collision runs about 0.16%, and exhausting four rebuilds needs
four in a row, on the order of 1e-11. A shielded coinbase pushing builds to ~500 ms
still leaves it near 1e-7. A bound of 4 is already far more headroom than the real
block rate asks for, and the measured single-producer runs agree: zero withholds.

The right way to read the storm numbers is as an amplifier. It makes a rare race
observable in 45 seconds so two builds can be compared. The ratios between builds
transfer; the absolute counts do not.
