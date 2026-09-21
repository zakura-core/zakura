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

| Metric | main | #1088 |
| --- | --- | --- |
| Client-visible withholds | 1 789 | **67** |
| Templates served | 13 484 | 14 262 |
| Latency p50 / p90 | 39 / 92 ms | 40 / 90 ms |
| Node CPU seconds | 97.2 | 99.6 |
| Blocks produced | 618 | 628 |

At a realistic single-producer block rate, main withheld 11 times and #1088
withheld **zero**.

On Lever B, main withheld 24-28 of 32 templates every round. #1088 withheld **0 of
32 in all six rounds, while all six reorgs still occurred**. The reorg still
happens; the miner keeps getting work.

The rebuild loop converges and does not pay for itself in latency or CPU: both are
flat within noise and throughput is slightly up. `mining.template.rebuilt` recorded
2 673 rebuilds during the storm against 125 withholds that still reached clients,
consistent with those residual cases exhausting `MAX_TEMPLATE_REBUILDS` at a block
every 72 ms — far faster than any real network. The bound behaves as documented.

#1088 also puts the storm below the pre-#1074 baseline, at 67 withholds against 84.
