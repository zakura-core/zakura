# Header validation

This module validates observable Zcash block-header rules. Validation is split
into two stages:

1. **Prepare** checks facts that depend only on the supplied headers,
   authenticated network policy, the supplied parent height, and local time.
   It returns a sealed `PreparedHeaderBatch`; it does not read or mutate the
   retained header graph.
2. **Contextual admission** binds that prepared result to the live retained
   parent and predecessor history. Only planner admission can turn prepared
   evidence into retained graph state.

This keeps parallel proof-of-work checks independent of storage while ensuring
that branch-sensitive results cannot be detached from the branch that justified
them. A prepared batch is validation evidence, not a commit.

## Context-free preparation

[`prepare_headers`](prepare/pipeline.rs) performs:

| Check | Authority |
| --- | --- |
| Nonempty batch and per-transition size bound | Zakura resource policy |
| Supported signed header version and locally computed full-header hash | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) and [§7.7.2](https://zips.z.cash/protocol/protocol.pdf#difficulty) |
| Checked height inference from the supplied parent height | Local trust rule required by height-dependent consensus checks |
| Height- and network-specific commitment-field interpretation | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) structure; value authentication requires the full block |
| Valid compact target within the network proof-of-work limit | [Zcash §§7.7.3–7.7.4](https://zips.z.cash/protocol/protocol.pdf#diffadjustment) |
| Little-endian header hash at or below its target | [Zcash §7.7.2](https://zips.z.cash/protocol/protocol.pdf#difficulty) |
| Equihash solution shape and proof | [Zcash §7.7.1](https://zips.z.cash/protocol/protocol.pdf#equihash) |
| Accept now or assign `DeferredUntil(nTime - 2 hours)` | [Zcash §7.6 full-validator rule](https://zips.z.cash/protocol/protocol.pdf#blockheader); explicitly nondeterministic, not consensus |
| Exact per-block work | [Zcash §7.7.5](https://zips.z.cash/protocol/protocol.pdf#workdef) |
| Receipt binding the parent, network, and trust-anchor digest | Zakura stale-work protection |

Authenticated custom networks may waive the proof, hash-filter, and contextual
`ThresholdBits` equality checks when proof of work is explicitly disabled.
Mainnet and the default public Testnet cannot receive that waiver.

## Retained-branch contextual admission

The transition planner's
[`admit_prepared_headers`](../transition/planner/event_effects/header_admission.rs)
uses the retained graph and up to 28 predecessor headers to check:

| Check | Authority |
| --- | --- |
| Receipt still matches the live parent, network, and trust anchors | Zakura stale-work protection |
| First-parent and internal batch linkage by `hashPrevBlock` | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Prepared hash, height, and work recompute exactly | Zakura evidence-integrity rule |
| `nBits == ThresholdBits(height)`, including the 17-block averaging window, damping, and Testnet ZIP 205/208 behavior | [Zcash §§7.6 and 7.7.3](https://zips.z.cash/protocol/protocol.pdf#diffadjustment) |
| `nTime` is strictly greater than median-time-past of up to 11 predecessors | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Active 90-minute median-time upper bound, including the Mainnet height-1 exception | [Zcash §7.6](https://zips.z.cash/protocol/protocol.pdf#blockheader) |
| Finality, settled-upgrade pins, checkpoints, completion shape, ownership, and auxiliary provenance | Zakura local chain policy |

The difficulty and time calculations are implemented in
[`contextual/`](contextual/).

## Full-block boundary

Preparation validates only the **structure and height-dependent interpretation**
of the header commitment field. Authenticating `hashMerkleRoot`, the final
Sapling root, ZIP-221 history roots, or NU5+ ZIP-244 block commitments requires
the block body and state.

The full-block consensus path also checks the two-million-byte block limit,
requires at least one transaction, and enforces that the first transaction is
coinbase and later transactions are not. Those rules remain in body sync and
are intentionally outside this header-validation module.

For the atomic transition and commit model around admission, see
[`transition/README.md`](../transition/README.md).

## Difficulty Explained

`nBits` is a compact encoding of a 256-bit target number, `T`.

A mined header is valid only when:

```
header hash ≤ T
```

- Smaller target -> fewer acceptable hashes -> harder mining.
- Larger target -> more acceptable hashes -> easier mining.

`nBits` is essentially a base-256 “mantissa + exponent” encoding of that target, similar to scientific notation.

### The basic adjustment idea

The intended formula is conceptually:

```
new target ≈ recent average target × actual mining time / desired mining time
```

If blocks arrived too quickly:

```
actual time < desired time
-> target becomes smaller
-> mining becomes harder
```

If blocks arrived too slowly:

```
actual time > desired time
-> target becomes larger
-> mining becomes easier
```

Unlike Bitcoin’s historical two-week adjustment, Zcash recalculates this for every block.

### Step-by-step calculation

**1. Determine the desired block spacing**

```
desired spacing = 75 seconds
desired 17-block timespan = 17 × 75 = 1,275 seconds (21 minutes and 15 seconds)
```

**2. Average the previous 17 targets**

Zakura expands the nBits values from blocks:

```
h-17 ... h-1
```

and calculates their exact arithmetic mean:

```
MeanTarget = average(previous 17 targets)
```

Why average targets?

- Because the previous block alone could be unusually easy or hard. A 17-block average provides a smoother baseline and reduces oscillation
- Not median because Targets are validated consensus values, not arbitrary measurements. The arithmetic mean answers: "How hard, on average, were the last 17 blocks expected to be?".

**3. Estimate how long those blocks took**

Using raw first and last timestamps would let one unusual timestamp have too much influence. Zcash therefore compares two timestamp medians:

```
newer median = median(times from h-11 through h-1)
older median = median(times from h-28 through h-18)

actual timespan = newer median − older median
```

This is why up to 28 predecessor headers are required.

The timestamps from `h-17` through `h-12` are deliberately not part of either median. The two 11-block groups provide robust estimates of time near opposite ends of a roughly 17-block interval.

Why medians?

- A median tolerates individual dishonest or inaccurate timestamps. Moving it substantially requires manipulating several timestamps, not just one.

**4. Damp the difference**

Suppose the desired timespan is 1,275 seconds, but the measured timespan is 1,700 seconds.

Without damping, the algorithm would react to the entire 425-second difference. Instead:

```
difference = 1,700 − 1,275 = 425
damped difference = trunc(425 / 4) = 106
damped timespan = 1,275 + 106 = 1,381 seconds
```

Only one quarter of the observed deviation is applied. Why?

- Proof-of-work block arrival is naturally random. Reacting fully after every block would cause difficulty to overcorrect and oscillate.
- Division truncates toward zero, including for negative values.

**5. Bound the adjustment**

The damped timespan is restricted to:

```
minimum = 84% of desired timespan
maximum = 132% of desired timespan
```

For a 1,275-second desired timespan:

```
minimum = 1,071 seconds
maximum = 1,683 seconds
```

Therefore, even an extreme timestamp interval cannot make the target scale by less than approximately 0.84 or more than 1.32 in one adjustment.

These are called `PoWMaxAdjustUp` = 16% and `PoWMaxAdjustDown` = 32% in the specification. Strictly speaking, these percentages bound the timespan/target adjustment. Percentage changes in reciprocal human-readable “difficulty” are not exactly symmetrical.

Why bound it?

- Prevent abrupt changes caused by random block timing.
- Reduce the effect of timestamp manipulation.
- Prevent one unusual window from catastrophically changing difficulty.
- Give miners time to respond to real hash-rate changes.

**6. Scale the average target**

Conceptually:

```
new target =
    MeanTarget × bounded timespan / desired timespan
```

Using the earlier example:

```
bounded timespan = 1,381
desired timespan = 1,275

new target ≈ MeanTarget × 1.083
```

The target becomes about 8.3% larger, so the next block is somewhat easier.

**7. Apply the proof-of-work limit**

The result is capped:

```
new target = min(new target, PoWLimit)
```

`PoWLimit` is the easiest target the network permits.

Without this cap, prolonged slow mining or manipulated timestamps could eventually make proof of work arbitrarily easy.

**8. Convert the target to nBits**

Finally:

```
expected nBits = ToCompact(new target)
```

The candidate header must contain exactly that nBits value.

## Parameter Choice

Zcash combines:

- A 17-block target window, and
- An 11-block median window placed 17 blocks earlier.

```
MeanTarget:
targets from h-17 through h-1       = 17 blocks

Newer median:
times from h-11 through h-1         = 11 blocks

Older median:
times from h-28 through h-18        = 11 blocks
```

The actual timespan is:

```
MedianTime(h) − MedianTime(h − 17)
```

Each `MedianTime(x)` looks backward 11 blocks. Therefore the older median reaches back:

```
(h − 17) − 11 = h − 28
```

Diagram:

```
h-28 ........ h-18 | h-17 ...... h-12 | h-11 ........ h-1 | h
 older median       target-only         newer median      candidate
<---------------- newest 17 targets ------------------>
<------------------ 28 retained predecessors -------->
```

Why this arrangement?

- The two medians represent timestamps approximately 17 block heights apart.
- Eleven timestamps around each endpoint reduce the influence of timestamp outliers.
- The newest 17 targets provide the recent difficulty baseline.
- Some data overlaps: the newest 11 blocks contribute both targets and the newer timestamp median.

So 28 is not an arbitrary smoothing period. It follows directly from:

```
17-block measured interval + 11-block endpoint median = 28 predecessors
```

**What about 17 and 11?**

These are protocol design parameters, not mathematically inevitable values.

- Why 17 targets?

Zcash inherited a DigiShield-style per-block adjustment. A 17-block average balances:

- **Responsiveness**: a smaller window reacts quickly to hash-rate changes.
- **Stability**: a larger window smooths random block-arrival variance.

At Zcash’s original 150-second spacing:

```
17 × 150 seconds = 2,550 seconds = 42.5 minutes
```

So the difficulty baseline covered roughly 42 minutes of mining history. Later Zcash rationale identifies preserving this wall-clock smoothing period as the original purpose of 17.

If the window were much smaller, difficulty would chase random fast/slow streaks. If much larger, a real hash-rate change would leave blocks too fast or slow for longer.

- Why median over 11 timestamps?

The value 11 comes from Bitcoin-style median-time-past handling. Its useful properties are:

- It is odd, so there is one unambiguous middle value.
- One extreme timestamp cannot move the median.
- To fully control the median, an attacker generally needs influence over at least 6 of the 11 timestamps.
- It smooths timestamp noise without introducing an excessively long delay.

Conceptually:

```
11 timestamps → sorted → choose the 6th
```

The exact choice of 11 does not have a known formal proof showing it is optimal. It is an inherited engineering choice that offers a reasonable robustness/latency compromise.

The calculation uses up to 28 predecessors:

- The previous 17 difficulty targets are averaged.
- Two groups of up to 11 timestamps estimate how quickly blocks were produced.
- Sudden changes are damped.
- Difficulty can increase by at most 16% or decrease by at most 32% in one adjustment.
- The result can never become easier than the network proof-of-work limit.

Relevant specification:

- [Zcash §7.7.2](https://zips.z.cash/protocol/protocol.pdf#difficulty)
- [Zcash nBits encoding](https://zips.z.cash/protocol/protocol.pdf#nbits)
- [ZIP-208: Shorter Block Target Spacing](https://zips.z.cash/zip-0208)
- [ZIP-218: 25-second Block Target Spacing](https://github.com/zcash/zips/blob/main/zips/zip-0218.md)

### Explore the difficulty adjustment

The ignored
[`table_driven_difficulty_simulator`](contextual/tests/validation.rs)
prints the mean target, median-based actual timespan, damped and bounded
timespans, expanded target, and expected `nBits` for representative timing,
timestamp-manipulation, target-sample, and Testnet minimum-difficulty cases.
Its current 17/11-window cases are checked against the production
`AdjustedDifficulty` calculation; alternative windows such as 34 targets are
simulation-only.

Run it with:

```sh
cargo test -p zakura-header-chain table_driven_difficulty_simulator -- --ignored --nocapture
```
