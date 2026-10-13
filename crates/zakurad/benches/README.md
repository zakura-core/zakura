# Mempool eviction benchmark

Run the optimized benchmark with:

```sh
cargo bench -p zakura --bench mempool_eviction --features mempool-bench --locked
```

To run only the default-sized chain cases:

```sh
cargo bench -p zakura --bench mempool_eviction --features mempool-bench \
  --locked -- 'chain/.*/8000'
```

The benchmark uses actual `Storage` and `VerifiedSet` implementations with
synthetic verified transactions. Proofs and signatures are placeholders:
network downloads, cryptographic verification, and gossip are excluded.

Each fixture fills its configured cost limit. It covers independent
transactions, parent-child-grandchild chains, two parents sharing a child, and
triangles where a shared grandchild must be counted once. The pool sizes are
one eighth of the default maximum transaction count and the full default
maximum: currently 1,000 and 8,000 transactions.

For each fixture, the benchmark measures:

- `indexed`: eviction planning using the maintained ordered index.
- `heap_baseline`: the previous implementation that rebuilt a heap from every
  transaction. It is retained only in the benchmark feature, and its results
  are checked against the indexed implementation before measurement.
- `admission`: `Storage::insert_with_evicted_ids`, including policy checks,
  victim planning, removals, rejection-cache updates, and insertion. Each
  iteration restores the pool outside the timer, including original insertion
  times. It snapshots only the victims, without cloning the full pool.
- `admission_with_room`: successful admission with spare capacity, to expose
  the cost of maintaining the index during ordinary insertion.

Each path covers an insufficient-fee rejection, a small successful admission,
and a successful admission at the default 250,000-byte transaction limit.
Rejections clear between measured admissions so the benchmark exercises actual
admission rather than measuring a rejection-cache hit. Setup, restoration, and
correctness assertions are excluded from admission timings.

Criterion remains a development dependency. The feature-gated storage runner
uses standard timing types and passes named samples to the benchmark binary.
Every selection sample uses an `Instant` timer; its overhead is included.

The `ancestor_chain/admission` cases add a child to a two-transaction chain in
an otherwise full pool at both sizes. They measure successful admission and
rejection when the child outbids a victim but its ancestor-inclusive rate does
not. These cases include the maximum two protected ancestors.

Criterion writes reports beneath `target/criterion`. The shared benchmark
workflow includes this target, including PR comparisons with the `C-benchmark`
label. Compare the 1,000- and 8,000-entry results to detect a return to full-pool
work; avoid treating a fixed wall-clock threshold as portable across machines.

## Example results

On an Apple M4 Max using the optimized bench profile, the 8,000-entry fixtures
produced these small-newcomer timings (microseconds, rounded):

| Shape | Heap selection | Indexed selection | Storage admission |
| --- | ---: | ---: | ---: |
| independent | 782.3 | 0.092 | 1.60 |
| chain | 2760.5 | 0.331 | 4.01 |
| join | 3384.9 | 0.308 | 5.22 |
| triangle | 3523.6 | 0.428 | 4.31 |

These are warm storage measurements, excluding cryptographic verification.
Admission includes committing the eviction and inserting the newcomer; heap
and indexed selection timings cover victim planning only. Machine-dependent
latencies are evidence for this change, not fixed performance requirements.

With ancestor-inclusive admission pricing, the same machine produced these
warm timings for a child with two protected ancestors (microseconds, rounded):

| Pool entries | Rejected child | Admitted child |
| ---: | ---: | ---: |
| 1,000 | 0.55 | 3.14 |
| 8,000 | 0.56 | 3.09 |

These cases include policy checks and, on success, victim removal, cache
updates, and insertion. Their similar timings at both sizes exercise the
bounded ancestor calculation and indexed selection without a full-pool scan.
