# Retired state snapshot cleanup

## Behavior

Publishing a new non-finalized reader view can release the last references to
the previous chain and free its indexes on the state writer. This happens before
the best-tip notification and block response, and can happen again after
finalization. It is memory disposal, not a database snapshot or backup operation.

The writer now takes the retired view from the watch channel and tries to hand
it to one cleanup thread. A zero-capacity channel accepts it only when the worker
is waiting. If the worker is busy or unavailable, the writer disposes of the view
inline. There is no backlog and no wait for handoff capacity. On writer exit, the
channel closes and the worker is joined after its current disposal finishes.

Publication, tip notification, response, and finalization retain their existing
order. Readers keep their shared references. A reader that holds the last
reference can still perform final disposal later on its own thread.

At most one retired view is held for background cleanup. This is a count bound,
not a byte limit: one view can reference several large chains. The optimization
moves work and can temporarily retain memory. It does not eliminate cleanup CPU.

## Metrics

- `state.snapshot_cleanup.offloaded`: successful handoffs.
- `state.snapshot_cleanup.inline`: inline disposals, labeled by `reason` as
  `busy`, `disconnected`, or `unavailable`.
- `state.snapshot_cleanup.background.duration_seconds`: worker disposal time.
- `state.snapshot_cleanup.inline.duration_seconds`: fallback disposal time.

These measure disposal of the publisher's reference. They do not include later
destruction by a reader that outlives it. A high busy-fallback count during
catch-up is expected and keeps retention bounded.

## Reproduction

The ignored tests use actual chain index types and ephemeral test state. They
do not access an existing node database.

```sh
cargo test -p zakura-state --lib snapshot_cleanup --locked
cargo test -p zakura-state --lib --profile ci-tests --locked \
  compare_snapshot_publication -- --ignored --nocapture --test-threads=1
cargo test -p zakura-state --lib --profile ci-tests --locked \
  compare_snapshot_state_replay -- --ignored --nocapture --test-threads=1
```

Set `ZAKURA_SNAPSHOT_BENCH_MODE=inline` or `deferred` to run the publication
variants in separate processes. Measure process CPU and peak resident memory
around the compiled test executable, excluding compilation.

## Initial experiment

Measured on 2026-09-25, Apple M3 Ultra, 28 cores, 96 GiB RAM, Rust 1.97.1,
`ci-tests` profile (optimization level 2, no LTO). Base `920b90154`. Both variants
use the same tracing configuration with no subscriber, isolating this change
from logging overhead. These are local experimental results, not a production
Linux or miner turnaround measurement.

Publication fixtures contain 1,000 block records and 10,000 transaction/UTXO
entries per chain. They are synthetic histories, not replayed mainnet blocks.
Fixture construction is excluded from publication timing. Each case has three
warmup rounds and 20 measured rounds, repeated in three separate processes per
variant with alternating process order. Values below are medians across runs.

| Workload | Inline publication median | Deferred publication median |
| --- | ---: | ---: |
| One chain, worker idle | 125 us | 1.8 us |
| Five chains, worker idle | 684 us | 5.5 us |
| Ten chains, worker idle | 1,420 us | 5.9 us |
| Five chains, bursts of eight | 725 us | 720 us |
| Five chains, retained reader, worker idle | 0.4 us | 3.9 us |

The tradeoffs matter. In the five-chain burst case, publication p95 increased
from 0.825 ms to 2.400 ms while median time through completed disposal of the
whole burst was roughly unchanged (5.845 ms versus 5.805 ms). Concurrent freeing
can contend with inline cleanup. When another reader owns the old chains,
handoff can add overhead to what would have been a cheap reference release.

Across the complete publication benchmark, median process CPU increased from
4.93 to 5.24 seconds, about 6%. Peak resident memory ranged from 1,061–1,173 MiB
inline and 1,041–1,093 MiB deferred. Those process figures include fixture
construction and teardown; they do not establish a node memory ceiling.

The separate contextual-state replay starts from the same 1,000-block test
state and applies the same 64 prepared fake children per variant. It exercises
contextual commits, publication, in-memory finalization, and post-finalization
publication. Across six alternating pairs, median commit-to-publication time
fell from 0.300 to 0.220 ms. Median total time through completed cleanup for all
64 blocks fell from 41.33 to 34.44 ms. This excludes semantic verification,
finalized database writes, mining RPC, and network delivery.

Before production rollout, measure a representative Linux node workload and
block-received-to-mining-template latency, including total CPU and peak memory.
The current evidence supports lower writer latency when disposal is expensive
and the worker is available. It does not establish a universal throughput win
or an end-to-end mining improvement.
