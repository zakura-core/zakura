# UTXO lookup contention in #918

The local warm-database probe found a small latency cost from overlapping lookups.
Unrelated-read p99 stayed below 0.1 ms, and blocking-queue p99 stayed below 0.03 ms.
Four-transaction workloads completed 16–24% faster. Sixteen-transaction workloads took
roughly the same time or up to 5% longer. These results support keeping shared budgeting
and batching in #919 as follow-up work. They do not establish performance on a cold
production database.

## What changed

#918 changes the timing of existing reads. Each transaction can start up to 64 external
lookups instead of one. It uses the same state requests and database reads. Block-local
inputs do not need those reads. Missing-output waits release their blocking worker after
the database read returns.

The limit multiplies across transactions: 16 transactions can start 1,024 lookups.
The Tower buffer limits requests awaiting dispatch, not all dispatched requests.
Aggregate pressure is therefore a valid review concern.

The aggregate regression test runs 2, 8, and 16 transactions with 1,001 disjoint inputs
each. It withholds responses and checks the exact aggregate window. It then checks
one-for-one refill, cancellation of a failed transaction's window, continued waits in
other transactions, and cancellation when the caller drops all lookups.

## Focused database results

Each workload resolves four or sixteen disjoint sets of 1,001 external inputs through
the real buffered `StateService::AwaitUtxo` path. The baseline permits one outstanding
lookup per transaction. The treatment permits 64. The harness models lookup scheduling;
it does not run transaction verification.

A concurrent probe requests an unrelated missing transaction through `ReadStateService`.
Another probe measures the delay before a no-op blocking task starts. The harness samples
both probes after each 1 ms sleep. It does not impose a fixed request arrival rate.

Ranges below cover two passes and blocking-pool limits of 32 and 512 threads.
Each pass resolves 20 complete workloads. The second pass reverses baseline/treatment
order. Workload completion is a mean; p99 columns describe probe latency within each run.

| Concurrent transactions | Scheduling | Workload completion | Unrelated read p99 | Blocking queue p99 |
| --- | --- | ---: | ---: | ---: |
| None | Idle reference | — | 68–143 µs | 29–59 µs |
| 4 | One lookup per transaction | 49–53 ms | 48–55 µs | 14–18 µs |
| 4 | 64 lookups per transaction | 39–41 ms | 66–84 µs | 21–29 µs |
| 16 | One lookup per transaction | 147–158 ms | 62–74 µs | 22–23 µs |
| 16 | 64 lookups per transaction | 154–164 ms | 68–78 µs | 22–25 µs |

The four-transaction treatment added 17–30 µs to unrelated-read p99 in paired runs.
The largest treatment read latency was 368 µs. The largest treatment blocking-queue
delay was 72 µs. These observations show no sustained queue buildup in this workload.
They do not define an acceptable latency threshold for every deployment.

The run used a Ryzen AI 9 HX 370, Linux, Rust 1.97.0, and four Tokio workers.
The state crate used optimization level 3; dependencies retained their development
settings. The ephemeral RocksDB database contained 16,016 synthetic output indexes.
The harness flushed writes to SST files and warmed every key before measurement.
The indexes do not form a valid chain. Each loaded run collected 376–1,588 probe samples.
The [raw measurements](utxo-lookup-contention.csv) record the September 7, 2026 run.
For idle rows, `set_ms` is the 500 ms reference interval divided by 20; it is not a
lookup measurement.

## What this evidence supports

The earlier benchmarks answered different questions. #918's delayed mock showed that
overlap removes serial waiting. #919's single-set database probe compared batching with
individual requests. Neither measured contention across transactions.

The new probe addresses aggregate scheduling on the existing database path. A full
network experiment also measures transport, synchronization, and cryptographic work.
It can assess production behavior, but it does not isolate lookup scheduling.
Requiring that experiment as the default merge gate would need a specific workload or
latency requirement that this focused probe cannot answer.

Cold-cache I/O, concurrent writes, competing cryptographic blocking tasks, and production
sync remain unmeasured. A reviewer who needs evidence for one of those conditions should
name the condition and acceptable regression. The next test can then target that concern.
The current evidence does not require adopting #919's state API and resolver redesign
before merging #918.

## Reproduce

```sh
cargo +1.97.0 test -p zakura-consensus --locked --lib block_utxo_aggregate
cargo +1.97.0 test -p zakura-state --locked --lib \
  --config 'profile.dev.package.zakura-state.opt-level=3' \
  utxo_state_contention -- --ignored --nocapture
```

The ignored probe prints every run's completion time, sample count, and latency statistics.
It uses the configured temporary directory for its ephemeral database.
