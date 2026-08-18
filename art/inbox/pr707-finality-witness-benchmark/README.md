# PR 707 finality witness benchmark

These release-mode RocksDB results use Regtest headers and a finality depth of 1,000.
The baseline is the original PR head at `59d90ed7d50751d58948480c0a2f5292dd89a422`.
The main comparison uses `6c60b658103ab7188ae04cd703998a7f4a42c16f`.

The repaired ordinary advance performs two logical witness point reads and two witness row writes.
Its exact 2,185-byte batch stays below the 16 KiB limit.
The original PR head performed 1,000 logical witness point reads and 1,000 witness row writes.
Its exact batch was 232,669 bytes.
The repaired batch reduces witness batch bytes by 99.06%.

The repaired one-block reorg performs four witness reads and four witness row writes before history eviction.
The repaired 32-block reorg performs 66 witness reads and 66 witness row writes.
These counts show that reorg I/O scales with the replaced suffix.

At 65,540 advances, the history remains bounded at 65,536 rows.
The witness DAG contains 66,535 rows.
Startup audits the full retained history and witness DAG in 335 ms.

The repaired median ordinary latency is 696 microseconds.
The original PR median is 3.23 milliseconds.
The current main median is 486 microseconds.
The repaired path improves the original PR by 78.43%.
It is 43.33% slower than current main.
The investigation attributes the difference to the authenticated durability work that main does not perform: two witness point reads, two witness row writes, and 521 additional batch bytes per finality advance.
Current main could not complete the generated-history restart audit because it lacks the durable historical witness representation.

Run the repaired benchmark with:

```console
ZAKURA_FINALITY_BENCH_ADVANCES=65540 ZAKURA_FINALITY_BENCH_DEPTH=1000 \
  cargo bench -p zakura-state --bench finality_witness --features internal-bench
```
