# Standalone disk result mutations

Apply each patch separately, run its named library test, then restore the patch
and rerun the baseline. A build failure or unrelated timeout is not detection.

| Patch | Crate and test | Required failure |
| --- | --- | --- |
| `late-transaction-admission.patch` | `zakura-state`: `disk_reads_reserve_their_recorded_bound_before_decoding` | A transaction read succeeds without room for its stored bound. |
| `early-transaction-release.patch` | `zakura-node-services`: `standalone_transaction_reservation_precedes_decode_and_outlives_clones` | A live allocation no longer holds its measured charge. |
| `private-lookup-pool.patch` | `zakura-state`: `disk_transactions_keep_shared_admission_through_clones_and_database_drop` | A returned disk allocation leaves the shared budget unused. |
| `private-iterator-pool.patch` | Same state test | The iterator succeeds while a retained service result fills the shared budget. |
| `unfunded-transaction-result.patch` | Same state test | A returned copy carries no standalone charge. |
| `retained-replay-input.patch` | Same state test | Historical replay holds one block's transaction while it reads the next. |
| `private-header-pool.patch` | `zakura-state`: `disk_header_and_raw_block_results_keep_shared_admission` | A returned disk header leaves the shared budget unused. |
| `unfunded-header-result.patch` | Same results test | A returned header copy carries no standalone charge. |
| `hidden-find-refusal.patch` | Same results test | An empty Find response hides a capacity refusal. |
| `private-raw-block-pool.patch` | Same results test | A returned serialized block leaves the shared budget unused. |

The state test uses real RocksDB and service reads. Weak allocation probes do
not retain payloads. The test checks capacity refusal separately from absent
data, then observes settlement after the final holder and database handles end.
It also checks that format validation, which is not network work, succeeds while
the budget is full. These native tests do not establish deterministic backend
replay.
