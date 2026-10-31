# Standalone disk result mutations

Apply each patch separately, run its named library test, then restore the patch
and rerun the baseline. A build failure or unrelated timeout is not detection.

| Patch | Crate and test | Required failure |
| --- | --- | --- |
| `late-transaction-admission.patch` | `zakura-node-services`: `standalone_transaction_admission_precedes_decode_and_outlives_clones` | The callback runs before admission. |
| `early-transaction-release.patch` | Same owner test | A live allocation no longer holds its original permit. |
| `private-lookup-pool.patch` | `zakura-state`: `disk_transactions_keep_shared_admission_through_clones_and_database_drop` | A returned disk allocation leaves the database pool unused. |
| `private-iterator-pool.patch` | Same state test | The iterator succeeds while a retained service result occupies the shared pool. |
| `unfunded-transaction-result.patch` | Same state test | A returned raw copy leaves the database pool unused. |
| `retained-replay-input.patch` | Same state test | Historical replay cannot progress with one transaction slot. |
| `private-header-pool.patch` | `zakura-state`: `disk_header_and_raw_block_results_keep_shared_admission` | A returned disk header leaves the database pool unused. |
| `unfunded-header-result.patch` | Same results test | A returned raw header copy leaves the database pool unused. |
| `hidden-find-refusal.patch` | Same results test | An empty Find response hides a capacity refusal. |
| `private-raw-block-pool.patch` | Same results test | A returned serialized block leaves the database pool unused. |

The state test uses real RocksDB and service reads. Weak allocation probes do
not retain payloads. The test checks capacity refusal separately from absent
data, then observes settlement after the final holder and database handles end.
It also checks that format validation reports capacity refusal and succeeds
after release. These native tests do not establish deterministic backend replay
or a decoded-byte bound.
