# Blocking read admission mutations

These patches exercise native workers through the production state adapters.
Run them separately from a clean checkout, then restore each patch and rerun its
test. A compilation error or unrelated worker timeout does not count as detection.

| Patch | `zakura-state` library test | Required failure |
| --- | --- | --- |
| `early-worker-release.patch` | `blocking_read_slot_survives_caller_drop_and_worker_panic` | The independently observed live worker no longer owns a slot. |
| `private-read-pool.patch` | `read_workers_are_shared_with_owned_ranges_after_caller_drop` | The ordinary read finishes while the shared worker remains occupied. |
| `control-read-starvation.patch` | `read_workers_are_shared_with_owned_ranges_after_caller_drop` | The chain-tip read cannot progress while the data worker remains occupied. |
| `private-range-pool.patch` | `read_workers_are_shared_with_owned_ranges_after_caller_drop` | The owned range read finishes while the shared worker remains occupied. |

Invoke each test with `cargo +1.97.0 test --locked -p zakura-state --lib <test>`.
The service test uses real RocksDB data and returns a stored genesis block after
capacity becomes available. Its blocked job runs on Tokio's real blocking pool.
These tests do not provide deterministic replay or a bound on retained results,
waiting requests, direct synchronous database users, or writer work.
