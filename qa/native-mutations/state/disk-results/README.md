# Disk-result admission mutations

Apply each patch separately and restore production source before the next case.
Run the named library test with `cargo test -p <crate> --lib <test>`.
A compilation error or unrelated timeout does not count as detection.

| Patch | Crate and test | Required failure |
| --- | --- | --- |
| `late-decode-admission.patch` | `zakura-node-services`: `decode_admission_precedes_materialization_and_survives_final_holders` | The decoder starts with zero reserved blocks. |
| `private-result-pool.patch` | `zakura-state`: `disk_block_results_keep_shared_admission_through_final_payload_holders` | A second disk read succeeds while another result exhausts the shared pool. |
| `late-tip-admission.patch` | `zakura-state`: `retained_tip_capacity_refuses_invalidation_before_changing_the_writer` | A refused invalidation changed the writer's selected tip. |
| `../../block/decoded-retention/raw-transaction-escape.patch` | `zakura-state`: `disk_block_results_keep_shared_admission_through_final_payload_holders` | The independent payload probe sees two raw transaction references. |

The state test uses real RocksDB reads and checks retention after the database
handles end. These native tests do not establish deterministic replay, a byte
bound, or admission for standalone header and transaction reads.
