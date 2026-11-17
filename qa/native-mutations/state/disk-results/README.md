# Disk-result admission mutations

Apply each patch separately and restore production source before the next case.
Run the named library test with `cargo test -p <crate> --lib <test>`.
A compilation error or unrelated timeout does not count as detection.

| Patch | Crate and test | Required failure |
| --- | --- | --- |
| `late-decode-admission.patch` | `zakura-node-services`: `decode_admission_precedes_materialization_and_survives_final_holders` | The decoder starts with zero reserved blocks. |
| `private-result-pool.patch` | `zakura-state`: `disk_block_results_keep_shared_admission_through_final_payload_holders` | A returned disk block leaves the shared retained budget unused. |
| `max-size-reservation.patch` | `zakura-state`: `disk_reads_reserve_their_recorded_bound_before_decoding` | A read with room for its recorded bound is refused. |
| `measured-block-reservation.patch` | Same reservation test | A read succeeds without room for its recorded bound. |
| `late-tip-admission.patch` | `zakura-state`: `retained_tip_capacity_refuses_invalidation_before_changing_the_writer` | A refused invalidation changed the writer's selected tip. |

The state tests use real RocksDB reads and check retention after the database
handles end. These native tests do not establish deterministic replay.
