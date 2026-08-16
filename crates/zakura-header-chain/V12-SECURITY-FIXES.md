# V12 security fixes

This document maps nine V12 security findings to their corrections and primary regression tests.

| Finding | Correction | Primary regression |
| --- | --- | --- |
| F-225509 | Durable metadata stores the complete network-policy digest. Recovery checks it before collection reads. | `f_225509_network_policy_digest_binds_every_validation_parameter` |
| F-225510 | Configuration rejects conflicting bootstrap, release, and local trust pins. | `f_225510_engine_config_rejects_conflicting_trust_sources` |
| F-225511 | Full-state events reject deferred and ineligible retained path members. | `f_225511_verified_chain_change_rejects_inherited_ineligibility` |
| F-225512 | Recovery preserves elapsed deferrals. Startup settles them through one normal planner transition before publication. | `f_225512_startup_commits_deferred_reevaluation_before_publication` |
| F-225514 | Each transition binds to one process-local engine capability and one source revision. | `f_225514_transition_installs_only_on_its_exact_source_engine` |
| F-225516 | Full-state path acceptance validates and updates every retained path member. | `f_225516_accepted_side_path_verifies_every_retained_member` |
| F-225520 | Recovery authenticates each headers-only selected-tip witness through the independent canonical index. | `f_225520_rocksdb_recovery_rejects_a_forged_headers_only_witness` |
| F-225521 | The planner reports checkpoint finality only when it appends a finality record. | `f_225521_empty_checkpoint_growth_has_no_finality_effect` |
| F-225522 | Finality-consumed work returns `AlreadyApplied` before mutable replay-conflict checks. | `f_225522_finality_consumed_header_work_precedes_replay_conflict` |

## Durable format

The network-policy field changes the provisional header-chain format from version 2 to version 3.

Release v1.2.0 predates the header-chain durable format. No release tag contains version 2 of this provisional format.
The change therefore updates the schema directly. A version 2 store must rebuild because recovery cannot infer its
complete network policy from a network identifier.

## Performance contracts

The corrections preserve these path-specific budgets:

- Policy hashing performs constant work over a fixed field set.
- Trust-pin construction performs one ordered pass over the configured pins.
- Full-state validation performs one pass over the supplied path with indexed node lookups.
- Source-engine installation performs a constant-time capability and revision comparison.
- Finality-effect and replay decisions perform constant work without allocation.
- Startup deferred settlement performs one bounded recovery pass and one normal transition.
- Recovery performs one canonical-index lookup for each accepted headers-only finality witness.

Ordinary header admission adds no finding-related graph scan. These corrections do not clone the complete graph or
sort all retained nodes or candidate tips.
