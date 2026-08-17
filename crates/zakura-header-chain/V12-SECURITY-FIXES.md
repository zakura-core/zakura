# V12 security fixes

This document maps nine V12 security findings to their corrections and primary regression tests.

| Finding | Correction | Primary regression |
| --- | --- | --- |
| F-225509 | Durable metadata stores the complete network-policy digest. Recovery checks it before collection reads. Version-one migration accepts only Mainnet because that format cannot authenticate a configurable policy. | `version_one_migration_rejects_an_ambiguous_network_policy_without_writing` |
| F-225510 | Configuration rejects conflicting bootstrap, release, and local trust pins. | `engine_config_rejects_conflicting_trust_sources` |
| F-225511 | Full-state events reject deferred and ineligible retained path members. | `verified_chain_change_rejects_inherited_ineligibility` |
| F-225512 | Recovery preserves elapsed deferrals. Startup settles them through one normal planner transition before publication. | `startup_commits_deferred_reevaluation_before_publication` |
| F-225514 | Each transition binds to one process-local engine capability and one source revision. | `transition_installs_only_on_its_exact_source_engine` |
| F-225516 | Full-state path acceptance validates and updates every retained path member. | `accepted_side_path_verifies_every_retained_member` |
| F-225520 | Recovery authenticates each historical current frontier through linked predecessor context or the canonical index. It authenticates a settled selected-tip witness through the canonical index. It authenticates an above-finalized witness through retained rows to the current finalized frontier. | `recovery_authenticates_historical_witnesses_against_the_current_frontier` |
| F-225521 | The planner reports checkpoint finality only when it appends a finality record. | `empty_checkpoint_growth_has_no_finality_effect` |
| F-225522 | Replay handling checks conflicts first, returns `AlreadyApplied` for finality-consumed work second, and checks exact fingerprint equality third. | `replay_conflict_precedes_finality_consumed_header_work` |

## Durable format

The network-policy field changes the provisional header-chain format from version 2 to version 3.

No release tag contains version 2 of this provisional format. The change therefore updates the schema directly.
A version 2 store must rebuild because recovery cannot infer its complete network policy from a network identifier.

Startup still migrates a released Mainnet version 1 store. Mainnet has one fixed policy. Version 1 recorded only the
network identifier, so startup rejects Testnet and Regtest migrations because those identifiers cannot authenticate
the original configurable policy.

## Performance contracts

The corrections preserve these path-specific budgets:

- Policy hashing performs constant work over a fixed field set.
- Trust-pin construction performs one ordered pass over the configured pins.
- Full-state validation performs one pass over the supplied path with indexed node lookups.
- Source-engine installation performs a constant-time capability and revision comparison.
- Finality-effect and replay decisions perform constant work without allocation.
- Startup deferred settlement performs one bounded recovery pass and one normal transition.
- Recovery uses the bounded predecessor context before it performs one canonical-index lookup for an older historical
  current frontier. Recovery performs one canonical-index lookup for each settled headers-only witness. Recovery
  performs one bounded parent walk of at most `local_finality_depth` retained lookups for each witness above the
  finalized frontier.

Ordinary header admission adds no finding-related graph scan. These corrections do not clone the complete graph or
sort all retained nodes or candidate tips.
