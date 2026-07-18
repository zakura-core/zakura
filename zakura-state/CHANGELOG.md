# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0] - 2026-07-18

### Breaking Changes

- Removed `ZakuraDb::spawn_format_change`; database format upgrades now
  complete synchronously during startup
  ([#240](https://github.com/zakura-core/zakura/pull/240)).
- Added variants to the exhaustive `FinalFrontiersGenerationError` and
  `RollbackFinalizedStateError` enums for missing Sprout state and stable-tip
  validation failures
  ([#239](https://github.com/zakura-core/zakura/pull/239),
  [#241](https://github.com/zakura-core/zakura/pull/241)).

### Added

- Added authenticated VCT Sprout-history artifact generation and validation
  APIs, plus an offline generator binary
  ([#241](https://github.com/zakura-core/zakura/pull/241)).
- Added an offline validation API for auditing repaired Sprout anchors in
  archive or pruned Mainnet state databases
  ([#247](https://github.com/zakura-core/zakura/pull/247)).
- Embedded the reviewed Mainnet Sprout-history artifact used by startup repair
  and offline validation
  ([#250](https://github.com/zakura-core/zakura/pull/250)).

### Fixed

- Persist Sprout history during verified-commitment-tree fast sync and validate
  the reconstructed checkpoint frontier
  ([#239](https://github.com/zakura-core/zakura/pull/239)).
- Complete database upgrades before exposing the finalized state service
  ([#240](https://github.com/zakura-core/zakura/pull/240)).
- Automatically repair eligible legacy VCT Sprout history during writable
  startup using the reviewed embedded Mainnet artifact. Startup fails safely
  when repair cannot run or its inputs do not validate
  ([#244](https://github.com/zakura-core/zakura/pull/244),
  [#250](https://github.com/zakura-core/zakura/pull/250)).

## [2.0.0] - 2026-07-17

### Breaking Changes

- Removed the `DerefMut` impl on `CheckpointVerifiedBlock`, making the cached
  checkpoint authorizing-data root structurally tied to the wrapped block
  ([#208](https://github.com/zakura-core/zakura/pull/208)).

### Added

- `CheckpointVerifiedBlock::with_precomputed_auth_data_root`, a consuming API
  for supplying a precomputed authorizing-data root
  ([#208](https://github.com/zakura-core/zakura/pull/208)).

### Security

- `service::check::utxo::remaining_transaction_value` now converts the block's
  spent UTXO set once per block instead of cloning it for every transaction,
  removing quadratic work from transparent value-pool validation that a
  specially crafted block could exploit to stall block verification
  ([GHSA-4g24-549m-hp75](https://github.com/zakura-core/zakura/security/advisories/GHSA-4g24-549m-hp75)).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-state/CHANGELOG.md).
