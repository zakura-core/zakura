# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `produce_settled_final_frontiers_bytes`: produce VCT final-frontier bytes at
  a height below the finalized tip after proving Sprout is settled across the
  window, with two new `FinalFrontiersGenerationError` variants
  (`SproutChangedAboveRequestedHeight`, `MissingBlockInSproutWindow`).

## [3.0.0] - 2026-07-20

### Breaking Changes

- Changed Sprout tip-tree lookups to return `Result` when the tree is missing,
  and made tree-batch preparation report validation errors.
- Removed `ZakuraDb::spawn_format_change`; database format upgrades now finish
  during initialization.
- Added variants to public finalized-state error enums for missing Sprout data,
  tip changes, and invalid VCT repair state.

### Added

- Added authenticated VCT Sprout-history artifact generation and validation
  APIs, plus an offline generator binary and exact-versioned crates.io
  packaging that keeps the embedded repair bytes outside `zakura-state` and
  reuses one validated decode throughout startup repair.

### Changed

- `FindBlockHashes` now returns only the contiguous prefix of block hashes whose
  full bodies are serveable, while other chain-identity reads continue using
  retained indexes after pruning.

### Fixed

- Contextual difficulty validation returns the consensus proof-of-work limit
  for all candidate heights at or below the averaging window, including
  height 17 with a full difficulty context.

### Security

- The state service now accepts children of a block that was accepted and has the same
  block header hash (due to [ZIP-244](https://zips.z.cash/zip-0244)) as a block that
  was previously rejected
  ([GHSA-8gxx-hc65-vv82](https://github.com/ZcashFoundation/zebra/security/advisories/GHSA-8gxx-hc65-vv82)).

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
