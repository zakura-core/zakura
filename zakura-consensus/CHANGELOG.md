# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0-rc1] - 2026-07-19

### Added

- Added `sapling_prover()`, re-exported at the crate root, returning the
  process-wide bundled Sapling prover so callers reuse one parsed copy of the
  proving parameters.
- Added `TransactionError::SaplingVerificationFailed` and
  `TransactionError::Halo2VerificationFailed` variants so failed shielded
  proof verifications keep their concrete error and mempool misbehavior
  score.

### Changed

- Mempool transactions must satisfy the ZIP-317 fee policy before script and
  proof verification runs; block validation is unchanged.
- Mempool transactions with invalid Orchard or Ironwood proof sizes are
  rejected before proof verification and the sending peer is banned.
- Duplicate transparent-spend and duplicate-nullifier mempool errors no
  longer carry a peer misbehavior penalty.
- Boxed script and signature verification errors are downcast back to their
  concrete `TransactionError` variants instead of being reported as internal
  conversion failures.
- Mempool rejections of NU6.2 branch-ID transactions no longer penalize the
  relaying peer during the first 40 heights after NU6.3 activation; consensus
  validation is unchanged.

## [3.0.0-rc0] - 2026-07-19

### Breaking Changes

- `zakura-state` moved to 3.0.0-rc0. State service types appear in this crate's
  public `init` signatures, so the state major version is part of this crate's
  API; no APIs defined in this crate changed.

## [2.0.0] - 2026-07-17

### Breaking Changes

- `zakura-state` moved to 2.0.0. State service types appear in this crate's
  public `init` signatures, so the state major version is part of this crate's
  API; no APIs defined in this crate changed.

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-consensus/CHANGELOG.md).
