# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2026-07-20

### Added

- Added `NoteCommitmentTrees::update_sprout_tree` for updating the Sprout
  note-commitment tree from a block.

### Changed

- Transparent signature hashes reuse transaction-wide precomputed ZIP 143,
  ZIP 243, and ZIP 244 components across input checks instead of hashing them
  again for every signature
  ([#281](https://github.com/zakura-core/zakura/pull/281)).

## [1.1.0] - 2026-07-17

### Added

- `Block::attributed_memory_size_bytes()` for deterministic decoded-block
  memory attribution
  ([#159](https://github.com/zakura-core/zakura/pull/159)).

### Security

- `Transaction::value_balance` now looks up only the transaction's own spent
  outpoints in the provided UTXO map instead of cloning and converting the
  whole map on every call, so per-transaction callers no longer perform
  quadratic work over a block's spends
  ([GHSA-4g24-549m-hp75](https://github.com/zakura-core/zakura/security/advisories/GHSA-4g24-549m-hp75)).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-chain/CHANGELOG.md).
