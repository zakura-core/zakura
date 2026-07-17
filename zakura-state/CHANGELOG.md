# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-07-17

### Breaking Changes

- Removed the `DerefMut` impl on `CheckpointVerifiedBlock`, making the cached
  checkpoint authorizing-data root structurally tied to the wrapped block
  ([#208](https://github.com/zakura-core/zakura/pull/208)).

### Added

- `CheckpointVerifiedBlock::with_precomputed_auth_data_root`, a consuming API
  for supplying a precomputed authorizing-data root
  ([#208](https://github.com/zakura-core/zakura/pull/208)).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-state/CHANGELOG.md).
