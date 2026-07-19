# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `zakura-checkpoints` offline mode (`--state-cache-dir`, `--full-list`,
  `--mainnet-frontier-output`, behind the new `zakura-checkpoints-offline`
  feature): export Mainnet checkpoints and the coupled VCT frontier artifact
  from a quiesced state database without a running node.

## [1.0.2-rc0] - 2026-07-19

### Changed

- Updated the bundled tools to the release-candidate Zakura crate graph; no
  APIs defined in this crate changed.

## [1.0.1] - 2026-07-17

### Changed

- `zakura-rpc` moved to 2.0.0 (internal dependency of the bundled tools; no
  API defined in this crate changed).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-utils/CHANGELOG.md).
