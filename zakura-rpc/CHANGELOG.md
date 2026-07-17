# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-07-17

### Breaking Changes

- `zakura-state`, `zakura-network`, and `zakura-consensus` moved to 2.0.0.
  Their service types appear in this crate's public server signatures, so
  their major versions are part of this crate's API; no APIs defined in this
  crate changed.

### Changed

- Removed the obsolete `ZALLET`-gated external wallet path from the build
  script; setting that environment variable no longer clones or compiles an
  external repository during a `zakura-rpc` build
  ([#206](https://github.com/zakura-core/zakura/pull/206)).

### Security

- `select_mempool_transactions` now reserves the serialized block header,
  transaction count, and maximum pool-modified coinbase size before filling
  the remaining block space, so generated block templates can no longer
  exceed the consensus block size limit
  ([GHSA-95m2-vx53-v2jw](https://github.com/zakura-core/zakura/security/advisories/GHSA-95m2-vx53-v2jw)).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-rpc/CHANGELOG.md).
