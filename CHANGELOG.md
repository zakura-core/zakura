# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Fixed

- Block sync now preserves peer accountability when duplicate requests become
  obsolete, without disconnecting healthy peers during local pipeline stalls.

## [1.0.0] - 2026-07-15

Initial release of Zakura.

### Fixed

- Header sync now keeps timed-out ranges in a bounded, single-owner work queue,
  retries them indefinitely with short peer-local avoidance, and commits
  pipelined responses in height order.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
