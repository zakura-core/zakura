# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

Initial release of Zakura.

### Fixed

- Block sync now reserves the maximum accepted body size before issuing requests,
  preventing underestimated size hints from expanding the resident-memory budget.
  The former `size_deviation_tolerance` setting has been replaced by a fixed 2× policy.
- Pruned finalized blocks remain visible to chain-identity queries, including peer
  block-hash responses and RPC confirmation lookups, after their bodies are removed.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
