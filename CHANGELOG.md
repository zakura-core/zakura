# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

Initial release of Zakura.

### Fixed

- Pruned finalized blocks remain visible to chain-identity queries, including peer
  block-hash responses and RPC confirmation lookups, after their bodies are removed.
- Fixed a permanent block-sync stall on the Zakura P2P stack. A peer that block sync
  parked after missing its no-progress liveness deadline was refused a block-sync
  stream when the transport redialled it inside the cooldown, and that refusal was
  never revisited -- so the peer stayed block-sync-dark for the life of the
  connection. With every peer eventually parked, block sync settled at zero peers and
  body sync stopped for good while header sync kept tracking the tip.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
