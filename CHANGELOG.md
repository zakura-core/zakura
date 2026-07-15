# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

Initial release of Zakura.

### Fixed

- Pruned finalized blocks remain visible to chain-identity queries, including peer
  block-hash responses and RPC confirmation lookups, after their bodies are removed.
- Fixed a permanent block-sync stall on the Zakura P2P stack. Block sync now parks
  only its local service session when a peer misses the no-progress liveness
  deadline, preserving sibling services on the shared connection. The transport
  also re-checks temporary demand refusals for every negotiated ordered service, so
  a session is re-offered after its cooldown or capacity limit clears without
  requiring a transport redial.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
