# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Changed

- Block-sync now keeps its apply backlog in serialized wire form and decodes
  bodies only for the verifier submission window, so decoded memory is bounded
  regardless of backlog depth. Admission accounting charges serialized pools at
  wire size, and the default look-ahead budget is a 1.5 GiB memory target:
  initial-sync memory no longer grows with block era
  ([#190](https://github.com/zakura-core/zakura/pull/190)). The
  `MALLOC_ARENA_MAX` mitigation from
  [#148](https://github.com/zakura-core/zakura/pull/148) remains as the
  complementary allocator-retention layer.

## [1.0.0] - 2026-07-14

Initial release of Zakura.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
