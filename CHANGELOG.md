# CHANGELOG

All notable changes to Zakura are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

Initial release of Zakura.

### Fixed

- Prevent Zakura header sync from matching late or retired header responses to
  newer requests on the same peer stream. Dual-version header-sync negotiation
  now prefers a request-id capable v7 stream while preserving v6 compatibility.
  V7 responses are matched to exact outstanding requests, and retained v6
  streams are retired and reopened on the same connection with a fresh session
  generation after ambiguous timeout/cancellation paths, so delayed responses
  cannot consume unrelated work or trigger stale-anchor recovery. Known
  limitation: only the connection initiator reopens a retired stream, so a
  v6-only peer that dialed this node loses header sync on that connection
  after an ambiguous timeout until it reconnects; header sync on connections
  this node dialed, and all v7 sessions, are unaffected.

Zakura is a fork of the Zcash Foundation's
[Zebra](https://github.com/ZcashFoundation/zebra), forked at Zebra v5.0.0. For
the history of this codebase before the fork, see
[upstream's CHANGELOG](https://github.com/ZcashFoundation/zebra/blob/main/CHANGELOG.md).
