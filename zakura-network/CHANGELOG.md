# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Track the highest block served to a configured compatibility peer, seeded by
  its handshake height and reset when its connection closes.

### Breaking Changes

- Add an opt-in `Config::expose_peer_addresses` field for unredacted legacy
  peer address labels in peer activity logs and metrics. Downstream exhaustive
  `Config` struct literals must initialize the new field
  ([#258](https://github.com/zakura-core/zakura/pull/258)).

## [3.0.0-rc0] - 2026-07-19

### Breaking Changes

- Added `ConnectionInfo::is_protected_peer`, requiring downstream struct
  literals to specify whether a configured peer is protected from overload
  disconnects.
- Added `HeaderSyncStartError::AnchorAboveVerifiedBlockTip` for invalid
  checkpoint anchors.

### Added

- Added `ConnectedAddr::is_protected_peer` for identifying configured
  block-gossip and zcashd-compat peers.

## [2.0.0] - 2026-07-17

### Breaking Changes

- Removed `truncate_headers_to_byte_budget` from the header-sync API; header
  budget enforcement is internal to the service
  ([#222](https://github.com/zakura-core/zakura/pull/222)).
- Removed `HeaderSyncPeerSession::try_send_get_headers`; request preparation
  and registration are internal to the session lifecycle
  ([#222](https://github.com/zakura-core/zakura/pull/222)).
- The `HeaderSyncEvent::WireMessage` variant is now test-only; production
  inbound messages carry a session generation via
  `HeaderSyncEvent::SessionWireMessage`
  ([#222](https://github.com/zakura-core/zakura/pull/222)).
- Renamed `ZakuraBlockSyncConfig::clamp_inflight_block_bytes_to_floor` to
  `clamp_inflight_block_bytes_to_request_floor`; the clamp now targets the
  per-request floor under wire-honest accounting
  ([#154](https://github.com/zakura-core/zakura/pull/154)).

### Changed

- The default block-sync look-ahead budget
  (`ZakuraBlockSyncConfig::max_reorder_lookahead_bytes`) is now a 1.5 GiB
  resident-memory target: the apply backlog stays in serialized wire form and
  admission accounting charges serialized pools at wire size
  ([#190](https://github.com/zakura-core/zakura/pull/190)).

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-network/CHANGELOG.md).
