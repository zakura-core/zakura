# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes

- Replace `AddressBook::bans`'s `Arc<IndexMap<IpAddr, Instant>>` return value
  with the read-only `BannedIps` handle, which is re-exported and provides
  membership checks through `BannedIps::contains`
  ([#286](https://github.com/zakura-core/zakura/pull/286)).

### Changed

- Ordered services can now configure which connection endpoint opens their
  stream and whether an ended session is re-admitted. Reactors can request an
  exact retry deadline, wait for a reactor state change, or retire the session
  for the current connection. Re-admission uses bounded per-kind wake
  scheduling, and negotiated ordered streams must fit within the aggregate
  inbound queue limit.

### Fixed

- Block-sync no-progress liveness now parks only the local block-sync session
  and can re-admit the peer after its cooldown without requiring a transport
  redial. If block sync reaches the tip during that cooldown, the parked session
  stays closed until new body work appears.
- Completed short-lived discovery exchanges are not repeatedly reopened while
  another reactor keeps the shared transport connection alive.
- Repeated no-progress block-sync stalls on one connection now disconnect the
  peer after one bounded re-admission, and discovery teardown cannot leak
  retired-session state.
- Block-sync enforces peer admission caps atomically when installing sessions.
- Skip Equihash and difficulty-filter checks during native header sync for
  configured Testnets with `disable_pow = true`, rather than only Regtest
  ([#289](https://github.com/zakura-core/zakura/pull/289)).

## [3.0.0] - 2026-07-20

### Breaking Changes

- Add an opt-in `Config::expose_peer_addresses` field for unredacted legacy
  peer address labels in peer activity logs and metrics. Downstream exhaustive
  `Config` struct literals must initialize the new field
  ([#258](https://github.com/zakura-core/zakura/pull/258)).
- Added `ConnectionInfo::is_protected_peer`, requiring downstream struct
  literals to specify whether a configured peer is protected from overload
  disconnects.
- Added `HeaderSyncStartError::AnchorAboveVerifiedBlockTip` for invalid
  checkpoint anchors.

### Added

- Added `legacy_peer_request.jsonl` tracing for attributed legacy `FindBlocks`
  responses and block downloads when `[network.zakura] trace_dir` is set.
- Added `ConnectedAddr::is_protected_peer` for identifying configured
  block-gossip and zcashd-compat peers.

### Fixed

- Pruned nodes no longer advertise retained block hashes in legacy `getblocks`
  responses when their corresponding block bodies are unavailable.

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
