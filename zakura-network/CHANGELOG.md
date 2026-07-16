# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Ordered services can now configure which connection endpoint opens their
  stream and whether an ended session is re-admitted. Reactors can request an
  exact retry deadline, wait for a reactor state change, or retire the session
  for the current connection.

### Fixed

- Block-sync no-progress liveness now parks only the local block-sync session
  and can re-admit the peer after its cooldown without requiring a transport
  redial. If block sync reaches the tip during that cooldown, the parked session
  stays closed until new body work appears.
- Completed short-lived discovery exchanges are not repeatedly reopened while
  another reactor keeps the shared transport connection alive.

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-network/CHANGELOG.md).
