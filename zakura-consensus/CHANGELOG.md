# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0] - 2026-07-18

### Breaking Changes

- `zakura-state` moved to 3.0.0. State service types appear in this crate's
  public `init` signatures, so the state major version is part of this crate's
  API; no APIs defined in this crate changed.

## [2.0.0] - 2026-07-17

### Breaking Changes

- `zakura-state` moved to 2.0.0. State service types appear in this crate's
  public `init` signatures, so the state major version is part of this crate's
  API; no APIs defined in this crate changed.

## [1.0.0] - 2026-07-15

First "stable" release. However, be advised that the API may still greatly
change so major version bumps can be common.

## Pre-fork history

This crate was forked from Zebra at v5.0.0. Earlier history is available in the
[upstream changelog](https://github.com/ZcashFoundation/zebra/blob/v5.0.0/zebra-consensus/CHANGELOG.md).
