//! Executable message contracts for native block sync.
//!
//! Each contract combines an independent wire oracle with the real production
//! boundary needed for its behavioral claims. The repository-wide catalog and
//! authoring standard live in `docs/specs/native-p2p/README.md`.
//! `GetBlocks` is the first completed contract.

mod get_blocks;
mod lifecycle_regressions;
mod runner;
mod serving_model;
mod serving_regulation;
