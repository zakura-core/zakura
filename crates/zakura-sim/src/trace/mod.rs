//! Trace files: a run's records on disk, and reading them back (traces.md).
//!
//! A trace directory holds `trace.jsonl`, whose line 1 is `meta`, line 2 is
//! `init`, then one line per step and `end` last.

pub mod meta;

pub use meta::{BuildInfo, MetaRecord};
