//! Storage failures reported while assembling durable engine inputs.

use thiserror::Error;

/// Failure to read one coherent store view.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StoreError {
    /// Store rows or indexes are internally incoherent.
    #[error("incoherent header-chain store: {0}")]
    Incoherent(&'static str),
    /// A required row is unavailable because of a local storage failure.
    #[error("header-chain storage unavailable: {0}")]
    Unavailable(&'static str),
}
