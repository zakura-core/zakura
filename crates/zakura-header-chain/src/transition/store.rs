//! Storage failures reported while assembling durable engine inputs.

use thiserror::Error;

/// Failure to read one coherent store view.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StoreError {
    /// The store contains internally incoherent rows or indexes.
    #[error("incoherent header-chain store: {0}")]
    Incoherent(&'static str),
    /// A local storage failure made a required row unavailable.
    #[error("header-chain storage unavailable: {0}")]
    Unavailable(&'static str),
}
