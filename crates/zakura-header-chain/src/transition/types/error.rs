//! Leaf error types shared by the typed transition surface.

use thiserror::Error;

/// Failure to read one coherent store view.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StoreError {
    /// A durable collection exceeded its configured recovery bound.
    #[error("header-chain {collection} exceeds recovery limit {limit}")]
    LimitExceeded {
        /// Stable collection name.
        collection: &'static str,
        /// Maximum rows recovery accepts.
        limit: usize,
    },
    /// The store contains internally incoherent rows or indexes.
    #[error("incoherent header-chain store: {0}")]
    Incoherent(&'static str),
    /// A local storage failure made a required row unavailable.
    #[error("header-chain storage unavailable: {0}")]
    Unavailable(&'static str),
}

/// Invalid construction at the transition type boundary.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionTypeError {
    /// Header insertion batches must be nonempty.
    #[error("prepared header batch must be nonempty")]
    EmptyHeaderBatch,
    /// Header insertion batches must fit the frozen engine transition bound.
    #[error("prepared header batch exceeds the engine transition limit")]
    OversizedHeaderBatch,
    /// The prepared path did not contain the new finalized parent.
    #[error("prepared header batch cannot rebase to the requested parent")]
    InvalidPreparedRebase,
    /// Advisory body size exceeded the canonical block limit.
    #[error("invalid advisory body size {0}")]
    InvalidBodySize(u32),
}
