//! Read-only store boundary used by pure transition planning.

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, ChainScore, EngineMetadata, EngineSnapshot, FinalityRecord, HeaderNode,
    ValidationLease,
};

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

/// Coherent read snapshot consumed by pure transition planning.
pub trait StoreRead {
    /// Return the atomic externally meaningful snapshot.
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError>;
    /// Return complete singleton metadata from the same version.
    fn metadata(&self) -> Result<EngineMetadata, StoreError>;
    /// Read one exact node.
    fn node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError>;
    /// Read direct children in deterministic raw-hash order.
    fn children(&self, parent: block::Hash) -> Result<Vec<block::Hash>, StoreError>;
    /// Read every retained hash at one height.
    fn hashes_at_height(&self, height: block::Height) -> Result<Vec<block::Hash>, StoreError>;
    /// Read the selected-header projection at one height.
    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError>;
    /// Read the full-state verified projection at one height.
    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError>;
    /// Read all candidate tips and their comparable scores.
    fn candidate_tips(&self) -> Result<Vec<(ChainScore, block::Hash)>, StoreError>;
    /// Read an exact branch-local validation lease.
    fn validation_context(&self, parent: block::Hash) -> Result<ValidationLease, StoreError>;
    /// Read bounded hash-keyed auxiliary deliveries.
    fn aux_deliveries(&self, hash: block::Hash) -> Result<Vec<AuxDelivery>, StoreError>;
    /// Read append-only finality provenance.
    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError>;
}
