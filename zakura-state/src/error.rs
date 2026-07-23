//! Error types for Zebra's state.

use std::{path::PathBuf, sync::Arc};

use chrono::{DateTime, Utc};
use derive_new::new;
use thiserror::Error;

use zakura_chain::{
    amount::{self, NegativeAllowed, NonNegative},
    block,
    history_tree::HistoryTreeError,
    ironwood, orchard, sapling, sprout, transaction, transparent,
    value_balance::{ValueBalance, ValueBalanceError},
    work::difficulty::CompactDifficulty,
};

use crate::{constants::MIN_TRANSPARENT_COINBASE_MATURITY, HashOrHeight, KnownBlock};

/// A wrapper for type erased errors that is itself clonable and implements the
/// Error trait
#[derive(Debug, Error, Clone)]
#[error(transparent)]
pub struct CloneError {
    source: Arc<dyn std::error::Error + Send + Sync + 'static>,
}

impl From<CommitSemanticallyVerifiedError> for CloneError {
    fn from(source: CommitSemanticallyVerifiedError) -> Self {
        let source = Arc::new(source);
        Self { source }
    }
}

impl From<BoxError> for CloneError {
    fn from(source: BoxError) -> Self {
        let source = Arc::from(source);
        Self { source }
    }
}

/// A boxed [`std::error::Error`].
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The finalized database has blocks but no persisted Sprout tip frontier.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("missing Sprout note commitment tree at finalized tip {tip:?}")]
pub struct MissingSproutTipTree {
    /// The finalized tip whose Sprout frontier is missing.
    pub tip: block::Height,
}

/// An error describing why opening the finalized state database failed.
///
/// These errors are recoverable open-time failures that the caller can report,
/// as opposed to invariant violations that indicate a bug.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StateInitError {
    /// A read-only state was requested, but the configured cache directory is
    /// missing or unreadable.
    ///
    /// A read-only secondary instance must never create the primary's cache
    /// directory, so a missing or unreadable directory is a fatal configuration
    /// error rather than something to be created.
    #[error(
        "cannot open read-only state: cache directory {path:?} is missing or unreadable. \
         Hint: a read-only state requires an existing Zakura cache directory; check that the \
         state cache_dir in the Zakura config points at a running Zakura node's cache directory"
    )]
    ReadOnlyCacheDirUnreadable {
        /// The configured cache directory that could not be read.
        path: PathBuf,
        /// The underlying I/O error returned while reading the directory.
        source: std::io::Error,
    },

    /// A read-only state was requested, but no database exists at the expected
    /// path.
    ///
    /// A read-only secondary instance cannot create a database, so the absence
    /// of an existing database is a fatal configuration error.
    #[error(
        "cannot open read-only state: no database found at {path:?}. \
         Hint: a read-only state requires an existing finalized database created by a running \
         Zakura node; check that the state cache_dir in the Zakura config points at that node's \
         cache directory"
    )]
    ReadOnlyDatabaseNotFound {
        /// The database path at which no database was found.
        path: PathBuf,
    },

    /// A read-only state was requested together with an ephemeral database.
    ///
    /// A read-only secondary follows another process's primary database and must
    /// never delete it, whereas an ephemeral database deletes its files on drop. The
    /// two are mutually exclusive, so requesting both is a fatal configuration error.
    #[error(
        "cannot open read-only state: an ephemeral database was also requested. \
         Hint: a read-only state follows an existing Zakura node's database and must not \
         delete it; set `ephemeral = false`, or do not request a read-only state"
    )]
    ReadOnlyEphemeralConflict,

    /// A Mainnet VCT database written before the Sprout-history repair format is unsafe to use.
    #[error(
        "cannot open {mode} state: this verified-commitment-tree database may be missing historical Sprout anchors. \
         {reason}. Hint: reopen the database writable with a build containing the reviewed repair artifact, or discard it and resync"
    )]
    VctSproutHistoryRepairRequired {
        /// Whether this was a writable primary or read-only secondary open.
        mode: &'static str,
        /// Why startup could not perform the repair.
        reason: &'static str,
    },

    /// The embedded repair inputs or the local marker-scoped canonical state did not validate.
    #[error(
        "cannot open state: verified-commitment-tree Sprout-history repair validation failed: {reason}. \
         Hint: use a build with mutually consistent checkpoint, frontier, and repair artifacts; \
         if the local canonical marker binding is invalid, discard the database and resync"
    )]
    VctSproutHistoryRepairInvalid {
        /// The failed build-level or database-level validation.
        reason: String,
    },
}

/// An error describing why a block could not be queued to be committed to the state.
#[derive(Debug, Error, Clone, PartialEq, Eq, new)]
pub enum CommitBlockError {
    #[error("block hash is a duplicate: already in {location}")]
    /// The block is a duplicate: it is already queued or committed in the state.
    Duplicate {
        /// Hash or height of the duplicated block.
        hash_or_height: Option<HashOrHeight>,
        /// Location in the state where the block can be found.
        location: KnownBlock,
    },

    /// Contextual validation failed.
    #[error("could not contextually validate semantically verified block")]
    ValidateContextError(#[from] Box<ValidateContextError>),

    /// The body mutation could not commit its matching fork-aware header transition.
    #[error("could not commit matching header-chain transition: {error}")]
    HeaderChainError {
        /// Stable local error diagnostic; never peer-attributed.
        error: String,
    },

    /// The write task exited (likely during shutdown).
    #[error("block commit task exited. Is Zakura shutting down?")]
    #[non_exhaustive]
    WriteTaskExited,
}

impl CommitBlockError {
    /// Returns `true` if this is definitely a duplicate commit request.
    /// Some duplicate requests might not be detected, and therefore return `false`.
    pub fn is_duplicate_request(&self) -> bool {
        matches!(self, CommitBlockError::Duplicate { .. })
    }

    /// Returns the state location for duplicate commit requests.
    pub fn duplicate_location(&self) -> Option<&KnownBlock> {
        match self {
            CommitBlockError::Duplicate { location, .. } => Some(location),
            _ => None,
        }
    }

    /// Returns the missing VCT supplied-root height for retryable root-fetch stalls.
    pub fn vct_supplied_root_unavailable_height(&self) -> Option<block::Height> {
        match self {
            CommitBlockError::ValidateContextError(error) => {
                error.vct_supplied_root_unavailable_height()
            }
            _ => None,
        }
    }

    /// Returns the height for any retryable VCT root stall (absent/evicted root, or one
    /// not yet verifiable for lack of a stored successor header). See
    /// [`ValidateContextError::vct_retryable_height`].
    pub fn vct_retryable_height(&self) -> Option<block::Height> {
        match self {
            CommitBlockError::ValidateContextError(error) => error.vct_retryable_height(),
            _ => None,
        }
    }

    /// Returns a suggested misbehaviour score increment for a certain error.
    pub fn misbehavior_score(&self) -> u32 {
        0
    }

    /// Classify this commit result before supplier identity and stable evidence are attached.
    pub fn body_verification_class(&self) -> zakura_header_chain::BodyVerificationClass {
        use zakura_header_chain::{BodyVerificationClass, TransientBodyFailureKind};

        match self {
            Self::Duplicate { .. } => BodyVerificationClass::Duplicate,
            Self::ValidateContextError(error) => error.body_verification_class(),
            Self::HeaderChainError { .. } => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::Storage)
            }
            Self::WriteTaskExited => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
        }
    }
}

/// An error describing why a `CommitSemanticallyVerified` request failed.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("could not commit semantically-verified block")]
pub struct CommitSemanticallyVerifiedError(#[from] CommitBlockError);

impl CommitSemanticallyVerifiedError {
    /// Returns the [`CommitBlockError`] describing why the commit failed.
    pub fn inner(&self) -> &CommitBlockError {
        &self.0
    }

    /// Returns the state location for duplicate commit requests.
    pub fn duplicate_location(&self) -> Option<&KnownBlock> {
        self.0.duplicate_location()
    }
}

impl From<ValidateContextError> for CommitSemanticallyVerifiedError {
    fn from(value: ValidateContextError) -> Self {
        Self(CommitBlockError::ValidateContextError(Box::new(value)))
    }
}

#[derive(Debug, Error)]
pub enum LayeredStateError<E: std::error::Error + std::fmt::Display> {
    #[error("{0}")]
    State(E),
    #[error("{0}")]
    Layer(BoxError),
}

impl<E: std::error::Error + 'static> From<BoxError> for LayeredStateError<E> {
    fn from(err: BoxError) -> Self {
        match err.downcast::<E>() {
            Ok(state_err) => Self::State(*state_err),
            Err(layer_error) => Self::Layer(layer_error),
        }
    }
}

/// An error describing why a `CommitCheckpointVerifiedBlock` request failed.
#[derive(Debug, Error, Clone)]
#[error("could not commit checkpoint-verified block")]
pub struct CommitCheckpointVerifiedError {
    #[source]
    inner: CommitBlockError,
    vct_failure: Option<VctCommitFailure>,
}

/// Exact VCT verification input implicated by a failed checkpoint commit.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum VctCommitFailure {
    /// A direct current-root check or fold failed.
    CurrentRoots,
    /// The successor boundary rejected the candidate containing the current roots.
    SuccessorBoundary,
}

impl CommitCheckpointVerifiedError {
    /// Returns the [`CommitBlockError`] describing why the commit failed.
    pub fn inner(&self) -> &CommitBlockError {
        &self.inner
    }

    /// Returns the state location for duplicate commit requests.
    pub fn duplicate_location(&self) -> Option<&KnownBlock> {
        self.inner.duplicate_location()
    }

    /// Returns the missing VCT supplied-root height for retryable root-fetch stalls.
    pub fn vct_supplied_root_unavailable_height(&self) -> Option<block::Height> {
        self.inner.vct_supplied_root_unavailable_height()
    }

    /// Returns the height for any retryable VCT root stall (absent/evicted root, or one
    /// not yet verifiable for lack of a stored successor header). See
    /// [`ValidateContextError::vct_retryable_height`].
    pub fn vct_retryable_height(&self) -> Option<block::Height> {
        self.inner.vct_retryable_height()
    }

    pub(crate) fn with_vct_failure(mut self, failure: VctCommitFailure) -> Self {
        self.vct_failure = Some(failure);
        self
    }

    pub(crate) fn vct_failure(&self) -> Option<VctCommitFailure> {
        self.vct_failure
    }
}

impl From<CommitBlockError> for CommitCheckpointVerifiedError {
    fn from(inner: CommitBlockError) -> Self {
        Self {
            inner,
            vct_failure: None,
        }
    }
}

impl From<ValidateContextError> for CommitCheckpointVerifiedError {
    fn from(value: ValidateContextError) -> Self {
        CommitBlockError::ValidateContextError(Box::new(value)).into()
    }
}

/// An error describing why a `InvalidateBlock` request failed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InvalidateError {
    /// The state is currently checkpointing blocks and cannot accept invalidation requests.
    #[error("cannot invalidate blocks while still committing checkpointed blocks")]
    ProcessingCheckpointedBlocks,

    /// Sending the invalidate request to the block write task failed.
    #[error("failed to send invalidate block request to block write task")]
    SendInvalidateRequestFailed,

    /// The invalidate request was dropped before processing.
    #[error("invalidate block request was unexpectedly dropped")]
    InvalidateRequestDropped,

    /// The block hash was not found in any non-finalized chain.
    #[error("block hash {0} not found in any non-finalized chain")]
    BlockNotFound(block::Hash),

    /// The staged state mutation disagreed with or could not commit its header transition.
    #[error("could not commit matching header-chain invalidation: {error}")]
    HeaderChain {
        /// Stable local error diagnostic; never peer-attributed.
        error: String,
    },
}

/// An error describing why a `ReconsiderBlock` request failed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReconsiderError {
    /// The block is not found in the list of invalidated blocks.
    #[error("Block with hash {0} was not previously invalidated")]
    MissingInvalidatedBlock(block::Hash),

    /// The block's parent is missing from the non-finalized state.
    #[error("Parent chain not found for block {0}")]
    ParentChainNotFound(block::Hash),

    /// There were no invalidated blocks when at least one was expected.
    #[error("Invalidated blocks list is empty when it should contain at least one block")]
    InvalidatedBlocksEmpty,

    /// The state is currently checkpointing blocks and cannot accept reconsider requests.
    #[error("cannot reconsider blocks while still committing checkpointed blocks")]
    CheckpointCommitInProgress,

    /// Sending the reconsider request to the block write task failed.
    #[error("failed to send reconsider block request to block write task")]
    ReconsiderSendFailed,

    /// The reconsider request was dropped before processing.
    #[error("reconsider block request was unexpectedly dropped")]
    ReconsiderResponseDropped,

    /// Replaying an invalidated block into the restored chain failed contextual
    /// validation.
    #[error("replaying a previously invalidated block failed contextual validation: {0}")]
    ReplayFailed(#[source] ValidateContextError),

    /// The finalized parent chain is missing its Sprout tip frontier.
    #[error(transparent)]
    MissingSproutTipTree(#[from] MissingSproutTipTree),

    /// The staged state mutation disagreed with or could not commit its header transition.
    #[error("could not commit matching header-chain reconsideration: {error}")]
    HeaderChain {
        /// Stable local error diagnostic; never peer-attributed.
        error: String,
    },
}

/// An error describing why a block failed contextual validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum ValidateContextError {
    #[error(transparent)]
    MissingSproutTipTree(#[from] MissingSproutTipTree),

    #[error("block hash {block_hash} was previously invalidated")]
    #[non_exhaustive]
    BlockPreviouslyInvalidated { block_hash: block::Hash },

    #[error("block parent not found in any chain, or not enough blocks in chain")]
    #[non_exhaustive]
    NotReadyToBeCommitted,

    #[error(
        "verified-commitment-trees fast path has no valid supplied root for height \
         {height:?}: the note-commitment frontier is frozen, so this block cannot be \
         committed until a verifiable root is fetched from a peer (retryable)"
    )]
    #[non_exhaustive]
    VctSuppliedRootUnavailable { height: block::Height },

    #[error(
        "verified-commitment-trees fast path cannot yet verify the supplied root for height \
         {height:?}: no successor header is stored to confirm it against the header chain, and \
         committing it unverified would persist a root that is only checked one block later \
         (irreversibly, once on disk). Commit is deferred until the successor header arrives (retryable)"
    )]
    #[non_exhaustive]
    VctSuppliedRootAwaitingSuccessor { height: block::Height },

    #[error(
        "checkpoint block at {height:?} has authorizing-data root {actual:?}, but its cached \
         header prevalidation requires {expected:?}"
    )]
    #[non_exhaustive]
    VctBlockAuthDataRootMismatch {
        height: block::Height,
        expected: block::merkle::AuthDataRoot,
        actual: block::merkle::AuthDataRoot,
    },

    #[error(
        "locally reconstructed Sprout root at the VCT handoff height {height:?} is \
         {actual:?}, but the embedded handoff frontier requires {expected:?}"
    )]
    #[non_exhaustive]
    VctSproutHandoffRootMismatch {
        height: block::Height,
        expected: sprout::tree::Root,
        actual: sprout::tree::Root,
    },

    #[error("block height {candidate_height:?} is lower than the current finalized height {finalized_tip_height:?}")]
    #[non_exhaustive]
    OrphanedBlock {
        candidate_height: block::Height,
        finalized_tip_height: block::Height,
    },

    #[error("block height {candidate_height:?} is not one greater than its parent block's height {parent_height:?}")]
    #[non_exhaustive]
    NonSequentialBlock {
        candidate_height: block::Height,
        parent_height: block::Height,
    },

    #[error("block time {candidate_time:?} is less than or equal to the median-time-past for the block {median_time_past:?}")]
    #[non_exhaustive]
    TimeTooEarly {
        candidate_time: DateTime<Utc>,
        median_time_past: DateTime<Utc>,
    },

    #[error("block time {candidate_time:?} is greater than the median-time-past for the block plus 90 minutes {block_time_max:?}")]
    #[non_exhaustive]
    TimeTooLate {
        candidate_time: DateTime<Utc>,
        block_time_max: DateTime<Utc>,
    },

    #[error("block difficulty threshold {difficulty_threshold:?} is not equal to the expected difficulty for the block {expected_difficulty:?}")]
    #[non_exhaustive]
    InvalidDifficultyThreshold {
        difficulty_threshold: CompactDifficulty,
        expected_difficulty: CompactDifficulty,
    },

    #[error("transparent double-spend: {outpoint:?} is spent twice in {location:?}")]
    #[non_exhaustive]
    DuplicateTransparentSpend {
        outpoint: transparent::OutPoint,
        location: &'static str,
    },

    #[error("missing transparent output: possible double-spend of {outpoint:?} in {location:?}")]
    #[non_exhaustive]
    MissingTransparentOutput {
        outpoint: transparent::OutPoint,
        location: &'static str,
    },

    #[error("out-of-order transparent spend: {outpoint:?} is created by a later transaction in the same block")]
    #[non_exhaustive]
    EarlyTransparentSpend { outpoint: transparent::OutPoint },

    #[error(
        "unshielded transparent coinbase spend: {outpoint:?} \
         must be spent in a transaction which only has shielded outputs"
    )]
    #[non_exhaustive]
    UnshieldedTransparentCoinbaseSpend { outpoint: transparent::OutPoint },

    #[error(
        "immature transparent coinbase spend: \
        attempt to spend {outpoint:?} at {spend_height:?}, \
        but spends are invalid before {min_spend_height:?}, \
        which is {MIN_TRANSPARENT_COINBASE_MATURITY:?} blocks \
        after it was created at {created_height:?}"
    )]
    #[non_exhaustive]
    ImmatureTransparentCoinbaseSpend {
        outpoint: transparent::OutPoint,
        spend_height: block::Height,
        min_spend_height: block::Height,
        created_height: block::Height,
    },

    #[error("sprout double-spend: duplicate nullifier: {nullifier:?}, in finalized state: {in_finalized_state:?}")]
    #[non_exhaustive]
    DuplicateSproutNullifier {
        nullifier: sprout::Nullifier,
        in_finalized_state: bool,
    },

    #[error("sapling double-spend: duplicate nullifier: {nullifier:?}, in finalized state: {in_finalized_state:?}")]
    #[non_exhaustive]
    DuplicateSaplingNullifier {
        nullifier: sapling::Nullifier,
        in_finalized_state: bool,
    },

    #[error("orchard double-spend: duplicate nullifier: {nullifier:?}, in finalized state: {in_finalized_state:?}")]
    #[non_exhaustive]
    DuplicateOrchardNullifier {
        nullifier: orchard::Nullifier,
        in_finalized_state: bool,
    },

    #[error("ironwood double-spend: duplicate nullifier: {nullifier:?}, in finalized state: {in_finalized_state:?}")]
    #[non_exhaustive]
    DuplicateIronwoodNullifier {
        nullifier: ironwood::Nullifier,
        in_finalized_state: bool,
    },

    #[error(
        "the remaining value in the transparent transaction value pool MUST be nonnegative:\n\
         {amount_error:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    NegativeRemainingTransactionValue {
        amount_error: amount::Error,
        height: block::Height,
        tx_index_in_block: usize,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "error calculating the remaining value in the transaction value pool:\n\
         {amount_error:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    CalculateRemainingTransactionValue {
        amount_error: amount::Error,
        height: block::Height,
        tx_index_in_block: usize,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "error calculating value balances for the remaining value in the transaction value pool:\n\
         {value_balance_error:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    CalculateTransactionValueBalances {
        value_balance_error: ValueBalanceError,
        height: block::Height,
        tx_index_in_block: usize,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "error calculating the block chain value pool change:\n\
         {value_balance_error:?},\n\
         {height:?}, {block_hash:?},\n\
         transactions: {transaction_count:?}, spent UTXOs: {spent_utxo_count:?}"
    )]
    #[non_exhaustive]
    CalculateBlockChainValueChange {
        value_balance_error: ValueBalanceError,
        height: block::Height,
        block_hash: block::Hash,
        transaction_count: usize,
        spent_utxo_count: usize,
    },

    #[error(
        "error adding value balances to the chain value pool:\n\
         {value_balance_error:?},\n\
         {chain_value_pools:?},\n\
         {block_value_pool_change:?},\n\
         {height:?}"
    )]
    #[non_exhaustive]
    AddValuePool {
        value_balance_error: ValueBalanceError,
        chain_value_pools: Box<ValueBalance<NonNegative>>,
        block_value_pool_change: Box<ValueBalance<NegativeAllowed>>,
        height: Option<block::Height>,
    },

    #[error("error updating a note commitment tree: {0}")]
    NoteCommitmentTreeError(#[from] zakura_chain::parallel::tree::NoteCommitmentTreeError),

    #[error("error building the history tree: {0}")]
    HistoryTreeError(#[from] Arc<HistoryTreeError>),

    #[error("block contains an invalid commitment: {0}")]
    InvalidBlockCommitment(#[from] block::CommitmentError),

    #[error(
        "unknown Sprout anchor: {anchor:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    UnknownSproutAnchor {
        anchor: sprout::tree::Root,
        height: Option<block::Height>,
        tx_index_in_block: Option<usize>,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "unknown Sapling anchor: {anchor:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    UnknownSaplingAnchor {
        anchor: sapling::tree::Root,
        height: Option<block::Height>,
        tx_index_in_block: Option<usize>,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "unknown Orchard anchor: {anchor:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    UnknownOrchardAnchor {
        anchor: orchard::tree::Root,
        height: Option<block::Height>,
        tx_index_in_block: Option<usize>,
        transaction_hash: transaction::Hash,
    },

    #[error(
        "unknown Ironwood anchor: {anchor:?},\n\
         {height:?}, index in block: {tx_index_in_block:?}, {transaction_hash:?}"
    )]
    #[non_exhaustive]
    UnknownIronwoodAnchor {
        anchor: ironwood::tree::Root,
        height: Option<block::Height>,
        tx_index_in_block: Option<usize>,
        transaction_hash: transaction::Hash,
    },
}

impl ValidateContextError {
    /// Classify contextual validation without conflating peer data and local availability.
    pub fn body_verification_class(&self) -> zakura_header_chain::BodyVerificationClass {
        use zakura_chain::block::CommitmentError;
        use zakura_header_chain::{
            BodyCommitmentKind, BodyRuleId, BodyVerificationClass, TransientBodyFailureKind,
        };

        let consensus = |rule| BodyVerificationClass::ConsensusInvalid(BodyRuleId::new(rule));
        match self {
            Self::MissingSproutTipTree(_) | Self::NotReadyToBeCommitted => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::MissingContext)
            }
            Self::BlockPreviouslyInvalidated { .. } | Self::OrphanedBlock { .. } => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::Canceled)
            }
            Self::VctSuppliedRootUnavailable { .. }
            | Self::VctSuppliedRootAwaitingSuccessor { .. } => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::MissingContext)
            }
            Self::VctBlockAuthDataRootMismatch { .. } => {
                BodyVerificationClass::PayloadMismatch(BodyCommitmentKind::AuthDataRoot)
            }
            Self::VctSproutHandoffRootMismatch { .. }
            | Self::NoteCommitmentTreeError(_)
            | Self::HistoryTreeError(_) => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::Storage)
            }
            Self::InvalidBlockCommitment(error) => {
                let kind = match error {
                    CommitmentError::InvalidAuthDataRoot { .. } => BodyCommitmentKind::AuthDataRoot,
                    CommitmentError::InvalidFinalSaplingRoot { .. } => {
                        BodyCommitmentKind::Other("final_sapling_root")
                    }
                    CommitmentError::InvalidChainHistoryActivationReserved { .. } => {
                        BodyCommitmentKind::Other("chain_history_activation_reserved")
                    }
                    CommitmentError::InvalidChainHistoryRoot { .. } => {
                        BodyCommitmentKind::Other("chain_history_root")
                    }
                    CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. } => {
                        BodyCommitmentKind::Other("chain_history_block_tx_auth_commitment")
                    }
                    CommitmentError::InvalidPreNu5OrchardRoot { .. } => {
                        BodyCommitmentKind::Other("pre_nu5_orchard_root")
                    }
                    CommitmentError::InvalidPreNu6_3IronwoodRoot { .. } => {
                        BodyCommitmentKind::Other("pre_nu6_3_ironwood_root")
                    }
                    CommitmentError::MissingBlockHeight { .. } => {
                        BodyCommitmentKind::Other("missing_block_height")
                    }
                    CommitmentError::InvalidSapingRootBytes => {
                        BodyCommitmentKind::Other("invalid_sapling_root_bytes")
                    }
                };
                BodyVerificationClass::PayloadMismatch(kind)
            }
            Self::NonSequentialBlock { .. } => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::MissingContext)
            }
            Self::TimeTooEarly { .. }
            | Self::TimeTooLate { .. }
            | Self::InvalidDifficultyThreshold { .. } => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::DuplicateTransparentSpend { .. } => {
                consensus("context.duplicate_transparent_spend")
            }
            Self::MissingTransparentOutput { .. } => {
                consensus("context.missing_transparent_output")
            }
            Self::EarlyTransparentSpend { .. } => consensus("context.early_transparent_spend"),
            Self::UnshieldedTransparentCoinbaseSpend { .. } => {
                consensus("context.unshielded_transparent_coinbase_spend")
            }
            Self::ImmatureTransparentCoinbaseSpend { .. } => {
                consensus("context.immature_transparent_coinbase_spend")
            }
            Self::DuplicateSproutNullifier { .. } => {
                consensus("context.duplicate_sprout_nullifier")
            }
            Self::DuplicateSaplingNullifier { .. } => {
                consensus("context.duplicate_sapling_nullifier")
            }
            Self::DuplicateOrchardNullifier { .. } => {
                consensus("context.duplicate_orchard_nullifier")
            }
            Self::DuplicateIronwoodNullifier { .. } => {
                consensus("context.duplicate_ironwood_nullifier")
            }
            Self::NegativeRemainingTransactionValue { .. } => {
                consensus("context.negative_remaining_transaction_value")
            }
            Self::CalculateRemainingTransactionValue { .. } => {
                consensus("context.calculate_remaining_transaction_value")
            }
            Self::CalculateTransactionValueBalances { .. } => {
                consensus("context.calculate_transaction_value_balances")
            }
            Self::CalculateBlockChainValueChange { .. } => {
                consensus("context.calculate_block_chain_value_change")
            }
            Self::AddValuePool { .. } => consensus("context.add_value_pool"),
            Self::UnknownSproutAnchor { .. } => consensus("context.unknown_sprout_anchor"),
            Self::UnknownSaplingAnchor { .. } => consensus("context.unknown_sapling_anchor"),
            Self::UnknownOrchardAnchor { .. } => consensus("context.unknown_orchard_anchor"),
            Self::UnknownIronwoodAnchor { .. } => consensus("context.unknown_ironwood_anchor"),
        }
    }

    /// Returns the missing VCT supplied-root height for retryable root stalls.
    ///
    /// This is the subset of [`Self::vct_retryable_height`] where the supplied root itself is
    /// missing: it was never delivered with its header range, or was evicted after failing
    /// verification. It can only be filled by a later re-delivery of that header range (for
    /// example another fanout peer's response); roots are not individually re-requested. An
    /// await-successor stall ([`Self::vct_retryable_height`] but not this) already has its root
    /// and only waits for the next header to be stored.
    pub fn vct_supplied_root_unavailable_height(&self) -> Option<block::Height> {
        match self {
            ValidateContextError::VctSuppliedRootUnavailable { height } => Some(*height),
            _ => None,
        }
    }

    /// Returns the height for any retryable VCT root stall: either an absent/evicted supplied
    /// root ([`Self::VctSuppliedRootUnavailable`]) or one not yet verifiable because no successor
    /// is buffered to confirm it ([`Self::VctSuppliedRootAwaitingSuccessor`]). The write loop
    /// parks and retries the same block for both; the former polls slower because nothing is
    /// actively fetching a replacement root.
    pub fn vct_retryable_height(&self) -> Option<block::Height> {
        match self {
            ValidateContextError::VctSuppliedRootUnavailable { height }
            | ValidateContextError::VctSuppliedRootAwaitingSuccessor { height } => Some(*height),
            _ => None,
        }
    }
}

impl From<sprout::tree::NoteCommitmentTreeError> for ValidateContextError {
    fn from(value: sprout::tree::NoteCommitmentTreeError) -> Self {
        ValidateContextError::NoteCommitmentTreeError(value.into())
    }
}

/// Trait for creating the corresponding duplicate nullifier error from a nullifier.
pub trait DuplicateNullifierError {
    /// Returns the corresponding duplicate nullifier error for `self`.
    fn duplicate_nullifier_error(&self, in_finalized_state: bool) -> ValidateContextError;
}

impl DuplicateNullifierError for sprout::Nullifier {
    fn duplicate_nullifier_error(&self, in_finalized_state: bool) -> ValidateContextError {
        ValidateContextError::DuplicateSproutNullifier {
            nullifier: *self,
            in_finalized_state,
        }
    }
}

impl DuplicateNullifierError for sapling::Nullifier {
    fn duplicate_nullifier_error(&self, in_finalized_state: bool) -> ValidateContextError {
        ValidateContextError::DuplicateSaplingNullifier {
            nullifier: *self,
            in_finalized_state,
        }
    }
}

impl DuplicateNullifierError for orchard::Nullifier {
    fn duplicate_nullifier_error(&self, in_finalized_state: bool) -> ValidateContextError {
        ValidateContextError::DuplicateOrchardNullifier {
            nullifier: *self,
            in_finalized_state,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block::Height;
    use zakura_header_chain::{
        BodyCommitmentKind, BodyVerificationClass, TransientBodyFailureKind,
    };

    #[test]
    fn body_verification_classes_preserve_attribution_boundaries() {
        assert_eq!(
            ValidateContextError::VctSuppliedRootUnavailable { height: Height(7) }
                .body_verification_class(),
            BodyVerificationClass::Retryable(TransientBodyFailureKind::MissingContext)
        );
        assert_eq!(
            ValidateContextError::VctBlockAuthDataRootMismatch {
                height: Height(7),
                expected: block::merkle::AuthDataRoot::from([1; 32]),
                actual: block::merkle::AuthDataRoot::from([2; 32]),
            }
            .body_verification_class(),
            BodyVerificationClass::PayloadMismatch(BodyCommitmentKind::AuthDataRoot)
        );
        assert_eq!(
            ValidateContextError::DuplicateTransparentSpend {
                outpoint: transparent::OutPoint {
                    hash: [3; 32].into(),
                    index: 0,
                },
                location: "test chain",
            }
            .body_verification_class(),
            BodyVerificationClass::ConsensusInvalid(zakura_header_chain::BodyRuleId::new(
                "context.duplicate_transparent_spend"
            ))
        );
        assert_eq!(
            CommitBlockError::HeaderChainError {
                error: "local transition failure".to_owned(),
            }
            .body_verification_class(),
            BodyVerificationClass::Retryable(TransientBodyFailureKind::Storage)
        );
        assert_eq!(
            CommitBlockError::Duplicate {
                hash_or_height: None,
                location: KnownBlock::BestChain,
            }
            .body_verification_class(),
            BodyVerificationClass::Duplicate
        );
    }

    #[test]
    fn commit_block_error_misbehavior_scores() {
        let context_err = CommitBlockError::ValidateContextError(Box::new(
            ValidateContextError::NonSequentialBlock {
                candidate_height: Height(5),
                parent_height: Height(3),
            },
        ));
        assert_eq!(context_err.misbehavior_score(), 0);

        let dup_err = CommitBlockError::Duplicate {
            hash_or_height: None,
            location: KnownBlock::BestChain,
        };
        assert_eq!(dup_err.misbehavior_score(), 0);
    }

    #[test]
    fn checkpoint_error_exposes_retryable_vct_root_height() {
        let height = Height(42);
        let retryable =
            CommitCheckpointVerifiedError::from(ValidateContextError::VctSuppliedRootUnavailable {
                height,
            })
            .with_vct_failure(VctCommitFailure::SuccessorBoundary);
        assert_eq!(
            retryable.vct_supplied_root_unavailable_height(),
            Some(height),
            "checkpoint commit errors expose retryable VCT root misses"
        );
        assert_eq!(
            retryable.vct_failure(),
            Some(VctCommitFailure::SuccessorBoundary),
            "checkpoint errors preserve the exact VCT verifier stage"
        );

        let non_retryable: CommitCheckpointVerifiedError =
            ValidateContextError::NonSequentialBlock {
                candidate_height: Height(5),
                parent_height: Height(3),
            }
            .into();
        assert_eq!(
            non_retryable.vct_supplied_root_unavailable_height(),
            None,
            "unrelated validation errors are not treated as VCT root misses"
        );
        assert_eq!(
            non_retryable.vct_retryable_height(),
            None,
            "unrelated validation errors are not retryable VCT stalls"
        );
    }

    /// An await-successor stall is retryable (the write loop parks and re-commits) but is
    /// *not* a missing-root case: the root is present, only its successor is missing. So it
    /// must surface through `vct_retryable_height` while
    /// `vct_supplied_root_unavailable_height` (which selects the slower missing-root wait)
    /// stays `None` — otherwise the committer would poll slowly for a root it already holds.
    #[test]
    fn await_successor_is_retryable_but_not_root_unavailable() {
        let height = Height(7);
        let awaiting: CommitCheckpointVerifiedError =
            ValidateContextError::VctSuppliedRootAwaitingSuccessor { height }.into();

        assert_eq!(
            awaiting.vct_retryable_height(),
            Some(height),
            "an await-successor stall is retryable",
        );
        assert_eq!(
            awaiting.vct_supplied_root_unavailable_height(),
            None,
            "an await-successor stall is not a missing root (the root is present)",
        );

        // The unavailable case is both retryable and a missing root.
        let unavailable: CommitCheckpointVerifiedError =
            ValidateContextError::VctSuppliedRootUnavailable { height }.into();
        assert_eq!(unavailable.vct_retryable_height(), Some(height));
        assert_eq!(
            unavailable.vct_supplied_root_unavailable_height(),
            Some(height)
        );
    }
}
