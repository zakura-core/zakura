//! Errors that can occur when checking consensus rules.
//!
//! Each error variant corresponds to a consensus rule, so enumerating
//! all possible verification failures enumerates the consensus rules we
//! implement, and ensures that we don't reject blocks or transactions
//! for a non-enumerated reason.

use std::{array::TryFromSliceError, convert::Infallible};

use chrono::{DateTime, Utc};
use thiserror::Error;

use zakura_chain::{
    amount, block, ironwood, orchard,
    parameters::subsidy::SubsidyError,
    sapling, sprout,
    transparent::{self, MIN_TRANSPARENT_COINBASE_MATURITY},
};
use zakura_state::ValidateContextError;
use zcash_protocol::value::BalanceError;

use crate::{block::MAX_BLOCK_SIGOPS, transaction::check::MAX_STANDARD_SCRIPTSIG_SIZE, BoxError};

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

/// Workaround for format string identifier rules.
const MAX_EXPIRY_HEIGHT: block::Height = block::Height::MAX_EXPIRY_HEIGHT;

/// Errors for semantic transaction validation.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
#[allow(missing_docs)]
pub enum TransactionError {
    #[error("first transaction must be coinbase")]
    CoinbasePosition,

    #[error("coinbase input found in non-coinbase transaction")]
    CoinbaseAfterFirst,

    #[error("coinbase transaction MUST NOT have any JoinSplit descriptions")]
    CoinbaseHasJoinSplit,

    #[error("coinbase transaction MUST NOT have any Spend descriptions")]
    CoinbaseHasSpend,

    #[error("coinbase transaction MUST NOT have any Output descriptions pre-Heartwood")]
    CoinbaseHasOutputPreHeartwood,

    #[error("coinbase transaction MUST NOT have the EnableSpendsOrchard flag set")]
    CoinbaseHasEnableSpendsOrchard,

    #[error("coinbase transaction MUST NOT have the EnableSpendsIronwood flag set")]
    CoinbaseHasEnableSpendsIronwood,

    #[error("coinbase transaction MUST NOT have an Orchard shielded bundle")]
    CoinbaseHasOrchardShieldedData,

    #[error("coinbase transaction Sapling or Orchard outputs MUST be decryptable with an all-zero outgoing viewing key")]
    CoinbaseOutputsNotDecryptable,

    #[error("coinbase inputs MUST NOT exist in mempool")]
    CoinbaseInMempool,

    #[error("non-coinbase transactions MUST NOT have coinbase inputs")]
    NonCoinbaseHasCoinbaseInput,

    #[error("the tx is not coinbase, but it should be")]
    NotCoinbase,

    #[error("transaction is locked until after block height {}", _0.0)]
    LockedUntilAfterBlockHeight(block::Height),

    #[error("transaction is locked until after block time {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    LockedUntilAfterBlockTime(DateTime<Utc>),

    #[error(
        "coinbase expiry {expiry_height:?} must be the same as the block {block_height:?} \
         after NU5 activation, failing transaction: {transaction_hash:?}"
    )]
    CoinbaseExpiryBlockHeight {
        expiry_height: Option<zakura_chain::block::Height>,
        block_height: zakura_chain::block::Height,
        transaction_hash: zakura_chain::transaction::Hash,
    },

    #[error("could not construct coinbase tx: {0}")]
    CoinbaseConstruction(String),

    #[error(
        "expiry {expiry_height:?} must be less than the maximum {MAX_EXPIRY_HEIGHT:?} \
         coinbase: {is_coinbase}, block: {block_height:?}, failing transaction: {transaction_hash:?}"
    )]
    MaximumExpiryHeight {
        expiry_height: zakura_chain::block::Height,
        is_coinbase: bool,
        block_height: zakura_chain::block::Height,
        transaction_hash: zakura_chain::transaction::Hash,
    },

    #[error(
        "transaction must not be mined at a block {block_height:?} \
         greater than its expiry {expiry_height:?}, failing transaction {transaction_hash:?}"
    )]
    ExpiredTransaction {
        expiry_height: zakura_chain::block::Height,
        block_height: zakura_chain::block::Height,
        transaction_hash: zakura_chain::transaction::Hash,
    },

    #[error("coinbase transaction failed subsidy validation: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    Subsidy(#[from] SubsidyError),

    #[error("transaction version number MUST be >= 4")]
    WrongVersion,

    #[error("transaction version {0} not supported by the network upgrade {1:?}")]
    UnsupportedByNetworkUpgrade(u32, zakura_chain::parameters::NetworkUpgrade),

    #[error("must have at least one input: transparent, shielded spend, or joinsplit")]
    NoInputs,

    #[error("must have at least one output: transparent, shielded output, or joinsplit")]
    NoOutputs,

    #[error("if there are no Spends or Outputs, the value balance MUST be 0.")]
    BadBalance,

    #[error("could not verify a transparent script: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    Script(#[from] zakura_script::Error),

    #[error("Sapling proof or signature verification failed")]
    SaplingVerificationFailed,

    #[error("Orchard or Ironwood Halo2 proof verification failed")]
    Halo2VerificationFailed,

    #[error("spend description cv and rk MUST NOT be of small order")]
    SmallOrder,

    // TODO: the underlying error is bellman::VerificationError, but it does not implement
    // Arbitrary as required here.
    #[error("spend proof MUST be valid given a primary input formed from the other fields except spendAuthSig: {0}")]
    Groth16(String),

    // TODO: the underlying error is io::Error, but it does not implement Clone as required here.
    #[error("Groth16 proof is malformed: {0}")]
    MalformedGroth16(String),

    #[error(
        "Sprout joinSplitSig MUST represent a valid signature under joinSplitPubKey of dataToBeSigned: {0}"
    )]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    Ed25519(#[from] zakura_chain::primitives::ed25519::Error),

    #[error("Sapling bindingSig MUST represent a valid signature under the transaction binding validating key bvk of SigHash: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    RedJubjub(zakura_chain::primitives::redjubjub::Error),

    #[error("Orchard bindingSig MUST represent a valid signature under the transaction binding validating key bvk of SigHash: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    RedPallas(zakura_chain::primitives::reddsa::Error),

    #[error("could not convert an asynchronous verification error: {0}")]
    InternalDowncastError(String),

    #[error("either vpub_old or vpub_new must be zero")]
    BothVPubsNonZero,

    #[error("adding to the sprout pool is disabled after Canopy")]
    DisabledAddToSproutPool,

    #[error("adding to the orchard pool is disabled after NU6.3")]
    DisabledAddToOrchardPool,

    #[error("could not calculate the transaction fee")]
    IncorrectFee,

    #[error("transparent double-spend: {_0:?} is spent twice")]
    DuplicateTransparentSpend(transparent::OutPoint),

    #[error("sprout double-spend: duplicate nullifier: {_0:?}")]
    DuplicateSproutNullifier(sprout::Nullifier),

    #[error("sapling double-spend: duplicate nullifier: {_0:?}")]
    DuplicateSaplingNullifier(sapling::Nullifier),

    #[error("orchard double-spend: duplicate nullifier: {_0:?}")]
    DuplicateOrchardNullifier(orchard::Nullifier),

    #[error("ironwood double-spend: duplicate nullifier: {_0:?}")]
    DuplicateIronwoodNullifier(ironwood::Nullifier),

    #[error("must have at least one active orchard flag")]
    NotEnoughFlags,

    #[error("must have at least enable spend or enable output flag set")]
    NotEnoughIronwoodFlags,

    #[error("Orchard transactions MUST NOT have the EnableCrossAddress flag set")]
    OrchardHasEnableCrossAddress,

    #[error("could not find transparent input UTXO in the best chain or mempool")]
    TransparentInputNotFound,

    #[error("could not contextually validate transaction on best chain: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    // This error variant is at least 128 bytes
    ValidateContextError(Box<ValidateContextError>),

    #[error("could not validate mempool transaction lock time on best chain: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    // TODO: turn this into a typed error
    ValidateMempoolLockTimeError(String),

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

    #[error(
        "unshielded transparent coinbase spend: {outpoint:?} \
         must be spent in a transaction which only has shielded outputs"
    )]
    #[non_exhaustive]
    UnshieldedTransparentCoinbaseSpend {
        outpoint: transparent::OutPoint,
        min_spend_height: block::Height,
    },

    #[error(
        "failed to verify ZIP-317 transaction rules, transaction was not inserted to mempool: {0}"
    )]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    Zip317(#[from] zakura_chain::transaction::zip317::Error),

    // Mempool standardness (policy) rejections, applied before script verification.
    // These are not consensus rules: the same input scripts are valid in blocks.
    #[error(
        "mempool transaction input {input_index} has a {size} byte scriptSig, \
         above the {MAX_STANDARD_SCRIPTSIG_SIZE} byte standardness limit"
    )]
    NonStandardScriptSigSize { input_index: usize, size: usize },

    #[error("mempool transaction input {input_index} has a non-push-only scriptSig")]
    NonStandardScriptSigNotPushOnly { input_index: usize },

    #[error("mempool transaction has non-standard transparent inputs")]
    NonStandardInputs,

    #[error("transaction uses an incorrect consensus branch id")]
    WrongConsensusBranchId,

    #[error(
        "mempool transaction uses the NU6.2 consensus branch id during the NU6.3 grace period"
    )]
    WrongConsensusBranchIdNu6_3GracePeriod,

    #[error("wrong tx format: tx version is ≥ 5, but `nConsensusBranchId` is missing")]
    MissingConsensusBranchId,

    #[error("input/output error")]
    Io(String),

    #[error("failed to convert a slice")]
    TryFromSlice(String),

    #[error("invalid amount")]
    Amount(String),

    #[error("invalid balance")]
    Balance(String),

    #[error("Orchard proof has a non-canonical size")]
    OrchardProofSize,

    #[error("Ironwood proof has a non-canonical size")]
    IronwoodProofSize,

    #[error("unexpected error")]
    Other(String),
}

impl From<ValidateContextError> for TransactionError {
    fn from(err: ValidateContextError) -> Self {
        TransactionError::ValidateContextError(Box::new(err))
    }
}

impl From<zcash_transparent::builder::Error> for TransactionError {
    fn from(err: zcash_transparent::builder::Error) -> Self {
        TransactionError::CoinbaseConstruction(err.to_string())
    }
}

impl From<zcash_primitives::transaction::builder::Error<Infallible>> for TransactionError {
    fn from(err: zcash_primitives::transaction::builder::Error<Infallible>) -> Self {
        TransactionError::CoinbaseConstruction(err.to_string())
    }
}

impl From<BalanceError> for TransactionError {
    fn from(err: BalanceError) -> Self {
        TransactionError::Balance(err.to_string())
    }
}

impl From<libzcash_script::Error> for TransactionError {
    fn from(err: libzcash_script::Error) -> Self {
        TransactionError::Script(zakura_script::Error::from(err))
    }
}

impl From<std::io::Error> for TransactionError {
    fn from(err: std::io::Error) -> Self {
        TransactionError::Io(err.to_string())
    }
}

impl From<TryFromSliceError> for TransactionError {
    fn from(err: TryFromSliceError) -> Self {
        TransactionError::TryFromSlice(err.to_string())
    }
}

impl From<amount::Error> for TransactionError {
    fn from(err: amount::Error) -> Self {
        TransactionError::Amount(err.to_string())
    }
}

impl From<BoxError> for TransactionError {
    fn from(mut err: BoxError) -> Self {
        match err.downcast::<zakura_script::Error>() {
            Ok(e) => return TransactionError::Script(*e),
            Err(e) => err = e,
        }

        match err.downcast::<zakura_chain::primitives::ed25519::Error>() {
            Ok(e) => return TransactionError::Ed25519(*e),
            Err(e) => err = e,
        }
        match err.downcast::<zakura_chain::primitives::redjubjub::Error>() {
            Ok(e) => return TransactionError::RedJubjub(*e),
            Err(e) => err = e,
        }

        match err.downcast::<zakura_chain::primitives::reddsa::Error>() {
            Ok(e) => return TransactionError::RedPallas(*e),
            Err(e) => err = e,
        }

        match err.downcast::<ValidateContextError>() {
            Ok(e) => return (*e).into(),
            Err(e) => err = e,
        }

        // buffered transaction verifier service error
        match err.downcast::<TransactionError>() {
            Ok(e) => return *e,
            Err(e) => err = e,
        }

        TransactionError::InternalDowncastError(format!(
            "downcast to known transaction error type failed, original error: {err:?}",
        ))
    }
}

impl TransactionError {
    /// Classify a transaction-verification failure for the body-evidence boundary.
    pub fn body_verification_class(&self) -> zakura_header_chain::BodyVerificationClass {
        use zakura_header_chain::{BodyRuleId, BodyVerificationClass, TransientBodyFailureKind};

        let consensus = |rule| BodyVerificationClass::ConsensusInvalid(BodyRuleId::new(rule));
        match self {
            Self::CoinbasePosition => consensus("transaction.coinbase_position"),
            Self::CoinbaseAfterFirst => consensus("transaction.coinbase_after_first"),
            Self::CoinbaseHasJoinSplit => consensus("transaction.coinbase_has_joinsplit"),
            Self::CoinbaseHasSpend => consensus("transaction.coinbase_has_spend"),
            Self::CoinbaseHasOutputPreHeartwood => {
                consensus("transaction.coinbase_has_output_pre_heartwood")
            }
            Self::CoinbaseHasEnableSpendsOrchard => {
                consensus("transaction.coinbase_enables_orchard_spends")
            }
            Self::CoinbaseHasEnableSpendsIronwood => {
                consensus("transaction.coinbase_enables_ironwood_spends")
            }
            Self::CoinbaseHasOrchardShieldedData => {
                consensus("transaction.coinbase_has_orchard_shielded_data")
            }
            Self::CoinbaseOutputsNotDecryptable => {
                consensus("transaction.coinbase_outputs_not_decryptable")
            }
            Self::CoinbaseInMempool => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::NonCoinbaseHasCoinbaseInput => {
                consensus("transaction.non_coinbase_has_coinbase_input")
            }
            Self::NotCoinbase => consensus("transaction.not_coinbase"),
            Self::LockedUntilAfterBlockHeight(_) => {
                consensus("transaction.locked_until_after_block_height")
            }
            Self::LockedUntilAfterBlockTime(_) => {
                consensus("transaction.locked_until_after_block_time")
            }
            Self::CoinbaseExpiryBlockHeight { .. } => {
                consensus("transaction.coinbase_expiry_block_height")
            }
            Self::CoinbaseConstruction(_) => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::MaximumExpiryHeight { .. } => consensus("transaction.maximum_expiry_height"),
            Self::ExpiredTransaction { .. } => consensus("transaction.expired"),
            Self::Subsidy(_) => consensus("transaction.subsidy"),
            Self::WrongVersion => consensus("transaction.wrong_version"),
            Self::UnsupportedByNetworkUpgrade(_, _) => {
                consensus("transaction.unsupported_by_network_upgrade")
            }
            Self::NoInputs => consensus("transaction.no_inputs"),
            Self::NoOutputs => consensus("transaction.no_outputs"),
            Self::BadBalance => consensus("transaction.bad_balance"),
            Self::Script(_) => consensus("transaction.script"),
            Self::SaplingVerificationFailed => consensus("transaction.sapling_verification"),
            Self::Halo2VerificationFailed => consensus("transaction.halo2_verification"),
            Self::SmallOrder => consensus("transaction.small_order"),
            Self::Groth16(_) => consensus("transaction.groth16"),
            Self::MalformedGroth16(_) => consensus("transaction.malformed_groth16"),
            Self::Ed25519(_) => consensus("transaction.ed25519"),
            Self::RedJubjub(_) => consensus("transaction.redjubjub"),
            Self::RedPallas(_) => consensus("transaction.redpallas"),
            Self::InternalDowncastError(_) => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::BothVPubsNonZero => consensus("transaction.both_vpubs_nonzero"),
            Self::DisabledAddToSproutPool => consensus("transaction.disabled_sprout_pool_add"),
            Self::DisabledAddToOrchardPool => consensus("transaction.disabled_orchard_pool_add"),
            Self::IncorrectFee => consensus("transaction.incorrect_fee"),
            Self::DuplicateTransparentSpend(_) => {
                consensus("transaction.duplicate_transparent_spend")
            }
            Self::DuplicateSproutNullifier(_) => {
                consensus("transaction.duplicate_sprout_nullifier")
            }
            Self::DuplicateSaplingNullifier(_) => {
                consensus("transaction.duplicate_sapling_nullifier")
            }
            Self::DuplicateOrchardNullifier(_) => {
                consensus("transaction.duplicate_orchard_nullifier")
            }
            Self::DuplicateIronwoodNullifier(_) => {
                consensus("transaction.duplicate_ironwood_nullifier")
            }
            Self::NotEnoughFlags => consensus("transaction.orchard_flags"),
            Self::NotEnoughIronwoodFlags => consensus("transaction.ironwood_flags"),
            Self::OrchardHasEnableCrossAddress => {
                consensus("transaction.orchard_enable_cross_address")
            }
            Self::TransparentInputNotFound => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::MissingContext)
            }
            Self::ValidateContextError(error) => error.body_verification_class(),
            Self::ValidateMempoolLockTimeError(_)
            | Self::Zip317(_)
            | Self::NonStandardScriptSigSize { .. }
            | Self::NonStandardScriptSigNotPushOnly { .. }
            | Self::NonStandardInputs
            | Self::WrongConsensusBranchIdNu6_3GracePeriod => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::WrongConsensusBranchId => consensus("transaction.wrong_consensus_branch_id"),
            Self::MissingConsensusBranchId => consensus("transaction.missing_consensus_branch_id"),
            Self::Io(_) | Self::TryFromSlice(_) | Self::Other(_) => {
                BodyVerificationClass::Retryable(TransientBodyFailureKind::VerifierUnavailable)
            }
            Self::Amount(_) => consensus("transaction.amount"),
            Self::Balance(_) => consensus("transaction.balance"),
            Self::OrchardProofSize => consensus("transaction.orchard_proof_size"),
            Self::IronwoodProofSize => consensus("transaction.ironwood_proof_size"),
            Self::ImmatureTransparentCoinbaseSpend { .. } => {
                consensus("transaction.immature_transparent_coinbase_spend")
            }
            Self::UnshieldedTransparentCoinbaseSpend { .. } => {
                consensus("transaction.unshielded_transparent_coinbase_spend")
            }
        }
    }

    /// Returns a suggested misbehaviour score increment for a certain error when
    /// verifying a mempool transaction.
    pub fn mempool_misbehavior_score(&self) -> u32 {
        use TransactionError::*;

        // TODO: Adjust these values based on zcashd (#9258).
        match self {
            ImmatureTransparentCoinbaseSpend { .. }
            | UnshieldedTransparentCoinbaseSpend { .. }
            | CoinbasePosition
            | CoinbaseAfterFirst
            | CoinbaseHasJoinSplit
            | CoinbaseHasSpend
            | CoinbaseHasOutputPreHeartwood
            | CoinbaseHasEnableSpendsOrchard
            | CoinbaseHasEnableSpendsIronwood
            | CoinbaseHasOrchardShieldedData
            | CoinbaseOutputsNotDecryptable
            | CoinbaseInMempool
            | NonCoinbaseHasCoinbaseInput
            | CoinbaseExpiryBlockHeight { .. }
            | IncorrectFee
            | Subsidy(_)
            | WrongVersion
            | NoInputs
            | NoOutputs
            | BadBalance
            | Script(_)
            | SaplingVerificationFailed
            | Halo2VerificationFailed
            | SmallOrder
            | Groth16(_)
            | MalformedGroth16(_)
            | Ed25519(_)
            | RedJubjub(_)
            | RedPallas(_)
            | BothVPubsNonZero
            | DisabledAddToSproutPool
            | DisabledAddToOrchardPool
            | NotEnoughFlags
            | NotEnoughIronwoodFlags
            | OrchardHasEnableCrossAddress
            | OrchardProofSize
            | IronwoodProofSize
            | WrongConsensusBranchId
            | MissingConsensusBranchId
            | LockedUntilAfterBlockHeight(_)
            | LockedUntilAfterBlockTime(_) => 100,

            // NU6.2 mempool transactions are invalid under NU6.3 rules, but
            // honest peers can relay them briefly while their chain tips converge.
            WrongConsensusBranchIdNu6_3GracePeriod => 0,

            // TODO: Consider add peer penalty 1 if these are very old
            DuplicateTransparentSpend(_)
            | DuplicateSproutNullifier(_)
            | DuplicateSaplingNullifier(_)
            | DuplicateOrchardNullifier(_)
            | DuplicateIronwoodNullifier(_) => 0,

            // Standardness (policy) rejections must not be punished: non-standard
            // transactions are consensus-valid, and zcashd relays a reject message
            // without a DoS score for them.
            _other => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boxed_signature_errors_are_preserved() {
        let script_error: BoxError = Box::new(zakura_script::Error::ScriptInvalid);
        assert_eq!(
            TransactionError::from(script_error),
            TransactionError::Script(zakura_script::Error::ScriptInvalid)
        );

        let ed25519_error: BoxError =
            Box::new(zakura_chain::primitives::ed25519::Error::InvalidSignature);
        assert_eq!(
            TransactionError::from(ed25519_error),
            TransactionError::Ed25519(zakura_chain::primitives::ed25519::Error::InvalidSignature)
        );

        let redjubjub_error: BoxError =
            Box::new(zakura_chain::primitives::redjubjub::Error::InvalidSignature);
        assert_eq!(
            TransactionError::from(redjubjub_error),
            TransactionError::RedJubjub(
                zakura_chain::primitives::redjubjub::Error::InvalidSignature
            )
        );

        let redpallas_error: BoxError =
            Box::new(zakura_chain::primitives::reddsa::Error::InvalidSignature);
        assert_eq!(
            TransactionError::from(redpallas_error),
            TransactionError::RedPallas(zakura_chain::primitives::reddsa::Error::InvalidSignature)
        );
    }

    #[test]
    fn verification_errors_have_high_misbehavior_score() {
        for error in [
            TransactionError::Script(zakura_script::Error::ScriptInvalid),
            TransactionError::SaplingVerificationFailed,
            TransactionError::Halo2VerificationFailed,
            TransactionError::Ed25519(zakura_chain::primitives::ed25519::Error::InvalidSignature),
            TransactionError::RedJubjub(
                zakura_chain::primitives::redjubjub::Error::InvalidSignature,
            ),
            TransactionError::RedPallas(zakura_chain::primitives::reddsa::Error::InvalidSignature),
        ] {
            assert_eq!(error.mempool_misbehavior_score(), 100);
        }
    }

    #[test]
    fn duplicate_spend_errors_have_no_misbehavior_score() {
        let orchard_nullifier = orchard::Nullifier::try_from([0; 32])
            .expect("zero is a valid Pallas base-field encoding");

        for error in [
            TransactionError::DuplicateTransparentSpend(transparent::OutPoint {
                hash: [0; 32].into(),
                index: 0,
            }),
            TransactionError::DuplicateSproutNullifier([0; 32].into()),
            TransactionError::DuplicateSaplingNullifier([0; 32].into()),
            TransactionError::DuplicateOrchardNullifier(orchard_nullifier),
            TransactionError::DuplicateIronwoodNullifier(orchard_nullifier),
        ] {
            assert_eq!(error.mempool_misbehavior_score(), 0);
        }
    }
}

#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum BlockError {
    #[error(transparent)]
    InvalidHeaderEncoding(#[from] zakura_header_chain::HeaderEncodingError),

    #[error("block contains invalid transactions")]
    Transaction(#[from] TransactionError),

    #[error("block has no transactions")]
    NoTransactions,

    #[error("block has mismatched merkle root")]
    BadMerkleRoot {
        actual: zakura_chain::block::merkle::Root,
        expected: zakura_chain::block::merkle::Root,
    },

    #[error("block contains duplicate transactions")]
    DuplicateTransaction,

    #[error("block {0:?} is already in present in the state {1:?}")]
    AlreadyInChain(zakura_chain::block::Hash, zakura_state::KnownBlock),

    #[error("invalid block {0:?}: missing block height")]
    MissingHeight(zakura_chain::block::Hash),

    #[error("invalid block height {0:?} in {1:?}: greater than the maximum height {2:?}")]
    MaxHeight(
        zakura_chain::block::Height,
        zakura_chain::block::Hash,
        zakura_chain::block::Height,
    ),

    #[error("invalid difficulty threshold in block header {0:?} {1:?}")]
    InvalidDifficulty(zakura_chain::block::Height, zakura_chain::block::Hash),

    #[error("block {0:?} has a difficulty threshold {2:?} that is easier than the {3:?} difficulty limit {4:?}, hash: {1:?}")]
    TargetDifficultyLimit(
        zakura_chain::block::Height,
        zakura_chain::block::Hash,
        zakura_chain::work::difficulty::ExpandedDifficulty,
        zakura_chain::parameters::Network,
        zakura_chain::work::difficulty::ExpandedDifficulty,
    ),

    #[error(
        "block {0:?} on {3:?} has a hash {1:?} that is easier than its difficulty threshold {2:?}"
    )]
    DifficultyFilter(
        zakura_chain::block::Height,
        zakura_chain::block::Hash,
        zakura_chain::work::difficulty::ExpandedDifficulty,
        zakura_chain::parameters::Network,
    ),

    #[error("transaction has wrong consensus branch id for block network upgrade")]
    WrongTransactionConsensusBranchId,

    #[error(
        "block {height:?} {hash:?} has {sigops} legacy transparent signature operations, \
         but the limit is {MAX_BLOCK_SIGOPS}"
    )]
    TooManyTransparentSignatureOperations {
        height: zakura_chain::block::Height,
        hash: zakura_chain::block::Hash,
        sigops: u32,
    },

    #[error("summing miner fees for block {height:?} {hash:?} failed: {source:?}")]
    SummingMinerFees {
        height: zakura_chain::block::Height,
        hash: zakura_chain::block::Hash,
        source: amount::Error,
    },

    #[error("unexpected error occurred: {0}")]
    Other(String),
}

impl From<SubsidyError> for BlockError {
    fn from(err: SubsidyError) -> BlockError {
        BlockError::Transaction(TransactionError::Subsidy(err))
    }
}

impl From<amount::Error> for BlockError {
    fn from(e: amount::Error) -> Self {
        Self::from(SubsidyError::from(e))
    }
}

impl BlockError {
    /// Returns `true` if this is definitely a duplicate request.
    /// Some duplicate requests might not be detected, and therefore return `false`.
    pub fn is_duplicate_request(&self) -> bool {
        matches!(self, BlockError::AlreadyInChain(..))
    }

    /// Returns a suggested misbehaviour score increment for a certain error.
    pub(crate) fn misbehavior_score(&self) -> u32 {
        use BlockError::*;

        match self {
            InvalidHeaderEncoding(_)
            | MissingHeight(_)
            | MaxHeight(_, _, _)
            | InvalidDifficulty(_, _)
            | TargetDifficultyLimit(_, _, _, _, _)
            | DifficultyFilter(_, _, _, _)
            | NoTransactions
            | BadMerkleRoot { .. }
            | WrongTransactionConsensusBranchId
            | TooManyTransparentSignatureOperations { .. } => 100,
            Transaction(err) => err.mempool_misbehavior_score(),
            _other => 0,
        }
    }
}
