//! Error types for `ParametersBuilder`.

use std::path::PathBuf;

use thiserror::Error;

use crate::{block::Height, parameters::subsidy::FundingStreamReceiver};

/// An error that can occur when building `Parameters` using `ParametersBuilder`.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ParametersBuilderError {
    #[error("cannot use reserved network name '{network_name}' as configured Testnet name, reserved names: {reserved_names:?}")]
    #[non_exhaustive]
    ReservedNetworkName {
        network_name: String,
        reserved_names: Vec<&'static str>,
    },

    #[error("network name {network_name} is too long, must be {max_length} characters or less")]
    #[non_exhaustive]
    NetworkNameTooLong {
        network_name: String,
        max_length: usize,
    },

    #[error("network name must include only alphanumeric characters or '_'")]
    #[non_exhaustive]
    InvalidCharacter,

    #[error("network magic should be distinct from reserved network magics")]
    #[non_exhaustive]
    ReservedNetworkMagic,

    #[error("configured genesis hash must parse")]
    #[non_exhaustive]
    InvalidGenesisHash,

    #[error(
        "activation heights on ParametersBuilder must not be set after setting funding streams"
    )]
    #[non_exhaustive]
    LockFundingStreams,

    #[error("activation height must be valid")]
    #[non_exhaustive]
    InvalidActivationHeight,

    #[error("Height(0) is reserved for the `Genesis` upgrade")]
    #[non_exhaustive]
    InvalidHeightZero,

    #[error("network upgrades must be activated in order specified by the protocol")]
    #[non_exhaustive]
    OutOfOrderUpgrades,

    #[error("difficulty limits are valid expanded values")]
    #[non_exhaustive]
    InvaildDifficultyLimits,

    #[error("halving interval on ParametersBuilder must not be set after setting funding streams")]
    #[non_exhaustive]
    HalvingIntervalAfterFundingStreams,

    #[error("checkpoints file format must be valid")]
    #[non_exhaustive]
    InvalidCheckpointsFormat,

    #[error("must parse checkpoints")]
    #[non_exhaustive]
    FailedToParseDefaultCheckpoint,

    #[error("could not read file at configured checkpoints file path: {path_buf:?}")]
    #[non_exhaustive]
    FailedToReadCheckpointFile { path_buf: PathBuf },

    #[error("could not parse checkpoints at the provided path: {path_buf:?}, err: {err}")]
    #[non_exhaustive]
    FailedToParseCheckpointFile { path_buf: PathBuf, err: String },

    #[error("configured checkpoints must be valid")]
    #[non_exhaustive]
    InvalidCustomCheckpoints,

    #[error("first checkpoint hash must match genesis hash")]
    #[non_exhaustive]
    CheckpointGenesisMismatch,

    #[error(
        "checkpoints must be provided for block heights below the mandatory checkpoint height"
    )]
    #[non_exhaustive]
    InsufficientCheckpointCoverage,

    #[error(
        "funding stream address {address} for receiver {receiver:?} must be a P2SH address, \
         because the funding stream consensus rule only accepts P2SH outputs"
    )]
    #[non_exhaustive]
    FundingStreamAddressNotP2SH {
        receiver: FundingStreamReceiver,
        address: String,
    },

    #[error(
        "slow start interval {slow_start_interval:?} must divide the block subsidy limit into \
         a rate that is a multiple of five, because the founders reward is an exact fifth of \
         the block subsidy"
    )]
    #[non_exhaustive]
    IndivisibleFoundersReward { slow_start_interval: Height },

    #[error("lockbox disbursement address {address} must parse as a transparent address: {err}")]
    #[non_exhaustive]
    InvalidLockboxDisbursementAddress { address: String, err: String },

    #[error(
        "lockbox disbursement address {address} must be a P2SH address, because the lockbox \
         disbursement consensus rule only accepts P2SH outputs"
    )]
    #[non_exhaustive]
    LockboxDisbursementAddressNotP2SH { address: String },

    #[error(
        "the configured lockbox disbursement amounts must sum to a valid amount, because the \
         deferred pool balance is calculated from their total"
    )]
    #[non_exhaustive]
    InvalidLockboxDisbursementTotal,
}
