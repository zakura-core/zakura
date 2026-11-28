//! The parts of a v7 transaction that earlier transaction versions do not have.

#[cfg(any(test, feature = "proptest-impl"))]
use serde::Serialize;

use crate::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block,
    memory::{vec_capacity_bytes, AttributedMemorySize},
};

/// The largest `blockHeight` a coinbase bundle can carry.
const MAX_COINBASE_BLOCK_HEIGHT: u32 = 499_999_999;

/// The maximum length of a coinbase bundle's `coinbaseData`.
pub const MAX_COINBASE_DATA_LEN: usize = 94;

// Block templates build v7 coinbases with zcash_primitives, which enforces its own copy of
// this limit, so a template must never accept data that the builder rejects.
const _: () =
    assert!(MAX_COINBASE_DATA_LEN == zcash_primitives::transaction::v7::MAX_COINBASE_DATA_LEN);

/// The `mValuePoolDeltas` entries of a v7 transaction other than the Sapling, Orchard,
/// and Ironwood balancing values.
///
/// The shielded bundles carry their balancing values as their own value balances, so
/// they are not repeated here. A zero delta is an absent entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct ValueDeltas {
    /// The transparent bundle's delta: the value of the spent outputs minus the value of
    /// the created outputs.
    pub transparent: Amount<NegativeAllowed>,
    /// The coinbase bundle's delta: the block subsidy less the lockbox deposit and the
    /// ZIP 235 fee contribution.
    pub coinbase: Amount<NegativeAllowed>,
    /// The fee bundle's delta (ZIP 2002): the negated fee of a non-coinbase transaction,
    /// or the fees that a coinbase transaction collects.
    pub fee: Amount<NegativeAllowed>,
    /// The ZIP 233 NSM bundle's delta.
    pub nsm: Amount<NegativeAllowed>,
}

/// The parts of a v7 transaction that earlier transaction versions do not have.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct V7Fields {
    /// The value pool deltas that no bundle carries as its value balance.
    pub value_deltas: ValueDeltas,
    /// The coinbase bundle, present exactly in a coinbase transaction.
    pub coinbase: Option<CoinbaseBundle>,
}

impl AttributedMemorySize for V7Fields {
    fn attributed_memory_size_bytes(&self) -> u64 {
        self.coinbase.as_ref().map_or(0, |coinbase| {
            vec_capacity_bytes(&coinbase.coinbase_data).saturating_add(
                coinbase
                    .miner_poll
                    .as_ref()
                    .map_or(0, |poll| vec_capacity_bytes(&poll.signals)),
            )
        })
    }
}

/// A miner's signal on one subject of a poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct MinerSignal {
    /// The subject of the signal.
    pub signal_code: u8,
    /// The miner's level of support for the subject.
    pub level_of_support: u8,
}

/// The signals a miner gives in response to one poll.
///
/// Consensus assigns no meaning to the signals beyond their encoding rules.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct MinerPoll {
    signals: Vec<MinerSignal>,
    poll_digest: [u8; 16],
}

impl MinerPoll {
    /// Constructs a poll response from signals in strictly ascending order of
    /// `signal_code`, which implies there are at most 256.
    pub fn new(signals: Vec<MinerSignal>, poll_digest: [u8; 16]) -> Result<Self, CoinbaseError> {
        if signals.is_empty() {
            return Err(CoinbaseError::EmptyMinerPoll);
        }
        if signals
            .windows(2)
            .any(|pair| pair[1].signal_code <= pair[0].signal_code)
        {
            return Err(CoinbaseError::UnorderedMinerSignals);
        }
        Ok(Self {
            signals,
            poll_digest,
        })
    }

    /// Returns the signals, in ascending order of `signal_code`.
    pub fn signals(&self) -> &[MinerSignal] {
        &self.signals
    }

    /// Returns the digest identifying the poll that the signals respond to.
    pub fn poll_digest(&self) -> &[u8; 16] {
        &self.poll_digest
    }
}

/// A violation of the coinbase bundle's encoding rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoinbaseError {
    /// `blockHeight` is not in `{1 .. 499999999}`.
    BlockHeightOutOfRange,
    /// `lockboxValue` is greater than `blockSubsidy`.
    LockboxExceedsSubsidy,
    /// `coinbaseData` is longer than [`MAX_COINBASE_DATA_LEN`] bytes.
    CoinbaseDataTooLong,
    /// The signals are not in strictly ascending order of `signal_code`.
    UnorderedMinerSignals,
    /// A poll response has no signals; it is encoded by omitting the poll instead.
    EmptyMinerPoll,
}

impl CoinbaseError {
    /// Describes the violated rule, for errors that carry static messages.
    pub fn message(self) -> &'static str {
        match self {
            Self::BlockHeightOutOfRange => "coinbase blockHeight is out of range",
            Self::LockboxExceedsSubsidy => "coinbase lockboxValue exceeds its blockSubsidy",
            Self::CoinbaseDataTooLong => "coinbaseData is longer than 94 bytes",
            Self::UnorderedMinerSignals => "miner signals are not in strictly ascending order",
            Self::EmptyMinerPoll => "a miner poll response needs at least one signal",
        }
    }
}

impl std::fmt::Display for CoinbaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for CoinbaseError {}

/// The coinbase bundle of a v7 coinbase transaction ([ZIP 248]).
///
/// It replaces the coinbase input of earlier transaction versions: its presence makes a
/// v7 transaction a coinbase transaction, and it states the block height. It has no
/// authorizing data.
///
/// [ZIP 248]: https://zips.z.cash/zip-0248
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct CoinbaseBundle {
    height: block::Height,
    block_subsidy: Amount<NonNegative>,
    lockbox_value: Amount<NonNegative>,
    coinbase_data: Vec<u8>,
    miner_poll: Option<MinerPoll>,
}

impl CoinbaseBundle {
    /// Constructs a coinbase bundle, checking the ZIP 248 rules on its own fields.
    ///
    /// Whether the fields match the block that contains the transaction is checked by
    /// consensus, not here.
    pub fn new(
        height: block::Height,
        block_subsidy: Amount<NonNegative>,
        lockbox_value: Amount<NonNegative>,
        coinbase_data: Vec<u8>,
        miner_poll: Option<MinerPoll>,
    ) -> Result<Self, CoinbaseError> {
        if !(1..=MAX_COINBASE_BLOCK_HEIGHT).contains(&height.0) {
            return Err(CoinbaseError::BlockHeightOutOfRange);
        }
        if lockbox_value > block_subsidy {
            return Err(CoinbaseError::LockboxExceedsSubsidy);
        }
        if coinbase_data.len() > MAX_COINBASE_DATA_LEN {
            return Err(CoinbaseError::CoinbaseDataTooLong);
        }
        Ok(Self {
            height,
            block_subsidy,
            lockbox_value,
            coinbase_data,
            miner_poll,
        })
    }

    /// Returns the height of the block that the transaction is mined in.
    pub fn height(&self) -> block::Height {
        self.height
    }

    /// Returns the block subsidy for that block.
    pub fn block_subsidy(&self) -> Amount<NonNegative> {
        self.block_subsidy
    }

    /// Returns the part of the block subsidy deposited into the lockbox.
    pub fn lockbox_value(&self) -> Amount<NonNegative> {
        self.lockbox_value
    }

    /// Returns the data chosen by the miner.
    pub fn coinbase_data(&self) -> &[u8] {
        &self.coinbase_data
    }

    /// Returns the miner's response to a poll, if any.
    pub fn miner_poll(&self) -> Option<&MinerPoll> {
        self.miner_poll.as_ref()
    }
}
