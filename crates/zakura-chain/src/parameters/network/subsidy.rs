//! Calculations for Block Subsidy and Funding Streams
//!
//! This module contains the consensus parameters which are required for
//! verification.
//!
//! Some consensus parameters change based on network upgrades. Each network
//! upgrade happens at a particular block height. Some parameters have a value
//! (or function) before the upgrade height, at the upgrade height, and after
//! the upgrade height. (For example, the value of the reserved field in the
//! block header during the Heartwood upgrade.)
//!
//! Typically, consensus parameters are accessed via a function that takes a
//! `Network` and `block::Height`.

pub(crate) mod constants;
mod fees;

pub use fees::miner_fee_share;

use std::{collections::HashMap, sync::OnceLock};

use crate::{
    amount::{self, Amount, NegativeAllowed, NonNegative, MAX_MONEY},
    block::{Height, HeightDiff},
    parameters::{Network, NetworkUpgrade, NU7_POW_TARGET_SPACING_RATIO},
    transparent,
};

use constants::{
    mainnet, testnet, BLOSSOM_POW_TARGET_SPACING_RATIO, FUNDING_STREAM_RECEIVER_DENOMINATOR,
    FUNDING_STREAM_SPECIFICATION, LOCKBOX_SPECIFICATION, MAX_BLOCK_SUBSIDY,
    POST_BLOSSOM_HALVING_INTERVAL, PRE_BLOSSOM_HALVING_INTERVAL,
};

/// The funding stream receiver categories.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FundingStreamReceiver {
    /// The Electric Coin Company (Bootstrap Foundation) funding stream.
    #[serde(rename = "ECC")]
    Ecc,

    /// The Zcash Foundation funding stream.
    ZcashFoundation,

    /// The Major Grants (Zcash Community Grants) funding stream.
    MajorGrants,

    /// The deferred pool contribution, see [ZIP-1015](https://zips.z.cash/zip-1015) for more details.
    Deferred,
}

impl FundingStreamReceiver {
    /// Returns a human-readable name and a specification URL for the receiver, as described in
    /// [ZIP-1014] and [`zcashd`] before NU6. After NU6, the specification is in the [ZIP-1015].
    ///
    /// [ZIP-1014]: https://zips.z.cash/zip-1014#abstract
    /// [`zcashd`]: https://github.com/zcash/zcash/blob/3f09cfa00a3c90336580a127e0096d99e25a38d6/src/consensus/funding.cpp#L13-L32
    /// [ZIP-1015]: https://zips.z.cash/zip-1015
    pub fn info(&self, is_post_nu6: bool) -> (&'static str, &'static str) {
        if is_post_nu6 {
            (
                match self {
                    FundingStreamReceiver::Ecc => "Electric Coin Company",
                    FundingStreamReceiver::ZcashFoundation => "Zcash Foundation",
                    FundingStreamReceiver::MajorGrants => "Zcash Community Grants NU6",
                    FundingStreamReceiver::Deferred => "Lockbox NU6",
                },
                LOCKBOX_SPECIFICATION,
            )
        } else {
            (
                match self {
                    FundingStreamReceiver::Ecc => "Electric Coin Company",
                    FundingStreamReceiver::ZcashFoundation => "Zcash Foundation",
                    FundingStreamReceiver::MajorGrants => "Major Grants",
                    FundingStreamReceiver::Deferred => "Lockbox NU6",
                },
                FUNDING_STREAM_SPECIFICATION,
            )
        }
    }

    /// Returns true if this [`FundingStreamReceiver`] is [`FundingStreamReceiver::Deferred`].
    pub fn is_deferred(&self) -> bool {
        matches!(self, Self::Deferred)
    }
}

/// Funding stream recipients and height ranges.
#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct FundingStreams {
    /// Start and end Heights for funding streams
    /// as described in [protocol specification §7.10.1][7.10.1].
    ///
    /// [7.10.1]: https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams
    height_range: std::ops::Range<Height>,
    /// Funding stream recipients by [`FundingStreamReceiver`].
    recipients: HashMap<FundingStreamReceiver, FundingStreamRecipient>,
}

impl FundingStreams {
    /// Creates a new [`FundingStreams`].
    pub fn new(
        height_range: std::ops::Range<Height>,
        recipients: HashMap<FundingStreamReceiver, FundingStreamRecipient>,
    ) -> Self {
        Self {
            height_range,
            recipients,
        }
    }

    /// Creates a new empty [`FundingStreams`] representing no funding streams.
    pub fn empty() -> Self {
        Self::new(Height::MAX..Height::MAX, HashMap::new())
    }

    /// Returns height range where these [`FundingStreams`] should apply.
    pub fn height_range(&self) -> &std::ops::Range<Height> {
        &self.height_range
    }

    /// Returns recipients of these [`FundingStreams`].
    pub fn recipients(&self) -> &HashMap<FundingStreamReceiver, FundingStreamRecipient> {
        &self.recipients
    }

    /// Returns a recipient with the provided receiver.
    pub fn recipient(&self, receiver: FundingStreamReceiver) -> Option<&FundingStreamRecipient> {
        self.recipients.get(&receiver)
    }

    /// Accepts a target number of addresses that all recipients of this funding stream
    /// except the [`FundingStreamReceiver::Deferred`] receiver should have.
    ///
    /// Extends the addresses for all funding stream recipients by repeating their
    /// existing addresses until reaching the provided target number of addresses.
    pub fn extend_recipient_addresses(&mut self, target_len: usize) {
        for (receiver, recipient) in &mut self.recipients {
            if receiver.is_deferred() {
                continue;
            }

            recipient.extend_addresses(target_len);
        }
    }
}

/// A funding stream recipient as specified in [protocol specification §7.10.1][7.10.1]
///
/// [7.10.1]: https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams
#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct FundingStreamRecipient {
    /// The numerator for each funding stream receiver category
    /// as described in [protocol specification §7.10.1][7.10.1].
    ///
    /// [7.10.1]: https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams
    numerator: u64,
    /// Addresses for the funding stream recipient
    addresses: Vec<transparent::Address>,
}

impl FundingStreamRecipient {
    /// Creates a new [`FundingStreamRecipient`].
    pub fn new<I, T>(numerator: u64, addresses: I) -> Self
    where
        T: ToString,
        I: IntoIterator<Item = T>,
    {
        Self {
            numerator,
            addresses: addresses
                .into_iter()
                .map(|addr| {
                    let addr = addr.to_string();
                    addr.parse()
                        .expect("funding stream address must deserialize")
                })
                .collect(),
        }
    }

    /// Returns the numerator for this funding stream.
    pub fn numerator(&self) -> u64 {
        self.numerator
    }

    /// Returns the receiver of this funding stream.
    pub fn addresses(&self) -> &[transparent::Address] {
        &self.addresses
    }

    /// Accepts a target number of addresses that this recipient should have.
    ///
    /// Extends the addresses for this funding stream recipient by repeating
    /// existing addresses until reaching the provided target number of addresses.
    ///
    /// # Panics
    ///
    /// If there are no recipient addresses.
    pub fn extend_addresses(&mut self, target_len: usize) {
        assert!(
            !self.addresses.is_empty(),
            "cannot extend addresses for empty recipient"
        );

        self.addresses = self
            .addresses
            .iter()
            .cycle()
            .take(target_len)
            .cloned()
            .collect();
    }
}

/// Functionality specific to block subsidy-related consensus rules
pub trait ParameterSubsidy {
    /// Returns the minimum height after the first halving
    /// as described in [protocol specification §7.10][7.10]
    ///
    /// [7.10]: <https://zips.z.cash/protocol/protocol.pdf#fundingstreams>
    fn height_for_first_halving(&self) -> Height;

    /// Returns the halving interval after Blossom
    fn post_blossom_halving_interval(&self) -> HeightDiff;

    /// Returns the halving interval before Blossom
    fn pre_blossom_halving_interval(&self) -> HeightDiff;

    /// Returns the address change interval for funding streams
    /// as described in [protocol specification §7.10][7.10].
    ///
    /// > FSRecipientChangeInterval := PostBlossomHalvingInterval / 48
    ///
    /// [7.10]: https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams
    fn funding_stream_address_change_interval(&self) -> HeightDiff;

    /// Returns the expected public seed or configured override, or zero when unset.
    /// State derives the actual seed from monetary pools unless a configured override applies.
    fn initial_nsm_value_balance(&self) -> Amount<NonNegative>;
}

/// Network methods related to Block Subsidy and Funding Streams
impl ParameterSubsidy for Network {
    fn height_for_first_halving(&self) -> Height {
        // First halving on Mainnet is at Canopy
        // while in Testnet is at block constant height of `1_116_000`
        // <https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams>
        //
        // Regtest and configured testnets derive it, because their target
        // spacings, including the 25 second spacing after NU7, set the height.
        match self {
            Network::Mainnet => NetworkUpgrade::Canopy
                .activation_height(self)
                .expect("canopy activation height should be available"),
            Network::Testnet(params) => {
                if params.is_default_testnet() {
                    testnet::FIRST_HALVING
                } else {
                    height_for_halving(1, self).expect("first halving height should be available")
                }
            }
        }
    }

    fn post_blossom_halving_interval(&self) -> HeightDiff {
        match self {
            Network::Mainnet => POST_BLOSSOM_HALVING_INTERVAL,
            Network::Testnet(params) => params.post_blossom_halving_interval(),
        }
    }

    fn pre_blossom_halving_interval(&self) -> HeightDiff {
        match self {
            Network::Mainnet => PRE_BLOSSOM_HALVING_INTERVAL,
            Network::Testnet(params) => params.pre_blossom_halving_interval(),
        }
    }

    fn funding_stream_address_change_interval(&self) -> HeightDiff {
        self.post_blossom_halving_interval() / 48
    }

    fn initial_nsm_value_balance(&self) -> Amount<NonNegative> {
        match self {
            Network::Mainnet => mainnet::INITIAL_NSM_VALUE_BALANCE,
            Network::Testnet(params) => params.initial_nsm_value_balance(),
        }
    }
}

/// Returns the address change period
/// as described in [protocol specification §7.10][7.10]
///
/// [7.10]: https://zips.z.cash/protocol/protocol.pdf#fundingstreams
pub fn funding_stream_address_period<N: ParameterSubsidy>(
    height: Height,
    network: &N,
) -> HeightDiff {
    // Spec equation: `address_period = floor((height -
    // (height_for_halving(1) - post_blossom_halving_interval)) /
    // funding_stream_address_change_interval)`,
    // <https://zips.z.cash/protocol/protocol.pdf#fundingstreams>
    //
    // Note that the brackets make it so the post-Blossom halving interval is
    // added to the total.

    let height_after_first_halving = height - network.height_for_first_halving();

    // `div_euclid` matches the specification's floor because the interval is
    // positive. The regression test uses a height one block before the
    // address-period anchor: its numerator is -1, so `/` would truncate it
    // to 0 rather than floor it to -1.
    (height_after_first_halving + network.post_blossom_halving_interval())
        .div_euclid(network.funding_stream_address_change_interval())
}

/// The first block height of the halving at the provided halving index for a network.
///
/// See `Halving(height)`, as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
pub fn height_for_halving(halving: u32, network: &Network) -> Option<Height> {
    if halving == 0 {
        return Some(Height(0));
    }

    if self::halving(Height::MAX, network) < halving {
        return None;
    }

    // `halving` is monotonic. Search its complete height domain so this inverse
    // automatically includes every target-spacing era.
    let mut low = Height::MIN.0;
    let mut high = Height::MAX.0;
    while low < high {
        let middle = low + (high - low) / 2;
        if self::halving(Height(middle), network) < halving {
            low = middle + 1;
        } else {
            high = middle;
        }
    }

    Some(Height(low))
}

/// Returns the `fs.Value(height)` for each stream receiver
/// as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
pub fn funding_stream_values(
    height: Height,
    network: &Network,
    expected_block_subsidy: Amount<NonNegative>,
) -> Result<HashMap<FundingStreamReceiver, Amount<NonNegative>>, amount::Error> {
    let mut results = HashMap::new();

    if expected_block_subsidy.is_zero() {
        return Ok(results);
    }

    if NetworkUpgrade::current(network, height) >= NetworkUpgrade::Canopy {
        let funding_streams = network.funding_streams(height);
        if let Some(funding_streams) = funding_streams {
            // From NU7, each stream's share of the halving subsidy follows the exact ZIP 218
            // rounding rule, see [`Nu7RoundingGroup`]. Any ZIP 234 reissuance bonus above
            // the halving subsidy keeps the specification's floor rule. On the public
            // networks the Revision 2 streams end long before reissuance starts, so the
            // bonus is zero while a stream pays.
            let group = nu7_rounding_group(height, network);
            let bonus = match group {
                Some(group) => {
                    let halving_subsidy =
                        Amount::try_from(group.share(group.post_blossom_subsidy()))?;
                    (expected_block_subsidy - halving_subsidy)?
                }
                None => expected_block_subsidy,
            };

            for (&receiver, recipient) in funding_streams.recipients() {
                // - Spec equation: `fs.value = floor(block_subsidy(height)*(fs.numerator/fs.denominator))`:
                //   https://zips.z.cash/protocol/protocol.pdf#subsidies
                // - In Rust, "integer division rounds towards zero":
                //   https://doc.rust-lang.org/stable/reference/expressions/operator-expr.html#arithmetic-and-logical-binary-operators
                //   This is the same as `floor()`, because these numbers are all positive.
                let floor_share =
                    ((bonus * recipient.numerator())? / FUNDING_STREAM_RECEIVER_DENOMINATOR)?;

                let amount_value = match group {
                    Some(group) => {
                        let post_blossom_value =
                            group.post_blossom_stream_value(recipient.numerator());
                        (Amount::try_from(group.share(post_blossom_value))? + floor_share)?
                    }
                    None => floor_share,
                };

                results.insert(receiver, amount_value);
            }
        }
    }

    Ok(results)
}

/// Block subsidy errors.
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum SubsidyError {
    #[error("no coinbase transaction in block")]
    NoCoinbase,

    #[error("funding stream expected output not found")]
    FundingStreamNotFound,

    #[error("founders reward output not found")]
    FoundersRewardNotFound,

    #[error("one-time lockbox disbursement output not found")]
    OneTimeLockboxDisbursementNotFound,

    #[error("miner fees are invalid")]
    InvalidMinerFees,

    #[error("ZIP 234 block subsidy needs the NSM value balance after the parent block")]
    MissingNsmValueBalance,

    #[error(
        "issued supply exceeds the scheduled supply, so the ZIP 234 NSM value balance is negative"
    )]
    NegativeNsmValueBalance,

    #[error("addition of amounts overflowed")]
    Overflow,

    #[error("subtraction of amounts underflowed")]
    Underflow,

    #[error("unsupported height")]
    UnsupportedHeight,

    #[error("invalid amount")]
    InvalidAmount(#[from] amount::Error),
}

/// The divisor used for halvings.
///
/// `1 << Halving(height)`, as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
///
/// Returns `None` if the divisor would overflow a `u64`.
pub fn halving_divisor(height: Height, network: &Network) -> Option<u64> {
    // Some far-future shifts can be more than 63 bits
    1u64.checked_shl(halving(height, network))
}

/// The halving index for a block height and network.
///
/// `Halving(height)`, as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
pub fn halving(height: Height, network: &Network) -> u32 {
    let slow_start_shift = network.slow_start_shift();
    if height < slow_start_shift {
        return 0;
    }

    // Each target spacing era contributes (blocks in the era * era spacing) to a
    // running total of block seconds, which the pre-Blossom halving interval
    // measured in seconds then divides. This is the spec's segmented sum of
    // fractions with the common denominator factored out, so it stays in integer
    // arithmetic no matter how many spacing eras a network has. ZIP 218 adds a
    // third era at NU7.
    //
    // The spec's first term is `BlossomActivationHeight - SlowStartShift`
    // pre-Blossom blocks, which is negative when Blossom activates below
    // `SlowStartShift`. So the sum starts at `-SlowStartShift` pre-Blossom blocks
    // instead of clamping each era at `SlowStartShift`.
    let pre_blossom_spacing_seconds = NetworkUpgrade::Genesis.target_spacing().num_seconds();
    let mut total_block_seconds: HeightDiff =
        -HeightDiff::from(slow_start_shift.0) * pre_blossom_spacing_seconds;

    let mut eras = NetworkUpgrade::target_spacings(network)
        .filter(|(era_start, _)| *era_start <= height)
        .peekable();

    while let Some((era_start, era_spacing)) = eras.next() {
        let era_end = eras
            .peek()
            .map(|(next_start, _)| *next_start)
            .unwrap_or(height);
        total_block_seconds += (era_end - era_start) * era_spacing.num_seconds();
    }

    let pre_blossom_denominator =
        network.pre_blossom_halving_interval() * pre_blossom_spacing_seconds;

    // The sum is negative just above `SlowStartShift` when Blossom activates
    // below it. The spec's floor then gives a negative index, which has no
    // meaning, so this returns zero there.
    (total_block_seconds / pre_blossom_denominator)
        .max(0)
        .try_into()
        .expect("halving index is non-negative and fits in u32")
}

/// A block's position in its ZIP 218 rounding group.
///
/// ZIP 218 divides the block subsidy by `NU7PoWTargetSpacingRatio` (`R` = 3) at NU7, so a
/// post-NU7 block pays a third of what a post-Blossom block paid at the same halving. The
/// post-Blossom subsidy and the funding stream values do not divide evenly by three, so
/// taking `floor` at every block pays the streams less and the miner more than their
/// specified shares, by a few zatoshi for every three blocks.
///
/// This type implements the exact rule instead. Post-NU7 blocks form groups of `R`
/// consecutive blocks aligned to the NU7 activation height `A`. Each amount `V` that a
/// post-Blossom block would pay at this halving is split over the group so that the block
/// at index `k = (height − A) mod R` pays
///
/// > `floor((k + 1) · V / R) − floor(k · V / R)`
///
/// Every group therefore pays exactly `V`, and every block pays `floor(V / R)` or one
/// zatoshi more. For the block subsidy, the first block of each group pays the plain
/// ZIP 218 amount. A funding stream's `V` is its share of the post-Blossom subsidy, so a
/// stream whose `V` divides evenly by `R` is paid exactly at every block, where the plain
/// rule's `floor` of a share of an already-floored subsidy could drop a zatoshi. Halving
/// heights and the ZIP 214 funding stream end heights fall on group boundaries, because
/// every post-NU7 halving interval is a multiple of `R` blocks and the pre-NU7 intervals
/// map onto `R` blocks each.
///
/// The miner's share is the remainder of the subsidy after the streams, as before, so the
/// coinbase still balances exactly at every block.
///
/// See [`nu7_rounding_group`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Nu7RoundingGroup {
    /// The number of blocks per group, `NU7PoWTargetSpacingRatio`.
    blocks_per_group: u64,
    /// The number of blocks from NU7 activation up to this block.
    blocks_since_activation: u64,
    /// `floor(MaxBlockSubsidy / (BlossomPoWTargetSpacingRatio · 2^Halving(height)))`, the
    /// subsidy of a post-Blossom block at this block's halving.
    post_blossom_subsidy: u64,
}

impl Nu7RoundingGroup {
    /// Returns this block's index within its group, `(height − A) mod R`.
    pub fn index(&self) -> u64 {
        self.blocks_since_activation % self.blocks_per_group
    }

    /// Returns the subsidy a post-Blossom block pays at this block's halving.
    pub fn post_blossom_subsidy(&self) -> u64 {
        self.post_blossom_subsidy
    }

    /// Returns the value a post-Blossom block pays at this block's halving to a funding
    /// stream with `numerator`, `floor(post_blossom_subsidy · numerator / 100)`.
    pub fn post_blossom_stream_value(&self, numerator: u64) -> u64 {
        self.post_blossom_subsidy * numerator / FUNDING_STREAM_RECEIVER_DENOMINATOR
    }

    /// Returns this block's share of `post_blossom_value`.
    pub fn share(&self, post_blossom_value: u64) -> u64 {
        u64::try_from(self.run_total(post_blossom_value, 1))
            .expect("a share is at most the post-Blossom value, which fits in u64")
    }

    /// Returns the total that this block and the `blocks − 1` blocks after it pay to a
    /// recipient whose post-Blossom value is `post_blossom_value`.
    ///
    /// The shares telescope: the blocks from NU7 activation through any height pay
    /// `floor(blocks · V / R)` in total, so a run's total is a difference of two floors.
    /// The run must not cross a halving, where `V` changes.
    pub fn run_total(&self, post_blossom_value: u64, blocks: u128) -> u128 {
        let paid_through = |blocks: u128| {
            blocks * u128::from(post_blossom_value) / u128::from(self.blocks_per_group)
        };
        let start = u128::from(self.blocks_since_activation);

        paid_through(start + blocks) - paid_through(start)
    }
}

/// Returns the ZIP 218 rounding group of the block at `height`, or `None` if the exact
/// rounding rule does not apply there.
///
/// The rule applies from NU7 activation to every block after the slow start whose
/// halving subsidy is not zero. The slow start pays a per-block rate that ZIP 218 leaves
/// unchanged, and a zero subsidy has nothing to split.
///
/// See [`Nu7RoundingGroup`].
pub fn nu7_rounding_group(height: Height, net: &Network) -> Option<Nu7RoundingGroup> {
    let activation = NetworkUpgrade::Nu7.activation_height(net)?;
    if height < activation || height < net.slow_start_interval() {
        return None;
    }
    let halving_div = halving_divisor(height, net)?;

    let blocks_since_activation = u64::try_from(height - activation)
        .expect("the height is at or above the activation height");

    Some(Nu7RoundingGroup {
        blocks_per_group: u64::from(NU7_POW_TARGET_SPACING_RATIO),
        blocks_since_activation,
        post_blossom_subsidy: MAX_BLOCK_SUBSIDY
            / u64::from(BLOSSOM_POW_TARGET_SPACING_RATIO)
            / halving_div,
    })
}

/// `ln(2)` scaled by [`BLOCK_SUBSIDY_FRACTION_DENOMINATOR`] and rounded up to a
/// multiple of 1,680,000, as specified by the [halving-preserving NSM draft].
///
/// [halving-preserving NSM draft]: https://github.com/zcash/zips/blob/60720df9e971e19d8b6f67f1869426d78c96d250/zips/draft-judah-nsm-halving-preserving-issuance.md
pub const LN2_SCALED: u128 = 6_931_680_000;

/// The numerator of the [halving-preserving NSM draft]'s `BLOCK_SUBSIDY_FRACTION` from NU7.
///
/// ZIP 218 reduces the target spacing from 75 to 25 seconds, tripling the number
/// of blocks in the same four-year payout half-life. This is
/// `floor(LN2_SCALED / 5_040_000)`. The fraction is fixed on every network,
/// including custom networks with shorter or longer scheduled halving intervals.
/// Reissuance always uses the NU7 payout rate; future spacing changes must
/// explicitly revisit this constant.
///
/// [halving-preserving NSM draft]: https://github.com/zcash/zips/blob/60720df9e971e19d8b6f67f1869426d78c96d250/zips/draft-judah-nsm-halving-preserving-issuance.md
pub const BLOCK_SUBSIDY_FRACTION_NUMERATOR: u128 = 1_375;

/// The denominator of the [halving-preserving NSM draft]'s `BLOCK_SUBSIDY_FRACTION`.
///
/// [halving-preserving NSM draft]: https://github.com/zcash/zips/blob/60720df9e971e19d8b6f67f1869426d78c96d250/zips/draft-judah-nsm-halving-preserving-issuance.md
pub const BLOCK_SUBSIDY_FRACTION_DENOMINATOR: u128 = 10_000_000_000;

/// The halving era whose internal NSM reference crossing determines the proposed
/// reissuance start.
const NSM_REISSUANCE_START_HALVING: u32 = 3;

/// Calculates the proposed NSM reissuance crossing on `network`'s actual spacing
/// schedule, without activating reissuance.
///
/// The crossing is the first height after the third halving where the reference
/// NSM subsidy, assuming no funds were removed from circulation, is less than the
/// scheduled block subsidy. ZIP 218's 25-second spacing, its effect on halving
/// heights, and per-block subsidy rounding are included through `network`.
///
/// Searches through [`Height::MAX`] if the fourth halving is beyond the supported
/// height range. Returns `None` when NU7 is not configured or the third halving era
/// has no crossing within the supported heights.
pub(crate) fn nsm_reissuance_crossing_height(
    network: &Network,
) -> Result<Option<Height>, SubsidyError> {
    let Some(nu7) = NetworkUpgrade::Nu7.activation_height(network) else {
        return Ok(None);
    };
    let Some(third_halving) = height_for_halving(NSM_REISSUANCE_START_HALVING, network) else {
        return Ok(None);
    };
    let run_end = match height_for_halving(NSM_REISSUANCE_START_HALVING + 1, network) {
        Some(fourth_halving) => fourth_halving
            .previous()
            .map_err(|_| SubsidyError::UnsupportedHeight)?,
        None => Height::MAX,
    };

    let first_candidate = Height(
        third_halving
            .0
            .checked_add(1)
            .ok_or(SubsidyError::Overflow)?,
    )
    .max(nu7);
    if first_candidate > run_end {
        return Ok(None);
    }

    let parent = first_candidate
        .previous()
        .map_err(|_| SubsidyError::UnsupportedHeight)?;
    let supply_before_first = scheduled_issuance_zatoshis(parent, network)?;
    let max_money = u128::try_from(MAX_MONEY).map_err(|_| SubsidyError::Overflow)?;

    // The run stays in the third halving era, so it is one ZIP 218 rounding run.
    if let Some(group) = nu7_rounding_group(first_candidate, network) {
        return Ok(first_nsm_crossing_in_rounding_run(
            first_candidate.0,
            run_end.0,
            supply_before_first,
            group,
            max_money,
        )?
        .map(Height));
    }

    let subsidy = amount_to_u128(halving_block_subsidy(first_candidate, network)?);

    Ok(first_nsm_crossing_in_subsidy_run(
        first_candidate.0,
        run_end.0,
        supply_before_first,
        subsidy,
        max_money,
    )?
    .map(Height))
}

/// Returns the first reference NSM crossing in a run of ZIP 218 rounding groups.
///
/// `group` is the rounding group of `first`, and `supply_before_first` is the scheduled
/// supply after its parent. Every block in the run shares `first`'s halving.
pub(super) fn first_nsm_crossing_in_rounding_run(
    first: u32,
    run_end: u32,
    supply_before_first: u128,
    group: Nu7RoundingGroup,
    max_money: u128,
) -> Result<Option<u32>, SubsidyError> {
    let value = group.post_blossom_subsidy();

    // No block in the run pays more than `ceil(V / R)`. A run that paid that much at
    // every block would have issued at least as much by any height, leaving at most the
    // same reserve, and would offer at least as large a subsidy there. So wherever the
    // exact run crosses, that run has already crossed: its crossing is a lower bound.
    let max_share = u128::from(value).div_ceil(u128::from(group.blocks_per_group));
    let Some(lower_bound) = first_nsm_crossing_in_subsidy_run(
        first,
        run_end,
        supply_before_first,
        max_share,
        max_money,
    )?
    else {
        return Ok(None);
    };

    // The bound overstates issuance by less than one zatoshi per block, which moves the
    // crossing by a fraction of a block, so this scan ends within a few blocks. It stops at
    // the run end regardless.
    for height in lower_bound..=run_end {
        let blocks_before = u128::from(height - first);
        let supply_before = supply_before_first
            .checked_add(group.run_total(value, blocks_before))
            .ok_or(SubsidyError::Overflow)?;
        let subsidy =
            group.run_total(value, blocks_before + 1) - group.run_total(value, blocks_before);

        let reserve = max_money.saturating_sub(supply_before);
        let reference = reserve
            .checked_mul(BLOCK_SUBSIDY_FRACTION_NUMERATOR)
            .ok_or(SubsidyError::Overflow)?
            .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);

        if reference < subsidy {
            return Ok(Some(height));
        }
    }

    Ok(None)
}

/// Returns the first reference NSM crossing in a constant-subsidy run.
///
/// `supply_before_first` is the scheduled supply after the parent of `first`.
pub(super) fn first_nsm_crossing_in_subsidy_run(
    first: u32,
    run_end: u32,
    supply_before_first: u128,
    subsidy: u128,
    max_money: u128,
) -> Result<Option<u32>, SubsidyError> {
    if first > run_end || subsidy == 0 {
        return Ok(None);
    }

    // `ceil(fraction * reserve) < subsidy` exactly when the reserve is at most
    // this threshold. Subtracting one implements the strict inequality.
    let max_reserve = subsidy
        .checked_sub(1)
        .and_then(|subsidy| subsidy.checked_mul(BLOCK_SUBSIDY_FRACTION_DENOMINATOR))
        .ok_or(SubsidyError::Overflow)?
        / BLOCK_SUBSIDY_FRACTION_NUMERATOR;
    let reserve = max_money.saturating_sub(supply_before_first);
    let blocks_until_crossing = reserve.saturating_sub(max_reserve).div_ceil(subsidy);
    let crossing = u128::from(first)
        .checked_add(blocks_until_crossing)
        .ok_or(SubsidyError::Overflow)?;

    if crossing > u128::from(run_end) {
        Ok(None)
    } else {
        Ok(Some(
            u32::try_from(crossing).map_err(|_| SubsidyError::Overflow)?,
        ))
    }
}

/// Returns the NSM reissuance start height on `network`, or `None` if it is not scheduled.
///
/// NU7 activation determines the first reference crossing strictly after the third
/// halving and before the fourth halving. No crossing leaves reissuance unscheduled.
/// The result depends only on network parameters, never chain balances.
/// Reissuance never starts on a network without NU7. There is no deployment-height
/// constant or configuration override.
pub fn nsm_reissuance_height(network: &Network) -> Option<Height> {
    NetworkUpgrade::Nu7.activation_height(network)?;
    let derive = || {
        nsm_reissuance_crossing_height(network)
            .expect("validated network schedules fit the u128 crossing arithmetic")
    };
    // Block validation queries this repeatedly; immutable parameters determine the
    // result, including the absence of a crossing, so derive it only once per network.
    let start = match network {
        Network::Mainnet => {
            static MAINNET_CROSSING: OnceLock<Option<Height>> = OnceLock::new();
            *MAINNET_CROSSING.get_or_init(derive)
        }
        Network::Testnet(params) => {
            #[cfg(any(test, feature = "proptest-impl"))]
            if let Some(start) = params.test_nsm_reissuance_height() {
                return Some(start.max(NetworkUpgrade::Nu7.activation_height(network)?));
            }
            params.nsm_reissuance_crossing_height().get_or_init(derive)
        }
    }?;

    Some(start)
}

/// Converts a non-negative amount to a `u128`.
fn amount_to_u128(amount: Amount<NonNegative>) -> u128 {
    u128::try_from(i64::from(amount)).expect("non-negative amounts fit in u128")
}

/// Returns whether NSM reissuance is active on `network` at `height`.
///
/// Callers use this to decide whether to fetch the money reserve for the block subsidy.
pub fn is_zip234_active(network: &Network, height: Height) -> bool {
    nsm_reissuance_height(network).is_some_and(|start| height >= start)
}

/// Validates a signed parent NSM value balance for [`reissuance_bonus`].
///
/// The state stores a signed balance. Returns [`SubsidyError::NegativeNsmValueBalance`] if
/// the parent's chain issued more than the halving schedule.
pub fn parent_nsm_value_balance(
    nsm_value_balance: Amount<NegativeAllowed>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    nsm_value_balance
        .constrain()
        .map_err(|_| SubsidyError::NegativeNsmValueBalance)
}

/// Applies the [halving-preserving NSM draft] reissuance fraction to `amount`, rounding up.
///
/// [halving-preserving NSM draft]: https://github.com/zcash/zips/blob/60720df9e971e19d8b6f67f1869426d78c96d250/zips/draft-judah-nsm-halving-preserving-issuance.md
fn reissuance_amount(amount: Amount<NonNegative>) -> Result<Amount<NonNegative>, SubsidyError> {
    let subsidy = amount_to_u128(amount)
        .checked_mul(BLOCK_SUBSIDY_FRACTION_NUMERATOR)
        .ok_or(SubsidyError::Overflow)?
        .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);

    let subsidy = i64::try_from(subsidy).map_err(|_| SubsidyError::Overflow)?;

    Ok(Amount::try_from(subsidy)?)
}

/// Returns the reissuance bonus from the [halving-preserving NSM draft], given
/// the `NsmValueBalance` after the parent block. This is the arithmetic component
/// of `AdditionalBlockSubsidy`.
///
/// The halving schedule keeps issuing new ZEC. The bonus reissues value removed from
/// circulation. This helper does not check the reissuance activation height;
/// callers must gate its use and supply the balance from the actual parent.
/// It always uses the fixed NU7 fraction, even for a pre-activation balance.
/// A zero balance pays zero; a positive balance pays at least one zatoshi and
/// never more than that balance.
///
/// The state supplies the balance after the parent block; see
/// `Block::nsm_value_balance_change`.
///
/// [halving-preserving NSM draft]: https://github.com/zcash/zips/blob/60720df9e971e19d8b6f67f1869426d78c96d250/zips/draft-judah-nsm-halving-preserving-issuance.md
pub fn reissuance_bonus(
    nsm_value_balance: Amount<NonNegative>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    reissuance_amount(nsm_value_balance)
}

/// Returns `ExpectedIssuedSupply(height)` from zips#1354: the total block subsidy the
/// halving schedule issues for blocks `0..=height`. The genesis block's subsidy is zero.
///
/// The subsidy is linear in the height through the slow start, and piecewise constant
/// afterwards, changing only where a halving or a target spacing era begins. Summing over
/// those pieces is exact, and takes a bounded number of steps no matter how tall the chain
/// is.
pub fn expected_issued_supply(
    height: Height,
    net: &Network,
) -> Result<Amount<NonNegative>, SubsidyError> {
    let total = scheduled_issuance_zatoshis(height, net)?;
    let max_money = u128::try_from(MAX_MONEY).map_err(|_| SubsidyError::Overflow)?;
    Ok(Amount::try_from(
        i64::try_from(total.min(max_money)).map_err(|_| SubsidyError::Overflow)?,
    )?)
}

/// Return cumulative scheduled zatoshi without the Amount limit.
/// Migrations subtract the pre-NU7 baseline before constraining the eligible balance.
/// Custom schedules can exceed MAX_MONEY, so clamping either operand would lose value.
pub fn scheduled_issuance_zatoshis(height: Height, net: &Network) -> Result<u128, SubsidyError> {
    let slow_start_shift = u128::from(net.slow_start_shift().0);
    let slow_start_interval = u128::from(net.slow_start_interval().0);
    let height = u128::from(height.0);
    let mut total: u128 = 0;

    // The slow start issues `rate * h` below the shift and `rate * (h + 1)` from the shift
    // up to the interval, so each phase is a sum of consecutive integers.
    if slow_start_interval > 0 && slow_start_shift > 0 {
        let rate = u128::from(MAX_BLOCK_SUBSIDY) / slow_start_interval;
        let sum_from_one_through = |n: u128| n * (n + 1) / 2;

        // A short halving interval can overflow the halving divisor inside the slow start.
        // From that height on, every block subsidy is zero.
        let last_paying = first_overflowed_halving_height(net, slow_start_interval - 1)
            .map_or(slow_start_interval - 1, |cutoff| cutoff.saturating_sub(1));
        let slow_start_end = height.min(last_paying);

        // `rate * h` for h in 1..=min(slow_start_end, shift - 1).
        let first_phase_end = slow_start_end.min(slow_start_shift - 1);
        total += sum_from_one_through(first_phase_end) * rate;

        // `rate * (h + 1)` for h in shift..=slow_start_end, which is
        // `rate * k` for k in shift + 1..=slow_start_end + 1.
        if slow_start_end >= slow_start_shift {
            total += (sum_from_one_through(slow_start_end + 1)
                - sum_from_one_through(slow_start_shift))
                * rate;
        }
    }

    // After the slow start the subsidy only changes at a halving or a spacing era start,
    // so walk those boundaries and multiply each run of blocks by its subsidy.
    let mut block =
        u32::try_from(slow_start_interval.max(1)).map_err(|_| SubsidyError::Overflow)?;
    let height = u32::try_from(height).map_err(|_| SubsidyError::Overflow)?;

    while block <= height {
        // Once the halving divisor overflows, every later subsidy is zero.
        if halving_divisor(Height(block), net).is_none() {
            break;
        }
        let subsidy = u128::try_from(i64::from(halving_block_subsidy(Height(block), net)?))
            .map_err(|_| SubsidyError::Overflow)?;

        // The next boundary is whichever comes first: the end of this halving era, the
        // start of the next spacing era, or the end of the range.
        let run_end = next_subsidy_boundary(Height(block), net)
            .map(|boundary| {
                boundary
                    .previous()
                    .expect("a subsidy boundary after a block is above genesis")
                    .0
            })
            .unwrap_or(height)
            .min(height);
        let run_blocks = u128::from(run_end - block) + 1;

        // A run never crosses a halving, so within a ZIP 218 rounding run the shares
        // telescope into one difference of floors.
        total += match nu7_rounding_group(Height(block), net) {
            Some(group) => group.run_total(group.post_blossom_subsidy(), run_blocks),
            None => run_blocks * subsidy,
        };

        if run_end == height || run_end == u32::MAX {
            break;
        }
        block = run_end + 1;
    }

    Ok(total)
}

/// Returns the lowest height at or below `last` whose halving divisor overflows, if any.
fn first_overflowed_halving_height(net: &Network, last: u128) -> Option<u128> {
    let last = u32::try_from(last).ok()?;
    if halving_divisor(Height(last), net).is_some() {
        return None;
    }

    // `halving` is non-decreasing, so binary search for the first overflow.
    let (mut low, mut high) = (0, last);
    while low < high {
        let mid = low + (high - low) / 2;
        if halving_divisor(Height(mid), net).is_none() {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Some(u128::from(low))
}

/// Returns the lowest height above `height` at which the halving block subsidy changes,
/// or `None` if it never changes again.
fn next_subsidy_boundary(height: Height, net: &Network) -> Option<Height> {
    let current_halving = halving(height, net);

    // The next spacing era, if any, starts a new run.
    let next_spacing_era = NetworkUpgrade::target_spacings(net)
        .map(|(era_start, _)| era_start)
        .find(|era_start| *era_start > height);

    // `halving` is non-decreasing, so binary search for where it next increases.
    let mut low = height.0.checked_add(1)?;
    let mut high = Height::MAX_AS_U32;
    let next_halving = if halving(Height(high), net) > current_halving {
        while low < high {
            let mid = low + (high - low) / 2;

            if halving(Height(mid), net) > current_halving {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        Some(Height(low))
    } else {
        None
    };

    match (next_spacing_era, next_halving) {
        (Some(spacing), Some(halving)) => Some(spacing.min(halving)),
        (boundary, None) | (None, boundary) => boundary,
    }
}

/// `BlockSubsidy(height)` as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
pub fn block_subsidy(
    height: Height,
    net: &Network,
    nsm_value_balance: Option<Amount<NonNegative>>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    if is_zip234_active(net, height) {
        // The caller reads the NSM value balance from the parent block, so every caller
        // that can reach a ZIP 234 height must supply it.
        let nsm_value_balance = nsm_value_balance.ok_or(SubsidyError::MissingNsmValueBalance)?;

        let halving_subsidy = halving_block_subsidy(height, net)?;
        let bonus = reissuance_bonus(nsm_value_balance)?;

        return Ok((halving_subsidy + bonus)?);
    }

    halving_block_subsidy(height, net)
}

/// `BlockSubsidy(height)` under the halving schedule, ignoring ZIP 234.
///
/// ZIP 234 issues this subsidy plus a reissuance bonus. See [`block_subsidy`].
pub fn halving_block_subsidy(
    height: Height,
    net: &Network,
) -> Result<Amount<NonNegative>, SubsidyError> {
    let Some(halving_div) = halving_divisor(height, net) else {
        return Ok(Amount::zero());
    };

    let slow_start_interval = net.slow_start_interval();

    // The `floor` fn used in the spec is implicit in Rust's division of primitive integer types.

    let amount = if height < slow_start_interval {
        let slow_start_rate = MAX_BLOCK_SUBSIDY / u64::from(slow_start_interval);

        if height < net.slow_start_shift() {
            slow_start_rate * u64::from(height)
        } else {
            slow_start_rate * (u64::from(height) + 1)
        }
    } else if let Some(group) = nu7_rounding_group(height, net) {
        // From NU7, the post-Blossom subsidy is split exactly over each group of
        // `NU7PoWTargetSpacingRatio` blocks, see `Nu7RoundingGroup`.
        group.share(group.post_blossom_subsidy())
    } else {
        // Each spacing era scales the per-block subsidy by
        // `current_spacing / pre_blossom_spacing`, which keeps issuance per unit of
        // wall-clock time constant across spacing changes. Blossom divides the
        // subsidy by 2, and ZIP 218 divides it by a further 3 at NU7. The casts are
        // safe because target spacings are small positive constants.
        let current_spacing_seconds =
            NetworkUpgrade::target_spacing_for_height(net, height).num_seconds() as u64;
        let pre_blossom_spacing_seconds =
            NetworkUpgrade::Genesis.target_spacing().num_seconds() as u64;

        MAX_BLOCK_SUBSIDY * current_spacing_seconds / pre_blossom_spacing_seconds / halving_div
    };

    Ok(Amount::try_from(amount)?)
}

/// `MinerSubsidy(height)` as described in [protocol specification §7.8][7.8]
///
/// [7.8]: https://zips.z.cash/protocol/protocol.pdf#subsidies
pub fn miner_subsidy(
    height: Height,
    network: &Network,
    expected_block_subsidy: Amount<NonNegative>,
) -> Result<Amount<NonNegative>, amount::Error> {
    let founders_reward = founders_reward(network, height);

    let funding_streams_sum = funding_stream_values(height, network, expected_block_subsidy)?
        .values()
        .sum::<Result<Amount<NonNegative>, _>>()?;

    expected_block_subsidy - founders_reward - funding_streams_sum
}

/// Returns the founders reward address for a given height and network as described in [§7.9].
///
/// [§7.9]: <https://zips.z.cash/protocol/protocol.pdf#foundersreward>
pub fn founders_reward_address(net: &Network, height: Height) -> Option<transparent::Address> {
    let founders_address_list = net.founder_address_list();
    let num_founder_addresses = u32::try_from(founders_address_list.len()).ok()?;
    let slow_start_shift = u32::from(net.slow_start_shift());
    let pre_blossom_halving_interval = u32::try_from(net.pre_blossom_halving_interval()).ok()?;

    let founder_address_change_interval = slow_start_shift
        .checked_add(pre_blossom_halving_interval)?
        .div_ceil(num_founder_addresses);

    let founder_address_adjusted_height =
        if NetworkUpgrade::current(net, height) < NetworkUpgrade::Blossom {
            u32::from(height)
        } else {
            NetworkUpgrade::Blossom
                .activation_height(net)
                .and_then(|h| {
                    let blossom_activation_height = u32::from(h);
                    let height = u32::from(height);

                    blossom_activation_height.checked_add(
                        height.checked_sub(blossom_activation_height)?
                            / BLOSSOM_POW_TARGET_SPACING_RATIO,
                    )
                })?
        };

    let founder_address_index =
        usize::try_from(founder_address_adjusted_height / founder_address_change_interval).ok()?;

    founders_address_list
        .get(founder_address_index)
        .and_then(|a| a.parse().ok())
}

/// `FoundersReward(height)` as described in [§7.8].
///
/// [§7.8]: <https://zips.z.cash/protocol/protocol.pdf#subsidies>
pub fn founders_reward(net: &Network, height: Height) -> Amount<NonNegative> {
    // The founders reward is 20% of the block subsidy before the first halving, and 0 afterwards.
    //
    // On custom testnets, the first halving can occur later than Canopy, which causes an
    // inconsistency in the definition of the founders reward, which should occur only before
    // Canopy, so we check if Canopy is active as well.
    if halving(height, net) < 1 && NetworkUpgrade::current(net, height) < NetworkUpgrade::Canopy {
        // The founders reward ends at the first halving, which is long before ZIP 234
        // starts, so the halving schedule is the whole subsidy here.
        halving_block_subsidy(height, net)
            .map(|subsidy| subsidy.div_exact(5))
            .expect("block subsidy must be valid for founders rewards")
    } else {
        Amount::zero()
    }
}

#[cfg(test)]
mod tests;
