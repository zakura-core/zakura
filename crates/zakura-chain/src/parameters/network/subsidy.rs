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

use std::collections::HashMap;

use crate::{
    amount::{self, Amount, NonNegative, MAX_MONEY},
    block::{Height, HeightDiff},
    parameters::{
        Network, NetworkUpgrade, ZIP234_ENABLED, ZIP234_HALVINGS_ENABLED, ZIP234_SMOOTHING_ENABLED,
    },
    transparent,
};

use constants::{
    regtest, testnet, BLOSSOM_POW_TARGET_SPACING_RATIO, FUNDING_STREAM_RECEIVER_DENOMINATOR,
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
}

/// Network methods related to Block Subsidy and Funding Streams
impl ParameterSubsidy for Network {
    fn height_for_first_halving(&self) -> Height {
        // First halving on Mainnet is at Canopy
        // while in Testnet is at block constant height of `1_116_000`
        // <https://zips.z.cash/protocol/protocol.pdf#zip214fundingstreams>
        match self {
            Network::Mainnet => NetworkUpgrade::Canopy
                .activation_height(self)
                .expect("canopy activation height should be available"),
            Network::Testnet(params) => {
                if params.is_regtest() {
                    regtest::FIRST_HALVING
                } else if params.is_default_testnet() {
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
}

/// Returns the address change period
/// as described in [protocol specification §7.10][7.10]
///
/// [7.10]: https://zips.z.cash/protocol/protocol.pdf#fundingstreams
pub fn funding_stream_address_period<N: ParameterSubsidy>(height: Height, network: &N) -> u32 {
    // Spec equation: `address_period = floor((height - (height_for_halving(1) - post_blossom_halving_interval))/funding_stream_address_change_interval)`,
    // <https://zips.z.cash/protocol/protocol.pdf#fundingstreams>
    //
    // Note that the brackets make it so the post blossom halving interval is added to the total.
    //
    // In Rust, "integer division rounds towards zero":
    // <https://doc.rust-lang.org/stable/reference/expressions/operator-expr.html#arithmetic-and-logical-binary-operators>
    // This is the same as `floor()`, because these numbers are all positive.

    let height_after_first_halving = height - network.height_for_first_halving();

    let address_period = (height_after_first_halving + network.post_blossom_halving_interval())
        / network.funding_stream_address_change_interval();

    address_period
        .try_into()
        .expect("all values are positive and smaller than the input height")
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

    let slow_start_shift = i64::from(network.slow_start_shift().0);
    let blossom_height = i64::from(NetworkUpgrade::Blossom.activation_height(network)?.0);
    let pre_blossom_halving_interval = network.pre_blossom_halving_interval();
    let halving_index = i64::from(halving);

    let unscaled_height = halving_index.checked_mul(pre_blossom_halving_interval)?;

    let pre_blossom_height = unscaled_height
        .min(blossom_height)
        .checked_add(slow_start_shift)?;

    let post_blossom_height = 0
        .max(unscaled_height - blossom_height)
        .checked_mul(i64::from(BLOSSOM_POW_TARGET_SPACING_RATIO))?
        .checked_add(slow_start_shift)?;

    let height = pre_blossom_height.checked_add(post_blossom_height)?;

    let height = u32::try_from(height).ok()?;
    height.try_into().ok()
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
            for (&receiver, recipient) in funding_streams.recipients() {
                // - Spec equation: `fs.value = floor(block_subsidy(height)*(fs.numerator/fs.denominator))`:
                //   https://zips.z.cash/protocol/protocol.pdf#subsidies
                // - In Rust, "integer division rounds towards zero":
                //   https://doc.rust-lang.org/stable/reference/expressions/operator-expr.html#arithmetic-and-logical-binary-operators
                //   This is the same as `floor()`, because these numbers are all positive.
                let amount_value = ((expected_block_subsidy * recipient.numerator())?
                    / FUNDING_STREAM_RECEIVER_DENOMINATOR)?;

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

    #[error("ZIP 234 block subsidy needs the money reserve after the parent block")]
    MissingMoneyReserve,
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
    let pre_blossom_spacing_seconds = NetworkUpgrade::Genesis.target_spacing().num_seconds();
    let mut total_block_seconds: HeightDiff = 0;

    let mut eras = NetworkUpgrade::target_spacings(network)
        .filter(|(era_start, _)| *era_start <= height)
        .peekable();

    while let Some((era_start, era_spacing)) = eras.next() {
        let era_end = eras
            .peek()
            .map(|(next_start, _)| *next_start)
            .unwrap_or(height);
        let era_blocks = (era_end - era_start.max(slow_start_shift)).max(0);
        total_block_seconds += era_blocks * era_spacing.num_seconds();
    }

    let pre_blossom_denominator =
        network.pre_blossom_halving_interval() * pre_blossom_spacing_seconds;

    (total_block_seconds / pre_blossom_denominator)
        .try_into()
        .expect("halving index is non-negative and fits in u32")
}

/// The numerator of [ZIP 234]'s `BLOCK_SUBSIDY_FRACTION`.
///
/// [ZIP 234]: https://zips.z.cash/zip-0234
pub const BLOCK_SUBSIDY_FRACTION_NUMERATOR: u128 = 4_126;

/// The denominator of [`BLOCK_SUBSIDY_FRACTION_NUMERATOR`].
///
/// The fraction satisfies `(1 - BLOCK_SUBSIDY_FRACTION) ^ PostBlossomHalvingInterval` is
/// approximately one half, so a smoothed curve issues about as much over four years as a
/// halving era does.
pub const BLOCK_SUBSIDY_FRACTION_DENOMINATOR: u128 = 10_000_000_000;

/// Returns the height at which [ZIP 234] starts to apply on `network`, or `None` if the
/// network does not activate NU7.
///
/// # Consensus
///
/// > `DEPLOYMENT_BLOCK_HEIGHT` [is] the lowest height after the second halving following
/// > the activation of Network Upgrade 7.
///
/// This searches [`halving`] rather than inverting it with [`height_for_halving`],
/// because [`halving`] already folds over every target spacing era, including the one
/// ZIP 218 adds at NU7, and the ZIP 234 start height falls inside that era.
///
/// [ZIP 234]: https://zips.z.cash/zip-0234
pub fn zip234_start_height(network: &Network) -> Option<Height> {
    let nu7 = NetworkUpgrade::Nu7.activation_height(network)?;
    let target_halving = halving(nu7, network).checked_add(2)?;

    // `halving` is non-decreasing in height, so the lowest height reaching
    // `target_halving` is a binary search away.
    let mut low = nu7.0;
    let mut high = Height::MAX_AS_U32;

    if halving(Height(high), network) < target_halving {
        return None;
    }

    while low < high {
        let mid = low + (high - low) / 2;

        if halving(Height(mid), network) < target_halving {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    Some(Height(low))
}

/// Returns whether a ZIP 234 issuance option is compiled in and applies to `network` at
/// `height`.
///
/// [`zip234_start_height`] gives the ZIP's height whatever the build, so that the height
/// arithmetic is testable everywhere. This is the check that decides whether a block
/// subsidy actually follows ZIP 234, and so whether a caller has to fetch the money
/// reserve.
pub fn is_zip234_active(network: &Network, height: Height) -> bool {
    ZIP234_ENABLED && zip234_start_height(network).is_some_and(|start| height >= start)
}

/// Returns the smoothed block subsidy for `money_reserve`.
///
/// # Consensus
///
/// > BlockSubsidy(height) = ceiling(BLOCK_SUBSIDY_FRACTION * MoneyReserveAfter(height-1))
///
/// [ZIP 234]: https://zips.z.cash/zip-0234
fn smoothed_block_subsidy(
    money_reserve: Amount<NonNegative>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    let money_reserve =
        u128::try_from(i64::from(money_reserve)).map_err(|_| SubsidyError::Underflow)?;

    let subsidy = money_reserve
        .checked_mul(BLOCK_SUBSIDY_FRACTION_NUMERATOR)
        .ok_or(SubsidyError::Overflow)?
        .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);

    let subsidy = i64::try_from(subsidy).map_err(|_| SubsidyError::Overflow)?;

    Ok(Amount::try_from(subsidy)?)
}

/// Returns the [ZIP 234] reissuance bonus for a block at `height`, given the money
/// reserve after its parent.
///
/// This is the "preserve halvings" option on the NU7 ballot: the halving schedule keeps
/// issuing new ZEC, and the bonus reissues what has been removed from circulation, at the
/// same fraction that smooths the curve in the other option.
///
/// The deficit is what the halving schedule has issued so far minus what is actually in
/// the chain value pools. The only way the chain falls behind its own schedule is value
/// leaving circulation, so the deficit is exactly what is left to reissue.
///
/// [ZIP 234]: https://zips.z.cash/zip-0234
fn reissuance_bonus(
    height: Height,
    net: &Network,
    money_reserve: Amount<NonNegative>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    let max_money = Amount::<NonNegative>::try_from(MAX_MONEY)?;
    let parent = height.previous().unwrap_or(Height(0));

    let scheduled_supply = cumulative_halving_subsidies(parent, net)?;
    let issued_supply = (max_money - money_reserve)?;

    // A chain can be ahead of its own schedule, because a coinbase before NU6 could claim
    // less than the full subsidy without the difference ever being issued. Saturating at
    // zero leaves nothing to reissue in that case.
    let Ok(deficit) = scheduled_supply - issued_supply else {
        return Ok(Amount::zero());
    };

    smoothed_block_subsidy(deficit)
}

/// Returns the total block subsidy the halving schedule issues for blocks `1..=height`.
///
/// The subsidy is linear in the height through the slow start, and piecewise constant
/// afterwards, changing only where a halving or a target spacing era begins. Summing over
/// those pieces is exact, and takes a bounded number of steps no matter how tall the chain
/// is.
fn cumulative_halving_subsidies(
    height: Height,
    net: &Network,
) -> Result<Amount<NonNegative>, SubsidyError> {
    let slow_start_shift = u128::from(net.slow_start_shift().0);
    let slow_start_interval = u128::from(net.slow_start_interval().0);
    let height = u128::from(height.0);
    let mut total: u128 = 0;

    // The slow start issues `rate * h` below the shift and `rate * (h + 1)` from the shift
    // up to the interval, so each phase is a triangular number rather than a rectangle.
    if slow_start_interval > 0 && slow_start_shift > 0 {
        let rate = u128::from(MAX_BLOCK_SUBSIDY) / slow_start_interval;
        let triangle = |n: u128| n * (n + 1) / 2;

        // `rate * h` for h in 1..=min(height, shift - 1).
        let first_phase_end = height.min(slow_start_shift - 1);
        total += triangle(first_phase_end) * rate;

        // `rate * (h + 1)` for h in shift..=min(height, interval - 1), which is
        // `rate * k` for k in shift + 1..=that end + 1.
        if height >= slow_start_shift {
            let second_phase_end = height.min(slow_start_interval - 1);
            total += (triangle(second_phase_end + 1) - triangle(slow_start_shift)) * rate;
        }
    }

    // After the slow start the subsidy only changes at a halving or a spacing era start,
    // so walk those boundaries and multiply each run of blocks by its subsidy.
    let mut block =
        u32::try_from(slow_start_interval.max(1)).map_err(|_| SubsidyError::Overflow)?;
    let height = u32::try_from(height).map_err(|_| SubsidyError::Overflow)?;

    while block <= height {
        let subsidy = u128::try_from(i64::from(halving_block_subsidy(Height(block), net)?))
            .map_err(|_| SubsidyError::Overflow)?;

        // The next boundary is whichever comes first: the end of this halving era, the
        // start of the next spacing era, or the end of the range.
        let next_boundary = next_subsidy_boundary(Height(block), net)
            .unwrap_or(Height::MAX)
            .0;
        let run_end = next_boundary.saturating_sub(1).min(height);
        let run_blocks = u128::from(run_end - block) + 1;

        total += run_blocks * subsidy;

        if run_end == height || run_end == u32::MAX {
            break;
        }
        block = run_end + 1;
    }

    let total = i64::try_from(total).map_err(|_| SubsidyError::Overflow)?;

    Ok(Amount::try_from(total)?)
}

/// Test-only accessor for [`cumulative_halving_subsidies`].
#[cfg(any(test, feature = "proptest-impl"))]
pub fn cumulative_halving_subsidies_for_tests(
    height: Height,
    net: &Network,
) -> Result<Amount<NonNegative>, SubsidyError> {
    cumulative_halving_subsidies(height, net)
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
    let mut low = height.0 + 1;
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
    money_reserve: Option<Amount<NonNegative>>,
) -> Result<Amount<NonNegative>, SubsidyError> {
    if is_zip234_active(net, height) {
        // The caller reads the money reserve from the parent block, so every caller that
        // can reach a ZIP 234 height must supply it.
        let money_reserve = money_reserve.ok_or(SubsidyError::MissingMoneyReserve)?;

        if ZIP234_SMOOTHING_ENABLED {
            // The smoothed curve replaces halvings outright.
            return smoothed_block_subsidy(money_reserve);
        }

        if ZIP234_HALVINGS_ENABLED {
            // Halvings stay, and the bonus reissues what has been removed from
            // circulation at the same smoothing fraction.
            let halving_subsidy = halving_block_subsidy(height, net)?;
            let bonus = reissuance_bonus(height, net, money_reserve)?;

            return Ok((halving_subsidy + bonus)?);
        }
    }

    halving_block_subsidy(height, net)
}

/// `BlockSubsidy(height)` under the halving schedule, ignoring ZIP 234.
///
/// ZIP 234's "preserve halvings" option issues this plus a reissuance bonus, and its
/// "smooth issuance curve" option replaces it. See [`block_subsidy`].
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
