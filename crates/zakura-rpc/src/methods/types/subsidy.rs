//! Types for the `getblocksubsidy` RPC.

use derive_new::new;
use getset::{CopyGetters, Getters};
use zakura_chain::{
    amount::{Amount, NonNegative},
    parameters::subsidy::FundingStreamReceiver,
    transparent,
};

use super::zec::Zec;

/// A response to a `getblocksubsidy` RPC request
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    Getters,
    CopyGetters,
    new,
)]
pub struct GetBlockSubsidyResponse {
    /// An array of funding stream descriptions.
    /// Always present before NU6, because Zebra returns an error for heights before the first halving.
    #[serde(rename = "fundingstreams", skip_serializing_if = "Vec::is_empty")]
    #[getset(get = "pub")]
    pub(crate) funding_streams: Vec<FundingStream>,

    /// An array of lockbox stream descriptions.
    /// Always present after NU6.
    #[serde(rename = "lockboxstreams", skip_serializing_if = "Vec::is_empty")]
    #[getset(get = "pub")]
    pub(crate) lockbox_streams: Vec<FundingStream>,

    /// The mining reward amount in ZEC.
    ///
    /// This does not include the miner fee.
    #[getset(get_copy = "pub")]
    pub(crate) miner: Zec<NonNegative>,

    /// The founders' reward amount in ZEC.
    ///
    /// Zebra returns an error when asked for founders reward heights,
    /// because it checkpoints those blocks instead.
    #[getset(get_copy = "pub")]
    pub(crate) founders: Zec<NonNegative>,

    /// The total funding stream amount in ZEC.
    #[serde(rename = "fundingstreamstotal")]
    #[getset(get_copy = "pub")]
    pub(crate) funding_streams_total: Zec<NonNegative>,

    /// The total lockbox stream amount in ZEC.
    #[serde(rename = "lockboxtotal")]
    #[getset(get_copy = "pub")]
    pub(crate) lockbox_total: Zec<NonNegative>,

    /// The total block subsidy amount in ZEC.
    ///
    /// This does not include the miner fee.
    #[serde(rename = "totalblocksubsidy")]
    #[getset(get_copy = "pub")]
    pub(crate) total_block_subsidy: Zec<NonNegative>,
}

/// A single funding stream's information in a  `getblocksubsidy` RPC request
#[derive(
    Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Getters, CopyGetters, new,
)]
pub struct FundingStream {
    /// A description of the funding stream recipient.
    #[getset(get = "pub")]
    pub recipient: String,

    /// A URL for the specification of this funding stream.
    #[getset(get = "pub")]
    pub specification: String,

    /// The funding stream amount in ZEC.
    #[getset(get_copy = "pub")]
    pub value: Zec<NonNegative>,

    /// The funding stream amount in zatoshis.
    #[serde(rename = "valueZat")]
    #[getset(get_copy = "pub")]
    pub value_zat: Amount<NonNegative>,

    /// The transparent or Sapling address of the funding stream recipient.
    ///
    /// The current Zcash funding streams only use transparent addresses,
    /// so Zebra doesn't support Sapling addresses in this RPC.
    ///
    /// This is optional so we can support funding streams with no addresses (lockbox streams).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    pub address: Option<transparent::Address>,
}

impl FundingStream {
    /// Convert a `receiver`, `value`, and `address` into a `FundingStream` response.
    pub(crate) fn new_internal(
        is_post_nu6: bool,
        receiver: FundingStreamReceiver,
        value: Amount<NonNegative>,
        address: Option<&transparent::Address>,
    ) -> FundingStream {
        let (name, specification) = receiver.info(is_post_nu6);

        FundingStream {
            recipient: name.to_string(),
            specification: specification.to_string(),
            value: value.into(),
            value_zat: value,
            address: address.cloned(),
        }
    }
}
