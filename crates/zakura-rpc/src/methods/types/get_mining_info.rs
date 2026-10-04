//! Response type for the `getmininginfo` RPC.

use derive_new::new;
use getset::{CopyGetters, Getters};
use zakura_chain::parameters::Network;

/// Response to a `getmininginfo` RPC request.
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    Getters,
    CopyGetters,
    new,
)]
pub struct GetMiningInfoResponse {
    /// The current tip height.
    #[serde(rename = "blocks")]
    #[getset(get_copy = "pub")]
    tip_height: u32,

    /// The size of the last mined block if any.
    #[serde(rename = "currentblocksize", skip_serializing_if = "Option::is_none")]
    #[getset(get_copy = "pub")]
    current_block_size: Option<usize>,

    /// The number of transactions in the last mined block if any.
    #[serde(rename = "currentblocktx", skip_serializing_if = "Option::is_none")]
    #[getset(get_copy = "pub")]
    current_block_tx: Option<usize>,

    /// The estimated network solution rate in Sol/s.
    #[getset(get_copy = "pub")]
    networksolps: u64,

    /// The estimated network solution rate in Sol/s.
    #[getset(get_copy = "pub")]
    networkhashps: u64,

    /// Current network name as defined in BIP70 (main, test, regtest)
    #[getset(get = "pub")]
    chain: String,

    /// If using testnet or not
    #[getset(get_copy = "pub")]
    testnet: bool,
}

impl GetMiningInfoResponse {
    /// Creates a new `getmininginfo` response
    pub(crate) fn new_internal(
        tip_height: u32,
        current_block_size: Option<usize>,
        current_block_tx: Option<usize>,
        network: Network,
        networksolps: u64,
    ) -> Self {
        Self {
            tip_height,
            current_block_size,
            current_block_tx,
            networksolps,
            networkhashps: networksolps,
            chain: network.bip70_network_name(),
            testnet: network.is_a_test_network(),
        }
    }
}
