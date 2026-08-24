//! An array of [`ChainTip`] is the output of the `getchaintips` RPC method.

use zakura_chain::block::{self, Height};
use zakura_state::{ChainTipInfo, ChainTipStatus};

/// The status of a chain tip, as reported by `getchaintips`.
///
/// These are zcashd's status values. Zakura never returns `valid-headers` or
/// `unknown`: every block in its non-finalized state is contextually verified, so a
/// tip is either fully valid, invalidated, or known only by its header.
#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainTipStatusResponse {
    /// The tip of the current best chain.
    Active,

    /// A fully validated tip that is not part of the best chain.
    ValidFork,

    /// The node selected this header tip, but not all block bodies for its branch
    /// are available.
    HeadersOnly,

    /// This branch contains at least one invalid block.
    Invalid,
}

impl From<ChainTipStatus> for ChainTipStatusResponse {
    fn from(status: ChainTipStatus) -> Self {
        match status {
            ChainTipStatus::Active => ChainTipStatusResponse::Active,
            ChainTipStatus::ValidFork => ChainTipStatusResponse::ValidFork,
            ChainTipStatus::HeadersOnly => ChainTipStatusResponse::HeadersOnly,
            ChainTipStatus::Invalid => ChainTipStatusResponse::Invalid,
        }
    }
}

/// Item of the `getchaintips` response.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChainTip {
    /// The height of the chain tip.
    pub height: Height,

    /// The block hash of the chain tip.
    #[serde(with = "hex")]
    pub hash: block::Hash,

    /// The length of the branch connecting this tip to the best chain.
    ///
    /// Zero for the best chain's own tip.
    pub branchlen: u32,

    /// The status of the chain ending at this tip.
    pub status: ChainTipStatusResponse,
}

impl From<ChainTipInfo> for ChainTip {
    fn from(tip: ChainTipInfo) -> Self {
        ChainTip {
            height: tip.height,
            hash: tip.hash,
            branchlen: tip.branch_len,
            status: tip.status.into(),
        }
    }
}

/// Response type for the `getchaintips` RPC method.
pub type GetChainTipsResponse = Vec<ChainTip>;
