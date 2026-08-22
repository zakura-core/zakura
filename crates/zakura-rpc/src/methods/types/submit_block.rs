//! Parameter, response, and lifecycle types for mined-block RPCs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};

use zakura_chain::block;

use crate::methods::hex_data::HexData;

// Allow doc links to these imports.
#[allow(unused_imports)]
use crate::methods::GetBlockTemplateHandler;

/// Optional argument `jsonparametersobject` for `submitblock` RPC request
///
/// See the notes for the [`submit_block`](crate::methods::RpcServer::submit_block) RPC.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, schemars::JsonSchema)]
pub struct SubmitBlockParameters {
    /// The workid for the block template.
    ///
    /// > If the server provided a workid, it MUST be included with submissions,
    ///
    /// Rationale:
    ///
    /// > If servers allow all mutations, it may be hard to identify which job it is based on.
    /// > While it may be possible to verify the submission by its content, it is much easier
    /// > to compare it to the job issued. It is very easy for the miner to keep track of this.
    /// > Therefore, using a "workid" is a very cheap solution to enable more mutations.
    ///
    /// <https://en.bitcoin.it/wiki/BIP_0022#Rationale>
    #[serde(rename = "workid")]
    pub work_id: Option<String>,
}

/// A compact mined-block submission.
#[derive(
    Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub struct SubmitSolutionParameters {
    /// The prepared candidate identifier returned by `getblocktemplate`.
    #[serde(rename = "workid")]
    pub work_id: String,

    /// The complete hex-encoded solved block header.
    pub header: HexData,
}

const MAX_PENDING_BLOCKS: usize = 16;

/// The RPC path that admitted a mined block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinedBlockSubmission {
    /// The miner submitted a complete block.
    FullBlock,
    /// The miner submitted a compact solved header.
    CompactHeader,
}

impl MinedBlockSubmission {
    /// Returns the RPC method name for metrics.
    pub const fn rpc_method(self) -> &'static str {
        match self {
            Self::FullBlock => "submitblock",
            Self::CompactHeader => "submitsolution",
        }
    }
}

/// A mined-block lifecycle event consumed by the block gossip task.
#[derive(Debug)]
pub enum MinedBlockEvent {
    /// State admitted the block, so peers can receive its inventory before commit completes.
    Early {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
        /// When the RPC accepted the submitted bytes.
        submitted_at: std::time::Instant,
        /// The RPC method that accepted the submission.
        submission: MinedBlockSubmission,
        /// Reports whether the early network advertisement completed.
        advertised: oneshot::Sender<bool>,
    },
    /// The contextual commit completed.
    Committed {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
        /// Whether the early advertisement completed successfully.
        early_advertised: bool,
    },
    /// The contextual commit failed after state admission.
    Failed {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
        /// Whether peers received an early inventory.
        early_advertised: bool,
    },
}

/// Holds admitted block bodies until their contextual commits finish.
#[derive(Clone, Debug, Default)]
pub struct PendingBlockRegistry(Arc<Mutex<HashMap<block::Hash, Arc<block::Block>>>>);

impl PendingBlockRegistry {
    /// Inserts a block before its early inventory is sent.
    ///
    /// Returns false when the bounded registry is full.
    pub fn insert(&self, block: Arc<block::Block>) -> bool {
        let hash = block.hash();
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(&hash) {
            return false;
        }
        if entries.len() >= MAX_PENDING_BLOCKS {
            metrics::counter!("mining.pending_registry.saturated").increment(1);
            return false;
        }

        entries.insert(hash, block);
        true
    }

    /// Removes a block after its contextual commit settles.
    ///
    /// A distinct submission with the same hash cannot remove the registered block.
    pub fn remove(&self, block: &Arc<block::Block>, committed: bool) {
        let hash = block.hash();
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owns_entry = entries
            .get(&hash)
            .is_some_and(|registered| Arc::ptr_eq(registered, block));
        let removed = owns_entry && entries.remove(&hash).is_some();

        if removed && !committed {
            metrics::counter!("mining.pending_registry.uncommitted").increment(1);
        }
    }

    /// Returns an admitted block body before contextual commit finishes.
    pub fn get(&self, hash: block::Hash) -> Option<Arc<block::Block>> {
        let block = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .cloned();

        if block.is_some() {
            metrics::counter!("mining.pending_registry.served").increment(1);
        }
        block
    }
}

/// Response to a `submitblock` RPC request.
///
/// Zebra never returns "duplicate-invalid", because it does not store invalid blocks.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubmitBlockErrorResponse {
    /// Block was already committed to the non-finalized or finalized state
    Duplicate,
    /// Block was already added to the state queue or channel, but not yet committed to the non-finalized state
    DuplicateInconclusive,
    /// Block was already committed to the non-finalized state, but not on the best chain
    Inconclusive,
    /// Block rejected as invalid
    Rejected,
}

/// Response to a `submitblock` RPC request.
///
/// Zebra never returns "duplicate-invalid", because it does not store invalid blocks.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SubmitBlockResponse {
    /// Block was not successfully submitted, return error
    ErrorResponse(SubmitBlockErrorResponse),
    /// Block successfully submitted, returns null
    Accepted,
}

impl Default for SubmitBlockResponse {
    fn default() -> Self {
        Self::ErrorResponse(SubmitBlockErrorResponse::Rejected)
    }
}

impl From<SubmitBlockErrorResponse> for SubmitBlockResponse {
    fn from(error_response: SubmitBlockErrorResponse) -> Self {
        Self::ErrorResponse(error_response)
    }
}

/// A compact submission rejection reason.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubmitSolutionErrorResponse {
    /// The block was already committed.
    Duplicate,
    /// The block was already queued but has not committed.
    DuplicateInconclusive,
    /// The block committed to a side chain.
    Inconclusive,
    /// Consensus rejected the block.
    Rejected,
    /// The prepared candidate no longer exists.
    StaleWork,
    /// The solved header changed a preserved candidate field.
    CandidateMismatch,
}

/// A `submitsolution` response.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SubmitSolutionResponse {
    /// The compact submission failed.
    ErrorResponse(SubmitSolutionErrorResponse),
    /// The compact submission committed successfully and returns `null`.
    Accepted,
}

impl From<SubmitSolutionErrorResponse> for SubmitSolutionResponse {
    fn from(error: SubmitSolutionErrorResponse) -> Self {
        Self::ErrorResponse(error)
    }
}

impl From<SubmitBlockResponse> for SubmitSolutionResponse {
    fn from(response: SubmitBlockResponse) -> Self {
        match response {
            SubmitBlockResponse::Accepted => Self::Accepted,
            SubmitBlockResponse::ErrorResponse(error) => Self::ErrorResponse(match error {
                SubmitBlockErrorResponse::Duplicate => SubmitSolutionErrorResponse::Duplicate,
                SubmitBlockErrorResponse::DuplicateInconclusive => {
                    SubmitSolutionErrorResponse::DuplicateInconclusive
                }
                SubmitBlockErrorResponse::Inconclusive => SubmitSolutionErrorResponse::Inconclusive,
                SubmitBlockErrorResponse::Rejected => SubmitSolutionErrorResponse::Rejected,
            }),
        }
    }
}

/// A submit block channel, used to inform the gossip task about mined blocks.
pub struct SubmitBlockChannel {
    /// The channel sender
    sender: mpsc::Sender<MinedBlockEvent>,
    /// The channel receiver
    receiver: mpsc::Receiver<MinedBlockEvent>,
}

impl SubmitBlockChannel {
    /// Creates a new submit block channel
    pub fn new() -> Self {
        /// How many unread messages the submit block channel should buffer before rejecting sends.
        ///
        /// This should be large enough to usually avoid rejecting sends. This channel is used by
        /// the block hash gossip task, which waits for a ready peer in the peer set while
        /// processing messages from this channel and could be much slower to gossip block hashes
        /// than it is to commit blocks and produce new block templates.
        const SUBMIT_BLOCK_CHANNEL_CAPACITY: usize = 10_000;

        let (sender, receiver) = mpsc::channel(SUBMIT_BLOCK_CHANNEL_CAPACITY);
        Self { sender, receiver }
    }

    /// Get the channel sender
    pub fn sender(&self) -> mpsc::Sender<MinedBlockEvent> {
        self.sender.clone()
    }

    /// Get the channel receiver
    pub fn receiver(self) -> mpsc::Receiver<MinedBlockEvent> {
        self.receiver
    }
}

impl Default for SubmitBlockChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    fn test_block() -> Arc<Block> {
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis test vector is valid")
    }

    #[test]
    fn pending_block_is_served_before_commit() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        assert_eq!(registry.get(hash), Some(block));
    }

    #[test]
    fn committed_block_leaves_the_registry() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        registry.remove(&block, true);

        assert_eq!(registry.get(hash), None);
    }

    #[test]
    fn uncommitted_block_leaves_the_registry() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        registry.remove(&block, false);

        assert_eq!(registry.get(hash), None);
    }

    #[test]
    fn duplicate_submission_cannot_remove_registered_block() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let duplicate = Arc::new((*block).clone());
        let hash = block.hash();
        assert!(registry.insert(block.clone()));
        assert!(!registry.insert(duplicate.clone()));

        registry.remove(&duplicate, false);

        assert_eq!(registry.get(hash), Some(block));
    }

    #[test]
    fn unknown_hash_is_not_served() {
        let registry = PendingBlockRegistry::default();
        assert_eq!(registry.get(test_block().hash()), None);
    }

    #[test]
    fn submit_solution_responses_match_mining_rpc_conventions() {
        assert_eq!(
            serde_json::to_value(SubmitSolutionResponse::Accepted)
                .expect("accepted response serialization succeeds"),
            serde_json::Value::Null
        );
        assert_eq!(
            serde_json::to_value(SubmitSolutionResponse::from(
                SubmitSolutionErrorResponse::StaleWork
            ))
            .expect("rejection response serialization succeeds"),
            serde_json::Value::String("stale-work".to_owned())
        );
    }

    #[test]
    fn pending_registry_is_bounded() {
        let registry = PendingBlockRegistry::default();
        let original = test_block();
        for nonce in 0..MAX_PENDING_BLOCKS {
            let mut block = (*original).clone();
            let nonce = u8::try_from(nonce).expect("the registry bound fits in u8");
            Arc::make_mut(&mut block.header).nonce = [nonce; 32].into();
            assert!(registry.insert(Arc::new(block)));
        }

        let mut overflow = (*original).clone();
        Arc::make_mut(&mut overflow.header).nonce = [u8::MAX; 32].into();
        assert!(!registry.insert(Arc::new(overflow)));
    }
}
