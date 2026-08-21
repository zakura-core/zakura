//! Parameter, response, and lifecycle types for mined-block RPCs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, oneshot};

use zakura_chain::{block, serialization::ZcashSerialize};
use zakura_network as zn;

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
const MAX_PENDING_BLOCK_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_CLAIMANTS_PER_BLOCK: usize = 16;
const MAX_RELAY_ONCE_RECORDS: usize = 1_024;
const PENDING_BLOCK_TTL: Duration = Duration::from_secs(10 * 60);

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

/// The source and timing context for an early block relay.
#[derive(Clone, Debug)]
pub enum BlockRelaySource {
    /// A locally submitted mined block.
    Mined {
        /// When the RPC accepted the submitted bytes.
        submitted_at: std::time::Instant,
        /// The RPC method that accepted the submission.
        submission: MinedBlockSubmission,
    },
    /// A block received from a peer.
    Peer {
        /// When semantic verification authorized relay.
        authorized_at: std::time::Instant,
        /// The peer that supplied the block, when known.
        advertiser: Option<zn::PeerSource>,
    },
}

/// A block lifecycle event consumed by the block gossip task.
#[derive(Debug)]
pub enum BlockRelayEvent {
    /// Consensus authorized relay, so peers can receive inventory before state commit.
    Early {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
        /// The relay source and timing context.
        source: BlockRelaySource,
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
        /// The relay source and timing context.
        source: BlockRelaySource,
    },
    /// Verification or commit failed after relay authorization.
    Failed {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
        /// Whether peers received an early inventory.
        early_advertised: bool,
        /// The relay source and timing context.
        source: BlockRelaySource,
    },
}

/// Holds relay-authorized block bodies until their contextual commits finish.
#[derive(Clone, Debug, Default)]
pub struct PendingBlockRegistry(Arc<Mutex<PendingBlockRegistryInner>>);

#[derive(Debug, Default)]
struct PendingBlockRegistryInner {
    entries: HashMap<block::Hash, PendingBlock>,
    relayed: HashMap<block::Hash, Instant>,
    total_bytes: usize,
}

#[derive(Debug)]
struct PendingBlock {
    block: Arc<block::Block>,
    inserted_at: Instant,
    serialized_size: usize,
    active_claims: HashMap<usize, usize>,
}

impl PendingBlockRegistryInner {
    fn prune_expired(&mut self, now: Instant) {
        let mut expired_count = 0;
        let mut expired_bytes = 0;
        self.entries.retain(|_, pending| {
            let retain = now.saturating_duration_since(pending.inserted_at) < PENDING_BLOCK_TTL;
            if !retain {
                expired_count += 1;
                expired_bytes += pending.serialized_size;
            }
            retain
        });
        self.total_bytes = self.total_bytes.saturating_sub(expired_bytes);
        if expired_count > 0 {
            metrics::counter!("block_relay.pending_registry.expired").increment(expired_count);
        }

        self.relayed
            .retain(|_, relayed_at| now.saturating_duration_since(*relayed_at) < PENDING_BLOCK_TTL);
    }
}

impl PendingBlockRegistry {
    /// Inserts a block before its early inventory is sent.
    ///
    /// Returns true when this caller reserved the hash and should originate its advertisement.
    /// An active duplicate acquires an independent claim and returns true only when the previous
    /// relay reservation expired or was canceled. A recent settled hash or a full bound returns
    /// false without a claim.
    pub fn insert(&self, block: Arc<block::Block>) -> bool {
        let hash = block.hash();
        let claimant = Arc::as_ptr(&block) as usize;
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        entries.prune_expired(now);
        if let Some(pending) = entries.entries.get_mut(&hash) {
            if let Some(claims) = pending.active_claims.get_mut(&claimant) {
                *claims = claims.saturating_add(1);
            } else if pending.active_claims.len() < MAX_PENDING_CLAIMANTS_PER_BLOCK {
                pending.active_claims.insert(claimant, 1);
            } else {
                metrics::counter!("block_relay.pending_registry.claimants_saturated").increment(1);
                return false;
            }
            pending.inserted_at = now;
            if entries.relayed.insert(hash, now).is_none() {
                return true;
            }
            return false;
        }
        if entries.relayed.contains_key(&hash) {
            metrics::counter!("block_relay.relay_once.suppressed").increment(1);
            return false;
        }
        let serialized_size = block.zcash_serialized_size();
        if entries.entries.len() >= MAX_PENDING_BLOCKS
            || entries.total_bytes.saturating_add(serialized_size) > MAX_PENDING_BLOCK_BYTES
            || entries.relayed.len() >= MAX_RELAY_ONCE_RECORDS
        {
            metrics::counter!("block_relay.pending_registry.saturated").increment(1);
            return false;
        }

        entries.total_bytes += serialized_size;
        entries.relayed.insert(hash, now);
        entries.entries.insert(
            hash,
            PendingBlock {
                block,
                inserted_at: now,
                serialized_size,
                active_claims: HashMap::from([(claimant, 1)]),
            },
        );
        true
    }

    /// Releases a relay reservation when the gossip queue rejects its event.
    pub fn cancel_relay_reservation(&self, block: &Arc<block::Block>) {
        let hash = block.hash();
        let claimant = Arc::as_ptr(block) as usize;
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries
            .entries
            .get(&hash)
            .is_some_and(|pending| pending.active_claims.contains_key(&claimant))
        {
            entries.relayed.remove(&hash);
        }
    }

    /// Removes a block after its contextual commit settles.
    ///
    /// The registry keeps the body while another relay-authorized submission still owns a claim.
    pub fn remove(&self, block: &Arc<block::Block>, committed: bool) {
        let hash = block.hash();
        let claimant = Arc::as_ptr(block) as usize;
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_remove = entries.entries.get_mut(&hash).is_some_and(|pending| {
            if committed {
                return true;
            }

            let Some(claims) = pending.active_claims.get_mut(&claimant) else {
                return false;
            };
            *claims -= 1;
            if *claims == 0 {
                pending.active_claims.remove(&claimant);
            }
            pending.active_claims.is_empty()
        });
        let removed = should_remove
            .then(|| entries.entries.remove(&hash))
            .flatten();

        if let Some(removed) = removed {
            entries.total_bytes = entries.total_bytes.saturating_sub(removed.serialized_size);
            if !committed {
                metrics::counter!("block_relay.pending_registry.uncommitted").increment(1);
            }
        }
    }

    /// Returns an admitted block body before contextual commit finishes.
    pub fn get(&self, hash: block::Hash) -> Option<Arc<block::Block>> {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.prune_expired(Instant::now());
        let block = entries
            .entries
            .get(&hash)
            .map(|pending| pending.block.clone());

        if block.is_some() {
            metrics::counter!("block_relay.pending_registry.served").increment(1);
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
    /// Block reached the configured acknowledgement milestone and returns null.
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
    /// The compact submission reached the configured acknowledgement milestone and returns `null`.
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

/// A channel that sends relay lifecycle events to the block gossip task.
pub struct BlockRelayChannel {
    /// The channel sender
    sender: mpsc::Sender<BlockRelayEvent>,
    /// The channel receiver
    receiver: mpsc::Receiver<BlockRelayEvent>,
}

impl BlockRelayChannel {
    /// Creates a new block relay channel.
    pub fn new() -> Self {
        /// How many unread messages the block relay channel should buffer before rejecting sends.
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
    pub fn sender(&self) -> mpsc::Sender<BlockRelayEvent> {
        self.sender.clone()
    }

    /// Get the channel receiver
    pub fn receiver(self) -> mpsc::Receiver<BlockRelayEvent> {
        self.receiver
    }
}

impl Default for BlockRelayChannel {
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
    fn replacement_claim_keeps_the_original_body_registered() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let duplicate = Arc::new((*block).clone());
        let hash = block.hash();
        assert!(registry.insert(block.clone()));
        assert!(!registry.insert(duplicate.clone()));

        registry.remove(&block, false);

        assert_eq!(registry.get(hash), Some(block));
        registry.remove(&duplicate, false);
        assert_eq!(registry.get(hash), None);
    }

    #[test]
    fn repeated_arc_claims_settle_independently() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));
        assert!(!registry.insert(block.clone()));

        registry.remove(&block, false);
        assert_eq!(registry.get(hash), Some(block.clone()));

        registry.remove(&block, false);
        assert_eq!(registry.get(hash), None);
    }

    #[test]
    fn settled_hash_is_not_relayed_again_before_expiry() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let duplicate = Arc::new((*block).clone());
        let hash = block.hash();
        assert!(registry.insert(block.clone()));
        registry.remove(&block, false);

        assert!(!registry.insert(duplicate));
        assert_eq!(registry.get(hash), None);
    }

    #[test]
    fn failed_enqueue_releases_the_relay_reservation() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let duplicate = Arc::new((*block).clone());
        assert!(registry.insert(block.clone()));
        registry.cancel_relay_reservation(&block);

        assert!(registry.insert(duplicate));
    }

    #[test]
    fn pending_claimants_are_bounded() {
        let registry = PendingBlockRegistry::default();
        let original = test_block();
        let hash = original.hash();
        assert!(registry.insert(original.clone()));
        let duplicates: Vec<_> = (0..MAX_PENDING_CLAIMANTS_PER_BLOCK)
            .map(|_| Arc::new((*original).clone()))
            .collect();
        for duplicate in &duplicates {
            assert!(!registry.insert(duplicate.clone()));
        }

        let entries = registry
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            entries
                .entries
                .get(&hash)
                .expect("the original body remains registered")
                .active_claims
                .len(),
            MAX_PENDING_CLAIMANTS_PER_BLOCK
        );
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

    #[test]
    fn pending_registry_expires_stalled_entries() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block));

        {
            let mut entries = registry
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entries.prune_expired(Instant::now() + PENDING_BLOCK_TTL);
        }

        assert_eq!(registry.get(hash), None);
    }
}
