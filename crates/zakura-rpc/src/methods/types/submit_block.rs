//! Parameter and response types for the `submitblock` RPC.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};

use zakura_chain::block;

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

/// The maximum time a peer waits for an early-advertised block to commit.
pub const PENDING_BLOCK_WAIT: Duration = Duration::from_secs(15);

const MAX_PENDING_BLOCKS: usize = 16;
const MAX_PENDING_BLOCK_WAITS: usize = 32;

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

#[derive(Clone, Debug)]
enum PendingStatus {
    Waiting,
    Committed(Arc<block::Block>),
    Failed,
}

#[derive(Debug)]
struct PendingBlock {
    status: watch::Sender<PendingStatus>,
}

/// Stores pending blocks and bounds peer waits.
#[derive(Debug)]
struct PendingBlockRegistryInner {
    entries: Mutex<HashMap<block::Hash, PendingBlock>>,
    wait_permits: Arc<Semaphore>,
}

/// Holds early-advertised block bodies until their contextual commits finish.
#[derive(Clone, Debug)]
pub struct PendingBlockRegistry(Arc<PendingBlockRegistryInner>);

impl Default for PendingBlockRegistry {
    fn default() -> Self {
        Self(Arc::new(PendingBlockRegistryInner {
            entries: Mutex::new(HashMap::new()),
            wait_permits: Arc::new(Semaphore::new(MAX_PENDING_BLOCK_WAITS)),
        }))
    }
}

impl PendingBlockRegistry {
    /// Inserts a block before its early inventory is sent.
    ///
    /// Returns false when the bounded registry is full.
    pub fn insert(&self, block: Arc<block::Block>) -> bool {
        let hash = block.hash();
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(&hash) {
            return false;
        }
        if entries.len() >= MAX_PENDING_BLOCKS {
            metrics::counter!("mining.pending_registry.saturated").increment(1);
            return false;
        }

        let (status, _receiver) = watch::channel(PendingStatus::Waiting);
        entries.insert(hash, PendingBlock { status });
        true
    }

    /// Resolves peer waiters and removes a terminal entry.
    pub fn resolve(&self, hash: block::Hash, result: Result<Arc<block::Block>, ()>) {
        let entry = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&hash);
        let Some(entry) = entry else {
            return;
        };
        let status = match result {
            Ok(block) => PendingStatus::Committed(block),
            Err(()) => PendingStatus::Failed,
        };
        entry.status.send_replace(status);
    }

    /// Waits for an early-advertised block to commit.
    pub fn wait(
        &self,
        hash: block::Hash,
    ) -> impl std::future::Future<Output = Option<Arc<block::Block>>> + Send + 'static {
        let status = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .map(|entry| entry.status.subscribe());
        let wait_permit = status.as_ref().and_then(|_| {
            self.0
                .wait_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    metrics::counter!("mining.pending_peer_wait.saturated").increment(1);
                })
                .ok()
        });
        let deadline = tokio::time::Instant::now() + PENDING_BLOCK_WAIT;

        async move {
            let mut status = status?;
            let _wait_permit: OwnedSemaphorePermit = wait_permit?;
            let start = std::time::Instant::now();
            let result = tokio::time::timeout_at(deadline, async {
                loop {
                    let current = status.borrow().clone();
                    match current {
                        PendingStatus::Waiting => {
                            if status.changed().await.is_err() {
                                return None;
                            }
                        }
                        PendingStatus::Committed(block) => return Some(block),
                        PendingStatus::Failed => return None,
                    }
                }
            })
            .await
            .ok()
            .flatten();
            metrics::histogram!("mining.pending_peer_wait.duration_seconds")
                .record(start.elapsed().as_secs_f64());
            result
        }
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

    #[tokio::test]
    async fn pending_block_waits_for_success() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        let wait = tokio::spawn({
            let registry = registry.clone();
            async move { registry.wait(hash).await }
        });
        tokio::task::yield_now().await;
        registry.resolve(hash, Ok(block.clone()));

        assert_eq!(wait.await.expect("wait task succeeds"), Some(block));
    }

    #[tokio::test]
    async fn pending_block_wait_subscribes_before_polling() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        let wait = registry.wait(hash);
        registry.resolve(hash, Ok(block.clone()));

        assert_eq!(wait.await, Some(block));
    }

    #[tokio::test]
    async fn pending_block_failure_returns_not_found() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block));

        let wait = tokio::spawn({
            let registry = registry.clone();
            async move { registry.wait(hash).await }
        });
        tokio::task::yield_now().await;
        registry.resolve(hash, Err(()));

        assert_eq!(wait.await.expect("wait task succeeds"), None);
    }

    #[tokio::test]
    async fn pending_block_waits_are_bounded() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        assert!(registry.insert(block.clone()));

        let waits: Vec<_> = (0..MAX_PENDING_BLOCK_WAITS)
            .map(|_| registry.wait(hash))
            .collect();
        assert_eq!(registry.wait(hash).await, None);

        drop(waits);
        let wait = registry.wait(hash);
        registry.resolve(hash, Ok(block.clone()));
        assert_eq!(wait.await, Some(block));
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
