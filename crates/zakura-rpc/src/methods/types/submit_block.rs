//! Parameter and response types for the `submitblock` RPC.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};

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
        /// Cancels the advertisement if contextual verification rejects the block.
        pending: PendingBlockSignal,
    },
    /// The contextual commit completed.
    Committed {
        /// The block hash.
        hash: block::Hash,
        /// The block height.
        height: block::Height,
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
    owner_id: u64,
    status: watch::Sender<PendingStatus>,
}

/// Stores pending blocks and coalesces peer waits by block hash.
#[derive(Debug)]
struct PendingBlockRegistryInner {
    entries: Mutex<HashMap<block::Hash, PendingBlock>>,
    next_owner_id: AtomicU64,
}

/// Holds early-advertised block bodies until their contextual commits finish.
#[derive(Clone, Debug)]
pub struct PendingBlockRegistry(Arc<PendingBlockRegistryInner>);

impl Default for PendingBlockRegistry {
    fn default() -> Self {
        Self(Arc::new(PendingBlockRegistryInner {
            entries: Mutex::new(HashMap::new()),
            next_owner_id: AtomicU64::new(1),
        }))
    }
}

/// Reports whether an early-advertised block remains valid.
#[derive(Debug)]
pub struct PendingBlockSignal(watch::Receiver<PendingStatus>);

impl PendingBlockSignal {
    /// Returns true unless contextual verification has rejected the block.
    pub fn is_valid(&self) -> bool {
        !matches!(*self.0.borrow(), PendingStatus::Failed)
    }

    /// Resolves when contextual verification rejects the block.
    pub async fn wait_for_failure(&mut self) {
        loop {
            let status = self.0.borrow_and_update().clone();
            match status {
                PendingStatus::Failed => return,
                PendingStatus::Committed(_) => std::future::pending::<()>().await,
                PendingStatus::Waiting => {}
            }

            if self.0.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Owns one pending-block registry entry.
#[derive(Debug)]
pub(crate) struct PendingBlockRegistration {
    registry: PendingBlockRegistry,
    hash: block::Hash,
    owner_id: u64,
    resolved: bool,
}

impl PendingBlockRegistration {
    /// Returns a signal that cancels stale early inventory after commit failure.
    pub(crate) fn signal(&self) -> PendingBlockSignal {
        let entries = self
            .registry
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let status = entries
            .get(&self.hash)
            .filter(|entry| entry.owner_id == self.owner_id)
            .expect("registration owns its entry until it resolves")
            .status
            .subscribe();
        PendingBlockSignal(status)
    }

    /// Resolves this registration and wakes its peer waiters.
    pub(crate) fn resolve(mut self, result: Result<Arc<block::Block>, ()>) {
        self.registry.resolve(self.hash, self.owner_id, result);
        self.resolved = true;
    }
}

impl Drop for PendingBlockRegistration {
    fn drop(&mut self) {
        if !self.resolved {
            self.registry.resolve(self.hash, self.owner_id, Err(()));
        }
    }
}

impl PendingBlockRegistry {
    /// Inserts a block before its early inventory is sent.
    ///
    /// Returns no registration when the hash already has an owner or the registry is full.
    pub(crate) fn insert(&self, block: Arc<block::Block>) -> Option<PendingBlockRegistration> {
        let hash = block.hash();
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(&hash) {
            return None;
        }
        if entries.len() >= MAX_PENDING_BLOCKS {
            metrics::counter!("mining.pending_registry.saturated").increment(1);
            return None;
        }

        let owner_id = self.0.next_owner_id.fetch_add(1, Ordering::Relaxed);
        let (status, _receiver) = watch::channel(PendingStatus::Waiting);
        entries.insert(hash, PendingBlock { owner_id, status });
        Some(PendingBlockRegistration {
            registry: self.clone(),
            hash,
            owner_id,
            resolved: false,
        })
    }

    fn resolve(&self, hash: block::Hash, owner_id: u64, result: Result<Arc<block::Block>, ()>) {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries
            .get(&hash)
            .is_none_or(|entry| entry.owner_id != owner_id)
        {
            return;
        }
        let entry = entries
            .remove(&hash)
            .expect("entry exists because its owner matched under the same lock");
        drop(entries);

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
        let deadline = tokio::time::Instant::now() + PENDING_BLOCK_WAIT;

        async move {
            let mut status = status?;
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
    sender: mpsc::UnboundedSender<MinedBlockEvent>,
    /// The channel receiver
    receiver: mpsc::UnboundedReceiver<MinedBlockEvent>,
}

impl SubmitBlockChannel {
    /// Creates a new submit block channel
    pub fn new() -> Self {
        // Only admitted early events and successful commit events enter this channel. Invalid and
        // duplicate submissions cannot fill it, and the gossip task does not wait for peer
        // readiness while consuming it.
        let (sender, receiver) = mpsc::unbounded_channel();
        Self { sender, receiver }
    }

    /// Get the channel sender
    pub fn sender(&self) -> mpsc::UnboundedSender<MinedBlockEvent> {
        self.sender.clone()
    }

    /// Get the channel receiver
    pub fn receiver(self) -> mpsc::UnboundedReceiver<MinedBlockEvent> {
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
        let registration = registry
            .insert(block.clone())
            .expect("the registry accepts the block");
        assert!(registry.insert(block.clone()).is_none());

        let wait = tokio::spawn({
            let registry = registry.clone();
            async move { registry.wait(hash).await }
        });
        tokio::task::yield_now().await;
        registration.resolve(Ok(block.clone()));

        assert_eq!(wait.await.expect("wait task succeeds"), Some(block));
    }

    #[tokio::test]
    async fn pending_block_wait_subscribes_before_polling() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let registration = registry
            .insert(block.clone())
            .expect("the registry accepts the block");

        let wait = registry.wait(hash);
        registration.resolve(Ok(block.clone()));

        assert_eq!(wait.await, Some(block));
    }

    #[tokio::test]
    async fn pending_block_failure_returns_not_found() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let registration = registry
            .insert(block)
            .expect("the registry accepts the block");

        let wait = tokio::spawn({
            let registry = registry.clone();
            async move { registry.wait(hash).await }
        });
        tokio::task::yield_now().await;
        registration.resolve(Err(()));

        assert_eq!(wait.await.expect("wait task succeeds"), None);
    }

    #[tokio::test]
    async fn pending_block_failure_cancels_stale_inventory() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let registration = registry
            .insert(block)
            .expect("the registry accepts the block");
        let mut signal = registration.signal();

        assert!(signal.is_valid());
        registration.resolve(Err(()));
        signal.wait_for_failure().await;
        assert!(!signal.is_valid());
    }

    #[tokio::test]
    async fn pending_block_waits_for_one_hash_are_coalesced() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let registration = registry
            .insert(block.clone())
            .expect("the registry accepts the block");

        let waits: Vec<_> = (0..64).map(|_| registry.wait(hash)).collect();
        registration.resolve(Ok(block.clone()));
        for result in futures::future::join_all(waits).await {
            assert_eq!(result, Some(block.clone()));
        }
    }

    #[test]
    fn pending_registry_is_bounded() {
        let registry = PendingBlockRegistry::default();
        let original = test_block();
        let mut registrations = Vec::new();
        for nonce in 0..MAX_PENDING_BLOCKS {
            let mut block = (*original).clone();
            let nonce = u8::try_from(nonce).expect("the registry bound fits in u8");
            Arc::make_mut(&mut block.header).nonce = [nonce; 32].into();
            registrations.push(
                registry
                    .insert(Arc::new(block))
                    .expect("the registry has capacity"),
            );
        }

        let mut overflow = (*original).clone();
        Arc::make_mut(&mut overflow.header).nonce = [u8::MAX; 32].into();
        assert!(registry.insert(Arc::new(overflow)).is_none());
        drop(registrations);
    }
}
