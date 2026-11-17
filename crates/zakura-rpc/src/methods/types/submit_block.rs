//! Parameter and response types for the `submitblock` RPC.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use tokio::sync::mpsc;
use zakura_node_services::block_relay::{
    PendingBlockSendPermit, PendingBlockStatus as PendingStatus, ServedBlock,
};
use zakura_rt::sync::watch;

use zakura_chain::block;

use crate::methods::hex_data::HexData;

// Allow doc links to these imports.
#[allow(unused_imports)]
use crate::methods::GetBlockTemplateHandler;

/// Optional argument `jsonparametersobject` for `submitblock` RPC request
///
/// See the notes for the [`submit_block`](crate::methods::RpcServer::submit_block) RPC.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
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
    ///
    /// Zakura accepts this field but does not read it. The verifier identifies a prepared
    /// candidate by its content, so a submission reuses the prepared work whether or not the
    /// miner echoes the work ID back.
    #[serde(rename = "workid")]
    pub work_id: Option<String>,
}

/// A compact mined-block submission.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SubmitSolutionParameters {
    /// The prepared candidate identifier returned by `getblocktemplate`.
    #[serde(rename = "workid")]
    pub work_id: String,

    /// The complete hex-encoded solved block header.
    pub header: HexData,
}

const MAX_PENDING_BLOCKS: usize = 16;
const MAX_MINED_SUBMISSIONS: usize = 16;

/// Bounds detached mined submissions before they reach verification or the writer.
#[derive(Clone, Debug, Default)]
pub(crate) struct MinedSubmissions(Arc<Mutex<HashSet<block::Hash>>>);

/// Holds submission capacity until verification and contextual commit finish.
pub(crate) struct MinedSubmission {
    submissions: MinedSubmissions,
    hash: block::Hash,
}

impl MinedSubmissions {
    pub(crate) fn reserve(
        &self,
        hash: block::Hash,
    ) -> Result<MinedSubmission, SubmitBlockErrorResponse> {
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains(&hash) {
            return Err(SubmitBlockErrorResponse::DuplicateInconclusive);
        }
        if entries.len() >= MAX_MINED_SUBMISSIONS {
            return Err(SubmitBlockErrorResponse::Inconclusive);
        }
        entries.insert(hash);
        Ok(MinedSubmission {
            submissions: self.clone(),
            hash,
        })
    }
}

impl Drop for MinedSubmission {
    fn drop(&mut self) {
        self.submissions
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.hash);
    }
}

/// The RPC path that admitted a mined block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinedBlockSubmission {
    /// The miner submitted a complete block.
    FullBlock,
    /// The miner submitted a compact solved header.
    CompactHeader,
    /// An operator-authorized caller requested relay before local verification.
    TrustedBlock,
}

impl MinedBlockSubmission {
    /// Returns the RPC method name for metrics.
    pub const fn rpc_method(self) -> &'static str {
        match self {
            Self::FullBlock => "submitblock",
            Self::CompactHeader => "submitsolution",
            Self::TrustedBlock => "submittrustedblock",
        }
    }
}

/// The authority for relaying a local submission before commit.
///
/// State-authorized relay stops waiting for readiness at commitment, but a dispatched broadcast
/// may finish; contextual failure cancels it. Trusted relay preserves dispatched work on commitment,
/// but failure withdraws both announcements and queued bodies. Both policies retain the gossip deadline and committed fallback.
#[derive(Debug)]
pub enum EarlyBlockRelay {
    /// Consensus authorized relay and state admitted the candidate.
    StateAdmission(zakura_state::BlockAdmission),
    /// An operator explicitly authorized relay before validation.
    TrustedSubmission,
}

/// A local block eligible for announcement before commit.
#[derive(Debug)]
pub struct EarlyMinedBlock {
    /// Relay authority and lifecycle used to suppress obsolete early dispatch.
    pub relay: EarlyBlockRelay,
    /// Block hash.
    pub hash: block::Hash,
    /// Block height.
    pub height: block::Height,
    /// Serialized size in bytes.
    pub size: usize,
    /// When the RPC accepted the submitted bytes.
    pub submitted_at: std::time::Instant,
    /// The RPC method that accepted the submission.
    pub submission: MinedBlockSubmission,
    /// Cancels the advertisement if contextual verification rejects the block.
    pub pending: PendingBlockSignal,
}

#[derive(Debug)]
struct PendingBlock {
    owner_id: u64,
    block: Arc<block::Block>,
    status: watch::Sender<PendingStatus>,
    permit: PendingBlockSendPermit,
}

/// Stores pending block bodies with their registration owners.
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
#[derive(Clone, Debug)]
pub struct PendingBlockSignal {
    status: watch::Receiver<PendingStatus>,
    dispatched: Arc<AtomicBool>,
    permit: PendingBlockSendPermit,
}

impl PendingBlockSignal {
    /// Retains this attempt's permission through the peer write boundary.
    pub fn send_permit(&self) -> PendingBlockSendPermit {
        self.permit.clone()
    }

    /// Records dispatch to the peer service, without claiming peer delivery.
    pub fn mark_dispatched(&self) {
        self.dispatched.store(true, Ordering::Relaxed);
    }

    /// Whether the peer service was called for this pending body.
    pub fn was_dispatched(&self) -> bool {
        self.dispatched.load(Ordering::Relaxed)
    }

    /// Whether this submission is still awaiting verification and commit.
    pub fn is_pending(&self) -> bool {
        matches!(*self.status.borrow(), PendingStatus::Waiting)
    }

    /// Waits for commit or failure, so a queued trusted announcement can be withdrawn.
    pub async fn wait_for_resolution(&mut self) {
        while self.is_pending() {
            if self.status.changed().await.is_err() {
                return;
            }
        }
    }

    /// Returns true unless contextual verification has rejected the block.
    pub fn is_valid(&self) -> bool {
        !matches!(*self.status.borrow(), PendingStatus::Failed)
    }

    /// Returns a signal for a block that never fails, so tests in other crates can build an
    /// early mined-block event without owning a registry entry.
    #[cfg(feature = "proptest-impl")]
    pub fn valid_for_tests() -> Self {
        // Dropping the sender leaves the signal reading `Waiting`, so it stays valid and its
        // failure future never resolves.
        let (_, permit) = PendingBlockSendPermit::channel();
        Self {
            status: permit.subscribe(),
            dispatched: Arc::default(),
            permit,
        }
    }

    /// Resolves when contextual verification rejects the block.
    pub async fn wait_for_failure(&mut self) {
        loop {
            let status = self.status.borrow_and_update().clone();
            match status {
                PendingStatus::Failed => return,
                PendingStatus::Committed => std::future::pending::<()>().await,
                PendingStatus::Waiting => {}
            }

            if self.status.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Owns one pending-block registry entry.
#[derive(Debug)]
pub struct PendingBlockRegistration {
    registry: PendingBlockRegistry,
    hash: block::Hash,
    owner_id: u64,
    /// A receiver for this registration's own entry, kept so `signal` needs no lookup.
    receiver: watch::Receiver<PendingStatus>,
    dispatched: Arc<AtomicBool>,
    permit: PendingBlockSendPermit,
    resolved: bool,
}

impl PendingBlockRegistration {
    /// Returns a signal that cancels stale early inventory after commit failure.
    pub fn signal(&self) -> PendingBlockSignal {
        PendingBlockSignal {
            status: self.receiver.clone(),
            dispatched: self.dispatched.clone(),
            permit: self.permit.clone(),
        }
    }

    /// Resolves this registration and invalidates failed early advertisements.
    pub fn resolve(mut self, result: Result<Arc<block::Block>, ()>) {
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
    pub fn insert(&self, block: Arc<block::Block>) -> Option<PendingBlockRegistration> {
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
        let (status, permit) = PendingBlockSendPermit::channel();
        let receiver = permit.subscribe();
        entries.insert(
            hash,
            PendingBlock {
                owner_id,
                block,
                status,
                permit: permit.clone(),
            },
        );
        Some(PendingBlockRegistration {
            registry: self.clone(),
            hash,
            owner_id,
            receiver,
            dispatched: Arc::default(),
            permit,
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
        let status = match result {
            Ok(_) => PendingStatus::Committed,
            Err(()) => PendingStatus::Failed,
        };
        // Publish under the registry lock before removing the body. Escaped send permits
        // order their synchronous write starts against this same status update.
        entries
            .get(&hash)
            .expect("entry exists because its owner matched under the same lock")
            .status
            .send_replace(status);
        entries.remove(&hash);
    }

    /// Returns a temporary peer response with its attempt-specific write permission.
    pub fn get_for_peer(&self, hash: block::Hash) -> Option<ServedBlock> {
        let block = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .map(|entry| ServedBlock {
                block: entry.block.clone().into(),
                pending_send: Some(entry.permit.clone()),
            });
        if block.is_some() {
            metrics::counter!("mining.pending_registry.served").increment(1);
        }
        block
    }

    /// Inspects a pending body, which may still be awaiting consensus validation.
    /// Peer serving must use [`Self::get_for_peer`] to retain its withdrawal permission.
    pub fn get(&self, hash: block::Hash) -> Option<Arc<block::Block>> {
        let block = self
            .0
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&hash)
            .map(|entry| Arc::clone(&entry.block));
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
    /// The node could not establish acceptance, so the caller can retry.
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
    /// The node could not establish acceptance, so the caller can retry.
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

/// Carries optional early local-block announcements to gossip.
pub struct EarlyMinedBlockChannel {
    /// The channel sender
    sender: mpsc::UnboundedSender<EarlyMinedBlock>,
    /// The channel receiver
    receiver: mpsc::UnboundedReceiver<EarlyMinedBlock>,
}

impl EarlyMinedBlockChannel {
    /// Creates a new submit block channel
    pub fn new() -> Self {
        // Only authorized early events enter this channel. Committed announcements come
        // from state, and gossip consumes both feeds without waiting for peer readiness.
        let (sender, receiver) = mpsc::unbounded_channel();
        Self { sender, receiver }
    }

    /// Get the channel sender
    pub fn sender(&self) -> mpsc::UnboundedSender<EarlyMinedBlock> {
        self.sender.clone()
    }

    /// Get the channel receiver
    pub fn receiver(self) -> mpsc::UnboundedReceiver<EarlyMinedBlock> {
        self.receiver
    }
}

impl Default for EarlyMinedBlockChannel {
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
    fn retained_peer_body_is_withdrawn_on_rejection_or_owner_drop() {
        for drop_owner in [false, true] {
            let registry = PendingBlockRegistry::default();
            let block = test_block();
            let hash = block.hash();
            let owner = registry.insert(block.clone()).unwrap();
            let retained = registry.get_for_peer(hash).unwrap();
            let permit = retained.pending_send.unwrap();
            assert!(permit.try_start_write(|| ()).is_some());
            if drop_owner {
                drop(owner);
            } else {
                owner.resolve(Err(()));
            }
            assert!(registry.get_for_peer(hash).is_none());
            assert!(permit
                .try_start_write(|| panic!("a retained rejected body cannot be sent"))
                .is_none());
            let retry = registry.insert(block.clone()).unwrap();
            let current = registry.get_for_peer(hash).unwrap();
            assert!(current
                .pending_send
                .unwrap()
                .try_start_write(|| ())
                .is_some());
            assert!(permit.try_start_write(|| ()).is_none());
            retry.resolve(Ok(block));
        }
    }

    #[test]
    fn retained_peer_body_remains_sendable_after_commit() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let owner = registry.insert(block.clone()).unwrap();
        let retained = registry.get_for_peer(hash).unwrap();
        owner.resolve(Ok(block));
        assert!(registry.get_for_peer(hash).is_none());
        assert!(retained
            .pending_send
            .unwrap()
            .try_start_write(|| ())
            .is_some());
    }

    #[test]
    fn mined_submissions_bound_and_release_verification_ownership() {
        let submissions = MinedSubmissions::default();
        let hash = block::Hash([0; 32]);
        let first = submissions.reserve(hash).unwrap();
        assert!(matches!(
            submissions.reserve(hash),
            Err(SubmitBlockErrorResponse::DuplicateInconclusive)
        ));
        let mut guards = vec![first];
        for n in 1..MAX_MINED_SUBMISSIONS {
            guards.push(
                submissions
                    .reserve(block::Hash([u8::try_from(n).unwrap(); 32]))
                    .unwrap(),
            );
        }
        assert!(matches!(
            submissions.reserve(block::Hash([255; 32])),
            Err(SubmitBlockErrorResponse::Inconclusive)
        ));
        drop(guards);
        assert!(submissions.reserve(hash).is_ok());
    }

    #[test]
    fn pending_block_is_served_until_commit() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let registration = registry
            .insert(block.clone())
            .expect("the registry accepts the block");
        assert!(
            registry.insert(block.clone()).is_none(),
            "one hash has one owner"
        );
        assert_eq!(registry.get(hash), Some(block.clone()));
        registration.resolve(Ok(block));
        assert_eq!(registry.get(hash), None);
    }

    #[tokio::test]
    async fn pending_block_failure_removes_body_and_cancels_stale_inventory() {
        let registry = PendingBlockRegistry::default();
        let block = test_block();
        let hash = block.hash();
        let registration = registry
            .insert(block.clone())
            .expect("the registry accepts the block");
        let mut signal = registration.signal();
        assert!(signal.is_valid());
        assert_eq!(registry.get(hash), Some(block));
        registration.resolve(Err(()));
        assert_eq!(registry.get(hash), None);
        signal.wait_for_failure().await;
        assert!(!signal.is_valid());
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
