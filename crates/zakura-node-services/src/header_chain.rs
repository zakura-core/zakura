//! Typed boundary between header-sync policy and header-chain state.

use std::{error::Error, fmt, future::Future, num::NonZeroU64, pin::Pin, sync::Arc};

use zakura_chain::block;
use zakura_header_chain::{
    AuxDelivery, BodyWorkOwner, CommittedStallReceipt, EngineSnapshot, Frontier, HeaderChainError,
    HeaderLocator, HeaderSyncWorkOwner, HeaderWorkAuthority, InsertHeaders, SourceId,
    TargetCompletion, VctRepairContext,
};

/// A boxed operation returned by [`Port`].
pub type HeaderChainFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Process-local capability that authenticates values sealed by one header-chain adapter.
///
/// An adapter keeps this key private and uses clones only inside its own asynchronous tasks.
/// The adapter cannot open values that a different key sealed.
/// The adapter cannot use those values as retained-path handles.
#[doc(hidden)]
#[derive(Clone)]
pub struct AdapterKey(Arc<()>);

impl AdapterKey {
    /// Create the unique capability for one header-chain adapter instance.
    pub fn new() -> Self {
        Self(Arc::new(()))
    }

    fn authenticates(&self, seal: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.0, seal)
    }
}

impl Default for AdapterKey {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AdapterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdapterKey(<redacted>)")
    }
}

/// A local failure while executing a header-chain port operation.
#[derive(Clone, Debug)]
pub enum PortError {
    /// The adapter's operation deadline elapsed.
    Timeout,
    /// The backing service became unavailable or returned an invalid reply.
    Unavailable {
        /// Original failure, when the backing service supplied one.
        source: Option<Arc<dyn Error + Send + Sync + 'static>>,
    },
}

/// Result of resolving an exact selected-range auxiliary repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VctRepairContextReply {
    /// The requested owner and height still identify the selected branch.
    Resolved(VctRepairContext),
    /// The owner or selected height is no longer current.
    Stale,
}

/// Wire-neutral request for an immutable retained header path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquirePath {
    /// Stable requester identity.
    pub source: SourceId,
    /// Ordered-stream generation.
    pub session_id: u64,
    /// Exact generation and branch to retain.
    pub scope: HeaderWorkAuthority,
    /// Exact target branch.
    pub target_tip_hash: block::Hash,
    /// Requester-order locator hashes.
    pub locator_hashes: Vec<block::Hash>,
}

/// An immutable retained path acquired from the header-chain port.
#[derive(Clone)]
pub struct RetainedHeaderPath {
    adapter_seal: Arc<()>,
    adapter_id: u64,
    source: SourceId,
    session_id: u64,
    /// Nearest locator ancestor on the target’s chain.
    pub common_ancestor: Frontier,
    /// Exact retained target.
    pub target: Frontier,
    /// Original request scope, retained for correlation across local head updates.
    pub scope: HeaderWorkAuthority,
}

impl RetainedHeaderPath {
    /// Construct a retained path in a state adapter.
    #[doc(hidden)]
    pub fn from_adapter(
        adapter_key: &AdapterKey,
        adapter_id: u64,
        source: SourceId,
        session_id: u64,
        common_ancestor: Frontier,
        target: Frontier,
        scope: HeaderWorkAuthority,
    ) -> Self {
        Self {
            adapter_seal: adapter_key.0.clone(),
            adapter_id,
            source,
            session_id,
            common_ancestor,
            target,
            scope,
        }
    }

    /// Return the opaque identity only to the adapter that issued this path.
    #[doc(hidden)]
    pub fn adapter_identity(&self, adapter_key: &AdapterKey) -> Option<(u64, SourceId, u64)> {
        adapter_key.authenticates(&self.adapter_seal).then_some((
            self.adapter_id,
            self.source,
            self.session_id,
        ))
    }

    /// Return the non-authoritative handle used to correlate this path inside its caller.
    ///
    /// This value cannot authenticate the path without the issuing adapter's private key.
    pub fn handle_id(&self) -> u64 {
        self.adapter_id
    }
}

impl fmt::Debug for RetainedHeaderPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedHeaderPath")
            .field("adapter_identity", &"<redacted>")
            .field("common_ancestor", &self.common_ancestor)
            .field("target", &self.target)
            .field("scope", &self.scope)
            .finish()
    }
}

impl PartialEq for RetainedHeaderPath {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.adapter_seal, &other.adapter_seal)
            && self.adapter_id == other.adapter_id
            && self.source == other.source
            && self.session_id == other.session_id
            && self.common_ancestor == other.common_ancestor
            && self.target == other.target
            && self.scope == other.scope
    }
}

impl Eq for RetainedHeaderPath {}

/// A sticky notification for one occupied serving resource.
///
/// State registers the signal while it holds the capacity lock. A release remains
/// observable even when the reactor receives the refusal after the release.
#[derive(Clone, Debug)]
pub struct ServingCapacitySignal(zakura_rt::sync::watch::Sender<bool>);

impl Default for ServingCapacitySignal {
    fn default() -> Self {
        Self(zakura_rt::sync::watch::channel(false).0)
    }
}

impl PartialEq for ServingCapacitySignal {
    fn eq(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }
}

impl Eq for ServingCapacitySignal {}

impl ServingCapacitySignal {
    /// Record a confirmed release of this resource.
    pub fn release(&self) {
        self.0.send_replace(true);
    }

    /// Return whether state has released this resource.
    pub fn is_released(&self) -> bool {
        *self.0.borrow()
    }

    /// Wait for release, including a release before this call.
    pub async fn released(&self) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|released| *released).await;
    }
}

/// Result of acquiring an immutable retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquirePathReply {
    /// The adapter retains the requested target path.
    Acquired(Box<RetainedHeaderPath>),
    /// The adapter no longer retains the target.
    TargetNotRetained,
    /// No requester locator lies on the retained target path.
    NoLocatorIntersection,
    /// Pruning removed the required target history.
    HistoryPruned,
    /// State cannot currently retain another path for a non-capacity reason.
    Busy,
    /// An occupied resource refused the request.
    CapacityBusy(ServingCapacitySignal),
}

/// Outcome of admitting a single exact header.
///
/// Coalesces by hash with header sync so a header already retained on either
/// the selected chain or a retained fork returns [`Self::Admitted`] without
/// re-validating.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExactHeaderAdmission {
    /// Exact header is retained/admitted at `height`.
    Admitted {
        /// Consensus height of the admitted header.
        height: block::Height,
        /// `true` when headerchain already retained this hash (e.g., via header sync),
        /// allowing BPv1 to record `header.duplicate_bytes` (`decision-2`).
        already_known: bool,
    },
    /// Parent header `parent_hash` is not yet retained in headerchain.
    MissingParent {
        /// Hash of the missing parent header.
        parent_hash: block::Hash,
    },
    /// Header failed consensus/headerchain validation (`bpv1-close-3`).
    PeerInvalid,
    /// Local storage/service failure (never faults peer).
    LocalError,
}

impl ExactHeaderAdmission {
    /// Classifies a failed prepare or apply of an exact-header target.
    pub fn target_failure(error: &HeaderChainError) -> Self {
        if error.is_automatic_header_peer_fault() {
            Self::PeerInvalid
        } else {
            Self::LocalError
        }
    }

    /// Classifies a successful apply of an exact-header target at `height`.
    pub fn applied(outcome: ApplyHeaderTargetOutcome, height: block::Height) -> Self {
        match outcome {
            ApplyHeaderTargetOutcome::Applied => Self::Admitted {
                height,
                already_known: false,
            },
            ApplyHeaderTargetOutcome::ResourceStalled(_) => Self::LocalError,
        }
    }
}

/// Where a header hash sits relative to the selected header chain.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SelectedChainMembership {
    /// The selected chain contains the header at this frontier.
    Selected(Frontier),
    /// The adapter retains the header off the selected chain.
    NotSelected,
    /// The adapter retains no header with this hash.
    NotRetained,
}

/// A bounded read from an already acquired retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadPath {
    /// Common ancestor or previous page tip.
    pub after_hash: block::Hash,
    /// Maximum number of returned headers.
    pub max_header_count: u32,
    /// Whether the caller requested schema-v1 commitment roots.
    pub want_tree_aux: bool,
}

/// One raw page from an immutable retained header path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedHeaderPathPage {
    /// Exact page ancestor.
    pub common_ancestor: Frontier,
    /// Exact retained target.
    pub target: Frontier,
    /// Original request scope, retained for correlation across local head updates.
    pub scope: HeaderWorkAuthority,
    /// Canonical headers in parent-first order.
    pub headers: Vec<Arc<block::Header>>,
    /// Parallel auxiliary deliveries for each retained header.
    pub aux_deliveries: Vec<Vec<AuxDelivery>>,
    /// Authoritative commitment roots loaded from finalized state, parallel to `headers`.
    ///
    /// These roots do not have peer-delivery provenance.
    /// The page keeps the roots separate from `aux_deliveries`.
    /// A serving adapter may use the roots for finalized historical pages.
    pub finalized_tree_aux: Vec<Option<zakura_header_chain::TreeAuxRecordV1>>,
    /// Committed serialized body sizes looked up by header hash in the full state, parallel
    /// to `headers`. No peer-delivery provenance; a serving adapter prefers these over
    /// advertised delivery sizes. `None` where the block is not committed locally.
    pub finalized_body_sizes: Vec<Option<std::num::NonZeroU32>>,
    /// Whether this page reaches the immutable target.
    pub complete: bool,
}

/// Result of reading an immutable retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadPathReply {
    /// The requested page remains available.
    Page(Box<RetainedHeaderPathPage>),
    /// The lease expired or became unavailable.
    Unavailable,
}

/// One header and its unauthenticated parallel metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetEntry {
    /// Canonical Zcash block header.
    pub header: Arc<block::Header>,
    /// Serialized-body-size hint.
    /// Zero means unknown.
    pub body_size: u32,
    /// Optional schema-1 commitment record.
    pub tree_aux: Option<zakura_header_chain::TreeAuxRecordV1>,
}

/// A header-target entry list bounded by the engine's production transition cap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetEntries(Vec<TargetEntry>);

impl TargetEntries {
    /// Borrow the bounded entries in parent-first order.
    pub fn as_slice(&self) -> &[TargetEntry] {
        &self.0
    }

    /// Consume the bounded wrapper without reallocating its entries.
    pub fn into_vec(self) -> Vec<TargetEntry> {
        self.0
    }

    /// Return the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the target contains no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<Vec<TargetEntry>> for TargetEntries {
    type Error = TargetEntriesError;

    fn try_from(entries: Vec<TargetEntry>) -> Result<Self, Self::Error> {
        if entries.len() > zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1 {
            return Err(TargetEntriesError {
                actual: entries.len(),
                maximum: zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1,
            });
        }
        Ok(Self(entries))
    }
}

/// A header-target list exceeded the frozen production transition cap.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TargetEntriesError {
    /// Supplied entry count.
    pub actual: usize,
    /// Maximum accepted entry count.
    pub maximum: usize,
}

impl fmt::Display for TargetEntriesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "header target contains {} entries, maximum is {}",
            self.actual, self.maximum
        )
    }
}

impl Error for TargetEntriesError {}

/// Complete input to deterministic target preparation.
#[derive(Clone, Debug)]
pub struct PrepareHeaderTarget {
    /// Stable supplier identity.
    pub source: SourceId,
    /// Exact asynchronous owner.
    pub owner: HeaderSyncWorkOwner,
    /// Exact initial locator intersection.
    pub common_ancestor: Frontier,
    /// Exact advertised target.
    pub target: Frontier,
    /// Response entries in parent-first order.
    pub entries: TargetEntries,
    /// Proof that the response satisfies its target purpose.
    pub completion: TargetCompletion,
}

impl PrepareHeaderTarget {
    /// Builds the one-header target that [`Port::admit_exact_header`] prepares
    /// on top of the retained `parent`. The request's `target` holds the
    /// header's height. Header work uses the finalized frontier from `snapshot`.
    ///
    /// Returns [`ExactHeaderAdmission::PeerInvalid`] when the header's height
    /// overflows.
    pub fn exact_header(
        header: Arc<block::Header>,
        parent: Frontier,
        snapshot: &EngineSnapshot,
        source: SourceId,
        session_id: u64,
        request_id: NonZeroU64,
    ) -> Result<Self, ExactHeaderAdmission> {
        let Ok(height) = parent.height.next() else {
            return Err(ExactHeaderAdmission::PeerInvalid);
        };
        let hash = header.hash();
        let owner = HeaderSyncWorkOwner::Header(
            HeaderWorkAuthority::for_target(snapshot, hash).bind(session_id, request_id),
        );
        let entries = TargetEntries::try_from(vec![TargetEntry {
            header,
            body_size: 0,
            tree_aux: None,
        }])
        .map_err(|_| ExactHeaderAdmission::LocalError)?;
        Ok(Self {
            source,
            owner,
            common_ancestor: parent,
            target: Frontier::new(height, hash),
            entries,
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
        })
    }
}

/// A target sealed by the port's preparation operation.
#[derive(Clone)]
pub struct PreparedHeaderTarget {
    adapter_seal: Arc<()>,
    insert: Box<InsertHeaders>,
}

impl PreparedHeaderTarget {
    /// Seal an insertion in a state adapter.
    #[doc(hidden)]
    pub fn from_insert(adapter_key: &AdapterKey, insert: Box<InsertHeaders>) -> Self {
        Self {
            adapter_seal: adapter_key.0.clone(),
            insert,
        }
    }

    /// Consume a sealed target only in the adapter that prepared it.
    #[doc(hidden)]
    pub fn into_insert(self, adapter_key: &AdapterKey) -> Result<Box<InsertHeaders>, Self> {
        if adapter_key.authenticates(&self.adapter_seal) {
            Ok(self.insert)
        } else {
            Err(self)
        }
    }

    /// Return the work owner without exposing the sealed insertion.
    pub fn owner(&self) -> HeaderSyncWorkOwner {
        self.insert.owner
    }

    /// Return the supplier identity without exposing the sealed insertion.
    pub fn source(&self) -> SourceId {
        self.insert.source
    }

    /// Return the pursued target without exposing the sealed insertion.
    pub fn target_tip_hash(&self) -> block::Hash {
        self.insert.target_tip_hash
    }

    /// Return the number of sealed auxiliary deliveries.
    pub fn auxiliary_delivery_count(&self) -> usize {
        self.insert.aux.len()
    }
}

impl fmt::Debug for PreparedHeaderTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedHeaderTarget")
            .field("adapter_seal", &"<redacted>")
            .field("owner", &self.insert.owner)
            .finish_non_exhaustive()
    }
}

/// Result of target preparation.
pub type PrepareHeaderTargetReply = Result<PreparedHeaderTarget, Arc<HeaderChainError>>;

/// Result of atomically applying a prepared target.
pub type ApplyHeaderTargetReply = Result<ApplyHeaderTargetOutcome, Arc<HeaderChainError>>;

/// Successful state-side outcome of applying one prepared header target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ApplyHeaderTargetOutcome {
    /// State committed the target or recognized its idempotent replay.
    Applied,
    /// State committed the resource-stall outcome without admitting the target.
    ResourceStalled(CommittedStallReceipt),
}

/// Capacity owned by one locator read through its last non-cancellable operation.
///
/// Clones share one permit. Adapters must move a clone into accepted work before
/// awaiting it, so dropping a timed-out caller cannot release that work's capacity.
#[derive(Clone, Debug)]
pub struct LocatorReadPermit(Arc<zakura_rt::sync::OwnedSemaphorePermit>);

impl LocatorReadPermit {
    /// Wrap a permit acquired by the locator scheduler.
    pub fn new(permit: zakura_rt::sync::OwnedSemaphorePermit) -> Self {
        Self(Arc::new(permit))
    }
}

impl PartialEq for LocatorReadPermit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for LocatorReadPermit {}

/// Header-chain operations that header-sync policy requires.
///
/// Each operation returns one typed future for its request and reply.
/// An implementation enforces its local deadline.
/// An implementation translates its backing service protocol into this contract.
pub trait Port: Send + Sync + 'static {
    /// Return a coherent locator for the current selected path.
    ///
    /// Retain `permit` in any accepted work that can outlive this future. A timeout
    /// or cancellation must not release capacity while that work still executes.
    fn continuation_locator(
        &self,
        permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>>;

    /// Resolve selected-path context for the auxiliary repair at `height`.
    fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>>;

    /// Retain the target path identified by `request`.
    ///
    /// An acquired reply reserves capacity until a successful read or explicit release.
    /// Local retention may evict the path before a page snapshot is captured.
    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>>;

    /// Return one bounded page from a retained target path.
    ///
    /// The port returns [`ReadPathReply::Unavailable`] when the path is no longer retained.
    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>>;

    /// Release a retained target path.
    ///
    /// The port accepts repeated releases for the same path.
    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>>;

    /// Validate and seal a complete target outside the serialized state writer.
    ///
    /// The port returns a target that only its issuing adapter can apply.
    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply>;

    /// Atomically apply a sealed target.
    ///
    /// The port returns the committed state result or the transition failure.
    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply>;

    /// Watch the selected header chain.
    ///
    /// The value is the latest committed engine snapshot, or `None` before the
    /// first commit. It changes after each committed transition, so a change
    /// wakes work that follows the selected tip. The default never changes,
    /// for a port without durable header-chain state.
    fn selected_chain(&self) -> zakura_rt::sync::watch::Receiver<Option<EngineSnapshot>> {
        zakura_rt::sync::watch::channel(None).1
    }

    /// Report whether the selected chain contains the header `hash`.
    ///
    /// The answer comes from the latest committed selected chain, which is at
    /// least as new as the latest value of [`Self::selected_chain`]. It costs
    /// no ancestry walk and no retained-path lease.
    fn selected_chain_contains(
        &self,
        hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>>;

    /// Admit a single exact header into headerchain.
    ///
    /// Coalesces by hash with header sync: if `header` is already retained
    /// (selected chain or retained fork), returns [`ExactHeaderAdmission::Admitted`]
    /// immediately without re-validating.
    fn admit_exact_header(
        &self,
        _header: Arc<block::Header>,
        _source: SourceId,
        _session_id: u64,
        _request_id: std::num::NonZeroU64,
    ) -> HeaderChainFuture<'_, Result<ExactHeaderAdmission, PortError>> {
        Box::pin(async { Ok(ExactHeaderAdmission::LocalError) })
    }
}

impl std::fmt::Debug for dyn Port {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Port")
    }
}

/// A node uses this port when it has no durable header-chain state.
#[derive(Debug, Default)]
pub struct UnavailablePort;

impl Port for UnavailablePort {
    fn continuation_locator(
        &self,
        _permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        Box::pin(async { Err(PortError::Unavailable { source: None }) })
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        Box::pin(async { Err(PortError::Unavailable { source: None }) })
    }

    fn acquire_header_path(
        &self,
        _request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        Box::pin(async { Ok(AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: RetainedHeaderPath,
        _request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        Box::pin(async { Ok(ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        Box::pin(async move {
            Err(Arc::new(HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(request.owner.header_authority().branch),
                None,
            )))
        })
    }

    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        let owner = target.owner();
        Box::pin(async move {
            Err(Arc::new(HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                None,
            )))
        })
    }

    fn selected_chain_contains(
        &self,
        _hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        Box::pin(async { Ok(SelectedChainMembership::NotRetained) })
    }
}

/// Inert port used by state-machine tests that drive completions explicitly.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct InertHeaderChainPort;

impl Port for InertHeaderChainPort {
    fn continuation_locator(
        &self,
        _permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        Box::pin(std::future::pending())
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        Box::pin(std::future::pending())
    }

    fn acquire_header_path(
        &self,
        _request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        Box::pin(std::future::pending())
    }

    fn read_header_path(
        &self,
        _path: RetainedHeaderPath,
        _request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        Box::pin(std::future::pending())
    }

    fn release_header_path(
        &self,
        _path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        Box::pin(std::future::pending())
    }

    fn prepare_header_target(
        &self,
        _request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        Box::pin(std::future::pending())
    }

    fn apply_header_target(
        &self,
        _target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        Box::pin(std::future::pending())
    }

    fn selected_chain_contains(
        &self,
        _hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        Box::pin(std::future::pending())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_header_uses_finalized_authority_when_the_parent_is_unfinalized() {
        use zakura_header_chain::{
            BranchId, ChainScore, EngineMode, FrontierSet, HeaderGeneration, StateVersion,
            SuffixWork, VerifiedGeneration,
        };

        let finalized = Frontier::new(block::Height(2), block::Hash([2; 32]));
        let parent = Frontier::new(block::Height(3), block::Hash([3; 32]));
        let snapshot = EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(4),
            verified_generation: VerifiedGeneration::new(1),
            frontiers: FrontierSet {
                finalized,
                header_best: parent,
                verified_best: parent,
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), parent.hash),
            oldest_retained_height: finalized.height,
            alarms: Default::default(),
        };
        let mut header = *block::genesis::regtest_genesis_block().header;
        header.previous_block_hash = parent.hash;
        let hash = header.hash();
        let target = PrepareHeaderTarget::exact_header(
            Arc::new(header),
            parent,
            &snapshot,
            SourceId::from_digest([0x5a; 32]),
            7,
            NonZeroU64::MIN,
        )
        .expect("the retained parent has a valid next height");

        assert_eq!(target.common_ancestor, parent);
        assert_eq!(target.target, Frontier::new(block::Height(4), hash));
        assert_eq!(
            target.owner.header_authority(),
            HeaderWorkAuthority {
                header_generation: snapshot.header_generation,
                branch: BranchId::new(finalized.hash, hash),
            }
        );
    }

    #[test]
    fn retained_path_identity_requires_the_issuing_adapter_key() {
        let issuing_key = AdapterKey::new();
        let foreign_key = AdapterKey::new();
        let source = SourceId::from_digest([0x5a; 32]);
        let common_ancestor = Frontier::new(block::Height(2), block::Hash([2; 32]));
        let target = Frontier::new(block::Height(3), block::Hash([3; 32]));
        let scope = HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(4),
            branch: zakura_header_chain::BranchId::new(common_ancestor.hash, target.hash),
        };
        let path = RetainedHeaderPath::from_adapter(
            &issuing_key,
            123_456_789,
            source,
            7,
            common_ancestor,
            target,
            scope,
        );

        assert_eq!(
            path.adapter_identity(&issuing_key),
            Some((123_456_789, source, 7))
        );
        assert_eq!(path.adapter_identity(&foreign_key), None);
        assert!(!format!("{path:?}").contains("123456789"));
    }

    #[test]
    fn header_target_entries_enforce_the_engine_batch_cap() {
        let entry = TargetEntry {
            header: zakura_chain::block::genesis::regtest_genesis_block()
                .header
                .clone(),
            body_size: 0,
            tree_aux: None,
        };
        let limit = zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1;

        let accepted = TargetEntries::try_from(vec![entry.clone(); limit])
            .expect("the exact engine batch limit is accepted");
        assert_eq!(accepted.len(), limit);

        assert_eq!(
            TargetEntries::try_from(vec![entry; limit + 1]),
            Err(TargetEntriesError {
                actual: limit + 1,
                maximum: limit,
            })
        );
    }
}
