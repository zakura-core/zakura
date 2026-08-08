//! Typed boundary between header-sync policy and header-chain state.

use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc};

use zakura_chain::block;
use zakura_header_chain::{
    AuxDelivery, BodyWorkOwner, CommittedStallReceipt, Frontier, HeaderChainError, HeaderLocator,
    HeaderSyncWorkOwner, HeaderWorkAuthority, InsertHeaders, SourceId, TargetCompletion,
    VctRepairContext,
};

/// A boxed operation returned by [`HeaderChainPort`].
pub type HeaderChainFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Process-local capability that authenticates values sealed by one header-chain adapter.
///
/// An adapter keeps this key private and uses clones only inside its own asynchronous tasks.
/// Values sealed by a different key cannot be opened or used as retained-path handles.
#[doc(hidden)]
#[derive(Clone)]
pub struct HeaderChainAdapterKey(Arc<()>);

impl HeaderChainAdapterKey {
    /// Create the unique capability for one header-chain adapter instance.
    pub fn new() -> Self {
        Self(Arc::new(()))
    }

    fn authenticates(&self, seal: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.0, seal)
    }
}

impl Default for HeaderChainAdapterKey {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for HeaderChainAdapterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HeaderChainAdapterKey(<redacted>)")
    }
}

/// A local failure while executing a header-chain port operation.
#[derive(Clone, Debug)]
pub enum HeaderChainPortError {
    /// The adapter's operation deadline elapsed.
    Timeout,
    /// The backing service was unavailable or returned an invalid reply.
    Unavailable {
        /// Original failure, when the backing service supplied one.
        source: Option<Arc<dyn Error + Send + Sync + 'static>>,
    },
}

/// Result of resolving an exact selected-header auxiliary repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VctRepairContextReply {
    /// The requested owner and height still identify the selected branch.
    Resolved(VctRepairContext),
    /// The owner or selected height is no longer current.
    Stale,
}

/// Wire-neutral request for an immutable retained header path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquireHeaderPath {
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
    /// First requester-order locator intersection.
    pub common_ancestor: Frontier,
    /// Exact retained target.
    pub target: Frontier,
    /// Exact generation and branch fixed at acquisition.
    pub scope: HeaderWorkAuthority,
}

impl RetainedHeaderPath {
    /// Construct a retained path in a state adapter.
    #[doc(hidden)]
    pub fn from_adapter(
        adapter_key: &HeaderChainAdapterKey,
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
    pub fn adapter_identity(
        &self,
        adapter_key: &HeaderChainAdapterKey,
    ) -> Option<(u64, SourceId, u64)> {
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

/// Result of acquiring an immutable retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquireHeaderPathReply {
    /// The requested target path is retained.
    Acquired(Box<RetainedHeaderPath>),
    /// The target is no longer retained.
    TargetNotRetained,
    /// No requester locator lies on the retained target path.
    NoLocatorIntersection,
    /// Required target history has been pruned.
    HistoryPruned,
    /// State cannot currently retain another path.
    Busy,
}

/// A bounded read from an already acquired retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadHeaderPath {
    /// Common ancestor or previous page tip.
    pub after_hash: block::Hash,
    /// Maximum number of returned headers.
    pub max_header_count: u32,
}

/// One raw page from an immutable retained header path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedHeaderPathPage {
    /// Exact page ancestor.
    pub common_ancestor: Frontier,
    /// Exact retained target.
    pub target: Frontier,
    /// Exact generation and branch fixed at acquisition.
    pub scope: HeaderWorkAuthority,
    /// Canonical headers in parent-first order.
    pub headers: Vec<Arc<block::Header>>,
    /// Parallel auxiliary deliveries for each retained header.
    pub aux_deliveries: Vec<Vec<AuxDelivery>>,
    /// Whether this page reaches the immutable target.
    pub complete: bool,
}

/// Result of reading an immutable retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadHeaderPathReply {
    /// The requested page remains available.
    Page(Box<RetainedHeaderPathPage>),
    /// The lease expired or became unavailable.
    Unavailable,
}

/// One header and its unauthenticated parallel metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderTargetEntry {
    /// Canonical Zcash block header.
    pub header: Arc<block::Header>,
    /// Serialized-body-size hint; zero means unknown.
    pub body_size: u32,
    /// Optional schema-1 commitment record.
    pub tree_aux: Option<zakura_header_chain::TreeAuxRecordV1>,
}

/// A header-target entry list bounded by the engine's production transition cap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderTargetEntries(Vec<HeaderTargetEntry>);

impl HeaderTargetEntries {
    /// Borrow the bounded entries in parent-first order.
    pub fn as_slice(&self) -> &[HeaderTargetEntry] {
        &self.0
    }

    /// Consume the bounded wrapper without reallocating its entries.
    pub fn into_vec(self) -> Vec<HeaderTargetEntry> {
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

impl TryFrom<Vec<HeaderTargetEntry>> for HeaderTargetEntries {
    type Error = HeaderTargetEntriesError;

    fn try_from(entries: Vec<HeaderTargetEntry>) -> Result<Self, Self::Error> {
        if entries.len() > zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1 {
            return Err(HeaderTargetEntriesError {
                actual: entries.len(),
                maximum: zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1,
            });
        }
        Ok(Self(entries))
    }
}

/// A header-target list exceeded the frozen production transition cap.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderTargetEntriesError {
    /// Supplied entry count.
    pub actual: usize,
    /// Maximum accepted entry count.
    pub maximum: usize,
}

impl fmt::Display for HeaderTargetEntriesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "header target contains {} entries, maximum is {}",
            self.actual, self.maximum
        )
    }
}

impl Error for HeaderTargetEntriesError {}

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
    pub entries: HeaderTargetEntries,
    /// Proof that the response satisfies its target purpose.
    pub completion: TargetCompletion,
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
    pub fn from_insert(adapter_key: &HeaderChainAdapterKey, insert: Box<InsertHeaders>) -> Self {
        Self {
            adapter_seal: adapter_key.0.clone(),
            insert,
        }
    }

    /// Consume a sealed target only in the adapter that prepared it.
    #[doc(hidden)]
    pub fn into_insert(
        self,
        adapter_key: &HeaderChainAdapterKey,
    ) -> Result<Box<InsertHeaders>, Self> {
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

/// Header-chain operations needed by header-sync policy.
///
/// Each request and its typed reply share one future. Implementations own local
/// deadlines and translation to any backing service protocol.
pub trait HeaderChainPort: Send + Sync + 'static {
    /// Read one coherent selected-path continuation locator.
    fn continuation_locator(
        &self,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, HeaderChainPortError>>;

    /// Resolve one exact selected-header auxiliary repair.
    fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, HeaderChainPortError>>;

    /// Acquire an immutable retained target path.
    fn acquire_header_path(
        &self,
        request: AcquireHeaderPath,
    ) -> HeaderChainFuture<'_, Result<AcquireHeaderPathReply, HeaderChainPortError>>;

    /// Read one bounded page from an immutable retained target path.
    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadHeaderPath,
    ) -> HeaderChainFuture<'_, Result<ReadHeaderPathReply, HeaderChainPortError>>;

    /// Idempotently release an immutable retained target path.
    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), HeaderChainPortError>>;

    /// Validate and seal one complete target outside the serialized writer.
    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply>;

    /// Atomically apply one sealed target.
    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply>;
}

impl std::fmt::Debug for dyn HeaderChainPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HeaderChainPort")
    }
}

/// Explicit unavailable port used when no durable header-chain state is attached.
#[derive(Debug, Default)]
pub struct UnavailableHeaderChainPort;

impl HeaderChainPort for UnavailableHeaderChainPort {
    fn continuation_locator(
        &self,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, HeaderChainPortError>> {
        Box::pin(async { Err(HeaderChainPortError::Unavailable { source: None }) })
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, HeaderChainPortError>> {
        Box::pin(async { Err(HeaderChainPortError::Unavailable { source: None }) })
    }

    fn acquire_header_path(
        &self,
        _request: AcquireHeaderPath,
    ) -> HeaderChainFuture<'_, Result<AcquireHeaderPathReply, HeaderChainPortError>> {
        Box::pin(async { Ok(AcquireHeaderPathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: RetainedHeaderPath,
        _request: ReadHeaderPath,
    ) -> HeaderChainFuture<'_, Result<ReadHeaderPathReply, HeaderChainPortError>> {
        Box::pin(async { Ok(ReadHeaderPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), HeaderChainPortError>> {
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
}

/// Inert port used by state-machine tests that drive completions explicitly.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct InertHeaderChainPort;

impl HeaderChainPort for InertHeaderChainPort {
    fn continuation_locator(
        &self,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, HeaderChainPortError>> {
        Box::pin(std::future::pending())
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, HeaderChainPortError>> {
        Box::pin(std::future::pending())
    }

    fn acquire_header_path(
        &self,
        _request: AcquireHeaderPath,
    ) -> HeaderChainFuture<'_, Result<AcquireHeaderPathReply, HeaderChainPortError>> {
        Box::pin(std::future::pending())
    }

    fn read_header_path(
        &self,
        _path: RetainedHeaderPath,
        _request: ReadHeaderPath,
    ) -> HeaderChainFuture<'_, Result<ReadHeaderPathReply, HeaderChainPortError>> {
        Box::pin(std::future::pending())
    }

    fn release_header_path(
        &self,
        _path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), HeaderChainPortError>> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MinimalMock;

    impl HeaderChainPort for MinimalMock {
        fn continuation_locator(
            &self,
        ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, HeaderChainPortError>> {
            Box::pin(async { Ok(None) })
        }

        fn vct_repair_context(
            &self,
            _owner: BodyWorkOwner,
            _height: block::Height,
        ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, HeaderChainPortError>> {
            Box::pin(async { Ok(VctRepairContextReply::Stale) })
        }

        fn acquire_header_path(
            &self,
            _request: AcquireHeaderPath,
        ) -> HeaderChainFuture<'_, Result<AcquireHeaderPathReply, HeaderChainPortError>> {
            Box::pin(async { Ok(AcquireHeaderPathReply::Busy) })
        }

        fn read_header_path(
            &self,
            _path: RetainedHeaderPath,
            _request: ReadHeaderPath,
        ) -> HeaderChainFuture<'_, Result<ReadHeaderPathReply, HeaderChainPortError>> {
            Box::pin(async { Ok(ReadHeaderPathReply::Unavailable) })
        }

        fn release_header_path(
            &self,
            _path: RetainedHeaderPath,
        ) -> HeaderChainFuture<'_, Result<(), HeaderChainPortError>> {
            Box::pin(async { Ok(()) })
        }

        fn prepare_header_target(
            &self,
            _request: PrepareHeaderTarget,
        ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
            unreachable!("the mock need not construct a state service")
        }

        fn apply_header_target(
            &self,
            _target: PreparedHeaderTarget,
        ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
            unreachable!("the mock need not construct a state service")
        }
    }

    #[tokio::test]
    async fn port_is_object_safe_and_mockable_without_state_services() {
        let port: Arc<dyn HeaderChainPort> = Arc::new(MinimalMock);
        assert!(port.continuation_locator().await.unwrap().is_none());
    }

    #[test]
    fn retained_path_identity_requires_the_issuing_adapter_key() {
        let issuing_key = HeaderChainAdapterKey::new();
        let foreign_key = HeaderChainAdapterKey::new();
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
        let entry = HeaderTargetEntry {
            header: zakura_chain::block::genesis::regtest_genesis_block()
                .header
                .clone(),
            body_size: 0,
            tree_aux: None,
        };
        let limit = zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1;

        let accepted = HeaderTargetEntries::try_from(vec![entry.clone(); limit])
            .expect("the exact engine batch limit is accepted");
        assert_eq!(accepted.len(), limit);

        assert_eq!(
            HeaderTargetEntries::try_from(vec![entry; limit + 1]),
            Err(HeaderTargetEntriesError {
                actual: limit + 1,
                maximum: limit,
            })
        );
    }
}
