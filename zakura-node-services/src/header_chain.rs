//! Typed boundary between header-sync policy and header-chain state.

use std::{error::Error, future::Future, pin::Pin, sync::Arc};

use zakura_chain::{block, parameters::Network};
use zakura_header_chain::{
    AuxDelivery, Frontier, HeaderChainError, HeaderLocator, InsertHeaders, SourceId,
    TargetCompletion, VctRepairContext, WorkOwner, WorkScope,
};

/// A boxed operation returned by [`HeaderChainPort`].
pub type HeaderChainFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    pub scope: WorkScope,
    /// Exact target branch.
    pub target_tip_hash: block::Hash,
    /// Requester-order locator hashes.
    pub locator_hashes: Vec<block::Hash>,
}

/// Adapter-private identity for an immutable retained path.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderPathToken(u64);

impl HeaderPathToken {
    /// Construct a token in a state adapter.
    #[doc(hidden)]
    pub fn from_adapter_id(id: u64) -> Self {
        Self(id)
    }

    /// Recover the adapter's identity in that same adapter.
    #[doc(hidden)]
    pub fn adapter_id(self) -> u64 {
        self.0
    }
}

/// An immutable retained path acquired from the header-chain port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedHeaderPath {
    token: HeaderPathToken,
    source: SourceId,
    session_id: u64,
    /// First requester-order locator intersection.
    pub common_ancestor: Frontier,
    /// Exact retained target.
    pub target: Frontier,
    /// Exact generation and branch fixed at acquisition.
    pub scope: WorkScope,
}

impl RetainedHeaderPath {
    /// Construct a retained path in a state adapter.
    #[doc(hidden)]
    pub fn from_adapter(
        token: HeaderPathToken,
        source: SourceId,
        session_id: u64,
        common_ancestor: Frontier,
        target: Frontier,
        scope: WorkScope,
    ) -> Self {
        Self {
            token,
            source,
            session_id,
            common_ancestor,
            target,
            scope,
        }
    }

    /// Return the opaque token to a state adapter.
    #[doc(hidden)]
    pub fn adapter_identity(&self) -> (HeaderPathToken, SourceId, u64) {
        (self.token, self.source, self.session_id)
    }
}

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
    pub scope: WorkScope,
    /// Retained nodes in parent-first order.
    pub nodes: Vec<zakura_header_chain::HeaderNode>,
    /// Parallel auxiliary deliveries for each retained node.
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

/// Complete input to deterministic target preparation.
#[derive(Clone, Debug)]
pub struct PrepareHeaderTarget {
    /// Stable supplier identity.
    pub source: SourceId,
    /// Authenticated network parameters.
    pub network: Network,
    /// Exact asynchronous owner.
    pub owner: WorkOwner,
    /// Exact initial locator intersection.
    pub common_ancestor: Frontier,
    /// Exact advertised target.
    pub target: Frontier,
    /// Response entries in parent-first order.
    pub entries: Vec<HeaderTargetEntry>,
    /// Proof that the response satisfies its target purpose.
    pub completion: TargetCompletion,
}

/// A target sealed by the port's preparation operation.
#[derive(Clone, Debug)]
pub struct PreparedHeaderTarget(Box<InsertHeaders>);

impl PreparedHeaderTarget {
    /// Seal an insertion in a state adapter.
    #[doc(hidden)]
    pub fn from_insert(insert: Box<InsertHeaders>) -> Self {
        Self(insert)
    }

    /// Consume a sealed target in a state adapter.
    #[doc(hidden)]
    pub fn into_insert(self) -> Box<InsertHeaders> {
        self.0
    }
}

/// Result of target preparation.
pub type PrepareHeaderTargetReply = Result<PreparedHeaderTarget, Arc<HeaderChainError>>;

/// Result of atomically applying a prepared target.
pub type ApplyHeaderTargetReply = Result<(), Arc<HeaderChainError>>;

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
        owner: WorkOwner,
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
        _owner: WorkOwner,
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
                zakura_header_chain::ErrorSubject::Branch(request.owner.branch),
                None,
            )))
        })
    }

    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        let owner = target.0.owner;
        Box::pin(async move {
            Err(Arc::new(HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
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
        _owner: WorkOwner,
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
            _owner: WorkOwner,
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
}
