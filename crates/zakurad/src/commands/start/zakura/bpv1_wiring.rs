//! Production BPv1 node wiring for `zakurad`.
//!
//! Attaches BPv1 as an opt-in v2 front on `Zakura` and `Dual` stacks only
//! (strictly absent in `Legacy`, zero changes under `peer/`, `peer_set/`,
//! `inbound/`, or `ChainSync` per `dual-1`).

use std::{num::NonZeroU64, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use zakura_chain::{block, parameters::Network};
use zakura_node_services::{
    block_memory::{BlockHandle, DecodedBlockPool, OwnedBlock},
    body_claims::SharedBodyClaims,
};

use zakura_network::zakura::{
    bpv1::{
        bpv1_custom_service, classify_body_verification_class, BlockVerifierPort, Bpv1Handle,
        Bpv1Ports, CommittedBlockPort, HavePacing, HeaderChainAdmissionAdapter, MemoryBodyStore,
        NodeId, PortFuture, Profile, VerifierVerdict, WorkId, ZakuraBpv1Config, VERIFY_DEADLINE,
    },
    CustomService, ZakuraHeaderSyncDriverStartup,
};

use super::{block_verify_error_class, BlockApplyTerminal, SyncCoordinator};

/// Bounds one BPv1 submission to the router.
///
/// A block whose parent is missing can wait in the verifier for its parent's block
/// info, its UTXOs, or a place in the state's orphan queue. BPv1's block table
/// falls back at its verification deadline, so the adapter cancels the router call
/// at the same bound. A timeout returns [`VerifierVerdict::LocalCapacity`] (#2718).
const SUBMIT_TIMEOUT: Duration = VERIFY_DEADLINE;

/// Production [`BlockVerifierPort`] wrapping `zakurad`'s shared `block_verifier_router`
/// (`dual-2`, `dual-4`, `decision-8`).
///
/// Maps `BodyVerificationClass::PayloadMismatch` (bad tx Merkle root OR bad ZIP 244
/// auth-data root) to [`VerifierVerdict::RootMismatch`] without ever marking the header
/// invalid (`bpv1-43`).
///
/// BPv1 applies blocks under the same ownership rule as native block sync. It submits
/// only while native sync owns block applies, and each submission is an apply the
/// coordinator counts, so the legacy fallback drain waits for it. When the fallback
/// starts, the adapter stops waiting and transfers the submission to legacy, as native
/// block sync does: a block that waits for its parent would otherwise hold the drain
/// until the submit timeout. In `LegacyBootstrap` and during the legacy fallback the
/// adapter submits nothing. Both cases return [`VerifierVerdict::LocalCapacity`], and
/// legacy delivers the block. BPv1 releases it, or parks it while its parent is not
/// committed (`bpv1-84`).
pub(crate) struct RouterBlockVerifierAdapter<V> {
    router: V,
    coordinator: Arc<SyncCoordinator>,
}

impl<V> RouterBlockVerifierAdapter<V> {
    pub(crate) fn new(router: V, coordinator: Arc<SyncCoordinator>) -> Self {
        Self {
            router,
            coordinator,
        }
    }
}

impl<V> BlockVerifierPort for RouterBlockVerifierAdapter<V>
where
    V: Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + Sync + 'static,
    V::Error: std::fmt::Debug + Send + Sync + 'static,
    V::Future: Send + 'static,
{
    fn submit(&self, block: OwnedBlock, _work_id: WorkId) -> PortFuture<'_, VerifierVerdict> {
        let router = self.router.clone();
        let coordinator = self.coordinator.clone();
        let apply = coordinator
            .queue_apply()
            .and_then(|operation| operation.accept());
        Box::pin(async move {
            let Some(apply) = apply else {
                metrics::counter!("sync.bpv1.submit.not_native").increment(1);
                return VerifierVerdict::LocalCapacity;
            };
            let commit =
                router.oneshot(zakura_consensus::Request::Commit(BlockHandle::Owned(block)));
            let verdict = tokio::select! {
                biased;
                result = tokio::time::timeout(SUBMIT_TIMEOUT, commit) => match result {
                    Ok(Ok(_)) => VerifierVerdict::Valid,
                    Ok(Err(error)) => {
                        classify_body_verification_class(block_verify_error_class(&error))
                    }
                    Err(_elapsed) => {
                        metrics::counter!("sync.bpv1.submit.timeout").increment(1);
                        VerifierVerdict::LocalCapacity
                    }
                },
                () = coordinator.wait_for_legacy_yield() => {
                    metrics::counter!("sync.bpv1.submit.transferred_to_legacy").increment(1);
                    apply.complete(BlockApplyTerminal::TransferredToLegacy);
                    return VerifierVerdict::LocalCapacity;
                }
            };
            apply.complete(match verdict {
                VerifierVerdict::Valid | VerifierVerdict::Duplicate => {
                    BlockApplyTerminal::Committed
                }
                VerifierVerdict::RootMismatch
                | VerifierVerdict::LocalCapacity
                | VerifierVerdict::Invalid => BlockApplyTerminal::Rejected,
            });
            verdict
        })
    }
}

/// Production [`CommittedBlockPort`] over `zakurad`'s read-only state service.
///
/// `ReadRequest::AnyChainBlock` checks the non-finalized chains in memory before the
/// finalized database, so a block that just committed costs no disk read. A failed read
/// answers `None`, as a missing block does.
pub(crate) struct StateCommittedBlocks<S> {
    state: S,
}

impl<S> StateCommittedBlocks<S> {
    pub(crate) fn new(state: S) -> Self {
        Self { state }
    }
}

impl<S> CommittedBlockPort for StateCommittedBlocks<S>
where
    S: Service<zakura_state::ReadRequest, Response = zakura_state::ReadResponse>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Error: std::fmt::Debug,
    S::Future: Send + 'static,
{
    fn committed_block(&self, hash: block::Hash) -> PortFuture<'_, Option<BlockHandle>> {
        let state = self.state.clone();
        Box::pin(async move {
            match state
                .oneshot(zakura_state::ReadRequest::AnyChainBlock(hash.into()))
                .await
            {
                Ok(zakura_state::ReadResponse::Block(block)) => block,
                Ok(_) => None,
                Err(error) => {
                    tracing::debug!(%hash, ?error, "bpv1 committed block read failed");
                    None
                }
            }
        })
    }
}

/// Builds the opt-in BPv1 [`CustomService`] and [`Bpv1Handle`] for `Zakura` / `Dual` startup,
/// sharing `body_claims` with native `block_sync`.
///
/// The service follows `committed_snapshots`: the verified tip moves its
/// propagation window and ends committed fallback blocks, and the best header
/// wakes metadata pending on its parent.
///
/// Sizes `DecodedBlockPool` to cover `decoded_backend_blocks` (non-finalized
/// state retention) plus active BPv1 collection headroom.
/// The service runs as `local_node`, submits assembled blocks to `verifier`, and reads
/// the bodies native block sync committed from `committed_blocks`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_bpv1_service(
    config: &ZakuraBpv1Config,
    have_pacing: HavePacing,
    network: Network,
    local_node: NodeId,
    verifier: Arc<dyn BlockVerifierPort>,
    committed_blocks: Arc<dyn CommittedBlockPort>,
    driver_startup: &mut ZakuraHeaderSyncDriverStartup,
    shutdown: CancellationToken,
) -> (CustomService, Bpv1Handle) {
    let body_claims = SharedBodyClaims::new();
    driver_startup.body_claims = Some(body_claims.clone());
    // Non-finalized state holds committed `OwnedBlock`s until finality (~100 blocks);
    // sizing strictly at `max_concurrent_blocks * 2` (8) would exhaust steady-state
    // admission.
    let pool_blocks = driver_startup
        .decoded_backend_blocks
        .max(128)
        .saturating_add((config.max_concurrent_blocks as u64).max(1) * 2);
    let decoded_pool = DecodedBlockPool::for_block_count(
        NonZeroU64::new(pool_blocks).expect("non-zero decoded block count"),
    );
    let ports = Bpv1Ports {
        header_admission: Arc::new(HeaderChainAdmissionAdapter::new(
            driver_startup.header_chain_port.clone(),
        )),
        body_store: Arc::new(MemoryBodyStore::new(decoded_pool)),
        verifier,
        fallback: Arc::new(body_claims),
        committed_blocks,
        committed_snapshots: driver_startup.committed_snapshots.clone(),
    };
    bpv1_custom_service(
        config.clone(),
        have_pacing,
        Profile::W2,
        network,
        local_node,
        ports,
        shutdown,
    )
}

#[cfg(test)]
mod router_tests;

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tower::service_fn;
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
    use zakura_network::zakura::bpv1::{BlockVerifierPort, VerifierVerdict, WorkId};
    use zakura_node_services::block_memory::{Admission, DecodedBlockPool, OwnedBlock};

    use super::{RouterBlockVerifierAdapter, SyncCoordinator, SUBMIT_TIMEOUT};

    fn owned_genesis() -> OwnedBlock {
        let block: Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis vector deserializes");
        DecodedBlockPool::for_block_count(NonZeroU64::new(4).expect("4 is non-zero"))
            .try_admit(block, Admission::WithinLimit)
            .expect("an empty pool admits one block")
    }

    /// A router that counts its calls and answers each with `answer`.
    fn counting_router<F>(
        answer: F,
    ) -> (
        impl tower::Service<
                zakura_consensus::Request,
                Response = zakura_chain::block::Hash,
                Error = zakura_consensus::BoxError,
                Future = impl Send,
            > + Clone
            + Send
            + Sync
            + 'static,
        Arc<AtomicUsize>,
    )
    where
        F: Fn(
                zakura_chain::block::Hash,
            ) -> futures::future::BoxFuture<
                'static,
                Result<zakura_chain::block::Hash, zakura_consensus::BoxError>,
            > + Clone
            + Send
            + Sync
            + 'static,
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = service_fn({
            let calls = calls.clone();
            move |request: zakura_consensus::Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                answer(request.block().hash())
            }
        });
        (router, calls)
    }

    /// BPv1 submits while native sync owns block applies.
    #[tokio::test]
    async fn submits_while_native_sync_owns_applies() {
        let _init_guard = zakura_test::init();
        let (router, calls) = counting_router(|hash| Box::pin(async move { Ok(hash) }));
        let adapter = RouterBlockVerifierAdapter::new(router, SyncCoordinator::new());

        let verdict = adapter.submit(owned_genesis(), WorkId(1)).await;

        assert_eq!(verdict, VerifierVerdict::Valid);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// In the legacy bootstrap, legacy owns applies, so BPv1 submits nothing and
    /// releases the block.
    #[tokio::test]
    async fn submits_nothing_while_legacy_owns_applies() {
        let _init_guard = zakura_test::init();
        let (router, calls) = counting_router(|hash| Box::pin(async move { Ok(hash) }));
        let adapter =
            RouterBlockVerifierAdapter::new(router, SyncCoordinator::new_legacy_bootstrap());

        let verdict = adapter.submit(owned_genesis(), WorkId(1)).await;

        assert_eq!(verdict, VerifierVerdict::LocalCapacity);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    /// A submission the router never answers ends at the timeout, so the block
    /// falls back and frees its slot.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_submission_times_out_into_fallback() {
        let _init_guard = zakura_test::init();
        let (router, _calls) =
            counting_router(|_hash| Box::pin(std::future::pending::<Result<_, _>>()));
        let adapter = RouterBlockVerifierAdapter::new(router, SyncCoordinator::new());

        let started = tokio::time::Instant::now();
        let verdict = adapter.submit(owned_genesis(), WorkId(1)).await;

        assert_eq!(verdict, VerifierVerdict::LocalCapacity);
        assert_eq!(started.elapsed(), SUBMIT_TIMEOUT);
    }

    /// The legacy fallback transfers a BPv1 submission that waits in the verifier, as it
    /// does native block sync's applies, so the drain ends at once instead of at the
    /// submit timeout. Once legacy owns applies, BPv1 submits nothing.
    #[tokio::test]
    async fn a_waiting_bpv1_submission_transfers_to_the_legacy_fallback() {
        let _init_guard = zakura_test::init();
        let (router, calls) =
            counting_router(|_hash| Box::pin(std::future::pending::<Result<_, _>>()));
        let coordinator = SyncCoordinator::new();
        let adapter = Arc::new(RouterBlockVerifierAdapter::new(router, coordinator.clone()));

        let submission = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.submit(owned_genesis(), WorkId(1)).await }
        });
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let lease = tokio::time::timeout(
            Duration::from_secs(10),
            coordinator.acquire_legacy_fallback(Duration::from_secs(60)),
        )
        .await
        .expect("the drain ends once the waiting submission transfers");
        assert!(lease.is_ok());
        assert_eq!(
            submission.await.expect("the submission task finishes"),
            VerifierVerdict::LocalCapacity
        );
        assert_eq!(
            adapter.submit(owned_genesis(), WorkId(2)).await,
            VerifierVerdict::LocalCapacity
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
