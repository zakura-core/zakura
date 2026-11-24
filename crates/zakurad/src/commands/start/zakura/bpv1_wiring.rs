//! Production BPv1 node wiring for `zakurad`.
//!
//! Attaches BPv1 as an opt-in v2 front on `Zakura` and `Dual` stacks only
//! (strictly absent in `Legacy`, zero changes under `peer/`, `peer_set/`,
//! `inbound/`, or `ChainSync` per `dual-1`).

use std::{num::NonZeroU64, sync::Arc};

use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use zakura_chain::{block, parameters::Network};
use zakura_node_services::{
    block_memory::{BlockHandle, DecodedBlockPool, OwnedBlock},
    body_claims::SharedBodyClaims,
};

use zakura_network::zakura::{
    bpv1::{
        classify_body_verification_class, BlockVerifierPort, Bpv1Ports, Bpv1Startup, HavePacing,
        HeaderChainAdmissionAdapter, MemoryBodyStore, NodeId, PortFuture, VerifierVerdict, WorkId,
        ZakuraBpv1Config, NODE_PROFILE,
    },
    ZakuraHeaderSyncDriverStartup,
};

use super::block_verify_error_class;

/// Production [`BlockVerifierPort`] wrapping `zakurad`'s shared `block_verifier_router`
/// (`dual-2`, `dual-4`, `decision-8`).
///
/// Maps `BodyVerificationClass::PayloadMismatch` (bad tx Merkle root OR bad ZIP 244
/// auth-data root) to [`VerifierVerdict::RootMismatch`] without ever marking the header
/// invalid (`bpv1-43`).
pub(crate) struct RouterBlockVerifierAdapter<V> {
    router: V,
}

impl<V> RouterBlockVerifierAdapter<V> {
    pub(crate) fn new(router: V) -> Self {
        Self { router }
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
        Box::pin(async move {
            match router
                .oneshot(zakura_consensus::Request::Commit(BlockHandle::Owned(block)))
                .await
            {
                Ok(_) => VerifierVerdict::Valid,
                Err(error) => {
                    let class = block_verify_error_class(&error);
                    classify_body_verification_class(class)
                }
            }
        })
    }
}

/// Builds what the endpoint needs to start opt-in BPv1 on `Zakura` / `Dual` stacks, sharing
/// `body_claims` with native `block_sync`. The endpoint starts it once it has built the memory
/// pool BPv1 leases from.
///
/// The service follows `committed_snapshots`: the verified tip moves its
/// propagation window and ends committed fallback blocks, and the best header
/// wakes metadata pending on its parent.
///
/// Sizes `DecodedBlockPool` to cover `decoded_backend_blocks` (non-finalized
/// state retention) plus active BPv1 collection headroom.
/// The service runs as `local_node` and submits assembled blocks to `verifier`.
pub(crate) fn bpv1_startup(
    config: &ZakuraBpv1Config,
    have_pacing: HavePacing,
    network: Network,
    local_node: NodeId,
    verifier: Arc<dyn BlockVerifierPort>,
    driver_startup: &mut ZakuraHeaderSyncDriverStartup,
    shutdown: CancellationToken,
) -> Bpv1Startup {
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
        committed_snapshots: driver_startup.committed_snapshots.clone(),
    };
    Bpv1Startup {
        config: config.clone(),
        have_pacing,
        profile: NODE_PROFILE,
        network,
        local_node,
        ports,
        shutdown,
    }
}
