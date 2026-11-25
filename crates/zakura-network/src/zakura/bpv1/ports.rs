//! BPv1 node-service ports and production/test/inert adapters.
//!
//! Isolates the pure BPv1 machines from concrete node services behind five
//! narrow traits:
//! - [`HeaderAdmissionPort`]: exact-header admission into headerchain.
//! - [`BodyStorePort`]: assembled [`OwnedBlock`]s (`DecodedBlockPool`).
//! - [`BlockVerifierPort`]: shared consensus verifier router mapping `PayloadMismatch` -> `RootMismatch` (`decision-8`).
//! - [`BlockFallbackPort`]: shared v2 [`SharedBodyClaims`] set with native block sync.

use std::{future::Future, num::NonZeroU64, pin::Pin, sync::Arc};

use zakura_chain::block::{self, Block, Header};
use zakura_header_chain::{BodyVerificationClass, SourceId};
use zakura_node_services::{
    block_memory::{Admission, AdmissionFailure, DecodedBlockPool, OwnedBlock},
    body_claims::{BodyClaimRelease, SharedBodyClaims},
    header_chain::{ExactHeaderAdmission, Port as HeaderChainPort, PortError},
};
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    blocks::{ClaimRelease, HeaderAdmissionResult, VerifierVerdict, WorkId},
    unknown_parent::TargetBound,
};

/// Boxed future returned by async BPv1 port operations.
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ============================================================================
// 1. HeaderAdmissionPort
// ============================================================================

/// Exact-header admission port into headerchain.
pub trait HeaderAdmissionPort: Send + Sync + 'static {
    /// Admit one exact `header` from `source_peer` under `work_id`.
    fn admit(
        &self,
        header: Arc<Header>,
        source_peer: NodeId,
        session_gen: u64,
        work_id: WorkId,
    ) -> PortFuture<'_, HeaderAdmissionResult>;

    /// The target bound above headerchain's best header, or `None` when
    /// headerchain holds no context for it. The default holds none.
    fn target_bound(&self) -> PortFuture<'_, Option<TargetBound>> {
        Box::pin(async { None })
    }
}

/// Production header admission adapter over `zakura_node_services::header_chain::Port`.
///
/// `BlockTable.admitting` admits each block once, and headerchain reports a header
/// it already retains as `already_known` (`decision-2`).
#[derive(Clone, Debug)]
pub struct HeaderChainAdmissionAdapter {
    port: Arc<dyn HeaderChainPort>,
}

impl HeaderChainAdmissionAdapter {
    /// Wraps a durable/engine header-chain [`HeaderChainPort`].
    pub fn new(port: Arc<dyn HeaderChainPort>) -> Self {
        Self { port }
    }
}

impl HeaderAdmissionPort for HeaderChainAdmissionAdapter {
    fn admit(
        &self,
        header: Arc<Header>,
        source_peer: NodeId,
        session_gen: u64,
        work_id: WorkId,
    ) -> PortFuture<'_, HeaderAdmissionResult> {
        let port = self.port.clone();
        Box::pin(async move {
            use zakura_chain::serialization::ZcashSerialize;

            // Widening header byte length (<= ~1.5 KiB) to u64 is lossless.
            let header_len = header.zcash_serialized_size() as u64;
            let source = SourceId::from_digest(*source_peer.as_bytes());
            let request_id =
                NonZeroU64::new(work_id.0.max(1)).expect("work_id.max(1) is always non-zero");
            let admission = port
                .admit_exact_header(header, source, session_gen, request_id)
                .await;
            if let Ok(ExactHeaderAdmission::Admitted {
                already_known: true,
                ..
            }) = admission
            {
                metrics::counter!("zakura.p2p.bpv1.header.duplicate_bytes").increment(header_len);
            }
            admission_result(admission)
        })
    }

    /// Reads the validation lease for a child of the best header that the
    /// latest committed snapshot names.
    fn target_bound(&self) -> PortFuture<'_, Option<TargetBound>> {
        let best = self
            .port
            .selected_chain()
            .borrow()
            .as_ref()
            .map(|snapshot| snapshot.frontiers.header_best);
        let port = self.port.clone();
        Box::pin(async move {
            let lease = port.header_validation_lease(best?.hash).await.ok()??;
            TargetBound::from_lease(&lease)
        })
    }
}

/// Maps headerchain's exact-header admission onto [`HeaderAdmissionResult`].
pub(super) fn admission_result(
    admission: Result<ExactHeaderAdmission, PortError>,
) -> HeaderAdmissionResult {
    match admission {
        Ok(ExactHeaderAdmission::Admitted { height, .. }) => {
            HeaderAdmissionResult::Admitted { height: height.0 }
        }
        Ok(ExactHeaderAdmission::MissingParent { parent_hash }) => {
            HeaderAdmissionResult::MissingParent { parent_hash }
        }
        Ok(ExactHeaderAdmission::PeerInvalid) => HeaderAdmissionResult::PeerInvalid,
        Ok(ExactHeaderAdmission::LocalError) | Err(_) => HeaderAdmissionResult::LocalError,
    }
}

// ============================================================================
// 2. BodyStorePort
// ============================================================================

/// Assembled body storage port. `BlockTable`
/// charges part buffers itself, before proof.
pub trait BodyStorePort: Send + Sync + 'static {
    /// Admits an assembled [`Block`] into [`DecodedBlockPool`], producing one
    /// [`OwnedBlock`] whose charge follows the handle through verification and
    /// state until the final holder drops (`dual-4`).
    fn admit_assembled(&self, block: Block) -> Result<OwnedBlock, AdmissionFailure>;
}

/// Production [`BodyStorePort`] backed by [`DecodedBlockPool`].
#[derive(Clone, Debug)]
pub struct MemoryBodyStore {
    decoded_pool: DecodedBlockPool,
}

impl MemoryBodyStore {
    /// Creates a production body store wrapping `decoded_pool`.
    pub fn new(decoded_pool: DecodedBlockPool) -> Self {
        Self { decoded_pool }
    }
}

impl BodyStorePort for MemoryBodyStore {
    fn admit_assembled(&self, block: Block) -> Result<OwnedBlock, AdmissionFailure> {
        self.decoded_pool
            .try_admit(block, Admission::WithinLimit)
            .map_err(|rejected| rejected.reason)
    }
}

// ============================================================================
// 3. BlockVerifierPort (`decision-8`)
// ============================================================================

/// Maps `zakura_header_chain::BodyVerificationClass` (produced by
/// `VerifyBlockError::body_verification_class()`) into BPv1's four verdicts
/// (`decision-8`):
///
/// - `PayloadMismatch(_)` (bad tx Merkle root OR bad ZIP 244 auth-data root)
///   -> [`VerifierVerdict::RootMismatch`] (triggers fallback & retires bad encoding; NEVER marks header invalid).
/// - `Duplicate` -> [`VerifierVerdict::Duplicate`].
/// - `ConsensusInvalid(rule)` -> [`VerifierVerdict::Invalid`].
/// - `Retryable(_)` -> [`VerifierVerdict::LocalCapacity`] (hands off to native block sync
///   fallback without faulting the encoding or marking a valid header invalid).
pub fn classify_body_verification_class(class: BodyVerificationClass) -> VerifierVerdict {
    match class {
        BodyVerificationClass::PayloadMismatch(_) => VerifierVerdict::RootMismatch,
        BodyVerificationClass::Duplicate => VerifierVerdict::Duplicate,
        BodyVerificationClass::ConsensusInvalid(_) => VerifierVerdict::Invalid,
        BodyVerificationClass::Retryable(_) => VerifierVerdict::LocalCapacity,
    }
}

/// Consensus block verification & commit port.
pub trait BlockVerifierPort: Send + Sync + 'static {
    /// Submits an assembled [`OwnedBlock`] to the shared consensus verifier router.
    fn submit(&self, block: OwnedBlock, work_id: WorkId) -> PortFuture<'_, VerifierVerdict>;
}

// ============================================================================
// 4. BlockFallbackPort
// ============================================================================

/// Shared v2 body-claim port coordinating BPv1 and native block sync.
pub trait BlockFallbackPort: Send + Sync + 'static {
    /// Claims `hash` until `deadline` so native block sync skips fetching it.
    fn claim(&self, hash: block::Hash, deadline: Instant) -> bool;

    /// Releases `hash` with `reason`. `ClaimRelease::Fallback` and
    /// `ClaimRelease::Repair` wake native block sync.
    fn release(&self, hash: block::Hash, reason: ClaimRelease) -> bool;
}

/// The body claims BPv1 shares with native `block_sync`.
impl BlockFallbackPort for SharedBodyClaims {
    fn claim(&self, hash: block::Hash, deadline: Instant) -> bool {
        self.claim_at(hash, deadline, Instant::now())
    }

    fn release(&self, hash: block::Hash, reason: ClaimRelease) -> bool {
        let mapped = match reason {
            ClaimRelease::Committed => BodyClaimRelease::Committed,
            // A repair-round release wakes native block sync to race BPv1
            // (decision-22), as a fallback does.
            ClaimRelease::Fallback | ClaimRelease::Repair => BodyClaimRelease::Fallback,
            ClaimRelease::Retired => BodyClaimRelease::Retired,
        };
        SharedBodyClaims::release(self, hash, mapped)
    }
}
