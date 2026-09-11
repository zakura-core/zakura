//! Resource admission for serving inbound `GetBlocks` requests.
//!
//! This module turns the generic regulation primitives into one message policy.
//! Each session holds one decoded request while waiting for capacity.
//! The separate data reader continues processing downloads during this wait.
//! Admission acquires a response producer before the state query starts. The query,
//! result, and queued frames share that producer until the last owner drops. A blocked
//! transport writer therefore prevents another query for the same peer, including after reconnect.

use std::sync::Arc;

use super::{config::*, *};
use crate::zakura::{
    regulation::{
        RequestAdmission, RequestSession, ResponsePermit, SlotBudget, WorkAttempt, WorkLease,
    },
    transport::FrameGuard,
};

mod observations;
mod policy;
use policy::GetBlocksPolicy;

/// Header-level size limits from the implemented message policies.
pub(super) fn message_payload_limits() -> &'static [(u16, usize)] {
    GetBlocksPolicy::PAYLOAD_LIMITS
}

/// One decoded request held at the admission boundary until work is available.
#[derive(Debug)]
pub(super) struct GetBlocksRequest {
    pub(super) start_height: block::Height,
    pub(super) count: u32,
}

/// Validate that every legal request can eventually fit every configured bound.
pub(super) fn validate_config(config: &ZakuraBlockSyncConfig) -> Result<(), &'static str> {
    if u64::from(config.max_response_bytes) < block::MAX_BLOCK_BYTES {
        return Err("max_response_bytes must cover one maximum-size block");
    }
    let regulation = &config.get_blocks_regulation;
    if regulation.node_active_requests == 0 {
        return Err("get_blocks_regulation.node_active_requests must be greater than zero");
    }
    if regulation.node_active_requests > tokio::sync::Semaphore::MAX_PERMITS {
        return Err("get_blocks_regulation.node_active_requests exceeds Tokio's semaphore limit");
    }

    Ok(())
}

/// Node-owned GetBlocks resources shared by every peer routine.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingRegulator {
    inner: Arc<RegulatorInner>,
}

#[derive(Debug)]
struct RegulatorInner {
    admission: RequestAdmission<GetBlocksPolicy>,
    metrics: observations::ServingMetrics,
    #[cfg(test)]
    node_active: SlotBudget,
}

impl GetBlocksServingRegulator {
    /// Create the GetBlocks node policy from validated block-sync configuration.
    pub(super) fn new(config: ZakuraBlockSyncConfig) -> Self {
        debug_assert!(validate_config(&config).is_ok());
        let regulation = &config.get_blocks_regulation;
        let node_active = SlotBudget::new(regulation.node_active_requests)
            .expect("GetBlocks configuration validates the active-request capacity");
        Self {
            inner: Arc::new(RegulatorInner {
                metrics: observations::ServingMetrics::default(),
                admission: RequestAdmission::new(
                    GetBlocksPolicy::new(&config),
                    node_active.clone(),
                    GetBlocksPolicy::PEER_PRODUCERS,
                ),
                #[cfg(test)]
                node_active,
            }),
        }
    }

    /// Create one session policy within the node admission bounds.
    pub(super) fn session(&self, peer: ZakuraPeerId) -> GetBlocksServingSession {
        let work = self.inner.admission.session(&peer);

        GetBlocksServingSession {
            work,
            metrics: self.inner.metrics.clone(),
        }
    }

    pub(super) fn publish_metrics(&self) {
        self.inner.metrics.publish();
    }

    #[cfg(test)]
    pub(super) fn locks_for_test(&self) -> zakura_test::resources::LockSnapshot {
        self.inner.admission.session_lock_probe.snapshot()
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> ServingRegulationSnapshot {
        ServingRegulationSnapshot {
            node_active: self.inner.node_active.reserved(),
            peer_active: self.inner.admission.reserved_by_peers(),
        }
    }
}

/// Per-session entry point for decoding and work admission.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksServingSession {
    metrics: observations::ServingMetrics,
    work: RequestSession<GetBlocksPolicy>,
}

impl GetBlocksServingSession {
    /// Wait in peer-then-node order. The enclosing session cancels this wait.
    pub(super) async fn admit_request(&self, request: &GetBlocksRequest) -> GetBlocksServingPermit {
        let _waiting = self.metrics.waiting();
        AdmissionAttempt {
            metrics: self.metrics.clone(),
            work: self.work.admit(request).await,
        }
        .commit()
    }

    /// Apply the declared codec before admitting an inbound request.
    pub(super) fn decode_request(
        &self,
        frame: Frame,
    ) -> Result<GetBlocksRequest, BlockSyncWireError> {
        self.work.decode(frame)
    }
}

/// Provisional ownership of every resource needed before state work starts.
#[derive(Debug)]
#[must_use = "dropping a GetBlocks admission attempt rolls back every reservation"]
pub(super) struct AdmissionAttempt {
    metrics: observations::ServingMetrics,
    work: WorkAttempt,
}

impl AdmissionAttempt {
    /// Transfer admitted resources to the sequential response producer.
    pub(super) fn commit(self) -> GetBlocksServingPermit {
        metrics::counter!("sync.block.serving.admitted").increment(1);
        GetBlocksServingPermit {
            response: self.work.commit(),
            observation: self.metrics.active(),
            #[cfg(test)]
            encode_probe: None,
        }
    }
}

/// Committed ownership retained while producing and writing a response.
#[derive(Debug)]
#[must_use = "the response producer retains this permit until its ending is queued"]
pub(super) struct GetBlocksServingPermit {
    response: ResponsePermit,
    observation: Arc<observations::Active>,
    #[cfg(test)]
    pub(super) encode_probe: Option<Arc<zakura_test::execution::ExecutionProbe>>,
}

impl GetBlocksServingPermit {
    pub(super) fn can_queue_frame(&self, bytes: u64) -> bool {
        self.response.can_queue_frame(bytes)
    }

    pub(super) fn frame_guard(&mut self, bytes: u64) -> FrameGuard {
        FrameGuard::new(Arc::new((
            self.response.frame_guard(bytes),
            self.observation.clone(),
        )))
    }

    pub(super) fn work_lease(&self) -> BlockRangeReadLease {
        BlockRangeReadLease {
            work: self.response.work_lease(),
            _observation: self.observation.clone(),
        }
    }
}

/// Capacity retained by a serving query and its completed response.
///
/// The storage adapter claims execution once and moves this lease into the
/// actual blocking job and its returned result. Clones retain the same capacity
/// and cannot start another read. Dropping the response producer cancels delivery;
/// capacity returns after the last worker, result, and frame owner drops.
#[derive(Clone, Debug)]
pub struct BlockRangeReadLease {
    work: WorkLease,
    _observation: Arc<observations::Active>,
}

impl BlockRangeReadLease {
    /// Claim the only execution, serialized against producer cancellation.
    ///
    /// If closure wins, no read starts. If the claim wins, the worker retains
    /// capacity until the read finishes, even if delivery is then cancelled.
    pub fn try_start(&self) -> bool {
        self.work.try_start()
    }

    /// Whether the request no longer has a live delivery owner.
    pub fn is_cancelled(&self) -> bool {
        self.work.is_cancelled()
    }
}

#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ServingRegulationSnapshot {
    pub(super) node_active: usize,
    pub(super) peer_active: usize,
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;
