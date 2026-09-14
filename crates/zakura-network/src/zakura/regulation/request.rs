//! Admission and ownership for requests that produce a finite response.
//!
//! Policies supply the codec and response bound. Sequential serving tasks wait
//! for admission before dispatching work; this layer owns capacity and lifetimes.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio_util::sync::CancellationToken;

use super::{slots::WeakSlotBudget, SlotBudget, SlotPermit};
use crate::zakura::transport::{Frame, FrameGuard};
use crate::zakura::ZakuraPeerId;

/// Message-specific rules used by request admission.
///
/// Decode must use the production codec and validate the request before returning
/// it. The response bound must cover every frame the handler may produce. This
/// interface is for finite requests, not announcements or subscription lifetimes.
pub(crate) trait RequestPolicy {
    type Request;
    type Error;

    fn decode(&self, frame: Frame) -> Result<Self::Request, Self::Error>;
    fn response_cap(&self, request: &Self::Request) -> u64;
}

/// Node work capacity shared by the sessions of one configured request policy.
#[derive(Clone, Debug)]
pub(crate) struct RequestAdmission<P> {
    policy: P,
    node: SlotBudget,
    peer_capacity: usize,
    peers: Arc<Mutex<HashMap<ZakuraPeerId, WeakSlotBudget>>>,
}

impl<P: RequestPolicy + Clone> RequestAdmission<P> {
    pub(crate) fn new(policy: P, node: SlotBudget, peer_capacity: usize) -> Self {
        // Validate before a session is created rather than panicking on ingress.
        SlotBudget::new(peer_capacity).expect("request peer capacity is validated");
        Self {
            policy,
            node,
            peer_capacity,
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Reconnects share the authenticated peer's capacity with its old work.
    pub(crate) fn session(&self, peer: &ZakuraPeerId) -> RequestSession<P> {
        let mut peers = self
            .peers
            .lock()
            .expect("peer budget registry is not poisoned");
        // Permits keep their semaphore alive even after the old session closes.
        // Remove expired identities on each connection so churn cannot grow the map.
        peers.retain(|_, budget| budget.is_alive());
        let budget = peers
            .get(peer)
            .and_then(WeakSlotBudget::upgrade)
            .unwrap_or_else(|| {
                let budget = SlotBudget::new(self.peer_capacity)
                    .expect("request peer capacity was validated at construction");
                peers.insert(peer.clone(), budget.downgrade());
                budget
            });
        RequestSession {
            policy: self.policy.clone(),
            node: self.node.clone(),
            peer: budget,
        }
    }

    #[cfg(test)]
    pub(crate) fn reserved_by_peers(&self) -> usize {
        self.peers
            .lock()
            .unwrap()
            .values()
            .filter_map(WeakSlotBudget::upgrade)
            .map(|budget| budget.reserved())
            .sum()
    }
}

/// One session's request policy, sharing capacity with the peer's other sessions.
#[derive(Clone, Debug)]
pub(crate) struct RequestSession<P> {
    policy: P,
    node: SlotBudget,
    peer: SlotBudget,
}

impl<P: RequestPolicy> RequestSession<P> {
    pub(crate) fn decode(&self, frame: Frame) -> Result<P::Request, P::Error> {
        self.policy.decode(frame)
    }

    /// One sequential serving task waits for its peer before entering the node
    /// queue. A previous response cannot make this peer hold extra node slots.
    /// Dropping this future removes its FIFO waiter and releases a partial claim.
    pub(crate) async fn admit(&self, request: &P::Request) -> WorkAttempt {
        let peer = reserve_response_slot(&self.peer, WorkBound::Peer).await;
        let node = reserve_response_slot(&self.node, WorkBound::Node).await;
        WorkAttempt {
            resources: Arc::new(WorkResources {
                _peer: peer,
                _node: node,
            }),
            response_cap: self.policy.response_cap(request),
        }
    }

    #[cfg(test)]
    pub(crate) fn peer_budget(&self) -> &SlotBudget {
        &self.peer
    }
}

/// Scope of the work capacity that delayed a request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkBound {
    Peer,
    Node,
}

impl WorkBound {
    /// Stable resource names used by delay metrics and traces.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Peer => "peer_active",
            Self::Node => "node_active",
        }
    }
}

async fn reserve_response_slot(budget: &SlotBudget, bound: WorkBound) -> SlotPermit {
    if let Some(permit) = budget.try_reserve() {
        return permit;
    }
    metrics::counter!("sync.block.serving.delayed", "bound" => bound.label()).increment(1);
    budget.reserve().await
}

/// Provisional work ownership. Dropping it rolls back admission.
#[derive(Debug)]
#[must_use = "dropping an admission attempt returns its capacity"]
pub(crate) struct WorkAttempt {
    resources: Arc<WorkResources>,
    response_cap: u64,
}

impl WorkAttempt {
    pub(crate) fn commit(self) -> ResponsePermit {
        ResponsePermit {
            resources: self.resources,
            execution: Arc::new(Execution::default()),
            response_cap: self.response_cap,
            queued_bytes: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct WorkResources {
    _peer: SlotPermit,
    _node: SlotPermit,
}

/// The handler's response ownership.
///
/// Dropping it prevents unclaimed work from starting and signals cancellation
/// to already running work. This does not interrupt an operation in progress or
/// release capacity held by work leases and queued frame guards.
#[derive(Debug)]
#[must_use = "retain the response permit until settlement or cancellation"]
pub(crate) struct ResponsePermit {
    resources: Arc<WorkResources>,
    execution: Arc<Execution>,
    response_cap: u64,
    queued_bytes: u64,
}

impl ResponsePermit {
    pub(crate) fn can_queue_frame(&self, bytes: u64) -> bool {
        bytes <= self.response_cap.saturating_sub(self.queued_bytes)
    }

    /// Call only once queue capacity is reserved. The guard lives through writing.
    pub(crate) fn frame_guard(&mut self, bytes: u64) -> FrameGuard {
        assert!(
            self.can_queue_frame(bytes),
            "encoded response fits its declared cap"
        );
        self.queued_bytes += bytes;
        FrameGuard::new(self.resources.clone())
    }

    pub(crate) fn work_lease(&self) -> WorkLease {
        WorkLease {
            _resources: self.resources.clone(),
            execution: self.execution.clone(),
        }
    }
}

impl Drop for ResponsePermit {
    fn drop(&mut self) {
        self.execution.cancel();
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
enum ExecutionState {
    #[default]
    Queued,
    Started,
    Cancelled,
}

#[derive(Debug, Default)]
struct Execution {
    state: Mutex<ExecutionState>,
    cancelled: CancellationToken,
}

impl Execution {
    fn try_start(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("request execution state is not poisoned");
        if *state != ExecutionState::Queued {
            return false;
        }
        *state = ExecutionState::Started;
        true
    }

    fn cancel(&self) {
        *self
            .state
            .lock()
            .expect("request execution state is not poisoned") = ExecutionState::Cancelled;
        self.cancelled.cancel();
    }
}

/// Capacity shared by an execution and its returned result.
///
/// Clones share one execution claim. Cancellation prevents an unclaimed start
/// and lets running work skip further steps at its next cancellation check.
/// An operation and its returned result must retain this lease until they are
/// released, even after cancellation.
#[derive(Clone, Debug)]
pub(crate) struct WorkLease {
    _resources: Arc<WorkResources>,
    execution: Arc<Execution>,
}

impl WorkLease {
    pub(crate) fn try_start(&self) -> bool {
        self.execution.try_start()
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        self.execution.cancelled.is_cancelled()
    }
}

#[cfg(test)]
mod tests;
