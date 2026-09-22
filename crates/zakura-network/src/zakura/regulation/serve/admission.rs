//! Serving capacity: node execution slots and per-peer budgets.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio_util::sync::CancellationToken;

use crate::zakura::{
    regulation::{
        slots::{WeakOutputByteBudget, WeakSlotBudget},
        OutputByteBudget, OutputGrant, SlotBudget,
    },
    ZakuraPeerId,
};

use super::lease::ExecutionSlots;

/// Per-peer serving limits.
#[derive(Copy, Clone, Debug)]
pub(crate) struct PeerServeLimits {
    /// Requests from one peer that may execute at once.
    pub(crate) execution_slots: usize,
    /// Response bytes from one peer's requests that may wait to be written.
    pub(crate) output_bytes: u32,
}

/// Serving capacity shared by every session of one service.
///
/// A peer's budgets live as long as any session or unfinished response holds
/// them. A reconnect finds and shares the live budgets instead of getting new
/// ones.
#[derive(Clone, Debug)]
pub(crate) struct ServeCapacity {
    node: SlotBudget,
    limits: PeerServeLimits,
    peers: Arc<Mutex<HashMap<ZakuraPeerId, WeakPeerServeBudgets>>>,
}

impl ServeCapacity {
    /// Capacity with `node_slots` node-wide execution slots.
    pub(crate) fn new(node_slots: usize, limits: PeerServeLimits) -> Self {
        Self {
            node: SlotBudget::new(node_slots).expect("serving node slots are a small constant"),
            limits,
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn node(&self) -> SlotBudget {
        self.node.clone()
    }

    /// Node execution slots in use.
    #[cfg(test)]
    pub(crate) fn node_slots_held(&self) -> usize {
        self.node.reserved()
    }

    /// `peer`'s execution slots and output bytes in use.
    #[cfg(test)]
    pub(crate) fn peer_held(&self, peer: &ZakuraPeerId) -> (usize, usize) {
        let budgets = self.peer(peer);
        (budgets.execution.reserved(), budgets.output.granted())
    }

    /// The live budgets for `peer`, created if none are live.
    pub(super) fn peer(&self, peer: &ZakuraPeerId) -> PeerServeBudgets {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Drop expired identities on each lookup so churn cannot grow the map.
        peers.retain(|_, budgets| budgets.execution.is_alive());
        if let Some(budgets) = peers.get(peer).and_then(WeakPeerServeBudgets::upgrade) {
            return budgets;
        }
        let budgets = PeerServeBudgets {
            execution: SlotBudget::new(self.limits.execution_slots)
                .expect("serving peer slots are a small constant"),
            output: OutputByteBudget::new(self.limits.output_bytes),
        };
        peers.insert(peer.clone(), budgets.downgrade());
        budgets
    }
}

/// One peer's execution slots and output bytes.
#[derive(Clone, Debug)]
pub(super) struct PeerServeBudgets {
    pub(super) execution: SlotBudget,
    pub(super) output: OutputByteBudget,
}

impl PeerServeBudgets {
    fn downgrade(&self) -> WeakPeerServeBudgets {
        WeakPeerServeBudgets {
            execution: self.execution.downgrade(),
            output: self.output.downgrade(),
        }
    }
}

#[derive(Debug)]
struct WeakPeerServeBudgets {
    execution: WeakSlotBudget,
    output: WeakOutputByteBudget,
}

impl WeakPeerServeBudgets {
    fn upgrade(&self) -> Option<PeerServeBudgets> {
        Some(PeerServeBudgets {
            execution: self.execution.upgrade()?,
            output: self.output.upgrade()?,
        })
    }
}

/// One admission attempt for one request.
pub(super) struct ServeAdmission<'a> {
    pub(super) node: &'a SlotBudget,
    pub(super) peer: &'a PeerServeBudgets,
    pub(super) cancel: &'a CancellationToken,
}

impl ServeAdmission<'_> {
    /// Acquire, in order, a peer slot, an output grant, and a node slot.
    ///
    /// Returns `None` if the session is cancelled first; nothing stays held.
    pub(super) async fn admit(self, response_cap: u32) -> Option<(ExecutionSlots, OutputGrant)> {
        // A grant above the whole budget would never complete. Such a response
        // takes the whole budget; its sink still enforces the declared cap.
        let response_cap = response_cap.min(self.peer.output.capacity());
        let peer = self
            .wait("peer_execution", self.peer.execution.reserve())
            .await?;
        let output = self
            .wait("peer_output", self.peer.output.grant(response_cap))
            .await?;
        let node = self.wait("node_execution", self.node.reserve()).await?;
        Some((ExecutionSlots::new(peer, node), output))
    }

    async fn wait<T>(
        &self,
        bound: &'static str,
        acquire: impl std::future::Future<Output = T>,
    ) -> Option<T> {
        tokio::pin!(acquire);
        if let Some(ready) = futures::FutureExt::now_or_never(&mut acquire) {
            return Some(ready);
        }
        metrics::counter!("zakura.p2p.serve.delayed", "bound" => bound).increment(1);
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => None,
            acquired = acquire => Some(acquired),
        }
    }
}
