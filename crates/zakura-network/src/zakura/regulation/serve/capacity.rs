//! Serving capacity: node budgets, per-peer budgets, and serving metrics.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, PoisonError,
    },
};

use thiserror::Error;

use crate::zakura::{
    regulation::{
        slots::{WeakOutputByteBudget, WeakSlotBudget},
        OutputByteBudget, SlotBudget,
    },
    MessageRole, MessageRule, ZakuraPeerId,
};

/// Serving limits from configuration.
///
/// [`sizing`](crate::zakura::regulation::sizing) derives each default from the
/// throughput target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServeLimits {
    /// `produce` steps that may run at once across the node.
    pub(crate) node_execution: usize,
    /// `produce` steps that may run at once for one peer.
    pub(crate) peer_execution: usize,
    /// Unsent response bytes one peer may hold.
    pub(crate) peer_output_bytes: u64,
    /// Unsent response bytes all peers may hold together.
    pub(crate) node_output_bytes: u64,
}

/// A serving configuration that cannot work.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ServeConfigError {
    /// The row is not a request row.
    #[error("message type {message_type} is not a request row")]
    NotARequest {
        /// The row's message type.
        message_type: u16,
    },
    /// A limit is zero or too large to represent.
    #[error("serving limit {limit} must be positive and representable")]
    Limit {
        /// The limit's name.
        limit: &'static str,
    },
}

/// Metric labels and test counters for one request row.
#[derive(Debug)]
pub(super) struct ServeMetrics {
    service: &'static str,
    message_type: String,
    active: AtomicUsize,
    waiting: AtomicUsize,
    over_limit: AtomicU64,
}

impl ServeMetrics {
    pub(super) fn delayed(&self, bound: &'static str) {
        metrics::counter!(
            "zakura.p2p.serve.delayed",
            "service" => self.service,
            "message_type" => self.message_type.clone(),
            "bound" => bound,
        )
        .increment(1);
    }

    pub(super) fn admitted(&self) {
        metrics::counter!(
            "zakura.p2p.serve.admitted",
            "service" => self.service,
            "message_type" => self.message_type.clone(),
        )
        .increment(1);
    }

    /// Count a request above the advertised limit but within the margin.
    pub(super) fn over_limit(&self, open: u32, limit: u32) {
        self.over_limit.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "zakura.p2p.serve.over_limit",
            "service" => self.service,
            "message_type" => self.message_type.clone(),
        )
        .increment(1);
        tracing::debug!(
            service = self.service,
            message_type = %self.message_type,
            open,
            limit,
            "peer has more open requests than advertised; serving them within the margin"
        );
    }

    pub(super) fn ended(&self, outcome: &'static str) {
        metrics::counter!(
            "zakura.p2p.serve.ended",
            "service" => self.service,
            "message_type" => self.message_type.clone(),
            "outcome" => outcome,
        )
        .increment(1);
    }

    /// Count one job as waiting for capacity until the guard drops.
    pub(super) fn waiting(self: &Arc<Self>) -> Tally {
        Tally::new(self.clone(), Gauge::Waiting)
    }

    /// Count one job as running `produce` until the guard drops.
    pub(super) fn active(self: &Arc<Self>) -> Tally {
        Tally::new(self.clone(), Gauge::Active)
    }

    fn set(&self, gauge: Gauge, value: usize) {
        let name = match gauge {
            Gauge::Waiting => "zakura.p2p.serve.waiting",
            Gauge::Active => "zakura.p2p.serve.active",
        };
        // Gauges are small counts, exactly representable as f64.
        metrics::gauge!(
            name,
            "service" => self.service,
            "message_type" => self.message_type.clone(),
        )
        .set(value as f64);
    }

    fn counter(&self, gauge: Gauge) -> &AtomicUsize {
        match gauge {
            Gauge::Waiting => &self.waiting,
            Gauge::Active => &self.active,
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum Gauge {
    Waiting,
    Active,
}

/// One unit of a serving gauge, returned when dropped.
#[derive(Debug)]
pub(super) struct Tally {
    metrics: Arc<ServeMetrics>,
    gauge: Gauge,
}

impl Tally {
    fn new(metrics: Arc<ServeMetrics>, gauge: Gauge) -> Self {
        let value = metrics.counter(gauge).fetch_add(1, Ordering::Relaxed) + 1;
        metrics.set(gauge, value);
        Self { metrics, gauge }
    }
}

impl Drop for Tally {
    fn drop(&mut self) {
        let value = self
            .metrics
            .counter(self.gauge)
            .fetch_sub(1, Ordering::Relaxed)
            - 1;
        self.metrics.set(self.gauge, value);
    }
}

/// Serving capacity for one request row, shared by every session of the node.
///
/// A peer's budgets live as long as any session or unfinished response holds
/// them. A reconnect finds and shares the live budgets instead of getting new
/// ones, so reconnecting never adds capacity.
#[derive(Clone, Debug)]
pub(crate) struct ServeCapacity {
    pub(super) request: &'static MessageRule,
    pub(super) max_in_flight: u32,
    limits: ServeLimits,
    pub(super) node_execution: SlotBudget,
    pub(super) node_output: OutputByteBudget,
    peers: Arc<Mutex<HashMap<ZakuraPeerId, WeakPeerBudgets>>>,
    pub(super) metrics: Arc<ServeMetrics>,
}

impl ServeCapacity {
    /// Capacity for the request `row` of `service`.
    pub(crate) fn new(
        service: &'static str,
        request: &'static MessageRule,
        limits: ServeLimits,
    ) -> Result<Self, ServeConfigError> {
        let MessageRole::Request { max_in_flight, .. } = request.role else {
            return Err(ServeConfigError::NotARequest {
                message_type: request.message_type,
            });
        };
        let limit = |limit| ServeConfigError::Limit { limit };
        SlotBudget::new(limits.peer_execution).map_err(|_| limit("peer_execution"))?;
        OutputByteBudget::new(limits.peer_output_bytes).map_err(|_| limit("peer_output_bytes"))?;
        Ok(Self {
            request,
            max_in_flight,
            limits,
            node_execution: SlotBudget::new(limits.node_execution)
                .map_err(|_| limit("node_execution"))?,
            node_output: OutputByteBudget::new(limits.node_output_bytes)
                .map_err(|_| limit("node_output_bytes"))?,
            peers: Arc::default(),
            metrics: Arc::new(ServeMetrics {
                service,
                message_type: request.message_type.to_string(),
                active: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
                over_limit: AtomicU64::new(0),
            }),
        })
    }

    /// The live budgets for `peer`, created if none are live.
    pub(super) fn peer(&self, peer: &ZakuraPeerId) -> PeerBudgets {
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        // Drop expired identities on each lookup so churn cannot grow the map.
        peers.retain(|_, budgets| budgets.execution.is_alive());
        if let Some(budgets) = peers.get(peer).and_then(WeakPeerBudgets::upgrade) {
            return budgets;
        }
        let budgets = PeerBudgets {
            execution: SlotBudget::new(self.limits.peer_execution)
                .expect("peer execution was validated when the capacity was built"),
            output: OutputByteBudget::new(self.limits.peer_output_bytes)
                .expect("peer output bytes were validated when the capacity was built"),
        };
        peers.insert(peer.clone(), budgets.downgrade());
        budgets
    }

    /// Node execution slots in use.
    #[cfg(test)]
    pub(crate) fn node_execution_held(&self) -> usize {
        self.node_execution.reserved()
    }

    /// Node output bytes granted.
    #[cfg(test)]
    pub(crate) fn node_output_held(&self) -> u64 {
        self.node_output.granted()
    }

    /// `peer`'s execution slots and output bytes in use.
    #[cfg(test)]
    pub(crate) fn peer_held(&self, peer: &ZakuraPeerId) -> (usize, u64) {
        let budgets = self.peer(peer);
        (budgets.execution.reserved(), budgets.output.granted())
    }

    /// Take every free node execution slot and output byte, so serving
    /// waits until the returned hold drops.
    #[cfg(test)]
    pub(crate) fn hold_node_for_test(&self) -> NodeHold {
        NodeHold {
            _execution: self.node_execution.hold_free(),
            _output: self.node_output.hold_free(),
        }
    }

    /// Requests served above the advertised limit, within the margin.
    #[cfg(test)]
    pub(crate) fn over_limit_count(&self) -> u64 {
        self.metrics.over_limit.load(Ordering::Relaxed)
    }

    /// Jobs running `produce`, and jobs waiting for capacity.
    #[cfg(test)]
    pub(crate) fn active_and_waiting(&self) -> (usize, usize) {
        (
            self.metrics.active.load(Ordering::Relaxed),
            self.metrics.waiting.load(Ordering::Relaxed),
        )
    }
}

/// Every node slot and byte a test took. Dropping it releases them.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NodeHold {
    _execution: Vec<crate::zakura::regulation::SlotPermit>,
    _output: Option<crate::zakura::regulation::OutputGrant>,
}

/// One peer's execution slots and output bytes.
#[derive(Clone, Debug)]
pub(super) struct PeerBudgets {
    pub(super) execution: SlotBudget,
    pub(super) output: OutputByteBudget,
}

impl PeerBudgets {
    fn downgrade(&self) -> WeakPeerBudgets {
        WeakPeerBudgets {
            execution: self.execution.downgrade(),
            output: self.output.downgrade(),
        }
    }
}

#[derive(Debug)]
struct WeakPeerBudgets {
    execution: WeakSlotBudget,
    output: WeakOutputByteBudget,
}

impl WeakPeerBudgets {
    fn upgrade(&self) -> Option<PeerBudgets> {
        Some(PeerBudgets {
            execution: self.execution.upgrade()?,
            output: self.output.upgrade()?,
        })
    }
}
