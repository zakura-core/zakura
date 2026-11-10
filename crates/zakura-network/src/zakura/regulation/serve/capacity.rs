//! Serving capacity: node budgets, per-peer budgets, and serving metrics.

use std::sync::Arc;

use thiserror::Error;
use zakura_rt::{
    collections::HashMap,
    sync::{
        counter::{AtomicU64, AtomicUsize, Ordering},
        Mutex, PoisonError,
    },
};

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
    /// Unsent responses one peer may hold, independent of encoded size.
    pub(crate) peer_output_responses: usize,
    /// Unsent responses all peers may hold together.
    pub(crate) node_output_responses: usize,
    /// Unsent encoded response bytes one peer may hold.
    pub(crate) peer_output_bytes: u64,
    /// Unsent encoded response bytes all peers may hold together.
    pub(crate) node_output_bytes: u64,
}

/// A serving configuration that cannot work.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ServeConfigError {
    /// The row is not a request or subscription row.
    #[error("message type {message_type} is not a request or subscription row")]
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
///
/// The counters only feed gauges and tests, so they're `sync::counter`
/// atomics, which cost no preemption points in the simulator.
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
    pub(super) node_output_responses: SlotBudget,
    peers: Arc<Mutex<HashMap<ZakuraPeerId, WeakPeerBudgets>>>,
    pub(super) metrics: Arc<ServeMetrics>,
    /// Told each lookup's pruning and each push's acquisition, in test and
    /// simulator builds.
    #[cfg(any(test, zakura_sim))]
    pub(super) reports: Option<ServeReports>,
}

/// What a [`ServeCapacity`] tells in test and simulator builds: after each
/// lookup's pruning, the peers whose budgets had expired just before it, the
/// peers it removed and those of them with any budget still alive, after a
/// lookup that replaces budgets any of which is still alive, that peer as
/// removed and alive,
/// and after each budget a push takes, the
/// pushing peer and the budgets in use then. An expired entry never comes
/// alive again, so an honest pruning removes every peer it found expired.
#[cfg(any(test, zakura_sim))]
#[derive(Clone)]
pub(crate) struct ServeReports {
    pruned: Arc<dyn Fn(Vec<ZakuraPeerId>, Vec<ZakuraPeerId>, Vec<ZakuraPeerId>) + Send + Sync>,
    pub(super) acquired: Arc<dyn Fn(&ZakuraPeerId, super::push::ServeInUse) + Send + Sync>,
}

#[cfg(any(test, zakura_sim))]
impl ServeReports {
    /// Reports that call `pruned` with the expired peers, the removed ones
    /// and the removed ones still alive, and `acquired` with each acquiring
    /// peer and the budgets in use.
    pub(crate) fn new(
        pruned: impl Fn(Vec<ZakuraPeerId>, Vec<ZakuraPeerId>, Vec<ZakuraPeerId>) + Send + Sync + 'static,
        acquired: impl Fn(&ZakuraPeerId, super::push::ServeInUse) + Send + Sync + 'static,
    ) -> Self {
        ServeReports {
            pruned: Arc::new(pruned),
            acquired: Arc::new(acquired),
        }
    }
}

#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for ServeReports {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ServeReports")
    }
}

impl ServeCapacity {
    /// Capacity for the request or subscription `row` of `service`.
    ///
    /// A subscription row's capacity serves its pages through
    /// [`Self::push`].
    pub(crate) fn new(
        service: &'static str,
        request: &'static MessageRule,
        limits: ServeLimits,
    ) -> Result<Self, ServeConfigError> {
        let max_in_flight = match request.role {
            MessageRole::Request { max_in_flight, .. } => max_in_flight,
            MessageRole::Subscription { max_live, .. } => max_live,
            _ => {
                return Err(ServeConfigError::NotARequest {
                    message_type: request.message_type,
                })
            }
        };
        let limit = |limit| ServeConfigError::Limit { limit };
        SlotBudget::new(limits.peer_output_responses)
            .map_err(|_| limit("peer_output_responses"))?;
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
            node_output_responses: SlotBudget::new(limits.node_output_responses)
                .map_err(|_| limit("node_output_responses"))?,
            peers: Arc::default(),
            metrics: Arc::new(ServeMetrics {
                service,
                message_type: request.message_type.to_string(),
                active: AtomicUsize::new(0),
                waiting: AtomicUsize::new(0),
                over_limit: AtomicU64::new(0),
            }),
            #[cfg(any(test, zakura_sim))]
            reports: None,
        })
    }

    /// This capacity, telling `reports` each lookup's pruning and each
    /// push's acquisition.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn with_reports(self, reports: Option<ServeReports>) -> Self {
        Self { reports, ..self }
    }

    /// The live budgets for `peer`, created if none are live.
    pub(super) fn peer(&self, peer: &ZakuraPeerId) -> PeerBudgets {
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        // Drop expired identities on each lookup so churn cannot grow the map.
        // The report's reads peek, so they add no scheduling point.
        #[cfg(any(test, zakura_sim))]
        let expired: Vec<ZakuraPeerId> = peers
            .iter()
            .filter(|(_, budgets)| !budgets.execution.peek_alive())
            .map(|(peer, _)| peer.clone())
            .collect();
        #[cfg(any(test, zakura_sim))]
        let (mut removed, mut removed_live) = (Vec::new(), Vec::new());
        peers.retain(|peer, budgets| {
            let keep = budgets.execution.is_alive();
            // Each removal, with its liveness at its own decision, since
            // the other entries' checks are scheduling points.
            #[cfg(any(test, zakura_sim))]
            if !keep {
                if budgets.peek_any_alive() {
                    removed_live.push(peer.clone());
                }
                removed.push(peer.clone());
            }
            #[cfg(not(any(test, zakura_sim)))]
            let _ = peer;
            keep
        });
        #[cfg(any(test, zakura_sim))]
        if let Some(reports) = &self.reports {
            (reports.pruned)(expired, removed, removed_live);
        }
        if let Some(budgets) = peers.get(peer).and_then(WeakPeerBudgets::upgrade) {
            return budgets;
        }
        let budgets = PeerBudgets {
            output_responses: SlotBudget::new(self.limits.peer_output_responses)
                .expect("peer output responses were validated when the capacity was built"),
            execution: SlotBudget::new(self.limits.peer_execution)
                .expect("peer execution was validated when the capacity was built"),
            output: OutputByteBudget::new(self.limits.peer_output_bytes)
                .expect("peer output bytes were validated when the capacity was built"),
        };
        #[cfg(not(any(test, zakura_sim)))]
        peers.insert(peer.clone(), budgets.downgrade());
        #[cfg(any(test, zakura_sim))]
        {
            let replaced = peers.insert(peer.clone(), budgets.downgrade());
            // Any live budget of the replaced family is live work's, which
            // the new family now serves apart from.
            if let (Some(reports), Some(replaced)) = (&self.reports, replaced) {
                if replaced.peek_any_alive() {
                    (reports.pruned)(Vec::new(), vec![peer.clone()], vec![peer.clone()]);
                }
            }
        }
        budgets
    }

    /// The limits this capacity was built with.
    #[cfg(test)]
    pub(crate) fn limits(&self) -> ServeLimits {
        self.limits
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
    pub(super) output_responses: SlotBudget,
    pub(super) execution: SlotBudget,
    pub(super) output: OutputByteBudget,
}

impl PeerBudgets {
    fn downgrade(&self) -> WeakPeerBudgets {
        WeakPeerBudgets {
            output_responses: self.output_responses.downgrade(),
            execution: self.execution.downgrade(),
            output: self.output.downgrade(),
        }
    }
}

#[derive(Debug)]
struct WeakPeerBudgets {
    output_responses: WeakSlotBudget,
    execution: WeakSlotBudget,
    output: WeakOutputByteBudget,
}

impl WeakPeerBudgets {
    /// Whether any of these budgets is still alive, read with no simulator
    /// preemption point, for an observer's report. Every permit and grant
    /// drops before the family it keeps, so an honest family's budgets live
    /// and die together.
    #[cfg(any(test, zakura_sim))]
    fn peek_any_alive(&self) -> bool {
        self.output_responses.peek_alive()
            || self.execution.peek_alive()
            || self.output.peek_alive()
    }

    fn upgrade(&self) -> Option<PeerBudgets> {
        Some(PeerBudgets {
            output_responses: self.output_responses.upgrade()?,
            execution: self.execution.upgrade()?,
            output: self.output.upgrade()?,
        })
    }
}
