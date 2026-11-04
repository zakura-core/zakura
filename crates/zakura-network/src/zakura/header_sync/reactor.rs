use std::{cell::Cell, future::Future, num::NonZeroU64, panic::AssertUnwindSafe, pin::Pin};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use zakura_chain::block;
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::NodeId;
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{mpsc, watch, Semaphore},
    task::JoinHandle,
    time::{self, Instant},
};

use super::{
    config::{header_sync_header_bytes_for_network, COMMON_HEADER_BYTES},
    events::{HeaderPortOperation, PortDispatch},
    scheduler::{
        completed_targets::CompletedHeaderTargets,
        peer_work::{
            decide_offer, page_next, repair_page, HeaderTargetPhase, HeaderWorkEnd, OfferDecision,
            OfferFacts, OfferedPeerWork, PageFacts, PageNext, PeerWorkPriority, PeerWorkQueue,
            QueueWorkResult, RepairPage, RetainedHeaders, SpentOffer, StorageCall,
            MAX_HEADER_CHUNK_RESERVATION_V1,
        },
        repair::{RepairPolicyState, RepairRequirement, RepairRequirementSlot},
        status::StatusPublisher,
    },
    v9::{self, Admitted, Ended, PageLimits},
    *,
};
use crate::zakura::{
    trace::{
        header_sync_trace as hs_trace, peer_label as trace_peer_label,
        queue_send_trace as qs_trace, HEADER_SYNC_TABLE, QUEUE_SEND_TABLE,
    },
    transport::sim_arg,
    OrderedSendError, ServicePeerDirection, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
    ZakuraPeerId,
};

mod state;

pub use super::scheduler::peer_work::StorageCallKind;

#[cfg(any(test, zakura_sim))]
use super::observe::{self, ReactorEvent};
use state::{
    repair_candidates, vct_error_retry, CloseAttempt, LocalOperationExpiry, LocatorDisposition,
    LocatorKey, LocatorProposal, ReactorState, RepairCandidate, RepairStallDue,
    RepairSupplierFacts, RepairSupplierLimits, SchedulingOpportunity, VctLocalOperation,
    VctRepairRetry, VctRepairRetryAttribution, VctRepairStall, VctRepairStallOutcome,
    VctSupplierRejections,
};

/// Reports the event that `$event` builds to the reactor's observer.
///
/// `$event` is evaluated only when an observer is installed. Outside test and
/// simulator builds the macro expands to nothing.
#[cfg(any(test, zakura_sim))]
macro_rules! observe {
    ($reactor:expr, $event:expr) => {
        if let Some(observer) = &$reactor.startup.observer.0 {
            observer.observe($event)
        }
    };
}

/// Reports the event that `$event` builds to the reactor's observer.
///
/// Outside test and simulator builds the macro expands to nothing.
#[cfg(not(any(test, zakura_sim)))]
macro_rules! observe {
    ($reactor:expr, $event:expr) => {};
}

mod dispatch;
mod repair;
mod trace;

use dispatch::{end_retained, PendingPortOperation, StorageCompletion, StorageOutcome};
use repair::vct_repair_predecessor;
use trace::{header_direction_label, ordered_send_error_label};

const INTERNAL_VCT_REPAIR_SESSION_ID: u64 = u64::MAX;

const VCT_REPAIR_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Minimum interval between unchanged header-snapshot refresh trace rows.
///
/// The trace records every frontier advance and reanchor.
/// Metrics and the committed snapshot remain exact.
/// The trace samples identical refresh diagnostics to bound long-running JSONL traces.
const SNAPSHOT_REFRESH_TRACE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

fn snapshot_refresh_trace_due(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= SNAPSHOT_REFRESH_TRACE_INTERVAL)
}
/// Bound admitted and reserved headers to one wire page ahead of verified bodies.
const INTEGRATED_HEADER_BODY_WINDOW_V1: u32 = MAX_HS_RANGE;
/// Refill and publish selected-chain headers in whole batches.
///
/// One batch spans a maximum checkpoint range plus the first header of its successor.
const HEADER_REFILL_BATCH_V1: usize =
    zakura_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP + 1;

/// Spawn the canonical header-sync reactor.
pub fn spawn_header_sync_reactor(
    startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderPortOperation>,
        JoinHandle<()>,
    ),
    HeaderSyncStartError,
> {
    let (handle, actions, reactor) = build_header_sync_reactor(startup)?;
    Ok((
        handle,
        actions,
        spawn_erased_named("header_sync_reactor", reactor.run()),
    ))
}

fn build_header_sync_reactor(
    mut startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderPortOperation>,
        HeaderSyncReactor,
    ),
    HeaderSyncStartError,
> {
    let committed_snapshot = startup
        .committed_snapshots
        .as_ref()
        .and_then(|snapshots| snapshots.borrow().clone());
    if let Some(snapshot) = committed_snapshot.as_ref() {
        startup.frontiers = FullStateFrontiers {
            finalized_height: snapshot.frontiers.finalized.height,
            verified_block_tip: snapshot.frontiers.verified_best.height,
            verified_block_hash: snapshot.frontiers.verified_best.hash,
        };
        startup.best_header_tip = Some((
            snapshot.frontiers.header_best.height,
            snapshot.frontiers.header_best.hash,
        ));
    }
    if startup.anchor.0 > startup.frontiers.verified_block_tip {
        return Err(HeaderSyncStartError::AnchorAboveVerifiedBlockTip {
            anchor_height: startup.anchor.0,
            verified_block_tip: startup.frontiers.verified_block_tip,
        });
    }

    let (events_tx, events_rx) = mpsc::channel(128);
    let (lifecycle_tx, lifecycle_rx) = super::events::LifecycleSender::channel(
        startup
            .config
            .peer_limits
            .max_inbound_peers
            .saturating_add(startup.config.peer_limits.max_outbound_peers),
    );
    let (actions_tx, actions_rx) = mpsc::channel(128);
    let initial_tip = startup.best_header_tip.unwrap_or(startup.anchor);
    let (tip_tx, tip_rx) = watch::channel(initial_tip);
    let (peers_tx, peers_rx) =
        watch::channel(ServicePeerSnapshot::new(0, 0, startup.config.peer_limits));
    let (candidates_tx, candidates_rx) = watch::channel(ZakuraHeaderSyncCandidateState {
        target_height: next_height(initial_tip.0),
        admitted_node_ids: Vec::new(),
        backed_off_node_ids: Vec::new(),
    });

    // Status must advertise room for one page under every advertised schema.
    // Every network encodes Status with the common rows, whose pages are the
    // largest.
    let smallest_page = v9::smallest_page(COMMON_HEADER_BYTES, AuxSchema::V1);
    let page_limits = PageLimits {
        max_headers: startup.config.advertised_max_headers_per_response(),
        // The frame cap and the protocol cap both fit u32.
        max_bytes: startup
            .max_frame_bytes
            .saturating_sub(FRAME_HEADER_BYTES as u32)
            .min(MAX_HS_MESSAGE_BYTES as u32)
            .max(smallest_page as u32),
        aux_schema_mask: AuxSchema::V1.mask_bit(),
    };
    let vct_repair_status = startup
        .vct_root_repairs
        .as_ref()
        .map_or_else(Default::default, |repairs| *repairs.borrow());
    let handle = HeaderSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        tip: tip_rx,
        peers: peers_rx,
        candidates: candidates_rx,
        trace: startup.trace.clone(),
        network: startup.network.clone(),
        layout: v9::layout(&startup.network),
        port: startup.header_chain_port.clone(),
        serving: v9::serving_capacity(
            startup
                .config
                .serving_sessions(crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS),
            startup.config.node_serve_output_bytes,
        ),
        page_limits,
    };
    let header_bytes = header_sync_header_bytes_for_network(&startup.network);
    let mut reactor = HeaderSyncReactor {
        startup,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        pending_port_operations: FuturesUnordered::new(),
        locator_capacity: std::sync::Arc::new(Semaphore::new(
            zakura_header_chain::MAX_STAGED_TARGETS_V1,
        )),
        locator_capacity_waiting: false,
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
        header_bytes,
        page_limits,
        committed_snapshot,
        vct_repair_status,
        sessions: HashMap::new(),
        state: ReactorState::new(Instant::now()),
        peer_work_queue: PeerWorkQueue::default(),
        last_snapshot_refresh_trace_at: Cell::new(None),
    };
    if let Some(snapshot) = reactor.committed_snapshot.as_ref() {
        reactor.emit_snapshot_observed(None, snapshot);
    }
    reactor.schedule_current_vct_repair();
    Ok((handle, actions_rx, reactor))
}

/// Runtime handles of one admitted peer's current session.
#[derive(Debug)]
struct SessionHandles {
    session: PeerSession,
    status_publisher: Option<StatusPublisher>,
}

#[derive(Debug)]
struct HeaderSyncReactor {
    startup: HeaderSyncStartup,
    events: mpsc::Receiver<Event>,
    lifecycle: mpsc::Receiver<super::events::LifecycleEvent>,
    #[cfg_attr(not(any(test, feature = "zakura-testkit")), allow(dead_code))]
    actions: mpsc::Sender<HeaderPortOperation>,
    pending_port_operations: FuturesUnordered<PendingPortOperation>,
    // Unlike queue ownership, this capacity survives eviction and caller timeouts.
    locator_capacity: std::sync::Arc<Semaphore>,
    locator_capacity_waiting: bool,
    tip: watch::Sender<(block::Height, block::Hash)>,
    peers: watch::Sender<ServicePeerSnapshot>,
    candidates: watch::Sender<ZakuraHeaderSyncCandidateState>,
    /// One header's wire size on this network, fixed for the reactor's life.
    ///
    /// Checking a configured testnet for Regtest builds its parameters, and
    /// every headroom check needs this, so it is computed once.
    header_bytes: usize,
    /// The serving limits this node advertises in its `Status`.
    page_limits: PageLimits,
    committed_snapshot: Option<zakura_header_chain::EngineSnapshot>,
    vct_repair_status: zakura_header_chain::VctRootRepairStatus,
    /// Runtime handles of each admitted peer's current session.
    ///
    /// Its keys equal the keys of [`ReactorState::peers`].
    sessions: HashMap<ZakuraPeerId, SessionHandles>,
    state: ReactorState,
    peer_work_queue: PeerWorkQueue,
    last_snapshot_refresh_trace_at: Cell<Option<Instant>>,
}

/// Why an active header request was retired.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderRequestTerminal {
    /// The publisher no longer retains the initial target.
    TargetNotRetained,
    /// No locator hash lies on the target path.
    NoLocatorIntersection,
    /// The publisher pruned history on the target path.
    HistoryPruned,
    /// The publisher's selected chain changed between `Status` and `Open`.
    TargetNotSelected,
    /// The publisher's selected chain stopped extending the cursor.
    SubscriptionSuperseded,
    /// The publisher received `Close`.
    SubscriptionClosed,
    /// The owning session disconnected.
    Disconnected,
    /// A local failure ended the request.
    LocalError,
    /// The peer sent a malformed response.
    MalformedResponse,
    /// The repair supplier sent auxiliary input that local state excludes.
    ExcludedAuxiliaryInput,
    /// The VCT repair the request served no longer needs it.
    RepairObsolete,
    /// State committed a resource stall instead of the target.
    ResourceStalled,
    /// Sending to the peer failed.
    SendError,
    /// A new session for the same peer replaced the owning session.
    SessionReplaced,
    /// The reactor shut down.
    Shutdown,
    /// A committed snapshot made the request's authority obsolete.
    SnapshotObsolete,
    /// Staging the response's pages was refused.
    StagingRefused,
    /// State admitted the target.
    TargetAdmitted,
    /// State or validation rejected the target.
    TargetRejected,
    /// The request's deadline passed.
    TimedOut,
}

impl HeaderRequestTerminal {
    fn needs_terminal_trace(self) -> bool {
        !matches!(
            self,
            Self::Disconnected
                | Self::SessionReplaced
                | Self::SnapshotObsolete
                | Self::TargetAdmitted
                | Self::TargetRejected
        )
    }

    /// The reason's stable snake-case name, as traces and metrics record it.
    pub fn label(self) -> &'static str {
        match self {
            Self::TargetNotRetained => "target_not_retained",
            Self::NoLocatorIntersection => "no_locator_intersection",
            Self::HistoryPruned => "history_pruned",
            Self::TargetNotSelected => "target_not_selected",
            Self::SubscriptionSuperseded => "subscription_superseded",
            Self::SubscriptionClosed => "subscription_closed",
            Self::Disconnected => "disconnected",
            Self::LocalError => "local_error",
            Self::MalformedResponse => "malformed_response",
            Self::ExcludedAuxiliaryInput => "excluded_auxiliary_input",
            Self::RepairObsolete => "repair_obsolete",
            Self::ResourceStalled => "resource_stalled",
            Self::SendError => "send_error",
            Self::SessionReplaced => "session_replaced",
            Self::Shutdown => "shutdown",
            Self::SnapshotObsolete => "snapshot_obsolete",
            Self::StagingRefused => "staging_refused",
            Self::TargetAdmitted => "target_admitted",
            Self::TargetRejected => "target_rejected",
            Self::TimedOut => "timed_out",
        }
    }
}

impl From<HeadersOutcomeCode> for HeaderRequestTerminal {
    fn from(outcome: HeadersOutcomeCode) -> Self {
        match outcome {
            HeadersOutcomeCode::TargetNotRetained => Self::TargetNotRetained,
            HeadersOutcomeCode::NoLocatorIntersection => Self::NoLocatorIntersection,
            HeadersOutcomeCode::HistoryPruned => Self::HistoryPruned,
            HeadersOutcomeCode::TargetNotSelected => Self::TargetNotSelected,
            HeadersOutcomeCode::SubscriptionSuperseded => Self::SubscriptionSuperseded,
            HeadersOutcomeCode::SubscriptionClosed => Self::SubscriptionClosed,
        }
    }
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
fn port_header_entry(entry: HeaderEntry) -> zakura_node_services::header_chain::TargetEntry {
    zakura_node_services::header_chain::TargetEntry {
        header: entry.header,
        body_size: entry.body_size,
        tree_aux: entry.tree_aux,
    }
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
pub(super) fn selected_port_aux_delivery(
    deliveries: &[zakura_header_chain::AuxDelivery],
    schema: AuxSchema,
) -> Option<zakura_header_chain::AuxDelivery> {
    deliveries
        .iter()
        .copied()
        .filter(|delivery| {
            !delivery.is_rejected()
                && match schema {
                    AuxSchema::None => matches!(
                        delivery.body_size,
                        zakura_header_chain::BodySizeHint::Known(_)
                    ),
                    AuxSchema::V1 => delivery.tree_aux.is_some(),
                }
        })
        .min_by_key(|delivery| (!delivery.is_authenticated(), delivery.delivery_id))
}

impl HeaderSyncReactor {
    async fn run(mut self) {
        let mut committed_snapshots = self.startup.committed_snapshots.clone();
        let mut vct_root_repairs = self.startup.vct_root_repairs.clone();
        let mut capacity_releases = self.peer_work_queue.capacity_releases();
        let terminal_outcome = loop {
            if self
                .state
                .vct_local_operation
                .as_ref()
                .is_some_and(|operation| operation.fatal_sent)
            {
                self.startup.shutdown.cancelled().await;
                break HeaderRequestTerminal::Shutdown;
            }
            if self.startup.shutdown.is_cancelled() {
                break HeaderRequestTerminal::Shutdown;
            }
            // Retire already-ready state operations before any event can enforce
            // their hard deadlines. Bound this pass to the current operation set.
            for _ in 0..self.pending_port_operations.len() {
                let Some(Some(completion)) = self.pending_port_operations.next().now_or_never()
                else {
                    break;
                };
                self.handle_port_completion(completion);
            }
            // Inspect watches on every turn, even when peer queues never become empty.
            // `changed` delivers a final unseen value before reporting channel closure.
            if let Some(snapshots) = committed_snapshots.as_mut() {
                match snapshots.changed().now_or_never() {
                    Some(Ok(())) => {
                        let snapshot = snapshots.borrow_and_update().clone();
                        if let Some(snapshot) = snapshot {
                            self.observe_latest_committed_snapshot(snapshot);
                        }
                    }
                    Some(Err(_)) => committed_snapshots = None,
                    None => {}
                }
            }
            if let Some(repairs) = vct_root_repairs.as_mut() {
                match repairs.changed().now_or_never() {
                    Some(Ok(())) => {
                        let status = *repairs.borrow_and_update();
                        self.observe_vct_root_repair(status);
                    }
                    Some(Err(_)) => vct_root_repairs = None,
                    None => {}
                }
            }
            // One item from each queue per turn prevents either queue monopolizing the loop.
            // Session readers wait for connection acknowledgement before publishing peer events.
            let mut handled_event = false;
            if let Ok(event) = self.lifecycle.try_recv() {
                self.handle_lifecycle(event);
                handled_event = true;
            }
            if let Ok(event) = self.events.try_recv() {
                self.handle_event(event);
                handled_event = true;
            }
            let maintenance = self.next_maintenance_deadline();
            metrics::counter!("sync.header.reactor.iterations").increment(1);
            let locator_capacity = self.locator_capacity.clone();
            zakura_rt::select! {
                biased;
                _ = self.startup.shutdown.cancelled() => break HeaderRequestTerminal::Shutdown,
                completion = async {
                    if self.pending_port_operations.is_empty() {
                        std::future::pending().await
                    } else {
                        self.pending_port_operations.next().await
                    }
                } => {
                    if let Some(completion) = completion {
                        self.handle_port_completion(completion);
                    }
                }
                permit = locator_capacity.acquire_owned(), if self.locator_capacity_waiting => {
                    drop(permit.expect("the locator semaphore is never closed"));
                    self.locator_capacity_waiting = false;
                },
                _ = capacity_releases.changed() => {},
                _ = time::sleep_until(maintenance) => self.refresh_statuses(),
                event = self.lifecycle.recv() => match event {
                    Some(event) => self.handle_lifecycle(event),
                    None => break HeaderRequestTerminal::Shutdown,
                },
                event = self.events.recv() => match event {
                    Some(event) => self.handle_event(event),
                    None => break HeaderRequestTerminal::Shutdown,
                },
                _ = zakura_rt::task::yield_now(), if handled_event => {},
                changed = async {
                    match committed_snapshots.as_mut() {
                        Some(snapshots) => snapshots.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok() {
                        if let Some(snapshot) = committed_snapshots
                            .as_ref()
                            .and_then(|snapshots| snapshots.borrow().clone())
                        {
                            self.observe_latest_committed_snapshot(snapshot);
                        }
                    } else {
                        committed_snapshots = None;
                    }
                }
                changed = async {
                    match vct_root_repairs.as_mut() {
                        Some(repairs) => repairs.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok() {
                        if let Some(status) =
                            vct_root_repairs.as_ref().map(|repairs| *repairs.borrow())
                        {
                            self.observe_vct_root_repair(status);
                        }
                    } else {
                        vct_root_repairs = None;
                    }
                }
            }
            self.schedule_cached_statuses();
        };
        self.retire_all_peer_work(terminal_outcome);
    }

    /// Acknowledge admission only after the reactor has installed or refused the session.
    fn handle_lifecycle(&mut self, event: super::events::LifecycleEvent) {
        self.handle_event(event.event);
        let _ = event.applied.send(());
    }

    fn handle_event(&mut self, event: Event) {
        metrics::counter!(
            "sync.header.reactor.events",
            "event" => event.metrics_label()
        )
        .increment(1);
        match event {
            Event::PeerConnected(session) => self.handle_peer_connected(session),
            Event::PeerDisconnected {
                peer,
                session_id,
                reason,
            } => self.handle_peer_disconnected(&peer, session_id, reason),
            Event::AdvisorySummary { .. } => {}
            Event::Status {
                peer,
                session_id,
                status,
            } => self.handle_status(peer, session_id, status),
            Event::Page {
                peer,
                session_id,
                page,
                admitted,
            } => self.handle_page(peer, session_id, page, admitted),
            Event::Outcome {
                peer,
                session_id,
                ended,
            } => self.handle_outcome(peer, session_id, ended),
            // An external driver's results enter through the same typed boundary as
            // direct storage results. They hold no capacity lease.
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderLocatorReady {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator,
            } => self.handle_storage_completion(
                StorageCompletion::ExternalLocator {
                    peer,
                    session_id,
                    target_tip_hash,
                    scope,
                    locator,
                },
                None,
            ),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::VctRepairContextReady { owner, result } => self.handle_storage_completion(
                StorageCompletion::VctRepairContext { owner, result },
                None,
            ),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderTargetPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_storage_completion(
                StorageCompletion::TargetPrepared {
                    peer,
                    source,
                    owner,
                    result,
                },
                None,
            ),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result,
            } => self.handle_storage_completion(
                StorageCompletion::TargetAdmission {
                    peer,
                    source,
                    owner,
                    result,
                },
                None,
            ),
        }
    }

    fn handle_port_completion(&mut self, outcome: StorageOutcome) {
        #[cfg(any(test, zakura_sim))]
        let panicked = matches!(outcome, StorageOutcome::Panicked(_));
        let call = match &outcome {
            StorageOutcome::Completed { call, .. } => *call,
            StorageOutcome::Panicked(context) => context.call,
        };
        if !self.state.calls.settle(call) {
            tracing::error!(
                ?call,
                "a header storage call settled twice or never started"
            );
            metrics::counter!("sync.header.port.unknown_settlement").increment(1);
            return;
        }
        match outcome {
            StorageOutcome::Completed {
                completion,
                retained,
                ..
            } => self.handle_storage_completion(*completion, retained),
            StorageOutcome::Panicked(context) => self.handle_port_panic(*context),
        }
        // Apply publishes its snapshot before completing. Observe it before another
        // event can reserve the capacity that this completion released.
        self.refresh_committed_snapshot();
        #[cfg(any(test, zakura_sim))]
        observe!(
            self,
            ReactorEvent::Handled {
                call: call.number,
                panicked
            }
        );
    }

    /// Handle `completion`. `retained` stays charged until its handler returns,
    /// or moves to the apply that the handler dispatches.
    fn handle_storage_completion(
        &mut self,
        completion: StorageCompletion,
        retained: Option<RetainedHeaders>,
    ) {
        match completion {
            StorageCompletion::Locator { key, locator } => {
                self.finish_header_locator_query(key, locator)
            }
            #[cfg(any(test, feature = "zakura-testkit"))]
            StorageCompletion::ExternalLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator,
            } => {
                self.handle_header_locator_ready(peer, session_id, target_tip_hash, scope, locator)
            }
            StorageCompletion::VctRepairContext { owner, result } => {
                self.handle_vct_repair_context_ready(owner, result)
            }
            StorageCompletion::TargetPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_target_prepared(peer, source, owner, result, retained),
            StorageCompletion::TargetAdmission {
                peer,
                source,
                owner,
                result,
            } => self.handle_target_admission(peer, source, owner, result, retained),
        }
    }

    fn refresh_committed_snapshot(&mut self) {
        let latest_snapshot = self
            .startup
            .committed_snapshots
            .as_ref()
            .and_then(|snapshots| snapshots.borrow().clone());
        if let Some(snapshot) = latest_snapshot {
            self.observe_latest_committed_snapshot(snapshot);
        }
    }

    fn handle_peer_connected(&mut self, session: PeerSession) {
        self.refresh_committed_snapshot();
        let peer = session.peer_id().clone();
        if self.state.readmission_refused(&peer, Instant::now()) {
            session.cancel_token().cancel();
            metrics::counter!("sync.header.peer.readmission_refused.total").increment(1);
            return;
        }
        let direction = session.direction();
        let replaces_same_direction = self
            .sessions
            .get(&peer)
            .is_some_and(|handles| handles.session.direction() == direction);
        let replaced_repair = self.peer_work_queue.active(&peer).and_then(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then_some((active.owner, active.source, active.phase))
        });
        let owns_local_repair_operation = replaced_repair.is_some_and(|(_, _, phase)| {
            matches!(
                phase,
                HeaderTargetPhase::Preparing | HeaderTargetPhase::Applying
            )
        });
        let at_capacity = !replaces_same_direction
            && self.admitted_count(direction)
                >= match direction {
                    ServicePeerDirection::Inbound => {
                        self.startup.config.peer_limits.max_inbound_peers
                    }
                    ServicePeerDirection::Outbound => {
                        self.startup.config.peer_limits.max_outbound_peers
                    }
                };
        if at_capacity {
            session.cancel_token().cancel();
            return;
        }

        let status_publisher = self.committed_snapshot.as_ref().map(|snapshot| {
            StatusPublisher::new(
                Status::from_snapshot(snapshot, self.page_limits),
                self.startup.status_refresh_interval,
                Instant::now(),
            )
        });
        let replaces_existing_peer = self.sessions.contains_key(&peer);
        if replaces_existing_peer && !owns_local_repair_operation {
            self.retire_peer_work(&peer, HeaderRequestTerminal::SessionReplaced);
        }
        let replaced_schedule = self.state.admit_session(&peer);
        let previous = self.sessions.insert(
            peer.clone(),
            SessionHandles {
                session,
                status_publisher,
            },
        );
        debug_assert_eq!(
            replaced_schedule,
            previous.is_some(),
            "sessions and schedules share their keys"
        );
        if let Some(previous) = previous {
            previous.session.cancel_token().cancel();
            if let Some((owner, source)) = replaced_repair.and_then(|(owner, source, phase)| {
                if phase == HeaderTargetPhase::Receiving {
                    owner.body_owner().map(|owner| (owner, source))
                } else {
                    None
                }
            }) {
                self.retry_vct_repair(
                    owner,
                    VctRepairRetry::supplier(source, HeaderRequestTerminal::SessionReplaced),
                );
            }
            self.state.requeue_supplier(source_id_from_peer(&peer));
        }
        self.publish_peer_state();
        let admitted = self
            .sessions
            .get(&peer)
            .expect("the admitted peer was just installed");
        self.emit_peer_lifecycle(
            hs_trace::HEADER_PEER_CONNECTED,
            &peer,
            admitted.session.session_id(),
            admitted.session.direction(),
            None,
        );
        self.send_status(&peer);
    }

    fn handle_peer_disconnected(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) {
        let Some(direction) = self.sessions.get(peer).and_then(|handles| {
            (handles.session.session_id() == session_id).then(|| handles.session.direction())
        }) else {
            return;
        };
        let abandoned_repair = self.peer_work_queue.active(peer).and_then(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then(|| {
                (
                    active
                        .owner
                        .body_owner()
                        .expect("an auxiliary repair has body authority"),
                    active.source,
                    active.phase,
                )
            })
        });
        let owns_local_repair_operation = abandoned_repair.is_some_and(|(_, _, phase)| {
            matches!(
                phase,
                HeaderTargetPhase::Preparing | HeaderTargetPhase::Applying
            )
        });
        if !owns_local_repair_operation {
            self.retire_peer_work(peer, HeaderRequestTerminal::Disconnected);
        }
        self.sessions.remove(peer);
        self.state.remove_session(peer);
        if let Some((owner, source, HeaderTargetPhase::Receiving)) = abandoned_repair {
            self.retry_vct_repair(
                owner,
                VctRepairRetry::supplier(source, HeaderRequestTerminal::Disconnected),
            );
        }
        self.prune_vct_supplier_history();
        self.publish_peer_state();
        self.emit_peer_lifecycle(
            hs_trace::HEADER_PEER_DISCONNECTED,
            peer,
            session_id,
            direction,
            Some(reason),
        );
    }

    /// Record `peer`'s `status` from its current session as an unspent
    /// offer, and let repair and ordinary discovery consider it.
    fn handle_status(&mut self, peer: ZakuraPeerId, session_id: u64, status: Status) {
        let Some(handles) = self.sessions.get(&peer) else {
            return;
        };
        if handles.session.session_id() != session_id {
            return;
        }
        metrics::counter!("sync.header.peer.status.received").increment(1);
        self.emit_status(hs_trace::HEADER_STATUS_RECEIVED, &peer, session_id, &status);
        self.state.record_status(&peer, status.clone());
        if let Some(task) = self.state.vct_repair.current_mut() {
            task.observe_supplier_status(source_id_from_peer(&peer));
        }
        self.request_vct_repair_context();
        self.try_assign_vct_repair();
        // This offer is considered synchronously. If it remains blocked, changing
        // its payload cannot make another peer eligible. Shared ownership and
        // effective locator claims below invalidate the global pass when needed.
        self.consider_advertised_header_target(peer, session_id, status);
    }

    /// Stage ordinary work for `peer`'s latest status.
    ///
    /// A status held back by another peer's ownership, full slots, missing headroom, or a
    /// resource stall stays unspent. [`Self::schedule_cached_statuses`] stages it once that
    /// condition clears.
    fn consider_advertised_header_target(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        status: Status,
    ) {
        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        let target_tip_hash = status.selected_tip_hash;
        let target = AdvertisedHeaderTarget {
            scope: zakura_header_chain::HeaderWorkAuthority::for_target(local, target_tip_hash),
            session_id,
            status,
        };
        let work_order = target.claimed_work_order(local);
        let decision = decide_offer(
            OfferFacts {
                pruned_pause: self.state.pruned_pause_holds(
                    &peer,
                    local.frontiers.header_best,
                    Instant::now(),
                ),
                covered: self.state.completed_targets.contains(
                    local.header_generation,
                    zakura_header_chain::BranchId::new(
                        local.frontiers.finalized.hash,
                        target_tip_hash,
                    ),
                ),
                eligible: target.is_discovery_eligible(local)
                    && self.negotiated_response_limits(&target.status).1 != 0,
                work: self
                    .peer_work_queue
                    .active(&peer)
                    .map_or(OfferedPeerWork::Idle, |active| {
                        if matches!(active.purpose, HeaderTargetPurpose::Normal)
                            && active.target.status.selected_tip_hash == target_tip_hash
                        {
                            OfferedPeerWork::SameOrdinaryTarget
                        } else {
                            OfferedPeerWork::Other
                        }
                    }),
            },
            || {
                self.ordinary_discovery_paused()
                    || self.ordinary_headroom(&peer, &target.status) == 0
            },
            || self.subscription_ready(&peer, target_tip_hash),
        );
        match decision {
            OfferDecision::Spend {
                cause,
                close_subscription,
            } => {
                if close_subscription {
                    self.close_subscription(&peer);
                }
                self.peer_work_queue.remove_unstarted(&peer);
                self.state.spend_status(&peer);
                match cause {
                    SpentOffer::PrunedPause => {
                        metrics::counter!("sync.header.target.pruned_pause").increment(1);
                    }
                    SpentOffer::Covered => {
                        metrics::counter!("sync.header.target.covered").increment(1);
                    }
                    SpentOffer::Ineligible => {}
                }
            }
            OfferDecision::AlreadyActive { spend } => {
                if spend {
                    self.state.spend_status(&peer);
                }
                metrics::counter!("sync.header.target.already_active").increment(1);
            }
            OfferDecision::HeldBack => self.peer_work_queue.remove_unstarted(&peer),
            OfferDecision::AwaitSubscription => self.close_subscription(&peer),
            OfferDecision::Stage => self.stage_offer(peer, target, work_order),
        }
    }

    /// Stage `target` for `peer`, and request its locator when the queue takes it.
    fn stage_offer(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        work_order: Option<std::cmp::Ordering>,
    ) {
        let session_id = target.session_id;
        let target_tip_hash = target.status.selected_tip_hash;
        let scope = target.scope;
        let previous_claim = self
            .peer_work_queue
            .awaiting_target(&peer)
            .map(|target| self.pending_locator_header_count(&target.status));
        let next_claim = self.pending_locator_header_count(&target.status);
        match self.peer_work_queue.stage_distinct_target(
            peer.clone(),
            target,
            PeerWorkPriority::from_work_order(work_order),
        ) {
            QueueWorkResult::NeedsLocator => {
                if previous_claim.is_some_and(|claimed| claimed != next_claim) {
                    self.state.bump_scheduling_revision();
                }
                self.state.spend_status(&peer);
                if self
                    .dispatch_action(HeaderPortOperation::QueryHeaderLocator {
                        peer: peer.clone(),
                        session_id,
                        target_tip_hash,
                        scope,
                    })
                    .is_refused()
                {
                    self.peer_work_queue.remove_unstarted(&peer);
                }
            }
            QueueWorkResult::AlreadyActive => {
                metrics::counter!("sync.header.target.already_active").increment(1);
            }
            QueueWorkResult::TargetAlreadyAssigned => {
                metrics::counter!("sync.header.target.duplicate_suppressed").increment(1);
            }
            QueueWorkResult::AtCapacity => {
                metrics::counter!("sync.header.target.capacity_refused").increment(1);
                observe!(
                    self,
                    ReactorEvent::OfferWaiting {
                        peer: peer.clone(),
                        session_id,
                        target_tip_hash,
                        cause: observe::WaitCause::StagingFull,
                    }
                );
            }
        }
    }

    /// Compare root-owned opportunity state in constant time before inspecting offers.
    fn scheduling_opportunity(&self) -> SchedulingOpportunity {
        SchedulingOpportunity {
            policy_revision: self.state.scheduling_revision,
            queue_revision: self.peer_work_queue.scheduling_revision(),
            claimed_headers: self.peer_work_queue.claimed_header_count(),
            available_locators: self.locator_capacity.available_permits(),
            resource_stalled: self.ordinary_discovery_paused(),
        }
    }

    /// Stage idle unspent offers only after an eligibility input changes.
    fn schedule_cached_statuses(&mut self) {
        let opportunity = self.scheduling_opportunity();
        if !self.state.cached_pass_wanted(opportunity) {
            return;
        }
        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        if self.ordinary_discovery_paused()
            || self.peer_work_queue.len() >= zakura_header_chain::MAX_STAGED_TARGETS_V1
        {
            return;
        }
        if self.locator_capacity.available_permits() == 0 {
            // Cached offers can become idle without a fresh dispatch refusal.
            // Arm the wakeup even when we skip their scan at exhausted capacity.
            self.locator_capacity_waiting = true;
            return;
        }
        if !self.state.take_cached_pass(Instant::now()) {
            return;
        }
        let peers = self.sessions.len();
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_CACHED_OFFER_PASS.into(),
            );
            row.insert(hs_trace::PEERS_CONSIDERED.into(), peers.into());
        });
        let offers = self.state.cached_offers(
            local,
            |peer| self.peer_work_queue.has_work(peer),
            |peer| {
                self.sessions
                    .get(peer)
                    .expect("sessions and schedules share their keys")
                    .session
                    .session_id()
            },
        );
        for (peer, target) in offers {
            if self.peer_work_queue.len() >= zakura_header_chain::MAX_STAGED_TARGETS_V1 {
                break;
            }
            // Leave a held-back status unspent without counting it as refused on every event.
            if self
                .peer_work_queue
                .is_target_assigned(target.status.selected_tip_hash)
                || self.ordinary_headroom(&peer, &target.status) == 0
            {
                continue;
            }
            self.consider_advertised_header_target(peer, target.session_id, target.status);
        }
        self.state.finish_cached_pass(opportunity, Instant::now());
    }

    /// Whether `peer`'s session can start work toward `target`: it holds no
    /// subscription, or one that a `Grant` can continue toward `target`.
    fn subscription_ready(&self, peer: &ZakuraPeerId, target: block::Hash) -> bool {
        self.sessions.get(peer).is_some_and(|handles| {
            let subscriber = handles.session.subscriber();
            subscriber.live().is_none()
                || subscriber
                    .resumable()
                    .is_some_and(|(_, resumable, _, _)| resumable == target)
        })
    }

    /// Return an unstarted ordinary offer to the scheduler when a local blocker defers it.
    fn defer_ordinary_target(&mut self, peer: &ZakuraPeerId) {
        self.state.retain_offer(peer);
        self.peer_work_queue.remove_unstarted(peer);
    }

    /// Whether a committed resource stall still holds back new ordinary
    /// attempts. See [`ReactorState::ordinary_discovery_paused`].
    fn ordinary_discovery_paused(&self) -> bool {
        self.state.ordinary_discovery_paused(
            self.committed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.state_version),
        )
    }

    /// Return the headers that one ordinary request to `status`'s sender could reserve now.
    ///
    /// Locator reads in flight for other peers count at their negotiated size, because each
    /// one reserves that headroom when it completes.
    fn ordinary_headroom(&self, peer: &ZakuraPeerId, status: &Status) -> u32 {
        let Some(local) = self.committed_snapshot.as_ref() else {
            return 0;
        };
        let pending = self
            .peer_work_queue
            .awaiting_targets()
            .filter(|(owner, _)| *owner != peer)
            .map(|(_, target)| self.pending_locator_header_count(&target.status))
            .fold(0, usize::saturating_add);
        let claimed = self
            .peer_work_queue
            .claimed_header_count()
            .saturating_add(pending);
        self.peer_work_queue
            .reservable_header_count(self.negotiated_response_limits(status).1.min(
                Self::request_header_prefix_remaining(local, claimed, status.selected_tip_height),
            ))
    }

    /// Remember that `peer` ended an ordinary round asking for `schema` with
    /// `HistoryPruned`. See [`ReactorState::note_history_pruned`].
    fn note_history_pruned(&mut self, peer: &ZakuraPeerId, schema: AuxSchema) {
        let tip = self
            .committed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.frontiers.header_best);
        if self
            .state
            .note_history_pruned(peer, schema, tip, Instant::now())
        {
            metrics::counter!("sync.header.target.aux_fallback").increment(1);
        }
    }

    /// The effective virtual claim used by ordinary headroom and cache invalidation.
    fn pending_locator_header_count(&self, status: &Status) -> usize {
        usize::try_from(self.negotiated_response_limits(status).1)
            .unwrap_or(usize::MAX)
            .min(MAX_HEADER_CHUNK_RESERVATION_V1)
    }

    /// Return the response schema and the most headers that one response from `status`'s
    /// sender can carry under both sides' advertised limits.
    fn negotiated_response_limits(&self, status: &Status) -> (AuxSchema, u32) {
        let tree_aux_schema = if status.tree_aux_schema_mask
            & self.page_limits.aux_schema_mask
            & AuxSchema::V1.mask_bit()
            != 0
        {
            AuxSchema::V1
        } else {
            AuxSchema::None
        };
        let byte_limited_count = v9::page_capacity(
            self.header_bytes,
            tree_aux_schema,
            usize::try_from(status.max_message_bytes.min(self.page_limits.max_bytes))
                .unwrap_or(usize::MAX),
        );
        let header_count = status
            .max_headers_per_response
            .min(self.page_limits.max_headers)
            .min(byte_limited_count)
            .min(MAX_HS_RANGE);
        (tree_aux_schema, header_count)
    }

    /// Stage a page that `peer`'s subscriber admitted, then validate the
    /// staged work, wait for the rest of the round's credit, or grant more.
    ///
    /// A page for no current work, or one after the work stopped receiving,
    /// already spent its credit and is dropped.
    fn handle_page(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        response: Headers,
        admitted: Admitted,
    ) {
        let Some(active) = self
            .peer_work_queue
            .active(&peer)
            .filter(|active| active.awaits_page(admitted.id, session_id))
        else {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        };
        self.emit_header_response(
            hs_trace::HEADER_RESPONSE_RECEIVED,
            &peer,
            session_id,
            active.owner.header_authority(),
            response.subscription_id,
            response.initial_target_tip_hash,
            response.common_ancestor_height,
            response.common_ancestor_hash,
            response.entries.len(),
            admitted.reaches_target,
            response.tree_aux_schema,
        );
        if !self.repair_page_accepted(&peer, session_id, &response, admitted.reaches_target) {
            return;
        }
        let returned_ancestor = zakura_header_chain::Frontier::new(
            response.common_ancestor_height,
            response.common_ancestor_hash,
        );
        let page_matches = self.peer_work_queue.active(&peer).is_some_and(|active| {
            active.matches_response_page(response.initial_target_tip_hash, returned_ancestor)
        });
        if !page_matches
            || !self.peer_work_queue.stage_page(
                &peer,
                returned_ancestor,
                response.entries,
                admitted.cursor,
            )
        {
            self.reject_page(peer, session_id);
            return;
        }
        self.reset_unproductive_requests(&peer, session_id);
        self.state
            .arm_request_deadline(peer.clone(), Instant::now(), self.startup.request_timeout);

        let active = self
            .peer_work_queue
            .active(&peer)
            .expect("the page was just staged on this work");
        let facts = self.page_facts(&peer, active, admitted.reaches_target);
        let next = page_next(
            active,
            facts,
            || self.peer_work_queue.budget_is_full(),
            || self.durable_prefix_full(active),
        );
        if next.closes_subscription() {
            self.close_subscription(&peer);
        }
        match next {
            PageNext::WaitCredit => {}
            PageNext::Malformed { .. } => self.reject_page(peer, session_id),
            PageNext::Prepare { bounded_prefix } => {
                if bounded_prefix && facts.checkpoint_prefix_ready {
                    metrics::counter!("sync.header.checkpoint_prefix.prepared.total").increment(1);
                }
                self.prepare_staged_work(&peer, bounded_prefix);
            }
            PageNext::Continue { staged_tip } => self.continue_round(&peer, staged_tip),
        }
    }

    /// Judge a page for `peer`'s selected-range repair, and retry the repair
    /// when it fails. Ordinary work has nothing to judge here.
    #[cfg_attr(not(any(test, zakura_sim)), allow(unused_variables))]
    fn repair_page_accepted(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        response: &Headers,
        reaches_target: bool,
    ) -> bool {
        let Some(active) = self.peer_work_queue.active(peer).filter(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
        }) else {
            return true;
        };
        let source = active.source;
        let owner = active
            .owner
            .body_owner()
            .expect("an auxiliary repair has body authority");
        let episode = self
            .peer_work_queue
            .repair_episode(active.owner)
            .expect("an active repair binds its evidence episode");
        let context = self
            .state
            .vct_repair
            .get(owner)
            .and_then(RepairRequirement::assigned_context);
        match repair_page(active, episode, context, response, reaches_target) {
            RepairPage::Accepted => true,
            RepairPage::Malformed => {
                self.retry_vct_repair(
                    owner,
                    VctRepairRetry::supplier(source, HeaderRequestTerminal::MalformedResponse),
                );
                sim_arg!(self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage); session_id);
                false
            }
            RepairPage::ExcludedInput => {
                metrics::counter!("sync.header.vct.repair.excluded_input.total").increment(1);
                self.retry_vct_repair(owner, VctRepairRetry::excluded_input(source));
                false
            }
        }
    }

    /// Retire `peer`'s work over a page its session sent, and record the violation.
    #[cfg_attr(not(any(test, zakura_sim)), allow(unused_variables))]
    fn reject_page(&mut self, peer: ZakuraPeerId, session_id: u64) {
        self.retire_peer_work(&peer, HeaderRequestTerminal::MalformedResponse);
        sim_arg!(self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage); session_id);
    }

    /// Read the lock-free facts that decide what follows the page just staged
    /// on `active`. [`page_next`] reads the locked ones only when it needs them.
    fn page_facts(
        &self,
        peer: &ZakuraPeerId,
        active: &ActiveHeaderRequest,
        reaches_target: bool,
    ) -> PageFacts {
        PageFacts {
            reaches_target,
            reserved: self.peer_work_queue.reserved_header_count(peer),
            checkpoint_prefix_ready: self
                .committed_snapshot
                .as_ref()
                .is_some_and(|snapshot| Self::should_prepare_checkpoint_prefix(snapshot, active)),
        }
    }

    /// Whether the durable prefix has no room for `active`'s next grant.
    fn durable_prefix_full(&self, active: &ActiveHeaderRequest) -> bool {
        self.committed_snapshot.as_ref().is_some_and(|snapshot| {
            Self::request_header_prefix_remaining(
                snapshot,
                self.peer_work_queue.claimed_header_count(),
                active.target.status.selected_tip_height,
            ) == 0
        })
    }

    /// Dispatch the prepare that validates `peer`'s staged headers.
    fn prepare_staged_work(&mut self, peer: &ZakuraPeerId, bounded_prefix: bool) {
        let action = self
            .peer_work_queue
            .begin_prepare(peer, bounded_prefix)
            .expect("the decided work is receiving and holds staged headers");
        let active = self
            .peer_work_queue
            .active(peer)
            .expect("the prepare's round stays active");
        let (owner, source) = (active.owner, active.source);
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        if is_repair {
            let body_owner = owner
                .body_owner()
                .expect("an auxiliary repair has body authority");
            let Some(task) = self.state.vct_repair.get(body_owner) else {
                self.retire_peer_work(peer, HeaderRequestTerminal::RepairObsolete);
                return;
            };
            if !matches!(task.state, RepairPolicyState::Assigned { .. }) {
                self.retry_vct_repair(
                    body_owner,
                    VctRepairRetry::local(source, HeaderRequestTerminal::RepairObsolete),
                );
                return;
            }
        }
        if self.dispatch_action(action).is_refused() {
            self.fail_work(
                peer,
                owner,
                is_repair,
                VctRepairRetry::local(source, HeaderRequestTerminal::LocalError),
                HeaderRequestTerminal::LocalError,
            );
        }
    }

    /// End `peer`'s failed work: a repair retries as `retry`, and any other work
    /// retires as `terminal`.
    fn fail_work(
        &mut self,
        peer: &ZakuraPeerId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        is_repair: bool,
        retry: VctRepairRetry,
        terminal: HeaderRequestTerminal,
    ) {
        if is_repair {
            self.retry_vct_repair(
                owner
                    .body_owner()
                    .expect("an auxiliary repair has body authority"),
                retry,
            );
        } else {
            self.retire_peer_work(peer, terminal);
        }
    }

    /// Grant `peer`'s round more credit toward its target from `staged_tip`.
    fn continue_round(&mut self, peer: &ZakuraPeerId, staged_tip: zakura_header_chain::Frontier) {
        let active = self
            .peer_work_queue
            .active(peer)
            .expect("the decided work is active");
        let locator = active.continuation_locator(staged_tip);
        let negotiated_header_count = active.max_header_count;
        let tree_aux_schema = active.tree_aux_schema;
        let target_tip_hash = active.target.status.selected_tip_hash;
        let request_id = active.request_id;
        let target_tip_height = active.target.status.selected_tip_height;
        let request_scope = active.owner.header_authority();
        let max_header_count = self
            .peer_work_queue
            .reservable_header_count(negotiated_header_count)
            .min(self.committed_snapshot.as_ref().map_or(0, |snapshot| {
                Self::request_header_prefix_remaining(
                    snapshot,
                    self.peer_work_queue.claimed_header_count(),
                    target_tip_height,
                )
            }));
        debug_assert!(
            max_header_count > 0,
            "a full owned or durable prefix returned before continuation"
        );
        if max_header_count == 0 || !self.peer_work_queue.reserve_request(peer, max_header_count) {
            self.retire_peer_work(peer, HeaderRequestTerminal::StagingRefused);
            return;
        }
        let Some(session) = self
            .sessions
            .get(peer)
            .map(|handles| handles.session.clone())
        else {
            self.retire_peer_work(peer, HeaderRequestTerminal::Disconnected);
            return;
        };
        // Every staged header and the new reservation make up the window, so
        // the grant adds exactly the new reservation.
        let staging = u32::try_from(self.peer_work_queue.owned_header_count(peer))
            .expect("the header budget capacity fits u32")
            .saturating_add(max_header_count);
        match session.try_grant(staging) {
            Ok(true) => {
                self.emit_header_request(
                    peer,
                    session.session_id(),
                    request_scope,
                    request_id,
                    target_tip_hash,
                    &locator,
                    max_header_count,
                    tree_aux_schema,
                );
            }
            Ok(false) => {
                self.peer_work_queue.cancel_request_reservation(peer);
                self.retire_peer_work(peer, HeaderRequestTerminal::StagingRefused);
            }
            Err(error) => {
                self.peer_work_queue.cancel_request_reservation(peer);
                self.emit_queue_send_failed(peer, &session, "SubscribeHeaders", &error, None);
                self.retire_peer_work(peer, HeaderRequestTerminal::SendError);
            }
        }
    }

    /// End `peer`'s receiving work when its subscription's terminal outcome
    /// arrives. An outcome for work that already holds its pages, or for a
    /// subscription this node closed, changes nothing.
    fn handle_outcome(&mut self, peer: ZakuraPeerId, session_id: u64, ended: Ended) {
        // A terminal outcome lets the session open again, which an offer held
        // by `Self::subscription_ready` waits for.
        self.state.bump_scheduling_revision();
        // The session holds one live or closing subscription, so this is the
        // outcome of any `Close` this node sent it.
        self.state.settle_close(&peer, session_id);
        let Some(active) = self.peer_work_queue.active(&peer) else {
            return;
        };
        if active.request_id.get() != ended.id || active.owner.session_id() != session_id {
            return;
        }
        // Remember a pruned publisher even when the round already holds its
        // pages, so the next round picks its schema from it.
        if ended.outcome == HeadersOutcomeCode::HistoryPruned
            && matches!(active.purpose, HeaderTargetPurpose::Normal)
        {
            let schema = active.tree_aux_schema;
            self.note_history_pruned(&peer, schema);
        }
        let Some(active) = self.peer_work_queue.active(&peer) else {
            return;
        };
        if active.phase != HeaderTargetPhase::Receiving {
            return;
        }
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        let terminal_outcome = HeaderRequestTerminal::from(ended.outcome);
        if is_repair {
            self.retry_vct_repair(
                active
                    .owner
                    .body_owner()
                    .expect("an auxiliary repair has body authority"),
                VctRepairRetry::supplier(active.source, terminal_outcome),
            );
            metrics::counter!(
                "sync.header.vct.repair.outcome.total",
                "outcome" => format!("{:?}", ended.outcome)
            )
            .increment(1);
        } else {
            self.retire_peer_work(&peer, terminal_outcome);
            metrics::counter!(
                "sync.header.target.outcome",
                "outcome" => format!("{:?}", ended.outcome)
            )
            .increment(1);
        }
    }

    /// Whether `retained`'s call is exactly the one that `peer`'s current round waits
    /// for. An external driver's result carries no call and matches its work by
    /// phase, source, and owner alone.
    fn settles_current_call(
        &mut self,
        peer: &ZakuraPeerId,
        retained: Option<&RetainedHeaders>,
    ) -> bool {
        retained.is_none_or(|retained| self.peer_work_queue.settle_call(peer, retained.work()))
    }

    fn handle_target_admission(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetAdmissionResult,
        mut retained: Option<RetainedHeaders>,
    ) {
        self.finish_vct_local_operation(owner, HeaderTargetPhase::Applying);
        end_retained(&mut retained, HeaderWorkEnd::Stale);
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        if active.phase != HeaderTargetPhase::Applying
            || active.source != source
            || active.owner != owner
            || !self.settles_current_call(&peer, retained.as_ref())
        {
            return;
        }
        end_retained(&mut retained, HeaderWorkEnd::Settled);
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        let completion_authority =
            match self.registered_completion_authority(&peer, source, &owner, is_repair) {
                Ok((authority, outcome)) => {
                    metrics::counter!(
                        "sync.header.target.completion_gate.total",
                        "outcome" => outcome
                    )
                    .increment(1);
                    authority
                }
                Err(reason) => {
                    Self::record_stale_completion(reason);
                    self.fail_work(
                        &peer,
                        owner,
                        is_repair,
                        VctRepairRetry::local(source, HeaderRequestTerminal::RepairObsolete),
                        HeaderRequestTerminal::SnapshotObsolete,
                    );
                    return;
                }
            };
        let repair_generation = match active.purpose {
            HeaderTargetPurpose::Normal => None,
            HeaderTargetPurpose::SelectedAuxiliaryRepair {
                repair_generation, ..
            } => Some(repair_generation),
        };
        let repair_header_count = owner
            .body_owner()
            .and_then(|repair_owner| self.state.vct_repair.get(repair_owner))
            .and_then(|task| match &task.state {
                RepairPolicyState::Assigned { context } => Some(context.selected_header_count()),
                _ => None,
            });
        match &result {
            HeaderTargetAdmissionResult::Applied => {
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_ADMITTED,
                    "apply",
                    &peer,
                    owner,
                    None,
                );
                // Every header through the last staged page passed validation,
                // so the subscription's next update acknowledges that page.
                if let (Some(cursor), Some(handles)) = (active.cursor, self.sessions.get(&peer)) {
                    if handles.session.session_id() == owner.session_id() {
                        handles.session.accept(active.request_id.get(), &cursor);
                    }
                }
                // An admitted ordinary round ends the peer's `HistoryPruned`
                // pause, and one with records ends its schema fallback.
                if active.purpose == HeaderTargetPurpose::Normal {
                    self.state
                        .note_ordinary_round_admitted(&peer, active.tree_aux_schema);
                }
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_REJECTED,
                    "apply",
                    &peer,
                    owner,
                    Some(error),
                );
            }
            HeaderTargetAdmissionResult::ResourceStalled(receipt) => {
                tracing::warn!(
                    ?receipt,
                    "header target admission stopped by a committed local resource alarm"
                );
                metrics::counter!(
                    "sync.header.target.resource_stalled.total",
                    "alarm_changed" => receipt.alarm_changed.to_string()
                )
                .increment(1);
            }
        }
        self.retire_peer_work(
            &peer,
            match &result {
                HeaderTargetAdmissionResult::Applied => HeaderRequestTerminal::TargetAdmitted,
                HeaderTargetAdmissionResult::Failed(_) => HeaderRequestTerminal::TargetRejected,
                HeaderTargetAdmissionResult::ResourceStalled(_) => {
                    HeaderRequestTerminal::ResourceStalled
                }
            },
        );
        if repair_generation.is_some() {
            let repair_owner = owner
                .body_owner()
                .expect("an auxiliary repair admission has body authority");
            match result {
                HeaderTargetAdmissionResult::Applied => {
                    self.state.complete_repair(repair_owner, source);
                    if let Some(task) = self.state.vct_repair.get(repair_owner) {
                        self.emit_vct_repair_state(task, "admission", Some("applied"));
                    }
                    metrics::counter!("sync.header.vct.repair.admitted.total").increment(1);
                    metrics::counter!("sync.header.vct.repair.admitted.headers").increment(
                        u64::try_from(repair_header_count.unwrap_or(1))
                            .expect("a bounded repair count fits u64"),
                    );
                }
                HeaderTargetAdmissionResult::Failed(error) => {
                    if error.is_auxiliary_capacity_refusal() {
                        let blocked_at = self
                            .committed_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.state_version)
                            .or_else(|| {
                                self.state.vct_repair.get(repair_owner).and_then(|task| {
                                    let RepairPolicyState::Assigned { context } = &task.state
                                    else {
                                        return None;
                                    };
                                    Some(context.state_version)
                                })
                            });
                        let blocked = blocked_at.is_some_and(|state_version| {
                            self.state
                                .vct_repair
                                .get_mut(repair_owner)
                                .is_some_and(|task| {
                                    task.wait_for_state_change(state_version).is_ok()
                                })
                        });
                        if blocked {
                            self.replay_committed_state_version(repair_owner);
                            if let Some(task) = self.state.vct_repair.get(repair_owner) {
                                self.emit_vct_repair_state(
                                    task,
                                    "wait",
                                    Some("auxiliary_capacity"),
                                );
                            }
                        } else {
                            self.retry_vct_repair(
                                repair_owner,
                                VctRepairRetry::local(source, HeaderRequestTerminal::LocalError),
                            );
                        }
                        metrics::counter!("sync.header.vct.repair.auxiliary_capacity.total")
                            .increment(1);
                    } else {
                        self.retry_vct_repair(
                            repair_owner,
                            vct_error_retry(source, HeaderRequestTerminal::TargetRejected, &error),
                        );
                    }
                    self.handle_typed_failure(peer, source, Some(owner), &error);
                }
                HeaderTargetAdmissionResult::ResourceStalled(receipt) => {
                    let blocked = self
                        .state
                        .vct_repair
                        .get_mut(repair_owner)
                        .is_some_and(|task| {
                            task.wait_for_state_change(receipt.state_version).is_ok()
                        });
                    if blocked {
                        self.replay_committed_state_version(repair_owner);
                        if let Some(task) = self.state.vct_repair.get(repair_owner) {
                            self.emit_vct_repair_state(task, "wait", Some("resource_state_change"));
                        }
                    }
                    metrics::counter!("sync.header.vct.repair.resource_stalled.total").increment(1);
                }
            }
            return;
        }
        match result {
            HeaderTargetAdmissionResult::Applied => {
                self.state.completed_targets.mark(
                    completion_authority.header_generation,
                    completion_authority.branch,
                );
                metrics::counter!("sync.header.target.admitted").increment(1);
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.handle_typed_failure(peer, source, Some(owner), &error);
            }
            HeaderTargetAdmissionResult::ResourceStalled(receipt) => {
                self.state.note_resource_stall(receipt.state_version);
            }
        }
    }

    fn handle_target_prepared(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetPreparationResult,
        mut retained: Option<RetainedHeaders>,
    ) {
        self.finish_vct_local_operation(owner, HeaderTargetPhase::Preparing);
        end_retained(&mut retained, HeaderWorkEnd::Stale);
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        let purpose = active.purpose;
        let is_repair = matches!(purpose, HeaderTargetPurpose::SelectedAuxiliaryRepair { .. });
        if active.phase != HeaderTargetPhase::Preparing
            || active.source != source
            || active.owner != owner
            || !self.settles_current_call(&peer, retained.as_ref())
        {
            return;
        }
        if !self.preparation_has_authority(
            &peer,
            source,
            &owner,
            is_repair,
            self.peer_work_queue.owned_header_count(&peer),
        ) {
            end_retained(&mut retained, HeaderWorkEnd::NoAuthority);
            self.fail_work(
                &peer,
                owner,
                is_repair,
                VctRepairRetry::local(source, HeaderRequestTerminal::RepairObsolete),
                HeaderRequestTerminal::SnapshotObsolete,
            );
            return;
        }
        match result {
            HeaderTargetPreparationResult::Prepared(target) => {
                if target.owner() != owner || target.source() != source {
                    end_retained(&mut retained, HeaderWorkEnd::OwnerMismatch);
                    return;
                }
                if is_repair {
                    let repair_owner = owner
                        .body_owner()
                        .expect("an auxiliary repair preparation has body authority");
                    let valid = self.state.vct_repair.get(repair_owner).is_some_and(|task| {
                        let RepairPolicyState::Assigned { context } = &task.state else {
                            return false;
                        };
                        target.target_tip_hash() == context.request_target().hash
                            && target.auxiliary_delivery_count() == context.selected_header_count()
                    });
                    if !valid {
                        end_retained(&mut retained, HeaderWorkEnd::RepairInvalid);
                        self.retry_vct_repair(
                            repair_owner,
                            VctRepairRetry::local(source, HeaderRequestTerminal::RepairObsolete),
                        );
                        return;
                    }
                }
                self.state.arm_request_deadline(
                    peer.clone(),
                    Instant::now(),
                    self.startup.request_timeout,
                );
                assert!(
                    self.peer_work_queue.begin_apply(&peer),
                    "the exact preparing request passed the completion gate"
                );
                let dispatch = self.dispatch_holding(
                    HeaderPortOperation::ApplyHeaderTarget {
                        purpose: purpose.clone(),
                        peer: peer.clone(),
                        source,
                        owner,
                        target,
                    },
                    retained.take(),
                );
                if dispatch.is_refused() {
                    self.fail_work(
                        &peer,
                        owner,
                        is_repair,
                        VctRepairRetry::local(source, HeaderRequestTerminal::LocalError),
                        HeaderRequestTerminal::LocalError,
                    );
                }
            }
            HeaderTargetPreparationResult::Failed(error) => {
                end_retained(&mut retained, HeaderWorkEnd::PrepareFailed);
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_REJECTED,
                    "prepare",
                    &peer,
                    owner,
                    Some(&error),
                );
                self.fail_work(
                    &peer,
                    owner,
                    is_repair,
                    vct_error_retry(source, HeaderRequestTerminal::TargetRejected, &error),
                    HeaderRequestTerminal::TargetRejected,
                );
                self.handle_typed_failure(peer, source, Some(owner), &error);
            }
        }
    }

    /// Replay the newest committed state version against one just-blocked repair.
    ///
    /// Repair state changes are edge-triggered. A refusal can name a version the reactor has
    /// already observed, and `StateBlocked` has no maintenance deadline, so a task blocked at a
    /// superseded coordinate would wait for a snapshot that already arrived.
    fn replay_committed_state_version(&mut self, owner: zakura_header_chain::BodyWorkOwner) {
        let Some(state_version) = self
            .committed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.state_version)
        else {
            return;
        };
        if let Some(task) = self.state.vct_repair.get_mut(owner) {
            task.observe_state_change(state_version);
        }
    }

    /// Settle `key`'s locator read and use it for the peer's awaiting target.
    ///
    /// See [`state::LocatorQueries::settle`] for when a read serves the target.
    fn finish_header_locator_query(
        &mut self,
        key: LocatorKey,
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        let target = self.peer_work_queue.awaiting_target(&key.peer).cloned();
        let disposition = self
            .state
            .locators
            .settle(&key, target.as_ref(), locator.is_some());
        if disposition == LocatorDisposition::Stale {
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        }
        let peer = key.peer;
        let target = target.expect("only a read for an awaiting target settles as usable");
        if disposition == LocatorDisposition::Refetch {
            // Keep the current staged target and fetch a fresh locator for it.
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            if self
                .dispatch_action(HeaderPortOperation::QueryHeaderLocator {
                    peer: peer.clone(),
                    session_id: target.session_id,
                    target_tip_hash: target.status.selected_tip_hash,
                    scope: target.scope,
                })
                .is_refused()
            {
                self.peer_work_queue.remove_unstarted(&peer);
            }
            return;
        }
        self.handle_header_locator_ready(
            peer,
            target.session_id,
            target.status.selected_tip_hash,
            target.scope,
            locator,
        );
    }

    fn handle_header_locator_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        target_tip_hash: block::Hash,
        scope: zakura_header_chain::HeaderWorkAuthority,
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        let Some(target) = self
            .peer_work_queue
            .awaiting(&peer, session_id, target_tip_hash, scope)
            .cloned()
        else {
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        };
        let Some(locator) = locator else {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.locator_unavailable").increment(1);
            return;
        };
        let Some(local) = self.committed_snapshot.clone() else {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        };
        if !target.is_current(&local) {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        }
        if self.ordinary_discovery_paused() {
            self.defer_ordinary_target(&peer);
            return;
        }
        let Some(session) = self
            .sessions
            .get(&peer)
            .map(|handles| handles.session.clone())
        else {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        };
        if locator
            .entries()
            .iter()
            .any(|entry| entry.hash == target_tip_hash)
        {
            // The target is already in the local DAG, so no other peer needs to fetch it.
            self.state
                .completed_targets
                .mark(target.scope.header_generation, target.scope.branch);
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.already_known.total").increment(1);
            return;
        }
        let (negotiated_schema, negotiated_header_count) =
            self.negotiated_response_limits(&target.status);
        let max_header_count = negotiated_header_count.min(Self::request_header_prefix_remaining(
            &local,
            self.peer_work_queue.claimed_header_count(),
            target.status.selected_tip_height,
        ));
        let max_header_count = self
            .peer_work_queue
            .reservable_header_count(max_header_count);
        // A subscription that stopped at a committed prefix continues from it,
        // while its schema still fits the round.
        let now = Instant::now();
        let resumable = session.subscriber().resumable();
        let resumed = resumable.filter(|&(_, tip, accepted, schema)| {
            let accepted_frontier =
                zakura_header_chain::Frontier::new(accepted.height, accepted.hash);
            tip == target_tip_hash
                && locator.entries().contains(&accepted_frontier)
                && self.state.ordinary_schema(
                    &peer,
                    schema,
                    &target.status,
                    accepted.height,
                    max_header_count,
                    now,
                ) == schema
        });
        if session.subscriber().live().is_some() && resumed.is_none() {
            // The next `Open` follows the live subscription's outcome.
            self.close_subscription(&peer);
            self.defer_ordinary_target(&peer);
            return;
        }
        let tree_aux_schema = match resumed {
            Some((_, _, _, schema)) => schema,
            None => locator.entries().first().map_or(AuxSchema::None, |start| {
                self.state.ordinary_schema(
                    &peer,
                    negotiated_schema,
                    &target.status,
                    start.height,
                    max_header_count,
                    now,
                )
            }),
        };
        if max_header_count == 0
            || !self
                .peer_work_queue
                .reserve_request(&peer, max_header_count)
        {
            self.defer_ordinary_target(&peer);
            return;
        }
        let source = source_id_from_peer(&peer);
        let (sent, locator) = match resumed {
            Some((id, _, accepted, _)) => {
                let accepted = zakura_header_chain::Frontier::new(accepted.height, accepted.hash);
                let sent = match session.try_grant(max_header_count) {
                    Ok(true) => {
                        Ok(HeaderSyncRequestId::new(id).expect("subscription IDs are nonzero"))
                    }
                    Ok(false) => Err(OrderedSendError::Full),
                    Err(error) => Err(error),
                };
                (
                    sent,
                    zakura_header_chain::HeaderLocator::for_continuation(accepted),
                )
            }
            None => (
                session.try_open(
                    &target.status,
                    locator.entries(),
                    tree_aux_schema,
                    max_header_count,
                ),
                locator,
            ),
        };

        match sent {
            Ok(request_id) => {
                self.emit_header_request(
                    &peer,
                    session_id,
                    target.scope,
                    request_id,
                    target_tip_hash,
                    &locator,
                    max_header_count,
                    tree_aux_schema,
                );
                let owner = target.scope.bind(
                    session_id,
                    NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
                );
                let started = self.peer_work_queue.start(ActiveHeaderRequest::new(
                    HeaderTargetPurpose::Normal,
                    peer.clone(),
                    source,
                    locator,
                    owner.into(),
                    target,
                    request_id,
                    negotiated_header_count,
                    tree_aux_schema,
                ));
                debug_assert!(
                    started,
                    "the matching locator was checked before publication"
                );
                if started {
                    observe!(
                        self,
                        ReactorEvent::started(
                            self.peer_work_queue
                                .active(&peer)
                                .expect("the request was just started")
                        )
                    );
                    self.state.arm_request_deadline(
                        peer.clone(),
                        Instant::now(),
                        self.startup.request_timeout,
                    );
                } else {
                    self.close_subscription(&peer);
                    self.peer_work_queue.remove_unstarted(&peer);
                }
                metrics::counter!("sync.header.target.requested").increment(1);
            }
            Err(error) => {
                self.peer_work_queue.cancel_request_reservation(&peer);
                self.peer_work_queue.remove_unstarted(&peer);
                self.emit_queue_send_failed(&peer, &session, "SubscribeHeaders", &error, None);
                metrics::counter!(
                    "sync.header.target.send_failed",
                    "reason" => ordered_send_error_label(&error)
                )
                .increment(1);
            }
        }
    }

    /// Bound ordinary target prefixes by shared selected-path space in the durable DAG.
    ///
    /// Retention protects the complete selected path.
    /// Downloading more entries than this headroom can only cause a resource refusal.
    /// All response reservations and staged entries share the headroom.
    /// A protected side path can still make state refuse the insertion during application.
    fn durable_header_prefix_remaining(
        snapshot: &zakura_header_chain::EngineSnapshot,
        claimed: usize,
    ) -> u32 {
        let selected_non_finalized = snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.finalized.height.0);
        let selected_non_finalized = usize::try_from(selected_non_finalized)
            .expect("u32 header divergence fits usize on supported targets");
        let remaining = zakura_header_chain::MAX_NON_FINALIZED_NODES_V1
            .saturating_sub(selected_non_finalized)
            .saturating_sub(claimed);
        u32::try_from(remaining).unwrap_or(u32::MAX)
    }

    /// Publish a normal selected-chain extension once it holds a whole refill batch.
    ///
    /// The extension does not wait for the body backlog to drain or for new body credits.
    fn should_prepare_checkpoint_prefix(
        snapshot: &zakura_header_chain::EngineSnapshot,
        active: &ActiveHeaderRequest,
    ) -> bool {
        snapshot.mode == zakura_header_chain::EngineMode::Integrated
            && matches!(active.purpose, HeaderTargetPurpose::Normal)
            && active.common_ancestor == Some(snapshot.frontiers.header_best)
            && active.entries.len() >= HEADER_REFILL_BATCH_V1
    }

    /// Return requester headroom after both the durable DAG limit and the integrated body window.
    ///
    /// A drained window stays closed until it regains a whole refill batch of room.
    /// That bound applies to the window rather than to the grant.
    /// Other claims can leave less than a batch free, and the smaller top-up still keeps
    /// the body backlog supplied.
    /// A target the window already reaches is never withheld, so a short final suffix
    /// stays reachable.
    /// Reservations and staged entries consume the same bounded window.
    fn request_header_prefix_remaining(
        snapshot: &zakura_header_chain::EngineSnapshot,
        claimed: usize,
        target_tip_height: block::Height,
    ) -> u32 {
        let durable = Self::durable_header_prefix_remaining(snapshot, claimed);
        let body_lag = snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.verified_best.height.0);
        let body_window = INTEGRATED_HEADER_BODY_WINDOW_V1.saturating_sub(body_lag);
        let target_remaining = target_tip_height
            .0
            .saturating_sub(snapshot.frontiers.header_best.height.0);
        let refill_batch =
            u32::try_from(HEADER_REFILL_BATCH_V1).expect("the refill batch fits a block height");
        if body_window < refill_batch && target_remaining > body_window {
            return 0;
        }
        let claimed = u32::try_from(claimed).unwrap_or(u32::MAX);
        durable.min(body_window.saturating_sub(claimed))
    }

    fn observe_latest_committed_snapshot(&mut self, snapshot: zakura_header_chain::EngineSnapshot) {
        if self.committed_snapshot.as_ref() == Some(&snapshot) {
            return;
        }

        self.state.bump_scheduling_revision();
        let header_authority_changed = self.committed_snapshot.as_ref().is_some_and(|old| {
            old.header_generation != snapshot.header_generation
                || old.frontiers.finalized != snapshot.frontiers.finalized
        });
        self.emit_snapshot_observed(self.committed_snapshot.as_ref(), &snapshot);
        self.retire_obsolete_work(&snapshot);
        let old_tip = self
            .committed_snapshot
            .as_ref()
            .map(|old| old.frontiers.header_best);
        let new_tip = snapshot.frontiers.header_best;
        self.startup.frontiers = FullStateFrontiers {
            finalized_height: snapshot.frontiers.finalized.height,
            verified_block_tip: snapshot.frontiers.verified_best.height,
            verified_block_hash: snapshot.frontiers.verified_best.hash,
        };
        let status = Status::from_snapshot(&snapshot, self.page_limits);
        let now = Instant::now();
        self.committed_snapshot = Some(snapshot);
        self.schedule_current_vct_repair();
        self.request_vct_repair_context();
        for handles in self.sessions.values_mut() {
            match handles.status_publisher.as_mut() {
                Some(publisher) => publisher.observe(status.clone(), now),
                None => {
                    handles.status_publisher = Some(StatusPublisher::new(
                        status.clone(),
                        self.startup.status_refresh_interval,
                        now,
                    ));
                }
            }
        }
        if old_tip != Some(new_tip) {
            let _ = self.tip.send((new_tip.height, new_tip.hash));
            self.publish_peer_state();
        }
        self.refresh_statuses();
        if header_authority_changed {
            // Eligibility, completed targets, and locators all follow the header authority.
            self.state
                .restore_idle_offers(|peer| self.peer_work_queue.has_work(peer));
        }
        self.schedule_cached_statuses();
    }

    fn retire_obsolete_work(&mut self, snapshot: &zakura_header_chain::EngineSnapshot) {
        self.peer_work_queue.retire_obsolete_unstarted(snapshot);
        if let Some(task) = self.state.vct_repair.retain_current(snapshot) {
            if let Some(peer) = self
                .peer_work_queue
                .active_owner(task.owner.into())
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer, HeaderRequestTerminal::SnapshotObsolete);
            }
        } else if let Some(task) = self.state.vct_repair.current_mut() {
            task.observe_state_change(snapshot.state_version);
        }
        self.state
            .completed_targets
            .retain_current(snapshot.header_generation, snapshot.frontiers.finalized);
        for active in self.peer_work_queue.retire_obsolete_active(snapshot) {
            self.state.clear_request_deadline(&active.peer);
            self.emit_request_terminal(&active, HeaderRequestTerminal::SnapshotObsolete);
            self.cancel_active_request(&active);
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    fn send_status(&mut self, peer: &ZakuraPeerId) -> bool {
        let now = Instant::now();
        let Some((session, status)) = self.sessions.get(peer).and_then(|handles| {
            let publisher = handles.status_publisher.as_ref()?;
            publisher
                .due(now)
                .then(|| (handles.session.clone(), publisher.desired()))
        }) else {
            return false;
        };
        match session.try_send_status(status.clone()) {
            Ok(()) => {
                self.emit_status(
                    hs_trace::HEADER_STATUS_SENT,
                    peer,
                    session.session_id(),
                    &status,
                );
                if let Some(publisher) = self
                    .sessions
                    .get_mut(peer)
                    .and_then(|handles| handles.status_publisher.as_mut())
                {
                    publisher.record_sent(status, now);
                }
                metrics::counter!("sync.header.peer.status.sent").increment(1);
                true
            }
            Err(error) => {
                if let Some(publisher) = self
                    .sessions
                    .get_mut(peer)
                    .and_then(|handles| handles.status_publisher.as_mut())
                {
                    publisher.record_failed(now);
                }
                tracing::debug!(?peer, ?error, "failed to queue header-sync Status");
                self.emit_queue_send_failed(peer, &session, "Status", &error, None);
                false
            }
        }
    }

    fn refresh_statuses(&mut self) {
        let now = Instant::now();
        if self.report_fatal_vct_local_operation(now) {
            return;
        }
        self.refresh_vct_repair_stall(now);
        self.retire_timed_out_requests(now);
        self.state.prune_pruned_peers(now);
        self.retry_pending_closes(now);
        self.retire_unanswered_closes(now);
        if self.state.prune_readmission_cooldowns(now) {
            self.publish_peer_state();
        }
        let peers: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, handles)| {
                handles
                    .status_publisher
                    .as_ref()
                    .is_some_and(|publisher| publisher.due(now))
            })
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in peers {
            self.send_status(&peer);
        }
        self.request_vct_repair_context();
        self.try_assign_vct_repair();
    }

    fn next_maintenance_deadline(&self) -> Instant {
        let status_deadline = self
            .sessions
            .values()
            .filter_map(|handles| {
                handles
                    .status_publisher
                    .as_ref()
                    .map(StatusPublisher::next_deadline)
            })
            .min();
        status_deadline
            .into_iter()
            .chain(self.state.next_deadline())
            .min()
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(60))
    }

    fn retire_timed_out_requests(&mut self, now: Instant) {
        for peer in self.state.timed_out_requests(now) {
            let repair = self.peer_work_queue.active(&peer).and_then(|active| {
                matches!(
                    active.purpose,
                    HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
                )
                .then(|| {
                    (
                        active
                            .owner
                            .body_owner()
                            .expect("an auxiliary repair has body authority"),
                        active.source,
                        active.phase,
                    )
                })
            });
            if let Some((owner, source, phase)) = repair {
                match phase {
                    HeaderTargetPhase::Receiving => {
                        if let Some(task) = self.state.vct_repair.get(owner) {
                            self.emit_vct_repair_state(task, "timeout", Some("timed_out"));
                        }
                        let session_id = self
                            .peer_work_queue
                            .active(&peer)
                            .map(|active| active.owner.session_id());
                        self.retry_vct_repair(
                            owner,
                            VctRepairRetry::supplier(source, HeaderRequestTerminal::TimedOut),
                        );
                        // A repair reserves aggregate header budget that ordinary staging cannot
                        // use, so a supplier that withholds its response until the deadline must
                        // be charged like any other unresponsive peer.
                        if let Some(session_id) = session_id {
                            self.charge_unproductive_request(&peer, session_id, "unresponsive");
                        }
                        metrics::counter!("sync.header.vct.repair.timed_out.total").increment(1);
                    }
                    HeaderTargetPhase::Preparing | HeaderTargetPhase::Applying => {
                        let Some(task) = self.state.vct_repair.get(owner).cloned() else {
                            self.retire_peer_work(&peer, HeaderRequestTerminal::RepairObsolete);
                            continue;
                        };
                        self.emit_vct_repair_state(
                            &task,
                            "local_operation",
                            Some(match phase {
                                HeaderTargetPhase::Preparing => "preparing_pending",
                                HeaderTargetPhase::Applying => "applying_pending",
                                HeaderTargetPhase::Receiving => unreachable!(
                                    "the receiving phase is handled before local operation polling"
                                ),
                            }),
                        );
                        if let Some(predecessor) = vct_repair_predecessor(&task) {
                            self.note_vct_repair_stall(
                                &task,
                                predecessor,
                                VctSupplierRejections::default(),
                                VctRepairStallOutcome::LocalOperationPending,
                                now,
                            );
                        }
                        self.state
                            .arm_request_deadline(peer, now, self.startup.request_timeout);
                        metrics::counter!(
                            "sync.header.vct.repair.local_operation_pending.total",
                            "phase" => match phase {
                                HeaderTargetPhase::Preparing => "preparing",
                                HeaderTargetPhase::Applying => "applying",
                                HeaderTargetPhase::Receiving => unreachable!(
                                    "the receiving phase is handled before local operation polling"
                                ),
                            }
                        )
                        .increment(1);
                    }
                }
            } else {
                let session_id = self
                    .peer_work_queue
                    .active(&peer)
                    .map(|active| active.owner.session_id());
                self.retire_peer_work(&peer, HeaderRequestTerminal::TimedOut);
                metrics::counter!("sync.header.target.timed_out.total").increment(1);
                if let Some(session_id) = session_id {
                    self.charge_unproductive_request(&peer, session_id, "unresponsive");
                }
            }
        }
    }

    fn admitted_count(&self, direction: ServicePeerDirection) -> usize {
        self.sessions
            .values()
            .filter(|handles| handles.session.direction() == direction)
            .count()
    }

    fn publish_peer_state(&self) {
        let snapshot = ServicePeerSnapshot::new(
            self.admitted_count(ServicePeerDirection::Inbound),
            self.admitted_count(ServicePeerDirection::Outbound),
            self.startup.config.peer_limits,
        );
        let _ = self.peers.send(snapshot);
        let mut admitted_node_ids: Vec<_> =
            self.sessions.keys().filter_map(node_id_from_peer).collect();
        admitted_node_ids.sort_unstable();
        admitted_node_ids.dedup();
        let now = Instant::now();
        let mut backed_off_node_ids: Vec<_> = self
            .state
            .unproductive_peer_cooldowns
            .iter()
            .filter(|(_, until)| **until > now)
            .filter_map(|(peer, _)| node_id_from_peer(peer))
            .collect();
        backed_off_node_ids.sort_unstable();
        backed_off_node_ids.dedup();
        let tip = *self.tip.borrow();
        let _ = self.candidates.send(ZakuraHeaderSyncCandidateState {
            target_height: next_height(tip.0),
            admitted_node_ids,
            backed_off_node_ids,
        });
    }

    fn registered_completion_authority(
        &self,
        peer: &ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::HeaderSyncWorkOwner,
        is_repair: bool,
    ) -> Result<
        (zakura_header_chain::HeaderWorkAuthority, &'static str),
        zakura_header_chain::StaleReason,
    > {
        let Some(current) = self.committed_snapshot.as_ref() else {
            return Err(zakura_header_chain::StaleReason::MissingOwner);
        };
        let decision = zakura_header_chain::Gate::check_registered(
            current,
            self.peer_work_queue.registered_attempt(peer),
            source,
            owner,
        );
        match decision {
            zakura_header_chain::CompletionDecision::Current => {
                Ok((owner.header_authority(), "current"))
            }
            zakura_header_chain::CompletionDecision::Stale(reason) => {
                let header = owner.header_authority();
                let registered =
                    self.peer_work_queue.registered_attempt(peer) == Some((source, *owner));
                let rebase_candidate = !is_repair
                    && owner.body_owner().is_none()
                    && registered
                    && header.header_generation.get() < current.header_generation.get()
                    && header.branch.anchor_hash != current.frontiers.finalized.hash;
                if !rebase_candidate {
                    return Err(reason);
                }
                Ok((
                    zakura_header_chain::HeaderWorkAuthority::for_target(
                        current,
                        header.branch.target_tip_hash,
                    ),
                    "rebase_candidate",
                ))
            }
        }
    }

    /// Keep exact ordinary header work alive across a monotone full-state finality advance.
    ///
    /// The rebase gate grants no durable authority.
    /// The serialized state planner authenticates the finality path and proves ancestry.
    /// The planner trims any finalized prefix and then rebases or rejects the insertion.
    /// Body-authorized VCT repair work remains bound to each generation.
    fn preparation_has_authority(
        &self,
        peer: &ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::HeaderSyncWorkOwner,
        is_repair: bool,
        header_count: usize,
    ) -> bool {
        let outcome = match self.registered_completion_authority(peer, source, owner, is_repair) {
            Ok((_, outcome)) => outcome,
            Err(reason) => {
                Self::record_stale_completion(reason);
                return false;
            }
        };
        let header_count = u64::try_from(header_count)
            .expect("the bounded header target count fits in a metric counter");
        metrics::counter!(
            "sync.header.target.preparation_gate.total",
            "outcome" => outcome
        )
        .increment(1);
        metrics::counter!(
            "sync.header.target.preparation_gate.headers.total",
            "outcome" => outcome
        )
        .increment(header_count);
        true
    }

    fn record_stale_completion(reason: zakura_header_chain::StaleReason) {
        metrics::counter!(
            "sync.header_chain.stale_completion.total",
            "kind" => format!("{reason:?}")
        )
        .increment(1);
    }

    /// Charges one unproductive request against `peer`'s exact session, dropping that
    /// session once it reaches the configured limit.
    ///
    /// Charge a strike only to the session that raised it.
    /// Discard a strike when a replacement session already exists.
    /// Return whether the reactor dropped the session.
    fn charge_unproductive_request(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) -> bool {
        let limit = self.startup.config.max_unproductive_header_requests;
        if !self.is_current_session(peer, session_id) {
            return false;
        }
        if !self.state.charge_unproductive_request(peer, limit) {
            return false;
        }
        self.drop_unproductive_peer(peer, session_id, reason);
        true
    }

    /// Clears `peer`'s strike count after its session supplies usable work.
    fn reset_unproductive_requests(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if self.is_current_session(peer, session_id) {
            self.state.reset_unproductive_requests(peer);
        }
    }

    /// Whether `session_id` names `peer`'s admitted session.
    fn is_current_session(&self, peer: &ZakuraPeerId, session_id: u64) -> bool {
        self.sessions
            .get(peer)
            .is_some_and(|handles| handles.session.session_id() == session_id)
    }

    /// Closes one exact unproductive session and blocks its readmission for the cooldown.
    fn drop_unproductive_peer(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) {
        let Some(handles) = self.sessions.get(peer) else {
            return;
        };
        if handles.session.session_id() != session_id {
            return;
        }
        handles.session.cancel_token().cancel();

        self.state.start_readmission_cooldown(
            peer,
            Instant::now(),
            self.startup.config.unproductive_peer_cooldown,
        );
        metrics::counter!("sync.header.peer.dropped.total", "reason" => reason).increment(1);
        let _ = self.dispatch_action(HeaderPortOperation::DropPeer {
            peer: peer.clone(),
            session_id,
            reason,
        });
        self.handle_peer_disconnected(peer, session_id, reason);
    }

    fn retire_peer_work(&mut self, peer: &ZakuraPeerId, terminal_outcome: HeaderRequestTerminal) {
        self.state.clear_request_deadline(peer);
        let reserved = self.peer_work_queue.reserved_header_count(peer);
        let owned = self.peer_work_queue.owned_header_count(peer);
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.emit_request_terminal(&active, terminal_outcome);
            self.cancel_active_request(&active);
        }
        let released = reserved.saturating_add(owned);
        if released != 0 {
            metrics::counter!(
                "sync.header.chunk_budget.released.total",
                "terminal" => terminal_outcome.label()
            )
            .increment(u64::try_from(released).unwrap_or(u64::MAX));
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    #[cfg(test)]
    fn clear_peer_work_for_test(&mut self, peer: &ZakuraPeerId) {
        self.state.clear_request_deadline(peer);
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.cancel_active_request(&active);
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    fn retire_all_peer_work(&mut self, terminal_outcome: HeaderRequestTerminal) {
        let peers: Vec<_> = self.sessions.keys().cloned().collect();
        for peer in peers {
            self.retire_peer_work(&peer, terminal_outcome);
        }
    }

    /// Close `active`'s subscription, unless a `Grant` can continue it toward
    /// its target after a committed prefix.
    ///
    /// A subscription kept for the next round still holds its `Open`'s
    /// started exchange, so retiring the session while it is open closes the
    /// connection, as during a round. The next round resumes it, or the peer's
    /// next status that cannot closes it; see
    /// [`Self::consider_advertised_header_target`].
    fn cancel_active_request(&mut self, active: &ActiveHeaderRequest) {
        let Some(handles) = self.sessions.get(&active.peer) else {
            return;
        };
        if handles.session.session_id() == active.owner.session_id()
            && handles.session.subscriber().resumable().is_none()
        {
            self.close_subscription(&active.peer);
        }
    }

    /// Close `peer`'s live subscription unless it is already closing, and
    /// give its terminal outcome until the request timeout. A session that
    /// has not answered by then is dropped as unresponsive, so a publisher
    /// that never ends the subscription cannot park the peer.
    ///
    /// A `Close` that the session's full queue refuses starts no deadline,
    /// since the publisher has nothing to answer yet. It is retried on each
    /// maintenance pass instead.
    fn close_subscription(&mut self, peer: &ZakuraPeerId) {
        let Some(handles) = self.sessions.get(peer) else {
            return;
        };
        let session_id = handles.session.session_id();
        let refused = matches!(handles.session.try_close(), Err(OrderedSendError::Full));
        let attempt = if refused {
            CloseAttempt::Refused
        } else {
            CloseAttempt::Sent {
                live: handles.session.subscriber().live().is_some(),
            }
        };
        self.state.record_close_attempt(
            peer,
            session_id,
            attempt,
            Instant::now(),
            self.startup.request_timeout,
        );
    }

    /// Try again each `Close` that a full queue refused and is due by `now`,
    /// while its session lasts.
    fn retry_pending_closes(&mut self, now: Instant) {
        for (peer, session_id) in self.state.take_due_closes(now) {
            if self
                .sessions
                .get(&peer)
                .is_some_and(|handles| handles.session.session_id() == session_id)
            {
                self.close_subscription(&peer);
            }
        }
    }

    /// Drop each session that has not ended a subscription this node closed
    /// by its deadline.
    fn retire_unanswered_closes(&mut self, now: Instant) {
        for (peer, session_id) in self.state.take_expired_closes(now) {
            let unanswered = self.sessions.get(&peer).is_some_and(|handles| {
                handles.session.session_id() == session_id
                    && handles.session.subscriber().live().is_some()
            });
            if unanswered {
                metrics::counter!("sync.header.subscription.close_timed_out.total").increment(1);
                self.drop_unproductive_peer(&peer, session_id, "unresponsive");
            }
        }
    }

    /// Close the subscription of the session that `source` and `owner` name.
    fn cancel_owned_request(
        &mut self,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
    ) {
        let Some(peer) = self.sessions.iter().find_map(|(peer, handles)| {
            (handles.session.session_id() == owner.session_id()
                && source_id_from_peer(peer) == source)
                .then(|| peer.clone())
        }) else {
            return;
        };
        self.close_subscription(&peer);
    }

    /// Count `error`, and dispatch it as `peer`'s violation when it is an
    /// automatic header-peer fault of `source`. See [`Self::dispatch_misbehavior`]
    /// for `owner`.
    fn handle_typed_failure(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: Option<zakura_header_chain::HeaderSyncWorkOwner>,
        error: &zakura_header_chain::HeaderChainError,
    ) {
        metrics::counter!(
            "sync.header.failure.total",
            "category" => error.category.metrics_label(),
            "attribution" => error.attribution.metrics_label(),
        )
        .increment(1);
        let zakura_header_chain::Attribution::HeaderPeer(attributed_source) = error.attribution
        else {
            return;
        };
        if attributed_source != source || !error.is_automatic_header_peer_fault() {
            return;
        }
        let reason = match error.category {
            zakura_header_chain::ErrorCategory::MalformedProtocol => {
                HeaderSyncMisbehavior::MalformedMessage
            }
            zakura_header_chain::ErrorCategory::InvalidHeader => {
                HeaderSyncMisbehavior::InvalidHeader
            }
            _ => return,
        };
        sim_arg!(self.dispatch_misbehavior(peer, reason, owner); None);
    }

    /// Record a violation that a reactor check found in an admitted page.
    ///
    /// It does not disconnect. These checks compare the page with local
    /// scheduler state, so a mismatch can be local. The session reader
    /// already disconnects for the reservation violations themselves.
    ///
    /// In test and simulator builds, `origin` is the session of the message
    /// that showed the violation.
    fn report_misbehavior(
        &mut self,
        peer: ZakuraPeerId,
        reason: HeaderSyncMisbehavior,
        #[cfg(any(test, zakura_sim))] origin: u64,
    ) {
        let category = match reason {
            HeaderSyncMisbehavior::MalformedMessage => {
                zakura_header_chain::ErrorCategory::MalformedProtocol
            }
            HeaderSyncMisbehavior::InvalidHeader => {
                zakura_header_chain::ErrorCategory::InvalidHeader
            }
        };
        metrics::counter!(
            "sync.header.failure.total",
            "category" => category.metrics_label(),
            "attribution" => "header_peer",
        )
        .increment(1);
        sim_arg!(self.dispatch_misbehavior(peer, reason, None); Some(origin));
    }

    /// Record `peer`'s violation. A fault in `owner`'s work also disconnects
    /// the session that did that work, while it is still current, and its
    /// subscription never acknowledges the failed cursor. The peer then waits
    /// out the readmission cooldown, so it cannot reconnect and restage the
    /// same branch at once.
    ///
    /// In test and simulator builds, it reports `Misbehavior` with `owner`'s
    /// session as the origin, or else `reported`, the session of the message
    /// that showed the violation.
    fn dispatch_misbehavior(
        &mut self,
        peer: ZakuraPeerId,
        reason: HeaderSyncMisbehavior,
        owner: Option<zakura_header_chain::HeaderSyncWorkOwner>,
        #[cfg(any(test, zakura_sim))] reported: Option<u64>,
    ) {
        let session = self.sessions.get(&peer).map(|handles| &handles.session);
        let session_id = session.map(|session| session.session_id());
        observe!(
            self,
            ReactorEvent::Misbehavior {
                peer: peer.clone(),
                reason,
                origin: owner.map(|owner| owner.session_id()).or(reported),
                current: session_id,
            }
        );
        let faulted = owner.filter(|owner| Some(owner.session_id()) == session_id);
        if let (Some(owner), Some(session)) = (faulted, session) {
            session.disconnect_for_header_fault(owner.request_id().get());
        }
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_PEER_VIOLATION.into(),
            );
            row.insert(hs_trace::PEER.into(), trace_peer_label(&peer).into());
            row.insert(
                hs_trace::SESSION_ID.into(),
                session_id.map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::REASON.into(),
                match reason {
                    HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
                    HeaderSyncMisbehavior::InvalidHeader => "invalid_header",
                }
                .into(),
            );
            row.insert(hs_trace::BOUNDARY.into(), "engine".into());
            row.insert(
                hs_trace::DISPOSITION.into(),
                if faulted.is_some() {
                    "disconnect"
                } else {
                    "record"
                }
                .into(),
            );
        });
        if faulted.is_some() {
            self.state.start_readmission_cooldown(
                &peer,
                Instant::now(),
                self.startup.config.unproductive_peer_cooldown,
            );
        }
        let _ = self.dispatch_action(HeaderPortOperation::Misbehavior { peer, reason });
    }
}

fn next_height(height: block::Height) -> block::Height {
    block::Height(height.0.saturating_add(1).min(block::Height::MAX.0))
}

fn node_id_from_peer(peer: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

fn source_id_from_peer(peer: &ZakuraPeerId) -> zakura_header_chain::SourceId {
    zakura_header_chain::SourceId::from_digest(peer.digest())
}

#[cfg(any(test, feature = "p2p-fuzz"))]
mod machine_tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use machine_tests::{
    trace_header_reactor_bytes, HeaderMachineFailure, HeaderMachinePhase, HeaderReactorHistory,
    HeaderReactorReadView, HeaderReactorSessionView, HeaderReactorStallView, HeaderReactorStep,
    HeaderReactorView,
};

#[cfg(test)]
mod tests;
