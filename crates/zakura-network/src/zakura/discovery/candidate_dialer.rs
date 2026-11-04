//! Background dialer that connects out to peers learned through discovery.
//!
//! Bootstrap dials are owned by [`super::dialer`]; this dialer pulls dial
//! candidates from the discovery book, reserves per-IP capacity so it never
//! exceeds the connection caps, and dials them under the same admission control
//! as bootstrap peers. Discovery success is the peer appearing in the supervisor
//! registration watch, not the dial future completing.
//!
//! [`CandidateDialerState`] owns the dial policy. [`CandidateDialerIO`] owns
//! the endpoint, the discovery handle, and the dial workers.

use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use tracing::debug;
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    sync::{oneshot, watch},
    task::{AbortOnDropHandle, JoinHandle},
    time::Instant,
};

use super::dialer::native_discovery_dial;
use super::protocol::{ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle};
use super::redial::ZAKURA_REDIAL_HEALTHY_CONNECTION;
use super::trace::{DialOwner, DialPhase, DiscoveryDialPhaseEvent, DiscoveryDialResultEvent};
use crate::zakura::{
    handler::{DialRegistration, DialReport},
    ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, ZakuraPeerId, ZakuraServiceId,
    ZakuraTrace,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod model;
mod state;
#[cfg(test)]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use model::{replay_candidate_dial_bytes, CandidateDialReplaySummary};

use state::{
    classify_dial, CandidateDialerState, DialAttemptKey, DialFinish, DialObservation,
    PeerRegistration,
};

/// How often the discovery dialer wakes to look for new candidates.
const ZAKURA_DISCOVERY_DIAL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum DiscoveryDialResult {
    Registered,
    ConnectedElsewhere,
    ShortLivedRegistered,
    Failed,
    AlpnMismatch,
    LocalResourceLimit,
}

impl DiscoveryDialResult {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::ConnectedElsewhere => "connected_elsewhere",
            Self::ShortLivedRegistered => "short_lived_registered",
            Self::Failed => "failed",
            Self::AlpnMismatch => "alpn_mismatch",
            Self::LocalResourceLimit => "local_resource_limit",
        }
    }
}

#[derive(Debug)]
struct DiscoveryDialOutcome {
    /// The IP whose handshake TLS authenticated, if this worker's dial got that far.
    admitted_ip: Option<IpAddr>,
    result: DiscoveryDialResult,
    /// How the worker's task ended. Shutdown can abort a task before its first
    /// poll.
    phase: DialPhase,
}

/// Spawn the long-lived discovery candidate dialer for `endpoint`.
///
/// Returns the task handle so the caller can track it under the endpoint
/// shutdown owner; the loop also observes the endpoint shutdown token directly.
pub(crate) fn spawn_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
) -> JoinHandle<()> {
    spawn_erased_named(
        "discovery_dialer",
        run_native_discovery_dialer(endpoint, discovery, limits, sought_services),
    )
}

/// Seed the discovery book with the configured bootstrap peers as trusted static
/// dial candidates, so the candidate dialer maintains them even before any peer
/// gossips a signed record for them.
pub(crate) async fn insert_static_bootstrap_candidates(
    discovery: &ZakuraDiscoveryHandle,
    bootstrap_peers: &[String],
) {
    for entry in bootstrap_peers {
        match super::dialer::parse_bootstrap_peer(entry) {
            Ok(node_addr) => {
                if let Err(error) = discovery.insert_static_candidate(node_addr).await {
                    debug!(%entry, ?error, "ignoring un-insertable Zakura bootstrap candidate");
                }
            }
            Err(error) => {
                debug!(%entry, ?error, "ignoring malformed Zakura bootstrap peer");
            }
        }
    }
}

pub(crate) async fn run_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
) {
    let shutdown = endpoint.background_shutdown_token();
    let trace = endpoint.trace();
    let mut registered = endpoint.supervisor().subscribe();
    let mut state = CandidateDialerState::new(discovery.dial_backoff().await);
    let mut io = CandidateDialerIO {
        endpoint,
        discovery,
        limits,
        sought_services,
        trace,
        workers: FuturesUnordered::new(),
    };

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if state.begin_round(Instant::now()) {
            io.spawn_dial_round(&mut state).await;
        }

        zakura_rt::select! {
            biased;
            // Endpoint shutdown cancels this token. The dialer holds an endpoint
            // clone, so the supervisor registration watch never closes on its
            // own; this is the only reliable teardown signal.
            _ = shutdown.cancelled() => break,
            joined = io.workers.next(), if !io.workers.is_empty() => {
                if let Some((attempt, outcome)) = joined {
                    io.settle(&mut state, attempt, outcome).await;
                }
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = zakura_rt::time::sleep(ZAKURA_DISCOVERY_DIAL_INTERVAL) => {}
        }
    }
    io.shutdown(&mut state).await;
}

/// The candidate dialer's runtime resources: the endpoint, the discovery
/// handle, and the dial workers. [`CandidateDialerState`] decides what they do.
struct CandidateDialerIO {
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
    trace: ZakuraTrace,
    workers: FuturesUnordered<DiscoveryDialWorker>,
}

impl CandidateDialerIO {
    /// Dials the discovery book's candidates while the endpoint has admission
    /// capacity, one worker per attempt that `state` records.
    async fn spawn_dial_round(&mut self, state: &mut CandidateDialerState) {
        if !self.endpoint.has_discovery_admission_capacity() {
            return;
        }

        let candidates = self
            .discovery
            .dial_candidates_preferring_any_service(
                &self.sought_services,
                &state.in_flight_node_ids(),
            )
            .await;

        for candidate in candidates {
            if !self.endpoint.has_discovery_admission_capacity() {
                return;
            }
            let Some((node_addr, reserved_ips)) = discovery_node_addr_with_reserved_ip_capacity(
                &self.endpoint,
                state,
                &candidate,
                Instant::now(),
            )
            .await
            else {
                continue;
            };
            let Some(attempt) = state.record_attempt(candidate.node_id, reserved_ips) else {
                continue;
            };

            self.discovery.mark_dial_attempt(&attempt.node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.started").increment(1);
            trace_dial_phase(&self.trace, attempt, DialPhase::Recorded);
            self.workers.push(DiscoveryDialWorker::spawn(
                attempt,
                run_discovery_dial_once(
                    self.endpoint.clone(),
                    node_addr,
                    self.limits.clone(),
                    attempt,
                ),
            ));
        }
    }

    /// Settles a finished worker's attempt in `state`, then records its
    /// result in the discovery book, the trace, and the metrics.
    async fn settle(
        &self,
        state: &mut CandidateDialerState,
        attempt: DialAttemptKey,
        outcome: DiscoveryDialOutcome,
    ) {
        trace_dial_phase(&self.trace, attempt, outcome.phase);
        if !state.complete(attempt, outcome.result, outcome.admitted_ip, Instant::now()) {
            return;
        }
        apply_discovery_dial_result(
            &self.discovery,
            &attempt.node_id,
            outcome.result,
            &self.trace,
        )
        .await;
    }

    /// Join cancelled workers before discarding their logical reservations.
    /// Local shutdown does not record remote dial failures in the discovery book.
    async fn shutdown(&mut self, state: &mut CandidateDialerState) {
        for worker in &self.workers {
            worker.task.abort();
        }
        while let Some((attempt, outcome)) = self.workers.next().await {
            trace_dial_phase(&self.trace, attempt, outcome.phase);
            state.complete(attempt, outcome.result, outcome.admitted_ip, Instant::now());
        }
    }
}

/// The join owner keeps the attempt identity even if the task never starts.
/// Dropping the owner also cancels its task.
struct DiscoveryDialWorker {
    attempt: DialAttemptKey,
    task: AbortOnDropHandle<DiscoveryDialOutcome>,
}

impl DiscoveryDialWorker {
    fn spawn(
        attempt: DialAttemptKey,
        future: impl Future<Output = DiscoveryDialOutcome> + Send + 'static,
    ) -> Self {
        Self {
            attempt,
            task: AbortOnDropHandle::new(spawn_erased_named("discovery_worker", future)),
        }
    }
}

impl Future for DiscoveryDialWorker {
    type Output = (DialAttemptKey, DiscoveryDialOutcome);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let outcome = match std::task::ready!(Pin::new(&mut self.task).poll(cx)) {
            Ok(outcome) => outcome,
            Err(error) => {
                debug!(?error, attempt = ?self.attempt, "Zakura discovery dial worker failed");
                metrics::counter!("zakura.p2p.discovery.dial.worker_failed").increment(1);
                let phase = if error.is_panic() {
                    metrics::counter!("zakura.p2p.discovery.dial.worker_panicked").increment(1);
                    tracing::error!(attempt = ?self.attempt, "Zakura discovery dial worker panicked");
                    DialPhase::WorkerPanicked
                } else {
                    DialPhase::WorkerCancelled
                };
                DiscoveryDialOutcome {
                    admitted_ip: None,
                    result: DiscoveryDialResult::Failed,
                    phase,
                }
            }
        };
        Poll::Ready((self.attempt, outcome))
    }
}

/// Returns the addresses of `candidate` to dial and the canonical IPs they
/// reserve: the addresses `state` allows whose IPs the supervisor accepts.
async fn discovery_node_addr_with_reserved_ip_capacity(
    endpoint: &ZakuraEndpoint,
    state: &CandidateDialerState,
    candidate: &ZakuraDiscoveryDialCandidate,
    now: Instant,
) -> Option<(NodeAddr, Vec<IpAddr>)> {
    let mut direct_addrs = Vec::new();
    let mut reserved_ips = Vec::new();
    for address in state.candidate_addresses(candidate, now) {
        if endpoint
            .supervisor()
            .can_accept_remote_ip_with_in_flight(address.ip, state.in_flight_dials_to(address.ip))
            .await
        {
            reserved_ips.push(address.ip);
            direct_addrs.push(address.addr);
        }
    }

    (!direct_addrs.is_empty()).then(|| {
        (
            NodeAddr::with_addrs(candidate.node_id, direct_addrs),
            reserved_ips,
        )
    })
}

async fn run_discovery_dial_once(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    attempt: DialAttemptKey,
) -> DiscoveryDialOutcome {
    let finished = |result| DiscoveryDialOutcome {
        admitted_ip: None,
        result,
        phase: DialPhase::WorkerFinished,
    };
    let Ok(peer_id) = ZakuraPeerId::new(attempt.node_id.as_bytes().to_vec()) else {
        return finished(DiscoveryDialResult::Failed);
    };
    let mut registered = endpoint.supervisor().subscribe();
    if registered.borrow_and_update().contains(&peer_id) {
        return finished(DiscoveryDialResult::ConnectedElsewhere);
    }

    let trace = endpoint.trace();
    let (admitted_ip_tx, admitted_ip_rx) = oneshot::channel();
    let (registration_tx, registration_rx) = oneshot::channel();
    let report = DialReport {
        admitted_ip: admitted_ip_tx,
        registration: registration_tx,
    };
    let dial = AbortOnDropHandle::new(spawn_erased_named("discovery_dial", {
        let endpoint = endpoint.clone();
        async move { native_discovery_dial(&endpoint, node_addr, &limits, report).await }
    }));
    DialWatch {
        attempt,
        peer_id,
        registered,
        dial,
        admitted_ip_rx,
        admitted_ip: None,
        registration_rx,
        own_registration: None,
        registration: PeerRegistration::Absent,
        trace,
    }
    .run()
    .await
}

/// One attempt's own dial and what the registrations show for its peer.
struct DialWatch {
    attempt: DialAttemptKey,
    peer_id: ZakuraPeerId,
    registered: watch::Receiver<Vec<ZakuraPeerId>>,
    dial: AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
    /// Carries the IP whose handshake TLS authenticated. The dial task sends it
    /// before it registers the peer.
    admitted_ip_rx: oneshot::Receiver<IpAddr>,
    admitted_ip: Option<IpAddr>,
    /// Carries how this attempt's own registration ended. The dial sends it
    /// before it serves the connection, so it is final once the dial ends.
    registration_rx: oneshot::Receiver<DialRegistration>,
    own_registration: Option<DialRegistration>,
    registration: PeerRegistration,
    trace: ZakuraTrace,
}

impl DialWatch {
    /// Waits until the observations classify the attempt, then gives the dial
    /// to its owner.
    ///
    /// A dial that authenticated its peer hands its serving task to the
    /// connection's lifetime. Any other dial ends with its attempt, so its
    /// handshake cannot outlive the per-IP reservation that this attempt's
    /// settlement releases. Cancellation before this point drops the guard and
    /// aborts the dial.
    async fn run(mut self) -> DiscoveryDialOutcome {
        let result = self.classified().await;
        let admitted_ip = self.admitted_ip();
        if admitted_ip.is_some() {
            drop(self.dial.detach());
        }
        DiscoveryDialOutcome {
            admitted_ip,
            result,
            phase: DialPhase::WorkerFinished,
        }
    }

    async fn classified(&mut self) -> DiscoveryDialResult {
        loop {
            let observation = if self.peer_is_registered() {
                self.registration_began();
                self.observe_registered().await
            } else {
                self.observe_unregistered().await
            };
            let Some(observation) = observation else {
                continue;
            };
            if observation == DialObservation::RegistrationEnded {
                self.registration = PeerRegistration::Ended;
                trace_dial_phase(&self.trace, self.attempt, DialPhase::RegistrationEnded);
            }
            let registered = self.own_registration() == Some(DialRegistration::Registered);
            if let Some(result) = classify_dial(observation, registered) {
                return result;
            }
        }
    }

    /// Waits while the peer is unregistered. Returns `None` once the
    /// registrations change.
    async fn observe_unregistered(&mut self) -> Option<DialObservation> {
        zakura_rt::select! {
            dial = &mut self.dial => Some(self.dial_ended(dial)),
            changed = self.registered.changed() => {
                changed.is_err().then_some(DialObservation::WatchClosed)
            }
        }
    }

    /// Waits while the peer is registered, for at most the time that makes a
    /// registration healthy.
    async fn observe_registered(&mut self) -> Option<DialObservation> {
        let healthy = zakura_rt::time::sleep(ZAKURA_REDIAL_HEALTHY_CONNECTION);
        zakura_rt::pin!(healthy);
        loop {
            if !self.peer_is_registered() {
                return Some(DialObservation::RegistrationEnded);
            }
            zakura_rt::select! {
                dial = &mut self.dial => return Some(self.dial_ended(dial)),
                changed = self.registered.changed() => {
                    if changed.is_err() {
                        return Some(DialObservation::WatchClosed);
                    }
                }
                _ = &mut healthy => return Some(DialObservation::RegistrationHealthy),
            }
        }
    }

    /// Whether the peer is registered now, marking the watch seen.
    fn peer_is_registered(&mut self) -> bool {
        self.registered.borrow_and_update().contains(&self.peer_id)
    }

    fn registration_began(&mut self) {
        self.registration = PeerRegistration::Live;
        trace_dial_phase(&self.trace, self.attempt, DialPhase::RegistrationSeen);
    }

    /// The observation for a dial that ended with `dial`, which reads the
    /// registrations once more.
    fn dial_ended(
        &mut self,
        dial: Result<Result<(), ZakuraHandlerError>, zakura_rt::task::JoinError>,
    ) -> DialObservation {
        let registration = if self.peer_is_registered() {
            PeerRegistration::Live
        } else if self.registration == PeerRegistration::Absent {
            PeerRegistration::Absent
        } else {
            PeerRegistration::Ended
        };
        DialObservation::DialEnded(dial_finish(dial, self.own_registration()), registration)
    }

    /// How this attempt's own registration ended, once the dial reported it.
    fn own_registration(&mut self) -> Option<DialRegistration> {
        if self.own_registration.is_none() {
            self.own_registration = self.registration_rx.try_recv().ok();
        }
        self.own_registration
    }

    /// The IP this attempt's dial authenticated, once the dial reported it.
    fn admitted_ip(&mut self) -> Option<IpAddr> {
        if self.admitted_ip.is_none() {
            self.admitted_ip = self.admitted_ip_rx.try_recv().ok();
        }
        self.admitted_ip
    }
}

/// How a finished dial task ended, given what the dial reported about its own
/// registration.
fn dial_finish(
    dial: Result<Result<(), ZakuraHandlerError>, zakura_rt::task::JoinError>,
    own_registration: Option<DialRegistration>,
) -> DialFinish {
    match dial {
        // The dial returns `Ok(())` after it serves its connection, and also
        // when this node refuses the authenticated peer before registration.
        Ok(Ok(())) => match own_registration {
            Some(DialRegistration::Registered) => DialFinish::Served,
            Some(DialRegistration::LocalLimit) => DialFinish::LocalLimit,
            None => DialFinish::Refused,
        },
        Ok(Err(ZakuraHandlerError::ResourceLimit(_))) => DialFinish::LocalLimit,
        Ok(Err(error)) if error.is_alpn_mismatch() => {
            debug!(
                ?error,
                "Zakura discovery peer speaks another protocol version"
            );
            DialFinish::AlpnMismatch
        }
        Ok(Err(error)) => {
            debug!(?error, "Zakura discovery dial failed");
            DialFinish::Failed
        }
        Err(error) => {
            debug!(?error, "Zakura discovery dial task failed");
            DialFinish::Failed
        }
    }
}

fn trace_dial_phase(trace: &ZakuraTrace, attempt: DialAttemptKey, phase: DialPhase) {
    trace.emit_event(|| {
        DiscoveryDialPhaseEvent::new(
            attempt.node_id.as_bytes(),
            DialOwner::Candidate,
            attempt.sequence(),
            phase,
        )
    });
}

async fn apply_discovery_dial_result(
    discovery: &ZakuraDiscoveryHandle,
    node_id: &NodeId,
    result: DiscoveryDialResult,
    trace: &ZakuraTrace,
) {
    trace.emit_event(|| DiscoveryDialResultEvent::new(node_id.as_bytes(), result));

    match result {
        DiscoveryDialResult::Registered => {
            discovery.mark_dial_success(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.succeeded").increment(1);
        }
        // Another connection's registration proves none of this record's
        // addresses, so it confirms nothing.
        DiscoveryDialResult::ConnectedElsewhere => {
            metrics::counter!("zakura.p2p.discovery.dial.connected_elsewhere").increment(1);
        }
        DiscoveryDialResult::ShortLivedRegistered => {
            discovery.mark_short_lived_exchange(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.short_lived_registered").increment(1);
        }
        DiscoveryDialResult::Failed => {
            discovery.mark_dial_failure(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.failed").increment(1);
        }
        DiscoveryDialResult::AlpnMismatch => {
            discovery.mark_alpn_mismatch(node_id).await;
            metrics::counter!("zakura.p2p.discovery.dial.alpn_mismatch").increment(1);
        }
        DiscoveryDialResult::LocalResourceLimit => {
            metrics::counter!("zakura.p2p.discovery.dial.local_resource_limit").increment(1);
        }
    }
}
