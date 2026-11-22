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
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tracing::debug;
use zakura_node_services::task::spawn_erased_named;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    collections::HashSet,
    sync::{oneshot, watch},
    task::{AbortOnDropHandle, JoinHandle},
    time::Instant,
};

use super::dialer::native_discovery_dial;
use super::protocol::{DialAdmission, ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle};
use super::redial::ZAKURA_REDIAL_HEALTHY_CONNECTION;
use super::trace::{DialOwner, DialPhase, DiscoveryDialPhaseEvent, DiscoveryDialResultEvent};
use crate::zakura::{
    handler::{DialRegistration, DialReport, NativeDialKind},
    PreferredGateways, ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, ZakuraPeerId,
    ZakuraServiceId, ZakuraTrace,
};

mod preferred;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod replay;
mod state;
#[cfg(all(test, not(zakura_sim)))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use replay::{replay_candidate_dial_bytes, CandidateDialReplaySummary};

use preferred::{
    admit_preferred_dials, classify_gateways, DeferredDials, PreferredGatewayInput, PreferredRoom,
    PreferredRound, RoundClaims,
};
pub use preferred::{MinerGateway, MinerGatewayRanking};

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

/// The attempts one round admitted, with the addresses to dial.
#[derive(Debug, Default)]
struct DialRound {
    /// Dials to preferred gateways, highest rank first.
    preferred: Vec<(DialAttemptKey, NodeAddr)>,
    /// Ordinary dials, in the discovery book's dial order.
    ordinary: Vec<(DialAttemptKey, NodeAddr)>,
}

#[derive(Debug)]
struct DiscoveryDialOutcome {
    /// The address whose handshake TLS authenticated, if this worker's dial got that far.
    admitted_addr: Option<SocketAddr>,
    result: DiscoveryDialResult,
    /// How the worker's task ended. Shutdown can abort a task before its first
    /// poll.
    phase: DialPhase,
}

/// Spawn the long-lived discovery candidate dialer for `endpoint`.
///
/// `miner_gateways` carries the mining gateways the node prefers to stay
/// connected to; without it the node prefers none.
///
/// Returns the task handle so the caller can track it under the endpoint
/// shutdown owner; the loop also observes the endpoint shutdown token directly.
pub(crate) fn spawn_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
    miner_gateways: Option<watch::Receiver<MinerGatewayRanking>>,
) -> JoinHandle<()> {
    spawn_erased_named(
        "discovery_dialer",
        run_native_discovery_dialer(endpoint, discovery, limits, sought_services, miner_gateways),
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
    miner_gateways: Option<watch::Receiver<MinerGatewayRanking>>,
) {
    let shutdown = endpoint.background_shutdown_token();
    let trace = endpoint.trace();
    let mut state = CandidateDialerState::new(discovery.dial_backoff().await);
    let preferred = PreferredGatewayInput::new(miner_gateways, endpoint.local_node_id());
    let mut io = CandidateDialerIO {
        preferred,
        deferred: DeferredDials::new(endpoint.local_node_id()),
        preferred_hold_limit: discovery.preferred_hold_limit().await,
        max_concurrent_dials: discovery.max_concurrent_dials().await,
        preferred_attempts: HashSet::default(),
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
        let now = Instant::now();
        io.start_due_round(&mut state, now).await;
        // A worker completion wakes the loop before the round deadline, and
        // `begin_round` then refuses to start a round, so completions can't
        // speed up the cadence.
        let next_round = state.next_round(now);

        zakura_rt::select! {
            biased;
            // Endpoint shutdown cancels this token. It is the only teardown
            // signal, because the dialer holds an endpoint clone.
            _ = shutdown.cancelled() => break,
            joined = io.workers.next(), if !io.workers.is_empty() => {
                if let Some((attempt, outcome)) = joined {
                    io.settle(&mut state, attempt, outcome).await;
                }
            }
            _ = zakura_rt::time::sleep_until(next_round) => {}
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
    /// The mining gateways the node prefers to stay connected to.
    preferred: PreferredGatewayInput,
    /// The members whose dial waits out a grace.
    deferred: DeferredDials,
    /// The most connection places the node may hold for them.
    preferred_hold_limit: usize,
    /// The most dials, preferred or ordinary, in flight at once.
    max_concurrent_dials: usize,
    /// The attempts in flight that the preferred pass started.
    preferred_attempts: HashSet<DialAttemptKey>,
    trace: ZakuraTrace,
    workers: FuturesUnordered<DiscoveryDialWorker>,
}

impl CandidateDialerIO {
    /// Settles every worker that has already finished, then starts a dial
    /// round if one is due at `now`.
    ///
    /// The loop's `select!` settles one finished worker per wake, so several
    /// workers that finish with the round timer would otherwise still count
    /// as in flight and shrink the round.
    async fn start_due_round(&mut self, state: &mut CandidateDialerState, now: Instant) {
        while let Some(Some((attempt, outcome))) = self.workers.next().now_or_never() {
            self.settle(state, attempt, outcome).await;
        }
        if state.begin_round(now) {
            self.spawn_dial_round(state).await;
        }
    }

    /// Runs one dial round: admits its attempts with [`Self::admit_round`]
    /// from the peers registered now, and starts one worker per attempt.
    async fn spawn_dial_round(&mut self, state: &mut CandidateDialerState) {
        let registered: Vec<NodeId> = self
            .endpoint
            .supervisor()
            .subscribe()
            .borrow()
            .iter()
            .filter_map(ZakuraPeerId::node_id)
            .collect();
        let round = self.admit_round(state, &registered, Instant::now()).await;
        for (attempt, node_addr) in round.preferred {
            self.preferred_attempts.insert(attempt);
            metrics::counter!("zakura.p2p.discovery.dial.preferred.started").increment(1);
            self.spawn_dial(attempt, node_addr, NativeDialKind::Maintained)
                .await;
        }
        for (attempt, node_addr) in round.ordinary {
            self.spawn_dial(attempt, node_addr, NativeDialKind::Discovery)
                .await;
        }
    }

    /// Admits one round's dials at `now`, given the peers `registered` now:
    ///
    /// 1. classifies the ranked gateways, forms the preferred set and hands
    ///    it to the connection-slot checks ([`Self::publish_preferred_gateways`]);
    /// 2. admits the set's members, highest rank first, that are neither
    ///    connected nor being dialed, a member with a lower node ID only
    ///    after a grace ([`admit_preferred_dials`]);
    /// 3. admits the discovery book's other candidates with what is left
    ///    ([`admit_dial_round`]).
    async fn admit_round(
        &mut self,
        state: &mut CandidateDialerState,
        registered: &[NodeId],
        now: Instant,
    ) -> DialRound {
        let round = self.publish_preferred_gateways(state, registered).await;
        let room = PreferredRoom {
            connections: self.endpoint.preferred_dial_permits(),
            address_attempts: self.endpoint.preferred_dial_capacity(),
            dials: self
                .max_concurrent_dials
                .saturating_sub(state.in_flight_node_ids().len()),
        };
        let supervisor = self.endpoint.supervisor();
        let (preferred, claims) = admit_preferred_dials(
            state,
            &round.set,
            &round.dialable,
            &mut self.deferred,
            room,
            |ip, in_flight| supervisor.can_accept_remote_ip_with_in_flight(ip, in_flight),
            now,
        );
        let ordinary = admit_dial_round(
            &self.endpoint,
            &self.discovery,
            &self.sought_services,
            state,
            claims,
            now,
        )
        .await;
        DialRound {
            preferred,
            ordinary,
        }
    }

    /// Classifies the ranked gateways given the peers `registered` now, forms
    /// the preferred set and hands it to the connection-slot checks.
    ///
    /// Every round publishes again, so a gateway that connects, disconnects
    /// or enters or leaves a backoff moves within one round interval, and a
    /// set the slot checks re-formed for an inbound gateway is replaced by
    /// one formed from the registrations.
    async fn publish_preferred_gateways(
        &mut self,
        state: &CandidateDialerState,
        registered: &[NodeId],
    ) -> PreferredRound {
        let classified = classify_gateways(
            self.preferred.current(),
            registered,
            &state.in_flight_node_ids(),
            &self.discovery,
        )
        .await;
        let formed = PreferredGateways::form(classified.ranking, self.preferred_hold_limit);
        let round = PreferredRound {
            set: formed.set().to_vec(),
            dialable: classified.dialable,
        };
        self.endpoint.publish_preferred_gateways(formed);
        round
    }

    /// Starts a worker that dials `node_addr` for `attempt` as a `kind` dial.
    async fn spawn_dial(
        &mut self,
        attempt: DialAttemptKey,
        node_addr: NodeAddr,
        kind: NativeDialKind,
    ) {
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
                kind,
            ),
        ));
    }

    /// Settles a finished worker's attempt in `state`, then records its
    /// result in the discovery book, the trace, and the metrics.
    async fn settle(
        &mut self,
        state: &mut CandidateDialerState,
        attempt: DialAttemptKey,
        outcome: DiscoveryDialOutcome,
    ) {
        trace_dial_phase(&self.trace, attempt, outcome.phase);
        let preferred = self.preferred_attempts.remove(&attempt);
        let admitted_ip = outcome.admitted_addr.map(|addr| addr.ip());
        if !state.complete(attempt, outcome.result, admitted_ip, Instant::now()) {
            return;
        }
        if preferred {
            count_preferred_dial_result(outcome.result);
        }
        apply_discovery_dial_result(
            &self.discovery,
            &attempt.node_id,
            outcome.result,
            outcome.admitted_addr,
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
        self.preferred_attempts.clear();
        while let Some((attempt, outcome)) = self.workers.next().await {
            trace_dial_phase(&self.trace, attempt, outcome.phase);
            let admitted_ip = outcome.admitted_addr.map(|addr| addr.ip());
            state.complete(attempt, outcome.result, admitted_ip, Instant::now());
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
                    admitted_addr: None,
                    result: DiscoveryDialResult::Failed,
                    phase,
                }
            }
        };
        Poll::Ready((self.attempt, outcome))
    }
}

/// Records an attempt in `state` for each discovery-book candidate that this
/// round can dial at `now`, in the book's dial order, and returns the
/// attempts with the addresses to dial.
///
/// A candidate that `state` or the endpoint can't take now doesn't use a dial
/// slot, so the next candidate in order takes its place. Skipped candidates
/// include those whose IPs are all at the per-IP cap or in backoff, and those
/// with more addresses than the endpoint has free discovery slots: the
/// transport claims a slot for every address or fails the whole dial as a
/// local limit. The round also admits no more candidates than the handler has
/// free ordinary connection slots, since each dial holds one.
///
/// `preferred` is what this round's preferred pass already claimed, and the
/// preferred set's members are left to that pass.
async fn admit_dial_round(
    endpoint: &ZakuraEndpoint,
    discovery: &ZakuraDiscoveryHandle,
    sought_services: &[ZakuraServiceId],
    state: &mut CandidateDialerState,
    preferred: RoundClaims,
    now: Instant,
) -> Vec<(DialAttemptKey, NodeAddr)> {
    // No worker of this round dials before the round ends, so the round
    // counts down the slots and permits its own attempts will claim. A
    // preferred dial takes a held place, so it leaves the ordinary slots
    // alone unless the free slots run out first.
    let mut free_slots = endpoint
        .discovery_admission_capacity()
        .saturating_sub(preferred.address_attempts);
    let mut free_permits = endpoint.discovery_dial_permits().min(
        endpoint
            .preferred_dial_permits()
            .saturating_sub(preferred.dials),
    );
    if free_slots == 0 || free_permits == 0 {
        return Vec::new();
    }
    let supervisor = endpoint.supervisor();
    let in_flight_node_ids = state.in_flight_node_ids();
    let held_places = endpoint.held_places();
    let mut admitted = Vec::new();
    discovery
        .admit_dial_candidates_preferring_any_service(
            sought_services,
            &in_flight_node_ids,
            &held_places,
            |candidate| {
                if free_slots == 0 || free_permits == 0 {
                    return DialAdmission::Exhausted;
                }
                if held_places.members.contains(&candidate.node_id) {
                    return DialAdmission::Skipped;
                }
                let Some((node_addr, reserved_ips)) = discovery_node_addr_with_reserved_ip_capacity(
                    state,
                    &candidate,
                    |ip, in_flight| supervisor.can_accept_remote_ip_with_in_flight(ip, in_flight),
                    now,
                ) else {
                    return DialAdmission::Skipped;
                };
                if reserved_ips.len() > free_slots {
                    return DialAdmission::Skipped;
                }
                let claimed_slots = reserved_ips.len();
                let Some(attempt) = state.record_attempt(candidate.node_id, reserved_ips) else {
                    return DialAdmission::Skipped;
                };
                free_slots -= claimed_slots;
                free_permits -= 1;
                admitted.push((attempt, node_addr));
                DialAdmission::Admitted
            },
        )
        .await;
    admitted
}

/// Returns the addresses of `candidate` to dial and their canonical IPs, whose
/// admission hosts the attempt reserves: the addresses `state` allows whose
/// hosts the supervisor accepts. `accepts_ip(ip, in_flight)` is the
/// supervisor's per-IP check, given this dialer's attempts in flight to `ip`.
fn discovery_node_addr_with_reserved_ip_capacity(
    state: &CandidateDialerState,
    candidate: &ZakuraDiscoveryDialCandidate,
    accepts_ip: impl Fn(IpAddr, usize) -> bool,
    now: Instant,
) -> Option<(NodeAddr, Vec<IpAddr>)> {
    let mut direct_addrs = Vec::new();
    let mut reserved_ips = Vec::new();
    for address in state.candidate_addresses(candidate, now) {
        if accepts_ip(address.ip, state.in_flight_dials_to(address.ip)) {
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

/// Dials `node_addr` once as a `kind` dial and classifies the attempt by
/// what the registrations show for its peer.
async fn run_discovery_dial_once(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    attempt: DialAttemptKey,
    kind: NativeDialKind,
) -> DiscoveryDialOutcome {
    let finished = |result| DiscoveryDialOutcome {
        admitted_addr: None,
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
    let (admitted_addr_tx, admitted_addr_rx) = oneshot::channel();
    let (registration_tx, registration_rx) = oneshot::channel();
    let report = DialReport {
        admitted_addr: admitted_addr_tx,
        registration: registration_tx,
    };
    let dial = AbortOnDropHandle::new(spawn_erased_named("discovery_dial", {
        let endpoint = endpoint.clone();
        async move { native_discovery_dial(&endpoint, node_addr, &limits, report, kind).await }
    }));
    DialWatch {
        attempt,
        peer_id,
        registered,
        dial,
        admitted_addr_rx,
        admitted_addr: None,
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
    /// Carries the address whose handshake TLS authenticated. The dial task
    /// sends it before it registers the peer.
    admitted_addr_rx: oneshot::Receiver<SocketAddr>,
    admitted_addr: Option<SocketAddr>,
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
        let admitted_addr = self.admitted_addr();
        if admitted_addr.is_some() {
            drop(self.dial.detach());
        }
        DiscoveryDialOutcome {
            admitted_addr,
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

    /// The address this attempt's dial authenticated, once the dial reported it.
    fn admitted_addr(&mut self) -> Option<SocketAddr> {
        if self.admitted_addr.is_none() {
            self.admitted_addr = self.admitted_addr_rx.try_recv().ok();
        }
        self.admitted_addr
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

/// Counts a preferred dial's `result` in the
/// `zakura.p2p.discovery.dial.preferred.*` counters.
fn count_preferred_dial_result(result: DiscoveryDialResult) {
    match result {
        DiscoveryDialResult::Registered => {
            metrics::counter!("zakura.p2p.discovery.dial.preferred.succeeded").increment(1);
        }
        DiscoveryDialResult::Failed | DiscoveryDialResult::AlpnMismatch => {
            metrics::counter!("zakura.p2p.discovery.dial.preferred.failed").increment(1);
        }
        DiscoveryDialResult::LocalResourceLimit => {
            metrics::counter!("zakura.p2p.discovery.dial.preferred.local_resource_limit")
                .increment(1);
        }
        DiscoveryDialResult::ConnectedElsewhere | DiscoveryDialResult::ShortLivedRegistered => {}
    }
}

async fn apply_discovery_dial_result(
    discovery: &ZakuraDiscoveryHandle,
    node_id: &NodeId,
    result: DiscoveryDialResult,
    admitted_addr: Option<SocketAddr>,
    trace: &ZakuraTrace,
) {
    trace.emit_event(|| DiscoveryDialResultEvent::new(node_id.as_bytes(), result));

    match result {
        DiscoveryDialResult::Registered => {
            discovery.mark_dial_success(node_id, admitted_addr).await;
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
