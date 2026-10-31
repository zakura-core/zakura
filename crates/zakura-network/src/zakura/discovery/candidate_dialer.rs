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
use super::trace::{DialPhase, DiscoveryDialPhaseEvent, DiscoveryDialResultEvent};
use crate::zakura::{
    ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, ZakuraPeerId, ZakuraServiceId,
    ZakuraTrace,
};

mod state;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod state_tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use state_tests::{replay_candidate_dial_bytes, CandidateDialReplaySummary};

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

/// How one dial worker's task ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WorkerEnd {
    /// The task returned its classified result.
    Finished,
    /// Shutdown aborted the task, possibly before its first poll.
    Cancelled,
    /// The task panicked.
    Panicked,
}

#[derive(Debug)]
struct DiscoveryDialOutcome {
    /// The IP whose handshake TLS authenticated, if this worker's dial got that far.
    admitted_ip: Option<IpAddr>,
    result: DiscoveryDialResult,
    worker: WorkerEnd,
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
        trace_dial_phase(&self.trace, attempt, outcome.worker.phase());
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
            trace_dial_phase(&self.trace, attempt, outcome.worker.phase());
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
                let worker = if error.is_panic() {
                    metrics::counter!("zakura.p2p.discovery.dial.worker_panicked").increment(1);
                    tracing::error!(attempt = ?self.attempt, "Zakura discovery dial worker panicked");
                    WorkerEnd::Panicked
                } else {
                    WorkerEnd::Cancelled
                };
                DiscoveryDialOutcome {
                    admitted_ip: None,
                    result: DiscoveryDialResult::Failed,
                    worker,
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
        if can_accept_discovery_dial_ip(endpoint, address.ip, state.in_flight_dials_to(address.ip))
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

async fn can_accept_discovery_dial_ip(
    endpoint: &ZakuraEndpoint,
    remote_ip: IpAddr,
    in_flight: usize,
) -> bool {
    endpoint
        .supervisor()
        .can_accept_remote_ip_with_in_flight(remote_ip, in_flight)
        .await
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
        worker: WorkerEnd::Finished,
    };
    let Ok(peer_id) = ZakuraPeerId::new(attempt.node_id.as_bytes().to_vec()) else {
        return finished(DiscoveryDialResult::Failed);
    };
    let mut registered = endpoint.supervisor().subscribe();
    if registered
        .borrow_and_update()
        .iter()
        .any(|id| id == &peer_id)
    {
        return finished(DiscoveryDialResult::ConnectedElsewhere);
    }

    let trace = endpoint.trace();
    let (admitted_ip_tx, admitted_ip_rx) = oneshot::channel();
    let dial = AbortOnDropHandle::new(spawn_erased_named("discovery_dial", {
        let endpoint = endpoint.clone();
        async move { native_discovery_dial(&endpoint, node_addr, &limits, admitted_ip_tx).await }
    }));
    DialWatch {
        attempt,
        peer_id,
        registered,
        dial,
        admitted_ip_rx,
        admitted_ip: None,
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
            worker: WorkerEnd::Finished,
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
            if let Some(result) = classify_dial(observation, self.admitted_ip().is_some()) {
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
        self.registered
            .borrow_and_update()
            .iter()
            .any(|id| id == &self.peer_id)
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
        DialObservation::DialEnded(dial_finish(dial), registration)
    }

    /// The IP this attempt's dial authenticated, once the dial reported it.
    fn admitted_ip(&mut self) -> Option<IpAddr> {
        if self.admitted_ip.is_none() {
            self.admitted_ip = self.admitted_ip_rx.try_recv().ok();
        }
        self.admitted_ip
    }
}

/// How a finished dial task ended.
fn dial_finish(
    dial: Result<Result<(), ZakuraHandlerError>, zakura_rt::task::JoinError>,
) -> DialFinish {
    match dial {
        // `native_bootstrap_dial` returns `Ok(())` only after the connection
        // finishes.
        Ok(Ok(())) => DialFinish::Served,
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
        DiscoveryDialPhaseEvent::new(attempt.node_id.as_bytes(), attempt.sequence(), phase)
    });
}

impl WorkerEnd {
    fn phase(self) -> DialPhase {
        match self {
            Self::Finished => DialPhase::WorkerFinished,
            Self::Cancelled => DialPhase::WorkerCancelled,
            Self::Panicked => DialPhase::WorkerPanicked,
        }
    }
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
        DiscoveryDialResult::ConnectedElsewhere => {
            discovery.mark_dial_success(node_id).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn peer_id(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32-byte test peer id is valid")
    }

    fn node_id(byte: u8) -> NodeId {
        zakura_quic::NodeSecretKey::from_bytes(&[byte; 32]).public()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_unpolled_worker_settles_only_its_attempt() {
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let first = state.record_attempt(node_id(3), vec![ip]).unwrap();
        let second = state.record_attempt(node_id(4), vec![ip]).unwrap();
        let retained = std::sync::Arc::new(());
        let observed = std::sync::Arc::downgrade(&retained);
        let worker = DiscoveryDialWorker::spawn(first, async move {
            let _retained = retained;
            std::future::pending().await
        });
        worker.task.abort();
        assert!(
            observed.upgrade().is_some(),
            "abort requests cancellation before task destruction"
        );
        assert_eq!(state.in_flight_dials_to(ip), 2);
        let (attempt, outcome) = zakura_rt::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert!(
            observed.upgrade().is_none(),
            "joining observes destruction of the task payload"
        );
        assert_eq!(attempt, first);
        assert_eq!(outcome.result, DiscoveryDialResult::Failed);
        assert!(state.complete(attempt, outcome.result, outcome.admitted_ip, Instant::now()));
        assert_eq!(state.in_flight_dials_to(ip), 1);
        assert_eq!(state.in_flight_node_ids(), vec![second.node_id]);
        let replacement = state.record_attempt(first.node_id, vec![ip]).unwrap();
        assert!(!state.complete(attempt, outcome.result, outcome.admitted_ip, Instant::now()));
        assert_eq!(state.in_flight_dials_to(ip), 2);
        assert!(state.complete(
            replacement,
            DiscoveryDialResult::ConnectedElsewhere,
            None,
            Instant::now()
        ));
        assert_eq!(state.in_flight_node_ids(), vec![second.node_id]);
    }

    #[tokio::test]
    async fn shutdown_joins_started_and_unpolled_workers_before_discarding_reservations() {
        let node = crate::zakura::testkit::ZakuraTestNode::builder(9212)
            .spawn()
            .await
            .unwrap();
        let mut io = CandidateDialerIO {
            endpoint: node.endpoint(),
            discovery: node.discovery(),
            limits: node.limits().clone(),
            sought_services: Vec::new(),
            trace: node.endpoint().trace(),
            workers: FuturesUnordered::new(),
        };
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mut probes = Vec::new();
        for index in 0..2 {
            let attempt = state.record_attempt(node_id(index + 3), vec![ip]).unwrap();
            let retained = std::sync::Arc::new(());
            probes.push(std::sync::Arc::downgrade(&retained));
            let (started_tx, started_rx) = oneshot::channel();
            io.workers
                .push(DiscoveryDialWorker::spawn(attempt, async move {
                    let _retained = retained;
                    let _ = started_tx.send(());
                    std::future::pending().await
                }));
            if index == 0 {
                zakura_rt::time::timeout(Duration::from_secs(5), started_rx)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
        assert_eq!(state.in_flight_dials_to(ip), 2);
        zakura_rt::time::timeout(Duration::from_secs(5), io.shutdown(&mut state))
            .await
            .unwrap();
        assert!(io.workers.is_empty());
        assert!(state.in_flight_node_ids().is_empty());
        assert_eq!(state.in_flight_dials_to(ip), 0);
        assert!(probes.iter().all(|probe| probe.upgrade().is_none()));
        drop(io);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_worker_releases_its_real_quic_dial_capacity() {
        zakura_rt::time::timeout(Duration::from_secs(15), async {
            let limits = ZakuraLocalLimits {
                max_connections: 2,
                max_pending_handshakes: 1,
                ..ZakuraLocalLimits::from_config(&crate::Config::default())
            };
            let node = crate::zakura::testkit::ZakuraTestNode::builder(9213)
                .limits(limits.clone())
                .quic_config(zakura_quic::QuicConfig {
                    handshake_timeout_secs: Some(30),
                    ..Default::default()
                })
                .spawn()
                .await
                .unwrap();
            let endpoint = node.endpoint();
            let ip = IpAddr::from([127, 0, 0, 1]);
            let target = node_id(6);
            let mut state =
                CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
            for _ in 0..2 {
                let silent = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                assert!(endpoint.has_discovery_admission_capacity());
                let attempt = state.record_attempt(target, vec![ip]).unwrap();
                let worker = DiscoveryDialWorker::spawn(
                    attempt,
                    run_discovery_dial_once(
                        endpoint.clone(),
                        NodeAddr::with_addrs(target, [silent.local_addr().unwrap()]),
                        limits.clone(),
                        attempt,
                    ),
                );
                let mut packet = [0; 2048];
                silent.recv_from(&mut packet).await.unwrap();
                assert!(
                    !endpoint.has_discovery_admission_capacity(),
                    "the real transport holds the sole discovery slot"
                );
                worker.task.abort();
                let (completed, outcome) = worker.await;
                assert_eq!(completed, attempt);
                assert_eq!(outcome.result, DiscoveryDialResult::Failed);
                assert!(state.complete(
                    completed,
                    outcome.result,
                    outcome.admitted_ip,
                    Instant::now()
                ));
                zakura_rt::time::timeout(Duration::from_secs(2), async {
                    while !endpoint.has_discovery_admission_capacity() {
                        zakura_rt::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect(
                    "worker cancellation releases transport capacity before the handshake timeout",
                );
                assert_eq!(state.in_flight_dials_to(ip), 0);
            }
            node.shutdown().await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn panicked_worker_returns_metadata_needed_to_release_reservations() {
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let attempt = state.record_attempt(node_id(3), vec![ip]).unwrap();
        let worker = DiscoveryDialWorker::spawn(attempt, async { panic!("test worker panic") });
        let (completed, outcome) = zakura_rt::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap();
        assert_eq!(completed, attempt);
        assert_eq!(outcome.result, DiscoveryDialResult::Failed);
        assert!(state.complete(
            completed,
            outcome.result,
            outcome.admitted_ip,
            Instant::now()
        ));
        assert!(state.in_flight_node_ids().is_empty());
        assert_eq!(state.in_flight_dials_to(ip), 0);
    }

    const DIAL_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(93, 184, 216, 34));

    /// One attempt's watch of a dial that runs `dial`, with the registration
    /// watch's sender and the sender that reports the authenticated IP.
    fn dial_watch(
        byte: u8,
        registered: Vec<ZakuraPeerId>,
        dial: impl Future<Output = Result<(), ZakuraHandlerError>> + Send + 'static,
    ) -> (
        DialWatch,
        watch::Sender<Vec<ZakuraPeerId>>,
        oneshot::Sender<IpAddr>,
    ) {
        let (registered_tx, registered_rx) = watch::channel(registered);
        let (admitted_tx, admitted_ip_rx) = oneshot::channel();
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let watch = DialWatch {
            attempt: state
                .record_attempt(node_id(byte), vec![DIAL_IP])
                .expect("a fresh dialer records the attempt"),
            peer_id: peer_id(byte),
            registered: registered_rx,
            dial: AbortOnDropHandle::new(zakura_rt::task::spawn(dial)),
            admitted_ip_rx,
            admitted_ip: None,
            registration: PeerRegistration::Absent,
            trace: ZakuraTrace::default(),
        };
        (watch, registered_tx, admitted_tx)
    }

    #[tokio::test(start_paused = true)]
    async fn registration_drop_before_healthy_threshold_is_short_lived() {
        let peer = peer_id(7);
        let (watch, registered_tx, admitted_tx) = dial_watch(7, vec![peer], std::future::pending());
        admitted_tx.send(DIAL_IP).unwrap();

        let settled = tokio::spawn(watch.run());
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the registration is live");

        registered_tx.send_replace(Vec::new());
        let outcome = settled.await.unwrap();
        assert_eq!(outcome.result, DiscoveryDialResult::ShortLivedRegistered);
        assert_eq!(outcome.admitted_ip, Some(DIAL_IP));
    }

    #[tokio::test(start_paused = true)]
    async fn dial_completion_with_live_registration_is_connected_elsewhere() {
        let (watch, _registered_tx, _admitted_tx) =
            dial_watch(9, vec![peer_id(9)], std::future::ready(Ok(())));
        assert_eq!(
            watch.run().await.result,
            DiscoveryDialResult::ConnectedElsewhere
        );
    }

    /// A dial that served its connection before the watch showed the peer ran
    /// a real, short connection, not a failed dial.
    #[tokio::test(start_paused = true)]
    async fn a_served_dial_with_a_delayed_registration_is_short_lived() {
        let (watch, _registered_tx, admitted_tx) =
            dial_watch(10, Vec::new(), std::future::ready(Ok(())));
        admitted_tx.send(DIAL_IP).unwrap();
        let outcome = watch.run().await;
        assert_eq!(outcome.result, DiscoveryDialResult::ShortLivedRegistered);
        assert_eq!(outcome.admitted_ip, Some(DIAL_IP));
    }

    /// Another connection's registration ending says nothing about this
    /// attempt, which goes on to report its own dial's failure.
    #[tokio::test(start_paused = true)]
    async fn another_connections_registration_ending_keeps_the_attempt_waiting() {
        let (fail_tx, fail_rx) = tokio::sync::oneshot::channel::<()>();
        let (watch, registered_tx, _admitted_tx) = dial_watch(11, vec![peer_id(11)], async move {
            let _ = fail_rx.await;
            Err(ZakuraHandlerError::ResourceLimit(
                "the test dial hit a limit",
            ))
        });
        let settled = tokio::spawn(watch.run());
        tokio::task::yield_now().await;

        registered_tx.send_replace(Vec::new());
        tokio::task::yield_now().await;
        assert!(!settled.is_finished(), "the attempt's own dial still runs");

        fail_tx.send(()).unwrap();
        let outcome = settled.await.unwrap();
        assert_eq!(outcome.result, DiscoveryDialResult::LocalResourceLimit);
        assert_eq!(outcome.admitted_ip, None);
    }

    /// A registration that lasts on another connection does not credit this
    /// attempt, and the attempt's own handshake ends with it.
    #[tokio::test(start_paused = true)]
    async fn another_connections_healthy_registration_ends_the_unfinished_dial() {
        let retained = std::sync::Arc::new(());
        let observed = std::sync::Arc::downgrade(&retained);
        let (watch, _registered_tx, _admitted_tx) = dial_watch(12, vec![peer_id(12)], async move {
            let _retained = retained;
            std::future::pending().await
        });
        let outcome = watch.run().await;
        assert_eq!(outcome.result, DiscoveryDialResult::ConnectedElsewhere);
        assert_eq!(outcome.admitted_ip, None);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(observed.upgrade().is_none(), "the dial was aborted");
    }

    /// This attempt's healthy connection keeps serving past its attempt.
    #[tokio::test(start_paused = true)]
    async fn an_authenticated_healthy_registration_hands_the_dial_to_its_connection() {
        let retained = std::sync::Arc::new(());
        let observed = std::sync::Arc::downgrade(&retained);
        let (watch, _registered_tx, admitted_tx) = dial_watch(13, vec![peer_id(13)], async move {
            let _retained = retained;
            std::future::pending().await
        });
        admitted_tx.send(DIAL_IP).unwrap();
        let outcome = watch.run().await;
        assert_eq!(outcome.result, DiscoveryDialResult::Registered);
        assert_eq!(outcome.admitted_ip, Some(DIAL_IP));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(observed.upgrade().is_some(), "the dial keeps serving");
    }

    /// A cancelled worker reports its identity and its end.
    #[tokio::test]
    async fn a_cancelled_worker_reports_how_it_ended() {
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let attempt = state.record_attempt(node_id(14), vec![DIAL_IP]).unwrap();
        let worker = DiscoveryDialWorker::spawn(attempt, std::future::pending());
        worker.task.abort();
        let (settled, outcome) = worker.await;
        assert_eq!(settled, attempt);
        assert_eq!(outcome.worker, WorkerEnd::Cancelled);
    }

    // V12 F-71: the reservation counts one dial per IP, and the endpoint
    // starts one handshake per address. A record that lists several ports on
    // one IP must get one address per IP, so attempts equal reservations.
    #[tokio::test]
    async fn dial_keeps_the_first_address_of_each_ip() {
        use std::net::SocketAddr;

        let node = crate::zakura::testkit::ZakuraTestNode::builder(9211)
            .spawn()
            .await
            .expect("dialer spawns");
        let shared: IpAddr = IpAddr::from([93, 184, 216, 34]);
        let other: IpAddr = IpAddr::from([198, 51, 100, 7]);
        let mapped_shared: IpAddr = "::ffff:93.184.216.34"
            .parse()
            .expect("mapped test address parses");
        let candidate = ZakuraDiscoveryDialCandidate {
            node_id: node_id(5),
            direct_addrs: vec![
                SocketAddr::new(shared, 8233),
                SocketAddr::new(shared, 8234),
                SocketAddr::new(other, 8233),
                SocketAddr::new(mapped_shared, 8235),
                SocketAddr::new(other, 8234),
            ],
            is_static: false,
        };

        let (node_addr, reserved_ips) = discovery_node_addr_with_reserved_ip_capacity(
            &node.endpoint(),
            &CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600))),
            &candidate,
            Instant::now(),
        )
        .await
        .expect("both IPs have capacity");

        assert_eq!(
            node_addr.direct,
            vec![SocketAddr::new(shared, 8233), SocketAddr::new(other, 8233)]
        );
        assert_eq!(reserved_ips, vec![shared, other]);
        node.shutdown().await;
    }
}
