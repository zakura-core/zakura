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

use std::{net::IpAddr, panic::AssertUnwindSafe, time::Duration};

use futures::FutureExt;
use tracing::debug;
use zakura_node_services::task::{erased, spawn_erased_named};
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    sync::{oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

use super::dialer::native_discovery_dial;
use super::protocol::{ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle};
use super::redial::ZAKURA_REDIAL_HEALTHY_CONNECTION;
use super::trace::DiscoveryDialResultEvent;
use crate::zakura::{
    ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, ZakuraPeerId, ZakuraServiceId,
    ZakuraTrace,
};

mod state;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod state_tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use state_tests::{replay_candidate_dial_bytes, CandidateDialReplaySummary};

use state::{CandidateDialerState, DialAttemptKey};

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
struct DiscoveryDialWorkerResult {
    attempt: DialAttemptKey,
    /// The IP whose handshake TLS authenticated, if this worker's dial got that far.
    admitted_ip: Option<IpAddr>,
    result: DiscoveryDialResult,
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
        workers: JoinSet::new(),
    };

    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if state.begin_round(Instant::now()) {
            io.spawn_dial_round(&mut state).await;
        }

        zakura_rt::select! {
            biased;
            // Endpoint shutdown cancels this token. The dialer holds an endpoint
            // clone, so the supervisor registration watch never closes on its
            // own; this is the only reliable teardown signal.
            _ = shutdown.cancelled() => return,
            joined = io.workers.join_next(), if !io.workers.is_empty() => {
                match joined {
                    Some(Ok(worker_result)) => io.settle(&mut state, worker_result).await,
                    Some(Err(error)) => {
                        debug!(?error, "Zakura discovery dial worker failed");
                        metrics::counter!("zakura.p2p.discovery.dial.worker_failed").increment(1);
                    }
                    None => {}
                }
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = zakura_rt::time::sleep(ZAKURA_DISCOVERY_DIAL_INTERVAL) => {}
        }
    }
}

/// The candidate dialer's runtime resources: the endpoint, the discovery
/// handle, and the dial workers. [`CandidateDialerState`] decides what they do.
struct CandidateDialerIO {
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
    sought_services: Vec<ZakuraServiceId>,
    trace: ZakuraTrace,
    workers: JoinSet<DiscoveryDialWorkerResult>,
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
            self.workers.spawn(erased(
                AssertUnwindSafe(run_discovery_dial_once(
                    self.endpoint.clone(),
                    node_addr,
                    self.limits.clone(),
                    attempt,
                ))
                .catch_unwind()
                .map(move |result| recover_discovery_dial_worker_panic(attempt, result)),
            ));
        }
    }

    /// Settles a finished worker's attempt in `state`, then records its
    /// result in the discovery book, the trace, and the metrics.
    async fn settle(&self, state: &mut CandidateDialerState, worker: DiscoveryDialWorkerResult) {
        if !state.complete(
            worker.attempt,
            worker.result,
            worker.admitted_ip,
            Instant::now(),
        ) {
            return;
        }
        apply_discovery_dial_result(
            &self.discovery,
            &worker.attempt.node_id,
            worker.result,
            &self.trace,
        )
        .await;
    }
}

/// Turns a panicked worker into a failed result for its `attempt`, so the
/// dialer still settles the attempt's reservations.
fn recover_discovery_dial_worker_panic(
    attempt: DialAttemptKey,
    result: std::thread::Result<DiscoveryDialWorkerResult>,
) -> DiscoveryDialWorkerResult {
    result.unwrap_or_else(|_| {
        metrics::counter!("zakura.p2p.discovery.dial.worker_panicked").increment(1);
        let node_id = attempt.node_id;
        tracing::error!(?node_id, "Zakura discovery dial worker panicked");
        DiscoveryDialWorkerResult {
            attempt,
            admitted_ip: None,
            result: DiscoveryDialResult::Failed,
        }
    })
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
) -> DiscoveryDialWorkerResult {
    let Ok(peer_id) = ZakuraPeerId::new(attempt.node_id.as_bytes().to_vec()) else {
        return DiscoveryDialWorkerResult {
            attempt,
            admitted_ip: None,
            result: DiscoveryDialResult::Failed,
        };
    };
    let mut registered = endpoint.supervisor().subscribe();
    if registered
        .borrow_and_update()
        .iter()
        .any(|id| id == &peer_id)
    {
        return DiscoveryDialWorkerResult {
            attempt,
            admitted_ip: None,
            result: DiscoveryDialResult::ConnectedElsewhere,
        };
    }

    let (admitted_ip_tx, mut admitted_ip_rx) = oneshot::channel();
    let dial = spawn_erased_named("discovery_dial", {
        let endpoint = endpoint.clone();
        async move { native_discovery_dial(&endpoint, node_addr, &limits, admitted_ip_tx).await }
    });
    zakura_rt::pin!(dial);

    let result = loop {
        if registered
            .borrow_and_update()
            .iter()
            .any(|id| id == &peer_id)
        {
            break wait_for_discovery_registration_to_settle(&mut registered, &peer_id, &mut dial)
                .await;
        }

        zakura_rt::select! {
            dial_result = &mut dial => {
                break match dial_result {
                    // `native_bootstrap_dial` returns `Ok(())` only after the connection
                    // finishes; discovery success is the peer appearing in the registration watch.
                    Ok(Ok(())) => {
                        if registered.borrow_and_update().iter().any(|id| id == &peer_id) {
                            DiscoveryDialResult::ConnectedElsewhere
                        } else {
                            DiscoveryDialResult::Failed
                        }
                    }
                    Ok(Err(ZakuraHandlerError::ResourceLimit(_))) => {
                        DiscoveryDialResult::LocalResourceLimit
                    }
                    Ok(Err(error)) if error.is_alpn_mismatch() => {
                        debug!(?error, "Zakura discovery peer speaks another protocol version");
                        DiscoveryDialResult::AlpnMismatch
                    }
                    Ok(Err(error)) => {
                        debug!(?error, "Zakura discovery dial failed");
                        DiscoveryDialResult::Failed
                    }
                    Err(error) => {
                        debug!(?error, "Zakura discovery dial task failed");
                        DiscoveryDialResult::Failed
                    }
                };
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    break DiscoveryDialResult::Failed;
                }
            }
        }
    };

    DiscoveryDialWorkerResult {
        attempt,
        // The dial task sends the admitted IP before it registers the peer. An
        // empty channel means this dial never authenticated the peer itself.
        admitted_ip: admitted_ip_rx.try_recv().ok(),
        result,
    }
}

async fn wait_for_discovery_registration_to_settle(
    registered: &mut watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &ZakuraPeerId,
    dial: &mut std::pin::Pin<&mut JoinHandle<Result<(), crate::zakura::ZakuraHandlerError>>>,
) -> DiscoveryDialResult {
    let healthy = zakura_rt::time::sleep(ZAKURA_REDIAL_HEALTHY_CONNECTION);
    zakura_rt::pin!(healthy);

    loop {
        if !registered
            .borrow_and_update()
            .iter()
            .any(|id| id == peer_id)
        {
            return DiscoveryDialResult::ShortLivedRegistered;
        }

        zakura_rt::select! {
            _dial_result = &mut *dial => {
                let still_registered = registered
                    .borrow_and_update()
                    .iter()
                    .any(|id| id == peer_id);
                return if still_registered {
                    DiscoveryDialResult::ConnectedElsewhere
                } else {
                    DiscoveryDialResult::ShortLivedRegistered
                };
            }
            changed = registered.changed() => {
                if changed.is_err() {
                    return DiscoveryDialResult::Failed;
                }
            }
            _ = &mut healthy => return DiscoveryDialResult::Registered,
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

    #[test]
    fn panicked_worker_returns_metadata_needed_to_release_reservations() {
        let node_id = zakura_quic::NodeSecretKey::from_bytes(&[3; 32]).public();
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        let attempt = state
            .record_attempt(node_id, vec![ip])
            .expect("an idle dialer records the first attempt");

        let worker_result =
            recover_discovery_dial_worker_panic(attempt, Err(Box::new("test worker panic")));
        assert_eq!(worker_result.attempt, attempt);
        assert_eq!(worker_result.result, DiscoveryDialResult::Failed);

        assert!(state.complete(
            worker_result.attempt,
            worker_result.result,
            worker_result.admitted_ip,
            Instant::now(),
        ));
        assert!(state.in_flight_node_ids().is_empty());
        assert_eq!(state.in_flight_dials_to(ip), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn registration_drop_before_healthy_threshold_is_short_lived() {
        let peer_id = peer_id(7);
        let (registered_tx, mut registered_rx) = tokio::sync::watch::channel(vec![peer_id.clone()]);
        let dial = tokio::spawn(async {
            std::future::pending::<Result<(), crate::zakura::ZakuraHandlerError>>().await
        });
        tokio::pin!(dial);

        let settled =
            wait_for_discovery_registration_to_settle(&mut registered_rx, &peer_id, &mut dial);
        tokio::pin!(settled);
        tokio::select! {
            biased;
            result = &mut settled => panic!("settled before registration dropped: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }

        registered_tx.send_replace(Vec::new());
        assert_eq!(settled.await, DiscoveryDialResult::ShortLivedRegistered);
    }

    #[tokio::test(start_paused = true)]
    async fn dial_completion_with_live_registration_is_connected_elsewhere() {
        let peer_id = peer_id(9);
        let (_registered_tx, mut registered_rx) =
            tokio::sync::watch::channel(vec![peer_id.clone()]);
        let dial = tokio::spawn(async { Ok(()) });
        tokio::pin!(dial);

        assert_eq!(
            wait_for_discovery_registration_to_settle(&mut registered_rx, &peer_id, &mut dial)
                .await,
            DiscoveryDialResult::ConnectedElsewhere
        );
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
