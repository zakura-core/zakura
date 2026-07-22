//! Background dialer that connects out to peers learned through discovery.
//!
//! Bootstrap dials are owned by [`super::dialer`]; this dialer pulls dial
//! candidates from the discovery book, reserves per-IP capacity so it never
//! exceeds the connection caps, and dials them under the same admission control
//! as bootstrap peers. Discovery success is the peer appearing in the supervisor
//! registration watch, not the dial future completing.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    panic::AssertUnwindSafe,
    time::Duration,
};

use futures::FutureExt;
use iroh::{NodeAddr, NodeId};
use tokio::{task::JoinSet, time::Instant};
use tracing::debug;

use super::dialer::native_bootstrap_dial;
use super::protocol::{ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle};
use super::redial::ZAKURA_REDIAL_HEALTHY_CONNECTION;
use crate::zakura::{
    canonical_ip,
    trace::{discovery_trace as d_trace, peer_label, DISCOVERY_TABLE},
    ZakuraEndpoint, ZakuraHandlerError, ZakuraLocalLimits, ZakuraPeerId,
};

/// How often the discovery dialer wakes to look for new candidates.
const ZAKURA_DISCOVERY_DIAL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DiscoveryDialResult {
    Registered,
    ConnectedElsewhere,
    ShortLivedRegistered,
    Failed,
    LocalResourceLimit,
}

impl DiscoveryDialResult {
    fn label(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::ConnectedElsewhere => "connected_elsewhere",
            Self::ShortLivedRegistered => "short_lived_registered",
            Self::Failed => "failed",
            Self::LocalResourceLimit => "local_resource_limit",
        }
    }
}

#[derive(Debug)]
struct DiscoveryDialWorkerResult {
    node_id: NodeId,
    reserved_ips: Vec<IpAddr>,
    result: DiscoveryDialResult,
}

#[derive(Copy, Clone, Debug)]
struct DiscoveryIpBackoff {
    failure_count: u32,
    retry_at: Instant,
}

/// Spawn the long-lived discovery candidate dialer for `endpoint`.
///
/// Returns the task handle so the caller can track it under the endpoint
/// shutdown owner; the loop also observes the endpoint shutdown token directly.
pub(crate) fn spawn_native_discovery_dialer(
    endpoint: ZakuraEndpoint,
    discovery: ZakuraDiscoveryHandle,
    limits: ZakuraLocalLimits,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_native_discovery_dialer(endpoint, discovery, limits))
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
) {
    let shutdown = endpoint.background_shutdown_token();
    let trace = endpoint.trace();
    let mut registered = endpoint.supervisor().subscribe();
    let mut in_flight = HashSet::new();
    let mut in_flight_by_ip = HashMap::new();
    let mut dial_backoff_by_ip = HashMap::new();
    let dial_backoff = discovery.dial_backoff().await;
    let mut workers = JoinSet::new();

    loop {
        if shutdown.is_cancelled() {
            return;
        }
        prune_discovery_ip_backoff(&mut dial_backoff_by_ip, dial_backoff.1, Instant::now());

        spawn_discovery_dial_candidates(
            &endpoint,
            &discovery,
            &limits,
            &mut in_flight,
            &mut in_flight_by_ip,
            &dial_backoff_by_ip,
            &mut workers,
        )
        .await;

        tokio::select! {
            biased;
            // Endpoint shutdown cancels this token. The dialer holds an endpoint
            // clone, so the supervisor registration watch never closes on its
            // own; this is the only reliable teardown signal.
            _ = shutdown.cancelled() => return,
            joined = workers.join_next(), if !workers.is_empty() => {
                match joined {
                    Some(Ok(worker_result)) => {
                        in_flight.remove(&worker_result.node_id);
                        release_discovery_in_flight_ips(
                            &mut in_flight_by_ip,
                            &worker_result.reserved_ips,
                        );
                        apply_discovery_ip_dial_result(
                            &mut dial_backoff_by_ip,
                            &worker_result.reserved_ips,
                            worker_result.result,
                            dial_backoff,
                            Instant::now(),
                        );
                        apply_discovery_dial_result(
                            &discovery,
                            &worker_result.node_id,
                            worker_result.result,
                            &trace,
                        ).await;
                    }
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
            _ = tokio::time::sleep(ZAKURA_DISCOVERY_DIAL_INTERVAL) => {}
        }
    }
}

async fn spawn_discovery_dial_candidates(
    endpoint: &ZakuraEndpoint,
    discovery: &ZakuraDiscoveryHandle,
    limits: &ZakuraLocalLimits,
    in_flight: &mut HashSet<NodeId>,
    in_flight_by_ip: &mut HashMap<IpAddr, usize>,
    dial_backoff_by_ip: &HashMap<IpAddr, DiscoveryIpBackoff>,
    workers: &mut JoinSet<DiscoveryDialWorkerResult>,
) {
    if !endpoint.has_native_admission_capacity() {
        return;
    }

    let in_flight_node_ids: Vec<_> = in_flight.iter().copied().collect();
    for candidate in discovery.dial_candidates(&[], &in_flight_node_ids).await {
        if !endpoint.has_native_admission_capacity() {
            return;
        }
        let Some((node_addr, reserved_ips)) = discovery_node_addr_with_reserved_ip_capacity(
            endpoint,
            &candidate,
            in_flight_by_ip,
            dial_backoff_by_ip,
            Instant::now(),
        )
        .await
        else {
            continue;
        };
        let node_id = candidate.node_id;
        if !in_flight.insert(node_id) {
            continue;
        }
        reserve_discovery_in_flight_ips(in_flight_by_ip, &reserved_ips);

        discovery.mark_dial_attempt(&node_id).await;
        metrics::counter!("zakura.p2p.discovery.dial.started").increment(1);
        workers.spawn({
            let reserved_ips_on_panic = reserved_ips.clone();
            AssertUnwindSafe(run_discovery_dial_once(
                endpoint.clone(),
                node_addr,
                limits.clone(),
                node_id,
                reserved_ips,
            ))
            .catch_unwind()
            .map(move |result| {
                recover_discovery_dial_worker_panic(node_id, reserved_ips_on_panic, result)
            })
        });
    }
}

fn recover_discovery_dial_worker_panic(
    node_id: NodeId,
    reserved_ips: Vec<IpAddr>,
    result: std::thread::Result<DiscoveryDialWorkerResult>,
) -> DiscoveryDialWorkerResult {
    result.unwrap_or_else(|_| {
        metrics::counter!("zakura.p2p.discovery.dial.worker_panicked").increment(1);
        tracing::error!(?node_id, "Zakura discovery dial worker panicked");
        DiscoveryDialWorkerResult {
            node_id,
            reserved_ips,
            result: DiscoveryDialResult::Failed,
        }
    })
}

async fn discovery_node_addr_with_reserved_ip_capacity(
    endpoint: &ZakuraEndpoint,
    candidate: &ZakuraDiscoveryDialCandidate,
    in_flight_by_ip: &HashMap<IpAddr, usize>,
    dial_backoff_by_ip: &HashMap<IpAddr, DiscoveryIpBackoff>,
    now: Instant,
) -> Option<(NodeAddr, Vec<IpAddr>)> {
    let mut direct_addrs = Vec::new();
    let mut reserved_ips = Vec::new();
    for addr in &candidate.direct_addrs {
        let dial_ip = canonical_ip(addr.ip());
        if !discovery_ip_is_in_backoff(dial_backoff_by_ip, dial_ip, now)
            && can_accept_discovery_dial_ip(endpoint, dial_ip, in_flight_by_ip).await
        {
            if !reserved_ips.contains(&dial_ip) {
                reserved_ips.push(dial_ip);
            }
            direct_addrs.push(*addr);
        }
    }

    (!direct_addrs.is_empty()).then(|| {
        (
            NodeAddr::new(candidate.node_id).with_direct_addresses(direct_addrs),
            reserved_ips,
        )
    })
}

async fn can_accept_discovery_dial_ip(
    endpoint: &ZakuraEndpoint,
    remote_ip: IpAddr,
    in_flight_by_ip: &HashMap<IpAddr, usize>,
) -> bool {
    let in_flight = in_flight_by_ip.get(&remote_ip).copied().unwrap_or_default();
    endpoint
        .supervisor()
        .can_accept_remote_ip_with_in_flight(remote_ip, in_flight)
        .await
}

fn reserve_discovery_in_flight_ips(in_flight_by_ip: &mut HashMap<IpAddr, usize>, ips: &[IpAddr]) {
    for ip in ips {
        *in_flight_by_ip.entry(*ip).or_default() += 1;
    }
}

fn release_discovery_in_flight_ips(in_flight_by_ip: &mut HashMap<IpAddr, usize>, ips: &[IpAddr]) {
    for ip in ips {
        let Some(count) = in_flight_by_ip.get_mut(ip) else {
            continue;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            in_flight_by_ip.remove(ip);
        }
    }
}

fn discovery_ip_is_in_backoff(
    dial_backoff_by_ip: &HashMap<IpAddr, DiscoveryIpBackoff>,
    ip: IpAddr,
    now: Instant,
) -> bool {
    dial_backoff_by_ip
        .get(&ip)
        .is_some_and(|backoff| now < backoff.retry_at)
}

fn prune_discovery_ip_backoff(
    dial_backoff_by_ip: &mut HashMap<IpAddr, DiscoveryIpBackoff>,
    retention: Duration,
    now: Instant,
) {
    dial_backoff_by_ip
        .retain(|_, backoff| now.saturating_duration_since(backoff.retry_at) <= retention);
}

fn apply_discovery_ip_dial_result(
    dial_backoff_by_ip: &mut HashMap<IpAddr, DiscoveryIpBackoff>,
    ips: &[IpAddr],
    result: DiscoveryDialResult,
    dial_backoff: (Duration, Duration),
    now: Instant,
) {
    match result {
        DiscoveryDialResult::Registered
        | DiscoveryDialResult::ConnectedElsewhere
        | DiscoveryDialResult::ShortLivedRegistered => {
            for ip in ips {
                dial_backoff_by_ip.remove(ip);
            }
        }
        DiscoveryDialResult::Failed => {
            for ip in ips {
                let failure_count = dial_backoff_by_ip
                    .get(ip)
                    .map_or(1, |backoff| backoff.failure_count.saturating_add(1));
                dial_backoff_by_ip.insert(
                    *ip,
                    DiscoveryIpBackoff {
                        failure_count,
                        retry_at: now
                            + discovery_ip_dial_backoff(
                                failure_count,
                                dial_backoff.0,
                                dial_backoff.1,
                            ),
                    },
                );
            }
        }
        DiscoveryDialResult::LocalResourceLimit => {}
    }
}

fn discovery_ip_dial_backoff(
    failure_count: u32,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> Duration {
    let shift = failure_count.saturating_sub(1).min(10);
    dial_backoff_base
        .saturating_mul(1u32 << shift)
        .min(dial_backoff_max)
}

async fn run_discovery_dial_once(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    node_id: NodeId,
    reserved_ips: Vec<IpAddr>,
) -> DiscoveryDialWorkerResult {
    let Ok(peer_id) = ZakuraPeerId::new(node_id.as_bytes().to_vec()) else {
        return DiscoveryDialWorkerResult {
            node_id,
            reserved_ips,
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
            node_id,
            reserved_ips,
            result: DiscoveryDialResult::ConnectedElsewhere,
        };
    }

    let dial = tokio::spawn({
        let endpoint = endpoint.clone();
        async move { native_bootstrap_dial(&endpoint, node_addr, &limits).await }
    });
    tokio::pin!(dial);

    let result = loop {
        if registered
            .borrow_and_update()
            .iter()
            .any(|id| id == &peer_id)
        {
            break wait_for_discovery_registration_to_settle(&mut registered, &peer_id, &mut dial)
                .await;
        }

        tokio::select! {
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
        node_id,
        reserved_ips,
        result,
    }
}

async fn wait_for_discovery_registration_to_settle(
    registered: &mut tokio::sync::watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &ZakuraPeerId,
    dial: &mut std::pin::Pin<
        &mut tokio::task::JoinHandle<Result<(), crate::zakura::ZakuraHandlerError>>,
    >,
) -> DiscoveryDialResult {
    let healthy = tokio::time::sleep(ZAKURA_REDIAL_HEALTHY_CONNECTION);
    tokio::pin!(healthy);

    loop {
        if !registered
            .borrow_and_update()
            .iter()
            .any(|id| id == peer_id)
        {
            return DiscoveryDialResult::ShortLivedRegistered;
        }

        tokio::select! {
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
    trace: &crate::zakura::ZakuraTrace,
) {
    trace.emit_with(DISCOVERY_TABLE, |row| {
        row.insert(
            d_trace::EVENT.to_string(),
            serde_json::Value::String(d_trace::DISCOVERY_DIAL_RESULT.to_string()),
        );
        row.insert(
            d_trace::RESULT.to_string(),
            serde_json::Value::String(result.label().to_string()),
        );
        let peer = ZakuraPeerId::new(node_id.as_bytes().to_vec())
            .map(|peer_id| peer_label(&peer_id))
            .ok();
        row.insert(
            d_trace::PEER.to_string(),
            peer.map_or(serde_json::Value::Null, serde_json::Value::String),
        );
    });

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

    #[test]
    fn panicked_worker_returns_metadata_needed_to_release_reservations() {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mut in_flight = HashSet::from([node_id]);
        let mut in_flight_by_ip = HashMap::new();
        reserve_discovery_in_flight_ips(&mut in_flight_by_ip, &[ip]);

        let worker_result = recover_discovery_dial_worker_panic(
            node_id,
            vec![ip],
            Err(Box::new("test worker panic")),
        );
        assert_eq!(worker_result.node_id, node_id);
        assert_eq!(worker_result.reserved_ips, vec![ip]);
        assert_eq!(worker_result.result, DiscoveryDialResult::Failed);

        in_flight.remove(&worker_result.node_id);
        release_discovery_in_flight_ips(&mut in_flight_by_ip, &worker_result.reserved_ips);
        assert!(in_flight.is_empty());
        assert!(in_flight_by_ip.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_targets_back_off_all_records_for_the_same_ip() {
        let ip = IpAddr::from([93, 184, 216, 34]);
        let mapped_ip: IpAddr = "::ffff:93.184.216.34"
            .parse()
            .expect("mapped test address parses");
        let six_to_four_ip: IpAddr = "2002:5db8:d822::"
            .parse()
            .expect("6to4 test address parses");
        let teredo_ip: IpAddr = "2001:0:c000:22d::a247:27dd"
            .parse()
            .expect("Teredo test address parses");
        assert_eq!(canonical_ip(mapped_ip), ip);
        assert_eq!(canonical_ip(six_to_four_ip), ip);
        assert_eq!(canonical_ip(teredo_ip), ip);
        let now = Instant::now();
        let dial_backoff = (Duration::from_secs(60), Duration::from_secs(3_600));
        let mut backoff_by_ip = HashMap::new();

        apply_discovery_ip_dial_result(
            &mut backoff_by_ip,
            &[ip],
            DiscoveryDialResult::Failed,
            dial_backoff,
            now,
        );
        assert!(discovery_ip_is_in_backoff(&backoff_by_ip, ip, now));
        assert!(discovery_ip_is_in_backoff(
            &backoff_by_ip,
            canonical_ip(mapped_ip),
            now
        ));
        assert!(!discovery_ip_is_in_backoff(
            &backoff_by_ip,
            ip,
            now + Duration::from_secs(60)
        ));

        let second_attempt = now + Duration::from_secs(60);
        apply_discovery_ip_dial_result(
            &mut backoff_by_ip,
            &[ip],
            DiscoveryDialResult::Failed,
            dial_backoff,
            second_attempt,
        );
        assert!(discovery_ip_is_in_backoff(
            &backoff_by_ip,
            ip,
            second_attempt + Duration::from_secs(119)
        ));
        assert!(!discovery_ip_is_in_backoff(
            &backoff_by_ip,
            ip,
            second_attempt + Duration::from_secs(120)
        ));

        apply_discovery_ip_dial_result(
            &mut backoff_by_ip,
            &[ip],
            DiscoveryDialResult::Registered,
            dial_backoff,
            second_attempt,
        );
        assert!(!backoff_by_ip.contains_key(&ip));
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
}
