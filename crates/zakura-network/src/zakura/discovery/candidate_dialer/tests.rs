//! Machine tests for [`CandidateDialerState`]: worked scenarios, the backoff
//! rules, and a proptest against an independent model.
//!
//! The model restates the documented rules. It never calls the code it
//! checks.

use std::net::{IpAddr, SocketAddr};

use proptest::prelude::*;
use zakura_quic::NodeId;

use super::model::*;
use super::state::{classify_dial, DialFinish, DialObservation, PeerRegistration};
use super::state::{CandidateAddress, CandidateDialerState};
use super::*;
use crate::zakura::canonical_ip;

const DIAL_BACKOFF: (Duration, Duration) = (Duration::from_secs(60), Duration::from_secs(3_600));

/// Records an attempt to `node` that reserves `ips`, then completes it.
fn settle(
    state: &mut CandidateDialerState,
    node: NodeId,
    ips: &[IpAddr],
    admitted_ip: Option<IpAddr>,
    result: DiscoveryDialResult,
    now: Instant,
) {
    let attempt = state
        .record_attempt(node, ips.to_vec())
        .expect("the node has no attempt in flight");
    assert!(state.complete(attempt, result, admitted_ip, now));
}

// Worked scenario: an attempt's settlement releases the IPs it reserved, not
// the ones its node advertises now.
#[tokio::test(start_paused = true)]
async fn panicked_attempt_releases_its_original_reservations() {
    let a = node_id(1);
    let b = node_id(2);
    let first = IpAddr::from([93, 184, 216, 34]);
    let second = IpAddr::from([198, 51, 100, 7]);
    let moved = IpAddr::from([203, 0, 113, 9]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let attempt_a = state
        .record_attempt(a, vec![first, second])
        .expect("an idle dialer records A");
    let attempt_b = state
        .record_attempt(b, vec![first])
        .expect("B is a different node");
    assert_eq!(state.in_flight_dials_to(first), 2);

    // A's record now lists another address. A stays in flight under its old
    // reservations, so no second attempt to A starts.
    let moved_candidate = candidate(a, vec![SocketAddr::new(moved, 8233)]);
    assert_eq!(
        state.candidate_addresses(&moved_candidate, now),
        vec![CandidateAddress {
            addr: SocketAddr::new(moved, 8233),
            ip: moved,
        }]
    );
    assert_eq!(state.record_attempt(a, vec![moved]), None);
    assert_eq!(state.in_flight_dials_to(moved), 0);

    // A's worker panics, which the dialer settles as a failure.
    assert!(state.complete(attempt_a, DiscoveryDialResult::Failed, None, now));
    assert_eq!(state.in_flight_dials_to(first), 1);
    assert_eq!(state.in_flight_dials_to(second), 0);
    assert_eq!(state.in_flight_dials_to(moved), 0);
    assert!(state.is_backed_off(a, first, now));
    assert!(state.is_backed_off(a, second, now));
    assert!(!state.is_backed_off(a, moved, now));
    assert!(!state.is_backed_off(b, first, now));
    assert_eq!(state.in_flight_node_ids(), vec![b]);

    assert!(state.complete(attempt_b, DiscoveryDialResult::Registered, Some(first), now));
    assert_eq!(state.in_flight_dials_to(first), 0);
    assert!(state.in_flight_node_ids().is_empty());
}

// Worked scenario: a duplicate completion of a settled attempt changes
// nothing, even when a newer attempt to the same node is in flight.
#[tokio::test(start_paused = true)]
async fn duplicate_completion_leaves_the_next_attempt_in_flight() {
    let node = node_id(1);
    let other = node_id(2);
    let ip = IpAddr::from([93, 184, 216, 34]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let attempt_a = state
        .record_attempt(node, vec![ip])
        .expect("an idle dialer records A");
    assert!(state.complete(attempt_a, DiscoveryDialResult::Registered, Some(ip), now));

    let attempt_b = state
        .record_attempt(node, vec![ip])
        .expect("A settled, so the node can be dialed again");
    let attempt_c = state
        .record_attempt(other, vec![ip])
        .expect("C is a different node");
    assert_ne!(attempt_a, attempt_b);

    for result in [
        DiscoveryDialResult::Failed,
        DiscoveryDialResult::LocalResourceLimit,
        DiscoveryDialResult::Registered,
    ] {
        assert!(!state.complete(attempt_a, result, None, now));
    }
    assert_eq!(state.in_flight_dials_to(ip), 2);
    assert!(!state.is_backed_off(node, ip, now));
    assert!(state.begin_round(now), "a stale local limit must not pause");

    assert!(state.complete(attempt_b, DiscoveryDialResult::Failed, None, now));
    assert!(state.complete(attempt_c, DiscoveryDialResult::Failed, None, now));
    assert_eq!(state.in_flight_dials_to(ip), 0);
}

// A local limit pauses the next round for one tick without backing off the
// candidate's IPs.
#[tokio::test(start_paused = true)]
async fn local_limit_pauses_without_backing_off() {
    let node = node_id(1);
    let ip = IpAddr::from([93, 184, 216, 34]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &[ip],
        None,
        DiscoveryDialResult::LocalResourceLimit,
        now,
    );

    assert!(!state.is_backed_off(node, ip, now));
    assert!(!state.begin_round(now + Duration::from_millis(999)));
    assert!(state.begin_round(now + Duration::from_secs(1)));
    assert!(state.begin_round(now + Duration::from_secs(1)));
}

#[tokio::test(start_paused = true)]
async fn failed_dials_back_off_only_the_failing_node_for_that_ip() {
    let failing_node = node_id(1);
    let honest_node = node_id(2);
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
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        now,
    );
    // The failing node is backed off for this IP, canonicalised so v4/v6
    // variants of the same address share the entry.
    assert!(state.is_backed_off(failing_node, ip, now));
    assert!(state.is_backed_off(failing_node, canonical_ip(mapped_ip), now));
    // A different node advertising the same IP is NOT poisoned: a signature
    // proves node-key control, not IP ownership.
    assert!(!state.is_backed_off(honest_node, ip, now));
    assert!(!state.is_backed_off(failing_node, ip, now + Duration::from_secs(60)));

    let second_attempt = now + Duration::from_secs(60);
    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        second_attempt,
    );
    assert!(state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(119)));
    assert!(!state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(120)));

    settle(
        &mut state,
        failing_node,
        &[ip],
        Some(ip),
        DiscoveryDialResult::Registered,
        second_attempt,
    );
    // A third failure would wait the first step again only if the success
    // removed the entry.
    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        second_attempt,
    );
    assert!(!state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(60)));
}

// V12 F-305587: an ALPN mismatch from one stale address used to put every
// address of the record into the ten-minute floor. The floor now lives on
// the node in the discovery book, and each address gets the ordinary backoff.
#[tokio::test(start_paused = true)]
async fn alpn_mismatch_gives_each_address_the_ordinary_backoff() {
    let node = node_id(1);
    let ips = [
        IpAddr::from([93, 184, 216, 34]),
        IpAddr::from([198, 51, 100, 7]),
    ];
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &ips,
        None,
        DiscoveryDialResult::AlpnMismatch,
        now,
    );

    for ip in ips {
        assert!(state.is_backed_off(node, ip, now + Duration::from_secs(59)));
        assert!(!state.is_backed_off(node, ip, now + Duration::from_secs(60)));
    }
}

// V12 F-58: a record can pair one live endpoint with decoy addresses. A
// success through the live endpoint must not reset the decoys' backoff.
#[tokio::test(start_paused = true)]
async fn success_clears_backoff_only_for_the_admitted_ip() {
    let node = node_id(1);
    let admitted = IpAddr::from([93, 184, 216, 34]);
    let decoy = IpAddr::from([198, 51, 100, 7]);
    let mapped_admitted: IpAddr = "::ffff:93.184.216.34"
        .parse()
        .expect("mapped test address parses");
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &[admitted, decoy],
        None,
        DiscoveryDialResult::Failed,
        now,
    );
    for result in [
        DiscoveryDialResult::Registered,
        DiscoveryDialResult::ConnectedElsewhere,
        DiscoveryDialResult::ShortLivedRegistered,
    ] {
        settle(
            &mut state,
            node,
            &[admitted, decoy],
            Some(mapped_admitted),
            result,
            now,
        );
        assert!(!state.is_backed_off(node, admitted, now));
        // The decoy keeps its single failure: its backoff ends after the
        // first step.
        assert!(
            state.is_backed_off(node, decoy, now + Duration::from_secs(59)),
            "{result:?} must keep the decoy's failure metadata"
        );
        assert!(!state.is_backed_off(node, decoy, now + Duration::from_secs(60)));
    }

    // A success this dial didn't authenticate itself resets nothing.
    settle(
        &mut state,
        node,
        &[decoy],
        None,
        DiscoveryDialResult::ConnectedElsewhere,
        now,
    );
    assert!(state.is_backed_off(node, decoy, now));
}

fn dial_result() -> impl Strategy<Value = DiscoveryDialResult> {
    prop_oneof![
        Just(DiscoveryDialResult::Registered),
        Just(DiscoveryDialResult::ConnectedElsewhere),
        Just(DiscoveryDialResult::ShortLivedRegistered),
        Just(DiscoveryDialResult::Failed),
        Just(DiscoveryDialResult::AlpnMismatch),
        Just(DiscoveryDialResult::LocalResourceLimit),
    ]
}

fn dialer_op() -> impl Strategy<Value = DialerOp> {
    prop_oneof![
        2 => (0u64..25_000).prop_map(DialerOp::Advance),
        2 => Just(DialerOp::Round),
        4 => (
            0..MODEL_NODES,
            prop::collection::vec(0..MODEL_ADDRESSES.len(), 0..6),
            prop::array::uniform3(prop::bool::weighted(0.2)),
        )
            .prop_map(|(node, addresses, refused)| DialerOp::Dial {
                node,
                addresses,
                refused,
            }),
        4 => (
            any::<usize>(),
            dial_result(),
            prop::option::of(0..MODEL_ADDRESSES.len()),
        )
            .prop_map(|(pick, result, admitted)| DialerOp::Complete {
                pick,
                result,
                admitted,
            }),
        1 => (any::<usize>(), dial_result())
            .prop_map(|(pick, result)| DialerOp::Duplicate { pick, result }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn candidate_dialer_matches_independent_model(ops in prop::collection::vec(dialer_op(), 1..80)) {
        run(ops, |_, _, _| {});
    }

    #[test]
    fn candidate_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_candidate_dial_bytes(&bytes);
    }
}

const WITNESS: &[u8] = &[
    2, 7, 0, 3, 0, 3, 4, 0, 3, 0, 30, 0, 1, 0, 0, 2, 7, 0, 3, 0, 5, 1, 0, 0, 0, 10, 0, 1, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn candidate_witness_exercises_identity_backoff_and_local_limits() {
    let summary = replay_candidate_dial_bytes(WITNESS);
    assert_eq!(
        summary,
        CandidateDialReplaySummary {
            attempts: 2,
            completions: 2,
            duplicates: 1,
            local_limits: 1,
            failures: 1,
            rounds: 3,
        }
    );
    assert_eq!(
        replay_candidate_dial_bytes(&[]),
        CandidateDialReplaySummary::default()
    );
    println!("tier=Machine target=discovery_candidate decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "discovery/dial-attempts")]
fn candidate_witness_rejects_premature_reservation_release() {
    run(decode(WITNESS), |state, model, now| {
        if let Some(attempt) = model.in_flight.first() {
            state.complete(attempt.key, DiscoveryDialResult::Registered, None, now);
        }
    });
}

#[test]
#[should_panic(expected = "discovery/dial-attempts")]
fn candidate_witness_rejects_lost_backoff() {
    run(decode(WITNESS), |state, _, now| {
        state.begin_round(now + Duration::from_secs(120));
    });
}

/// Every observation against the outcome the attempt's identity earns. A
/// registration without this attempt's own registration report never credits
/// or charges the attempt, so a dial refused before registration earns no
/// credit even when another connection's registration ended meanwhile.
#[test]
fn registrations_credit_only_the_attempt_that_registered() {
    use DialFinish::{AlpnMismatch, Failed, LocalLimit, Refused, Served};
    use DialObservation::{RegistrationEnded as RegEnded, RegistrationHealthy, WatchClosed};
    use DiscoveryDialResult as R;
    use PeerRegistration::{Absent, Ended, Live};
    let dial = DialObservation::DialEnded;
    let rows = [
        (dial(Served, Live), true, Some(R::ShortLivedRegistered)),
        (dial(Served, Live), false, Some(R::ConnectedElsewhere)),
        (dial(Failed, Live), true, Some(R::ShortLivedRegistered)),
        (dial(Failed, Live), false, Some(R::ConnectedElsewhere)),
        (dial(LocalLimit, Live), false, Some(R::ConnectedElsewhere)),
        (dial(Served, Ended), true, Some(R::ShortLivedRegistered)),
        (dial(Failed, Ended), true, Some(R::ShortLivedRegistered)),
        (dial(Failed, Ended), false, Some(R::Failed)),
        (dial(LocalLimit, Ended), false, Some(R::LocalResourceLimit)),
        (dial(AlpnMismatch, Ended), false, Some(R::AlpnMismatch)),
        (dial(Served, Absent), true, Some(R::ShortLivedRegistered)),
        (dial(Served, Absent), false, Some(R::Failed)),
        (dial(Refused, Absent), false, Some(R::Failed)),
        (dial(Refused, Ended), false, Some(R::Failed)),
        (dial(Refused, Live), false, Some(R::ConnectedElsewhere)),
        (dial(Failed, Absent), true, Some(R::Failed)),
        (dial(LocalLimit, Absent), true, Some(R::LocalResourceLimit)),
        (dial(AlpnMismatch, Absent), false, Some(R::AlpnMismatch)),
        (RegistrationHealthy, true, Some(R::Registered)),
        (RegistrationHealthy, false, Some(R::ConnectedElsewhere)),
        (RegEnded, true, Some(R::ShortLivedRegistered)),
        (RegEnded, false, None),
        (WatchClosed, true, Some(R::Failed)),
        (WatchClosed, false, Some(R::Failed)),
    ];
    for (observation, registered, expected) in rows {
        assert_eq!(
            classify_dial(observation, registered),
            expected,
            "{observation:?} registered={registered}"
        );
    }
}

/// A dial that returns `Ok(())` served its connection only if it reported its
/// own registration. A refusal before registration fails the attempt, and a
/// local admission refusal pauses the dialer instead.
#[test]
fn a_dial_refused_before_registration_is_not_served() {
    let served = || Ok(Ok(()));
    assert_eq!(
        dial_finish(served(), Some(DialRegistration::Registered)),
        DialFinish::Served
    );
    assert_eq!(
        dial_finish(served(), Some(DialRegistration::LocalLimit)),
        DialFinish::LocalLimit
    );
    assert_eq!(dial_finish(served(), None), DialFinish::Refused);

    let refused = DialObservation::DialEnded(dial_finish(served(), None), PeerRegistration::Absent);
    assert_eq!(
        classify_dial(refused, false),
        Some(DiscoveryDialResult::Failed)
    );
    let local = DialObservation::DialEnded(
        dial_finish(served(), Some(DialRegistration::LocalLimit)),
        PeerRegistration::Absent,
    );
    assert_eq!(
        classify_dial(local, false),
        Some(DiscoveryDialResult::LocalResourceLimit)
    );

    // The refused attempt keeps its backoff and charges every reserved IP.
    let ip = IpAddr::from([93, 184, 216, 34]);
    let now = Instant::now();
    let mut state =
        CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
    let first = state.record_attempt(node_id(5), vec![ip]).unwrap();
    state.complete(first, DiscoveryDialResult::Failed, None, now);
    let second = state.record_attempt(node_id(5), vec![ip]).unwrap();
    let result = classify_dial(refused, false).expect("a finished dial classifies");
    state.complete(second, result, Some(ip), now);
    assert!(state.is_backed_off(node_id(5), ip, now));
}

/// Another connection's registration confirms nothing: the record this
/// attempt dialed stays unconfirmed, so this node never samples it to others.
/// The attempt's own registration confirms it.
#[tokio::test]
async fn connected_elsewhere_leaves_the_record_unconfirmed() -> Result<(), crate::BoxError> {
    use crate::zakura::{ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig};
    use zakura_chain::parameters::Network;

    let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    let local = |seed: u8, direct_addrs| ZakuraDiscoveryLocalConfig {
        secret_key: zakura_quic::NodeSecretKey::from_bytes(&[seed; 32]),
        direct_addrs,
        services: vec![ZakuraServiceId::discovery()],
        zakura_protocol_min: handshake.zakura_protocol_min,
        zakura_protocol_max: handshake.zakura_protocol_max,
        network_id: handshake.network_id,
        chain_id: handshake.chain_id,
        last_authored_sequence: None,
    };
    let (_connected_tx, connected) = watch::channel(Vec::new());
    let handle = ZakuraDiscoveryHandle::new(
        local(80, Vec::new()),
        ZakuraDiscoveryConfig::default(),
        connected.clone(),
    )?;
    let candidate = ZakuraDiscoveryHandle::new(
        local(81, vec![SocketAddr::from(([93, 184, 216, 34], 8233))]),
        ZakuraDiscoveryConfig::default(),
        connected,
    )?;
    let record = (*candidate.current_self_record()).clone();
    let node = record.body.node_id;
    handle
        .import_peer_records([record], Some(node_id(82)))
        .await?;
    let sampled = || handle.sample_peers(node_id(83), 10, &[], &[]);
    let trace = ZakuraTrace::default();

    apply_discovery_dial_result(
        &handle,
        &node,
        DiscoveryDialResult::ConnectedElsewhere,
        &trace,
    )
    .await;
    assert!(sampled().await.is_empty(), "the record stays unconfirmed");

    apply_discovery_dial_result(&handle, &node, DiscoveryDialResult::Registered, &trace).await;
    assert_eq!(
        sampled().await.len(),
        1,
        "the attempt's own registration confirms it"
    );
    Ok(())
}

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
    assert_eq!(outcome.phase, DialPhase::WorkerCancelled);
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
            .expect("worker cancellation releases transport capacity before the handshake timeout");
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
/// watch's sender and the senders that report the authenticated IP and the
/// dial's own registration.
fn dial_watch(
    byte: u8,
    registered: Vec<ZakuraPeerId>,
    dial: impl Future<Output = Result<(), ZakuraHandlerError>> + Send + 'static,
) -> (DialWatch, watch::Sender<Vec<ZakuraPeerId>>, DialReport) {
    let (registered_tx, registered_rx) = watch::channel(registered);
    let (admitted_tx, admitted_ip_rx) = oneshot::channel();
    let (registration_tx, registration_rx) = oneshot::channel();
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
        registration_rx,
        own_registration: None,
        registration: PeerRegistration::Absent,
        trace: ZakuraTrace::default(),
    };
    let report = DialReport {
        admitted_ip: admitted_tx,
        registration: registration_tx,
    };
    (watch, registered_tx, report)
}

#[tokio::test(start_paused = true)]
async fn registration_drop_before_healthy_threshold_is_short_lived() {
    let peer = peer_id(7);
    let (watch, registered_tx, report) = dial_watch(7, vec![peer], std::future::pending());
    report.admitted_ip.send(DIAL_IP).unwrap();
    report
        .registration
        .send(DialRegistration::Registered)
        .unwrap();

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
    let (watch, _registered_tx, _report) =
        dial_watch(9, vec![peer_id(9)], std::future::ready(Ok(())));
    assert_eq!(
        watch.run().await.result,
        DiscoveryDialResult::ConnectedElsewhere
    );
}

/// Another connection's registration that ends while this attempt's dial
/// authenticated but has not registered earns no credit. The attempt waits
/// for its own dial, which a refusal then fails.
#[tokio::test(start_paused = true)]
async fn another_connections_registration_end_does_not_credit_an_unregistered_dial() {
    let peer = peer_id(11);
    let (refuse_tx, refuse_rx) = oneshot::channel::<()>();
    let (watch, registered_tx, report) = dial_watch(11, vec![peer], async move {
        let _ = refuse_rx.await;
        Ok(())
    });
    report.admitted_ip.send(DIAL_IP).unwrap();

    let settled = tokio::spawn(watch.run());
    tokio::task::yield_now().await;
    registered_tx.send_replace(Vec::new());
    tokio::task::yield_now().await;
    assert!(
        !settled.is_finished(),
        "a registration end without this dial's report waits for the dial"
    );

    drop(report.registration);
    refuse_tx.send(()).unwrap();
    let outcome = settled.await.unwrap();
    assert_eq!(outcome.result, DiscoveryDialResult::Failed);
}

/// A dial that served its connection before the watch showed the peer ran
/// a real, short connection, not a failed dial.
#[tokio::test(start_paused = true)]
async fn a_served_dial_with_a_delayed_registration_is_short_lived() {
    let (watch, _registered_tx, report) = dial_watch(10, Vec::new(), std::future::ready(Ok(())));
    report.admitted_ip.send(DIAL_IP).unwrap();
    report
        .registration
        .send(DialRegistration::Registered)
        .unwrap();
    let outcome = watch.run().await;
    assert_eq!(outcome.result, DiscoveryDialResult::ShortLivedRegistered);
    assert_eq!(outcome.admitted_ip, Some(DIAL_IP));
}

/// A dial that authenticated its peer and was refused before registration
/// ends with `Ok(())` too. It fails the attempt and keeps the IP's backoff.
/// A local admission refusal pauses the dialer instead.
#[tokio::test(start_paused = true)]
async fn a_dial_refused_before_registration_fails_the_attempt() {
    let (watch, _registered_tx, report) = dial_watch(14, Vec::new(), std::future::ready(Ok(())));
    report.admitted_ip.send(DIAL_IP).unwrap();
    drop(report.registration);
    let outcome = watch.run().await;
    assert_eq!(outcome.result, DiscoveryDialResult::Failed);
    assert_eq!(outcome.admitted_ip, Some(DIAL_IP));

    let (watch, _registered_tx, report) = dial_watch(15, Vec::new(), std::future::ready(Ok(())));
    report.admitted_ip.send(DIAL_IP).unwrap();
    report
        .registration
        .send(DialRegistration::LocalLimit)
        .unwrap();
    assert_eq!(
        watch.run().await.result,
        DiscoveryDialResult::LocalResourceLimit
    );
}

/// Another connection's registration ending says nothing about this
/// attempt, which goes on to report its own dial's failure.
#[tokio::test(start_paused = true)]
async fn another_connections_registration_ending_keeps_the_attempt_waiting() {
    let (fail_tx, fail_rx) = tokio::sync::oneshot::channel::<()>();
    let (watch, registered_tx, _report) = dial_watch(11, vec![peer_id(11)], async move {
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
    let (watch, _registered_tx, _report) = dial_watch(12, vec![peer_id(12)], async move {
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
    let (watch, _registered_tx, report) = dial_watch(13, vec![peer_id(13)], async move {
        let _retained = retained;
        std::future::pending().await
    });
    report.admitted_ip.send(DIAL_IP).unwrap();
    report
        .registration
        .send(DialRegistration::Registered)
        .unwrap();
    let outcome = watch.run().await;
    assert_eq!(outcome.result, DiscoveryDialResult::Registered);
    assert_eq!(outcome.admitted_ip, Some(DIAL_IP));
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(observed.upgrade().is_some(), "the dial keeps serving");
}

/// A cancelled worker reports its identity and its end.
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
