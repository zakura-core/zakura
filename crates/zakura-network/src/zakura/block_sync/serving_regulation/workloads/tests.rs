use super::{profile::*, runner::*};

fn policy() -> Policy {
    Policy {
        max_blocks_per_response: 1,
        max_inflight_requests: 32,
        max_response_bytes: 33_554_432,
        max_inbound_peers: 4,
        max_outbound_peers: 4,
        request_overhead_bytes: 65_536,
        peer_rate_bytes_per_second: 1_000_000_000,
        peer_rate_capacity_bytes: 100_000_000,
        peer_outstanding_bytes: 2_000_010,
        node_rate_bytes_per_second: 1_000_000_000,
        node_rate_capacity_bytes: 100_000_000,
        node_outstanding_bytes: 2_000_010,
        peer_pending_requests: 1,
        node_pending_requests: 1,
        node_active_requests: 1,
        query_timeout_ms: 8_000,
    }
}

fn request(session: usize, sequence: u64) -> Request {
    Request {
        peer: session,
        session,
        message_sequence: sequence,
        decoded_us: 10,
        retained_us: 10,
        admitted_us: 10,
        pending_release_us: [12, 12],
        committed_us: 13,
        bound_us: 14,
        query_us: [15, 110],
        settlement_us: [1010, 1020],
        start_height: 100,
        count: 1,
        request_overhead: 65_536,
        response_cap: 2_000_010,
        frames: vec![Frame {
            payload_bytes: 100,
            queued_us: 210,
            write_started_us: 310,
            write_returned_us: 4910,
            release_us: [5010, 5020],
        }],
        waits: vec![Wait {
            stage: "reactor_queue".into(),
            interval_us: [10, 11],
        }],
    }
}

fn profile(requests: Vec<Request>, peers: usize) -> Profile {
    Profile {
        version: 1,
        profile: "completed_getblocks_application_lifetimes".into(),
        time_unit: "microseconds".into(),
        observation_boundary: "peer_routine_decode".into(),
        all_observation_counters_reconciled: true,
        completed_request_profiles_verified: true,
        instantaneous_global_balances_reconstructed: false,
        write_return_semantics: "success_or_error_not_peer_receipt".into(),
        peers,
        sessions: (0..peers)
            .map(|peer| Session {
                peer,
                session: peer,
                start_us: 0,
                end_us: 6000,
            })
            .collect(),
        requests,
    }
}

#[test]
fn captured_workload_frame_ownership_delays_admission_past_request_settlement() {
    let profile = profile(vec![request(0, 1), request(0, 2), request(0, 3)], 1);
    let first = replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Forward,
    )
    .unwrap();
    assert_eq!(first.requests[0].admitted_us, 10);
    assert_eq!(first.requests[1].admitted_us, 5020);
    assert_eq!(first.requests[2].admitted_us, 10030);
    assert_eq!(first.requests[2].last_release_us, Some(15040));
    assert!(first.max_external_backlog >= 2);
    assert_eq!(first.max_observed_node_pending, 1);
    assert_eq!(first.max_observed_node_bytes, 2_000_010);
    assert!(first.all_resource_owners_drained);
    let repeat = replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Forward,
    )
    .unwrap();
    assert_eq!(first.requests, repeat.requests);
}

#[test]
fn captured_workload_release_edges_and_session_ties_are_separate_scenarios() {
    let profile = profile(vec![request(0, 1), request(1, 1)], 2);
    let first = replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Start,
        SessionOrder::Forward,
    )
    .unwrap();
    let reverse = replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Reverse,
    )
    .unwrap();
    assert_eq!(first.requests[0].admitted_us, 10);
    assert_eq!(first.requests[1].admitted_us, 5010);
    assert_eq!(reverse.requests[1].admitted_us, 10);
    assert_eq!(reverse.requests[0].admitted_us, 5020);
}

#[test]
fn captured_workload_rate_delay_uses_actual_spending_and_refunds() {
    let profile = profile(vec![request(0, 1), request(0, 2)], 1);
    let mut limited = policy();
    limited.node_rate_capacity_bytes = 2_065_546;
    limited.node_rate_bytes_per_second = 65_636;
    let result = replay(
        &profile,
        &policy(),
        &limited,
        ReleaseEdge::Finish,
        SessionOrder::Forward,
    )
    .unwrap();
    // A full refund would admit early, and charging the entire worst case would
    // delay for over 30 seconds. Actual spending is fixed work plus 100 bytes.
    assert!((1_000_010..=1_000_011).contains(&result.requests[1].admitted_us));
    assert_eq!(
        result.requests[1].last_release_us.unwrap() - result.requests[1].admitted_us,
        5010
    );
}

#[test]
fn captured_workload_uses_the_real_fair_active_slot_waiter() {
    let profile = profile(vec![request(0, 1), request(0, 2), request(1, 1)], 2);
    let mut candidate = policy();
    candidate.node_pending_requests = 2;
    candidate.node_outstanding_bytes = 4_000_020;
    candidate.peer_outstanding_bytes = 4_000_020;
    let result = replay(
        &profile,
        &policy(),
        &candidate,
        ReleaseEdge::Finish,
        SessionOrder::Forward,
    )
    .unwrap();
    // Peer 1 waited first. Polling peer 0 first again must not steal its slot.
    assert_eq!(result.requests[0].admitted_us, 10);
    assert_eq!(result.requests[2].admitted_us, 1020);
    assert_eq!(result.requests[1].admitted_us, 2030);
}

#[test]
fn captured_workload_zero_timestamp_is_not_an_unfinished_owner() {
    let mut request = request(0, 1);
    for timestamp in [
        &mut request.decoded_us,
        &mut request.retained_us,
        &mut request.admitted_us,
        &mut request.committed_us,
        &mut request.bound_us,
    ] {
        *timestamp = 0;
    }
    request.pending_release_us = [0; 2];
    request.query_us = [0; 2];
    request.settlement_us = [0; 2];
    request.frames[0] = Frame {
        payload_bytes: 100,
        queued_us: 0,
        write_started_us: 0,
        write_returned_us: 0,
        release_us: [0; 2],
    };
    request.waits[0].interval_us = [0; 2];
    let profile = profile(vec![request], 1);
    let result = replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Forward,
    )
    .unwrap();
    assert_eq!(result.requests[0].last_release_us, Some(0));
    assert!(result.all_resource_owners_drained);
}

#[test]
fn captured_workload_rejects_policy_and_dependency_mismatches() {
    let mut profile = profile(vec![request(0, 1)], 1);
    let mut captured = policy();
    captured.request_overhead_bytes += 1;
    assert!(replay(
        &profile,
        &captured,
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Forward
    )
    .is_err());
    profile.requests.push(request(0, 2));
    profile.requests.swap(0, 1);
    assert!(profile.validate(&policy(), &policy()).is_err());
    profile.requests[0].query_us[1] = 1;
    assert!(replay(
        &profile,
        &policy(),
        &policy(),
        ReleaseEdge::Finish,
        SessionOrder::Forward
    )
    .is_err());
}

/// The full corpus stays with its trace/metrics/configuration provenance, outside
/// the source tree. This entry point performs no networking or wall-clock waits.
#[test]
#[ignore = "requires a complete local capture and explicit policy files"]
fn captured_workload_local_corpus() {
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };
    let path = |name| {
        std::env::var(name).unwrap_or_else(|_| panic!("set {name} to a local artifact path"))
    };
    let profile: Profile = serde_json::from_reader(BufReader::new(
        File::open(path("GETBLOCKS_WORKLOAD")).unwrap(),
    ))
    .unwrap();
    let read_policy = |name| {
        let document: serde_json::Value =
            serde_json::from_reader(File::open(path(name)).unwrap()).unwrap();
        serde_json::from_value::<Policy>(document["values"].clone()).unwrap()
    };
    let captured = read_policy("GETBLOCKS_CAPTURE_POLICY");
    let candidate = read_policy("GETBLOCKS_CANDIDATE_POLICY");
    let mut reports = Vec::new();
    for edge in [ReleaseEdge::Start, ReleaseEdge::Finish] {
        for order in [SessionOrder::Forward, SessionOrder::Reverse] {
            reports.push(replay(&profile, &captured, &candidate, edge, order).unwrap());
        }
    }
    let output = File::create_new(path("GETBLOCKS_REPLAY_OUTPUT")).unwrap();
    serde_json::to_writer(
        BufWriter::new(output),
        &serde_json::json!({
            "captured_policy": captured,
            "candidate_policy": candidate,
            "reports": reports,
        }),
    )
    .unwrap();
}
