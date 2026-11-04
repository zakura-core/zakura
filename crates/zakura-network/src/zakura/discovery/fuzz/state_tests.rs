//! Scenarios, generated histories, and mutants for discovery's machine drivers.

use proptest::prelude::*;

use super::super::protocol::{RecordVerdict, MAX_SERVICE_SUMMARIES_PER_RESPONSE};
use super::*;

#[test]
#[allow(
    clippy::print_stdout,
    reason = "fresh-process checks compare the signed sample history"
)]
fn controlled_disclosure_replays_signed_samples_across_insertion_orders() {
    fn history(reverse: bool) -> Vec<Vec<Vec<u8>>> {
        let mut book = ZakuraDiscoveryBook::with_disclosure_key(
            ZakuraDiscoveryBookLimits {
                max_records: 16,
                max_imported_records_per_response: SAMPLE_WINDOW,
                ..ZakuraDiscoveryBookLimits::default()
            },
            None,
            DisclosureKey::new([7; 32]),
        );
        let mut records: Vec<_> = (0..12)
            .map(|index| {
                record(
                    &secret(80 + index),
                    10 + index,
                    1,
                    START + 4 * DISCLOSURE_EPOCH,
                )
            })
            .collect();
        if reverse {
            records.reverse();
        }
        for record in records {
            let source = record.body.node_id;
            book.import_record(record, Some(source), START, &context(START))
                .unwrap();
        }
        let mut rng = StdRng::seed_from_u64(42);
        let mut history = Vec::new();
        for epoch in 0..3 {
            for requester in 0..RELAYS {
                let records = book.sample_peers_with_rng(
                    SAMPLE_WINDOW,
                    &[],
                    &[],
                    START + epoch * DISCLOSURE_EPOCH,
                    relay(requester),
                    &mut rng,
                );
                assert_eq!(records.len(), SAMPLE_WINDOW);
                history.push(
                    records
                        .into_iter()
                        .map(|record| {
                            let mut bytes = record.body.encode_for_signature().unwrap();
                            bytes.extend_from_slice(&record.signature.to_bytes());
                            bytes
                        })
                        .collect(),
                );
            }
        }
        history
    }
    let expected = history(false);
    assert_eq!(
        expected,
        history(true),
        "hash-table order must not change disclosure replay"
    );
    println!(
        "signed_disclosure_history={}",
        serde_json::to_string(&expected).unwrap()
    );
}

fn supplier() -> impl Strategy<Value = Supplier> {
    prop_oneof![
        2 => Just(Supplier::Author),
        3 => (0..RELAYS).prop_map(Supplier::Relay),
        1 => Just(Supplier::Nobody),
    ]
}

fn book_op() -> impl Strategy<Value = BookOp> {
    prop_oneof![
        6 => (0..AUTHORS, 0u64..4, -400i64..120, supplier()).prop_map(
            |(author, sequence, expires_in, supplier)| BookOp::Import {
                author,
                sequence,
                expires_in,
                supplier,
            }
        ),
        1 => (0..AUTHORS).prop_map(BookOp::DialSuccess),
        1 => (0..AUTHORS).prop_map(BookOp::DialFailure),
        2 => (0..RELAYS, 0usize..5).prop_map(|(requester, limit)| BookOp::Sample { requester, limit }),
        2 => prop_oneof![0u64..60, 500u64..700].prop_map(BookOp::Advance),
    ]
}

fn run_book(
    ops: impl IntoIterator<Item = BookOp>,
    seed: u64,
    mut after_step: impl FnMut(&mut ZakuraDiscoveryBook, u64),
) {
    let mut driver = BookDriver::new(seed);
    for op in ops {
        driver.apply(op);
        driver.check(&mut after_step);
    }
}

proptest! {
 #![proptest_config(ProptestConfig::with_cases(48))]
 #[test]
 fn book_matches_independent_model(seed in any::<u64>(), ops in prop::collection::vec(book_op(), 1..64)) { run_book(ops, seed, |_, _| {}); }
 #[test]
 fn book_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) { run_book(decode_book_v2(&bytes), 0, |_, _| {}); }
}

fn state_op() -> impl Strategy<Value = StateOp> {
    let key = || (0..PEERS, 0u64..3, 0u64..3);
    prop_oneof![
        3 => (key(), any::<bool>()).prop_map(|((peer, conn_id, session_id), outbound)| StateOp::Admit {
            peer,
            conn_id,
            session_id,
            outbound,
        }),
        1 => key().prop_map(|(peer, conn_id, session_id)| StateOp::RemoveSession {
            peer,
            conn_id,
            session_id,
        }),
        1 => (0..PEERS, 0u64..3).prop_map(|(peer, conn_id)| StateOp::RemoveConnection { peer, conn_id }),
        2 => (0..PEERS, any::<bool>()).prop_map(|(peer, connected)| StateOp::SetConnected { peer, connected }),
        2 => key().prop_map(|(peer, conn_id, session_id)| StateOp::StartHello {
            peer,
            conn_id,
            session_id,
        }),
        2 => (0usize..8).prop_map(StateOp::FinishHello),
        3 => (key(), 1u64..60).prop_map(|((peer, conn_id, session_id), ttl)| StateOp::ReceiveServices {
            peer,
            conn_id,
            session_id,
            ttl,
        }),
        2 => (0u64..40).prop_map(StateOp::Advance),
    ]
}

fn run_state(
    ops: impl IntoIterator<Item = StateOp>,
    mut after_step: impl FnMut(&mut DiscoveryState, &StateModel),
) {
    let mut driver = StateDriver::new();
    for op in ops {
        driver.apply(op);
        after_step(&mut driver.state, &driver.model);
        driver.check_reads();
        driver.check_sessions();
    }
}

proptest! {
 #![proptest_config(ProptestConfig::with_cases(256))]
 #[test]
 fn discovery_state_matches_independent_model(ops in prop::collection::vec(state_op(), 1..64)) { run_state(ops, |_, _| {}); }
 #[test]
 fn state_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) { run_state(decode_state_v2(&bytes), |_, _| {}); }
}

// Worked scenarios.

/// Session A's `Hello` verifies, B replaces A, and then A's verification
/// completes. A installs no record, no service hint, and no live summary, and
/// B stays admitted and serves normally.
#[test]
fn late_verification_for_a_replaced_session_changes_nothing() {
    let mut state = discovery_state();
    let node_id = peer_secret(0).public();
    let connected = [node_id];
    let a = session(0, 1, 1);
    let b = session(0, 1, 2);
    let now = START;

    assert_eq!(
        state.admit_session(a.clone(), ServicePeerDirection::Inbound),
        ServiceAdmissionDecision::Admit
    );
    let verified_for_a =
        VerifiedRecord::verify(hello(0), &context(now)).expect("the hello is authentic");
    assert_eq!(
        state.admit_session(b.clone(), ServicePeerDirection::Inbound),
        ServiceAdmissionDecision::Admit
    );

    let late = state.install_session_record(&a, verified_for_a, &connected, now, &mut Vec::new());
    assert!(matches!(late, Err(SessionRecordError::Superseded)));
    assert_eq!(
        late.map_err(|error| error.verdict()),
        Err(RecordVerdict::Drop),
        "A's late result does not penalize the peer"
    );
    state
        .receive_session_live_summaries(&a, node_id, services(node_id, now + 10), &connected, now)
        .expect("the summaries decode");
    assert_eq!(state.book().get(&node_id), None);
    assert_eq!(state.active_services(node_id, &connected, now), None);
    assert_eq!(state.live_service_summaries(node_id, &connected, now), None);
    assert!(
        state.is_current_session(&b),
        "A's late result keeps B admitted"
    );
    assert_eq!(state.peer_snapshot().inbound_peers, 1);

    // A's late disconnect does not remove B either.
    state.remove_session(&a);
    assert!(state.is_current_session(&b));

    let verified_for_b =
        VerifiedRecord::verify(hello(0), &context(now)).expect("the hello is authentic");
    assert_eq!(
        state
            .install_session_record(&b, verified_for_b, &connected, now, &mut Vec::new())
            .ok(),
        Some(ImportOutcome::Added)
    );
    state
        .receive_session_live_summaries(&b, node_id, services(node_id, now + 10), &connected, now)
        .expect("the summaries decode");
    assert_eq!(
        state
            .live_service_summaries(node_id, &connected, now)
            .map(|summaries| summaries.len()),
        Some(1)
    );
}

/// A replacement that changes direction needs a free slot in the destination
/// pool. While that pool is full, the replacement is rejected and the old
/// session stays admitted. A same-direction replacement keeps its slot.
#[test]
fn direction_change_replacement_respects_destination_cap() {
    let mut state = discovery_state();
    let admit = |state: &mut DiscoveryState, key: &DiscoverySessionKey, outbound| {
        state.admit_session(key.clone(), direction(outbound))
    };
    for peer in 0..PEER_CAP {
        assert_eq!(
            admit(&mut state, &session(peer, 1, 1), true),
            ServiceAdmissionDecision::Admit
        );
    }
    let inbound = session(PEER_CAP, 1, 1);
    assert_eq!(
        admit(&mut state, &inbound, false),
        ServiceAdmissionDecision::Admit
    );

    let to_outbound = session(PEER_CAP, 2, 1);
    assert_eq!(
        admit(&mut state, &to_outbound, true),
        ServiceAdmissionDecision::RejectFull,
        "the outbound pool is full"
    );
    assert!(state.is_current_session(&inbound), "the old session stays");
    assert_eq!(state.peer_snapshot().inbound_peers, 1);
    assert_eq!(state.peer_snapshot().outbound_peers, PEER_CAP);

    let same_direction = session(0, 2, 1);
    assert_eq!(
        admit(&mut state, &same_direction, true),
        ServiceAdmissionDecision::Admit,
        "a same-direction replacement keeps its slot"
    );
    assert_eq!(state.peer_snapshot().outbound_peers, PEER_CAP);

    state.remove_session(&same_direction);
    assert_eq!(
        admit(&mut state, &to_outbound, true),
        ServiceAdmissionDecision::Admit,
        "a free outbound slot admits the direction change"
    );
    assert_eq!(state.peer_snapshot().inbound_peers, 0);
    assert_eq!(state.peer_snapshot().outbound_peers, PEER_CAP);
}

/// A peer's record hint ends at its signed expiry, and its live summaries end
/// at their local TTL. Moving the time past the signed expiry while the local
/// TTL has not passed drops the record hint and keeps the live summaries.
///
/// Both deadlines are Unix seconds today; the test pins that they are two
/// separate deadlines.
#[test]
fn signed_record_expiry_does_not_expire_the_live_summary_ttl() {
    let mut state = discovery_state();
    let node_id = peer_secret(0).public();
    let connected = [node_id];
    let a = session(0, 1, 1);
    let now = START;
    let signed_expiry = now + 5;

    state.admit_session(a.clone(), ServicePeerDirection::Inbound);
    let short_lived = record_with_services(
        &peer_secret(0),
        100,
        1,
        signed_expiry,
        vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
    );
    let verified = VerifiedRecord::verify(short_lived, &context(now)).expect("authentic");
    state
        .install_session_record(&a, verified, &connected, now, &mut Vec::new())
        .expect("the record installs");
    state
        .receive_session_live_summaries(&a, node_id, services(node_id, u64::MAX), &connected, now)
        .expect("the summaries decode");
    assert_eq!(
        state.active_services(node_id, &connected, now),
        Some(vec![
            ZakuraServiceId::block_sync(),
            ZakuraServiceId::discovery()
        ]),
        "the record hint lists its signed services"
    );

    let after_signed_expiry = signed_expiry + 1;
    assert_eq!(
        state.active_services(node_id, &connected, after_signed_expiry),
        Some(vec![ZakuraServiceId::discovery()]),
        "only the live summaries back the hint after the signed expiry"
    );
    let live = state
        .live_service_summaries(node_id, &connected, after_signed_expiry)
        .expect("the peer keeps an entry");
    assert_eq!(live.len(), 1, "the local TTL has not passed");
    assert_eq!(live[0].expires_at_unix_secs, now + LIVE_SUMMARY_TTL);

    assert_eq!(
        state.live_service_summaries(node_id, &connected, now + LIVE_SUMMARY_TTL),
        None,
        "the local TTL has passed"
    );
}

/// The self-record sequence starts after the persisted sequence or at the
/// wall-clock seed, whichever is later, and strictly increases.
#[test]
fn self_record_sequence_resumes_after_the_persisted_sequence() {
    let restarted = ZakuraDiscoveryLocalConfig {
        last_authored_sequence: Some(100),
        ..local_config()
    };
    let mut local = LocalRecordState::new(&restarted, 5).expect("valid config");
    let ttl = Duration::from_secs(60);
    assert_eq!(local.next_body(START, 5, ttl).sequence, 101);
    assert_eq!(local.next_body(START, 5, ttl).sequence, 102);
    assert_eq!(local.next_body(START, 1_000, ttl).sequence, 1_000);
    assert_eq!(
        local.next_body(START, 1_000, ttl).expires_at_unix_secs,
        START + 60
    );

    let mut state = discovery_state();
    let body = state.next_self_record_body(START, 7);
    let current = ZakuraNodeRecord::sign(body, &secret(1)).expect("the local key signs");
    let refresh = ZakuraDiscoveryConfig::default().refresh_interval.as_secs();
    let expires = current.body.expires_at_unix_secs;
    assert!(state.self_record_is_fresh(&current, expires - refresh - 1));
    assert!(!state.self_record_is_fresh(&current, expires - refresh));
}

#[test]
fn connection_churn_cannot_grow_retained_service_hints() {
    let mut state = discovery_state();
    let peers: Vec<_> = (0..16u8).map(|peer| secret(200 + peer)).collect();
    for (index, peer) in peers.iter().enumerate() {
        let address = 20 + u8::try_from(index).expect("test indexes are small");
        let record = record(peer, address, 1, START + 50_000);
        let verified =
            VerifiedRecord::verify(record, &context(START)).expect("the fixture is authentic");
        state
            .install_connected_peer_record(verified, &[peer.public()], START, &mut Vec::new())
            .expect("the record installs");
        assert_eq!(
            state.retained_hints().len(),
            1,
            "a disconnected peer's hint leaves at the next install"
        );
    }
}

#[test]
fn successive_responses_cannot_grow_one_peers_live_summaries() {
    let mut state = discovery_state();
    let node_id = peer_secret(0).public();
    let summary = DiscoveryServiceSummary {
        peer_exchange_slots_free: 1,
        max_records_per_response: 32,
        expected_disconnect_after_exchange: true,
    };
    let response = |index: usize| {
        let mut envelope =
            ServiceSummaryEnvelope::discovery(&summary).expect("the summary encodes");
        envelope.service_id =
            ZakuraServiceId::new(format!("x{index}")).expect("a short service id is valid");
        Services {
            node_id,
            expires_at_unix_secs: START + 20,
            summaries: vec![envelope],
        }
    };
    let responses = 2 * MAX_SERVICE_SUMMARIES_PER_RESPONSE;
    for index in 0..responses {
        state
            .receive_live_summaries(node_id, response(index), &[node_id], START)
            .expect("the summary decodes");
    }
    let cached = state
        .live_service_summaries(node_id, &[node_id], START)
        .expect("the connected peer has a hint");
    assert_eq!(cached.len(), MAX_SERVICE_SUMMARIES_PER_RESPONSE);
    assert_eq!(
        cached.last().map(|summary| summary.service_id.clone()),
        response(responses - 1)
            .summaries
            .first()
            .map(|envelope| envelope.service_id.clone()),
        "the newest summary stays"
    );
}

#[test]
fn book_trace_reports_exact_install_replacement_and_eviction() {
    let steps = trace_discovery_book_bytes(BOOK_TRACE_WITNESS);
    assert!(steps.iter().all(|step| step.failure.is_none()), "{steps:?}");
    for step in &steps {
        assert_eq!(step.actual, step.expected);
    }
    let changes = || steps.iter().flat_map(|step| &step.actual.changes);
    assert!(changes().any(|change| matches!(change, BookFact::Installed { .. })));
    assert!(changes().any(|change| matches!(change, BookFact::Replaced { .. })));
    assert!(changes().any(|change| matches!(
        change,
        BookFact::Evicted {
            cause: EvictionFact::BookLimit,
            ..
        }
    )));
    assert!(changes().any(|change| matches!(
        change,
        BookFact::Evicted {
            cause: EvictionFact::RelayQuota,
            ..
        }
    )));
    assert!(steps.iter().any(|step| !step.sample.is_empty()));
    let results = || steps.iter().filter_map(|step| step.actual.result);
    assert!(results().any(|result| result == ImportFact::RelayFull));
    assert!(results().any(|result| result == ImportFact::Expired));
    let entries = || steps.iter().flat_map(|step| &step.actual.entries);
    assert!(entries().any(|entry| entry.last_success.is_some()));
    assert!(entries().any(|entry| entry.failures > 0));
    assert!(steps
        .windows(2)
        .any(|pair| pair[1].actual.now > pair[0].actual.now));
    assert_eq!(steps, trace_discovery_book_bytes(BOOK_TRACE_WITNESS));
    assert!(trace_discovery_book_bytes(&[]).is_empty());
}

#[test]
fn session_trace_separates_stale_results_and_retained_hints() {
    let steps = trace_discovery_state_bytes(STATE_TRACE_WITNESS);
    assert!(steps.iter().all(|step| step.failure.is_none()), "{steps:?}");
    for step in &steps {
        assert_eq!(step.actual, step.expected);
    }
    let installations = || {
        steps
            .iter()
            .filter_map(|step| step.actual.installation.as_ref())
    };
    assert!(installations().any(|result| *result == InstallFact::Superseded));
    assert!(installations().any(|result| matches!(result, InstallFact::Installed(_))));
    assert!(steps.iter().any(|step| !step.actual.hints.is_empty()));
    assert!(steps.iter().any(|step| !step.actual.admitted.is_empty()));
    let live =
        |step: &DiscoverySessionStep| step.actual.hints.iter().any(|hint| hint.live_summaries > 0);
    assert!(
        steps
            .windows(2)
            .any(|pair| live(&pair[0]) && !live(&pair[1])),
        "a live summary expires"
    );
    assert!(trace_discovery_state_bytes(&[]).is_empty());
}

/// Fills the book, replaces a record, evicts on quota and on the record limit, samples,
/// moves a record to another relay, rejects on relay quota and expiry, and updates
/// dial metadata.
const BOOK_TRACE_WITNESS: &[u8] = &[
    // Authors 0..=3 import their own records, valid for 110 seconds.
    0, 0, 1, 255, 0, 1, 1, 255, 0, 2, 1, 255, 0, 3, 1, 255, // A dial confirms author 0.
    3, 0, 0, 0, // Author 4 evicts one of the unconfirmed records.
    0, 4, 1, 255, // Sample four, under a window of three.
    6, 0, 4, 0, // Author 0 replaces its record with a later sequence.
    0, 0, 3, 255, // Advance 765 seconds, so every record expires.
    5, 0, 0, 255, // Relay 0 takes authors 0 and 1.
    1, 0, 2, 255, 1, 1, 2, 255, // Advance again, so its entries expire.
    5, 0, 0, 255, // Relay 0's next new record, from author 5, removes them.
    1, 5, 2, 255, // Relay 1 supplies author 5's later sequence, so the attribution moves.
    1, 11, 3, 255, // Relay 1 takes author 0, which fills its quota.
    1, 6, 2, 255, // Relay 1 cannot take author 1.
    1, 7, 2, 255, // Author 2's record expired before it arrived.
    0, 2, 2, 0, // A dial fails for author 0.
    4, 0, 0, 0,
];

/// Installs a current result, rejects a stale result, retains a hint, and reads it.
const STATE_TRACE_WITNESS: &[u8] = &[
    0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 1, 1, 5, 0, 0, 0, 4, 0, 1, 1, 5, 0, 0, 0, 6, 0, 1, 1,
    7, 0, 0, 40, 8, 0, 0, 0, // Peer 0 disconnects while peer 1's `Hello` installs.
    3, 0, 1, 0, 0, 1, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0,
];

#[test]
#[should_panic(expected = "discovery/record-book")]
fn discovery_book_witness_rejects_unearned_confirmation() {
    run_book(decode_book_v2(BOOK_TRACE_WITNESS), 0, |book, now| {
        book.mark_dial_success(&author(0).public(), now);
    });
}

#[test]
#[should_panic(expected = "discovery/record-book")]
fn discovery_book_witness_rejects_extra_dial_failure() {
    run_book(decode_book_v2(BOOK_TRACE_WITNESS), 0, |book, now| {
        book.mark_dial_failure(&author(0).public(), now);
    });
}

#[test]
#[should_panic(expected = "discovery/session-installation")]
fn discovery_state_witness_rejects_lost_admission() {
    run_state(decode_state_v2(STATE_TRACE_WITNESS), |state, model| {
        if let Some((conn, id, _)) = model.peers[0].admitted {
            state.remove_session(&session(0, conn, id));
        }
    });
}

#[test]
#[should_panic(expected = "discovery/session-installation")]
fn discovery_state_witness_rejects_unearned_service_hint() {
    run_state(decode_state_v2(STATE_TRACE_WITNESS), |state, model| {
        if model.peers[0].connected {
            if let Some((conn, id, _)) = model.peers[0].admitted {
                let verified = VerifiedRecord::verify(hello(0), &context(START))
                    .expect("fixture is authentic");
                state
                    .install_session_record(
                        &session(0, conn, id),
                        verified,
                        &[],
                        START,
                        &mut Vec::new(),
                    )
                    .expect("session is current");
            }
        }
    });
}
