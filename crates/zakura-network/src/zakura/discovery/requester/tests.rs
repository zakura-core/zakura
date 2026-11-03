//! Unit tests and witnesses for the requester's queries and their settlement.

use super::model::*;
use super::*;
use crate::zakura::{
    discovery::protocol::{
        DiscoveryServiceSummary, ServiceSummaryEnvelope, Services, ZakuraServiceId,
    },
    framed_channel,
    regulation::{PrecheckSlot, UNFINISHED_EXCHANGE},
    CloseCause,
};

const PEERS: u16 = MSG_DISCOVERY_PEERS as u16;

impl Queries {
    /// Claims as [`Queries::claim`] does, and returns what the admitted
    /// answer's query asked.
    fn claim_asked(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Option<Asked>, ClaimRefused> {
        self.claim(message_type, payload_len)
            .map(|answer| answer.map(|answer| answer.admitted.asked.clone()))
    }
}

/// The payload length of a `Peers` with `records` maximal records.
fn peers_len(records: u16) -> usize {
    usize::try_from(peers_cap(records).bytes).expect("a small cap fits usize")
}

struct Session {
    queries: Queries,
    /// The peer's end of the stream that carries this node's queries.
    written: FramedRecv,
    connection: CancellationToken,
    close_cause: CloseCause,
    cancel: CancellationToken,
}

/// One session's queries, answered on `recv`.
fn session_with(pool: ReservationPool, recv: &FramedRecv) -> Session {
    let (send, written) = framed_channel(4);
    let connection = CancellationToken::new();
    let close_cause = CloseCause::new();
    let cancel = CancellationToken::new();
    let fence = WriterFence::new(connection.clone(), close_cause.clone());
    Session {
        queries: Queries::new(pool, fence, recv, send, cancel.clone()),
        written,
        connection,
        close_cause,
        cancel,
    }
}

/// One session's queries on an in-process channel, which has no
/// transport reader.
fn session(pool: &ReservationPool) -> Session {
    let (_send, recv) = framed_channel(1);
    session_with(pool.clone(), &recv)
}

/// A query is sent only once its previous answer arrives, and each
/// answer claims its reservation exactly once.
#[tokio::test]
async fn one_get_peers_is_live_and_its_answer_claims_it_once() {
    let Session {
        queries,
        mut written,
        ..
    } = session(&reservation_pool());

    assert!(matches!(
        queries.claim_asked(PEERS, 2),
        Err(ClaimRefused::Unsolicited { .. })
    ));
    queries
        .send_get_peers(get_peers(3))
        .await
        .expect("the first query is sent");
    assert!(matches!(
        queries.send_get_peers(get_peers(3)).await,
        Err(QueryUnsent::NotDue)
    ));
    written.recv().await.expect("the query was queued");

    assert_eq!(
        queries.claim_asked(PEERS, peers_len(3)),
        Ok(Some(Asked::Peers { limit: 3 }))
    );
    assert!(matches!(
        queries.claim_asked(PEERS, 2),
        Err(ClaimRefused::Unsolicited { .. })
    ));
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("an answered query frees its kind");
    written.recv().await.expect("the query was queued");
    assert!(matches!(
        queries.claim_asked(PEERS, peers_len(1) + 1),
        Err(ClaimRefused::OverBytes { .. })
    ));
    assert_eq!(queries.claim_asked(GET_PEERS.message_type, 2), Ok(None));
}

/// The payload length of a `Services` with the most summaries that a
/// `GetServices` wanting `wanted_len` services allows.
fn services_len(wanted_len: usize) -> usize {
    usize::try_from(services_cap(wanted_len).bytes).expect("a small cap fits usize")
}

const SERVICES: u16 = MSG_DISCOVERY_SERVICES as u16;

/// A `GetServices` reservation is independent of the `GetPeers` one. Its
/// answer claims it once, returns the query, and stays within
/// `42 + min(n, 8) × 294` bytes.
#[tokio::test]
async fn get_services_is_reserved_beside_get_peers_and_claimed_once() {
    let Session {
        queries,
        mut written,
        ..
    } = session(&reservation_pool());
    let wanted = get_services(vec![ZakuraServiceId::discovery()]);

    assert!(matches!(
        queries.claim_asked(SERVICES, services_len(0)),
        Err(ClaimRefused::Unsolicited { .. })
    ));
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("GetPeers is sent");
    queries
        .send_get_services(wanted.clone())
        .await
        .expect("GetServices is sent beside GetPeers");
    assert!(matches!(
        queries.send_get_services(get_services(Vec::new())).await,
        Err(QueryUnsent::NotDue)
    ));
    written.recv().await.expect("GetPeers was queued");
    written.recv().await.expect("GetServices was queued");

    assert_eq!(
        queries.claim_asked(SERVICES, services_len(1)),
        Ok(Some(Asked::Services(wanted.clone())))
    );
    assert!(matches!(
        queries.claim_asked(SERVICES, services_len(1)),
        Err(ClaimRefused::Unsolicited { .. })
    ));
    assert_eq!(
        queries.claim_asked(PEERS, peers_len(1)),
        Ok(Some(Asked::Peers { limit: 1 }))
    );

    // One wanted service allows one summary, below the row's largest
    // answer.
    queries
        .send_get_services(wanted)
        .await
        .expect("an answered query frees its kind");
    written.recv().await.expect("GetServices was queued");
    assert!(matches!(
        queries.claim_asked(SERVICES, services_len(1) + 1),
        Err(ClaimRefused::OverBytes { .. })
    ));

    // An empty wanted list reserves the largest answer.
    queries
        .send_get_services(get_services(Vec::new()))
        .await
        .expect("an answered query frees its kind");
    written.recv().await.expect("GetServices was queued");
    assert_eq!(
        queries.claim_asked(SERVICES, services_len(0)),
        Ok(Some(Asked::Services(get_services(Vec::new()))))
    );
}

/// A wanted list bounds the answer's summaries, and an empty list allows
/// any service.
#[test]
fn a_services_answer_holds_only_wanted_summaries() -> Result<(), crate::BoxError> {
    let summary = ServiceSummaryEnvelope::discovery(&DiscoveryServiceSummary {
        peer_exchange_slots_free: 1,
        max_records_per_response: 32,
        expected_disconnect_after_exchange: false,
    })?;
    let services = |summaries| {
        DiscoveryMessage::Services(Services {
            node_id: zakura_quic::NodeSecretKey::from_bytes(&[1; 32]).public(),
            expires_at_unix_secs: u64::MAX,
            summaries,
        })
    };
    let asked = |wanted| Asked::Services(get_services(wanted));

    let discovery = vec![ZakuraServiceId::discovery()];
    let header_sync = vec![ZakuraServiceId::header_sync()];
    assert_eq!(
        asked(Vec::new()).check(&services(vec![summary.clone()])),
        Ok(())
    );
    assert_eq!(
        asked(discovery).check(&services(vec![summary.clone()])),
        Ok(())
    );
    assert!(asked(header_sync.clone())
        .check(&services(vec![summary]))
        .is_err());
    assert_eq!(asked(header_sync).check(&services(Vec::new())), Ok(()));
    Ok(())
}

/// The transport reader queues a `Services` only while a `GetServices`
/// is owed, and refuses one above its reservation. A live `GetPeers`
/// lends it no credit.
#[tokio::test]
async fn the_transport_reader_queues_only_owed_services() {
    let slot = PrecheckSlot::default();
    let (_send, recv) = framed_channel(1);
    let recv = recv.with_precheck(slot.clone());
    let Session {
        queries,
        mut written,
        ..
    } = session_with(reservation_pool(), &recv);
    let precheck = slot.get().expect("the queries attach their precheck");

    assert_eq!(
        precheck.check(SERVICES, services_len(0)),
        Err(FrameRejection::Unsolicited)
    );
    queries
        .send_get_peers(get_peers(32))
        .await
        .expect("GetPeers is sent");
    queries
        .send_get_services(get_services(Vec::new()))
        .await
        .expect("the query is sent");
    written.recv().await.expect("GetPeers was queued");
    written.recv().await.expect("the query was queued");
    assert!(matches!(
        precheck.check(SERVICES, services_len(0) + 1),
        Err(FrameRejection::AboveReservation { .. })
    ));
    precheck
        .check(SERVICES, services_len(0))
        .expect("the owed answer queues");
    assert!(matches!(
        precheck.check(SERVICES, services_len(0)),
        Err(FrameRejection::AboveOwed { .. })
    ));
}

/// The transport reader queues at most the one `Peers` this node is owed,
/// even while a `GetServices` is owed too. Its claim releases the queued
/// answer.
#[tokio::test]
async fn the_transport_reader_queues_at_most_the_owed_peers() {
    let slot = PrecheckSlot::default();
    let (_send, recv) = framed_channel(1);
    let recv = recv.with_precheck(slot.clone());
    let Session {
        queries,
        mut written,
        ..
    } = session_with(reservation_pool(), &recv);
    let precheck = slot.get().expect("the queries attach their precheck");

    assert_eq!(
        precheck.check(PEERS, peers_len(1)),
        Err(FrameRejection::Unsolicited)
    );
    queries
        .send_get_services(get_services(Vec::new()))
        .await
        .expect("GetServices is sent");
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("the query is sent");
    written.recv().await.expect("GetServices was queued");
    written.recv().await.expect("the query was queued");

    precheck
        .check(PEERS, peers_len(1))
        .expect("the owed answer queues");
    assert!(matches!(
        precheck.check(PEERS, peers_len(1)),
        Err(FrameRejection::AboveOwed { .. })
    ));
    assert_eq!(
        queries.claim_asked(PEERS, peers_len(1)),
        Ok(Some(Asked::Peers { limit: 1 }))
    );
    assert_eq!(
        precheck.check(PEERS, peers_len(1)),
        Err(FrameRejection::Unsolicited)
    );

    // The claim released the queued answer, so the next answer queues.
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("an answered query frees its kind");
    written.recv().await.expect("the query was queued");
    precheck
        .check(PEERS, peers_len(1))
        .expect("the next owed answer queues");
}

/// A query that is never written gives its reservation and pool entry
/// back, so the next exchange can send one.
#[tokio::test]
async fn an_unwritten_query_gives_its_reservation_back() {
    let pool = reservation_pool();

    let Session {
        queries, cancel, ..
    } = session(&pool);
    cancel.cancel();
    assert!(matches!(
        queries.send_get_peers(get_peers(1)).await,
        Err(QueryUnsent::Closed)
    ));
    assert_eq!(pool.held(), 0);
    assert_eq!(queries.lock().state.len(), 0);

    let Session {
        queries, written, ..
    } = session(&pool);
    drop(written);
    assert!(matches!(
        queries.send_get_peers(get_peers(1)).await,
        Err(QueryUnsent::Closed)
    ));
    assert_eq!(pool.held(), 0);
    assert_eq!(queries.lock().state.len(), 0);
}

/// A full node-wide pool defers the query to the next exchange.
#[tokio::test]
async fn a_full_pool_defers_the_query() {
    let pool = ReservationPool::new(1).expect("one entry is a valid pool");
    let _held = pool.try_entry().expect("the pool has one entry");
    let Session { queries, .. } = session(&pool);
    assert!(matches!(
        queries.send_get_peers(get_peers(1)).await,
        Err(QueryUnsent::NotDue)
    ));
}

/// Retiring the queries closes the connection only when a written query
/// is unanswered, and returns every reservation to the pool.
#[tokio::test]
async fn retiring_closes_the_connection_only_for_an_unanswered_written_query() {
    let pool = reservation_pool();

    // Queued but never written: the transport skips it.
    let Session {
        queries,
        mut written,
        connection,
        ..
    } = session(&pool);
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("the query is queued");
    queries.retire();
    assert!(!connection.is_cancelled());
    assert!(
        written.try_recv().is_err(),
        "a fenced query is never written"
    );
    assert_eq!(pool.held(), 0);

    // Written and answered.
    let Session {
        queries,
        mut written,
        connection,
        ..
    } = session(&pool);
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("the query is queued");
    written.recv().await.expect("the query is written");
    queries
        .claim(PEERS, 2)
        .expect("the answer claims the query");
    queries.retire();
    assert!(!connection.is_cancelled());

    // Written and unanswered.
    let Session {
        queries,
        mut written,
        connection,
        close_cause,
        ..
    } = session(&pool);
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("the query is queued");
    written.recv().await.expect("the query is written");
    queries.retire();
    assert!(connection.is_cancelled());
    assert_eq!(close_cause.get_or("none"), UNFINISHED_EXCHANGE);
    assert_eq!(pool.held(), 0, "retiring returns every reservation");
}

/// A claim ends the query's protocol authority. The answer keeps the pool
/// entry and its kind until it settles, so a refresh cannot stack a second
/// answer behind one that waits for verification.
#[tokio::test]
async fn a_claimed_answer_holds_its_entry_and_kind_until_it_settles() {
    let pool = reservation_pool();
    let Session {
        queries,
        mut written,
        ..
    } = session(&pool);
    queries.send_get_peers(get_peers(1)).await.unwrap();
    written.recv().await.expect("the query was queued");

    let answer = queries
        .claim(PEERS, peers_len(1))
        .expect("the answer fits its query")
        .expect("the frame answers a query");
    assert_eq!(pool.held(), 1, "the claimed answer keeps its pool entry");
    assert!(matches!(
        queries.claim(PEERS, 2),
        Err(ClaimRefused::Unsolicited { .. })
    ));
    assert!(
        matches!(
            queries.send_get_peers(get_peers(1)).await,
            Err(QueryUnsent::NotDue)
        ),
        "an unsettled answer keeps its kind"
    );
    queries
        .send_get_services(get_services(Vec::new()))
        .await
        .expect("the other kind stays independent");
    assert_eq!(pool.held(), 2);

    drop(answer);
    assert_eq!(pool.held(), 1, "settlement frees exactly its own entry");
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("settlement frees the kind");
}

/// Spawns `answers_settled` and checks that something holds it.
async fn held_settled_wait(queries: &Queries) -> zakura_rt::task::JoinHandle<()> {
    let waiting = queries.clone();
    let settled = zakura_rt::task::spawn(async move { waiting.answers_settled().await });
    zakura_rt::task::yield_now().await;
    assert!(!settled.is_finished(), "the hold keeps the wait open");
    settled
}

/// Drops the last `hold` on `settled` and checks that settlement wakes it.
async fn assert_settles<H>(settled: zakura_rt::task::JoinHandle<()>, hold: H) {
    drop(hold);
    zakura_rt::time::timeout(std::time::Duration::from_secs(5), settled)
        .await
        .expect("settlement wakes the wait")
        .unwrap();
}

/// A `Hello` in verification holds the settled wait, and it takes no pool
/// entry and blocks no query.
#[tokio::test]
async fn a_hello_in_verification_holds_only_the_settled_wait() {
    let pool = reservation_pool();
    let Session {
        queries,
        written: _written,
        ..
    } = session(&pool);

    let hello = queries.verifying_hello();
    assert_eq!(pool.held(), 0);
    queries
        .send_get_peers(get_peers(1))
        .await
        .expect("a Hello in verification blocks no query");
    let settled = held_settled_wait(&queries).await;
    assert_settles(settled, hello).await;
}

/// `answers_settled` waits for a claimed answer and ignores a query the
/// peer has not answered.
#[tokio::test]
async fn answers_settled_waits_for_a_claimed_answer_only() {
    let Session {
        queries,
        written: _written,
        ..
    } = session(&reservation_pool());
    queries.send_get_peers(get_peers(1)).await.unwrap();
    queries
        .send_get_services(get_services(Vec::new()))
        .await
        .unwrap();
    zakura_rt::time::timeout(std::time::Duration::from_secs(5), queries.answers_settled())
        .await
        .expect("an unanswered query holds nothing");

    let answer = queries.claim(PEERS, peers_len(1)).unwrap().unwrap();
    let settled = held_settled_wait(&queries).await;
    assert_settles(settled, answer).await;
}

/// A queued answer holds the wait until its claim and settlement, on the
/// real transport precheck.
#[tokio::test]
async fn answers_settled_waits_for_a_frame_the_transport_queued() {
    let slot = PrecheckSlot::default();
    let (_peer, recv) = framed_channel(1);
    let recv = recv.with_precheck(slot.clone());
    let Session {
        queries,
        written: _written,
        ..
    } = session_with(reservation_pool(), &recv);
    queries.send_get_peers(get_peers(1)).await.unwrap();
    slot.get()
        .expect("the session attached its precheck")
        .check(PEERS, peers_len(1))
        .expect("the transport admits the owed answer");

    let settled = held_settled_wait(&queries).await;
    let answer = queries.claim(PEERS, peers_len(1)).unwrap().unwrap();
    zakura_rt::task::yield_now().await;
    assert!(!settled.is_finished(), "the claimed answer holds the wait");
    assert_settles(settled, answer).await;
}

/// The reader can claim the queued frame between the wait's two reads. The
/// wait reads the settling count and the queue under the claim's lock, so the
/// claimed answer still holds it.
#[tokio::test]
async fn answers_settled_sees_a_claim_that_takes_the_queued_frame() {
    let slot = PrecheckSlot::default();
    let (_peer, recv) = framed_channel(1);
    let recv = recv.with_precheck(slot.clone());
    let Session {
        queries,
        written: _written,
        ..
    } = session_with(reservation_pool(), &recv);
    queries.send_get_peers(get_peers(1)).await.unwrap();
    slot.get()
        .expect("the session attached its precheck")
        .check(PEERS, peers_len(1))
        .expect("the transport admits the owed answer");

    let mut live = queries.lock();
    let waiting = queries.clone();
    let settled =
        std::thread::spawn(move || futures::executor::block_on(waiting.answers_settled()));
    // Let the wait reach the lock this test holds.
    std::thread::sleep(std::time::Duration::from_millis(100));
    let answer = queries
        .claim_locked(&mut live, QueryKind::Peers, PEERS, peers_len(1))
        .unwrap();
    drop(live);
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!settled.is_finished(), "the claimed answer holds the wait");

    drop(answer);
    for _ in 0..500 {
        if settled.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(settled.is_finished(), "settlement wakes the wait");
    settled.join().expect("the wait does not panic");
}

fn query_kind() -> impl proptest::strategy::Strategy<Value = QueryKind> {
    proptest::sample::select(QUERY_KINDS.to_vec())
}

fn query_op() -> impl proptest::strategy::Strategy<Value = QueryOp> {
    use proptest::prelude::*;
    prop_oneof![
        3 => (query_kind(), 0usize..4).prop_map(|(kind, size)| QueryOp::Reserve { kind, size }),
        2 => query_kind().prop_map(QueryOp::Queued),
        1 => query_kind().prop_map(QueryOp::ReleaseUnsent),
        3 => (query_kind(), proptest::option::of(0u64..2))
            .prop_map(|(kind, excess)| QueryOp::Claim { kind, excess }),
    ]
}

proptest::proptest! {
    #[test]
    fn settlement_bytes_match_the_model(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=SETTLEMENT_MAX_OPS)) {
        replay_discovery_settlement_bytes(&bytes);
    }
}

/// Exercises each hold: a live query and an unsettled answer each refuse
/// the next query of their kind, an answer outlives its retired session, a
/// retirement with a written unanswered query closes its connection, and the
/// transport and the claim each refuse an answer.
const SETTLEMENT_WITNESS: [u8; 21] = [
    16, 2, 4, 16, 6, 16, 10, 14, 12, 16, 2, 14, 24, 17, 21, 7, 16, 4, 4, 10, 12,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn settlement_witness_exercises_holds_and_retirement() {
    let summary = replay_discovery_settlement_bytes(&SETTLEMENT_WITNESS);
    assert_eq!(
        summary,
        DiscoverySettlementReplaySummary {
            transitions: 21,
            sent: 4,
            not_due: 2,
            written: 2,
            delivered: 3,
            refused: 1,
            claimed: 1,
            oversized: 1,
            settled: 1,
            hellos: 2,
            stale: 1,
            retired: 2,
            closed: 1,
        }
    );
    assert_eq!(
        replay_discovery_settlement_bytes(&[]),
        DiscoverySettlementReplaySummary::default()
    );
    println!("tier=Machine target=discovery_query decoder=2 {summary:?}");
}

/// The pool bounds a session's queries while answers of retired sessions
/// hold their entries.
#[test]
fn settlement_pool_defers_a_query_behind_retired_answers() {
    // Three retired sessions each leave one unsettled `Peers` answer, then
    // the full pool defers the next query until one settles.
    let one_retired_answer = [16, 4, 6, 14];
    let mut bytes: Vec<u8> = one_retired_answer.repeat(3);
    bytes.extend([16, 24, 16]);
    let summary = replay_discovery_settlement_bytes(&bytes);
    assert_eq!(summary.not_due, 1);
    assert_eq!(summary.settled, 1);
    assert_eq!(summary.sent, 4);
}

#[test]
#[should_panic(expected = "discovery/answer-settlement")]
fn settlement_witness_rejects_a_ledger_that_counts_a_phantom_hello() {
    run_settlement(decode_settlement(&[10]), |queries, _| {
        queries
            .settling
            .send_modify(|settling| settling.hellos += 1);
    });
}

#[test]
#[should_panic(expected = "discovery/answer-settlement")]
fn settlement_witness_rejects_leaking_a_pool_entry() {
    run_settlement(decode_settlement(&[16]), |_, pool| {
        std::mem::forget(pool.try_entry());
    });
}

proptest::proptest! {
    #[test]
    fn live_queries_match_independent_model(ops in proptest::collection::vec(query_op(), 1..96)) {
        run(ops, |_| {});
    }

    #[test]
    fn query_bytes_match_the_same_model(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=512)) {
        replay_discovery_query_bytes(&bytes);
    }
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn query_witness_exercises_ownership_and_caps() {
    let summary = replay_discovery_query_bytes(&[8, 24, 2, 4, 14, 6, 1, 23, 0, 4]);
    assert_eq!(
        summary,
        DiscoveryQueryReplaySummary {
            transitions: 10,
            reserved: 3,
            refused: 1,
            released: 1,
            admitted: 1,
            oversized: 1,
            stale: 1,
        }
    );
    assert_eq!(
        replay_discovery_query_bytes(&[]),
        DiscoveryQueryReplaySummary::default()
    );
    println!("tier=Machine target=discovery_query decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "discovery/query-ownership")]
fn query_witness_rejects_losing_a_reservation() {
    run(decode(&[0]), |state| {
        state.live.clear();
    });
}

#[test]
#[should_panic(expected = "discovery/query-ownership")]
fn query_witness_rejects_releasing_a_queued_query() {
    run(decode(&[0, 2]), |state| {
        state
            .live
            .retain(|_, query| query.phase != QueryPhase::Queued);
    });
}
