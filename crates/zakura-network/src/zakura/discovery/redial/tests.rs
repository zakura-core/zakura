//! Machine tests for [`RedialState`]: worked scenarios and a proptest against
//! an independent model.
//!
//! The model restates the documented rules. It never calls the code it
//! checks.

use proptest::prelude::*;

use super::model::*;
use super::state::{RedialNext, RedialStart, RedialState};
use super::*;
use crate::zakura::{trace::peer_label, ZAKURA_ALPN_MISMATCH_BACKOFF};

// Worked scenario: local limits retry at the initial backoff and leave the
// remote-failure budget and the backoff growth untouched.
#[test]
fn repeated_local_limits_keep_the_remote_failure_budget() {
    let initial = Duration::from_secs(1);
    let mut state = RedialState::new(RedialPolicy::maintain_bounded(
        initial,
        Duration::from_secs(30),
        2,
    ));

    for _ in 0..10 {
        assert_eq!(state.start(false), RedialStart::Dial);
        assert_eq!(
            state.record(DialResult::LocalLimit),
            RedialNext::RetryAfter(initial)
        );
    }
    // The first counted failure still waits the initial backoff.
    assert_eq!(
        state.record(DialResult::Failed),
        RedialNext::RetryAfter(initial)
    );
    assert_eq!(
        state.record(DialResult::LocalLimit),
        RedialNext::RetryAfter(initial)
    );
    // The second counted failure exhausts the budget of two.
    assert_eq!(state.record(DialResult::Failed), RedialNext::Stop);
}

// A healthy connection keeps its outcome while its deregistration lags, and
// a lasting registration upgrades a failed dial.
#[test]
fn lagging_registration_only_upgrades_an_attempt() {
    let brief = Duration::from_millis(1);
    assert_eq!(
        DialResult::Healthy.with_lagging_registration(brief),
        DialResult::Healthy
    );
    assert_eq!(
        DialResult::Failed.with_lagging_registration(brief),
        DialResult::Failed
    );
    assert_eq!(
        DialResult::Failed.with_lagging_registration(HEALTHY),
        DialResult::Healthy
    );
    assert_eq!(
        DialResult::AlpnMismatch.with_lagging_registration(HEALTHY - brief),
        DialResult::AlpnMismatch
    );
}

fn model_policy() -> impl Strategy<Value = ModelPolicy> {
    let mode = prop_oneof![
        (1usize..5, 1u64..120).prop_map(|(attempts, settle)| ModelMode::ConnectOnce {
            attempts,
            settle: Duration::from_secs(settle),
        }),
        Just(ModelMode::Maintain),
        (1usize..5).prop_map(|attempts| ModelMode::MaintainBounded { attempts }),
    ];
    (mode, 1u64..30, 1u64..120).prop_map(|(mode, initial, max)| ModelPolicy {
        mode,
        initial: Duration::from_secs(initial),
        max: Duration::from_secs(max),
    })
}

fn redial_op() -> impl Strategy<Value = RedialOp> {
    let result = prop_oneof![
        Just(DialResult::Healthy),
        Just(DialResult::Failed),
        Just(DialResult::AlpnMismatch),
        Just(DialResult::LocalLimit),
    ];
    let lasted = prop::option::of(0u64..120);
    prop_oneof![
        (any::<bool>(), lasted.clone())
            .prop_map(|(registered, lasted)| RedialOp::Start { registered, lasted }),
        (result, lasted).prop_map(|(result, lagging)| RedialOp::Dial { result, lagging }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn redial_state_matches_independent_model(
        policy in model_policy(),
        ops in prop::collection::vec(redial_op(), 1..64),
    ) {
        run(policy, ops, |_, _| {});
    }

    #[test]
    fn redial_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_redial_bytes(&bytes);
    }
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn redial_witness_exercises_limits_resets_and_lagging_registration() {
    let summary = replay_redial_bytes(&[1, 0, 29, 3, 0, 0, 7, 0, 0, 3, 0, 0, 3, 1, 60, 5, 0, 0]);
    assert_eq!(
        summary,
        RedialReplaySummary {
            scored: 5,
            local_limits: 1,
            resets: 1,
            lagging_upgrades: 1,
            stops: 0
        }
    );
    assert_eq!(replay_redial_bytes(&[0, 0, 29, 3, 0, 0]).stops, 1);
    assert_eq!(replay_redial_bytes(&[0, 0, 29, 2, 0, 0]).resets, 1);
    assert_eq!(replay_redial_bytes(&[]), RedialReplaySummary::default());
    println!("tier=Machine target=discovery_redial decoder=1 {summary:?}");
}

fn failure_sequence() -> impl Iterator<Item = RedialOp> {
    [
        DialResult::Failed,
        DialResult::LocalLimit,
        DialResult::Failed,
    ]
    .into_iter()
    .map(|result| RedialOp::Dial {
        result,
        lagging: None,
    })
}

fn maintained_policy() -> ModelPolicy {
    ModelPolicy {
        mode: ModelMode::Maintain,
        initial: Duration::from_secs(1),
        max: Duration::from_secs(30),
    }
}

#[test]
#[should_panic(expected = "discovery/redial-policy")]
fn redial_witness_rejects_resetting_after_failure() {
    run(maintained_policy(), failure_sequence(), |state, _| {
        state.registration_changed()
    });
}

#[test]
#[should_panic(expected = "discovery/redial-policy")]
fn redial_witness_rejects_charging_local_limits() {
    run(maintained_policy(), failure_sequence(), |state, result| {
        if result == DialResult::LocalLimit {
            let _ = state.record(DialResult::Failed);
        }
    });
}

fn redial_test_peer_id() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![9u8; 32]).expect("32-byte node id is valid")
}

/// Runs the supervisor without a trace, for tests of its dial policy.
async fn untraced_supervisor<F>(
    peer_id: ZakuraPeerId,
    registered: watch::Receiver<Vec<ZakuraPeerId>>,
    policy: RedialPolicy,
    dial: F,
) where
    F: FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>>,
{
    run_dial_supervisor(peer_id, registered, ZakuraTrace::noop(), policy, dial).await
}

fn count_dial(
    calls: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    result: DialResult,
) -> impl FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>> {
    let calls = calls.clone();
    move || {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { result }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    }
}

fn dial_count(calls: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> usize {
    calls.load(std::sync::atomic::Ordering::SeqCst)
}

/// A `connect_once` dial that keeps failing gives up after `max_attempts`.
#[tokio::test]
async fn dial_supervisor_connect_once_gives_up_after_max_attempts() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 3);

    tokio::time::timeout(
        Duration::from_secs(5),
        untraced_supervisor(
            redial_test_peer_id(),
            registered,
            policy,
            count_dial(&calls, DialResult::Failed),
        ),
    )
    .await
    .expect("connect_once must stop after exhausting its attempts");

    assert_eq!(dial_count(&calls), 3);
}

/// A `connect_once` dial that connects healthily stops without re-dialing.
#[tokio::test]
async fn dial_supervisor_connect_once_stops_after_healthy_connection() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 3);

    tokio::time::timeout(
        Duration::from_secs(5),
        untraced_supervisor(
            redial_test_peer_id(),
            registered,
            policy,
            count_dial(&calls, DialResult::Healthy),
        ),
    )
    .await
    .expect("connect_once returns once it has connected");

    assert_eq!(dial_count(&calls), 1);
}

/// A peer already registered (e.g. it dialed us first) is not re-dialed if
/// it remains registered long enough to count as healthy.
#[tokio::test]
async fn dial_supervisor_connect_once_skips_stably_registered_peer() {
    let peer_id = redial_test_peer_id();
    let (_tx, registered) = tokio::sync::watch::channel(vec![peer_id.clone()]);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::connect_once_with_registered_settle(
        Duration::from_millis(1),
        Duration::from_millis(1),
        3,
        Duration::from_millis(5),
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        untraced_supervisor(
            peer_id,
            registered,
            policy,
            count_dial(&calls, DialResult::Failed),
        ),
    )
    .await
    .expect("an already-connected connect-once peer returns immediately");

    assert_eq!(dial_count(&calls), 0);
}

/// A connect-once upgrade hand-off must not give up just because the peer
/// was momentarily registered by an inbound dial. If that registration drops
/// before it is healthy, the outbound hand-off should retry.
#[tokio::test]
async fn dial_supervisor_connect_once_redials_after_transient_registration() {
    let peer_id = redial_test_peer_id();
    let (registered_tx, registered) = tokio::sync::watch::channel(vec![peer_id.clone()]);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::connect_once_with_registered_settle(
        Duration::from_millis(1),
        Duration::from_millis(1),
        3,
        Duration::from_secs(1),
    );

    let supervisor = tokio::spawn(untraced_supervisor(
        peer_id,
        registered,
        policy,
        count_dial(&calls, DialResult::Healthy),
    ));

    registered_tx
        .send(Vec::new())
        .expect("dial supervisor still watches registrations");

    tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("connect_once exits after retrying a transient registration")
        .expect("dial supervisor task must not panic");

    assert_eq!(dial_count(&calls), 1);
}

/// A `maintain` peer keeps re-dialing after each connection drops.
#[tokio::test]
async fn dial_supervisor_maintain_redials_after_drop() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::maintain(Duration::from_millis(1), Duration::from_millis(1));

    let dial_calls = calls.clone();
    let dial = move || {
        dial_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            DialResult::Healthy
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };

    let supervisor = tokio::spawn(untraced_supervisor(
        redial_test_peer_id(),
        registered,
        policy,
        dial,
    ));

    let deadline = Instant::now() + Duration::from_secs(5);
    while dial_count(&calls) < 3 {
        assert!(
            Instant::now() < deadline,
            "maintain never re-dialed after the connection dropped",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    supervisor.abort();
}

/// A bounded maintained dial (legacy->Zakura upgrades) stops after its
/// attempt limit, so a peer-supplied address that never answers does not
/// get a dial that lives forever.
///
/// Regression test for V12 #298060 and #299229.
#[tokio::test(start_paused = true)]
async fn dial_supervisor_maintain_bounded_gives_up_after_max_attempts() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::maintain_bounded(Duration::from_secs(1), Duration::from_secs(30), 6);

    tokio::time::timeout(
        Duration::from_secs(600),
        untraced_supervisor(
            redial_test_peer_id(),
            registered,
            policy,
            count_dial(&calls, DialResult::Failed),
        ),
    )
    .await
    .expect("a bounded maintained dial must stop after exhausting its attempts");

    assert_eq!(dial_count(&calls), 6);
}

/// A healthy connection resets the failure count of a bounded maintained
/// dial, so a live upgraded peer keeps its dial through repeated drops.
#[tokio::test(start_paused = true)]
async fn dial_supervisor_maintain_bounded_survives_healthy_connection_drops() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = RedialPolicy::maintain_bounded(Duration::from_secs(1), Duration::from_secs(30), 3);

    // Two failed redials follow each healthy connection. Without the reset,
    // the failure after the first healthy connection would be the third
    // and end the dial.
    let dial_calls = calls.clone();
    let dial = move || {
        let call = dial_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result = if call % 3 == 2 {
            DialResult::Healthy
        } else {
            DialResult::Failed
        };
        Box::pin(async move { result }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };

    let supervisor = tokio::spawn(untraced_supervisor(
        redial_test_peer_id(),
        registered,
        policy,
        dial,
    ));

    while dial_count(&calls) < 12 {
        assert!(
            !supervisor.is_finished(),
            "a healthy connection's drop ended a bounded maintained dial after {} dials",
            dial_count(&calls),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!supervisor.is_finished());
    supervisor.abort();
}

/// A lagging deregistration must not turn one healthy connection into a
/// second failed attempt.
#[tokio::test(start_paused = true)]
async fn dial_supervisor_maintain_preserves_healthy_result_while_deregistration_lags() {
    let peer_id = redial_test_peer_id();
    let (registered_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let dial_calls = calls.clone();
    let dial_peer_id = peer_id.clone();
    let dial = move || {
        let call = dial_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let registered_tx = registered_tx.clone();
        let dial_peer_id = dial_peer_id.clone();
        Box::pin(async move {
            if call > 0 {
                return std::future::pending().await;
            }

            let _ = registered_tx.send(vec![dial_peer_id]);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let _ = registered_tx.send(Vec::new());
            });
            DialResult::Healthy
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };

    let supervisor = tokio::spawn(untraced_supervisor(
        peer_id,
        registered,
        RedialPolicy::maintain(Duration::from_secs(1), Duration::from_secs(30)),
        dial,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;
    supervisor.abort();

    assert_eq!(
        dial_count(&calls),
        2,
        "a healthy connection must reset backoff after its registration disappears",
    );
}

/// A bootstrap peer that shares no service beyond discovery closes every
/// connection as soon as the discovery exchange completes. The maintained
/// dial must back off on that, not re-dial at once forever.
#[tokio::test(start_paused = true)]
async fn dial_supervisor_maintain_backs_off_a_peer_that_closes_every_connection() {
    let peer_id = redial_test_peer_id();
    let (registered_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let dial_calls = calls.clone();
    let dial_peer_id = peer_id.clone();
    let dial = move || {
        dial_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let registered_tx = registered_tx.clone();
        let dial_peer_id = dial_peer_id.clone();
        Box::pin(async move {
            // The connection establishes and registers, serves the
            // discovery exchange, and is then closed as discovery-only.
            let _ = registered_tx.send(vec![dial_peer_id]);
            tokio::time::sleep(Duration::from_millis(50)).await;
            // The supervisor observes the deregistration only after this
            // dial returns, so it sees the peer as still registered.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let _ = registered_tx.send(Vec::new());
            });
            DialResult::Failed
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };

    let supervisor = tokio::spawn(untraced_supervisor(
        peer_id,
        registered,
        RedialPolicy::maintain(Duration::from_secs(1), Duration::from_secs(30)),
        dial,
    ));

    tokio::time::sleep(Duration::from_secs(60)).await;
    supervisor.abort();

    // Backing off 1s, 2s, 4s, ... to the 30s ceiling allows six dials in
    // one minute. Without backoff the loop re-dials every 51ms.
    assert_eq!(
        dial_count(&calls),
        6,
        "a peer that closes every connection must use one backoff step per dial",
    );
}

/// A peer on another ALPN is redialed no sooner than
/// [`ZAKURA_ALPN_MISMATCH_BACKOFF`], however short the policy's backoff
/// (zakura-quic DIAL-5).
/// A dial that hits this node's own limit retries after the initial
/// backoff without using up an attempt or growing the backoff.
#[tokio::test(start_paused = true)]
async fn dial_supervisor_retries_local_limits_without_counting_them() {
    const LOCAL_LIMITS: usize = 10;
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dial = {
        let calls = calls.clone();
        move || {
            let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = if call < LOCAL_LIMITS {
                DialResult::LocalLimit
            } else {
                DialResult::Failed
            };
            Box::pin(async move { result }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
        }
    };
    let supervisor = tokio::spawn(untraced_supervisor(
        redial_test_peer_id(),
        registered,
        RedialPolicy::maintain_bounded(Duration::from_secs(1), Duration::from_secs(30), 2),
        dial,
    ));

    // One retry per initial backoff: the backoff never grew.
    tokio::time::sleep(Duration::from_millis(9_500)).await;
    assert_eq!(dial_count(&calls), LOCAL_LIMITS);
    // The local limits used up none of the two attempts.
    tokio::time::timeout(Duration::from_secs(60), supervisor)
        .await
        .expect("the dial stops after its two counted attempts")
        .unwrap();
    assert_eq!(dial_count(&calls), LOCAL_LIMITS + 2);
}

#[tokio::test(start_paused = true)]
async fn dial_supervisor_backs_off_ten_minutes_after_alpn_mismatch() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let supervisor = tokio::spawn(untraced_supervisor(
        redial_test_peer_id(),
        registered,
        RedialPolicy::maintain(Duration::from_secs(1), Duration::from_secs(30)),
        count_dial(&calls, DialResult::AlpnMismatch),
    ));

    tokio::time::sleep(ZAKURA_ALPN_MISMATCH_BACKOFF - Duration::from_secs(1)).await;
    assert_eq!(
        dial_count(&calls),
        1,
        "the redial must wait out the ALPN backoff"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        dial_count(&calls),
        2,
        "the redial runs once the ALPN backoff ends"
    );
    supervisor.abort();
}

/// A trace that queues its rows, and the queue's `(owner, attempt, phase)`
/// reader.
fn captured_trace() -> (ZakuraTrace, impl FnMut() -> Vec<(String, u64, String)>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let trace = ZakuraTrace::new(zakura_jsonl_trace::JsonlTracer::new(tx), "node");
    let rows = move || {
        let mut rows = Vec::new();
        while let Ok(event) = rx.try_recv() {
            let row: serde_json::Value =
                serde_json::from_slice(&event.line).expect("each trace row is JSON");
            assert_eq!(row["event"], "discovery_dial_phase");
            assert_eq!(row["peer"], peer_label(&redial_test_peer_id()));
            rows.push((
                row["owner"]
                    .as_str()
                    .expect("rows name their owner")
                    .to_owned(),
                row["attempt"].as_u64().expect("rows number their attempt"),
                row["phase"]
                    .as_str()
                    .expect("rows name their phase")
                    .to_owned(),
            ));
        }
        rows
    };
    (trace, rows)
}

/// Each redial attempt traces its start, the registrations during its dial,
/// and the dial's return, under its own number.
#[tokio::test(start_paused = true)]
async fn redial_attempts_trace_their_registration_phases() {
    let peer_id = redial_test_peer_id();
    let (registered_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let (trace, mut rows) = captured_trace();
    let dial_peer = peer_id.clone();
    let dial = move || {
        let registered_tx = registered_tx.clone();
        let dial_peer = dial_peer.clone();
        Box::pin(async move {
            let _ = registered_tx.send(vec![dial_peer]);
            tokio::time::sleep(Duration::from_millis(1)).await;
            let _ = registered_tx.send(Vec::new());
            tokio::time::sleep(Duration::from_millis(1)).await;
            DialResult::Failed
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };

    tokio::time::timeout(
        Duration::from_secs(5),
        run_dial_supervisor(
            peer_id,
            registered,
            trace,
            RedialPolicy::connect_once(Duration::from_millis(1), Duration::from_millis(1), 2),
            dial,
        ),
    )
    .await
    .expect("connect_once stops after its two attempts");

    let expected = |attempt, phase: &str| ("redial".to_owned(), attempt, phase.to_owned());
    assert_eq!(
        rows(),
        [0, 1]
            .into_iter()
            .flat_map(|attempt| [
                expected(attempt, "recorded"),
                expected(attempt, "registration_seen"),
                expected(attempt, "registration_ended"),
                expected(attempt, "worker_finished"),
            ])
            .collect::<Vec<_>>()
    );
}

/// A registration from another connection gets an attempt's rows too, since
/// the loop scores it as an attempt. No dial runs for it.
#[tokio::test(start_paused = true)]
async fn a_scored_registration_traces_no_dial() {
    let peer_id = redial_test_peer_id();
    let (registered_tx, registered) = tokio::sync::watch::channel(vec![peer_id.clone()]);
    let (trace, mut rows) = captured_trace();
    let supervisor = tokio::spawn(run_dial_supervisor(
        peer_id,
        registered,
        trace,
        RedialPolicy::maintain_bounded(Duration::from_secs(1), Duration::from_secs(1), 1),
        count_dial(&Default::default(), DialResult::Failed),
    ));
    tokio::time::sleep(Duration::from_millis(1)).await;
    registered_tx.send(Vec::new()).expect("the loop watches");
    tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("one failure ends the bounded dial")
        .expect("the supervisor does not panic");

    let phases: Vec<_> = rows().into_iter().map(|row| row.2).collect();
    assert_eq!(
        phases,
        ["recorded", "registration_seen", "registration_ended"]
    );
}

/// Shutdown drops the supervisor during a dial, which ends the attempt in
/// the trace.
#[tokio::test(start_paused = true)]
async fn a_dropped_supervisor_traces_a_cancelled_dial() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let (trace, mut rows) = captured_trace();
    let dial =
        || Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = DialResult> + Send>>;
    let supervised = run_dial_supervisor(
        redial_test_peer_id(),
        registered,
        trace,
        RedialPolicy::maintain(Duration::from_secs(1), Duration::from_secs(1)),
        dial,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), supervised)
            .await
            .is_err(),
        "the dial never returns"
    );

    let phases: Vec<_> = rows().into_iter().map(|row| row.2).collect();
    assert_eq!(phases, ["recorded", "worker_cancelled"]);
}

/// A dial that panics ends its attempt in the trace as it unwinds.
#[tokio::test]
async fn a_panicking_dial_traces_its_end() {
    let (_tx, registered) = tokio::sync::watch::channel(Vec::<ZakuraPeerId>::new());
    let (trace, mut rows) = captured_trace();
    let dial = || {
        Box::pin(async { panic!("the dial panics") as DialResult })
            as Pin<Box<dyn Future<Output = DialResult> + Send>>
    };
    let supervisor = tokio::spawn(run_dial_supervisor(
        redial_test_peer_id(),
        registered,
        trace,
        RedialPolicy::maintain(Duration::from_secs(1), Duration::from_secs(1)),
        dial,
    ));
    let joined = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("the panic ends the task");
    assert!(joined.is_err_and(|error| error.is_panic()));

    let phases: Vec<_> = rows().into_iter().map(|row| row.2).collect();
    assert_eq!(phases, ["recorded", "worker_panicked"]);
}
