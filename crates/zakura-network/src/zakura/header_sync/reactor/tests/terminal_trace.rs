use super::*;
use crate::zakura::{
    testkit::{TraceCapture, TraceValue},
    CloseCause,
};

fn connect(reactor: &mut HeaderSyncReactor, peer: ZakuraPeerId, session_id: u64) {
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        session_id,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        CloseCause::new(),
    ));
}

fn trace_active_request(reactor: &HeaderSyncReactor, peer: &ZakuraPeerId) -> ActiveHeaderRequest {
    let active = reactor
        .peer_work_queue
        .active(peer)
        .expect("the fixture has one active request")
        .clone();
    reactor.emit_header_request(
        peer,
        active.owner.session_id(),
        active.owner.header_authority(),
        active.request_id,
        active.target.status.selected_tip_hash,
        &active.sent_locator,
        active.max_header_count,
        active.tree_aux_schema,
    );
    active
}

#[tokio::test]
async fn busy_outcome_emits_exact_bounded_request_terminal() {
    let mut capture = TraceCapture::for_test("busy_outcome_emits_exact_bounded_request_terminal")
        .expect("trace capture starts");
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.trace = crate::zakura::ZakuraTrace::new(capture.tracer(), "terminal-test");
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the traced reactor builds");
    let peer = peer();
    let session_id = 7;
    connect(&mut reactor, peer.clone(), session_id);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), session_id);
    let active = trace_active_request(&reactor, &peer);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the request remains active")
        .phase = HeaderTargetPhase::Receiving;

    reactor.handle_headers_outcome(
        peer.clone(),
        session_id,
        active.owner.header_authority(),
        HeadersOutcome {
            request_id: active.request_id.get(),
            target_tip_hash: active.target.status.selected_tip_hash,
            outcome: HeadersOutcomeCode::Busy,
        },
    );

    capture.flush().await;
    let reader = capture.reader().expect("the trace reloads");
    let header_trace = reader.table(HEADER_SYNC_TABLE.table());
    let target_hash = active.target.status.selected_tip_hash.to_string();
    let anchor_hash = active
        .owner
        .header_authority()
        .branch
        .anchor_hash
        .to_string();
    header_trace.assert_row(
        hs_trace::HEADER_REQUEST_TERMINAL,
        &[
            (hs_trace::SESSION_ID, TraceValue::U64(session_id)),
            (hs_trace::DIRECTION, TraceValue::Str("inbound")),
            (
                hs_trace::REQUEST_ID,
                TraceValue::U64(active.request_id.get()),
            ),
            (hs_trace::BRANCH_ANCHOR, TraceValue::Str(&anchor_hash)),
            (hs_trace::BRANCH_TARGET, TraceValue::Str(&target_hash)),
            (hs_trace::TARGET_HASH, TraceValue::Str(&target_hash)),
            (hs_trace::OUTCOME, TraceValue::Str("busy")),
        ],
    );
    assert!(reactor.peer_work_queue.active(&peer).is_none());
    let _ = capture.finish().await.expect("trace capture finishes");
}

#[tokio::test]
async fn timeout_emits_request_terminal_evidence() {
    let mut capture = TraceCapture::for_test("timeout_emits_request_terminal_evidence")
        .expect("trace capture starts");
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.trace = crate::zakura::ZakuraTrace::new(capture.tracer(), "timeout-test");
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the traced reactor builds");
    let peer = peer();
    let session_id = 11;
    connect(&mut reactor, peer.clone(), session_id);
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), session_id);
    trace_active_request(&reactor, &peer);

    let deadline = Instant::now();
    reactor.request_deadlines.insert(peer, deadline);
    reactor.retire_timed_out_requests(deadline);

    capture.flush().await;
    let reader = capture.reader().expect("the trace reloads");
    let rows = reader.table(HEADER_SYNC_TABLE.table()).rows();
    let terminal = rows
        .iter()
        .position(|row| {
            row.get("event").and_then(serde_json::Value::as_str)
                == Some(hs_trace::HEADER_REQUEST_TERMINAL)
        })
        .expect("timeout emits terminal evidence");
    assert_eq!(
        rows[terminal]
            .get(hs_trace::OUTCOME)
            .and_then(serde_json::Value::as_str),
        Some("timed_out")
    );
    assert_eq!(
        rows[terminal]
            .get(hs_trace::DIRECTION)
            .and_then(serde_json::Value::as_str),
        Some("inbound")
    );
    let _ = capture.finish().await.expect("trace capture finishes");
}
