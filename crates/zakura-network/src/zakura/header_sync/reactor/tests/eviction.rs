use super::*;
use crate::zakura::transport::FramedRecv;

/// One admitted header-sync session with the state needed to drive it unproductive.
struct EvictionFixture {
    reactor: HeaderSyncReactor,
    actions: mpsc::Receiver<HeaderPortOperation>,
    snapshot: zakura_header_chain::EngineSnapshot,
    peer: ZakuraPeerId,
    cancel: CancellationToken,
    /// Kept alive so the session's outbound queue never fails for want of a reader.
    _outbound: FramedRecv,
    _snapshots_tx: watch::Sender<Option<zakura_header_chain::EngineSnapshot>>,
    _handle: HeaderSyncHandle,
}

fn eviction_fixture(max_unproductive_header_requests: u32, session_id: u64) -> EvictionFixture {
    let mut startup = startup(CancellationToken::new());
    startup.config.max_unproductive_header_requests = max_unproductive_header_requests;
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the eviction fixture builds");
    let peer = peer();
    let cancel = CancellationToken::new();
    let (send, outbound) = framed_channel(8);
    reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(peer.clone(), session_id, send, cancel.clone()),
    ));
    assert!(
        reactor.peer_state.contains_key(&peer),
        "the fixture peer starts admitted"
    );
    EvictionFixture {
        reactor,
        actions,
        snapshot,
        peer,
        cancel,
        _outbound: outbound,
        _snapshots_tx: snapshots_tx,
        _handle: handle,
    }
}

impl EvictionFixture {
    /// Strike count charged against the currently admitted session, if any.
    fn unproductive_requests(&self) -> Option<u32> {
        self.reactor
            .peer_state
            .get(&self.peer)
            .map(|state| state.unproductive_requests)
    }

    fn is_admitted(&self) -> bool {
        self.reactor.peer_state.contains_key(&self.peer)
    }

    /// Publish one request owned by `session_id` and let its deadline expire unanswered.
    fn time_out_one_request(&mut self, session_id: u64) {
        seed_applying_request(
            &mut self.reactor,
            &self.snapshot,
            self.peer.clone(),
            session_id,
        );
        let deadline = Instant::now();
        self.reactor
            .request_deadlines
            .insert(self.peer.clone(), deadline);
        self.reactor.retire_timed_out_requests(deadline);
    }

    /// Stage one request in the receiving phase, returning its owning scope.
    fn start_receiving(&mut self, session_id: u64) -> zakura_header_chain::HeaderWorkAuthority {
        seed_applying_request(
            &mut self.reactor,
            &self.snapshot,
            self.peer.clone(),
            session_id,
        );
        let active = self
            .reactor
            .peer_work_queue
            .active_mut(&self.peer)
            .expect("the seeded request is active");
        active.phase = HeaderTargetPhase::Receiving;
        active.common_ancestor = None;
        active.owner.header_authority()
    }

    /// Answer the active request with one usable header, completing its target.
    fn answer_with_one_header(&mut self, session_id: u64) {
        let anchor = self.snapshot.frontiers.finalized;
        let scope = self.start_receiving(session_id);
        let active = self
            .reactor
            .peer_work_queue
            .active_mut(&self.peer)
            .expect("the receiving request is active");
        let entry = active
            .entries
            .pop()
            .expect("the seeded request stages exactly one entry");
        let response = Headers {
            request_id: active.request_id.get(),
            target_tip_hash: active.target.status.selected_tip_hash,
            common_ancestor_height: anchor.height,
            common_ancestor_hash: anchor.hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![entry],
        };
        let _ = active;
        self.reactor
            .peer_work_queue
            .set_capacity_for_test(&self.peer, 0, 1);
        self.reactor
            .handle_headers(self.peer.clone(), session_id, scope, response);
        // The port owns the completed target.
        // Clear the request so the next request can enter staging.
        self.reactor.clear_peer_work_for_test(&self.peer);
    }

    /// Answer the active request with the "you are already at my selected tip" reply.
    fn answer_already_at_our_tip(&mut self, session_id: u64) {
        let scope = self.start_receiving(session_id);
        let active = self
            .reactor
            .peer_work_queue
            .active_mut(&self.peer)
            .expect("the receiving request is active");
        let target = zakura_header_chain::Frontier::new(
            active.target.status.selected_tip_height,
            active.target.status.selected_tip_hash,
        );
        // The already-known reply names the target as the common ancestor.
        // The request must include that target in its locator.
        active.sent_locator = zakura_header_chain::HeaderLocator::for_continuation(target);
        active.entries.clear();
        let response = Headers {
            request_id: active.request_id.get(),
            target_tip_hash: target.hash,
            common_ancestor_height: target.height,
            common_ancestor_hash: target.hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: Vec::new(),
        };
        let _ = active;
        self.reactor
            .peer_work_queue
            .set_capacity_for_test(&self.peer, 0, 1);
        self.reactor
            .handle_headers(self.peer.clone(), session_id, scope, response);
    }

    /// Drain and return every action the reactor has emitted so far.
    fn drain_actions(&mut self) -> Vec<HeaderPortOperation> {
        let mut drained = Vec::new();
        while let Ok(action) = self.actions.try_recv() {
            drained.push(action);
        }
        drained
    }
}

#[test]
fn unresponsive_header_peer_is_dropped_after_the_configured_strikes() {
    let mut fixture = eviction_fixture(3, 7);

    for expected in 1..3 {
        fixture.time_out_one_request(7);
        assert_eq!(
            fixture.unproductive_requests(),
            Some(expected),
            "each unanswered request charges exactly one strike"
        );
        assert!(
            fixture.is_admitted() && !fixture.cancel.is_cancelled(),
            "a peer below the configured limit keeps its session"
        );
        assert!(
            fixture.drain_actions().is_empty(),
            "a strike below the limit emits no action"
        );
    }

    fixture.time_out_one_request(7);

    assert!(
        !fixture.is_admitted(),
        "reaching the limit removes the peer's reactor state"
    );
    assert!(
        fixture.cancel.is_cancelled(),
        "reaching the limit cancels the exact session that stopped answering"
    );
    let actions = fixture.drain_actions();
    assert!(
        actions.iter().any(|action| matches!(
            action,
            HeaderPortOperation::DropPeer {
                peer,
                session_id: 7,
                reason: "unresponsive",
            } if peer == &fixture.peer
        )),
        "the drop is reported as its own action: {actions:?}"
    );
}

#[test]
fn a_productive_response_resets_the_unproductive_counter() {
    let mut fixture = eviction_fixture(3, 7);

    for _ in 0..2 {
        fixture.time_out_one_request(7);
    }
    assert_eq!(fixture.unproductive_requests(), Some(2));

    fixture.answer_with_one_header(7);
    assert_eq!(
        fixture.unproductive_requests(),
        Some(0),
        "one usable header clears the peer's accumulated strikes"
    );
    let _ = fixture.drain_actions();

    for _ in 0..2 {
        fixture.time_out_one_request(7);
    }

    assert!(
        fixture.is_admitted() && !fixture.cancel.is_cancelled(),
        "strikes that straddle a productive response must not accumulate across it"
    );
    assert_eq!(fixture.unproductive_requests(), Some(2));
}

#[test]
fn a_peer_at_our_tip_is_not_charged_a_strike() {
    let mut fixture = eviction_fixture(3, 7);

    for _ in 0..4 {
        fixture.answer_already_at_our_tip(7);
        assert_eq!(
            fixture.unproductive_requests(),
            Some(0),
            "answering that we already hold the selected tip is a correct answer"
        );
    }

    assert!(
        fixture.is_admitted() && !fixture.cancel.is_cancelled(),
        "an honest peer at our tip is never evicted, however often it answers"
    );
    // Without this, the test would also pass if every reply were rejected as malformed
    // before reaching the already-known branch — which charges no strike either.
    let actions = fixture.drain_actions();
    assert!(
        actions.is_empty(),
        "the already-known reply is accepted, so it emits neither a violation nor a drop: \
         {actions:?}"
    );
}

#[test]
fn a_strike_from_a_superseded_session_cannot_drop_its_replacement() {
    // One strike causes eviction.
    // A mis-scoped charge would therefore evict the replacement immediately.
    let mut fixture = eviction_fixture(1, 7);
    let replacement = CancellationToken::new();
    let (send, _replacement_outbound) = framed_channel(8);
    fixture.reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(fixture.peer.clone(), 8, send, replacement.clone()),
    ));
    assert!(
        fixture.cancel.is_cancelled(),
        "the replacement closes the session it supersedes"
    );
    let _ = fixture.drain_actions();

    // A request still owned by session 7 times out after session 8 took over.
    fixture.time_out_one_request(7);

    assert!(
        fixture.is_admitted() && !replacement.is_cancelled(),
        "a strike raised for a superseded session cannot close its replacement"
    );
    assert_eq!(
        fixture.unproductive_requests(),
        Some(0),
        "nor can it be charged to the replacement's count"
    );
    assert!(
        !fixture
            .drain_actions()
            .iter()
            .any(|action| matches!(action, HeaderPortOperation::DropPeer { .. })),
        "a stale strike emits no drop"
    );
}

#[test]
fn dropping_an_unresponsive_peer_does_not_report_misbehavior() {
    let mut fixture = eviction_fixture(1, 7);

    fixture.time_out_one_request(7);

    assert!(!fixture.is_admitted());
    let actions = fixture.drain_actions();
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, HeaderPortOperation::DropPeer { .. })),
        "the control: this fixture really did drop the peer"
    );
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, HeaderPortOperation::Misbehavior { .. })),
        "not answering is not a protocol violation, and must never feed a ban score: {actions:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_peer_is_refused_readmission_until_its_cooldown_expires() {
    let mut fixture = eviction_fixture(1, 7);
    let cooldown = fixture.reactor.startup.config.unproductive_peer_cooldown;
    assert!(
        !cooldown.is_zero(),
        "the default configuration must define a cooldown for this test to mean anything"
    );

    fixture.time_out_one_request(7);
    assert!(!fixture.is_admitted());
    let node_id = node_id_from_peer(&fixture.peer).expect("the test peer has a node identity");
    assert_eq!(
        fixture.reactor.candidates.borrow().backed_off_node_ids,
        vec![node_id],
        "the transport is told not to reopen a peer during its cooldown"
    );

    let refused = CancellationToken::new();
    let (send, _refused_outbound) = framed_channel(8);
    fixture.reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(fixture.peer.clone(), 8, send, refused.clone()),
    ));
    assert!(
        !fixture.is_admitted() && refused.is_cancelled(),
        "discovery redials a dropped peer at once, because our drop follows a dial that \
         succeeded; header sync must refuse it without spending another request timeout"
    );

    time::advance(cooldown + std::time::Duration::from_secs(1)).await;
    fixture.reactor.refresh_statuses();
    assert!(
        fixture
            .reactor
            .candidates
            .borrow()
            .backed_off_node_ids
            .is_empty(),
        "maintenance republishes cooldown expiry"
    );
    let readmitted = CancellationToken::new();
    let (send, _readmitted_outbound) = framed_channel(8);
    fixture.reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(fixture.peer.clone(), 9, send, readmitted.clone()),
    ));

    assert!(
        fixture.is_admitted() && !readmitted.is_cancelled(),
        "the cooldown expires; it is not a permanent ban"
    );
    assert_eq!(
        fixture.unproductive_requests(),
        Some(0),
        "a readmitted peer starts from a clean count"
    );
}
