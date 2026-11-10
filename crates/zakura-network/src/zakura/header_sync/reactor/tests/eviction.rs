use super::*;
use crate::zakura::{
    header_sync::HeaderSyncService, transport::FramedRecv, Service, ServicePeerDirection,
    SessionDemand, ZAKURA_CAP_HEADER_SYNC,
};
use tokio::time;

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
    handle: HeaderSyncHandle,
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
        reactor.sessions.contains_key(&peer),
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
        handle,
    }
}

impl EvictionFixture {
    /// Strike count charged against the currently admitted session, if any.
    fn unproductive_requests(&self) -> Option<u32> {
        self.reactor
            .state
            .peers
            .get(&self.peer)
            .map(|schedule| schedule.unproductive_requests)
    }

    fn is_admitted(&self) -> bool {
        self.reactor.sessions.contains_key(&self.peer)
    }

    /// Publish one request owned by `session_id` and let its deadline expire unanswered.
    fn time_out_one_request(&mut self, session_id: u64) {
        self.start_receiving(session_id);
        let deadline = Instant::now();
        self.reactor
            .state
            .request_deadlines
            .insert(self.peer.clone(), deadline);
        self.reactor.retire_timed_out_requests(deadline);
    }

    /// Stage one request in the receiving phase.
    fn start_receiving(&mut self, session_id: u64) {
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
    }

    /// Answer the active request with one usable header, completing its target.
    fn answer_with_one_header(&mut self, session_id: u64) {
        let anchor = self.snapshot.frontiers.finalized;
        self.start_receiving(session_id);
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
            subscription_id: active.request_id.get(),
            initial_target_tip_hash: active.target.status.selected_tip_hash,
            common_ancestor_height: anchor.height,
            common_ancestor_hash: anchor.hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![entry],
        };
        self.reactor
            .peer_work_queue
            .set_capacity_for_test(&self.peer, 0, 1);
        self.reactor
            .deliver_page(self.peer.clone(), session_id, response);
        // The port owns the completed target.
        // Clear the request so the next request can enter staging.
        self.reactor.clear_peer_work_for_test(&self.peer);
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

/// A peer whose answer our own apply is still handling isn't charged when
/// the request's deadline passes. It answered, and the apply may already
/// have finished without the reactor having handled it yet.
#[test]
fn a_timeout_during_our_own_apply_charges_no_strike() {
    let mut fixture = eviction_fixture(1, 7);
    seed_applying_request(
        &mut fixture.reactor,
        &fixture.snapshot,
        fixture.peer.clone(),
        7,
    );
    let deadline = Instant::now();
    fixture
        .reactor
        .state
        .request_deadlines
        .insert(fixture.peer.clone(), deadline);

    fixture.reactor.retire_timed_out_requests(deadline);

    assert!(
        fixture
            .reactor
            .peer_work_queue
            .active(&fixture.peer)
            .is_none(),
        "the timed-out request still retires"
    );
    assert_eq!(fixture.unproductive_requests(), Some(0));
    assert!(fixture.is_admitted() && !fixture.cancel.is_cancelled());
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
fn strikes_survive_session_replacement() {
    let mut fixture = eviction_fixture(3, 7);
    for _ in 0..2 {
        fixture.time_out_one_request(7);
    }
    assert_eq!(fixture.unproductive_requests(), Some(2));

    let replacement = CancellationToken::new();
    let (send, _replacement_outbound) = framed_channel(8);
    fixture.reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(fixture.peer.clone(), 8, send, replacement.clone()),
    ));
    assert_eq!(
        fixture.unproductive_requests(),
        Some(2),
        "replacing a session cannot reset its strikes"
    );

    fixture.time_out_one_request(8);
    assert!(
        !fixture.is_admitted() && replacement.is_cancelled(),
        "the replacement's first strike reaches the limit"
    );
}

impl EvictionFixture {
    /// Disconnect session `old` and connect `new` for the same peer.
    fn reconnect(&mut self, old: u64, new: u64) -> CancellationToken {
        self.reactor
            .handle_peer_disconnected(&self.peer.clone(), old, "test disconnect");
        let cancel = CancellationToken::new();
        let (send, outbound) = framed_channel(8);
        self._outbound = outbound;
        self.reactor.handle_event(Event::PeerConnected(
            PeerSession::from_parts_with_session_id(self.peer.clone(), new, send, cancel.clone()),
        ));
        cancel
    }
}

/// Covers a plain reconnect, and a replacement whose old session's disconnect
/// reaches the reactor before the new session's connect.
#[tokio::test(start_paused = true)]
async fn strikes_survive_a_reconnect_within_the_cooldown() {
    let mut fixture = eviction_fixture(3, 7);
    for _ in 0..2 {
        fixture.time_out_one_request(7);
    }
    time::advance(fixture.reactor.startup.config.unproductive_peer_cooldown / 2).await;
    let reconnected = fixture.reconnect(7, 8);
    assert_eq!(
        fixture.unproductive_requests(),
        Some(2),
        "reconnecting cannot reset strikes"
    );

    fixture.time_out_one_request(8);
    assert!(
        !fixture.is_admitted() && reconnected.is_cancelled(),
        "the reconnected session's first strike reaches the limit"
    );
}

#[tokio::test(start_paused = true)]
async fn strikes_expire_after_a_cooldown_away() {
    let mut fixture = eviction_fixture(3, 7);
    for _ in 0..2 {
        fixture.time_out_one_request(7);
    }
    fixture
        .reactor
        .handle_peer_disconnected(&fixture.peer.clone(), 7, "test disconnect");
    time::advance(fixture.reactor.startup.config.unproductive_peer_cooldown).await;
    fixture.reactor.refresh_statuses();
    assert!(fixture.reactor.state.departed_strikes.is_empty());

    let (send, outbound) = framed_channel(8);
    fixture._outbound = outbound;
    fixture.reactor.handle_event(Event::PeerConnected(
        PeerSession::from_parts_with_session_id(
            fixture.peer.clone(),
            8,
            send,
            CancellationToken::new(),
        ),
    ));
    assert_eq!(fixture.unproductive_requests(), Some(0));
}

impl EvictionFixture {
    /// Advertise a target from `session_id` and answer its locator read, so
    /// the reactor sends that session a header request.
    fn start_request(&mut self, session_id: u64) {
        let anchor = self.snapshot.frontiers.finalized;
        let target = block::Hash([0x52; 32]);
        self.reactor.handle_status(
            self.peer.clone(),
            session_id,
            Status {
                work_anchor_height: anchor.height,
                work_anchor_hash: anchor.hash,
                selected_tip_height: block::Height(100),
                selected_tip_hash: target,
                suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
                oldest_retained_height: anchor.height,
                max_headers_per_response: u32::MAX,
                max_subscriptions: 1,
                max_message_bytes: 2_000_000,
                tree_aux_schema_mask: 0,
            },
        );
        for action in self.drain_actions() {
            if let HeaderPortOperation::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } = action
            {
                self.reactor.handle_event(Event::HeaderLocatorReady {
                    peer,
                    session_id,
                    target_tip_hash,
                    scope,
                    locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
                });
            }
        }
        let active = self
            .reactor
            .peer_work_queue
            .active(&self.peer)
            .expect("the advertised target starts a request");
        assert_eq!(active.owner.session_id(), session_id);
        assert_eq!(active.phase, HeaderTargetPhase::Receiving);
    }

    /// Answer the request `start_request` sent `session_id` with one page of
    /// one header that extends the anchor but doesn't reach the target.
    fn answer_started_request(&mut self, session_id: u64) {
        let anchor = self.snapshot.frontiers.finalized;
        let active = self
            .reactor
            .peer_work_queue
            .active(&self.peer)
            .expect("the started request is active");
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = anchor.hash;
        header.time += chrono::Duration::seconds(1);
        let response = Headers {
            subscription_id: active.request_id.get(),
            initial_target_tip_hash: active.target.status.selected_tip_hash,
            common_ancestor_height: anchor.height,
            common_ancestor_hash: anchor.hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: Arc::new(header),
                body_size: 0,
                tree_aux: None,
            }],
        };
        self.reactor
            .deliver_page(self.peer.clone(), session_id, response);
    }

    /// Replace the admitted session with `session_id`, and return its token.
    fn replace_session(&mut self, session_id: u64) -> CancellationToken {
        let cancel = CancellationToken::new();
        let (send, outbound) = framed_channel(8);
        self._outbound = outbound;
        self.reactor.handle_event(Event::PeerConnected(
            PeerSession::from_parts_with_session_id(
                self.peer.clone(),
                session_id,
                send,
                cancel.clone(),
            ),
        ));
        cancel
    }
}

/// A replacement can't restart the clock on a request its peer never
/// answered. The replacement's first request gets only the time that was
/// left, so replacing the session one second before each timeout still gets
/// charged.
#[tokio::test(start_paused = true)]
async fn a_replacement_keeps_its_unanswered_requests_deadline() {
    let mut fixture = eviction_fixture(1, 7);
    let timeout = fixture.reactor.startup.request_timeout;
    let started = Instant::now();
    fixture.start_request(7);

    time::advance(timeout - std::time::Duration::from_secs(1)).await;
    let replacement = fixture.replace_session(8);
    fixture.start_request(8);
    assert_eq!(
        fixture.reactor.state.request_deadlines.get(&fixture.peer),
        Some(&(started + timeout)),
        "the replacement's first request keeps the unanswered deadline"
    );

    time::advance(std::time::Duration::from_secs(1)).await;
    fixture.reactor.retire_timed_out_requests(Instant::now());
    assert!(
        replacement.is_cancelled(),
        "the unanswered request is charged on its original schedule"
    );
}

/// A second replacement before the first one gets a request keeps the
/// carried time, so replacing twice per timeout doesn't reset it.
#[tokio::test(start_paused = true)]
async fn a_second_replacement_keeps_the_carried_time() {
    let mut fixture = eviction_fixture(1, 7);
    let timeout = fixture.reactor.startup.request_timeout;
    let started = Instant::now();
    fixture.start_request(7);
    time::advance(timeout - std::time::Duration::from_secs(1)).await;
    let _first = fixture.replace_session(8);
    let second = fixture.replace_session(9);

    fixture.start_request(9);
    assert_eq!(
        fixture.reactor.state.request_deadlines.get(&fixture.peer),
        Some(&(started + timeout))
    );
    time::advance(std::time::Duration::from_secs(1)).await;
    fixture.reactor.retire_timed_out_requests(Instant::now());
    assert!(second.is_cancelled());
}

/// The carried time counts from when the node sends the replacement's first
/// request, so the node's own delay before sending isn't charged to the peer.
#[tokio::test(start_paused = true)]
async fn a_delayed_first_request_still_gets_the_carried_time() {
    let mut fixture = eviction_fixture(1, 7);
    let timeout = fixture.reactor.startup.request_timeout;
    fixture.start_request(7);
    time::advance(timeout - std::time::Duration::from_secs(1)).await;
    let replacement = fixture.replace_session(8);
    // The node doesn't send the replacement a request until well after the
    // replaced request's deadline.
    time::advance(timeout).await;

    let sent = Instant::now();
    fixture.start_request(8);
    assert_eq!(
        fixture.reactor.state.request_deadlines.get(&fixture.peer),
        Some(&(sent + std::time::Duration::from_secs(1)))
    );
    fixture.reactor.retire_timed_out_requests(Instant::now());
    assert!(
        !replacement.is_cancelled(),
        "the peer isn't charged before it could answer"
    );
}

/// The carried time binds only the replacement's first deadline. A staged
/// page re-arms it in full, so a peer that answers pays nothing for the
/// replacement.
#[tokio::test(start_paused = true)]
async fn an_answered_replacement_request_gets_a_full_timeout() {
    let mut fixture = eviction_fixture(1, 7);
    let timeout = fixture.reactor.startup.request_timeout;
    fixture.start_request(7);
    time::advance(timeout - std::time::Duration::from_secs(1)).await;
    let replacement = fixture.replace_session(8);
    fixture.start_request(8);

    let answered = Instant::now();
    fixture.answer_started_request(8);
    assert_eq!(
        fixture.reactor.state.request_deadlines.get(&fixture.peer),
        Some(&(answered + timeout)),
        "the staged page re-arms a full timeout"
    );
    time::advance(std::time::Duration::from_secs(1)).await;
    fixture.reactor.retire_timed_out_requests(Instant::now());
    assert!(!replacement.is_cancelled());
}

/// A replacement that retires no unanswered request gives the replacement's
/// first request a full timeout.
#[tokio::test(start_paused = true)]
async fn only_an_unanswered_request_carries_its_deadline() {
    let mut fixture = eviction_fixture(1, 7);
    let timeout = fixture.reactor.startup.request_timeout;
    time::advance(timeout - std::time::Duration::from_secs(1)).await;
    let _replacement = fixture.replace_session(8);
    let now = Instant::now();
    fixture.start_request(8);
    assert_eq!(
        fixture.reactor.state.request_deadlines.get(&fixture.peer),
        Some(&(now + timeout)),
        "an idle session's replacement starts a fresh deadline"
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

/// A peer that redials late in its cooldown is re-offered when the cooldown
/// ends, not one fixed back-off after its redial. Maintenance wakes at the
/// cooldown's end and republishes it, which wakes the header service's demand.
#[tokio::test(start_paused = true)]
async fn a_redial_late_in_the_cooldown_reopens_when_it_ends() {
    let mut fixture = eviction_fixture(1, 7);
    let cooldown = std::time::Duration::from_secs(15);
    let second = std::time::Duration::from_secs(1);
    fixture.reactor.startup.config.unproductive_peer_cooldown = cooldown;
    let ends = Instant::now() + cooldown;
    fixture.time_out_one_request(7);
    assert!(!fixture.is_admitted());

    time::advance(cooldown - second).await;
    let service = HeaderSyncService::new(fixture.handle.clone());
    let demand = || {
        service.session_demand(
            1,
            &fixture.peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        )
    };
    let SessionDemand::WaitForChange(mut reopen) = demand() else {
        panic!("the service waits for the reactor to end the cooldown");
    };
    assert!(futures::poll!(&mut reopen).is_pending());
    assert_eq!(
        fixture.reactor.next_maintenance_deadline(),
        ends,
        "maintenance wakes when the cooldown ends"
    );

    time::advance(second).await;
    fixture.reactor.refresh_statuses();
    assert!(
        futures::poll!(&mut reopen).is_ready(),
        "republishing the cooldown's end wakes the parked demand"
    );
    assert!(matches!(demand(), SessionDemand::OpenNow));
    assert!(
        fixture.reactor.next_maintenance_deadline() > Instant::now(),
        "a pruned cooldown doesn't wake maintenance again"
    );
}

/// A cooldown that a later fault extends holds until its new end. The
/// cached end still wakes maintenance at the original deadline, which
/// refuses the peer, keeps the parked demand waiting, and moves the next
/// wake to the new end. An unrelated publication doesn't wake the demand.
#[tokio::test(start_paused = true)]
async fn an_extended_cooldown_holds_through_its_original_end() {
    let mut fixture = eviction_fixture(1, 7);
    let cooldown = std::time::Duration::from_secs(15);
    let extended_after = std::time::Duration::from_secs(5);
    fixture.reactor.startup.config.unproductive_peer_cooldown = cooldown;
    let first_end = Instant::now() + cooldown;
    let extended_end = first_end + extended_after;
    fixture.time_out_one_request(7);

    let limit = fixture.reactor.admitted_peer_limit();
    time::advance(extended_after).await;
    fixture.reactor.state.start_readmission_cooldown(
        &fixture.peer,
        Instant::now(),
        cooldown,
        limit,
    );
    let service = HeaderSyncService::new(fixture.handle.clone());
    let demand = || {
        service.session_demand(
            1,
            &fixture.peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        )
    };
    let SessionDemand::WaitForChange(mut reopen) = demand() else {
        panic!("the service waits for the reactor to end the cooldown");
    };
    assert_eq!(fixture.reactor.next_maintenance_deadline(), first_end);

    time::advance(first_end - Instant::now()).await;
    fixture.reactor.refresh_statuses();
    fixture.reactor.publish_peer_state();
    assert!(fixture
        .reactor
        .state
        .readmission_refused(&fixture.peer, Instant::now()));
    assert!(
        futures::poll!(&mut reopen).is_pending(),
        "the original end and an unrelated publication keep the demand parked"
    );
    assert_eq!(
        fixture.reactor.next_maintenance_deadline(),
        extended_end,
        "the original end's wake moves the next one to the extended end"
    );

    time::advance(extended_after).await;
    fixture.reactor.refresh_statuses();
    assert!(futures::poll!(&mut reopen).is_ready());
    assert!(matches!(demand(), SessionDemand::OpenNow));
}

/// The published backed-off set is sorted, which the header service's binary
/// search relies on. Every cooled peer is found and parked, and a peer that
/// sorts between them is not.
#[tokio::test(start_paused = true)]
async fn the_backed_off_set_is_sorted_for_the_services_search() {
    let mut fixture = eviction_fixture(1, 7);
    let cooldown = fixture.reactor.startup.config.unproductive_peer_cooldown;
    let mut peers: Vec<_> = (0x60_u8..0x90)
        .map(|byte| ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has 32 bytes"))
        .filter(|peer| node_id_from_peer(peer).is_some())
        .take(5)
        .collect();
    assert_eq!(peers.len(), 5, "enough test bytes decode as node IDs");
    let open = peers.remove(2);
    let limit = fixture.reactor.admitted_peer_limit();
    for peer in peers.iter().rev() {
        fixture
            .reactor
            .state
            .start_readmission_cooldown(peer, Instant::now(), cooldown, limit);
    }
    fixture.reactor.publish_peer_state();

    let published = fixture
        .reactor
        .candidates
        .borrow()
        .backed_off_node_ids
        .clone();
    assert_eq!(published.len(), peers.len());
    assert!(published.windows(2).all(|pair| pair[0] < pair[1]));
    let service = HeaderSyncService::new(fixture.handle.clone());
    let demand = |peer: &ZakuraPeerId| {
        service.session_demand(
            1,
            peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        )
    };
    for peer in &peers {
        assert!(matches!(demand(peer), SessionDemand::WaitForChange(_)));
    }
    assert!(matches!(demand(&open), SessionDemand::OpenNow));
}

/// Fresh identities that each fault through a freed slot end the earliest
/// cooldowns instead of growing the map past the node's admitted connections,
/// however large the configured peer limits are.
#[tokio::test(start_paused = true)]
async fn faulting_identities_past_the_admitted_connections_keep_the_cooldown_cap() {
    let mut fixture = eviction_fixture(1, 7);
    fixture.reactor.startup.config.peer_limits.max_inbound_peers = 1 << 20;
    fixture
        .reactor
        .startup
        .config
        .peer_limits
        .max_outbound_peers = 1 << 20;
    fixture.reactor.startup.max_connections = 10;
    let limit = fixture.reactor.admitted_peer_limit();
    assert_eq!(limit, 10, "the admitted connections clamp the peer limits");
    let mut peers = vec![fixture.peer.clone()];
    fixture
        .reactor
        .drop_unproductive_peer(&fixture.peer, 7, "unproductive");
    let mut outbound = Vec::new();
    for (session_id, byte) in (8_u64..).zip(0x80_u8..0x80 + 12) {
        time::advance(std::time::Duration::from_secs(1)).await;
        let peer = ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has 32 bytes");
        let (send, recv) = framed_channel(8);
        outbound.push(recv);
        fixture.reactor.handle_event(Event::PeerConnected(
            PeerSession::from_parts_with_session_id(
                peer.clone(),
                session_id,
                send,
                CancellationToken::new(),
            ),
        ));
        assert!(fixture.reactor.sessions.contains_key(&peer));
        fixture
            .reactor
            .drop_unproductive_peer(&peer, session_id, "unproductive");
        peers.push(peer);
    }

    let held = &fixture.reactor.state.unproductive_peer_cooldowns;
    assert_eq!(held.len(), limit, "the admitted connections cap the map");
    assert!(
        peers[peers.len() - limit..]
            .iter()
            .all(|peer| held.contains_key(peer)),
        "the latest cooldowns stay"
    );
}

/// The per-identity cap is the smaller of the peer limits and the admitted
/// connections, and never below the floor.
#[tokio::test]
async fn the_peer_identity_cap_follows_the_smaller_limit_with_a_floor() {
    let mut fixture = eviction_fixture(1, 7);
    let cases = [
        // (inbound, outbound, admitted connections, cap)
        (256, 256, 92, 92),
        (1 << 20, 1 << 20, 10, 10),
        (6, 6, 92, 12),
        (1, 1, 92, MIN_PEER_IDENTITY_STATE),
        (256, 256, 2, MIN_PEER_IDENTITY_STATE),
    ];
    for (inbound, outbound, connections, cap) in cases {
        let startup = &mut fixture.reactor.startup;
        startup.config.peer_limits.max_inbound_peers = inbound;
        startup.config.peer_limits.max_outbound_peers = outbound;
        startup.max_connections = connections;
        assert_eq!(
            fixture.reactor.admitted_peer_limit(),
            cap,
            "{inbound} inbound, {outbound} outbound, {connections} connections"
        );
    }
}

/// Header serving and the lifecycle queue are sized for the header-sync peers
/// the node can admit: the smaller of the peer limits and the admitted
/// connections.
#[test]
fn serving_and_lifecycle_follow_the_admitted_header_peers() {
    let cases = [
        // (inbound, outbound, admitted connections, header peers)
        (256, 256, 16, 16),
        (256, 256, 200, 200),
        (256, 256, 512, 512),
        (6, 6, 92, 12),
    ];
    for (inbound, outbound, connections, peers) in cases {
        let mut startup = startup(CancellationToken::new());
        startup.config.peer_limits.max_inbound_peers = inbound;
        startup.config.peer_limits.max_outbound_peers = outbound;
        startup.max_connections = connections;
        let (handle, _actions, _reactor) =
            build_header_sync_reactor(startup).expect("the sizing fixture builds");
        let case = format!("{inbound} inbound, {outbound} outbound, {connections} connections");
        assert_eq!(
            handle.serving.limits(),
            v9::serving_limits(v9::SERVE_PEER_OUTPUT_BYTES, peers, None),
            "{case}"
        );
        assert_eq!(
            handle.lifecycle.capacity(),
            super::super::events::LIFECYCLE_SLOTS_PER_PEER * peers,
            "{case}"
        );
    }
}

/// The test node builder sizes header sync from its own connection limit, as
/// the production handler does.
#[tokio::test]
async fn the_test_node_sizes_header_sync_from_its_connection_limit() {
    use crate::zakura::{testkit::ZakuraTestNode, ZakuraLocalLimits};
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut limits = ZakuraLocalLimits::from_config(&crate::Config::default());
    limits.max_connections = 16;
    let node = ZakuraTestNode::builder(86500)
        .limits(limits)
        .supported_capabilities(ZAKURA_CAP_HEADER_SYNC)
        .header_sync_driver(
            network,
            anchor,
            FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
        )
        .spawn()
        .await
        .expect("the test node starts");
    let handle = node.header_sync().expect("the node runs header sync");
    let config = ZakuraHeaderSyncConfig::default().for_node_connections(16);

    assert_eq!(
        handle.serving.limits(),
        v9::serving_limits(
            v9::SERVE_PEER_OUTPUT_BYTES,
            16,
            config.node_serve_output_bytes
        )
    );
    assert_eq!(
        handle.lifecycle.capacity(),
        super::super::events::LIFECYCLE_SLOTS_PER_PEER * 16
    );
}

/// Past the cap the cooldown that ends first is evicted, never the one just
/// started, and refreshing a held cooldown evicts nothing.
#[tokio::test(start_paused = true)]
async fn the_cooldown_cap_evicts_the_earliest_end() {
    let mut fixture = eviction_fixture(1, 7);
    let cooldown = fixture.reactor.startup.config.unproductive_peer_cooldown;
    let max_cooldowns = 2;
    let peer = |byte: u8| ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has 32 bytes");
    let (a, b, c, d, e, f) = (
        peer(0x80),
        peer(0x81),
        peer(0x82),
        peer(0x83),
        peer(0x84),
        peer(0x85),
    );
    let state = &mut fixture.reactor.state;
    let held = |state: &ReactorState| {
        let mut held: Vec<_> = state.unproductive_peer_cooldowns.keys().cloned().collect();
        held.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        held
    };

    for peer in [&a, &b, &c] {
        state.start_readmission_cooldown(peer, Instant::now(), cooldown, max_cooldowns);
        time::advance(std::time::Duration::from_secs(1)).await;
    }
    assert_eq!(held(state), [b.clone(), c.clone()], "A ended first");

    state.start_readmission_cooldown(&b, Instant::now(), cooldown, max_cooldowns);
    assert_eq!(
        held(state),
        [b.clone(), c.clone()],
        "a refresh evicts nothing"
    );

    time::advance(std::time::Duration::from_secs(1)).await;
    state.start_readmission_cooldown(&d, Instant::now(), cooldown, max_cooldowns);
    assert_eq!(held(state), [b.clone(), d.clone()], "C now ends first");

    state.start_readmission_cooldown(&e, Instant::now(), cooldown, max_cooldowns);
    assert_eq!(held(state), [d, e], "the refreshed B now ends first");

    state.start_readmission_cooldown(&f, Instant::now(), cooldown, max_cooldowns);
    assert_eq!(state.unproductive_peer_cooldowns.len(), max_cooldowns);
    assert!(
        state.unproductive_peer_cooldowns.contains_key(&f),
        "a tie on the end keeps the cooldown just started"
    );
}

/// Fresh identities that each leave with a strike end the earliest holds
/// instead of growing the held set past its limit.
#[tokio::test(start_paused = true)]
async fn the_departed_limit_bounds_held_strikes() {
    let mut fixture = eviction_fixture(3, 7);
    let hold = fixture.reactor.startup.config.unproductive_peer_cooldown;
    let max_departed = 2;
    let peers: Vec<_> = (0_u8..4)
        .map(|index| {
            ZakuraPeerId::new(vec![0x80 + index; 32]).expect("the test peer ID has 32 bytes")
        })
        .collect();
    for peer in &peers {
        let state = &mut fixture.reactor.state;
        state.admit_session(peer, Instant::now());
        state.charge_unproductive_request(peer, 3);
        state.remove_session(peer, Instant::now(), hold, max_departed);
        time::advance(std::time::Duration::from_secs(1)).await;
    }

    let held = &fixture.reactor.state.departed_strikes;
    assert_eq!(held.len(), max_departed);
    assert!(
        peers[2..].iter().all(|peer| held.contains_key(peer)),
        "the latest holds stay"
    );
}
