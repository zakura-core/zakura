mod admission;
mod aux_fallback;
mod closing;
mod completion;
mod eviction;
mod peer_faults;
mod port_panics;
mod status_offers;
mod terminal_trace;
mod timeouts;
mod vct_repair;

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use zakura_chain::{block::genesis::regtest_genesis_block, parameters::Network};

use super::*;
use crate::zakura::{
    framed_channel,
    wire_codec::{decode_frame, encode_frame},
    Frame, LOCAL_MAX_MESSAGE_BYTES,
};

fn peer() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![0x71; 32]).expect("the test peer ID has the required length")
}

#[test]
fn repeated_snapshot_refresh_traces_are_sampled() {
    let now = Instant::now();

    assert!(snapshot_refresh_trace_due(None, now));
    assert!(!snapshot_refresh_trace_due(
        Some(now),
        now + std::time::Duration::from_secs(9)
    ));
    assert!(snapshot_refresh_trace_due(
        Some(now),
        now + std::time::Duration::from_secs(10)
    ));
}

fn stale_failure(
    owner: zakura_header_chain::HeaderSyncWorkOwner,
) -> Arc<zakura_header_chain::HeaderChainError> {
    Arc::new(zakura_header_chain::HeaderChainError::stale_target(
        zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
    ))
}

fn local_failure(
    owner: zakura_header_chain::HeaderSyncWorkOwner,
) -> Arc<zakura_header_chain::HeaderChainError> {
    Arc::new(zakura_header_chain::HeaderChainError::local_resource(
        zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
        None,
    ))
}

fn invalid_header_failure(
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
) -> Arc<zakura_header_chain::HeaderChainError> {
    Arc::new(zakura_header_chain::HeaderChainError::invalid_header(
        zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(
            owner.header_authority().branch.target_tip_hash,
        )),
        zakura_header_chain::RuleId::new("LC-VAL-02"),
        zakura_header_chain::EvidenceId::from_digest([0x71; 32]),
        source,
        None,
    ))
}

/// What `peer`'s subscriber admits for `page`: through the session's
/// subscriber when it holds the page's subscription, as the session reader
/// does, or as admitted when a test seeded the work without one.
fn admit_page(reactor: &HeaderSyncReactor, peer: &ZakuraPeerId, page: &Headers) -> v9::Admitted {
    if let Some(state) = reactor.peer_state.get(peer) {
        let mut subscriber = state.session.subscriber();
        if subscriber.live() == Some(page.subscription_id) {
            let frame = encode_frame(&v9::RegtestMessage::Headers(page.clone()))
                .expect("the test page encodes");
            return subscriber
                .receive_page(page, frame.payload.len())
                .expect("the test page is conformant");
        }
    }
    let count = u32::try_from(page.entries.len()).expect("a page fits u32");
    let last = page.entries.last().expect("a page has a header");
    v9::Admitted {
        id: page.subscription_id,
        parent: zakura_header_chain::Frontier::new(
            page.common_ancestor_height,
            page.common_ancestor_hash,
        ),
        cursor: v9::Cursor {
            height: block::Height(page.common_ancestor_height.0 + count),
            hash: last.header.hash(),
            headers: u64::from(count),
            bytes: 0,
        },
        reaches_target: page.reaches_initial_target,
        after_close: false,
    }
}

impl HeaderSyncReactor {
    /// Deliver `page` from `peer`'s session `session_id` once its subscriber
    /// admitted it. See [`admit_page`].
    fn deliver_page(&mut self, peer: ZakuraPeerId, session_id: u64, page: Headers) {
        let admitted = admit_page(self, &peer, &page);
        self.handle_page(peer, session_id, page, admitted);
    }

    /// Deliver `outcome` from `peer`'s session `session_id` as its subscriber
    /// admits it, or as admitted when a test seeded the work without one.
    fn deliver_outcome(&mut self, peer: ZakuraPeerId, session_id: u64, outcome: HeadersOutcome) {
        let live = self.peer_state.get(&peer).and_then(|state| {
            let mut subscriber = state.session.subscriber();
            (subscriber.live() == Some(outcome.subscription_id)).then(|| {
                subscriber
                    .receive_outcome(&outcome)
                    .expect("the test outcome is conformant")
            })
        });
        let ended = live.unwrap_or(v9::Ended {
            id: outcome.subscription_id,
            outcome: outcome.outcome,
            pages: 0,
            reached: false,
            accepted: v9::Cursor {
                height: block::Height(0),
                hash: block::Hash([0; 32]),
                headers: 0,
                bytes: 0,
            },
        });
        self.handle_outcome(peer, session_id, ended);
    }
}

/// The subscription update in `frame`.
fn update(frame: Frame) -> v9::SubscribeHeaders {
    match decode_frame::<v9::RegtestMessage>(&frame).expect("the update decodes") {
        v9::Message::SubscribeHeaders(update) => update,
        other => panic!("expected a subscription update, got {other:?}"),
    }
}

/// The `Status` in `frame`.
fn decode_status(frame: Frame) -> Status {
    match decode_frame::<v9::RegtestMessage>(&frame).expect("the status decodes") {
        v9::Message::Status(status) => status,
        other => panic!("expected a status, got {other:?}"),
    }
}

/// The `Page` event for `page`, admitted by `session`'s subscriber as its
/// session reader does.
fn page_event(session: &PeerSession, page: Headers) -> Event {
    let frame =
        encode_frame(&v9::RegtestMessage::Headers(page.clone())).expect("the test page encodes");
    let admitted = session
        .subscriber()
        .receive_page(&page, frame.payload.len())
        .expect("the test page is conformant");
    Event::Page {
        peer: session.peer_id().clone(),
        session_id: session.session_id(),
        page,
        admitted,
    }
}

/// The `Outcome` event for `outcome`, admitted by `session`'s subscriber as
/// its session reader does.
fn outcome_event(session: &PeerSession, outcome: HeadersOutcome) -> Event {
    let ended = session
        .subscriber()
        .receive_outcome(&outcome)
        .expect("the test outcome is conformant");
    Event::Outcome {
        peer: session.peer_id().clone(),
        session_id: session.session_id(),
        ended,
    }
}

fn startup(shutdown: CancellationToken) -> HeaderSyncStartup {
    let network = Network::new_regtest(Default::default());
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.shutdown = shutdown;
    startup
}

fn committed_snapshot(
    anchor: zakura_header_chain::Frontier,
) -> zakura_header_chain::EngineSnapshot {
    zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(3),
        header_generation: zakura_header_chain::HeaderGeneration::new(4),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(5),
        frontiers: zakura_header_chain::FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            anchor.hash,
        ),
        oldest_retained_height: anchor.height,
        alarms: Default::default(),
    }
}

fn seed_applying_request(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: ZakuraPeerId,
    session_id: u64,
) -> (
    zakura_header_chain::SourceId,
    zakura_header_chain::HeaderSyncWorkOwner,
    zakura_header_chain::BranchId,
) {
    let source = source_id_from_peer(&peer);
    let anchor = snapshot.frontiers.finalized;
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = anchor.hash;
    header.time += chrono::Duration::seconds(1);
    let header = Arc::new(header);
    let target = zakura_header_chain::Frontier::new(
        anchor
            .height
            .next()
            .expect("the genesis fixture has a next height"),
        header.hash(),
    );
    let request_id = HeaderSyncRequestId::new(9).expect("nine is nonzero");
    let owner: zakura_header_chain::HeaderSyncWorkOwner = zakura_header_chain::HeaderWorkOwner {
        authority: zakura_header_chain::HeaderWorkAuthority::for_target(snapshot, target.hash),
        session_id,
        request_id: NonZeroU64::new(request_id.get()).expect("the request ID is nonzero"),
    }
    .into();
    let advertised = AdvertisedHeaderTarget {
        scope: zakura_header_chain::HeaderWorkAuthority::for_target(snapshot, target.hash),
        session_id: owner.session_id(),
        status: Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: target.height,
            selected_tip_hash: target.hash,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_subscriptions: 1,
            max_message_bytes: 1_000,
            tree_aux_schema_mask: 0,
        },
    };
    assert_eq!(
        reactor
            .peer_work_queue
            .stage(peer.clone(), advertised.clone(), PeerWorkPriority::Normal,),
        QueueWorkResult::NeedsLocator
    );
    assert!(reactor.peer_work_queue.reserve_request(&peer, 1));
    assert!(reactor.peer_work_queue.start(ActiveHeaderRequest {
        purpose: HeaderTargetPurpose::Normal,
        peer: peer.clone(),
        source,
        target: advertised,
        sent_locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
        request_id,
        owner,
        common_ancestor: Some(anchor),
        entries: vec![HeaderEntry {
            header,
            body_size: 0,
            tree_aux: None,
        }],
        phase: HeaderTargetPhase::Applying,
        cursor: None,
        max_header_count: 1,
        tree_aux_schema: AuxSchema::None,
    }));
    reactor.peer_work_queue.set_capacity_for_test(&peer, 1, 1);
    (source, owner, owner.header_authority().branch)
}

fn peer_violation_fixture() -> (
    HeaderSyncReactor,
    mpsc::Receiver<HeaderPortOperation>,
    zakura_header_chain::EngineSnapshot,
    ZakuraPeerId,
    zakura_header_chain::SourceId,
    zakura_header_chain::HeaderSyncWorkOwner,
) {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown);
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the violation fixture starts");
    let peer = peer();
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 0);
    (reactor, actions, snapshot, peer, source, owner)
}

fn assert_peer_violation(
    actions: &mut mpsc::Receiver<HeaderPortOperation>,
    expected: HeaderSyncMisbehavior,
) {
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::Misbehavior { reason, .. }) if reason == expected
    ));
    assert!(
        actions.try_recv().is_err(),
        "one invalid response emits exactly one peer violation"
    );
}

async fn next_action(actions: &mut mpsc::Receiver<HeaderPortOperation>) -> HeaderPortOperation {
    time::timeout(std::time::Duration::from_secs(1), actions.recv())
        .await
        .expect("the reactor emits the expected action promptly")
        .expect("the reactor action channel stays open")
}
