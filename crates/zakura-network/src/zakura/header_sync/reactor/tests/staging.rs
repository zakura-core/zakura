//! Staged work (M6a-5a): each page's `Staged` with the digest of the
//! entries the incarnation then holds, the settled snapshot's held digest,
//! and each outcome's `Outcome` with what the reactor did with it.
//!
//! The simulator's fold compares these digests with what the frames
//! delivered. These tests show the reactor's half: its digests cover the
//! entries it actually holds, so a second writer that changes them,
//! reporting nothing, shows in the next snapshot.

use super::{
    aux_fallback::{answer_locator, pruned_status},
    identities::{connect, genesis, headers, observed, settle, Recorder},
    timeouts::seed_vct_active_request,
    *,
};
use crate::zakura::CloseCause;
use observe::{FrameTag, HeldDigest, OutcomeDisposition};

/// A page of `entries` that extends the committed tip on `open`'s
/// subscription.
fn page(
    snapshot: &zakura_header_chain::EngineSnapshot,
    open: &v9::SubscribeHeaders,
    target: block::Hash,
    entries: Vec<HeaderEntry>,
) -> Headers {
    Headers {
        subscription_id: open.subscription_id,
        initial_target_tip_hash: target,
        common_ancestor_height: snapshot.frontiers.header_best.height,
        common_ancestor_hash: snapshot.frontiers.header_best.hash,
        reaches_initial_target: false,
        tree_aux_schema: AuxSchema::None,
        entries,
    }
}

/// The tag a session reader gives `page` read as frame `seq` on QUIC
/// stream 4: its decoded entries' digest, taken before anything else sees
/// them.
fn tag(page: &Headers, seq: u64) -> FrameTag {
    FrameTag {
        quic_stream: 4,
        seq,
        payload_len: 1_000,
        received: HeldDigest::of(&page.entries),
    }
}

/// Delivers `page` as frame `seq`, as the reactor's loop does.
fn deliver_tagged(reactor: &mut HeaderSyncReactor, peer: &ZakuraPeerId, page: Headers, seq: u64) {
    let admitted = admit_page(reactor, peer, &page);
    let frame = tag(&page, seq);
    reactor.handle_page(peer.clone(), 0, page, admitted, Some(frame));
    reactor.schedule_cached_statuses();
}

/// The `Staged` events in `events`.
fn staged(events: &[ReactorEvent]) -> Vec<(u64, Option<FrameTag>, usize, HeldDigest)> {
    events
        .iter()
        .filter_map(|event| match event {
            ReactorEvent::Staged {
                incarnation,
                frame,
                count,
                after,
            } => Some((*incarnation, *frame, *count, *after)),
            _ => None,
        })
        .collect()
}

/// The held digest of `snapshot`'s one active work.
fn held(snapshot: &observe::ReactorSnapshot) -> (usize, HeldDigest) {
    match &snapshot.work[..] {
        [observe::WorkEntry {
            phase: observe::WorkPhase::Active {
                held, held_digest, ..
            },
            ..
        }] => (*held, *held_digest),
        other => panic!("one active work: {other:?}"),
    }
}

/// A one-entry sequence's digest changes with each field of its entry: the
/// header, its body-size hint and its auxiliary record. Digests compose, so
/// a sequence's digest is its prefix's extended by the rest's, and the
/// empty sequence changes nothing.
///
/// This fails if the digest hashes header hashes only.
#[test]
fn the_held_digest_covers_every_entry_field() {
    let entries = headers(genesis().frontiers.header_best, 3);
    let base = HeldDigest::of(&entries);
    assert_eq!(base.len, 3);

    let mut header = entries.clone();
    let mut changed = *header[1].header;
    changed.time += chrono::Duration::seconds(30);
    header[1].header = Arc::new(changed);
    let mut body_size = entries.clone();
    body_size[1].body_size += 1;
    let mut tree_aux = entries.clone();
    tree_aux[1].tree_aux = Some(TreeAuxRecordV1 {
        height: block::Height(2),
        sapling_root: Default::default(),
        orchard_root: Default::default(),
        ironwood_root: Default::default(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
    });
    let mut other_aux = tree_aux.clone();
    other_aux[1]
        .tree_aux
        .as_mut()
        .expect("just set")
        .sapling_tx_count = 1;
    let digests = [
        base,
        HeldDigest::of(&header),
        HeldDigest::of(&body_size),
        HeldDigest::of(&tree_aux),
        HeldDigest::of(&other_aux),
    ];
    for (at, digest) in digests.iter().enumerate() {
        assert_eq!(digest.len, 3);
        for other in &digests[at + 1..] {
            assert_ne!(digest, other, "each field changes the digest");
        }
    }

    for split in 0..=3 {
        let (prefix, rest) = entries.split_at(split);
        assert_eq!(
            HeldDigest::of(prefix).then(HeldDigest::of(rest)),
            base,
            "split at {split}"
        );
    }
    assert_eq!(HeldDigest::default().then(base), base);
    assert_eq!(base.then(HeldDigest::default()), base);
    // Order matters.
    let mut swapped = entries.clone();
    swapped.swap(0, 2);
    assert_ne!(HeldDigest::of(&swapped), base);
}

/// Each staged page reports its frame and the digest of every entry its
/// incarnation then holds, which is the digest the frames delivered,
/// composed, and the settled snapshot holds that digest. A second writer
/// that appends an entry, or replaces one entry's header, body-size hint or
/// auxiliary record, reporting nothing, changes the snapshot's held digest,
/// so the simulator's own expected digest no longer matches it.
///
/// This fails if the snapshot's digest comes from the reported pages
/// rather than the entries held.
#[test]
fn a_second_writer_changes_the_held_digest() {
    let snapshot = genesis();
    let recorder = Arc::new(Recorder::default());
    let (mut reactor, mut actions) = observed(&snapshot, &recorder, false);
    let (peer, _send, mut outbound) = connect(&mut reactor, 0x71);
    let target = block::Hash([0x53; 32]);
    let mut status = pruned_status(&snapshot, target);
    status.work_anchor_height = snapshot.frontiers.finalized.height;
    status.selected_tip_height = block::Height(100);
    status.max_headers_per_response = 2;
    reactor.handle_status(peer.clone(), 0, status);
    reactor.schedule_cached_statuses();
    let open = answer_locator(&mut reactor, &mut actions, &peer, &mut outbound, target);
    recorder.take();

    let entries = headers(snapshot.frontiers.header_best, 4);
    let first = page(&snapshot, &open, target, entries[..2].to_vec());
    let mut second = page(&snapshot, &open, target, entries[2..].to_vec());
    second.common_ancestor_height = block::Height(2);
    second.common_ancestor_hash = entries[1].header.hash();
    let (tag0, tag1) = (tag(&first, 0), tag(&second, 1));
    let expected = tag0.received.then(tag1.received);
    deliver_tagged(&mut reactor, &peer, first, 0);
    deliver_tagged(&mut reactor, &peer, second, 1);
    let events = recorder.take();
    assert_eq!(
        staged(&events),
        [
            (1, Some(tag0), 2, tag0.received),
            (1, Some(tag1), 2, expected),
        ],
        "each page names its frame, and its digest extends the last: {events:?}"
    );
    assert_eq!(expected, HeldDigest::of(&entries));
    assert_eq!(held(&settle(&reactor, &recorder)), (4, expected));

    // A second writer appends an entry and reports nothing.
    let extra = headers(
        zakura_header_chain::Frontier::new(block::Height(4), entries[3].header.hash()),
        1,
    );
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the round is active");
    active.entries.extend(extra.iter().cloned());
    let (count, appended) = held(&settle(&reactor, &recorder));
    assert_eq!((count, appended.len), (5, 5));
    assert_ne!(appended, expected);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the round is active")
        .entries
        .pop();
    assert_eq!(held(&settle(&reactor, &recorder)), (4, expected));

    // Each equal-count substitution changes the digest too.
    type Substitute = fn(&mut HeaderEntry);
    let substitutes: [(&str, Substitute); 3] = [
        ("header", |entry| {
            let mut header = *entry.header;
            header.time += chrono::Duration::seconds(30);
            entry.header = Arc::new(header);
        }),
        ("body size", |entry| entry.body_size += 1),
        ("tree aux", |entry| {
            entry.tree_aux = Some(TreeAuxRecordV1 {
                height: block::Height(2),
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx_count: 0,
                orchard_tx_count: 0,
                ironwood_tx_count: 0,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
            })
        }),
    ];
    for (field, substitute) in substitutes {
        let active = reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the round is active");
        let kept = active.entries[1].clone();
        substitute(&mut active.entries[1]);
        let (count, substituted) = held(&settle(&reactor, &recorder));
        assert_eq!(count, 4, "{field}");
        assert_ne!(substituted, expected, "{field}");
        reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the round is active")
            .entries[1] = kept;
    }
}

/// The `Outcome` events in `events`.
fn outcomes(events: Vec<ReactorEvent>) -> Vec<ReactorEvent> {
    events
        .into_iter()
        .filter(|event| matches!(event, ReactorEvent::Outcome { .. }))
        .collect()
}

/// A reactor with `peer` connected on session 7, observed by `recorder`.
fn outcome_reactor(
    recorder: &Arc<Recorder>,
    peer: &ZakuraPeerId,
) -> (HeaderSyncReactor, zakura_header_chain::EngineSnapshot) {
    let snapshot = genesis();
    let (mut reactor, _actions) = observed(&snapshot, recorder, true);
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        CloseCause::new(),
    ));
    (reactor, snapshot)
}

/// Each exit of the outcome handler reports what it did: no active work,
/// another request's work, a matching request that already holds its
/// pages, an ordinary request it retires, and a repair it retries. Only
/// the last two name a change to their incarnation's work.
///
/// This fails if a request past receiving is reported as retired.
#[test]
fn outcomes_report_their_disposition() {
    let peer = ZakuraPeerId::new(vec![0x71; 32]).expect("the test peer ID has the required length");
    let outcome = |subscription_id| HeadersOutcome {
        subscription_id,
        initial_target_tip_hash: block::Hash([0x53; 32]),
        outcome: HeadersOutcomeCode::TargetNotSelected,
    };
    let expect = |subscription, disposition| ReactorEvent::Outcome {
        session_id: 7,
        subscription,
        frame: None,
        disposition,
    };
    let cases: [(
        &str,
        Option<HeaderTargetPhase>,
        bool,
        u64,
        OutcomeDisposition,
    ); 5] = [
        ("no work", None, false, 9, OutcomeDisposition::NoActive),
        (
            "another request",
            Some(HeaderTargetPhase::Applying),
            false,
            10,
            OutcomeDisposition::OtherRequest,
        ),
        (
            "past receiving",
            Some(HeaderTargetPhase::Applying),
            false,
            9,
            OutcomeDisposition::NotReceiving { incarnation: 1 },
        ),
        (
            "receiving",
            Some(HeaderTargetPhase::Receiving),
            false,
            9,
            OutcomeDisposition::Retired { incarnation: 1 },
        ),
        (
            "receiving repair",
            Some(HeaderTargetPhase::Receiving),
            true,
            9,
            OutcomeDisposition::Retried { incarnation: 1 },
        ),
    ];
    for (case, phase, repair, subscription, disposition) in cases {
        let recorder = Arc::new(Recorder::default());
        let (mut reactor, snapshot) = outcome_reactor(&recorder, &peer);
        match (phase, repair) {
            (None, _) => {}
            (Some(phase), false) => {
                seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
                reactor
                    .peer_work_queue
                    .active_mut(&peer)
                    .expect("just seeded")
                    .phase = phase;
            }
            (Some(phase), true) => {
                seed_vct_active_request(&mut reactor, &snapshot, peer.clone(), 7, phase);
            }
        }
        recorder.take();
        reactor.deliver_outcome(peer.clone(), 7, outcome(subscription));
        assert_eq!(
            outcomes(recorder.take()),
            [expect(subscription, disposition)],
            "{case}"
        );
        let active = reactor.peer_work_queue.active(&peer).is_some();
        assert_eq!(
            active,
            !matches!(
                disposition,
                OutcomeDisposition::NoActive
                    | OutcomeDisposition::Retired { .. }
                    | OutcomeDisposition::Retried { .. }
            ),
            "{case}: only a retirement or a retry ends the work"
        );
    }
}
