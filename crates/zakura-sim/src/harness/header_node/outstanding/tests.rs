//! The outstanding-work rule's rows, fed by hand.

use std::{num::NonZeroU64, time::Duration};

use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, Frontier, HeaderGeneration, HeaderSyncWorkOwner,
    HeaderWorkAuthority, SourceId, VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{
        CallEnd, HeaderRequestTerminal, HeaderTargetPhase, PortCall, ReactorCause, ReactorEvent,
        ReactorSizes, RepairSources, RequestRef, ReservationPurpose, ServeInUse, SubscriberChange,
    },
    HeaderTargetPurpose, ServicePeerDirection, ZakuraPeerId,
};

use super::*;
use zakura_rt::sync::CancellationToken;

/// A named change to a reactor's sizes report.
type Mutation = Box<dyn Fn(&mut ReactorSizes)>;

/// Mirror `n`'s peer ID.
fn mirror(n: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![n; 32]).expect("32 bytes is a valid peer ID")
}

/// `peer`'s admission as session `session_id`.
fn admitted(peer: ZakuraPeerId, session_id: u64) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer,
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: None,
    }
}

/// Locator call `call` for `peer`.
fn locator(call: u64, peer: ZakuraPeerId) -> ReactorEvent {
    ReactorEvent::Dispatched {
        call,
        op: PortCall::Locator {
            peer,
            session_id: 1,
            target_tip_hash: block::Hash([3; 32]),
            scope: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
            },
        },
    }
}

/// The sizes of a reactor holding mirror 2's session, with `change`
/// applied.
fn sizes(change: impl FnOnce(&mut ReactorSizes)) -> ReactorEvent {
    let mut sizes = ReactorSizes {
        sessions: vec![mirror(2)],
        scheduled: vec![mirror(2)],
        ..ReactorSizes::default()
    };
    change(&mut sizes);
    ReactorEvent::Sizes { sizes }
}

/// An engine that holds only the tip `[3; 32]`.
fn held() -> HeldHeaders {
    HeldHeaders::new(|| Some(HashSet::from([block::Hash([3; 32])])))
}

/// The departed VCT repair suppliers the rule under test allows.
const DEPARTED: usize = 4;

/// The rule after mirror 2's admission, with a peer limit of 8 and a
/// departed limit of [`DEPARTED`].
fn rule() -> Outstanding {
    let mut rule = Outstanding::new(OutstandingEvidence::default(), 8, DEPARTED, held());
    rule.observe(&admitted(mirror(2), 1));
    rule
}

/// A reactor whose containers match its facts breaks nothing, and the
/// drain after its `Sizes` makes the rule applicable.
#[test]
fn matching_sizes_hold_and_apply() {
    let evidence = OutstandingEvidence::default();
    let mut rule = Outstanding::new(evidence.clone(), 8, DEPARTED, held());
    rule.observe(&admitted(mirror(2), 1));
    rule.observe(&locator(1, mirror(2)));
    rule.observe(&sizes(|sizes| {
        sizes.cached_offers = vec![mirror(2)];
        sizes.locator_reads = vec![(mirror(2), 1)];
        sizes.storage_calls = vec![1];
        sizes.port_futures = 1;
        sizes.supplier_order = vec![mirror(2)];
    }));
    assert_eq!(rule.drain(4), Vec::<String>::new());
    assert!(evidence.applicable_by(4));
    assert!(!evidence.applicable_by(3));
    assert_eq!(evidence.peaks().get("locator reads"), Some(&1));
}

/// Each membership rule breaks on a key whose peer or call no longer has
/// what its entry is for.
#[test]
fn a_stale_key_breaks_its_row() {
    let cases: [(&str, Mutation); 12] = [
        (
            "request deadlines",
            Box::new(|sizes| sizes.request_deadlines = vec![mirror(2)]),
        ),
        (
            "reservations",
            Box::new(|sizes| sizes.reservations = vec![(mirror(2), Some(1))]),
        ),
        (
            "staged capacity",
            Box::new(|sizes| sizes.staged_capacity = vec![(mirror(2), 0)]),
        ),
        (
            "repair episodes",
            Box::new(|sizes| sizes.repair_episodes = vec![round(4).owner]),
        ),
        (
            "completed targets",
            Box::new(|sizes| {
                sizes.completed_targets = vec![(
                    HeaderGeneration::new(1),
                    BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
                )]
            }),
        ),
        (
            "close deadlines",
            Box::new(|sizes| sizes.close_deadlines = vec![(mirror(9), 1)]),
        ),
        (
            "a replaced session's pending close",
            Box::new(|sizes| sizes.pending_closes = vec![(mirror(2), 2)]),
        ),
        (
            "supplier order",
            Box::new(|sizes| sizes.supplier_order = vec![mirror(2), mirror(2)]),
        ),
        (
            "locator reads",
            Box::new(|sizes| sizes.locator_reads = vec![(mirror(2), 5)]),
        ),
        (
            "storage calls",
            Box::new(|sizes| {
                sizes.storage_calls = vec![1];
                sizes.port_futures = 1;
            }),
        ),
        ("port futures", Box::new(|sizes| sizes.port_futures = 2)),
        (
            "admitted peers",
            Box::new(|sizes| sizes.sessions = vec![mirror(2), mirror(9)]),
        ),
    ];
    for (row, change) in cases {
        let mut rule = rule();
        // Call 1 dispatched and ended, so it's no longer pending.
        rule.observe(&locator(1, mirror(2)));
        rule.observe(&ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::Replied,
        });
        rule.observe(&sizes(change));
        let breaks = rule.drain(1);
        assert_eq!(breaks.len(), 1, "{row}: {breaks:?}");
        assert!(
            breaks[0].starts_with("OutstandingWork: the reactor "),
            "{row}: {breaks:?}"
        );
    }
}

/// The slot list naming mirror 2's round `incarnation` as its only slot,
/// started.
fn active_slot(incarnation: u64) -> ReactorEvent {
    ReactorEvent::Slots {
        slots: vec![zakura_network::zakura::observe::WorkSlot {
            peer: mirror(2),
            session_id: 1,
            target_tip_hash: block::Hash([3; 32]),
            phase: Some(HeaderTargetPhase::Receiving),
            incarnation,
            repair: false,
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
            },
        }],
    }
}

/// Mirror 2's round `incarnation`.
fn round(incarnation: u64) -> RequestRef {
    let authority = HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(1),
        branch: BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
    };
    RequestRef {
        peer: mirror(2),
        session_id: 1,
        request_id: 1,
        incarnation,
        owner: HeaderSyncWorkOwner::Header(authority.bind(1, NonZeroU64::MIN)),
        target_tip_hash: block::Hash([3; 32]),
    }
}

/// Mirror 2's round `incarnation` starting.
fn started(incarnation: u64) -> ReactorEvent {
    ReactorEvent::Started {
        request: round(incarnation),
        purpose: HeaderTargetPurpose::Normal,
        locator: Vec::new(),
    }
}

/// A slot's lifetime is the round-life properties', so an active slot whose
/// round was released, or whose session ended, breaks nothing here; an
/// unstarted slot needs an open locator read; a completed target holds
/// once a `TargetCompleted` names it.
#[test]
fn slots_and_completions_join_their_facts() {
    let mut rule = rule();
    rule.observe(&started(4));
    rule.observe(&active_slot(4));
    let target = (
        HeaderGeneration::new(1),
        BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
    );
    rule.observe(&ReactorEvent::TargetCompleted {
        header_generation: target.0,
        branch: target.1,
        already_known: true,
    });
    rule.observe(&sizes(|sizes| sizes.completed_targets = vec![target]));
    assert_eq!(rule.drain(1), Vec::<String>::new());

    // A released round's slot, and one whose session was replaced, are
    // reset-invalidates-work's and peer-retirement-progress's.
    rule.observe(&ReactorEvent::RoundReleased {
        peer: mirror(2),
        incarnation: 4,
        queue_owned: 0,
    });
    rule.observe(&admitted(mirror(2), 2));
    rule.observe(&sizes(|_| {}));
    assert_eq!(rule.drain(2), Vec::<String>::new());

    // An unstarted slot needs an open locator read.
    rule.observe(&unstarted_slot(2));
    rule.observe(&sizes(|_| {}));
    let breaks = rule.drain(3);
    assert!(
        matches!(&breaks[..], [only] if only.contains("no open locator read")),
        "{breaks:?}"
    );
    rule.observe(&locator(7, mirror(2)));
    rule.observe(&sizes(|sizes| {
        sizes.storage_calls = vec![7];
        sizes.port_futures = 1;
    }));
    assert_eq!(rule.drain(4), Vec::<String>::new());
}

/// Mirror 2's only slot, awaiting its locator for session `session_id`.
fn unstarted_slot(session_id: u64) -> ReactorEvent {
    let ReactorEvent::Slots { mut slots } = active_slot(0) else {
        unreachable!("active_slot reports slots")
    };
    slots[0].session_id = session_id;
    slots[0].phase = None;
    ReactorEvent::Slots { slots }
}

/// A completed target a `TargetCompleted` named still breaks the rule
/// when the engine doesn't hold its tip, as when a check took an unknown
/// target as known.
#[test]
fn a_completed_target_needs_its_tip_held() {
    let mut rule = rule();
    let unknown = (
        HeaderGeneration::new(1),
        BranchId::new(block::Hash([1; 32]), block::Hash([4; 32])),
    );
    rule.observe(&ReactorEvent::TargetCompleted {
        header_generation: unknown.0,
        branch: unknown.1,
        already_known: true,
    });
    rule.observe(&sizes(|sizes| sizes.completed_targets = vec![unknown]));
    let breaks = rule.drain(1);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("which the node's engine doesn't hold"),
        "{breaks:?}"
    );
}

/// A departed strike needs strikes and a peer with no admitted session,
/// and a maintenance unit must end with no strike or cooldown expired,
/// though other units may leave expired ones waiting.
#[test]
fn strikes_and_cooldowns_expire_by_maintenance() {
    use std::time::Duration;

    let mut rule = rule();
    let expired = |sizes: &mut ReactorSizes| {
        sizes.departed_strikes = vec![(mirror(8), 1, Duration::ZERO)];
        sizes.cooldowns = vec![(mirror(9), Duration::ZERO)];
    };
    rule.observe(&ReactorEvent::CauseSet {
        cause: ReactorCause::Event,
    });
    rule.observe(&sizes(expired));
    assert_eq!(rule.drain(1), Vec::<String>::new());

    rule.observe(&ReactorEvent::CauseSet {
        cause: ReactorCause::Maintenance,
    });
    rule.observe(&sizes(expired));
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(breaks[0].contains("expired departed strikes"), "{breaks:?}");
    assert!(breaks[1].contains("expired cooldowns"), "{breaks:?}");

    rule.observe(&sizes(|sizes| {
        sizes.departed_strikes = vec![
            (mirror(2), 1, Duration::from_secs(1)),
            (mirror(8), 0, Duration::from_secs(1)),
        ];
    }));
    let breaks = rule.drain(3);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks[0].contains("which have an admitted session"),
        "{breaks:?}"
    );
    assert!(breaks[1].contains("with no strikes"), "{breaks:?}");
}

/// An active round's leases must match the pages it staged, so a lease
/// kept twice, or one no page staged, breaks the rule, and so does a
/// reservation backing another round, while the round's own reservation
/// and deadline hold.
#[test]
fn leases_match_their_rounds_pages() {
    let mut rule = rule();
    rule.observe(&started(4));
    rule.observe(&active_slot(4));
    rule.observe(&ReactorEvent::ReservationOpened {
        reservation: 1,
        peer: mirror(2),
        purpose: ReservationPurpose::Ordinary,
        headers: 10,
        request: None,
    });
    rule.observe(&ReactorEvent::ReservationBound {
        reservation: 1,
        request: round(4),
        max_headers: 10,
    });
    rule.observe(&sizes(|sizes| {
        sizes.reservations = vec![(mirror(2), Some(1))];
        sizes.request_deadlines = vec![mirror(2)];
    }));
    assert_eq!(rule.drain(1), Vec::<String>::new());
    // The peer's next round reusing the old round's reservation breaks.
    rule.observe(&started(5));
    rule.observe(&active_slot(5));
    rule.observe(&sizes(|sizes| {
        sizes.reservations = vec![(mirror(2), Some(1))];
        sizes.request_deadlines = vec![mirror(2)];
    }));
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("holds reservation Some(1)"),
        "{breaks:?}"
    );
    rule.observe(&sizes(|sizes| {
        sizes.staged_capacity = vec![(mirror(2), 1)];
        sizes.staged_entries = vec![(mirror(2), 3)];
    }));
    let breaks = rule.drain(3);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks[0].contains("3 staged entries") && breaks[0].contains("should hold 0"),
        "{breaks:?}"
    );
    assert!(
        breaks[1].contains("holds 1 leases for") && breaks[1].contains("staged 0 pages"),
        "{breaks:?}"
    );
}

/// A row past its bound breaks it, and a `Slots` list past the queue's
/// 16 slots does too.
#[test]
fn a_row_past_its_bound_breaks() {
    let mut rule = rule();
    rule.observe(&sizes(|sizes| {
        sizes.staged_entries = vec![(mirror(2), CHUNK_HEADERS + 1)]
    }));
    let breaks = rule.drain(1);
    // The entries also have no active slot to belong to.
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks[0].contains("staged entries, past its 4000"),
        "{breaks:?}"
    );

    let peers: Vec<ZakuraPeerId> = (0..=16).map(|n| mirror(n + 10)).collect();
    let slots = peers
        .iter()
        .map(|peer| zakura_network::zakura::observe::WorkSlot {
            peer: peer.clone(),
            session_id: 1,
            target_tip_hash: block::Hash([3; 32]),
            phase: None,
            incarnation: 0,
            repair: false,
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
            },
        })
        .collect();
    rule.observe(&ReactorEvent::Slots { slots });
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("17 slots"), "{breaks:?}");
}

/// The pruned-peer map holds up to the admitted peer limit, as #2209 caps
/// it, and breaks past it.
#[test]
fn pruned_peers_past_the_peer_limit_break() {
    let mut rule = rule();
    rule.observe(&sizes(|sizes| {
        sizes.pruned_peers = (0..8).map(|n| mirror(n + 10)).collect();
    }));
    assert_eq!(rule.drain(1), Vec::<String>::new());
    rule.observe(&sizes(|sizes| {
        sizes.pruned_peers = (0..9).map(|n| mirror(n + 10)).collect();
    }));
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("9 pruned peers, past its 8"),
        "{breaks:?}"
    );
}

/// Open locator calls past the 16 permits break the rule, even when each
/// has its port future.
#[test]
fn open_locator_calls_past_the_permits_break() {
    let mut rule = rule();
    for call in 1..=17 {
        rule.observe(&locator(call, mirror(2)));
    }
    rule.observe(&sizes(|sizes| {
        sizes.storage_calls = (1..=17).collect();
        sizes.port_futures = 17;
    }));
    let breaks = rule.drain(1);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("17 open locator calls"), "{breaks:?}");
}

/// An ended session's peer leaves the admitted set, so keys for it break
/// their rows, but a later session's end for another session doesn't
/// remove the current one.
#[test]
fn only_the_current_sessions_end_removes_a_peer() {
    let mut rule = rule();
    rule.observe(&admitted(mirror(2), 2));
    rule.observe(&ReactorEvent::SessionEnded {
        peer: mirror(2),
        session_id: 1,
        reason: "replaced",
    });
    rule.observe(&sizes(|sizes| sizes.cached_offers = vec![mirror(2)]));
    assert_eq!(rule.drain(1), Vec::<String>::new());
    rule.observe(&ReactorEvent::SessionEnded {
        peer: mirror(2),
        session_id: 2,
        reason: "disconnect",
    });
    rule.observe(&sizes(|sizes| {
        sizes.sessions.clear();
        sizes.scheduled.clear();
        sizes.cached_offers = vec![mirror(2)];
    }));
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("cached offers"), "{breaks:?}");
}

/// Mirror 2's publisher pass on session 1, with `change` applied to an
/// honest one.
fn publisher_pass(
    change: impl FnOnce(&mut Vec<u64>, &mut Vec<u64>, &mut ServeInUse),
) -> ReactorEvent {
    let (mut waits, mut stalls, mut in_use) = (vec![3], vec![3], ServeInUse::default());
    in_use.node_execution = NODE_EXECUTION;
    in_use.peer_execution = PEER_EXECUTION;
    change(&mut waits, &mut stalls, &mut in_use);
    ReactorEvent::PublisherServing {
        peer: mirror(2),
        session_id: 1,
        site: PublisherSite::Pass,
        live: vec![(1, SENT_CURSOR_RING)],
        subscriptions: vec![1],
        tombstones: PUBLICATION_SLOTS,
        terminals: PUBLICATION_SLOTS,
        job: true,
        slots: PUBLICATION_SLOTS,
        waits,
        stalls,
        in_use: Some(in_use),
    }
}

/// Mirror 2's session 1 ledger's `change` of `subscription`.
fn publication(subscription: u64, change: PublicationChange) -> ReactorEvent {
    ReactorEvent::PublicationChanged {
        peer: mirror(2),
        session_id: 1,
        subscription,
        change,
    }
}

/// The serving rows (hdr-9d.2): a publisher pass at its bounds holds, a kept
/// wait no subscription stalls on breaks, and so do a ninth node execution
/// slot and a peer's output past 64 MiB. Every site judges the containers
/// against the ledger's open and end facts and the bounds, and only a pass
/// judges the waits.
#[test]
fn a_publisher_pass_holds_at_its_bounds_and_breaks_past_them() {
    let mut rule = rule();
    rule.observe(&publication(1, PublicationChange::Opened));
    rule.observe(&publisher_pass(|_, _, _| {}));
    assert_eq!(rule.drain(1), Vec::<String>::new());
    let cases: [(&str, PassChange); 3] = [
        (
            "which no subscription is",
            Box::new(|_, stalls, _| stalls.clear()),
        ),
        (
            "9 node execution slots",
            Box::new(|_, _, in_use| in_use.node_execution = NODE_EXECUTION + 1),
        ),
        (
            "output bytes, past its",
            Box::new(|_, _, in_use| in_use.peer_output = SERVE_PEER_OUTPUT_BYTES + 1),
        ),
    ];
    for (step, (needle, change)) in (2..).zip(cases) {
        rule.observe(&publisher_pass(change));
        let breaks = rule.drain(step);
        assert_eq!(breaks.len(), 1, "{needle}: {breaks:?}");
        assert!(breaks[0].contains(needle), "{needle}: {breaks:?}");
    }
    // Right after an end: a stray wait isn't judged before its pruning, but
    // a third terminal outcome or tombstone breaks.
    let at_end = |change: &dyn Fn(&mut ReactorEvent)| {
        let mut event = publisher_pass(|_, stalls, _| stalls.clear());
        if let ReactorEvent::PublisherServing { site, in_use, .. } = &mut event {
            *site = PublisherSite::EndDue;
            *in_use = None;
        }
        change(&mut event);
        event
    };
    rule.observe(&at_end(&|_| {}));
    assert_eq!(rule.drain(5), Vec::<String>::new());
    for (step, needle, change) in [
        (
            6,
            "3 publisher terminal outcomes, past its 2",
            &(|event: &mut ReactorEvent| {
                if let ReactorEvent::PublisherServing { terminals, .. } = event {
                    *terminals += 1;
                }
            }) as &dyn Fn(&mut ReactorEvent),
        ),
        (
            7,
            "3 publisher tombstones, past its 2",
            &|event: &mut ReactorEvent| {
                if let ReactorEvent::PublisherServing { tombstones, .. } = event {
                    *tombstones += 1;
                }
            },
        ),
        (
            8,
            "4097 sent-cursor ring, past its 4096",
            &|event: &mut ReactorEvent| {
                if let ReactorEvent::PublisherServing { site, live, .. } = event {
                    *site = PublisherSite::Commit;
                    live[0].1 += 1;
                }
            },
        ),
    ] {
        rule.observe(&at_end(change));
        let breaks = rule.drain(step);
        assert!(
            matches!(&breaks[..], [only] if only.contains(needle)),
            "{needle}: {breaks:?}"
        );
    }
    // An ended subscription kept in both containers breaks against the
    // ledger's own end, though the two containers agree.
    rule.observe(&publication(1, PublicationChange::Ended));
    rule.observe(&at_end(&|_| {}));
    let breaks = rule.drain(9);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks
            .iter()
            .all(|only| only.contains("but its ledger opened and hasn't ended []")),
        "{breaks:?}"
    );
}

/// A change to an honest publisher pass's waits, stalls and budgets.
type PassChange = Box<dyn Fn(&mut Vec<u64>, &mut Vec<u64>, &mut ServeInUse)>;

/// The service and publisher rows (hdr-9d.2): a cadence report raises no
/// break, since the row is listed, not checked, a second publisher for a
/// live session breaks, a pruning that keeps an expired peer's budgets
/// breaks, and the subscriber's unaccepted cursors must be its claims past
/// its acceptance.
#[test]
fn service_and_session_rows_join_their_facts() {
    let mut rule = rule();
    // The cadence row is listed, not checked.
    rule.observe(&ReactorEvent::ServiceSizes {
        cadences: vec![(mirror(9), 4)],
    });
    assert_eq!(rule.drain(1), Vec::<String>::new());

    let started = ReactorEvent::PublisherStarted {
        peer: mirror(2),
        session_id: 1,
    };
    rule.observe(&started);
    rule.observe(&started);
    let breaks = rule.drain(2);
    assert!(
        matches!(&breaks[..], [only] if only.contains("a second publisher")),
        "{breaks:?}"
    );

    rule.observe(&ReactorEvent::ServingPeers {
        expired: vec![mirror(7)],
        removed: vec![mirror(7), mirror(8)],
        removed_live: Vec::new(),
    });
    assert_eq!(rule.drain(3), Vec::<String>::new());
    // Removing a live entry in an expired one's place keeps that one, and
    // gives the live peer's next session budgets apart from its live ones.
    rule.observe(&ReactorEvent::ServingPeers {
        expired: vec![mirror(7)],
        removed: vec![mirror(8)],
        removed_live: vec![mirror(8)],
    });
    let breaks = rule.drain(3);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks
            .iter()
            .any(|only| only.contains("replaced the live budgets of")),
        "{breaks:?}"
    );
    assert!(
        breaks
            .iter()
            .any(|only| only.contains("kept the expired budgets of")),
        "{breaks:?}"
    );
    // A lookup that replaces budgets it could have reused reports that
    // peer removed and alive.
    rule.observe(&ReactorEvent::ServingPeers {
        expired: Vec::new(),
        removed: vec![mirror(8)],
        removed_live: vec![mirror(8)],
    });
    let breaks = rule.drain(3);
    assert!(
        matches!(&breaks[..], [only] if only.contains("replaced the live budgets of")),
        "{breaks:?}"
    );
    rule.observe(&ReactorEvent::ServingAcquired {
        peer: mirror(2),
        in_use: ServeInUse {
            node_execution: NODE_EXECUTION + 1,
            ..ServeInUse::default()
        },
    });
    let breaks = rule.drain(3);
    assert!(
        matches!(&breaks[..], [only] if only.contains("9 node execution slots in use")),
        "{breaks:?}"
    );

    let cursor = |height: u32| {
        Frontier::new(
            block::Height(height),
            block::Hash([u8::try_from(height).expect("the test's heights fit a byte"); 32]),
        )
    };
    let claimed = |height| ReactorEvent::CursorClaimed {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        through: cursor(height),
        headers: 0,
        bytes: 0,
        page_last: cursor(height),
        page_headers: 1,
        page_bytes: 0,
        totals: Default::default(),
    };
    let serving = |unaccepted: Vec<Frontier>| ReactorEvent::SubscriberServing {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        change: SubscriberChange::Claim,
        unaccepted: Some(unaccepted),
        live: 1,
        ended: 0,
    };
    rule.observe(&claimed(1));
    rule.observe(&claimed(2));
    rule.observe(&ReactorEvent::CursorsAccepted {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        through: cursor(1),
        headers: 0,
        bytes: 0,
        stored: (cursor(1), 0, 0),
        totals: Default::default(),
    });
    rule.observe(&serving(vec![cursor(2)]));
    assert_eq!(rule.drain(4), Vec::<String>::new());
    // A stored cursor that isn't the page's last is commit-only-authorized's
    // to judge, so the row joins by the page's own last header.
    rule.observe(&ReactorEvent::CursorClaimed {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        through: cursor(2),
        headers: 0,
        bytes: 0,
        page_last: cursor(3),
        page_headers: 1,
        page_bytes: 0,
        totals: Default::default(),
    });
    rule.observe(&serving(vec![cursor(2), cursor(3)]));
    assert_eq!(rule.drain(4), Vec::<String>::new());
    // So is a stored height that miscounts the page.
    let miscounted = Frontier::new(block::Height(2), cursor(3).hash);
    rule.observe(&serving(vec![cursor(2), miscounted]));
    assert_eq!(rule.drain(4), Vec::<String>::new());
    rule.observe(&serving(vec![cursor(1), cursor(2)]));
    let breaks = rule.drain(5);
    assert!(
        matches!(&breaks[..], [only] if only.contains("but its claims past its acceptance")),
        "{breaks:?}"
    );
    // An ended subscription's report carries no cursors, but its table
    // can't remember two ended keys.
    rule.observe(&ReactorEvent::SubscriberServing {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        change: SubscriberChange::Outcome,
        unaccepted: None,
        live: 0,
        ended: 2,
    });
    let breaks = rule.drain(6);
    assert!(
        matches!(&breaks[..], [only] if only.contains("2 ended keys, past its 1")),
        "{breaks:?}"
    );
}

/// Round 2's rows (hdr-9d.2): a subscription kept after its outcome breaks;
/// once the reactor's shutdown ran, a run at rest with a publisher still
/// running is lingering; and a slot kept past its round's release breaks
/// for a reason no other property owns, but not for an obsolete one.
#[test]
fn ended_subscriptions_shutdown_and_released_slots() {
    let mut ended = rule();
    ended.observe(&ReactorEvent::SubscriberServing {
        peer: mirror(2),
        session_id: 1,
        subscription: 1,
        change: SubscriberChange::Outcome,
        unaccepted: Some(Vec::new()),
        live: 1,
        ended: 1,
    });
    let breaks = ended.drain(1);
    assert!(
        matches!(&breaks[..], [only] if only.contains("keeps subscription 1 after its Outcome")),
        "{breaks:?}"
    );

    let evidence = OutstandingEvidence::default();
    let mut shut = Outstanding::new(evidence.clone(), 8, DEPARTED, held());
    shut.observe(&ReactorEvent::PublisherStarted {
        peer: mirror(2),
        session_id: 1,
    });
    shut.observe(&ReactorEvent::CauseSet {
        cause: ReactorCause::Shutdown,
    });
    evidence.end(&crate::RunEnd {
        outcome: crate::Outcome::Idle {
            status: crate::IdleStatus::Quiescent,
        },
        steps: 1,
        violation: None,
        drain: None,
        harness: None,
        rest: Some(zakura_scheduler::Status::Quiescent),
    });
    assert_eq!(evidence.lingering_at_rest(), [(mirror(2), 1)]);

    // A publisher whose session's token was cancelled lingers at rest,
    // whatever cancelled it, and one whose token holds doesn't.
    let evidence = OutstandingEvidence::default();
    let mut cancelled = Outstanding::new(evidence.clone(), 8, DEPARTED, held());
    for session_id in [6, 7] {
        cancelled.observe(&ReactorEvent::PublisherStarted {
            peer: mirror(3),
            session_id,
        });
    }
    let (six, seven) = (CancellationToken::new(), CancellationToken::new());
    cancelled.session_cancel(&mirror(3), 6, six.clone());
    cancelled.session_cancel(&mirror(3), 7, seven);
    six.cancel();
    evidence.end(&crate::RunEnd {
        outcome: crate::Outcome::Idle {
            status: crate::IdleStatus::Quiescent,
        },
        steps: 1,
        violation: None,
        drain: None,
        harness: None,
        rest: Some(zakura_scheduler::Status::Quiescent),
    });
    assert_eq!(evidence.lingering_at_rest(), [(mirror(3), 6)]);

    // A session the reactor ended must have its token cancelled by the end
    // of that unit's `Sizes`.
    // A session that ends before its token is known waits for it.
    let mut ended = rule();
    let token = CancellationToken::new();
    ended.observe(&ReactorEvent::SessionEnded {
        peer: mirror(3),
        session_id: 8,
        reason: "test",
    });
    ended.observe(&sizes(|_| {}));
    assert_eq!(ended.drain(1), Vec::<String>::new());
    ended.session_cancel(&mirror(3), 8, token.clone());
    ended.observe(&sizes(|_| {}));
    let breaks = ended.drain(1);
    assert!(
        matches!(&breaks[..], [only] if only.contains("but its token isn't cancelled")),
        "{breaks:?}"
    );
    // So must a session refused at the peer cap.
    let capped = CancellationToken::new();
    ended.session_cancel(&mirror(4), 10, capped);
    ended.observe(&ReactorEvent::SessionAtCapacity {
        peer: mirror(4),
        session_id: 10,
    });
    ended.observe(&sizes(|_| {}));
    let breaks = ended.drain(1);
    assert!(
        matches!(&breaks[..], [only] if only.contains("session 10, but its token isn't cancelled")),
        "{breaks:?}"
    );
    ended.session_cancel(&mirror(3), 9, token.clone());
    token.cancel();
    ended.observe(&ReactorEvent::SessionRefused {
        peer: mirror(3),
        session_id: 9,
    });
    ended.observe(&sizes(|_| {}));
    assert_eq!(ended.drain(2), Vec::<String>::new());

    for (reason, breaks) in [
        (HeaderRequestTerminal::TargetAdmitted, 1),
        (HeaderRequestTerminal::SnapshotObsolete, 0),
    ] {
        let mut released = rule();
        released.observe(&started(4));
        released.observe(&active_slot(4));
        released.observe(&ReactorEvent::RoundReleased {
            peer: mirror(2),
            incarnation: 4,
            queue_owned: 0,
        });
        released.observe(&ReactorEvent::Retired {
            request: round(4),
            phase: HeaderTargetPhase::Receiving,
            purpose: HeaderTargetPurpose::Normal,
            reason,
        });
        released.observe(&sizes(|_| {}));
        let found = released.drain(1);
        assert_eq!(found.len(), breaks, "{reason:?}: {found:?}");
    }
}

/// The retained-path rows (hdr-9d.2): 16 open leases and 16 cached paths
/// hold, a 17th of either breaks, and each one's end frees its room, so a
/// 17th after an end holds.
#[test]
fn retained_paths_hold_at_sixteen_and_break_past_it() {
    use crate::observe::{StorageBranch, StorageFact, StorageLease, StoragePoint, WorkScope};

    let lease = |lease_id| StorageLease {
        peer: "mirror-2".to_owned(),
        session_id: 1,
        lease_id,
    };
    let point = || StoragePoint {
        height: 1,
        hash: "00".to_owned(),
    };
    let leased = |lease_id| StorageFact::Leased {
        lease: lease(lease_id),
        scope: WorkScope {
            header_generation: 0,
            branch: StorageBranch {
                anchor: "00".to_owned(),
                target: "00".to_owned(),
            },
        },
        target: point(),
        common_ancestor: point(),
        next: None,
        capacity: "general".to_owned(),
        continued_from: None,
    };
    let cached = |lease_id| StorageFact::RetentionCached {
        lease: lease(lease_id),
        last: point(),
        next: None,
    };
    for (row, fact, end, needle) in [
        (
            "leases",
            &leased as &dyn Fn(u64) -> StorageFact,
            &(|lease_id| StorageFact::CapabilityEnded {
                lease: lease(lease_id),
                release: "lease-released".to_owned(),
            }) as &dyn Fn(u64) -> StorageFact,
            "holds 17 retained-path leases, past its 16",
        ),
        (
            "cached paths",
            &cached,
            &|lease_id| StorageFact::RetentionEnded {
                lease: lease(lease_id),
                why: "with-capability".to_owned(),
                into: None,
            },
            "cache holds 17 retained paths, past its 16",
        ),
    ] {
        let mut rule = rule();
        for lease_id in 0..16 {
            rule.storage(&fact(lease_id));
        }
        assert_eq!(rule.drain(1), Vec::<String>::new(), "{row}");
        rule.storage(&fact(16));
        let breaks = rule.drain(2);
        assert!(
            matches!(&breaks[..], [only] if only.contains(needle)),
            "{row}: {breaks:?}"
        );
        rule.storage(&end(16));
        rule.storage(&end(15));
        rule.storage(&fact(17));
        assert_eq!(rule.drain(3), Vec::<String>::new(), "{row}");
    }
}

/// What `outstanding-work-bounded` reads, with no tap behind it.
#[derive(Clone, Default)]
struct Probe {
    claims: super::super::claim::ClaimEvidence,
    outstanding: OutstandingEvidence,
}

impl crate::domain::header_sync::OutstandingView for Probe {
    fn claims(&self) -> &super::super::claim::ClaimEvidence {
        &self.claims
    }

    fn outstanding(&self) -> &OutstandingEvidence {
        &self.outstanding
    }
}

impl crate::property::HasView for Probe {
    type View = Probe;

    /// A copy sharing the stores.
    fn view(&self) -> Probe {
        self.clone()
    }

    /// Keeps the run's end for the end check.
    fn ended(&self, end: &crate::RunEnd) {
        self.outstanding.end(end);
    }

    /// [`crate::domain::header_sync::outstanding_end_violations`].
    fn end_violations(&self, _: &crate::ObservationLog) -> Vec<crate::property::EndViolation> {
        crate::domain::header_sync::outstanding_end_violations(&self.outstanding)
    }
}

/// `outstanding-work-bounded`'s outcome over a run with one worker, where
/// a publisher whose session's token is cancelled is still running from
/// the start, and the run's tasks are `starve`d of their worker or not.
fn lingering_outcome(starve: bool) -> crate::PropertyOutcome {
    use crate::property::{Assumptions, Registry, RegistryChecker, Source, Sources};
    let probe = Probe::default();
    let mut publisher = Outstanding::new(probe.outstanding.clone(), 8, DEPARTED, held());
    publisher.observe(&ReactorEvent::PublisherStarted {
        peer: mirror(3),
        session_id: 6,
    });
    let token = CancellationToken::new();
    publisher.session_cancel(&mirror(3), 6, token.clone());
    token.cancel();
    let registry = Registry::new(vec![
        crate::domain::header_sync::outstanding_work_bounded::<Probe>(),
    ])
    .expect("one property");
    let mut checker = RegistryChecker::new(
        registry,
        std::rc::Rc::new(probe),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let config = crate::RunConfig {
        scheduler: zakura_scheduler::SchedulerConfig {
            workers: 1,
            stack_size: 64 * 1024,
            ..zakura_scheduler::SchedulerConfig::default()
        },
        budget: 0,
        drain_bound: 1_000,
        ..crate::RunConfig::default()
    };
    let mut sim = crate::Sim::new(config, crate::Plan::from_fuzz(&[]));
    // The holder yields holding the lock, the waiter takes the only worker
    // and parks on it, and the holder is ready with no worker to run on.
    let lock = std::sync::Arc::new(zakura_rt::sync::Mutex::new(()));
    let held_lock = lock.clone();
    sim.scheduler().spawn_root("holder", async move {
        #[allow(clippy::await_holding_lock)]
        {
            let _guard = held_lock.lock().unwrap();
            zakura_rt::task::yield_now().await;
        }
    });
    if starve {
        sim.scheduler().spawn_root("waiter", async move {
            drop(lock.lock().unwrap());
        });
    }
    let report = sim.run(&mut checker, &mut crate::MemorySink::default());
    report.properties[0].outcome.clone()
}

/// Sol 1 and Terra 2: a cancelled publisher isn't judged at a worker-starved
/// step, where a ready task has no worker yet, only once the run is at
/// rest.
#[test]
fn a_worker_starved_step_does_not_judge_a_cancelled_publisher() {
    assert!(
        matches!(
            lingering_outcome(false),
            crate::PropertyOutcome::Violated { .. }
        ),
        "a run at rest judges it"
    );
    assert!(
        !matches!(
            lingering_outcome(true),
            crate::PropertyOutcome::Violated { .. }
        ),
        "{:?}",
        lingering_outcome(true)
    );
}

/// Mirror `n`'s VCT repair source, its peer's digest.
fn source(n: u8) -> SourceId {
    SourceId::from_digest(mirror(n).digest())
}

/// A VCT repair context read, call `call`, for the repair owner of
/// request `request`.
fn context(call: u64, request: u64) -> ReactorEvent {
    let authority = BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(1),
            branch: BranchId::new(block::Hash([1; 32]), block::Hash([3; 32])),
        },
        verified_generation: VerifiedGeneration::new(1),
        body_work_epoch: BodyWorkEpoch::new(0),
    };
    ReactorEvent::Dispatched {
        call,
        op: PortCall::VctContext {
            owner: authority.bind(
                u64::MAX,
                NonZeroU64::new(request).expect("tests name nonzero requests"),
            ),
            height: block::Height(2),
        },
    }
}

/// The departed repair suppliers stay within the departed limit: at it
/// they hold, and one past it breaks. Every set may hold the admitted
/// peer's source and each departed one, which is the per-set bound.
#[test]
fn repair_sources_at_and_one_past_the_departed_limit() {
    let at_limit = |sizes: &mut ReactorSizes| {
        let departed: Vec<SourceId> = (0..4).map(|n| source(n + 10)).collect();
        let every: Vec<SourceId> = std::iter::once(source(2))
            .chain(departed.iter().copied())
            .collect();
        sizes.repair_sources = RepairSources {
            tried: every.clone(),
            excluded_input: every.clone(),
            busy: every.clone(),
            target_refusals: every,
            departed,
        };
    };
    let mut held = rule();
    held.observe(&sizes(at_limit));
    assert_eq!(held.drain(1), Vec::<String>::new());

    let mut past = rule();
    past.observe(&sizes(|sizes| {
        at_limit(sizes);
        sizes.repair_sources.departed.push(source(14));
    }));
    let found = past.drain(1);
    assert!(
        found
            .iter()
            .any(|found| found.contains("holds 5 departed repair sources, past its 4")),
        "{found:?}"
    );
}

/// A departed supplier's held history may not name an admitted peer, and
/// a set's key must be an admitted peer's source or a held departure, so a
/// departed source that's never held or pruned breaks.
#[test]
fn a_stray_repair_source_breaks_its_row() {
    let mut connected = rule();
    connected.observe(&sizes(|sizes| {
        sizes.repair_sources.tried = vec![source(2)];
        sizes.repair_sources.departed = vec![source(2)];
    }));
    let found = connected.drain(1);
    assert!(
        found
            .iter()
            .any(|found| found.contains("which are admitted peers")),
        "{found:?}"
    );

    let rows: [(&str, Mutation); 4] = [
        (
            "tried repair sources",
            Box::new(|sizes| sizes.repair_sources.tried = vec![source(9)]),
        ),
        (
            "excluded repair sources",
            Box::new(|sizes| sizes.repair_sources.excluded_input = vec![source(9)]),
        ),
        (
            "busy repair sources",
            Box::new(|sizes| sizes.repair_sources.busy = vec![source(9)]),
        ),
        (
            "repair target refusals",
            Box::new(|sizes| sizes.repair_sources.target_refusals = vec![source(9)]),
        ),
    ];
    for (row, change) in rows {
        let mut stray = rule();
        stray.observe(&sizes(&change));
        let found = stray.drain(1);
        assert!(
            found
                .iter()
                .any(|found| found.contains(&format!("holds {row} ["))
                    && found.contains("held departures")),
            "{row}: {found:?}"
        );
        let mut departed = rule();
        departed.observe(&sizes(|sizes| {
            change(sizes);
            sizes.repair_sources.departed = vec![source(9)];
        }));
        assert_eq!(departed.drain(1), Vec::<String>::new(), "{row}");
    }
}

/// One open repair context read per owner holds, and a second for the same
/// owner breaks. A replaced episode's read, under another owner, may still
/// be open beside its successor's.
#[test]
fn repair_context_reads_at_and_one_past_one_per_owner() {
    let mut one = rule();
    one.observe(&context(1, 1));
    one.observe(&context(2, 2));
    one.observe(&sizes(|sizes| {
        sizes.storage_calls = vec![1, 2];
        sizes.port_futures = 2;
    }));
    assert_eq!(one.drain(1), Vec::<String>::new());

    let mut two = rule();
    two.observe(&context(1, 1));
    two.observe(&context(2, 1));
    two.observe(&sizes(|sizes| {
        sizes.storage_calls = vec![1, 2];
        sizes.port_futures = 2;
    }));
    let found = two.drain(1);
    assert!(
        found
            .iter()
            .any(|found| found.contains("holds 2 open repair context reads")),
        "{found:?}"
    );
}

/// An open repair context read may stay open up to the request timeout
/// after its dispatch, and breaks one moment past it.
#[test]
fn a_repair_context_read_past_its_timeout_breaks() {
    let dispatched = Duration::from_secs(5);
    for (now, breaks) in [
        (dispatched + REPAIR_CONTEXT_TIMEOUT, false),
        (
            dispatched + REPAIR_CONTEXT_TIMEOUT + Duration::from_millis(1),
            true,
        ),
    ] {
        let mut rule = rule();
        rule.at(Some(dispatched));
        rule.observe(&context(1, 1));
        rule.at(Some(now));
        rule.observe(&sizes(|sizes| {
            sizes.storage_calls = vec![1];
            sizes.port_futures = 1;
        }));
        let found = rule.drain(1);
        assert_eq!(
            found
                .iter()
                .any(|found| found.contains("past the 30s request timeout")),
            breaks,
            "{now:?}: {found:?}"
        );
    }
}

/// A repair context read that ends past the request timeout breaks at its
/// `CallEnded`, even when no `Sizes` report saw it open that late.
#[test]
fn a_repair_context_read_ending_past_its_timeout_breaks() {
    let dispatched = Duration::from_secs(5);
    for (ended, breaks) in [
        (dispatched + REPAIR_CONTEXT_TIMEOUT, false),
        (
            dispatched + REPAIR_CONTEXT_TIMEOUT + Duration::from_millis(1),
            true,
        ),
    ] {
        let mut rule = rule();
        rule.at(Some(dispatched));
        rule.observe(&context(1, 1));
        rule.at(Some(dispatched + REPAIR_CONTEXT_TIMEOUT));
        rule.observe(&sizes(|sizes| {
            sizes.storage_calls = vec![1];
            sizes.port_futures = 1;
        }));
        rule.at(Some(ended));
        rule.observe(&ReactorEvent::CallEnded {
            call: 1,
            end: CallEnd::TimedOut,
        });
        let found = rule.drain(1);
        assert_eq!(
            found
                .iter()
                .any(|found| found.contains("ended a repair context read")),
            breaks,
            "{ended:?}: {found:?}"
        );
    }
}

/// A read still open at the shutdown unit's `Sizes`, or dropped by the
/// teardown, isn't judged by age, since a reactor parked on a fatal
/// local-operation deadline stops polling its calls until shutdown.
#[test]
fn a_shutdown_judges_no_repair_context_read_by_age() {
    let late = Duration::from_secs(5) + REPAIR_CONTEXT_TIMEOUT * 2;
    let mut rule = rule();
    rule.at(Some(Duration::from_secs(5)));
    rule.observe(&context(1, 1));
    rule.at(Some(late));
    rule.observe(&ReactorEvent::CauseSet {
        cause: ReactorCause::Shutdown,
    });
    rule.observe(&sizes(|sizes| {
        sizes.storage_calls = vec![1];
        sizes.port_futures = 1;
    }));
    rule.observe(&ReactorEvent::CallEnded {
        call: 1,
        end: CallEnd::Dropped,
    });
    let found = rule.drain(1);
    assert!(
        !found
            .iter()
            .any(|found| found.contains("repair context read")),
        "{found:?}"
    );
}
