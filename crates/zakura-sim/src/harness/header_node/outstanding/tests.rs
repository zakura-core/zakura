//! The outstanding-work rule's rows, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::{BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority};
use zakura_network::zakura::{
    observe::{
        CallEnd, HeaderTargetPhase, PortCall, ReactorCause, ReactorEvent, ReactorSizes, RequestRef,
        ReservationPurpose,
    },
    HeaderTargetPurpose, ServicePeerDirection, ZakuraPeerId,
};

use super::*;

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

/// The rule after mirror 2's admission, with a peer limit of 8.
fn rule() -> Outstanding {
    let mut rule = Outstanding::new(OutstandingEvidence::default(), 8, held());
    rule.observe(&admitted(mirror(2), 1));
    rule
}

/// A reactor whose containers match its facts breaks nothing, and the
/// drain after its `Sizes` makes the rule applicable.
#[test]
fn matching_sizes_hold_and_apply() {
    let evidence = OutstandingEvidence::default();
    let mut rule = Outstanding::new(evidence.clone(), 8, held());
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

/// An active slot whose round was released breaks the rule, and so does
/// one whose session was replaced unless it's a repair's local operation;
/// an unstarted slot needs its admitted session and an open locator read;
/// a completed target holds once a `TargetCompleted` names it.
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

    rule.observe(&ReactorEvent::RoundReleased {
        peer: mirror(2),
        incarnation: 4,
        queue_owned: 0,
    });
    rule.observe(&sizes(|_| {}));
    let breaks = rule.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("round 4, which isn't a started"),
        "{breaks:?}"
    );

    // An ordinary round's slot doesn't outlive its session, but a repair's
    // preparing or applying one does.
    rule.observe(&admitted(mirror(2), 2));
    rule.observe(&started(5));
    rule.observe(&active_slot(5));
    rule.observe(&sizes(|_| {}));
    let breaks = rule.drain(3);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("Receiving slot") && breaks[0].contains("session 1"),
        "{breaks:?}"
    );
    let ReactorEvent::Slots { mut slots } = active_slot(5) else {
        unreachable!("active_slot reports slots")
    };
    slots[0].repair = true;
    slots[0].phase = Some(HeaderTargetPhase::Applying);
    rule.observe(&ReactorEvent::Slots { slots });
    rule.observe(&sizes(|sizes| sizes.staged_entries = vec![(mirror(2), 0)]));
    assert_eq!(rule.drain(3), Vec::<String>::new());

    // An unstarted slot needs its admitted session and an open locator read.
    rule.observe(&unstarted_slot(1));
    rule.observe(&sizes(|_| {}));
    let breaks = rule.drain(4);
    assert_eq!(breaks.len(), 2, "{breaks:?}");
    assert!(
        breaks[0].contains("session 1, which isn't admitted"),
        "{breaks:?}"
    );
    assert!(breaks[1].contains("no open locator read"), "{breaks:?}");
    rule.observe(&unstarted_slot(2));
    rule.observe(&locator(7, mirror(2)));
    rule.observe(&sizes(|sizes| {
        sizes.storage_calls = vec![7];
        sizes.port_futures = 1;
    }));
    assert_eq!(rule.drain(5), Vec::<String>::new());
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
