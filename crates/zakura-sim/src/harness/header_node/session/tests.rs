//! Each rule of the session view, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::SourceId;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{HeaderRequestTerminal, HeaderTargetPhase, RequestRef},
    HeaderTargetPurpose,
};

use super::*;

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
}

/// The authority every test owner carries.
fn authority() -> HeaderWorkAuthority {
    let hash = block::Hash([1; 32]);
    HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(1),
        branch: BranchId::new(hash, hash),
    }
}

/// Ordinary work of session `session_id`.
fn ordinary(session_id: u64) -> HeaderSyncWorkOwner {
    HeaderSyncWorkOwner::Header(authority().bind(session_id, NonZeroU64::MIN))
}

/// An auxiliary repair of session `session_id`.
fn repair(session_id: u64) -> HeaderSyncWorkOwner {
    HeaderSyncWorkOwner::BodyRepair(
        BodyWorkAuthority {
            header: authority(),
            verified_generation: VerifiedGeneration::new(1),
            body_work_epoch: BodyWorkEpoch::default(),
        }
        .bind(session_id, NonZeroU64::MIN),
    )
}

/// The link's origin, as `Topology::link` sets it.
fn origin(id: u64) -> Option<SessionOrigin> {
    Some(SessionOrigin {
        connection: id,
        quic_stream: None,
        generation: id,
    })
}

/// The reactor's admission of the mirror's session `session_id`, inbound.
fn admitted(session_id: u64, replaced: Option<u64>, origin: Option<SessionOrigin>) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id,
        direction: ServicePeerDirection::Inbound,
        replaced,
        origin,
    }
}

/// The start of a request owned by `owner`.
fn started(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    ReactorEvent::Started {
        request: RequestRef {
            peer: mirror(),
            session_id: owner.session_id(),
            request_id: 1,
            incarnation: 1,
            owner,
            target_tip_hash: block::Hash([1; 32]),
        },
        purpose: HeaderTargetPurpose::Normal,
    }
}

/// A prepare (`apply` false) or apply dispatch for `owner`.
fn dispatched(apply: bool, owner: HeaderSyncWorkOwner) -> ReactorEvent {
    let (peer, source) = (mirror(), SourceId::from_digest([2; 32]));
    ReactorEvent::Dispatched {
        call: 1,
        op: if apply {
            PortCall::Apply {
                peer,
                source,
                owner,
            }
        } else {
            PortCall::Prepare {
                peer,
                source,
                owner,
            }
        },
    }
}

/// A view that joins, with the mirror's links on connections `links`, each
/// dialed by the mirror.
fn linked(links: &[u64]) -> SessionEvidence {
    let view = SessionEvidence::default();
    view.expect_links();
    for id in links {
        view.link_opened(
            &mirror(),
            "mirror-0",
            origin(*id).expect("a link has an origin"),
            *id,
            "mirror-0",
            ServicePeerDirection::Inbound,
        );
    }
    view
}

/// The rule `events` break on `view`, if any.
fn broken(view: &SessionEvidence, events: &[ReactorEvent]) -> Option<String> {
    for event in events {
        view.reactor(event);
    }
    view.broken()
}

/// An admission that agrees with its link passes, and the view keeps both
/// sides' facts in order. A close is recorded once.
#[test]
fn an_admission_joins_its_link() {
    let view = linked(&[1]);
    assert_eq!(broken(&view, &[admitted(1, None, origin(1))]), None);
    view.link_closed(1);
    view.link_closed(1);
    view.reactor(&ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 1,
        reason: "test",
    });
    assert_eq!(
        view.facts(),
        [
            SessionFact::LinkOpened {
                peer: "mirror-0".to_owned(),
                connection: 1,
                session_id: 1,
                dialer: "mirror-0".to_owned(),
            },
            SessionFact::SessionAdmitted {
                peer: "mirror-0".to_owned(),
                session_id: 1,
                direction: ServicePeerDirection::Inbound,
                replaced: None,
                origin: origin(1),
            },
            SessionFact::LinkClosed {
                peer: "mirror-0".to_owned(),
                connection: 1,
            },
            SessionFact::SessionEnded {
                peer: "mirror-0".to_owned(),
                session_id: 1,
                reason: "test".to_owned(),
            },
        ]
    );
}

/// An admission with no origin, an origin naming no link, the wrong
/// generation or a QUIC stream, a wrong session ID, peer or direction, and a
/// second admission of one link each break the join.
#[test]
fn every_join_mismatch_breaks() {
    let other = ZakuraPeerId::new(vec![3; 32]).expect("32 bytes is a valid peer ID");
    let wrong_peer = ReactorEvent::SessionAdmitted {
        peer: other,
        session_id: 1,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: origin(1),
    };
    let outbound = ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id: 1,
        direction: ServicePeerDirection::Outbound,
        replaced: None,
        origin: origin(1),
    };
    let wrong_generation = Some(SessionOrigin {
        connection: 1,
        quic_stream: None,
        generation: 2,
    });
    let with_stream = Some(SessionOrigin {
        connection: 1,
        quic_stream: Some(99),
        generation: 1,
    });
    for (events, expected) in [
        (vec![admitted(1, None, None)], "with no origin"),
        (vec![admitted(1, None, origin(9))], "names no link"),
        (
            vec![admitted(1, None, wrong_generation)],
            "its link has origin",
        ),
        (vec![admitted(1, None, with_stream)], "its link has origin"),
        (vec![admitted(2, None, origin(1))], "its link has session 1"),
        (vec![wrong_peer], "its link has peer mirror-0"),
        (vec![outbound], "its link has direction Inbound"),
        (
            vec![admitted(1, None, origin(1)), admitted(1, None, origin(1))],
            "admitted connection 1 twice",
        ),
    ] {
        let why = broken(&linked(&[1]), &events).expect("the join breaks");
        assert!(
            why.starts_with("SessionJoin:") && why.contains(expected),
            "{why}"
        );
    }
}

/// Without links reported, the view keeps the facts and doesn't join.
#[test]
fn a_view_without_links_does_not_join() {
    let view = SessionEvidence::default();
    assert_eq!(broken(&view, &[admitted(1, None, None)]), None);
}

/// After a replacement or an end, a new start or an ordinary apply naming
/// the old session breaks the rule. A prepare dispatch, work of the live
/// session, and work before the session left don't.
#[test]
fn departed_session_work_breaks() {
    let replaced = || {
        vec![
            admitted(1, None, origin(1)),
            started(ordinary(1)),
            dispatched(true, ordinary(1)),
            admitted(2, Some(1), origin(2)),
            dispatched(false, ordinary(1)),
            started(ordinary(2)),
            dispatched(true, ordinary(2)),
        ]
    };
    let ended = || {
        vec![
            admitted(1, None, origin(1)),
            ReactorEvent::SessionEnded {
                peer: mirror(),
                session_id: 1,
                reason: "test",
            },
        ]
    };
    assert_eq!(broken(&linked(&[1, 2]), &replaced()), None);
    for (mut events, late) in [
        (replaced(), started(ordinary(1))),
        (replaced(), dispatched(true, ordinary(1))),
        (ended(), started(ordinary(1))),
        (ended(), dispatched(true, ordinary(1))),
    ] {
        events.push(late);
        let why = broken(&linked(&[1, 2]), &events).expect("the late work breaks the rule");
        assert!(why.starts_with("DepartedSessionWork:"), "{why}");
    }
}

/// The retirement of `owner`'s request.
fn retired(owner: HeaderSyncWorkOwner) -> ReactorEvent {
    let ReactorEvent::Started { request, purpose } = started(owner) else {
        unreachable!("`started` builds a start");
    };
    ReactorEvent::Retired {
        request,
        phase: HeaderTargetPhase::Applying,
        purpose,
        reason: HeaderRequestTerminal::SessionReplaced,
    }
}

/// A repair that was preparing when its session left may apply once. One
/// prepared only afterwards may not, nor one that never prepared, nor one
/// that retired or was already applying before its session left.
#[test]
fn a_kept_repair_may_apply_once() {
    let left = || ReactorEvent::SessionEnded {
        peer: mirror(),
        session_id: 1,
        reason: "test",
    };
    let first = || admitted(1, None, origin(1));
    let prepare = || dispatched(false, repair(1));
    let apply = || dispatched(true, repair(1));
    for (events, legal) in [
        (vec![first(), prepare(), left(), apply()], true),
        (vec![first(), prepare(), left(), apply(), apply()], false),
        (vec![first(), left(), prepare(), apply()], false),
        (vec![first(), left(), apply()], false),
        (
            vec![first(), prepare(), retired(repair(1)), left(), apply()],
            false,
        ),
        (
            vec![first(), prepare(), left(), retired(repair(1)), apply()],
            false,
        ),
        (vec![first(), prepare(), apply(), left(), apply()], false),
    ] {
        assert_eq!(
            broken(&linked(&[1]), &events).is_none(),
            legal,
            "{events:?}"
        );
    }
}
