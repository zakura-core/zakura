use zakura_scheduler::SimInstant;

use super::{HoldWakeCheck, HoldWakeEvidence, HoldWakeFact};
use crate::stores::EvidenceStores;
use crate::{
    harness::header_node::{
        policy::{Allow, Hold, OpKind, Predicates, Select, Trigger},
        stamp::Stamps,
    },
    observe::{
        ReactorFact, StorageBranch, StorageFact, StorageLease, StoragePoint, StorageRequest,
        StorageResult, WorkScope,
    },
};

/// A locator submission of operation `id` by reactor call `call`.
fn submitted(id: u64, call: Option<u64>) -> StorageFact {
    StorageFact::Submitted {
        id,
        request: StorageRequest::ContinuationLocator,
        digest: String::new(),
        call,
    }
}

/// Operation `id`'s completion, delivered or not.
fn completed(id: u64, delivered: bool) -> StorageFact {
    StorageFact::Completed {
        id,
        outcome: "unavailable".to_owned(),
        result: StorageResult::Fault {
            fault: "unavailable".to_owned(),
        },
        delivered,
        generation: 0,
        state: String::new(),
    }
}

/// The reactor's handling of `call`.
fn handled(call: u64) -> ReactorFact {
    ReactorFact::Handled {
        call,
        panicked: false,
    }
}

/// Predicates holding the second locator until released, and a check
/// stamped by `stamps`.
fn setup(stamps: &Stamps) -> (Predicates, HoldWakeCheck, HoldWakeEvidence) {
    let predicates = Predicates::new(vec![Hold {
        select: Select::nth(OpKind::Locator, 2),
        until: Trigger::Released,
        then: vec![Allow::Engine],
        deadline_ms: None,
    }]);
    let evidence = HoldWakeEvidence::new(&EvidenceStores::default());
    evidence.stamp_with(stamps.clone());
    (predicates, HoldWakeCheck::new(evidence.clone()), evidence)
}

/// Feeds `fact` to `predicates`, then to `check`, as the storage source
/// does.
fn feed(predicates: &mut Predicates, check: &mut HoldWakeCheck, fact: &StorageFact) {
    predicates.record(fact);
    check.storage(fact, predicates, |peer| Some(format!("mirror {peer}")));
}

/// Only the held operation's delivered reply opens a fact, and only its
/// call's handling answers it. Another call's handling, and a second
/// handling of the same call, add nothing.
#[test]
fn a_delivered_held_reply_opens_and_its_handling_answers() {
    let stamps = Stamps::default();
    let (mut predicates, mut check, evidence) = setup(&stamps);
    for fact in [
        submitted(1, Some(1)),
        completed(1, true),
        submitted(2, Some(2)),
    ] {
        feed(&mut predicates, &mut check, &fact);
    }
    check.reactor(&handled(1));
    assert!(
        evidence.facts().is_empty(),
        "the unheld locator opens nothing"
    );
    feed(&mut predicates, &mut check, &completed(2, true));
    check.reactor(&handled(3));
    check.reactor(&handled(2));
    check.reactor(&handled(2));
    assert_eq!(
        evidence.facts(),
        [
            (
                1,
                HoldWakeFact::Opened {
                    call: 2,
                    op: 2,
                    at: SimInstant::default()
                }
            ),
            (2, HoldWakeFact::Handled { call: 2, op: 2 }),
        ]
    );
    assert_eq!(
        evidence.fact(2),
        Some(HoldWakeFact::Handled { call: 2, op: 2 })
    );
    assert_eq!(evidence.fact(3), None);
}

/// A held reply nobody waited for opens nothing, and neither does a held
/// operation no reactor call submitted.
#[test]
fn an_undelivered_or_callless_held_reply_opens_nothing() {
    let stamps = Stamps::default();
    let (mut predicates, mut check, evidence) = setup(&stamps);
    for fact in [
        submitted(1, Some(1)),
        submitted(2, Some(2)),
        completed(2, false),
    ] {
        feed(&mut predicates, &mut check, &fact);
    }
    check.reactor(&handled(2));
    let (mut predicates, mut check, callless) = setup(&stamps);
    for fact in [submitted(1, None), submitted(2, None), completed(2, true)] {
        feed(&mut predicates, &mut check, &fact);
    }
    assert!(evidence.facts().is_empty());
    assert!(callless.facts().is_empty());
}

/// A page read of operation `id` on a lease for `peer`'s session `session`.
fn read(id: u64, peer: &str, session: u64) -> StorageFact {
    StorageFact::Submitted {
        id,
        request: StorageRequest::ReadHeaderPath {
            lease: Some(StorageLease {
                peer: peer.to_owned(),
                session_id: session,
                lease_id: id,
            }),
            scope: WorkScope {
                header_generation: 0,
                branch: StorageBranch {
                    anchor: String::new(),
                    target: String::new(),
                },
            },
            after: String::new(),
            max_headers: 1,
        },
        digest: String::new(),
        call: None,
    }
}

/// Operation `id`'s completion with a page, delivered or not.
fn page(id: u64, delivered: bool) -> StorageFact {
    StorageFact::Completed {
        id,
        outcome: "engine".to_owned(),
        result: StorageResult::Page {
            last: StoragePoint {
                height: 1,
                hash: String::new(),
            },
            count: 1,
            complete: true,
        },
        delivered,
        generation: 0,
        state: String::new(),
    }
}

/// Predicates holding the first two page reads until released, and a check
/// stamped by `stamps`.
fn serving_setup(stamps: &Stamps) -> (Predicates, HoldWakeCheck, HoldWakeEvidence) {
    let predicates = Predicates::new(
        [1, 2]
            .map(|nth| Hold {
                select: Select::nth(OpKind::Read, nth),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            })
            .to_vec(),
    );
    let evidence = HoldWakeEvidence::new(&EvidenceStores::default());
    evidence.stamp_with(stamps.clone());
    (predicates, HoldWakeCheck::new(evidence.clone()), evidence)
}

/// A held page read whose page reaches its job opens a served fact for its
/// mirror's session, and only the page its own read produced answers it. A
/// page queued for another session or peer, a page another read of the
/// same session produced (another subscription's, or a later one), and the
/// same page twice add nothing.
#[test]
fn a_served_held_read_opens_and_its_sessions_page_answers() {
    let stamps = Stamps::default();
    let (mut predicates, mut check, evidence) = serving_setup(&stamps);
    for fact in [read(1, "aa", 7), page(1, true)] {
        feed(&mut predicates, &mut check, &fact);
    }
    check.queued("aa", 8, 1);
    check.queued("bb", 7, 1);
    check.queued("aa", 7, 2);
    check.queued("aa", 7, 1);
    check.queued("aa", 7, 1);
    assert_eq!(
        evidence.facts(),
        [
            (
                1,
                HoldWakeFact::Served {
                    op: 1,
                    peer: "mirror aa".to_owned(),
                    session_id: 7,
                    at: SimInstant::default(),
                }
            ),
            (
                2,
                HoldWakeFact::Queued {
                    op: 1,
                    session_id: 7
                }
            ),
        ]
    );
    assert_eq!(evidence.served_peer(1).as_deref(), Some("mirror aa"));
    assert_eq!(evidence.served_peer(2), None);
}

/// A held page read that fails or isn't delivered opens nothing, and
/// neither does one whose peer has no harness name.
#[test]
fn a_failed_undelivered_or_unnamed_read_opens_nothing() {
    let stamps = Stamps::default();
    let (mut predicates, mut check, evidence) = serving_setup(&stamps);
    for fact in [
        read(1, "aa", 7),
        completed(1, true),
        read(2, "aa", 7),
        page(2, false),
    ] {
        feed(&mut predicates, &mut check, &fact);
    }
    let (mut predicates, mut unnamed_check, unnamed) = serving_setup(&stamps);
    for fact in [read(1, "aa", 7), page(1, true)] {
        predicates.record(&fact);
        unnamed_check.storage(&fact, &predicates, |_| None);
    }
    check.queued("aa", 7, 1);
    check.queued("aa", 7, 2);
    unnamed_check.queued("aa", 7, 1);
    assert!(evidence.facts().is_empty());
    assert!(unnamed.facts().is_empty());
}

/// A hold-wake fact kept after the node's stores froze is dropped.
#[test]
fn a_frozen_hold_wake_store_drops_a_fact() {
    crate::stores::assert_freeze_drops_writes(
        HoldWakeEvidence::armed,
        |evidence| {
            evidence.push(HoldWakeFact::Opened {
                call: 1,
                op: 2,
                at: SimInstant::ZERO,
            });
        },
        HoldWakeEvidence::facts,
    );
}
