use zakura_scheduler::SimInstant;

use super::{HoldWakeCheck, HoldWakeEvidence, HoldWakeFact};
use crate::{
    harness::header_node::{
        policy::{Allow, Hold, OpKind, Predicates, Select, Trigger},
        stamp::Stamps,
    },
    observe::{ReactorFact, StorageFact, StorageRequest, StorageResult},
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
    let evidence = HoldWakeEvidence::default();
    evidence.stamp_with(stamps.clone());
    (predicates, HoldWakeCheck::new(evidence.clone()), evidence)
}

/// Feeds `fact` to `predicates`, then to `check`, as the storage source
/// does.
fn feed(predicates: &mut Predicates, check: &mut HoldWakeCheck, fact: &StorageFact) {
    predicates.record(fact);
    check.storage(fact, predicates);
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
