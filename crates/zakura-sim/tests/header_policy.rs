//! Outcome predicates (M4-B2), driven by scripted storage facts, reactor
//! events and pending operations, without a node.
#![cfg(zakura_sim)]

use std::time::Duration;

use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::header_node::{
        policy::{Allow, Hold, LinkEffect, OpKind, Predicates, Select, Trigger},
        Base,
    },
    observe::{
        ReactorCall, ReactorFact, StorageBranch, StorageFact, StorageRequest, WorkOwner, WorkScope,
    },
    Boundary,
};
use zakura_storage::header::{
    controlled::{OpId, PendingOp},
    Fault, HeaderOp, Outcome,
};

/// Every outcome, the legal set of a pending operation past its timeout.
const ALL: [Outcome; 4] = [
    Outcome::Engine,
    Outcome::Fault(Fault::Timeout),
    Outcome::Fault(Fault::Unavailable),
    Outcome::Fault(Fault::LocalResource),
];

/// A header owner in `session`.
fn owner(session: u64) -> WorkOwner {
    WorkOwner {
        kind: "header".to_owned(),
        scope: WorkScope {
            header_generation: 1,
            branch: StorageBranch {
                anchor: "00".to_owned(),
                target: "11".to_owned(),
            },
        },
        verified_generation: None,
        body_work_epoch: None,
        session_id: session,
        request_id: 1,
    }
}

/// A request of `kind`, attributed to `session` where the kind carries one.
fn request(kind: OpKind, session: u64) -> StorageRequest {
    match kind {
        OpKind::Locator => StorageRequest::ContinuationLocator,
        OpKind::Membership => StorageRequest::SelectedChainContains {
            hash: "11".to_owned(),
        },
        OpKind::Prepare => StorageRequest::PrepareHeaderTarget {
            owner: owner(session),
            source: "aa".to_owned(),
            common_ancestor: zakura_sim::observe::StoragePoint {
                height: 0,
                hash: "00".to_owned(),
            },
            target: zakura_sim::observe::StoragePoint {
                height: 1,
                hash: "11".to_owned(),
            },
            headers: 1,
        },
        OpKind::Apply => StorageRequest::ApplyHeaderTarget {
            owner: owner(session),
            source: "aa".to_owned(),
            parent: None,
            target: "11".to_owned(),
        },
        other => panic!("these tests don't submit {other:?}"),
    }
}

/// Operation `id` of `kind` was submitted in `session`.
fn submitted(id: u64, kind: OpKind, session: u64) -> StorageFact {
    StorageFact::Submitted {
        id,
        request: request(kind, session),
        digest: String::new(),
    }
}

/// Operation `id` of `kind`, pending for `since`.
fn pending(id: u64, kind: OpKind, since: Duration) -> PendingOp {
    PendingOp {
        id: OpId(id),
        op: match kind {
            OpKind::Locator => HeaderOp::ContinuationLocator,
            OpKind::Membership => HeaderOp::SelectedChainContains,
            OpKind::Prepare => HeaderOp::PrepareHeaderTarget,
            OpKind::Apply => HeaderOp::ApplyHeaderTarget,
            other => panic!("these tests don't submit {other:?}"),
        },
        enabled: ALL.to_vec(),
        since_submit: since,
        caller_waiting: true,
    }
}

/// A hold on `select` until `until`, then `engine`.
fn hold(select: Select, until: Trigger) -> Hold {
    Hold {
        select,
        until,
        then: vec![Allow::Engine],
        deadline_ms: None,
    }
}

/// Every outcome `predicates` offers for `op` under `base`.
fn offered(predicates: &Predicates, base: Base, op: &PendingOp) -> Vec<Outcome> {
    ALL.into_iter()
        .filter(|outcome| predicates.offers(base, op, *outcome))
        .collect()
}

/// `op`'s boundaries at `now`, with a 5 s base bound.
fn boundaries(predicates: &Predicates, op: &PendingOp, now: SimInstant) -> Vec<Boundary> {
    let mut out = Vec::new();
    predicates.boundaries(Some(Duration::from_secs(5)), op, now, &mut out);
    out
}

const HOUR: Duration = Duration::from_secs(3_600);

/// A locator held for 2 h stops time at its submission plus 2 h, offers
/// nothing before then, and offers its late `engine` from that instant.
#[test]
fn virtual_time_trigger_opens_on_time() {
    let two_hours = u64::try_from((2 * HOUR).as_millis()).unwrap();
    let mut predicates = Predicates::new(vec![hold(
        Select::nth(OpKind::Locator, 1),
        Trigger::VirtualTime(two_hours),
    )]);
    predicates.record(&submitted(1, OpKind::Locator, 0));
    // Submitted at 10 s, so 2 h later is 2 h 10 s.
    let at = |d: Duration| SimInstant::from_start(Duration::from_secs(10) + d);

    let early = pending(1, OpKind::Locator, HOUR);
    assert_eq!(offered(&predicates, Base::Any, &early), []);
    assert_eq!(
        boundaries(&predicates, &early, at(HOUR)),
        [Boundary::Opens(at(2 * HOUR))],
        "time stops at the opening, and the held op ignores the bound"
    );

    let open = pending(1, OpKind::Locator, 2 * HOUR);
    assert_eq!(offered(&predicates, Base::Any, &open), [Outcome::Engine]);
    assert_eq!(boundaries(&predicates, &open, at(2 * HOUR)), []);
}

/// The same hold with a 3 h `deadline_ms` also stops time at its
/// submission plus 3 h, both before and after it opens.
#[test]
fn deadline_stops_time_at_submission_plus_its_bound() {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap();
    let mut predicates = Predicates::new(vec![Hold {
        deadline_ms: Some(ms(3 * HOUR)),
        ..hold(
            Select::nth(OpKind::Locator, 1),
            Trigger::VirtualTime(ms(2 * HOUR)),
        )
    }]);
    predicates.record(&submitted(1, OpKind::Locator, 0));
    // Submitted at 10 s, as above.
    let at = |d: Duration| SimInstant::from_start(Duration::from_secs(10) + d);

    let early = pending(1, OpKind::Locator, HOUR);
    assert_eq!(
        boundaries(&predicates, &early, at(HOUR)),
        [
            Boundary::Opens(at(2 * HOUR)),
            Boundary::Deadline(at(3 * HOUR))
        ]
    );
    let open = pending(1, OpKind::Locator, 2 * HOUR);
    assert_eq!(offered(&predicates, Base::Any, &open), [Outcome::Engine]);
    assert_eq!(
        boundaries(&predicates, &open, at(2 * HOUR)),
        [Boundary::Deadline(at(3 * HOUR))]
    );
}

/// A hold until `CallerDropped(held)` waits for its own operation's caller.
/// Another operation's caller dropping first doesn't open it.
#[test]
fn caller_dropped_held_waits_for_its_own_drop() {
    let mut predicates = Predicates::new(vec![hold(
        Select::nth(OpKind::Locator, 1),
        Trigger::CallerDropped(Select::held()),
    )]);
    predicates.record(&submitted(1, OpKind::Locator, 0));
    predicates.record(&submitted(2, OpKind::Locator, 0));
    let held = pending(1, OpKind::Locator, Duration::ZERO);

    predicates.record(&StorageFact::CallerDropped { id: 2 });
    assert_eq!(
        offered(&predicates, Base::Any, &held),
        [],
        "operation 2's drop leaves the hold on operation 1 closed"
    );
    predicates.record(&StorageFact::CallerDropped { id: 1 });
    assert_eq!(offered(&predicates, Base::Any, &held), [Outcome::Engine]);
}

/// `Count(16, ...)` stays closed after 15 caller drops and opens on the
/// 16th. `All` opens only once each part holds, in either order.
#[test]
fn count_and_all_wait_for_every_part() {
    let locators = Select {
        kind: Some(OpKind::Locator),
        ..Select::default()
    };
    let mut predicates = Predicates::new(vec![hold(
        Select::nth(OpKind::Apply, 1),
        Trigger::Count(16, Box::new(Trigger::CallerDropped(locators))),
    )]);
    for id in 1..=16 {
        predicates.record(&submitted(id, OpKind::Locator, 0));
    }
    predicates.record(&submitted(17, OpKind::Apply, 1));
    let apply = pending(17, OpKind::Apply, Duration::ZERO);
    for id in 1..=15 {
        predicates.record(&StorageFact::CallerDropped { id });
        assert_eq!(
            offered(&predicates, Base::Any, &apply),
            [],
            "after {id} drops"
        );
    }
    predicates.record(&StorageFact::CallerDropped { id: 16 });
    assert_eq!(offered(&predicates, Base::Any, &apply), [Outcome::Engine]);

    let both = Trigger::All(vec![
        Trigger::ReplyTaken(Select::nth(OpKind::Membership, 1)),
        Trigger::Closed("mirror-0".to_owned()),
    ]);
    for link_first in [false, true] {
        let mut predicates =
            Predicates::new(vec![hold(Select::nth(OpKind::Prepare, 1), both.clone())]);
        predicates.record(&submitted(1, OpKind::Prepare, 1));
        predicates.record(&submitted(2, OpKind::Membership, 0));
        let prepare = pending(1, OpKind::Prepare, Duration::ZERO);
        let parts: [&dyn Fn(&mut Predicates); 2] =
            [&|p| p.record(&StorageFact::ReplyTaken { id: 2 }), &|p| {
                p.record_link(LinkEffect::Closed, "mirror-0")
            }];
        let (first, second) = if link_first {
            (parts[1], parts[0])
        } else {
            (parts[0], parts[1])
        };
        first(&mut predicates);
        assert_eq!(offered(&predicates, Base::Any, &prepare), []);
        // A link that was only made doesn't count as closed.
        predicates.record_link(LinkEffect::Linked, "mirror-0");
        assert_eq!(offered(&predicates, Base::Any, &prepare), []);
        second(&mut predicates);
        assert_eq!(offered(&predicates, Base::Any, &prepare), [Outcome::Engine]);
    }
}

/// A hold picks only its own instances. The second locator and an apply in
/// another session follow the base's outcomes and its bound, while the
/// held ones offer nothing and ignore the bound.
#[test]
fn unpicked_ops_follow_the_base() {
    let never = Trigger::Closed("nobody".to_owned());
    let mut predicates = Predicates::new(vec![
        hold(Select::nth(OpKind::Locator, 1), never.clone()),
        hold(
            Select {
                kind: Some(OpKind::Apply),
                session: Some(2),
                ..Select::default()
            },
            never,
        ),
    ]);
    for (id, kind, session) in [
        (1, OpKind::Locator, 0),
        (2, OpKind::Locator, 0),
        (3, OpKind::Apply, 1),
        (4, OpKind::Apply, 2),
    ] {
        predicates.record(&submitted(id, kind, session));
    }
    let now = SimInstant::from_start(Duration::from_secs(1));
    let since = Duration::from_secs(1);
    let deadline = [Boundary::Deadline(SimInstant::from_start(
        Duration::from_secs(5),
    ))];
    for (id, kind, held) in [
        (1, OpKind::Locator, true),
        (2, OpKind::Locator, false),
        (3, OpKind::Apply, false),
        (4, OpKind::Apply, true),
    ] {
        let op = pending(id, kind, since);
        let (outcomes, bounds): (Vec<Outcome>, &[Boundary]) = if held {
            (Vec::new(), &[])
        } else {
            (vec![Outcome::Engine], &deadline)
        };
        assert_eq!(
            offered(&predicates, Base::SuccessOnly, &op),
            outcomes,
            "op {id}"
        );
        assert_eq!(boundaries(&predicates, &op, now), bounds, "op {id}");
    }
}

/// Call `call` of `kind` was dispatched for `session`.
fn dispatched(call: u64, kind: OpKind, session: u64) -> ReactorFact {
    let port = match kind {
        OpKind::Locator => ReactorCall::Locator {
            peer: "aa".to_owned(),
            session_id: session,
            target: "11".to_owned(),
            scope: owner(session).scope,
        },
        OpKind::Prepare => ReactorCall::Prepare {
            peer: "aa".to_owned(),
            source: "aa".to_owned(),
            owner: owner(session),
        },
        OpKind::Apply => ReactorCall::Apply {
            peer: "aa".to_owned(),
            source: "aa".to_owned(),
            owner: owner(session),
        },
        other => panic!("these tests don't dispatch {other:?}"),
    };
    ReactorFact::Dispatched { call, port }
}

/// Call `call` ended `end`, paired with `op`.
fn call_ended(call: u64, end: &str, op: Option<u64>) -> ReactorFact {
    ReactorFact::CallEnded {
        call,
        end: end.to_owned(),
        op,
    }
}

/// The reactor handled call `call`.
fn handled(call: u64) -> ReactorFact {
    ReactorFact::Handled {
        call,
        panicked: false,
    }
}

/// A `Handled` trigger selects a call through its paired operation, by
/// kind, ordinal, session or as the held operation, and only once the
/// reactor has handled it: a reply taken or a call ended isn't enough.
#[test]
fn handled_selects_through_the_paired_operation() {
    let apply = |select: Select| Trigger::Handled(select);
    let session = |session: u64| Select {
        kind: Some(OpKind::Apply),
        session: Some(session),
        ..Select::default()
    };
    let holds = vec![
        hold(
            Select::nth(OpKind::Locator, 1),
            apply(Select::nth(OpKind::Apply, 1)),
        ),
        hold(Select::nth(OpKind::Locator, 2), apply(session(1))),
        hold(Select::nth(OpKind::Locator, 3), apply(session(2))),
        hold(
            Select::nth(OpKind::Locator, 4),
            apply(Select::nth(OpKind::Apply, 2)),
        ),
        hold(Select::nth(OpKind::Apply, 1), apply(Select::held())),
    ];
    let mut predicates = Predicates::new(holds);
    for id in 1..=4 {
        predicates.record(&submitted(id, OpKind::Locator, 0));
    }
    predicates.record(&submitted(5, OpKind::Apply, 1));
    let ops = |predicates: &Predicates| {
        [1, 2, 3, 4]
            .map(|id| pending(id, OpKind::Locator, Duration::ZERO))
            .into_iter()
            .chain([pending(5, OpKind::Apply, Duration::ZERO)])
            .map(|op| !offered(predicates, Base::Any, &op).is_empty())
            .collect::<Vec<_>>()
    };
    predicates.record_reactor(&dispatched(1, OpKind::Apply, 1));
    predicates.record(&StorageFact::ReplyTaken { id: 5 });
    predicates.record_reactor(&call_ended(1, "replied", Some(5)));
    assert_eq!(ops(&predicates), [false; 5]);
    predicates.record_reactor(&handled(1));
    assert_eq!(ops(&predicates), [true, true, false, false, true]);
}

/// A call paired with no operation, here a preparation the reactor never
/// submitted, is selected by its own kind and session, never by an
/// ordinal or as the held operation, which name operations.
#[test]
fn handled_without_an_operation_selects_by_the_calls_kind_and_session() {
    let prepare = |session: Option<u64>| Select {
        kind: Some(OpKind::Prepare),
        session,
        ..Select::default()
    };
    let holds = vec![
        hold(
            Select::nth(OpKind::Locator, 1),
            Trigger::Handled(prepare(Some(3))),
        ),
        hold(
            Select::nth(OpKind::Locator, 2),
            Trigger::Handled(prepare(None)),
        ),
        hold(
            Select::nth(OpKind::Locator, 3),
            Trigger::Handled(prepare(Some(4))),
        ),
        hold(
            Select::nth(OpKind::Locator, 4),
            Trigger::Handled(Select::nth(OpKind::Prepare, 1)),
        ),
        hold(
            Select::nth(OpKind::Locator, 5),
            Trigger::Handled(Select {
                kind: Some(OpKind::Apply),
                ..Select::default()
            }),
        ),
        hold(
            Select::nth(OpKind::Locator, 6),
            Trigger::Handled(Select::held()),
        ),
    ];
    let mut predicates = Predicates::new(holds);
    for id in 1..=6 {
        predicates.record(&submitted(id, OpKind::Locator, 0));
    }
    predicates.record_reactor(&dispatched(1, OpKind::Prepare, 3));
    predicates.record_reactor(&call_ended(1, "not-submitted", None));
    predicates.record_reactor(&handled(1));
    let open: Vec<bool> = (1..=6)
        .map(|id| {
            let op = pending(id, OpKind::Locator, Duration::ZERO);
            !offered(&predicates, Base::Any, &op).is_empty()
        })
        .collect();
    assert_eq!(open, [true, true, false, false, false, false]);
}

/// Everything the predicates answer, for a fixed set of questions.
fn answers(predicates: &Predicates) -> String {
    let now = SimInstant::from_start(HOUR);
    let mut out = Vec::new();
    for (id, kind) in [(1, OpKind::Locator), (2, OpKind::Apply)] {
        for since in [Duration::ZERO, HOUR] {
            let op = pending(id, kind, since);
            out.push(format!(
                "{id}/{since:?}: {:?} {:?}",
                offered(predicates, Base::SuccessOnly, &op),
                boundaries(predicates, &op, now)
            ));
        }
    }
    let gate = Trigger::Count(2, Box::new(Trigger::Linked("mirror-0".to_owned())));
    let mut gate_bounds = Vec::new();
    predicates.control_boundaries(&Trigger::VirtualTime(7_200_000), now, &mut gate_bounds);
    out.push(format!(
        "{:?} {gate_bounds:?}",
        predicates.control_holds(&gate, now)
    ));
    out.join("\n")
}

/// One scripted thing the run records.
type Record = Box<dyn Fn(&mut Predicates)>;

/// Asking twice with nothing recorded between gives the same answers, and
/// so does a fresh copy fed the same facts, so only recording changes them.
#[test]
fn policy_is_pure_between_steps() {
    let holds = vec![
        hold(
            Select::nth(OpKind::Apply, 1),
            Trigger::All(vec![
                Trigger::Completed(Select::nth(OpKind::Locator, 1)),
                Trigger::VirtualTime(1_000),
            ]),
        ),
        hold(
            Select::nth(OpKind::Locator, 1),
            Trigger::CallerDropped(Select::held()),
        ),
    ];
    let script: Vec<Record> = vec![
        Box::new(|p| p.record(&submitted(1, OpKind::Locator, 0))),
        Box::new(|p| p.record(&submitted(2, OpKind::Apply, 1))),
        Box::new(|p| p.record_link(LinkEffect::Linked, "mirror-0")),
        Box::new(|p| p.record(&StorageFact::CallerDropped { id: 1 })),
        Box::new(|p| p.record_link(LinkEffect::Linked, "mirror-0")),
    ];
    let mut predicates = Predicates::new(holds.clone());
    let mut seen = vec![answers(&predicates)];
    for (step, record) in script.iter().enumerate() {
        record(&mut predicates);
        let first = answers(&predicates);
        assert_eq!(answers(&predicates), first, "asked twice after {step}");
        let mut fresh = Predicates::new(holds.clone());
        script[..=step].iter().for_each(|record| record(&mut fresh));
        assert_eq!(answers(&fresh), first, "a replay of the same facts");
        seen.push(first);
    }
    // The script moves the answers, so the comparisons above can fail.
    seen.dedup();
    assert!(seen.len() > 2, "{seen:#?}");
}
