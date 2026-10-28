//! The block node's outcome predicates (BS2), driven by scripted storage
//! facts, mirror read events and link effects, without a node.
#![cfg(zakura_sim)]

use std::time::Duration;

use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{
        block_node::policy::{
            undriven, Block, BlockAllow, BlockKind, Hold, MirrorEvent, Predicates, Select, Trigger,
        },
        policy::{Family, LinkEffect},
    },
    observe::{StorageBranch, StorageFact, StorageRequest, WorkOwner, WorkScope},
};
use zakura_storage::block::{BlockOutcome, Commitment, RetryKind, Verdict};

/// Body work owned by `session`.
fn owner(session: u64) -> WorkOwner {
    WorkOwner {
        kind: "body".to_owned(),
        scope: WorkScope {
            header_generation: 1,
            branch: StorageBranch {
                anchor: "00".to_owned(),
                target: "11".to_owned(),
            },
        },
        verified_generation: Some(1),
        body_work_epoch: Some(1),
        session_id: session,
        request_id: 1,
    }
}

/// The node's submission `id` of a body for `session`.
fn submitted(id: u64, session: u64) -> StorageFact {
    StorageFact::BlockSubmitted {
        id,
        request: StorageRequest::SubmitBlock {
            owner: owner(session),
            source: "aa".to_owned(),
            token: id,
            height: Some(1),
            hash: "11".to_owned(),
            parent: "00".to_owned(),
            class: "full".to_owned(),
        },
        stage: Some("queued".to_owned()),
    }
}

/// Whether a control step's `trigger` holds, at the run's start.
fn holds(predicates: &Predicates, trigger: &Trigger) -> bool {
    predicates.control_holds(trigger, SimInstant::default())
}

/// A hold on the first mirror read until `until`, offering `then` once
/// open.
fn mirror_hold(until: Trigger, then: Vec<BlockAllow>) -> Predicates {
    Predicates::new(vec![Hold {
        select: Select::nth(BlockKind::MirrorRead, 1),
        until,
        then,
        deadline_ms: None,
    }])
}

/// A held mirror read serves only once its trigger holds. Reads the hold
/// didn't pick serve at once.
#[test]
fn a_held_mirror_read_serves_once_its_trigger_holds() {
    let mut predicates = mirror_hold(
        Trigger::Submitted(Select::nth(BlockKind::MirrorRead, 2)),
        vec![BlockAllow::Serve],
    );
    predicates.record_mirror(MirrorEvent::Submitted(1));
    assert!(!predicates.serves(1, Duration::ZERO));
    predicates.record_mirror(MirrorEvent::Submitted(2));
    assert!(predicates.serves(2, Duration::ZERO), "read 2 isn't held");
    assert!(predicates.serves(1, Duration::ZERO));
}

/// An open hold that offers no serving never serves its read.
#[test]
fn an_open_hold_serves_only_what_it_offers() {
    let mut predicates = mirror_hold(Trigger::VirtualTime(10), vec![BlockAllow::Error]);
    predicates.record_mirror(MirrorEvent::Submitted(1));
    assert!(!predicates.serves(1, Duration::from_millis(10)));
}

/// A virtual-time hold on a mirror read counts from the read's arrival.
#[test]
fn a_mirror_read_times_its_hold_from_its_arrival() {
    let mut predicates = mirror_hold(Trigger::VirtualTime(10), vec![BlockAllow::Serve]);
    predicates.record_mirror(MirrorEvent::Submitted(1));
    assert!(!predicates.serves(1, Duration::from_millis(9)));
    assert!(predicates.serves(1, Duration::from_millis(10)));
}

/// Mirror reads count as their own kind, and their events count as the
/// node's operations' do.
#[test]
fn mirror_events_count_under_the_mirror_kind() {
    let mut predicates = Predicates::new(Vec::new());
    for event in [
        MirrorEvent::Submitted(1),
        MirrorEvent::Served(1),
        MirrorEvent::Taken(1),
        MirrorEvent::Submitted(2),
        MirrorEvent::Dropped(2),
    ] {
        predicates.record_mirror(event);
    }
    let mirror = Select {
        kind: Some(BlockKind::MirrorRead),
        ..Select::default()
    };
    assert!(holds(
        &predicates,
        &Trigger::Count(2, Box::new(Trigger::Submitted(mirror.clone())))
    ));
    assert!(holds(
        &predicates,
        &Trigger::Completed(Select::nth(BlockKind::MirrorRead, 1))
    ));
    assert!(holds(
        &predicates,
        &Trigger::ReplyTaken(Select::nth(BlockKind::MirrorRead, 1))
    ));
    assert!(holds(
        &predicates,
        &Trigger::CallerDropped(Select::nth(BlockKind::MirrorRead, 2))
    ));
    assert!(!holds(
        &predicates,
        &Trigger::Completed(Select::nth(BlockKind::MirrorRead, 2))
    ));
    assert!(!holds(
        &predicates,
        &Trigger::Submitted(Select {
            kind: Some(BlockKind::Read),
            ..Select::default()
        })
    ));
}

/// A submission counts as an admission first, and as an apply once
/// admitted, in its owner's session. Each stage takes its own ordinal.
#[test]
fn an_admission_starts_the_verdict_stage() {
    let mut predicates = Predicates::new(Vec::new());
    predicates.record(&submitted(1, 7));
    predicates.record(&submitted(2, 8));
    let apply = |session| {
        Trigger::Submitted(Select {
            kind: Some(BlockKind::Apply),
            session: Some(session),
            ..Select::default()
        })
    };
    assert!(holds(
        &predicates,
        &Trigger::Submitted(Select::nth(BlockKind::Admit, 2))
    ));
    assert!(!holds(&predicates, &apply(8)));
    predicates.record(&StorageFact::Admitted { id: 2 });
    assert!(holds(&predicates, &apply(8)));
    assert!(!holds(&predicates, &apply(7)));
    assert!(holds(
        &predicates,
        &Trigger::Submitted(Select::nth(BlockKind::Apply, 1))
    ));
    let count = |n, kind| {
        Trigger::Count(
            n,
            Box::new(Trigger::Submitted(Select {
                kind: Some(kind),
                ..Select::default()
            })),
        )
    };
    assert!(
        holds(
            &predicates,
            &Trigger::Submitted(Select::nth(BlockKind::Admit, 2))
        ),
        "a later stage leaves the earlier stage's submission as it was"
    );
    assert!(holds(&predicates, &count(2, BlockKind::Admit)));
    assert!(
        !holds(&predicates, &count(2, BlockKind::Apply)),
        "each stage counts once, under its own kind"
    );
}

/// A reply that reached block sync's handle counts as queued, not taken:
/// only a range read's caller takes its reply.
#[test]
fn a_delivered_reply_is_queued_and_a_taken_read_is_taken() {
    let mut predicates = Predicates::new(Vec::new());
    predicates.record(&submitted(1, 7));
    predicates.record(&StorageFact::Admitted { id: 1 });
    predicates.record(&StorageFact::ReplyDelivered { id: 1 });
    let apply = Select::nth(BlockKind::Apply, 1);
    assert!(holds(&predicates, &Trigger::ReplyQueued(apply.clone())));
    assert!(!holds(&predicates, &Trigger::ReplyTaken(apply)));

    predicates.record(&StorageFact::BlockSubmitted {
        id: 2,
        request: StorageRequest::ReadRange {
            start_height: 1,
            count: 4,
            max_body_bytes: 10_000,
        },
        stage: Some("pending".to_owned()),
    });
    predicates.record(&StorageFact::ReadTaken { id: 2 });
    let read = Select::nth(BlockKind::Read, 1);
    assert!(holds(&predicates, &Trigger::ReplyTaken(read.clone())));
    assert!(!holds(&predicates, &Trigger::ReplyQueued(read)));
}

/// A block scenario can't wait for a handled call, or for a taken reply
/// other than a range read's, anywhere in a trigger.
#[test]
fn undriven_triggers_are_the_ones_the_block_node_never_records() {
    let taken = |kind| Trigger::ReplyTaken(Select::nth(kind, 1));
    let handled = Trigger::Handled(Select::nth(BlockKind::Apply, 1));
    for trigger in [
        handled.clone(),
        taken(BlockKind::Query),
        taken(BlockKind::Apply),
        taken(BlockKind::Record),
        Trigger::ReplyTaken(Select::default()),
        Trigger::Count(2, Box::new(taken(BlockKind::Admit))),
        Trigger::All(vec![Trigger::VirtualTime(5), handled]),
    ] {
        assert!(undriven(&trigger).is_some(), "{trigger:?}");
    }
    for trigger in [
        taken(BlockKind::Read),
        taken(BlockKind::MirrorRead),
        Trigger::ReplyQueued(Select::nth(BlockKind::Apply, 1)),
        Trigger::Completed(Select::nth(BlockKind::Query, 1)),
        Trigger::All(vec![Trigger::VirtualTime(5), taken(BlockKind::Read)]),
    ] {
        assert_eq!(undriven(&trigger), None, "{trigger:?}");
    }
}

/// A reset trigger counts only resets, of the actor it names.
#[test]
fn a_reset_trigger_counts_only_resets() {
    let mut predicates = Predicates::new(Vec::new());
    let reset = Trigger::Reset("node".to_owned());
    predicates.record_link(LinkEffect::Grown, "node");
    predicates.record_link(LinkEffect::Reset, "mirror-0");
    assert!(!holds(&predicates, &reset));
    assert!(holds(&predicates, &Trigger::Grown("node".to_owned())));
    predicates.record_link(LinkEffect::Reset, "node");
    assert!(holds(&predicates, &reset));
}

/// Each allowance admits exactly its verdict class, whatever the kind
/// within it.
#[test]
fn allowances_name_verdict_classes() {
    let verdict = BlockOutcome::Verdict;
    assert!(Block::allows(
        BlockAllow::PayloadMismatch,
        verdict(Verdict::PayloadMismatch(Commitment::FinalSaplingRoot))
    ));
    assert!(Block::allows(
        BlockAllow::Retryable,
        verdict(Verdict::Retryable(RetryKind::Canceled))
    ));
    assert!(Block::allows(BlockAllow::Serve, BlockOutcome::Serve));
    assert!(!Block::allows(
        BlockAllow::Committed,
        verdict(Verdict::Ambiguous)
    ));
    assert!(!Block::allows(
        BlockAllow::Committed,
        verdict(Verdict::Duplicate)
    ));
    assert!(!Block::allows(BlockAllow::Admit, BlockOutcome::Apply));
}
