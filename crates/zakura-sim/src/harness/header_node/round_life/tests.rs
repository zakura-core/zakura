//! The reset check's rules, fed by hand.

use std::num::NonZeroU64;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BranchId, Frontier, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId,
    StateVersion, VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{HeaderTargetPhase, RequestRef},
    ZakuraPeerId,
};

use super::*;
use crate::observe::StoragePoint;

/// The anchor every authority here names.
const ANCHOR: block::Hash = block::Hash([1; 32]);

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
}

/// Header authority under `generation`.
fn authority(generation: u64) -> HeaderWorkAuthority {
    anchored(generation, ANCHOR)
}

/// Header authority under `generation` and `anchor`.
fn anchored(generation: u64, anchor: block::Hash) -> HeaderWorkAuthority {
    HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(generation),
        branch: BranchId::new(anchor, block::Hash([3; 32])),
    }
}

/// Ordinary work of session 7 under `generation`.
fn owner(generation: u64) -> HeaderSyncWorkOwner {
    HeaderSyncWorkOwner::Header(authority(generation).bind(7, NonZeroU64::MIN))
}

/// The reactor's adoption of `generation`.
fn adopted(generation: u64) -> ReactorEvent {
    let anchor = Frontier::new(block::Height(0), ANCHOR);
    ReactorEvent::SnapshotAdopted {
        state_version: StateVersion::new(generation),
        header_generation: HeaderGeneration::new(generation),
        finalized: anchor,
        header_best: anchor,
        suffix_work: U256::from(0_u32),
        oldest_retained: block::Height(0),
        verified_generation: VerifiedGeneration::new(1),
        verified_best: anchor,
    }
}

/// The reactor's move into `cause`.
fn cause(cause: ReactorCause) -> ReactorEvent {
    ReactorEvent::CauseSet { cause }
}

/// Locator call `call`, dispatched under `generation`.
fn locator(call: u64, generation: u64) -> ReactorEvent {
    ReactorEvent::Dispatched {
        call,
        op: PortCall::Locator {
            peer: mirror(),
            session_id: 7,
            target_tip_hash: block::Hash([3; 32]),
            scope: authority(generation),
        },
    }
}

/// Round `incarnation`'s ordinary start under `generation`.
fn started(incarnation: u64, generation: u64) -> ReactorEvent {
    ReactorEvent::Started {
        request: RequestRef {
            peer: mirror(),
            session_id: 7,
            request_id: 1,
            incarnation,
            owner: owner(generation),
            target_tip_hash: block::Hash([3; 32]),
        },
        purpose: HeaderTargetPurpose::Normal,
    }
}

/// The queue's slots: the mirror's round `incarnation` in `phase` under
/// `generation`, or its awaiting target with no phase.
fn slots(phase: Option<HeaderTargetPhase>, incarnation: u64, generation: u64) -> ReactorEvent {
    ReactorEvent::Slots {
        slots: vec![WorkSlot {
            peer: mirror(),
            session_id: 7,
            target_tip_hash: block::Hash([3; 32]),
            phase,
            incarnation,
            repair: false,
            authority: authority(generation),
        }],
    }
}

/// The queue's slots once it holds none.
fn empty() -> ReactorEvent {
    ReactorEvent::Slots { slots: Vec::new() }
}

/// An apply for the mirror under `generation`, as call `call`.
fn apply(call: u64, generation: u64) -> ReactorEvent {
    ReactorEvent::Dispatched {
        call,
        op: PortCall::Apply {
            peer: mirror(),
            source: SourceId::from_digest([2; 32]),
            owner: owner(generation),
        },
    }
}

/// Storage's submission of operation `id` for call `call`.
fn submitted(id: u64, call: u64) -> StorageFact {
    StorageFact::Submitted {
        id,
        request: crate::observe::StorageRequest::ContinuationLocator,
        digest: String::new(),
        call: Some(call),
    }
}

/// Storage's locator answer to operation `id`, read under `generation`.
fn read(id: u64, generation: u64) -> StorageFact {
    let point = StoragePoint {
        height: 0,
        hash: String::new(),
    };
    StorageFact::Completed {
        id,
        outcome: "engine".to_owned(),
        result: StorageResult::Locator {
            tip: point.clone(),
            header_generation: generation,
            finalized: point,
        },
        delivered: true,
        generation,
        state: String::new(),
    }
}

/// An operator invalidation that left the generation at `generation`.
fn reset(generation: u64) -> StorageFact {
    StorageFact::Reset {
        kind: "invalidate".to_owned(),
        target: String::new(),
        generation,
    }
}

/// A check on an engine at generation 1, fed `events` from the reactor,
/// with `facts` from storage first, and the breaks its drain then finds.
fn breaks(facts: &[StorageFact], events: &[ReactorEvent]) -> (Vec<String>, RoundLifeEvidence) {
    let evidence = RoundLifeEvidence::default();
    let mut check = RoundLife::new(evidence.clone(), 1);
    check.storage(facts, 1);
    for event in events {
        check.observe(event, true);
    }
    let breaks = check.drain(1).expect("no evidence is missing");
    (breaks, evidence)
}

/// A round started from its own locator read, then retired in the unit
/// that adopts a new generation, holds, and the check applies.
#[test]
fn a_round_retired_at_adoption_holds() {
    let (found, evidence) = breaks(
        &[submitted(1, 1), read(1, 1)],
        &[
            adopted(1),
            locator(1, 1),
            cause(ReactorCause::Completion { call: 1 }),
            started(1, 1),
            slots(Some(HeaderTargetPhase::Receiving), 1, 1),
            cause(ReactorCause::Idle),
            cause(ReactorCause::Watch),
            adopted(2),
            empty(),
            cause(ReactorCause::Idle),
        ],
    );
    assert_eq!(found, Vec::<String>::new());
    assert!(evidence.applicable_by(1));
}

/// The events of [`a_round_retired_at_adoption_holds`] up to the new
/// adoption, with the round still in the queue under `owner` generation.
fn kept(owner: u64) -> Vec<ReactorEvent> {
    vec![
        adopted(1),
        locator(1, 1),
        cause(ReactorCause::Completion { call: 1 }),
        started(1, 1),
        slots(Some(HeaderTargetPhase::Receiving), 1, 1),
        cause(ReactorCause::Idle),
        cause(ReactorCause::Watch),
        adopted(2),
        slots(Some(HeaderTargetPhase::Receiving), 1, owner),
        cause(ReactorCause::Idle),
    ]
}

/// A round kept past the unit that adopted a new generation breaks the
/// rule once, whether its owner kept the old generation or was rewritten
/// to the new one, since the check reads the generation its round started
/// under.
#[test]
fn a_round_kept_past_adoption_breaks_once() {
    for owner in [1, 2] {
        let mut events = kept(owner);
        events.push(cause(ReactorCause::Pass));
        let (found, _) = breaks(&[submitted(1, 1), read(1, 1)], &events);
        assert_eq!(found.len(), 1, "{owner}: {found:?}");
        assert!(found[0].contains("still in the queue"), "{found:?}");
    }
}

/// An awaiting slot that keeps an older generation past an adoption
/// breaks the rule.
#[test]
fn an_awaiting_slot_kept_past_adoption_breaks() {
    let (found, _) = breaks(
        &[],
        &[
            adopted(1),
            slots(None, 0, 1),
            cause(ReactorCause::Idle),
            adopted(2),
            cause(ReactorCause::Idle),
        ],
    );
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("awaits its locator"), "{found:?}");
}

/// A live round's apply dispatched under the old generation after an
/// adoption breaks the dispatch rule, before the unit ends.
#[test]
fn an_old_generation_dispatch_breaks() {
    let mut events = kept(1);
    events.pop();
    events.push(apply(2, 1));
    let (found, _) = breaks(&[submitted(1, 1), read(1, 1)], &events);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("dispatched round 1's apply"), "{found:?}");
}

/// A start outside a locator completion, from a locator dispatched under
/// another generation, or from a read older than the adoption breaks the
/// locator rule.
#[test]
fn a_start_off_its_locator_breaks() {
    let off_cause = breaks(&[], &[adopted(1), cause(ReactorCause::Pass), started(1, 1)]).0;
    assert!(
        off_cause[0].contains("not a locator call's completion"),
        "{off_cause:?}"
    );
    let wrong_call = breaks(
        &[],
        &[
            adopted(1),
            cause(ReactorCause::Completion { call: 4 }),
            started(1, 1),
        ],
    )
    .0;
    assert!(
        wrong_call[0].contains("isn't a locator read"),
        "{wrong_call:?}"
    );
    let other_generation = breaks(
        &[submitted(1, 1), read(1, 2)],
        &[
            adopted(1),
            locator(1, 1),
            adopted(2),
            cause(ReactorCause::Completion { call: 1 }),
            started(1, 2),
        ],
    )
    .0;
    assert!(
        other_generation[0].contains("dispatched under 1"),
        "{other_generation:?}"
    );
    let old_read = breaks(
        &[submitted(1, 1), read(1, 1)],
        &[
            adopted(1),
            locator(1, 1),
            adopted(2),
            cause(ReactorCause::Completion { call: 1 }),
            started(1, 1),
        ],
    )
    .0;
    assert!(
        old_read[0].contains("older than the adopted"),
        "{old_read:?}"
    );
}

/// A locator start whose read storage never reported is missing evidence.
#[test]
fn a_start_from_an_unreported_read_is_missing_evidence() {
    let mut check = RoundLife::new(RoundLifeEvidence::default(), 1);
    for event in [
        adopted(1),
        locator(1, 1),
        cause(ReactorCause::Completion { call: 1 }),
        started(1, 1),
    ] {
        check.observe(&event, true);
    }
    assert!(check
        .drain(1)
        .expect_err("the read is missing")
        .starts_with("RoundLifeJoin"));
}

/// A reset that leaves the generation where the engine had it breaks the
/// generation rule, and one that advances it doesn't. Either applies.
#[test]
fn a_reset_must_advance_the_generation() {
    let (found, evidence) = breaks(&[reset(1)], &[]);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].contains("left the header generation at 1"),
        "{found:?}"
    );
    assert!(evidence.applicable_by(1));
    let (found, evidence) = breaks(&[reset(2)], &[]);
    assert_eq!(found, Vec::<String>::new());
    assert!(evidence.applicable_by(1));
}

/// A run with no start, reset or generation change under a live round
/// doesn't apply, and facts from another task are ignored.
#[test]
fn a_quiet_run_does_not_apply() {
    let evidence = RoundLifeEvidence::default();
    let mut check = RoundLife::new(evidence.clone(), 1);
    check.observe(&adopted(1), true);
    check.observe(&started(1, 1), false);
    check.observe(&adopted(2), true);
    assert_eq!(check.drain(1), Ok(Vec::new()));
    assert!(!evidence.any());
}

/// An owner rewritten to an older generation under another anchor isn't
/// excused as a rebase: the exception follows the round's start, whose
/// anchor never moved. Both the slot and a dispatch under that owner break.
#[test]
fn a_rewritten_anchor_does_not_hide_an_old_generation() {
    let other = block::Hash([9; 32]);
    let mut events = vec![
        adopted(1),
        locator(1, 1),
        cause(ReactorCause::Completion { call: 1 }),
        started(1, 1),
        ReactorEvent::Slots {
            slots: vec![WorkSlot {
                peer: mirror(),
                session_id: 7,
                target_tip_hash: block::Hash([3; 32]),
                phase: Some(HeaderTargetPhase::Receiving),
                incarnation: 1,
                repair: false,
                authority: anchored(0, other),
            }],
        },
    ];
    events.push(ReactorEvent::Dispatched {
        call: 2,
        op: PortCall::Apply {
            peer: mirror(),
            source: SourceId::from_digest([2; 32]),
            owner: HeaderSyncWorkOwner::Header(anchored(0, other).bind(7, NonZeroU64::MIN)),
        },
    });
    events.push(cause(ReactorCause::Idle));
    let (found, _) = breaks(&[submitted(1, 1), read(1, 1)], &events);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].contains("dispatched round 1's apply"), "{found:?}");
    assert!(found[1].contains("owner holds generation 0"), "{found:?}");
}
