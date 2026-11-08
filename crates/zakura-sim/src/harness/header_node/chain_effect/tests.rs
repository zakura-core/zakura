//! The stale-chain check's rules, fed by hand.

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{Frontier, HeaderGeneration, StateVersion, VerifiedGeneration};

use super::*;
use crate::observe::{StorageBranch, StorageResult, WorkOwner};

/// The engine at `generation` with state digest `state` and anchor `a`.
fn engine(generation: u64, state: &str) -> EngineAt {
    EngineAt {
        generation,
        anchor: "a".to_owned(),
        state: state.to_owned(),
    }
}

/// Apply operation `id`'s submission, under `generation` and `anchor`.
fn apply(id: u64, generation: u64, anchor: &str) -> StorageFact {
    StorageFact::Submitted {
        id,
        request: StorageRequest::ApplyHeaderTarget {
            owner: WorkOwner {
                kind: "header".to_owned(),
                scope: WorkScope {
                    header_generation: generation,
                    branch: StorageBranch {
                        anchor: anchor.to_owned(),
                        target: "t".to_owned(),
                    },
                },
                verified_generation: None,
                body_work_epoch: None,
                session_id: 1,
                request_id: 1,
            },
            source: String::new(),
            parent: None,
            target: "t".to_owned(),
        },
        digest: String::new(),
        call: Some(1),
    }
}

/// Operation `id`'s completion, leaving the engine's digest at `state`.
fn completed(id: u64, state: &str) -> StorageFact {
    StorageFact::Completed {
        id,
        outcome: "engine".to_owned(),
        result: StorageResult::Rejected,
        delivered: true,
        generation: 0,
        state: state.to_owned(),
    }
}

/// Operation `id`'s commit with its reply held, leaving `state`.
fn committed(id: u64, state: &str) -> StorageFact {
    StorageFact::Committed {
        id,
        generation: 0,
        state: state.to_owned(),
    }
}

/// The reactor's adoption of `version` at `generation`.
fn adopted(version: u64, generation: u64) -> ReactorEvent {
    let anchor = Frontier::new(block::Height(0), block::Hash([1; 32]));
    ReactorEvent::SnapshotAdopted {
        state_version: StateVersion::new(version),
        header_generation: HeaderGeneration::new(generation),
        finalized: anchor,
        header_best: anchor,
        suffix_work: U256::from(0_u32),
        oldest_retained: block::Height(0),
        verified_generation: VerifiedGeneration::new(1),
        verified_best: anchor,
    }
}

/// A check on an engine at generation 2 with digest `s0`, fed each step's
/// facts with the engine they leave, then `events`, and the breaks its
/// drain then finds.
fn breaks(
    steps: Vec<(Vec<StorageFact>, EngineAt)>,
    events: &[ReactorEvent],
) -> (Vec<String>, ChainEffectEvidence) {
    let evidence = ChainEffectEvidence::default();
    let mut check = ChainEffect::new(evidence.clone(), engine(2, "s0"));
    for (facts, after) in steps {
        check.storage(&facts, after);
    }
    for event in events {
        check.observe(event, true);
    }
    let found = check.drain(1).expect("no evidence is missing");
    (found, evidence)
}

/// An apply under the engine's generation and anchor that changes it
/// holds and applies, and one under an older generation or another anchor
/// breaks, whether it runs straight through or commits with its reply
/// held. Its later delivery isn't judged again.
#[test]
fn an_apply_must_change_the_engine_under_its_own_authority() {
    let (found, evidence) = breaks(
        vec![
            (vec![apply(1, 2, "a")], engine(2, "s0")),
            (vec![completed(1, "s1")], engine(3, "s1")),
        ],
        &[],
    );
    assert_eq!(found, Vec::<String>::new());
    assert!(evidence.applicable_by(1));
    for (generation, anchor) in [(1, "a"), (2, "b")] {
        let (found, _) = breaks(
            vec![
                (vec![apply(1, generation, anchor)], engine(2, "s0")),
                (vec![committed(1, "s1")], engine(3, "s1")),
                (vec![completed(1, "s1")], engine(3, "s1")),
            ],
            &[],
        );
        assert_eq!(found.len(), 1, "{generation} {anchor}: {found:?}");
        assert!(
            found[0].starts_with("StaleChainEffect: apply operation 1"),
            "{found:?}"
        );
    }
}

/// A stale apply that changes nothing, as an engine refusal does, is
/// neither judged nor makes the check apply.
#[test]
fn an_apply_that_changes_nothing_is_not_judged() {
    let (found, evidence) = breaks(
        vec![
            (vec![apply(1, 1, "a")], engine(2, "s0")),
            (vec![completed(1, "s0")], engine(2, "s0")),
        ],
        &[],
    );
    assert_eq!(found, Vec::<String>::new());
    assert!(!evidence.any());
}

/// Adoptions that never move back hold and apply from the second, and one
/// that moves back in state version or generation breaks.
#[test]
fn adoption_never_moves_back() {
    let (found, evidence) = breaks(Vec::new(), &[adopted(1, 1), adopted(2, 2)]);
    assert_eq!(found, Vec::<String>::new());
    assert!(evidence.applicable_by(1));
    for back in [adopted(1, 2), adopted(2, 1)] {
        let (found, _) = breaks(Vec::new(), &[adopted(2, 2), back]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("after version 2"), "{found:?}");
    }
}

/// An apply execution, held or run straight through, with no reported
/// submission is missing evidence.
#[test]
fn an_unsubmitted_apply_is_missing_evidence() {
    for fact in [committed(4, "s1"), completed(4, "s1")] {
        let mut check = ChainEffect::new(ChainEffectEvidence::default(), engine(2, "s0"));
        check.storage(std::slice::from_ref(&fact), engine(3, "s1"));
        assert!(
            check
                .drain(1)
                .expect_err("the submission is missing")
                .starts_with("ChainEffectJoin"),
            "{fact:?}"
        );
    }
}
