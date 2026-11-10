//! The publication check's rules, fed by hand.

use zakura_node_services::header_chain::Port as _;
use zakura_storage::header::{
    adapter::EnginePort,
    controlled::{EngineRun, OpId, RunKind},
    evidence::ResultIdentity,
    fixture::HeaderFixture,
};

use super::*;
use crate::harness::header_node::ready;

/// An engine at genesis, its fixture and a check seeded with it.
fn seeded() -> (HeaderFixture, EnginePort, Publication, PublicationEvidence) {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let evidence = PublicationEvidence::default();
    let check = Publication::new(evidence.clone(), port.snapshot());
    (fixture, port, check, evidence)
}

/// Applies `len` main-line headers past `port`'s tip, returning the run
/// that recorded it.
fn commit(fixture: &HeaderFixture, port: &EnginePort, len: u32) -> EngineRun {
    let before = port.durable_state();
    let snapshot = port.snapshot();
    let tip = snapshot.frontiers.header_best;
    let headers = fixture.extend(tip, len, 0);
    let request = fixture.target_request(&snapshot, tip, &headers, 1);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    ready(port.apply_header_target(prepared)).expect("fixture headers apply");
    EngineRun {
        kind: RunKind::Apply(OpId(1), ResultIdentity::Applied),
        clock: zakura_rt::time::utc_now().into(),
        before,
        after: port.durable_state(),
        snapshot: port.snapshot(),
    }
}

/// The `SnapshotAdopted` event that adopts `snapshot`.
fn adopted(snapshot: &EngineSnapshot) -> ReactorEvent {
    ReactorEvent::SnapshotAdopted {
        state_version: snapshot.state_version,
        header_generation: snapshot.header_generation,
        finalized: snapshot.frontiers.finalized,
        header_best: snapshot.frontiers.header_best,
        suffix_work: snapshot.header_best_score.suffix_work.as_u256(),
        oldest_retained: snapshot.oldest_retained_height,
        verified_generation: snapshot.verified_generation,
        verified_best: snapshot.frontiers.verified_best,
    }
}

/// The `TipPublished` event for `snapshot`'s tip.
fn tip_published(snapshot: &EngineSnapshot, delivered: bool) -> ReactorEvent {
    ReactorEvent::TipPublished {
        height: snapshot.frontiers.header_best.height,
        hash: snapshot.frontiers.header_best.hash,
        delivered,
    }
}

/// A commit published in its observation, then adopted and its tip
/// published, holds, and the check applies.
#[test]
fn a_faithful_history_holds() {
    let (fixture, port, mut check, evidence) = seeded();
    let seed = port.snapshot();
    check.observe(&adopted(&seed), true);
    check.observe(&tip_published(&seed, true), true);
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&run.snapshot), true);
    check.observe(&tip_published(&run.snapshot, true), true);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(evidence.applicable_by(1));
    assert_eq!(evidence.unpublished_at_settled_end(), None);
}

/// A committed state the port didn't publish in its observation breaks
/// the publication rule, and so does a publication no record holds.
#[test]
fn a_skipped_or_foreign_publication_breaks() {
    let (fixture, port, mut check, _) = seeded();
    let run = commit(&fixture, &port, 2);
    port.take_publications();
    check.run(&run, false);
    check.publications(&[]);
    let breaks = check.drain(1);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("last publication"), "{breaks:?}");

    let mut foreign = run.snapshot.clone();
    foreign.frontiers.header_best = fixture.anchor();
    check.publications(&[foreign]);
    let breaks = check.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("differs from every engine record"),
        "{breaks:?}"
    );
}

/// An adoption that matches no engine record, by version or by its
/// fields, breaks the adoption rule.
#[test]
fn an_unfaithful_adoption_breaks() {
    let (fixture, port, mut check, _) = seeded();
    let seed = port.snapshot();
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    // The new generation with the old tip: a mixed adoption.
    let mut mixed = run.snapshot.clone();
    mixed.frontiers.header_best = seed.frontiers.header_best;
    mixed.header_best_score = seed.header_best_score;
    check.observe(&adopted(&mixed), true);
    let breaks = check.drain(1);
    assert!(
        breaks
            .iter()
            .any(|detail| detail.contains("[\"tip\", \"work\"]")),
        "{breaks:?}"
    );
    let mut unknown = run.snapshot;
    unknown.state_version = zakura_header_chain::StateVersion::new(999);
    check.observe(&adopted(&unknown), true);
    let breaks = check.drain(2);
    assert!(
        breaks[0].contains("which no engine record holds"),
        "{breaks:?}"
    );
}

/// The tip rules: a lost or stale tip publication breaks, and a tip still
/// owed at the next adoption, or at a settled end, does too.
#[test]
fn the_tip_rules() {
    let (fixture, port, mut check, evidence) = seeded();
    let seed = port.snapshot();
    check.observe(&adopted(&seed), true);
    assert_eq!(evidence.owed(), Some(seed.frontiers.header_best));
    check.observe(&tip_published(&seed, false), true);
    assert_eq!(evidence.owed(), None);
    assert!(check.drain(1)[0].starts_with("TipLost"));

    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&run.snapshot), true);
    check.observe(&tip_published(&seed, true), true);
    assert!(check.drain(2)[0].starts_with("StaleTip"));

    let next = commit(&fixture, &port, 1);
    check.run(&next, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&next.snapshot), true);
    let later = commit(&fixture, &port, 1);
    check.run(&later, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&later.snapshot), true);
    assert!(check.drain(3)[0].starts_with("UnpublishedTip"));
    evidence.end(false);
    assert_eq!(evidence.unpublished_at_settled_end(), None);
    evidence.end(true);
    assert_eq!(
        evidence.unpublished_at_settled_end(),
        Some(later.snapshot.frontiers.header_best)
    );
}

/// The setup observation's drain, step 0, which no property checks, keeps
/// its breaks for step 1's.
#[test]
fn startup_breaks_wait_for_step_one() {
    let (_, port, mut check, evidence) = seeded();
    let mut wrong = port.snapshot();
    wrong.state_version = zakura_header_chain::StateVersion::new(999);
    check.observe(&adopted(&wrong), true);
    assert_eq!(check.drain(0), Vec::<String>::new());
    assert!(!evidence.any());
    assert!(check.drain(1)[0].contains("which no engine record holds"));
    assert!(evidence.applicable_by(1));
}

/// Events another task reported are left alone.
#[test]
fn off_reactor_events_are_not_judged() {
    let (_, port, mut check, evidence) = seeded();
    let mut wrong = port.snapshot();
    wrong.state_version = zakura_header_chain::StateVersion::new(999);
    check.observe(&adopted(&wrong), false);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(!evidence.any());
}

/// A run the commit-contents check found corrupt taints the check: what it
/// committed and every later fact go unjudged, and a settled end owes
/// nothing.
#[test]
fn a_corrupt_run_taints_the_check() {
    let (fixture, port, mut check, evidence) = seeded();
    let run = commit(&fixture, &port, 2);
    port.take_publications();
    check.run(&run, true);
    check.publications(&[]);
    check.observe(&adopted(&run.snapshot), true);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(evidence.tainted_by(1));
    assert!(!evidence.any());
    evidence.end(true);
    assert_eq!(evidence.unpublished_at_settled_end(), None);
}
