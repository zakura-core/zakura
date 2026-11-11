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

/// The `SnapshotStored` event that reports `snapshot` as stored.
fn stored(snapshot: &EngineSnapshot) -> ReactorEvent {
    ReactorEvent::SnapshotStored {
        snapshot: Some(snapshot.clone()),
    }
}

/// Feeds `check` the reactor's adoption of `snapshot` and its faithful
/// store.
fn adopt(check: &mut Publication, snapshot: &EngineSnapshot) {
    check.observe(&adopted(snapshot), true);
    check.observe(&stored(snapshot), true);
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
    adopt(&mut check, &seed);
    check.observe(&tip_published(&seed, true), true);
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &run.snapshot);
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
    check.observe(&stored(&mixed), true);
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
    adopt(&mut check, &seed);
    assert_eq!(evidence.owed(), Some(seed.frontiers.header_best));
    check.observe(&tip_published(&seed, false), true);
    assert_eq!(evidence.owed(), None);
    assert!(check.drain(1)[0].starts_with("TipLost"));

    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &run.snapshot);
    check.observe(&tip_published(&seed, true), true);
    assert!(check.drain(2)[0].starts_with("StaleTip"));

    let next = commit(&fixture, &port, 1);
    check.run(&next, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &next.snapshot);
    let later = commit(&fixture, &port, 1);
    check.run(&later, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &later.snapshot);
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
    adopt(&mut check, &run.snapshot);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(evidence.tainted_by(1));
    assert!(!evidence.any());
    evidence.end(true);
    assert_eq!(evidence.unpublished_at_settled_end(), None);
}

/// The test mirror's peer ID.
fn mirror() -> zakura_network::zakura::ZakuraPeerId {
    zakura_network::zakura::ZakuraPeerId::new(vec![9; 32]).expect("32 bytes is a valid peer ID")
}

/// The mirror's admission as session 1.
fn admitted_one() -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer: mirror(),
        session_id: 1,
        direction: zakura_network::zakura::ServicePeerDirection::Inbound,
        replaced: None,
        origin: None,
    }
}

/// The `Status` advertising `snapshot`, queued for session 1.
fn status_queued(snapshot: &EngineSnapshot) -> ReactorEvent {
    ReactorEvent::StatusQueued {
        peer: mirror(),
        session_id: 1,
        status: zakura_network::zakura::Status {
            work_anchor_height: snapshot.frontiers.finalized.height,
            work_anchor_hash: snapshot.frontiers.finalized.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: snapshot.header_best_score.suffix_work.as_u256(),
            oldest_retained_height: snapshot.oldest_retained_height,
            max_headers_per_response: 1,
            max_subscriptions: 1,
            max_message_bytes: 1,
            tree_aux_schema_mask: 1,
        },
    }
}

/// A check whose evidence `arming` arms, seeded with a fresh engine.
fn armed(arming: Arming) -> (HeaderFixture, EnginePort, Publication, PublicationEvidence) {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let evidence = PublicationEvidence::armed(arming);
    let check = Publication::new(evidence.clone(), port.snapshot());
    (fixture, port, check, evidence)
}

/// The kinds of `evidence`'s facts, in stamp order.
fn kinds(evidence: &PublicationEvidence) -> Vec<&'static str> {
    evidence
        .facts()
        .into_iter()
        .map(|(_, fact)| match fact {
            PublicationFact::Committed { .. } => "committed",
            PublicationFact::Adopted { .. } => "adopted",
            PublicationFact::StatusOwed { .. } => "owed",
            PublicationFact::StatusPaid { .. } => "paid",
            PublicationFact::Tainted => "tainted",
        })
        .collect()
}

/// In a status fixture, a session is owed its first `Status` at admission
/// and paid by one that advertises the adoption. A commit's adoption that
/// changes what it advertises owes another, a second change while it's owed
/// opens none, and a `Status` advertising the newest pays it.
#[test]
fn status_debts_open_at_admission_and_at_a_change() {
    let (fixture, port, mut check, evidence) = armed(Arming {
        armed: true,
        status: true,
    });
    let seed = port.snapshot();
    adopt(&mut check, &seed);
    check.observe(&tip_published(&seed, true), true);
    check.observe(&admitted_one(), true);
    check.observe(&status_queued(&seed), true);
    for len in [2, 1] {
        let run = commit(&fixture, &port, len);
        check.run(&run, false);
        check.publications(&port.take_publications());
        adopt(&mut check, &run.snapshot);
        check.observe(&tip_published(&run.snapshot, true), true);
    }
    check.observe(&status_queued(&port.snapshot()), true);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert_eq!(
        kinds(&evidence),
        [
            "adopted",
            "owed",
            "paid",
            "committed",
            "adopted",
            "owed",
            "committed",
            "adopted",
            "paid"
        ]
    );
    assert_eq!(evidence.owed_peer(1), Some(mirror()));
    assert_eq!(
        evidence.adopted_before(u64::MAX),
        Some(port.snapshot().state_version.get())
    );
}

/// A refused queue attempt (`StatusDeferred`) neither pays a session's
/// debt nor opens another, so the deadline stays the one admission opened,
/// and the `Status` its retry queues pays it.
#[test]
fn a_deferral_keeps_the_debt_its_retry_pays() {
    let (_, port, mut check, evidence) = armed(Arming {
        armed: true,
        status: true,
    });
    let seed = port.snapshot();
    check.observe(&adopted(&seed), true);
    check.observe(&tip_published(&seed, true), true);
    check.observe(&admitted_one(), true);
    let owed = evidence.facts();
    check.observe(
        &ReactorEvent::StatusDeferred {
            peer: mirror(),
            session_id: 1,
        },
        true,
    );
    assert_eq!(evidence.facts(), owed);
    check.observe(&status_queued(&seed), true);
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert_eq!(kinds(&evidence), ["adopted", "owed", "paid"]);
}

/// Outside a status fixture the evidence keeps commits and adoptions only,
/// and unarmed it keeps nothing.
#[test]
fn only_a_status_fixture_keeps_status_debts() {
    for (arming, expected) in [
        (
            Arming {
                armed: true,
                status: false,
            },
            vec!["adopted"],
        ),
        (Arming::default(), Vec::new()),
    ] {
        let (_, port, mut check, evidence) = armed(arming);
        let seed = port.snapshot();
        adopt(&mut check, &seed);
        check.observe(&admitted_one(), true);
        check.observe(&status_queued(&seed), true);
        assert_eq!(kinds(&evidence), expected, "{arming:?}");
    }
}

/// A queued `Status` that doesn't advertise the latest adoption breaks with
/// `StaleStatus`, and pays no debt.
#[test]
fn a_stale_status_breaks_and_pays_nothing() {
    let (fixture, port, mut check, evidence) = armed(Arming {
        armed: true,
        status: true,
    });
    let seed = port.snapshot();
    check.observe(&admitted_one(), true);
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &run.snapshot);
    check.observe(&status_queued(&seed), true);
    let breaks = check.drain(1);
    assert!(breaks[0].starts_with("StaleStatus"), "{breaks:?}");
    assert_eq!(kinds(&evidence), ["owed", "committed", "adopted"]);
}

/// A corrupt run keeps a `Tainted` fact, which answers every open
/// obligation, and nothing after it.
#[test]
fn a_taint_is_kept_and_ends_the_facts() {
    let (fixture, port, mut check, evidence) = armed(Arming {
        armed: true,
        status: true,
    });
    let run = commit(&fixture, &port, 2);
    check.run(&run, true);
    adopt(&mut check, &run.snapshot);
    check.observe(&admitted_one(), true);
    assert_eq!(kinds(&evidence), ["tainted"]);
}

/// A store that differs from the adoption it follows breaks, and so does an
/// adoption still unstored when its unit of work ends or the next adoption
/// comes.
#[test]
fn the_store_must_equal_its_adoption() {
    let (fixture, port, mut check, _) = seeded();
    let seed = port.snapshot();
    adopt(&mut check, &seed);
    check.observe(&tip_published(&seed, true), true);
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&run.snapshot), true);
    check.observe(&tip_published(&run.snapshot, true), true);
    // The new state stored with the old tip.
    let mut kept = run.snapshot.clone();
    kept.frontiers.header_best = seed.frontiers.header_best;
    check.observe(&stored(&kept), true);
    let breaks = check.drain(1);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("stored"), "{breaks:?}");

    let next = commit(&fixture, &port, 1);
    check.run(&next, false);
    check.publications(&port.take_publications());
    check.observe(&adopted(&next.snapshot), true);
    check.observe(&tip_published(&next.snapshot, true), true);
    check.observe(
        &ReactorEvent::Sizes {
            sizes: Default::default(),
        },
        true,
    );
    let breaks = check.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("hadn't stored it when its unit"),
        "{breaks:?}"
    );
}

/// A store that left nothing breaks, and so does one whose copy differs
/// from the engine's record in a field the adoption doesn't carry, such as
/// its mode.
#[test]
fn a_store_must_be_the_whole_record() {
    let (_, port, mut check, _) = seeded();
    let seed = port.snapshot();
    check.observe(&adopted(&seed), true);
    check.observe(&tip_published(&seed, true), true);
    check.observe(&ReactorEvent::SnapshotStored { snapshot: None }, true);
    let breaks = check.drain(1);
    assert!(
        breaks[0].contains("stored no committed snapshot"),
        "{breaks:?}"
    );
    let mut moded = seed.clone();
    moded.mode = match seed.mode {
        zakura_header_chain::EngineMode::HeadersOnly => zakura_header_chain::EngineMode::Integrated,
        _ => zakura_header_chain::EngineMode::HeadersOnly,
    };
    check.observe(&adopted(&seed), true);
    check.observe(&stored(&moded), true);
    let breaks = check.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(breaks[0].contains("whose engine record is"), "{breaks:?}");
}

/// The node's peer ID, as a mirror's reactor names it.
fn node() -> zakura_network::zakura::ZakuraPeerId {
    zakura_network::zakura::ZakuraPeerId::new(vec![1; 32]).expect("32 bytes is a valid peer ID")
}

/// A mirror's session `session_id` with `peer` ending for `reason`.
fn ended(
    peer: zakura_network::zakura::ZakuraPeerId,
    session_id: u64,
    reason: &'static str,
) -> ReactorEvent {
    ReactorEvent::SessionEnded {
        peer,
        session_id,
        reason,
    }
}

/// A mirror that ends its session with the node on a protocol reject
/// breaks the check at the next drain, and makes it applicable. Another
/// reason, or a session with another peer, is left alone.
#[test]
fn a_mirror_refusal_breaks() {
    let (_, _, mut check, evidence) = seeded();
    let observer = evidence.mirror("mirror-0", node());
    observer.observe(ended(node(), 3, "remote_closed"));
    observer.observe(ended(mirror(), 4, "service_protocol_reject"));
    assert_eq!(check.drain(1), Vec::<String>::new());
    assert!(!evidence.any());
    observer.observe(ended(node(), 5, "service_protocol_reject"));
    let breaks = check.drain(2);
    assert_eq!(breaks.len(), 1, "{breaks:?}");
    assert!(
        breaks[0].contains("mirror-0 ended session 5 with the node"),
        "{breaks:?}"
    );
    assert!(evidence.applicable_by(2));
    assert_eq!(check.drain(3), Vec::<String>::new());
}
