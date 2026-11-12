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
    let evidence = PublicationEvidence::new(&EvidenceStores::default());
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
    let evidence = PublicationEvidence::armed(&EvidenceStores::default(), arming);
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

/// One adoption that changes what every paid session was advertised opens
/// a debt for each, stamped in session order, so the facts and their
/// stamps don't depend on how the check holds its sessions.
#[test]
fn an_adoption_stamps_its_debts_in_session_order() {
    let (fixture, port, mut check, evidence) = armed(Arming {
        armed: true,
        status: true,
    });
    let seed = port.snapshot();
    adopt(&mut check, &seed);
    check.observe(&tip_published(&seed, true), true);
    let sessions = [7, 2, 9, 4, 1, 12, 5, 3];
    for session_id in sessions {
        check.observe(
            &ReactorEvent::SessionAdmitted {
                peer: mirror(),
                session_id,
                direction: zakura_network::zakura::ServicePeerDirection::Inbound,
                replaced: None,
                origin: None,
            },
            true,
        );
        let ReactorEvent::StatusQueued { peer, status, .. } = status_queued(&seed) else {
            unreachable!("`status_queued` queues a status");
        };
        check.observe(
            &ReactorEvent::StatusQueued {
                peer,
                session_id,
                status,
            },
            true,
        );
    }
    let paid = evidence.facts().len();
    let run = commit(&fixture, &port, 2);
    check.run(&run, false);
    check.publications(&port.take_publications());
    adopt(&mut check, &run.snapshot);
    let owed: Vec<(u64, u64)> = evidence.facts()[paid..]
        .iter()
        .filter_map(|(stamp, fact)| match fact {
            PublicationFact::StatusOwed { session_id, .. } => Some((*stamp, *session_id)),
            _ => None,
        })
        .collect();
    let mut ordered = sessions;
    ordered.sort_unstable();
    assert_eq!(
        owed.iter().map(|(_, session)| *session).collect::<Vec<_>>(),
        ordered,
        "{owed:?}"
    );
    assert!(
        owed.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "{owed:?}"
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

/// A publication fact kept after the node's stores froze is dropped.
#[test]
fn a_frozen_publication_store_drops_a_fact() {
    crate::stores::assert_freeze_drops_writes(
        |stores| {
            PublicationEvidence::armed(
                stores,
                Arming {
                    armed: true,
                    status: false,
                },
            )
        },
        |evidence| {
            evidence.keep(PublicationFact::Committed {
                state_version: 1,
                at: SimInstant::ZERO,
            });
        },
        PublicationEvidence::facts,
    );
}

/// Armed publication evidence and its node's claims, with no tap behind
/// them.
#[derive(Clone)]
struct Boundary {
    claims: super::super::claim::ClaimEvidence,
    publication: PublicationEvidence,
    ends: super::super::ends::EndEvidence,
}

impl Default for Boundary {
    /// Empty stores in one handle of their own, the publication store
    /// armed outside a status fixture.
    fn default() -> Self {
        let stores = EvidenceStores::default();
        Boundary {
            claims: super::super::claim::ClaimEvidence::new(&stores),
            publication: PublicationEvidence::armed(
                &stores,
                Arming {
                    armed: true,
                    status: false,
                },
            ),
            ends: super::super::ends::EndEvidence::new(&stores),
        }
    }
}

impl crate::domain::header_sync::PublicationView for Boundary {
    fn claims(&self) -> &super::super::claim::ClaimEvidence {
        &self.claims
    }

    fn publication(&self) -> &PublicationEvidence {
        &self.publication
    }

    fn ends(&self) -> &super::super::ends::EndEvidence {
        &self.ends
    }
}

impl crate::property::HasView for Boundary {
    type View = Boundary;

    /// A copy sharing the stores.
    fn view(&self) -> Boundary {
        self.clone()
    }

    /// The publication store's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        Some(
            self.publication
                .facts()
                .into_iter()
                .map(|(stamp, _)| stamp)
                .filter(|stamp| after.is_none_or(|after| *stamp > after))
                .collect(),
        )
    }
}

/// The claim break [`Boundary`]'s drain finds beside its commit: the
/// commit's skipped publication.
const SKIPPED: &str = "StatePublicationConsistent: the engine committed state version 2 and \
                       the port's last publication in that step was None, not it";

impl crate::harness::scripted::Script for Boundary {
    const NAME: &'static str = "test/publication-break-beside-a-commit";
    /// Whether step 1's drain also found the commit's publication skipped.
    type Case = bool;

    /// The publication check.
    fn properties() -> Vec<crate::property::Property<crate::harness::scripted::ScriptRun<Boundary>>>
    {
        vec![crate::domain::header_sync::state_publication_consistent()]
    }

    /// The schedule and the view.
    fn sources() -> crate::property::Sources {
        use crate::property::Source;
        crate::property::Sources::observed(&[Source::Schedule, Source::View])
    }

    /// At step 1: a commit of state version 2, which nothing adopts, and,
    /// for a break, its skipped publication, as the tap's drain keeps
    /// them.
    fn observe(&self, broke: &bool, log: &mut crate::ObservationLog) {
        let step = log.step();
        if step != 1 {
            return;
        }
        self.publication.keep(PublicationFact::Committed {
            state_version: 2,
            at: now(),
        });
        self.publication.change(|store| {
            store.applicable.get_or_insert(step);
        });
        if *broke {
            self.claims.record(super::super::claim::ClaimRecord {
                step,
                claim: super::super::claim::Claim::StatePublicationConsistent,
                detail: SKIPPED.to_owned(),
                attribution: None,
            });
        }
    }
}

/// Sol P2: a claim break at the boundary that commits state version 2
/// still registers the commit's adoption obligation, so a run that never
/// adopts it reports both failures, in the full report recorded beside the
/// trace and its fresh replay. Without the break, the obligation's failure
/// is the same one.
#[test]
fn a_claim_break_beside_an_unadopted_commit_reports_both() {
    use crate::{
        domain::header_sync::STATE_PUBLICATION_CONSISTENT,
        harness::scripted::{findings, sidecar},
    };
    let unadopted = crate::Violation {
        property: STATE_PUBLICATION_CONSISTENT.to_owned(),
        step: 1,
        witness: vec![1],
        expected: "Owner(\"adoption-of-state-version-2\") discharged before quiescence".to_owned(),
        actual: "the scheduler is Quiescent after step 1 with the obligation from step 1 open"
            .to_owned(),
    };
    let skipped = crate::Violation {
        property: STATE_PUBLICATION_CONSISTENT.to_owned(),
        step: 1,
        witness: vec![1],
        expected: "committed states are published and adopted as recorded, and advertised as \
                   adopted"
            .to_owned(),
        actual: SKIPPED.to_owned(),
    };
    for (broke, expected) in [
        (true, vec![skipped.clone(), unadopted.clone()]),
        (false, vec![unadopted.clone()]),
    ] {
        let full = sidecar::<Boundary>(&broke, &[]);
        let found = findings(&full, STATE_PUBLICATION_CONSISTENT);
        assert_eq!(found.violations, expected, "broke: {broke}");
        assert_eq!(
            found.outcome,
            crate::PropertyOutcome::Violated { witness: vec![1] },
            "broke: {broke}"
        );
    }
}
