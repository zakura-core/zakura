//! hdr-13: `header-sync/bounded-adversary-progress` after each of fuzz-12's
//! hostile prefix classes, in `header-sync/storage`. Each witness's
//! recovery owes the node the source's chain, and the node meets it.
#![cfg(zakura_sim)]

mod support;

use std::{sync::Arc, time::Duration};

use support::{
    inputs,
    storage_witnesses::{self, OWNER, TAKER},
};
use zakura_header_chain::Frontier;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync_progress::{BOUNDED_ADVERSARY_PROGRESS, RECOVERY_PROGRESS_BOUND},
    harness::{
        header_node::recovery::RecoveryEvidence,
        header_sync::{Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    property::{CheckEvent, Lifecycle},
    Checker, MemorySink, ObservationLog, Outcome, PlanInput, PropertyReport, RunEnd, RunReport,
    StepCtx, Violation,
};
use zakura_storage::header::controlled::ControlledPort;

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// A run, with the node's selected tip after each step.
struct Run {
    report: RunReport,
    recovery: RecoveryEvidence,
    /// Each step, its time, and the node engine's selected tip after it.
    tips: Vec<(u64, SimInstant, Frontier)>,
    /// Each lifecycle point the registry reported for
    /// `bounded-adversary-progress`, with its step.
    points: Vec<(u64, Lifecycle)>,
}

impl Run {
    /// How long after the `recovered` pick the property's obligation was
    /// discharged: when the registry reported the discharge, or zero if it
    /// opened none, since the pick met the objective. An obligation that
    /// opened must have been discharged.
    fn met_after(&self) -> Duration {
        let record = self.recovery.record().expect("the pick wrote its record");
        let at = |wanted: Lifecycle| {
            self.points
                .iter()
                .find(|(_, point)| *point == wanted)
                .map(|(step, _)| *step)
        };
        let Some(opened) = at(Lifecycle::Obliged) else {
            return Duration::ZERO;
        };
        let discharged = at(Lifecycle::Discharged)
            .unwrap_or_else(|| panic!("opened at {opened}, never discharged: {:?}", self.points));
        let (_, now, _) = self
            .tips
            .iter()
            .find(|(step, ..)| *step == discharged)
            .expect("every step is seen");
        now.saturating_duration_since(record.at)
    }
}

/// Keeps the node's selected tip after each step and the registry's
/// lifecycle points for `bounded-adversary-progress`. The registry hands a
/// step's points over before the step's `after_step`, so each takes that
/// step.
struct Watch {
    node: Arc<ControlledPort>,
    tips: Vec<(u64, SimInstant, Frontier)>,
    pending: Vec<Lifecycle>,
    points: Vec<(u64, Lifecycle)>,
}

impl Checker for Watch {
    /// Keeps the tip and the step's points.
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let tip = self.node.engine().snapshot().frontiers.header_best;
        self.tips.push((cx.step, cx.now, tip));
        self.points
            .extend(self.pending.drain(..).map(|point| (cx.step, point)));
        None
    }

    /// Reports nothing.
    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }

    /// Keeps the property's lifecycle points until the step's `after_step`.
    fn observe_events(&mut self, events: &[CheckEvent]) {
        self.pending
            .extend(events.iter().filter_map(|event| match event {
                CheckEvent::Lifecycle { property, to }
                    if property.as_str() == BOUNDED_ADVERSARY_PROGRESS =>
                {
                    Some(*to)
                }
                _ => None,
            }));
    }
}

/// Runs `scenario` under `input` with no violation or harness failure, and
/// every property but this one holding or not applying.
fn run(class: &str, scenario: &StorageScenario, input: PlanInput) -> Run {
    let ports = Storage::prepare(scenario);
    let mut watch = Watch {
        node: ports.node.clone(),
        tips: Vec::new(),
        pending: Vec::new(),
        points: Vec::new(),
    };
    let recovery = ports.recovery.clone();
    let report = run_prepared::<Storage>(
        scenario,
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut watch),
    )
    .unwrap_or_else(|error| panic!("{class}: {error}"));
    let end = &report.end;
    assert_eq!(
        (&end.violation, &end.harness),
        (&None, &None),
        "{class}: {end:?}"
    );
    assert!(
        matches!(end.outcome, Outcome::Idle { .. }),
        "{class}: ends idle at its finish: {end:?}"
    );
    for property in &report.properties {
        if property.property != BOUNDED_ADVERSARY_PROGRESS {
            assert!(
                matches!(
                    property.outcome,
                    PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable
                ),
                "{class}: {}: {:?}",
                property.property,
                property.outcome
            );
        }
    }
    Run {
        report,
        recovery,
        tips: watch.tips,
        points: watch.points,
    }
}

/// A recovery witness for each prefix class: fuzz-12's, with hdr-13's owed
/// variant for a class whose fuzz-12 witness owes nothing
/// ([`storage_witnesses::owed_recovery_witnesses`]), and for the close class
/// the departure, the class's completion by the plan
/// ([`storage_witnesses::departure_recovery`]).
fn witnesses() -> Vec<(&'static str, StorageScenario)> {
    let owed = storage_witnesses::owed_recovery_witnesses();
    let mut witnesses = storage_witnesses::recovery_witnesses();
    for (class, scenario) in &mut witnesses {
        if let Some((_, variant)) = owed.iter().find(|(owed, _)| owed == class) {
            *scenario = variant.clone();
        }
        if *class == "closes" {
            *scenario = storage_witnesses::departure_recovery();
        }
    }
    witnesses
}

/// After each prefix class, under 8 plans, the recovery owes the node a
/// header past its baseline, and the node meets it within the bound:
/// applicable once and held. It prints the virtual time from the
/// `recovered` pick to the registry's discharge. With no stall the node has
/// synced from its source by the pick, so that's zero. The reply-hold
/// witness's grown source sends its new tip only behind its one-second
/// status floor, so the node meets it after the pick. The close class runs
/// as the departure: A departs holding the target, and B completes it.
#[test]
#[allow(clippy::print_stdout)]
fn every_prefix_class_recovers() {
    for (class, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let run = run(class, &scenario, input);
            assert_eq!(
                outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
                &PropertyOutcome::Held { applicable: 1 },
                "{class}"
            );
            let waited = run.met_after();
            assert!(waited < RECOVERY_PROGRESS_BOUND, "{class}: {waited:?}");
            let record = run.recovery.record().expect("the pick wrote its record");
            if class == "closes" {
                // The departure: A was closed, and B is the one member left.
                let members: Vec<&str> =
                    record.membership.iter().map(|m| m.peer.as_str()).collect();
                assert_eq!(members, [TAKER], "{class}");
                assert_eq!(record.source.peer, TAKER, "{class}");
            }
            println!(
                "{class}: recovered at {:?}, met {waited:?} later",
                record.at
            );
        }
    }
}

/// The close class is the departure's completion: A held the target when
/// the harness closed it, and B, the only member left, supplies a header
/// of the objective within the bound, under 8 plans.
#[test]
#[allow(clippy::print_stdout)]
fn the_surviving_peer_completes_a_departed_target() {
    let scenario = storage_witnesses::departure_recovery();
    for input in inputs(8, 256) {
        let run = run("departure", &scenario, input);
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        let picks: Vec<&str> = run
            .report
            .picks
            .iter()
            .map(|p| p.identity.as_str())
            .collect();
        let at = |identity: &str| {
            picks
                .iter()
                .position(|pick| *pick == identity)
                .unwrap_or_else(|| panic!("{identity} is picked"))
        };
        assert!(
            at(&format!("peer:{OWNER}:close")) < at(&format!("peer:{TAKER}:recover")),
            "A closes before the recover"
        );
        let record = run.recovery.record().expect("the pick wrote its record");
        let members: Vec<&str> = record.membership.iter().map(|m| m.peer.as_str()).collect();
        assert_eq!(members, [TAKER], "only B is left");
        assert_eq!(record.source_tip.0.height.0, 6);
        let waited = run.met_after();
        assert!(waited < RECOVERY_PROGRESS_BOUND, "{waited:?}");
        println!(
            "departure: recovered at {:?}, met {waited:?} later",
            record.at
        );
    }
}
