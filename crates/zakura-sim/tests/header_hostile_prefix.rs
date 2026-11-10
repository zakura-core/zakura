//! hdr-13: `header-sync/bounded-adversary-progress` after each of fuzz-12's
//! hostile prefix classes, in `header-sync/storage`. Each witness's
//! recovery owes the node the source's chain, and the node meets it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use support::{
    inputs,
    storage_witnesses::{self, OWNER, TAKER},
};
use zakura_header_chain::Frontier;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::{after_step, PropertyOutcome},
    domain::header_sync_progress::{BOUNDED_ADVERSARY_PROGRESS, RECOVERY_PROGRESS_BOUND},
    harness::{
        header_node::{
            recovery::{Disruption, RecoveryEvidence, RECOVERY_BOUND},
            ControlAction,
        },
        header_sync::{grammar, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    property::{CheckEvent, Inconclusive, Lifecycle},
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

/// `at` as virtual time since the run's start.
fn since_start(at: SimInstant) -> Duration {
    at.saturating_duration_since(SimInstant::from_start(Duration::ZERO))
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

/// The loss path: the declared stall's recovery owes the node the grown
/// tip, and the harness closes the source right after the `recovered`
/// pick, a close only a typed scenario can take. That ends
/// `StableMembership` and `HonestPeer` with the obligation open, so the
/// property ends `Inconclusive` with `assumption-lost`, not violated, under
/// 8 plans.
#[test]
fn closing_the_source_after_recovered_loses_the_assumptions() {
    let mut scenario = storage_witnesses::grown_source();
    scenario.controls.push(storage_witnesses::step(
        storage_witnesses::SOURCE,
        ControlAction::Close,
        None,
    ));
    assert!(
        grammar::encode(&scenario).is_err(),
        "the grammar refuses it"
    );
    for input in inputs(8, 256) {
        let run = run("loss", &scenario, input);
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Inconclusive {
                reason: Inconclusive::AssumptionLost.to_string()
            }
        );
        let record = run.recovery.record().expect("the pick wrote its record");
        let close = run
            .report
            .picks
            .iter()
            .find(|pick| pick.identity == format!("peer:{}:close", storage_witnesses::SOURCE))
            .expect("the close is picked");
        assert!(close.step > record.step, "the close comes after the pick");
        let (_, objective) = run.recovery.objective().expect("the pick wrote its record");
        let objective = objective.expect("a successor is owed");
        let met_by_close = run
            .tips
            .iter()
            .any(|(step, _, tip)| *step <= close.step && objective.met_by(tip));
        assert!(!met_by_close, "the obligation was open at the close");
    }
}

/// A typed close after `recovered` keeps its trigger's time boundary. The
/// declared stall's close of its source is gated to half a second, so after
/// the pick the clock stops at exactly 500 ms rather than running to the
/// grown tip's status floor at one second. The close then comes before the
/// deadline with the obligation open, so the assumptions are lost, under 8
/// plans. (Time advances are actions too, so the close may follow one.)
#[test]
fn a_delayed_close_after_recovered_keeps_its_time() {
    let mut scenario = storage_witnesses::grown_source();
    scenario.controls.push(storage_witnesses::step(
        storage_witnesses::SOURCE,
        ControlAction::Close,
        Some(zakura_sim::harness::header_node::policy::Trigger::VirtualTime(500)),
    ));
    for input in inputs(8, 256) {
        let run = run("delayed close", &scenario, input);
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Inconclusive {
                reason: Inconclusive::AssumptionLost.to_string()
            }
        );
        let record = run.recovery.record().expect("the pick wrote its record");
        assert!(
            run.report.picks.iter().any(|pick| {
                pick.step > record.step && pick.identity == "advance-time:500000000"
            }),
            "the clock stops at the close's time"
        );
    }
}

/// A close after `recovered` of a peer that was no member at the pick
/// changes nothing recovery recorded, so it can't end the obligation. The
/// declared stall gains a second mirror, which the harness closes before
/// the recover and closes again after the pick, with the grown tip still
/// owed. The obligation stays open, the node meets it, and the property
/// holds, under 8 plans.
#[test]
fn closing_a_departed_peer_after_recovered_loses_nothing() {
    let mut scenario = storage_witnesses::grown_source();
    scenario.mirrors.push(storage_witnesses::mirror(
        zakura_sim::harness::header_node::Chain::main(2),
        zakura_sim::harness::header_sync::Dialer::Mirror,
    ));
    let close = |peer: &str| storage_witnesses::step(peer, ControlAction::Close, None);
    scenario.controls.insert(0, close("mirror-1"));
    scenario.controls.push(close("mirror-1"));
    for input in inputs(8, 256) {
        let run = run("departed close", &scenario, input);
        let record = run.recovery.record().expect("the pick wrote its record");
        assert!(
            record
                .membership
                .iter()
                .all(|member| member.peer != "mirror-1"),
            "mirror-1 was no member"
        );
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
    }
}

/// Progress after the conditions point, moved from fuzz-12.4b. fuzz-12's
/// barrier opens only once no task is ready, so a node with a live source
/// has synced from it by the `recovered` pick. The declared stall leaves
/// the source's grown tip behind its one-second status floor instead. At
/// the pick the node isn't at the source's recorded tip, it reaches it
/// later within the bound, and the property holds, under 8 plans. It
/// prints the virtual time from the pick to that apply.
#[test]
#[allow(clippy::print_stdout)]
fn the_node_reaches_the_source_tip_after_the_conditions_point() {
    let scenario = storage_witnesses::grown_source();
    for input in inputs(8, 256) {
        let run = run("stall", &scenario, input);
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        let record = run.recovery.record().expect("the pick wrote its record");
        let source_tip = record.source_tip.0;
        assert_eq!(source_tip.height.0, 10, "the grown tip is recorded");
        assert_ne!(record.node_tip.0, source_tip, "not there at the pick");
        let (_, reached, _) = run
            .tips
            .iter()
            .find(|(step, _, tip)| *step > record.step && *tip == source_tip)
            .expect("the node reaches the source's recorded tip");
        let waited = reached.saturating_duration_since(record.at);
        assert!(
            Duration::ZERO < waited && waited < RECOVERY_PROGRESS_BOUND,
            "{waited:?}"
        );
        println!(
            "stall: recovered at {:?}, at the source's tip {waited:?} later",
            record.at
        );
    }
}

/// A binding recovery deadline, moved from fuzz-12.4b. A fair recovery
/// completes each storage operation long before its 1 s bound. Under the
/// declared `slow-storage` disruption each completes only at its deadline,
/// a second after the later of the `Recover` pick and its submission. Under
/// 8 plans an operation is still pending when time reaches its deadline,
/// time never passes a deadline while its operation is pending, and the
/// node still meets the objective within 30 s, so the property holds.
#[test]
#[allow(clippy::print_stdout)]
fn the_recovery_storage_bound_binds_under_slow_storage() {
    let mut scenario = storage_witnesses::recovering(vec![storage_witnesses::recover(None)]);
    scenario.policy.disruptions = vec![Disruption::SlowStorage];
    assert!(
        grammar::encode(&scenario).is_err(),
        "the grammar refuses it"
    );
    for input in inputs(8, 256) {
        let ports = Storage::prepare(&scenario);
        let (recovery, node) = (ports.recovery.clone(), ports.node.clone());
        // Each step's time and the operations pending after it, each with
        // when it was submitted, both since the run's start.
        let seen = Rc::new(RefCell::new(Vec::new()));
        let into = seen.clone();
        let mut checker = after_step(move |cx| {
            let now = since_start(cx.now);
            let pending: Vec<(u64, Duration)> = node
                .pending()
                .iter()
                .map(|op| (op.id.0, now.saturating_sub(op.since_submit)))
                .collect();
            into.borrow_mut().push((cx.step, now, pending));
            None
        });
        let report = run_prepared::<Storage>(
            &scenario,
            ports,
            input,
            &mut MemorySink::default(),
            Some(&mut checker),
        )
        .expect("the run starts");
        let end = &report.end;
        assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
        assert!(matches!(end.outcome, Outcome::Idle { .. }), "{end:?}");
        assert_eq!(
            outcome(&report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        let started = since_start(recovery.started().expect("the recover is picked"));
        let deadline = |submitted: Duration| submitted.max(started) + RECOVERY_BOUND;
        let seen = seen.take();
        let mut bound = 0;
        for (step, now, pending) in seen.iter().filter(|(_, now, _)| *now >= started) {
            for (id, submitted) in pending {
                let due = deadline(*submitted);
                assert!(
                    *now <= due,
                    "step {step}: operation {id} pending past {due:?}"
                );
                if *now == due {
                    bound += 1;
                }
            }
        }
        assert!(bound > 0, "some operation is pending at its deadline");
        let record = recovery.record().expect("the pick wrote its record");
        println!(
            "slow storage: {bound} steps at a deadline, recovered at {:?}",
            record.at
        );
    }
}
