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
    domain::{
        header_sync::PEER_RETIREMENT_PROGRESS,
        header_sync_progress::{BOUNDED_ADVERSARY_PROGRESS, RECOVERY_PROGRESS_BOUND},
    },
    harness::{
        header_node::{
            ends::{EndFact, EnvironmentAction},
            policy::{Allow, OpKind, Select, Trigger},
            recovery::{recovery_window, Disruption, RecoveryEvidence, RECOVERY_BOUND},
            retirement::HandoffFact,
            Chain, ControlAction,
        },
        header_sync::{
            grammar,
            liveness::{LivenessScenario, RecoveryFixture},
            Liveness, Storage, StorageScenario,
        },
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    property::{CheckEvent, Inconclusive, Lifecycle},
    By, Checker, MemorySink, Observation, ObservationLog, Outcome, Plan, PlanChoice, PlanInput,
    PropertyReport, Record, RunEnd, RunReport, StepCtx, Violation,
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
/// property ends `Inconclusive` with `assumption-lost`, not violated, and
/// the registry reports `Lifecycle::AssumptionLost` once, under 8 plans.
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
        // The registry reports the loss once, at the step whose facts
        // carried the close, after the obligation it ended.
        let lost: Vec<u64> = run
            .points
            .iter()
            .filter(|(_, point)| *point == Lifecycle::AssumptionLost)
            .map(|(step, _)| *step)
            .collect();
        let [lost] = lost[..] else {
            panic!("one loss: {:?}", run.points);
        };
        assert!(lost >= close.step, "{:?}", run.points);
        assert!(
            run.points
                .iter()
                .any(|(step, point)| *point == Lifecycle::Obliged && *step < lost),
            "{:?}",
            run.points
        );
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
        assert!(
            !run.points
                .iter()
                .any(|(_, point)| *point == Lifecycle::AssumptionLost),
            "{:?}",
            run.points
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

/// One run of the declared stall under `script`, then round-robin: its
/// records, each step's pick and enabled set, the `recovered` pick's step,
/// and the first step after it at which storage committed one of the node's
/// applies, by its own facts.
struct Scripted {
    end: RunEnd,
    records: Vec<Record>,
    steps: Vec<(u64, String, Vec<String>)>,
    recovered: u64,
    applied: Option<u64>,
}

/// Runs `scenario` under `script`.
fn scripted(scenario: &StorageScenario, script: Vec<PlanChoice>) -> Scripted {
    let ports = Storage::prepare(scenario);
    let recovery = ports.recovery.clone();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let into = seen.clone();
    let mut checker = after_step(move |cx| {
        let enabled = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
        into.borrow_mut()
            .push((cx.step, cx.pick.to_string(), enabled));
        None
    });
    let mut sink = MemorySink::default();
    let report = run_prepared::<Storage>(
        scenario,
        ports,
        PlanInput::Script(Plan { choices: script }),
        &mut sink,
        Some(&mut checker),
    )
    .expect("the run starts");
    let record = recovery.record().expect("the pick wrote its record");
    // The node's applies, by operation, from its storage facts: the first
    // one storage commits after the pick, held reply or not.
    let mut applies = Vec::new();
    let mut applied = None;
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::ApplyHeaderTarget { .. },
                        ..
                    },
                ..
            } => applies.push(*id),
            Observation::Storage {
                fact: StorageFact::Committed { id, .. } | StorageFact::Completed { id, .. },
                ..
            } if entry.step > record.step && applies.contains(id) => {
                applied = Some(entry.step);
                break;
            }
            _ => {}
        }
    }
    Scripted {
        end: report.end,
        records: sink.records,
        steps: seen.take(),
        recovered: record.step,
        applied,
    }
}

/// The script that replays `records` up to, not including, step `until`:
/// every choice the run made by input or default, picks by identity and
/// draws by value, in the order it asked for them. Forced ones take no
/// entry.
fn replay(records: &[Record], until: u64) -> Vec<PlanChoice> {
    let draws = |draws: &[zakura_sim::record::DrawRecord]| -> Vec<PlanChoice> {
        draws
            .iter()
            .filter(|draw| draw.by != By::Forced)
            .map(|draw| PlanChoice::Draw(draw.value))
            .collect()
    };
    let mut script = Vec::new();
    for record in records {
        match record {
            Record::Init(init) => script.extend(draws(&init.draws)),
            Record::Step(step) if step.step < until => {
                if step.by != By::Forced {
                    script.push(PlanChoice::Pick(step.pick.clone()));
                }
                script.extend(draws(&step.draws));
            }
            _ => {}
        }
    }
    script
}

/// The scheduling-only negative control, moved from fuzz-12.4b. The
/// declared stall's fair run applies its first header `n` steps after the
/// `recovered` pick. The same scenario and the same `Recover`, with the
/// fair recovery policy off (`unfair`) and a scripted Plan that, after the
/// pick, never completes the node's storage while any other action is
/// ready, preferring to move time, applies nothing within those `n` steps.
/// Only scheduling differs. It's a comparison in steps, not virtual time:
/// the policy's bound is on picks. Both runs take the same round-robin
/// defaults before the `Recover` pick, and the unfair Plan is built here
/// pick by pick from each run's own records, so it follows the code as it
/// changes. It prints both counts.
#[test]
#[allow(clippy::print_stdout)]
fn an_unfair_plan_starves_the_recovery_the_fair_policy_completes() {
    let fair = storage_witnesses::grown_source();
    let fair_run = scripted(&fair, Vec::new());
    let end = &fair_run.end;
    assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
    let applied = fair_run.applied.expect("the fair run applies");
    let steps = applied - fair_run.recovered;
    let mut unfair = fair.clone();
    unfair.policy.disruptions = vec![Disruption::Unfair];
    let mut script = Vec::new();
    let mut rounds = 0;
    let run = loop {
        let run = scripted(&unfair, script.clone());
        let window = run.recovered + 1..=run.recovered + steps;
        let starve = run.steps.iter().find(|(step, pick, enabled)| {
            window.contains(step)
                && pick.starts_with("storage:")
                && enabled.iter().any(|option| !option.starts_with("storage:"))
        });
        let Some((step, _, enabled)) = starve else {
            break run;
        };
        let other = enabled
            .iter()
            .find(|option| option.starts_with("advance-time:"))
            .or_else(|| {
                enabled
                    .iter()
                    .find(|option| !option.starts_with("storage:"))
            })
            .expect("the step had another option");
        script = replay(&run.records, *step);
        script.push(PlanChoice::Pick(other.clone()));
        rounds += 1;
        assert!(rounds < 10_000, "the adversary converges");
    };
    let window_end = run.recovered + steps;
    // The unfair run is judged only if it ran clean through the window: a
    // violation or failure that stopped it early would show no apply too.
    let end = &run.end;
    assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
    assert!(matches!(end.outcome, Outcome::Idle { .. }), "{end:?}");
    assert!(
        run.steps
            .last()
            .is_some_and(|(step, ..)| *step >= window_end),
        "the unfair run ended before step {window_end}: {end:?}"
    );
    assert!(
        run.applied.is_none_or(|step| step > window_end),
        "the unfair run applied at {:?}, within {steps} steps of its pick at {}",
        run.applied,
        run.recovered
    );
    println!(
        "fair: applied {steps} steps after its pick; unfair: {:?} after, in {rounds} rounds",
        run.applied.map(|step| step - run.recovered)
    );
}

/// The departure recovery fixture arms `peer-retirement-progress`'s
/// hand-off, with mirror A as the owner and B as the taker, so the close
/// class's completion runs beside it. Under 8 plans the hand-off opens for
/// A's target when A's session departs holding it, a locator read for that
/// same target then reaches storage for another peer's session, B's, and
/// that comes before the harness begins stopping the node. Both
/// properties hold, and the run ends idle.
#[test]
fn the_departure_recovery_hands_the_target_to_the_survivor() {
    let fixture = LivenessScenario::Recovery(RecoveryFixture::Departure);
    assert!(fixture.roles().is_some());
    for input in inputs(8, 256) {
        let ports = Liveness::prepare(&fixture);
        let (retirement, ends) = (ports.retirement.clone(), ports.ends.clone());
        let report =
            run_prepared::<Liveness>(&fixture, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        let end = &report.end;
        assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
        assert!(matches!(end.outcome, Outcome::Idle { .. }), "{end:?}");
        assert_eq!(
            outcome(&report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        assert!(
            matches!(
                outcome(&report, PEER_RETIREMENT_PROGRESS),
                PropertyOutcome::Held { .. }
            ),
            "{:?}",
            outcome(&report, PEER_RETIREMENT_PROGRESS)
        );
        let facts = retirement.facts();
        let (opened, target, departed) = facts
            .iter()
            .find_map(|(stamp, fact)| match fact {
                HandoffFact::Opened {
                    target, departed, ..
                } => Some((*stamp, *target, departed.clone())),
                _ => None,
            })
            .unwrap_or_else(|| panic!("the hand-off opens: {facts:?}"));
        let read = facts
            .iter()
            .find_map(|(stamp, fact)| match fact {
                HandoffFact::Read {
                    target: read,
                    session,
                } if *stamp > opened && *read == target && session.0 != departed.0 => Some(*stamp),
                _ => None,
            })
            .unwrap_or_else(|| panic!("B reads A's target: {facts:?}"));
        let stopped = ends
            .facts()
            .into_iter()
            .find_map(|(stamp, fact)| match fact {
                EndFact::Environment {
                    action: EnvironmentAction::Shutdown,
                    ..
                } => Some(stamp),
                _ => None,
            });
        assert!(
            stopped.is_none_or(|stopped| read < stopped),
            "the read comes before the stop"
        );
    }
}

/// Recovery ends within its stated window. Every class witness reaches its
/// `recovered` pick within `recovery_window` of its `Recover` pick, at the
/// node's default readmission cooldown, under 8 plans. (A node that never
/// lets its source back in ends unreached within it, which the planted
/// `pause-outlasts-cooldown` case shows.)
#[test]
fn every_recovery_ends_within_its_window() {
    let window = recovery_window(
        zakura_network::zakura::ZakuraHeaderSyncConfig::default().unproductive_peer_cooldown,
    );
    for (class, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let run = run(class, &scenario, input);
            let started = run.recovery.started().expect("the recover is picked");
            let record = run.recovery.record().expect("the pick wrote its record");
            let took = record.at.saturating_duration_since(started);
            assert!(took <= window, "{class}: {took:?} past {window:?}");
        }
    }
}

/// A source's own cooldown of the node, after a prefix that held the
/// node's serving storage, isn't the node refusing its source. The node is
/// at A8 and the source behind at A2. Every read lease the node takes to
/// serve it is held, so the source times out three requests and drops the
/// node, and the recover comes at that drop. Recovery's first relink is
/// refused, and it waits a cooldown before the next, which is admitted, so
/// it never ends unreached, under 8 plans. Nothing is owed, since the node
/// holds the heaviest chain.
#[test]
fn a_source_cooling_down_on_the_node_isnt_unreached() {
    let mut scenario = StorageScenario {
        node: Chain::main(8),
        ..storage_witnesses::recovering(vec![storage_witnesses::recover(Some(Trigger::Closed(
            storage_witnesses::SOURCE.to_owned(),
        )))])
    };
    scenario.mirrors[0].chain = Chain::main(2);
    scenario.policy.holds.push(storage_witnesses::hold(
        Select {
            kind: Some(OpKind::Acquire),
            ..Select::default()
        },
        Trigger::Released,
        Allow::Engine,
    ));
    for input in inputs(8, 256) {
        let run = run("cooldown", &scenario, input);
        let relinks = run
            .report
            .picks
            .iter()
            .filter(|pick| pick.identity.ends_with(":recovery-relink"))
            .count();
        assert!(!run.recovery.unreached(), "recovery reaches its source");
        assert!(relinks >= 2, "the first relink is refused: {relinks}");
        assert_eq!(
            outcome(&run.report, BOUNDED_ADVERSARY_PROGRESS),
            &PropertyOutcome::NotApplicable
        );
    }
}
