//! The mixed resource witness on `header-sync/storage`: the one history
//! where all four resource rules run together. Those are
//! `chunk-budget-bound`, `teardown-returns-baseline`,
//! `stale-completion-inert` and `outstanding-work-bounded`.
//!
//! The scenario closes and relinks both mirrors, so each comes back on a
//! fresh connection and session. It holds the node's first prepare across
//! its round's retirement, and holds the first apply's reply across an
//! invalidation, then reconsiders. It then shuts the node down and stops its
//! storage. Each of the four rules must hold and apply, every control step
//! must be picked, and the node must settle at its reactor's exit.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc};

use support::inputs;
use zakura_sim::{
    check::after_step,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, OUTSTANDING_WORK_BOUNDED, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            claim::SettlementEvidence,
            ends::{EndEvidence, EndFact, EnvironmentAction},
            policy::{Allow, OpKind, ReplyHold, Select, Trigger},
            session::{SessionEvidence, SessionFact},
            stale::StaleEvidence,
            Chain, ControlAction, Policy, Reset,
        },
        header_sync::{
            storage_witnesses::{hold, mirror, step, NODE, SOURCE},
            Dialer, Storage, StorageScenario, Weights,
        },
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Observation, PlanInput, RunReport,
};

/// The mirror below the node, which the node dials and serves.
const SERVED: &str = "mirror-1";

/// The four resource rules, each of which must hold and apply.
const RESOURCES: [&str; 4] = [
    CHUNK_BUDGET_BOUND,
    TEARDOWN_RETURNS_BASELINE,
    STALE_COMPLETION_INERT,
    OUTSTANDING_WORK_BOUNDED,
];

/// The control steps the run must pick, in script order: its completion
/// condition, ending with the `Shutdown` and the `StopStorage`.
const PICKS: [&str; 8] = [
    "peer:mirror-0:close",
    "peer:mirror-0:relink",
    "peer:node:reset",
    "peer:node:reset",
    "peer:mirror-1:close",
    "peer:mirror-1:relink",
    "peer:node:shutdown",
    "peer:node:stop-storage",
];

/// The node at A2 syncing from `mirror-0` at A8, and serving `mirror-1` at
/// A1, which it dials.
///
/// - The node's first prepare is held until `mirror-0`'s second session is
///   admitted, and `mirror-0`'s link closes once that prepare is submitted
///   and relinks once closed. The prepare's round retires with its session,
///   so it ends stale.
/// - The first apply's reply is held until the node resets. The node
///   invalidates its tip once that apply commits, and reconsiders once its
///   reply is delivered.
/// - `mirror-1`'s link then closes and relinks, so the node's serving
///   session churns too.
/// - The node shuts down once it admits `mirror-1`'s second session, and
///   its storage stops after that.
fn mixed_resources() -> StorageScenario {
    let first = |kind| Select::nth(kind, 1);
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            mirror(Chain::main(8), Dialer::Mirror),
            mirror(Chain::main(1), Dialer::Node),
        ],
        policy: Policy {
            holds: vec![hold(
                first(OpKind::Prepare),
                Trigger::Count(2, Box::new(Trigger::SessionAdmitted(SOURCE.to_owned()))),
                Allow::Engine,
            )],
            reply_holds: vec![ReplyHold {
                select: first(OpKind::Apply),
                until: Trigger::Reset(NODE.to_owned()),
            }],
            ..Policy::converging()
        },
        controls: vec![
            step(
                SOURCE,
                ControlAction::Close,
                Some(Trigger::Submitted(first(OpKind::Prepare))),
            ),
            step(
                SOURCE,
                ControlAction::Relink,
                Some(Trigger::Closed(SOURCE.to_owned())),
            ),
            step(
                NODE,
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::Committed(first(OpKind::Apply))),
            ),
            step(
                NODE,
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Completed(first(OpKind::Apply))),
            ),
            step(SERVED, ControlAction::Close, None),
            step(
                SERVED,
                ControlAction::Relink,
                Some(Trigger::Closed(SERVED.to_owned())),
            ),
            step(
                NODE,
                ControlAction::Shutdown,
                Some(Trigger::Count(
                    2,
                    Box::new(Trigger::SessionAdmitted(SERVED.to_owned())),
                )),
            ),
            step(NODE, ControlAction::StopStorage, None),
        ],
        weights: Weights::default(),
    }
}

/// One run of the scenario and the evidence its node left.
struct Run {
    report: RunReport,
    /// Every control step picked, by identity, in order.
    picks: Vec<String>,
    rounds: SettlementEvidence,
    stale: StaleEvidence,
    sessions: SessionEvidence,
    ends: EndEvidence,
}

/// Runs `scenario` under `input`, which must end with no violation and no
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Run {
    let ports = Storage::prepare(scenario);
    let (rounds, stale, sessions, ends) = (
        ports.rounds.clone(),
        ports.stale_calls.clone(),
        ports.sessions.clone(),
        ports.ends.clone(),
    );
    let picks = Rc::new(RefCell::new(Vec::new()));
    let into = picks.clone();
    let mut checker = after_step(move |cx| {
        let pick = cx.pick.to_string();
        if pick.starts_with("peer:") {
            into.borrow_mut().push(pick);
        }
        None
    });
    let report = run_prepared::<Storage>(
        scenario,
        ports,
        input,
        &mut MemorySink::default(),
        Some(&mut checker),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        (&report.end.violation, &report.end.harness),
        (&None, &None),
        "{:?}",
        report.end
    );
    let picks = picks.take();
    Run {
        report,
        picks,
        rounds,
        stale,
        sessions,
        ends,
    }
}

/// Requires that the first apply's storage committed it, then invalidated
/// its tip, and only then delivered its reply.
fn apply_reply_crosses_reset(observations: &[Observation]) -> Result<(), String> {
    let facts: Vec<&StorageFact> = observations
        .iter()
        .filter_map(|observation| match observation {
            Observation::Storage { fact, .. } => Some(fact),
            _ => None,
        })
        .collect();
    let apply = facts
        .iter()
        .find_map(|fact| match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::ApplyHeaderTarget { .. },
                ..
            } => Some(*id),
            _ => None,
        })
        .ok_or("no apply was submitted")?;
    let committed = facts
        .iter()
        .position(|fact| matches!(fact, StorageFact::Committed { id, .. } if *id == apply))
        .ok_or(format!("apply {apply} never committed"))?;
    let reset = facts[committed..]
        .iter()
        .position(|fact| matches!(fact, StorageFact::Reset { kind, .. } if kind == "invalidate"))
        .ok_or(format!("no invalidation followed apply {apply}'s commit"))?
        + committed;
    let delivered = facts
        .iter()
        .position(|fact| {
            matches!(fact, StorageFact::Completed { id, delivered: true, .. } if *id == apply)
        })
        .ok_or(format!("apply {apply}'s reply was never delivered"))?;
    if delivered < reset {
        return Err(format!(
            "apply {apply}'s reply was delivered before the reset"
        ));
    }
    Ok(())
}

/// `peer`'s links in `facts`, as `(connection, session)`, and how many
/// sessions the node admitted from it.
fn links_of(facts: &[SessionFact], peer: &str) -> (Vec<(u64, u64)>, usize) {
    let mut links = Vec::new();
    let mut admissions = 0;
    for fact in facts {
        match fact {
            SessionFact::LinkOpened {
                peer: linked,
                connection,
                session_id,
                ..
            } if linked == peer => links.push((*connection, *session_id)),
            SessionFact::SessionAdmitted { peer: admitted, .. } if admitted == peer => {
                admissions += 1
            }
            _ => {}
        }
    }
    (links, admissions)
}

/// Checks `run` is the mixed witness:
///
/// - every control step is picked in order;
/// - each resource rule held and applied;
/// - a held prepare ended stale at a step's drain;
/// - the first apply's reply was held across the reset;
/// - both mirrors came back on fresh connections and sessions;
/// - the harness stopped the node, which settled at its reactor's exit.
fn witnessed(label: &str, run: &Run) {
    assert_eq!(run.picks, PICKS, "{label}");
    for id in RESOURCES {
        let outcome = &run
            .report
            .properties
            .iter()
            .find(|property| property.property == id)
            .unwrap_or_else(|| panic!("{label}: {id} reports"))
            .outcome;
        assert!(
            matches!(outcome, zakura_sim::PropertyOutcome::Held { applicable } if *applicable > 0),
            "{label}: {id}: {outcome:?}"
        );
    }
    assert!(
        run.stale
            .windows()
            .iter()
            .any(|window| !window.apply && window.step.is_some()),
        "{label}: a prepare ends stale: {:?}",
        run.stale.windows()
    );
    let observations: Vec<Observation> = run
        .report
        .log
        .entries()
        .iter()
        .map(|entry| entry.obs.clone())
        .collect();
    if let Err(why) = apply_reply_crosses_reset(&observations) {
        panic!("{label}: the held apply reply crosses the reset: {why}");
    }
    assert!(run.rounds.any_released(), "{label}: a round is released");
    assert!(
        run.rounds.settled(),
        "{label}: the stopped node settles: {:?}",
        run.rounds.rows()
    );
    assert_eq!(
        run.ends.reactor_exited(),
        None,
        "{label}: the harness stopped it"
    );
    assert!(
        run.ends.facts().iter().any(|(_, fact)| matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        )),
        "{label}: the harness's Shutdown is recorded"
    );
    let facts = run.sessions.facts();
    for peer in [SOURCE, SERVED] {
        let (links, admissions) = links_of(&facts, peer);
        let [(old_connection, old_session), (new_connection, new_session)] = links[..] else {
            panic!("{label}: {peer} links twice: {links:?}");
        };
        assert_ne!(old_connection, new_connection, "{label}: {peer}");
        assert_ne!(old_session, new_session, "{label}: {peer}");
        assert_eq!(admissions, 2, "{label}: {peer}");
    }
}

/// The scenario is the mixed witness under generated plans, so no honest
/// schedule breaks a rule or skips a step.
#[test]
fn the_mixed_scenario_holds_under_generated_plans() {
    let scenario = mixed_resources();
    for (index, input) in inputs(8, 256).into_iter().enumerate() {
        witnessed(&format!("plan {index}"), &run(&scenario, input));
    }
}

/// Each resource rule's passing witness on the scenario: the rule meets its
/// trigger history and holds, and no property is violated.
#[test]
fn each_resource_rule_holds_in_the_mixed_witness() {
    let input = inputs(2, 256).remove(1);
    for id in RESOURCES {
        check_witness::<Storage>(
            WitnessScenario {
                scenario: mixed_resources(),
                input: input.clone(),
            },
            id,
            Expect::Holds,
        );
    }
}
