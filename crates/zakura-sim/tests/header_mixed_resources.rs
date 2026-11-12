//! hdr-14.8: gate 5's mixed resource witness on `header-sync/storage`, the
//! one history where all four `Sim` resource rules run together:
//! `chunk-budget-bound`, `teardown-returns-baseline`,
//! `stale-completion-inert` and `outstanding-work-bounded`.
//!
//! The scenario closes and relinks both mirrors, so each comes back on a
//! fresh connection and session. It holds the node's first prepare across
//! its round's retirement, holds the first apply's reply across an
//! invalidation, reconsiders, then shuts the node down and stops its
//! storage, which the harness offers only once the stop is done. Each of
//! the four rules must hold and apply, every other property must hold or
//! stay `NotApplicable` with the reason [`NOT_APPLICABLE`] lists, every
//! control step must be picked, and the node must settle at its reactor's
//! exit. The acceptance manifest counts the committed fixture's history once
//! per rule (`rows.rs`'s `MIXED_RESOURCES`).
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, path::PathBuf, process::Command, rc::Rc};

use support::{
    byte_inputs, inputs,
    storage_witnesses::{hold, mirror, step, NODE, SOURCE},
};
use zakura_header_chain::{HeaderGeneration, HeaderSyncWorkOwner};
use zakura_network::zakura::observe::ReactorEvent;
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::{
    check::{after_step, PropertyOutcome},
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, OUTSTANDING_WORK_BOUNDED, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            claim::SettlementEvidence,
            ends::{EndEvidence, EndFact, EnvironmentAction},
            policy::{Allow, OpKind, ReplyHold, Select, Trigger},
            reservation::ReservationEvidence,
            session::{SessionEvidence, SessionFact},
            stale::{StaleEvidence, StaleWindow},
            Chain, ControlAction, Policy, Reset,
        },
        header_sync::{Dialer, Storage, StorageScenario, Weights},
        run_prepared, Subsystem,
    },
    observe::{ReactorCall, ReactorFact, StorageFact, StorageRequest, WorkOwner},
    storage::work_owner,
    MemorySink, NaReason, Observation, Plan, PlanInput, RunReport,
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

/// The storage harness's properties that stay `NotApplicable` here, each
/// with its reason: no scenario of this harness declares a livelock
/// assumption or a recovery. `rows.rs`'s `MIXED_NA` lists the same.
const NOT_APPLICABLE: [(&str, NaReason); 2] = [
    ("sim/no-livelock", NaReason::Undeclared),
    (
        "header-sync/bounded-adversary-progress",
        NaReason::Undeclared,
    ),
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

/// The committed fixture's stem.
const STEM: &str = "header-resources-mixed";

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
    reservations: ReservationEvidence,
}

/// Runs `scenario` under `input`, which must end with no violation and no
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> Run {
    run_into(scenario, input, &mut MemorySink::default())
}

/// [`run`], recording into `sink`.
fn run_into(
    scenario: &StorageScenario,
    input: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> Run {
    let ports = Storage::prepare(scenario);
    let (rounds, stale, sessions, ends, reservations) = (
        ports.rounds.clone(),
        ports.stale_calls.clone(),
        ports.sessions.clone(),
        ports.ends.clone(),
        ports.reservations.clone(),
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
    let report = run_prepared::<Storage>(scenario, ports, input, sink, Some(&mut checker))
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
        reservations,
    }
}

/// What the run left of the scenario's selected operations: the log's
/// observations, the stale calls and the work queue's accounting facts.
#[derive(Clone)]
struct Selected {
    obs: Vec<Observation>,
    stale: Vec<StaleWindow>,
    accounting: Vec<ReactorEvent>,
}

impl Selected {
    /// `run`'s.
    fn of(run: &Run) -> Selected {
        Selected {
            obs: run
                .report
                .log
                .entries()
                .iter()
                .map(|entry| entry.obs.clone())
                .collect(),
            stale: run.stale.windows(),
            accounting: run.reservations.facts(),
        }
    }

    /// The first storage operation `is` matches, as its submission gives
    /// it: its ID, the reactor call that submitted it and its owner.
    fn first_submitted(
        &self,
        is: impl Fn(&StorageRequest) -> Option<&WorkOwner>,
    ) -> Option<(u64, Option<u64>, WorkOwner)> {
        self.obs.iter().find_map(|obs| match obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        id, request, call, ..
                    },
                ..
            } => is(request).map(|owner| (*id, *call, owner.clone())),
            _ => None,
        })
    }

    /// The index of the first observation `is` matches.
    fn position(&self, is: impl Fn(&Observation) -> bool) -> Option<usize> {
        self.obs.iter().position(is)
    }

    /// Checks the scenario's held first prepare is the one that ended stale
    /// and whose round was released, joined through its identities: its
    /// storage operation, submitted by a reactor call that dispatched a
    /// prepare for the same owner and ended paired with that operation, a
    /// stale window of that prepare call, the round of that same owner, in
    /// full (its kind, header generation, branch, session and request),
    /// retired before the call ended and backed by a reservation, and the
    /// queue's release of that round, by its peer and incarnation.
    fn held_prepare_joined(&self) -> Result<(), String> {
        let (op, call, owner) = self
            .first_submitted(|request| match request {
                StorageRequest::PrepareHeaderTarget { owner, .. } => Some(owner),
                _ => None,
            })
            .ok_or("no prepare was submitted")?;
        let call = call.ok_or(format!("prepare {op} names no reactor call"))?;
        self.position(|obs| {
            matches!(obs, Observation::Reactor {
                event: ReactorFact::Dispatched {
                    call: c,
                    port: ReactorCall::Prepare { owner: o, .. },
                },
                ..
            } if *c == call && *o == owner)
        })
        .ok_or(format!("call {call} dispatched no prepare for {owner:?}"))?;
        let ended = self
            .position(|obs| {
                matches!(obs, Observation::Reactor {
                    event: ReactorFact::CallEnded { call: c, op: o, .. },
                    ..
                } if *c == call && *o == Some(op))
            })
            .ok_or(format!(
                "call {call} never ended paired with operation {op}"
            ))?;
        if !self
            .stale
            .iter()
            .any(|window| window.call == call && !window.apply && window.step.is_some())
        {
            return Err(format!(
                "prepare call {call} didn't end stale: {:?}",
                self.stale
            ));
        }
        self.obs[..ended]
            .iter()
            .find(|obs| {
                matches!(obs, Observation::Reactor {
                    event: ReactorFact::Retired { request, .. },
                    ..
                } if request.owner == owner
                    && (request.session_id, request.request_id)
                        == (owner.session_id, owner.request_id))
            })
            .ok_or(format!(
                "{owner:?}'s round didn't retire before call {call} ended"
            ))?;
        let round = self
            .accounting
            .iter()
            .find_map(|event| match event {
                ReactorEvent::ReservationBound { request, .. }
                    if work_owner(request.owner) == owner
                        && (request.session_id, request.request_id)
                            == (owner.session_id, owner.request_id) =>
                {
                    Some((request.peer.clone(), request.incarnation))
                }
                _ => None,
            })
            .ok_or(format!("no reservation backs {owner:?}'s round"))?;
        let released = self.accounting.iter().any(|event| {
            matches!(event, ReactorEvent::RoundReleased { peer, incarnation, .. }
                if (peer, *incarnation) == (&round.0, round.1))
        });
        if !released {
            return Err(format!("round {} was never released", round.1));
        }
        Ok(())
    }

    /// Checks the scenario's selected apply, the first, kept its reply
    /// across the reset: storage committed it, the node then invalidated
    /// its tip, and only then delivered the apply's reply.
    fn apply_reply_crosses_reset(&self) -> Result<(), String> {
        let (op, ..) = self
            .first_submitted(|request| match request {
                StorageRequest::ApplyHeaderTarget { owner, .. } => Some(owner),
                _ => None,
            })
            .ok_or("no apply was submitted")?;
        let committed = self
            .position(|obs| {
                matches!(obs, Observation::Storage {
                    fact: StorageFact::Committed { id, .. },
                    ..
                } if *id == op)
            })
            .ok_or(format!("apply {op} never committed"))?;
        let reset = self.obs[committed..]
            .iter()
            .position(|obs| {
                matches!(obs, Observation::Storage {
                    fact: StorageFact::Reset { kind, .. },
                    ..
                } if kind == "invalidate")
            })
            .ok_or(format!("no invalidation followed apply {op}'s commit"))?
            + committed;
        let delivered = |obs: &Observation| {
            matches!(obs, Observation::Storage {
                fact: StorageFact::Completed { id, delivered: true, .. },
                ..
            } if *id == op)
        };
        match self.position(delivered) {
            Some(at) if at > reset => Ok(()),
            Some(at) => Err(format!(
                "apply {op}'s reply was delivered at {at}, before the reset at {reset}"
            )),
            None => Err(format!("apply {op}'s reply was never delivered")),
        }
    }
}

/// Checks `run` is the mixed witness: every control step picked in order,
/// each resource rule held and applied, every other property held or not
/// applicable as [`NOT_APPLICABLE`] lists, the held first prepare ended
/// stale with its round released ([`Selected::held_prepare_joined`]), the
/// selected apply's reply held across the reset
/// ([`Selected::apply_reply_crosses_reset`]), both mirrors back on fresh
/// identities, and the node
/// stopped by the harness and settled at its reactor's exit.
fn witnessed(label: &str, run: &Run) {
    assert_eq!(run.picks, PICKS, "{label}");
    for property in &run.report.properties {
        let id = property.property.as_str();
        let listed = NOT_APPLICABLE.iter().find(|(na, _)| *na == id);
        match (&property.outcome, listed) {
            (PropertyOutcome::Held { applicable }, None) if *applicable > 0 => {}
            (PropertyOutcome::NotApplicable { reason }, Some((_, expected)))
                if reason == expected => {}
            (outcome, _) => panic!("{label}: {id}: {outcome:?}"),
        }
    }
    for id in RESOURCES
        .into_iter()
        .chain(NOT_APPLICABLE.map(|(id, _)| id))
    {
        assert!(
            run.report.properties.iter().any(|p| p.property == id),
            "{label}: {id} reports"
        );
    }
    let selected = Selected::of(run);
    if let Err(why) = selected.held_prepare_joined() {
        panic!("{label}: the held prepare ends stale and its round is released: {why}");
    }
    if let Err(why) = selected.apply_reply_crosses_reset() {
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
        let links: Vec<(u64, u64)> = facts
            .iter()
            .filter_map(|fact| match fact {
                SessionFact::LinkOpened {
                    peer: at,
                    connection,
                    session_id,
                    ..
                } if at == peer => Some((*connection, *session_id)),
                _ => None,
            })
            .collect();
        let admitted: Vec<u64> = facts
            .iter()
            .filter_map(|fact| match fact {
                SessionFact::SessionAdmitted {
                    peer: at,
                    session_id,
                    ..
                } if at == peer => Some(*session_id),
                _ => None,
            })
            .collect();
        let [(old_connection, old_session), (new_connection, new_session)] = links[..] else {
            panic!("{label}: {peer} links twice: {links:?}");
        };
        assert_ne!(old_connection, new_connection, "{label}: {peer}");
        assert_ne!(old_session, new_session, "{label}: {peer}");
        assert_eq!(admitted, [old_session, new_session], "{label}: {peer}");
    }
}

/// The Plan bytes the committed fixture runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture file `<STEM>.<extension>`.
fn fixture_file(extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{STEM}.{extension}"))
}

/// The committed fixture is [`mixed_resources`] as JSON, with the fixed Plan
/// bytes, and is the mixed witness under them.
#[test]
fn the_mixed_fixture_holds_every_resource_rule() {
    let committed: StorageScenario = serde_json::from_str(
        &fs::read_to_string(fixture_file("json")).expect("the fixture is committed"),
    )
    .expect("the fixture is a storage scenario");
    assert_eq!(
        committed,
        mixed_resources(),
        "run `regenerate_mixed_resources_fixture`"
    );
    let plan = fs::read(fixture_file("plan")).expect("the Plan is committed");
    assert_eq!(
        plan,
        committed_plan(),
        "run `regenerate_mixed_resources_fixture`"
    );
    // The run is the manifest history `STEM`, which the acceptance tool
    // records from this declaration (C18).
    let ran = run_declared(
        Storage::NAME,
        STEM,
        &committed,
        Plan::from_fuzz(&plan),
        run_into,
    );
    witnessed(STEM, &ran);
}

/// The scenario is the mixed witness under generated plans too, so no
/// honest schedule breaks a rule or skips a step.
#[test]
fn the_mixed_scenario_holds_under_generated_plans() {
    let scenario = mixed_resources();
    for (index, input) in inputs(8, 256).into_iter().enumerate() {
        witnessed(&format!("plan {index}"), &run(&scenario, input));
    }
}

/// The held prepare's join and the apply reply's crossing each fail when
/// one identity changes on its own: the storage operation the call ended
/// paired with, the reactor call the operation names, the owner the call
/// dispatched for, the stale window's call, the retired round's owner, by
/// its request, header generation or branch, the reserved round's owner,
/// by its header generation or branch, and the released round's
/// incarnation, and for the apply, a reply delivered before the reset.
#[test]
fn the_mixed_joins_fail_one_identity_off() {
    let committed: StorageScenario = serde_json::from_str(
        &fs::read_to_string(fixture_file("json")).expect("the fixture is committed"),
    )
    .expect("the fixture is a storage scenario");
    let plan = fs::read(fixture_file("plan")).expect("the Plan is committed");
    let selected = Selected::of(&run(&committed, Plan::from_fuzz(&plan)));
    selected
        .held_prepare_joined()
        .expect("the committed run joins");
    selected
        .apply_reply_crosses_reset()
        .expect("the committed reply crosses");
    let (op, call, _) = selected
        .first_submitted(|request| match request {
            StorageRequest::PrepareHeaderTarget { owner, .. } => Some(owner),
            _ => None,
        })
        .expect("a prepare was submitted");
    let call = call.expect("a reactor call submitted it");

    /// `selected` with `change` applied to each observation.
    fn each_obs(selected: &Selected, change: impl Fn(&mut Observation)) -> Selected {
        let mut changed = selected.clone();
        changed.obs.iter_mut().for_each(change);
        changed
    }
    let off: Vec<(&str, Selected)> = vec![
        (
            "operation",
            each_obs(&selected, |obs| {
                if let Observation::Reactor {
                    event: ReactorFact::CallEnded { call: c, op: o, .. },
                    ..
                } = obs
                {
                    if *c == call && *o == Some(op) {
                        *o = Some(op + 1000);
                    }
                }
            }),
        ),
        (
            "call",
            each_obs(&selected, |obs| {
                if let Observation::Storage {
                    fact: StorageFact::Submitted { id, call: c, .. },
                    ..
                } = obs
                {
                    if *id == op {
                        *c = c.map(|c| c + 1000);
                    }
                }
            }),
        ),
        (
            "owner",
            each_obs(&selected, |obs| {
                if let Observation::Reactor {
                    event:
                        ReactorFact::Dispatched {
                            call: c,
                            port: ReactorCall::Prepare { owner, .. },
                        },
                    ..
                } = obs
                {
                    if *c == call {
                        owner.request_id += 1000;
                    }
                }
            }),
        ),
        ("stale call", {
            let mut changed = selected.clone();
            for window in &mut changed.stale {
                if window.call == call {
                    window.call += 1000;
                }
            }
            changed
        }),
        (
            "retired owner",
            each_obs(&selected, |obs| {
                if let Observation::Reactor {
                    event: ReactorFact::Retired { request, .. },
                    ..
                } = obs
                {
                    request.owner.request_id += 1000;
                }
            }),
        ),
        (
            "retired generation",
            each_obs(&selected, |obs| {
                if let Observation::Reactor {
                    event: ReactorFact::Retired { request, .. },
                    ..
                } = obs
                {
                    request.owner.scope.header_generation += 1000;
                }
            }),
        ),
        (
            "retired branch",
            each_obs(&selected, |obs| {
                if let Observation::Reactor {
                    event: ReactorFact::Retired { request, .. },
                    ..
                } = obs
                {
                    request.owner.scope.branch.target = "another target".into();
                }
            }),
        ),
        ("reserved generation", {
            let mut changed = selected.clone();
            for event in &mut changed.accounting {
                if let ReactorEvent::ReservationBound { request, .. } = event {
                    if let HeaderSyncWorkOwner::Header(owner) = &mut request.owner {
                        let generation = owner.authority.header_generation.get();
                        owner.authority.header_generation =
                            HeaderGeneration::new(generation + 1000);
                    }
                }
            }
            changed
        }),
        ("reserved branch", {
            let mut changed = selected.clone();
            for event in &mut changed.accounting {
                if let ReactorEvent::ReservationBound { request, .. } = event {
                    if let HeaderSyncWorkOwner::Header(owner) = &mut request.owner {
                        owner.authority.branch.target_tip_hash = owner.authority.branch.anchor_hash;
                    }
                }
            }
            changed
        }),
        ("released round", {
            let mut changed = selected.clone();
            for event in &mut changed.accounting {
                if let ReactorEvent::RoundReleased { incarnation, .. } = event {
                    *incarnation += 1000;
                }
            }
            changed
        }),
    ];
    for (identity, changed) in off {
        assert!(changed.held_prepare_joined().is_err(), "{identity}");
        assert!(changed.apply_reply_crosses_reset().is_ok(), "{identity}");
    }

    let (apply, ..) = selected
        .first_submitted(|request| match request {
            StorageRequest::ApplyHeaderTarget { owner, .. } => Some(owner),
            _ => None,
        })
        .expect("an apply was submitted");
    let reset = selected
        .position(|obs| {
            matches!(obs, Observation::Storage {
                fact: StorageFact::Reset { kind, .. },
                ..
            } if kind == "invalidate")
        })
        .expect("the node invalidated");
    let mut early = selected.clone();
    let reply = early
        .position(|obs| {
            matches!(obs, Observation::Storage {
                fact: StorageFact::Completed { id, delivered: true, .. },
                ..
            } if *id == apply)
        })
        .expect("the reply was delivered");
    let moved = early.obs.remove(reply);
    early.obs.insert(reset, moved);
    assert!(early.apply_reply_crosses_reset().is_err());
    assert!(early.held_prepare_joined().is_ok());
}

/// Rewrites the committed fixture from [`mixed_resources`].
#[test]
#[ignore = "rewrites the committed mixed resource fixture"]
fn regenerate_mixed_resources_fixture() {
    let json = serde_json::to_string_pretty(&mixed_resources()).expect("scenarios serialize");
    fs::write(fixture_file("json"), json + "\n").expect("the fixture directory exists");
    fs::write(fixture_file("plan"), committed_plan()).expect("the fixture directory exists");
}

/// `record --witness` judges the committed fixture for each resource rule as
/// `cargo xtask planted` runs it, expecting a hold, and `replay` reruns each
/// trace Exact in a fresh process.
#[test]
fn the_mixed_fixture_records_and_replays_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    let scenario = fs::read_to_string(fixture_file("json")).expect("the fixture is committed");
    let plan = fixture_file("plan");
    for property in RESOURCES {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &scenario,
            "--plan",
            plan.to_str().expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            property,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{property}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{property}: {witness}"
        );
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{property}: {replayed:?}"
        );
    }
}
