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

use serde::{Deserialize, Serialize};
use support::{
    byte_inputs, inputs,
    one_key_off::assert_one_key_off,
    storage_witnesses::{hold, mirror, step, NODE, SOURCE},
};
use zakura_network::zakura::observe::ReactorEvent;
use zakura_sim::harness::header_acceptance::{
    history::run_declared,
    refs::{joined, Ref, WorkRef},
};
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

/// A reactor call's dispatch of a prepare: the call and the prepare's
/// owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Dispatch {
    call: u64,
    owner: WorkOwner,
}
impl Ref for Dispatch {}

/// A reactor call paired with the storage operation it submitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Paired {
    call: u64,
    op: u64,
}
impl Ref for Paired {}

/// A stale window: its call, and whether it was an apply's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StaleCall {
    call: u64,
    apply: bool,
}
impl Ref for StaleCall {}

/// `bytes` in lowercase hex, as a peer's key.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A round's release, by its peer, in hex, and incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Release {
    peer: String,
    incarnation: u64,
}
impl Ref for Release {}

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

    /// The first prepare's operation, the reactor call that submitted it
    /// and its owner, or why there's none.
    fn held_prepare(&self) -> Result<(u64, u64, WorkOwner), String> {
        let (op, call, owner) = self
            .first_submitted(|request| match request {
                StorageRequest::PrepareHeaderTarget { owner, .. } => Some(owner),
                _ => None,
            })
            .ok_or("no prepare was submitted")?;
        let call = call.ok_or(format!("prepare {op} names no reactor call"))?;
        Ok((op, call, owner))
    }

    /// Whether a reactor call dispatched `want`.
    fn dispatched(&self, want: &Dispatch) -> bool {
        let of = |obs: &Observation| match obs {
            Observation::Reactor {
                event:
                    ReactorFact::Dispatched {
                        call,
                        port: ReactorCall::Prepare { owner, .. },
                    },
                ..
            } => Some(Dispatch {
                call: *call,
                owner: owner.clone(),
            }),
            _ => None,
        };
        !joined(&self.obs, of, want).is_empty()
    }

    /// The position of the first observation of a call ending paired with
    /// its operation, `want`.
    fn ended(&self, want: &Paired) -> Option<usize> {
        self.obs.iter().position(|obs| match obs {
            Observation::Reactor {
                event: ReactorFact::CallEnded { call, op, .. },
                ..
            } => op.map(|op| Paired { call: *call, op }).as_ref() == Some(want),
            _ => false,
        })
    }

    /// Whether `want`'s stale window ended at a step's drain.
    fn ended_stale(&self, want: &StaleCall) -> bool {
        let of = |window: &StaleWindow| {
            Some(StaleCall {
                call: window.call,
                apply: window.apply,
            })
        };
        joined(&self.stale, of, want)
            .iter()
            .any(|window| window.step.is_some())
    }

    /// Whether `want`'s round retired before the observation at `ended`.
    fn retired_before(&self, want: &WorkRef, ended: usize) -> bool {
        let of = |obs: &Observation| match obs {
            Observation::Reactor {
                event: ReactorFact::Retired { request, .. },
                ..
            } => Some(WorkRef {
                owner: request.owner.clone(),
                session_id: request.session_id,
                request_id: request.request_id,
            }),
            _ => None,
        };
        !joined(&self.obs[..ended], of, want).is_empty()
    }

    /// The round a reservation backs for `want`, by its peer and
    /// incarnation.
    fn reserved(&self, want: &WorkRef) -> Option<Release> {
        let of = |event: &ReactorEvent| match event {
            ReactorEvent::ReservationBound { request, .. } => Some(WorkRef {
                owner: work_owner(request.owner),
                session_id: request.session_id,
                request_id: request.request_id,
            }),
            _ => None,
        };
        joined(&self.accounting, of, want)
            .into_iter()
            .find_map(|event| match event {
                ReactorEvent::ReservationBound { request, .. } => Some(Release {
                    peer: hex(request.peer.as_bytes()),
                    incarnation: request.incarnation,
                }),
                _ => None,
            })
    }

    /// Whether the queue released `want`.
    fn released(&self, want: &Release) -> bool {
        let of = |event: &ReactorEvent| match event {
            ReactorEvent::RoundReleased {
                peer, incarnation, ..
            } => Some(Release {
                peer: hex(peer.as_bytes()),
                incarnation: *incarnation,
            }),
            _ => None,
        };
        !joined(&self.accounting, of, want).is_empty()
    }

    /// Checks the scenario's held first prepare is the one that ended stale
    /// and whose round was released, joined through its identities, each a
    /// whole ref: its storage operation, submitted by a reactor call that
    /// dispatched a prepare for the same owner ([`Dispatch`]) and ended
    /// paired with that operation ([`Paired`]), a stale window of that
    /// prepare call ([`StaleCall`]), the round of that same owner, in full
    /// ([`WorkRef`]), retired before the call ended and backed by a
    /// reservation, and the queue's release of that round ([`Release`]).
    fn held_prepare_joined(&self) -> Result<(), String> {
        let (op, call, owner) = self.held_prepare()?;
        let dispatch = Dispatch {
            call,
            owner: owner.clone(),
        };
        if !self.dispatched(&dispatch) {
            return Err(format!("no call dispatched {dispatch:?}"));
        }
        let ended = self
            .ended(&Paired { call, op })
            .ok_or(format!("call {call} never ended paired with operation {op}"))?;
        if !self.ended_stale(&StaleCall { call, apply: false }) {
            return Err(format!(
                "prepare call {call} didn't end stale: {:?}",
                self.stale
            ));
        }
        let round = WorkRef {
            session_id: owner.session_id,
            request_id: owner.request_id,
            owner,
        };
        if !self.retired_before(&round, ended) {
            return Err(format!("{round:?} didn't retire before call {call} ended"));
        }
        let release = self
            .reserved(&round)
            .ok_or(format!("no reservation backs {round:?}"))?;
        if !self.released(&release) {
            return Err(format!("{release:?} was never released"));
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

/// Each of the held prepare's joins, and so the witness, fails when any
/// one key of its ref moves on its own (Rule 25's template): the call's
/// dispatch of the prepare's owner, every field of it, the call paired
/// with the operation, the prepare's stale window, the round retired and
/// reserved for the owner in full, and the round's release. The apply
/// reply's crossing fails for a reply delivered before the reset.
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
    let (op, call, owner) = selected.held_prepare().expect("a prepare was submitted");

    let dispatch = Dispatch {
        call,
        owner: owner.clone(),
    };
    assert_one_key_off(&dispatch, |dispatch| selected.dispatched(dispatch));
    let paired = Paired { call, op };
    let ended = selected.ended(&paired).expect("the call ended paired");
    assert_one_key_off(&paired, |paired| selected.ended(paired).is_some());
    assert_one_key_off(&StaleCall { call, apply: false }, |stale| {
        selected.ended_stale(stale)
    });
    let round = WorkRef {
        session_id: owner.session_id,
        request_id: owner.request_id,
        owner,
    };
    assert_one_key_off(&round, |round| selected.retired_before(round, ended));
    let release = selected.reserved(&round).expect("a reservation backs it");
    assert_one_key_off(&round, |round| selected.reserved(round).is_some());
    assert_one_key_off(&release, |release| selected.released(release));

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
