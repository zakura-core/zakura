//! fuzz-12 item 5's recovery phase in `header-sync/storage`. A `Recover`
//! step ends a hostile prefix: controls stop, invalidations are
//! reconsidered, holds retire and scheduling turns fair. The harness then
//! drives the conditions and offers `recovered` at a quiet step once the
//! source has a live session.
//!
//! Every witness runs the node at the main chain's 2nd header and the
//! source, `mirror-0`, at its 8th, the heaviest chain in the run, so its
//! tip doesn't move after the record.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    process::{Command, Output},
    rc::Rc,
    sync::Arc,
};

use support::{inputs, storage_witnesses};
use tempfile::TempDir;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    check::after_step,
    check::Phase,
    harness::{
        header_node::{
            policy::Trigger,
            recovery::{
                live_sessions, membership_changed, session_lasts, Member, RecoveryEvidence,
                RecoveryRecord,
            },
            session::{SessionEvidence, SessionFact},
            Chain, ControlAction, ControlStep, Policy,
        },
        header_sync::{
            grammar, storage_config, storage_properties, storage_sources, Dialer, MirrorSpec,
            Storage, StorageScenario, Weights,
        },
        run_prepared, Subsystem,
    },
    observe::StorageFact,
    property::{Assume, Assumptions, Registry, RegistryChecker},
    record::{Record, StepRecord},
    By, MemorySink, Observation, PlanInput, RunReport, Sim,
};
use zakura_storage::header::controlled::ControlledPort;

/// The node's name.
const NODE: &str = "node";

/// The source's name.
const SOURCE: &str = "mirror-0";

/// `action` on `peer`, offered once `when` holds, or from the start.
fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// A recover of the source, offered once `when` holds.
fn recover(when: Option<Trigger>) -> ControlStep {
    step(SOURCE, ControlAction::Recover, when)
}

/// The node at the main chain's 2nd header, the source at its 8th dialing
/// the node, under the converging policy, with `controls`.
fn scenario(controls: Vec<ControlStep>) -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(8),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        }],
        weights: Weights::default(),
        policy: Policy::converging(),
        controls,
    }
}

/// One step of a run as a checker saw it.
#[derive(Clone, Debug)]
struct Seen {
    step: u64,
    pick: String,
    enabled: Vec<String>,
    now: SimInstant,
    node_tip: u32,
}

/// A run and what it left.
struct Run {
    report: RunReport,
    records: Vec<StepRecord>,
    seen: Vec<Seen>,
    recovery: RecoveryEvidence,
    sessions: SessionEvidence,
    node: Arc<ControlledPort>,
}

/// Runs `scenario` under `input`, seeing every step.
fn run(scenario: &StorageScenario, input: PlanInput) -> Run {
    let ports = Storage::prepare(scenario);
    let (recovery, sessions, node) = (
        ports.recovery.clone(),
        ports.sessions.clone(),
        ports.node.clone(),
    );
    let seen = Rc::new(RefCell::new(Vec::new()));
    let (into, port) = (seen.clone(), node.clone());
    let mut checker = after_step(move |cx| {
        into.borrow_mut().push(Seen {
            step: cx.step,
            pick: cx.pick.to_string(),
            enabled: cx.enabled.iter().map(|e| e.identity.to_string()).collect(),
            now: cx.now,
            node_tip: port.engine().snapshot().frontiers.header_best.height.0,
        });
        None
    });
    let mut sink = MemorySink::default();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut sink, Some(&mut checker))
        .unwrap_or_else(|error| panic!("{error}"));
    let records = sink
        .records
        .into_iter()
        .filter_map(|record| match record {
            Record::Step(step) => Some(step),
            _ => None,
        })
        .collect();
    let seen = seen.take();
    Run {
        report,
        records,
        seen,
        recovery,
        sessions,
        node,
    }
}

impl Run {
    /// The first step that picked `identity`.
    fn picked(&self, identity: &str) -> Option<&Seen> {
        self.seen.iter().find(|seen| seen.pick == identity)
    }

    /// Checks every rule a recovery witness keeps, and returns its record.
    fn recovered(&self) -> RecoveryRecord {
        self.recovered_at().0
    }

    /// [`recovered`](Self::recovered), and when the recover, the
    /// `recovered` pick and the node's first apply of the source's tip came.
    fn recovered_at(&self) -> (RecoveryRecord, [SimInstant; 3]) {
        let end = &self.report.end;
        assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
        let started = self
            .picked(&format!("peer:{SOURCE}:recover"))
            .expect("the recover is picked")
            .clone();
        let done = self
            .picked("peer:node:recovered")
            .expect("recovery reaches its conditions")
            .clone();
        assert!(started.step < done.step);
        for seen in &self.seen {
            if seen.enabled.iter().any(|e| e == "peer:node:recovered") {
                // Offered only at a quiet step, after the recover.
                assert!(seen.step > started.step);
                assert!(
                    !seen.enabled.iter().any(|e| e.starts_with("task:")),
                    "step {}: {:?}",
                    seen.step,
                    seen.enabled
                );
            }
            if seen.step > started.step && seen.pick.starts_with("peer:") {
                // No control step after the recover, only recovery's own.
                assert!(
                    seen.pick == "peer:node:recovered"
                        || seen.pick == format!("peer:{SOURCE}:recovery-relink"),
                    "step {}: {}",
                    seen.step,
                    seen.pick
                );
            }
        }
        for record in self.records.iter().filter(|r| r.step > started.step) {
            let fair = if record.options == 1 {
                By::Forced
            } else {
                By::Recovery
            };
            assert_eq!(record.by, fair, "step {}", record.step);
        }
        let record = self.recovery.record().expect("the pick wrote its record");
        assert_eq!(record.source.peer, SOURCE);
        assert_eq!(record.source_tip.0.height.0, 8);
        assert!(record.membership.contains(&record.source));
        assert_eq!(
            record.source_chain.last(),
            Some(&record.source_tip.0),
            "the chain ends at the source's tip"
        );
        // The record is the pick's, and keeps every member's selected chain
        // and the node's, each read from its own engine, ending at its tip.
        assert_eq!((record.step, record.at), (done.step, done.now));
        let members: Vec<_> = record.members.iter().map(|m| m.member.clone()).collect();
        assert_eq!(members, record.membership);
        let source = record
            .members
            .iter()
            .find(|m| m.member == record.source)
            .expect("the source is a member");
        assert_eq!(source.selected.chain.last(), Some(&record.source_tip.0));
        assert_eq!(source.selected.work, record.source_tip.1);
        assert_eq!(record.node_chain.chain.last(), Some(&record.node_tip.0));
        assert_eq!(record.node_chain.work, record.node_tip.1);
        assert!(record
            .source_chain
            .iter()
            .all(|h| source.selected.chain.contains(h)));
        // The barrier opens only once no task is ready, so a node with a
        // live source has finished syncing from it: it's at the source's
        // tip at the record already, and still there at the end. Nothing
        // changes the membership or ends the source's session after it.
        assert_eq!(record.node_tip.0, record.source_tip.0);
        assert_eq!(
            self.node.engine().snapshot().frontiers.header_best,
            record.source_tip.0
        );
        let facts = self.sessions.facts();
        assert!(!membership_changed(&facts, record.facts), "{facts:?}");
        assert!(session_lasts(&facts, record.facts, &record.source));
        let reached = self
            .seen
            .iter()
            .find(|seen| seen.step >= started.step && seen.node_tip == 8)
            .expect("the node reaches the source's tip");
        (record, [started.now, done.now, reached.now])
    }
}

/// Every witness, by the prefix class it recovers from.
fn witnesses() -> Vec<(&'static str, StorageScenario)> {
    storage_witnesses::recovery_witnesses()
}

/// The storage facts `run` logged, with their steps.
fn storage_facts(run: &Run) -> Vec<(u64, StorageFact)> {
    run.report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { fact, .. } => Some((entry.step, fact.clone())),
            _ => None,
        })
        .collect()
}

/// After each prefix class, under 8 plans, recovery reaches its conditions
/// at a quiet step, the node reaches the source's recorded tip, and the
/// membership holds to the end. It prints when each run recovered and
/// reached the tip, in virtual time.
#[test]
#[allow(clippy::print_stdout)]
fn recovery_follows_every_prefix_class() {
    for (class, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let (_, [recover, recovered, tip]) = run(&scenario, input).recovered_at();
            println!("{class}: recover at {recover:?}, recovered at {recovered:?}, tip at {tip:?}");
        }
    }
}

/// Each witness's prefix shows its class at the recover: the source's link
/// closed, a held prepare or a committed reply still waiting, failed
/// prepares, or an invalidation in force. Recovery then undoes it.
#[test]
fn each_witness_recovers_from_its_class() {
    let classes: Vec<_> = witnesses();
    let input = || inputs(1, 256).remove(0);
    for (class, scenario) in classes {
        let run = run(&scenario, input());
        let recovered = run.recovered();
        let started = run.picked(&format!("peer:{SOURCE}:recover")).unwrap().step;
        let facts = storage_facts(&run);
        match class {
            "closes" => assert!(run
                .picked(&format!("peer:{SOURCE}:recovery-relink"))
                .is_some_and(|relink| relink.step > started)),
            "holds" | "reply holds" => {
                // The held operation completes only after the recover.
                let held = facts
                    .iter()
                    .find_map(|(_, fact)| match fact {
                        StorageFact::Committed { id, .. } if class == "reply holds" => Some(*id),
                        StorageFact::Submitted { id, request, .. }
                            if class == "holds"
                                && matches!(
                                    request,
                                    zakura_sim::observe::StorageRequest::PrepareHeaderTarget { .. }
                                ) =>
                        {
                            Some(*id)
                        }
                        _ => None,
                    })
                    .unwrap();
                let done = facts
                    .iter()
                    .find(|(_, fact)| matches!(fact, StorageFact::Completed { id, .. } if *id == held))
                    .unwrap();
                assert!(done.0 > started, "{class}: {done:?}");
            }
            "faults" => assert!(facts.iter().any(|(at, fact)| *at < started
                && matches!(fact, StorageFact::Completed { outcome, .. } if outcome != "engine"))),
            "invalidations" => {
                let resets: Vec<_> = facts
                    .iter()
                    .filter_map(|(at, fact)| match fact {
                        StorageFact::Reset { kind, .. } => Some((*at, kind.as_str())),
                        _ => None,
                    })
                    .collect();
                assert_eq!(resets.len(), 2, "{resets:?}");
                assert!(resets[0].0 < started && resets[0].1 == "invalidate");
                assert_eq!(resets[1], (started, "reconsider"));
                // The baseline is the node's chain before the reconsider,
                // without the invalidated tip the reconsider restores.
                let baseline = run
                    .recovery
                    .baseline()
                    .expect("the recover kept a baseline");
                let tip = *baseline.chain.last().expect("the chain has its anchor");
                assert!(tip.height < recovered.node_tip.0.height, "{baseline:?}");
                assert!(baseline.work < recovered.node_tip.1);
            }
            _ => unreachable!("every class is checked"),
        }
        assert_eq!(
            recovered.node_tip.0.height.0,
            run.seen
                .iter()
                .find(|s| s.pick == "peer:node:recovered")
                .unwrap()
                .node_tip
        );
    }
}

/// A budget that ends right after the `Recover` pick, before `recovered`,
/// leaves the recovery to the drain. The drain offers its quiet
/// `recovered` once no task is ready, and the run ends dry, with neither
/// a violation nor a harness failure. The holds witness keeps its source's
/// session live, since the drain doesn't relink a closed one.
#[test]
fn a_budget_cut_after_recover_still_reaches_recovered() {
    let (_, scenario) = witnesses()
        .into_iter()
        .find(|(class, _)| *class == "holds")
        .expect("a holds witness");
    let input = || inputs(1, 256).remove(0);
    let full = run(&scenario, input());
    let started = full
        .picked(&format!("peer:{SOURCE}:recover"))
        .expect("the recover is picked")
        .step;
    let config = zakura_sim::RunConfig {
        budget: started,
        ..storage_config()
    };
    let mut sim = Sim::new(config, input());
    let storage = Rc::new(Storage::build(
        &mut sim,
        &scenario,
        Storage::prepare(&scenario),
    ));
    let registry = Registry::new(storage_properties()).expect("the storage IDs are unique");
    let mut checker = RegistryChecker::new(
        registry,
        Rc::clone(&storage),
        Assumptions::none(),
        &storage_sources(),
    );
    let report = sim.run(&mut checker, &mut MemorySink::default());
    drop((checker, storage));
    let end = &report.end;
    assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
    let recovered = report
        .picks
        .iter()
        .find(|pick| pick.identity == "peer:node:recovered")
        .expect("the drain offers recovered");
    assert_eq!(recovered.phase, Phase::Drain);
    assert!(recovered.step > started);
}

/// The recover waits for the control task to carry out every earlier step.
/// After the growth's pick, the control task runs before the recover is
/// first offered, and while the recover is offered the task is at most
/// ready to wait for its next step.
#[test]
fn the_recover_waits_for_earlier_steps() {
    let scenario = scenario(vec![
        step(SOURCE, ControlAction::Grow { len: 2 }, None),
        recover(None),
    ]);
    for input in inputs(8, 256) {
        let run = run(&scenario, input);
        assert_eq!(run.report.end.harness, None);
        let grow = run.picked("peer:mirror-0:grow").unwrap().step;
        let offered: Vec<&Seen> = run
            .seen
            .iter()
            .filter(|seen| seen.enabled.iter().any(|e| e == "peer:mirror-0:recover"))
            .collect();
        let first = offered.first().expect("the recover is offered").step;
        assert!(run.seen.iter().any(|seen| seen.step > grow
            && seen.step < first
            && seen.pick.starts_with("task:control.")));
        for seen in offered {
            assert!(
                seen.enabled
                    .iter()
                    .filter(|e| e.starts_with("task:control."))
                    .all(|e| e.contains(":recv#")),
                "step {}: {:?}",
                seen.step,
                seen.enabled
            );
        }
    }
}

/// A recover must come last, after no shutdown, churn or flood.
#[test]
fn a_recover_out_of_place_is_refused() {
    let cases = [
        vec![recover(None), step(SOURCE, ControlAction::Close, None)],
        vec![step(NODE, ControlAction::Shutdown, None), recover(None)],
        vec![step(SOURCE, ControlAction::Churn, None), recover(None)],
        vec![step(SOURCE, ControlAction::Flood, None), recover(None)],
    ];
    for controls in cases {
        let scenario = scenario(controls);
        let refused = std::panic::catch_unwind(|| Storage::prepare(&scenario));
        assert!(refused.is_err(), "{:?}", scenario.controls);
    }
}

/// A recover names a mirror, so one naming the node fails the run.
#[test]
fn a_recover_of_the_node_fails_the_run() {
    let scenario = scenario(vec![step(NODE, ControlAction::Recover, None)]);
    let failed = run_prepared::<Storage>(
        &scenario,
        Storage::prepare(&scenario),
        inputs(1, 64).remove(0),
        &mut MemorySink::default(),
        None,
    )
    .expect_err("the run fails")
    .to_string();
    assert!(failed.contains("a recover names a mirror"), "{failed}");
}

/// With a second mirror that the prefix relinks and then closes, the
/// relinked session's admission is still queued when its link closes. In
/// every plan the reactor then admits it and ends it, and all of that comes
/// before `recovered`, which never counts it as live.
#[test]
fn recovered_waits_for_other_links_to_settle() {
    let mut scenario = scenario(vec![
        step("mirror-1", ControlAction::Relink, None),
        step("mirror-1", ControlAction::Close, None),
        recover(None),
    ]);
    scenario.mirrors.push(MirrorSpec {
        chain: Chain::main(5),
        dialer: Dialer::Mirror,
        max_headers_per_response: None,
    });
    for input in inputs(16, 256) {
        let run = run(&scenario, input);
        let record = run.recovered();
        let facts = run.sessions.facts();
        // The relinked session: mirror-1's second link.
        let (connection, session) = facts
            .iter()
            .filter_map(|fact| match fact {
                SessionFact::LinkOpened {
                    peer,
                    connection,
                    session_id,
                    ..
                } if peer == "mirror-1" => Some((*connection, *session_id)),
                _ => None,
            })
            .nth(1)
            .expect("the prefix relinks mirror-1");
        let at = |found: &dyn Fn(&SessionFact) -> bool| {
            facts
                .iter()
                .position(found)
                .unwrap_or_else(|| panic!("session {session}: {facts:?}"))
        };
        let closed = at(
            &|fact| matches!(fact, SessionFact::LinkClosed { connection: c, .. } if *c == connection),
        );
        let admitted = at(
            &|fact| matches!(fact, SessionFact::SessionAdmitted { session_id, .. } if *session_id == session),
        );
        let ended = at(
            &|fact| matches!(fact, SessionFact::SessionEnded { session_id, .. } if *session_id == session),
        );
        assert!(closed < admitted && admitted < ended, "{facts:?}");
        assert!(ended < record.facts, "{facts:?}");
        assert_eq!(record.membership, live_sessions(&facts[..record.facts]));
        assert!(!record.membership.iter().any(|m| m.session_id == session));
    }
}

/// A scenario with a recover declares hdr-13's five assumptions, and one
/// without declares none.
#[test]
fn a_recover_declares_the_five_assumptions() {
    let with = Storage::assumptions(&scenario(vec![recover(None)]));
    for assume in [
        Assume::StableMembership,
        Assume::HonestPeer,
        Assume::BoundedStorage,
        Assume::BoundedDelivery,
        Assume::FairScheduling,
    ] {
        assert!(with.declares(assume), "{assume:?}");
    }
    let without = Storage::assumptions(&scenario(vec![step(SOURCE, ControlAction::Close, None)]));
    assert!(!without.declares(Assume::FairScheduling));
    assert!(!without.declares(Assume::StableMembership));
}

/// A link to `peer` on `connection` with session `session_id`.
fn opened(peer: &str, connection: u64, session_id: u64) -> SessionFact {
    SessionFact::LinkOpened {
        peer: peer.into(),
        connection,
        session_id,
        dialer: peer.into(),
    }
}

/// `peer`'s session `session_id`, admitted.
fn admitted(peer: &str, session_id: u64, replaced: Option<u64>) -> SessionFact {
    SessionFact::SessionAdmitted {
        peer: peer.into(),
        session_id,
        direction: zakura_network::zakura::ServicePeerDirection::Inbound,
        replaced,
        origin: None,
    }
}

/// The membership helpers over hand-built facts: a session the service ends
/// while its connection stays open changes the membership and ends the
/// source's session at that step, as a closed link or a replacement does.
/// They say what the node's sessions did. The assumptions read only the
/// environment's ends (`RecoveryEvidence::holds`).
#[test]
fn membership_and_the_source_follow_the_session_facts() {
    let source = Member {
        peer: SOURCE.into(),
        session_id: 1,
    };
    let before = vec![
        opened(SOURCE, 1, 1),
        admitted(SOURCE, 1, None),
        opened("mirror-1", 2, 2),
        admitted("mirror-1", 2, None),
    ];
    assert_eq!(live_sessions(&before).len(), 2);
    let at = before.len();
    assert!(!membership_changed(&before, at));
    assert!(session_lasts(&before, at, &source));

    // The service ends the source's session, and its link stays open.
    let mut ended = before.clone();
    ended.push(SessionFact::SessionEnded {
        peer: SOURCE.into(),
        session_id: 1,
        reason: "unproductive".into(),
    });
    assert!(membership_changed(&ended, at));
    assert!(!session_lasts(&ended, at, &source));
    assert_eq!(live_sessions(&ended).len(), 1);

    // Another peer's link closing changes the membership, not the source.
    let mut closed = before.clone();
    closed.push(SessionFact::LinkClosed {
        peer: "mirror-1".into(),
        connection: 2,
    });
    assert!(membership_changed(&closed, at));
    assert!(session_lasts(&closed, at, &source));

    // A session admitted after its link closed isn't live: the close
    // came first, so a lost disconnect can't make it the source.
    let late = vec![
        opened(SOURCE, 1, 1),
        SessionFact::LinkClosed {
            peer: SOURCE.into(),
            connection: 1,
        },
        admitted(SOURCE, 1, None),
    ];
    assert!(live_sessions(&late).is_empty());

    // A replacement ends the source's old session.
    let mut replaced = before.clone();
    replaced.extend([opened(SOURCE, 3, 3), admitted(SOURCE, 3, Some(1))]);
    assert!(!session_lasts(&replaced, at, &source));
    assert_eq!(
        live_sessions(&replaced),
        [
            Member {
                peer: SOURCE.into(),
                session_id: 3
            },
            Member {
                peer: "mirror-1".into(),
                session_id: 2
            }
        ]
    );
}

/// When each storage operation's limit falls in `run`, from the recover on:
/// for each operation that completed after the recover, the virtual time it
/// completed and the latest it may have, 1 s after the later of the recover
/// and its submission, or its commit for a held reply.
fn bounded_completions(run: &Run) -> Vec<(u64, SimInstant, SimInstant)> {
    let time = |step: u64| {
        run.seen
            .iter()
            .find(|seen| seen.step == step)
            .map_or(SimInstant::from_start(std::time::Duration::ZERO), |seen| {
                seen.now
            })
    };
    let started = run
        .picked(&format!("peer:{SOURCE}:recover"))
        .expect("the recover is picked");
    let mut begun = std::collections::BTreeMap::new();
    let mut found = Vec::new();
    for (step, fact) in storage_facts(run) {
        match fact {
            StorageFact::Submitted { id, .. } | StorageFact::Committed { id, .. } => {
                begun.insert(id, time(step));
            }
            StorageFact::Completed { id, .. } if step > started.step => {
                let from = begun[&id].max(started.now);
                let limit = from
                    .checked_add(zakura_sim::harness::header_node::recovery::RECOVERY_BOUND)
                    .unwrap();
                found.push((id, time(step), limit));
            }
            _ => {}
        }
    }
    found
}

/// Every operation that completes after the recover does so within 1 s of
/// the later of the recover and its own start: a held prepare and a held
/// reply that were waiting at the recover, and every operation submitted
/// after it. The held classes always have one. After an invalidation the
/// reconsideration puts the node back on its tip, so it may have none.
#[test]
fn recovery_bounds_every_storage_operation() {
    let mut total = 0;
    for (class, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let run = run(&scenario, input);
            let bounded = bounded_completions(&run);
            if matches!(class, "holds" | "reply holds" | "closes") {
                assert!(!bounded.is_empty(), "{class}");
            }
            total += bounded.len();
            for (id, at, limit) in bounded {
                assert!(
                    at <= limit,
                    "{class}: operation {id} at {at:?}, past {limit:?}"
                );
            }
        }
    }
    assert!(total > 40, "{total}");
}

/// Every pick and observation of `run`.
fn history(run: &Run) -> Vec<String> {
    run.seen
        .iter()
        .map(|seen| format!("{} {}", seen.step, seen.pick))
        .chain(
            run.report
                .log
                .entries()
                .iter()
                .map(|entry| format!("{} {:?}", entry.step, entry.obs)),
        )
        .collect()
}

/// A recovery depends only on its scenario and plan: A twice, and A, B, A
/// in one process, give A's history each time, and B's differs.
#[test]
fn recovery_repeats_a_a_and_a_b_a() {
    let scenario = &witnesses()[0].1;
    let plans = inputs(2, 256);
    let histories: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| history(&run(scenario, plan.clone())))
        .collect();
    assert!(histories[0] == histories[1]);
    assert!(histories[0] == histories[3]);
    assert!(histories[0] != histories[2]);
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// The step records of the trace in `dir`, without its meta line, which
/// names the trace and its recording.
fn step_lines(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("trace.jsonl"))
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with(r#"{"meta""#))
        .map(str::to_owned)
        .collect()
}

/// `record` runs a recovery witness from its bytes in one process, a second
/// `record` recomputes it from the same bytes in another, and `replay`
/// reruns the first trace Exact in a third. The two recordings make the
/// same picks and draws, so the policy recomputes them from the bytes, not
/// only from a record.
#[test]
fn recovery_replays_exact_in_a_fresh_process() {
    let PlanInput::Bytes(plan) = &inputs(1, 256)[0] else {
        unreachable!("fuzz inputs are bytes");
    };
    for (class, scenario) in witnesses() {
        let dir = TempDir::new().unwrap();
        let plan_file = dir.path().join("plan");
        std::fs::write(&plan_file, plan).unwrap();
        let record = |out: &std::path::Path| {
            zakura_sim(&[
                "record",
                "--sim",
                Storage::NAME,
                "--scenario",
                &serde_json::to_string(&scenario).unwrap(),
                "--plan",
                plan_file.to_str().unwrap(),
                "--out",
                out.to_str().unwrap(),
            ])
        };
        let (trace, again) = (dir.path().join("trace"), dir.path().join("again"));
        for out in [&trace, &again] {
            let recorded = record(out);
            assert!(recorded.status.success(), "{class}: {recorded:?}");
        }
        assert_eq!(step_lines(&trace), step_lines(&again), "{class}");
        let lines = std::fs::read_to_string(trace.join("trace.jsonl")).unwrap();
        assert!(lines.contains(r#""by":"recovery""#), "{class}");
        assert!(lines.contains("peer:node:recovered"), "{class}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{class}: {replayed:?}"
        );
    }
}

/// Generator 3 encodes a recover, so every witness round-trips through its
/// canonical prefix, and the fuzzer can reach each one.
#[test]
fn the_grammar_encodes_every_recovery_witness() {
    for (class, scenario) in witnesses() {
        let prefix = grammar::encode(&scenario).unwrap_or_else(|error| panic!("{class}: {error}"));
        assert_eq!(grammar::decode(&prefix).unwrap(), scenario, "{class}");
    }
}
