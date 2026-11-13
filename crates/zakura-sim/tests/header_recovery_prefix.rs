//! hdr-13's prefix bound: a `header-sync/storage` scenario with a `Recover`
//! step executes at most `PREFIX_BOUND` hostile events before its pick. The
//! harness records each where it executes: the control source's pick, a
//! hold's catch at the operation's submission, and a fault's completion.
//! Each test joins that record with the run's log, both ways, so neither
//! can claim an event the other doesn't show.
#![cfg(zakura_sim)]

mod support;

use std::collections::BTreeMap;

use support::{
    byte_inputs, inputs,
    storage_witnesses::{hold, recover, recovering, reset, SOURCE},
};
use zakura_sim::{
    domain::header_sync_progress::BOUNDED_ADVERSARY_PROGRESS,
    harness::{
        header_node::{
            policy::{Allow, OpKind, ReplyHold, Select, Trigger},
            recovery::{Hostile, HostileEvent, PREFIX_BOUND},
            Base, ControlAction, ControlStep, Reset,
        },
        header_sync::{grammar, storage_config, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The bound, as a count.
fn bound() -> usize {
    usize::try_from(PREFIX_BOUND).expect("the bound fits a usize")
}

/// A hold that fails every prepare, then once `after` has completed,
/// `steps` reconsiders, each a control step that changes nothing, since
/// nothing is invalidated, then a recover, or none.
fn scenario(steps: usize, after: Trigger, recovers: bool) -> StorageScenario {
    let mut controls: Vec<_> = (0..steps)
        .map(|_| reset(Reset::Reconsider, after.clone()))
        .collect();
    if recovers {
        controls.push(recover(None));
    }
    let mut scenario = recovering(controls);
    scenario.policy.holds.push(hold(
        Select {
            kind: Some(OpKind::Prepare),
            ..Select::default()
        },
        Trigger::VirtualTime(0),
        Allow::LocalResource,
    ));
    scenario
}

/// [`scenario`] after two prepares have failed, four hostile events, so
/// thirty reconsiders take the prefix past the bound.
fn failing(steps: usize, recovers: bool) -> StorageScenario {
    scenario(
        steps,
        Trigger::Completed(Select::nth(OpKind::Prepare, 2)),
        recovers,
    )
}

/// What a run's prefix did.
#[derive(Debug)]
struct Prefix {
    /// The harness's record, or `None` for an unbounded prefix.
    record: Option<Vec<HostileEvent>>,
    /// The step of the recover pick, if it came.
    recovered: Option<u64>,
}

/// Runs `scenario` under `input`, which must end with no violation or
/// harness failure, and checks its record against its log
/// ([`check_record`]).
fn run(scenario: &StorageScenario, input: PlanInput) -> Prefix {
    let ports = Storage::prepare(scenario);
    let recovery = ports.recovery.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    let end = &report.end;
    assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
    let recovered = report
        .picks
        .iter()
        .find(|pick| pick.identity == format!("peer:{SOURCE}:recover"))
        .map(|pick| pick.step);
    let record = recovery.prefix().events();
    if let Some(record) = &record {
        check_record(scenario, &report, record, recovered);
    }
    Prefix { record, recovered }
}

/// Joins `record` with `report`'s log, before the recover pick at
/// `recovered`:
///
/// - every control step picked is recorded as `Step` at its pick, and
///   nothing else is;
/// - every completion the log shows faulted is recorded as `Fault` at its
///   step, unless its only legal outcome was `rejected`, and every `Fault`
///   recorded is one the log shows;
/// - every operation a hold of `scenario` selects is recorded as `Caught` at
///   its submission while fewer than the bound were recorded, and none is
///   after. One recorded once the bound is reached would be the 33rd. A
///   prepare not caught never fails, and an apply not caught commits no
///   held reply;
/// - nothing is recorded at or after the recover pick, or past the bound.
fn check_record(
    scenario: &StorageScenario,
    report: &RunReport,
    record: &[HostileEvent],
    recovered: Option<u64>,
) {
    assert!(record.len() <= bound(), "{record:?}");
    let before = |step: u64| recovered.is_none_or(|at| step < at);
    assert!(record.iter().all(|event| before(event.step)), "{record:?}");
    let recorded = |hostile: Hostile| record.iter().find(|event| event.hostile == hostile);
    // How many were recorded before `step`.
    let spent_before = |step: u64| record.iter().filter(|event| event.step < step).count();
    let picked: Vec<u64> = report
        .picks
        .iter()
        .filter(|pick| {
            before(pick.step)
                && pick.identity.starts_with("peer:")
                && !pick.identity.ends_with(":recover")
                && pick.identity != "peer:node:finish"
        })
        .map(|pick| pick.step)
        .collect();
    let steps: Vec<u64> = record
        .iter()
        .filter(|event| matches!(event.hostile, Hostile::Step(_)))
        .map(|event| event.step)
        .collect();
    assert_eq!(picked, steps, "control picks against recorded steps");
    let prepare_hold = scenario
        .policy
        .holds
        .iter()
        .any(|hold| hold.select.kind == Some(OpKind::Prepare));
    let reply_hold = !scenario.policy.reply_holds.is_empty();
    let mut kinds: BTreeMap<u64, OpKind> = BTreeMap::new();
    let mut uncaught = Vec::new();
    let mut faulted = Vec::new();
    for entry in report
        .log
        .entries()
        .iter()
        .filter(|entry| before(entry.step))
    {
        let Observation::Storage { fact, .. } = &entry.obs else {
            continue;
        };
        match fact {
            StorageFact::Submitted { id, request, .. } => {
                let kind = match request {
                    StorageRequest::PrepareHeaderTarget { .. } => OpKind::Prepare,
                    StorageRequest::ApplyHeaderTarget { .. } => OpKind::Apply,
                    _ => continue,
                };
                kinds.insert(*id, kind);
                let held = (kind == OpKind::Prepare && prepare_hold)
                    || (kind == OpKind::Apply && reply_hold);
                if !held {
                    continue;
                }
                let caught = recorded(Hostile::Caught(*id));
                if spent_before(entry.step) < bound() {
                    assert_eq!(
                        caught.map(|event| event.step),
                        Some(entry.step),
                        "operation {id} is caught at its submission"
                    );
                } else {
                    assert!(caught.is_none(), "operation {id} is the 33rd: {record:?}");
                    uncaught.push(*id);
                }
            }
            StorageFact::Committed { id, .. } => {
                assert!(!uncaught.contains(id), "uncaught apply {id} held its reply");
            }
            StorageFact::Completed { id, outcome, .. } if outcome != "engine" => {
                assert!(!uncaught.contains(id), "uncaught operation {id} failed");
                faulted.push(*id);
                match recorded(Hostile::Fault(*id)) {
                    Some(event) => assert_eq!(event.step, entry.step, "operation {id}"),
                    None => assert_eq!(outcome, "rejected", "operation {id}'s fault is recorded"),
                }
            }
            _ => {}
        }
    }
    for event in record {
        match event.hostile {
            Hostile::Caught(id) => assert!(kinds.contains_key(&id), "caught {id} was submitted"),
            Hostile::Fault(id) => assert!(faulted.contains(&id), "fault {id} is in the log"),
            Hostile::Step(_) => {}
        }
    }
}

/// Checks a bounded prefix that reached the bound and recovered.
fn assert_full(prefix: &Prefix) {
    assert!(prefix.recovered.is_some(), "{prefix:?}");
    let record = prefix.record.as_ref().expect("a recover bounds the prefix");
    assert_eq!(record.len(), bound(), "{record:?}");
}

/// A typed script longer than the bound runs only up to it: the steps
/// after are dropped, the recover still comes, and the prepares the hold
/// failed meanwhile count too.
#[test]
fn a_typed_prefix_stops_at_the_bound() {
    for input in inputs(8, 256) {
        let prefix = run(&failing(40, true), input);
        assert_full(&prefix);
        let steps = prefix
            .record
            .iter()
            .flatten()
            .filter(|event| matches!(event.hostile, Hostile::Step(_)))
            .count();
        assert!(steps < 40, "{prefix:?}");
    }
}

/// Once the bound is reached the hold catches nothing new, so a prepare
/// submitted after it succeeds. Here the recover waits for the node's first
/// apply, which the hold alone would never allow.
#[test]
fn a_full_prefix_lets_later_operations_through() {
    let mut scenario = failing(30, true);
    scenario
        .controls
        .last_mut()
        .expect("the scenario ends in its recover")
        .when = Some(Trigger::Completed(Select::nth(OpKind::Apply, 1)));
    for input in inputs(8, 256) {
        assert_full(&run(&scenario, input));
    }
}

/// The same kind of scenario decoded from its generator-3 prefix stops at
/// the bound too. Thirty reconsiders, the hold and the recover take all 32
/// slots, and the two failed prepares take the prefix past 32.
#[test]
fn a_decoded_prefix_stops_at_the_bound() {
    let typed = failing(30, true);
    let encoded = grammar::encode(&typed).expect("generator 3 encodes it");
    let decoded = Storage::draw(&encoded);
    assert_eq!(decoded, typed);
    for input in inputs(8, 256) {
        assert_full(&run(&decoded, input));
    }
}

/// A `Recover` whose trigger waits on a step the cutoff skips still comes.
/// Here the two failed prepares (four events) and 28 reconsiders fill the
/// prefix just before the source's growth, and the recover waits for that
/// growth. The cutoff skips the growth, and the recover comes at once,
/// typed and decoded alike.
#[test]
fn a_full_prefix_recovers_even_when_its_trigger_was_skipped() {
    let mut typed = failing(28, false);
    typed.controls.push(ControlStep {
        peer: SOURCE.to_owned(),
        action: ControlAction::Grow { len: 2 },
        when: None,
    });
    typed.controls.push(ControlStep {
        when: Some(Trigger::Grown(SOURCE.to_owned())),
        ..recover(None)
    });
    let encoded = grammar::encode(&typed).expect("generator 3 encodes it");
    let decoded = Storage::draw(&encoded);
    assert_eq!(decoded, typed);
    for scenario in [&typed, &decoded] {
        for input in inputs(8, 256) {
            let prefix = run(scenario, input);
            assert_full(&prefix);
            assert!(
                !prefix
                    .record
                    .iter()
                    .flatten()
                    .any(|event| event.hostile == Hostile::Step(28)),
                "the growth was skipped: {prefix:?}"
            );
        }
    }
}

/// A `Recover` whose trigger waits on node work that a stall keeps from
/// coming still comes, at the step cutoff, and its recovery is judged
/// (#2355). The first prepare is held with no release, and the recover waits
/// for it to complete, so only the cutoff, `storage_config()`'s budget, can
/// start recovery. One catch is the whole prefix, far from the bound, so
/// it's the cutoff and not a full prefix that ends it. The `Recover` pick
/// retires the hold, the node syncs to its source, and the property holds.
/// Before the cutoff offered the `Recover`, the run finished at that step
/// with nothing picked, and the property never applied.
#[test]
fn a_stalled_trigger_recovers_at_the_step_cutoff() {
    let mut scenario = recovering(vec![recover(Some(Trigger::Completed(Select::nth(
        OpKind::Prepare,
        1,
    ))))]);
    scenario.policy.holds.push(hold(
        Select {
            kind: Some(OpKind::Prepare),
            ..Select::default()
        },
        Trigger::Released,
        Allow::Engine,
    ));
    for input in inputs(8, 256) {
        let ports = Storage::prepare(&scenario);
        let recovery = ports.recovery.clone();
        let report =
            run_prepared::<Storage>(&scenario, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        let end = &report.end;
        assert_eq!((&end.violation, &end.harness), (&None, &None), "{end:?}");
        let recovered = report
            .picks
            .iter()
            .find(|pick| pick.identity == format!("peer:{SOURCE}:recover"))
            .map(|pick| pick.step)
            .expect("the cutoff offers the recover");
        assert!(
            recovered >= storage_config().budget,
            "the recover came at step {recovered}, before the cutoff"
        );
        let spent = recovery.prefix().spent().expect("the prefix is bounded");
        assert!(spent < PREFIX_BOUND, "the prefix filled: {spent}");
        let outcome = &report
            .properties
            .iter()
            .find(|reported| reported.property == BOUNDED_ADVERSARY_PROGRESS)
            .expect("the property reports")
            .outcome;
        assert!(
            matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
            "{outcome:?}"
        );
    }
}

/// Faults the `any` base offers with no hold, and a reply hold that catches
/// every apply, count too. With no prepare hold, `any` faults and reply
/// catches come as the node syncs, and the reconsiders start after its first
/// apply commits, so together they take the prefix past the bound, typed and
/// decoded alike, under long plans whose bytes pick faults. Some plan shows
/// each kind recorded, and some reaches the bound. A plan whose faults keep
/// the first apply from committing never recovers, and the record still
/// matches its log.
#[test]
fn faults_and_reply_holds_count_toward_the_bound() {
    let mut typed = scenario(30, Trigger::Committed(Select::nth(OpKind::Apply, 1)), true);
    typed.policy.holds.clear();
    typed.policy.base = Base::Any;
    typed.policy.reply_holds.push(ReplyHold {
        select: Select {
            kind: Some(OpKind::Apply),
            ..Select::default()
        },
        until: Trigger::VirtualTime(3_600_000),
    });
    let encoded = grammar::encode(&typed).expect("generator 3 encodes it");
    let decoded = Storage::draw(&encoded);
    assert_eq!(decoded, typed);
    let (mut faults, mut catches, mut full) = (0, 0, 0);
    for bytes in byte_inputs(8, 4_096) {
        for scenario in [&typed, &decoded] {
            let prefix = run(scenario, Plan::from_fuzz(&bytes));
            let record = prefix.record.clone().expect("a recover bounds the prefix");
            if record.len() == bound() {
                full += 1;
            }
            faults += record
                .iter()
                .filter(|event| matches!(event.hostile, Hostile::Fault(_)))
                .count();
            catches += record
                .iter()
                .filter(|event| matches!(event.hostile, Hostile::Caught(_)))
                .count();
        }
    }
    assert!(
        faults > 0 && catches > 0 && full > 0,
        "faults {faults}, catches {catches}, full {full}"
    );
}

/// Without a recover the prefix is unbounded: the harness records nothing,
/// and the same script and hold go past the bound.
#[test]
fn a_scenario_without_a_recover_is_unbounded() {
    for input in inputs(4, 256) {
        let scenario = failing(30, false);
        let ports = Storage::prepare(&scenario);
        let recovery = ports.recovery.clone();
        let report =
            run_prepared::<Storage>(&scenario, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(recovery.prefix().events(), None);
        let picks = report
            .picks
            .iter()
            .filter(|pick| pick.identity == "peer:node:reset")
            .count();
        let faults = report
            .log
            .entries()
            .iter()
            .filter(|entry| {
                matches!(&entry.obs, Observation::Storage {
                    fact: StorageFact::Completed { outcome, .. }, ..
                } if outcome != "engine")
            })
            .count();
        assert!(
            picks + 2 * faults > bound(),
            "{picks} picks, {faults} faults"
        );
    }
}
