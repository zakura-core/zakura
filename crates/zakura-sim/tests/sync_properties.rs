//! xsync-2: the shared-chain and epoch properties on `sync/storage`, where
//! the node's own header sync drives every reorganization block sync sees.
//! Each property's trigger history is reached by a committed saved
//! scenario, and holds there.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use support::{
    inputs,
    steer::{steer, Steer},
};
use zakura_sim::{
    domain::{
        block_sync::TEARDOWN_RETURNS_BASELINE as BLOCK_TEARDOWN_RETURNS_BASELINE,
        block_sync_semantic::{SemanticModel, FRONTIER_WORK_CONSISTENT, QUERY_WORK_AUTHORIZED},
        cross::{
            EpochHistory, BLOCK_REQUESTS_FOLLOW_HEADERS, HEADER_REORG_RETIRES_BLOCK_WORK,
            TEARDOWN_RETURNS_BASELINE, TIPS_CONSISTENT,
        },
    },
    harness::{
        block_node::policy::Trigger,
        run_prepared,
        sync::{SavedSync, SyncPorts, SyncStorage},
        HarnessError, Subsystem,
    },
    observe::{ReactorCall, ReactorFact},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport, StepCtx,
};

/// Runs saved scenario `saved` under `input`. Fails the test on a harness
/// failure.
fn run(saved: SavedSync, input: PlanInput) -> RunReport {
    run_with_ports(saved, input).0
}

/// [`run`], which also returns the ports, which outlive the run.
fn run_with_ports(saved: SavedSync, input: PlanInput) -> (RunReport, SyncPorts) {
    let scenario = saved.scenario();
    let ports = SyncStorage::prepare(&scenario);
    let sink = &mut MemorySink::default();
    match run_prepared::<SyncStorage>(&scenario, ports.clone(), input, sink, None) {
        Ok(report) => (report, ports),
        Err(HarnessError::Run { failure, report }) => panic!("{failure}: {:?}", report.end),
        Err(error) => panic!("{error}"),
    }
}

/// `property`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, property: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|report| report.property == property)
        .unwrap_or_else(|| panic!("the run reports {property}"))
        .outcome
}

/// Asserts that `property` held at least once in `report`.
fn applicable_and_held(report: &RunReport, property: &str) {
    assert!(
        matches!(
            outcome(report, property),
            PropertyOutcome::Held { applicable } if *applicable > 0
        ),
        "{property}: {:?}",
        outcome(report, property)
    );
}

/// `block-sync/query-work-authorized`'s witness: storage holds the node's
/// first needed-block query while the mirrors' chain grows. Header sync's
/// apply publishes a committed view with a new header target, and the
/// reactor's adoption retires the query under the same body-work epoch, so
/// its reply reaches the reactor under a retired authority and installs
/// nothing. Under the committed input and every generated Plan.
#[test]
fn a_reply_under_a_retired_authority_installs_nothing() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(4, 256)) {
        let report = run(SavedSync::RetiredQuery, input);
        let model = SemanticModel::of(&report.log);
        assert!(model.authority.saw_retired_reply, "{:?}", report.end);
        applicable_and_held(&report, QUERY_WORK_AUTHORIZED);
    }
}

/// `block-sync/frontier-work-consistent`'s witness: the mirrors' fork
/// reaches the node while block sync holds body work above the fork point.
/// The reactor adopts the new body-work epoch, and once no task can run,
/// no owner holds the old epoch's work above the verified tip.
#[test]
fn an_epoch_change_retires_the_old_epochs_work() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(4, 256)) {
        let report = run(SavedSync::Reorg, input);
        let model = SemanticModel::of(&report.log);
        assert!(
            model.authority.saw_epoch_change_with_work,
            "{:?}",
            report.end
        );
        applicable_and_held(&report, FRONTIER_WORK_CONSISTENT);
    }
}

/// `sync/tips-consistent`'s witness: the mirrors' fork reaches the engine
/// through the node's header sync, which prepares a branch forking below
/// its earlier target. The fixture DAG shows that a sampled view's selected
/// path replaced the last one's, and the engine published it under a new
/// body-work epoch.
#[test]
fn a_replaced_selection_starts_a_new_body_work_epoch() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(4, 256)) {
        let (report, ports) = run_with_ports(SavedSync::Reorg, input);
        assert!(
            ports.cross.borrow().saw_replaced_selection,
            "{:?}",
            report.end
        );
        assert!(
            EpochHistory::of(&report.log).epoch_changed,
            "{:?}",
            report.end
        );
        applicable_and_held(&report, TIPS_CONSISTENT);
    }
}

/// `sync/block-requests-follow-headers`' and
/// `sync/header-reorg-retires-block-work`'s witness at every pipeline stage
/// the harness can hold: the mirrors' fork reaches the node while storage
/// holds its first needed-block query (query), while a mirror holds a
/// published request's range read (request, and receive once the body
/// arrives after the reset), while storage holds a queued submission
/// (submit), and while storage holds an admitted apply (verdict). After the
/// sequencer applies the new body-work epoch, the node publishes and
/// submits only the new epoch's work, and both properties hold.
#[test]
fn a_reorg_at_each_pipeline_stage_retires_old_epoch_work() {
    let committed = Plan::from_fuzz(&[]);
    for saved in [
        SavedSync::Reorg,
        SavedSync::ReorgQuery,
        SavedSync::ReorgRequest,
        SavedSync::ReorgSubmit,
        SavedSync::ReorgVerdict,
    ] {
        for input in std::iter::once(committed.clone()).chain(inputs(2, 256)) {
            let (report, ports) = run_with_ports(saved, input);
            let history = EpochHistory::of(&report.log);
            let cross = ports.cross.borrow();
            match saved {
                SavedSync::ReorgQuery => assert!(
                    SemanticModel::of(&report.log).authority.saw_retired_reply,
                    "{}: the held query's reply arrives under a retired authority",
                    saved.name()
                ),
                SavedSync::ReorgSubmit | SavedSync::ReorgVerdict => assert!(
                    cross.saw_detached_submission,
                    "{}: the held submission outlives the epoch change detached",
                    saved.name()
                ),
                _ => {}
            }
            assert!(
                history.applied && history.request_after && history.submission_after,
                "{}: {history:?}: {:?}",
                saved.name(),
                report.end
            );
            applicable_and_held(&report, BLOCK_REQUESTS_FOLLOW_HEADERS);
            applicable_and_held(&report, HEADER_REORG_RETIRES_BLOCK_WORK);
        }
    }
}

/// Asserts that `report` held `sync/teardown-returns-baseline`, violated
/// nothing, and that its node's first header prepare, the held one, ended
/// `dropped`: the stopped reactor dropped the call unfinished. A later
/// prepare, from the other mirror after the round's deadline, may come
/// before a late shutdown.
fn dropped_a_held_prepare(label: &str, report: &RunReport) {
    assert_eq!(report.end.violation, None, "{label}: {:?}", report.end);
    applicable_and_held(report, TEARDOWN_RETURNS_BASELINE);
    let facts: Vec<&ReactorFact> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor { event, .. } => Some(event),
            _ => None,
        })
        .collect();
    let prepares: Vec<u64> = facts
        .iter()
        .filter_map(|fact| match fact {
            ReactorFact::Dispatched {
                call,
                port: ReactorCall::Prepare { .. },
            } => Some(*call),
            _ => None,
        })
        .collect();
    let Some(&prepare) = prepares.first() else {
        panic!("{label}: the node prepares: {prepares:?}");
    };
    assert!(
        facts.iter().any(|fact| matches!(
            fact,
            ReactorFact::CallEnded { call, end, .. } if *call == prepare && end == "dropped"
        )),
        "{label}: the stopped reactor drops the held prepare's call"
    );
}

/// The header half of the teardown matrix: the node shuts down while header
/// storage holds its first prepare, so the stopped reactor drops that
/// prepare's call unfinished, and the call's headers return only through
/// that drop. `sync/teardown-returns-baseline` applies and holds under the
/// committed input and generated Plans.
#[test]
fn header_teardown_across_a_held_prepare_returns_baseline() {
    let committed = Plan::from_fuzz(&[]);
    for (index, input) in std::iter::once(committed).chain(inputs(4, 256)).enumerate() {
        dropped_a_held_prepare(
            &format!("plan {index}"),
            &run(SavedSync::TeardownHeaders, input),
        );
    }
}

/// When [`a_late_teardown_still_drops_the_held_prepare`] first offers the
/// shutdown, in virtual time: well past the round's 30 s request deadline.
const LATE: Duration = Duration::from_secs(120);

/// The teardown fixture with its shutdown taken as early as it can be,
/// steered: every step that can advance time does, and the shutdown runs as
/// soon as it's offered, so nothing but time's own bounds keeps it after
/// the prepare's submission. It still drops the held prepare.
#[test]
fn a_rushed_teardown_still_drops_the_held_prepare() {
    let rushed = |cx: &StepCtx<'_>| -> Result<Steer, String> {
        let names: Vec<String> = cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .collect();
        Ok(names
            .iter()
            .find(|name| name.ends_with(":teardown"))
            .or_else(|| names.iter().find(|name| name.starts_with("advance-time")))
            .map_or(Steer::Fine, |pick| Steer::Pick(pick.clone())))
    };
    let scenario = SavedSync::TeardownHeaders.scenario();
    let (report, _, _) = steer(Plan::from_fuzz(&[]), &rushed, &mut |plan, checker| {
        let mut sink = MemorySink::default();
        let report = match run_prepared::<SyncStorage>(
            &scenario,
            SyncStorage::prepare(&scenario),
            plan,
            &mut sink,
            Some(checker),
        ) {
            Ok(report) => report,
            Err(HarnessError::Run { report, .. }) => *report,
            Err(error) => panic!("{error}"),
        };
        (report, sink.lines())
    });
    dropped_a_held_prepare("rushed", &report);
}

/// The teardown fixture with its shutdown first offered at [`LATE`], after
/// the round's request deadline, under the committed input and generated
/// Plans. The round's deadline retires the round first, and the node may
/// prepare again from its other mirror, but however late the shutdown, the
/// held prepare can't finish first, so the stopped reactor still drops its
/// call.
#[test]
fn a_late_teardown_still_drops_the_held_prepare() {
    let mut scenario = SavedSync::TeardownHeaders.scenario();
    let late = u64::try_from(LATE.as_millis()).expect("120 s in milliseconds fits u64");
    for step in &mut scenario.control {
        step.when = Some(Trigger::VirtualTime(late));
    }
    let committed = Plan::from_fuzz(&[]);
    for (index, input) in std::iter::once(committed).chain(inputs(4, 256)).enumerate() {
        let report = match run_prepared::<SyncStorage>(
            &scenario,
            SyncStorage::prepare(&scenario),
            input,
            &mut MemorySink::default(),
            None,
        ) {
            Ok(report) => report,
            Err(HarnessError::Run { failure, report }) => {
                panic!("plan {index}: {failure}: {:?}", report.end)
            }
            Err(error) => panic!("plan {index}: {error}"),
        };
        dropped_a_held_prepare(&format!("late plan {index}"), &report);
    }
}

/// xsync-4's teardown matrix: the node shuts down while block storage holds
/// its first needed-block query, and while it holds its second body's apply.
/// Each held operation completes after the shutdown. Once both settle,
/// `block-sync/teardown-returns-baseline` holds: no block-sync charge
/// remains, session slots included. `sync/teardown-returns-baseline` holds
/// too: header sync's tap saw its teardown settle, with no reservation,
/// lease, budget charge or locator permit left. Neither storage holds an
/// operation of the node's at the end, and no active property is violated.
#[test]
fn teardown_across_held_work_returns_baseline() {
    let committed = Plan::from_fuzz(&[]);
    for saved in [SavedSync::TeardownQuery, SavedSync::TeardownSubmission] {
        for input in std::iter::once(committed.clone()).chain(inputs(4, 256)) {
            let (report, ports) = run_with_ports(saved, input);
            assert_eq!(report.end.violation, None, "{}", saved.name());
            applicable_and_held(&report, BLOCK_TEARDOWN_RETURNS_BASELINE);
            applicable_and_held(&report, TEARDOWN_RETURNS_BASELINE);
            assert_eq!(ports.port.pending(), Vec::new(), "{}", saved.name());
            assert_eq!(ports.service.pending(), Vec::new(), "{}", saved.name());
        }
    }
}
