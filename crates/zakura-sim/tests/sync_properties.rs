//! xsync-2: the shared-chain and epoch properties on `sync/storage`, where
//! the node's own header sync drives every reorganization block sync sees.
//! Each property's trigger history is reached by a committed saved
//! scenario, and holds there.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    domain::block_sync_semantic::{SemanticModel, FRONTIER_WORK_CONSISTENT, QUERY_WORK_AUTHORIZED},
    harness::{
        run_prepared,
        sync::{SavedSync, SyncStorage},
        HarnessError, Subsystem,
    },
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Runs saved scenario `saved` under `input`. Fails the test on a harness
/// failure.
fn run(saved: SavedSync, input: PlanInput) -> RunReport {
    let scenario = saved.scenario();
    let ports = SyncStorage::prepare(&scenario);
    match run_prepared::<SyncStorage>(&scenario, ports, input, &mut MemorySink::default(), None) {
        Ok(report) => report,
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
/// first needed-block query until the mirrors' fork has reached the node's
/// header tip and the node has gone idle. Header sync's apply publishes a
/// committed view with a new authority, and the reactor's adoption retires
/// the query, so its reply reaches the reactor under a retired authority
/// and installs nothing. Under the committed
/// input and every generated Plan.
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
