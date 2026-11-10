//! xsync-2: the shared-chain and epoch properties on `sync/storage`, where
//! the node's own header sync drives every reorganization block sync sees.
//! Each property's trigger history is reached by a committed saved
//! scenario, and holds there.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    domain::{
        block_sync_semantic::{SemanticModel, FRONTIER_WORK_CONSISTENT, QUERY_WORK_AUTHORIZED},
        cross::{
            EpochHistory, BLOCK_REQUESTS_FOLLOW_HEADERS, HEADER_REORG_RETIRES_BLOCK_WORK,
            TIPS_CONSISTENT,
        },
    },
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
/// through the node's header sync, and the engine publishes a view whose
/// selected path replaced the last one's under a new body-work epoch.
#[test]
fn a_replaced_selection_starts_a_new_body_work_epoch() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(4, 256)) {
        let report = run(SavedSync::Reorg, input);
        assert!(
            EpochHistory::of(&report.log).epoch_changed,
            "{:?}",
            report.end
        );
        applicable_and_held(&report, TIPS_CONSISTENT);
    }
}

/// The Draft `sync/block-requests-follow-headers` and
/// `sync/header-reorg-retires-block-work` apply and hold once the fork
/// reaches block sync: the node requests and submits the fork's bodies
/// under the new body-work epoch, each the fork's block at its height.
#[test]
fn work_after_a_reorg_follows_the_new_branch() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(4, 256)) {
        let report = run(SavedSync::Reorg, input);
        let history = EpochHistory::of(&report.log);
        assert!(
            history.request_after && history.submission_after,
            "{history:?}: {:?}",
            report.end
        );
        applicable_and_held(&report, BLOCK_REQUESTS_FOLLOW_HEADERS);
        applicable_and_held(&report, HEADER_REORG_RETIRES_BLOCK_WORK);
    }
}
