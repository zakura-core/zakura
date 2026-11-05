//! hdr-4's fault attribution on the node's header storage: a failed prepare
//! or apply keeps whom its error blamed in the storage source's view-side
//! [`FaultAttributions`], while the logged result keeps its category alone.
//! The penalty rule reads both.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use support::{config, inputs, run};
use zakura_chain::{block, work::difficulty::CompactDifficulty};
use zakura_header_chain::{ErrorCategory, Frontier, SourceId};
use zakura_node_services::header_chain::Port;
use zakura_scheduler::SchedulerConfig;
use zakura_sim::{
    harness::header_node::{
        adoption::Acceptance,
        fault::{owed, FaultEvidence, OwedPenalty},
    },
    observe::{StorageFact, StorageResult},
    storage::{Blame, HeaderStorage},
    NoChecks, Observation, Outcome, Plan, PlanInput, RunConfig, RunReport,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

/// The supplier every request names.
const SOURCE: [u8; 32] = [9; 32];

/// 2 workers with the default stack, which engine planning needs.
fn storage_config() -> RunConfig {
    let mut config = config(2, 400);
    config.scheduler.stack_size = SchedulerConfig::default().stack_size;
    config
}

/// `header` with a target easier than Regtest's proof-of-work limit, a fault
/// preparation finds in the header alone.
fn easy_target(header: &block::Header) -> Arc<block::Header> {
    let mut header = *header;
    header.difficulty_threshold = CompactDifficulty::from_le_bytes(0x207f_ffff_u32.to_le_bytes());
    Arc::new(header)
}

/// `header` stamped with its parent's time, which isn't after the median
/// of its predecessors' times, a fault only admission against the retained
/// chain finds.
fn stale_time(header: &block::Header, parent: &block::Header) -> Arc<block::Header> {
    let mut header = *header;
    header.time = parent.time;
    Arc::new(header)
}

/// Runs a task that prepares `headers` on `parent` as request 1 and, if
/// that seals a target, applies it, each until the engine answers: a
/// storage fault a plan picks is retried. Returns the report and the
/// source's fault evidence, with the run's end recorded.
fn attempt(
    plan: PlanInput,
    parent: Frontier,
    headers: Vec<Arc<block::Header>>,
) -> (RunReport, FaultEvidence) {
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let mut faults = None;
    let report = run(
        storage_config(),
        plan,
        |sim| {
            let caller = port.clone();
            sim.scheduler().spawn_root("supplier", async move {
                let fixture = HeaderFixture::regtest();
                let target = loop {
                    let mut request =
                        fixture.target_request(&caller.engine().snapshot(), parent, &headers, 1);
                    request.source = SourceId::from_digest(SOURCE);
                    match caller.prepare_header_target(request).await {
                        Ok(target) => break target,
                        Err(error) if error.category == ErrorCategory::LocalResourceOrStorage => {}
                        Err(_) => return,
                    }
                };
                loop {
                    match caller.apply_header_target(target.clone()).await {
                        Err(error) if error.category == ErrorCategory::LocalResourceOrStorage => {}
                        _ => return,
                    }
                }
            });
            let storage = HeaderStorage::new(sim, port.clone());
            faults = Some(FaultEvidence::new(storage.attributions()));
            sim.add_source(Box::new(storage));
        },
        &mut NoChecks,
    );
    let faults = faults.expect("setup ran");
    faults.end(&report.end);
    (report, faults)
}

/// Each logged failure of the node's storage, as its operation ID and
/// result.
fn failures(report: &RunReport) -> Vec<(u64, StorageResult)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Completed {
                        id,
                        result:
                            result @ (StorageResult::PrepareFailed { .. }
                            | StorageResult::ApplyFailed { .. }),
                        ..
                    },
                ..
            } => Some((*id, result.clone())),
            _ => None,
        })
        .collect()
}

/// The default plan, then byte-chosen ones, which also pick storage faults.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(4, 64));
    plans
}

/// The supplier's source as the log writes it: its digest in hex.
fn source() -> String {
    SOURCE.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Asserts that `report` ended idle with exactly one engine failure, `last`,
/// whose operation blamed the supplier and owes one penalty from `stage`,
/// and that every other logged failure is a local storage fault that blamed
/// no peer and owes nothing.
fn assert_one_attributed(
    report: &RunReport,
    faults: &FaultEvidence,
    stage: &str,
    last: StorageResult,
) {
    assert!(
        matches!(report.end.outcome, Outcome::Idle { .. }),
        "{:?}",
        report.end
    );
    let failures = failures(report);
    let (op, result) = failures
        .last()
        .cloned()
        .expect("the supplier's work failed");
    assert_eq!(result, last);
    // The logged result keeps the category alone. The attribution is in the
    // view-side projection, keyed by the operation.
    assert_eq!(
        faults.attributions().get(0, op),
        Some(Blame::HeaderPeer(source()))
    );
    for (local, result) in &failures[..failures.len() - 1] {
        let category = match result {
            StorageResult::PrepareFailed { category } | StorageResult::ApplyFailed { category } => {
                category
            }
            _ => unreachable!("only failures are kept"),
        };
        assert_eq!(category, "local_resource_or_storage");
        assert_eq!(faults.attributions().get(0, *local), Some(Blame::None));
    }
    assert_eq!(
        faults.attributions().all().len(),
        failures.len(),
        "exactly the logged failures are attributed"
    );
    let owed = owed(report.log.entries(), faults.attributions()).unwrap();
    assert_eq!(owed.len(), 1, "{owed:?}");
    let OwedPenalty {
        op: owed_op,
        stage: owed_stage,
        source: owed_source,
        session_id,
        reason,
        ..
    } = &owed[0];
    assert_eq!(
        (
            *owed_op,
            *owed_stage,
            owed_source.as_str(),
            *session_id,
            *reason
        ),
        (op, stage, source().as_str(), 1, "invalid-header")
    );
    // No reactor runs here, so nothing penalizes the supplier, and the
    // settled run shows it.
    assert!(
        matches!(faults.penalized(report.log.entries()), Acceptance::Violated(why)
            if why.starts_with("UnpenalizedFault")),
        "{:?}",
        faults.penalized(report.log.entries())
    );
}

/// A batch whose last header has an easy target fails preparation with an
/// `InvalidHeader` that blames the request's source.
#[test]
fn an_attributed_prepare_failure_owes_its_source_a_penalty() {
    let fixture = HeaderFixture::regtest();
    let mut headers = fixture.extend(fixture.anchor(), 2, 0);
    headers[1] = easy_target(&headers[1]);
    for plan in plans() {
        let (report, faults) = attempt(plan, fixture.anchor(), headers.clone());
        assert_one_attributed(
            &report,
            &faults,
            "prepare",
            StorageResult::PrepareFailed {
                category: "invalid_header".to_owned(),
            },
        );
    }
}

/// A header stamped no later than its parent prepares, since preparation
/// reads no predecessors, and then fails admission with an `InvalidHeader`
/// that blames the target's source.
#[test]
fn an_attributed_apply_failure_owes_its_source_a_penalty() {
    let fixture = HeaderFixture::regtest();
    let mut headers = fixture.extend(fixture.anchor(), 2, 0);
    headers[1] = stale_time(&headers[1], &headers[0]);
    for plan in plans() {
        let (report, faults) = attempt(plan, fixture.anchor(), headers.clone());
        assert_one_attributed(
            &report,
            &faults,
            "apply",
            StorageResult::ApplyFailed {
                category: "invalid_header".to_owned(),
            },
        );
    }
}

/// The unattributed control: headers on a parent the node doesn't retain
/// fail preparation as stale work, which blames no peer, so nothing is
/// owed.
#[test]
fn an_unattributed_failure_owes_nothing() {
    let fixture = HeaderFixture::regtest();
    let orphan_parent = Frontier::new(block::Height(1), block::Hash([3; 32]));
    let headers = fixture.extend(orphan_parent, 1, 0);
    for plan in plans() {
        let (report, faults) = attempt(plan, orphan_parent, headers.clone());
        let failures = failures(&report);
        assert_eq!(
            failures.last().map(|(_, result)| result),
            Some(&StorageResult::PrepareFailed {
                category: "stale_target_or_generation".to_owned()
            })
        );
        for (op, _) in &failures {
            assert_eq!(faults.attributions().get(0, *op), Some(Blame::None));
        }
        assert_eq!(
            owed(report.log.entries(), faults.attributions()).unwrap(),
            Vec::new()
        );
        assert_eq!(faults.penalized(report.log.entries()), Acceptance::Holds);
    }
}
