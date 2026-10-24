//! The engine-backed header port inside a run (storage.md ST2.3, ST2.4 and
//! D29). A requester and a publisher share one port on the real scheduler.
//! Preparation runs on the run's thread with the scenario clock.
#![cfg(zakura_sim)]

mod support;

use std::sync::{Arc, Mutex};

use support::{config, inputs, run};
use zakura_header_chain::{EngineSnapshot, Frontier};
use zakura_node_services::header_chain::{Port, SelectedChainMembership};
use zakura_sim::{observe::Exit, NoChecks, Observation, PlanInput, RunReport};
use zakura_storage::header::{adapter::EnginePort, fixture::HeaderFixture};

/// What the publisher saw: each published snapshot, with the tip's
/// membership read right after it.
type Seen = Arc<Mutex<Vec<(EngineSnapshot, SelectedChainMembership)>>>;

/// Runs a requester that admits an extension and then a heavier fork, and
/// a publisher that follows the watch. Returns the report, the final
/// snapshot and what the publisher saw.
fn scenario(
    plan: PlanInput,
) -> (
    RunReport,
    EngineSnapshot,
    Vec<(EngineSnapshot, SelectedChainMembership)>,
) {
    let fixture = HeaderFixture::regtest();
    let port = Arc::new(EnginePort::new(&fixture));
    let seen: Seen = Arc::default();
    let mut config = config(2, 2_000);
    // Engine planning needs more than the tiny test stack. The default is
    // tokio's worker stack, as in production.
    config.scheduler.stack_size = zakura_scheduler::SchedulerConfig::default().stack_size;
    let report = run(
        config,
        plan,
        |sim| {
            let requester = port.clone();
            sim.scheduler().spawn_root("requester", async move {
                let anchor = fixture.anchor();
                let main = fixture.extend(anchor, 3, 0);
                admit(&fixture, &requester, anchor, &main, 1).await;
                let fork_parent = Frontier::new(zakura_chain::block::Height(1), main[0].hash());
                admit(
                    &fixture,
                    &requester,
                    fork_parent,
                    &fixture.extend(fork_parent, 4, 1),
                    2,
                )
                .await;
            });
            let publisher: Arc<dyn Port> = port.clone();
            let seen = seen.clone();
            sim.scheduler().spawn_root("publisher", async move {
                let mut watch = publisher.selected_chain();
                while watch.changed().await.is_ok() {
                    let snapshot = watch.borrow_and_update().clone().expect("a snapshot");
                    let tip = snapshot.frontiers.header_best;
                    let membership = publisher
                        .selected_chain_contains(tip.hash)
                        .await
                        .expect("membership reads");
                    let height = tip.height;
                    seen.lock().unwrap().push((snapshot, membership));
                    if height.0 == 5 {
                        break;
                    }
                }
            });
        },
        &mut NoChecks,
    );
    let seen = seen.lock().unwrap().clone();
    (report, port.snapshot(), seen)
}

/// Prepares and applies `headers` on `parent` through `port`.
async fn admit(
    fixture: &HeaderFixture,
    port: &EnginePort,
    parent: Frontier,
    headers: &[Arc<zakura_chain::block::Header>],
    request_id: u64,
) {
    let request = fixture.target_request(&port.snapshot(), parent, headers, request_id);
    let prepared = port
        .prepare_header_target(request)
        .await
        .expect("fixture headers prepare in the run");
    port.apply_header_target(prepared)
        .await
        .expect("fixture headers apply in the run");
}

#[test]
fn shared_port_commits_the_same_chain_under_every_plan() {
    let mut finals = std::collections::BTreeSet::new();
    for plan in inputs(16, 64) {
        let (report, last, seen) = scenario(plan);
        let panicked = report.log.entries().iter().any(|e| {
            matches!(
                e.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )
        });
        assert!(!panicked, "{:?}", report.end);
        assert_eq!(last.frontiers.header_best.height.0, 5);
        // Every published snapshot was committed, so the publisher's
        // membership read, made after it, finds that tip selected.
        assert!(!seen.is_empty());
        for (snapshot, membership) in &seen {
            assert!(snapshot.state_version <= last.state_version);
            match membership {
                SelectedChainMembership::Selected(frontier) => {
                    assert!(frontier.height <= last.frontiers.header_best.height);
                }
                // A later commit can reorganize past a tip the publisher saw.
                SelectedChainMembership::NotSelected => {
                    assert!(snapshot.frontiers.header_best.height.0 < 5);
                }
                SelectedChainMembership::NotRetained => panic!("a published tip is retained"),
            }
        }
        assert_eq!(seen.last().map(|(s, _)| s), Some(&last));
        finals.insert(format!("{last:?}"));
    }
    // Scheduling decides only when work runs, never what the engine commits.
    assert_eq!(finals.len(), 1);
}

#[test]
fn the_same_plan_repeats() {
    for plan in inputs(4, 64) {
        let (first, last, seen) = scenario(plan.clone());
        let (again, last_again, seen_again) = scenario(plan);
        let picks = |report: &RunReport| {
            report
                .picks
                .iter()
                .map(|p| p.identity.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(picks(&first), picks(&again));
        assert_eq!(last, last_again);
        assert_eq!(format!("{seen:?}"), format!("{seen_again:?}"));
    }
}

/// Lease deadlines run on virtual time. A lease left idle while the run
/// sleeps past 30 s is swept by another peer's acquire, which fires the
/// signal its owner's second acquire waits on.
#[test]
fn idle_leases_expire_on_virtual_time() {
    use zakura_node_services::header_chain::{AcquirePath, AcquirePathReply};

    let fixture = HeaderFixture::regtest();
    let port = Arc::new(EnginePort::new(&fixture));
    let released = Arc::new(Mutex::new(None));
    let mut config = config(2, 2_000);
    config.scheduler.stack_size = zakura_scheduler::SchedulerConfig::default().stack_size;
    let report = run(
        config,
        inputs(1, 0).remove(0),
        |sim| {
            let port = port.clone();
            let released = released.clone();
            sim.scheduler().spawn_root("leases", async move {
                let anchor = fixture.anchor();
                let main = fixture.extend(anchor, 2, 0);
                admit(&fixture, &port, anchor, &main, 1).await;
                let tip = main[1].hash();
                let acquire = |peer: u8| AcquirePath {
                    source: zakura_header_chain::SourceId::from_digest([peer; 32]),
                    session_id: 1,
                    scope: zakura_header_chain::HeaderWorkAuthority::for_target(
                        &port.snapshot(),
                        tip,
                    ),
                    target_tip_hash: tip,
                    locator_hashes: vec![anchor.hash],
                };
                let first = port.acquire_header_path(acquire(1)).await;
                assert!(matches!(first, Ok(AcquirePathReply::Acquired(_))));
                let Ok(AcquirePathReply::CapacityBusy(signal)) =
                    port.acquire_header_path(acquire(1)).await
                else {
                    panic!("a held lease refuses its peer");
                };
                zakura_rt::time::sleep(std::time::Duration::from_secs(31)).await;
                let other = port.acquire_header_path(acquire(2)).await;
                assert!(matches!(other, Ok(AcquirePathReply::Acquired(_))));
                *released.lock().unwrap() = Some(signal.is_released());
            });
        },
        &mut NoChecks,
    );
    assert_eq!(report.cleanup.panicked, 0, "{:?}", report.end);
    assert_eq!(*released.lock().unwrap(), Some(true));
    assert_eq!(port.live_leases(), 1);
}
