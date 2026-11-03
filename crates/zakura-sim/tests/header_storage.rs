//! The engine-backed header port inside a run (storage.md ST2.3, ST2.4 and
//! D29). A requester and a publisher share one port on the real scheduler.
//! Preparation runs on the run's thread with the scenario clock.
#![cfg(zakura_sim)]

mod support;

use std::sync::{Arc, Mutex};

use support::{config, inputs, run};
use zakura_header_chain::{EngineSnapshot, ErrorCategory, Frontier};
use zakura_node_services::header_chain::{
    ApplyHeaderTargetOutcome, ApplyHeaderTargetReply, Port, SelectedChainMembership,
};
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
                // Membership answers for the exact tip the watch showed.
                SelectedChainMembership::Selected(frontier) => {
                    assert_eq!(*frontier, snapshot.frontiers.header_best);
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

/// Prepares `headers` on `parent` through `port`, with every body size set
/// to `body_size`, against the snapshot the port has now.
async fn prepare(
    fixture: &HeaderFixture,
    port: &EnginePort,
    parent: Frontier,
    headers: &[Arc<zakura_chain::block::Header>],
    request_id: u64,
    body_size: u32,
) -> zakura_node_services::header_chain::PreparedHeaderTarget {
    let mut request = fixture.target_request(&port.snapshot(), parent, headers, request_id);
    let mut entries = request.entries.into_vec();
    for entry in &mut entries {
        entry.body_size = body_size;
    }
    request.entries = entries.try_into().expect("the batch still fits");
    port.prepare_header_target(request)
        .await
        .expect("fixture headers prepare in the run")
}

/// Polls `future` once, outside a run. The port completes each operation
/// on its first poll.
fn ready<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the port completes on its first poll"),
    }
}

/// Two applies race while a reader follows the watch and membership. Apply
/// A only adds body-size hints to the committed H1..H2, which moves the
/// state version but not the header generation. Apply B, prepared on that
/// same generation, extends to H3.
///
/// Under every plan, B applies, and A either applies or, if B committed
/// first, fails as stale. The engine ends at H3, the watch ends at the
/// engine's final snapshot, and a header the reader finds selected is never
/// above the watch's tip. Both watch checks hold only if install and
/// publication form one step. Some plan must commit both, so the race is
/// actually exercised.
#[test]
fn concurrent_applies_publish_in_commit_order() {
    type Outcome = Arc<Mutex<Option<ApplyHeaderTargetReply>>>;
    let mut both_committed = 0;
    for plan in inputs(64, 64) {
        let fixture = HeaderFixture::regtest();
        let port = Arc::new(EnginePort::new(&fixture));
        let anchor = fixture.anchor();
        let main = fixture.extend(anchor, 3, 0);
        let h2 = Frontier::new(zakura_chain::block::Height(2), main[1].hash());
        let h3 = Frontier::new(zakura_chain::block::Height(3), main[2].hash());
        let request = fixture.target_request(&port.snapshot(), anchor, &main[..2], 1);
        let prepared = ready(port.prepare_header_target(request)).expect("H1..H2 prepare");
        ready(port.apply_header_target(prepared)).expect("H1..H2 apply");
        let hints_version = port.snapshot().state_version;
        let violations = Arc::new(Mutex::new(Vec::new()));
        let (a_outcome, b_outcome): (Outcome, Outcome) = Default::default();
        let mut config = config(2, 4_000);
        config.scheduler.stack_size = zakura_scheduler::SchedulerConfig::default().stack_size;
        let report = run(
            config,
            plan,
            |sim| {
                let (a_port, a_fixture, a_headers) = (port.clone(), fixture.clone(), main.clone());
                let a_outcome = a_outcome.clone();
                sim.scheduler().spawn_root("apply-hints", async move {
                    let target =
                        prepare(&a_fixture, &a_port, anchor, &a_headers[..2], 2, 1_000).await;
                    let reply = a_port.apply_header_target(target).await;
                    *a_outcome.lock().unwrap() = Some(reply);
                });
                let (b_port, b_fixture, b_headers) = (port.clone(), fixture.clone(), main.clone());
                let b_outcome = b_outcome.clone();
                sim.scheduler().spawn_root("apply-h3", async move {
                    let target = prepare(&b_fixture, &b_port, h2, &b_headers[2..], 3, 0).await;
                    let reply = b_port.apply_header_target(target).await;
                    *b_outcome.lock().unwrap() = Some(reply);
                });
                let (r_port, violations) = (port.clone(), violations.clone());
                let hashes: Vec<_> = main.iter().map(|header| header.hash()).collect();
                sim.scheduler().spawn_root("reader", async move {
                    let watch = r_port.selected_chain();
                    for _ in 0..8 {
                        for hash in &hashes {
                            let membership = r_port
                                .selected_chain_contains(*hash)
                                .await
                                .expect("membership reads");
                            let published = watch
                                .borrow()
                                .as_ref()
                                .map(|snapshot| snapshot.frontiers.header_best.height);
                            if let SelectedChainMembership::Selected(frontier) = membership {
                                if published < Some(frontier.height) {
                                    violations.lock().unwrap().push(format!(
                                        "{frontier:?} selected above the watch's {published:?}"
                                    ));
                                }
                            }
                        }
                        zakura_rt::task::yield_now().await;
                    }
                });
            },
            &mut NoChecks,
        );
        let panicked = report.log.entries().iter().any(|e| {
            matches!(
                e.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )
        });
        assert!(!panicked, "a task panicked: {:?}", report.end);
        assert_eq!(report.cleanup.panicked, 0, "{:?}", report.end);
        let b = b_outcome.lock().unwrap().take().expect("apply B finished");
        assert_eq!(
            b.expect("apply B commits"),
            ApplyHeaderTargetOutcome::Applied
        );
        match a_outcome.lock().unwrap().take().expect("apply A finished") {
            Ok(outcome) => {
                assert_eq!(outcome, ApplyHeaderTargetOutcome::Applied);
                // Two commits after H1..H2: the hints and H3.
                assert_eq!(port.snapshot().state_version.get(), hints_version.get() + 2);
                both_committed += 1;
            }
            Err(error) => {
                assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);
                assert_eq!(port.snapshot().state_version.get(), hints_version.get() + 1);
            }
        }
        assert_eq!(port.snapshot().frontiers.header_best, h3);
        assert_eq!(
            port.selected_chain().borrow().as_ref(),
            Some(&port.snapshot()),
            "the watch ends at the final commit"
        );
        assert!(
            violations.lock().unwrap().is_empty(),
            "{:?}",
            violations.lock().unwrap()
        );
    }
    assert!(both_committed > 0, "no plan committed both applies");
}
