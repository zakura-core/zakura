//! Controlled header storage on the step loop (storage.md ST3.5 to ST3.8).
//! The `Plan` picks when and how each storage operation completes, between
//! the tasks that submitted them.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest, Sha256};
use support::{config, inputs, run};
use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, LocatorReadPermit, Port, ReadPath,
    ReadPathReply,
};
use zakura_rt::sync::Semaphore;
use zakura_scheduler::{SchedulerConfig, Status};
use zakura_sim::{
    check::after_step, observe::StorageEvent, IdleStatus, Observation, Outcome, PlanInput,
    RunConfig, RunReport, Sim, Violation,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture, HeaderOp};

/// A run config with 2 workers and `budget` steps. Tasks keep the default
/// stack, as the engine-backed scenario does.
fn storage_config(budget: u64) -> RunConfig {
    let mut config = config(2, budget);
    config.scheduler.stack_size = SchedulerConfig::default().stack_size;
    config
}

/// A violation of `property` at `step`.
fn violation(property: &str, step: u64, actual: String) -> Violation {
    Violation {
        property: property.to_owned(),
        step,
        witness: vec![step],
        expected: property.to_owned(),
        actual,
    }
}

/// What one delivery run did, beyond its report.
#[derive(Default)]
struct Seen {
    /// The requester applied both targets.
    requester_done: bool,
    /// The locator caller's timeout fired while its operation was pending.
    locator_timed_out: bool,
    /// Pages the publisher was served.
    pages: u32,
}

/// The applied targets, and the semaphore the locator caller's permit
/// comes from.
struct Delivery {
    port: Arc<ControlledPort>,
    locator_capacity: Arc<Semaphore>,
    seen: Arc<Mutex<Seen>>,
    main_tip: Frontier,
    fork_tip: Frontier,
}

/// Prepares and applies `headers` on `parent` as `request_id`, retrying
/// after any failure, since the run may pick a fault for either step.
async fn admit(
    port: &ControlledPort,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) {
    loop {
        let request = HeaderFixture::regtest().target_request(
            &port.engine().snapshot(),
            parent,
            headers,
            request_id,
        );
        let Ok(target) = port.prepare_header_target(request).await else {
            continue;
        };
        if let Ok(ApplyHeaderTargetOutcome::Applied) = port.apply_header_target(target).await {
            return;
        }
    }
}

/// Serves `target` to peer 1 in `session`, a page at a time. A served page
/// consumes its lease, so each page after the first acquires again from
/// the last header sent, as the v9 publisher does. Each path is released
/// afterwards, which after a served page is a repeat. Counts the pages.
async fn serve(port: &ControlledPort, target: Frontier, session_id: u64, seen: &Mutex<Seen>) {
    let mut after = HeaderFixture::regtest().anchor().hash;
    // The longest path, 5 headers at 2 a page, takes 3 pages.
    for _ in 0..3 {
        let acquire = AcquirePath {
            source: SourceId::from_digest([1; 32]),
            session_id,
            scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
            target_tip_hash: target.hash,
            locator_hashes: vec![after],
        };
        let Ok(AcquirePathReply::Acquired(path)) = port.acquire_header_path(acquire).await else {
            return;
        };
        let read = ReadPath {
            after_hash: path.common_ancestor.hash,
            max_header_count: 2,
            want_tree_aux: false,
        };
        let page = port.read_header_path((*path).clone(), read).await;
        let _ = port.release_header_path(*path).await;
        let Ok(ReadPathReply::Page(page)) = page else {
            return;
        };
        seen.lock().unwrap().pages += 1;
        match page.headers.last() {
            Some(last) if !page.complete => after = last.hash(),
            _ => return,
        }
    }
}

/// Builds the delivery scenario's port and shared state.
fn delivery() -> Delivery {
    let fixture = HeaderFixture::regtest();
    let main = fixture.extend(fixture.anchor(), 3, 0);
    let fork_parent = Frontier::new(block::Height(1), main[0].hash());
    let fork = fixture.extend(fork_parent, 4, 1);
    Delivery {
        port: Arc::new(ControlledPort::new(&fixture)),
        locator_capacity: Arc::new(Semaphore::new(1)),
        seen: Arc::default(),
        main_tip: Frontier::new(block::Height(3), main[2].hash()),
        fork_tip: Frontier::new(block::Height(5), fork[3].hash()),
    }
}

/// Spawns the delivery scenario's tasks, a requester that admits an
/// extension and then a heavier fork and a publisher that serves each new
/// tip, and the locator caller.
fn spawn_delivery(sim: &mut Sim, delivery: &Delivery) {
    let fixture = HeaderFixture::regtest();
    let anchor = fixture.anchor();
    let main = fixture.extend(anchor, 3, 0);
    let fork_parent = Frontier::new(block::Height(1), main[0].hash());
    let fork = fixture.extend(fork_parent, 4, 1);
    let fork_tip = delivery.fork_tip;

    let requester = delivery.port.clone();
    let requester_seen = delivery.seen.clone();
    sim.scheduler().spawn_root("requester", async move {
        admit(&requester, anchor, &main, 1).await;
        admit(&requester, fork_parent, &fork, 2).await;
        requester_seen.lock().unwrap().requester_done = true;
    });

    let publisher = delivery.port.clone();
    let publisher_seen = delivery.seen.clone();
    sim.scheduler().spawn_root("publisher", async move {
        let mut watch = publisher.selected_chain();
        let mut session = 0;
        while watch.changed().await.is_ok() {
            // Cloned so no borrow is held while the port takes its locks.
            let tip = watch
                .borrow_and_update()
                .clone()
                .expect("the engine publishes snapshots")
                .frontiers
                .header_best;
            let _ = publisher.selected_chain_contains(tip.hash).await;
            session += 1;
            serve(&publisher, tip, session, &publisher_seen).await;
            if tip == fork_tip {
                break;
            }
        }
    });

    spawn_locator(sim, delivery);
}

/// Spawns a locator caller that gives up after the reactor's 30 s timeout,
/// with a storage source for the port.
fn spawn_locator(sim: &mut Sim, delivery: &Delivery) {
    let locator = delivery.port.clone();
    let semaphore = delivery.locator_capacity.clone();
    let locator_seen = delivery.seen.clone();
    sim.scheduler().spawn_root("locator", async move {
        let permit = LocatorReadPermit::new(
            semaphore
                .try_acquire_owned()
                .expect("the locator caller is the only permit holder"),
        );
        let reply = zakura_rt::time::timeout(
            Duration::from_secs(30),
            locator.continuation_locator(permit.clone()),
        )
        .await;
        if reply.is_err() {
            locator_seen.lock().unwrap().locator_timed_out = true;
        }
    });

    sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::new(
        delivery.port.clone(),
    )));
}

/// Checks the storage invariants after every step of a delivery run:
///
/// * Each operation completes at most once, and only after its submission.
/// * A step that picks `storage:header:<id>:<outcome>` logs exactly that
///   completion, and no other step logs one, so a completion lands on the
///   step that caused it with the outcome the `Plan` picked.
/// * While the locator operation is pending, its permit is held, even after
///   its caller timed out.
/// * The watch holds exactly the engine's committed snapshot, so a commit
///   and its publication are one step.
fn delivery_checks(
    port: Arc<ControlledPort>,
    locator_capacity: Arc<Semaphore>,
) -> impl zakura_sim::Checker {
    let mut submitted = BTreeSet::new();
    let mut completed = BTreeSet::new();
    let watch = port.selected_chain();
    after_step(move |cx| {
        let mut completions = Vec::new();
        for obs in cx.observations() {
            let Observation::Storage { id, event, .. } = obs else {
                continue;
            };
            match event {
                StorageEvent::Submitted { .. } => {
                    submitted.insert(*id);
                }
                StorageEvent::Completed { outcome, .. } => {
                    completions.push(format!("storage:header:{id}:{outcome}"));
                    if !submitted.contains(id) || !completed.insert(*id) {
                        return Some(violation(
                            "storage/complete-once",
                            cx.step,
                            format!("operation {id} completed twice or before submission"),
                        ));
                    }
                }
                StorageEvent::CallerDropped | StorageEvent::ReplyDropped { .. } => {}
            }
        }
        let pick = cx.pick.to_string();
        let expected = if pick.starts_with("storage:header:") {
            vec![pick]
        } else {
            Vec::new()
        };
        if completions != expected {
            return Some(violation(
                "storage/completion-is-the-pick",
                cx.step,
                format!("picked {}, completed {completions:?}", cx.pick),
            ));
        }
        let locator_pending = port
            .pending()
            .iter()
            .any(|op| op.op == HeaderOp::ContinuationLocator);
        if locator_pending && locator_capacity.available_permits() != 0 {
            return Some(violation(
                "storage/locator-permit-held",
                cx.step,
                "the permit came back while its operation was pending".to_owned(),
            ));
        }
        let published = watch.borrow().clone();
        if published != Some(port.engine().snapshot()) {
            return Some(violation(
                "storage/watch-is-committed",
                cx.step,
                "the watch and the engine disagree".to_owned(),
            ));
        }
        None
    })
}

/// Runs `spawn` under `plan` with the invariant checks.
fn checked_run(plan: PlanInput, spawn: fn(&mut Sim, &Delivery)) -> (RunReport, Delivery) {
    let delivery = delivery();
    let mut checks = delivery_checks(delivery.port.clone(), delivery.locator_capacity.clone());
    let report = run(
        storage_config(4_000),
        plan,
        |sim| spawn(sim, &delivery),
        &mut checks,
    );
    (report, delivery)
}

/// Runs the delivery scenario under `plan` with the invariant checks.
fn delivery_run(plan: PlanInput) -> (RunReport, Delivery) {
    checked_run(plan, spawn_delivery)
}

/// ST3.5: under many plans, the requester, the publisher and a locator
/// caller share one controlled port, every invariant in
/// [`delivery_checks`] holds after every step, and a finished run commits
/// the fork. Some plans must commit both targets and serve pages, so each
/// race is actually exercised.
#[test]
fn controlled_header_delivery_holds_under_every_plan() {
    let (mut finished, mut served) = (0, 0);
    for plan in inputs(48, 256) {
        let (report, delivery) = delivery_run(plan);
        assert!(
            matches!(
                report.end.outcome,
                Outcome::Idle {
                    status: IdleStatus::Quiescent
                } | Outcome::Budget
            ),
            "{:?}",
            report.end
        );
        let seen = delivery.seen.lock().unwrap();
        if seen.requester_done {
            finished += 1;
            assert_eq!(
                delivery.port.engine().snapshot().frontiers.header_best,
                delivery.fork_tip
            );
            assert_ne!(delivery.main_tip, delivery.fork_tip);
        }
        served += u32::from(seen.pages > 0);
    }
    assert!(finished > 0, "no plan finished the requester");
    assert!(served > 0, "no plan served a page");
}

/// ST3.2 and ST3.6 in a run: when the reactor-style 30 s timeout fires,
/// the caller drops its future and its permit clone, but a pending
/// operation keeps the permit (checked after every step) until the run
/// completes it. The caller drops either before the completion, so nobody
/// takes the reply, or after it, when the completion lands while the
/// caller sits at a scheduling point between checking its reply and its
/// deadline. Both histories happen, and the reply goes untaken in both.
#[test]
fn a_timed_out_locator_keeps_its_permit_until_completion() {
    let (mut before, mut after) = (0, 0);
    // Few plans time the caller out at all, 6 of these 256, so it runs many.
    for plan in inputs(256, 16) {
        let (report, delivery) = checked_run(plan, spawn_locator);
        assert_eq!(
            report.end.outcome,
            Outcome::Idle {
                status: IdleStatus::Quiescent
            },
            "{:?}",
            report.end
        );
        assert_eq!(delivery.locator_capacity.available_permits(), 1);
        if !delivery.seen.lock().unwrap().locator_timed_out {
            continue;
        }
        let events: Vec<_> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Storage { event, .. } => Some(event.clone()),
                _ => None,
            })
            .collect();
        match events.as_slice() {
            [StorageEvent::Submitted { .. }, StorageEvent::CallerDropped, StorageEvent::Completed {
                delivered: false, ..
            }] => before += 1,
            [StorageEvent::Submitted { .. }, StorageEvent::Completed {
                delivered: true, ..
            }, StorageEvent::ReplyDropped { .. }] => after += 1,
            other => panic!("unexpected history {other:?}"),
        }
    }
    assert!(
        before > 0,
        "no plan timed the caller out before the completion"
    );
    assert!(
        after > 0,
        "no plan timed the caller out after the completion"
    );
}

/// ST3.7: a task waiting on storage leaves the scheduler with no ready task
/// and no timer, which it reports as `Deadlock`. The pending completion is
/// still an enabled action, so the run goes on, the completion wakes the
/// task, and the run ends with every task finished.
#[test]
fn a_completion_wakes_a_task_the_scheduler_sees_as_deadlocked() {
    let deadlocked_with_storage = Arc::new(Mutex::new(false));
    let seen = deadlocked_with_storage.clone();
    // A step's status is read after it and its enabled set before it, so
    // the next step's enabled set is what the run had at that status.
    let mut deadlocked = false;
    let mut checker = after_step(move |cx| {
        let storage = cx
            .enabled
            .iter()
            .any(|enabled| enabled.identity.to_string().starts_with("storage:header:"));
        if deadlocked && storage {
            *seen.lock().unwrap() = true;
        }
        deadlocked = cx.status == Status::Deadlock;
        None
    });
    let report = run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let caller = port.clone();
            sim.scheduler().spawn_root("caller", async move {
                let anchor = HeaderFixture::regtest().anchor();
                caller
                    .selected_chain_contains(anchor.hash)
                    .await
                    .expect("the engine outcome comes first");
            });
            sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::new(port)));
        },
        &mut checker,
    );
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    assert!(*deadlocked_with_storage.lock().unwrap());
    assert!(report
        .picks
        .iter()
        .any(|pick| pick.identity == "storage:header:1:engine"));
}

/// A digest of a run's storage observations, with their steps, and of the
/// port's final state.
fn history(plan: PlanInput) -> String {
    let (report, delivery) = delivery_run(plan);
    let mut hasher = Sha256::new();
    for entry in report.log.entries() {
        if let Observation::Storage { .. } = &entry.obs {
            hasher.update(entry.step.to_le_bytes());
            hasher.update(serde_json::to_vec(&entry.obs).unwrap());
        }
    }
    hasher.update(delivery.port.engine().state_digest());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// ST3.8: a plan's storage history and final state repeat exactly, and an
/// unrelated run in between changes nothing.
#[test]
fn storage_histories_repeat_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    let a = history(plans[0].clone());
    assert_eq!(history(plans[0].clone()), a);
    let b = history(plans[1].clone());
    assert_ne!(b, a, "the two plans should make different histories");
    assert_eq!(history(plans[0].clone()), a);
}

/// Set when this test binary runs as the fresh-process child.
const CHILD: &str = "ZAKURA_SIM_STORAGE_HISTORY_CHILD";

/// Marks the fingerprint in the child's output.
const MARKER: &str = "zakura-sim-storage-fingerprint=";

/// A digest over the histories of a fixed set of plans.
fn fingerprint() -> String {
    let mut hasher = Sha256::new();
    for plan in inputs(6, 256) {
        hasher.update(history(plan).as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Prints the fingerprint when run as the child, and does nothing otherwise.
#[test]
fn fresh_process_child() {
    if std::env::var_os(CHILD).is_some() {
        #[allow(
            clippy::print_stdout,
            reason = "the parent test reads the child's stdout"
        )]
        {
            println!("\n{MARKER}{}", fingerprint());
        }
    }
}

/// ST3.8: the same histories and final states come out of a fresh process.
#[test]
fn storage_histories_repeat_in_a_fresh_process() {
    let expected = fingerprint();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fresh_process_child", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .expect("the test binary reruns itself");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let at = stdout
        .find(MARKER)
        .unwrap_or_else(|| panic!("the child printed no fingerprint: {stdout}"))
        + MARKER.len();
    let child: String = stdout[at..]
        .chars()
        .take_while(char::is_ascii_hexdigit)
        .collect();
    assert_eq!(child, expected);
}

/// ST3.8: a run that ends with storage still pending gets every permit and
/// lease back when the source drops at shutdown, and nothing from shutdown
/// reaches the frozen log.
#[test]
fn shutdown_releases_everything_and_logs_nothing() {
    let mut held = None;
    let report = run(
        storage_config(1),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let semaphore = Arc::new(Semaphore::new(1));
            let caller = port.clone();
            let permits = semaphore.clone();
            sim.scheduler().spawn_root("locator", async move {
                let permit = LocatorReadPermit::new(
                    permits.try_acquire_owned().expect("the only permit holder"),
                );
                let _ = caller.continuation_locator(permit).await;
            });
            sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::new(
                port.clone(),
            )));
            held = Some((port, semaphore));
        },
        &mut zakura_sim::NoChecks,
    );
    let (port, semaphore) = held.expect("setup ran");
    assert_eq!(report.end.outcome, Outcome::Budget);
    let logged: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { event, .. } => Some(event.clone()),
            _ => None,
        })
        .collect();
    assert!(
        matches!(logged.as_slice(), [StorageEvent::Submitted { .. }]),
        "{logged:?}"
    );

    assert_eq!(semaphore.available_permits(), 1);
    assert!(port.pending().is_empty());
    assert_eq!(port.engine().live_leases(), 0);
    // The caller's cancellation and the shutdown happened after the log
    // froze, so they're still queued and were never logged.
    assert!(matches!(
        port.take_events().as_slice(),
        [
            zakura_storage::header::controlled::StorageEvent::CallerDropped { .. },
            zakura_storage::header::controlled::StorageEvent::ShutDown { .. },
        ]
    ));
}

/// D4: with a latency bound, time can't move past an operation's deadline
/// while it's pending, even when a long timer is the only other event.
#[test]
fn bounded_storage_completes_within_its_latency() {
    let bound = Duration::from_secs(10);
    for plan in inputs(24, 64) {
        let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
        let watched = port.clone();
        let mut checker = after_step(move |cx| {
            let late = watched.pending().iter().any(|op| op.since_submit > bound);
            late.then(|| {
                violation(
                    "storage/bounded-latency",
                    cx.step,
                    "an operation outlived its latency bound".to_owned(),
                )
            })
        });
        let report = run(
            storage_config(200),
            plan,
            |sim| {
                let caller = port.clone();
                sim.scheduler().spawn_root("caller", async move {
                    let anchor = HeaderFixture::regtest().anchor();
                    let _ = caller.selected_chain_contains(anchor.hash).await;
                });
                sim.scheduler().spawn_root("sleeper", async move {
                    zakura_rt::time::sleep(Duration::from_secs(60)).await;
                });
                sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::bounded(
                    port.clone(),
                    bound,
                )));
            },
            &mut checker,
        );
        assert_ne!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
    }
}

/// M1.2 for storage: time stops where an operation's timeout opens, here
/// 30 s, even when the next timer is a 60 s sleep, so the run can time the
/// operation out there.
#[test]
fn time_stops_where_a_timeout_opens() {
    let gate = "advance-time:30000000000";
    let mut stopped = 0;
    for plan in inputs(24, 32) {
        let mut checker = after_step(|cx| {
            let pending = cx
                .enabled
                .iter()
                .any(|enabled| enabled.identity.to_string() == "storage:header:1:engine");
            let to = cx.pick.to_string();
            let past_gate = to.starts_with("advance-time:")
                && to != gate
                && cx.now.since_start() > Duration::from_secs(30);
            let timeout_open = cx
                .enabled
                .iter()
                .any(|enabled| enabled.identity.to_string() == "storage:header:1:timeout");
            (pending && past_gate && !timeout_open).then(|| {
                violation(
                    "storage/time-stops-at-gates",
                    cx.step,
                    format!("{to} skipped the timeout gate at 30 s"),
                )
            })
        });
        let report = run(
            storage_config(200),
            plan,
            |sim| {
                let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
                let caller = port.clone();
                sim.scheduler().spawn_root("caller", async move {
                    let anchor = HeaderFixture::regtest().anchor();
                    let _ = caller.selected_chain_contains(anchor.hash).await;
                });
                sim.scheduler().spawn_root("sleeper", async move {
                    zakura_rt::time::sleep(Duration::from_secs(60)).await;
                });
                sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::new(port)));
            },
            &mut checker,
        );
        assert_ne!(report.end.outcome, Outcome::Violation, "{:?}", report.end);
        stopped += usize::from(report.picks.iter().any(|pick| pick.identity == gate));
    }
    assert!(stopped > 0, "no plan left the operation pending until 30 s");
}

/// What one abandonment run did.
#[derive(Default)]
struct Abandonment {
    /// The waiter got `CapacityBusy` and so registered a capacity signal.
    waited: bool,
    /// Steps where the run completed storage while the holder was stopped
    /// mid-poll after it started dropping its reply.
    interleaved: u32,
}

/// Spawns a task that submits peer 1's acquire of `target` in `session`,
/// leaves its reply unread until `drop_at`, then drops the call.
fn spawn_unread(
    sim: &mut Sim,
    name: &'static str,
    port: &Arc<ControlledPort>,
    session: u64,
    drop_at: u64,
) {
    let caller = port.clone();
    sim.scheduler().spawn_root(name, async move {
        let target = HeaderFixture::regtest().anchor();
        let mut call = caller.acquire_header_path(peer_acquire(&caller, target, session));
        std::future::poll_fn(|cx| {
            let _ = call.as_mut().poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        zakura_rt::time::sleep_until(
            zakura_rt::time::Instant::now() + Duration::from_secs(drop_at),
        )
        .await;
        drop(call);
    });
}

/// Peer 1's acquire in `session` of the empty path to `target`, which is
/// its own locator. It still takes a lease.
fn peer_acquire(port: &ControlledPort, target: Frontier, session_id: u64) -> AcquirePath {
    AcquirePath {
        source: SourceId::from_digest([1; 32]),
        session_id,
        scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
        target_tip_hash: target.hash,
        locator_hashes: vec![target.hash],
    }
}

/// Runs the abandonment scenario under `plan`: a holder leaves its
/// `Acquired` reply unread for 5 s and drops it, a waiter's acquire for the
/// same peer meanwhile gets `CapacityBusy` and waits on its signal, an idler
/// leaves a `CapacityBusy` reply unread until 12 s, and a competing task
/// keeps membership reads pending, so the run can complete storage at any
/// point of the drops.
fn abandonment_run(plan: PlanInput) -> (RunReport, Abandonment) {
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let seen = Arc::new(Mutex::new(Abandonment::default()));
    let mut checker = {
        let seen = seen.clone();
        after_step(move |cx| {
            let storage = cx.pick.to_string().starts_with("storage:");
            let holder_dropping = cx.now.since_start() >= Duration::from_secs(5)
                && cx.enabled.iter().any(|enabled| {
                    let identity = enabled.identity.to_string();
                    identity.starts_with("task:holder.1:") && !identity.contains(":poll#")
                });
            if storage && holder_dropping {
                seen.lock().unwrap().interleaved += 1;
            }
            None
        })
    };
    let report = run(
        storage_config(2_000),
        plan,
        |sim| {
            spawn_unread(sim, "holder", &port, 1, 5);
            spawn_unread(sim, "idler", &port, 3, 12);
            let waiter = port.clone();
            let waiter_seen = seen.clone();
            sim.scheduler().spawn_root("waiter", async move {
                zakura_rt::time::sleep(Duration::from_secs(1)).await;
                let target = HeaderFixture::regtest().anchor();
                let reply = waiter
                    .acquire_header_path(peer_acquire(&waiter, target, 2))
                    .await;
                if let Ok(AcquirePathReply::CapacityBusy(signal)) = reply {
                    waiter_seen.lock().unwrap().waited = true;
                    let _ =
                        zakura_rt::time::timeout(Duration::from_secs(60), signal.released()).await;
                }
            });
            let competing = port.clone();
            sim.scheduler().spawn_root("competing", async move {
                let anchor = HeaderFixture::regtest().anchor();
                let mut calls: Vec<_> = (0..16)
                    .map(|_| competing.selected_chain_contains(anchor.hash))
                    .collect();
                for call in &mut calls {
                    let _ = call.await;
                }
            });
            sim.add_source(Box::new(zakura_sim::storage::HeaderStorage::new(
                port.clone(),
            )));
        },
        &mut checker,
    );
    let seen = std::mem::take(&mut *seen.lock().unwrap());
    (report, seen)
}

/// #1722 review P1: under many plans, the run may complete storage at any
/// point while a task abandons an unread `Acquired` reply and wakes a
/// capacity waiter, or drops an unread `CapacityBusy` reply. No such run
/// panics or hangs, and every abandoned lease's end is recorded on its own
/// reply: on `ReplyDropped`, or on the acquire's completion when its caller
/// was already gone, never on another operation. Some plans must register a
/// waiter, complete storage mid-drop and record an abandoned lease on a
/// dropped reply, so the race is exercised.
#[test]
fn abandoning_unread_replies_in_a_run_keeps_the_storage_driver_working() {
    let (mut waited, mut interleaved, mut dropped) = (0, 0, 0);
    for plan in inputs(128, 64) {
        let (report, seen) = abandonment_run(plan);
        assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
        let mut ops = std::collections::BTreeMap::new();
        for entry in report.log.entries() {
            let Observation::Storage { id, event, .. } = &entry.obs else {
                continue;
            };
            let abandoned = |ends: &[zakura_sim::observe::LeaseEnd]| {
                ends.iter()
                    .any(|end| end.release == "lease-abandoned-acquire")
            };
            match event {
                StorageEvent::Submitted { op, .. } => {
                    ops.insert(*id, op.as_str());
                }
                StorageEvent::Completed { op, lease_ends, .. } if abandoned(lease_ends) => {
                    assert_eq!(
                        op, "acquire_header_path",
                        "operation {id} took an abandoned lease"
                    );
                }
                StorageEvent::ReplyDropped { lease_ends } if abandoned(lease_ends) => {
                    assert_eq!(
                        ops.get(id),
                        Some(&"acquire_header_path"),
                        "operation {id}'s dropped reply took an abandoned lease"
                    );
                    dropped += 1;
                }
                _ => {}
            }
        }
        waited += u32::from(seen.waited);
        interleaved += seen.interleaved;
    }
    assert!(waited > 0, "no plan registered a capacity waiter");
    assert!(interleaved > 0, "no plan completed storage mid-drop");
    assert!(dropped > 0, "no dropped reply ended an abandoned lease");
}
