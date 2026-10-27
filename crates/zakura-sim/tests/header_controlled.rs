//! Controlled header storage on the step loop (storage.md ST3.5 to ST3.8).
//! The `Plan` picks when and how each storage operation completes, between
//! the tasks that submitted them.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    num::NonZeroU64,
    process::Command,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest, Sha256};
use support::{config, inputs, recorded, run};
use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BodyWorkOwner, BranchId, Frontier, HeaderGeneration,
    HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId, VerifiedGeneration,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, LocatorReadPermit, Port, ReadPath,
    ReadPathReply,
};
use zakura_rt::sync::Semaphore;
use zakura_scheduler::{SchedulerConfig, SimInstant, Status};
use zakura_sim::{
    check::after_step,
    observe::{StorageFact, StorageRequest, StorageResult},
    property::{
        Assumptions, CheckCtx, HasView, Kind, Property, PropertyId, Registry, RegistryChecker,
        Source, Sources, Status as PropertyStatus, Verdict,
    },
    ActionSource, Boundary, HarnessFailure, Identity, IdleStatus, Observation, ObservationLog,
    Outcome, PlanInput, PropertyOutcome, RunConfig, RunReport, Sim, Violation,
};
use zakura_storage::header::{
    controlled::{ControlledPort, FactKind},
    evidence::RequestIdentity,
    fixture::HeaderFixture,
};

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

/// Whether `fact` heads its transaction, which it ends.
fn is_head(fact: &StorageFact) -> bool {
    matches!(
        fact,
        StorageFact::Submitted { .. }
            | StorageFact::CallerDropped { .. }
            | StorageFact::ReplyTaken { .. }
            | StorageFact::ReplyDropped { .. }
            | StorageFact::Completed { .. }
            | StorageFact::ShutDown { .. }
    )
}

/// The head of every storage transaction `report` logged, in order.
fn heads(report: &RunReport) -> Vec<StorageFact> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { fact, .. } if is_head(fact) => Some(fact.clone()),
            _ => None,
        })
        .collect()
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
    /// The locator caller holds its permit clone.
    caller_permit: bool,
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
        locator_seen.lock().unwrap().caller_permit = true;
        let reply = zakura_rt::time::timeout(
            Duration::from_secs(30),
            locator.continuation_locator(permit.clone()),
        )
        .await;
        if reply.is_err() {
            locator_seen.lock().unwrap().locator_timed_out = true;
        }
        // Cleared first: the last clone's drop frees the permit before its
        // scheduling point, so no step sees the permit back but the clone
        // still counted.
        locator_seen.lock().unwrap().caller_permit = false;
        drop(permit);
    });

    let storage = zakura_sim::storage::HeaderStorage::new(sim, delivery.port.clone());

    sim.add_source(Box::new(storage));
}

/// Checks the storage invariants after every step of a delivery run:
///
/// * Each operation completes at most once, and only after its submission.
/// * A step that picks `storage:header:<id>:<outcome>` logs exactly that
///   completion, and no other step logs one, so a completion lands on the
///   step that caused it with the outcome the `Plan` picked.
/// * The locator's semaphore has a permit out exactly while the caller
///   holds its clone or storage holds the operation's, so a clone leaked
///   anywhere else shows, even after both legitimate holders let go.
/// * The watch holds exactly the engine's committed snapshot, so a commit
///   and its publication are one step.
fn delivery_checks(
    port: Arc<ControlledPort>,
    locator_capacity: Arc<Semaphore>,
    seen: Arc<Mutex<Seen>>,
) -> impl zakura_sim::Checker {
    let mut submitted = BTreeSet::new();
    let mut completed = BTreeSet::new();
    let watch = port.selected_chain();
    after_step(move |cx| {
        let mut completions = Vec::new();
        for obs in cx.observations() {
            let Observation::Storage { fact, .. } = obs else {
                continue;
            };
            match fact {
                StorageFact::Submitted { id, .. } => {
                    submitted.insert(*id);
                }
                StorageFact::Completed { id, outcome, .. } => {
                    completions.push(format!("storage:header:{id}:{outcome}"));
                    if !submitted.contains(id) || !completed.insert(*id) {
                        return Some(violation(
                            "storage/complete-once",
                            cx.step,
                            format!("operation {id} completed twice or before submission"),
                        ));
                    }
                }
                _ => {}
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
        let held = seen.lock().unwrap().caller_permit || !port.owner_state().permits.is_empty();
        let available = locator_capacity.available_permits();
        if available != usize::from(!held) {
            return Some(violation(
                "storage/locator-permit-held",
                cx.step,
                format!("{available} permits free while held is {held}"),
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
    let mut checks = delivery_checks(
        delivery.port.clone(),
        delivery.locator_capacity.clone(),
        delivery.seen.clone(),
    );
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
        match heads(&report).as_slice() {
            [StorageFact::Submitted { .. }, StorageFact::CallerDropped { .. }, StorageFact::Completed {
                delivered: false, ..
            }] => before += 1,
            [StorageFact::Submitted { .. }, StorageFact::Completed {
                delivered: true, ..
            }, StorageFact::ReplyDropped { .. }] => after += 1,
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
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(Box::new(storage));
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
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port.clone());
            sim.add_source(Box::new(storage));
            held = Some((port, semaphore));
        },
        &mut zakura_sim::NoChecks,
    );
    let (port, semaphore) = held.expect("setup ran");
    assert_eq!(report.end.outcome, Outcome::Budget);
    let logged = heads(&report);
    assert!(
        matches!(logged.as_slice(), [StorageFact::Submitted { .. }]),
        "{logged:?}"
    );

    assert_eq!(semaphore.available_permits(), 1);
    assert!(port.pending().is_empty());
    assert_eq!(port.engine().live_leases(), 0);
    // The caller's cancellation and the shutdown happened after the log
    // froze, so they're still queued and were never logged.
    let kinds: Vec<_> = port
        .take_facts()
        .into_iter()
        .map(|fact| fact.kind)
        .collect();
    assert!(matches!(
        kinds.as_slice(),
        [
            FactKind::CallerDropped { .. },
            FactKind::PermitDropped { .. },
            FactKind::ShutDown { .. },
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
                let storage = zakura_sim::storage::HeaderStorage::bounded(sim, port.clone(), bound);
                sim.add_source(Box::new(storage));
            },
            &mut checker,
        );
        assert!(
            !matches!(report.end.outcome, Outcome::Violation | Outcome::Harness),
            "{:?}",
            report.end
        );
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
                let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
                sim.add_source(Box::new(storage));
            },
            &mut checker,
        );
        assert!(
            !matches!(report.end.outcome, Outcome::Violation | Outcome::Harness),
            "{:?}",
            report.end
        );
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
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port.clone());
            sim.add_source(Box::new(storage));
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
        let mut acquires = BTreeSet::new();
        // Whether the transaction being read ended an abandoned lease.
        let mut abandoned = false;
        for entry in report.log.entries() {
            let Observation::Storage { fact, .. } = &entry.obs else {
                continue;
            };
            match fact {
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::AcquireHeaderPath { .. },
                    ..
                } => {
                    acquires.insert(*id);
                }
                StorageFact::CapabilityEnded { release, .. } => {
                    abandoned |= release == "lease-abandoned-acquire";
                }
                StorageFact::Completed { id, .. } if abandoned => {
                    assert!(
                        acquires.contains(id),
                        "operation {id} took an abandoned lease"
                    );
                }
                StorageFact::ReplyDropped { id } if abandoned => {
                    assert!(
                        acquires.contains(id),
                        "operation {id}'s dropped reply took an abandoned lease"
                    );
                    dropped += 1;
                }
                _ => {}
            }
            if is_head(fact) {
                abandoned = false;
            }
        }
        waited += u32::from(seen.waited);
        interleaved += seen.interleaved;
    }
    assert!(waited > 0, "no plan registered a capacity waiter");
    assert!(interleaved > 0, "no plan completed storage mid-drop");
    assert!(dropped > 0, "no dropped reply ended an abandoned lease");
}

/// C+ owner encoding: a header owner, a body-repair owner, and a VCT
/// repair's body owner, each varied in one field at a time, all encode
/// differently, so no owner field is lost.
#[test]
fn every_owner_field_reaches_the_encoding() {
    let base = HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(1),
        branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
    };
    let authorities = [
        base,
        HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(2),
            ..base
        },
        HeaderWorkAuthority {
            branch: BranchId::new(block::Hash([3; 32]), block::Hash([2; 32])),
            ..base
        },
        HeaderWorkAuthority {
            branch: BranchId::new(block::Hash([1; 32]), block::Hash([4; 32])),
            ..base
        },
    ];
    let id = |n| NonZeroU64::new(n).expect("request IDs start at 1");
    let body = |header, verified, epoch| BodyWorkAuthority {
        header,
        verified_generation: VerifiedGeneration::new(verified),
        body_work_epoch: BodyWorkEpoch::new(epoch),
    };
    // Each varies one field of the first, which has every field at 1.
    let mut bodies: Vec<BodyWorkOwner> = authorities
        .iter()
        .map(|authority| body(*authority, 1, 1).bind(1, id(1)))
        .collect();
    bodies.extend([
        body(base, 2, 1).bind(1, id(1)),
        body(base, 1, 2).bind(1, id(1)),
        body(base, 1, 1).bind(2, id(1)),
        body(base, 1, 1).bind(1, id(2)),
    ]);
    let mut headers: Vec<HeaderSyncWorkOwner> = authorities
        .iter()
        .map(|authority| HeaderSyncWorkOwner::Header(authority.bind(1, id(1))))
        .collect();
    headers.extend([
        HeaderSyncWorkOwner::Header(base.bind(2, id(1))),
        HeaderSyncWorkOwner::Header(base.bind(1, id(2))),
    ]);
    let owners = headers
        .into_iter()
        .chain(bodies.iter().copied().map(HeaderSyncWorkOwner::BodyRepair));
    let mut encodings: Vec<String> = owners
        .map(|owner| {
            serde_json::to_string(&zakura_sim::storage::work_owner(owner))
                .expect("an owner encodes")
        })
        .collect();
    encodings.extend(bodies.into_iter().map(|owner| {
        let request = RequestIdentity::VctRepairContext {
            owner,
            height: block::Height(1),
        };
        serde_json::to_string(&zakura_sim::storage::request(&request)).expect("a request encodes")
    }));
    let distinct: BTreeSet<&String> = encodings.iter().collect();
    assert_eq!(distinct.len(), encodings.len(), "{encodings:#?}");
}

/// A storage source that forwards everything to `inner` but skips the
/// observation at step `skip`, as a buggy wrapper might.
struct SkipAt<S> {
    inner: S,
    skip: u64,
}

impl<S: ActionSource> ActionSource for SkipAt<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.skip {
            return Ok(());
        }
        self.inner.observe(log)
    }
}

/// A run where one task reads membership through a controlled port, with
/// the storage source added as `wrap` makes it.
fn wrapped_run(
    wrap: impl FnOnce(zakura_sim::storage::HeaderStorage) -> Box<dyn ActionSource>,
) -> RunReport {
    run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let caller = port.clone();
            sim.scheduler().spawn_root("caller", async move {
                let anchor = HeaderFixture::regtest().anchor();
                let _ = caller.selected_chain_contains(anchor.hash).await;
            });
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(wrap(storage));
        },
        &mut zakura_sim::NoChecks,
    )
}

/// [`wrapped_run`] with the source skipping its observation at `skip`, if
/// any.
fn skipping_run(skip: Option<u64>) -> RunReport {
    wrapped_run(|inner| match skip {
        Some(skip) => Box::new(SkipAt { inner, skip }),
        None => Box::new(inner),
    })
}

/// A storage source that forwards everything to `inner`, but at step `at`
/// observes into a copy of the log, or observes and then logs the last
/// entry again, as buggy wrappers might.
struct Divert<S> {
    inner: S,
    at: u64,
    duplicate: bool,
}

impl<S: ActionSource> ActionSource for Divert<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() != self.at {
            return self.inner.observe(log);
        }
        if !self.duplicate {
            return self.inner.observe(&mut log.clone());
        }
        let cursor = log.entries().len();
        self.inner.observe(log)?;
        if let Some(last) = log.entries()[cursor..].last() {
            log.push(last.obs.clone());
        }
        Ok(())
    }
}

/// The run checks its own log against what storage records it delivered,
/// so storage that delivers into a copy of the log, at setup, an interior
/// step or the final step, or whose delivery is logged twice, ends the run
/// at that step.
#[test]
fn storage_delivered_anywhere_but_the_run_log_ends_the_run() {
    let whole = skipping_run(None);
    let last = whole.end.steps;
    let delivering: BTreeSet<u64> = whole
        .log
        .entries()
        .iter()
        .filter(|entry| matches!(entry.obs, Observation::Storage { .. }))
        .map(|entry| entry.step)
        .collect();
    let interior = *delivering
        .iter()
        .find(|step| **step != 0 && **step != last)
        .expect("storage delivers at an interior step");
    assert!(delivering.contains(&last), "the final step delivers");
    for (at, duplicate) in [
        (0, false),
        (interior, false),
        (last, false),
        (interior, true),
    ] {
        let report = wrapped_run(|inner| {
            Box::new(Divert {
                inner,
                at,
                duplicate,
            })
        });
        assert_eq!(report.end.outcome, Outcome::Harness, "at {at}");
        assert_eq!(report.end.steps, at);
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("Undelivered"), "at {at}: {detail}");
    }
}

/// C1: the run holds each evidence source's checkpoint, so a wrapper that
/// skips one observation ends the run at that boundary, whether it's the
/// setup, an interior step or the final step.
#[test]
fn a_skipped_observation_ends_the_run_there() {
    let whole = skipping_run(None);
    assert_eq!(
        whole.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    let last = whole.end.steps;
    assert!(last > 2, "the run has an interior step");
    for (skip, failure) in [
        (0, "NotObserved"),
        (1, "BoundarySkipped"),
        (last, "BoundarySkipped"),
    ] {
        let report = skipping_run(Some(skip));
        assert_eq!(report.end.outcome, Outcome::Harness, "skipping {skip}");
        assert_eq!(report.end.steps, skip);
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with(failure), "skipping {skip}: {detail}");
    }
}

/// C1: storage registers its checkpoint when it's created, so one the run
/// never gets as a source still fails the run at setup, rather than
/// leaving its evidence unchecked.
#[test]
fn storage_the_run_never_observes_fails_at_setup() {
    let mut kept = None;
    let report = run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            kept = Some(zakura_sim::storage::HeaderStorage::new(sim, port));
        },
        &mut zakura_sim::NoChecks,
    );
    drop(kept);
    assert_eq!(report.end.outcome, Outcome::Harness);
    let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
        panic!("expected a source evidence failure, got {:?}", report.end);
    };
    assert_eq!(source, "storage:header#0");
    assert!(detail.starts_with("NotObserved"), "{detail}");
}

/// Storage binds its port at setup, so a port used between its creation
/// and the setup observation fails setup, rather than handing the run
/// facts from before it was bound.
#[test]
fn a_port_used_before_setup_fails_it() {
    let report = run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let storage = zakura_sim::storage::HeaderStorage::new(sim, Arc::clone(&port));
            let mut call = port.selected_chain_contains(HeaderFixture::regtest().anchor().hash);
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(call.as_mut().poll(&mut cx).is_pending());
            sim.add_source(Box::new(storage));
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(report.end.steps, 0, "setup fails");
    let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
        panic!("expected a source evidence failure, got {:?}", report.end);
    };
    assert!(detail.starts_with("activity before binding"), "{detail}");
}

/// A storage source that forwards everything to `inner`, but at step `at`
/// observes into a copy of the log and logs what it delivered there after
/// `edit` changes it, as a wrapper that normalizes or redacts might.
struct Rewrite<S> {
    inner: S,
    at: u64,
    edit: fn(&mut [Observation]),
}

impl<S: ActionSource> ActionSource for Rewrite<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() != self.at {
            return self.inner.observe(log);
        }
        let mut copy = log.clone();
        let cursor = copy.entries().len();
        self.inner.observe(&mut copy)?;
        let mut delivered: Vec<Observation> = copy.entries()[cursor..]
            .iter()
            .map(|entry| entry.obs.clone())
            .collect();
        (self.edit)(&mut delivered);
        for obs in delivered {
            log.push(obs);
        }
        Ok(())
    }
}

/// Leaves the delivery as it was.
fn unchanged(_: &mut [Observation]) {}

/// Changes the first fact's payload, keeping its sequence number. A step
/// observes twice when a task runs, and the second delivery may be empty.
fn alter_fact(delivered: &mut [Observation]) {
    if let Some(fact) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::Storage { fact, .. } => Some(fact),
        _ => None,
    }) {
        *fact = StorageFact::CallerDropped { id: 999 };
    }
}

/// Changes the state, keeping its watermark.
fn alter_state(delivered: &mut [Observation]) {
    if let Some(state) = delivered.iter_mut().find_map(|obs| match obs {
        Observation::StorageState { state, .. } => Some(state),
        _ => None,
    }) {
        state.last_lease_id += 1;
    }
}

/// Moves the state ahead of the fact before it.
fn swap_fact_and_state(delivered: &mut [Observation]) {
    let state = delivered
        .iter()
        .position(|obs| matches!(obs, Observation::StorageState { .. }));
    if let Some(state) = state.filter(|state| *state > 0) {
        delivered.swap(state - 1, state);
    }
}

/// The run log must hold exactly what storage delivered, so a wrapper that
/// relogs a delivery with a fact's payload or the state changed, keeping
/// their numbers, or with a fact and the state swapped, fails the run with
/// `Undelivered`. Relogging it unchanged passes.
#[test]
fn storage_entries_must_reach_the_log_unchanged() {
    let whole = skipping_run(None);
    let at = whole
        .log
        .entries()
        .iter()
        .find(|entry| entry.step > 0 && matches!(entry.obs, Observation::Storage { .. }))
        .expect("storage delivers a fact after setup")
        .step;
    let relogged = |edit| wrapped_run(move |inner| Box::new(Rewrite { inner, at, edit }));
    assert_eq!(
        relogged(unchanged).end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    for (name, edit) in [
        ("fact", alter_fact as fn(&mut [Observation])),
        ("state", alter_state),
        ("order", swap_fact_and_state),
    ] {
        let report = relogged(edit);
        assert_eq!(report.end.outcome, Outcome::Harness, "{name}");
        assert_eq!(report.end.steps, at, "{name}");
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("Undelivered"), "{name}: {detail}");
    }
}

/// Storage belongs to the run that created it. Storage made with a first
/// run that's then dropped, and added to a second run, whether through a
/// wrapper that skips its final observation or plainly, fails the second
/// run instead of letting its evidence go unchecked there.
#[test]
fn storage_from_a_dropped_run_fails_a_later_one() {
    let last = skipping_run(None).end.steps;
    for wrapped in [true, false] {
        let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
        let mut first = Sim::new(storage_config(100), PlanInput::Bytes(Vec::new()));
        let storage = zakura_sim::storage::HeaderStorage::new(&mut first, port.clone());
        drop(first);
        let mut second = Sim::new(storage_config(100), PlanInput::Bytes(Vec::new()));
        second.scheduler().spawn_root("caller", async move {
            let anchor = HeaderFixture::regtest().anchor();
            let _ = port.selected_chain_contains(anchor.hash).await;
        });
        if wrapped {
            second.add_source(Box::new(SkipAt {
                inner: storage,
                skip: last,
            }));
        } else {
            second.add_source(Box::new(storage));
        }
        let report = second.run(
            &mut zakura_sim::NoChecks,
            &mut zakura_sim::MemorySink::default(),
        );
        assert_eq!(report.end.outcome, Outcome::Harness, "wrapped: {wrapped}");
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("ForeignRun"), "{detail}");
    }
}

/// A run with two storage instances on their own ports, each read by its
/// own task, with instance `divert`, if any, observing into a copy of the
/// log at setup.
fn two_storage_run(divert: Option<u64>) -> RunReport {
    run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            for instance in 0..2 {
                let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
                let caller = port.clone();
                sim.scheduler().spawn_root(
                    ["caller-0", "caller-1"][instance as usize],
                    async move {
                        let anchor = HeaderFixture::regtest().anchor();
                        let _ = caller.selected_chain_contains(anchor.hash).await;
                    },
                );
                let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
                if divert == Some(instance) {
                    sim.add_source(Box::new(Divert {
                        inner: storage,
                        at: 0,
                        duplicate: false,
                    }));
                } else {
                    sim.add_source(Box::new(storage));
                }
            }
        },
        &mut zakura_sim::NoChecks,
    )
}

/// Storage instances are told apart by their registration index, not
/// their service: two instances that both deliver pass, and either one
/// delivering into a copy of the log fails, though the other's matching
/// entries are in the run log.
#[test]
fn each_storage_instance_is_checked_on_its_own() {
    let both = two_storage_run(None);
    assert_eq!(
        both.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        both.end
    );
    for divert in 0..2 {
        let report = two_storage_run(Some(divert));
        assert_eq!(report.end.outcome, Outcome::Harness, "diverting {divert}");
        assert_eq!(report.end.steps, 0, "the diverted setup fails");
        let Some(HarnessFailure::SourceEvidence { source, detail }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert_eq!(*source, format!("storage:header#{divert}"));
        assert!(detail.starts_with("Undelivered"), "{detail}");
    }
}

/// A storage source that forwards everything to `inner`, then drops it
/// right after its observation at step `at`, as a buggy wrapper might.
struct DropAfter<S> {
    inner: Option<S>,
    at: u64,
}

impl<S: ActionSource> ActionSource for DropAfter<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(inner) = &self.inner {
            inner.enabled(now, out);
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(inner) = &self.inner {
            inner.boundaries(now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner
            .as_mut()
            .expect("a dropped source offers no actions")
            .execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let Some(inner) = &mut self.inner else {
            return Ok(());
        };
        inner.observe(log)?;
        if log.step() == self.at {
            self.inner = None;
        }
        Ok(())
    }
}

/// Storage dropped while the run is live, right after its final
/// observation or at an interior step, fails the run, since its shutdown
/// facts are never observed. The run's own shutdown, after the log
/// freezes, still passes.
#[test]
fn storage_dropped_during_the_run_fails_it() {
    let whole = skipping_run(None);
    assert_eq!(
        whole.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    let last = whole.end.steps;
    for at in [last, 1] {
        let report = wrapped_run(|inner| {
            Box::new(DropAfter {
                inner: Some(inner),
                at,
            })
        });
        assert_eq!(report.end.outcome, Outcome::Harness, "dropping at {at}");
        assert_eq!(report.end.steps, at);
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("Dropped"), "dropping at {at}: {detail}");
    }
}

/// The world the harness-failure scenario's property reads: nothing.
struct Calm;

impl HasView for Calm {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

/// Holds at every step.
fn always(_: &mut CheckCtx<'_, Calm>) -> Verdict {
    Verdict::Holds
}

/// D2: a property holds at step 1, then a task steals the port's facts at
/// step 2, so storage's evidence fails there. The run ends at the harness
/// failure, and the property reports `harness-failed`, not `held`, both in
/// the report and in the serialized end record.
#[test]
fn a_property_is_inconclusive_after_an_evidence_failure() {
    const CALM: PropertyId = PropertyId::new("test/always");
    let property = Property {
        id: CALM,
        claim: "always holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: PropertyStatus::Active,
        check: always,
        witness: None,
    };
    let registry = Registry::new(vec![property]).expect("one property");
    let mut checker = RegistryChecker::new(
        registry,
        Rc::new(Calm),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let (report, lines) = recorded(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let thief = port.clone();
            sim.scheduler().spawn_root("thief", async move {
                zakura_rt::task::yield_now().await;
                let anchor = HeaderFixture::regtest().anchor();
                let mut call = thief.selected_chain_contains(anchor.hash);
                std::future::poll_fn(|cx| {
                    assert!(call.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                thief.take_facts();
                std::future::pending::<()>().await;
            });
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(Box::new(storage));
        },
        &mut checker,
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert_eq!(report.end.steps, 2);
    let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
        panic!("expected a source evidence failure, got {:?}", report.end);
    };
    // The stolen facts are the last ones, so the state is ahead of them.
    assert!(detail.starts_with("Stale"), "{detail}");
    let inconclusive = PropertyOutcome::Inconclusive {
        reason: "harness-failed".to_owned(),
    };
    assert_eq!(report.properties[0].outcome, inconclusive);
    let end = lines.last().expect("the run ends with an end record");
    assert!(
        end.contains(r#""inconclusive":{"reason":"harness-failed"}"#),
        "{end}"
    );
}

/// The storage facts and the other observations a run logged at `step`.
fn step_log(report: &RunReport, step: u64) -> Vec<&Observation> {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| entry.step == step)
        .map(|entry| &entry.obs)
        .collect()
}

/// The sequence numbers of the storage facts in `observations`.
fn seqs(observations: &[&Observation]) -> Vec<u64> {
    observations
        .iter()
        .filter_map(|obs| match obs {
            Observation::Storage { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

/// D3, task side: one poll takes a reply and submits two operations. In
/// that step the three storage facts come in sequence order, then the
/// state, and only then the task's own outcome.
#[test]
fn a_tasks_storage_facts_precede_its_outcome() {
    let report = run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let caller = port.clone();
            sim.scheduler().spawn_root("caller", async move {
                let anchor = HeaderFixture::regtest().anchor();
                let first = caller.selected_chain_contains(anchor.hash);
                let mut calls = [
                    caller.selected_chain_contains(anchor.hash),
                    caller.selected_chain_contains(anchor.hash),
                ];
                let _ = first.await;
                std::future::poll_fn(|cx| {
                    let mut done = true;
                    for call in &mut calls {
                        done &= call.as_mut().poll(cx).is_ready();
                    }
                    if done {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                })
                .await;
            });
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(Box::new(storage));
        },
        &mut zakura_sim::NoChecks,
    );
    assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    let step = report
        .log
        .entries()
        .iter()
        .find(|entry| {
            matches!(
                entry.obs,
                Observation::Storage {
                    fact: StorageFact::ReplyTaken { .. },
                    ..
                }
            )
        })
        .expect("the caller took its first reply")
        .step;
    let logged = step_log(&report, step);
    let facts: Vec<_> = logged
        .iter()
        .filter_map(|obs| match obs {
            Observation::Storage { fact, .. } => Some(fact),
            _ => None,
        })
        .collect();
    assert!(
        matches!(
            facts.as_slice(),
            [
                StorageFact::ReplyTaken { id: 1 },
                StorageFact::Submitted { id: 2, .. },
                StorageFact::Submitted { id: 3, .. },
            ]
        ),
        "{facts:?}"
    );
    assert!(seqs(&logged).is_sorted());
    assert!(
        matches!(
            logged.as_slice(),
            [
                Observation::Storage { .. },
                Observation::Storage { .. },
                Observation::Storage { .. },
                Observation::StorageState { .. },
                Observation::TaskWaiting { .. },
            ]
        ),
        "{logged:?}"
    );
}

/// Peer `n`'s acquire in session 1 of the empty path to the anchor.
fn anchor_acquire(port: &ControlledPort, n: u8) -> AcquirePath {
    let anchor = HeaderFixture::regtest().anchor();
    AcquirePath {
        source: SourceId::from_digest([n; 32]),
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), anchor.hash),
        target_tip_hash: anchor.hash,
        locator_hashes: vec![anchor.hash],
    }
}

/// D3, source side: one completion sweeps peer 1's idle lease, firing the
/// waiter a refused acquire left on it, then leases to peer 2. Its step
/// logs the four lease facts and the completion in sequence order.
#[test]
fn a_completions_lease_facts_come_in_order() {
    let report = run(
        storage_config(200),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let caller = port.clone();
            sim.scheduler().spawn_root("caller", async move {
                let held = caller.acquire_header_path(anchor_acquire(&caller, 1)).await;
                let refused = caller.acquire_header_path(anchor_acquire(&caller, 1)).await;
                assert!(matches!(refused, Ok(AcquirePathReply::CapacityBusy(_))));
                zakura_rt::time::sleep(Duration::from_secs(31)).await;
                let other = caller.acquire_header_path(anchor_acquire(&caller, 2)).await;
                assert!(matches!(other, Ok(AcquirePathReply::Acquired(_))));
                drop((held, refused, other));
            });
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(Box::new(storage));
        },
        &mut zakura_sim::NoChecks,
    );
    assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    let step = report
        .log
        .entries()
        .iter()
        .find(|entry| {
            matches!(
                &entry.obs,
                Observation::Storage {
                    fact: StorageFact::CapabilityEnded { release, .. },
                    ..
                } if release == "lease-idle-expired"
            )
        })
        .expect("the third acquire swept the idle lease")
        .step;
    let logged = step_log(&report, step);
    let facts: Vec<_> = logged
        .iter()
        .filter_map(|obs| match obs {
            Observation::Storage { fact, .. } => Some(fact),
            _ => None,
        })
        .collect();
    assert!(
        matches!(
            facts.as_slice(),
            [
                StorageFact::CapabilityEnded { .. },
                StorageFact::WaiterFired { .. },
                StorageFact::RetentionEnded { .. },
                StorageFact::Leased { .. },
                StorageFact::Completed { id: 3, .. },
            ]
        ),
        "{facts:?}"
    );
    assert!(seqs(&logged).is_sorted());
}

/// Retained after release: an acquire, then its release with no read,
/// completed by the run, which ends the live lease the release names. A
/// release that kept the lease and recorded nothing would end the run
/// with `MissingEffect`.
#[test]
fn a_release_ends_its_live_lease() {
    let report = run(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
            let caller = port.clone();
            sim.scheduler().spawn_root("caller", async move {
                let Ok(AcquirePathReply::Acquired(path)) =
                    caller.acquire_header_path(anchor_acquire(&caller, 1)).await
                else {
                    panic!("the only peer gets a lease");
                };
                caller
                    .release_header_path(*path)
                    .await
                    .expect("the engine outcome comes first");
            });
            let storage = zakura_sim::storage::HeaderStorage::new(sim, port);
            sim.add_source(Box::new(storage));
        },
        &mut zakura_sim::NoChecks,
    );
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        report.end
    );
    let facts: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { fact, .. } => Some(fact),
            _ => None,
        })
        .collect();
    let leased = facts
        .iter()
        .find_map(|fact| match fact {
            StorageFact::Leased { lease, .. } => Some(lease),
            _ => None,
        })
        .expect("the acquire leased");
    let release = facts
        .iter()
        .find_map(|fact| match fact {
            StorageFact::Submitted {
                request: StorageRequest::ReleaseHeaderPath { lease, .. },
                ..
            } => lease.as_ref(),
            _ => None,
        })
        .expect("the release was submitted");
    assert_eq!(release, leased, "the release names the live lease");
    assert!(facts.iter().any(|fact| matches!(
        fact,
        StorageFact::CapabilityEnded { lease, release }
            if lease == leased && release == "lease-released"
    )));
    assert!(facts.iter().any(|fact| matches!(
        fact,
        StorageFact::Completed {
            result: StorageResult::Released,
            ..
        }
    )));
}

/// Where [`late_drop_run`] drops its storage.
#[derive(Copy, Clone, Debug, PartialEq)]
enum LateDrop {
    /// Nowhere: the run's own shutdown drops it.
    Never,
    /// In the checker's `after_step` at step `.0`.
    AfterStep(u64),
    /// In the checker's `at_end`.
    AtEnd,
    /// In the sink, as it takes step `.0`'s record.
    Sink(u64),
    /// In the checker's `at_end`, which then also draws from the plan,
    /// harness code's `DrawOutsideStep`.
    AtEndDrawing,
}

/// Storage a source and a cleanup checker or sink share.
type Shared = Rc<std::cell::RefCell<Option<zakura_sim::storage::HeaderStorage>>>;

/// A source that forwards to shared storage while it's there.
struct SharedSource(Shared);

impl ActionSource for SharedSource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(inner) = &*self.0.borrow() {
            inner.enabled(now, out);
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(inner) = &*self.0.borrow() {
            inner.boundaries(now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.0
            .borrow_mut()
            .as_mut()
            .expect("dropped storage offers no actions")
            .execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        match &mut *self.0.borrow_mut() {
            Some(inner) => inner.observe(log),
            None => Ok(()),
        }
    }
}

/// A checker that drops the shared storage at `when`, and records the
/// outcome its `at_end` saw.
struct Dropper {
    shared: Shared,
    when: LateDrop,
    saw: Rc<std::cell::Cell<Option<Outcome>>>,
}

impl zakura_sim::Checker for Dropper {
    fn after_step(&mut self, cx: &zakura_sim::StepCtx<'_>) -> Option<Violation> {
        if self.when == LateDrop::AfterStep(cx.step) {
            self.shared.borrow_mut().take();
        }
        None
    }

    fn at_end(
        &mut self,
        end: &zakura_sim::RunEnd,
        _: &ObservationLog,
    ) -> Vec<zakura_sim::PropertyReport> {
        self.saw.set(Some(end.outcome));
        if matches!(self.when, LateDrop::AtEnd | LateDrop::AtEndDrawing) {
            self.shared.borrow_mut().take();
        }
        if self.when == LateDrop::AtEndDrawing {
            // Seeding a map draws.
            let _: zakura_rt::collections::HashMap<u8, u8> = Default::default();
        }
        Vec::new()
    }
}

/// A sink that keeps every record and drops the shared storage at `when`.
struct DroppingSink {
    shared: Shared,
    when: LateDrop,
    records: zakura_sim::MemorySink,
}

impl zakura_sim::RecordSink for DroppingSink {
    fn record(&mut self, record: zakura_sim::Record) {
        if let zakura_sim::Record::Step(step) = &record {
            if self.when == LateDrop::Sink(step.step) {
                self.shared.borrow_mut().take();
            }
        }
        self.records.record(record);
    }
}

/// One task's membership read, with a property that always holds, and the
/// storage dropped at `when`. Returns the report, the records, and the
/// outcome the checkers' `at_end` saw.
fn late_drop_run(when: LateDrop) -> (RunReport, Vec<String>, Outcome) {
    let mut sim = Sim::new(storage_config(100), PlanInput::Bytes(Vec::new()));
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let caller = port.clone();
    sim.scheduler().spawn_root("caller", async move {
        let anchor = HeaderFixture::regtest().anchor();
        let _ = caller.selected_chain_contains(anchor.hash).await;
    });
    let storage = zakura_sim::storage::HeaderStorage::new(&mut sim, port);
    let shared: Shared = Rc::new(std::cell::RefCell::new(Some(storage)));
    sim.add_source(Box::new(SharedSource(shared.clone())));
    let property = Property {
        id: PropertyId::new("test/always"),
        claim: "always holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: PropertyStatus::Active,
        check: always,
        witness: None,
    };
    let registry = Registry::new(vec![property]).expect("one property");
    let calm = RegistryChecker::new(
        registry,
        Rc::new(Calm),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let saw = Rc::new(std::cell::Cell::new(None));
    let mut checker = zakura_sim::property::Tee(
        calm,
        Dropper {
            shared: shared.clone(),
            when,
            saw: saw.clone(),
        },
    );
    let mut sink = DroppingSink {
        shared,
        when,
        records: zakura_sim::MemorySink::default(),
    };
    let report = sim.run(&mut checker, &mut sink);
    let saw = saw.get().expect("the run called at_end");
    (report, sink.records.lines(), saw)
}

/// Any drop of storage before the log freezes fails the run, wherever it
/// happens: in the last step's `after_step`, in `at_end`, or in the sink as
/// it takes the last step's record. A drop before `at_end` already reaches
/// it as a harness failure. The property reports `harness-failed` in the
/// report and in the end record. The run's own shutdown, after the log
/// freezes, still passes.
#[test]
fn storage_dropped_after_the_last_step_fails_the_run() {
    let (whole, _, _) = late_drop_run(LateDrop::Never);
    assert_eq!(
        whole.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        whole.end
    );
    assert_eq!(
        whole.properties[0].outcome,
        PropertyOutcome::Held { applicable: 3 }
    );
    let last = whole.end.steps;
    for when in [
        LateDrop::AfterStep(last),
        LateDrop::AtEnd,
        LateDrop::Sink(last),
    ] {
        let (report, lines, saw) = late_drop_run(when);
        assert_eq!(report.end.outcome, Outcome::Harness, "{when:?}");
        if when != LateDrop::AtEnd {
            assert_eq!(saw, Outcome::Harness, "{when:?} reaches at_end");
        }
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with("Dropped"), "{when:?}: {detail}");
        assert_eq!(
            report.properties[0].outcome,
            PropertyOutcome::Inconclusive {
                reason: "harness-failed".to_owned()
            },
            "{when:?}"
        );
        let end = lines.last().expect("the run ends with an end record");
        assert!(
            end.contains(r#""inconclusive":{"reason":"harness-failed"}"#),
            "{when:?}: {end}"
        );
    }
}

/// The property's outcome after any harness failure, in the report and
/// the end record alike.
fn assert_harness_failed(report: &RunReport, lines: &[String]) {
    assert_eq!(
        report.properties[0].outcome,
        PropertyOutcome::Inconclusive {
            reason: "harness-failed".to_owned()
        }
    );
    let end = lines.last().expect("the run ends with an end record");
    assert!(
        end.contains(r#""inconclusive":{"reason":"harness-failed"}"#),
        "{end}"
    );
}

/// A drop inside `at_end` that also draws ends at `DrawOutsideStep`, which
/// outranks the drop, and the property still reports `harness-failed`,
/// though `at_end` reported it held.
#[test]
fn a_dropping_at_end_that_draws_is_still_inconclusive() {
    let (report, lines, saw) = late_drop_run(LateDrop::AtEndDrawing);
    assert_eq!(
        saw,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        }
    );
    assert_eq!(report.end.harness, Some(HarnessFailure::DrawOutsideStep));
    assert_harness_failed(&report, &lines);
}

/// A checker that draws from the plan in `at_end`.
struct DrawsAtEnd;

impl zakura_sim::Checker for DrawsAtEnd {
    fn after_step(&mut self, _: &zakura_sim::StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(
        &mut self,
        _: &zakura_sim::RunEnd,
        _: &ObservationLog,
    ) -> Vec<zakura_sim::PropertyReport> {
        // Seeding a map draws.
        let _: zakura_rt::collections::HashMap<u8, u8> = Default::default();
        Vec::new()
    }
}

/// D2 covers any harness failure, not only storage's: with no storage at
/// all, a draw in `at_end` leaves a property that held inconclusive.
#[test]
fn a_drawing_at_end_without_storage_is_inconclusive() {
    let property = Property {
        id: PropertyId::new("test/always"),
        claim: "always holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: PropertyStatus::Active,
        check: always,
        witness: None,
    };
    let registry = Registry::new(vec![property]).expect("one property");
    let calm = RegistryChecker::new(
        registry,
        Rc::new(Calm),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let mut checker = zakura_sim::property::Tee(calm, DrawsAtEnd);
    let (report, lines) = recorded(
        storage_config(100),
        PlanInput::Bytes(Vec::new()),
        |sim| {
            sim.scheduler().spawn_root("idle", async {
                zakura_rt::task::yield_now().await;
            });
        },
        &mut checker,
    );
    assert_eq!(report.end.harness, Some(HarnessFailure::DrawOutsideStep));
    assert_harness_failed(&report, &lines);
}

/// What [`live_port_run`] does with the port beyond the run's own use.
#[derive(Copy, Clone, Debug, PartialEq)]
enum Poke {
    /// Nothing.
    Nothing,
    /// Polls a call on another thread, outside the bound run, just before
    /// storage's observation at step `.0`.
    RefuseBefore(u64),
    /// Polls a call on another thread in the checker's `at_end`.
    RefuseAtEnd,
    /// Shuts the port down in the checker's `at_end`, with the storage
    /// still alive.
    ShutDownAtEnd,
}

/// Polls a membership call on `port` from a thread with no run, which a
/// bound port refuses.
fn poll_outside_the_run(port: &ControlledPort) {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let anchor = HeaderFixture::regtest().anchor();
                let mut call = port.selected_chain_contains(anchor.hash);
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(call.as_mut().poll(&mut cx).is_pending());
            })
            .join()
            .expect("the poll doesn't panic");
    });
}

/// A storage source that, at step `at`, uses its port from outside the
/// run just before observing.
struct RefuseBefore<S> {
    inner: S,
    port: Arc<ControlledPort>,
    at: u64,
}

impl<S: ActionSource> ActionSource for RefuseBefore<S> {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        self.inner.enabled(now, out);
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.inner.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.inner.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.step() == self.at {
            poll_outside_the_run(&self.port);
        }
        self.inner.observe(log)
    }
}

/// A checker that uses the port in `at_end` as `poke` says.
struct PokeAtEnd {
    port: Arc<ControlledPort>,
    poke: Poke,
}

impl zakura_sim::Checker for PokeAtEnd {
    fn after_step(&mut self, _: &zakura_sim::StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(
        &mut self,
        _: &zakura_sim::RunEnd,
        _: &ObservationLog,
    ) -> Vec<zakura_sim::PropertyReport> {
        match self.poke {
            Poke::RefuseAtEnd => poll_outside_the_run(&self.port),
            Poke::ShutDownAtEnd => self.port.shut_down(),
            Poke::Nothing | Poke::RefuseBefore(_) => {}
        }
        Vec::new()
    }
}

/// One task's membership read, with a property that always holds, and the
/// port used as `poke` says. Returns the report and the records.
fn live_port_run(poke: Poke) -> (RunReport, Vec<String>) {
    let mut sim = Sim::new(storage_config(100), PlanInput::Bytes(Vec::new()));
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let caller = port.clone();
    sim.scheduler().spawn_root("caller", async move {
        let anchor = HeaderFixture::regtest().anchor();
        let _ = caller.selected_chain_contains(anchor.hash).await;
    });
    let storage = zakura_sim::storage::HeaderStorage::new(&mut sim, port.clone());
    match poke {
        Poke::RefuseBefore(at) => sim.add_source(Box::new(RefuseBefore {
            inner: storage,
            port: port.clone(),
            at,
        })),
        _ => sim.add_source(Box::new(storage)),
    };
    let property = Property {
        id: PropertyId::new("test/always"),
        claim: "always holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View],
        status: PropertyStatus::Active,
        check: always,
        witness: None,
    };
    let registry = Registry::new(vec![property]).expect("one property");
    let calm = RegistryChecker::new(
        registry,
        Rc::new(Calm),
        Assumptions::none(),
        &Sources::observed(&[Source::View]),
    );
    let mut checker = zakura_sim::property::Tee(calm, PokeAtEnd { port, poke });
    let mut sink = zakura_sim::MemorySink::default();
    let report = sim.run(&mut checker, &mut sink);
    (report, sink.lines())
}

/// Every evidence check reads the port live, so use outside the run, or
/// facts published after storage's last observation, fail the run at that
/// check, at a step or at the final check before the log freezes. The
/// property then reports `harness-failed`. A run whose port does nothing
/// beyond what storage observed passes.
#[test]
fn the_port_is_checked_live_at_every_evidence_check() {
    let (quiet, _) = live_port_run(Poke::Nothing);
    assert_eq!(
        quiet.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        quiet.end
    );
    assert_eq!(
        quiet.properties[0].outcome,
        PropertyOutcome::Held { applicable: 3 }
    );
    let last = quiet.end.steps;
    for (poke, step, failure) in [
        (Poke::RefuseBefore(1), 1, "RefusedOutsideRun"),
        (Poke::RefuseAtEnd, last, "RefusedOutsideRun"),
        (Poke::ShutDownAtEnd, last, "Unobserved"),
    ] {
        let (report, lines) = live_port_run(poke);
        assert_eq!(report.end.outcome, Outcome::Harness, "{poke:?}");
        assert_eq!(report.end.steps, step, "{poke:?}");
        let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
            panic!("expected a source evidence failure, got {:?}", report.end);
        };
        assert!(detail.starts_with(failure), "{poke:?}: {detail}");
        assert_harness_failed(&report, &lines);
    }
}
