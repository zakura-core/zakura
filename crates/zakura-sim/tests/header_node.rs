//! A header-sync node on controlled storage (M4-B1): the node's real reactor
//! and publisher submit every storage call to one `ControlledPort`, and every
//! completion comes from the `Plan`.
#![cfg(zakura_sim)]

mod support;

use support::{config, inputs};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::mpsc;
use zakura_sim::{
    harness::{
        header_node::{
            guard_dispatch,
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, DispatchFlag, Fork, Policy,
        },
        header_sync::{
            Dialer, MirrorSpec, NodeChecks, Ports, Storage, StorageScenario, DIRECT_DISPATCH,
        },
        run_prepared, HarnessError, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport, Sim,
};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, ports)
}

/// Every request the node's storage saw submitted, in order.
fn requests(report: &RunReport) -> Vec<StorageRequest> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Submitted { request, .. },
                ..
            } => Some(request.clone()),
            _ => None,
        })
        .collect()
}

/// A mirror holding `chain`.
fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec { chain, dialer }
}

/// The hex peer identity of the actor whose key is `seed` repeated, as
/// storage attribution writes it.
fn peer(seed: u8) -> String {
    let id = NodeSecretKey::from_bytes(&[seed; 32]).public();
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// The node at genesis syncs to `mirror-0`'s A5 under every plan, through a
/// locator, a prepare and an apply on its own port, each completed by the
/// converging policy.
#[test]
fn node_syncs_from_a_mirror_on_controlled_storage() {
    let scenario = StorageScenario::default();
    for input in inputs(32, 256) {
        let (report, ports) = run(&scenario, input);
        for entry in report.log.entries() {
            if let Observation::Storage {
                fact: StorageFact::Completed { outcome, .. },
                ..
            } = &entry.obs
            {
                assert!(
                    ["engine", "rejected"].contains(&outcome.as_str()),
                    "{outcome}"
                );
            }
        }
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(5).tip(&ports.fixture),
            "the node reaches mirror-0's tip"
        );
        let requests = requests(&report);
        for kind in [
            |r: &StorageRequest| matches!(r, StorageRequest::ContinuationLocator),
            |r: &StorageRequest| matches!(r, StorageRequest::PrepareHeaderTarget { .. }),
            |r: &StorageRequest| matches!(r, StorageRequest::ApplyHeaderTarget { .. }),
        ] {
            assert!(requests.iter().any(kind), "{requests:?}");
        }
    }
}

/// The worked example: `mirror-0`, on a heavier fork, dials the node, and the
/// node dials `mirror-1` at genesis. The node's requester and publisher both
/// use its one port, so its log holds both in one ID sequence, which
/// `NodeChecks` checks, and `mirror-1` follows it to `mirror-0`'s tip.
#[test]
fn one_port_serves_requester_and_publisher() {
    let scenario = StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            mirror(
                Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 4 }),
                },
                Dialer::Mirror,
            ),
            mirror(Chain::default(), Dialer::Node),
        ],
        ..StorageScenario::default()
    };
    // Actors' key seeds follow their order, and links count sessions from 1
    // in mirror order. `mirror-0` also pulls the node's own branch, so both
    // peers acquire from the node's publisher.
    let (mirror_0, mirror_1) = ((peer(2), 1), (peer(3), 2));
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let requests = requests(&report);
        let (mut requester, mut mirror_1_acquires, mut reads) = (0, 0, 0);
        for request in &requests {
            let lease = match request {
                StorageRequest::ContinuationLocator
                | StorageRequest::PrepareHeaderTarget { .. }
                | StorageRequest::ApplyHeaderTarget { .. } => {
                    requester += 1;
                    continue;
                }
                StorageRequest::AcquireHeaderPath {
                    peer, session_id, ..
                } => {
                    mirror_1_acquires += usize::from(*peer == mirror_1.0);
                    Some((peer, *session_id))
                }
                StorageRequest::ReadHeaderPath { lease, .. } => {
                    reads += 1;
                    lease.as_ref().map(|lease| (&lease.peer, lease.session_id))
                }
                StorageRequest::ReleaseHeaderPath { lease, .. } => {
                    lease.as_ref().map(|lease| (&lease.peer, lease.session_id))
                }
                // Membership reads carry no peer or session.
                _ => continue,
            };
            // Each path operation carries the session of the peer it serves.
            assert!(
                [&mirror_0, &mirror_1]
                    .into_iter()
                    .any(|(peer, session)| lease == Some((peer, *session))),
                "{request:?}"
            );
        }
        assert!(
            requester > 0 && mirror_1_acquires > 0 && reads > 0,
            "the node's log has requester and publisher operations: {requests:?}"
        );
        let tip = ports.node.engine().snapshot().frontiers.header_best;
        assert_eq!(tip, scenario.mirrors[0].chain.tip(&ports.fixture));
        assert_eq!(
            ports.mirrors[1].snapshot().frontiers.header_best,
            tip,
            "mirror-1 follows the node"
        );
    }
}

/// A reactor that sends an operation to its action channel, which only
/// `External` dispatch does, fails the run as `harness/direct-dispatch` on
/// the step the guard sees it.
#[test]
fn direct_dispatch_guard_reports_a_violation() {
    let flag = DispatchFlag::default();
    let mut sim = Sim::new(config(2, 100), inputs(1, 0).remove(0));
    let (actions, received) = mpsc::channel(1);
    sim.scheduler().spawn_root("reactor", async move {
        actions
            .send(())
            .await
            .expect("the guard holds the receiver");
    });
    guard_dispatch(&mut sim, received, flag.clone());
    let report = sim.run(&mut NodeChecks::new(flag), &mut MemorySink::default());
    let violation = report.end.violation.expect("the guard trips");
    assert_eq!(violation.property, DIRECT_DISPATCH);
    let pick = &report.picks[usize::try_from(violation.step).unwrap() - 1];
    assert!(
        pick.identity.starts_with("task:dispatch-guard"),
        "{:?}",
        report.picks
    );
}

/// `Control` closes `mirror-0`'s link and relinks it on a new connection and
/// session, at points the `Plan` picks. The node still reaches `mirror-0`'s
/// tip, and some plan closes early enough that it syncs over the new session.
#[test]
fn control_closes_and_relinks_a_mirror() {
    let scenario = StorageScenario {
        controls: [ControlAction::Close, ControlAction::Relink]
            .into_iter()
            .map(|action| ControlStep {
                peer: "mirror-0".to_owned(),
                action,
                when: None,
            })
            .collect(),
        ..StorageScenario::default()
    };
    let mut over_relink = 0;
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let picks: Vec<&str> = report.picks.iter().map(|p| p.identity.as_str()).collect();
        assert!(picks.contains(&"peer:mirror-0:close") && picks.contains(&"peer:mirror-0:relink"));
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(5).tip(&ports.fixture)
        );
        // The relink is the second link, so its session is 2.
        over_relink += usize::from(requests(&report).iter().any(|request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { owner, .. } if owner.session_id == 2)
        }));
    }
    assert!(over_relink > 0, "no plan synced over the new session");
}

/// A `Control` close takes effect, not just its pick. With no relink to
/// retire the session, only the close can end it, so both sides' session
/// pipes must exit, and only after the close.
#[test]
fn control_close_ends_both_sides_of_the_session() {
    let scenario = StorageScenario {
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Close,
            when: None,
        }],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, _) = run(&scenario, input);
        let close = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:mirror-0:close")
            .expect("the close is picked")
            .step;
        let pipes: Vec<(u64, &str)> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::TaskExited { task, .. } if task.starts_with("header_sync_pipe") => {
                    Some((entry.step, task.as_str()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            pipes.len(),
            2,
            "the node's and mirror-0's pipes both end: {pipes:?}"
        );
        assert!(
            pipes.iter().all(|(step, _)| *step > close),
            "after the close at step {close}: {pipes:?}"
        );
    }
}

/// Each storage fact of Nora's for operations of `kind`, with the virtual
/// time it was logged at in nanoseconds and its step.
fn facts_of(report: &RunReport, kind: fn(&StorageRequest) -> bool) -> Vec<(u64, u64, StorageFact)> {
    let (mut now, mut ids) = (0, std::collections::BTreeSet::new());
    let mut out = Vec::new();
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = *to_ns,
            Observation::Storage {
                instance: 0, fact, ..
            } => {
                let id = match fact {
                    StorageFact::Submitted { id, request, .. } if kind(request) => {
                        ids.insert(*id);
                        *id
                    }
                    StorageFact::Submitted { .. } => continue,
                    StorageFact::Completed { id, .. } => *id,
                    _ => continue,
                };
                if ids.contains(&id) {
                    out.push((now, entry.step, fact.clone()));
                }
            }
            _ => {}
        }
    }
    out
}

/// Nora's first apply, held for 10 s of virtual time, completes no sooner
/// than that, while every other operation follows the 5 s bound.
#[test]
fn held_apply_waits_for_its_virtual_time() {
    let scenario = StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Apply, 1),
                until: Trigger::VirtualTime(10_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, ports) = run(&scenario, input);
        let applies = facts_of(&report, |request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { .. })
        });
        let [(submitted, _, StorageFact::Submitted { .. }), (completed, _, StorageFact::Completed { .. }), ..] =
            applies.as_slice()
        else {
            panic!("the apply is submitted and completes: {applies:?}");
        };
        assert!(
            completed - submitted >= 10_000_000_000,
            "held from {submitted} ns to {completed} ns"
        );
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(5).tip(&ports.fixture)
        );
    }
}

/// A `Control` step gated on Nora's first apply completing is never picked
/// before that completion.
#[test]
fn control_step_waits_for_its_trigger() {
    let scenario = StorageScenario {
        controls: vec![ControlStep {
            peer: "mira".to_owned(),
            action: ControlAction::Close,
            when: Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
        }],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, _) = run(&scenario, input);
        let applies = facts_of(&report, |request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { .. })
        });
        let completed = applies.iter().find_map(|(_, step, fact)| {
            matches!(fact, StorageFact::Completed { .. }).then_some(*step)
        });
        let close = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:mira:close")
            .expect("the close opens and is picked")
            .step;
        // An early close can also stop the sync, so no apply completes.
        assert!(
            completed.is_some_and(|completed| close > completed),
            "close at {close}, apply completed at {completed:?}"
        );
    }
}

/// A `Control` step on `peer`.
fn step(action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: "mira".to_owned(),
        action,
        when,
    }
}

/// `Closed` counts connections, not close calls. Closing Mira's link twice
/// closes one connection, so a relink gated on two closures never opens.
/// Closing a second, relinked connection does open it.
#[test]
fn closing_twice_counts_one_closure() {
    let closures = |n| {
        Some(Trigger::Count(
            n,
            Box::new(Trigger::Closed("mira".to_owned())),
        ))
    };
    let links = |n| {
        Some(Trigger::Count(
            n,
            Box::new(Trigger::Linked("mira".to_owned())),
        ))
    };
    let twice = StorageScenario {
        controls: vec![
            step(ControlAction::Close, None),
            step(ControlAction::Close, None),
            step(ControlAction::Relink, closures(2)),
        ],
        ..StorageScenario::default()
    };
    let distinct = StorageScenario {
        controls: vec![
            step(ControlAction::Close, None),
            step(ControlAction::Relink, closures(1)),
            step(ControlAction::Close, links(2)),
            step(ControlAction::Relink, closures(2)),
        ],
        ..StorageScenario::default()
    };
    for input in inputs(4, 256) {
        let (report, _) = run(&twice, input.clone());
        let picks: Vec<&str> = report.picks.iter().map(|p| p.identity.as_str()).collect();
        let count = |action| picks.iter().filter(|pick| **pick == action).count();
        assert_eq!(count("peer:mira:close"), 2);
        assert_eq!(count("peer:mira:relink"), 0, "one connection closed");

        let (report, _) = run(&distinct, input);
        let relinks = report
            .picks
            .iter()
            .filter(|pick| pick.identity == "peer:mira:relink")
            .count();
        assert_eq!(relinks, 2, "two connections closed");
    }
}

/// Mira grows to A6 mid-run, through her own port. Once the growth is
/// witnessed, `Control` closes her link and relinks her, and Nora follows
/// her to the new tip.
#[test]
fn a_grown_mirror_leads_nora_to_its_new_tip() {
    let mira = || "mira".to_owned();
    let scenario = StorageScenario {
        controls: vec![
            step(ControlAction::Grow { len: 1 }, None),
            step(ControlAction::Close, Some(Trigger::Grown(mira()))),
            step(ControlAction::Relink, Some(Trigger::Closed(mira()))),
        ],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, ports) = run(&scenario, input);
        let relinked = report
            .picks
            .iter()
            .any(|pick| pick.identity == "peer:mira:relink");
        assert!(relinked, "the growth, close and relink all happened");
        let a6 = Chain::main(6).tip(&ports.fixture);
        assert_eq!(ports.mirrors[0].snapshot().frontiers.header_best, a6);
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            a6,
            "Nora reaches Mira's new tip"
        );
    }
}

/// A `Grow` that storage can't admit fails the run instead of counting as
/// grown. Mira starts 64,000 headers past A5 with finality at genesis, so
/// growing her by 4,000 more passes the 65,536 retained-header limit, and
/// the apply stalls without admitting the target.
#[test]
fn a_stalled_growth_fails_the_run() {
    use zakura_header_chain::Frontier;
    use zakura_node_services::header_chain::Port;

    /// Polls `future` once, outside a run.
    fn ready<F: std::future::Future>(future: F) -> F::Output {
        let waker = std::task::Waker::noop();
        match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(waker)) {
            std::task::Poll::Ready(output) => output,
            std::task::Poll::Pending => panic!("an engine port finishes on its first poll"),
        }
    }

    let scenario = StorageScenario {
        controls: vec![
            step(ControlAction::Grow { len: 4_000 }, None),
            step(
                ControlAction::Close,
                Some(Trigger::Grown("mira".to_owned())),
            ),
        ],
        ..StorageScenario::default()
    };
    let ports = Storage::prepare(&scenario);
    let mira = &ports.mirrors[0];
    for request_id in 1..=16 {
        let tip: Frontier = mira.snapshot().frontiers.header_best;
        let headers = ports.fixture.extend(tip, 4_000, 0);
        let request = ports
            .fixture
            .target_request(&mira.snapshot(), tip, &headers, request_id);
        let prepared = ready(mira.prepare_header_target(request)).expect("a batch prepares");
        ready(mira.apply_header_target(prepared)).expect("a batch applies");
    }
    let before = mira.snapshot().frontiers.header_best;
    assert_eq!(before.height.0, 64_005);

    let error = run_prepared::<Storage>(
        &scenario,
        ports.clone(),
        inputs(1, 0).remove(0),
        &mut MemorySink::default(),
        None,
    )
    .expect_err("the stalled growth fails the run");
    let HarnessError::Run { failure, report } = error else {
        panic!("{error}");
    };
    assert!(failure.to_string().contains("ControlFailed"), "{failure}");
    assert!(failure.to_string().contains("ResourceStalled"), "{failure}");
    assert_eq!(
        mira.snapshot().frontiers.header_best,
        before,
        "Mira didn't grow"
    );
    assert!(
        !report
            .picks
            .iter()
            .any(|pick| pick.identity == "peer:mira:close"),
        "the gated close never opened"
    );
}
