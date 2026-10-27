//! A header-sync node on controlled storage (M4-B1): Nora's real reactor and
//! publisher submit every storage call to one `ControlledPort`, and every
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
        run_prepared, Subsystem,
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

/// Every request Nora's storage saw submitted, in order.
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

/// A mirror named `name` holding `chain`.
fn mirror(name: &str, chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec {
        name: name.to_owned(),
        chain,
        dialer,
    }
}

/// The hex peer identity of the actor whose key is `seed` repeated, as
/// storage attribution writes it.
fn peer(seed: u8) -> String {
    let id = NodeSecretKey::from_bytes(&[seed; 32]).public();
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// Nora at genesis syncs to Mira's A5 under every plan, through a locator,
/// a prepare and an apply on her own port, each completed by the
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
            "Nora reaches Mira's tip"
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

/// The worked example: Mira, on a heavier fork, dials Nora, and Nora dials
/// Leo at genesis. Nora's requester and publisher both use her one port, so
/// her log holds both in one ID sequence, which `NodeChecks` checks, and
/// Leo follows her to Mira's tip.
#[test]
fn one_port_serves_requester_and_publisher() {
    let scenario = StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            mirror(
                "mira",
                Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 4 }),
                },
                Dialer::Mirror,
            ),
            mirror("leo", Chain::default(), Dialer::Node),
        ],
        ..StorageScenario::default()
    };
    // Actors' key seeds follow their order, and links count sessions from 1
    // in mirror order. Mira also pulls Nora's own branch, so both peers
    // acquire from Nora's publisher.
    let (mira, leo) = ((peer(2), 1), (peer(3), 2));
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let requests = requests(&report);
        let (mut requester, mut leo_acquires, mut reads) = (0, 0, 0);
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
                    leo_acquires += usize::from(*peer == leo.0);
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
                [&mira, &leo]
                    .into_iter()
                    .any(|(peer, session)| lease == Some((peer, *session))),
                "{request:?}"
            );
        }
        assert!(
            requester > 0 && leo_acquires > 0 && reads > 0,
            "Nora's log has requester and publisher operations: {requests:?}"
        );
        let tip = ports.node.engine().snapshot().frontiers.header_best;
        assert_eq!(tip, scenario.mirrors[0].chain.tip(&ports.fixture));
        assert_eq!(
            ports.mirrors[1].snapshot().frontiers.header_best,
            tip,
            "Leo follows Nora"
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

/// `Control` closes Mira's link and relinks her on a new connection and
/// session, at points the `Plan` picks. Nora still reaches Mira's tip, and
/// some plan closes early enough that she syncs over the new session.
#[test]
fn control_closes_and_relinks_a_mirror() {
    let scenario = StorageScenario {
        controls: [ControlAction::Close, ControlAction::Relink]
            .into_iter()
            .map(|action| ControlStep {
                peer: "mira".to_owned(),
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
        assert!(picks.contains(&"peer:mira:close") && picks.contains(&"peer:mira:relink"));
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
            peer: "mira".to_owned(),
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
            .find(|pick| pick.identity == "peer:mira:close")
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
            "Nora's and Mira's pipes both end: {pipes:?}"
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
