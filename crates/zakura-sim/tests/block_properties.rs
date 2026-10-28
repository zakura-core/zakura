//! The block-sync scenario's validation histories (BS5, gates BS.6–BS.8):
//! sim.md's six cases, an honest block flow, a stale query after a reset, a
//! delayed apply that a reset detaches, a late body, a serving overflow and
//! a teardown, each pass on the real node. Two bypass witnesses, direct work
//! insertion and an alternate storage dispatch, write block sync's owner
//! state past its block service, and the observations and the view show
//! it. Every case repeats A/A and A/B/A, and replays Exact in a fresh
//! process.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeSet,
    process::{Command, Output},
};

use support::inputs;
use tempfile::TempDir;
use zakura_network::zakura::{BlockSyncMessage, MSG_BS_BLOCK};
use zakura_sim::{
    harness::{
        block_node::{
            control::{ControlAction, ControlStep, Fork},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            probe::ProbeFrame,
            view::BlockSyncView,
            Policy, PROBE,
        },
        block_sync::{BlockScenario, MirrorSpec, Ports, Storage},
        header_sync::Dialer,
        run_prepared, Subsystem,
    },
    observe::{BlockSyncFact, BlockSyncHolder, StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run. The run must end with no violation and no
/// harness failure.
fn node_run(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
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
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    (report, ports)
}

/// One mirror that dials the node and one the node dials.
fn two_mirrors() -> Vec<MirrorSpec> {
    vec![
        MirrorSpec {
            dialer: Dialer::Mirror,
        },
        MirrorSpec {
            dialer: Dialer::Node,
        },
    ]
}

/// Every operation of `kind`.
fn every(kind: BlockKind) -> Select {
    Select {
        kind: Some(kind),
        ..Select::default()
    }
}

/// A hold on `select` until `until`, then offering `then`.
fn hold(select: Select, until: Trigger, then: Vec<BlockAllow>) -> Hold {
    Hold {
        select,
        until,
        then,
        deadline_ms: None,
    }
}

/// Case 1: two mirrors serve 16 blocks under the converging policy.
fn honest_flow() -> BlockScenario {
    BlockScenario {
        blocks: 16,
        mirrors: two_mirrors(),
        ..BlockScenario::default()
    }
}

/// Case 2: the node's first query waits until a fork at height 4 has reset
/// the node's headers, so its answer comes under a scope that's gone.
fn stale_query() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::Query, 1),
                Trigger::Reset("node".to_owned()),
                vec![BlockAllow::Answer],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::Reset {
                fork: Fork {
                    branch: 1,
                    at: 4,
                    len: 8,
                },
                node_first: false,
            },
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Query, 1))),
        }],
        ..BlockScenario::default()
    }
}

/// Case 3: the node holds two bodies, and its first submission, height 3,
/// waits five seconds for its verdict. A fork at height 3 resets the node's
/// headers meanwhile, which detaches the waiting submission.
fn delayed_apply() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        node_bodies: 2,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::Apply, 1),
                Trigger::VirtualTime(5_000),
                vec![
                    BlockAllow::Committed,
                    BlockAllow::Duplicate,
                    BlockAllow::Retryable,
                ],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::Reset {
                fork: Fork {
                    branch: 1,
                    at: 3,
                    len: 8,
                },
                node_first: false,
            },
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
        }],
        ..BlockScenario::default()
    }
}

/// Case 4: two mirrors, and the first range read either serves waits ten
/// seconds, past block sync's 8 s request timeout, so its body arrives
/// after the node's request for it timed out.
fn late_body() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        mirrors: two_mirrors(),
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::MirrorRead, 1),
                Trigger::VirtualTime(10_000),
                vec![BlockAllow::Serve],
            )],
            ..Policy::converging()
        },
        ..BlockScenario::default()
    }
}

/// The node's serving limit in the serving-overflow case.
const SERVING_LIMIT: u32 = 2;

/// Case 5: a node holding all eight bodies serves at most
/// [`SERVING_LIMIT`] requests at once, and its range reads wait until the
/// probe's link closes. The probe sends one request per height at once,
/// without waiting for any response, which commits the node past twice its
/// limit.
fn serving_overflow() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        node_bodies: 8,
        mirrors: Vec::new(),
        probe: true,
        serving_limit: Some(SERVING_LIMIT),
        policy: Policy {
            holds: vec![hold(
                every(BlockKind::Read),
                Trigger::Closed(PROBE.to_owned()),
                vec![BlockAllow::Serve],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::ProbeSend {
                frames: (1..=8)
                    .map(|start_height| ProbeFrame::GetBlocks {
                        start_height,
                        count: 1,
                    })
                    .collect(),
            },
            when: None,
        }],
        ..BlockScenario::default()
    }
}

/// Case 6: the node's verdicts wait until its link closes, and it's torn
/// down once two submissions wait, so its sequencer holds bodies then.
fn teardown() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                every(BlockKind::Apply),
                Trigger::Closed("mirror-0".to_owned()),
                vec![BlockAllow::Committed],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::Teardown,
            when: Some(Trigger::Count(
                2,
                Box::new(Trigger::Submitted(every(BlockKind::Apply))),
            )),
        }],
        ..BlockScenario::default()
    }
}

/// Bypass 1: the node's first query waits a second, and the work goes in
/// straight through its handle as soon as the query arrives.
fn insert_work() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::Query, 1),
                Trigger::VirtualTime(1_000),
                vec![BlockAllow::Answer],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::InsertWork,
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Query, 1))),
        }],
        ..BlockScenario::default()
    }
}

/// Bypass 2: the node's first verdict waits a second, and a committed
/// verdict goes in straight through its handle as soon as the submission
/// is admitted.
fn dispatch_verdict() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::Apply, 1),
                Trigger::VirtualTime(1_000),
                vec![BlockAllow::Committed],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::DispatchVerdict,
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
        }],
        ..BlockScenario::default()
    }
}

/// Every case and bypass, by name.
fn cases() -> Vec<(&'static str, BlockScenario)> {
    vec![
        ("honest flow", honest_flow()),
        ("stale query", stale_query()),
        ("delayed apply", delayed_apply()),
        ("late body", late_body()),
        ("serving overflow", serving_overflow()),
        ("teardown", teardown()),
        ("insert work", insert_work()),
        ("dispatch verdict", dispatch_verdict()),
    ]
}

/// The step of `report`'s first pick of `identity`.
fn picked(report: &RunReport, identity: &str) -> Option<u64> {
    report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .map(|pick| pick.step)
}

/// The node's block storage facts in `report`, with their steps.
fn facts(report: &RunReport) -> Vec<(u64, &StorageFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { service, fact, .. } if service == "block" => {
                Some((entry.step, fact))
            }
            _ => None,
        })
        .collect()
}

/// The node's block-sync events in `report`, with their steps, in order.
fn events(report: &RunReport) -> Vec<(u64, &BlockSyncFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::BlockSync { event, .. } => Some((entry.step, event)),
            _ => None,
        })
        .collect()
}

/// The step at which block storage completed operation `id`, if it did.
fn completed(report: &RunReport, id: u64) -> Option<u64> {
    facts(report)
        .into_iter()
        .find_map(|(step, fact)| match fact {
            StorageFact::BlockCompleted { id: done, .. } if *done == id => Some(step),
            _ => None,
        })
}

/// Each block storage operation of `report`: its ID, its request, and the
/// step it arrived at.
fn submissions(report: &RunReport) -> Vec<(u64, &StorageRequest, u64)> {
    facts(report)
        .into_iter()
        .filter_map(|(step, fact)| match fact {
            StorageFact::BlockSubmitted { id, request, .. } => Some((*id, request, step)),
            _ => None,
        })
        .collect()
}

/// The step of the first event about the detached holder of `token` that
/// made `change`.
fn detached(report: &RunReport, token: u64, change: &str) -> Option<u64> {
    events(report)
        .into_iter()
        .find_map(|(step, event)| match event {
            BlockSyncFact::Holder {
                holder: BlockSyncHolder::Detached { token: held },
                change: made,
                ..
            } if *held == token && made == change => Some(step),
            _ => None,
        })
}

/// The node converged on the live chain: its published verified tip is the
/// chain's tip.
fn assert_converged(ports: &Ports) {
    assert_eq!(
        ports.node.view().frontiers.verified_best,
        ports.live.tip(),
        "the node commits the live chain"
    );
}

/// The view's owners hold nothing, and the fold applied every event.
fn assert_settled(view: &BlockSyncView) {
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
    assert_eq!(view.reserved, 0, "the byte budget drains");
    assert!(view.handoffs.is_empty(), "{:?}", view.handoffs);
    assert!(view.holders.is_empty(), "{:?}", view.holders);
    assert!(view.leases.is_empty(), "{:?}", view.leases);
}

/// Case 1: the node submits every height, and each submission's owner
/// names a request published before it for that height: its session,
/// request and scope. The owner's verified generation moves on as the node
/// commits, so the case doesn't compare it. The view ends settled.
#[test]
fn an_honest_block_flow_submits_only_what_it_requested() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&honest_flow(), input);
        assert_converged(&ports);
        let events = events(&report);
        let requested: Vec<_> = events
            .iter()
            .filter_map(|(step, event)| match event {
                BlockSyncFact::Request {
                    owner,
                    heights,
                    step: moved,
                    ..
                } if moved == "published" => Some((*step, owner, heights)),
                _ => None,
            })
            .collect();
        let mut heights = BTreeSet::new();
        for (_, request, at) in submissions(&report) {
            let StorageRequest::SubmitBlock {
                owner,
                height: Some(height),
                ..
            } = request
            else {
                continue;
            };
            heights.insert(*height);
            assert!(
                requested.iter().any(|(step, by, asked)| *step < at
                    && (by.session_id, by.request_id, &by.scope)
                        == (owner.session_id, owner.request_id, &owner.scope)
                    && asked.contains(height)),
                "the submission of {height} names a request published before it"
            );
        }
        assert_eq!(heights, (1..=16).collect(), "every height is submitted");
        assert_settled(&ports.view.borrow());
    }
}

/// Case 2: the first query is answered only after the node's headers
/// switched to the fork, so its answer carries the old body-work epoch.
/// Block sync asks again under the new one, and the node commits the fork.
#[test]
fn a_stale_query_after_a_reset_is_asked_again() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&stale_query(), input);
        assert_eq!(ports.live.tip().height.0, 12);
        assert_converged(&ports);
        let reset = picked(&report, "peer:node:reset-apply").expect("the node resets");
        let queries: Vec<_> = submissions(&report)
            .into_iter()
            .filter_map(|(id, request, _)| match request {
                StorageRequest::QueryNeededBlocks { scope, .. } => Some((id, scope)),
                _ => None,
            })
            .collect();
        let (first, scope) = queries[0];
        assert!(
            completed(&report, first).expect("the held query is answered") > reset,
            "the first query is answered after the reset"
        );
        assert!(
            queries[1..]
                .iter()
                .any(|(_, later)| later.body_work_epoch > scope.body_work_epoch),
            "block sync asks again under the new epoch"
        );
        assert_settled(&ports.view.borrow());
    }
}

/// Case 3: the submission of height 3 waits for its verdict when the reset
/// lands, so the sequencer detaches it and keeps it charged. Its late
/// verdict releases the detached holder, and the view ends settled.
///
/// The node then stops downloading the fork: its work queue keeps the
/// fork's heights pending and no routine requests them, under every plan
/// and verdict tried. So the case doesn't claim convergence.
#[test]
fn a_reset_detaches_a_delayed_apply_and_its_late_verdict_releases_it() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&delayed_apply(), input);
        let (op, token) = submissions(&report)
            .into_iter()
            .find_map(|(id, request, _)| match request {
                StorageRequest::SubmitBlock {
                    height: Some(3),
                    token,
                    ..
                } => Some((id, *token)),
                _ => None,
            })
            .expect("height 3 is submitted");
        let retained = detached(&report, token, "retained").expect("the reset detaches it");
        let verdict = completed(&report, op).expect("the delayed verdict arrives");
        let released = detached(&report, token, "released").expect("the verdict releases it");
        assert!(retained < verdict && verdict <= released);
        let view = ports.view.borrow();
        assert!(view.sequencer.submissions.is_empty());
        assert_settled(&view);
    }
}

/// Case 4: the held read's body reaches the node after its request timed
/// out. The node judges the late body on its connection, closes no link
/// for it, and commits every body.
#[test]
fn a_late_body_is_judged_without_closing_its_link() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&late_body(), input);
        assert_converged(&ports);
        let (served, mirror) = report
            .picks
            .iter()
            .find_map(|pick| {
                let index = pick
                    .identity
                    .strip_prefix("storage:mirror-")?
                    .strip_suffix(":1:serve")?;
                Some((pick.step, format!("mirror-{index}")))
            })
            .expect("the held read is served");
        let links = ports.effects.links.borrow();
        let link = links
            .iter()
            .find(|link| link.peer == mirror)
            .expect("the mirror links");
        assert!(
            events(&report).into_iter().any(|(step, event)| matches!(
                event,
                BlockSyncFact::FrameJudged { connection, message_type, .. }
                    if *connection == link.conn_id && *message_type == u16::from(MSG_BS_BLOCK) && step > served
            )),
            "the late body reaches the node"
        );
        assert!(links.iter().all(|link| !link.is_closed()), "{links:?}");
        assert_settled(&ports.view.borrow());
    }
}

/// Case 5: the node advertises its serving limit. The probe's requests
/// exceed twice that while none is served, so the node closes the probe's
/// connection, after starting no more range reads than it committed to.
/// Every read the node started pairs with a lease, and every lease ends
/// released.
#[test]
fn a_serving_overflow_closes_the_flooding_peer() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&serving_overflow(), input);
        let advertised = ports
            .probe
            .received()
            .into_iter()
            .find_map(|message| match message {
                Some(BlockSyncMessage::Status(status)) => Some(status.max_inflight_requests),
                _ => None,
            })
            .expect("the node advertises its status");
        assert_eq!(advertised, SERVING_LIMIT);
        let links = ports.effects.links.borrow();
        let probe = links
            .iter()
            .find(|link| link.peer == PROBE)
            .expect("the probe links");
        assert_eq!(probe.cause.get(), Some("service_protocol_reject"));
        let reads: BTreeSet<_> = submissions(&report)
            .into_iter()
            .filter(|(_, request, _)| matches!(request, StorageRequest::ReadRange { .. }))
            .map(|(id, _, _)| id)
            .collect();
        assert!(!reads.is_empty());
        let committed = usize::try_from(2 * SERVING_LIMIT).expect("a small u32 fits a usize");
        assert!(reads.len() <= committed, "{reads:?}");
        let view = ports.view.borrow();
        assert_eq!(view.reads.keys().copied().collect::<BTreeSet<_>>(), reads);
        assert_settled(&view);
    }
}

/// Case 6: tearing the node down ends its block sync while its sequencer
/// holds two bodies. The sequencer reports its close, releases every body
/// it holds, and publishes an empty snapshot. The tap expects the observer
/// to go, so the run ends healthy, and the view ends at zero.
#[test]
fn a_teardown_returns_the_node_to_zero() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&teardown(), input);
        let torn = picked(&report, "peer:node:teardown").expect("the node is torn down");
        let events = events(&report);
        let closed = events
            .iter()
            .position(|(_, event)| matches!(event, BlockSyncFact::SequencerClosed))
            .expect("the sequencer closes");
        assert!(events[closed].0 > torn);
        let after = &events[closed + 1..];
        assert!(
            after.iter().any(|(_, event)| matches!(
                event,
                BlockSyncFact::Holder { change, .. } if change == "released"
            )),
            "the sequencer releases what it held"
        );
        let last = after
            .iter()
            .rev()
            .find_map(|(_, event)| match event {
                BlockSyncFact::Sequencer {
                    reorder,
                    applying,
                    submissions,
                } => Some(reorder.is_empty() && applying.is_empty() && submissions.is_empty()),
                _ => None,
            })
            .expect("the sequencer publishes a final snapshot");
        assert!(last, "the final snapshot is empty");
        let view = ports.view.borrow();
        assert!(view.sequencer_closed);
        assert_settled(&view);
        assert!(view.work_queue.iter().all(|item| item.reserved_bytes == 0));
    }
}

/// A torn-down node takes no links, so relinking after a teardown fails
/// the run.
#[test]
fn a_relink_after_a_teardown_fails_the_run() {
    let scenario = BlockScenario {
        control: vec![
            ControlStep {
                action: ControlAction::Teardown,
                when: None,
            },
            ControlStep {
                action: ControlAction::Relink {
                    peer: "mirror-0".to_owned(),
                },
                when: Some(Trigger::Closed("mirror-0".to_owned())),
            },
        ],
        ..BlockScenario::default()
    };
    let error = run_prepared::<Storage>(
        &scenario,
        Storage::prepare(&scenario),
        PlanInput::Bytes(Vec::new()),
        &mut MemorySink::default(),
        None,
    )
    .expect_err("the relink fails the run");
    assert!(
        error
            .to_string()
            .contains("ControlFailed: a torn-down node takes no links"),
        "{error}"
    );
}

/// Bypass 1, direct work insertion: block sync's routines request bodies
/// before its block service answers the query, so the work queue holds
/// work that no storage answer backs. The service's late answer is stale,
/// and the node still commits every body.
#[test]
fn inserted_work_reaches_the_work_queue_without_an_answer() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&insert_work(), input);
        assert_converged(&ports);
        let inserted = picked(&report, "peer:node:insert-work").expect("the work goes in");
        let answered = completed(&report, 1).expect("the held query is answered");
        assert!(inserted < answered);
        let events = events(&report);
        assert!(
            events.iter().any(|(step, event)| *step < answered
                && matches!(event, BlockSyncFact::WorkQueue { items } if !items.is_empty())),
            "the work queue holds work before the answer"
        );
        assert!(
            events.iter().any(|(at, event)| *at < answered
                && matches!(event, BlockSyncFact::Request { step, .. } if step == "published")),
            "a routine requests it before the answer"
        );
        assert_settled(&ports.view.borrow());
    }
}

/// Bypass 2, an alternate storage dispatch: the sequencer finishes the
/// first submission, releasing its holder, while block storage still holds
/// the operation uncommitted. Storage's own commit comes later, and the
/// node still commits every body.
#[test]
fn a_dispatched_verdict_releases_a_submission_storage_still_holds() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&dispatch_verdict(), input);
        assert_converged(&ports);
        let (op, token) = submissions(&report)
            .into_iter()
            .find_map(|(id, request, _)| match request {
                StorageRequest::SubmitBlock { token, .. } => Some((id, *token)),
                _ => None,
            })
            .expect("a body is submitted");
        let dispatched =
            picked(&report, "peer:node:dispatch-verdict").expect("the verdict goes in");
        let released = events(&report)
            .into_iter()
            .find_map(|(step, event)| match event {
                BlockSyncFact::Holder {
                    holder: BlockSyncHolder::Submitted { token: held },
                    change,
                    ..
                } if *held == token && change == "released" => Some(step),
                _ => None,
            })
            .expect("the sequencer releases the submission");
        let committed = completed(&report, op).expect("storage commits it later");
        assert!(dispatched <= released && released < committed);
        assert_settled(&ports.view.borrow());
    }
}

/// Every pick and every observation of `report`, in order.
fn history(report: &RunReport) -> Vec<String> {
    report
        .picks
        .iter()
        .map(|pick| format!("{} {}", pick.step, pick.identity))
        .chain(
            report
                .log
                .entries()
                .iter()
                .map(|entry| format!("{} {:?}", entry.step, entry.obs)),
        )
        .collect()
}

/// Each case's history depends only on its scenario and plan: plan A twice
/// gives one history, and so does A, then B, then A. B's differs, so the
/// comparison can fail.
#[test]
fn every_case_repeats_a_a_and_a_b_a() {
    let plans = inputs(2, 256);
    for (name, scenario) in cases() {
        let histories: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
            .into_iter()
            .map(|plan| history(&node_run(&scenario, plan.clone()).0))
            .collect();
        assert!(histories[0] == histories[1], "{name}: plan A, then A");
        assert!(
            histories[0] == histories[3],
            "{name}: plan A, then B, then A"
        );
        assert!(histories[0] != histories[2], "{name}: plan B differs");
    }
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// BS.8: `record` runs each case's scenario under a plan in one process,
/// and `replay` reruns its trace Exact in another.
#[test]
fn every_case_replays_exact_in_a_fresh_process() {
    let PlanInput::Bytes(plan) = &inputs(1, 256)[0] else {
        unreachable!("fuzz inputs are bytes");
    };
    for (name, scenario) in cases() {
        let dir = TempDir::new().unwrap();
        let plan_file = dir.path().join("plan");
        let trace = dir.path().join("trace");
        std::fs::write(&plan_file, plan).unwrap();
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &serde_json::to_string(&scenario).unwrap(),
            "--plan",
            plan_file.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
