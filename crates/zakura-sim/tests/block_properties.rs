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
    cell::RefCell,
    collections::BTreeSet,
    process::{Command, Output},
    rc::Rc,
};

use support::inputs;
use tempfile::TempDir;
use zakura_network::zakura::{block_sync_observe::Holder, BlockSyncMessage, MSG_BS_BLOCK};
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
    Checker, MemorySink, Observation, ObservationLog, Outcome, PlanInput, PropertyReport, RunEnd,
    RunReport, StepCtx, Violation,
};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run. The run must end with no violation and no
/// harness failure, with every sequencer snapshot matching the holder
/// events.
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
    assert_snapshots_match(&ports);
    (report, ports)
}

/// Asserts that the view folded every event, and that each sequencer
/// snapshot matched the holder events.
fn assert_snapshots_match(ports: &Ports) {
    let view = ports.view.borrow();
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
    assert_eq!(view.owners.holder_gap(), None);
}

/// A checker that hands `sample` the node's view after every step.
struct Sampler<F> {
    view: Rc<RefCell<BlockSyncView>>,
    sample: F,
}

impl<F: FnMut(u64, &BlockSyncView)> Checker for Sampler<F> {
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        (self.sample)(cx.step, &self.view.borrow());
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
}

/// [`node_run`], handing `sample` the node's view after every step.
fn sampled_run(
    scenario: &BlockScenario,
    input: PlanInput,
    sample: impl FnMut(u64, &BlockSyncView),
) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let mut sampler = Sampler {
        view: ports.view.clone(),
        sample,
    };
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        Some(&mut sampler),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_snapshots_match(&ports);
    (report, ports)
}

/// One mirror that dials the node and one the node dials.
fn two_mirrors() -> Vec<MirrorSpec> {
    vec![
        MirrorSpec::new(Dialer::Mirror),
        MirrorSpec::new(Dialer::Node),
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
/// the node's headers, and two more seconds, so block sync has seen the new
/// scope before the answer under the old one arrives.
fn stale_query() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::Query, 1),
                Trigger::All(vec![
                    Trigger::Reset("node".to_owned()),
                    Trigger::VirtualTime(2_000),
                ]),
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
                apply_when: None,
            },
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Query, 1))),
        }],
        ..BlockScenario::default()
    }
}

/// Case 3: the node holds two bodies, and its first submission, height 3,
/// waits five seconds for its verdict. A fork at height 2 replaces height 3
/// meanwhile, which detaches the waiting submission.
///
/// The mirror switches to the fork first, so it answers the node's old
/// requests with fork bodies. Block sync rejects those as incompatible and
/// closes the link with an unfinished exchange. The harness models no
/// transport, so a `Relink` on that close stands in for production's redial.
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
        control: vec![
            ControlStep {
                action: ControlAction::Reset {
                    fork: Fork {
                        branch: 1,
                        at: 2,
                        len: 8,
                    },
                    node_first: false,
                    apply_when: None,
                },
                when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
            },
            relink_on_close(),
        ],
        ..BlockScenario::default()
    }
}

/// Mirror 0 links again each time a link to it closes.
fn relink_on_close() -> ControlStep {
    ControlStep {
        action: ControlAction::Relink {
            peer: "mirror-0".to_owned(),
        },
        when: Some(Trigger::Closed("mirror-0".to_owned())),
    }
}

/// When the late-body case links its mirror again: past the 180 s
/// no-progress cooldown that parks the session after the late request.
const RELINK_MS: u64 = 215_000;

/// Case 4: one mirror, whose first range read waits ten seconds, past block
/// sync's 8 s request timeout, so its body arrives after the node's request
/// for it timed out.
///
/// The timed-out request leaves the session's probe cap spent, so block
/// sync parks the session for its no-progress cooldown. Production then
/// reopens the session when the service asks for one; the harness models
/// no transport, so a `Relink` after the cooldown stands in for that.
fn late_body() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![hold(
                Select::nth(BlockKind::MirrorRead, 1),
                Trigger::VirtualTime(10_000),
                vec![BlockAllow::Serve],
            )],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::Relink {
                peer: "mirror-0".to_owned(),
            },
            when: Some(Trigger::VirtualTime(RELINK_MS)),
        }],
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

/// The ID of block storage's first needed-blocks query in `report`.
fn first_query(report: &RunReport) -> u64 {
    submissions(report)
        .into_iter()
        .find_map(|(id, request, _)| {
            matches!(request, StorageRequest::QueryNeededBlocks { .. }).then_some(id)
        })
        .expect("block sync queries its needed blocks first")
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

/// The view's owners hold nothing, the fold applied every event, and every
/// storage operation paired.
fn assert_settled(view: &BlockSyncView) {
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
    assert!(view.pairing.is_empty(), "{:?}", view.pairing);
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

/// Case 2: the first query is answered after block sync saw the fork's
/// headers, so its answer carries the old scope. Block sync drops it: the
/// work queue stays empty until a second query, under the new body-work
/// epoch, is answered. No request goes out under the old epoch, and the
/// node commits the fork.
#[test]
fn a_stale_query_after_a_reset_is_asked_again() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&stale_query(), input);
        assert_eq!(ports.live.tip().height.0, 12);
        assert_converged(&ports);
        let reset = picked(&report, "peer:node:reset-apply").expect("the node resets");
        let queries: Vec<_> = submissions(&report)
            .into_iter()
            .filter_map(|(id, request, step)| match request {
                StorageRequest::QueryNeededBlocks { scope, .. } => Some((id, scope, step)),
                _ => None,
            })
            .collect();
        let (first, stale, _) = queries[0];
        let (second, fresh, asked) = queries[1];
        let answered = completed(&report, first).expect("the held query is answered");
        let renewed = completed(&report, second).expect("the second query is answered");
        assert!(
            reset < answered && answered < asked,
            "{reset} {answered} {asked}"
        );
        assert!(fresh.body_work_epoch > stale.body_work_epoch);
        let events = events(&report);
        assert!(
            !events
                .iter()
                .any(|(step, event)| (answered..renewed).contains(step)
                    && matches!(event, BlockSyncFact::WorkQueue { items } if !items.is_empty())),
            "the stale answer adds no work"
        );
        assert!(
            !events.iter().any(|(_, event)| matches!(
                event,
                BlockSyncFact::Request { owner, step, .. }
                    if step == "published" && owner.body_work_epoch == Some(stale.body_work_epoch)
            )),
            "no request goes out under the old epoch"
        );
        assert_settled(&ports.view.borrow());
    }
}

/// Case 3: the submission of height 3 waits for its verdict when the fork
/// replaces that height, so the sequencer detaches it and keeps it charged.
/// Its late verdict releases the detached holder.
///
/// The late verdict commits the replaced block. Some schedules still reach
/// the fork's tip. The others end in the known stall
/// (`lane3-issues/case3-stall.md`): the engine keeps the body-work epoch,
/// since the commit only extends the verified chain and leaves the selected
/// chain as it was, and the work queue retires the fork's height 3 by
/// height as the verified tip passes it. The tip then stays at the replaced
/// block, and nothing requests the fork's height 3 again, while the fork's
/// heights above it are submitted or held. Every input must end in one of
/// those two shapes, and at least one must converge. Once the planned fix
/// lands, a verified tip that leaves the selected branch starts a new
/// body-work epoch, and every input must converge.
#[test]
#[allow(clippy::print_stdout)]
fn a_reset_detaches_a_delayed_apply_and_its_late_verdict_releases_it() {
    let (mut converged, mut stalled) = (0, 0);
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&delayed_apply(), input);
        let (op, token, hash) = submissions(&report)
            .into_iter()
            .find_map(|(id, request, _)| match request {
                StorageRequest::SubmitBlock {
                    height: Some(3),
                    token,
                    hash,
                    ..
                } => Some((id, *token, hash.clone())),
                _ => None,
            })
            .expect("height 3 is submitted");
        let fork: Vec<(u32, String)> = ports
            .live
            .blocks()
            .into_iter()
            .map(|block| (block.height.0, storage_hash(block.hash())))
            .collect();
        let replacement = fork
            .iter()
            .find(|(height, _)| *height == 3)
            .expect("the fork holds height 3");
        assert_ne!(replacement.1, hash, "the fork replaces height 3");
        let retained = detached(&report, token, "retained").expect("the reset detaches it");
        let verdict = completed(&report, op).expect("the delayed verdict arrives");
        let released = detached(&report, token, "released").expect("the verdict releases it");
        assert!(retained < verdict && verdict <= released);
        assert_eq!(ports.live.tip().height.0, 10);
        let tip = ports.node.view().frontiers.verified_best;
        if tip == ports.live.tip() {
            converged += 1;
            let view = ports.view.borrow();
            assert!(view.sequencer.submissions.is_empty());
            assert_settled(&view);
        } else {
            stalled += 1;
            assert_stalled_at_replaced_height(&report, &ports.view.borrow(), &hash, &fork, verdict);
        }
    }
    println!("case 3: {converged} inputs converged, {stalled} stalled at the replaced height");
    assert!(converged > 0, "some input converges");
}

/// `hash` as block storage's facts write it: hex in its byte order.
fn storage_hash(hash: zakura_chain::block::Hash) -> String {
    hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The known case-3 stall: the run spent its step budget with the last
/// published verified tip at the replaced height-3 block `replaced`, and no
/// holder or work item remains at height 3, so nothing can bring the fork's
/// height 3 back. Sync went on above it: the node submitted, or still
/// holds, each of the fork's heights 4 to 10 (`fork` lists the live chain
/// by height and storage hash). No installed transition after the stale
/// verdict, at step `verdict`, invalidated body work, so the epoch stayed.
fn assert_stalled_at_replaced_height(
    report: &RunReport,
    view: &BlockSyncView,
    replaced: &str,
    fork: &[(u32, String)],
    verdict: u64,
) {
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    let invalidated = facts(report).into_iter().find(|(step, fact)| {
        *step >= verdict
            && matches!(
                fact,
                StorageFact::TransitionInstalled {
                    invalidates_body_work: true,
                    ..
                }
            )
    });
    assert_eq!(invalidated, None, "the stale commit keeps the epoch");
    let submitted: BTreeSet<&str> = submissions(report)
        .into_iter()
        .filter_map(|(_, request, _)| match request {
            StorageRequest::SubmitBlock { hash, .. } => Some(hash.as_str()),
            _ => None,
        })
        .collect();
    let held = |height: u32| {
        view.sequencer
            .reorder
            .iter()
            .chain(&view.sequencer.applying)
            .any(|body| body.height.0 == height)
    };
    let above: Vec<_> = fork
        .iter()
        .filter(|(height, _)| (4..=10).contains(height))
        .collect();
    assert_eq!(above.len(), 7, "the fork holds heights 4 to 10");
    for (height, hash) in above {
        assert!(
            submitted.contains(hash.as_str()) || held(*height),
            "the node submitted or holds the fork's height {height}"
        );
    }
    let tip = facts(report)
        .into_iter()
        .rev()
        .find_map(|(_, fact)| match fact {
            StorageFact::ViewPublished { verified_best, .. } => Some(verified_best),
            _ => None,
        })
        .expect("block storage publishes views");
    assert_eq!(
        tip.height, 3,
        "the verified tip sits at the replaced height"
    );
    assert_eq!(tip.hash, replaced, "the replaced block commits");
    let height = |at: &zakura_chain::block::Height| at.0 == 3;
    assert!(
        !view.work_queue.iter().any(|item| height(&item.height)),
        "the work queue dropped height 3: {:?}",
        view.work_queue
    );
    let sequencer = &view.sequencer;
    assert!(
        !sequencer
            .reorder
            .iter()
            .chain(&sequencer.applying)
            .any(|body| height(&body.height))
            && !sequencer
                .submissions
                .iter()
                .any(|submission| height(&submission.height)),
        "the sequencer holds no height-3 body: {sequencer:?}"
    );
}

/// Case 4: the node's first request times out before the held read is
/// served, which returns its height to the work queue unowned. The late
/// body still reaches the node on the request's connection, which stays
/// open, with no other request on that session between. After the relink,
/// a new session requests the height again, and the node commits every
/// body.
#[test]
fn a_late_body_is_judged_without_closing_its_link() {
    for input in inputs(4, 256) {
        let (report, ports) = node_run(&late_body(), input);
        assert_converged(&ports);
        let served = picked(&report, "storage:mirror-0:1:serve").expect("the held read is served");
        let events = events(&report);
        let (asked, session, height) = events
            .iter()
            .find_map(|(step, event)| match event {
                BlockSyncFact::Request {
                    owner,
                    heights,
                    step: moved,
                    ..
                } if moved == "published" => Some((*step, owner.session_id, heights[0])),
                _ => None,
            })
            .expect("the node requests a body");
        assert!(
            events
                .iter()
                .any(|(step, event)| (asked..served).contains(step)
                    && matches!(event, BlockSyncFact::WorkQueue { items } if items
                    .iter()
                    .any(|item| item.height == height && item.owner.is_none()))),
            "the request times out before the late body"
        );
        let connection = events
            .iter()
            .find_map(|(_, event)| match event {
                BlockSyncFact::SessionAdmitted {
                    connection,
                    session_id,
                    ..
                } if *session_id == session => Some(*connection),
                _ => None,
            })
            .expect("the session was admitted");
        let judged = events
            .iter()
            .find_map(|(step, event)| match event {
                BlockSyncFact::FrameJudged {
                    connection: on,
                    message_type,
                    ..
                } if *on == connection
                    && *message_type == u16::from(MSG_BS_BLOCK)
                    && *step > served =>
                {
                    Some(*step)
                }
                _ => None,
            })
            .expect("the late body reaches the node");
        let published_on = |range: std::ops::Range<u64>, on: &dyn Fn(u64) -> bool| {
            events.iter().any(|(step, event)| {
                range.contains(step)
                    && matches!(event, BlockSyncFact::Request { owner, heights, step: moved, .. }
                        if moved == "published" && on(owner.session_id) && heights.contains(&height))
            })
        };
        assert!(
            !published_on(asked + 1..judged, &|id| id == session),
            "the late body answers the held request"
        );
        assert!(
            published_on(judged..report.end.steps + 1, &|id| id != session),
            "a new session asks for the height again"
        );
        let links = ports.effects.links.borrow();
        let held = links
            .iter()
            .find(|link| link.conn_id == connection)
            .expect("the held request's link");
        assert!(!held.is_closed(), "{links:?}");
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
/// it holds, and publishes an empty snapshot. The node's reactor,
/// sequencer and action tasks exit. The tap expects the observer to go, so
/// the run ends healthy, and the view ends at zero.
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
        let exited: BTreeSet<_> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::TaskExited { task, .. } if entry.step > torn => Some(task.as_str()),
                _ => None,
            })
            .collect();
        // The node spawns first, so its tasks are each name's first.
        for task in [
            "block-node.1",
            "block-actions.1",
            "block_sync_reactor.1",
            "block_sync_sequencer.1",
        ] {
            assert!(exited.contains(task), "{task} outlives the teardown");
        }
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
/// before its block service answers the query, so the work queue, in the
/// events and in the view, holds work that no storage answer backs. The
/// service's late answer is stale, and the node still commits every body.
#[test]
fn inserted_work_reaches_the_work_queue_without_an_answer() {
    for input in inputs(4, 256) {
        let mut queued = None;
        let (report, ports) = sampled_run(&insert_work(), input, |step, view| {
            if queued.is_none() && !view.work_queue.is_empty() {
                queued = Some(step);
            }
        });
        assert_converged(&ports);
        let inserted = picked(&report, "peer:node:insert-work").expect("the work goes in");
        let answered =
            completed(&report, first_query(&report)).expect("the held query is answered");
        assert!(inserted < answered);
        let queued = queued.expect("the view's work queue fills");
        assert!(inserted <= queued && queued < answered, "{queued}");
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
/// first submission, releasing its holder in the events and in the view,
/// while block storage still holds the operation uncommitted. Storage's
/// own commit comes later, and the node still commits every body.
#[test]
fn a_dispatched_verdict_releases_a_submission_storage_still_holds() {
    for input in inputs(4, 256) {
        let mut submitted = Vec::new();
        let (report, ports) = sampled_run(&dispatch_verdict(), input, |step, view| {
            let tokens: Vec<_> = view
                .holders
                .iter()
                .filter_map(|(holder, _)| match holder {
                    Holder::Submitted { token } => Some(*token),
                    _ => None,
                })
                .collect();
            submitted.push((step, tokens));
        });
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
        let held_at = |step: u64| {
            submitted
                .iter()
                .find(|(at, _)| *at == step)
                .is_some_and(|(_, tokens)| tokens.contains(&token))
        };
        assert!(held_at(dispatched - 1), "the view holds the submission");
        assert!(!held_at(released), "the view releases it before the commit");
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
