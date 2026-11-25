//! Block sync's resource bounds: each scenario input reaches its property's
//! trigger history on the real node and holds, honest runs hold every bound
//! across A/A and A/B/A and across worker counts, teardown returns every
//! charge, and the owner model folds the observer's effects as the bounds
//! need. `cargo xtask planted` runs
//! its cases' inputs with each property's bypass patch applied.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_network::zakura::FreeSlots;
use zakura_sim::{
    domain::{
        block_sync::{
            BodyHolder, Limits, OwnerInput, OwnerModel, RequestedHeight, BUDGET_LEDGERS_AGREE,
            ESTIMATE_CEILING, PAYLOAD_INVENTORY_AGREES, RETAINED_BYTE_BOUND,
        },
        memory_pool::{CONSENSUS_ONCE_PER_HEIGHT, POOL_BOUNDED, TEARDOWN_RETURNS_ZERO},
    },
    harness::{
        block_node::{
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            PROBE,
        },
        block_semantic::STALLED_RESET_TAG,
        block_sync::{
            BlockScenario, HandoffPause, MirrorSpec, Ports, Storage, PEERS_LEAVE_SERVED_TAG,
            PEERS_LEAVE_TAG, STUCK_FLOOR_TAG,
        },
        header_sync::Dialer,
        run_prepared, Subsystem,
    },
    observe::{BlockSyncFact, StorageBranch, WorkOwner, WorkScope},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, PropertyReport, RunReport,
};

/// The resource bounds every run that rests judges. Teardown applies only
/// once every peer leaves.
const IDS: [&str; 3] = [RETAINED_BYTE_BOUND, BUDGET_LEDGERS_AGREE, POOL_BOUNDED];

/// The scenario `tag` picks, under the Plan `plan`.
fn tagged(tag: [u8; 2], plan: &[u8]) -> WitnessScenario<Storage> {
    WitnessScenario {
        scenario: Storage::draw(&tag),
        input: Plan::from_fuzz(plan),
    }
}

/// The stalled-reset scenario under an arbitrary short Plan: the first of
/// [`support::byte_inputs`]'s stream, up to 256 bytes.
fn stalled_reset() -> WitnessScenario<Storage> {
    tagged(STALLED_RESET_TAG, &support::byte_inputs(1, 256)[0])
}

/// The stuck-floor scenario fills the commit window and holds the retained
/// bound (`mem-2`: retained wire bytes stay within live pool leases + base).
#[test]
#[ignore = "a debug run takes minutes; run it on demand with `--ignored` and `--profile planted`"]
fn the_stuck_floor_fills_the_commit_window_within_retained_byte_bound() {
    let mut witness = tagged(STUCK_FLOOR_TAG, &[]);
    assert_eq!(witness.scenario, BlockScenario::stuck_floor());
    witness.scenario.steps = Some(400_000);
    holds(witness, RETAINED_BYTE_BOUND);
}

/// The production node captures every runtime payload through receipt and
/// teardown. With `lost-payload-release.patch`, the sequencer drops a
/// payload's release, and the same input violates the property.
#[test]
fn the_stalled_reset_input_holds_payload_inventory_agrees() {
    holds(stalled_reset(), PAYLOAD_INVENTORY_AGREES);
}

/// The stalled-reset input holds the ledgers (including the memory pool's
/// block-sync + consensus count) at every quiescence boundary after a charged
/// handoff. With `handoff-keeps-charge.patch`, a routine keeps its charge
/// after the handoff, and the same input violates them.
#[test]
fn the_stalled_reset_input_holds_budget_ledgers_agree() {
    holds(stalled_reset(), BUDGET_LEDGERS_AGREE);
}

/// The floor witness (P5.3): with routines paused in their handoffs, the
/// floor's owners churn: `mirror-3`'s routine retires when its link closes,
/// a request for height 7, which every mirror withholds, times out and is
/// asked again, and the chain resets to a fork. The pool gauge, the retained
/// bound and the ledgers hold throughout. Height 7 sits in the commit window,
/// so each request for it leases from `Consensus`, and `Consensus` still
/// funds each height once (`mem-15`). This harness has no transport credit,
/// so a timed-out request releases its lease; the wire harness drains it.
#[test]
fn the_floor_churn_holds_the_pool_and_ledger_bounds() {
    let scenario = BlockScenario::floor_churn();
    let (report, ports) = node_run_with_ports(&scenario, Plan::from_fuzz(&[]));
    for id in [
        POOL_BOUNDED,
        BUDGET_LEDGERS_AGREE,
        RETAINED_BYTE_BOUND,
        CONSENSUS_ONCE_PER_HEIGHT,
    ] {
        let outcome = outcome(&report, id);
        assert!(
            matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
            "{id}: {outcome:?}"
        );
    }
    let reset = report
        .picks
        .iter()
        .find(|pick| pick.identity == "peer:chain:reset")
        .map(|pick| pick.step)
        .expect("the chain resets");
    let retired = ports
        .effects
        .links
        .borrow()
        .iter()
        .find(|link| link.peer == "mirror-3")
        .map(|link| link.conn_id)
        .expect("mirror-3 links");
    let facts: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::BlockSync { event, .. } => Some((entry.step, event)),
            _ => None,
        })
        .collect();
    assert!(
        facts.iter().any(|(step, fact)| *step < reset
            && matches!(fact, BlockSyncFact::Routine { connection, ended: true, .. }
                if *connection == retired)),
        "mirror-3's routine retires before the reset"
    );
    let (asked, asked_on) = facts
        .iter()
        .find_map(|(step, fact)| match fact {
            BlockSyncFact::Request {
                owner,
                heights,
                step: moved,
                ..
            } if moved == "written" && heights.contains(&7) => {
                let conn = facts.iter().find_map(|(_, f)| match f {
                    BlockSyncFact::SessionAdmitted {
                        connection,
                        session_id,
                        ..
                    } if *session_id == owner.session_id => Some(*connection),
                    _ => None,
                })?;
                (conn != retired).then_some((*step, conn))
            }
            _ => None,
        })
        .expect("a surviving link asks for height 7");
    assert_ne!(asked_on, retired, "height 7 is asked on a link that stays");
    let timed_out = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } if *to_ns >= 8_000_000_000 => Some(entry.step),
            _ => None,
        })
        .expect("virtual time passes the request timeout");
    assert!(asked < timed_out && timed_out < reset);
    assert!(
        facts
            .iter()
            .any(|(step, fact)| (timed_out..reset).contains(step)
                && matches!(fact, BlockSyncFact::WorkQueue { items } if items
                .iter()
                .any(|item| item.height == 7 && item.owner.is_none()))),
        "the request for height 7 times out before the reset"
    );
    let asked_for_7 = facts
        .iter()
        .filter(|(_, fact)| {
            matches!(fact, BlockSyncFact::Request { heights, step, .. }
            if step == "written" && heights.contains(&7))
        })
        .count();
    assert!(
        asked_for_7 >= 2,
        "height 7 is asked again after its timeout"
    );
    assert!(
        zakura_sim::domain::memory_pool::consensus_height_reissued(&report.log),
        "a second consensus lease funds a height after the first one's request timed out"
    );
}

/// A fuzz prefix of `len` bytes from xorshift64 seeded with `seed`.
fn fuzz_prefix(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Whether the node asks for height 7, which every mirror withholds,
/// before the chain resets.
fn asks_for_the_withheld_floor_before_the_reset(report: &RunReport) -> bool {
    let reset = report
        .picks
        .iter()
        .find(|pick| pick.identity == "peer:chain:reset")
        .map(|pick| pick.step)
        .expect("the chain resets");
    report.log.entries().iter().any(|entry| {
        entry.step < reset
            && matches!(&entry.obs, Observation::BlockSync {
                event: BlockSyncFact::Request { heights, step, .. },
                ..
            } if step == "written" && heights.contains(&7))
    })
}

/// The floor churn with routines paused in their handoffs from height 5,
/// stopped past the 12 s chain reset.
fn floor_churn_from_height_5() -> BlockScenario {
    BlockScenario {
        handoff_pause: Some(HandoffPause {
            from_height: 5,
            steps: 40_000,
        }),
        steps: Some(40_000),
        ..BlockScenario::floor_churn()
    }
}

/// #1812: with the pause from height 5, this plan leaves free only a
/// routine with no RTprop sample. On main, it deferred the floor to the
/// routines paused in their handoffs, so no routine asked for height 7
/// before the reset.
#[test]
fn an_unmeasured_routine_asks_for_the_floor_while_the_others_are_paused() {
    let report = node_run(
        &floor_churn_from_height_5(),
        Plan::from_fuzz(&fuzz_prefix(2, 64)),
    );
    assert!(asks_for_the_withheld_floor_before_the_reset(&report));
}

/// [`an_unmeasured_routine_asks_for_the_floor_while_the_others_are_paused`]
/// on 24 plans of each prefix length. On main, 7 of the 48 plans never ask
/// for height 7 before the reset.
#[test]
#[ignore = "48 debug runs take about 80 s; run it on demand with `--ignored`"]
fn every_floor_churn_plan_asks_for_the_floor_before_the_reset() {
    let scenario = floor_churn_from_height_5();
    let stalled: Vec<_> = [64, 32]
        .into_iter()
        .flat_map(|len| (1..=24).map(move |seed| (len, seed)))
        .filter(|&(len, seed)| {
            let report = node_run(&scenario, Plan::from_fuzz(&fuzz_prefix(seed, len)));
            !asks_for_the_withheld_floor_before_the_reset(&report)
        })
        .collect();
    assert_eq!(
        stalled,
        [],
        "(prefix length, seed) plans that stall the floor"
    );
}

/// Both mirrors leave while the node owes them requests. Once every routine
/// has ended, the input queue is empty and the run rests, every live pool
/// lease, the byte budget, the work queue's tally and the routines' unreceived
/// estimates are zero, and no handoff is open (`mem-11`, catalogue G4).
#[test]
fn block_sync_teardown_returns_baseline() {
    let witness = tagged(PEERS_LEAVE_TAG, &[]);
    assert_eq!(witness.scenario, BlockScenario::peers_leave());
    let report = node_run(&witness.scenario, witness.input.clone());
    for id in IDS.into_iter().chain([TEARDOWN_RETURNS_ZERO]) {
        let outcome = outcome(&report, id);
        assert!(
            matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
            "{id}: {outcome:?}"
        );
    }
    check_witness(witness, TEARDOWN_RETURNS_ZERO, Expect::Holds);
}

/// The peers-leave run with a probe the node serves and links that reserve
/// its session slots. The node holds a session slot for each link during
/// the run, and teardown settles with every slot and pool lease returned.
#[test]
fn teardown_returns_session_slots() {
    let scenario = BlockScenario::peers_leave_served();
    assert_eq!(
        Storage::draw(&PEERS_LEAVE_SERVED_TAG),
        scenario,
        "the generator draws this scenario"
    );
    let witness = WitnessScenario::<Storage> {
        scenario,
        input: Plan::from_fuzz(&[]),
    };
    let (report, ports) = node_run_with_ports(&witness.scenario, witness.input.clone());
    let outcome = outcome(&report, TEARDOWN_RETURNS_ZERO);
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{outcome:?}"
    );
    {
        let view = ports.view.borrow();
        let owners = &view.owners;
        let peaks = owners.peaks();
        assert_eq!(peaks.session_slots, 3, "{peaks:?}");
        assert_eq!(owners.session_slots(), 0);
        assert_eq!(view.pool.borrow().total_leased(), 0);
        assert_eq!(view.pool.borrow().live_lease_count(), 0);
    }
    check_witness(witness, TEARDOWN_RETURNS_ZERO, Expect::Holds);
}

/// The served run with the node's first serving read, for height 1, held
/// until the probe's link has closed and 60 s have passed since the read's
/// submission. Teardown waits for the lease, and holds once the read returns it.
#[test]
fn teardown_waits_for_a_serving_read_held_across_a_close() {
    let mut scenario = BlockScenario::peers_leave_served();
    scenario.policy.holds.push(Hold {
        select: Select::nth(BlockKind::Read, 1),
        until: Trigger::All(vec![
            Trigger::Closed(PROBE.to_owned()),
            Trigger::VirtualTime(60_000),
        ]),
        then: vec![BlockAllow::Serve],
        deadline_ms: None,
    });
    let (report, ports) = node_run_with_ports(&scenario, Plan::from_fuzz(&[]));
    let outcome = outcome(&report, TEARDOWN_RETURNS_ZERO);
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{outcome:?}"
    );
    let ends: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.obs,
                Observation::BlockSync {
                    event: BlockSyncFact::Routine { ended: true, .. },
                    ..
                }
            )
        })
        .map(|entry| entry.step)
        .collect();
    assert_eq!(ends.len(), 3, "every link's routine ends: {ends:?}");
    let last_close = ends.into_iter().max().expect("three routines end");
    let released = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::BlockSync {
                event: BlockSyncFact::ServingLease { start: 1, step, .. },
                ..
            } if step == "released" => Some(entry.step),
            _ => None,
        })
        .expect("the held read returns its lease");
    assert!(
        released > last_close,
        "the read for height 1 returned its lease at step {released}, before the last \
         routine ended at step {last_close}"
    );
    let view = ports.view.borrow();
    assert_eq!(view.owners.serving_leases(), 0);
    assert_eq!(view.owners.session_slots(), 0);
    assert_eq!(view.pool.borrow().total_leased(), 0);
}

/// P5.2: the short budget on 1, 2 and 4 workers, each with 2 and 4
/// mirrors, holds every bound.
#[test]
fn every_worker_count_holds_the_resource_bounds() {
    for workers in [1, 2, 4] {
        for mirrors in [2, 4] {
            let scenario = BlockScenario {
                workers: Some(workers),
                // Receipt availability adds preemption points before quiescence.
                steps: Some(64_000),
                mirrors: BlockScenario::short_budget().mirrors[..mirrors].to_vec(),
                ..BlockScenario::short_budget()
            };
            let report = node_run(&scenario, Plan::from_fuzz(&[]));
            for id in IDS {
                let outcome = outcome(&report, id);
                assert!(
                    matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
                    "W = {workers}, {mirrors} mirrors: {id}: {outcome:?}"
                );
            }
        }
    }
}

/// Runs `scenario` under `input`. The run must end with no violation and
/// no harness failure, with every block storage operation paired, and with
/// every sequencer snapshot matching the holder events.
fn node_run(scenario: &BlockScenario, input: PlanInput) -> RunReport {
    node_run_with_ports(scenario, input).0
}

/// [`node_run`], returning the ports too.
fn node_run_with_ports(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
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
    {
        let view = ports.view.borrow();
        assert!(view.pairing.is_empty(), "{:?}", view.pairing);
        assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
        assert_eq!(view.owners.holder_gap(), None);
    }
    (report, ports)
}

/// Checks `witness` as a witness test of `property` that expects it to
/// hold, after a run of the same scenario and Plan pairs every block
/// storage operation.
fn holds(witness: WitnessScenario<Storage>, property: &str) {
    node_run(&witness.scenario, witness.input.clone());
    check_witness(witness, property, Expect::Holds);
}

/// `id`'s report in `report`.
fn property<'a>(report: &'a RunReport, id: &str) -> &'a PropertyReport {
    report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} has no report"))
}

/// `id`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &property(report, id).outcome
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

/// Honest runs, BS5's two-mirror flow among them, hold each bound in
/// [`IDS`] at least once. None reaches the window's top, so the look-ahead
/// bound holds with nothing above the window; the above-window test is its
/// real witness. A, then B, then A gives A's history twice, and B's
/// differs.
#[test]
fn honest_runs_hold_every_resource_bound_a_b_a() {
    let two_mirrors = BlockScenario {
        blocks: 16,
        mirrors: vec![
            MirrorSpec::new(Dialer::Mirror),
            MirrorSpec::new(Dialer::Node),
        ],
        ..BlockScenario::default()
    };
    let plans = inputs(2, 256);
    for (name, scenario) in [
        ("default", BlockScenario::default()),
        ("two mirrors", two_mirrors),
        ("short budget", BlockScenario::short_budget()),
    ] {
        let reports: Vec<_> = [&plans[0], &plans[1], &plans[0]]
            .into_iter()
            .map(|plan| node_run(&scenario, plan.clone()))
            .collect();
        for report in &reports {
            for id in IDS {
                let outcome = outcome(report, id);
                assert!(
                    matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
                    "{name}: {id}: {outcome:?}"
                );
            }
            let peaks = |id| &property(report, id).peaks;
            assert!(
                peaks(POOL_BOUNDED)["pool-leased-bytes"] > 0,
                "{name}: {:?}",
                peaks(POOL_BOUNDED)
            );
            assert!(
                peaks(RETAINED_BYTE_BOUND)["retained-wire-bytes"] > 0,
                "{name}: {:?}",
                peaks(RETAINED_BYTE_BOUND)
            );
        }
        let histories: Vec<_> = reports.iter().map(history).collect();
        assert!(
            histories[0] == histories[2],
            "{name}: plan A, then B, then A"
        );
        assert!(histories[0] != histories[1], "{name}: plan B differs");
    }
}

/// `B`, the largest estimate.
const B: u64 = ESTIMATE_CEILING;

/// Limits with `M` = 10, one body per request, and bodies of at most 3
/// bytes.
fn small_limits() -> Limits {
    Limits {
        budget: 10,
        per_request: 1,
        largest_body: 3,
    }
}

/// A body request with `request_id`.
fn owner(request_id: u64) -> WorkOwner {
    WorkOwner {
        kind: "body".to_owned(),
        scope: WorkScope {
            header_generation: 1,
            branch: StorageBranch {
                anchor: "aa".to_owned(),
                target: "bb".to_owned(),
            },
        },
        verified_generation: Some(1),
        body_work_epoch: Some(0),
        session_id: 1,
        request_id,
    }
}

/// The handoff pauses end past `M + 2·k·B`.
#[test]
fn the_pause_bound_adds_two_requests_to_the_budget() {
    let limits = small_limits();
    assert_eq!(limits.pause_bound(), 10 + 2 * B);
    let two_per_request = Limits {
        per_request: 2,
        ..limits
    };
    assert_eq!(two_per_request.pause_bound(), 10 + 4 * B);
}

/// A body counts once while the input queue, the reorder buffer or
/// `applying` holds its allocation, and not at all for a submission.
#[test]
fn a_retained_body_counts_once_per_allocation() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    owners.apply(OwnerInput::Request {
        request: (1, 1),
        heights: vec![5],
        estimate: 3,
    });
    let held = |holder| OwnerInput::Held {
        holder,
        request: (1, 1),
        height: 5,
        bytes: 3,
        decoded_bytes: 0,
    };
    owners.apply(held(BodyHolder::Queued(1)));
    owners.apply(held(BodyHolder::Reorder(1)));
    assert_eq!(owners.retained(), 3);
    owners.apply(OwnerInput::Released {
        holder: BodyHolder::Queued(1),
    });
    owners.apply(held(BodyHolder::Submitted(9)));
    owners.apply(held(BodyHolder::Detached(10)));
    assert_eq!(owners.retained(), 3);
    owners.apply(OwnerInput::Released {
        holder: BodyHolder::Reorder(1),
    });
    assert_eq!(owners.retained(), 0);
    assert_eq!(owners.peaks().retained, 3);
    assert_eq!(owners.holder_gap(), None);
}

/// A body larger than the fixture allows, a retained body whose request's
/// estimate no input reported, a release of a body never taken, and a
/// handoff that ends without starting each leave the holders unknown.
#[test]
fn holder_inputs_the_model_cannot_apply_are_gaps() {
    let oversized = OwnerInput::Held {
        holder: BodyHolder::Reorder(1),
        request: (1, 1),
        height: 5,
        bytes: 4,
        decoded_bytes: 0,
    };
    let unestimated = OwnerInput::Held {
        holder: BodyHolder::Reorder(1),
        request: (1, 1),
        height: 5,
        bytes: 3,
        decoded_bytes: 0,
    };
    let never_taken = OwnerInput::Released {
        holder: BodyHolder::Applying(2),
    };
    let unstarted = OwnerInput::HandoffEnded {
        owner: owner(1),
        height: 5,
        charge: 7,
    };
    for input in [oversized, unestimated, never_taken, unstarted] {
        let mut owners = OwnerModel::new(small_limits(), 0);
        owners.apply(input.clone());
        assert!(owners.holder_gap().is_some(), "{input:?}");
        assert_eq!(owners.budget_gap(), None, "{input:?}");
    }
}

/// A sequencer snapshot must list every holder the model folded other than
/// the input queue's. A snapshot that lists a body whose retain event was
/// dropped, or omits one the events hold, leaves the holders unknown.
#[test]
fn a_snapshot_that_differs_from_the_holder_events_is_a_gap() {
    let model = || {
        let mut owners = OwnerModel::new(small_limits(), 0);
        owners.apply(OwnerInput::Request {
            request: (1, 1),
            heights: vec![5],
            estimate: 3,
        });
        owners
    };
    let held = |holder| OwnerInput::Held {
        holder,
        request: (1, 1),
        height: 5,
        bytes: 3,
        decoded_bytes: 0,
    };
    let snapshot = |holders: &[BodyHolder]| OwnerInput::Snapshot {
        payloads: Vec::new(),
        expected_payloads: Vec::new(),
        holders: holders.iter().map(|holder| (*holder, 5, 3)).collect(),
        detached_decoded: 0,
    };
    let mut owners = model();
    owners.apply(held(BodyHolder::Queued(1)));
    owners.apply(held(BodyHolder::Applying(2)));
    owners.apply(held(BodyHolder::Submitted(9)));
    owners.apply(snapshot(&[
        BodyHolder::Submitted(9),
        BodyHolder::Applying(2),
    ]));
    assert_eq!(owners.holder_gap(), None);

    let mut dropped = model();
    dropped.apply(snapshot(&[BodyHolder::Reorder(1)]));
    assert_eq!(dropped.retained(), 0);
    assert!(dropped.holder_gap().is_some());

    let mut omitted = model();
    omitted.apply(held(BodyHolder::Reorder(1)));
    omitted.apply(snapshot(&[]));
    assert!(omitted.holder_gap().is_some());
    assert_eq!(omitted.budget_gap(), None);
}

/// The budget's changes chain from the empty budget, with no maximum. A
/// change from another count, or a reported maximum, leaves the counter
/// unknown.
#[test]
fn budget_changes_chain_with_no_maximum() {
    let change = |before, after, limit| OwnerInput::BudgetChanged {
        before,
        after,
        limit,
    };
    let mut owners = OwnerModel::new(small_limits(), 0);
    owners.apply(change(0, 7, u64::MAX));
    owners.apply(change(7, 14, u64::MAX));
    owners.apply(change(14, 0, u64::MAX));
    assert_eq!((owners.reserved(), owners.peaks().reserved), (0, 14));
    assert_eq!(owners.budget_gap(), None);

    let mut skipped = OwnerModel::new(small_limits(), 0);
    skipped.apply(change(0, 7, u64::MAX));
    skipped.apply(change(3, 0, u64::MAX));
    assert!(skipped.budget_gap().is_some());

    let mut other_maximum = OwnerModel::new(small_limits(), 0);
    other_maximum.apply(change(0, 7, 10));
    assert!(other_maximum.budget_gap().is_some());
}

/// A handoff stays open from its start to its matching end, and a routine
/// stays live from its first snapshot to its last.
#[test]
fn handoffs_and_routines_follow_their_inputs() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    owners.apply(OwnerInput::HandoffStarted {
        owner: owner(1),
        height: 5,
        charge: 7,
    });
    assert_eq!(owners.handoffs().len(), 1);
    owners.apply(OwnerInput::HandoffEnded {
        owner: owner(1),
        height: 5,
        charge: 7,
    });
    assert!(owners.handoffs().is_empty());
    assert_eq!(owners.peaks().handoffs, 1);

    let snapshot = |ended| OwnerInput::Routine {
        connection: 4,
        generation: 1,
        ended,
        requests: Vec::new(),
    };
    owners.apply(snapshot(false));
    owners.apply(snapshot(false));
    assert_eq!(owners.live_routines(), 1);
    owners.apply(snapshot(true));
    assert_eq!(owners.live_routines(), 0);
    assert_eq!(owners.holder_gap(), None);
}

/// One unreceived height of request `request`, at `height`.
fn requested(request: u64, height: u32, estimate: u64, received: bool) -> RequestedHeight {
    RequestedHeight {
        request: (1, request),
        height,
        estimate,
        received,
    }
}

/// The routines' unreceived estimates count each routine's latest snapshot
/// once, without received heights, and a routine's last snapshot replaces
/// its earlier ones.
#[test]
fn unreceived_estimates_follow_each_routines_latest_snapshot() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    let snapshot = |generation, ended, requests| OwnerInput::Routine {
        connection: 4,
        generation,
        ended,
        requests,
    };
    owners.apply(snapshot(
        1,
        false,
        vec![requested(1, 5, 7, false), requested(1, 6, 2, true)],
    ));
    owners.apply(snapshot(2, false, vec![requested(2, 7, 3, false)]));
    assert_eq!(owners.unreceived_estimates(), 10);
    owners.apply(snapshot(1, true, Vec::new()));
    assert_eq!(owners.unreceived_estimates(), 3);
}

/// Session slots count against the first reading, of every kind together.
#[test]
fn session_slots_follow_their_readings() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    let free = |inbound, outbound, setup, inbound_setup| OwnerInput::SessionSlots {
        free: FreeSlots {
            inbound,
            outbound,
            setup,
            inbound_setup,
        },
    };
    assert_eq!(owners.session_slots(), 0);
    owners.apply(free(8, 8, 4, 3));
    assert_eq!(owners.session_slots(), 0);
    owners.apply(free(7, 8, 3, 2));
    assert_eq!(owners.session_slots(), 3);
    owners.apply(free(7, 7, 4, 3));
    assert_eq!(owners.session_slots(), 2);
    owners.apply(free(8, 8, 4, 3));
    assert_eq!(owners.session_slots(), 0);
    assert_eq!(owners.peaks().session_slots, 3);
}

/// Every peer has left once every link made is closed. A session counts as
/// unended until its routine reports its last snapshot, and so does a
/// routine that reported any.
#[test]
fn teardown_settles_when_links_close_and_routines_end() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    assert!(!owners.peers_left());
    owners.apply(OwnerInput::Links { made: 2, open: 1 });
    assert!(!owners.peers_left());
    owners.apply(OwnerInput::Links { made: 2, open: 0 });
    assert!(owners.peers_left());
    owners.apply(OwnerInput::SessionAdmitted {
        connection: 4,
        generation: 1,
    });
    owners.apply(OwnerInput::SessionAdmitted {
        connection: 5,
        generation: 2,
    });
    let snapshot = |connection, generation, ended| OwnerInput::Routine {
        connection,
        generation,
        ended,
        requests: Vec::new(),
    };
    owners.apply(snapshot(4, 1, false));
    owners.apply(snapshot(6, 3, false));
    assert_eq!(owners.unended(), 3);
    owners.apply(snapshot(4, 1, true));
    owners.apply(snapshot(6, 3, true));
    assert_eq!(owners.unended(), 1);
    owners.apply(snapshot(5, 2, true));
    assert_eq!(owners.unended(), 0);
}
