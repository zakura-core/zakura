//! Block sync's resource bounds (P5b, gates P5.1–P5.4): each committed
//! input reaches its property's trigger history on the real node and
//! holds, honest runs hold all four bounds across A/A and A/B/A and across
//! worker counts, teardown returns every charge, and the owner model folds
//! the observer's effects as the bounds need. `cargo xtask planted` runs
//! the same inputs with each property's bypass patch applied.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_scheduler::Status as SchedulerStatus;
use zakura_sim::{
    domain::block_sync::{
        BodyHolder, Limits, Lookahead, OwnerInput, OwnerModel, RequestedHeight,
        BUDGET_LEDGERS_AGREE, ESTIMATE_CEILING, ESTIMATE_FLOOR, LOOKAHEAD_OVERSHOOT_BOUND,
        RESERVED_BYTE_BOUND, RETAINED_BYTE_BOUND, TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        block_sync::{BlockScenario, MirrorSpec, Ports, Storage},
        header_sync::Dialer,
        run_prepared, Subsystem,
    },
    observe::{BlockSyncFact, StorageBranch, WorkOwner, WorkScope},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, PropertyReport, RunReport,
};

/// The committed input that picks the paused-handoffs scenario: routines
/// pause in their handoffs, so floor requests charge past `M`.
const PAUSED_HANDOFFS_INPUT: &[u8] = include_bytes!("fixtures/block-sync-paused-handoffs.bin");

/// The committed input that picks the stuck-floor scenario: the commit
/// window fills while height 3 never arrives.
const STUCK_FLOOR_INPUT: &[u8] = include_bytes!("fixtures/block-sync-stuck-floor.bin");

/// The committed input that picks the default scenario, whose routines hand
/// charged bodies to the sequencer.
const HANDOFF_INPUT: &[u8] = include_bytes!("fixtures/block-sync-handoff.bin");

/// The committed input that picks the above-window scenario: the node
/// requests heights above the commit window that never arrive.
const ABOVE_WINDOW_INPUT: &[u8] = include_bytes!("fixtures/block-sync-above-window.bin");

/// The committed input that picks the peers-leave scenario: both mirrors
/// leave while the node owes them requests.
const PEERS_LEAVE_INPUT: &[u8] = include_bytes!("fixtures/block-sync-peers-leave.bin");

/// The resource bounds every run that rests judges. Teardown applies only
/// once every peer leaves.
const IDS: [&str; 4] = [
    RETAINED_BYTE_BOUND,
    RESERVED_BYTE_BOUND,
    BUDGET_LEDGERS_AGREE,
    LOOKAHEAD_OVERSHOOT_BOUND,
];

/// The scenario and Plan that `zakura-sim record --input` runs for `input`.
fn committed(input: &[u8]) -> WitnessScenario<Storage> {
    let (prefix, plan) = input.split_at(Storage::SCENARIO_BYTES);
    WitnessScenario {
        scenario: Storage::draw(prefix),
        input: Plan::from_fuzz(plan),
    }
}

/// The paused-handoffs input holds the reserved bound after a floor request
/// charges past `M`. With `above-floor-charge.patch`, above-floor requests
/// charge past `M` too, and the same input violates it.
#[test]
fn the_paused_handoffs_input_holds_reserved_byte_bound() {
    holds(committed(PAUSED_HANDOFFS_INPUT), RESERVED_BYTE_BOUND);
}

/// D2's `h > 0` witness (P5.1, P5.3): with routines paused in their
/// handoffs, the byte budget passes `M + 2·B` and stays within
/// `M + 2·B + h·B`. Three paused routines and a fourth's charge take it to
/// `M + 2.5·B`.
#[test]
fn paused_handoffs_pass_m_plus_2b_within_the_handoff_allowance() {
    let witness = committed(PAUSED_HANDOFFS_INPUT);
    assert_eq!(witness.scenario, BlockScenario::paused_handoffs());
    let report = node_run(&witness.scenario, witness.input);
    let reserved = property(&report, RESERVED_BYTE_BOUND);
    assert!(
        matches!(reserved.outcome, PropertyOutcome::Held { .. }),
        "{reserved:?}"
    );
    let limits = Limits::of(&witness.scenario.node_config(), 4, 2_000);
    let peak = reserved.peaks["reserved-estimate-bytes"];
    let handoffs = reserved.peaks["open-handoffs"];
    assert!(peak > limits.reserved_bound(0), "{:?}", reserved.peaks);
    assert!(
        peak <= limits.reserved_bound(handoffs),
        "{:?}",
        reserved.peaks
    );
    assert_eq!(peak, limits.budget + 5 * B / 2, "{:?}", reserved.peaks);
}

/// The handoff input holds the ledgers at every quiescence boundary after
/// a charged handoff. With `handoff-keeps-charge.patch`, a routine keeps
/// its charge after the handoff, and the same input violates them.
#[test]
fn the_handoff_input_holds_budget_ledgers_agree() {
    holds(committed(HANDOFF_INPUT), BUDGET_LEDGERS_AGREE);
}

/// The stuck-floor scenario fills the commit window and holds the retained
/// bound. The committed input runs 1,500,000 steps, which
/// `wide-commit-window.patch` needs to retain 805 bodies. The window fills
/// within 400,000, so this test stops there.
#[test]
#[ignore = "a debug run takes minutes; `cargo xtask planted` runs the committed input unpatched"]
fn the_stuck_floor_fills_the_commit_window_within_retained_byte_bound() {
    let mut witness = committed(STUCK_FLOOR_INPUT);
    assert_eq!(witness.scenario, BlockScenario::stuck_floor());
    witness.scenario.steps = Some(400_000);
    holds(witness, RETAINED_BYTE_BOUND);
}

/// The floor witness (P5.3): routines paused in their handoffs fill the
/// budget past `M` with floor charges, then the floor's owners churn:
/// `mirror-3`'s routine retires when its link closes, a request for height
/// 7, which every mirror withholds, times out, and the chain resets to a
/// fork. The reserved bound holds after
/// every step, and the ledgers at every rest. The overshoot past `M` is
/// reported.
#[test]
#[allow(clippy::print_stdout)]
fn the_floor_churn_holds_the_reserved_bound() {
    let scenario = BlockScenario::floor_churn();
    let (report, ports) = node_run_with_ports(&scenario, Plan::from_fuzz(&[]));
    for id in [
        RESERVED_BYTE_BOUND,
        BUDGET_LEDGERS_AGREE,
        RETAINED_BYTE_BOUND,
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
    // Every mirror withholds height 7, so a request for it ends only at its
    // timeout or its link's close. The first one goes out on a link the
    // harness never closes, and once the 8 s request timeout passes, the
    // work queue holds height 7 unowned again, before the reset.
    let (asked, session) = facts
        .iter()
        .find_map(|(step, fact)| match fact {
            BlockSyncFact::Request {
                owner,
                heights,
                step: moved,
                ..
            } if moved == "written" && heights.contains(&7) => Some((*step, owner.session_id)),
            _ => None,
        })
        .expect("the node asks for height 7");
    let asked_on = facts
        .iter()
        .find_map(|(_, fact)| match fact {
            BlockSyncFact::SessionAdmitted {
                connection,
                session_id,
                ..
            } if *session_id == session => Some(*connection),
            _ => None,
        })
        .expect("the request's session is admitted");
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
    let limits = Limits::of(&scenario.node_config(), 4, 2_000);
    let peaks = &property(&report, RESERVED_BYTE_BOUND).peaks;
    let overshoot = peaks["past-budget-estimate-bytes"];
    println!(
        "floor churn: reserved peak {}, {overshoot} past M, with {} open handoffs at most",
        peaks["reserved-estimate-bytes"], peaks["open-handoffs"]
    );
    assert!(overshoot > 0, "{peaks:?}");
    assert!(
        peaks["reserved-estimate-bytes"] <= limits.reserved_bound(peaks["open-handoffs"]),
        "{peaks:?}"
    );
}

/// The above-window input holds the look-ahead bound while the node's
/// above-window estimates `E` pass `L`: each worker's admission sees the
/// others' requests only once they publish, so `E` can pass `L` by up to
/// `(W + 1)·k·B`. With `omit-outstanding-estimates.patch`, admission
/// ignores outstanding requests, and the same input violates it. A debug
/// run takes over a minute, so the test runs the input once, reads the
/// trigger from the peaks (an above-window estimate the bound counted), and
/// runs only on demand. The planted runner judges the trigger history on
/// the same input.
#[test]
#[ignore = "a debug run takes over a minute; run it on demand with `--ignored` and `--profile planted`"]
fn the_above_window_input_holds_lookahead_overshoot_bound() {
    let witness = committed(ABOVE_WINDOW_INPUT);
    assert_eq!(witness.scenario, BlockScenario::above_window());
    let report = node_run(&witness.scenario, witness.input);
    let lookahead = property(&report, LOOKAHEAD_OVERSHOOT_BOUND);
    assert!(
        matches!(lookahead.outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "{lookahead:?}"
    );
    let peaks = &lookahead.peaks;
    let limits = Limits::of(&witness.scenario.node_config(), 2, 2_000);
    assert!(peaks["past-lookahead-bytes"] > 0, "{peaks:?}");
    assert!(
        peaks["above-window-estimate-bytes"] <= limits.lookahead_bound(0),
        "{peaks:?}"
    );
}

/// `block_sync_teardown_returns_baseline`: both mirrors leave while the
/// node owes them requests. Once every routine has ended, the input queue
/// is empty and the run rests, the byte budget, the work queue's tally and
/// the routines' unreceived estimates are zero, and no handoff is open. With `skip-return-on-drop.patch`, a dropped routine keeps its
/// requests, and the same input violates it.
#[test]
fn block_sync_teardown_returns_baseline() {
    let witness = committed(PEERS_LEAVE_INPUT);
    assert_eq!(witness.scenario, BlockScenario::peers_leave());
    let report = node_run(&witness.scenario, witness.input.clone());
    for id in IDS.into_iter().chain([TEARDOWN_RETURNS_BASELINE]) {
        let outcome = outcome(&report, id);
        assert!(
            matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
            "{id}: {outcome:?}"
        );
    }
    check_witness(witness, TEARDOWN_RETURNS_BASELINE, Expect::Holds);
}

/// P5.2: the short budget on 1, 2 and 4 workers, each with 2 and 4
/// mirrors, holds every bound. The report gives each run's overshoot past
/// `M` and past `L`; the most need not grow with `W`.
#[test]
#[allow(clippy::print_stdout)]
fn every_worker_count_holds_the_resource_bounds() {
    for workers in [1, 2, 4] {
        for mirrors in [2, 4] {
            let scenario = BlockScenario {
                workers: Some(workers),
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
            let peak = |id, quantity| property(&report, id).peaks[quantity];
            println!(
                "W = {workers}, {mirrors} mirrors: {} past M, {} past L",
                peak(RESERVED_BYTE_BOUND, "past-budget-estimate-bytes"),
                peak(LOOKAHEAD_OVERSHOOT_BOUND, "past-lookahead-bytes"),
            );
        }
    }
}

/// P5.2 on the above-window scenario, whose estimates pass `L`: 1, 2 and 4
/// workers each hold the look-ahead bound, and the report gives each run's
/// overshoot past `L`.
#[test]
#[ignore = "each debug run takes about 100 s"]
#[allow(clippy::print_stdout)]
fn every_worker_count_holds_the_lookahead_bound_above_the_window() {
    for workers in [1, 2, 4] {
        let scenario = BlockScenario {
            workers: Some(workers),
            ..BlockScenario::above_window()
        };
        let report = node_run(&scenario, Plan::from_fuzz(&[]));
        let lookahead = property(&report, LOOKAHEAD_OVERSHOOT_BOUND);
        assert!(
            matches!(lookahead.outcome, PropertyOutcome::Held { .. }),
            "W = {workers}: {lookahead:?}"
        );
        println!(
            "W = {workers}: E peaks at {}, {} past L",
            lookahead.peaks["above-window-estimate-bytes"], lookahead.peaks["past-lookahead-bytes"]
        );
    }
}

/// `lookahead_estimate_to_wire_growth` (P5.4): above-window requests that
/// reserve the smallest estimate get bodies about twice as large while the
/// sequencer can't commit. With `W` fixed, outstanding requests per peer
/// and the input queue's capacity vary. Each run holds the look-ahead
/// bound and retains above-window bodies past their estimates.
#[test]
#[ignore = "each debug run takes minutes"]
#[allow(clippy::print_stdout)]
fn lookahead_estimate_to_wire_growth() {
    let hint = BlockScenario::understated_hints(1, 401).size_hint;
    assert_eq!(hint.map(u64::from), Some(ESTIMATE_FLOOR));
    for (outstanding, queue) in [(1, 401), (2, 401), (4, 401), (4, 802)] {
        let scenario = BlockScenario::understated_hints(outstanding, queue);
        let report = node_run(&scenario, Plan::from_fuzz(&[]));
        let lookahead = property(&report, LOOKAHEAD_OVERSHOOT_BOUND);
        assert!(
            matches!(lookahead.outcome, PropertyOutcome::Held { .. }),
            "{outstanding} outstanding, Q = {queue}: {lookahead:?}"
        );
        let peaks = &lookahead.peaks;
        println!("{outstanding} outstanding, Q = {queue}: {peaks:?}");
        assert!(peaks["hint-error-bytes"] > 0, "{peaks:?}");
        assert!(
            peaks["above-window-wire-bytes"] > peaks["hint-error-bytes"],
            "{peaks:?}"
        );
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
                peaks(RESERVED_BYTE_BOUND)["reserved-estimate-bytes"] > 0,
                "{name}: {:?}",
                peaks(RESERVED_BYTE_BOUND)
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

/// Limits with `M` = 10, `L` = 5, `Q` = 401, one body per request,
/// bodies of at most 3 bytes, and 2 workers.
fn small_limits() -> Limits {
    Limits {
        budget: 10,
        lookahead: 5,
        queue: 401,
        per_request: 1,
        largest_body: 3,
        workers: 2,
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

/// The reserved bound adds `B` per open handoff to `M + 2·k·B`, and the
/// retained bound is `L + 401·b + Q·b`.
#[test]
fn the_bounds_add_up_their_allowances() {
    let limits = small_limits();
    assert_eq!(limits.reserved_bound(0), 10 + 2 * B);
    assert_eq!(limits.reserved_bound(3), 10 + 5 * B);
    assert_eq!(limits.retained_bound(), 5 + (401 + 401) * 3);
    let two_per_request = Limits {
        per_request: 2,
        ..limits
    };
    assert_eq!(two_per_request.reserved_bound(1), 10 + 5 * B);
    assert_eq!(limits.lookahead_bound(0), 5 + 3 * B);
    assert_eq!(limits.lookahead_bound(7), 5 + 7 + 3 * B);
    let four_workers = Limits {
        workers: 4,
        ..limits
    };
    assert_eq!(four_workers.lookahead_bound(0), 5 + 5 * B);
    assert_eq!(two_per_request.lookahead_bound(0), 5 + 6 * B);
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
    };
    let unestimated = OwnerInput::Held {
        holder: BodyHolder::Reorder(1),
        request: (1, 1),
        height: 5,
        bytes: 3,
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
    };
    let snapshot = |holders: &[BodyHolder]| OwnerInput::Snapshot {
        holders: holders.iter().map(|holder| (*holder, 5, 3)).collect(),
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

/// The budget's changes chain from the empty budget, at the configured
/// maximum. A change from another count, or at another maximum, leaves the
/// counter unknown.
#[test]
fn budget_changes_chain_at_the_configured_maximum() {
    let change = |before, after, limit| OwnerInput::BudgetChanged {
        before,
        after,
        limit,
    };
    let mut owners = OwnerModel::new(small_limits(), 0);
    owners.apply(change(0, 7, 10));
    owners.apply(change(7, 14, 10));
    owners.apply(change(14, 0, 10));
    assert_eq!((owners.reserved(), owners.peaks().reserved), (0, 14));
    assert_eq!(owners.budget_gap(), None);

    let mut skipped = OwnerModel::new(small_limits(), 0);
    skipped.apply(change(0, 7, 10));
    skipped.apply(change(3, 0, 10));
    assert!(skipped.budget_gap().is_some());

    let mut other_maximum = OwnerModel::new(small_limits(), 0);
    other_maximum.apply(change(0, 7, 11));
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

/// D2's `h` counts open handoffs only while a step leaves a task ready, and
/// at most one per live routine, so leaked handoffs can't widen the
/// reserved bound.
#[test]
fn counted_handoffs_stop_at_live_routines_and_rest() {
    let mut owners = OwnerModel::new(small_limits(), 0);
    let routine = |generation, ended| OwnerInput::Routine {
        connection: 4,
        generation,
        ended,
        requests: Vec::new(),
    };
    owners.apply(routine(1, false));
    owners.apply(routine(2, false));
    for request_id in 1..=3 {
        owners.apply(OwnerInput::HandoffStarted {
            owner: owner(request_id),
            height: 5,
            charge: 7,
        });
    }
    assert_eq!(owners.counted_handoffs(SchedulerStatus::Running), 2);
    assert_eq!(owners.counted_handoffs(SchedulerStatus::WorkerStarved), 2);
    owners.apply(routine(2, true));
    assert_eq!(owners.counted_handoffs(SchedulerStatus::Running), 1);
    for status in [
        SchedulerStatus::Quiescent,
        SchedulerStatus::WaitingOnTime,
        SchedulerStatus::WaitingOnExternal,
    ] {
        assert_eq!(owners.counted_handoffs(status), 0, "{status:?}");
    }
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

/// Above-window heights are those past the highest verified tip plus 401.
/// `R` counts their retained bodies once per allocation, `E` their
/// unreceived estimates, and `H` each retained body's bytes past its
/// request's estimate. A lower tip never moves heights back out.
#[test]
fn the_lookahead_quantities_count_only_above_the_window() {
    let mut owners = OwnerModel::new(small_limits(), 10);
    let above = 10 + 402;
    owners.apply(OwnerInput::Routine {
        connection: 4,
        generation: 1,
        ended: false,
        requests: vec![
            requested(1, above, 2, true),
            requested(2, above - 1, 5, false),
            requested(3, above + 1, 7, false),
        ],
    });
    for (allocation, request, height) in [(1, 1, above), (2, 2, above - 1)] {
        owners.apply(OwnerInput::Held {
            holder: BodyHolder::Reorder(allocation),
            request: (1, request),
            height,
            bytes: 3,
        });
    }
    owners.apply(OwnerInput::Held {
        holder: BodyHolder::Applying(1),
        request: (1, 1),
        height: above,
        bytes: 3,
    });
    assert_eq!(owners.holder_gap(), None);
    assert_eq!(
        owners.lookahead(),
        Lookahead {
            retained: 3,
            estimated: 7,
            hint_error: 1,
        }
    );
    owners.apply(OwnerInput::VerifiedTip { height: 11 });
    assert_eq!(owners.lookahead().retained, 0);
    owners.apply(OwnerInput::VerifiedTip { height: 5 });
    assert_eq!(owners.verified_tip(), 11);
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
