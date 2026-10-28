//! Block sync's resource bounds (P5b-1, gates P5.1–P5.4): each committed
//! input reaches its property's trigger history on the real node and
//! holds, honest runs hold all three bounds across A/A and A/B/A, and the
//! owner model folds the observer's effects as the bounds need. `cargo
//! xtask planted` runs the same inputs with each bound's bypass patch
//! applied.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_scheduler::Status as SchedulerStatus;
use zakura_sim::{
    domain::block_sync::{
        BodyHolder, Limits, OwnerInput, OwnerModel, BUDGET_LEDGERS_AGREE, ESTIMATE_CEILING,
        RESERVED_BYTE_BOUND, RETAINED_BYTE_BOUND,
    },
    harness::{
        block_sync::{BlockScenario, MirrorSpec, Storage},
        header_sync::Dialer,
        run_prepared, Subsystem,
    },
    observe::{StorageBranch, WorkOwner, WorkScope},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Plan, PlanInput, PropertyOutcome, PropertyReport, RunReport,
};

/// The committed input that picks the short-budget scenario: one request
/// leaves the byte budget short of the next.
const SHORT_BUDGET_INPUT: &[u8] = include_bytes!("fixtures/block-sync-short-budget.bin");

/// The committed input that picks the stuck-floor scenario: the commit
/// window fills while height 3 never arrives.
const STUCK_FLOOR_INPUT: &[u8] = include_bytes!("fixtures/block-sync-stuck-floor.bin");

/// The committed input that picks the default scenario, whose routines hand
/// charged bodies to the sequencer.
const HANDOFF_INPUT: &[u8] = include_bytes!("fixtures/block-sync-handoff.bin");

/// The three resource properties.
const IDS: [&str; 3] = [
    RETAINED_BYTE_BOUND,
    RESERVED_BYTE_BOUND,
    BUDGET_LEDGERS_AGREE,
];

/// The scenario and Plan that `zakura-sim record --input` runs for `input`.
fn committed(input: &[u8]) -> WitnessScenario<Storage> {
    let (prefix, plan) = input.split_at(Storage::SCENARIO_BYTES);
    WitnessScenario {
        scenario: Storage::draw(prefix),
        input: Plan::from_fuzz(plan),
    }
}

/// The short-budget input holds the reserved bound after its budget runs
/// short. With `above-floor-charge.patch`, above-floor requests charge
/// past `M` too, and the same input violates it. With
/// `handoff-keeps-charge.patch`, leaked charges pile up past `M + 2·B`
/// once the run rests, and the same input violates it too.
#[test]
fn the_short_budget_input_holds_reserved_byte_bound() {
    holds(committed(SHORT_BUDGET_INPUT), RESERVED_BYTE_BOUND);
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

/// Runs `scenario` under `input`. The run must end with no violation and
/// no harness failure, with every block storage operation paired, and with
/// every sequencer snapshot matching the holder events.
fn node_run(scenario: &BlockScenario, input: PlanInput) -> RunReport {
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
    let view = ports.view.borrow();
    assert!(view.pairing.is_empty(), "{:?}", view.pairing);
    assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
    assert_eq!(view.owners.holder_gap(), None);
    report
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

/// Honest runs, BS5's two-mirror flow among them, hold all three bounds,
/// each at least once. Plan A twice gives one history, and so does A, then
/// B, then A.
#[test]
fn honest_runs_hold_every_resource_bound_a_a_and_a_b_a() {
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
        let reports: Vec<_> = [&plans[0], &plans[0], &plans[1], &plans[0]]
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
        assert!(histories[0] == histories[1], "{name}: plan A, then A");
        assert!(
            histories[0] == histories[3],
            "{name}: plan A, then B, then A"
        );
        assert!(histories[0] != histories[2], "{name}: plan B differs");
    }
}

/// `B`, the largest estimate.
const B: u64 = ESTIMATE_CEILING;

/// Limits with `M` = 10, `L` = 5, `Q` = 401, one body per request, and
/// bodies of at most 3 bytes.
fn small_limits() -> Limits {
    Limits {
        budget: 10,
        lookahead: 5,
        queue: 401,
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
}

/// A body counts once while the input queue, the reorder buffer or
/// `applying` holds its allocation, and not at all for a submission.
#[test]
fn a_retained_body_counts_once_per_allocation() {
    let mut owners = OwnerModel::new(small_limits());
    let held = |holder| OwnerInput::Held {
        holder,
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

/// A body larger than the fixture allows, a release of a body never taken,
/// and a handoff that ends without starting each leave the holders unknown.
#[test]
fn holder_inputs_the_model_cannot_apply_are_gaps() {
    let oversized = OwnerInput::Held {
        holder: BodyHolder::Reorder(1),
        height: 5,
        bytes: 4,
    };
    let never_taken = OwnerInput::Released {
        holder: BodyHolder::Applying(2),
    };
    let unstarted = OwnerInput::HandoffEnded {
        owner: owner(1),
        height: 5,
        charge: 7,
    };
    for input in [oversized, never_taken, unstarted] {
        let mut owners = OwnerModel::new(small_limits());
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
    let held = |holder| OwnerInput::Held {
        holder,
        height: 5,
        bytes: 3,
    };
    let snapshot = |holders: &[BodyHolder]| OwnerInput::Snapshot {
        holders: holders.iter().map(|holder| (*holder, 5, 3)).collect(),
    };
    let mut owners = OwnerModel::new(small_limits());
    owners.apply(held(BodyHolder::Queued(1)));
    owners.apply(held(BodyHolder::Applying(2)));
    owners.apply(held(BodyHolder::Submitted(9)));
    owners.apply(snapshot(&[
        BodyHolder::Submitted(9),
        BodyHolder::Applying(2),
    ]));
    assert_eq!(owners.holder_gap(), None);

    let mut dropped = OwnerModel::new(small_limits());
    dropped.apply(snapshot(&[BodyHolder::Reorder(1)]));
    assert_eq!(dropped.retained(), 0);
    assert!(dropped.holder_gap().is_some());

    let mut omitted = OwnerModel::new(small_limits());
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
    let mut owners = OwnerModel::new(small_limits());
    owners.apply(change(0, 7, 10));
    owners.apply(change(7, 14, 10));
    owners.apply(change(14, 0, 10));
    assert_eq!((owners.reserved(), owners.peaks().reserved), (0, 14));
    assert_eq!(owners.budget_gap(), None);

    let mut skipped = OwnerModel::new(small_limits());
    skipped.apply(change(0, 7, 10));
    skipped.apply(change(3, 0, 10));
    assert!(skipped.budget_gap().is_some());

    let mut other_maximum = OwnerModel::new(small_limits());
    other_maximum.apply(change(0, 7, 11));
    assert!(other_maximum.budget_gap().is_some());
}

/// A handoff stays open from its start to its matching end, and a routine
/// stays live from its first snapshot to its last.
#[test]
fn handoffs_and_routines_follow_their_inputs() {
    let mut owners = OwnerModel::new(small_limits());
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
    let mut owners = OwnerModel::new(small_limits());
    let routine = |generation, ended| OwnerInput::Routine {
        connection: 4,
        generation,
        ended,
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
