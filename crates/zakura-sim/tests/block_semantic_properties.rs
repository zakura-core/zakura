//! Block sync's semantic properties (P6b, gates P6.1 and P6.2): each
//! seed reaches its property's trigger history on the real node,
//! and holds. An alternate submission callback
//! violates `submit-only-requested`, and the honest scenarios repeat across
//! A/A and A/B/A. `cargo xtask planted` runs the same seeds with each
//! property's bypass patch applied.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    domain::block_sync_semantic::{
        CAPACITY_RELEASE_WAKES, DETACHED_SUBMISSION_BOUNDED, INPUT_DECODED_BOUND,
        REACTOR_QUERY_LIFECYCLE, STALE_COMPLETION_INERT, SUBMIT_ONLY_REQUESTED,
    },
    harness::{
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            view::PairingKind,
            Policy,
        },
        block_sync::{BlockScenario, Ports, Storage},
        run_prepared, Subsystem,
    },
    seed,
    witness::{witness_prepared, Expect, WitnessScenario},
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The seed that picks the reset-mid-flight scenario: two mirrors serve
/// while the chain resets.
const RESET_MID_FLIGHT: &str = "block-sync-reset-mid-flight";

/// The seed that picks the stalled-reset scenario: storage never answers,
/// and a reset detaches the first submission.
const STALLED_RESET: &str = "block-sync-stalled-reset";

/// The seed that picks the decoded-input-stall scenario: the sequencer
/// stalls, and the mirror keeps delivering 2 MB bodies after the input
/// queue's decoded bytes fill the cap.
const DECODED_INPUT_STALL: &str = "block-sync-decoded-input-stall";

/// The seed that picks the stale-beside-current scenario: a reset detaches a
/// held submission, whose late retryable verdict reaches block sync while
/// the fork's first submission waits in storage.
const STALE_BESIDE_CURRENT: &str = "block-sync-stale-beside-current";

/// The seed that picks the capacity-release scenario, under a plan where
/// block sync handles the detached commit's tip before its reply.
const CAPACITY_FRONTIER_FIRST: &str = "block-sync-capacity-frontier-first";

/// Seed `name`'s full fuzz input.
fn input(name: &str) -> Vec<u8> {
    seed::find(name)
        .unwrap_or_else(|| panic!("{name} is a registered seed"))
        .input()
        .to_vec()
}

/// The scenario and Plan that `zakura-sim record --input` runs for `input`.
fn committed(input: &[u8]) -> WitnessScenario<Storage> {
    let (prefix, plan) = input.split_at(Storage::SCENARIO_BYTES);
    WitnessScenario {
        scenario: Storage::draw(prefix),
        input: Plan::from_fuzz(plan),
    }
}

/// The scenario and Plan that `zakura-sim record --seed` runs for seed
/// `name`.
fn seeded(name: &str) -> WitnessScenario<Storage> {
    committed(&input(name))
}

/// Checks `witness` as a witness test of `property` that expects `expect`,
/// and checks that the run paired every block storage operation. Returns
/// the run's ports, or why either check failed.
fn witnessed(
    witness: WitnessScenario<Storage>,
    property: &str,
    expect: Expect,
) -> Result<Ports, String> {
    let ports = Storage::prepare(&witness.scenario);
    witness_prepared::<Storage>(
        &witness.scenario,
        ports.clone(),
        witness.input,
        property,
        expect,
        &mut MemorySink::default(),
    )
    .map_err(|error| format!("{property}: {error}"))?;
    {
        let view = ports.view.borrow();
        if !view.pairing.is_empty() {
            return Err(format!("{:?}", view.pairing));
        }
    }
    Ok(ports)
}

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run. The run must end with no harness failure.
fn run(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    (report, ports)
}

/// Each seed draws the scenario its name says.
#[test]
fn each_seed_draws_its_scenario() {
    for (name, scenario) in [
        (RESET_MID_FLIGHT, BlockScenario::reset_mid_flight()),
        (STALLED_RESET, BlockScenario::stalled_reset()),
        (DECODED_INPUT_STALL, BlockScenario::decoded_input_stall()),
        (CAPACITY_FRONTIER_FIRST, BlockScenario::capacity_release()),
    ] {
        assert_eq!(seeded(name).scenario, scenario, "{name}");
    }
}

/// The reset-mid-flight input submits old-epoch and new-epoch bodies, some
/// through first-completion-wins, and each names the request that fetched
/// it. With `height-only.patch`, the sequencer adopts an old-epoch body
/// into the new epoch's work, and the same input violates the property.
/// The test counts each kind of submission and checks that the sequencer
/// refuses an old-epoch body after it applies the reset, so a change that
/// drops one fails here instead of weakening the planted case.
#[test]
fn the_reset_mid_flight_input_holds_submit_only_requested() {
    if let Err(shortfall) = reset_mid_flight_shortfall(&input(RESET_MID_FLIGHT)) {
        panic!("{shortfall}");
    }
}

/// Checks that `input` fills the reset-mid-flight input's role, or returns
/// why it falls short. Its run must be a witness that holds
/// `submit-only-requested` and pairs every block storage operation. It must
/// submit bodies of an old and a new epoch, and credit a submission through
/// first-completion-wins. And the sequencer must refuse an old epoch's body
/// as stale after it applied a newer epoch: that is the body
/// `height-only.patch` adopts.
fn reset_mid_flight_shortfall(input: &[u8]) -> Result<(), String> {
    let ports = witnessed(committed(input), SUBMIT_ONLY_REQUESTED, Expect::Holds)?;
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    // Each key counts at least one submission.
    let epochs = &model.submission_epochs;
    if epochs.len() < 2 || epochs.contains_key(&None) {
        return Err(format!(
            "submissions by epoch, which needs an old and a new one: {epochs:?}"
        ));
    }
    if model.credited_elsewhere == 0 {
        return Err("no first-completion-wins credit".to_owned());
    }
    if !model.saw_old_epoch_body_refused {
        return Err("the sequencer refused no old-epoch body under a newer epoch".to_owned());
    }
    Ok(())
}

/// The reset-mid-flight input's reset supersedes the reactor's query state,
/// and storage takes each accepted query under its exact identity. This input
/// isn't known to fill or close the action channel, so the test makes no claim
/// about refusals.
#[test]
fn the_reset_mid_flight_input_holds_reactor_query_lifecycle() {
    let ports = witnessed(
        seeded(RESET_MID_FLIGHT),
        REACTOR_QUERY_LIFECYCLE,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    assert!(model.reactor_queries.saw_query_at_storage);
}

/// The stale-beside-current seed's stale verdict releases only its own
/// submission. With `release-by-height.patch`, it releases the fork's
/// submission at the same height, which storage still holds, and the same
/// seed violates the property.
#[test]
fn the_stale_beside_current_input_holds_stale_completion_inert() {
    witnessed(
        seeded(STALE_BESIDE_CURRENT),
        STALE_COMPLETION_INERT,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

/// Under stalled storage, with no `BoundedStorage` assumption, the reset
/// detaches the held submission, which keeps its holder and its place
/// under the limit. The property holds. With `forget-on-reset.patch`, the
/// reset forgets the submission, and the same input violates it.
#[test]
fn the_stalled_reset_input_holds_detached_submission_bounded() {
    witnessed(
        seeded(STALLED_RESET),
        DETACHED_SUBMISSION_BOUNDED,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

/// Under stalled storage, the mirror's bodies keep entering the
/// sequencer's input queue. The decoded cap has room for these small
/// bodies, so each one keeps its decoded block, and the check judges each
/// handoff against the queue's and `applying`'s decoded bytes. The
/// decoded-input-stall input fills the cap.
#[test]
fn the_stalled_reset_input_holds_input_decoded_bound() {
    let ports = witnessed(seeded(STALLED_RESET), INPUT_DECODED_BOUND, Expect::Holds)
        .unwrap_or_else(|error| panic!("{error}"));
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    assert!(model.saw_decoded_queued);
    assert!(model.queued_decoded_peak > 0);
}

/// With the sequencer paused, the input queue's decoded bodies fill the
/// decoded-byte cap, and the mirror keeps delivering 2 MB bodies. Once the
/// cap fills, the routine hands over raw bytes, so no decoded handoff takes
/// the queue's and applying's decoded bytes past the cap by more than one
/// body. With `no-room-check.patch`, every handoff keeps its
/// decoded block, as on main, and the same input violates the property at
/// step 122,041.
#[test]
#[ignore = "a debug run takes minutes; run it on demand with `--ignored` and `--profile planted`"]
fn the_decoded_input_stall_input_holds_input_decoded_bound() {
    let ports = witnessed(
        seeded(DECODED_INPUT_STALL),
        INPUT_DECODED_BOUND,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    assert!(model.saw_decoded_queued);
    assert!(
        model.raw_queued_at_cap > 0,
        "no raw body entered a full queue"
    );
}

/// The capacity-release input whose plan handles the detached commit's tip
/// first: the frontier releases height 1, and the stale reply frees room
/// that wakes the waiting fork body. Before #1523's fix, that body stayed
/// unsubmitted until the node waited on time. The witness requires that
/// order: the verified tip reached the released submission's height, and
/// no `applying` entry holds it.
#[test]
#[ignore = "a debug run takes about eight minutes; run it on demand with `--ignored` and `--profile planted`"]
fn the_frontier_first_input_holds_capacity_release_wakes() {
    if let Err(shortfall) = capacity_frontier_first_shortfall(&input(CAPACITY_FRONTIER_FIRST)) {
        panic!("{shortfall}");
    }
}

/// Checks that `input` fills the frontier-first input's role, or returns
/// why it falls short. Its run must be a witness that holds
/// `capacity-release-wakes` and pairs every block storage operation, so it
/// reaches #1523's order: the verified tip passed a released submission's
/// height before its reply freed room beside a waiting body.
/// `stale-completion-no-wake.patch` also needs that submission to be older
/// than the reactor's authority. In this scenario storage releases height
/// 1, which it admitted before the reset.
fn capacity_frontier_first_shortfall(input: &[u8]) -> Result<(), String> {
    witnessed(committed(input), CAPACITY_RELEASE_WAKES, Expect::Holds).map(drop)
}

/// Bypass: an alternate callback hands the node's block service a body
/// that no request of the node's asked for. `submit-only-requested` stops
/// the run there, and the pairing shows the submission block sync never
/// dispatched.
#[test]
fn an_unrequested_submission_violates_submit_only_requested() {
    let scenario = BlockScenario {
        blocks: 8,
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(BlockKind::Apply, 1),
                until: Trigger::VirtualTime(1_000),
                then: vec![BlockAllow::Committed],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::SubmitUnrequested,
            when: Some(Trigger::Submitted(Select::nth(BlockKind::Apply, 1))),
        }],
        ..BlockScenario::default()
    };
    for input in inputs(2, 256) {
        let (report, ports) = run(&scenario, input);
        let violation = report.end.violation.as_ref().expect("the bypass is caught");
        assert_eq!(violation.property, SUBMIT_ONLY_REQUESTED, "{violation:?}");
        let view = ports.view.borrow();
        assert!(
            view.pairing
                .iter()
                .any(|anomaly| anomaly.kind == PairingKind::Unpaired),
            "{:?}",
            view.pairing
        );
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

/// The reset-mid-flight scenario's history depends only on its plan: A,
/// then B, then A gives A's history twice, and B's differs, so the
/// comparison can fail. Every run pairs each operation, matches each
/// sequencer snapshot with the holder events, and violates nothing.
#[test]
fn the_reset_mid_flight_scenario_repeats_a_b_a() {
    let plans = inputs(2, 256);
    let scenario = BlockScenario::reset_mid_flight();
    let histories: Vec<_> = [&plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, ports) = run(&scenario, plan.clone());
            assert_eq!(report.end.violation, None, "{:?}", report.end);
            assert!(
                report
                    .properties
                    .iter()
                    .all(|p| !matches!(p.outcome, PropertyOutcome::Violated { .. })),
                "{:?}",
                report.properties
            );
            let view = ports.view.borrow();
            assert!(view.pairing.is_empty(), "{:?}", view.pairing);
            assert!(view.anomalies.is_empty(), "{:?}", view.anomalies);
            assert_eq!(view.owners.holder_gap(), None);
            history(&report)
        })
        .collect();
    assert!(histories[0] == histories[2], "plan A, then B, then A");
    assert!(histories[0] != histories[1], "plan B differs");
}
