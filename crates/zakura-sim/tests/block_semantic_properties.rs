//! Block sync's semantic properties (P6b, gates P6.1 and P6.2): each
//! committed input reaches its property's trigger history on the real node,
//! and holds. An alternate submission callback
//! violates `submit-only-requested`, and the honest scenarios repeat across
//! A/A and A/B/A. `cargo xtask planted` runs the same inputs with each
//! property's bypass patch applied.
#![cfg(zakura_sim)]

mod support;

use support::{byte_inputs, inputs};
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
        block_semantic::{CAPACITY_RELEASE_TAG, RESET_MID_FLIGHT_TAG},
        block_sync::{BlockScenario, Ports, Storage},
        run_prepared, Subsystem,
    },
    seed::sha256,
    witness::{witness_prepared, Expect, WitnessScenario},
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The committed input that picks the reset-mid-flight scenario: two
/// mirrors serve while the chain resets.
const RESET_MID_FLIGHT_INPUT: &[u8] = include_bytes!("fixtures/block-sync-reset-mid-flight.bin");

/// The committed input that picks the stale-beside-current scenario: a
/// detached submission's verdict arrives while a fork submission waits.
const STALE_BESIDE_CURRENT_INPUT: &[u8] =
    include_bytes!("fixtures/block-sync-stale-beside-current.bin");

/// The committed input that picks the stalled-reset scenario: storage never
/// answers, and a reset detaches the first submission.
const STALLED_RESET_INPUT: &[u8] = include_bytes!("fixtures/block-sync-stalled-reset.bin");

/// The committed input that picks the decoded-input-stall scenario: the
/// sequencer stalls, and the mirror keeps delivering 2 MB bodies after the
/// input queue's decoded bytes fill the cap.
const DECODED_INPUT_STALL_INPUT: &[u8] =
    include_bytes!("fixtures/block-sync-decoded-input-stall.bin");

/// The committed input that picks the capacity-release scenario, under a
/// plan where block sync handles the detached commit's tip before its reply.
const CAPACITY_FRONTIER_FIRST_INPUT: &[u8] =
    include_bytes!("fixtures/block-sync-capacity-frontier-first.bin");

/// The committed input that picks the capacity-release scenario, under a
/// plan where block sync handles the detached commit's reply before its tip.
const CAPACITY_REPLY_FIRST_INPUT: &[u8] =
    include_bytes!("fixtures/block-sync-capacity-reply-first.bin");

/// The scenario and Plan that `zakura-sim record --input` runs for `input`.
fn committed(input: &[u8]) -> WitnessScenario<Storage> {
    let (prefix, plan) = input.split_at(Storage::SCENARIO_BYTES);
    WitnessScenario {
        scenario: Storage::draw(prefix),
        input: Plan::from_fuzz(plan),
    }
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

/// `id`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} has no report"))
        .outcome
}

/// Each committed input draws the scenario its sidecar names.
#[test]
fn each_committed_input_draws_its_scenario() {
    for (input, scenario) in [
        (RESET_MID_FLIGHT_INPUT, BlockScenario::reset_mid_flight()),
        (
            STALE_BESIDE_CURRENT_INPUT,
            BlockScenario::stale_beside_current(),
        ),
        (STALLED_RESET_INPUT, BlockScenario::stalled_reset()),
        (
            DECODED_INPUT_STALL_INPUT,
            BlockScenario::decoded_input_stall(),
        ),
        (
            CAPACITY_FRONTIER_FIRST_INPUT,
            BlockScenario::capacity_release(),
        ),
        (
            CAPACITY_REPLY_FIRST_INPUT,
            BlockScenario::capacity_release(),
        ),
    ] {
        assert_eq!(committed(input).scenario, scenario);
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
    if let Err(shortfall) = reset_mid_flight_shortfall(RESET_MID_FLIGHT_INPUT) {
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

/// How many fuzz inputs a regenerate tool tries.
const REGENERATE_CANDIDATES: usize = 64;

/// Rewrites the committed reset-mid-flight input with the first of
/// [`REGENERATE_CANDIDATES`] fuzz inputs that fills the input's role
/// ([`reset_mid_flight_shortfall`]), and its sidecar's SHA-256 to match. A
/// block-sync change that moves the input off the crossing needs only this
/// run, then `cargo xtask planted block-sync/submit-only-requested` to show
/// that `height-only.patch` is still caught.
#[test]
#[ignore = "rewrites the committed input"]
fn regenerate_reset_mid_flight_input() {
    regenerate(
        "block-sync-reset-mid-flight",
        RESET_MID_FLIGHT_TAG,
        1,
        reset_mid_flight_shortfall,
    );
}

/// Rewrites the committed input `name` under `tests/fixtures` with the
/// first of [`REGENERATE_CANDIDATES`] fuzz inputs, after the scenario tag
/// `tag`, for which `shortfall` finds nothing, and its sidecar's SHA-256 to
/// match. It runs `width` candidates at once, and still picks the first in
/// order, so the result doesn't depend on `width`. It prints each
/// candidate's shortfall, and panics if every candidate falls short.
#[allow(clippy::print_stdout)]
fn regenerate(name: &str, tag: [u8; 2], width: usize, shortfall: fn(&[u8]) -> Result<(), String>) {
    let candidates: Vec<Vec<u8>> = byte_inputs(REGENERATE_CANDIDATES, 16_384)
        .into_iter()
        .map(|plan| [&tag[..], &plan].concat())
        .collect();
    let mut chosen = None;
    for (batch, inputs) in candidates.chunks(width).enumerate() {
        let results: Vec<Result<(), String>> = std::thread::scope(|scope| {
            let runs: Vec<_> = inputs
                .iter()
                .map(|input| scope.spawn(move || shortfall(input)))
                .collect();
            runs.into_iter()
                .map(|run| run.join().expect("a candidate's check returns"))
                .collect()
        });
        for (offset, result) in results.iter().enumerate() {
            println!("{name}: candidate {}: {result:?}", batch * width + offset);
        }
        if let Some(offset) = results.iter().position(Result::is_ok) {
            chosen = Some(&inputs[offset]);
            break;
        }
    }
    let bytes = chosen.unwrap_or_else(|| {
        panic!("{name}: none of {REGENERATE_CANDIDATES} candidates fills the input's role")
    });
    let fixture = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::write(format!("{fixture}.bin"), bytes).expect("the fixture directory exists");
    let sidecar = format!("{fixture}.toml");
    let text = std::fs::read_to_string(&sidecar).expect("the input has a sidecar");
    let text: String = text
        .lines()
        .map(|line| {
            if line.starts_with("sha256 = ") {
                format!("sha256 = \"{}\"\n", sha256(bytes))
            } else {
                format!("{line}\n")
            }
        })
        .collect();
    std::fs::write(&sidecar, text).expect("the sidecar is writable");
}

/// The reset-mid-flight input's reset supersedes the reactor's query state,
/// and storage takes each accepted query under its exact identity. This input
/// isn't known to fill or close the action channel, so the test makes no claim
/// about refusals.
#[test]
fn the_reset_mid_flight_input_holds_reactor_query_lifecycle() {
    let ports = witnessed(
        committed(RESET_MID_FLIGHT_INPUT),
        REACTOR_QUERY_LIFECYCLE,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    assert!(model.reactor_queries.saw_query_at_storage);
}

/// The stale-beside-current input's stale verdict releases only its own
/// submission. With `release-by-height.patch`, it releases the fork's
/// submission at the same height, which storage still holds, and the same
/// input violates the property.
#[test]
fn the_stale_beside_current_input_holds_stale_completion_inert() {
    witnessed(
        committed(STALE_BESIDE_CURRENT_INPUT),
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
        committed(STALLED_RESET_INPUT),
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
    let ports = witnessed(
        committed(STALLED_RESET_INPUT),
        INPUT_DECODED_BOUND,
        Expect::Holds,
    )
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
        committed(DECODED_INPUT_STALL_INPUT),
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
    if let Err(shortfall) = capacity_frontier_first_shortfall(CAPACITY_FRONTIER_FIRST_INPUT) {
        panic!("{shortfall}");
    }
}

/// Checks that `input` fills the frontier-first input's role, or returns
/// why it falls short. Its run must be a witness that holds
/// `capacity-release-wakes` and pairs every block storage operation, so it
/// reaches #1523's order: the verified tip passed a released submission's
/// height before its reply freed room beside a waiting body.
/// `stale-completion-no-wake.patch` also needs that submission to be older
/// than the reactor's authority. The scenario guarantees it: storage
/// releases height 1, which it admitted before the reset.
fn capacity_frontier_first_shortfall(input: &[u8]) -> Result<(), String> {
    witnessed(committed(input), CAPACITY_RELEASE_WAKES, Expect::Holds).map(drop)
}

/// Rewrites the committed frontier-first input with the first of
/// [`REGENERATE_CANDIDATES`] fuzz inputs that fills the input's role
/// ([`capacity_frontier_first_shortfall`]), and its sidecar's SHA-256 to
/// match. A block-sync change that moves the input off #1523's order needs
/// only this run, then `cargo xtask planted block-sync/capacity-release-wakes`
/// to show that `stale-completion-no-wake.patch` is still caught.
#[test]
#[ignore = "rewrites the committed input"]
fn regenerate_capacity_frontier_first_input() {
    // A capacity-release run takes about 6 minutes and peaks near 1.4 GB,
    // so four candidates at once stay near 6 GB.
    regenerate(
        "block-sync-capacity-frontier-first",
        CAPACITY_RELEASE_TAG,
        4,
        capacity_frontier_first_shortfall,
    );
}

/// The capacity-release input whose plan handles the detached commit's
/// reply first, before the frontier releases height 1, so the freed room
/// wakes the waiting fork body. Room frees beside the waiting body, but not
/// in the frontier-first order, so this run isn't the property's witness: the test
/// checks the run directly.
#[test]
#[ignore = "a debug run takes about eight minutes; run it on demand with `--ignored` and `--profile planted`"]
fn the_reply_first_input_holds_capacity_release_wakes() {
    let witness = committed(CAPACITY_REPLY_FIRST_INPUT);
    let (report, ports) = run(&witness.scenario, witness.input);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert!(
        report
            .properties
            .iter()
            .all(|p| !matches!(p.outcome, PropertyOutcome::Violated { .. })),
        "{:?}",
        report.properties
    );
    assert!(
        matches!(
            outcome(&report, CAPACITY_RELEASE_WAKES),
            PropertyOutcome::Held { .. }
        ),
        "{:?}",
        outcome(&report, CAPACITY_RELEASE_WAKES)
    );
    let view = ports.view.borrow();
    let model = view.semantic.borrow();
    assert!(
        model.saw_room_freed && !model.saw_room_freed_past_frontier,
        "room freed: {}, past the frontier: {}",
        model.saw_room_freed,
        model.saw_room_freed_past_frontier
    );
    assert!(view.pairing.is_empty(), "{:?}", view.pairing);
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
