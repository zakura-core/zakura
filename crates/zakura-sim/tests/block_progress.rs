//! Step 10 for block sync: `block-sync/progress` runs the honest mirror
//! with k hostile ones, and `block-sync/adversary-progress` holds the node
//! to the deadline production's constants give for k, while each parked
//! adversary stays parked.
//!
//! Each fixed role mix ([`Mix`]) is a prefix of the drawn input, and the
//! nightly sim runs them all here, with a few drawn inputs. The bypass,
//! block sync that never takes stalled work back, is the planted case
//! `qa/planted/block-sync/adversary-progress/stall-never-reassigned`.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use support::byte_inputs;
use zakura_sim::{
    harness::{
        block_progress::{
            deadline, draw_mix, mix_prefix, Adversary, BlockProgress, Mix, Role,
            ADVERSARY_PROGRESS, MAX_ADVERSARIES, PARK, PROGRESS_BYTES,
        },
        block_sync::{BlockScenario, Ports},
        run_prepared, Subsystem,
    },
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Runs `scenario` under `plan`. The run must end without a harness
/// failure or a violation that stopped it.
fn run(scenario: &BlockScenario, plan: PlanInput) -> (RunReport, Ports) {
    let ports = BlockProgress::prepare(scenario);
    let report = run_prepared::<BlockProgress>(
        scenario,
        ports.clone(),
        plan,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, ports)
}

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|report| report.property == property)
        .unwrap_or_else(|| panic!("{property} is reported"))
        .outcome
}

/// Each mix's prefix draws the honest mirror with its adversaries, each
/// with a deadline inside the park, and a prefix whose adversaries outlast
/// the park drops the last of them.
///
/// This fails if a mix's deadline outlasts the park, so the property can't
/// apply.
#[test]
fn every_mix_fits_inside_the_park() {
    for mix in Mix::ALL {
        let prefix = mix.prefix();
        assert_eq!(prefix.len(), PROGRESS_BYTES);
        assert!(BlockProgress::accepts(&prefix));
        let scenario = mix.scenario();
        assert_eq!(
            scenario.mirrors.len(),
            1 + usize::from(prefix[1]),
            "{}",
            mix.name()
        );
        assert!(scenario.mirrors[0].fault.is_none());
        let deadline = deadline(&scenario).expect("the mix fits inside the park");
        assert!(deadline <= PARK, "{}: {deadline:?}", mix.name());
    }
    let most = [Adversary::new(Role::Silent, 0); MAX_ADVERSARIES];
    let drawn = BlockProgress::draw(&mix_prefix(&most));
    assert!(deadline(&drawn).is_ok());
    assert_eq!(draw_mix(&[]).mirrors.len(), 1);
}

/// On each fixed mix, and a few drawn inputs, the node submits every body
/// for commit, and `block-sync/adversary-progress` holds at its one
/// trigger.
///
/// This fails if an adversary holds work past its liveness turn, or the
/// node never hands stalled work to the honest mirror, so the last body
/// misses the deadline.
#[test]
fn the_role_mixes_hold_adversary_progress() {
    for mix in Mix::ALL {
        let scenario = mix.scenario();
        let (report, ports) = run(&scenario, Plan::from_fuzz(&[]));
        assert_eq!(
            outcome(&report, ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 },
            "{}",
            mix.name()
        );
        assert_eq!(
            ports.node.view().frontiers.verified_best.height.0,
            scenario.blocks,
            "{}: the node committed every body",
            mix.name()
        );
    }
}

/// Drawn prefixes with drawn Plans hold the property too.
#[test]
fn drawn_mixes_hold_adversary_progress() {
    for input in byte_inputs(4, 4_096) {
        let mut prefix = vec![1];
        prefix.extend(input.iter().copied().take(PROGRESS_BYTES - 1));
        let scenario = BlockProgress::draw(&prefix);
        let plan = Plan::from_fuzz(input.get(PROGRESS_BYTES - 1..).unwrap_or_default());
        let (report, _) = run(&scenario, plan);
        assert_eq!(
            outcome(&report, ADVERSARY_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 },
            "{scenario:?}"
        );
    }
}

/// The deadline counts one liveness turn per adversary and one for the
/// honest mirror.
///
/// This fails if the deadline stops counting turns.
#[test]
fn the_deadline_counts_one_turn_per_adversary() {
    let alone = deadline(&draw_mix(&[])).unwrap();
    let silent = deadline(&Mix::Silent.scenario()).unwrap();
    assert_eq!(silent, alone * 3);
    assert!(alone > Duration::from_secs(32), "{alone:?}");
}
