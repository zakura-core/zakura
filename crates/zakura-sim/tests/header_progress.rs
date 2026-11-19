//! Step 10 for header sync: `header-sync/progress` runs the progress fixture
//! with k adversaries beside the honest peer, and
//! `header-sync/honest-progress` holds the node to the deadline
//! production's constants give for k.
//!
//! Each fixed role mix ([`Mix`]) is a prefix of the drawn input, and the
//! nightly sim runs them all here, with a few drawn inputs. The bypass, a
//! stall rule that never takes a stalled target back, is the planted case
//! `qa/planted/header-sync/honest-progress/stall-never-reassigned`.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use support::byte_inputs;
use zakura_sim::{
    domain::header_sync_progress::{HONEST_PROGRESS, PROGRESS_BOUND},
    harness::{
        header_node::{
            adoption::{AdoptionFact, Stamped},
            StartupFacts,
        },
        header_sync::{
            progress::{
                draw_mix, mix_prefix, Adversary, HeaderProgress, Mix, Role, MAX_ADVERSARIES,
                PROGRESS_BYTES,
            },
            progress_adversaries, progress_bound, WirePorts, WireScenario,
        },
        run_prepared, Subsystem,
    },
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The node's tip in every mix, and the honest peer's successor.
const NODE_TIP: u32 = 50;

/// Runs `scenario` under `plan`. The run must end without a harness
/// failure or a violation that stopped it.
fn run(scenario: &WireScenario, plan: PlanInput) -> (RunReport, WirePorts) {
    let ports = HeaderProgress::prepare(scenario);
    let report = run_prepared::<HeaderProgress>(
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

/// The node's adoption facts in `ports`, in the order its reactor reported
/// them.
fn adoption(ports: &WirePorts) -> Vec<Stamped> {
    ports
        .adoption
        .borrow()
        .as_ref()
        .expect("the run built the node")
        .facts()
}

/// When the reactor first adopted a snapshot whose selected tip is at
/// `height`, if it did.
fn adopted_at(ports: &WirePorts, height: u32) -> Option<Duration> {
    adoption(ports)
        .iter()
        .find_map(|stamped| match &stamped.fact {
            AdoptionFact::Adopted(adoption) if adoption.header_best.height.0 == height => {
                Some(stamped.at)
            }
            _ => None,
        })
}

/// The node's startup facts in `ports`.
fn startup(ports: &WirePorts) -> StartupFacts {
    ports
        .startup
        .borrow()
        .clone()
        .expect("the run built the node")
}

/// Each mix's prefix draws the progress fixture with its adversaries, and a
/// prefix's k bytes past [`MAX_ADVERSARIES`] wrap.
///
/// This fails if a mix leaves the fixture, so the property can't apply.
#[test]
fn every_mix_is_the_progress_fixture_with_its_adversaries() {
    for mix in Mix::ALL {
        let prefix = mix.prefix();
        assert_eq!(prefix.len(), PROGRESS_BYTES);
        assert!(HeaderProgress::accepts(&prefix));
        let scenario = mix.scenario();
        assert_eq!(
            progress_adversaries(&scenario),
            Some(usize::from(prefix[1])),
            "{}",
            mix.name()
        );
        assert_eq!(scenario.honest_peers().into_iter().collect::<Vec<_>>(), [0]);
    }
    let wrapped = draw_mix(&[u8::try_from(MAX_ADVERSARIES + 2).unwrap(), 0, 0, 0, 0]);
    assert_eq!(progress_adversaries(&wrapped), Some(1));
    let honest = draw_mix(&[]);
    assert_eq!(progress_adversaries(&honest), Some(0));
    let silent = mix_prefix(0, &[Adversary::new(Role::Silent, 0, 0)]);
    assert_eq!(HeaderProgress::draw(&silent).peers.len(), 2);
}

/// On each fixed mix, and a few drawn inputs, the node adopts the honest
/// successor, and `header-sync/honest-progress` holds at its one trigger.
/// Every mix's adversaries dial before the honest peer, so their offers
/// wait ahead of its own.
///
/// This fails if an adversary holds the honest target past its stall-rule
/// turn, or the node takes turns out of order, so the successor misses the
/// deadline.
#[test]
fn the_role_mixes_hold_honest_progress() {
    for mix in Mix::ALL {
        let scenario = mix.scenario();
        let (report, ports) = run(&scenario, Plan::from_fuzz(&[]));
        assert_eq!(
            outcome(&report, HONEST_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 },
            "{}",
            mix.name()
        );
        assert_eq!(
            ports.selected_tip().height.0,
            NODE_TIP + 1,
            "{}: the node reached the honest tip",
            mix.name()
        );
        // The honest peer dials 2 s in, and reads the node's frames within
        // one more second.
        let bound = progress_bound(&startup(&ports), scenario.peers.len() - 1)
            .expect("production's constants bound header progress");
        let adopted = adopted_at(&ports, NODE_TIP + 1).expect("the reactor adopted the successor");
        assert!(
            adopted <= bound + Duration::from_secs(3),
            "{}: adopted at {adopted:?}, past {bound:?}",
            mix.name()
        );
    }
}

/// Drawn prefixes with drawn Plans hold the property too, wherever it
/// applies.
#[test]
fn drawn_mixes_hold_honest_progress() {
    for input in byte_inputs(4, 4_096) {
        let mut prefix = vec![1];
        prefix.extend(input.iter().copied().take(PROGRESS_BYTES - 1));
        let scenario = HeaderProgress::draw(&prefix);
        let plan = Plan::from_fuzz(input.get(PROGRESS_BYTES - 1..).unwrap_or_default());
        let (report, _) = run(&scenario, plan);
        assert!(
            matches!(
                outcome(&report, HONEST_PROGRESS),
                PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable { .. }
            ),
            "{scenario:?}: {:?}",
            outcome(&report, HONEST_PROGRESS)
        );
    }
}

/// Production's constants give one 30 s stall-rule turn, plus a cached-pass
/// refill and [`PROGRESS_TURN_SLACK`], per adversary and one for the honest
/// peer. Alone, the honest peer keeps properties.md's 30 s.
///
/// This fails if the deadline stops counting turns or loses its slack.
#[test]
fn the_deadline_counts_one_turn_per_adversary() {
    let (_, ports) = run(&draw_mix(&[]), Plan::from_fuzz(&[]));
    let startup = startup(&ports);
    assert_eq!(progress_bound(&startup, 0), Ok(PROGRESS_BOUND));
    let one = progress_bound(&startup, 1).unwrap();
    assert!(
        one > Duration::from_secs(62) && one < Duration::from_secs(63),
        "{one:?}"
    );
    assert_eq!(progress_bound(&startup, 3), Ok(one * 2));
}
