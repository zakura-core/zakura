//! hdr-8: `header-sync/honest-progress` on `header-sync/wire`'s progress
//! fixture. The node at 50 is dialed by one honest peer holding 51 headers,
//! on 0 to 10 ms links with no loss, with storage succeeding within 10 ms.
//! Once the node reads the peer's first `Status`, it must adopt the 51st
//! header within 30 virtual seconds, against production timers.
//!
//! The witness holds on every Plan here, well inside the bound. A peer that
//! leaves before the apply makes the property inconclusive, not violated,
//! and outside the fixture the property never applies. The bypass, an
//! apply the reactor skips, is the planted case
//! `qa/planted/header-sync/honest-progress/skipped-apply`.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command, time::Duration};

use support::inputs;
use tempfile::TempDir;
use zakura_sim::{
    domain::{
        header_sync_progress::{HONEST_PROGRESS, PROGRESS_BOUND},
        session::HONEST_NEVER_PUNISHED,
    },
    harness::{
        self,
        header_node::{
            policy::{Allow, Hold, Select, Trigger},
            Chain, Fork, Policy,
        },
        header_sync::{progress_fixture, HeaderWire, WirePorts, WireScenario},
        record_witness, run_prepared,
        wire_plans::{progress_scenario, SavedPlan},
        LinkEntry, LinkSpec, RecordInput, Subsystem,
    },
    observe::{StorageFact, StorageResult, TransportFact},
    peer::{
        honest::{CreditSpec, GrantPolicy, SubscribeSpec},
        PeerAction, PeerSpec, NODE_ADDR,
    },
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The environment variable that tells the fresh-process replay test it
/// runs as the child, and where the recorded trace is.
const CHILD: &str = "HEADER_WIRE_PROGRESS_REPLAY_CHILD";

/// The committed progress input, `tests/fixtures/header-wire-honest-progress.bin`:
/// the saved Plan's tag, then its Plan bytes.
fn committed() -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!(
            "header-wire-{}.bin",
            SavedPlan::HonestProgress.name()
        ));
    fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The committed input's Plan, after its two-byte scenario tag.
fn committed_plan() -> PlanInput {
    let input = committed();
    assert_eq!(
        HeaderWire::draw(&input[..HeaderWire::SCENARIO_BYTES]),
        progress_scenario(),
        "the committed input picks the progress fixture"
    );
    Plan::from_fuzz(&input[HeaderWire::SCENARIO_BYTES..])
}

/// The committed Plan, the default Plan and four byte-chosen ones.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![committed_plan(), Plan::from_fuzz(&[])];
    plans.extend(inputs(4, 16_384));
    plans
}

/// Runs `scenario` under `plan`. The run must end without a harness
/// failure or a violation that stopped it.
fn run(scenario: &WireScenario, plan: PlanInput) -> (RunReport, WirePorts) {
    let ports = HeaderWire::prepare(scenario);
    let report = run_prepared::<HeaderWire>(
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

/// Every property `report` has violated.
fn violated(report: &RunReport) -> Vec<&str> {
    report
        .properties
        .iter()
        .filter(|property| !property.violations.is_empty())
        .map(|property| property.property.as_str())
        .collect()
}

/// The virtual time from the node's first `FrameRead` to the first apply
/// storage completed as `Applied`, from the log's `TimeAdvanced` facts, or
/// `None` without both. The first read is at or before the honest `Status`
/// read that triggers the property, so this bounds the property's elapsed
/// time from above.
fn read_to_applied(report: &RunReport) -> Option<Duration> {
    let mut now = 0;
    let mut first_read = None;
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = *to_ns,
            Observation::Transport {
                event: TransportFact::FrameRead { .. },
                ..
            } => {
                first_read.get_or_insert(now);
            }
            Observation::Storage {
                fact:
                    StorageFact::Completed {
                        result: StorageResult::Applied,
                        ..
                    },
                ..
            } => return first_read.map(|read| Duration::from_nanos(now - read)),
            _ => {}
        }
    }
    None
}

/// On the committed input, the witness holds `header-sync/honest-progress`
/// with its trigger history: the node read the honest peer's `Status`, and
/// no property is violated. On every Plan here, the property holds at its
/// one trigger, the node's tip reaches the successor, the honest peer is
/// never punished, and the apply lands well inside the 30 s bound, within
/// one virtual second of the node's first read.
///
/// This fails if the reactor never adopts the successor, as the planted
/// skipped apply shows, or adopts it only after the bound.
#[test]
fn the_progress_fixture_holds_honest_progress() {
    check_witness::<HeaderWire>(
        WitnessScenario {
            scenario: progress_scenario(),
            input: committed_plan(),
        },
        HONEST_PROGRESS,
        Expect::Holds,
    );
    let scenario = progress_scenario();
    for plan in plans() {
        let (report, ports) = run(&scenario, plan);
        assert_eq!(
            outcome(&report, HONEST_PROGRESS),
            &PropertyOutcome::Held { applicable: 1 }
        );
        assert!(
            matches!(
                outcome(&report, HONEST_NEVER_PUNISHED),
                PropertyOutcome::Held { .. }
            ),
            "{:?}",
            outcome(&report, HONEST_NEVER_PUNISHED)
        );
        assert_eq!(violated(&report), Vec::<&str>::new());
        assert_eq!(ports.selected_tip(), Chain::main(51).tip(&ports.fixture));
        let elapsed = read_to_applied(&report).expect("the node read a frame and applied");
        assert!(
            elapsed <= Duration::from_secs(1) && elapsed < PROGRESS_BOUND,
            "the successor applied {elapsed:?} after the node's first read"
        );
    }
}

/// The honest peer disconnects after the node's honest session started
/// and before the successor applies. `StableMembership` stops holding while
/// the obligation is open, so the property ends `Inconclusive` with
/// `assumption-lost`, not `Violated`, and no property is violated. The
/// node's tip stays at 50, so the apply never landed.
///
/// This fails if a lost membership reads as a violation, or the obligation
/// outlives the assumption it rests on.
#[test]
fn a_peer_that_leaves_before_the_apply_makes_honest_progress_inconclusive() {
    let mut scenario = progress_scenario();
    scenario.peers[0].script = vec![PeerAction::Disconnect];
    assert!(progress_fixture(&scenario));
    let (report, ports) = run(&scenario, Plan::from_fuzz(&[]));
    assert_eq!(
        outcome(&report, HONEST_PROGRESS),
        &PropertyOutcome::Inconclusive {
            reason: "assumption-lost".to_owned()
        }
    );
    assert_eq!(violated(&report), Vec::<&str>::new());
    assert_eq!(ports.selected_tip(), Chain::main(50).tip(&ports.fixture));
    assert_eq!(read_to_applied(&report), None, "nothing applied");
}

/// A named change to a scenario.
type Change = (&'static str, fn(&mut WireScenario));

/// Only the progress fixture declares the property's assumptions. Each
/// change to it below leaves the fixture, so the property can't apply
/// there. The honest sync, which holds 70 headers past the node, reports
/// it `NotApplicable`.
///
/// This fails if the property applies to a scenario whose delivery,
/// storage or membership its bound doesn't cover.
#[test]
fn honest_progress_applies_only_in_its_fixture() {
    let fixture = progress_scenario();
    assert!(progress_fixture(&fixture));
    let mut closing = fixture.clone();
    closing.peers[0].script = vec![PeerAction::Close];
    assert!(progress_fixture(&closing));
    let changes: [Change; 10] = [
        ("a second peer", |s| s.peers.push(s.peers[0].clone())),
        ("a fork", |s| s.node.fork = Some(Fork { branch: 1, len: 1 })),
        ("two headers past the node", |s| {
            s.peers[0].honest.as_mut().expect("honest").chain = 52;
        }),
        ("a peer the node dials", |s| {
            s.peers[0] = PeerSpec {
                honest: s.peers[0].honest.clone(),
                ..PeerSpec::listening()
            };
        }),
        ("a subscription of its own", |s| {
            s.peers[0].honest.as_mut().expect("honest").subscribe = Some(SubscribeSpec {
                start: 50,
                open: CreditSpec {
                    objects: 1,
                    bytes: 263,
                },
                grants: GrantPolicy::Immediate,
            });
        }),
        ("a grant in the script", |s| {
            s.peers[0].script = vec![PeerAction::Grant];
        }),
        ("a reordering link", |s| s.net.default_link.reorder = true),
        ("a lossy link", |s| {
            s.net.links.push(LinkEntry {
                src: NODE_ADDR,
                dst: NODE_ADDR,
                model: LinkSpec {
                    loss_budget: 1,
                    ..s.net.default_link
                },
            });
        }),
        ("the converging storage bound", |s| {
            s.policy = Policy::converging();
        }),
        ("a storage hold", |s| {
            s.policy.holds.push(Hold {
                select: Select::default(),
                until: Trigger::VirtualTime(1),
                then: vec![Allow::Engine],
                deadline_ms: None,
            });
        }),
    ];
    for (name, change) in changes {
        let mut scenario = fixture.clone();
        change(&mut scenario);
        assert!(!progress_fixture(&scenario), "{name}");
    }
    let honest = SavedPlan::HonestSync.scenario();
    assert!(!progress_fixture(&honest));
    let (report, _) = run(&honest, Plan::from_fuzz(&[]));
    assert_eq!(
        outcome(&report, HONEST_PROGRESS),
        &PropertyOutcome::NotApplicable
    );
}

/// A run's evidence depends only on its scenario and Plan: Plan A twice,
/// with Plan B between, gives A's log, property reports and session facts
/// both times. B's log differs, so the comparison can fail.
#[test]
fn the_progress_witness_repeats_a_a_and_a_b_a() {
    let scenario = progress_scenario();
    let mut evidence = Vec::new();
    for plan in [
        committed_plan(),
        inputs(1, 16_384).remove(0),
        committed_plan(),
    ] {
        let (report, ports) = run(&scenario, plan);
        evidence.push((report.log, report.properties, ports.sessions.facts()));
    }
    assert_eq!(evidence[0].0.entries(), evidence[2].0.entries());
    assert_eq!(evidence[0].1, evidence[2].1);
    assert_eq!(evidence[0].2, evidence[2].2);
    assert_ne!(
        evidence[0].0.entries(),
        evidence[1].0.entries(),
        "plan B differs"
    );
}

/// The committed input, recorded as a witness test that expects a hold,
/// then replayed exactly in a fresh process of the same test binary,
/// gives the same end record and holds the property there too.
#[test]
fn the_progress_witness_replays_in_a_fresh_process() {
    let entry = harness::find(HeaderWire::NAME).expect("the wire harness is registered");
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(std::path::Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws, observations or end
        // record errs.
        let replay = replay_with(&trace, entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        let progress = replay
            .properties
            .iter()
            .find(|report| report.property == HONEST_PROGRESS)
            .map(|report| &report.outcome);
        assert_eq!(progress, Some(&PropertyOutcome::Held { applicable: 1 }));
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    let input = committed();
    let (_, judged) = record_witness(
        entry,
        RecordInput::Input(&input),
        dir.path(),
        HONEST_PROGRESS,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
    judged.unwrap_or_else(|error| panic!("{error}"));
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_progress_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    assert!(child.status.success(), "{child:?}");
}
