//! hdr-8: `header-sync/honest-progress` on `header-sync/wire`'s progress
//! fixture. The node at 50 is dialed by one honest peer holding 51 headers,
//! on 0 to 10 ms links with no loss, with storage succeeding within 10 ms.
//! Once the node reads the peer's first `Status`, it must adopt the 51st
//! header within 30 virtual seconds, against production timers.
//!
//! The witness holds on every Plan here, well inside the bound. A peer that
//! leaves before the apply withdraws its obligation, so it's not violated,
//! and outside the fixture the property never applies. The bypass, an
//! apply the reactor skips, is the planted case
//! `qa/planted/header-sync/honest-progress/skipped-apply`.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use serde_json::Value;
use support::inputs;
use tempfile::TempDir;
use zakura_sim::NaReason;
use zakura_sim::{
    domain::{
        header_sync_progress::{HONEST_PROGRESS, PROGRESS_BOUND},
        session::HONEST_NEVER_PUNISHED,
    },
    harness::{
        self,
        header_node::{
            adoption::{AdoptionFact, Stamped},
            policy::{Allow, Hold, Select, Trigger},
            Chain, Fork, Policy,
        },
        header_sync::{progress_fixture, HeaderWire, WirePorts, WireScenario},
        record, run_prepared,
        wire_plans::{progress_scenario, SavedPlan},
        HarnessEntry, HarnessError, LinkEntry, LinkSpec, RecordInput, Subsystem,
    },
    observe::{Entry, StorageFact, StorageResult, TransportFact},
    peer::{
        honest::{CreditSpec, GrantPolicy, SubscribeSpec},
        PeerAction, PeerSpec, NODE_ADDR,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::{check_witness, Expect, WitnessScenario},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, PropertyReport, RunReport,
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

/// The virtual time of the node's first `FrameRead`, from the log's
/// `TimeAdvanced` facts. It's at or before the honest `Status` read that
/// triggers the property.
fn first_read(report: &RunReport) -> Option<Duration> {
    let mut now = 0;
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = *to_ns,
            Observation::Transport {
                event: TransportFact::FrameRead { .. },
                ..
            } => return Some(Duration::from_nanos(now)),
            _ => {}
        }
    }
    None
}

/// The node's adoption facts in `ports`, in the order its reactor reported
/// them, each with its virtual time.
fn adoption(ports: &WirePorts) -> Vec<Stamped> {
    ports
        .adoption
        .borrow()
        .as_ref()
        .expect("the run built the node")
        .facts()
}

/// When the reactor adopted a snapshot whose selected tip is at `height`,
/// if it did.
fn adopted_at(facts: &[Stamped], height: u32) -> Option<Duration> {
    facts.iter().find_map(|stamped| match &stamped.fact {
        AdoptionFact::Adopted(adoption) if adoption.header_best.height.0 == height => {
            Some(stamped.at)
        }
        _ => None,
    })
}

/// Whether storage completed any apply as `Applied`.
fn applied(report: &RunReport) -> bool {
    report.log.entries().iter().any(|entry| {
        matches!(
            entry.obs,
            Observation::Storage {
                fact: StorageFact::Completed {
                    result: StorageResult::Applied,
                    ..
                },
                ..
            }
        )
    })
}

/// On the committed input, the witness holds `header-sync/honest-progress`
/// with its trigger history: the node read the honest peer's `Status`, and
/// no property is violated. On every Plan here, the property holds at its
/// one trigger, the node's tip reaches the successor, the honest peer is
/// never punished, and the reactor adopts the successor well inside the
/// 30 s bound, within one virtual second of the node's first read.
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
        let read = first_read(&report).expect("the node read a frame");
        let adopted = adopted_at(&adoption(&ports), 51).expect("the reactor adopted 51");
        let elapsed = adopted
            .checked_sub(read)
            .expect("the adoption follows the read");
        assert!(
            elapsed <= Duration::from_secs(1) && elapsed < PROGRESS_BOUND,
            "the reactor adopted the successor {elapsed:?} after the node's first read"
        );
    }
}

/// The progress fixture with an honest peer that disconnects once it has
/// served its page. Under the default Plan, it leaves before the apply.
fn leaving_scenario() -> WireScenario {
    let mut scenario = progress_scenario();
    scenario.peers[0].script = vec![PeerAction::Disconnect];
    scenario
}

/// The honest peer disconnects after the node's honest session started
/// and before the successor applies. `StableMembership` stops holding while
/// the obligation is open, which withdraws it, so the property isn't
/// `Violated`, and no property is violated. The evaluation that opened it
/// applied, so the lost assumption isn't a reason (#1840 rule 4), and the
/// property holds. The node's tip stays at 50, so the apply never landed.
///
/// This fails if a lost membership reads as a violation, or the obligation
/// outlives the assumption it rests on.
#[test]
fn a_peer_that_leaves_before_the_apply_withdraws_honest_progress() {
    let scenario = leaving_scenario();
    assert!(progress_fixture(&scenario));
    let (report, ports) = run(&scenario, Plan::from_fuzz(&[]));
    assert_eq!(
        outcome(&report, HONEST_PROGRESS),
        &PropertyOutcome::Held { applicable: 1 }
    );
    assert_eq!(violated(&report), Vec::<&str>::new());
    assert_eq!(ports.selected_tip(), Chain::main(50).tip(&ports.fixture));
    assert!(!applied(&report), "nothing applied");
    assert_eq!(adopted_at(&adoption(&ports), 51), None);
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
        &PropertyOutcome::not_applicable(NaReason::NeverApplied)
    );
}

/// The view-side evidence of a run in `ports`: the node's session facts,
/// which `StableMembership` reads, and its adoption facts with their times,
/// which discharge the property.
fn view_evidence(ports: &WirePorts) -> String {
    format!("{:?}\n{:?}\n", ports.sessions.facts(), adoption(ports))
}

/// Everything a run of `scenario` under `plan` leaves: every trace record
/// it writes, `init`, each step and `end` with its cleanup, as JSON lines,
/// its observation log, its property reports with every violation, and its
/// view-side evidence ([`view_evidence`]).
fn run_evidence(
    scenario: &WireScenario,
    plan: PlanInput,
) -> (Vec<String>, Vec<Entry>, Vec<PropertyReport>, String) {
    let ports = HeaderWire::prepare(scenario);
    let mut sink = MemorySink::default();
    let report = run_prepared::<HeaderWire>(scenario, ports.clone(), plan, &mut sink, None)
        .unwrap_or_else(|error| panic!("{error}"));
    (
        sink.lines(),
        report.log.entries().to_vec(),
        report.properties,
        view_evidence(&ports),
    )
}

/// A run's evidence depends only on its scenario and Plan
/// ([`run_evidence`]). Each witness, the progress fixture's and the leaving
/// peer's, gives the same evidence twice in a row, and again after the
/// other witness ran. The two witnesses' traces differ, so the comparison
/// can fail.
#[test]
fn the_progress_witnesses_repeat_a_a_and_a_b_a() {
    let progress = || run_evidence(&progress_scenario(), committed_plan());
    let leaving = || run_evidence(&leaving_scenario(), Plan::from_fuzz(&[]));
    let evidence = [
        progress(),
        progress(),
        leaving(),
        leaving(),
        progress(),
        leaving(),
    ];
    assert!(evidence[0].3.contains("Height(51)"), "{}", evidence[0].3);
    assert_eq!(evidence[0], evidence[1], "the progress witness, A/A");
    assert_eq!(evidence[2], evidence[3], "the leaving peer's, A/A");
    assert_eq!(evidence[0], evidence[4], "the progress witness, A/B/A");
    assert_eq!(evidence[2], evidence[5], "the leaving peer's, A/B/A");
    assert_ne!(evidence[0].0, evidence[2].0, "the witnesses differ");
}

thread_local! {
    /// The ports of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<WirePorts>> = const { RefCell::new(None) };
}

/// `header-sync/wire`'s own run, keeping its ports in [`CAPTURED`] so the
/// test can read the run's view-side evidence after it.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: WireScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = HeaderWire::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.clone()));
    run_prepared::<HeaderWire>(&scenario, ports, input, sink, None)
}

/// The view-side evidence of the run [`capture_run`] last kept.
fn captured() -> String {
    let ports = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    view_evidence(&ports)
}

/// Each witness, recorded, then replayed exactly in a fresh process of the
/// same test binary, gives the view-side evidence and end record its
/// recorded run gave, and its property outcome there too: the committed
/// input holds the property, and so does the leaving peer, whose lost
/// assumption withdrew the obligation.
#[test]
fn the_progress_witnesses_replay_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(HeaderWire::NAME).expect("the wire harness is registered")
    };
    let expected = [
        PropertyOutcome::Held { applicable: 1 },
        PropertyOutcome::Held { applicable: 1 },
    ];
    if let Some(dir) = std::env::var_os(CHILD) {
        for (index, expected) in expected.iter().enumerate() {
            let trace = TraceReader::open(&Path::new(&dir).join(index.to_string()))
                .expect("the trace was recorded");
            // A replay that leaves the trace's picks, draws, observations or
            // end record errs.
            let replay = replay_with(&trace, &entry, ReplayOptions::default())
                .unwrap_or_else(|error| panic!("{error}"));
            let progress = replay
                .properties
                .iter()
                .find(|report| report.property == HONEST_PROGRESS)
                .map(|report| &report.outcome);
            assert_eq!(progress, Some(expected));
            // The parent reads the evidence from the child's stdout.
            writeln!(
                std::io::stdout(),
                "EVIDENCE-BEGIN\n{}EVIDENCE-END",
                captured()
            )
            .expect("the test harness captures stdout");
        }
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    let input = committed();
    let leaving = serde_json::to_value(leaving_scenario()).expect("a scenario serializes");
    let witnesses = [
        RecordInput::Input(&input),
        RecordInput::Plan {
            scenario: &leaving,
            plan: &[],
        },
    ];
    let mut recorded = Vec::new();
    for (index, witness) in witnesses.into_iter().enumerate() {
        record(&entry, witness, &dir.path().join(index.to_string())).expect("the witness records");
        recorded.push(captured());
    }
    assert!(recorded[0].contains("Height(51)"), "{}", recorded[0]);
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_progress_witnesses_replay_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed: Vec<&str> = stdout
        .split("EVIDENCE-BEGIN\n")
        .skip(1)
        .filter_map(|rest| rest.split_once("EVIDENCE-END"))
        .map(|(evidence, _)| evidence)
        .collect();
    assert_eq!(replayed, recorded, "{stdout}");
}
