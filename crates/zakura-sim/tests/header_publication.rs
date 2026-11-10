//! hdr-12b: `header-sync/state-publication-consistent`'s safety rules on
//! `header-sync/storage`. Each witness holds and applies, with the hdr-6
//! controls holding or not applying beside it. `cargo xtask planted
//! header-sync/state-publication-consistent` checks that its bypasses break
//! it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, path::PathBuf, process::Command, rc::Rc};

use support::{byte_inputs, inputs};
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, STALE_COMPLETION_INERT, STATE_PUBLICATION_CONSISTENT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            adoption::AdoptionFact,
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Policy, Reset,
        },
        header_sync::{
            storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports,
            Storage, StorageScenario, StorageView,
        },
        run_prepared, Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    MemorySink, ObservationLog, PlanInput, RunConfig, RunEnd, RunReport, Sim,
};

/// Where [`Viewed`] leaves its storage harness for the test to read.
type Slot = Rc<RefCell<Option<Rc<Storage>>>>;

/// `header-sync/storage`, also left in a slot, so a test can read its view
/// once the run ends.
struct Viewed(Rc<Storage>);

impl HasView for Viewed {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The storage harness's stamped facts.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Viewed {
    const NAME: &'static str = "test/viewed-storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = (Ports, Slot);

    /// The storage harness's config.
    fn config(_: &StorageScenario) -> RunConfig {
        storage_config()
    }

    /// The storage harness's ports.
    fn prepare(scenario: &StorageScenario) -> (Ports, Slot) {
        (Storage::prepare(scenario), Slot::default())
    }

    /// The storage harness, also left in the slot.
    fn build(sim: &mut Sim, scenario: &StorageScenario, (ports, slot): (Ports, Slot)) -> Self {
        let storage = Rc::new(Storage::build(sim, scenario, ports));
        *slot.borrow_mut() = Some(storage.clone());
        Viewed(storage)
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: the scenarios are built by hand.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("the scenarios are built by hand")
    }
}

/// A mirror holding `chain`, which `dialer` links.
fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec { chain, dialer }
}

/// `peer`'s control step `action`, offered once `when` holds, or at once.
fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// The node and `mirror-0` both at A3, with no other traffic: the node's
/// only adoption is its startup one, of the engine as preloaded.
pub fn startup_only() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(3), Dialer::Mirror)],
        policy: Policy::converging(),
        controls: Vec::new(),
        weights: Default::default(),
    }
}

/// [`startup_only`] with an invalidation of the node's tip and then its
/// reconsideration: a quiet reset, committed with nothing to fetch.
pub fn quiet_reset() -> StorageScenario {
    StorageScenario {
        controls: vec![
            step(
                "node",
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                None,
            ),
            step(
                "node",
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Reset("node".to_owned())),
            ),
        ],
        ..startup_only()
    }
}

/// [`startup_only`] with only an invalidation of the node's tip, offered
/// once the node admits the mirror's session, after the first `Status`: the
/// reset moves the tip below where the mirror advertises, the reactor adopts
/// it alone, and the session's next `Status`, once the 1 s publication
/// floor passes, advertises it.
pub fn invalidate_only() -> StorageScenario {
    StorageScenario {
        controls: vec![step(
            "node",
            ControlAction::Reset(Reset::Invalidate { depth: 0 }),
            Some(Trigger::SessionAdmitted("mirror-0".to_owned())),
        )],
        ..startup_only()
    }
}

/// hdr-4's grow witness: the node at A3, `mirror-0` at A3 dialing in, and
/// the node dialing `mirror-1` at A9. The node's first locator read is held
/// while `mirror-0` grows to A5 and the node applies the growth, so a
/// target the adoption made obsolete settles late, and the node then syncs
/// A9 under a third generation.
pub fn grow() -> StorageScenario {
    let locator = || Select::nth(OpKind::Locator, 1);
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            mirror(Chain::main(3), Dialer::Mirror),
            mirror(Chain::main(9), Dialer::Node),
        ],
        policy: Policy {
            holds: vec![Hold {
                select: locator(),
                until: Trigger::Completed(Select::nth(OpKind::Apply, 1)),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![step(
            "mirror-0",
            ControlAction::Grow { len: 2 },
            Some(Trigger::Submitted(locator())),
        )],
        weights: Default::default(),
    }
}

/// The node and `mirror-0` both at A3, with an invalidation offered once the
/// node admits the mirror's session, inside the 1 s publication floor of
/// the status it queued then, and a reconsideration after it.
pub fn reset_inside_status_floor() -> StorageScenario {
    StorageScenario {
        controls: vec![
            step(
                "node",
                ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                Some(Trigger::SessionAdmitted("mirror-0".to_owned())),
            ),
            step(
                "node",
                ControlAction::Reset(Reset::Reconsider),
                Some(Trigger::Reset("node".to_owned())),
            ),
        ],
        ..startup_only()
    }
}

/// The node and `mirror-0` both at A4, and a harness commit of two more
/// main-line headers straight into the node's engine, which the port
/// publishes and the reactor adopts like an apply's.
pub fn harness_commit() -> StorageScenario {
    StorageScenario {
        node: Chain::main(4),
        mirrors: vec![mirror(Chain::main(4), Dialer::Mirror)],
        controls: vec![step("node", ControlAction::Commit { len: 2 }, None)],
        ..startup_only()
    }
}

/// The node at genesis syncing six headers from `mirror-0`.
pub fn sync() -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![mirror(Chain::main(6), Dialer::Mirror)],
        ..startup_only()
    }
}

/// Every witness, by name.
fn witnesses() -> [(&'static str, StorageScenario); 7] {
    [
        ("startup-only", startup_only()),
        ("quiet-reset", quiet_reset()),
        ("invalidate-only", invalidate_only()),
        ("grow", grow()),
        ("reset-inside-status-floor", reset_inside_status_floor()),
        ("harness-commit", harness_commit()),
        ("sync", sync()),
    ]
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure, returning the report and the view after it.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, StorageView) {
    let prepared = Viewed::prepare(scenario);
    let slot = prepared.1.clone();
    let report =
        run_prepared::<Viewed>(scenario, prepared, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let storage = slot.borrow_mut().take().expect("the harness was built");
    (report, storage.view())
}

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// Each witness holds `header-sync/state-publication-consistent` and
/// applies, and the hdr-6 controls hold or don't apply beside it.
#[test]
fn every_witness_holds_and_applies() {
    for (name, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let (report, _) = run(&scenario, input);
            assert!(
                matches!(
                    outcome(&report, STATE_PUBLICATION_CONSISTENT),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{name}: {:?}",
                outcome(&report, STATE_PUBLICATION_CONSISTENT)
            );
            for control in [
                CHUNK_BUDGET_BOUND,
                TEARDOWN_RETURNS_BASELINE,
                STALE_COMPLETION_INERT,
            ] {
                assert!(
                    matches!(
                        outcome(&report, control),
                        PropertyOutcome::Held { .. } | PropertyOutcome::NotApplicable
                    ),
                    "{name}: {control}: {:?}",
                    outcome(&report, control)
                );
            }
        }
    }
}

/// In the invalidate-only witness the session's `Status` after the reset,
/// queued once the floor passes, advertises the reset's tip.
#[test]
fn the_reset_is_advertised() {
    let scenario = invalidate_only();
    let fixture = zakura_storage::header::fixture::HeaderFixture::regtest();
    let reset_tip = Chain::main(2).tip(&fixture);
    for input in inputs(8, 256) {
        let (_, view) = run(&scenario, input);
        let advertised = view.adoption.facts().into_iter().any(|stamped| {
            matches!(
                stamped.fact,
                AdoptionFact::StatusQueued { status, .. }
                    if status.selected_tip_hash == reset_tip.hash
            )
        });
        assert!(advertised, "the reset's tip was advertised");
    }
}

/// With no other traffic, the reactor adopts once, the engine as preloaded:
/// its startup adoption matches the seeded record, and nothing else is
/// committed or published.
#[test]
fn a_quiet_startup_adopts_only_the_seeded_record() {
    let scenario = startup_only();
    for input in inputs(8, 256) {
        let seed = Storage::prepare(&scenario).node.engine().snapshot();
        let (report, view) = run(&scenario, input);
        let adoptions: Vec<_> = view
            .adoption
            .facts()
            .into_iter()
            .filter_map(|stamped| match stamped.fact {
                AdoptionFact::Adopted(adoption) => Some(adoption),
                _ => None,
            })
            .collect();
        assert_eq!(adoptions.len(), 1, "{adoptions:?}");
        assert_eq!(adoptions[0].state_version, seed.state_version);
        assert_eq!(adoptions[0].header_best, seed.frontiers.header_best);
        assert!(matches!(
            outcome(&report, STATE_PUBLICATION_CONSISTENT),
            PropertyOutcome::Held { .. }
        ));
    }
}

/// The witnesses `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-publication-<name>.json` with its Plan bytes
/// beside it in `header-publication-<name>.plan`.
fn committed_witnesses() -> [(&'static str, StorageScenario); 1] {
    [("invalidate-only", invalidate_only())]
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-publication-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-publication-{name}.{extension}"))
}

/// Each committed witness is its builder's scenario, as JSON, with the
/// fixed Plan bytes, and holds and applies under them.
#[test]
fn the_committed_witnesses_match_their_builders() {
    for (name, scenario) in committed_witnesses() {
        let committed: StorageScenario = serde_json::from_str(
            &fs::read_to_string(fixture(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            serde_json::to_value(&committed).expect("scenarios serialize"),
            serde_json::to_value(&scenario).expect("scenarios serialize"),
            "{name}: run `regenerate_publication_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_publication_witnesses`"
        );
        let (report, _) = run(&scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert!(
            matches!(
                outcome(&report, STATE_PUBLICATION_CONSISTENT),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed witnesses from their builders.
#[test]
#[ignore = "rewrites the committed publication witnesses"]
fn regenerate_publication_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// The invalidate-only witness's log and the property's report depend only
/// on the scenario and the plan: plan A twice, then B, then A again, gives
/// A's every time, and B's log differs.
#[test]
fn the_publication_witness_repeats_a_a_and_a_b_a() {
    let scenario = invalidate_only();
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, _) = run(&scenario, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}", outcome(&report, STATE_PUBLICATION_CONSISTENT)),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// `record --witness` judges each committed witness as `cargo xtask
/// planted` runs it, expecting a hold, and `replay` reruns the trace Exact
/// in a fresh process.
#[test]
fn the_committed_witnesses_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_witnesses() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario = fs::read_to_string(fixture(name, "json")).expect("the witness is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Storage::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture(name, "plan").to_str().expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            STATE_PUBLICATION_CONSISTENT,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{name}: {witness}"
        );
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
