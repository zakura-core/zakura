//! hdr-9c.1a: `header-sync/commit-only-authorized` on `header-sync/storage`.
//! Each witness holds and applies, with the hdr-6 controls holding or not
//! applying beside it. `cargo xtask planted header-sync/commit-only-authorized`
//! checks that its bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command};

use support::{byte_inputs, inputs};
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED, RESET_INVALIDATES_WORK, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Fork, Policy,
        },
        header_sync::{Dialer, MirrorSpec, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest, StorageResult},
    MemorySink, Observation, PlanInput, RunReport,
};
use zakura_storage::header::fixture::HeaderFixture;

/// A mirror holding `chain`, which dials the node.
fn mirror(chain: Chain) -> MirrorSpec {
    MirrorSpec {
        chain,
        dialer: Dialer::Mirror,
        max_headers_per_response: None,
    }
}

/// The node at A2 with `mirror-0` at A6 and `mirror-1` on a fork of A3,
/// five headers long, so the mirrors' chains share their first header past
/// the node's tip. The node takes the heavier fork first, and once its
/// first apply completes `mirror-0` grows its main chain by four headers,
/// past the fork, so the node takes the main chain from A3 too.
pub fn forks_sharing_a_header() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            mirror(Chain::main(6)),
            mirror(Chain {
                main: 3,
                fork: Some(Fork { branch: 1, len: 5 }),
            }),
        ],
        policy: Policy::converging(),
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Grow { len: 4 },
            when: Some(Trigger::Completed(Select::nth(OpKind::Apply, 1))),
        }],
        weights: Default::default(),
    }
}

/// The node at genesis syncing from `mirror-0` at A300, past one round's
/// fair share, so its round stages many pages and continues its
/// subscription with grants before it reaches the target. A subscription
/// resumed by a later round needs a round that stops at a bounded prefix,
/// which only a full 4,000-header body window or budget makes, so the
/// resumed path is the unit tests' (`round_life`).
pub fn multi_page_sync() -> StorageScenario {
    StorageScenario {
        node: Chain::default(),
        mirrors: vec![mirror(Chain::main(300))],
        policy: Policy::converging(),
        controls: Vec::new(),
        weights: Default::default(),
    }
}

/// `mirror-0`'s session is replaced while the node's first prepare is
/// held, until the node admits the second session.
pub fn relinked_mid_round() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![mirror(Chain::main(6))],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted("mirror-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Relink,
            when: Some(Trigger::Submitted(Select::nth(OpKind::Prepare, 1))),
        }],
        weights: Default::default(),
    }
}

/// Every witness, by name.
fn witnesses() -> [(&'static str, StorageScenario); 3] {
    [
        ("forks-sharing-a-header", forks_sharing_a_header()),
        ("multi-page-sync", multi_page_sync()),
        ("relinked-mid-round", relinked_mid_round()),
    ]
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure.
fn run(scenario: &StorageScenario, input: PlanInput) -> RunReport {
    let report = run_prepared::<Storage>(
        scenario,
        Storage::prepare(scenario),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    report
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

/// The node's `Started` facts, in order, as `(session, request, owner
/// target)`.
fn starts(report: &RunReport) -> Vec<(u64, u64, String)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                event: ReactorFact::Started { request, .. },
                ..
            } => Some((
                request.session_id,
                request.request_id,
                request.owner.scope.branch.target.clone(),
            )),
            _ => None,
        })
        .collect()
}

/// Each witness holds `header-sync/commit-only-authorized` and applies,
/// and the hdr-6 controls and `header-sync/reset-invalidates-work` hold or
/// don't apply beside it.
#[test]
fn every_witness_holds_and_applies() {
    for (name, scenario) in witnesses() {
        for input in inputs(8, 256) {
            let report = run(&scenario, input);
            assert!(
                matches!(
                    outcome(&report, COMMIT_ONLY_AUTHORIZED),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{name}: {:?}",
                outcome(&report, COMMIT_ONLY_AUTHORIZED)
            );
            for control in [
                CHUNK_BUDGET_BOUND,
                TEARDOWN_RETURNS_BASELINE,
                STALE_COMPLETION_INERT,
                RESET_INVALIDATES_WORK,
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

/// The targets the node applied, in order, by hash as the log writes it.
fn applied(report: &RunReport) -> Vec<String> {
    let mut targets = std::collections::HashMap::new();
    let mut applied = Vec::new();
    for entry in report.log.entries() {
        let Observation::Storage { fact, .. } = &entry.obs else {
            continue;
        };
        match fact {
            StorageFact::Submitted {
                request: StorageRequest::ApplyHeaderTarget { target, .. },
                id,
                ..
            } => {
                targets.insert(*id, target.clone());
            }
            StorageFact::Completed {
                id,
                result: StorageResult::Applied,
                ..
            } => applied.extend(targets.get(id).cloned()),
            _ => {}
        }
    }
    applied
}

/// `hash` as the log writes it.
fn logged(hash: zakura_chain::block::Hash) -> String {
    hash.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Each witness takes the path it names: the node applies the fork's tip,
/// then the grown main chain's, and ends on the main chain; the multi-page
/// sync reaches A300 in one round; and a replaced session's round starts
/// on the new session.
#[test]
fn each_witness_takes_the_path_it_names() {
    let fixture = HeaderFixture::regtest();
    let fork_tip = logged(
        Chain {
            main: 3,
            fork: Some(Fork { branch: 1, len: 5 }),
        }
        .tip(&fixture)
        .hash,
    );
    let main_tip = Chain::main(10).tip(&fixture);
    for input in inputs(8, 256) {
        let scenario = forks_sharing_a_header();
        let ports = Storage::prepare(&scenario);
        let node = ports.node.clone();
        let report = run_prepared::<Storage>(
            &scenario,
            ports,
            input.clone(),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        let applied = applied(&report);
        let fork_at = applied.iter().position(|target| *target == fork_tip);
        let main_at = applied
            .iter()
            .position(|target| *target == logged(main_tip.hash));
        assert!(
            matches!((fork_at, main_at), (Some(fork), Some(main)) if fork < main),
            "{applied:?}"
        );
        assert_eq!(node.engine().snapshot().frontiers.header_best, main_tip);

        let scenario = multi_page_sync();
        let ports = Storage::prepare(&scenario);
        let node = ports.node.clone();
        let report = run_prepared::<Storage>(
            &scenario,
            ports,
            input.clone(),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert_eq!(node.engine().snapshot().frontiers.header_best.height.0, 300);
        assert_eq!(starts(&report).len(), 1, "one round takes all 300");

        let started = starts(&run(&relinked_mid_round(), input));
        assert!(
            started.iter().any(|(session, _, _)| *session == 2),
            "{started:?}"
        );
    }
}

/// The witnesses `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-authority-<name>.json` with its Plan bytes
/// beside it in `header-authority-<name>.plan`.
fn committed_witnesses() -> [(&'static str, StorageScenario); 3] {
    witnesses()
}

/// The Plan bytes every committed witness runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture `header-authority-<name>.<extension>`.
fn fixture(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-authority-{name}.{extension}"))
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
            "{name}: run `regenerate_authority_witnesses`"
        );
        let plan =
            fs::read(fixture(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_authority_witnesses`"
        );
        let report = run(&scenario, zakura_sim::Plan::from_fuzz(&plan));
        assert!(
            matches!(
                outcome(&report, COMMIT_ONLY_AUTHORIZED),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed witnesses from their builders.
#[test]
#[ignore = "rewrites the committed authority witnesses"]
fn regenerate_authority_witnesses() {
    for (name, scenario) in committed_witnesses() {
        let json = serde_json::to_string_pretty(&scenario).expect("scenarios serialize");
        fs::write(fixture(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture(name, "plan"), committed_plan()).expect("the fixture directory exists");
    }
}

/// Each witness's log and the property's report depend only on the
/// scenario and the plan: plan A twice, then B, then A again, gives A's
/// every time, and B's log differs.
#[test]
fn every_authority_witness_repeats_a_a_and_a_b_a() {
    for (name, scenario) in witnesses() {
        let plans = [committed_plan(), vec![7; 256]];
        let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
            .into_iter()
            .map(|plan| {
                let report = run(&scenario, zakura_sim::Plan::from_fuzz(plan));
                (
                    format!("{:?}", report.log.entries()),
                    format!("{:?}", outcome(&report, COMMIT_ONLY_AUTHORIZED)),
                )
            })
            .collect();
        assert_eq!(runs[0], runs[1], "{name}: plan A, then A");
        assert_eq!(runs[0], runs[3], "{name}: plan A, then B, then A");
        assert_ne!(runs[0].0, runs[2].0, "{name}: plan B's log differs");
    }
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
            COMMIT_ONLY_AUTHORIZED,
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
