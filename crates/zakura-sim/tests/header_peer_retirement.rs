//! hdr-10b: `header-sync/peer-retirement-progress` on
//! `header-sync/liveness`'s departure fixtures and on
//! `header-sync/storage`. Each witness holds the property, with the hdr-6
//! and hdr-9 controls holding or not applying beside it. `cargo xtask
//! planted header-sync/peer-retirement-progress` checks that its bypasses
//! break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command};

use zakura_network::zakura::observe::HeaderTargetPhase;

use support::{byte_inputs, inputs};
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::NaReason;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED, HOLD_RELEASE_WAKES, PEER_RETIREMENT_PROGRESS,
        REQUEST_TIMEOUT, RESET_INVALIDATES_WORK, STALE_CHAIN_EFFECT_INERT, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            ends::{EndFact, EnvironmentAction},
            retirement::HandoffFact,
        },
        header_sync::{
            liveness::{Departs, LivenessScenario, Stage, LATE_PAGE_DELAY},
            Liveness, Storage, StorageScenario,
        },
        run_prepared, Subsystem,
    },
    MemorySink, Outcome, PlanInput, RunReport,
};

/// The regression controls every witness runs beside the property.
const CONTROLS: [&str; 7] = [
    CHUNK_BUDGET_BOUND,
    TEARDOWN_RETURNS_BASELINE,
    STALE_COMPLETION_INERT,
    RESET_INVALIDATES_WORK,
    STALE_CHAIN_EFFECT_INERT,
    COMMIT_ONLY_AUTHORIZED,
    HOLD_RELEASE_WAKES,
];

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// Runs departure `fixture` under `input`, which must end idle, after its
/// finish, never at its budget, and without a violation or harness
/// failure. Returns the report, the harness's end facts and the hand-off
/// facts.
fn run(fixture: LivenessScenario, input: PlanInput) -> (RunReport, Vec<EndFact>, Vec<HandoffFact>) {
    run_into(fixture, input, &mut MemorySink::default())
}

/// [`run`], recording into `sink`.
fn run_into(
    fixture: LivenessScenario,
    input: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> (RunReport, Vec<EndFact>, Vec<HandoffFact>) {
    let ports = Liveness::prepare(&fixture);
    let ends = ports.ends.clone();
    let handoffs = ports.retirement.clone();
    let report = run_prepared::<Liveness>(&fixture, ports, input, sink, None)
        .unwrap_or_else(|error| panic!("{fixture:?}: {error}"));
    assert!(
        matches!(report.end.outcome, Outcome::Idle { .. }),
        "{fixture:?}: {:?}",
        report.end
    );
    assert_eq!(report.end.violation, None, "{fixture:?}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{fixture:?}: {:?}", report.end);
    let ends = ends.facts().into_iter().map(|(_, fact)| fact).collect();
    let handoffs = handoffs.facts().into_iter().map(|(_, fact)| fact).collect();
    (report, ends, handoffs)
}

/// Each departure fixture holds `header-sync/peer-retirement-progress` and
/// applies under every plan. The harness closed or relinked `mirror-0`, as
/// the fixture says, after the fixture got ready, and its slot's going
/// opened one hand-off, which a locator read for the target from the
/// taker, `mirror-1`'s relinked session, answered. After a relink, the
/// reactor's cached pass offers the
/// target to `mirror-1` at the end of the replacement's own turn, before
/// `mirror-0`'s new session's first `Status` can arrive, so `mirror-1`
/// takes it there too. The controls hold or don't apply beside it, and
/// every run ends idle. Each fixture's slot goes from its named stage under
/// at least one plan: the receiving stage's departure is enabled once A's
/// request is reserved, which doesn't hold the round there, so a plan can
/// let it move on first.
#[test]
fn every_departure_fixture_holds() {
    for fixture in LivenessScenario::departures() {
        let LivenessScenario::Departure { by, stage } = fixture else {
            unreachable!("departures are departure fixtures");
        };
        let named = match stage {
            Stage::Awaiting => None,
            Stage::Receiving => Some(HeaderTargetPhase::Receiving),
            Stage::Preparing => Some(HeaderTargetPhase::Preparing),
            Stage::Applying => Some(HeaderTargetPhase::Applying),
        };
        let mut reached = 0;
        for input in inputs(6, 256) {
            let (report, ends, handoffs) = run(fixture, input);
            let action = match by {
                Departs::Close => EnvironmentAction::Close,
                Departs::Relink => EnvironmentAction::Relink,
            };
            assert!(
                ends.iter().any(|fact| matches!(fact,
                    EndFact::Environment { peer, action: acted, .. }
                        if peer == "mirror-0" && *acted == action)),
                "{fixture:?}: {ends:?}"
            );
            assert!(
                matches!(
                    outcome(&report, PEER_RETIREMENT_PROGRESS),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{fixture:?}: {:?}",
                outcome(&report, PEER_RETIREMENT_PROGRESS)
            );
            let ready: Vec<_> = handoffs
                .iter()
                .filter_map(|fact| match fact {
                    HandoffFact::Ready { taker, .. } => Some(taker.clone()),
                    _ => None,
                })
                .collect();
            let [taker] = &ready[..] else {
                panic!("{fixture:?}: the fixture gets ready once: {handoffs:?}");
            };
            let opened: Vec<_> = handoffs
                .iter()
                .enumerate()
                .filter_map(|(at, fact)| match fact {
                    HandoffFact::Opened {
                        target,
                        departed,
                        phase,
                        ..
                    } => {
                        reached += usize::from(*phase == named);
                        Some((at, *target, departed.clone()))
                    }
                    _ => None,
                })
                .collect();
            let [(at, target, departed)] = &opened[..] else {
                panic!("{fixture:?}: one hand-off opens: {handoffs:?}");
            };
            let answer = handoffs[at + 1..].iter().find_map(|fact| match fact {
                HandoffFact::Read {
                    target: read,
                    session,
                } if read == target && session != departed => Some(session.clone()),
                _ => None,
            });
            let answer = answer.unwrap_or_else(|| {
                panic!("{fixture:?}: another session reads the target: {handoffs:?}")
            });
            assert_eq!(answer, *taker, "{fixture:?}");
            for control in CONTROLS {
                assert!(
                    matches!(
                        outcome(&report, control),
                        PropertyOutcome::Held { .. }
                            | PropertyOutcome::NotApplicable {
                                reason: NaReason::NeverApplied
                            }
                    ),
                    "{fixture:?}: {control}: {:?}",
                    outcome(&report, control)
                );
            }
        }
        assert!(
            reached > 0,
            "{fixture:?}: no plan departs with the slot in its named stage"
        );
    }
}

/// Each timeout fixture holds the property and applies. Its first round,
/// `mirror-0`'s, armed its deadline only once the fixture was ready, each
/// arm obliging (at 20 virtual seconds or later with `late_apply`, its
/// apply's dispatch), and its slot went no sooner than
/// the request timeout after its last arm, which opened a hand-off that
/// `mirror-1`'s locator read answered. In the late-page fixture, the late
/// page's arm comes [`LATE_PAGE_DELAY`] after the start's, so the slot goes
/// past the start's deadline, which the run crossed without a violation.
/// Every run ends idle.
#[test]
fn every_timeout_fixture_holds_after_its_deadline() {
    let fixtures = [
        LivenessScenario::Timeout { late_apply: false },
        LivenessScenario::Timeout { late_apply: true },
        LivenessScenario::LatePage,
    ];
    for fixture in fixtures {
        for input in inputs(4, 256) {
            let (report, _, handoffs) = run(fixture, input);
            assert!(
                matches!(
                    outcome(&report, PEER_RETIREMENT_PROGRESS),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{fixture:?}: {:?}",
                outcome(&report, PEER_RETIREMENT_PROGRESS)
            );
            for control in CONTROLS {
                assert!(
                    matches!(
                        outcome(&report, control),
                        PropertyOutcome::Held { .. }
                            | PropertyOutcome::NotApplicable {
                                reason: NaReason::NeverApplied
                            }
                    ),
                    "{fixture:?}: {control}: {:?}",
                    outcome(&report, control)
                );
            }
            let ready = handoffs
                .iter()
                .position(|fact| matches!(fact, HandoffFact::Ready { .. }))
                .unwrap_or_else(|| panic!("{fixture:?}: the fixture gets ready: {handoffs:?}"));
            let first = handoffs
                .iter()
                .position(|fact| matches!(fact, HandoffFact::Armed { .. }))
                .expect("the first round arms its deadline");
            assert!(ready < first, "{fixture:?}: every arm follows readiness");
            assert!(
                handoffs
                    .iter()
                    .all(|fact| !matches!(fact, HandoffFact::Armed { obliges: false, .. })),
                "{fixture:?}: every arm obliges: {handoffs:?}"
            );
            let (first, first_arm) = match &handoffs[first] {
                HandoffFact::Armed {
                    round, session, at, ..
                } => ((*round, session.clone()), at.since_start()),
                _ => unreachable!("found as an arm"),
            };
            let last_arm = handoffs
                .iter()
                .filter_map(|fact| match fact {
                    HandoffFact::Armed { round, at, .. } if *round == first.0 => {
                        Some(at.since_start())
                    }
                    _ => None,
                })
                .max()
                .expect("the first round arms its deadline");
            match fixture {
                LivenessScenario::Timeout { late_apply: true } => assert!(
                    last_arm >= std::time::Duration::from_secs(20),
                    "{fixture:?}: the late apply rearms: {last_arm:?}"
                ),
                LivenessScenario::LatePage => assert!(
                    last_arm >= first_arm + LATE_PAGE_DELAY,
                    "{fixture:?}: the late page rearms: {first_arm:?}, then {last_arm:?}"
                ),
                _ => {}
            }
            let (at, opened) = handoffs
                .iter()
                .enumerate()
                .find_map(|(index, fact)| match fact {
                    HandoffFact::Opened { departed, at, .. } if *departed == first.1 => {
                        Some((at.since_start(), index))
                    }
                    _ => None,
                })
                .unwrap_or_else(|| {
                    panic!("{fixture:?}: the first round's slot goes: {handoffs:?}")
                });
            assert!(
                at >= last_arm + REQUEST_TIMEOUT,
                "{fixture:?}: its slot went at {at:?}, before its deadline"
            );
            assert!(
                handoffs[opened + 1..].iter().any(|fact| matches!(fact,
                    HandoffFact::Read { session, .. } if *session != first.1)),
                "{fixture:?}: another session reads the target"
            );
        }
    }
}

/// The model's request timeout is the one the harness's nodes start with,
/// whose defaults the harness doesn't change.
#[test]
fn the_request_timeout_is_the_nodes() {
    use zakura_network::zakura::{
        FullStateFrontiers, HeaderSyncStartup, ZakuraHeaderSyncConfig, LOCAL_MAX_MESSAGE_BYTES,
    };
    let fixture = zakura_storage::header::fixture::HeaderFixture::regtest();
    let anchor = fixture.anchor();
    let startup = HeaderSyncStartup::new(
        fixture.network().clone(),
        (anchor.height, anchor.hash),
        FullStateFrontiers {
            finalized_height: anchor.height,
            verified_block_tip: anchor.height,
            verified_block_hash: anchor.hash,
        },
        Some((anchor.height, anchor.hash)),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
        zakura_network::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
    );
    assert_eq!(startup.request_timeout, REQUEST_TIMEOUT);
}

/// `header-sync/storage` registers the property too, so its built-in
/// close-and-relink scenario holds it and applies under some plan.
#[test]
fn the_storage_harness_holds_it_across_relinks() {
    let scenario = StorageScenario::close_relink();
    let mut applied = 0;
    for input in inputs(8, 256) {
        let report = run_prepared::<Storage>(
            &scenario,
            Storage::prepare(&scenario),
            input,
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        match outcome(&report, PEER_RETIREMENT_PROGRESS) {
            PropertyOutcome::Held { applicable } if *applicable > 0 => applied += 1,
            PropertyOutcome::NotApplicable {
                reason: NaReason::NeverApplied,
            } => {}
            other => panic!("{other:?}"),
        }
    }
    assert!(applied > 0, "no plan departed a session with a slot");
}

/// The property's report and the full log depend only on the fixture and
/// the plan: the preparing replacement under plan A twice, then B, then A
/// again, gives A's every time, and B's log differs.
#[test]
fn the_departure_fixture_repeats_a_a_and_a_b_a() {
    let fixture = LivenessScenario::Departure {
        stage: Stage::Preparing,
        by: Departs::Relink,
    };
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, _, _) = run(fixture, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}", outcome(&report, PEER_RETIREMENT_PROGRESS)),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// The fixtures `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-retire-<name>.json` with its Plan bytes beside
/// it in `header-retire-<name>.plan`.
fn committed_fixtures() -> [(&'static str, LivenessScenario); 8] {
    [
        ("timeout", LivenessScenario::Timeout { late_apply: false }),
        ("late-page", LivenessScenario::LatePage),
        (
            "awaiting-close",
            LivenessScenario::Departure {
                stage: Stage::Awaiting,
                by: Departs::Close,
            },
        ),
        (
            "preparing-close",
            LivenessScenario::Departure {
                stage: Stage::Preparing,
                by: Departs::Close,
            },
        ),
        (
            "receiving-close",
            LivenessScenario::Departure {
                stage: Stage::Receiving,
                by: Departs::Close,
            },
        ),
        (
            "receiving-relink",
            LivenessScenario::Departure {
                stage: Stage::Receiving,
                by: Departs::Relink,
            },
        ),
        (
            "preparing-relink",
            LivenessScenario::Departure {
                stage: Stage::Preparing,
                by: Departs::Relink,
            },
        ),
        (
            "applying-relink",
            LivenessScenario::Departure {
                stage: Stage::Applying,
                by: Departs::Relink,
            },
        ),
    ]
}

/// The Plan bytes every committed fixture runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture file `header-retire-<name>.<extension>`.
fn fixture_file(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-retire-{name}.{extension}"))
}

/// Each committed fixture is its named fixture, as JSON, with the fixed
/// Plan bytes, and holds and applies under them.
#[test]
fn the_committed_fixtures_match_their_names() {
    for (name, fixture) in committed_fixtures() {
        let committed: LivenessScenario = serde_json::from_str(
            &fs::read_to_string(fixture_file(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            committed, fixture,
            "{name}: run `regenerate_retirement_fixtures`"
        );
        let plan =
            fs::read(fixture_file(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_retirement_fixtures`"
        );
        // Each run is a manifest history, which the acceptance tool
        // records from this declaration (C18).
        let (report, _, _) = run_declared(
            Liveness::NAME,
            &format!("header-retire-{name}"),
            &fixture,
            zakura_sim::Plan::from_fuzz(&plan),
            |fixture, input, sink| run_into(*fixture, input, sink),
        );
        assert!(
            matches!(
                outcome(&report, PEER_RETIREMENT_PROGRESS),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
    }
}

/// Rewrites the committed fixtures from their names.
#[test]
#[ignore = "rewrites the committed retirement fixtures"]
fn regenerate_retirement_fixtures() {
    for (name, fixture) in committed_fixtures() {
        let json = serde_json::to_string_pretty(&fixture).expect("fixtures serialize");
        fs::write(fixture_file(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture_file(name, "plan"), committed_plan())
            .expect("the fixture directory exists");
    }
}

/// `record --witness` judges each committed fixture as `cargo xtask
/// planted` runs it, expecting a hold, the trace ends idle, and `replay`
/// reruns it Exact in a fresh process.
#[test]
fn the_committed_fixtures_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_fixtures() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario =
            fs::read_to_string(fixture_file(name, "json")).expect("the fixture is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Liveness::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture_file(name, "plan")
                .to_str()
                .expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            PEER_RETIREMENT_PROGRESS,
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
        let end: serde_json::Value = fs::read_to_string(trace.join("trace.jsonl"))
            .expect("the trace is written")
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|line| line.get("end").cloned())
            .expect("the trace ends");
        assert_eq!(end["outcome"], "idle", "{name}: {end}");
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
