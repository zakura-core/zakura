//! hdr-4's reservations and page staging: the work queue's header capacity
//! accounting on `header-sync/storage` and `header-sync/wire`. The reactor
//! tap folds it in its `HeaderWorkLedger` on every run, so each run here also
//! checks the accounting contract: one live reservation per peer, opened =
//! staged + released, owned = staged − returned, every page staged on its own
//! round's reservation, every call holding its round's own pages for the
//! same owner, and a released round's reservation and unheld leases gone. These witnesses show the runs reach the cases that contract
//! names.
//!
//! The fork witness steered so that both mirrors' rounds stage before the
//! node's storage runs either preparation is committed as
//! `tests/fixtures/header-authority-overlapping-forks`, the scenario and
//! the script, which `foreign-leases` under
//! `qa/planted/header-sync/commit-only-authorized/` runs on
//! `header-sync/storage`. There the second mirror's prepare takes its round's
//! leases while the first round's are still staged.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use support::{
    inputs,
    steer::{before, steer, Steer},
};
use tempfile::TempDir;
use zakura_network::zakura::observe::{ReactorEvent, ReservationEnd};
use zakura_sim::{
    domain::header_sync::COMMIT_ONLY_AUTHORIZED,
    harness::{
        self,
        header_node::Policy,
        header_node::{
            policy::Trigger, reservation::ReservationEvidence, session::SessionFact, Chain,
            ControlAction, ControlStep, Fork,
        },
        header_sync::{Dialer, HeaderWire, MirrorSpec, Storage, StorageScenario, WireScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem, WITNESS_FILE,
    },
    observe::{
        ReactorCall, ReactorFact, StorageFact, StorageRequest, TransitionFact, TransportFact,
    },
    peer::{honest::HonestSpec, PeerSpec},
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::{check_witness, Expect, WitnessScenario},
    Checker, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport, StepCtx,
};

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure. Returns its report and the node's
/// accounting facts.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Vec<ReactorEvent>) {
    let ports = Storage::prepare(scenario);
    let reservations = ports.reservations.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    (report, reservations.facts())
}

/// `action` on `peer`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    }
}

/// A mirror holding `chain` that dials the node.
fn mirror(chain: Chain) -> MirrorSpec {
    MirrorSpec {
        chain,
        dialer: Dialer::Mirror,
        max_headers_per_response: None,
    }
}

/// The reservation-life witness: `mirror-0`, at A5, has its link closed
/// once the node reserves headers for it, so its round retires with headers
/// still reserved, then relinks once the close is made.
pub fn life_witness() -> StorageScenario {
    StorageScenario {
        controls: vec![
            step(
                "mirror-0",
                ControlAction::Close,
                Trigger::ReservationOpened("mirror-0".to_owned()),
            ),
            step(
                "mirror-0",
                ControlAction::Relink,
                Trigger::Closed("mirror-0".to_owned()),
            ),
        ],
        ..StorageScenario::default()
    }
}

/// The continuation witness: `mirror-0` at A300, past the 250-header fair
/// share, so its round takes a second reservation from a `Grant`.
pub fn continuation_witness() -> StorageScenario {
    StorageScenario {
        mirrors: vec![mirror(Chain::main(300))],
        ..StorageScenario::default()
    }
}

/// The fork witness: `mirror-0` and `mirror-1` both at height 5, on forks
/// of A1 from fixture branches 1 and 2, so the node works two rounds whose
/// pages differ at the same heights.
pub fn fork_witness() -> StorageScenario {
    let fork = |branch| Chain {
        main: 1,
        fork: Some(Fork { branch, len: 4 }),
    };
    StorageScenario {
        mirrors: vec![mirror(fork(1)), mirror(fork(2))],
        policy: Policy::converging(),
        ..StorageScenario::default()
    }
}

/// Each round's reservations, by incarnation: those a continuation opened
/// for it, then those bound to it, in order.
fn round_reservations(facts: &[ReactorEvent]) -> BTreeMap<u64, Vec<u64>> {
    let mut rounds: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for fact in facts {
        let (reservation, incarnation) = match fact {
            ReactorEvent::ReservationOpened {
                reservation,
                request: Some(request),
                ..
            }
            | ReactorEvent::ReservationBound {
                reservation,
                request,
                ..
            } => (*reservation, request.incarnation),
            _ => continue,
        };
        rounds.entry(incarnation).or_default().push(reservation);
    }
    rounds
}

/// The reservations that ended as `cause` still holding headers.
fn ended_holding(facts: &[ReactorEvent], cause: ReservationEnd) -> BTreeSet<u64> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            ReactorEvent::ReservationEnded {
                reservation,
                released,
                cause: ended,
            } if *ended == cause && *released > 0 => Some(*reservation),
            _ => None,
        })
        .collect()
}

/// A round can retire while its reservation still holds headers: closing
/// `mirror-0`'s link right after the node reserves for it releases a
/// reservation bound to a started round, with headers left, under some
/// plans. The run passes, so the ledger found that reservation and the
/// round's unheld leases gone when the queue released the round.
#[test]
fn a_round_retires_with_headers_still_reserved() {
    let scenario = life_witness();
    let mut retired_reserved = 0;
    for input in inputs(16, 256) {
        let (_, facts) = run(&scenario, input);
        let bound: BTreeSet<u64> = round_reservations(&facts).into_values().flatten().collect();
        if ended_holding(&facts, ReservationEnd::Released)
            .intersection(&bound)
            .next()
            .is_some()
        {
            retired_reserved += 1;
        }
    }
    assert!(
        retired_reserved > 0,
        "no plan retired a round with headers still reserved"
    );
}

/// A round longer than one fair share continues on a second reservation,
/// which a `Grant` opens naming the running round, and pages are staged on
/// both of that round's reservations.
#[test]
fn a_continuation_opens_a_second_reservation_for_its_round() {
    let scenario = continuation_witness();
    for input in inputs(4, 256) {
        let (_, facts) = run(&scenario, input);
        let continued = facts.iter().find_map(|fact| match fact {
            ReactorEvent::ReservationOpened {
                reservation,
                request: Some(request),
                ..
            } => Some((*reservation, request.incarnation)),
            _ => None,
        });
        let (continuation, round) = continued.expect("the round continues");
        let reservations = &round_reservations(&facts)[&round];
        assert_eq!(reservations.len(), 2, "{reservations:?}");
        assert_eq!(reservations[1], continuation, "{reservations:?}");
        for reservation in reservations {
            assert!(
                facts.iter().any(|fact| matches!(
                    fact,
                    ReactorEvent::PageStaged { reservation: spent, request, .. }
                        if spent == reservation && request.incarnation == round
                )),
                "reservation {reservation} stages a page: {facts:#?}"
            );
        }
    }
}

/// The peers whose prepares the node dispatched in `report`.
fn prepared_peers(report: &RunReport) -> BTreeSet<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Reactor {
                event:
                    ReactorFact::Dispatched {
                        port: ReactorCall::Prepare { peer, .. },
                        ..
                    },
                ..
            } => Some(peer.clone()),
            _ => None,
        })
        .collect()
}

/// Two mirrors on different forks at the same height: under some plans the
/// node stages pages from both and prepares both. The run passes, so every
/// prepare and apply held only its own round's pages and named the owner
/// they were staged for.
#[test]
fn two_forks_stage_and_prepare_their_own_pages() {
    let scenario = fork_witness();
    let mut both = 0;
    for input in inputs(16, 256) {
        let (report, facts) = run(&scenario, input);
        let staging: BTreeSet<_> = facts
            .iter()
            .filter_map(|fact| match fact {
                ReactorEvent::PageStaged { from_peer, .. } => Some(from_peer.digest()),
                _ => None,
            })
            .collect();
        if staging.len() == 2 && prepared_peers(&report).len() == 2 {
            both += 1;
        }
    }
    assert!(both > 0, "no plan staged and prepared both forks");
}

/// `header-sync/wire`'s honest sync: `peer-0` at A80 and `peer-1` at A120
/// both dial the node, at A50. Their tips differ, so the node can work both,
/// and its two header-sync streams number their frames over the same range.
fn wire_witness() -> WireScenario {
    let honest = |chain| PeerSpec {
        honest: Some(HonestSpec {
            chain,
            subscribe: None,
        }),
        ..PeerSpec::dialing()
    };
    WireScenario {
        node: Chain::main(50),
        peers: vec![honest(80), honest(120)],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

/// A transport stream: its connection and QUIC stream.
type StreamKey = (u64, u64);

/// On the wire, each staged page names the frame its transport reader
/// admitted it from, on its own session's stream: every `PageStaged` joins
/// exactly one `PageClaimed`, by the connection and QUIC stream its session
/// was admitted on and by frame number, with the same header count. Under
/// some plan both sessions stage pages and the two streams claim the same
/// frame number, so a join by frame number alone would be ambiguous.
#[test]
fn a_wire_page_joins_the_frame_its_reader_claimed() {
    let scenario = wire_witness();
    let mut overlapping = 0;
    for input in inputs(8, 16_384) {
        let ports = HeaderWire::prepare(&scenario);
        let (reservations, sessions) = (ports.reservations.clone(), ports.sessions.clone());
        let report =
            run_prepared::<HeaderWire>(&scenario, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let streams: BTreeMap<u64, StreamKey> = sessions
            .facts()
            .into_iter()
            .filter_map(|fact| match fact {
                SessionFact::SessionAdmitted {
                    session_id,
                    origin: Some(origin),
                    ..
                } => Some((
                    session_id,
                    (
                        origin.connection,
                        origin
                            .quic_stream
                            .expect("a wire session has a QUIC stream"),
                    ),
                )),
                _ => None,
            })
            .collect();
        let mut claimed: BTreeMap<(StreamKey, u64), u32> = BTreeMap::new();
        for entry in report.log.entries() {
            if let Observation::Transport {
                connection: Some(connection),
                event:
                    TransportFact::Regulated {
                        quic_stream,
                        transition:
                            TransitionFact::PageClaimed {
                                seq: Some(seq),
                                objects,
                                ..
                            },
                    },
                ..
            } = &entry.obs
            {
                let key = ((*connection, *quic_stream), *seq);
                assert!(
                    claimed.insert(key, *objects).is_none(),
                    "{key:?} claimed twice"
                );
            }
        }
        let mut joined = BTreeSet::new();
        for fact in reservations.facts() {
            let ReactorEvent::PageStaged {
                from_session,
                frame,
                headers,
                ..
            } = fact
            else {
                continue;
            };
            let stream = streams[&from_session];
            let frame = frame.expect("a transport reader admitted every page");
            assert_eq!(
                claimed.get(&(stream, frame)).copied().map(usize::try_from),
                Some(Ok(headers)),
                "session {from_session}'s frame {frame} was claimed on {stream:?} with the \
                 page's {headers} headers"
            );
            assert!(joined.insert((stream, frame)), "frame {frame} staged twice");
        }
        let frames_by_stream: BTreeMap<StreamKey, BTreeSet<u64>> =
            joined
                .iter()
                .fold(BTreeMap::new(), |mut by_stream, (stream, frame)| {
                    by_stream.entry(*stream).or_default().insert(*frame);
                    by_stream
                });
        let mut sets = frames_by_stream.values();
        if let (Some(first), Some(second)) = (sets.next(), sets.next()) {
            if first.intersection(second).next().is_some() {
                overlapping += 1;
            }
        }
    }
    assert!(
        overlapping > 0,
        "no plan staged pages from two streams with a shared frame number"
    );
}

/// The storage witnesses' accounting facts and full logs depend only on
/// scenario and plan: plan A twice in a row, then B, then A again, gives A's
/// every time, and B's log differs.
#[test]
fn the_reservation_witnesses_repeat_a_a_and_a_b_a() {
    let plans = inputs(3, 256);
    for scenario in [life_witness(), continuation_witness(), fork_witness()] {
        let runs: Vec<(String, String)> = [&plans[1], &plans[1], &plans[2], &plans[1]]
            .into_iter()
            .map(|input| {
                let (report, facts) = run(&scenario, input.clone());
                (format!("{:?}", report.log.entries()), format!("{facts:?}"))
            })
            .collect();
        assert_eq!(runs[0], runs[1], "plan A, then A");
        assert_eq!(runs[0], runs[3], "plan A, then B, then A");
        assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
    }
}

/// Set, to a directory of recorded traces, in the child process that
/// [`the_reservation_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_RESERVATION_TRACES";

thread_local! {
    /// The accounting facts of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<ReservationEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its accounting facts in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.reservations.clone()));
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// `header-sync/storage`'s registry entry, with its run replaced by
/// [`capture_run`].
fn capturing() -> HarnessEntry {
    HarnessEntry {
        run: capture_run,
        ..*harness::find(Storage::NAME).expect("the storage harness is registered")
    }
}

/// The accounting facts [`capture_run`] last kept, as text.
fn captured() -> String {
    let evidence = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    format!("{:?}\n", evidence.facts())
}

/// The storage witnesses, by the name their traces are recorded under.
fn witnesses() -> [(&'static str, StorageScenario); 3] {
    [
        ("life", life_witness()),
        ("continuation", continuation_witness()),
        ("fork", fork_witness()),
    ]
}

/// Each storage witness recorded under one plan in this process, then
/// replayed exactly in a fresh process of the same test binary, gives the
/// accounting facts its recorded run gave.
#[test]
fn the_reservation_witnesses_replay_in_a_fresh_process() {
    let entry = capturing();
    if let Some(dir) = std::env::var_os(CHILD) {
        let facts: String = witnesses()
            .into_iter()
            .map(|(name, _)| {
                let trace =
                    TraceReader::open(&Path::new(&dir).join(name)).expect("the trace was recorded");
                // A replay that leaves the trace's picks, draws or
                // observations, or fails where the recording didn't, errs.
                replay_with(&trace, &entry, ReplayOptions::default())
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
                captured()
            })
            .collect();
        // The parent reads the facts from the child's stdout.
        writeln!(std::io::stdout(), "FACTS-BEGIN\n{facts}FACTS-END")
            .expect("the test harness captures stdout");
        return;
    }
    let PlanInput::Bytes(plan) = inputs(1, 256).remove(0) else {
        unreachable!("fuzz inputs are bytes");
    };
    let dir = TempDir::new().expect("a temporary directory");
    let mut recorded = String::new();
    for (name, scenario) in witnesses() {
        let scenario = serde_json::to_value(&scenario).expect("a scenario serializes");
        let what = RecordInput::Plan {
            scenario: &scenario,
            plan: &plan,
        };
        record(&entry, what, &dir.path().join(name)).expect("the witness records");
        recorded.push_str(&captured());
    }
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_reservation_witnesses_replay_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("FACTS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("FACTS-END"))
        .map(|(facts, _)| facts)
        .unwrap_or_else(|| panic!("the child prints its facts: {stdout}"));
    assert_eq!(replayed, recorded);
}

/// The committed overlapping-forks scenario, as `header-sync/storage`'s
/// JSON.
const OVERLAPPING_FORKS: &str = "header-authority-overlapping-forks.json";

/// The committed overlapping-forks script, a scripted Plan as JSON.
const OVERLAPPING_FORKS_SCRIPT: &str = "header-authority-overlapping-forks.script.json";

/// The committed fixture file `name`.
fn fixture_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// The node's storage operations that are preparations before `cx`'s step,
/// by operation ID, each with the supplier it prepares for.
fn prepares_before(cx: &StepCtx<'_>) -> BTreeMap<u64, String> {
    before(cx)
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { source, .. },
                        ..
                    },
                ..
            } => Some((*id, source.clone())),
            _ => None,
        })
        .collect()
}

/// A steering rule: the node's storage runs no preparation until it has
/// been asked for one from each of two suppliers. Until then, a step that
/// would run one picks something else, time advancing only if nothing else
/// can. So each mirror's round stages its page and dispatches its prepare
/// while the other's leases are still staged.
fn both_prepares_before_either_runs(cx: &StepCtx<'_>) -> Result<Steer, String> {
    let prepares = prepares_before(cx);
    if prepares.values().collect::<BTreeSet<_>>().len() >= 2 {
        return Ok(Steer::Fine);
    }
    // A storage operation's identity is `storage:header:<id>:<outcome>`.
    let runs_prepare = |identity: &str| {
        identity
            .strip_prefix("storage:header:")
            .and_then(|rest| rest.split(':').next())
            .and_then(|id| id.parse::<u64>().ok())
            .is_some_and(|id| prepares.contains_key(&id))
    };
    if !runs_prepare(&cx.pick.to_string()) {
        return Ok(Steer::Fine);
    }
    let names: Vec<String> = cx
        .enabled
        .iter()
        .map(|enabled| enabled.identity.to_string())
        .collect();
    names
        .iter()
        .find(|name| !runs_prepare(name) && !name.starts_with("advance-time"))
        .or_else(|| names.iter().find(|name| !runs_prepare(name)))
        .cloned()
        .map(Steer::Pick)
        .ok_or_else(|| format!("only a held preparation can run: {names:?}"))
}

/// Runs `scenario` under `plan` on `header-sync/storage` itself, with
/// `extra` beside the registered properties, keeping a run that ended in a
/// harness failure. Returns the report and its records.
fn run_storage(
    scenario: &StorageScenario,
    plan: PlanInput,
    extra: Option<&mut dyn Checker>,
) -> (RunReport, Vec<String>) {
    let mut sink = MemorySink::default();
    let report =
        match run_prepared::<Storage>(scenario, Storage::prepare(scenario), plan, &mut sink, extra)
        {
            Ok(report) => report,
            Err(HarnessError::Run { report, .. }) => *report,
            Err(error) => panic!("{error}"),
        };
    (report, sink.lines())
}

/// The default Plan on `header-sync/storage`, steered by
/// [`both_prepares_before_either_runs`] over [`fork_witness`]: the script
/// `foreign-leases`'s planted case runs.
fn overlapping_forks_script() -> Plan {
    let scenario = fork_witness();
    let (_, plan, _) = steer(
        Plan::from_fuzz(&[]),
        &both_prepares_before_either_runs,
        &mut |plan, checker| run_storage(&scenario, plan, Some(checker)),
    );
    let PlanInput::Script(plan) = plan else {
        panic!(
            "the default Plan runs the first preparation before the second, so steering scripts it"
        );
    };
    plan
}

/// The committed overlapping-forks scenario and script, as `cargo xtask
/// planted` reads them.
fn committed_overlapping_forks() -> (StorageScenario, Plan) {
    let read = |name: &str| {
        fs::read_to_string(fixture_file(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
    };
    let scenario = serde_json::from_str(&read(OVERLAPPING_FORKS))
        .unwrap_or_else(|error| panic!("{OVERLAPPING_FORKS}: {error}"));
    let script = serde_json::from_str(&read(OVERLAPPING_FORKS_SCRIPT))
        .unwrap_or_else(|error| panic!("{OVERLAPPING_FORKS_SCRIPT}: {error}"));
    (scenario, script)
}

/// The committed overlapping-forks fixture is what its builders give: the
/// fork witness and the default Plan steered over it on
/// `header-sync/storage`.
#[test]
fn the_overlapping_forks_fixture_matches_its_builders() {
    let (scenario, script) = committed_overlapping_forks();
    assert_eq!(
        serde_json::to_value(&scenario).expect("scenarios serialize"),
        serde_json::to_value(fork_witness()).expect("scenarios serialize"),
        "run `regenerate_overlapping_forks_fixture`"
    );
    assert_eq!(
        script,
        overlapping_forks_script(),
        "run `regenerate_overlapping_forks_fixture`"
    );
}

/// Rewrites the committed overlapping-forks fixture from its builders.
#[test]
#[ignore = "rewrites the committed overlapping-forks fixture"]
fn regenerate_overlapping_forks_fixture() {
    let write = |name: &str, json: String| {
        fs::write(fixture_file(name), json + "\n").expect("the fixture directory exists");
    };
    write(
        OVERLAPPING_FORKS,
        serde_json::to_string_pretty(&fork_witness()).expect("scenarios serialize"),
    );
    write(
        OVERLAPPING_FORKS_SCRIPT,
        serde_json::to_string_pretty(&overlapping_forks_script()).expect("Plans serialize"),
    );
}

/// On `header-sync/storage` itself, the committed overlapping forks hold
/// `header-sync/commit-only-authorized`, which applies, with their
/// overlap: the node's storage is asked for both mirrors' preparations
/// before it runs either, and both rounds stage their pages before either
/// is released. So the second prepare took its round's leases while the
/// first round's were staged, and held only its own.
#[test]
fn the_overlapping_forks_fixture_prepares_both_while_both_are_staged() {
    let (scenario, script) = committed_overlapping_forks();
    check_witness::<Storage>(
        WitnessScenario {
            scenario: scenario.clone(),
            input: PlanInput::Script(script.clone()),
        },
        COMMIT_ONLY_AUTHORIZED,
        Expect::Holds,
    );
    let (report, facts) = run(&scenario, PlanInput::Script(script));
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == COMMIT_ONLY_AUTHORIZED)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { applicable }) if applicable > 0),
        "{outcome:?}"
    );
    let mut prepares = BTreeMap::new();
    let mut first_ran = None;
    for entry in report.log.entries() {
        let Observation::Storage { fact, .. } = &entry.obs else {
            continue;
        };
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::PrepareHeaderTarget { source, .. },
                ..
            } => {
                prepares.entry(source.clone()).or_insert(*id);
            }
            StorageFact::Completed { id, .. } if prepares.values().any(|first| first == id) => {
                first_ran.get_or_insert(prepares.len());
            }
            _ => {}
        }
    }
    assert_eq!(
        first_ran,
        Some(2),
        "both suppliers' preparations were asked for before the first ran: {prepares:?}"
    );
    let mut staged = BTreeSet::new();
    for fact in &facts {
        match fact {
            ReactorEvent::PageStaged { request, .. } => {
                staged.insert(request.incarnation);
            }
            ReactorEvent::RoundReleased { .. } => break,
            _ => {}
        }
    }
    assert_eq!(
        staged.len(),
        2,
        "both rounds staged before either was released"
    );
}

/// `record --witness` judges the committed overlapping forks as `cargo
/// xtask planted` runs them, from the scenario's text and `--script`,
/// expecting a hold, and `replay` reruns its trace Exact in a fresh
/// process.
#[test]
fn the_overlapping_forks_fixture_records_and_replays_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    let dir = TempDir::new().expect("a temporary directory");
    let trace = dir.path().join("trace");
    let scenario =
        fs::read_to_string(fixture_file(OVERLAPPING_FORKS)).expect("the fixture is committed");
    let recorded = zakura_sim(&[
        "record",
        "--sim",
        Storage::NAME,
        "--scenario",
        &scenario,
        "--script",
        fixture_file(OVERLAPPING_FORKS_SCRIPT)
            .to_str()
            .expect("the path is UTF-8"),
        "--out",
        trace.to_str().expect("the path is UTF-8"),
        "--witness",
        COMMIT_ONLY_AUTHORIZED,
        "--expect",
        "holds",
    ]);
    assert!(recorded.status.success(), "{recorded:?}");
    let witness: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(trace.join(WITNESS_FILE)).expect("witness.json"))
            .expect("witness.json is JSON");
    assert_eq!(
        witness["passed"],
        serde_json::Value::Bool(true),
        "{witness}"
    );
    let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
    let stdout = String::from_utf8_lossy(&replayed.stdout);
    assert!(
        replayed.status.success() && stdout.contains("every pick, draw and observation matched"),
        "{replayed:?}"
    );
}
