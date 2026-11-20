//! disc-2.3: `discovery/wire`'s saved Plans. Each Plan's input runs clean
//! and shows the history its scenario exists for: a
//! completed exchange, a timed-out one, verification on `discovery_verify`
//! tasks, a dial of a static candidate, and a late answer (disc-3.4). A
//! run repeats under its Plan
//! alone, with another run between, and replays Exact in a fresh process
//! through `zakura-sim replay`. A history a test needs that never happened fails
//! it: a missing observation is a harness failure, never evidence.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    path::Path,
    pin::pin,
    process::{Command, Output},
    task::{Context, Poll, Waker},
    time::Duration,
};

use tempfile::TempDir;
use zakura_network::zakura::{
    ZakuraDiscoveryHandle, DISCOVERY_INITIAL_EXCHANGE_TIMEOUT, ZAKURA_STREAM_DISCOVERY,
};
use zakura_quic::NodeId;
use zakura_sim::{
    domain::discovery::{is_source, late_answer, LateAnswer, LATE_RESPONSE_ADMITTED},
    harness::{
        discovery_service::{DiscoveryWire, NodeHandle, SavedPlan, WireScenario, RELAYED},
        run_prepared, HarnessError, Subsystem,
    },
    observe::TransportFact,
    peer::{discovery::relayed_node, peer_addr},
    MemorySink, Observation, Outcome, Plan, PropertyOutcome, RunReport,
};

/// Runs `scenario` under the Plan `plan`, and returns the report, even of a
/// run that ended in a harness failure, with its records and the node's
/// handle.
fn run(scenario: &WireScenario, plan: &[u8]) -> (RunReport, Vec<String>, NodeHandle) {
    let node = DiscoveryWire::prepare(scenario);
    let mut sink = MemorySink::default();
    let ran = run_prepared::<DiscoveryWire>(
        scenario,
        node.clone(),
        Plan::from_fuzz(plan),
        &mut sink,
        None,
    );
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, sink.lines(), node)
}

/// Saved Plan `plan`'s full input: its tag, then the Plan. Only the late
/// response needs Plan bytes, which a steered search builds
/// ([`support::discovery_inputs::late_response_input`]).
fn input(plan: SavedPlan) -> Vec<u8> {
    match plan {
        SavedPlan::LateResponse => support::discovery_inputs::late_response_input(),
        plan => plan.tag().to_vec(),
    }
}

/// Runs saved Plan `plan`'s input as `zakura-sim record --input` does: its
/// prefix picks the scenario, and the rest is the Plan.
fn replay(plan: SavedPlan) -> (RunReport, Vec<String>, NodeHandle) {
    let input = input(plan);
    let (prefix, plan) = input.split_at(DiscoveryWire::SCENARIO_BYTES);
    run(&DiscoveryWire::draw(prefix), plan)
}

/// A run's end record: its last line.
fn end(lines: &[String]) -> &str {
    lines.last().expect("a run writes its end record")
}

/// The virtual time of each log entry of `report`.
fn times(report: &RunReport) -> Vec<Duration> {
    let mut at = Duration::ZERO;
    report
        .log
        .entries()
        .iter()
        .map(|entry| {
            if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
                at = Duration::from_nanos(to_ns);
            }
            at
        })
        .collect()
}

/// The causes the node closed connections with, by connection, in log
/// order.
fn closes(report: &RunReport) -> Vec<(u64, String)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ConnectionClosed { connection, cause } => {
                Some((*connection, cause.clone()))
            }
            _ => None,
        })
        .collect()
}

/// How long after the node admitted its first discovery session the node
/// closed that session's connection, and how many timers fired between the
/// two.
fn admission_to_close(report: &RunReport) -> (Duration, usize) {
    let entries = report.log.entries();
    let times = times(report);
    let (admitted, connection) = entries
        .iter()
        .enumerate()
        .find_map(|(at, entry)| match &entry.obs {
            Observation::Session {
                connection, kind, ..
            } if *kind == ZAKURA_STREAM_DISCOVERY => Some((at, *connection)),
            _ => None,
        })
        .expect("the node admitted a discovery session");
    let closed = entries
        .iter()
        .position(|entry| {
            matches!(&entry.obs,
                Observation::ConnectionClosed { connection: closed, .. } if *closed == connection)
        })
        .expect("the node closed the session's connection");
    let fired = entries[admitted..closed]
        .iter()
        .map(|entry| match entry.obs {
            Observation::TimeAdvanced { fired, .. } => fired,
            _ => 0,
        })
        .sum();
    (times[closed] - times[admitted], fired)
}

/// The rows of the peers' discovery `PeerFrame`s on `connection`, with
/// their labels' names, in log order.
fn peer_frames(report: &RunReport, connection: u64) -> Vec<(&str, &'static str)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection: on,
                stream,
                row,
                label,
                ..
            } if *on == connection && stream.kind == ZAKURA_STREAM_DISCOVERY => {
                Some((row.as_deref().unwrap_or_default(), label.name()))
            }
            _ => None,
        })
        .collect()
}

/// How long after the initial exchange timeout the timed-out session's
/// connection may close: the close waits for scheduled tasks to run.
const TIMEOUT_SLACK: Duration = Duration::from_millis(100);

/// The three answers of an exchange, each conformant.
const EXCHANGE: [(&str, &str); 3] = [
    ("hello", "conformant"),
    ("peers", "conformant"),
    ("services", "conformant"),
];

/// The label a task was spawned with: `discovery_verify` for
/// `wire-node.1/discovery_verify.3`.
fn label(task: &str) -> &str {
    let last = task.rsplit('/').next().unwrap_or(task);
    last.split('.').next().unwrap_or(last)
}

/// The `discovery_verify` tasks the run spawned.
fn verifications(report: &RunReport) -> usize {
    report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(&entry.obs,
                Observation::TaskSpawned { task } if label(task) == "discovery_verify")
        })
        .count()
}

/// The book's record for `node`, read once the run has ended. Every task has
/// stopped by then, so the book's lock is free and the read finishes in one
/// poll.
fn book_record(handle: &ZakuraDiscoveryHandle, node: NodeId) -> bool {
    let read = pin!(handle.record_for(node));
    match read.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(record) => record.is_some(),
        Poll::Pending => panic!("the book's lock is free after the run"),
    }
}

/// The connections the node registered from peer-0's listening address.
fn registered_from_peer_0(report: &RunReport) -> Vec<u64> {
    let remote = peer_addr(0, 0).to_string();
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                remote: from,
                event: TransportFact::Registered,
                ..
            } if *from == remote => Some(*connection),
            _ => None,
        })
        .collect()
}

/// The log index where the node's discovery source ended its initial wait
/// on the wait's timer: the source's first entry after it reached its
/// `sleep` point at or after the session's admission plus
/// [`DISCOVERY_INITIAL_EXCHANGE_TIMEOUT`]. The wait is biased toward a
/// complete exchange, which the source checks before the timer in the
/// same poll, so a run from that point takes the timeout.
fn initial_wait_timed_out(report: &RunReport) -> Option<usize> {
    let entries = report.log.entries();
    let times = times(report);
    let admitted = entries.iter().position(|entry| {
        matches!(entry.obs, Observation::Session { kind, .. } if kind == ZAKURA_STREAM_DISCOVERY)
    })?;
    let slept = (admitted..entries.len()).find(|&at| {
        matches!(&entries[at].obs, Observation::TaskReached { task, at: point, .. }
            if is_source(task) && point.starts_with("sleep"))
            && times[at] - times[admitted] >= DISCOVERY_INITIAL_EXCHANGE_TIMEOUT
    })?;
    (slept + 1..entries.len()).find(|&at| {
        entries[at].step > entries[slept].step && entries[at].obs.ran_task().is_some_and(is_source)
    })
}

/// The first late answer in `report`'s log ([`late_answer`]), and its log
/// index.
fn first_late_answer(report: &RunReport) -> Option<(usize, LateAnswer)> {
    let entries = report.log.entries();
    (0..entries.len()).find_map(|at| {
        late_answer(entries, at)
            .expect("every discovery header read has its peer frame")
            .map(|late| (at, late))
    })
}

/// The properties each discovery session applies once on the node's
/// requests: one `Hello`, one `GetPeers` and one `GetServices`.
const SENDERS: [&str; 5] = [
    "discovery/s4v2/hello/sender-cadence",
    "discovery/s4v2/get-peers/sender-within-limit",
    "discovery/s4v2/get-peers/sender-cadence",
    "discovery/s4v2/get-services/sender-within-limit",
    "discovery/s4v2/get-services/sender-cadence",
];

/// The discovery sessions the node admitted.
fn sessions(report: &RunReport) -> u32 {
    let admitted = report
        .log
        .entries()
        .iter()
        .filter(|entry| {
            matches!(entry.obs, Observation::Session { kind, .. } if kind == ZAKURA_STREAM_DISCOVERY)
        })
        .count();
    u32::try_from(admitted).expect("a run admits few sessions")
}

/// Asserts that saved Plan `plan`'s run ran to its budget with no task
/// panicked or leaked, and applied exactly the properties its history
/// shows: each of [`SENDERS`] once per session,
/// `discovery/s4v2/conformant-never-closes` once per conformant answer
/// (three per session, two in `timed-out`, whose peer holds its `Hello`),
/// and, in `late-response`, [`LATE_RESPONSE_ADMITTED`] once. Each held, and
/// every other property had nothing to judge.
fn assert_applied(plan: SavedPlan, report: &RunReport) {
    let name = plan.name();
    assert_eq!(report.end.outcome, Outcome::Budget, "{name}");
    assert_eq!(
        (report.cleanup.panicked, report.cleanup.leaked),
        (0, 0),
        "{name}: {:?}",
        report.cleanup
    );
    let sessions = sessions(report);
    let answers = if plan == SavedPlan::TimedOut { 2 } else { 3 };
    let mut expected: BTreeMap<&str, u32> = SENDERS.map(|id| (id, sessions)).into();
    expected.insert("discovery/s4v2/conformant-never-closes", answers * sessions);
    if plan == SavedPlan::LateResponse {
        expected.insert(LATE_RESPONSE_ADMITTED, 1);
    }
    for property in &report.properties {
        let id = property.property.as_str();
        match &property.outcome {
            PropertyOutcome::Held { .. } if id == "sim/no-task-panic" => {}
            PropertyOutcome::Held { applicable } => {
                assert_eq!(expected.remove(id), Some(*applicable), "{name}: {id}");
            }
            PropertyOutcome::NotApplicable { reason } => {
                assert!(reason.is_benign(), "{name}: {id}: {reason:?}");
            }
            PropertyOutcome::Violated { .. } => panic!("{name}: {id} was violated"),
        }
    }
    assert!(expected.is_empty(), "{name}: never applied {expected:?}");
}

/// Each saved Plan's input runs to its budget, and
/// its prefix picks the saved scenario. No run fails, violates a property
/// or applies one its history doesn't ([`assert_applied`]), and each shows
/// its history:
///
/// - `exchange`: the peer's three answers are conformant, and the node
///   closes with `discovery_exchange_complete` before any timer fires after
///   the session's admission. The initial wait ends on cancellation,
///   `wait_complete` or its timer, and a cancelled wait closes nothing, so
///   `wait_complete` woke the node. A wait that times out closes the same
///   way once its answer wait ends. The book holds the record the peer
///   relayed. The peer's own `Hello` names its private `10.x` address, so
///   the node drops that record.
/// - `timed-out`: the node closes with `discovery_exchange_timed_out` once
///   its initial wait's timer fires and its answer wait ends, later after
///   admission than `exchange` closes, and before the peer's held `Hello`
///   goes.
/// - `verification`: the peer's `Hello` verifies on one `discovery_verify`
///   task, and its `Peers` on another, which checks every relayed record.
///   Every relayed record enters the book. The run doesn't show the
///   verification slot rule; `discovery/verification-capacity` covers it.
/// - `static-dial`: the node dials the listening peer it holds as a static
///   candidate, registers the connection, and the exchange on it
///   completes.
/// - `late-response`: the node's discovery source takes its initial wait's
///   timeout, and then the node's reader has the held `Peers` header, a
///   late answer. The answer completes the exchange, and the node closes
///   with `discovery_exchange_complete`.
///
#[test]
fn saved_plans_show_their_histories() {
    let mut waits = Vec::new();
    for plan in SavedPlan::ALL {
        let name = plan.name();
        assert_eq!(DiscoveryWire::draw(&plan.tag()), plan.scenario(), "{name}");
        let (report, _, node) = replay(plan);
        assert_eq!(report.end.harness, None, "{name}: {:?}", report.end);
        assert_eq!(report.end.violation, None, "{name}: {:?}", report.end);
        assert_applied(plan, &report);
        let closes = closes(&report);
        let handle = node.get().expect("the run built the node's handle");
        match plan {
            SavedPlan::Exchange => {
                let [(connection, cause)] = &closes[..] else {
                    panic!("{name}: {closes:?}");
                };
                assert_eq!(cause, "discovery_exchange_complete", "{name}");
                assert_eq!(peer_frames(&report, *connection), EXCHANGE, "{name}");
                assert!(
                    book_record(&handle, relayed_node(0)),
                    "{name}: no relayed record"
                );
                let (wait, fired) = admission_to_close(&report);
                assert_eq!(fired, 0, "{name}: a timer fired before the close");
                waits.push((plan, wait));
            }
            SavedPlan::TimedOut => {
                let [(connection, cause)] = &closes[..] else {
                    panic!("{name}: {closes:?}");
                };
                assert_eq!(cause, "discovery_exchange_timed_out", "{name}");
                assert_eq!(
                    peer_frames(&report, *connection),
                    [("peers", "conformant"), ("services", "conformant")],
                    "{name}: the held Hello never goes"
                );
                let (wait, fired) = admission_to_close(&report);
                assert_ne!(fired, 0, "{name}: no timer ended the initial wait");
                // The node's own timeout ends the wait, not a later timer.
                assert!(
                    (DISCOVERY_INITIAL_EXCHANGE_TIMEOUT
                        ..DISCOVERY_INITIAL_EXCHANGE_TIMEOUT + TIMEOUT_SLACK)
                        .contains(&wait),
                    "{name}: the initial wait lasted {wait:?}, the timeout is \
                     {DISCOVERY_INITIAL_EXCHANGE_TIMEOUT:?}"
                );
                waits.push((plan, wait));
            }
            SavedPlan::Verification => {
                assert_eq!(
                    closes
                        .iter()
                        .map(|(_, cause)| cause.as_str())
                        .collect::<Vec<_>>(),
                    ["discovery_exchange_complete"],
                    "{name}"
                );
                // The Hello's, then the Peers', which checks every relayed
                // record in one task.
                assert_eq!(
                    verifications(&report),
                    2,
                    "{name}: the Hello's and the Peers' verifications"
                );
                for index in 0..RELAYED {
                    assert!(
                        book_record(&handle, relayed_node(index)),
                        "{name}: relayed record {index} isn't in the book"
                    );
                }
            }
            SavedPlan::StaticDial => {
                let registered = registered_from_peer_0(&report);
                let Some(first) = registered.first() else {
                    panic!("{name}: the node registered no connection from peer-0");
                };
                assert_eq!(peer_frames(&report, *first), EXCHANGE, "{name}");
                assert_eq!(
                    closes.first(),
                    Some(&(*first, "discovery_exchange_complete".to_owned())),
                    "{name}"
                );
            }
            SavedPlan::LateResponse => {
                let [(connection, cause)] = &closes[..] else {
                    panic!("{name}: {closes:?}");
                };
                assert_eq!(cause, "discovery_exchange_complete", "{name}");
                assert_eq!(
                    peer_frames(&report, *connection),
                    [EXCHANGE[0], EXCHANGE[2], EXCHANGE[1]],
                    "{name}: the held Peers goes last"
                );
                let timed_out =
                    initial_wait_timed_out(&report).expect("the initial wait timed out");
                let (read, late) = first_late_answer(&report).expect("the node read a late answer");
                assert_eq!(
                    (late.connection, late.row.as_str()),
                    (*connection, "peers"),
                    "{name}"
                );
                assert!(
                    timed_out < read,
                    "{name}: the reader had the Peers header before the wait timed out"
                );
            }
        }
    }
    let [(SavedPlan::Exchange, completed), (SavedPlan::TimedOut, timed_out)] = waits[..] else {
        panic!("{waits:?}");
    };
    assert!(
        completed < timed_out,
        "the exchange closed {completed:?} after admission, the timeout {timed_out:?}"
    );
}

/// A/A and A/B/A: each saved Plan, run twice, then after the next saved
/// Plan, gives the same records, its observation digests and end record
/// among them, every time. The Plan between gives other records, so the
/// comparison can fail.
#[test]
fn saved_plans_repeat_a_a_and_a_b_a() {
    for (index, a) in SavedPlan::ALL.into_iter().enumerate() {
        let b = SavedPlan::ALL[(index + 1) % SavedPlan::ALL.len()];
        let runs: Vec<Vec<String>> = [a, a, b, a]
            .into_iter()
            .map(|plan| replay(plan).1)
            .collect();
        assert_eq!(runs[0], runs[1], "{} twice", a.name());
        assert_eq!(
            runs[0],
            runs[3],
            "{}, then {}, then again",
            a.name(),
            b.name()
        );
        assert_ne!(runs[0], runs[2], "{} differs from {}", b.name(), a.name());
    }
}

/// Runs the `zakura-sim` binary with `args`.
fn zakura_sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
        .args(args)
        .output()
        .expect("the binary runs")
}

/// The end record of the trace in `dir`: its `trace.jsonl`'s last line.
fn trace_end(dir: &Path) -> String {
    let trace = fs::read_to_string(dir.join(zakura_sim::trace::TRACE_FILE)).unwrap();
    trace.lines().last().unwrap().to_owned()
}

/// `zakura-sim record --input` runs each saved Plan's input in a fresh
/// process to the end record an in-process run writes, and `zakura-sim
/// replay` reruns its trace Exact in another.
#[test]
fn saved_plans_replay_exact_in_a_fresh_process() {
    for plan in SavedPlan::ALL {
        let name = plan.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let file = dir.path().join("input");
        fs::write(&file, input(plan)).unwrap();
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            DiscoveryWire::NAME,
            "--input",
            file.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        assert_eq!(trace_end(&trace), end(&replay(plan).1), "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
