//! disc-2.3: `discovery/wire`'s saved Plans. Each committed Plan replays to
//! its pinned end record, and shows the history its scenario exists for: a
//! completed exchange, a timed-out one, verification on `discovery_verify`
//! tasks, and a dial of a static candidate. A run repeats under its Plan
//! alone, with another run between, and replays Exact in a fresh process
//! through `zakura-sim replay`. A history a test needs that never happened fails
//! it: a missing observation is a harness failure, never evidence.
#![cfg(zakura_sim)]

use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
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
    harness::{
        discovery_service::{DiscoveryWire, NodeHandle, SavedPlan, WireScenario, RELAYED},
        run_prepared, HarnessError, Subsystem,
    },
    observe::TransportFact,
    peer::{discovery::relayed_node, peer_addr},
    seed, MemorySink, NoChecks, Observation, Plan, RunReport,
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
        Some(&mut NoChecks),
    );
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, sink.lines(), node)
}

/// Runs a committed input as `zakura-sim record --input` does: its prefix
/// picks the scenario, and the rest is the Plan.
fn replay(input: &[u8]) -> (RunReport, Vec<String>, NodeHandle) {
    let (prefix, plan) = input.split_at(DiscoveryWire::SCENARIO_BYTES);
    run(&DiscoveryWire::draw(prefix), plan)
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Saved Plan `plan`'s fixture path, without an extension.
fn base(plan: SavedPlan) -> PathBuf {
    fixtures().join(format!("discovery-wire-{}", plan.name()))
}

/// Saved Plan `plan`'s committed input and pinned end record.
fn committed(plan: SavedPlan) -> (Vec<u8>, String) {
    let base = base(plan);
    let input = fs::read(base.with_extension("bin"))
        .unwrap_or_else(|error| panic!("{}: {error}", plan.name()));
    let end = fs::read_to_string(base.with_extension("end.json"))
        .unwrap_or_else(|error| panic!("{}: {error}", plan.name()));
    (input, end.trim_end().to_owned())
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

/// Each saved Plan, committed under `tests/fixtures/discovery-wire-*.bin`
/// with its sidecar, replays to its pinned end record,
/// `discovery-wire-*.end.json`. Its prefix picks the saved scenario. No run
/// fails or violates a property, and each shows its history:
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
///
/// This fails if a saved Plan's end record changes. After a deliberate
/// change, regenerate them with `regenerate_saved_plans`.
#[test]
fn saved_plans_replay_their_end_records() {
    let mut waits = Vec::new();
    for plan in SavedPlan::ALL {
        let name = plan.name();
        let (input, pinned) = committed(plan);
        assert_eq!(DiscoveryWire::draw(&input[..2]), plan.scenario(), "{name}");
        let (report, lines, node) = replay(&input);
        assert_eq!(report.end.harness, None, "{name}: {:?}", report.end);
        assert_eq!(report.end.violation, None, "{name}: {:?}", report.end);
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
        }
        assert_eq!(
            end(&lines),
            pinned,
            "{name}: the saved Plan's end record changed; regenerate the saved Plans if that's \
             intended"
        );
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
            .map(|plan| replay(&committed(plan).0).1)
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

/// `zakura-sim record` runs each saved Plan's committed input in one
/// process, to its pinned end record, and `zakura-sim replay` reruns its
/// trace Exact in another.
#[test]
fn saved_plans_replay_exact_in_a_fresh_process() {
    for plan in SavedPlan::ALL {
        let name = plan.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let input = base(plan).with_extension("bin");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            DiscoveryWire::NAME,
            "--input",
            input.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        assert_eq!(trace_end(&trace), committed(plan).1, "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// Rewrites every saved Plan: its input, its sidecar and its end record.
/// Each input is its tag and the default Plan, the one disc-2.2's tests
/// run. Run it with `RUSTFLAGS="--cfg zakura_sim" cargo test
/// --manifest-path crates/zakura-sim/Cargo.toml --test
/// discovery_wire_lifecycle -- --ignored regenerate_saved_plans`.
#[test]
#[ignore = "rewrites the committed saved Plans"]
fn regenerate_saved_plans() {
    for plan in SavedPlan::ALL {
        let input = plan.tag().to_vec();
        let (_, lines, _) = replay(&input);
        let base = base(plan);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let scenario = serde_json::to_value(plan.scenario()).expect("scenarios serialize");
        let sidecar = format!(
            "# The committed fuzz input `discovery-wire-{name}.bin`: the harness that\n\
             # runs it, the generator version it was built for, and its SHA-256. Its\n\
             # first two bytes pick the saved `{name}` scenario, and\n\
             # `discovery-wire-{name}.end.json` is its pinned end record.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/discovery_wire_lifecycle.rs\"\n",
            name = plan.name(),
            harness = DiscoveryWire::NAME,
            generator = DiscoveryWire::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(&scenario),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
        fs::write(
            base.with_extension("end.json"),
            format!("{}\n", end(&lines)),
        )
        .expect("the fixture directory exists");
    }
}
