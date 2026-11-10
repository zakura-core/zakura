//! `sync/wire` (xsync-3): `sync/storage`'s node and mirrors on simulated
//! QUIC, each behind a real handler that registers header sync, block sync
//! and discovery, so all three services share each connection.
//!
//! Each saved scenario has a committed input. It replays to its pinned end
//! record, repeats A/A and A/B/A, and replays Exact in a fresh process
//! through `zakura-sim replay`.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::Duration,
};

use support::inputs;
use tempfile::TempDir;
use zakura_network::zakura::{
    ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_sim::{
    domain::session::{REPLACEMENT_PRESERVES_AUTHORIZATION, STALLED_SERVICE_ISOLATED},
    harness::{
        run_prepared,
        sync::SyncPorts,
        sync_wire::{SavedWire, SyncWire, CURATED, STALL_GROWTH, STALL_MS},
        HarnessError, Subsystem,
    },
    observe::{BlockSyncFact, StorageFact, StorageRequest, TransportFact},
    seed, MemorySink, Observation, ObservationLog, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Runs `saved` under `input`, and returns the report with its records and
/// the ports, which outlive the run. Fails the test on a harness failure or
/// an active property's violation.
fn run(saved: SavedWire, input: PlanInput) -> (RunReport, Vec<String>, SyncPorts) {
    run_scenario(&saved.scenario(), input)
}

/// [`run`] on any scenario.
fn run_scenario(
    scenario: &<SyncWire as Subsystem>::Scenario,
    input: PlanInput,
) -> (RunReport, Vec<String>, SyncPorts) {
    let ports = SyncWire::prepare(scenario);
    let mut sink = MemorySink::default();
    let report = match run_prepared::<SyncWire>(scenario, ports.clone(), input, &mut sink, None) {
        Ok(report) => report,
        Err(HarnessError::Run { failure, report }) => panic!("{failure}: {:?}", report.end),
        Err(error) => panic!("{error}"),
    };
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, sink.lines(), ports)
}

/// Runs a committed input as `zakura-sim record --input` does.
fn replay(input: &[u8]) -> (RunReport, Vec<String>, SyncPorts) {
    let (prefix, plan) = input.split_at(SyncWire::SCENARIO_BYTES);
    run_scenario(&SyncWire::draw(prefix), Plan::from_fuzz(plan))
}

/// Each connection's admitted session kinds, by peer, in connection order.
fn sessions(log: &ObservationLog) -> BTreeMap<String, Vec<(u64, BTreeSet<u16>)>> {
    let mut sessions: BTreeMap<String, Vec<(u64, BTreeSet<u16>)>> = BTreeMap::new();
    for entry in log.entries() {
        if let Observation::Session {
            connection,
            peer,
            kind,
            ..
        } = &entry.obs
        {
            let connections = sessions.entry(peer.clone()).or_default();
            match connections.iter_mut().find(|(id, _)| id == connection) {
                Some((_, kinds)) => {
                    kinds.insert(*kind);
                }
                None => connections.push((*connection, BTreeSet::from([*kind]))),
            }
        }
    }
    sessions
}

/// The three services' stream kinds.
fn every_service() -> BTreeSet<u16> {
    BTreeSet::from([
        ZAKURA_STREAM_HEADER_SYNC,
        ZAKURA_STREAM_BLOCK_SYNC,
        ZAKURA_STREAM_DISCOVERY,
    ])
}

/// `property`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, property: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|report| report.property == property)
        .unwrap_or_else(|| panic!("the run reports {property}"))
        .outcome
}

/// xsync-3: the node starts at genesis between two mirrors on simulated
/// QUIC, one dialing it and one it dials. Each connection carries a header
/// sync, a block sync and a discovery session, and both of the node's tips
/// reach the mirrors' tip.
#[test]
fn honest_sync_shares_one_connection_per_peer() {
    for input in inputs(2, 256) {
        let (report, _, ports) = run(SavedWire::Honest, input);
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        let sessions = sessions(&report.log);
        assert_eq!(sessions.len(), 2, "{sessions:?}");
        for (peer, connections) in &sessions {
            assert_eq!(connections.len(), 1, "{peer}: {connections:?}");
            assert_eq!(connections[0].1, every_service(), "{peer}");
        }
    }
}

/// The block-sync generations of each routine that ended after block sync
/// admitted a newer generation for its peer, by peer.
fn superseded_after_replacement(log: &ObservationLog) -> BTreeMap<String, Vec<u64>> {
    let mut newest: BTreeMap<String, u64> = BTreeMap::new();
    let mut superseded: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for entry in log.entries() {
        match &entry.obs {
            Observation::BlockSync {
                event:
                    BlockSyncFact::SessionAdmitted {
                        peer, generation, ..
                    },
                ..
            } => {
                newest.insert(peer.clone(), *generation);
            }
            Observation::BlockSync {
                event:
                    BlockSyncFact::Routine {
                        peer,
                        generation,
                        ended: true,
                        ..
                    },
                ..
            } if newest.get(peer).is_some_and(|newest| newest > generation) => {
                superseded
                    .entry(peer.clone())
                    .or_default()
                    .push(*generation);
            }
            _ => {}
        }
    }
    superseded
}

/// xsync-3: the node's connection to the mirror it dials closes once it
/// has asked storage to admit four bodies, and the node redials. The
/// replacement carries all three services' sessions again, each under a
/// session ID no earlier session carried, while the other mirror keeps its
/// one connection. Both of the node's tips still reach the mirrors' tip.
///
/// The scenario holds the closed connection's block-sync routine until
/// block sync admits the replacement's session, so exactly one routine, the
/// superseded one, ends after its replacement's admission, and its drop's
/// registry writes land on nothing the replacement owns.
#[test]
fn a_replaced_connection_reopens_every_service_under_fresh_ids() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(2, 256)) {
        let (report, _, ports) = run(SavedWire::Replace, input);
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        let sessions = sessions(&report.log);
        assert_eq!(sessions["mirror-0"].len(), 1, "{sessions:?}");
        let replaced = &sessions["mirror-1"];
        assert!(replaced.len() >= 2, "{sessions:?}");
        assert_eq!(replaced[1].1, every_service(), "{sessions:?}");
        let superseded = superseded_after_replacement(&report.log);
        assert_eq!(
            superseded.values().map(Vec::len).sum::<usize>(),
            1,
            "{superseded:?}"
        );
        assert!(
            matches!(
                outcome(&report, REPLACEMENT_PRESERVES_AUTHORIZATION),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{:?}",
            outcome(&report, REPLACEMENT_PRESERVES_AUTHORIZATION)
        );
    }
}

/// #2094's first case on the shared connection: every mirror's block-sync
/// range read stalls for [`STALL_MS`], so block sync's sessions hold
/// requests outstanding, while header sync's and discovery's sessions share
/// their connections. Once the first read is held, the mirrors' chains grow
/// by [`STALL_GROWTH`] blocks.
///
/// Header sync applies the grown tip, and the node reads a discovery frame,
/// inside a stall: while a block-sync request that waits at least
/// [`STALL_MS`] for its first body waits. No
/// connection closes, the run holds [`STALLED_SERVICE_ISOLATED`] with its
/// trigger history, and both tips reach the grown tip once the reads
/// complete.
#[test]
fn a_stalled_block_session_starves_no_other_service() {
    let committed = Plan::from_fuzz(&[]);
    for input in std::iter::once(committed).chain(inputs(2, 256)) {
        let (report, _, ports) = run(SavedWire::StalledBlocks, input);
        let tip = ports.mirror_tip();
        let fixture_tip = ports.chain.last().expect("the fixture has blocks").height.0;
        let grown = fixture_tip + STALL_GROWTH;
        assert_eq!(tip.height.0, grown, "{:?}", report.end);
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        let stalls = stalls(&report.log);
        let stalled = |step: u64| stalls.iter().any(|stall| stall.contains(&step));
        let tip_hash: String = tip
            .hash
            .0
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let tip_applied = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match &entry.obs {
                Observation::Storage {
                    fact:
                        StorageFact::Submitted {
                            request: StorageRequest::ApplyHeaderTarget { target, .. },
                            ..
                        },
                    ..
                } if *target == tip_hash => Some(entry.step),
                _ => None,
            });
        let discovery: BTreeSet<(u64, u64)> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Session {
                    connection,
                    quic_stream,
                    kind,
                    ..
                } if *kind == ZAKURA_STREAM_DISCOVERY => Some((*connection, *quic_stream)),
                _ => None,
            })
            .collect();
        let discovery_read = report
            .log
            .entries()
            .iter()
            .find_map(|entry| match &entry.obs {
                Observation::Transport {
                    connection: Some(connection),
                    event: TransportFact::FrameRead { quic_stream, .. },
                    ..
                } if stalled(entry.step) && discovery.contains(&(*connection, *quic_stream)) => {
                    Some(entry.step)
                }
                _ => None,
            });
        assert!(
            tip_applied.is_some_and(stalled),
            "the grown tip applied at {tip_applied:?}, outside the stalls {stalls:?}"
        );
        assert!(
            discovery_read.is_some(),
            "no discovery frame read inside the stalls {stalls:?}"
        );
        let sessions = sessions(&report.log);
        for (peer, connections) in &sessions {
            assert_eq!(connections.len(), 1, "{peer}: {connections:?}");
            assert_eq!(connections[0].1, every_service(), "{peer}");
        }
        assert!(
            matches!(
                outcome(&report, STALLED_SERVICE_ISOLATED),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{:?}",
            outcome(&report, STALLED_SERVICE_ISOLATED)
        );
    }
}

/// The stalls: each block-sync request's wait, from its publication to the
/// step before its first body arrived, that lasted at least [`STALL_MS`] of
/// virtual time. Fails the test if there is none.
fn stalls(log: &ObservationLog) -> Vec<std::ops::Range<u64>> {
    let mut now_ns = 0;
    let mut waiting = BTreeMap::new();
    let mut stalls = Vec::new();
    for entry in log.entries() {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now_ns = *to_ns,
            Observation::BlockSync {
                event: BlockSyncFact::Request { owner, step, .. },
                ..
            } if step == "published" => {
                waiting.insert((owner.session_id, owner.request_id), (entry.step, now_ns));
            }
            Observation::BlockSync {
                event: BlockSyncFact::HandoffStarted { owner, .. },
                ..
            } => {
                if let Some((start, since_ns)) =
                    waiting.remove(&(owner.session_id, owner.request_id))
                {
                    if Duration::from_nanos(now_ns - since_ns) >= Duration::from_millis(STALL_MS) {
                        stalls.push(start..entry.step);
                    }
                }
            }
            _ => {}
        }
    }
    assert!(!stalls.is_empty(), "no request waited {STALL_MS} ms");
    stalls
}

/// fuzz-13's combined actions: each curated combination of a chain change
/// at a pipeline stage, a connection replacement and a read stall runs its
/// committed Plan with no active property violated, and both of the node's
/// tips reach the mirrors' tip.
#[test]
fn curated_combined_actions_sync_both_tips() {
    for (name, prefix) in CURATED {
        let (report, _, ports) = run_scenario(&SyncWire::draw(&prefix), Plan::from_fuzz(&[]));
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "{name}: {:?}", report.end);
    }
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Saved scenario `saved`'s fixture path, without an extension.
fn base(saved: SavedWire) -> PathBuf {
    fixtures().join(format!("sync-wire-{}", saved.name()))
}

/// Saved scenario `saved`'s committed input and pinned end record.
fn committed(saved: SavedWire) -> (Vec<u8>, String) {
    let base = base(saved);
    let input = fs::read(base.with_extension("bin"))
        .unwrap_or_else(|error| panic!("{}: {error}", saved.name()));
    let end = fs::read_to_string(base.with_extension("end.json"))
        .unwrap_or_else(|error| panic!("{}: {error}", saved.name()));
    (input, end.trim_end().to_owned())
}

/// A run's end record: its last line.
fn end(lines: &[String]) -> &str {
    lines.last().expect("a run writes its end record")
}

/// Each saved scenario's committed input replays to its pinned end record.
#[test]
fn saved_scenarios_replay_their_end_records() {
    for saved in SavedWire::ALL {
        let name = saved.name();
        let (input, pinned) = committed(saved);
        assert_eq!(
            SyncWire::draw(&input[..SyncWire::SCENARIO_BYTES]),
            saved.scenario(),
            "{name}"
        );
        let (_, lines, _) = replay(&input);
        assert_eq!(end(&lines), pinned, "{name}");
    }
}

/// A/A and A/B/A: each saved scenario, run twice, then after the next one,
/// gives the same records every time. The one between gives other records,
/// so the comparison can fail.
#[test]
fn saved_scenarios_repeat_a_a_and_a_b_a() {
    for (index, a) in SavedWire::ALL.into_iter().enumerate() {
        let b = SavedWire::ALL[(index + 1) % SavedWire::ALL.len()];
        let runs: Vec<Vec<String>> = [a, a, b, a]
            .into_iter()
            .map(|saved| replay(&committed(saved).0).1)
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

/// `zakura-sim record` runs each saved scenario's committed input in one
/// process, to its pinned end record, and `zakura-sim replay` reruns its
/// trace Exact in another.
#[test]
fn saved_scenarios_replay_exact_in_a_fresh_process() {
    for saved in SavedWire::ALL {
        let name = saved.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let input = base(saved).with_extension("bin");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            SyncWire::NAME,
            "--input",
            input.to_str().unwrap(),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        assert_eq!(trace_end(&trace), committed(saved).1, "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// Rewrites every saved scenario's input, sidecar and end record, and each
/// curated combination's input and sidecar. Each input is its prefix and
/// the default Plan. Run it with `RUSTFLAGS="--cfg
/// zakura_sim" cargo test --manifest-path crates/zakura-sim/Cargo.toml
/// --test sync_wire -- --ignored regenerate_saved_scenarios`.
#[test]
#[ignore = "rewrites the committed saved scenarios"]
fn regenerate_saved_scenarios() {
    for saved in SavedWire::ALL {
        let input = saved.tag().to_vec();
        let (_, lines, _) = replay(&input);
        let base = base(saved);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let scenario = serde_json::to_value(saved.scenario()).expect("scenarios serialize");
        let mut sidecar = format!(
            "# The committed fuzz input `sync-wire-{name}.bin`: the harness that\n\
             # runs it, the generator version it was built for, and its SHA-256. Its\n\
             # first two bytes pick the saved `{name}` scenario, and\n\
             # `sync-wire-{name}.end.json` is its pinned end record.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/sync_wire.rs\"\n",
            name = saved.name(),
            harness = SyncWire::NAME,
            generator = SyncWire::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(&scenario),
        );
        for (property, expect) in saved.witnesses() {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"{expect}\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
        fs::write(
            base.with_extension("end.json"),
            format!("{}\n", end(&lines)),
        )
        .expect("the fixture directory exists");
    }
    for (name, prefix) in CURATED {
        let base = fixtures().join(format!("sync-wire-{name}"));
        fs::write(base.with_extension("bin"), prefix).expect("the fixture directory exists");
        let scenario = serde_json::to_value(SyncWire::draw(&prefix)).expect("scenarios serialize");
        let sidecar = format!(
            "# The committed fuzz input `sync-wire-{name}.bin`: the harness that\n\
             # runs it, the generator version it was built for, and its SHA-256. Its\n\
             # two bytes pick a combination of fuzz-13's actions, a seed of the\n\
             # `sync_wire` fuzz target.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/sync_wire.rs\"\n",
            harness = SyncWire::NAME,
            generator = SyncWire::GENERATOR,
            digest = seed::sha256(&prefix),
            scenario = seed::scenario_sha256(&scenario),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
