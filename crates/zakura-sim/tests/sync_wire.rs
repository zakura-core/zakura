//! `sync/wire` (xsync-3, sim-9.3): `sync/storage`'s node and mirrors on
//! simulated QUIC, each behind a real handler that registers header sync,
//! block sync and discovery, so all three services share each connection.
//!
//! Each saved scenario is a seed. It runs to its step budget holding its
//! witnesses, repeats A/A and A/B/A, and replays Exact in a fresh process
//! through `zakura-sim replay`.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    process::{Command, Output},
    time::Duration,
};

use support::inputs;
use tempfile::TempDir;
use zakura_network::zakura::{
    ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_sim::{
    domain::{
        connection::{CONNECTION_SHARES_BOUND, CREDIT_ISOLATED, SEND_ISOLATED},
        session::{REPLACEMENT_PRESERVES_AUTHORIZATION, STALLED_SERVICE_ISOLATED},
    },
    harness::{
        run_prepared,
        sync::SyncPorts,
        sync_wire::{SavedWire, SyncWire, CURATED, STALL_GROWTH, STALL_MS},
        HarnessError, Subsystem,
    },
    observe::{BlockSyncFact, StorageFact, StorageRequest, TransportFact},
    seed::{self, SeedSet},
    MemorySink, Observation, ObservationLog, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
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
    let (report, lines, ports) = run_scenario_judged_by_caller(scenario, input);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, lines, ports)
}

/// [`run_scenario`], but leaves an active property's violation to the
/// caller. Still fails the test on a harness failure.
fn run_scenario_judged_by_caller(
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
    (report, sink.lines(), ports)
}

/// Runs a full fuzz input as `zakura-sim record --seed` does.
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

/// The generation of each gated registry write the log shows refused on an
/// entry another generation owns.
fn refused_stale_writes(log: &ObservationLog) -> Vec<u64> {
    log.entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::BlockSync {
                event:
                    BlockSyncFact::StaleRegistryWrite {
                        generation,
                        landed: false,
                        ..
                    },
                ..
            } => Some(*generation),
            _ => None,
        })
        .collect()
}

/// xsync-3: the node's connection to the mirror it dials closes once it
/// has asked storage to admit four bodies, and the node redials. The
/// replacement carries all three services' sessions again, each under a
/// session ID no earlier session carried, while the other mirror keeps its
/// one connection. Both of the node's tips still reach the mirrors' tip.
///
/// The scenario holds the closed connection's block-sync routine until
/// block sync admits the replacement's session, so exactly one routine, the
/// superseded one, ends after its replacement's admission. Its drop's gated
/// registry writes meet the replacement's entry, and the gates refuse them.
#[test]
fn a_replaced_connection_reopens_every_service_under_fresh_ids() {
    // The seed's Plan is empty.
    let seed = Plan::from_fuzz(&[]);
    for input in std::iter::once(seed).chain(inputs(2, 256)) {
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
        let attempts = refused_stale_writes(&report.log);
        assert!(
            !attempts.is_empty()
                && attempts
                    .iter()
                    .all(|generation| superseded.values().flatten().any(|g| g == generation)),
            "the superseded routine's gated writes met its replacement's entry: \
             {attempts:?} {superseded:?}"
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

/// For each routine that ended after block sync admitted a newer generation
/// for its peer, the claims its peer's newest routine reported last before
/// that end.
fn replacement_claims_at_superseded_end(log: &ObservationLog) -> Vec<usize> {
    let mut newest: BTreeMap<String, u64> = BTreeMap::new();
    let mut claims: BTreeMap<(String, u64), usize> = BTreeMap::new();
    let mut met = Vec::new();
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
                        claims: published,
                        ended,
                        ..
                    },
                ..
            } => {
                let replacement = newest
                    .get(peer)
                    .copied()
                    .filter(|newest| newest > generation);
                if let (true, Some(replacement)) = (*ended, replacement) {
                    met.push(
                        claims
                            .get(&(peer.clone(), replacement))
                            .copied()
                            .unwrap_or(0),
                    );
                }
                claims.insert((peer.clone(), *generation), published.len());
            }
            _ => {}
        }
    }
    met
}

/// The node's connection to its one mirror closes once it has asked
/// storage to admit four bodies, and the node redials. The mirror's chain
/// grows after the redial, so the replacement's routine requests the new
/// bodies. The scenario holds the closed connection's block-sync routine
/// until the replacement's routine reports a claim, so the superseded
/// routine's drop meets an entry with claims. The gates refuse its writes,
/// and the registry's fingerprints of the entry show no change. Both of the
/// node's tips still reach the mirror's tip.
#[test]
fn a_superseded_routine_drops_beside_its_replacements_claims() {
    // The seed's Plan is empty.
    let seed = Plan::from_fuzz(&[]);
    for input in std::iter::once(seed).chain(inputs(2, 256)) {
        let (report, _, ports) = run(SavedWire::ReplaceClaims, input);
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
        let met = replacement_claims_at_superseded_end(&report.log);
        assert!(
            met.len() == 1 && met[0] > 0,
            "exactly one routine ended after its replacement's admission, beside \
             the replacement's claims: {met:?}"
        );
        assert!(
            !refused_stale_writes(&report.log).is_empty(),
            "the superseded routine's gated writes met its replacement's entry"
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

/// Round 8's tail: a budget that ends while `mirror-1`'s dialer sleeps out
/// its redial settle leaves the redial to the drain. The drain's clock
/// reaches that timer within the harness timer cap, though nothing is open,
/// so the replacement connection still opens, in the drain, and the run
/// ends dry rather than at its bound.
#[test]
fn a_redial_asleep_at_the_budget_finishes_in_the_drain() {
    let (full, _, _) = run(SavedWire::Replace, Plan::from_fuzz(&[]));
    let first_session = |report: &RunReport, connection: u64| {
        report
            .log
            .entries()
            .iter()
            .find(|entry| {
                matches!(&entry.obs, Observation::Session { connection: c, peer, .. }
                    if *c == connection && peer == "mirror-1")
            })
            .map(|entry| entry.step)
            .expect("the connection carried a session")
    };
    let replacement = sessions(&full.log)["mirror-1"][1].0;
    let opened = first_session(&full, replacement);
    // The last clock jump before the replacement is the redial's settle.
    let settle = full.picks[..usize::try_from(opened).expect("a run's steps fit usize")]
        .iter()
        .rev()
        .find(|pick| pick.identity.starts_with("advance-time:"))
        .map(|pick| pick.step)
        .expect("the redial waited out its settle");
    let mut scenario = SavedWire::Replace.scenario();
    scenario.sync.steps = Some(settle - 1);
    let (cut, _, _) = run_scenario(&scenario, Plan::from_fuzz(&[]));
    assert_eq!(cut.end.outcome, Outcome::Budget, "{:?}", cut.end);
    let replaced = &sessions(&cut.log)["mirror-1"];
    assert!(replaced.len() >= 2, "the redial ran: {replaced:?}");
    assert!(
        first_session(&cut, replaced[1].0) > settle - 1,
        "the replacement opened in the drain"
    );
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
    // The seed's Plan is empty.
    let seed = Plan::from_fuzz(&[]);
    for input in std::iter::once(seed).chain(inputs(2, 256)) {
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

/// sim-9.3: [`SavedWire::Saturated`] runs the node's connections with its
/// QUIC receive window at the control share and the mirrors' send windows
/// at the same floor, so large bodies saturate each connection while the
/// mirrors' chains grow and header sync and discovery share it.
///
/// The run holds [`CREDIT_ISOLATED`], [`SEND_ISOLATED`] and
/// [`CONNECTION_SHARES_BOUND`], each with its trigger history. Each peer
/// keeps one connection carrying every service, the node reads block-sync
/// frames on both, so neither connection starves the other, and both tips
/// reach the mirrors' grown tip.
#[test]
fn a_saturating_service_starves_no_other_on_its_connection() {
    let (report, _, ports) = run(SavedWire::Saturated, Plan::from_fuzz(&[]));
    let tip = ports.mirror_tip();
    let fixture_tip = ports.chain.last().expect("the fixture has blocks").height.0;
    assert_eq!(tip.height.0, fixture_tip + STALL_GROWTH, "{:?}", report.end);
    assert_eq!(ports.tips(), (tip, tip), "{:?}", report.end);
    let sessions = sessions(&report.log);
    let mut block_streams = BTreeSet::new();
    for (peer, connections) in &sessions {
        assert_eq!(connections.len(), 1, "{peer}: {connections:?}");
        assert_eq!(connections[0].1, every_service(), "{peer}");
    }
    for entry in report.log.entries() {
        if let Observation::Session {
            connection,
            quic_stream,
            kind,
            ..
        } = &entry.obs
        {
            if *kind == ZAKURA_STREAM_BLOCK_SYNC {
                block_streams.insert((*connection, *quic_stream));
            }
        }
    }
    let read_on: BTreeSet<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                event: TransportFact::FrameRead { quic_stream, .. },
                ..
            } if block_streams.contains(&(*connection, *quic_stream)) => Some(*connection),
            _ => None,
        })
        .collect();
    assert_eq!(
        read_on.len(),
        sessions.len(),
        "block frames read on {read_on:?}"
    );
    for property in [CREDIT_ISOLATED, SEND_ISOLATED, CONNECTION_SHARES_BOUND] {
        assert!(
            matches!(
                outcome(&report, property),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{property}: {:?}",
            outcome(&report, property)
        );
    }
}

/// fuzz-13's combined actions: each curated combination of a chain change
/// at a pipeline stage, a connection replacement and a read stall runs its
/// seed's empty Plan with no active property violated, and both of the node's
/// tips reach the mirrors' tip.
#[test]
fn curated_combined_actions_sync_both_tips() {
    for (name, prefix) in CURATED {
        let (report, _, ports) = run_scenario(&SyncWire::draw(&prefix), Plan::from_fuzz(&[]));
        let tip = ports.mirror_tip();
        assert_eq!(ports.tips(), (tip, tip), "{name}: {:?}", report.end);
    }
}

/// The seed that reaches zakura#2378's schedule: the fork while storage
/// holds a submission, with every read stalled, under a Plan that a steered
/// run recorded so that a block request's writer runs last while its frame
/// write is under way.
const RESET_MID_WRITE: &str = "sync-wire-reset-mid-write";

/// The steps at which a reset retired a block request whose frame write
/// had started and not finished.
fn mid_write_resets(log: &ObservationLog) -> Vec<u64> {
    let mut last = BTreeMap::new();
    let mut resets = Vec::new();
    for entry in log.entries() {
        if let Observation::BlockSync {
            event: BlockSyncFact::Request { owner, step, .. },
            ..
        } = &entry.obs
        {
            let request = (owner.session_id, owner.request_id);
            if step == "reset" {
                if last.get(&request).map(String::as_str) == Some("started") {
                    resets.push(entry.step);
                }
            } else {
                last.insert(request, step.clone());
            }
        }
    }
    resets
}

/// zakura#2378: a reset that retires a block request mid-write once
/// cancelled the whole block-sync session, and the writer fence closed the
/// honest connection with `unfinished_exchange` while the session's other
/// requests waited, which [`STALLED_SERVICE_ISOLATED`] forbids. The fence
/// now holds the session's token, so the reset ends only the session, and
/// the connection stays open.
///
/// [`RESET_MID_WRITE`] must validate as a corpus seed of `sync/wire` whose
/// only witness expects that property to hold. Its run must reset a request
/// mid-write and end with no violation. The test asks for a new seed if the
/// schedule stops reaching the mid-write reset.
#[test]
fn a_mid_write_reset_keeps_an_honest_connection() {
    let report = seed::load(SeedSet::Corpus, Some(SyncWire::NAME));
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    let seed = report
        .admitted
        .iter()
        .find(|seed| seed.name == RESET_MID_WRITE)
        .expect("the seed is in the corpus");
    let witnesses: Vec<(&str, &str)> = seed
        .witness
        .iter()
        .map(|witness| (witness.property.as_str(), witness.expect.as_str()))
        .collect();
    assert_eq!(witnesses, [(STALLED_SERVICE_ISOLATED, "holds")]);
    let (prefix, plan) = seed.input().split_at(SyncWire::SCENARIO_BYTES);
    let (report, _, _) =
        run_scenario_judged_by_caller(&SyncWire::draw(prefix), Plan::from_fuzz(plan));
    assert!(
        !mid_write_resets(&report.log).is_empty(),
        "{RESET_MID_WRITE} no longer resets a block request mid-write: build a new seed: {:?}",
        report.end
    );
    assert_eq!(report.end.violation, None, "{:?}", report.end);
}

/// A run's end record: its last line.
fn end(lines: &[String]) -> &str {
    lines.last().expect("a run writes its end record")
}

/// Each saved scenario's seed, its tag and the empty Plan, runs to its step
/// budget with no property violated, its drain runs dry, and cleanup finds
/// no panic, failed unwind or leaked task. Each of its witnesses applies
/// and holds.
#[test]
fn saved_scenarios_run_to_their_budget_holding_their_witnesses() {
    for saved in SavedWire::ALL {
        let name = saved.name();
        let input = saved.tag();
        assert_eq!(SyncWire::draw(&input), saved.scenario(), "{name}");
        let (report, _, _) = replay(&input);
        assert_eq!(
            report.end.outcome,
            Outcome::Budget,
            "{name}: {:?}",
            report.end
        );
        assert!(
            report
                .end
                .drain
                .as_ref()
                .is_some_and(|drain| !drain.exhausted),
            "{name}: {:?}",
            report.end
        );
        let cleanup = &report.cleanup;
        assert_eq!(
            (cleanup.panicked, cleanup.unwind_failures, cleanup.leaked),
            (0, 0, 0),
            "{name}: {cleanup:?}"
        );
        for property in &report.properties {
            assert!(
                !matches!(property.outcome, PropertyOutcome::Violated { .. }),
                "{name}: {property:?}"
            );
        }
        for (property, _) in saved.witnesses() {
            assert!(
                matches!(outcome(&report, property), PropertyOutcome::Held { applicable } if *applicable > 0),
                "{name}: {property}: {:?}",
                outcome(&report, property)
            );
        }
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
            .map(|saved| replay(&saved.tag()).1)
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

/// `zakura-sim record --seed` runs each saved scenario's seed in another
/// process to the end record this process's run gives, and `zakura-sim
/// replay` reruns its trace Exact in a third.
#[test]
fn saved_scenarios_replay_exact_in_a_fresh_process() {
    for saved in SavedWire::ALL {
        let name = saved.name();
        let dir = TempDir::new().unwrap();
        let trace = dir.path().join("trace");
        let recorded = zakura_sim(&[
            "record",
            "--seed",
            &format!("sync-wire-{name}"),
            "--out",
            trace.to_str().unwrap(),
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let (_, lines, _) = replay(&saved.tag());
        assert_eq!(trace_end(&trace), end(&lines), "{name}");
        let replayed = zakura_sim(&["replay", trace.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}
