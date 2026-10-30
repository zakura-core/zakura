//! M6a-6: output lifetimes on `header-sync/wire`. Every frame the node
//! queues on a header stream is an output whose life ends exactly once,
//! from its life's `Drop`, and every frame a peer reads maps to the output
//! its write started, with the fingerprint its producer queued. The output
//! ledger in the evidence hub fails a run that breaks either, so these
//! tests first assert the histories they need, then that the runs pass.
#![cfg(zakura_sim)]

use std::collections::BTreeMap;

use zakura_sim::{
    harness::{
        header_node::Chain,
        header_sync::{HeaderWire, WireScenario},
        run_prepared,
        wire_plans::SavedPlan,
        HarnessError, Subsystem,
    },
    observe::{OutputEnd, TransportFact},
    peer::{honest::HonestSpec, PeerSpec},
    MemorySink, NoChecks, Observation, Outcome, Plan, PlanInput, RunConfig, RunReport, Sim,
};

/// An output, by connection, QUIC stream and output number.
type OutputKey = (u64, u64, u64);

/// Runs `scenario` under `plan`, and returns the report, even of a run that
/// ended in a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    let mut sink = MemorySink::default();
    match run_prepared::<HeaderWire>(
        scenario,
        HeaderWire::prepare(scenario),
        plan,
        &mut sink,
        Some(&mut NoChecks),
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// Saved Plan `plan`'s scenario and input.
fn saved(plan: SavedPlan) -> (WireScenario, PlanInput) {
    let input = std::fs::read(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/header-wire-{}.bin", plan.name())),
    )
    .expect("the fixture is committed");
    let (prefix, bytes) = input.split_at(HeaderWire::SCENARIO_BYTES);
    (HeaderWire::draw(prefix), Plan::from_fuzz(bytes))
}

/// What a log says about each output: whether it was allocated and
/// enqueued, and every end it got.
#[derive(Debug, Default)]
struct Life {
    allocated: bool,
    enqueued: bool,
    ends: Vec<OutputEnd>,
}

/// Each output's life in `report`'s log, and how many frames the peers
/// read.
fn lives(report: &RunReport) -> (BTreeMap<OutputKey, Life>, usize) {
    let mut lives: BTreeMap<OutputKey, Life> = BTreeMap::new();
    let mut received = 0;
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::Transport {
                connection: Some(connection),
                event,
                ..
            } => match *event {
                TransportFact::OutputAllocated {
                    quic_stream,
                    output,
                    ..
                } => {
                    lives
                        .entry((*connection, quic_stream, output))
                        .or_default()
                        .allocated = true;
                }
                TransportFact::OutputEnqueued {
                    quic_stream,
                    output,
                    ..
                } => {
                    lives
                        .entry((*connection, quic_stream, output))
                        .or_default()
                        .enqueued = true;
                }
                TransportFact::OutputEnded {
                    quic_stream,
                    output,
                    outcome,
                } => lives
                    .entry((*connection, quic_stream, output))
                    .or_default()
                    .ends
                    .push(outcome),
                _ => {}
            },
            Observation::PeerReceived { .. } => received += 1,
            _ => {}
        }
    }
    (lives, received)
}

/// Over the 14 saved Plans, each output the node allocates ends exactly
/// once, or is still live when the log freezes, and the runs pass the
/// output ledger, so each frame a peer read was its output's frame. The
/// Plans first show written, dropped and never-queued outputs, and peer
/// reads to map.
///
/// This fails if an output can end twice, as a terminal reported from a
/// cancellation arm as well as from `Drop` would.
#[test]
fn every_output_ends_exactly_once() {
    let mut seen: BTreeMap<OutputEnd, usize> = BTreeMap::new();
    let mut received = 0;
    for plan in SavedPlan::ALL {
        let (scenario, input) = saved(plan);
        let report = run(&scenario, input);
        assert_eq!(
            report.end.harness,
            None,
            "{}: {:?}",
            plan.name(),
            report.end
        );
        let (lives, read) = lives(&report);
        received += read;
        for (key, life) in &lives {
            assert!(life.allocated, "{}: {key:?} ended unallocated", plan.name());
            assert!(
                life.ends.len() <= 1,
                "{}: {key:?} ended {:?}",
                plan.name(),
                life.ends
            );
            for end in &life.ends {
                *seen.entry(*end).or_default() += 1;
            }
        }
    }
    for end in [OutputEnd::Written, OutputEnd::Dropped, OutputEnd::NotQueued] {
        assert!(seen.contains_key(&end), "no {end:?} output: {seen:?}");
    }
    assert!(received > 0, "the peers read the node's frames");
}

/// An honest peer holding `chain` headers that dials the node at 50.
fn honest(chain: u32) -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain,
                subscribe: None,
            }),
            ..PeerSpec::dialing()
        }],
        ..WireScenario::default()
    }
}

/// Runs `scenario` under the empty Plan with `config`.
fn run_with(scenario: &WireScenario, config: RunConfig) -> RunReport {
    let mut sim = Sim::new(config, Plan::from_fuzz(&[]));
    let node = HeaderWire::build(&mut sim, scenario, HeaderWire::prepare(scenario));
    let report = sim.run(&mut NoChecks, &mut MemorySink::default());
    drop(node);
    report
}

/// A run cut by its budget with a frame still in a stream's queue leaves
/// that output pending: it was enqueued and never ended, and the run has no
/// harness failure.
///
/// This fails if live outputs are ended, or failed, at the freeze.
#[test]
fn a_truncated_run_leaves_queued_output_pending() {
    let scenario = honest(1_200);
    let full = HeaderWire::config(&scenario);
    let report = run_with(&scenario, full);
    // The steps where an output was enqueued and not yet ended.
    let mut open = BTreeMap::new();
    let mut queued_steps = Vec::new();
    for entry in report.log.entries() {
        if let Observation::Transport {
            connection: Some(connection),
            event,
            ..
        } = &entry.obs
        {
            match *event {
                TransportFact::OutputEnqueued {
                    quic_stream,
                    output,
                    ..
                } => {
                    open.insert((*connection, quic_stream, output), entry.step);
                }
                TransportFact::OutputDequeued {
                    quic_stream,
                    output,
                }
                | TransportFact::OutputEnded {
                    quic_stream,
                    output,
                    ..
                } => {
                    if let Some(step) = open.remove(&(*connection, quic_stream, output)) {
                        if step < entry.step {
                            queued_steps.push(step);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let step = *queued_steps
        .first()
        .expect("a frame waits in its queue past the step it was enqueued in");
    let config = RunConfig {
        budget: step + 1,
        drain_bound: 0,
        ..full
    };
    let report = run_with(&scenario, config);
    assert_eq!(report.end.outcome, Outcome::Budget, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let (lives, _) = lives(&report);
    assert!(
        lives
            .values()
            .any(|life| life.allocated && life.enqueued && life.ends.is_empty()),
        "a queued output is live at the freeze: {lives:?}"
    );
}
