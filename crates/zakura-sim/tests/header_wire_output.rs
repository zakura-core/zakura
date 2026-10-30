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
    peer::{
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        PeerSpec,
    },
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

/// Over the 14 saved Plans, every frame the node started writing on a
/// header stream has its producer's attempt, and the writer's decoded
/// summary of it, which the output ledger checks against the attempt. The
/// Plans first show attempts from the cadence's `Status`, the subscriber's
/// `Open` and the publisher's pages.
///
/// This fails if a producer reports no attempt, since the ledger then
/// fails the run with output without attempt.
#[test]
fn every_started_frame_has_its_attempt() {
    let mut producers: BTreeMap<String, usize> = BTreeMap::new();
    let mut started = 0;
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
        let mut attempts = BTreeMap::new();
        for entry in report.log.entries() {
            let Observation::Transport {
                connection: Some(connection),
                event,
                ..
            } = &entry.obs
            else {
                continue;
            };
            match event {
                TransportFact::OutputAttempt {
                    quic_stream,
                    output,
                    attempt,
                } => {
                    *producers.entry(attempt.producer.clone()).or_default() += 1;
                    attempts.insert((*connection, *quic_stream, *output), attempt.clone());
                }
                TransportFact::FrameWriteStarted {
                    quic_stream,
                    output: Some(output),
                    summary,
                    ..
                } => {
                    started += 1;
                    let attempt = attempts.get(&(*connection, *quic_stream, *output));
                    assert!(
                        attempt.is_some(),
                        "{}: {entry:?} has no attempt",
                        plan.name()
                    );
                    assert_eq!(summary.as_ref(), attempt, "{}", plan.name());
                }
                _ => {}
            }
        }
    }
    for producer in ["status", "open", "page"] {
        assert!(
            producers.contains_key(producer),
            "no {producer} attempt: {producers:?}"
        );
    }
    assert!(started > 0);
}

/// A page's summary is the page's own. `peer-0` holds 50 headers and
/// subscribes to the node, which holds 1,200, with two headers of credit,
/// granted back after each page. The node serves the subscription page
/// after page, and each page's summary names its own two headers and its
/// own last frontier, two above the last page's, never the subscription's
/// running total. The test first asserts at least two pages on one
/// subscription.
///
/// This fails if a page's summary carries cumulative totals.
#[test]
fn page_summaries_are_per_page() {
    let scenario = WireScenario {
        node: Chain::main(1_200),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 50,
                subscribe: Some(SubscribeSpec {
                    start: 50,
                    open: CreditSpec {
                        objects: 2,
                        bytes: 4_096,
                    },
                    grants: GrantPolicy::Immediate,
                }),
            }),
            ..PeerSpec::dialing()
        }],
        ..WireScenario::default()
    };
    let report = run(&scenario, Plan::from_fuzz(&[]));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let mut pages: BTreeMap<(u64, u64, u64), Vec<(u64, u32)>> = BTreeMap::new();
    for entry in report.log.entries() {
        if let Observation::Transport {
            connection: Some(connection),
            event:
                TransportFact::FrameWriteStarted {
                    quic_stream,
                    summary: Some(summary),
                    ..
                },
            ..
        } = &entry.obs
        {
            if summary.producer == "page" {
                let cursor = summary.cursor.as_ref().expect("a page names its frontier");
                pages
                    .entry((
                        *connection,
                        *quic_stream,
                        summary.subscription.expect("a page names its subscription"),
                    ))
                    .or_default()
                    .push((summary.objects, cursor.height));
            }
        }
    }
    let served = pages
        .values()
        .max_by_key(|pages| pages.len())
        .expect("the node served a page");
    assert!(
        served.len() >= 2,
        "one subscription got two pages: {pages:?}"
    );
    for pair in served.windows(2) {
        let ((_, first_height), (count, height)) = (pair[0], pair[1]);
        assert_eq!(count, 2, "each page holds its own two headers: {served:?}");
        assert_eq!(height, first_height + 2, "{served:?}");
    }
}
