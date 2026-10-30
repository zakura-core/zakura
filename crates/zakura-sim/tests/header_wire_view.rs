//! M6a-5b: stream-exact downstream rows on `header-sync/wire`. A header
//! stream that retires and reopens on the same connection numbers its
//! frames and subscriptions from the start again. This pins a history
//! where the node stages work on both stream incarnations. A history a
//! test needs that never happened fails it.
#![cfg(zakura_sim)]

mod support;

use std::collections::{BTreeMap, BTreeSet};

use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{HeaderWire, WireScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::ReactorFact,
    peer::{honest::HonestSpec, PeerAction, PeerSpec},
    Checker, MemorySink, NoChecks, Observation, Plan, PlanInput, RunReport,
};

/// Runs `scenario` under `plan` with `extra` beside the harness's checker,
/// and returns the report, even of a run that ended in a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput, extra: &mut dyn Checker) -> RunReport {
    let mut sink = MemorySink::default();
    match run_prepared::<HeaderWire>(
        scenario,
        HeaderWire::prepare(scenario),
        plan,
        &mut sink,
        Some(extra),
    ) {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// The node at 50 dials `peer-0`, which holds the 1,200-header tree and
/// finishes its side of the header stream once every subscription has
/// ended. The node's second locator is held for 5 s of virtual time, so the
/// finish falls between two of its rounds: after the first stream's pages
/// and before the next round, which runs on the reopened stream.
fn reopen() -> WireScenario {
    let mut peer = PeerSpec {
        honest: Some(HonestSpec {
            chain: 1_200,
            subscribe: None,
        }),
        ..PeerSpec::listening()
    };
    peer.script = vec![PeerAction::FinishHeader];
    WireScenario {
        node: Chain::main(50),
        peers: vec![peer],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 2),
                until: Trigger::VirtualTime(5_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// The page frames the node staged in `report`, by QUIC stream, as the
/// reactor's frame tags name them.
fn staged(report: &RunReport) -> BTreeMap<u64, BTreeSet<u64>> {
    let mut staged: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for entry in report.log.entries() {
        if let Observation::Reactor {
            event: ReactorFact::Staged {
                frame: Some(frame), ..
            },
            ..
        } = &entry.obs
        {
            staged
                .entry(frame.quic_stream)
                .or_default()
                .insert(frame.seq);
        }
    }
    staged
}

/// A header stream retires and reopens on one connection, and the node
/// stages pages on both QUIC streams, with frame seqs the reopened stream
/// counts from the start again.
///
/// This fails if the peer finishes its stream before the node's first
/// round staged a page, or if the node takes no page after the reopen.
#[test]
fn two_stream_incarnations_keep_their_work_apart() {
    let scenario = reopen();
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(support::inputs(2, 16_384));
    for plan in plans {
        let report = run(&scenario, plan, &mut NoChecks);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        let entries = report.log.entries();

        // The history: two header sessions on one connection, each on its
        // own QUIC stream, with staged pages on both whose seqs overlap.
        let sessions: Vec<(u64, u64)> = entries
            .iter()
            .filter_map(|entry| match entry.obs {
                Observation::Session {
                    connection,
                    quic_stream,
                    ..
                } => Some((connection, quic_stream)),
                _ => None,
            })
            .collect();
        assert_eq!(sessions.len(), 2, "the stream reopened: {sessions:?}");
        assert_eq!(sessions[0].0, sessions[1].0, "on one connection");
        assert_ne!(sessions[0].1, sessions[1].1, "on a new QUIC stream");
        let staged = staged(&report);
        let quic: Vec<u64> = staged.keys().copied().collect();
        assert_eq!(quic, [sessions[0].1, sessions[1].1], "{staged:?}");
        let overlap: BTreeSet<u64> = staged[&quic[0]]
            .intersection(&staged[&quic[1]])
            .copied()
            .collect();
        assert!(!overlap.is_empty(), "the seqs overlap: {staged:?}");
    }
}
