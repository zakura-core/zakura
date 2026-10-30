//! M6a-5b: stream-exact downstream rows on `header-sync/wire`. A header
//! stream that retires and reopens on the same connection numbers its
//! frames and subscriptions from the start again, so the node's staged
//! work on each QUIC stream must join that stream's frames only. A history
//! a test needs that never happened fails it.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use zakura_network::zakura::v9::RegtestMessage;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{HeaderWire, WireScenario, FAMILY},
        run_prepared, HarnessError, Subsystem,
    },
    observe::ReactorFact,
    peer::{honest::HonestSpec, PeerAction, PeerSpec},
    property::{Assumptions, HasView, Registry, RegistryChecker},
    table::{downstream_properties, Effect, StreamIdentity},
    Checker, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Nothing to view: the downstream checks read the log alone.
struct Nothing;

impl HasView for Nothing {
    type View = ();

    fn view(&self) {}
}

/// The header family's `receiver-work-authorized` properties on the wire
/// tier, one per response row. The other downstream templates need
/// peer-side credit rows that `header-sync/wire` doesn't log yet.
fn work_checker() -> RegistryChecker<Nothing> {
    let properties = downstream_properties::<Nothing>(
        FAMILY,
        &RegtestMessage::LAYOUT,
        &[Effect::Work],
        StreamIdentity::Exact,
    )
    .into_iter()
    .filter(|property| property.id.as_str().ends_with("/receiver-work-authorized"))
    .collect();
    RegistryChecker::new(
        Registry::new(properties).expect("one per row"),
        Rc::new(Nothing),
        Assumptions::none(),
        &HeaderWire::sources(),
    )
}

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
/// stages pages on both QUIC streams, with frame seqs and the subscription
/// ID the reopened stream counts from the start again. Each `WorkAdmitted`
/// names its frame's QUIC stream and joins exactly one page frame there,
/// though the other stream has a frame with the same connection, row, seq
/// and subscription, and `receiver-work-authorized` holds with work to
/// judge on both streams.
///
/// This fails if the joins ignore the QUIC stream: frame 1's work on the
/// reopened stream then adds up with the retired stream's frame 1, past
/// the bytes either frame delivered.
#[test]
fn two_stream_incarnations_keep_their_work_apart() {
    let scenario = reopen();
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(support::inputs(2, 16_384));
    for plan in plans {
        let mut checker = work_checker();
        let report = run(&scenario, plan, &mut checker);
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

        // The joins: each page's work names its QUIC stream and joins one
        // frame there, though an overlapping seq has a frame on both.
        let mut admitted: BTreeMap<u64, BTreeSet<(u64, u64)>> = BTreeMap::new();
        for (at, entry) in entries.iter().enumerate() {
            let Observation::WorkAdmitted {
                connection,
                quic_stream,
                frame,
                seq,
                exchange,
                ends: false,
                ..
            } = &entry.obs
            else {
                continue;
            };
            let quic_stream = quic_stream.expect("wire work names its QUIC stream");
            let frames = |on: Option<u64>| {
                entries[..at]
                    .iter()
                    .filter(|before| {
                        matches!(
                            &before.obs,
                            Observation::PeerFrame {
                                connection: c,
                                stream,
                                quic_stream: q,
                                seq: s,
                                row: Some(row),
                                exchange: Some(e),
                                ..
                            } if c == connection && s == seq && e == exchange
                                && *stream == frame.stream && *row == frame.row
                                && on.is_none_or(|on| *q == Some(on))
                        )
                    })
                    .count()
            };
            assert_eq!(frames(Some(quic_stream)), 1, "{:?}", entry.obs);
            if quic_stream == quic[1] && overlap.contains(seq) {
                assert_eq!(frames(None), 2, "both streams have the frame");
            }
            admitted
                .entry(quic_stream)
                .or_default()
                .insert((*seq, *exchange));
        }
        assert_eq!(
            admitted.keys().copied().collect::<Vec<_>>(),
            quic,
            "work on both streams"
        );
        let shared: Vec<_> = admitted[&quic[0]]
            .intersection(&admitted[&quic[1]])
            .collect();
        assert!(!shared.is_empty(), "a seq and subscription on both");
        let id = format!("{FAMILY}/s5v9/headers/receiver-work-authorized");
        let outcome = &report
            .properties
            .iter()
            .find(|property| property.property == id)
            .unwrap_or_else(|| panic!("{id} ran: {:?}", report.properties))
            .outcome;
        assert!(
            matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
            "{outcome:?}"
        );
    }
}
