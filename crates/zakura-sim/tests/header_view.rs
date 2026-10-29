//! M6a-1: the evidence hub on the header harnesses. Every source hands its
//! records to one writer, which logs them in the order they happened, by
//! the run-wide position each was emitted at. A storage fact published
//! during a reactor's turn sits between that turn's records, every row's
//! key grows, the storage policy's holds still release where they did, and
//! the saved Plans keep their end records. A history a test needs that
//! never happened fails it.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    fs,
    path::PathBuf,
    rc::Rc,
};

use zakura_network::zakura::{
    v9::RegtestMessage, ZAKURA_CAP_DISCOVERY, ZAKURA_HEADER_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_scheduler::{At, TaskId};
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, ControlAction, ControlStep, Fork, Policy,
        },
        header_sync::{Dialer, HeaderWire, MirrorSpec, Storage, StorageScenario, WireScenario},
        run_prepared,
        wire_plans::SavedPlan,
        HarnessError, Subsystem,
    },
    observe::{Entry, ReactorCall, ReactorFact, StorageFact, StorageRequest},
    property::{Assumptions, HasView, Property, Registry, RegistryChecker, Source, Sources},
    table::{sender_properties, table_properties},
    Checker, MemorySink, NoChecks, Observation, ObservationLog, Plan, PlanInput, PropertyReport,
    RunEnd, RunReport, Sim, StepCtx, TaskHold, Violation,
};
use zakura_sim::{
    model::is_fault_cause,
    observe::{ConnEnd, Direction, EmittedBody, StreamEnd, TransportFact},
    peer::{adversary::Act, honest::HonestSpec, PeerAction, PeerSpec},
};

/// Runs `S`'s `scenario` under `plan` with `extra` beside the harness's own
/// checker, and returns the report, even of a run that ended in a harness
/// failure, with its records.
fn run<S: Subsystem>(
    scenario: &S::Scenario,
    plan: PlanInput,
    extra: &mut dyn Checker,
) -> (RunReport, Vec<String>) {
    let mut sink = MemorySink::default();
    let ran = run_prepared::<S>(scenario, S::prepare(scenario), plan, &mut sink, Some(extra));
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    (report, sink.lines())
}

/// Saved Plan `plan`'s scenario and Plan, from its committed input.
fn saved(plan: SavedPlan) -> (WireScenario, PlanInput) {
    let input = fs::read(fixtures().join(format!("header-wire-{}.bin", plan.name())))
        .expect("the fixture is committed");
    let (prefix, bytes) = input.split_at(HeaderWire::SCENARIO_BYTES);
    (HeaderWire::draw(prefix), Plan::from_fuzz(bytes))
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The entries of each step of `log`, in order.
fn steps(log: &ObservationLog) -> Vec<&[Entry]> {
    log.entries().chunk_by(|a, b| a.step == b.step).collect()
}

/// The node on two mirrors, one on a fork, so its reactor sometimes submits
/// a storage call in the turn that handled the last one.
fn two_mirrors() -> StorageScenario {
    StorageScenario {
        node: Chain::main(2),
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(8),
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                chain: Chain {
                    main: 3,
                    fork: Some(Fork { branch: 1, len: 6 }),
                },
                dialer: Dialer::Node,
            },
        ],
        ..StorageScenario::default()
    }
}

/// A storage fact published during a reactor's turn is logged where it
/// happened against that turn's events, after an event that saw fewer facts
/// and before one that saw it. Each event's storage watermark is the
/// independent record of that order. The runs must show a fact taken
/// before a reactor event in its step, a reply the reactor then handles,
/// and a fact after one, a submission in the turn that handled the last
/// call. No run shows both in one step.
#[test]
fn a_storage_fact_and_a_node_record_keep_their_order() {
    let (mut before, mut after) = (0, 0);
    for input in support::inputs(24, 512) {
        let (report, _) = run::<Storage>(&two_mirrors(), input, &mut NoChecks);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        for step in steps(&report.log) {
            let reactor = step
                .iter()
                .enumerate()
                .filter_map(|(at, entry)| match entry.obs {
                    Observation::Reactor { storage_seq, .. } => Some((at, storage_seq)),
                    _ => None,
                });
            for (event, watermark) in reactor {
                for (at, entry) in step.iter().enumerate() {
                    let Observation::Storage {
                        instance: 0, seq, ..
                    } = entry.obs
                    else {
                        continue;
                    };
                    let happened_before = seq <= watermark;
                    assert_eq!(
                        at < event,
                        happened_before,
                        "step {}: fact {seq} at {at}, a reactor event that saw {watermark} at \
                         {event}",
                        entry.step
                    );
                    match happened_before {
                        true => before += 1,
                        false => after += 1,
                    }
                }
            }
        }
    }
    assert!(
        before > 0,
        "no fact came before a reactor event in its step"
    );
    assert!(after > 0, "no fact came after a reactor event in its step");
}

/// Records, per step, every storage completion the run offered.
#[derive(Default)]
struct Offered(Vec<(u64, Vec<String>)>);

impl Offered {
    /// The first step that offered `storage:header:<id>:engine`.
    fn first(&self, id: u64) -> Option<u64> {
        let identity = format!("storage:header:{id}:engine");
        self.0
            .iter()
            .find(|(_, offered)| offered.contains(&identity))
            .map(|(step, _)| *step)
    }
}

impl Checker for Offered {
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let offered = cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .filter(|identity| identity.starts_with("storage:"))
            .collect();
        self.0.push((cx.step, offered));
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
}

/// `kind`'s operations of session `session`.
fn of(kind: OpKind, session: u64) -> Select {
    Select {
        kind: Some(kind),
        session: Some(session),
        ..Select::default()
    }
}

/// A hold on `select` until `until`, then the engine, within
/// `deadline_ms` of its submission.
fn hold(select: Select, until: Trigger, deadline_ms: u64) -> Hold {
    Hold {
        select,
        until,
        then: vec![Allow::Engine],
        deadline_ms: Some(deadline_ms),
    }
}

/// M4.3's replacement while a prepare is pending: session 1's prepare is
/// held until session 2's is submitted, and session 2's prepare and apply
/// until the reactor has handled session 1's. `mirror-0` relinks as
/// session 2 once session 1's prepare is submitted.
fn replacement() -> StorageScenario {
    let handled = Trigger::Handled(of(OpKind::Prepare, 1));
    StorageScenario {
        policy: Policy {
            holds: vec![
                hold(
                    of(OpKind::Prepare, 1),
                    Trigger::Submitted(of(OpKind::Prepare, 2)),
                    20_000,
                ),
                hold(of(OpKind::Prepare, 2), handled.clone(), 25_000),
                hold(of(OpKind::Apply, 2), handled, 5_000),
            ],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Relink,
            when: Some(Trigger::Submitted(of(OpKind::Prepare, 1))),
        }],
        ..StorageScenario::default()
    }
}

/// The ID and step of each prepare the node submitted, by its session.
fn prepares(log: &ObservationLog) -> Vec<(u64, u64, u64)> {
    log.entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { owner, .. },
                        ..
                    },
                ..
            } => Some((owner.session_id, *id, entry.step)),
            _ => None,
        })
        .collect()
}

/// A hold whose trigger is a storage fact opens at the step after the
/// fact, and one whose trigger is a link change at the step after the
/// change, as before the hub, which now feeds the policy's predicates.
/// Session 1's prepare is offered first the step after session 2's
/// submission. The node's first locator, held until `mirror-0`'s link
/// closes, is offered first at the step the base build first offered it.
#[test]
fn storage_and_link_holds_still_fire() {
    let mut offered = Offered::default();
    let (report, _) = run::<Storage>(&replacement(), Plan::from_fuzz(&[]), &mut offered);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let prepares = prepares(&report.log);
    let [(1, first, _), (2, _, submitted)] = prepares[..] else {
        panic!("one prepare per session: {prepares:?}");
    };
    assert_eq!(offered.first(first), Some(submitted + 1), "{prepares:?}");

    let locator = Select::nth(OpKind::Locator, 1);
    let closing = StorageScenario {
        policy: Policy {
            holds: vec![hold(
                locator.clone(),
                Trigger::Closed("mirror-0".to_owned()),
                20_000,
            )],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Close,
            when: Some(Trigger::Submitted(locator)),
        }],
        ..StorageScenario::default()
    };
    let mut offered = Offered::default();
    let (report, _) = run::<Storage>(&closing, Plan::from_fuzz(&[]), &mut offered);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let close = report
        .picks
        .iter()
        .find(|pick| pick.identity == "peer:mirror-0:close")
        .expect("the close is picked once the locator is submitted")
        .step;
    let locator = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::ContinuationLocator,
                        ..
                    },
                ..
            } => Some(*id),
            _ => None,
        })
        .expect("the node asks for a locator");
    let released = offered.first(locator).expect("the held locator is offered");
    assert!(released > close, "held until the close, at {close}");
    assert_eq!(released, LINK_RELEASE, "the base build's step");
}

/// The step the base build, before the hub, first offered the held
/// locator in [`storage_and_link_holds_still_fire`].
const LINK_RELEASE: u64 = 176;

/// M4's `Handled`-triggered hold in the replacement history: session 2's
/// prepare, held until the reactor handles session 1's, is offered first
/// the step after that `Handled` event is logged.
#[test]
fn a_hold_released_on_handled_still_fires() {
    let mut offered = Offered::default();
    let (report, _) = run::<Storage>(&replacement(), Plan::from_fuzz(&[]), &mut offered);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let prepares = prepares(&report.log);
    let [(1, _, _), (2, second, submitted)] = prepares[..] else {
        panic!("one prepare per session: {prepares:?}");
    };
    let entries = report.log.entries();
    let call = entries
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Reactor {
                event:
                    ReactorFact::Dispatched {
                        call,
                        port: ReactorCall::Prepare { owner, .. },
                    },
                ..
            } if owner.session_id == 1 => Some(*call),
            _ => None,
        })
        .expect("session 1's prepare is dispatched");
    let handled = entries
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Reactor {
                event: ReactorFact::Handled { call: handled, .. },
                ..
            } if *handled == call => Some(entry.step),
            _ => None,
        })
        .expect("the reactor handles session 1's prepare");
    assert!(
        submitted <= handled,
        "session 2's prepare waits for the trigger"
    );
    assert_eq!(offered.first(second), Some(handled + 1));
}

/// A view-less harness, for the generated properties, which read only the
/// log.
struct Logged;

impl HasView for Logged {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

/// Every generated receiver and sender property of header sync's table, as
/// the saved Plans' pinned end records check them.
fn table_checker() -> RegistryChecker<Logged> {
    let mut properties: Vec<Property<Logged>> =
        table_properties("header-sync", &RegtestMessage::LAYOUT);
    properties.extend(sender_properties("header-sync", &RegtestMessage::LAYOUT));
    RegistryChecker::new(
        Registry::new(properties).expect("generated IDs are unique"),
        Rc::new(Logged),
        Assumptions::none(),
        &Sources::observed(&[Source::Frames, Source::Closes, Source::Storage]),
    )
}

/// Whether an observation is of one kind.
type Kind = fn(&Observation) -> bool;

/// The 14 saved header-wire Plans keep their pinned end records through the
/// hub, each run's keys grow through its whole log, and the runs log the
/// records of every source the harness has: the network, storage, the
/// reactor, the transport and the peers, with the rows derived from them.
#[test]
fn existing_runs_keep_their_records() {
    let mut seen = [false; 7];
    for plan in SavedPlan::ALL {
        let pinned =
            fs::read_to_string(fixtures().join(format!("header-wire-{}.end.json", plan.name())))
                .expect("the fixture is committed");
        let (scenario, input) = saved(plan);
        let (report, lines) = run::<HeaderWire>(&scenario, input, &mut table_checker());
        let entries = report.log.entries();
        let kinds: [(&str, Kind); 7] = [
            ("datagram", |obs| {
                matches!(obs, Observation::Datagram { .. })
            }),
            ("storage", |obs| matches!(obs, Observation::Storage { .. })),
            ("storage-state", |obs| {
                matches!(obs, Observation::StorageState { .. })
            }),
            ("reactor", |obs| matches!(obs, Observation::Reactor { .. })),
            ("transport", |obs| {
                matches!(obs, Observation::Transport { .. })
            }),
            ("peer-frame", |obs| {
                matches!(obs, Observation::PeerFrame { .. })
            }),
            ("frame-judged", |obs| {
                matches!(obs, Observation::FrameJudged { .. })
            }),
        ];
        for pair in entries.windows(2) {
            assert!(
                (pair[0].pos, pair[0].sub) < (pair[1].pos, pair[1].sub),
                "{}: keys out of order: {pair:?}",
                plan.name()
            );
        }
        for ((_, pred), seen) in kinds.iter().zip(&mut seen) {
            *seen |= entries.iter().any(|entry| pred(&entry.obs));
        }
        // Every run has a network, a transport and a peer, whether or not
        // its node reaches storage.
        for (kind, pred) in [kinds[0], kinds[4], kinds[5]] {
            assert!(
                entries.iter().any(|entry| pred(&entry.obs)),
                "{}: no {kind} row",
                plan.name()
            );
        }
        assert_eq!(
            lines.last().map(String::as_str),
            Some(pinned.trim_end()),
            "{}: the end record changed",
            plan.name()
        );
    }
    assert_eq!(seen, [true; 7], "the saved Plans log every source's rows");
}

/// An honest peer holding 120 headers that dials the node at 50, with
/// `script`.
fn honest(script: Vec<PeerAction>) -> WireScenario {
    WireScenario {
        node: Chain::main(50),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: None,
            }),
            script,
            ..PeerSpec::dialing()
        }],
        ..WireScenario::default()
    }
}

/// The honest peer writes its first frame as soon as its stream opens,
/// steps before the node admits the stream, with other node records in
/// between. The frame's `peer-emitted` row is in its own step, and its
/// resolved `peer-frame` names that row's position and lands before the
/// node's `header-read` of it. Each step's digest is the same on replay.
#[test]
fn a_delayed_peer_record_is_logged_in_its_own_step() {
    let (report, lines) =
        run::<HeaderWire>(&honest(Vec::new()), Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let first =
        |pred: &dyn Fn(&Observation) -> bool| entries.iter().position(|entry| pred(&entry.obs));
    let emitted = first(&|obs| {
        matches!(
            obs,
            Observation::PeerEmitted {
                body: EmittedBody::Frame { seq: 0, .. },
                ..
            }
        )
    })
    .expect("the peer writes a frame");
    let admitted = first(&|obs| matches!(obs, Observation::Session { .. }))
        .expect("the node admits the stream");
    let (written, admission) = (entries[emitted].step, entries[admitted].step);
    assert!(
        written < admission,
        "written at {written}, admitted at {admission}"
    );
    let between = entries[emitted..admitted]
        .iter()
        .filter(|entry| entry.step > written && entry.step < admission)
        .any(|entry| {
            matches!(
                entry.obs,
                Observation::Transport { .. } | Observation::Datagram { .. }
            )
        });
    assert!(
        between,
        "node records come between the write and the admission"
    );
    let frame = first(&|obs| matches!(obs, Observation::PeerFrame { seq: 0, .. }))
        .expect("the frame resolves");
    assert_eq!(entries[frame].origin, Some(entries[emitted].pos));
    assert_eq!(
        entries[frame].step, admission,
        "it resolves at the admission"
    );
    let read = first(&|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::HeaderRead { seq: 0, .. },
                ..
            }
        )
    })
    .expect("the node reads the frame");
    assert!(frame < read);
    let (_, replayed) = run::<HeaderWire>(&honest(Vec::new()), Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(lines, replayed, "every step's digest replays");
}

/// When the node faults the peer's session, the peer sees its stream reset
/// with code 0 and its connection closed with the node's code 0, the codes
/// the node's close sends, never values from the node's decision. A peer
/// that closes the connection itself sees its own close.
#[test]
fn the_peer_sees_the_close() {
    let (scenario, _) = saved(SavedPlan::Act(Act::OverCreditHeader));
    let (report, _) = run::<HeaderWire>(&scenario, Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let rows: Vec<&Observation> = report
        .log
        .entries()
        .iter()
        .map(|entry| &entry.obs)
        .collect();
    assert!(
        rows.iter().any(|obs| matches!(obs, Observation::ConnectionClosed { cause, .. } if is_fault_cause(cause))),
        "the node faults the session"
    );
    assert!(rows.iter().any(|obs| matches!(
        obs,
        Observation::PeerStreamEnded {
            direction: Direction::Receive,
            end: StreamEnd::Reset { code: 0 },
            ..
        }
    )));
    let ends: Vec<&ConnEnd> = rows
        .iter()
        .filter_map(|obs| match obs {
            Observation::PeerConnEnded { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert!(!ends.is_empty(), "the peer sees the close");
    assert!(
        ends.iter()
            .all(|reason| **reason == ConnEnd::ReceivedClose { code: 0 }),
        "{ends:?}"
    );

    let (report, _) = run::<HeaderWire>(
        &honest(vec![PeerAction::Close]),
        Plan::from_fuzz(&[]),
        &mut NoChecks,
    );
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let ends: Vec<&ConnEnd> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::PeerConnEnded { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert!(!ends.is_empty(), "the peer sees its own close");
    assert!(
        ends.iter().all(|reason| **reason == ConnEnd::LocalClose),
        "{ends:?}"
    );
}

/// Once the peer finishes its half of the header stream it has nothing
/// more to write, and the node resets the peer's receiving half. The peer's
/// reader reports the reset with its code while the connection lives, long
/// before the idle timeout closes it.
#[test]
fn a_reset_reaches_the_reader_without_a_write() {
    let (report, _) = run::<HeaderWire>(
        &honest(vec![PeerAction::FinishHeader]),
        Plan::from_fuzz(&[]),
        &mut NoChecks,
    );
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let finished = report
        .picks
        .iter()
        .find(|pick| pick.identity == "peer:peer-0:finish-header")
        .expect("the peer finishes its header stream")
        .step;
    let wrote_after = entries.iter().any(|entry| {
        entry.step > finished
            && matches!(
                entry.obs,
                Observation::PeerEmitted {
                    body: EmittedBody::Frame { .. },
                    ..
                }
            )
    });
    assert!(!wrote_after, "the peer writes nothing after it finishes");
    let reset = entries
        .iter()
        .position(|entry| {
            matches!(
                entry.obs,
                Observation::PeerStreamEnded {
                    direction: Direction::Receive,
                    end: StreamEnd::Reset { .. },
                    ..
                }
            )
        })
        .expect("the reader reports the reset");
    let lost = entries.iter().position(|entry| {
        matches!(
            entry.obs,
            Observation::PeerConnEnded { .. } | Observation::ConnectionClosed { .. }
        )
    });
    assert!(
        lost.is_none_or(|lost| reset < lost),
        "{lost:?} before {reset}"
    );
}

/// A frame the peer's reader finished is received at the read's boundary,
/// with the frame it read, whether the peer's task handles it in a later
/// step or never: its `peer-emitted` receipt holds the frame's flags and
/// payload digest, and the stream is admitted already, so its one
/// `peer-received` and one `node-frame` resolve in the receipt's own step,
/// naming it. The never-handled history is the second run's (see
/// [`HoldReceipt`]).
#[test]
fn a_received_frame_keeps_its_read_boundary() {
    let mut scenario = honest(vec![PeerAction::Disconnect]);
    scenario.peers[0]
        .honest
        .as_mut()
        .expect("the peer is honest")
        .chain = 1_200;
    let state = Rc::new(HoldState::default());
    let mut sim = Sim::new(HeaderWire::config(&scenario), Plan::from_fuzz(&[]));
    let _node = HeaderWire::build(&mut sim, &scenario, HeaderWire::prepare(&scenario));
    sim.hold_tasks(Box::new(HoldReceipt(state.clone())));
    let report = sim.run(&mut Watch(state.clone()), &mut MemorySink::default());
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let ended = entries
        .iter()
        .position(|entry| {
            matches!(
                entry.obs,
                Observation::PeerEmitted {
                    body: EmittedBody::ConnEnded { .. },
                    ..
                }
            )
        })
        .expect("the peer disconnects");
    let unhandled: Vec<&Entry> = entries[..ended]
        .iter()
        .filter(|entry| match &entry.obs {
            Observation::PeerEmitted {
                body: EmittedBody::Received { seq, .. },
                ..
            } => !entries.iter().any(|later| {
                matches!(later.obs, Observation::PeerEmitted {
                    body: EmittedBody::Handled { seq: s }, ..
                } if s == *seq)
            }),
            _ => false,
        })
        .collect();
    let [receipt] = unhandled[..] else {
        panic!("one frame is never handled: {unhandled:?}");
    };
    let resolved: Vec<&Observation> = entries
        .iter()
        .filter(|row| row.origin == Some(receipt.pos) && row.step == receipt.step)
        .map(|row| &row.obs)
        .collect();
    assert!(
        matches!(
            resolved[..],
            [
                Observation::PeerReceived { .. },
                Observation::NodeFrame { .. }
            ]
        ),
        "the never-handled frame resolves at its read: {resolved:?}"
    );

    let (report, _) = run::<HeaderWire>(&honest(Vec::new()), Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let mut later = 0;
    let mut digests = std::collections::BTreeMap::new();
    for (at, entry) in entries.iter().enumerate() {
        let Observation::PeerEmitted {
            remote,
            quic_stream,
            body:
                EmittedBody::Received {
                    seq,
                    message_type,
                    payload,
                    digest,
                    ..
                },
        } = &entry.obs
        else {
            continue;
        };
        assert_eq!(digest.len(), 16, "the receipt holds its payload's digest");
        digests
            .entry((*message_type, *payload))
            .or_insert_with(std::collections::BTreeSet::new)
            .insert(digest.clone());
        let resolved: Vec<&Entry> = entries
            .iter()
            .filter(|row| row.origin == Some(entry.pos))
            .collect();
        assert!(
            matches!(
                resolved[..],
                [
                    Entry {
                        obs: Observation::PeerReceived { seq: s, .. },
                        ..
                    },
                    Entry {
                        obs: Observation::NodeFrame { seq: n, .. },
                        ..
                    },
                ] if s == seq && n == seq
            ),
            "frame {seq}: {resolved:?}"
        );
        assert!(
            resolved.iter().all(|row| row.step == entry.step),
            "frame {seq} resolves at its read"
        );
        let handled = entries[at..]
            .iter()
            .find(|later| {
                matches!(&later.obs, Observation::PeerEmitted {
                    remote: r, quic_stream: q, body: EmittedBody::Handled { seq: s },
                } if (r, q, s) == (remote, quic_stream, seq))
            })
            .expect("the peer's task handles every frame in this run");
        later += usize::from(handled.step > entry.step);
    }
    assert!(later > 0, "no frame was handled after its read's step");
    assert!(
        digests.values().any(|seen| seen.len() > 1),
        "two frames of one type and length have different digests"
    );
}

/// What [`HoldReceipt`] holds on, which [`Watch`] keeps up to date.
#[derive(Default)]
struct HoldState {
    /// The plan has queued the peer's disconnect, and it hasn't ended yet.
    disconnecting: Cell<bool>,
    /// The receipts the peer's task hasn't handled, by stream and `seq`.
    waiting: RefCell<BTreeSet<(Option<u64>, u64)>>,
}

/// Once the peer's disconnect is queued, holds its task until its reader
/// has recorded a frame the task hasn't handled, then holds the reader at
/// its channel send, the point right after the receipt, while the task
/// disconnects and drops the channel. So the frame is read and never
/// handled.
struct HoldReceipt(Rc<HoldState>);

impl TaskHold for HoldReceipt {
    /// Holds a point of the peer module's tasks: the reader's send while a
    /// receipt waits, and any other point until one does.
    fn holds(&self, _: TaskId, at: &At) -> bool {
        let peer = at
            .site
            .is_some_and(|site| site.file().ends_with("zakura-sim/src/peer/mod.rs"));
        peer && self.0.disconnecting.get()
            && (at.op.name() == "send") == !self.0.waiting.borrow().is_empty()
    }
}

/// Keeps [`HoldState`] up to date from each step's rows.
struct Watch(Rc<HoldState>);

impl Checker for Watch {
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        if cx.pick.to_string() == "peer:peer-0:disconnect" {
            self.0.disconnecting.set(true);
        }
        let mut waiting = self.0.waiting.borrow_mut();
        for obs in cx.observations() {
            match obs {
                Observation::PeerEmitted {
                    quic_stream,
                    body: EmittedBody::Received { seq, .. },
                    ..
                } => {
                    waiting.insert((*quic_stream, *seq));
                }
                Observation::PeerEmitted {
                    quic_stream,
                    body: EmittedBody::Handled { seq },
                    ..
                } => {
                    waiting.remove(&(*quic_stream, *seq));
                }
                Observation::PeerEmitted {
                    body: EmittedBody::ConnEnded { .. },
                    ..
                } => self.0.disconnecting.set(false),
                _ => {}
            }
        }
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        Vec::new()
    }
}

/// A peer that didn't negotiate header sync opens a header stream, and the
/// node refuses it while the connection lives. The peer's scripted open
/// records how the stream ended, on a stream the node never admits, so it
/// keeps only its `peer-emitted` row.
#[test]
fn a_refused_open_keeps_its_end() {
    let scenario = WireScenario {
        peers: vec![PeerSpec {
            capabilities: ZAKURA_CAP_DISCOVERY,
            script: vec![PeerAction::Open {
                kind: ZAKURA_STREAM_HEADER_SYNC,
                version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
            }],
            ..PeerSpec::dialing()
        }],
        ..WireScenario::default()
    };
    let (report, _) = run::<HeaderWire>(&scenario, Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let entries = report.log.entries();
    let refused = entries
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Transport {
                event: TransportFact::StreamRefused { quic_stream, .. },
                ..
            } => Some(*quic_stream),
            _ => None,
        })
        .expect("the node refuses the stream");
    let ended = entries
        .iter()
        .position(|entry| {
            matches!(&entry.obs, Observation::PeerEmitted {
                quic_stream: Some(q),
                body: EmittedBody::StreamEnded { direction: Direction::Receive, .. },
                ..
            } if *q == refused)
        })
        .expect("the peer records the refused stream's end");
    assert!(
        !entries
            .iter()
            .any(|entry| entry.origin == Some(entries[ended].pos)),
        "an unadmitted stream's end doesn't resolve"
    );
    assert!(
        !entries[..ended]
            .iter()
            .any(|entry| matches!(entry.obs, Observation::PeerConnEnded { .. })),
        "the connection lives"
    );
}
