//! M6a-1: the evidence hub on the header harnesses. Every source hands its
//! records to one writer, which logs them in the order they happened, by
//! the run-wide position each was emitted at. A storage fact published
//! during a reactor's turn sits between that turn's records, every row's
//! key grows, the storage policy's holds still release where they did, and
//! the saved Plans keep their end records. A history a test needs that
//! never happened fails it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, rc::Rc};

use zakura_network::zakura::v9::RegtestMessage;
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
    RunEnd, RunReport, StepCtx, Violation,
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
