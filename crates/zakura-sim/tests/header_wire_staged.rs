//! M6a-5a: staged header work on `header-sync/wire`. Each page the node
//! stages is admitted work named by the frame it came in, which the
//! downstream `receiver-work-authorized` check judges against the peer's
//! labelled frames, and a prepare's take restarts the expected digest, so
//! held prepares and handoffs are never unframed work. A history a test
//! needs that never happened fails it.
#![cfg(zakura_sim)]

mod support;

use std::rc::Rc;

use zakura_network::zakura::v9::RegtestMessage;
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Policy,
        },
        header_sync::{HeaderWire, WireScenario, FAMILY},
        run_prepared,
        wire_plans::SavedPlan,
        HarnessError, Subsystem,
    },
    observe::{ReactorCall, ReactorFact, ReactorWorkState},
    peer::{honest::HonestSpec, PeerSpec},
    property::{Assumptions, HasView, Registry, RegistryChecker},
    table::{downstream_properties, Effect, StreamIdentity},
    Checker, MemorySink, NoChecks, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// Nothing to view: the downstream checks read the log alone.
struct Nothing;

impl HasView for Nothing {
    type View = ();

    fn view(&self) {}
}

/// The header family's `receiver-work-authorized` properties, one per
/// response row. The other downstream templates need peer-side credit rows
/// that `header-sync/wire` doesn't log yet.
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

/// An honest peer holding `chain` headers that dials the node at 50.
fn honest(chain: u32) -> WireScenario {
    WireScenario {
        node: zakura_sim::harness::header_node::Chain::main(50),
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

/// `receiver-work-authorized`'s outcome on `row` in `report`.
fn work_outcome<'r>(report: &'r RunReport, row: &str) -> &'r PropertyOutcome {
    let id = format!("{FAMILY}/s5v9/{row}/receiver-work-authorized");
    &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} ran: {:?}", report.properties))
        .outcome
}

/// On honest runs, each page the node stages is admitted work whose frame
/// is exactly one of the peer's page frames on the same connection, and
/// `receiver-work-authorized` holds on the page row with work to judge. No
/// held work is unframed.
///
/// This fails if a `Staged` names no frame.
#[test]
fn staged_work_names_its_frame() {
    let mut runs = vec![(honest(1_200), Plan::from_fuzz(&[]))];
    runs.extend(
        support::inputs(6, 256)
            .into_iter()
            .map(|input| (honest(400), input)),
    );
    let mut admitted_total = 0;
    for (scenario, input) in runs {
        let mut checker = work_checker();
        let report = run(&scenario, input, &mut checker);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let entries = report.log.entries();
        let mut admitted = 0;
        for (at, entry) in entries.iter().enumerate() {
            match &entry.obs {
                Observation::WorkAdmitted {
                    connection,
                    frame,
                    seq,
                    ends: false,
                    ..
                } => {
                    let frames = entries[..at]
                        .iter()
                        .filter(|before| {
                            matches!(
                                &before.obs,
                                Observation::PeerFrame {
                                    connection: c,
                                    stream,
                                    seq: s,
                                    row: Some(row),
                                    ..
                                } if c == connection && s == seq && *stream == frame.stream
                                    && *row == frame.row
                            )
                        })
                        .count();
                    assert_eq!(frames, 1, "{:?} joins one page frame", entry.obs);
                    admitted += 1;
                }
                Observation::UnframedWork { .. } => panic!("unframed: {:?}", entry.obs),
                _ => {}
            }
        }
        assert!(admitted > 0, "the node staged pages");
        admitted_total += admitted;
        assert!(
            matches!(
                work_outcome(&report, "headers"),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{:?}",
            work_outcome(&report, "headers")
        );
    }
    assert!(admitted_total > 0);
}

/// A hold on the `nth` `kind` operation until 1 s of virtual time has
/// passed.
fn held_for_a_second(kind: OpKind, nth: u32) -> Hold {
    Hold {
        select: Select::nth(kind, nth),
        until: Trigger::VirtualTime(1_000),
        then: vec![Allow::Engine],
        deadline_ms: Some(5_000),
    }
}

/// With the first prepare and the first apply each held for a second, the
/// node settles with a prepare held after its take, and again after a
/// handoff to an apply that hasn't completed. Its staged work's expected
/// digest restarts empty at each take, so neither boundary is unframed
/// work.
///
/// This fails if the take reads as a substitution.
#[test]
fn a_held_prepare_and_a_handoff_are_not_unframed() {
    let mut scenario = honest(1_200);
    scenario.policy = Policy {
        holds: vec![
            held_for_a_second(OpKind::Prepare, 1),
            held_for_a_second(OpKind::Apply, 1),
        ],
        ..Policy::converging()
    };
    let report = run(&scenario, Plan::from_fuzz(&[]), &mut NoChecks);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    // The settled snapshots that show an incarnation holding nothing while
    // a transfer it took is held in a prepare, then after the transfer
    // went to an apply and before that apply's call ends.
    let (mut held_prepare, mut handoff) = (0, 0);
    let mut taken = std::collections::BTreeMap::new();
    let mut prepares = std::collections::BTreeMap::new();
    let mut applies = std::collections::BTreeMap::new();
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::UnframedWork { .. } => panic!("unframed: {:?}", entry.obs),
            Observation::Reactor { event, .. } => match event {
                ReactorFact::EntriesTaken {
                    incarnation,
                    transfer,
                    ..
                } => {
                    taken.insert(*transfer, *incarnation);
                }
                ReactorFact::Dispatched {
                    call,
                    port: ReactorCall::Prepare { transfer, .. },
                } => {
                    prepares.insert(*call, taken[transfer]);
                }
                ReactorFact::CallEnded { call, .. } => {
                    prepares.remove(call);
                    applies.remove(call);
                }
                ReactorFact::TransferEnded {
                    transfer,
                    apply: Some(apply),
                    ..
                } => {
                    applies.insert(*apply, taken[transfer]);
                }
                ReactorFact::Settled {
                    snapshot: Some(snapshot),
                    ..
                } => {
                    for work in &snapshot.work {
                        let ReactorWorkState::Active {
                            incarnation,
                            held: 0,
                            held_digest,
                            ..
                        } = &work.state
                        else {
                            continue;
                        };
                        assert_eq!(held_digest.len, 0);
                        held_prepare +=
                            usize::from(prepares.values().any(|held| held == incarnation));
                        handoff += usize::from(applies.values().any(|held| held == incarnation));
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    assert!(
        held_prepare > 0,
        "a settled boundary falls while a prepare holds its take"
    );
    assert!(
        handoff > 0,
        "a settled boundary falls between a handoff and its apply's end"
    );
}

/// The saved Plans' runs admit staged work wherever they stage, and none
/// is unframed.
#[test]
fn saved_plans_have_no_unframed_work() {
    let mut admitted = 0;
    for plan in SavedPlan::ALL {
        let input = std::fs::read(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(format!("tests/fixtures/header-wire-{}.bin", plan.name())),
        )
        .expect("the fixture is committed");
        let (prefix, bytes) = input.split_at(HeaderWire::SCENARIO_BYTES);
        let scenario = HeaderWire::draw(prefix);
        let report = run(&scenario, Plan::from_fuzz(bytes), &mut NoChecks);
        for entry in report.log.entries() {
            match entry.obs {
                Observation::WorkAdmitted { .. } => admitted += 1,
                Observation::UnframedWork { .. } => {
                    panic!("{}: unframed: {:?}", plan.name(), entry.obs)
                }
                _ => {}
            }
        }
    }
    assert!(admitted > 0, "the saved Plans stage pages");
}
