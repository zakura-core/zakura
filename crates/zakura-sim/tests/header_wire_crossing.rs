//! The three crossing histories on `header-sync/wire`.
//!
//! Each history sends one `subscribe-headers` update for an unretained
//! `Open`, which the node ends at once, on a slow link from the node, so the
//! update meets the node's ending in flight:
//!
//! - the saved `outcome-crossing` Plan's one-page `Grant`;
//! - a `Close`;
//! - a `Grant` whose credit, on top of the `Open`'s page, is above the
//!   row's window.
//!
//! fuzz-13's [`crossings`] finds the crossing. Each test then checks the
//! node's consequence for the update the act wrote. The first two updates
//! are conformant, and the node doesn't punish their connection. The third
//! is `bad-cursor`, and the node closes its connection with a fault cause
//! after it judges the update.
#![cfg(zakura_sim)]

mod support;

use zakura_network::zakura::{v9::RegtestMessage, Credit, MessageRole};
use zakura_sim::{
    explore::state::crossing::{crossings, Crossed},
    harness::{
        header_sync::{HeaderWire, WireScenario},
        run_prepared,
        wire_plans::{act_scenario, SavedPlan},
        HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::StreamId,
    peer::adversary::{Act, Row},
    seed, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The ID of header sync's generated `conformant-never-closes`.
const NEVER_CLOSES: &str = "header-sync/s5v9/conformant-never-closes";

/// The ID of `subscribe-headers`' generated `bad-cursor-closes`.
const BAD_CURSOR_CLOSES: &str = "header-sync/s5v9/subscribe-headers/bad-cursor-closes";

/// One peer frame: its connection, stream, QUIC stream and sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Frame {
    connection: u64,
    stream: StreamId,
    quic_stream: u64,
    seq: u64,
}

/// Runs `scenario` under `plan`, and returns the report even of a run that
/// ended in a harness failure.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    let ports = HeaderWire::prepare(scenario);
    let ran = run_prepared::<HeaderWire>(scenario, ports, plan, &mut MemorySink::default(), None);
    match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// The saved `outcome-crossing` Plan's seed, run as `zakura-sim record
/// --seed` runs it: its tag picks the scenario, and the rest is the Plan.
fn saved_outcome_crossing() -> RunReport {
    let plan = SavedPlan::Act(Act::OutcomeCrossing);
    let name = format!("header-wire-{}", plan.name());
    let seed = seed::find(&name).unwrap_or_else(|| panic!("no seed is named {name}"));
    let (prefix, rest) = seed.input().split_at(HeaderWire::SCENARIO_BYTES);
    assert_eq!(prefix, plan.tag(), "the seed is the saved Plan's");
    let scenario = HeaderWire::draw(prefix);
    assert_eq!(scenario, plan.scenario());
    run(&scenario, Plan::from_fuzz(rest))
}

/// `act`'s crossing history: [`act_scenario`]'s setup, an unretained `Open`
/// then `act`, under the empty Plan.
fn crossing_history(act: Act) -> RunReport {
    run(&act_scenario(act), Plan::from_fuzz(&[]))
}

/// A `Grant` adding the whole `subscribe-headers` window, so the `Open`'s
/// page and it are above the window.
fn over_window() -> Act {
    let rule = RegtestMessage::ROWS
        .iter()
        .find(|rule| rule.name == Row::SubscribeHeaders.name())
        .expect("header sync has a subscribe-headers row");
    let MessageRole::Subscription {
        credit: Credit { objects, bytes },
        ..
    } = rule.role
    else {
        panic!("subscribe-headers is a subscription row");
    };
    Act::CrossingGrant { objects, bytes }
}

/// `report`'s observations, in log order.
fn observations(report: &RunReport) -> Vec<Observation> {
    report
        .log
        .entries()
        .iter()
        .map(|entry| entry.obs.clone())
        .collect()
}

/// The frame `act` wrote: the peer's last frame, before the act's `ActRan`,
/// on the connection and QUIC stream that `ActRan` names. The peer logs
/// `ActRan` once it has written the act's frames.
fn act_frame(observations: &[Observation], act: Act) -> Option<Frame> {
    let (ran, on) =
        observations
            .iter()
            .enumerate()
            .find_map(|(at, observation)| match observation {
                Observation::ActRan {
                    connection,
                    quic_stream,
                    act: name,
                } if name == act.name() => Some((at, (*connection, *quic_stream))),
                _ => None,
            })?;
    observations[..ran]
        .iter()
        .rev()
        .find_map(|observation| match *observation {
            Observation::PeerFrame {
                connection,
                stream,
                quic_stream: Some(quic_stream),
                seq,
                ..
            } if (connection, quic_stream) == on => Some(Frame {
                connection,
                stream,
                quic_stream,
                seq,
            }),
            _ => None,
        })
}

/// The one update that crossed the node's ending in `observations`. It
/// must carry `label` and be the frame `act` wrote.
fn crossed_act(observations: &[Observation], act: Act, label: Label) -> Frame {
    let found = crossings(observations);
    let [Crossed {
        connection,
        quic_stream,
        seq,
        label: crossed,
        ..
    }] = found[..]
    else {
        panic!("one crossed update, not {found:?}");
    };
    assert_eq!(crossed, label, "the crossed update's label");
    let written = act_frame(observations, act).unwrap_or_else(|| panic!("{act:?} wrote no frame"));
    assert_eq!(
        (connection, quic_stream, seq),
        (written.connection, written.quic_stream, written.seq),
        "the crossed update is the frame {act:?} wrote"
    );
    written
}

/// Where the node's `FrameJudged` of `frame` sits in `observations`.
fn judged_at(observations: &[Observation], frame: &Frame) -> usize {
    observations
        .iter()
        .position(|observation| {
            matches!(
                *observation,
                Observation::FrameJudged {
                    connection,
                    stream,
                    quic_stream: Some(quic_stream),
                    seq,
                } if Frame { connection, stream, quic_stream, seq } == *frame
            )
        })
        .unwrap_or_else(|| panic!("{frame:?} was never judged"))
}

/// The connection `observation` punishes, if it's a close with a fault
/// cause or a `Misbehavior`.
fn punished(observation: &Observation) -> Option<u64> {
    match observation {
        Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
            Some(*connection)
        }
        Observation::Misbehavior { connection } => Some(*connection),
        _ => None,
    }
}

/// Requires `id` held in `report`, applied at least once.
fn held_applied(report: &RunReport, id: &str) {
    let outcome = &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} is checked"))
        .outcome;
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{id} is {outcome:?}"
    );
}

/// Checks `act`'s conformant crossing history: the run ends with no harness
/// failure and no violation, and `act`'s update is the one that crossed the
/// node's ending, labelled conformant. The node judged the update and
/// punished nothing on its connection after that, and
/// `conformant-never-closes` held, applied.
fn conformant_crossing(report: &RunReport, act: Act) {
    assert_eq!(act.label(), Label::Conformant, "{act:?}");
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let observations = observations(report);
    let update = crossed_act(&observations, act, Label::Conformant);
    let judged = judged_at(&observations, &update);
    let punishment = observations[judged..]
        .iter()
        .find(|observation| punished(observation) == Some(update.connection));
    assert_eq!(punishment, None, "{update:?}");
    held_applied(report, NEVER_CLOSES);
}

/// `Act::UnretainedOpen` then `Act::OutcomeCrossing`, from the saved
/// `outcome-crossing` Plan: the one-page `Grant` crosses the node's ending,
/// labelled conformant, and draws no punishment.
#[test]
fn an_unretained_open_then_outcome_crossing_is_conformant_and_unpunished() {
    conformant_crossing(&saved_outcome_crossing(), Act::OutcomeCrossing);
}

/// A subscription `Close` for the unretained `Open` crosses the node's
/// ending, labelled conformant, and draws no punishment.
#[test]
fn a_crossing_close_is_conformant_and_unpunished() {
    conformant_crossing(&crossing_history(Act::CrossingClose), Act::CrossingClose);
}

/// A `Grant` above the window crosses the node's ending, labelled
/// `bad-cursor`. The connection's first close comes after the node judged
/// the update, with a fault cause, and `bad-cursor-closes` held, applied.
#[test]
fn an_over_window_crossing_grant_is_bad_cursor_and_closed() {
    let report = crossing_history(over_window());
    assert_eq!(over_window().label(), Label::BadCursor);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let observations = observations(&report);
    let update = crossed_act(&observations, over_window(), Label::BadCursor);
    let judged = judged_at(&observations, &update);
    let (closed, cause) = observations
        .iter()
        .enumerate()
        .find_map(|(at, observation)| match observation {
            Observation::ConnectionClosed { connection, cause }
                if *connection == update.connection =>
            {
                Some((at, cause))
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("connection {} never closed", update.connection));
    assert!(closed > judged, "closed with {cause} before the judgment");
    assert!(is_fault_cause(cause), "closed with {cause}");
    held_applied(&report, BAD_CURSOR_CLOSES);
}
