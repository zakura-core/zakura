//! hdr-14 item 3, gap 7: gate 3's three crossing histories on
//! `header-sync/wire`.
//!
//! Each history sends one `subscribe-headers` update for an unretained
//! `Open`, which the node ends at once, on a slow link from the node, so the
//! update meets the node's ending in flight: the saved `outcome-crossing`
//! Plan's one-page `Grant`, a `Close`, and a `Grant` whose credit, on top of
//! the `Open`'s page, is above the row's window. fuzz-13's
//! [`crossings`] finds the crossing, joined on the connection, QUIC stream,
//! frame sequence and subscription, and ordered by the run's wire stamps,
//! and names the update by its full [`FrameRef`]. It
//! reports only an update the node judged, after an ending of the same
//! subscription that the node wrote before the update's write returned and
//! the peer read after it. Its unit tests carry the order controls: an
//! ending written after the update, an unrelated ending around it, an
//! unjudged update and a read logged late.
//!
//! Each history then binds the update's label and consequence to the
//! crossing it returns, joining its judgment on that whole ref (Rule 25).
//! The first two are conformant and draw no punishment on their
//! connection. The third is `bad-cursor`, and the first close of its
//! connection comes after its judgment, with a fault cause. The controls
//! move each key of the update off by one, move its judgment to another
//! stream kind or version, drop its judgment, and put the close on an
//! unrelated fault: a close of another connection, or one decided before
//! the judgment. Each must fail.
#![cfg(zakura_sim)]

mod support;

use std::path::Path;

use support::one_key_off::assert_one_key_off;
use zakura_network::zakura::{v9::RegtestMessage, Credit, MessageRole};
use zakura_sim::{
    explore::state::crossing::{crossings, Crossed},
    harness::{
        header_acceptance::{
            companions::judged_frame,
            refs::{ConnectionRef, FrameRef, QuicStreamRef, Ref},
        },
        header_sync::{HeaderWire, WireScenario},
        run_prepared,
        wire_plans::{act_scenario, SavedPlan},
        HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    peer::adversary::{Act, Row},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The ID of header sync's generated `conformant-never-closes`.
const NEVER_CLOSES: &str = "header-sync/s5v9/conformant-never-closes";

/// The ID of `subscribe-headers`' generated `bad-cursor-closes`.
const BAD_CURSOR_CLOSES: &str = "header-sync/s5v9/subscribe-headers/bad-cursor-closes";

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

/// The saved `outcome-crossing` Plan's committed input, run as
/// `zakura-sim record --input` runs it: its tag picks the scenario, and the
/// rest is the Plan.
fn saved_outcome_crossing() -> RunReport {
    let plan = SavedPlan::Act(Act::OutcomeCrossing);
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/fixtures/header-wire-{}.bin", plan.name()));
    let input = std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let (prefix, rest) = input.split_at(HeaderWire::SCENARIO_BYTES);
    assert_eq!(prefix, plan.tag(), "the fixture is the saved Plan's");
    let scenario = HeaderWire::draw(prefix);
    assert_eq!(scenario, plan.scenario());
    run(&scenario, Plan::from_fuzz(rest))
}

/// `act`'s crossing history: [`act_scenario`]'s setup, an unretained `Open`
/// then `act`, under the empty Plan.
fn crossing_history(act: Act) -> RunReport {
    run(&act_scenario(act), Plan::from_fuzz(&[]))
}

/// The credit window of `subscribe-headers`' subscription role.
fn window() -> Credit {
    let rule = RegtestMessage::ROWS
        .iter()
        .find(|rule| rule.name == Row::SubscribeHeaders.name())
        .expect("header sync has a subscribe-headers row");
    let MessageRole::Subscription { credit, .. } = rule.role else {
        panic!("subscribe-headers is a subscription row");
    };
    credit
}

/// A `Grant` adding the whole window, so the `Open`'s page and it are
/// above the window.
fn over_window() -> Act {
    let window = window();
    Act::CrossingGrant {
        objects: window.objects,
        bytes: window.bytes,
    }
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

/// The frame `act` wrote: the peer's last frame on the stream its
/// `ActRan` names, before that `ActRan`, which the peer logs once it has
/// written the act's frames, as its `PeerFrame` names it.
fn act_frame(observations: &[Observation], act: Act) -> Option<FrameRef> {
    let (ran, stream) = observations
        .iter()
        .enumerate()
        .find_map(|(at, observation)| match observation {
            Observation::ActRan {
                connection,
                quic_stream,
                act: name,
            } if name == act.name() => Some((
                at,
                QuicStreamRef {
                    connection: *connection,
                    quic_stream: *quic_stream,
                },
            )),
            _ => None,
        })?;
    let on = |observation: &Observation| match *observation {
        Observation::PeerFrame {
            connection,
            quic_stream: Some(quic_stream),
            ..
        } => Some(QuicStreamRef {
            connection,
            quic_stream,
        }),
        _ => None,
    };
    let written = QuicStreamRef::joined(&stream, &observations[..ran], on);
    written.last().and_then(|observation| match **observation {
        Observation::PeerFrame {
            connection,
            stream,
            quic_stream: Some(quic_stream),
            seq,
            ..
        } => Some(FrameRef {
            connection,
            kind: stream.kind,
            version: stream.version,
            quic_stream,
            inbound: true,
            seq,
        }),
        _ => None,
    })
}

/// The one crossed update in `observations`, which must carry `label`.
fn crossed(observations: &[Observation], label: Label) -> Result<Crossed, String> {
    let [crossed] = &crossings(observations)[..] else {
        return Err(format!(
            "one crossed update, not {:?}",
            crossings(observations)
        ));
    };
    if crossed.label != label {
        return Err(format!("the crossed update is {:?}", crossed.label));
    }
    Ok(crossed.clone())
}

/// Where `update`'s `FrameJudged` sits in `observations`, joined on its
/// whole [`FrameRef`].
fn judged_at(observations: &[Observation], update: &FrameRef) -> Result<usize, String> {
    observations
        .iter()
        .position(|observation| FrameRef::joins(update, observation, judged_frame))
        .ok_or_else(|| format!("{update:?} was never judged"))
}

/// What `observation` punishes, if it's a connection's close with a fault
/// cause or a `Misbehavior`: that connection.
fn punished(observation: &Observation) -> Option<ConnectionRef> {
    match observation {
        Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
            Some(ConnectionRef {
                connection: *connection,
            })
        }
        Observation::Misbehavior { connection } => Some(ConnectionRef {
            connection: *connection,
        }),
        _ => None,
    }
}

/// Requires that the node closed `update`'s connection on its judgment:
/// the connection's first close, joined on its [`ConnectionRef`], comes
/// after the update's `FrameJudged`, with a fault cause. A close of another
/// connection, or one the node decided before it judged the update, is an
/// unrelated fault's. Returns the close's position.
fn closed_on_its_judgment(
    observations: &[Observation],
    update: &FrameRef,
) -> Result<usize, String> {
    let judged = judged_at(observations, update)?;
    let own = ConnectionRef {
        connection: update.connection,
    };
    let closed = |observation: &Observation| match observation {
        Observation::ConnectionClosed { connection, .. } => Some(ConnectionRef {
            connection: *connection,
        }),
        _ => None,
    };
    let (at, cause) = observations
        .iter()
        .enumerate()
        .find_map(|(at, observation)| match observation {
            Observation::ConnectionClosed { cause, .. }
                if ConnectionRef::joins(&own, observation, closed) =>
            {
                Some((at, cause))
            }
            _ => None,
        })
        .ok_or_else(|| format!("connection {} never closed", update.connection))?;
    if at < judged {
        return Err(format!(
            "connection {} closed with {cause} before {update:?} was judged",
            update.connection
        ));
    }
    if !is_fault_cause(cause) {
        return Err(format!(
            "connection {} closed with {cause}",
            update.connection
        ));
    }
    Ok(at)
}

/// Requires that the node judged `update` and drew no punishment for it:
/// no fault close and no `Misbehavior` after its judgment joined on its
/// connection's [`ConnectionRef`] ([`punished`]).
fn unpunished(observations: &[Observation], update: &FrameRef) -> Result<(), String> {
    let judged = judged_at(observations, update)?;
    let own = ConnectionRef {
        connection: update.connection,
    };
    match ConnectionRef::joined(&own, &observations[judged..], punished)[..] {
        [] => Ok(()),
        [first, ..] => Err(format!("{own:?} was punished by {first:?}")),
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

/// The one update that crossed the node's ending in `observations`,
/// labelled `label`, which must be the frame `act` wrote.
fn crossed_act(observations: &[Observation], act: Act, label: Label) -> FrameRef {
    let Crossed { update, .. } =
        crossed(observations, label).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        act_frame(observations, act).as_ref(),
        Some(&update),
        "{act:?}"
    );
    update
}

/// Checks `act`'s conformant crossing history: the run ends with no harness
/// failure and no violation, `act`'s update is the one that crossed the
/// node's ending, labelled conformant, and the node judged it and didn't
/// punish its connection, with `conformant-never-closes` held, applied.
/// Returns the run's observations and the crossed update.
fn conformant_crossing(report: &RunReport, act: Act) -> (Vec<Observation>, FrameRef) {
    assert_eq!(act.label(), Label::Conformant, "{act:?}");
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let observations = observations(report);
    let update = crossed_act(&observations, act, Label::Conformant);
    assert_eq!(unpunished(&observations, &update), Ok(()));
    held_applied(report, NEVER_CLOSES);
    (observations, update)
}

/// `observations` with `update`'s judgment moved, in turn, to another
/// stream kind and another stream version: each is another frame's
/// judgment, which the update's identity must not join.
fn restreamed(observations: &[Observation], update: &FrameRef) -> Vec<Vec<Observation>> {
    let judged = judged_at(observations, update).expect("the update was judged");
    let mut moved = Vec::new();
    for key in 0..2 {
        let mut changed = observations.to_vec();
        if let Observation::FrameJudged { stream, .. } = &mut changed[judged] {
            match key {
                0 => stream.kind += 1,
                _ => stream.version += 1,
            }
        }
        moved.push(changed);
    }
    moved
}

/// The controls of a conformant history: the same update with any one key
/// moved names no judged frame, and nor does its judgment moved to another
/// stream kind or version. A fault close or a `Misbehavior` added on its
/// connection after its judgment is punishment. The same close on another
/// connection isn't.
fn conformant_controls(observations: &[Observation], update: &FrameRef) {
    assert_one_key_off(update, |update| unpunished(observations, update).is_ok());
    for moved in restreamed(observations, update) {
        assert!(unpunished(&moved, update).is_err());
    }
    let judged = judged_at(observations, update).expect("the update was judged");
    let with = |extra: Observation| {
        let mut changed = observations.to_vec();
        changed.insert(judged + 1, extra);
        changed
    };
    let fault = |connection| Observation::ConnectionClosed {
        connection,
        cause: "service_protocol_reject".to_owned(),
    };
    assert!(is_fault_cause("service_protocol_reject"));
    assert!(unpunished(&with(fault(update.connection)), update).is_err());
    let misbehavior = Observation::Misbehavior {
        connection: update.connection,
    };
    assert!(unpunished(&with(misbehavior), update).is_err());
    assert_eq!(
        unpunished(&with(fault(update.connection + 1)), update),
        Ok(())
    );
}

/// `Act::UnretainedOpen` then `Act::OutcomeCrossing`, from the saved
/// `outcome-crossing` Plan: the one-page `Grant` crosses the node's ending,
/// labelled conformant, and draws no punishment.
#[test]
fn an_unretained_open_then_outcome_crossing_is_conformant_and_unpunished() {
    let report = saved_outcome_crossing();
    let (observations, update) = conformant_crossing(&report, Act::OutcomeCrossing);
    conformant_controls(&observations, &update);
}

/// A subscription `Close` for the unretained `Open` crosses the node's
/// ending, labelled conformant, and draws no punishment.
#[test]
fn a_crossing_close_is_conformant_and_unpunished() {
    let report = crossing_history(Act::CrossingClose);
    let (observations, update) = conformant_crossing(&report, Act::CrossingClose);
    conformant_controls(&observations, &update);
    // A fault close after the judgment punishes the update only on its
    // own connection's whole ref.
    let judged = judged_at(&observations, &update).expect("the update was judged");
    let own = ConnectionRef {
        connection: update.connection,
    };
    assert_one_key_off::<ConnectionRef>(&own, |closed| {
        let mut changed = observations.clone();
        let close = Observation::ConnectionClosed {
            connection: closed.connection,
            cause: "service_protocol_reject".to_owned(),
        };
        changed.insert(judged + 1, close);
        unpunished(&changed, &update).is_err()
    });
}

/// The frame an act wrote is the peer's last on the QUIC stream its
/// `ActRan` names, joined on that whole [`QuicStreamRef`]: an `ActRan` with
/// its connection or QUIC stream moved names another stream, and so
/// another frame or none (Rule 25's template).
#[test]
fn an_act_s_frame_joins_its_quic_stream() {
    let report = saved_outcome_crossing();
    let observations = observations(&report);
    let act = Act::OutcomeCrossing;
    let written = act_frame(&observations, act).expect("the act wrote a frame");
    let ran = observations
        .iter()
        .position(|observation| matches!(observation, Observation::ActRan { act: name, .. } if name == act.name()))
        .expect("the act ran");
    let Observation::ActRan {
        connection,
        quic_stream,
        ..
    } = observations[ran]
    else {
        unreachable!("the position is the act's ActRan");
    };
    let own = QuicStreamRef {
        connection,
        quic_stream,
    };
    let keys = assert_one_key_off::<QuicStreamRef>(&own, |stream| {
        let mut moved = observations.clone();
        if let Observation::ActRan {
            connection,
            quic_stream,
            ..
        } = &mut moved[ran]
        {
            *connection = stream.connection;
            *quic_stream = stream.quic_stream;
        }
        act_frame(&moved, act) == Some(written.clone())
    });
    assert_eq!(keys, 2);
}

/// A `Grant` above the window crosses the node's ending, labelled
/// `bad-cursor`, and the node closes its connection with a fault on its
/// judgment, with `bad-cursor-closes` held, applied. The controls: the same
/// update with any one key moved, the update never judged, and its
/// connection's close moved to another connection or to before the
/// judgment, as an unrelated fault's close would be, each fail.
#[test]
fn an_over_window_crossing_grant_is_bad_cursor_and_closed() {
    let report = crossing_history(over_window());
    assert_eq!(over_window().label(), Label::BadCursor);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let observations = observations(&report);
    let update = crossed_act(&observations, over_window(), Label::BadCursor);
    let close = closed_on_its_judgment(&observations, &update).expect("it closed on judgment");
    held_applied(&report, BAD_CURSOR_CLOSES);

    assert_one_key_off::<FrameRef>(&update, |update| {
        closed_on_its_judgment(&observations, update).is_ok()
    });
    let own = ConnectionRef {
        connection: update.connection,
    };
    assert_one_key_off::<ConnectionRef>(&own, |moved| {
        let mut elsewhere = observations.clone();
        if let Observation::ConnectionClosed { connection, .. } = &mut elsewhere[close] {
            *connection = moved.connection;
        }
        closed_on_its_judgment(&elsewhere, &update).is_ok()
    });

    for moved in restreamed(&observations, &update) {
        assert!(closed_on_its_judgment(&moved, &update).is_err());
    }

    let judged = judged_at(&observations, &update).expect("the update was judged");
    let mut unjudged = observations.clone();
    unjudged.remove(judged);
    assert!(crossed(&unjudged, Label::BadCursor).is_err());
    assert!(closed_on_its_judgment(&unjudged, &update).is_err());

    let mut early = observations.clone();
    let moved = early.remove(close);
    early.insert(judged, moved);
    assert!(closed_on_its_judgment(&early, &update).is_err());
}
