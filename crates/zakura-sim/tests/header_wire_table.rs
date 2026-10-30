//! M5-E1: receiver-direction table checks on `header-sync/wire` (M5.3). An
//! adversarial peer takes one `Plan`-picked act at a time, each the boundary
//! of a header-sync rule, from a setup history that brings its models to the
//! state the act needs. Each frame's label comes from the peer's models, its
//! judgement from the node's reader stage that decides it, and its
//! consequence from P3b's generated properties, which the harness registers
//! (M5-Q13, P3d).
#![cfg(zakura_sim)]

mod support;

use std::collections::BTreeMap;

use support::inputs;
use zakura_network::zakura::v9::RegtestMessage;
use zakura_sim::{
    harness::{
        header_node::{Chain, Policy},
        header_sync::{HeaderWire, WireScenario},
        run_prepared, HarnessError, LinkEntry, LinkSpec, NetScenario, Subsystem,
    },
    model::{is_fault_cause, FrameModel, Label, ReaderEnd},
    observe::{TransitionFact, TransportFact},
    peer::{
        adversary::{Act, AdversarySpec, ContentRule, CursorField, Row, CROSSING_LINK_MS},
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        limit::ADVERSARY_RATE,
        peer_addr, PeerAction, PeerSpec, NODE_ADDR,
    },
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The node's chain at the start of most runs.
const PRELOAD: u32 = 50;

/// One plain one-header page's credit.
const ONE_PAGE: CreditSpec = CreditSpec {
    objects: 1,
    bytes: 263,
};

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

/// The stage whose bytes judged a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// Its header, alone or with the reader's state.
    Header,
    /// Its whole payload.
    Frame,
}

/// The stage `act`'s frame is judged at, from the action table.
fn stage(act: Act) -> Stage {
    match act {
        Act::ShortPayload { .. }
        | Act::LongPayload { .. }
        | Act::UndeclaredType { .. }
        | Act::ReservedFlags { .. }
        | Act::Unsolicited { .. }
        | Act::OverCreditHeader
        | Act::ExtraEnding => Stage::Header,
        _ => Stage::Frame,
    }
}

/// The act's case list: per row, each header of the frame model's alphabet
/// as its act (a length at each bound and one past it, a flag bit, a type no
/// row declares), then the stateful acts and the content rules. The other
/// 15 flag bits run on `status`.
fn cases() -> Vec<Act> {
    let model = FrameModel::new(&RegtestMessage::ROWS, ReaderEnd::Persistent);
    let mut acts = Vec::new();
    for rule in &RegtestMessage::ROWS {
        let row = *Row::ALL
            .iter()
            .find(|row| row.name() == rule.name)
            .expect("every row has an act row");
        let (min, max) = (rule.payload.min(), rule.payload.max());
        for header in model.alphabet(rule) {
            let act = if header.message_type != rule.message_type {
                Act::UndeclaredType {
                    message_type: header.message_type,
                }
            } else if header.flags != 0 {
                Act::ReservedFlags { row, bit: 0 }
            } else if header.payload_len + 1 == min {
                Act::ShortPayload { row }
            } else if header.payload_len == max + 1 {
                Act::LongPayload { row }
            } else if header.payload_len == min {
                Act::MinPayload { row }
            } else if header.payload_len == max {
                Act::MaxPayload { row }
            } else {
                continue;
            };
            if !acts.contains(&act) {
                acts.push(act);
            }
        }
    }
    acts.extend((1..16).map(|bit| Act::ReservedFlags {
        row: Row::Status,
        bit,
    }));
    acts.extend([
        Act::UndeclaredType { message_type: 5 },
        Act::Unsolicited { row: Row::Headers },
        Act::Unsolicited {
            row: Row::HeadersOutcome,
        },
        Act::OverCreditHeader,
        Act::OverCreditObjects,
        Act::SecondOpen,
        Act::OverLive,
        Act::ExtraEnding,
        Act::ExtraEndingAhead,
        Act::AfterClose,
        Act::UnretainedOpen,
        Act::OutcomeCrossing,
        Act::StatusBurst,
    ]);
    acts.extend(CursorField::ALL.map(|field| Act::BadCursor { field }));
    acts.extend(ContentRule::ALL.map(|rule| Act::BadContent { rule }));
    acts.dedup();
    acts
}

/// An adversary that dials the node, holds `chain` headers, subscribes as
/// `subscribe` says, and acts out `script`.
fn adversary(
    chain: u32,
    subscribe: Option<SubscribeSpec>,
    spec: AdversarySpec,
    script: Vec<Act>,
) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        adversary: Some(spec),
        script: script
            .into_iter()
            .map(|act| PeerAction::Act { act })
            .collect(),
        ..PeerSpec::dialing()
    }
}

/// A subscription to the node from [`PRELOAD`] that grants one page and
/// holds every later grant.
fn held_subscription() -> SubscribeSpec {
    SubscribeSpec {
        start: PRELOAD,
        open: ONE_PAGE,
        grants: GrantPolicy::Held {
            grant: ONE_PAGE,
            quiet_ms: 0,
        },
    }
}

/// The setup history `act` needs, with `script` its acts and `withhold`
/// set to write its header faults without their payloads. The node holds
/// [`PRELOAD`] headers unless the act subscribes to it.
///
/// - Acts on the node's subscription: the peer holds 1,200 headers and
///   announces them, but serves no page, so the subscription stays live
///   with its credit unspent.
/// - `unsolicited`: the peer never announces, so the node never subscribes.
/// - `extra-ending`: the peer serves 120 headers, so the node syncs, closes
///   its subscription and gets `SubscriptionClosed`.
/// - Acts on the peer's own subscriptions: the node holds 120 headers (51
///   for `after-close`, whose one page reaches the target and makes the
///   peer close), and the peer subscribes from 50 with one page of credit.
/// - The rest: the peer holds the node's 50 headers, so nobody subscribes.
fn scenario(act: Act, script: Vec<Act>, withhold: bool) -> WireScenario {
    let quiet = AdversarySpec {
        announce: true,
        serve: false,
        withhold,
    };
    let (node, peer) = match act {
        Act::Unsolicited { .. } => (
            PRELOAD,
            adversary(
                1_200,
                None,
                AdversarySpec {
                    announce: false,
                    ..quiet
                },
                script,
            ),
        ),
        Act::ExtraEnding => (
            PRELOAD,
            adversary(
                120,
                None,
                AdversarySpec {
                    serve: true,
                    ..quiet
                },
                script,
            ),
        ),
        Act::SecondOpen
        | Act::OverLive
        | Act::BadCursor { .. }
        | Act::MinPayload {
            row: Row::SubscribeHeaders,
        } => (
            120,
            adversary(PRELOAD, Some(held_subscription()), quiet, script),
        ),
        Act::AfterClose => (
            PRELOAD + 1,
            adversary(PRELOAD, Some(held_subscription()), quiet, script),
        ),
        Act::ShortPayload { row }
        | Act::LongPayload { row }
        | Act::ReservedFlags { row, .. }
        | Act::MinPayload { row }
        | Act::MaxPayload { row }
            if matches!(row, Row::Headers | Row::HeadersOutcome) =>
        {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        Act::OverCreditHeader | Act::OverCreditObjects | Act::ExtraEndingAhead => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        Act::BadContent { rule } if matches!(rule.row(), Row::Headers | Row::HeadersOutcome) => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        _ => (PRELOAD, adversary(PRELOAD, None, quiet, script)),
    };
    let mut net = NetScenario::default();
    if matches!(act, Act::UnretainedOpen | Act::OutcomeCrossing) {
        // The node's ending takes longer to arrive than the crossing
        // `Grant` waits, so the `Grant` crosses it in flight.
        net.links.push(LinkEntry {
            src: NODE_ADDR,
            dst: peer_addr(0, 0),
            model: LinkSpec {
                min_delay_ms: CROSSING_LINK_MS,
                max_delay_ms: CROSSING_LINK_MS + 100,
                ..LinkSpec::default()
            },
        });
    }
    WireScenario {
        net,
        node: Chain::main(node),
        peers: vec![peer],
        policy: Policy::converging(),
    }
}

/// `act`'s script: the act, after the acts its setup history needs: a
/// second `Open` before the third, and the bursts before the one that
/// empties the bucket.
fn script(act: Act) -> Vec<Act> {
    match act {
        Act::OverLive => vec![Act::SecondOpen, Act::OverLive],
        Act::OutcomeCrossing => vec![Act::UnretainedOpen, Act::OutcomeCrossing],
        // 602 tokens, in bursts of 40, with two more each second. The
        // default plan picks a burst opened by the rate limiter only after
        // its next timer, so bursts come in pairs 10 s apart, which drain
        // 60 tokens.
        Act::StatusBurst => vec![Act::StatusBurst; 30],
        act => vec![act],
    }
}

/// One frame the peer sent, as its `PeerFrame` logged it, with where the
/// log has it.
#[derive(Clone, Debug)]
struct Sent {
    at: usize,
    connection: u64,
    quic_stream: u64,
    seq: u64,
    row: Option<String>,
    label: Label,
    exhausted: bool,
}

/// Every `PeerFrame` in `report`, in order.
fn sent(report: &RunReport) -> Vec<Sent> {
    report
        .log
        .entries()
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match &entry.obs {
            Observation::PeerFrame {
                connection,
                quic_stream,
                seq,
                row,
                label,
                exhausted,
                ..
            } => Some(Sent {
                at,
                connection: *connection,
                quic_stream: quic_stream.expect("a wire frame names its QUIC stream"),
                seq: *seq,
                row: row.clone(),
                label: *label,
                exhausted: *exhausted,
            }),
            _ => None,
        })
        .collect()
}

/// The step the `Plan` picked `act`'s identity at.
fn picked(report: &RunReport, act: Act) -> u64 {
    let identity = format!("peer:peer-0:{}", act.name());
    report
        .picks
        .iter()
        .find(|pick| pick.identity == identity)
        .unwrap_or_else(|| panic!("the plan never picked {identity}: {:?}", report.end))
        .step
}

/// `act`'s frame, among the frames sent after its pick: the burst's
/// exhausted `Status`, the crossing's `Grant`, the duplicate of a
/// back-to-back ending, or else the first frame on the act's row.
fn act_frame(report: &RunReport, act: Act) -> Sent {
    let step = picked(report, act);
    let entries = report.log.entries();
    let after: Vec<Sent> = sent(report)
        .into_iter()
        .filter(|frame| entries[frame.at].step >= step)
        .collect();
    let on_row = |frame: &&Sent| frame.row.as_deref() == Some(logged_row(act));
    let found = match act {
        Act::StatusBurst => after.iter().find(|frame| frame.exhausted),
        Act::ExtraEndingAhead => after.iter().filter(on_row).nth(1),
        Act::UndeclaredType { .. } => after.iter().find(|frame| frame.row.is_none()),
        _ => after.iter().find(on_row),
    };
    found
        .unwrap_or_else(|| panic!("{act:?} sent no frame: {:?}", report.end))
        .clone()
}

/// Where `frame`'s `FrameJudged` and `frame-read` sit in the log, if any.
fn judged_and_read(report: &RunReport, frame: &Sent) -> (Option<usize>, Option<usize>) {
    let key = (frame.connection, Some(frame.quic_stream), frame.seq);
    let entries = report.log.entries();
    let judged = entries.iter().position(|entry| {
        matches!(entry.obs, Observation::FrameJudged { connection, quic_stream, seq, .. }
            if (connection, quic_stream, seq) == key)
    });
    let read = entries.iter().position(|entry| {
        matches!(&entry.obs, Observation::Transport {
            connection: Some(connection),
            event: TransportFact::FrameRead { quic_stream, seq },
            ..
        } if (*connection, Some(*quic_stream), *seq) == key)
    });
    (judged, read)
}

/// The first close cause `report` decided on `connection`.
fn close_cause(report: &RunReport, connection: u64) -> Option<String> {
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::ConnectionClosed {
                connection: c,
                cause,
            } if *c == connection => Some(cause.clone()),
            _ => None,
        })
}

/// `id`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, id: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} is checked"))
        .outcome
}

/// The generated property that judges `frame`: its rule's `<label>-closes`,
/// or `conformant-never-closes` for a conformant frame. A `PeerFrame`'s row
/// is its label's rule's.
fn judging_property(frame: &Sent) -> String {
    let stream = "header-sync/s5v9";
    let row = frame.row.as_deref().unwrap_or_default();
    match frame.label {
        Label::Conformant => format!("{stream}/conformant-never-closes"),
        Label::UndeclaredType => format!("{stream}/undeclared-type-closes"),
        label => format!("{stream}/{row}/{}-closes", label.name()),
    }
}

/// The row a `PeerFrame` of `act` names: its label's rule's, which for a
/// page's `OverCredit` is the subscription row.
fn logged_row(act: Act) -> &'static str {
    if act.label() == Label::OverCredit {
        Row::SubscribeHeaders.name()
    } else {
        act.row().name()
    }
}

/// What one act's run showed.
struct Checked {
    report: RunReport,
    frame: Sent,
}

/// Runs `act` from its setup history under `plan`, and checks it: the
/// asserted precondition held (no harness failure), its frame got the
/// act's label, and the node judged it at the act's stage, with a header
/// stage fault never read whole. Then a fault's `<label>-closes` holds with
/// an obligation it discharged, and the node closed the connection with a
/// fault cause: header sync's consequence for every fault, content
/// included, though `bad-content-closes` would also take a `Misbehavior`.
/// A conformant act's
/// `conformant-never-closes` holds, applied, with no fault close. No
/// property of the table is violated.
fn check(act: Act, withhold: bool, plan: PlanInput) -> Checked {
    let report = run(&scenario(act, script(act), withhold), plan);
    assert_eq!(report.end.harness, None, "{act:?}: {:?}", report.end);
    let frame = act_frame(&report, act);
    assert_eq!(frame.label, act.label(), "{act:?}");
    let (judged, read) = judged_and_read(&report, &frame);
    let judged = judged.unwrap_or_else(|| panic!("{act:?}: its frame was never judged"));
    // A header-stage judgement's `FrameJudged` follows every reader event of
    // its step, so only the absence of a read tells the stages apart.
    match stage(act) {
        Stage::Header => assert_eq!(read, None, "{act:?}: a header-stage fault was read whole"),
        Stage::Frame => assert!(
            read.is_some_and(|read| read < judged),
            "{act:?}: judged before its payload was read"
        ),
    }
    let property = judging_property(&frame);
    assert!(
        matches!(outcome(&report, &property), PropertyOutcome::Held { applicable } if *applicable > 0),
        "{act:?}: {property} is {:?}",
        outcome(&report, &property)
    );
    let cause = close_cause(&report, frame.connection);
    if frame.label == Label::Conformant {
        assert!(
            cause.as_deref().is_none_or(|cause| !is_fault_cause(cause)),
            "{act:?}: a conformant peer was closed with {cause:?}"
        );
    } else {
        // Header sync's content consequence is a fault close too.
        let cause = cause.unwrap_or_else(|| panic!("{act:?}: no close"));
        assert!(is_fault_cause(&cause), "{act:?}: closed with {cause}");
    }
    if act == Act::OutcomeCrossing {
        crossed(&report, &frame);
    }
    if act == Act::StatusBurst {
        rebursts(&report);
    }
    assert_eq!(report.end.violation, None, "{act:?}");
    Checked { report, frame }
}

/// Requires that the crossing `Grant`, `frame`, crossed the node's ending
/// in flight: the node wrote the ending before the peer sent the `Grant`,
/// and the peer read it only after.
fn crossed(report: &RunReport, frame: &Sent) {
    let entries = report.log.entries();
    let written = entries.iter().position(|entry| {
        matches!(&entry.obs, Observation::Transport {
            connection: Some(connection),
            event: TransportFact::FrameWritten { quic_stream, message_type: 4, .. },
            ..
        } if (*connection, *quic_stream) == (frame.connection, frame.quic_stream))
    });
    let received = entries.iter().position(|entry| {
        matches!(entry.obs, Observation::PeerReceived { connection, quic_stream, message_type: 4, .. }
            if (connection, quic_stream) == (frame.connection, frame.quic_stream))
    });
    assert!(
        written.is_some_and(|written| written < frame.at),
        "the node wrote its ending before the Grant: {written:?}, {}",
        frame.at
    );
    assert!(
        received.is_some_and(|received| received > frame.at),
        "the peer read the ending after the Grant: {received:?}, {}",
        frame.at
    );
}

/// Requires that each `StatusBurst` picked after the bucket first ran dry
/// stops on its own exhausted `Status`: that frame is the burst's last and
/// its only exhausted one. A burst's frames are the `Status` frames logged
/// at the first step after its pick that logs any, since it writes them
/// with one call and a refresh `Status` takes a step of its own. A refill
/// between bursts holds fewer tokens than `BURST`, so a burst that stops on
/// an admitted frame fails. At least one burst must follow the first
/// exhaustion.
fn rebursts(report: &RunReport) {
    let identity = format!("peer:peer-0:{}", Act::StatusBurst.name());
    let picks: Vec<u64> = report
        .picks
        .iter()
        .filter(|pick| pick.identity == identity)
        .map(|pick| pick.step)
        .collect();
    let entries = report.log.entries();
    let status: Vec<(u64, bool)> = sent(report)
        .into_iter()
        .filter(|frame| frame.row.as_deref() == Some(Row::Status.name()))
        .map(|frame| (entries[frame.at].step, frame.exhausted))
        .collect();
    let dry = status
        .iter()
        .find(|(_, exhausted)| *exhausted)
        .map(|(step, _)| *step)
        .expect("check found the burst's exhausted Status");
    let mut later = 0;
    for &start in picks.iter().filter(|&&start| start > dry) {
        let step = status
            .iter()
            .map(|(step, _)| *step)
            .find(|step| *step >= start)
            .unwrap_or_else(|| panic!("the burst picked at step {start} sent nothing"));
        let burst: Vec<bool> = status
            .iter()
            .filter(|(at, _)| *at == step)
            .map(|(_, exhausted)| *exhausted)
            .collect();
        assert!(
            matches!(burst.split_last(), Some((true, rest)) if !rest.contains(&true)),
            "the burst picked at step {start} stops on its own exhausted Status: {burst:?}"
        );
        later += 1;
    }
    assert!(later > 0, "a burst follows the first exhaustion: {picks:?}");
}

/// Every act of the table, one focused plan each, with `SuccessOnly`
/// storage, M4's 5 s latency bound and no loss, so the act is the only
/// change: its precondition holds, its frame gets the expected label, the
/// node judges it at the stage the action table gives, a header-stage
/// fault closes without reading the frame whole, and the act's
/// `<label>-closes` or `conformant-never-closes` holds where it applies.
///
/// This fails if a boundary goes unlabelled or unjudged, or another guard
/// decides it.
#[test]
fn each_action_has_its_label_judgement_and_close() {
    for act in cases() {
        check(act, false, Plan::from_fuzz(&[]));
    }
}

/// Each header-stage fault, sent with its declared payload withheld. The
/// connection still closes with a fault cause, and the frame gets no
/// `frame-read`, so the reader's early guard decided it: the decoder's own
/// check can't, since it never gets a whole frame. The control requires
/// the frame's `FrameJudged` and its `<label>-closes` applied with an
/// obligation, so the property can't pass as not applicable.
///
/// This fails if the reader waits for the payload: with `check_header`'s
/// verdict bypassed in a temporary patch, which keeps `header-read` and the
/// `header-checked` checkpoint, the connection stays open and each
/// `<label>-closes` fails.
#[test]
fn the_reader_rejects_before_the_payload() {
    for act in cases()
        .into_iter()
        .filter(|act| stage(*act) == Stage::Header)
    {
        let checked = check(act, true, Plan::from_fuzz(&[]));
        let cause = close_cause(&checked.report, checked.frame.connection)
            .unwrap_or_else(|| panic!("{act:?}: no close"));
        assert!(is_fault_cause(&cause), "{act:?}: closed with {cause}");
    }
}

/// One act per frame-stage guard class, whose frame only that guard
/// refuses: a third `Open` (live-subscription admission), a page one
/// object over credit (object-credit claims), a duplicate ending read
/// ahead (endings), a `Grant` acknowledging an unknown cursor (cursor
/// history), and an unlinked page (content). Each act's `<label>-closes`
/// holds, and nothing else in the table fails.
///
/// This fails if another guard masks the intended one: a temporary patch
/// per class removes every enforcement path of its rule, recorded in the
/// commit message, and the matching template then fails, with no other
/// violation.
#[test]
fn the_intended_guard_decides() {
    for act in [
        Act::OverLive,
        Act::OverCreditObjects,
        Act::ExtraEndingAhead,
        Act::BadCursor {
            field: CursorField::Hash,
        },
        Act::BadContent {
            rule: ContentRule::Linkage,
        },
    ] {
        for plan in plans() {
            let checked = check(act, false, plan);
            assert_eq!(stage(act), Stage::Frame);
            let violated: Vec<_> = checked
                .report
                .properties
                .iter()
                .filter(|property| matches!(property.outcome, PropertyOutcome::Violated { .. }))
                .collect();
            assert!(violated.is_empty(), "{act:?}: {violated:?}");
        }
    }
}

/// The plans the multi-plan tests run: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// Within one history, a second `Open` beside one live subscription is
/// `Conformant`, inside the receiver's margin of twice `max_live`, and
/// `conformant-never-closes` holds for it. The third, beside two held, is
/// `OverLive` and closes the connection.
///
/// This fails if the node refuses the second `Open`, or keeps the third.
#[test]
fn second_open_is_tolerated_and_third_is_not() {
    for plan in plans() {
        let script = vec![Act::SecondOpen, Act::OverLive];
        let report = run(&scenario(Act::SecondOpen, script, false), plan);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let second = act_frame(&report, Act::SecondOpen);
        let third = act_frame(&report, Act::OverLive);
        assert_eq!(
            (second.label, third.label),
            (Label::Conformant, Label::OverLive)
        );
        assert_eq!(second.connection, third.connection, "one history");
        for frame in [&second, &third] {
            let (judged, read) = judged_and_read(&report, frame);
            assert!(judged.is_some() && read.is_some_and(|read| Some(read) < judged));
        }
        let entries = report.log.entries();
        let (third_judged, _) = judged_and_read(&report, &third);
        let applied = entries.iter().position(|entry| {
            matches!(
                &entry.obs,
                Observation::Transport {
                    event: TransportFact::Regulated {
                        transition: TransitionFact::OpenApplied {
                            subscription: 2,
                            ..
                        },
                        ..
                    },
                    ..
                }
            )
        });
        assert!(
            applied.is_some_and(|at| Some(at) < third_judged),
            "the node applied the second Open before the third was judged"
        );
        let closed_at = entries.iter().position(|entry| {
            matches!(entry.obs, Observation::ConnectionClosed { connection, .. } if connection == second.connection)
        });
        assert!(
            closed_at.is_some_and(|at| Some(at) > third_judged),
            "the close came only after the third Open was judged"
        );
        let cause = close_cause(&report, third.connection).expect("the third closed");
        assert!(is_fault_cause(&cause), "{cause}");
        for id in [
            "header-sync/s5v9/conformant-never-closes",
            "header-sync/s5v9/subscribe-headers/over-live-closes",
        ] {
            assert!(
                matches!(outcome(&report, id), PropertyOutcome::Held { applicable } if *applicable > 0),
                "{id}: {:?}",
                outcome(&report, id)
            );
        }
        assert_eq!(report.end.violation, None);
    }
}

/// An adversary with seven conformant `Status` acts, each ready as soon as
/// the last is written, under the default plan, which picks an enabled
/// peer action before it advances time. It takes at most
/// [`ADVERSARY_RATE`] acts in each whole virtual second, exactly that many
/// in each second but its last, and each second after the first opens at
/// the boundary the limiter published: time advances to the whole second
/// and the next act is picked there. The scenario declares
/// `BoundedAdversary` with the same rate.
///
/// This fails if the limiter lets a backlog through: with the budget check
/// removed in a temporary patch, all seven acts land in the first second.
#[test]
fn the_rate_limit_caps_adversary_actions() {
    let act = Act::MinPayload { row: Row::Status };
    let scenario = scenario(act, vec![act; 7], false);
    let assumptions = HeaderWire::assumptions(&scenario);
    let declared = assumptions.records();
    assert_eq!(declared.len(), 1);
    assert_eq!(
        declared[0].params,
        serde_json::json!({ "actions_per_second": ADVERSARY_RATE })
    );
    // Each pick takes the first enabled action, which is a peer's whenever
    // no task, delivery or storage completion is, so before time advances.
    let report = run(&scenario, Plan::from_fuzz(&vec![0; 1 << 20]));
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    let identity = format!("peer:peer-0:{}", act.name());
    let mut now = 0u64;
    let mut seconds: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut entries = report.log.entries().iter().peekable();
    for pick in &report.picks {
        while let Some(entry) = entries.next_if(|entry| entry.step < pick.step) {
            if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
                now = to_ns;
            }
        }
        if pick.identity == identity {
            seconds.entry(now / 1_000_000_000).or_default().push(now);
        }
    }
    let counts: Vec<usize> = seconds.values().map(Vec::len).collect();
    assert_eq!(counts.iter().sum::<usize>(), 7, "{seconds:?}");
    let rate = usize::try_from(ADVERSARY_RATE).expect("small");
    assert!(counts.iter().all(|count| *count <= rate), "{seconds:?}");
    assert!(
        counts[..counts.len() - 1]
            .iter()
            .all(|count| *count == rate),
        "every second but the last is full: {seconds:?}"
    );
    let occupied: Vec<u64> = seconds.keys().copied().collect();
    assert!(
        occupied.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "each second follows the last, so time moved to the limiter's boundary and no \
         later one: {seconds:?}"
    );
    for (second, picks) in seconds.iter().skip(1) {
        assert_eq!(
            picks[0],
            second * 1_000_000_000,
            "the second opened at its boundary: {seconds:?}"
        );
    }
}
