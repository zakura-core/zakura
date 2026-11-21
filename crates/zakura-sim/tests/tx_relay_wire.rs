//! `tx-relay/wire` (#2577): each `tx-relay` property applies and holds in
//! its named witness, a composed prefix. Each fault row of `tx-relay`'s
//! close table closes the connection in its witness, and its honest twin,
//! which keeps the rule the witness breaks, runs without a fault close.
//! Each reachable neutral close of the table closes in its own witness.
//! Every generated ID fails on a scripted history that bypasses its rule,
//! and each domain property fails on the hook history of a node that
//! breaks it.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeSet, sync::Arc};

use support::script::{run_under, Node, OBSERVED};
use zakura_chain::transaction::{self, UnminedTxId};
use zakura_network::zakura::{
    tx_relay_sim::{
        Received, SimEffect, SimState, TxRelayEvent, INITIAL_WINDOW, KNOWN_CAP, MAX_SERVES,
        PENDING_PER_PEER, TX_RELAY_STREAMS,
    },
    CloseOutcome, CloseRow, ZakuraPeerId,
};
use zakura_sim::{
    domain::tx_relay::{
        lock, RelayEvidence, SharedRelay, ANNOUNCE_AFTER_ADMISSION, COOLING_BODIES_DROPPED,
        FAULT_CLOSE_NAMES_A_BROKEN_ROW, HELLO_FIRST, IDS, NO_PROBE, PENDING_WITHIN_BOUND,
        REQUESTED_BODIES_ONLY, REQUESTS_WITHIN_LIMIT, SERVING_WITHIN_LIMIT, SKIP_SET_WITHIN_BOUND,
        STALLED_PEERS_PARKED,
    },
    harness::{
        run_prepared,
        tx_relay::{
            composed_prefix, read_log, wire_properties, RelayScenario, TxRelayWire, COMPOSED_BYTES,
            DOMAIN_WITNESSES, NEUTRAL_WITNESSES, ROW_WITNESSES, STALL_WITNESS,
        },
        HarnessError, Subsystem,
    },
    model::Label,
    peer::tx_relay::{header_fault, Answer, HeaderFault, HelloMode, RelayRow, RelayStep},
    table::{generated_trigger, GeneratedId, Template},
    NullSink, Observation, ObservationLog, Outcome, PlanInput, PropertyOutcome, RunReport,
};

/// Runs `prefix` under the all-defaults Plan, and returns its report and
/// evidence. No run may end at a harness failure.
fn run(prefix: &[u8]) -> (RunReport, SharedRelay) {
    let scenario = TxRelayWire::draw(prefix);
    let evidence = SharedRelay::default();
    let report = run_prepared::<TxRelayWire>(
        &scenario,
        Arc::clone(&evidence),
        PlanInput::Bytes(Vec::new()),
        &mut NullSink,
        None,
    )
    .unwrap_or_else(|error| match error {
        HarnessError::Run { failure, report } => {
            panic!("{prefix:02x?}: {failure:?} at {:?}", report.end)
        }
        other => panic!("{prefix:02x?}: {other:?}"),
    });
    assert_ne!(
        report.end.outcome,
        Outcome::Harness,
        "{prefix:02x?}: {:?}",
        report.end
    );
    read_log(&evidence, &report.log);
    (report, evidence)
}

/// The properties `report` shows violated.
fn violated(report: &RunReport) -> Vec<String> {
    report
        .properties
        .iter()
        .filter(|r| matches!(r.outcome, PropertyOutcome::Violated { .. }))
        .map(|r| format!("{}: {:?}", r.property, r.outcome))
        .collect()
}

/// `id`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == id)
        .unwrap_or_else(|| panic!("the run reports {id}: {:?}", report.properties))
        .outcome
}

/// True if `id` applied in `report` and held.
fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { applicable } if *applicable > 0)
}

/// The closes the node decided, in log order.
fn closes(report: &RunReport) -> Vec<CloseOutcome> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::ConnectionClosed { cause, .. } => Some(*cause),
            _ => None,
        })
        .collect()
}

/// Peer 0's `steps`, under node byte `node`, peers byte `peers` and answer
/// byte `answer`.
fn prefix(node: u8, peers: u8, answer: u8, steps: &[RelayStep]) -> [u8; COMPOSED_BYTES] {
    let steps: Vec<(usize, RelayStep)> = steps.iter().map(|step| (0, *step)).collect();
    composed_prefix(node, peers, answer, &steps).expect("a witness fits the grammar")
}

/// Waits ten seconds.
const WAIT: RelayStep = RelayStep::Wait { secs: 10 };

/// The bits of answer byte `answer` that pick `chosen`, among the
/// conformant answers or all of them as `faults` says.
fn answer(chosen: Answer, faults: bool) -> u8 {
    let index = Answer::ALL
        .into_iter()
        .filter(|answer| faults || answer.conformant())
        .position(|answer| answer == chosen)
        .expect("the answer is listed");
    u8::try_from(index).expect("a few answers")
}

/// A prefix decodes to the steps it encodes, the encoder refuses a step
/// that breaks a rule unless byte 1 allows one, a prefix with another
/// version byte draws the default scenario, and every domain property has
/// a witness.
#[test]
fn a_prefix_round_trips_its_steps() {
    let steps = [
        (0, RelayStep::AnnounceAll),
        (1, RelayStep::Fetch),
        (0, RelayStep::Probe { tx: 8 }),
    ];
    let mut prefix = composed_prefix(0, 1, 0, &steps).unwrap();
    let scenario = TxRelayWire::draw(&prefix);
    assert_eq!(
        scenario.peers[0].steps,
        [RelayStep::AnnounceAll, RelayStep::Probe { tx: 8 }]
    );
    assert_eq!(scenario.peers[1].steps, [RelayStep::Fetch]);
    assert!(TxRelayWire::accepts(&prefix));
    prefix[0] ^= 0xff;
    assert!(!TxRelayWire::accepts(&prefix));
    assert_eq!(TxRelayWire::draw(&prefix), RelayScenario::default());
    assert!(composed_prefix(0, 0, 0, &[(0, RelayStep::BadHello)]).is_err());
    let hostile = composed_prefix(0, 0x80, 0, &[(0, RelayStep::BadHello)]).unwrap();
    assert_eq!(
        TxRelayWire::draw(&hostile).peers[0].steps,
        [RelayStep::BadHello]
    );
    let witnessed: BTreeSet<&str> = DOMAIN_WITNESSES.iter().map(|(id, _)| *id).collect();
    assert_eq!(witnessed, IDS.into_iter().collect());
    assert_eq!(answer(Answer::Never, false), 2);
}

/// Runs domain property `id`'s witness: the property applies, reaches
/// its trigger and holds, and the run violates nothing.
fn holds_in_its_witness(id: &str) {
    let (_, witness) = DOMAIN_WITNESSES
        .iter()
        .find(|(witnessed, _)| *witnessed == id)
        .unwrap_or_else(|| panic!("{id} has a witness"));
    let (report, evidence) = run(&witness());
    assert!(held(&report, id), "{id}: {:?}", outcome(&report, id));
    let (triggered, peak) = {
        let evidence = lock(&evidence);
        let triggered = evidence.facts.get(id).is_some_and(|fact| fact.triggered);
        (triggered, evidence.peak)
    };
    assert!(
        triggered,
        "{id}: its witness never reached its trigger; peak {peak:?}, end {:?}",
        report.end
    );
    assert_eq!(violated(&report), Vec::<String>::new(), "{id}");
}

#[test]
fn requests_within_limit_holds_in_its_witness() {
    holds_in_its_witness(REQUESTS_WITHIN_LIMIT);
}

/// A session that ends with a window of bodies awaiting their verdicts
/// hands them to its peer's next session, and its last step counts them
/// once. The pool decides nothing before the reopen, so the window never
/// grows past its start.
#[test]
fn an_ended_sessions_submissions_count_once() {
    let reopen = [
        RelayStep::AnnounceAll,
        RelayStep::Wait { secs: 1 },
        RelayStep::Reopen,
        WAIT,
    ];
    let (report, evidence) = run(&prefix(0x03, 0, 0, &reopen));
    assert_eq!(violated(&report), Vec::<String>::new());
    let peak = lock(&evidence).peak;
    assert_eq!(peak.unsettled, INITIAL_WINDOW, "{peak:?}");
    assert_eq!(peak.requests, INITIAL_WINDOW, "{peak:?}");
}

#[test]
fn serving_within_limit_holds_in_its_witness() {
    holds_in_its_witness(SERVING_WITHIN_LIMIT);
}

#[test]
fn pending_within_bound_holds_in_its_witness() {
    holds_in_its_witness(PENDING_WITHIN_BOUND);
}

#[test]
fn skip_set_within_bound_holds_in_its_witness() {
    holds_in_its_witness(SKIP_SET_WITHIN_BOUND);
}

#[test]
fn requested_bodies_only_holds_in_its_witness() {
    holds_in_its_witness(REQUESTED_BODIES_ONLY);
}

#[test]
fn cooling_bodies_dropped_holds_in_its_witness() {
    holds_in_its_witness(COOLING_BODIES_DROPPED);
}

#[test]
fn stalled_peers_parked_holds_in_its_witness() {
    holds_in_its_witness(STALLED_PEERS_PARKED);
}

#[test]
fn hello_first_holds_in_its_witness() {
    holds_in_its_witness(HELLO_FIRST);
}

#[test]
fn no_probe_holds_in_its_witness() {
    holds_in_its_witness(NO_PROBE);
}

#[test]
fn announce_after_admission_holds_in_its_witness() {
    holds_in_its_witness(ANNOUNCE_AFTER_ADMISSION);
}

#[test]
fn fault_close_names_a_broken_row_holds_in_its_witness() {
    holds_in_its_witness(FAULT_CLOSE_NAMES_A_BROKEN_ROW);
}

// Each domain property's bypass: the hook history a node that breaks the
// property reports, built in code. The property must fail on it.

/// The id of universe-free body `n`.
fn tx(n: u8) -> UnminedTxId {
    UnminedTxId::Legacy(transaction::Hash([n; 32]))
}

/// A peer ID no scripted peer has.
fn stranger() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![0xee; 32]).expect("a 32-byte peer ID")
}

/// Session 1 of a stranger opens, fresh or `readmitted`.
fn opened(readmitted: bool) -> TxRelayEvent {
    TxRelayEvent::Opened {
        session: 1,
        peer: stranger(),
        readmitted,
        parked: false,
    }
}

/// A step of session 1 with `effects`, which leaves it holding `state`.
fn stepped(effects: Vec<SimEffect>, state: SimState) -> TxRelayEvent {
    TxRelayEvent::Stepped {
        session: 1,
        effects,
        state,
    }
}

/// A message from session 1's peer.
fn received(message: Received) -> TxRelayEvent {
    TxRelayEvent::Received {
        session: 1,
        message,
    }
}

/// Feeds `events` to fresh evidence, and requires that `id`, and only
/// `id`, fails.
fn breaks_in_its_bypass(id: &str, events: Vec<TxRelayEvent>) {
    let mut evidence = RelayEvidence::default();
    for event in events {
        evidence.step(event);
    }
    breaks(&evidence, id);
}

/// Requires that `id`, and only `id`, failed in `evidence`.
fn breaks(evidence: &RelayEvidence, id: &str) {
    let failed: Vec<&str> = evidence
        .facts
        .iter()
        .filter(|(_, fact)| fact.violation.is_some())
        .map(|(failed, _)| *failed)
        .collect();
    assert_eq!(failed, vec![id], "{:?}", evidence.facts);
}

/// A body of `size` bytes that session 1's peer announced and the node
/// asked for, then submitted as `body`.
fn submitted(body: UnminedTxId, cooling: bool) -> Vec<TxRelayEvent> {
    vec![
        opened(false),
        received(Received::Hello),
        received(Received::Inv(vec![(tx(1), 100)])),
        stepped(vec![SimEffect::Ask(tx(1))], SimState::default()),
        received(Received::Tx {
            id: tx(1),
            size: 100,
        }),
        stepped(
            vec![SimEffect::Submit {
                requested: Some(tx(1)),
                body,
                size: 100,
                cooling,
            }],
            SimState::default(),
        ),
    ]
}

#[test]
fn requests_within_limit_breaks_in_its_bypass() {
    let state = SimState {
        in_flight: MAX_SERVES + 1,
        ..SimState::default()
    };
    breaks_in_its_bypass(
        REQUESTS_WITHIN_LIMIT,
        vec![opened(false), stepped(Vec::new(), state)],
    );
}

#[test]
fn serving_within_limit_breaks_in_its_bypass() {
    let state = SimState {
        serving: MAX_SERVES + 1,
        ..SimState::default()
    };
    breaks_in_its_bypass(
        SERVING_WITHIN_LIMIT,
        vec![opened(false), stepped(Vec::new(), state)],
    );
}

#[test]
fn pending_within_bound_breaks_in_its_bypass() {
    let state = SimState {
        pending: PENDING_PER_PEER + 1,
        ..SimState::default()
    };
    breaks_in_its_bypass(
        PENDING_WITHIN_BOUND,
        vec![opened(false), stepped(Vec::new(), state)],
    );
}

#[test]
fn skip_set_within_bound_breaks_in_its_bypass() {
    let state = SimState {
        known: KNOWN_CAP + 1,
        ..SimState::default()
    };
    breaks_in_its_bypass(
        SKIP_SET_WITHIN_BOUND,
        vec![opened(false), stepped(Vec::new(), state)],
    );
}

#[test]
fn requested_bodies_only_breaks_in_its_bypass() {
    // The body's own id isn't the one the node asked for.
    breaks_in_its_bypass(REQUESTED_BODIES_ONLY, submitted(tx(2), false));
}

#[test]
fn cooling_bodies_dropped_breaks_in_its_bypass() {
    breaks_in_its_bypass(COOLING_BODIES_DROPPED, submitted(tx(1), true));
}

#[test]
fn stalled_peers_parked_breaks_in_its_bypass() {
    // The session a park readmitted stalls, and the node parks it again.
    breaks_in_its_bypass(
        STALLED_PEERS_PARKED,
        vec![
            opened(true),
            stepped(vec![SimEffect::Park], SimState::default()),
        ],
    );
}

#[test]
fn hello_first_breaks_in_its_bypass() {
    breaks_in_its_bypass(
        HELLO_FIRST,
        vec![
            opened(false),
            received(Received::Inv(vec![(tx(1), 100)])),
            stepped(vec![SimEffect::Ask(tx(1))], SimState::default()),
        ],
    );
}

#[test]
fn no_probe_breaks_in_its_bypass() {
    // The node serves an id it never announced on the session.
    breaks_in_its_bypass(
        NO_PROBE,
        vec![
            opened(false),
            received(Received::Hello),
            received(Received::GetTxs(tx(1))),
            stepped(vec![SimEffect::Serve(tx(1))], SimState::default()),
        ],
    );
}

#[test]
fn announce_after_admission_breaks_in_its_bypass() {
    // The node announces an id its pool never admitted.
    breaks_in_its_bypass(
        ANNOUNCE_AFTER_ADMISSION,
        vec![
            opened(false),
            TxRelayEvent::InvSent {
                session: 1,
                entries: vec![(tx(1), 100)],
            },
        ],
    );
}

#[test]
fn fault_close_names_a_broken_row_breaks_in_its_bypass() {
    // The node closes for a row no peer broke.
    let mut log = ObservationLog::default();
    log.push(Observation::ConnectionClosed {
        connection: 1,
        cause: CloseOutcome::fault(
            TX_RELAY_STREAMS[0].kind,
            TX_RELAY_STREAMS[0].version,
            CloseRow::RELAY_CLOSE_4,
        ),
    });
    let mut evidence = RelayEvidence::default();
    evidence.read_log(&log);
    breaks(&evidence, FAULT_CLOSE_NAMES_A_BROKEN_ROW);
}

/// Each fault row's witness closes the connection naming that row, and
/// its honest twin closes nothing for a fault. Neither run violates a
/// property.
#[test]
fn each_close_row_closes_in_its_witness_and_not_in_its_twin() {
    for witness in ROW_WITNESSES {
        let row = witness.row;
        let (report, _) = run(&(witness.witness)());
        let closed = closes(&report);
        assert!(
            closed.iter().any(|close| close.row() == Some(row)),
            "{row}: closes {closed:?}"
        );
        assert_eq!(violated(&report), Vec::<String>::new(), "{row}");
        let (twin, _) = run(&(witness.twin)());
        let faults: Vec<CloseOutcome> = closes(&twin)
            .into_iter()
            .filter(|close| close.is_fault())
            .collect();
        assert_eq!(faults, Vec::new(), "{row}'s twin");
        assert_eq!(violated(&twin), Vec::<String>::new(), "{row}'s twin");
    }
}

/// Each reachable neutral close of `tx-relay`'s table closes the
/// connection with its recorded detail in its witness, which violates
/// nothing.
#[test]
fn each_neutral_close_closes_in_its_witness() {
    for (detail, witness) in NEUTRAL_WITNESSES {
        let (report, _) = run(&witness());
        let closes = closes(&report);
        assert!(
            closes.iter().any(|close| close.label() == detail),
            "{detail}: closes {closes:?}"
        );
        assert_eq!(violated(&report), Vec::<String>::new(), "{detail}");
    }
}

/// The generated IDs no `tx-relay` scenario reaches, each with why.
const UNREACHED: [(&str, &str); 5] = [
    (
        "tx-relay/s9v1/inv/over-rate-closes",
        "no peer floods: it can't tell which Inv overran the node's bucket",
    ),
    (
        "tx-relay/s10v1/get-txs/over-margin-closes",
        "a request past the Hello depends on the node's writer, so its close is neutral",
    ),
    (
        "tx-relay/s10v1/not-found/bad-content-closes",
        "an unrequested NotFound is unsolicited, row 9",
    ),
    (
        "tx-relay/s10v1/not-found/over-owed-closes",
        "a second answer is unsolicited, never over-owed",
    ),
    (
        "tx-relay/s9v1/tx/over-owed-closes",
        "a second answer is unsolicited, never over-owed",
    ),
];

/// The witnesses of the generated IDs: the row witnesses, a header fault
/// of each row, an unrequested body, an undeclared type on each stream,
/// four sessions' `Hello`s on one connection, which run its bucket dry,
/// and an honest exchange.
fn generated_witnesses() -> Vec<[u8; COMPOSED_BYTES]> {
    let mut witnesses: Vec<[u8; COMPOSED_BYTES]> = ROW_WITNESSES
        .iter()
        .map(|witness| (witness.witness)())
        .collect();
    for row in RelayRow::ALL {
        for fault in [
            HeaderFault::ReservedFlags,
            HeaderFault::ShortPayload,
            HeaderFault::LongPayload,
        ] {
            witnesses.push(prefix(0, 0x80, 0, &[header_fault(row, fault), WAIT]));
        }
    }
    witnesses.push(prefix(
        0,
        0x80,
        0,
        &[RelayStep::UnrequestedTx { tx: 0 }, WAIT],
    ));
    for data in [true, false] {
        witnesses.push(prefix(
            0,
            0x80,
            0,
            &[RelayStep::UndeclaredType { data }, WAIT],
        ));
    }
    witnesses.push(prefix(0, 0x80, 0, &[RelayStep::UnrequestedNotFound, WAIT]));
    witnesses.push(prefix(
        0,
        0,
        0,
        &[
            RelayStep::Reopen,
            RelayStep::Reopen,
            RelayStep::Reopen,
            WAIT,
        ],
    ));
    witnesses.push(prefix(
        0,
        0x01,
        0,
        &[RelayStep::AnnounceAll, WAIT, RelayStep::FetchBurst, WAIT],
    ));
    witnesses
}

/// Every generated ID reaches its trigger and holds in one of its
/// witnesses, which violate nothing, unless [`UNREACHED`] names it.
#[test]
fn every_generated_property_holds_in_a_witness() {
    let mut reached = BTreeSet::new();
    for prefix in generated_witnesses() {
        let (report, _) = run(&prefix);
        assert_eq!(violated(&report), Vec::<String>::new(), "{prefix:02x?}");
        for property in wire_properties::<TxRelayWire>() {
            let name = property.id.to_string();
            if generated_trigger(&TX_RELAY_STREAMS, &property.id, &report.log)
                && held(&report, &name)
            {
                reached.insert(name);
            }
        }
    }
    let unreached: BTreeSet<String> = UNREACHED.iter().map(|(id, _)| (*id).to_owned()).collect();
    let all: BTreeSet<String> = wire_properties::<TxRelayWire>()
        .iter()
        .map(|property| property.id.to_string())
        .collect();
    let missing: Vec<&String> = all
        .iter()
        .filter(|id| !reached.contains(*id) && !unreached.contains(*id))
        .collect();
    assert_eq!(missing, Vec::<&String>::new());
    let stale: Vec<&String> = unreached.intersection(&reached).collect();
    assert_eq!(
        stale,
        Vec::<&String>::new(),
        "reached, yet listed unreached"
    );
}

/// The scripted history that bypasses generated ID `id`'s rule: a fault
/// the node never punishes, or a punishment of a conformant peer.
fn bypass(id: &GeneratedId<'_>) -> Vec<Vec<Observation>> {
    let row = id.row.map(str::to_owned);
    let peer = |label: Label, exhausted: bool| Observation::PeerFrame {
        connection: 1,
        stream: id.stream,
        quic_stream: None,
        seq: 0,
        row: if label == Label::UndeclaredType {
            None
        } else {
            row.clone().or(Some("hello".to_owned()))
        },
        rule_stream: id.stream,
        label,
        exhausted,
        exchange: None,
        payload: 9,
    };
    let judged = Observation::FrameJudged {
        connection: 1,
        stream: id.stream,
        quic_stream: None,
        seq: 0,
    };
    let fault_close = Observation::ConnectionClosed {
        connection: 1,
        cause: CloseOutcome::fault(id.stream.kind, id.stream.version, CloseRow::RELAY_CLOSE_3),
    };
    match id.template {
        Template::Closes(label) => vec![vec![peer(label, false)], vec![judged]],
        Template::ConformantNeverCloses => vec![
            vec![peer(Label::Conformant, false)],
            vec![judged, fault_close],
        ],
        Template::CadenceNeverCloses => vec![
            vec![peer(Label::Conformant, true)],
            vec![judged, fault_close],
        ],
        other => panic!("tx-relay registers no {other:?} template"),
    }
}

/// Every generated `tx-relay` ID fails on a scripted history that bypasses
/// its rule.
#[test]
fn scripted_bypasses_violate_every_generated_id() {
    for property in wire_properties::<Node>() {
        let name = property.id.to_string();
        let parsed = GeneratedId::parse(&name).expect("a generated ID parses back");
        let report = run_under(wire_properties(), OBSERVED, bypass(&parsed));
        assert!(
            matches!(outcome(&report, &name), PropertyOutcome::Violated { .. }),
            "{name}: {:?}",
            report.properties
        );
    }
}

/// The answer byte of the stall witness picks `Never` among the
/// conformant answers, and its peer announces fresh ids as each session
/// opens.
#[test]
fn the_stall_witness_never_answers() {
    let scenario = TxRelayWire::draw(&STALL_WITNESS());
    assert_eq!(scenario.peers[0].answer, Answer::Never);
    assert_eq!(scenario.peers[0].hello, HelloMode::First);
    assert!(scenario.peers[0].announce_on_open > 0);
}
