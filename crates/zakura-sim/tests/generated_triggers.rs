//! C7: the generated templates' trigger histories, on scripted logs. A
//! receiver template's trigger is the node judging a frame its row's rule
//! covers, with the template's label, and a sender template's is the node
//! sending a frame of the exchange its rule guards.
#![cfg(zakura_sim)]

mod support;

use support::feed::{FEED, ITEM};
use zakura_network::zakura::Stream;
use zakura_sim::{
    model::Label,
    observe::{RowAt, StreamId, TransitionFact, TransportFact},
    property::PropertyId,
    table::{generated_trigger, GeneratedId, Template},
    Observation, ObservationLog,
};

/// The `feed` stream: `watch`, its `item` pages and its `ended` outcome.
const FEED_STREAM: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    messages: Some(FEED),
    ..Stream::PERSISTENT
};

/// The layout the feed's properties come from.
const LAYOUT: &[Stream] = &[FEED_STREAM];

/// The feed stream's ID.
fn stream() -> StreamId {
    StreamId {
        kind: FEED_STREAM.kind,
        version: FEED_STREAM.version,
    }
}

/// The generated ID of `template` on the feed's `row`.
fn id(row: Option<&str>, template: Template) -> PropertyId {
    PropertyId::generated(
        GeneratedId {
            family: "feed",
            stream: stream(),
            row,
            template,
        }
        .to_string(),
    )
    .expect("a generated ID")
}

/// A log of `observations`, in order. A trigger reads their order, not
/// their steps.
fn log(observations: Vec<Observation>) -> ObservationLog {
    let mut log = ObservationLog::default();
    for obs in observations {
        log.push(obs);
    }
    log
}

/// The peer's frame `seq` on connection 1, on `row`, labelled `label`.
fn peer(seq: u64, row: &str, label: Label) -> Observation {
    Observation::PeerFrame {
        connection: 1,
        stream: stream(),
        quic_stream: None,
        seq,
        row: Some(row.to_owned()),
        rule_stream: stream(),
        label,
        exhausted: false,
        exchange: Some(7),
        payload: 8,
    }
}

/// The node judging the peer's frame `seq` on connection 1.
fn judged(seq: u64) -> Observation {
    Observation::FrameJudged {
        connection: 1,
        stream: stream(),
        quic_stream: None,
        seq,
    }
}

/// The node's frame `seq` on connection 1, on `row`, labelled `label`,
/// naming exchange 7, as the peer's frames do.
fn node(seq: u64, row: &str, label: Label) -> Observation {
    Observation::NodeFrame {
        connection: 1,
        stream: stream(),
        quic_stream: None,
        seq,
        row: Some(row.to_owned()),
        rule_stream: stream(),
        label,
        exhausted: false,
        exchange: Some(7),
    }
}

/// `frame`, a peer's or the node's, on QUIC stream `quic`.
fn on_quic(frame: Observation, quic: u64) -> Observation {
    match frame {
        Observation::PeerFrame {
            connection,
            stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            payload,
            ..
        } => Observation::PeerFrame {
            connection,
            stream,
            quic_stream: Some(quic),
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            payload,
        },
        Observation::NodeFrame {
            connection,
            stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            ..
        } => Observation::NodeFrame {
            connection,
            stream,
            quic_stream: Some(quic),
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
        },
        other => other,
    }
}

/// The node granting credit on connection 1's `watch` subscription `key`.
fn granted(key: u64) -> Observation {
    Observation::CreditGranted {
        connection: 1,
        subscription: RowAt::new(stream(), "watch"),
        exchange: key,
        objects: 4,
        bytes: 64,
        opens: true,
    }
}

/// The node starting to write its frame `seq` on connection 1's QUIC stream
/// `quic`.
fn write_started(quic: u64, seq: u64) -> Observation {
    Observation::Transport {
        instance: 0,
        seq: 1,
        node: "node".to_owned(),
        connection: Some(1),
        remote: "10.0.0.2:4000".to_owned(),
        event: TransportFact::FrameWriteStarted {
            quic_stream: quic,
            seq,
            message_type: ITEM,
        },
    }
}

/// `frame`, the node's, naming exchange `key` instead.
fn naming(frame: Observation, key: u64) -> Observation {
    match frame {
        Observation::NodeFrame {
            connection,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            ..
        } => Observation::NodeFrame {
            connection,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange: Some(key),
        },
        other => other,
    }
}

/// True if `observations` reach `id`'s trigger.
fn reaches(id: &PropertyId, observations: Vec<Observation>) -> bool {
    generated_trigger(LAYOUT, id, &log(observations))
}

/// A `<label>-closes` trigger needs the node to judge a frame with the
/// label on the ID's own row. The same frame on a sibling row, a frame of
/// another label, or a frame the node never judged doesn't reach it.
#[test]
fn a_closes_trigger_needs_its_label_on_its_row() {
    let item = id(Some("item"), Template::Closes(Label::LongPayload));
    let ended = id(Some("ended"), Template::Closes(Label::LongPayload));
    let long_item = vec![peer(0, "item", Label::LongPayload), judged(0)];
    assert!(reaches(&item, long_item.clone()));
    assert!(!reaches(&ended, long_item));
    assert!(!reaches(
        &item,
        vec![peer(0, "item", Label::ShortPayload), judged(0)]
    ));
    assert!(!reaches(&item, vec![peer(0, "item", Label::LongPayload)]));
}

/// The receiver templates without a label of their own read the node's
/// judging too: a conformant frame for `conformant-never-closes`, any frame
/// on the row for `receiver-work-authorized`, and for
/// `receiver-credit-conserved` a page of the subscription the node granted
/// that subscription credit before.
#[test]
fn receiver_triggers_need_a_judged_frame() {
    let conformant = id(None, Template::ConformantNeverCloses);
    assert!(reaches(
        &conformant,
        vec![peer(0, "item", Label::Conformant), judged(0)]
    ));
    assert!(!reaches(
        &conformant,
        vec![peer(0, "item", Label::LongPayload), judged(0)]
    ));

    let work = id(Some("item"), Template::WorkAuthorized);
    assert!(reaches(
        &work,
        vec![peer(0, "item", Label::Unsolicited), judged(0)]
    ));
    assert!(!reaches(
        &work,
        vec![peer(0, "ended", Label::Conformant), judged(0)]
    ));

    let credit = id(Some("watch"), Template::CreditConserved);
    // Every peer frame here names subscription 7.
    let page = vec![peer(0, "item", Label::Conformant), judged(0)];
    assert!(!reaches(&credit, page.clone()));
    assert!(reaches(&credit, [vec![granted(7)], page.clone()].concat()));
    // Credit granted to another subscription isn't this page's.
    assert!(!reaches(&credit, [vec![granted(8)], page].concat()));
    // Nor is a grant after the peer sent the page, even if the node judged
    // the page after it.
    assert!(!reaches(
        &credit,
        vec![peer(0, "item", Label::Conformant), granted(7), judged(0)]
    ));
    // The subscription's own update isn't a page of it, and nor is its
    // ending, which spends no credit.
    assert!(!reaches(
        &credit,
        vec![granted(7), peer(0, "watch", Label::Conformant), judged(0)]
    ));
    assert!(!reaches(
        &credit,
        vec![granted(7), peer(0, "ended", Label::Conformant), judged(0)]
    ));
}

/// A fault close of connection `connection`.
fn fault_closed(connection: u64) -> Observation {
    Observation::ConnectionClosed {
        connection,
        cause: "service_protocol_reject".to_owned(),
    }
}

/// `frame` moved to connection 2.
fn on_two(frame: Observation) -> Observation {
    match frame {
        Observation::PeerFrame {
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            payload,
            ..
        } => Observation::PeerFrame {
            connection: 2,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            payload,
        },
        Observation::FrameJudged {
            stream,
            quic_stream,
            seq,
            ..
        } => Observation::FrameJudged {
            connection: 2,
            stream,
            quic_stream,
            seq,
        },
        other => other,
    }
}

/// `conformant-never-closes`'s trigger joins the punishment it counts to
/// the connection it's on: connection 1's judged conformant frame doesn't
/// reach it for a fault close or `Misbehavior` of connection 2 before the
/// node judged anything there, and connection 2's own judged conformant
/// frame does. A punishment the property doesn't count, after a
/// non-conformant frame, leaves the trigger as connection 1's frame made it.
#[test]
fn conformant_never_closes_joins_the_punished_connection() {
    let conformant = id(None, Template::ConformantNeverCloses);
    let first = vec![peer(0, "item", Label::Conformant), judged(0)];
    assert!(reaches(
        &conformant,
        [first.clone(), vec![fault_closed(1)]].concat()
    ));
    for punished in [fault_closed(2), Observation::Misbehavior { connection: 2 }] {
        assert!(
            !reaches(
                &conformant,
                [first.clone(), vec![punished.clone()]].concat()
            ),
            "{punished:?}"
        );
        let own: Vec<Observation> = first.iter().cloned().map(on_two).collect();
        assert!(reaches(
            &conformant,
            [first.clone(), own, vec![punished.clone()]].concat()
        ));
    }
    let bad = vec![
        on_two(peer(0, "item", Label::LongPayload)),
        on_two(judged(0)),
    ];
    assert!(reaches(
        &conformant,
        [first, bad, vec![fault_closed(2)]].concat()
    ));
}

/// The node admitting work on `connection` from the peer's `item` frame
/// `seq` of `watch` subscription `key`.
fn work(connection: u64, seq: u64, key: u64) -> Observation {
    Observation::WorkAdmitted {
        connection,
        frame: RowAt::new(stream(), "item"),
        seq,
        request: RowAt::new(stream(), "watch"),
        exchange: key,
        objects: 1,
        bytes: 1,
        ends: false,
    }
}

/// The node starting to serve `connection`'s `watch` request `key`.
fn serving(connection: u64, key: u64) -> Observation {
    Observation::Serving {
        connection,
        request: RowAt::new(stream(), "watch"),
        exchange: key,
    }
}

/// The node's `item` output on `connection` for its `watch` request `key`.
fn output(connection: u64, key: u64) -> Observation {
    Observation::NodeOutput {
        connection,
        frame: RowAt::new(stream(), "item"),
        request: RowAt::new(stream(), "watch"),
        exchange: key,
        payload: 8,
        ends: false,
    }
}

/// `receiver-work-authorized`'s trigger joins each work the node admits
/// from its row to that work's own source frame: connection 1's judged
/// frame doesn't reach it for work from connection 2, from another frame,
/// from a frame the node judged only after the work, or from a frame the
/// peer never sent. Connection 2's own judged frame does, whatever its
/// label, since the violation is on its own trigger.
#[test]
fn work_authorized_joins_the_work_to_its_own_frame() {
    let work_id = id(Some("item"), Template::WorkAuthorized);
    let first = vec![peer(0, "item", Label::Conformant), judged(0)];
    assert!(reaches(
        &work_id,
        [first.clone(), vec![work(1, 0, 7)]].concat()
    ));
    // One key off each: the connection and the frame.
    assert!(!reaches(
        &work_id,
        [first.clone(), vec![work(2, 0, 7)]].concat()
    ));
    let other_row = vec![peer(1, "ended", Label::Conformant), judged(1)];
    assert!(!reaches(
        &work_id,
        [first.clone(), other_row, vec![work(1, 1, 7)]].concat()
    ));
    // No source frame at all, beside connection 1's.
    assert!(!reaches(
        &work_id,
        [first.clone(), vec![work(1, 5, 7)]].concat()
    ));
    assert!(!reaches(&work_id, vec![work(1, 0, 7)]));
    // Judged only after the work.
    assert!(!reaches(
        &work_id,
        vec![peer(0, "item", Label::Conformant), work(1, 0, 7), judged(0)]
    ));
    let own = vec![
        on_two(peer(0, "item", Label::Unsolicited)),
        on_two(judged(0)),
    ];
    assert!(reaches(
        &work_id,
        [first, own, vec![work(2, 0, 7)]].concat()
    ));
}

/// `receiver-credit-conserved`'s trigger joins each page's work the node
/// admits for its subscription to a judged page of that subscription on
/// the work's own connection and key, granted credit before it. Connection
/// 1's granted page doesn't reach it for connection 2's work, or for work
/// of another subscription key, even with no page behind it. An ending's
/// work, which the check doesn't count, leaves the trigger as it was.
#[test]
fn credit_conserved_joins_the_work_to_its_own_subscription() {
    let credit = id(Some("watch"), Template::CreditConserved);
    let first = vec![granted(7), peer(0, "item", Label::Conformant), judged(0)];
    assert!(reaches(
        &credit,
        [first.clone(), vec![work(1, 0, 7)]].concat()
    ));
    // One key off each: the connection and the subscription.
    assert!(!reaches(
        &credit,
        [first.clone(), vec![work(2, 0, 7)]].concat()
    ));
    assert!(!reaches(
        &credit,
        [first.clone(), vec![work(1, 0, 8)]].concat()
    ));
    let ending = match work(2, 9, 7) {
        Observation::WorkAdmitted {
            connection,
            frame,
            seq,
            request,
            exchange,
            objects,
            bytes,
            ..
        } => Observation::WorkAdmitted {
            connection,
            frame,
            seq,
            request,
            exchange,
            objects,
            bytes,
            ends: true,
        },
        _ => unreachable!(),
    };
    assert!(reaches(&credit, [first.clone(), vec![ending]].concat()));
    let own_grant = Observation::CreditGranted {
        connection: 2,
        subscription: RowAt::new(stream(), "watch"),
        exchange: 7,
        objects: 4,
        bytes: 64,
        opens: true,
    };
    let own = vec![
        own_grant,
        on_two(peer(0, "item", Label::Conformant)),
        on_two(judged(0)),
    ];
    assert!(reaches(&credit, [first, own, vec![work(2, 0, 7)]].concat()));
}

/// `receiver-serving-bounded`'s trigger joins each serving owner and output
/// for a request of its row to a judged request with the effect's own
/// connection and key. Connection 1's judged request doesn't reach it for
/// serving or output on connection 2, or for another request key with no
/// request behind it. Output for another request row, which the check
/// doesn't count, leaves the trigger as it was.
#[test]
fn serving_bounded_joins_each_effect_to_its_own_request() {
    let serving_id = id(Some("watch"), Template::ServingBounded);
    let first = vec![peer(0, "watch", Label::Conformant), judged(0)];
    for effect in [serving, output] {
        assert!(reaches(
            &serving_id,
            [first.clone(), vec![effect(1, 7)]].concat()
        ));
        // One key off each: the connection and the request.
        assert!(!reaches(
            &serving_id,
            [first.clone(), vec![effect(2, 7)]].concat()
        ));
        assert!(!reaches(
            &serving_id,
            [first.clone(), vec![effect(1, 8)]].concat()
        ));
        let own: Vec<Observation> = first.iter().cloned().map(on_two).collect();
        assert!(reaches(
            &serving_id,
            [first.clone(), own, vec![effect(2, 7)]].concat()
        ));
    }
    let elsewhere = Observation::NodeOutput {
        connection: 2,
        frame: RowAt::new(stream(), "ended"),
        request: RowAt::new(stream(), "item"),
        exchange: 7,
        payload: 8,
        ends: true,
    };
    assert!(reaches(&serving_id, [first, vec![elsewhere]].concat()));
}

/// The node applying the peer's grant of subscription `key` on connection
/// 1's QUIC stream `quic`, as its regulation logs it.
fn applied(quic: u64, key: u64) -> Observation {
    Observation::Transport {
        instance: 0,
        seq: 1,
        node: "node".to_owned(),
        connection: Some(1),
        remote: "10.0.0.2:4000".to_owned(),
        event: TransportFact::Regulated {
            quic_stream: quic,
            transition: TransitionFact::GrantApplied {
                subscription: key,
                objects: 4,
                bytes: 64,
            },
        },
    }
}

/// The node reading the peer's frame `seq` on connection 1's QUIC stream
/// `quic`.
fn read(quic: u64, seq: u64) -> Observation {
    Observation::Transport {
        instance: 0,
        seq: 1,
        node: "node".to_owned(),
        connection: Some(1),
        remote: "10.0.0.2:4000".to_owned(),
        event: TransportFact::FrameRead {
            quic_stream: quic,
            seq,
        },
    }
}

/// The node's frame `seq` on `row`, labelled `label`, on QUIC stream 4.
fn sent(seq: u64, row: &str, label: Label) -> Observation {
    on_quic(node(seq, row, label), 4)
}

/// `sender-within-credit` needs a page the node sent after it applied the
/// peer's grant or `Open` of that page's subscription, before it started
/// writing. The applied grant or `Open` alone, a grant the peer only offered, the node's own
/// subscription update, a grant applied after the write started, a grant of
/// another subscription, on another QUIC stream or on another connection,
/// or an ending, which spends no credit, doesn't reach it. A page past its
/// credit, which the subscription row itself enforces, does.
#[test]
fn sender_within_credit_needs_a_page_after_an_applied_grant() {
    let credit = id(Some("watch"), Template::SenderWithinCredit);
    let grant = applied(4, 7);
    let page = || sent(0, "item", Label::Conformant);
    assert!(reaches(&credit, vec![grant.clone(), page()]));
    assert!(reaches(
        &credit,
        vec![grant.clone(), sent(0, "watch", Label::OverCredit)]
    ));
    assert!(!reaches(&credit, vec![grant.clone()]));
    assert!(!reaches(&credit, vec![page()]));
    // The peer's `Open` applied with its initial credit, as its regulation
    // logs it: alone it doesn't reach the trigger, and before a page it does.
    let open = Observation::Transport {
        instance: 0,
        seq: 1,
        node: "node".to_owned(),
        connection: Some(1),
        remote: "10.0.0.2:4000".to_owned(),
        event: TransportFact::Regulated {
            quic_stream: 4,
            transition: TransitionFact::OpenApplied {
                subscription: 7,
                objects: 4,
                bytes: 64,
                seq: None,
            },
        },
    };
    assert!(!reaches(&credit, vec![open.clone()]));
    assert!(reaches(&credit, vec![open, page()]));
    assert!(!reaches(
        &credit,
        vec![grant.clone(), sent(0, "watch", Label::Conformant)]
    ));
    assert!(!reaches(
        &credit,
        vec![grant.clone(), sent(0, "ended", Label::Conformant)]
    ));
    // Offered by the peer but not applied by the node.
    assert!(!reaches(
        &credit,
        vec![on_quic(peer(0, "watch", Label::Conformant), 4), page()]
    ));
    // Applied only after the node started writing the page.
    assert!(!reaches(
        &credit,
        vec![write_started(4, 0), grant.clone(), page()]
    ));
    assert!(reaches(
        &credit,
        vec![grant.clone(), write_started(4, 0), page()]
    ));
    // One key off each: subscription, QUIC stream, connection.
    assert!(!reaches(&credit, vec![applied(4, 8), page()]));
    assert!(!reaches(&credit, vec![applied(0, 7), page()]));
    let elsewhere = match page() {
        Observation::NodeFrame {
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            ..
        } => Observation::NodeFrame {
            connection: 2,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
        },
        _ => unreachable!(),
    };
    assert!(!reaches(&credit, vec![grant, elsewhere]));
}

/// A response template's sender trigger needs the node's frame on the
/// response row after it took up the peer's exchange of the request row,
/// one key off failing it. For `sender-answers-open` and `sender-ends-once`,
/// an `Unsolicited` response reaches it alone, since the missing exchange is
/// the violation. A frame on the sibling response row doesn't reach it. A
/// frame that names no exchange, as a discovery frame doesn't, joins the
/// peer's conformant request on its stream that the node read before it
/// started writing.
#[test]
fn sender_response_triggers_need_the_exchange_first() {
    let answers = id(Some("item"), Template::SenderAnswersOpen);
    let opened = applied(4, 7);
    assert!(reaches(
        &answers,
        vec![opened.clone(), sent(0, "item", Label::Conformant)]
    ));
    assert!(!reaches(&answers, vec![sent(0, "item", Label::Conformant)]));
    assert!(!reaches(
        &answers,
        vec![opened.clone(), sent(0, "ended", Label::Conformant)]
    ));
    assert!(reaches(&answers, vec![sent(0, "item", Label::Unsolicited)]));
    assert!(!reaches(
        &answers,
        vec![sent(0, "ended", Label::Unsolicited)]
    ));
    let ends = id(Some("ended"), Template::SenderEndsOnce);
    assert!(reaches(
        &ends,
        vec![opened.clone(), sent(0, "ended", Label::Conformant)]
    ));
    assert!(reaches(&ends, vec![sent(0, "ended", Label::Unsolicited)]));
    // One key off each: exchange, QUIC stream.
    assert!(!reaches(
        &ends,
        vec![
            opened.clone(),
            naming(sent(0, "ended", Label::Conformant), 8)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![applied(0, 7), sent(0, "ended", Label::Conformant)]
    ));
    // A frame naming no exchange joins the peer's request on its stream.
    let keyless = |frame: Observation| match frame {
        Observation::NodeFrame {
            connection,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            ..
        } => Observation::NodeFrame {
            connection,
            stream,
            quic_stream,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange: None,
        },
        other => other,
    };
    let request = on_quic(peer(0, "watch", Label::Conformant), 4);
    let ending = || keyless(sent(0, "ended", Label::Conformant));
    assert!(reaches(&ends, vec![request.clone(), read(4, 0), ending()]));
    // Offered by the peer but not read by the node before it wrote.
    assert!(!reaches(&ends, vec![request.clone(), ending()]));
    assert!(!reaches(
        &ends,
        vec![request.clone(), write_started(4, 0), read(4, 0), ending()]
    ));
    // One key off each: the read's frame, QUIC stream, and the request.
    assert!(!reaches(&ends, vec![request.clone(), read(4, 1), ending()]));
    assert!(!reaches(
        &ends,
        vec![
            on_quic(peer(0, "watch", Label::Conformant), 0),
            read(0, 0),
            ending()
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![
            on_quic(peer(0, "watch", Label::LongPayload), 4),
            read(4, 0),
            ending()
        ]
    ));
    // On a link with no transport events, as block sync's relay is, the
    // exchange the relay's model opened for the request row stands in for
    // the read: the exchange the frame's `NodeOutput`, logged after it,
    // names, and not one an earlier response ended. One key off each: the
    // owed row, the connection and the exchange.
    let owed = |connection: u64, row: &str, key: u64| Observation::ExchangeOwed {
        connection,
        request: RowAt::new(stream(), row),
        exchange: key,
        frames: 1,
        bytes: 64,
    };
    let output = |key: u64, ends: bool| Observation::NodeOutput {
        connection: 1,
        frame: RowAt::new(stream(), "ended"),
        request: RowAt::new(stream(), "watch"),
        exchange: key,
        payload: 8,
        ends,
    };
    let request = peer(0, "watch", Label::Conformant);
    let ending = || keyless(node(0, "ended", Label::Conformant));
    assert!(reaches(
        &ends,
        vec![
            request.clone(),
            owed(1, "watch", 7),
            ending(),
            output(7, true)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![request.clone(), ending(), output(7, true)]
    ));
    assert!(!reaches(
        &ends,
        vec![
            owed(1, "watch", 7),
            request.clone(),
            ending(),
            output(7, true)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![
            request.clone(),
            owed(1, "item", 7),
            ending(),
            output(7, true)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![
            request.clone(),
            owed(2, "watch", 7),
            ending(),
            output(7, true)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![
            request.clone(),
            owed(1, "watch", 7),
            ending(),
            output(8, true)
        ]
    ));
    assert!(!reaches(
        &ends,
        vec![request.clone(), owed(1, "watch", 7), ending()]
    ));
    // An exchange an earlier response on another row ended doesn't open a
    // later one.
    let earlier = keyless(node(1, "item", Label::Conformant));
    assert!(!reaches(
        &ends,
        vec![
            request.clone(),
            owed(1, "watch", 7),
            earlier,
            output(7, true),
            ending(),
            output(7, true)
        ]
    ));
    // A link with transport events needs the read itself.
    assert!(!reaches(
        &ends,
        vec![
            on_quic(peer(0, "watch", Label::Conformant), 4),
            owed(1, "watch", 7),
            keyless(sent(0, "ended", Label::Conformant))
        ]
    ));
}

/// `sender-within-live` is the node opening or updating a subscription,
/// not a page past its credit on that row.
#[test]
fn sender_within_live_needs_an_update() {
    let live = id(Some("watch"), Template::SenderWithinLive);
    assert!(reaches(&live, vec![node(0, "watch", Label::Conformant)]));
    assert!(!reaches(&live, vec![node(0, "watch", Label::OverCredit)]));
    assert!(!reaches(&live, vec![node(0, "item", Label::Conformant)]));
}

/// An ID that isn't generated, or whose stream isn't in the layout, never
/// reaches a trigger.
#[test]
fn an_unknown_id_reaches_nothing() {
    let frames = vec![peer(0, "item", Label::LongPayload), judged(0)];
    assert!(!reaches(
        &PropertyId::new("feed/hand-written"),
        frames.clone()
    ));
    let other = PropertyId::generated(
        GeneratedId {
            family: "feed",
            stream: StreamId {
                kind: 9,
                version: 1,
            },
            row: Some("item"),
            template: Template::Closes(Label::LongPayload),
        }
        .to_string(),
    )
    .expect("a generated ID");
    assert!(!reaches(&other, frames));
}
