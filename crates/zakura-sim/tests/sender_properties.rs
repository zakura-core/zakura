//! The sender generated properties (properties.md, Generated properties,
//! per row, sender side), checked on scripted observations: the node's
//! frames as a peer's role models labelled them on arrival.
#![cfg(zakura_sim)]

mod support;

use support::{
    feed::FEED,
    script::{run_under, Node, OBSERVED},
};
use zakura_network::zakura::{testkit::regulation::TEST_FAMILY, Stream};
use zakura_sim::{
    model::Label,
    observe::StreamId,
    property::Property,
    table::{sender_properties, table_properties, GeneratedId, Template},
    Observation, PropertyOutcome, RunReport,
};

/// The `feed` stream: the `watch` subscription, its `item` pages and its
/// `ended` terminal outcome.
const FEED_STREAM: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    messages: Some(FEED),
    ..Stream::PERSISTENT
};

/// The regulation test family: the `get` request and its `part`, `done`
/// and `failed` responses, the `status` announcement, and the `ping`
/// request with a cadence and its `pong` response.
const TEST_STREAM: Stream = Stream {
    kind: 65,
    version: 2,
    frame_cap: 1024,
    capability: 1 << 17,
    messages: Some(TEST_FAMILY),
    ..Stream::PERSISTENT
};

/// The ID of `stream`.
fn id(stream: Stream) -> StreamId {
    StreamId {
        kind: stream.kind,
        version: stream.version,
    }
}

/// The sender and receiver properties of both fixture streams.
fn properties() -> Vec<Property<Node>> {
    let mut properties: Vec<Property<Node>> = sender_properties("feed", &[FEED_STREAM]);
    properties.extend(sender_properties("test", &[TEST_STREAM]));
    properties.extend(table_properties("feed", &[FEED_STREAM]));
    properties.extend(table_properties("test", &[TEST_STREAM]));
    properties
}

/// Runs `script` under [`properties`].
fn run_script(script: Vec<Vec<Observation>>) -> RunReport {
    run_under(properties(), OBSERVED, script)
}

/// A frame the node sent on connection 1 that the peer's models labelled
/// `label` under the rule of `row`.
fn sent(stream: Stream, seq: u64, row: &str, label: Label) -> Observation {
    node_frame(stream, seq, row, label, false)
}

/// A conformant frame of `row` charged to an empty cadence bucket.
fn exhausting(stream: Stream, seq: u64, row: &str) -> Observation {
    node_frame(stream, seq, row, Label::Conformant, true)
}

fn node_frame(stream: Stream, seq: u64, row: &str, label: Label, exhausted: bool) -> Observation {
    Observation::NodeFrame {
        connection: 1,
        stream: id(stream),
        quic_stream: None,
        seq,
        row: Some(row.to_owned()),
        rule_stream: id(stream),
        label,
        exhausted,
    }
}

/// The outcome the run reports for `id`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == id)
        .unwrap_or_else(|| panic!("{id} is registered"))
        .outcome
}

fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { applicable } if *applicable > 0)
}

fn not_applicable(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::NotApplicable)
}

/// The property that stopped the run, if any.
fn stopped_by(report: &RunReport) -> Option<&str> {
    report.end.violation.as_ref().map(|v| v.property.as_str())
}

const LIMIT: &str = "test/s65v2/get/sender-within-limit";
const ANSWERS: &str = "test/s65v2/part/sender-answers-open";
const ENDS: &str = "test/s65v2/done/sender-ends-once";
const LIVE: &str = "feed/s64v1/watch/sender-within-live";
const CREDIT: &str = "feed/s64v1/watch/sender-within-credit";
const ITEM_ANSWERS: &str = "feed/s64v1/item/sender-answers-open";
const STATUS_CADENCE: &str = "test/s65v2/status/sender-cadence";
const PING_CADENCE: &str = "test/s65v2/ping/sender-cadence";

#[test]
fn every_row_yields_its_sender_templates() {
    let ids = |family, stream| -> Vec<String> {
        sender_properties::<Node>(family, &[stream])
            .into_iter()
            .map(|p| p.id.to_string())
            .collect()
    };
    let test = ids("test", TEST_STREAM);
    // `get` and `ping` are requests and `ping` has a cadence; `part`
    // continues an exchange; `done`, `failed` and `pong` end one; `status`
    // is an announcement, which always has a cadence.
    let expected = [
        LIMIT,
        ANSWERS,
        ENDS,
        "test/s65v2/failed/sender-ends-once",
        STATUS_CADENCE,
        "test/s65v2/ping/sender-within-limit",
        PING_CADENCE,
        "test/s65v2/pong/sender-ends-once",
    ];
    assert_eq!(test, expected);
    for id in &test {
        let parsed = GeneratedId::parse(id).expect("a generated ID parses back");
        assert_eq!(parsed.to_string(), *id);
    }

    let feed = ids("feed", FEED_STREAM);
    assert_eq!(
        feed,
        [
            LIVE,
            CREDIT,
            ITEM_ANSWERS,
            "feed/s64v1/ended/sender-ends-once"
        ]
    );
    assert_eq!(
        GeneratedId::parse(CREDIT).map(|id| id.template),
        Some(Template::SenderWithinCredit)
    );
}

/// The control: conformant frames hold each template of their row, and a
/// row the node never sent stays not applicable.
#[test]
fn conformant_frames_hold() {
    let report = run_script(vec![
        vec![sent(TEST_STREAM, 0, "get", Label::Conformant)],
        vec![sent(TEST_STREAM, 1, "part", Label::Conformant)],
        vec![sent(TEST_STREAM, 2, "done", Label::Conformant)],
        vec![sent(FEED_STREAM, 0, "watch", Label::Conformant)],
        vec![sent(TEST_STREAM, 3, "status", Label::Conformant)],
    ]);
    assert_eq!(report.end.violation, None);
    for id in [LIMIT, ANSWERS, ENDS, LIVE, CREDIT, STATUS_CADENCE] {
        assert!(held(&report, id), "{id}: {:?}", outcome(&report, id));
    }
    assert!(not_applicable(&report, PING_CADENCE));
    assert!(not_applicable(&report, ITEM_ANSWERS));
}

/// A request past the advertised limit breaks `sender-within-limit`. The
/// peer labels it under the sender's rule, so the receiver's margin
/// doesn't excuse it, and the node's frame opens no receiver obligation.
#[test]
fn request_past_the_limit_fails() {
    let report = run_script(vec![
        vec![sent(TEST_STREAM, 0, "get", Label::Conformant)],
        vec![sent(TEST_STREAM, 1, "get", Label::OverMargin)],
    ]);
    assert_eq!(stopped_by(&report), Some(LIMIT));
    assert!(not_applicable(&report, "test/s65v2/get/over-margin-closes"));
    let violation = report.end.violation.expect("checked above");
    assert!(violation.actual.contains("OverMargin"), "{violation:?}");
}

/// A response for no open exchange breaks `sender-answers-open`; an ending
/// for no open exchange, such as a second ending, breaks
/// `sender-ends-once`. Each violation lands on one property.
#[test]
fn unsolicited_responses_fail_by_row() {
    let part = run_script(vec![vec![sent(TEST_STREAM, 0, "part", Label::Unsolicited)]]);
    assert_eq!(stopped_by(&part), Some(ANSWERS));
    assert!(not_applicable(&part, ENDS));

    let second_ending = run_script(vec![
        vec![sent(TEST_STREAM, 0, "done", Label::Conformant)],
        vec![sent(TEST_STREAM, 1, "done", Label::Unsolicited)],
    ]);
    assert_eq!(stopped_by(&second_ending), Some(ENDS));
    assert!(not_applicable(&second_ending, ANSWERS));
}

/// An `Open` past `max_live` breaks `sender-within-live`, and a page past
/// the granted credit breaks the subscription row's
/// `sender-within-credit`, not the page row's `sender-answers-open`.
#[test]
fn subscription_frames_fail_under_their_subscription_row() {
    let live = run_script(vec![
        vec![sent(FEED_STREAM, 0, "watch", Label::Conformant)],
        vec![sent(FEED_STREAM, 1, "watch", Label::OverLive)],
    ]);
    assert_eq!(stopped_by(&live), Some(LIVE));

    // The peer's models put a page's `OverCredit` on its subscription row.
    let credit = run_script(vec![
        vec![sent(FEED_STREAM, 0, "item", Label::Conformant)],
        vec![sent(FEED_STREAM, 1, "watch", Label::OverCredit)],
    ]);
    assert_eq!(stopped_by(&credit), Some(CREDIT));
    assert!(held(&credit, ITEM_ANSWERS));
    assert!(held(&credit, LIVE));
}

/// A frame charged to the peer's empty cadence bucket breaks
/// `sender-cadence` for its row only, though the peer still labels it
/// conformant.
#[test]
fn emptying_the_cadence_bucket_fails() {
    let report = run_script(vec![
        vec![sent(TEST_STREAM, 0, "ping", Label::Conformant)],
        vec![exhausting(TEST_STREAM, 1, "ping")],
    ]);
    assert_eq!(stopped_by(&report), Some(PING_CADENCE));
    assert!(held(&report, "test/s65v2/ping/sender-within-limit"));
    assert!(not_applicable(&report, STATUS_CADENCE));
}
