//! P3b: the downstream generated properties (properties.md, Downstream
//! templates), checked on scripted observations. Each test pairs an ordinary
//! rejection check with a bypass that admits the same work, serves the
//! request, or writes the output through another path: the downstream
//! property must fail while the rejection check still passes.
#![cfg(zakura_sim)]

mod support;

use support::{
    feed::FEED,
    script::{run_under, Node, OBSERVED},
};
use zakura_network::zakura::{testkit::regulation::TEST_FAMILY, Stream};
use zakura_sim::{
    model::Label,
    observe::{RowAt, StreamId},
    property::{Property, Source},
    table::{downstream_properties, table_properties, Effect, GeneratedId, Template},
    HarnessFailure, Observation, Outcome, PropertyOutcome, RunReport,
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
/// and `failed` responses, among others.
const TEST_STREAM: Stream = Stream {
    kind: 65,
    version: 2,
    frame_cap: 1024,
    capability: 1 << 17,
    messages: Some(TEST_FAMILY),
    ..Stream::PERSISTENT
};

const ALL: &[Effect] = &[Effect::Work, Effect::Serving, Effect::Output];

/// The ID of `stream`.
fn id(stream: Stream) -> StreamId {
    StreamId {
        kind: stream.kind,
        version: stream.version,
    }
}

/// Row `row` of `stream`.
fn row(stream: Stream, row: &str) -> RowAt {
    RowAt::new(id(stream), row)
}

/// The close and downstream properties of both fixture streams.
fn properties() -> Vec<Property<Node>> {
    let mut properties: Vec<Property<Node>> = table_properties("feed", &[FEED_STREAM]);
    properties.extend(table_properties("test", &[TEST_STREAM]));
    properties.extend(downstream_properties("feed", &[FEED_STREAM], ALL));
    properties.extend(downstream_properties("test", &[TEST_STREAM], ALL));
    properties
}

/// Runs `script` under [`properties`].
fn run_script(script: Vec<Vec<Observation>>) -> RunReport {
    run_under(properties(), OBSERVED, script)
}

/// A frame the peer sent on connection 1, labelled `label` under its own
/// row, for exchange `exchange`.
fn frame(
    stream: Stream,
    seq: u64,
    row: &str,
    label: Label,
    exchange: u64,
    payload: u64,
) -> Observation {
    Observation::PeerFrame {
        connection: 1,
        stream: id(stream),
        seq,
        row: Some(row.to_owned()),
        rule_stream: id(stream),
        label,
        exhausted: false,
        exchange: Some(exchange),
        payload,
    }
}

/// The node's reader reaching frame `seq` on `stream`.
fn judged(stream: Stream, seq: u64) -> Observation {
    Observation::FrameJudged {
        connection: 1,
        stream: id(stream),
        seq,
    }
}

/// The node closing connection 1 with `cause`.
fn closed(cause: &str) -> Observation {
    Observation::ConnectionClosed {
        connection: 1,
        cause: cause.to_owned(),
    }
}

/// The node admitting `bytes` of work from the `frame` row frame `seq`,
/// answering `request` for `exchange`.
fn work(
    frame: RowAt,
    seq: u64,
    request: RowAt,
    exchange: u64,
    objects: u64,
    bytes: u64,
) -> Observation {
    Observation::WorkAdmitted {
        connection: 1,
        frame,
        seq,
        request,
        exchange,
        objects,
        bytes,
        ends: false,
    }
}

/// The node's `get` work for exchange 7 from part frame `seq`.
fn part_work(seq: u64, bytes: u64) -> Observation {
    work(
        row(TEST_STREAM, "part"),
        seq,
        row(TEST_STREAM, "get"),
        7,
        1,
        bytes,
    )
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

fn violated(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Violated { .. })
}

const WORK: &str = "test/s65v2/part/receiver-work-authorized";
const UNSOLICITED: &str = "test/s65v2/part/unsolicited-closes";
const CREDIT: &str = "feed/s64v1/watch/receiver-credit-conserved";
const SERVING: &str = "test/s65v2/get/receiver-serving-bounded";
const OVER_MARGIN: &str = "test/s65v2/get/over-margin-closes";
const QUANTITY: &str = "test/s65v2/part/sender-within-response-quantity";
const QUANTITY_DONE: &str = "test/s65v2/done/sender-within-response-quantity";

#[test]
fn every_row_yields_its_downstream_templates() {
    let ids: Vec<String> = downstream_properties::<Node>("test", &[TEST_STREAM], ALL)
        .into_iter()
        .map(|p| p.id.to_string())
        .collect();
    // `get` and `ping` are requests; `part`, `done`, `failed` and `pong`
    // are responses, with two templates each; `status` is an announcement.
    assert_eq!(ids.len(), 2 + 4 * 2, "{ids:?}");
    for id in &ids {
        let parsed = GeneratedId::parse(id).expect("a generated ID parses back");
        assert_eq!(parsed.to_string(), *id);
    }
    assert!(ids.iter().any(|id| id == WORK));
    assert!(ids.iter().any(|id| id == SERVING));
    assert!(ids.iter().any(|id| id == QUANTITY));

    let feed: Vec<String> = downstream_properties::<Node>("feed", &[FEED_STREAM], ALL)
        .into_iter()
        .map(|p| p.id.to_string())
        .collect();
    assert!(feed.iter().any(|id| id == CREDIT));
    // Pages and terminal outcomes answer a subscription, which owes credit,
    // not frames and bytes, so they get no output check.
    assert_eq!(
        feed,
        [
            CREDIT,
            "feed/s64v1/item/receiver-work-authorized",
            "feed/s64v1/ended/receiver-work-authorized",
        ]
    );
    assert_eq!(
        GeneratedId::parse(CREDIT).map(|id| id.template),
        Some(Template::CreditConserved)
    );
}

/// A template whose effect the adapter doesn't observe isn't registered,
/// so a missing adapter shows as a missing ID, not as a pass.
#[test]
fn unobserved_effects_register_nothing() {
    assert!(downstream_properties::<Node>("test", &[TEST_STREAM], &[]).is_empty());
    let work_only: Vec<String> =
        downstream_properties::<Node>("test", &[TEST_STREAM], &[Effect::Work])
            .into_iter()
            .map(|p| p.id.to_string())
            .collect();
    assert!(work_only
        .iter()
        .all(|id| id.ends_with("/receiver-work-authorized")));
    assert_eq!(work_only.len(), 4);
}

/// The control: work within what conformant, judged frames delivered
/// holds, and later work up to the same total still holds.
#[test]
fn authorized_work_holds() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 7, 20)],
        vec![judged(TEST_STREAM, 0)],
        vec![part_work(0, 12)],
        vec![part_work(0, 8)],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(&report, WORK), "{:?}", report.properties);
}

/// A guard rejects an unsolicited part and the node closes, so the
/// ordinary rejection check holds; a second writer still admitted its work,
/// and a correct close doesn't excuse it.
#[test]
fn work_from_a_rejected_frame_fails_despite_the_close() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Unsolicited, 7, 20)],
        vec![
            judged(TEST_STREAM, 0),
            part_work(0, 20),
            closed("service_protocol_reject"),
        ],
    ]);
    assert!(held(&report, UNSOLICITED), "{:?}", report.properties);
    let violation = report.end.violation.expect("the work was unauthorized");
    assert_eq!(violation.property, WORK);
}

/// Work beyond the bytes conformant frames delivered fails, as does work
/// from a frame the reader never judged.
#[test]
fn work_beyond_what_was_delivered_fails() {
    let over = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 7, 20)],
        vec![judged(TEST_STREAM, 0), part_work(0, 12)],
        vec![part_work(0, 12)],
    ]);
    assert_eq!(
        over.end.violation.map(|v| v.property).as_deref(),
        Some(WORK)
    );

    let unjudged = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 7, 20)],
        vec![part_work(0, 1)],
    ]);
    assert_eq!(
        unjudged.end.violation.map(|v| v.property).as_deref(),
        Some(WORK)
    );

    let other_exchange = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 8, 20)],
        vec![judged(TEST_STREAM, 0), part_work(0, 1)],
    ]);
    assert_eq!(
        other_exchange.end.violation.map(|v| v.property).as_deref(),
        Some(WORK)
    );
}

/// Work names the frame it came from. A valid part's unused bytes don't
/// cover work from a later part the models rejected, while a delayed
/// callback for the valid part still holds. Work with no frame fails
/// whatever its size.
#[test]
fn work_spends_only_its_own_frames_bytes() {
    let script = |late_work| {
        vec![
            vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 7, 20)],
            vec![judged(TEST_STREAM, 0)],
            vec![frame(TEST_STREAM, 1, "done", Label::Conformant, 7, 4)],
            vec![judged(TEST_STREAM, 1)],
            vec![frame(TEST_STREAM, 2, "part", Label::Unsolicited, 7, 10)],
            vec![
                judged(TEST_STREAM, 2),
                late_work,
                closed("service_protocol_reject"),
            ],
        ]
    };
    let rejected = run_script(script(part_work(2, 10)));
    assert!(held(&rejected, UNSOLICITED), "{:?}", rejected.properties);
    let violation = rejected.end.violation.expect("frame 2 was rejected");
    assert_eq!(violation.property, WORK);
    assert!(violation.actual.contains("Unsolicited"), "{violation:?}");

    let delayed = run_script(script(part_work(0, 10)));
    assert_eq!(delayed.end.violation, None);
    assert!(held(&delayed, WORK), "{:?}", delayed.properties);

    let no_frame = run_script(vec![vec![part_work(0, 0)]]);
    assert_eq!(
        no_frame.end.violation.map(|v| v.property).as_deref(),
        Some(WORK)
    );
}

/// A storage callback that admits a retired owner's work after its
/// connection closed fails, even within what was delivered.
#[test]
fn stale_owner_work_after_close_fails() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "part", Label::Conformant, 7, 20)],
        vec![judged(TEST_STREAM, 0)],
        vec![closed("idle_timeout")],
        vec![part_work(0, 4)],
    ]);
    let violation = report
        .end
        .violation
        .expect("the owner retired at the close");
    assert_eq!(violation.property, WORK);
    assert!(
        violation.actual.contains("after the close"),
        "{violation:?}"
    );
}

/// The work checks need storage observations. A harness that observes
/// none can't register them.
#[test]
fn missing_storage_observations_fail_the_run() {
    let report = run_under(
        downstream_properties("test", &[TEST_STREAM], &[Effect::Work]),
        &[Source::Frames, Source::Closes, Source::Schedule],
        vec![vec![part_work(0, 1)]],
    );
    assert_eq!(report.end.outcome, Outcome::Harness);
    assert!(
        matches!(
            report.end.harness,
            Some(HarnessFailure::MissingSource { ref source, .. }) if source == "Storage"
        ),
        "{:?}",
        report.end.harness
    );
}

/// The node's `watch` subscription 3: `objects` and `bytes` of work from
/// item frame 0.
fn item_work(objects: u64, bytes: u64) -> Observation {
    work(
        row(FEED_STREAM, "item"),
        0,
        row(FEED_STREAM, "watch"),
        3,
        objects,
        bytes,
    )
}

/// The node granting `objects` and `bytes` on subscription 3.
fn granted(objects: u64, bytes: u64) -> Observation {
    credit(objects, bytes, false)
}

/// The node opening subscription 3 with `objects` and `bytes` of credit.
fn opened(objects: u64, bytes: u64) -> Observation {
    credit(objects, bytes, true)
}

/// The node's update to subscription 3, adding `objects` and `bytes`.
fn credit(objects: u64, bytes: u64, opens: bool) -> Observation {
    Observation::CreditGranted {
        connection: 1,
        subscription: row(FEED_STREAM, "watch"),
        exchange: 3,
        objects,
        bytes,
        opens,
    }
}

/// Work within the granted credit holds, including credit a crossed update
/// granted after the first page was sent but before its work.
#[test]
fn work_within_granted_credit_holds() {
    let report = run_script(vec![
        vec![opened(2, 32)],
        vec![
            frame(FEED_STREAM, 0, "item", Label::Conformant, 3, 30),
            judged(FEED_STREAM, 0),
        ],
        vec![granted(2, 32)],
        vec![item_work(3, 30)],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(&report, CREDIT), "{:?}", report.properties);
}

/// Work beyond the granted credit fails, in objects or in bytes, even when
/// the page itself was labelled conformant.
#[test]
fn work_beyond_granted_credit_fails() {
    for (objects, bytes) in [(5, 10), (1, 40)] {
        let report = run_script(vec![
            vec![opened(4, 32)],
            vec![
                frame(FEED_STREAM, 0, "item", Label::Conformant, 3, 32),
                judged(FEED_STREAM, 0),
            ],
            vec![item_work(objects, bytes)],
        ]);
        assert!(
            violated(&report, CREDIT),
            "{objects} {bytes}: {:?}",
            report.properties
        );
    }
}

/// A terminal outcome spends no credit: pages that use all the granted
/// bytes, then the `ended` frame's work, hold.
#[test]
fn terminal_work_spends_no_credit() {
    let mut ended = work(
        row(FEED_STREAM, "ended"),
        1,
        row(FEED_STREAM, "watch"),
        3,
        0,
        8,
    );
    if let Observation::WorkAdmitted { ends, .. } = &mut ended {
        *ends = true;
    }
    let report = run_script(vec![
        vec![opened(1, 32)],
        vec![
            frame(FEED_STREAM, 0, "item", Label::Conformant, 3, 32),
            frame(FEED_STREAM, 1, "ended", Label::Conformant, 3, 8),
            judged(FEED_STREAM, 0),
            judged(FEED_STREAM, 1),
        ],
        vec![item_work(1, 32), ended],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(&report, CREDIT), "{:?}", report.properties);
}

/// A key that opens again starts from its new grant: credit its earlier
/// subscription left unused doesn't cover the new one's work.
#[test]
fn a_reopened_key_starts_from_its_new_grant() {
    let report = run_script(vec![
        vec![opened(4, 64)],
        vec![opened(1, 64)],
        vec![
            frame(FEED_STREAM, 0, "item", Label::Conformant, 3, 32),
            judged(FEED_STREAM, 0),
        ],
        vec![item_work(2, 32)],
    ]);
    assert!(violated(&report, CREDIT), "{:?}", report.properties);
}

/// The node starting to serve `get` request `exchange`.
fn serving(exchange: u64) -> Observation {
    Observation::Serving {
        connection: 1,
        request: row(TEST_STREAM, "get"),
        exchange,
    }
}

/// The node retiring a serving owner of `get` request `exchange`.
fn serving_ended(exchange: u64) -> Observation {
    Observation::ServingEnded {
        connection: 1,
        request: row(TEST_STREAM, "get"),
        exchange,
    }
}

/// Serving a conformant, judged request once holds. Serving it twice, or
/// serving an over-margin request the node closed for, fails; the ordinary
/// over-margin check still holds.
#[test]
fn serving_is_bounded_by_conformant_requests() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4)],
        vec![judged(TEST_STREAM, 0), owed(2, 40), serving(1)],
        vec![output("done", 4), serving_ended(1)],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(&report, SERVING), "{:?}", report.properties);

    let twice = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4)],
        vec![judged(TEST_STREAM, 0), serving(1)],
        vec![serving(1)],
    ]);
    assert_eq!(
        twice.end.violation.map(|v| v.property).as_deref(),
        Some(SERVING)
    );

    let over_margin = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::OverMargin, 1, 4)],
        vec![
            judged(TEST_STREAM, 0),
            serving(1),
            closed("request_protocol_reject"),
        ],
    ]);
    assert!(
        held(&over_margin, OVER_MARGIN),
        "{:?}",
        over_margin.properties
    );
    assert_eq!(
        over_margin.end.violation.map(|v| v.property).as_deref(),
        Some(SERVING)
    );
}

/// An owner started after the node wrote the request's ending serves no
/// open request, though the ending itself was within what was owed.
#[test]
fn serving_an_answered_request_fails() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4)],
        vec![judged(TEST_STREAM, 0), owed(2, 40)],
        vec![output("done", 4)],
        vec![serving(1)],
    ]);
    assert!(held(&report, QUANTITY_DONE), "{:?}", report.properties);
    let violation = report.end.violation.expect("the request had ended");
    assert_eq!(violation.property, SERVING);
    assert!(
        violation.actual.contains("no request open"),
        "{violation:?}"
    );
}

/// Once the run rests, an owner whose request ended must have retired. An
/// owner that outlives its answered request fails, as does one that
/// outlives its connection.
#[test]
fn owners_outliving_their_requests_fail() {
    let leaked = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4)],
        vec![judged(TEST_STREAM, 0), owed(2, 40), serving(1)],
        vec![output("done", 4)],
    ]);
    let violation = leaked.end.violation.expect("the owner never retired");
    assert_eq!(violation.property, SERVING);
    assert!(
        violation.actual.contains("1 owners are live for 0"),
        "{violation:?}"
    );

    let closed_under = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4)],
        vec![judged(TEST_STREAM, 0), serving(1)],
        vec![closed("idle_timeout")],
    ]);
    assert_eq!(
        closed_under.end.violation.map(|v| v.property).as_deref(),
        Some(SERVING)
    );
}

/// The peer's conformant `get` request 1, judged, owed `frames` parts and
/// `bytes`.
fn requested(frames: u32, bytes: u64) -> Vec<Observation> {
    vec![
        frame(TEST_STREAM, 0, "get", Label::Conformant, 1, 4),
        judged(TEST_STREAM, 0),
        owed(frames, bytes),
    ]
}

/// The peer opening `get` exchange 1 with the node, owed `frames` parts
/// and `bytes`.
fn owed(frames: u32, bytes: u64) -> Observation {
    Observation::ExchangeOwed {
        connection: 1,
        request: row(TEST_STREAM, "get"),
        exchange: 1,
        frames,
        bytes,
    }
}

/// The node writing a `row` frame of `payload` bytes for exchange 1.
fn output(response: &str, payload: u64) -> Observation {
    Observation::NodeOutput {
        connection: 1,
        frame: row(TEST_STREAM, response),
        request: row(TEST_STREAM, "get"),
        exchange: 1,
        payload,
        ends: response != "part",
    }
}

/// Output within what the request owes holds: two parts and the ending.
#[test]
fn output_within_what_is_owed_holds() {
    let report = run_script(vec![
        requested(2, 40),
        vec![output("part", 16)],
        vec![output("part", 16)],
        vec![output("done", 4)],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(&report, QUANTITY), "{:?}", report.properties);
    assert!(held(&report, QUANTITY_DONE), "{:?}", report.properties);
    assert!(held(&report, SERVING), "{:?}", report.properties);
}

/// Output above the owed frames or bytes fails, as does output for an
/// exchange the peer never opened, that already ended, or whose connection
/// closed.
#[test]
fn output_above_what_is_owed_fails() {
    let cases: [(&str, Vec<Vec<Observation>>, &str); 5] = [
        (
            "a third part",
            vec![
                requested(2, 100),
                vec![output("part", 1), output("part", 1)],
                vec![output("part", 1)],
            ],
            QUANTITY,
        ),
        (
            "bytes past the ending's room",
            vec![
                requested(2, 20),
                vec![output("part", 18)],
                vec![output("done", 4)],
            ],
            QUANTITY_DONE,
        ),
        ("never opened", vec![vec![output("part", 1)]], QUANTITY),
        (
            "after the ending",
            vec![
                requested(2, 20),
                vec![output("done", 4)],
                vec![output("part", 1)],
            ],
            QUANTITY,
        ),
        (
            "after the connection closed",
            vec![
                requested(2, 40),
                vec![closed("idle_timeout")],
                vec![output("part", 8)],
            ],
            QUANTITY,
        ),
    ];
    for (case, script, property) in cases {
        let report = run_script(script);
        assert!(
            violated(&report, property),
            "{case}: {:?}",
            report.properties
        );
    }
}

/// Output answers only an open conformant request. A node that answers an
/// over-margin request without starting an owner fails, though the peer's
/// model recorded what the exchange owes and the ordinary over-margin check
/// holds.
#[test]
fn answering_an_over_margin_request_fails() {
    let report = run_script(vec![
        vec![frame(TEST_STREAM, 0, "get", Label::OverMargin, 1, 4)],
        vec![
            judged(TEST_STREAM, 0),
            owed(2, 40),
            output("done", 4),
            closed("request_protocol_reject"),
        ],
    ]);
    assert!(held(&report, OVER_MARGIN), "{:?}", report.properties);
    assert!(held(&report, QUANTITY_DONE), "{:?}", report.properties);
    let violation = report
        .end
        .violation
        .expect("no conformant request was open");
    assert_eq!(violation.property, SERVING);
    assert!(
        violation.actual.contains("no request open"),
        "{violation:?}"
    );
}

/// `receiver-serving-bounded` needs the node's outputs as well as its
/// owners, since an ending closes a request: an adapter that observes only
/// owners can't register it.
#[test]
fn serving_needs_outputs_too() {
    let ids = |effects| {
        downstream_properties::<Node>("test", &[TEST_STREAM], effects)
            .into_iter()
            .map(|p| p.id.as_str().to_owned())
            .collect::<Vec<_>>()
    };
    assert!(!ids(&[Effect::Serving]).iter().any(|id| id == SERVING));
    assert!(ids(&[Effect::Serving, Effect::Output])
        .iter()
        .any(|id| id == SERVING));
}
