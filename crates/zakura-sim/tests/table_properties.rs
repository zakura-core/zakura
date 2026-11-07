//! P3b: the consequence table and the generated receiver properties
//! (properties.md, Consequence table and Generated properties), checked on
//! scripted observations.
#![cfg(zakura_sim)]

mod support;

use std::collections::BTreeSet;

use support::{
    feed::{FEED, ITEM},
    script::{run_under, Node, OBSERVED},
};
use zakura_network::zakura::{
    testkit::regulation::{production_tables, TEST_FAMILY},
    MessageRole, MessageRule, Stream,
};
use zakura_sim::{
    model::{consequence, is_fault_cause, Consequence, Label},
    observe::StreamId,
    property::{Property, Source},
    table::{rule_row, table_properties, GeneratedId, Template},
    Observation, Outcome, PropertyOutcome, RunReport,
};

/// The `feed` stream: a subscription with its page and terminal outcome.
const FEED_STREAM: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    messages: Some(FEED),
    ..Stream::PERSISTENT
};

/// The regulation test family, whose `status` announcement has a cadence.
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

/// Runs `script`, one batch per step, under the generated properties of
/// `feed` on `FEED_STREAM` and `test` on `TEST_STREAM`.
fn run_script(script: Vec<Vec<Observation>>) -> RunReport {
    let mut properties: Vec<Property<Node>> = table_properties("feed", &[FEED_STREAM]);
    properties.extend(table_properties("test", &[TEST_STREAM]));
    run_under(properties, OBSERVED, script)
}

/// A frame the peer sent on `stream`, labelled `label` under `row`.
fn frame(stream: Stream, seq: u64, row: Option<&str>, label: Label) -> Observation {
    Observation::PeerFrame {
        connection: 1,
        stream: id(stream),
        quic_stream: None,
        seq,
        row: row.map(str::to_owned),
        rule_stream: id(stream),
        label,
        exhausted: false,
        exchange: None,
        payload: 0,
    }
}

/// The node's reader reaching frame `seq` on `stream`.
fn judged(stream: Stream, seq: u64) -> Observation {
    Observation::FrameJudged {
        connection: 1,
        stream: id(stream),
        quic_stream: None,
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

/// The outcome the run reports for the generated property `id`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == id)
        .unwrap_or_else(|| panic!("{id} is registered"))
        .outcome
}

/// True if `outcome` is `Held`.
fn held(outcome: &PropertyOutcome) -> bool {
    matches!(outcome, PropertyOutcome::Held { .. })
}

#[test]
fn every_row_yields_its_templates() {
    for table in production_tables()
        .into_iter()
        .map(|t| (t.family, t.stream))
    {
        let (family, stream) = table;
        let rules = stream.messages.expect("production streams have tables");
        let per_row: usize = rules
            .iter()
            .map(|rule| match rule.role {
                // Three framing labels, the role's own, `BadContent`, and
                // the cadence template. An announcement always has a cadence.
                MessageRole::Announcement { .. } => 3 + 1 + 1,
                MessageRole::Request { cadence, .. } => 3 + 1 + 1 + usize::from(cadence.is_some()),
                MessageRole::Response { .. } => 3 + 2 + 1,
                MessageRole::Subscription { cadence, .. } => {
                    3 + 4 + 1 + usize::from(cadence.is_some())
                }
            })
            .sum();
        let properties: Vec<Property<Node>> = table_properties(family, &[stream]);
        assert_eq!(properties.len(), 2 + per_row, "{family}");
        let prefix = format!("{family}/s{}v{}/", stream.kind, stream.version);
        assert!(properties
            .iter()
            .all(|p| p.id.as_str().starts_with(&prefix)));
    }
}

#[test]
fn generated_ids_are_unique_and_parse_back() {
    let mut all: Vec<Property<Node>> = Vec::new();
    for table in production_tables() {
        all.extend(table_properties(table.family, &[table.stream]));
    }
    all.extend(table_properties("feed", &[FEED_STREAM]));
    all.extend(table_properties("test", &[TEST_STREAM]));
    let ids: BTreeSet<&str> = all.iter().map(|p| p.id.as_str()).collect();
    assert_eq!(ids.len(), all.len(), "no two generated IDs collide");
    for id in ids {
        let parsed = GeneratedId::parse(id).unwrap_or_else(|| panic!("{id} parses"));
        assert_eq!(parsed.to_string(), id);
    }
    assert_eq!(
        GeneratedId::parse("feed/s64v1/watch/over-live-closes"),
        Some(GeneratedId {
            family: "feed",
            stream: id(FEED_STREAM),
            row: Some("watch"),
            template: Template::Closes(Label::OverLive),
        })
    );
    // Conformant frames have no `-closes` template; content faults do.
    assert_eq!(Template::parse("conformant-closes"), None);
    assert_eq!(
        Template::parse("bad-content-closes"),
        Some(Template::Closes(Label::BadContent))
    );
}

#[test]
fn consequences_follow_the_table() {
    for label in Label::ALL {
        let expected = match label {
            Label::Conformant => Consequence::NoPunishment,
            Label::BadContent => Consequence::FamilyContent,
            _ => Consequence::FaultClose,
        };
        assert_eq!(consequence(label), expected, "{label}");
    }
    assert!(is_fault_cause("service_protocol_reject"));
    assert!(is_fault_cause("ordered_read_rejected"));
    assert!(is_fault_cause("undersized_frame_cap"));
    for neutral in [
        "idle_timeout",
        "ordered_read_timeout",
        "ordered_read_transport",
        "unfinished_exchange",
    ] {
        assert!(!is_fault_cause(neutral), "{neutral}");
    }
}

#[test]
fn a_pages_over_credit_belongs_to_its_subscription_row() {
    let item = MessageRule::find(FEED, ITEM).unwrap();
    let layout = [FEED_STREAM];
    let rule = |label| {
        let (stream, row) = rule_row(&layout, &FEED_STREAM, item, label);
        (id(*stream), row.name)
    };
    assert_eq!(rule(Label::OverCredit), (id(FEED_STREAM), "watch"));
    assert_eq!(rule(Label::Unsolicited), (id(FEED_STREAM), "item"));
    assert_eq!(rule(Label::Conformant), (id(FEED_STREAM), "item"));
}

/// A conformant `feed` stream and a faulty `test` stream share one
/// connection. The node closes the connection for the fault, so the `feed`
/// stream's `conformant-never-closes` holds (review P-07).
#[test]
fn sibling_stream_fault_closes_without_violation() {
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
            judged(FEED_STREAM, 0),
        ],
        vec![
            frame(TEST_STREAM, 0, Some("ping"), Label::ReservedFlags),
            judged(TEST_STREAM, 0),
        ],
        vec![closed("service_protocol_reject")],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(outcome(&report, "feed/s64v1/conformant-never-closes")));
    assert!(held(outcome(
        &report,
        "test/s65v2/ping/reserved-flags-closes"
    )));
}

/// The node records the fault cause in a step after the one that judged the
/// frame, still before quiescence, so `<label>-closes` holds. A faulty frame
/// the reader never reached opens no obligation (review P-02).
#[test]
fn close_cause_recorded_before_quiescence() {
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::OverLive),
            judged(FEED_STREAM, 0),
            frame(FEED_STREAM, 1, Some("item"), Label::Unsolicited),
        ],
        vec![],
        vec![closed("service_protocol_reject")],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(outcome(&report, "feed/s64v1/watch/over-live-closes")));
    assert_eq!(
        *outcome(&report, "feed/s64v1/item/unsolicited-closes"),
        PropertyOutcome::NotApplicable
    );
}

#[test]
fn a_fault_without_a_fault_close_is_a_violation() {
    // A neutral close doesn't discharge the obligation.
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, None, Label::UndeclaredType),
            judged(FEED_STREAM, 0),
        ],
        vec![closed("idle_timeout")],
    ]);
    let violation = report.end.violation.expect("the fault went unpunished");
    assert_eq!(violation.property, "feed/s64v1/undeclared-type-closes");

    // Nor does a close of another connection, justified by its own fault.
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("ended"), Label::OverOwed),
            judged(FEED_STREAM, 0),
            Observation::PeerFrame {
                connection: 2,
                stream: id(TEST_STREAM),
                quic_stream: None,
                seq: 0,
                row: Some("ping".to_owned()),
                rule_stream: id(TEST_STREAM),
                label: Label::ReservedFlags,
                exhausted: false,
                exchange: None,
                payload: 0,
            },
            Observation::FrameJudged {
                connection: 2,
                stream: id(TEST_STREAM),
                quic_stream: None,
                seq: 0,
            },
        ],
        vec![Observation::ConnectionClosed {
            connection: 2,
            cause: "service_protocol_reject".to_owned(),
        }],
    ]);
    let violation = report.end.violation.expect("the fault went unpunished");
    assert_eq!(violation.property, "feed/s64v1/ended/over-owed-closes");
}

/// V12 F-298: a frame with bad content opens an obligation that a
/// `Misbehavior` or a fault close discharges, and nothing else does.
#[test]
fn bad_content_gets_its_family_consequence() {
    const ID: &str = "feed/s64v1/watch/bad-content-closes";
    let bad = || {
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::BadContent),
            judged(FEED_STREAM, 0),
        ]
    };
    for consequence in [
        Observation::Misbehavior { connection: 1 },
        closed("header_peer_fault"),
    ] {
        let report = run_script(vec![bad(), vec![consequence.clone()]]);
        assert_eq!(report.end.violation, None, "{consequence:?}");
        assert!(held(outcome(&report, ID)), "{consequence:?}");
    }
    let report = run_script(vec![bad(), vec![closed("idle_timeout")]]);
    let violation = report
        .end
        .violation
        .expect("the bad content went unpunished");
    assert_eq!(violation.property, ID);
}

#[test]
fn punishing_a_conformant_peer_is_a_violation() {
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
            judged(FEED_STREAM, 0),
        ],
        vec![closed("header_peer_fault")],
    ]);
    let violation = report.end.violation.expect("the close is unjustified");
    assert_eq!(violation.property, "feed/s64v1/conformant-never-closes");

    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("item"), Label::Conformant),
            judged(FEED_STREAM, 0),
        ],
        vec![Observation::Misbehavior { connection: 1 }],
    ]);
    let violation = report
        .end
        .violation
        .expect("the Misbehavior is unjustified");
    assert_eq!(violation.property, "feed/s64v1/conformant-never-closes");

    // A neutral close is no punishment.
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
            judged(FEED_STREAM, 0),
        ],
        vec![closed("idle_timeout")],
    ]);
    assert_eq!(report.end.violation, None);
    assert!(held(outcome(&report, "feed/s64v1/conformant-never-closes")));
}

/// A fault the node judges after its close can't justify the close, even
/// within one step.
#[test]
fn a_later_fault_does_not_justify_an_earlier_close() {
    let report = run_script(vec![vec![
        frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
        judged(FEED_STREAM, 0),
        frame(TEST_STREAM, 0, Some("ping"), Label::ReservedFlags),
        closed("service_protocol_reject"),
        judged(TEST_STREAM, 0),
    ]]);
    let violation = report.end.violation.expect("the close came first");
    assert_eq!(violation.property, "feed/s64v1/conformant-never-closes");
}

#[test]
fn emptying_a_cadence_bucket_is_never_punished() {
    let exhausting = Observation::PeerFrame {
        connection: 1,
        stream: id(TEST_STREAM),
        quic_stream: None,
        seq: 0,
        row: Some("status".to_owned()),
        rule_stream: id(TEST_STREAM),
        label: Label::Conformant,
        exhausted: true,
        exchange: None,
        payload: 0,
    };
    let report = run_script(vec![
        vec![exhausting, judged(TEST_STREAM, 0)],
        vec![closed("request_protocol_reject")],
    ]);
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("the close is unjustified");
    // Both claims cover the close; the registry reports the first.
    assert!(
        violation.property == "test/s65v2/conformant-never-closes"
            || violation.property == "test/s65v2/status/cadence-never-closes",
        "{violation:?}"
    );
    assert!(matches!(
        outcome(&report, "test/s65v2/status/cadence-never-closes"),
        PropertyOutcome::Violated { .. }
    ));
}

/// Review P3b-1: a judged frame with no label, or with two, fails the run
/// as a harness failure instead of dropping out of the checks.
#[test]
fn a_judged_frame_needs_exactly_one_label() {
    let unlabelled = run_script(vec![vec![judged(FEED_STREAM, 0)]]);
    let labelled_twice = run_script(vec![vec![
        frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
        frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
        judged(FEED_STREAM, 0),
    ]]);
    for (report, detail) in [
        (unlabelled, "has no PeerFrame label before it"),
        (labelled_twice, "has more than one PeerFrame label"),
    ] {
        assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
        let failure = report.end.harness.as_ref().expect("a harness failure");
        assert!(failure.to_string().ends_with(detail), "{failure}");
    }
}

/// Review P3b-2: two conformant connections punished at one boundary are
/// two violations.
#[test]
fn every_unjustified_punishment_is_a_violation() {
    let on = |connection, obs: Observation| match obs {
        Observation::PeerFrame {
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
            quic_stream: None,
            seq,
            row,
            rule_stream,
            label,
            exhausted,
            exchange,
            payload,
        },
        Observation::FrameJudged { stream, seq, .. } => Observation::FrameJudged {
            connection,
            stream,
            quic_stream: None,
            seq,
        },
        Observation::ConnectionClosed { cause, .. } => {
            Observation::ConnectionClosed { connection, cause }
        }
        obs => obs,
    };
    let report = run_script(vec![vec![
        frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
        judged(FEED_STREAM, 0),
        on(2, frame(FEED_STREAM, 0, Some("watch"), Label::Conformant)),
        on(2, judged(FEED_STREAM, 0)),
        closed("header_peer_fault"),
        on(2, closed("header_peer_fault")),
    ]]);
    let property = report
        .properties
        .iter()
        .find(|p| p.property == "feed/s64v1/conformant-never-closes")
        .expect("the property reports");
    let connections: Vec<&str> = property
        .violations
        .iter()
        .map(|v| v.expected.split('\'').next().unwrap_or_default())
        .collect();
    assert_eq!(connections, ["connection 1", "connection 2"]);
}

/// Review (#1752): a punishment before the node judged anything on the
/// connection is unjustified.
#[test]
fn punishment_before_any_judgement_is_a_violation() {
    let report = run_script(vec![
        vec![closed("header_peer_fault")],
        vec![
            frame(FEED_STREAM, 0, Some("watch"), Label::Conformant),
            judged(FEED_STREAM, 0),
        ],
    ]);
    let violation = report
        .end
        .violation
        .as_ref()
        .expect("nothing justified the close");
    assert!(
        violation.property.ends_with("/conformant-never-closes"),
        "{violation:?}"
    );
    for property in [
        "feed/s64v1/conformant-never-closes",
        "test/s65v2/conformant-never-closes",
    ] {
        assert!(
            matches!(outcome(&report, property), PropertyOutcome::Violated { .. }),
            "{property}"
        );
    }
}

/// Review (#1752): a fault whose rule has no `<label>-closes` fails the run
/// as missing evidence instead of passing unchecked.
#[test]
fn an_unclaimed_fault_is_missing_evidence() {
    for (row, label, claim) in [
        (
            Some("item"),
            Label::OverCredit,
            "feed/s64v1/item/over-credit-closes",
        ),
        (
            None,
            Label::ReservedFlags,
            "feed/s64v1/reserved-flags-closes",
        ),
    ] {
        let report = run_script(vec![vec![
            frame(FEED_STREAM, 0, row, label),
            judged(FEED_STREAM, 0),
        ]]);
        assert_eq!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
        let failure = report.end.harness.as_ref().expect("a harness failure");
        assert!(
            failure.to_string().ends_with(&format!("has no {claim}")),
            "{failure}"
        );
    }
}

/// Review (#1752): a declared type sent to the wrong reader keeps its row,
/// and the stream's `undeclared-type-closes` still claims it.
#[test]
fn an_undeclared_type_with_a_row_must_close() {
    let report = run_script(vec![
        vec![
            frame(FEED_STREAM, 0, Some("item"), Label::UndeclaredType),
            judged(FEED_STREAM, 0),
        ],
        vec![closed("idle_timeout")],
    ]);
    let violation = report.end.violation.expect("the fault went unpunished");
    assert_eq!(violation.property, "feed/s64v1/undeclared-type-closes");
}

/// Review (#1752): the templates that read `Misbehavior` need storage, so a
/// harness that doesn't observe it can't report them as held.
#[test]
fn misbehavior_readers_need_storage() {
    let properties: Vec<Property<Node>> = table_properties("feed", &[FEED_STREAM]);
    let needs_storage = |id: &str| {
        properties
            .iter()
            .find(|p| p.id.as_str() == id)
            .unwrap_or_else(|| panic!("{id} is generated"))
            .needs
            .contains(&Source::Storage)
    };
    assert!(needs_storage("feed/s64v1/conformant-never-closes"));
    assert!(needs_storage("feed/s64v1/watch/bad-content-closes"));
    assert!(!needs_storage("feed/s64v1/watch/over-live-closes"));
}

/// `watch` alone on `s64v1`, and its pages and terminal outcome on `s66v1`
/// of the same layout.
fn split_layout() -> [Stream; 2] {
    [
        Stream {
            messages: Some(&FEED[..1]),
            ..FEED_STREAM
        },
        Stream {
            kind: 66,
            messages: Some(&FEED[1..]),
            ..FEED_STREAM
        },
    ]
}

/// Review (#1752): a page's `OverCredit` belongs to its subscription row
/// even when the pages sit on another stream of the layout.
#[test]
fn an_over_credit_page_on_another_stream_belongs_to_its_subscription() {
    let layout = split_layout();
    let [watch, pages] = layout;
    let item = MessageRule::find(&FEED[1..], ITEM).unwrap();
    let (stream, row) = rule_row(&layout, &pages, item, Label::OverCredit);
    assert_eq!((id(*stream), row.name), (id(watch), "watch"));

    let page = Observation::PeerFrame {
        connection: 1,
        stream: id(pages),
        quic_stream: None,
        seq: 0,
        row: Some("watch".to_owned()),
        rule_stream: id(watch),
        label: Label::OverCredit,
        exhausted: false,
        exchange: None,
        payload: 0,
    };
    let report = run_under(
        table_properties("split", &layout),
        OBSERVED,
        vec![vec![page, judged(pages, 0)], vec![closed("idle_timeout")]],
    );
    let violation = report.end.violation.expect("the page went unpunished");
    assert_eq!(violation.property, "split/s64v1/watch/over-credit-closes");
}
