//! P3a: each role model labels fixture frames as properties.md's violation
//! table says (P3.1), including the three by-design `Conformant` cases. At
//! each limit a fixture sends the boundary frame and the frame one over.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use support::feed::{self, FEED};
use zakura_network::zakura::{testkit::regulation::TEST_FAMILY, Credit, MessageRule};
use zakura_sim::model::{
    CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Label, Labels, Owed, ReaderEnd,
    SubscriptionModel,
};

/// The row named `name` in `rules`.
fn row(rules: &'static [MessageRule], name: &str) -> &'static MessageRule {
    rules
        .iter()
        .find(|row| row.name == name)
        .unwrap_or_else(|| panic!("no row {name}"))
}

/// A header of `rules`' row `name` with `payload_len` bytes and no flags.
fn header(rules: &'static [MessageRule], name: &str, payload_len: usize) -> FrameHeader {
    FrameHeader {
        message_type: row(rules, name).message_type,
        flags: 0,
        payload_len,
    }
}

/// `UndeclaredType`, `ReservedFlags`, `ShortPayload`, and `LongPayload` at
/// each bound, and a frame with two faults.
#[test]
fn labels_fixture_frames_headers() {
    let persistent = FrameModel::new(TEST_FAMILY, ReaderEnd::Persistent);
    let part = |len| header(TEST_FAMILY, "part", len);
    // `part` carries 1 to 64 bytes behind its length prefix.
    let (min, max) = (
        row(TEST_FAMILY, "part").payload.min(),
        row(TEST_FAMILY, "part").payload.max(),
    );
    assert_eq!(persistent.labels(&part(min)).first(), Label::Conformant);
    assert_eq!(persistent.labels(&part(max)).first(), Label::Conformant);
    assert_eq!(
        persistent.labels(&part(min - 1)).first(),
        Label::ShortPayload
    );
    assert_eq!(
        persistent.labels(&part(max + 1)).first(),
        Label::LongPayload
    );
    let flagged = FrameHeader {
        flags: 1,
        ..part(min - 1)
    };
    let labels = persistent.labels(&flagged);
    assert_eq!(labels.first(), Label::ReservedFlags);
    assert!(labels.contains(Label::ShortPayload), "{labels:?}");
    let undeclared = FrameHeader {
        message_type: persistent.undeclared_type(),
        ..part(min)
    };
    assert_eq!(
        persistent.labels(&undeclared).first(),
        Label::UndeclaredType
    );

    // A responder reads requests only, and a requester responses only.
    let responder = FrameModel::new(TEST_FAMILY, ReaderEnd::Responder);
    assert_eq!(responder.labels(&part(min)).first(), Label::UndeclaredType);
    assert_eq!(
        responder.labels(&header(TEST_FAMILY, "get", 4)).first(),
        Label::Conformant
    );
    let requester = FrameModel::new(TEST_FAMILY, ReaderEnd::Requester);
    assert_eq!(
        requester.labels(&header(TEST_FAMILY, "get", 4)).first(),
        Label::UndeclaredType
    );

    // The alphabet reaches every header label and a conformant frame.
    let labels: Labels = persistent
        .alphabet(row(TEST_FAMILY, "part"))
        .iter()
        .fold(Labels::NONE, |all, header| {
            all.union(persistent.labels(header))
        });
    for label in [
        Label::UndeclaredType,
        Label::ReservedFlags,
        Label::ShortPayload,
        Label::LongPayload,
    ] {
        assert!(labels.contains(label), "the alphabet misses {label}");
    }
    assert!(persistent
        .alphabet(row(TEST_FAMILY, "part"))
        .iter()
        .any(|header| persistent.labels(header).is_conformant()));
}

/// `OverMargin` one request past twice the highest advertised limit;
/// requests inside the margin are conformant by design.
#[test]
fn labels_fixture_frames_requests() {
    let get = row(TEST_FAMILY, "get");
    let mut model = ExchangeModel::new(TEST_FAMILY, get.message_type).expect("get is a request");
    model.advertise(0, 2);
    for _ in 0..4 {
        assert!(model.request_frame(0).is_conformant());
    }
    assert_eq!(model.request_frame(0).first(), Label::OverMargin);
    assert_eq!(model.serving(0), 4, "a refused request opens nothing");

    // A lowered limit keeps the highest one; an answer frees a request.
    model.advertise(0, 1);
    assert_eq!(model.request_frame(0).first(), Label::OverMargin);
    assert!(model.answered(0));
    assert!(model.request_frame(0).is_conformant());

    // The row's `max_in_flight` of 4 caps an advertisement.
    model.advertise(1, 100);
    for _ in 0..8 {
        assert!(model.request_frame(1).is_conformant());
    }
    assert_eq!(model.request_frame(1).first(), Label::OverMargin);
}

/// `Unsolicited` and `OverOwed` at the frame and byte limits, a duplicate
/// ending, a response to another request, cancellation, and a late
/// response, which is conformant by design.
#[test]
fn labels_fixture_frames_responses() {
    let get = row(TEST_FAMILY, "get");
    let part = |len| header(TEST_FAMILY, "part", len);
    let done = header(TEST_FAMILY, "done", 4);
    let mut model = ExchangeModel::new(TEST_FAMILY, get.message_type).expect("get is a request");

    // Two parts and 10 bytes: the second part is the frame boundary.
    assert!(model.sent(
        0,
        1,
        Owed {
            frames: 2,
            bytes: 10
        }
    ));
    assert!(
        !model.sent(
            0,
            1,
            Owed {
                frames: 2,
                bytes: 10
            }
        ),
        "the key is open"
    );
    assert!(model.response_frame(0, 1, &part(3)).is_conformant());
    assert!(model.response_frame(0, 1, &part(3)).is_conformant());
    assert_eq!(
        model.response_frame(0, 1, &part(1)).first(),
        Label::OverOwed
    );
    // The ending fills the last 4 bytes exactly. A second one is unsolicited.
    assert!(model.response_frame(0, 1, &done).is_conformant());
    assert_eq!(
        model.response_frame(0, 1, &done).first(),
        Label::Unsolicited
    );

    // Bytes: a part leaves 3 bytes, and the 4-byte ending is one over.
    assert!(model.sent(
        0,
        2,
        Owed {
            frames: 5,
            bytes: 8
        }
    ));
    assert!(model.response_frame(0, 2, &part(5)).is_conformant());
    assert_eq!(model.response_frame(0, 2, &done).first(), Label::OverOwed);

    // Owed bytes always cover the largest ending.
    assert!(model.sent(
        0,
        3,
        Owed {
            frames: 0,
            bytes: 0
        }
    ));
    assert_eq!(
        model.owed(0, 3),
        Some(Owed {
            frames: 0,
            bytes: 4
        })
    );
    assert_eq!(
        model.response_frame(0, 3, &part(1)).first(),
        Label::OverOwed
    );
    // The node timed the exchange out locally; the late ending is conformant.
    assert!(model.response_frame(0, 3, &done).is_conformant());

    // `pong` answers `ping`, not `get`; an unknown key is unsolicited.
    let pong = header(TEST_FAMILY, "pong", 4);
    assert_eq!(
        model.response_frame(0, 2, &pong).first(),
        Label::Unsolicited
    );
    assert_eq!(
        model.response_frame(0, 9, &done).first(),
        Label::Unsolicited
    );

    // The session's end cancels its exchanges.
    model.end_session(0);
    assert_eq!(
        model.response_frame(0, 2, &done).first(),
        Label::Unsolicited
    );
}

/// A model of `FEED` on session 0.
fn feed_model() -> SubscriptionModel {
    SubscriptionModel::new(FEED, feed::WATCH).expect("watch is a subscription")
}

const ONE: Credit = Credit {
    objects: 1,
    bytes: 16,
};

/// The peer's updates to the node as publisher: `OverLive` past twice
/// `max_live`, opens inside the margin, `AfterClose`, `BadCursor`, and
/// crossed grants and closes.
#[test]
fn labels_fixture_frames_updates() {
    let mut model = feed_model();
    // `max_live` is 2: opens 3 and 4 are inside the margin.
    for key in 0..4 {
        assert!(model.open_frame(0, key, 0, ONE, 100 + key).is_conformant());
    }
    assert_eq!(model.open_frame(0, 4, 0, ONE, 104).first(), Label::OverLive);
    // Two faults: a reused key past the margin takes the first label.
    let labels = model.open_frame(0, 0, 0, ONE, 100);
    assert_eq!(labels.first(), Label::OverLive);
    assert!(labels.contains(Label::BadCursor), "{labels:?}");

    // The window is 4 objects and 64 bytes: a grant to the bound fits, and
    // one more object is over.
    let to_bound = Credit {
        objects: 3,
        bytes: 48,
    };
    assert!(model.grant_frame(0, 0, 1, 100, to_bound).is_conformant());
    assert_eq!(
        model.grant_frame(0, 0, 2, 100, ONE).first(),
        Label::BadCursor
    );
    // A send acknowledged later frees its room: the credit refill.
    assert!(model.send_page(0, 0, 1, 16, 1));
    assert!(model.grant_frame(0, 0, 2, 1, ONE).is_conformant());
    // Unknown acknowledgement, wrong sequence, and empty credit.
    assert_eq!(
        model.grant_frame(0, 0, 3, 77, ONE).first(),
        Label::BadCursor
    );
    assert_eq!(model.grant_frame(0, 0, 5, 1, ONE).first(), Label::BadCursor);
    let empty = Credit {
        objects: 0,
        bytes: 0,
    };
    assert_eq!(
        model.grant_frame(0, 0, 3, 1, empty).first(),
        Label::BadCursor
    );

    // After `Close`, any update is `AfterClose`.
    assert!(model.close_frame(0, 1, 1, 101).is_conformant());
    assert_eq!(
        model.grant_frame(0, 1, 2, 101, ONE).first(),
        Label::AfterClose
    );
    assert_eq!(model.close_frame(0, 1, 2, 101).first(), Label::AfterClose);

    // A grant that crossed the terminal outcome fits consumed plus credit.
    assert!(model.send_page(0, 2, 1, 16, 2));
    assert!(model.end(0, 2));
    assert!(model.grant_frame(0, 2, 1, 2, ONE).is_conformant());
    let full = Credit {
        objects: 4,
        bytes: 64,
    };
    assert_eq!(
        model.grant_frame(0, 2, 2, 2, full).first(),
        Label::BadCursor
    );
    // A crossed `Close` consumes the tombstone, so the next update is
    // unknown.
    assert!(model.close_frame(0, 2, 2, 2).is_conformant());
    assert_eq!(model.grant_frame(0, 2, 3, 2, ONE).first(), Label::BadCursor);

    // A crossed update after a `Close` received before the end.
    assert!(model.end(0, 1));
    assert_eq!(
        model.grant_frame(0, 1, 2, 101, ONE).first(),
        Label::AfterClose
    );
}

/// An `Open` proves the subscriber received the oldest terminal outcomes:
/// the publisher keeps `max_live - 1` live subscriptions and tombstones.
#[test]
fn labels_fixture_frames_tombstones() {
    let mut model = feed_model();
    for key in 0..2 {
        assert!(model.open_frame(0, key, 0, ONE, key).is_conformant());
        assert!(model.end(0, key));
    }
    assert_eq!(model.tombstones(0), 2);
    assert!(model.open_frame(0, 2, 0, ONE, 2).is_conformant());
    assert_eq!(model.tombstones(0), 1, "max_live - 1 - 0 live");
    // Key 0's tombstone went, so a crossed grant for it is unknown.
    assert_eq!(model.grant_frame(0, 0, 1, 0, ONE).first(), Label::BadCursor);
    assert!(model.grant_frame(0, 1, 1, 1, ONE).is_conformant());
}

/// The peer's pages to the node as subscriber: `OverCredit` one past the
/// unspent credit or with no object, `Unsolicited` after the end, a page
/// after the node's `Close`, and a refill after acceptance.
#[test]
fn labels_fixture_frames_pages() {
    let mut model = feed_model();
    let two = Credit {
        objects: 2,
        bytes: 20,
    };
    assert!(model.subscribe(0, 7, two, 0).is_some());
    assert!(model.subscribe(0, 7, two, 0).is_none(), "the key is live");
    assert_eq!(model.page_frame(0, 7, 0, 1, 1).first(), Label::OverCredit);
    assert_eq!(model.page_frame(0, 7, 1, 21, 1).first(), Label::OverCredit);
    assert!(model.page_frame(0, 7, 1, 10, 1).is_conformant());
    // The boundary page spends the last object and byte.
    assert!(model.page_frame(0, 7, 1, 10, 2).is_conformant());
    assert_eq!(model.page_frame(0, 7, 1, 1, 3).first(), Label::OverCredit);

    // Accepting pages lets the node grant again.
    let refill = Credit {
        objects: 3,
        bytes: 40,
    };
    assert!(
        model.grant(0, 7, refill).is_none(),
        "the window is still full"
    );
    assert!(model.accept(0, 7, 2));
    assert!(model.grant(0, 7, refill).is_some());
    assert!(model.page_frame(0, 7, 1, 30, 3).is_conformant());

    // Pages after the node's `Close` within the credit are conformant.
    assert!(model.close(0, 7).is_some());
    assert!(model.grant(0, 7, ONE).is_none(), "Close was sent");
    assert!(model.page_frame(0, 7, 2, 10, 4).is_conformant());
    assert!(model.ending_frame(0, 7).is_conformant());
    assert_eq!(model.ending_frame(0, 7).first(), Label::Unsolicited);
    assert_eq!(model.page_frame(0, 7, 1, 1, 5).first(), Label::Unsolicited);
    assert!(
        model.subscribe(0, 7, two, 0).is_none(),
        "the publisher may hold its tombstone"
    );
}

/// Cadence exhaustion is measured, never labelled: the bucket empties at
/// its capacity, refills one token per interval, and a local pause credits
/// tokens even above capacity.
#[test]
fn labels_fixture_frames_cadence() {
    let status = row(TEST_FAMILY, "status");
    let get = row(TEST_FAMILY, "get");
    let mut model = CadenceModel::new();
    let start = Duration::ZERO;
    assert_eq!(model.charge(0, 1, get, start), Charge::Exempt);
    for _ in 0..22 {
        assert_eq!(model.charge(0, 1, status, start), Charge::Admit);
    }
    assert_eq!(model.charge(0, 1, status, start), Charge::Exhausted);
    // Another connection or layout has its own bucket.
    assert_eq!(model.charge(1, 1, status, start), Charge::Admit);
    assert_eq!(model.charge(0, 2, status, start), Charge::Admit);

    // The refill interval is 15 seconds; 29 seconds earns one token.
    let later = Duration::from_secs(29);
    assert_eq!(model.charge(0, 1, status, later), Charge::Admit);
    assert_eq!(model.charge(0, 1, status, later), Charge::Exhausted);
    // The carried 14 seconds plus one more earns the next.
    let next = Duration::from_secs(30);
    assert_eq!(model.charge(0, 1, status, next), Charge::Admit);

    // A full bucket gains tokens from a pause, above its capacity.
    model.credit_pause(1, 2, TEST_FAMILY, Duration::from_secs(45), start);
    assert_eq!(model.tokens(1, 2, status.message_type), Some(25));
}
