//! P3a: each role model agrees with the production tool it models over
//! random frame sequences (properties.md, P3.2): the reader's header check,
//! `Serve`, `Reservations`, `Publications`, `Subscriptions`, and
//! `CadenceBuckets`.
//!
//! A frame agrees when the tool admits it exactly when the model labels it
//! `Conformant`, and when the tool's refusal names one of the model's
//! labels. The tool stops at its first fault, so it may name any of them.
//! After each step the tests also compare what both hold open. A
//! disagreement fails the test with both answers and proptest's seed.
#![cfg(zakura_sim)]

mod support;

use std::time::Duration;

use proptest::prelude::*;
use support::feed::{self, FEED};
use zakura_network::zakura::{
    testkit::regulation::{
        self, check_header, production_tables, Pages, Rates, Reader, Requests, Responses, Updates,
        TEST_FAMILY,
    },
    Credit, MessageRole, MessageRule,
};
use zakura_sim::model::{
    CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Label, Labels, Owed, ReaderEnd,
    SubscriptionModel, Update,
};

/// The row named `name` in `rules`.
fn row(rules: &'static [MessageRule], name: &str) -> &'static MessageRule {
    rules
        .iter()
        .find(|row| row.name == name)
        .unwrap_or_else(|| panic!("no row {name}"))
}

/// The model label a production refusal names.
fn label_of(refusal: &str) -> Label {
    match refusal {
        "unknown_message_type" => Label::UndeclaredType,
        "reserved_flags" => Label::ReservedFlags,
        "payload_too_short" => Label::ShortPayload,
        "over_committed" => Label::OverMargin,
        "unsolicited" | "wrong_request" => Label::Unsolicited,
        "over_frames" | "over_bytes" | "over_owed" => Label::OverOwed,
        "no_slot" => Label::OverLive,
        "over_credit" => Label::OverCredit,
        "after_close" => Label::AfterClose,
        "unknown"
        | "sequence"
        | "unknown_acknowledgement"
        | "empty_credit"
        | "above_window"
        | "crossed_above_window"
        | "reused_key" => Label::BadCursor,
        other => panic!("no label for the refusal {other}"),
    }
}

/// A subscriber's refusal: a page or terminal outcome for no live
/// subscription is unsolicited. An update for an unknown key stays
/// `unknown`.
fn page_refusal(refusal: &str) -> &str {
    match refusal {
        "unknown" => "unsolicited",
        other => other,
    }
}

/// Fails unless `production` and `model` agree on one frame.
fn agree(step: &str, production: Result<(), &str>, model: Labels) -> Result<(), TestCaseError> {
    match production {
        Ok(()) => prop_assert!(
            model.is_conformant(),
            "{step}: the tool admitted it, the model says {model:?}"
        ),
        Err(refusal) => prop_assert!(
            model.contains(label_of(refusal)) && !model.is_conformant(),
            "{step}: the tool refused it as {refusal}, the model says {model:?}"
        ),
    }
    Ok(())
}

/// Every tabled stream's rules and frame cap, the test family's included.
fn tables() -> Vec<(&'static [MessageRule], usize)> {
    let mut tables = vec![(TEST_FAMILY, 4096)];
    for table in production_tables() {
        let rules = table.stream.messages.expect("production tables are tabled");
        tables.push((
            rules,
            usize::try_from(table.stream.frame_cap).expect("u32 fits usize"),
        ));
    }
    tables
}

proptest! {
    /// The reader's header check and `FrameModel` agree on every header.
    #[test]
    fn frame_headers_agree(
        table in 0..4usize,
        reader in 0..3usize,
        message_type in 0..12u16,
        flags in prop_oneof![3 => Just(0u16), 1 => any::<u16>()],
        nudge in -2..=2i64,
        row_index in any::<prop::sample::Index>(),
        use_bound in any::<bool>(),
    ) {
        let (rules, frame_cap) = tables()[table];
        let (model_end, production_end) = [
            (ReaderEnd::Persistent, Reader::Persistent),
            (ReaderEnd::Responder, Reader::Responder),
            (ReaderEnd::Requester, Reader::Requester),
        ][reader];
        // Lengths near a row's bounds, where the labels change.
        let near = row_index.get(rules);
        let bound = if use_bound { near.payload.max() } else { near.payload.min() };
        let payload_len = usize::try_from(i64::try_from(bound).expect("fits").saturating_add(nudge).max(0))
            .expect("not negative");
        let message_type = if message_type < 8 { near.message_type } else { message_type };
        let header = FrameHeader { message_type, flags, payload_len };
        let model = FrameModel::new(rules, model_end).labels(&header);
        let production = check_header(rules, production_end, message_type, flags, frame_cap, payload_len)
            .and_then(|cap| if payload_len > cap { Err("oversize") } else { Ok(()) });
        match production {
            Err("oversize") => prop_assert!(
                model.contains(Label::LongPayload),
                "{header:?}: the reader refused it as oversize, the model says {model:?}"
            ),
            other => agree(&format!("{header:?}"), other, model)?,
        }
    }
}

/// One step of a session serving requests.
#[derive(Clone, Debug)]
enum Serving {
    Advertise(u32),
    Request,
    Answer,
}

fn serving() -> impl Strategy<Value = Serving> {
    prop_oneof![
        1 => (0..7u32).prop_map(Serving::Advertise),
        4 => Just(Serving::Request),
        2 => Just(Serving::Answer),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// `Serve` and `ExchangeModel` agree on each request and on the open
    /// count, across advertisements and answers.
    #[test]
    fn requests_agree(initial in 0..7u32, steps in prop::collection::vec(serving(), 1..40)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("a current-thread runtime builds");
        runtime.block_on(async {
            let get = row(TEST_FAMILY, "get");
            let mut requests = Requests::new(initial);
            let mut model = ExchangeModel::new(TEST_FAMILY, get.message_type).expect("get is a request");
            model.advertise(0, initial);
            for (index, step) in steps.into_iter().enumerate() {
                let at = format!("step {index} {step:?}");
                match step {
                    Serving::Advertise(limit) => {
                        requests.advertise(limit);
                        model.advertise(0, limit);
                    }
                    Serving::Request => {
                        let production = requests.admit().map_err(|_| "over_committed");
                        agree(&at, production, model.request_frame(0))?;
                    }
                    Serving::Answer => {
                        let open = requests.open() > 0;
                        requests.end_oldest().await;
                        prop_assert_eq!(model.answered(0), open, "{}", at);
                    }
                }
                prop_assert_eq!(requests.open(), model.serving(0), "{}: open requests", at);
            }
            Ok(())
        })?;
    }
}

/// One step of a requester's exchanges.
#[derive(Clone, Debug)]
enum Exchanging {
    Send {
        key: u64,
        frames: u32,
        bytes: u64,
    },
    TimeOut {
        key: u64,
    },
    Respond {
        key: u64,
        row: &'static str,
        len: usize,
    },
}

fn exchanging() -> impl Strategy<Value = Exchanging> {
    let key = 0..4u64;
    prop_oneof![
        2 => (key.clone(), 0..4u32, 0..80u64)
            .prop_map(|(key, frames, bytes)| Exchanging::Send { key, frames, bytes }),
        1 => key.clone().prop_map(|key| Exchanging::TimeOut { key }),
        6 => (key, prop::sample::select(vec!["part", "part", "done", "failed", "pong"]), 0..70usize)
            .prop_map(|(key, row, len)| Exchanging::Respond { key, row, len }),
    ]
}

proptest! {
    /// `Reservations` and `ExchangeModel` agree on each response and on the
    /// open exchanges. A timed-out exchange stays open in both.
    #[test]
    fn responses_agree(steps in prop::collection::vec(exchanging(), 1..60)) {
        let get = row(TEST_FAMILY, "get");
        let mut responses = Responses::new(TEST_FAMILY, 16);
        let mut model = ExchangeModel::new(TEST_FAMILY, get.message_type).expect("get is a request");
        for (index, step) in steps.into_iter().enumerate() {
            let at = format!("step {index} {step:?}");
            match step {
                Exchanging::Send { key, frames, bytes } => {
                    let production = responses.reserve(key, get.message_type, frames, bytes);
                    prop_assert_eq!(
                        production.is_ok(),
                        model.sent(0, key, Owed { frames, bytes }),
                        "{}: {:?}", at, production
                    );
                }
                Exchanging::TimeOut { key } => {
                    responses.abandon(key);
                }
                Exchanging::Respond { key, row: name, len } => {
                    let response = row(TEST_FAMILY, name);
                    let header = FrameHeader { message_type: response.message_type, flags: 0, payload_len: len };
                    let ends = name != "part";
                    let production = responses.claim(key, response.message_type, len, ends);
                    agree(&at, production, model.response_frame(0, key, &header))?;
                }
            }
            prop_assert_eq!(responses.len(), model.open().count(), "{}: open exchanges", at);
        }
    }
}

/// Credit of up to `objects` objects and `bytes` bytes, sometimes empty in
/// a unit.
fn credit(objects: u32, bytes: u32) -> impl Strategy<Value = Credit> {
    (0..=objects, 0..=bytes).prop_map(|(objects, bytes)| Credit { objects, bytes })
}

/// One step of the node publishing to a peer's subscriptions.
#[derive(Clone, Debug)]
enum Publishing {
    Open {
        key: u64,
        credit: Credit,
    },
    Grant {
        key: u64,
        ack: usize,
        credit: Credit,
    },
    Close {
        key: u64,
        ack: usize,
    },
    Page {
        key: u64,
        objects: u32,
        bytes: usize,
    },
    End {
        key: u64,
    },
    /// The terminal outcome's write finishes, freeing its slot.
    Written {
        key: u64,
    },
    /// Sends the next update with a sequence `skew` off.
    Skew(u32),
}

fn publishing() -> impl Strategy<Value = Publishing> {
    let key = 0..6u64;
    prop_oneof![
        2 => (key.clone(), credit(5, 80)).prop_map(|(key, credit)| Publishing::Open { key, credit }),
        3 => (key.clone(), 0..8usize, credit(3, 48))
            .prop_map(|(key, ack, credit)| Publishing::Grant { key, ack, credit }),
        1 => (key.clone(), 0..8usize).prop_map(|(key, ack)| Publishing::Close { key, ack }),
        4 => (key.clone(), 0..3u32, 1..33usize)
            .prop_map(|(key, objects, bytes)| Publishing::Page { key, objects, bytes }),
        2 => key.clone().prop_map(|key| Publishing::End { key }),
        2 => key.prop_map(|key| Publishing::Written { key }),
        1 => (1..3u32).prop_map(Publishing::Skew),
    ]
}

/// The generator's guesses at each key's next update and at cursors to
/// acknowledge. They steer the frames toward conformant ones; neither tool
/// reads them.
#[derive(Default)]
struct Guesses {
    next_sequence: std::collections::BTreeMap<u64, u32>,
    cursors: Vec<u64>,
    skew: u32,
}

impl Guesses {
    /// The sequence for `key`'s next update, and advances it.
    fn sequence(&mut self, key: u64) -> u32 {
        let next = self.next_sequence.entry(key).or_insert(1);
        let sequence = next.wrapping_add(std::mem::take(&mut self.skew));
        *next = next.saturating_add(1);
        sequence
    }

    /// A recent cursor, or 0.
    fn cursor(&self, pick: usize) -> u64 {
        self.cursors.iter().rev().nth(pick).copied().unwrap_or(0)
    }

    /// A fresh cursor, recorded for later acknowledgements.
    fn fresh(&mut self) -> u64 {
        let cursor = 1_000 + u64::try_from(self.cursors.len()).expect("fits");
        self.cursors.push(cursor);
        cursor
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// `Publications` and `SubscriptionModel` agree on each update, on each
    /// page the node may send, and on the tombstones held.
    #[test]
    fn updates_agree(steps in prop::collection::vec(publishing(), 1..80)) {
        let mut updates = Updates::new(FEED, feed::WATCH);
        let mut model = SubscriptionModel::new(FEED, feed::WATCH).expect("watch is a subscription");
        let mut guesses = Guesses::default();
        for (index, step) in steps.into_iter().enumerate() {
            let at = format!("step {index} {step:?}");
            match step {
                Publishing::Open { key, credit } => {
                    let sequence = std::mem::take(&mut guesses.skew);
                    guesses.next_sequence.insert(key, 1);
                    let start = guesses.fresh();
                    let production = updates.open(key, sequence, credit, start);
                    agree(&at, production, model.open_frame(0, key, sequence, credit, start))?;
                }
                Publishing::Grant { key, ack, credit } => {
                    let (sequence, ack) = (guesses.sequence(key), guesses.cursor(ack));
                    let production = updates.grant(key, sequence, ack, credit).map(|_| ());
                    agree(&at, production, model.grant_frame(0, key, sequence, ack, credit))?;
                }
                Publishing::Close { key, ack } => {
                    let (sequence, ack) = (guesses.sequence(key), guesses.cursor(ack));
                    let production = updates.close(key, sequence, ack).map(|_| ());
                    agree(&at, production, model.close_frame(0, key, sequence, ack))?;
                }
                Publishing::Page { key, objects, bytes } => {
                    let cursor = guesses.fresh();
                    let production = updates.send_page(key, objects, bytes, cursor);
                    prop_assert_eq!(
                        production.is_ok(),
                        model.send_page(0, key, objects, bytes, cursor),
                        "{}: {:?}", at, production
                    );
                }
                Publishing::End { key } => {
                    prop_assert_eq!(updates.end(key), model.end(0, key), "{}", at);
                }
                Publishing::Written { key } => {
                    prop_assert_eq!(
                        updates.terminal_written(key),
                        model.terminal_written(0, key),
                        "{}", at
                    );
                }
                Publishing::Skew(skew) => guesses.skew = skew,
            }
            prop_assert_eq!(updates.tombstones(), model.tombstones(0), "{}: tombstones", at);
        }
    }
}

/// Review (#1748): a terminal outcome still being written holds its slot,
/// so an `Open` that needs the slot is `OverLive` to both until the write
/// finishes.
#[test]
fn a_pending_terminal_write_holds_its_slot() {
    let MessageRole::Subscription { max_live, .. } = row(FEED, "watch").role else {
        panic!("watch is a subscription");
    };
    let slots = 2 * u64::from(max_live);
    let one = Credit {
        objects: 1,
        bytes: 1,
    };
    let mut updates = Updates::new(FEED, feed::WATCH);
    let mut model = SubscriptionModel::new(FEED, feed::WATCH).expect("watch is a subscription");
    for key in 0..slots {
        assert!(updates.open(key, 0, one, key).is_ok());
        assert!(model.open_frame(0, key, 0, one, key).is_conformant());
    }
    assert!(updates.end(0));
    assert!(model.end(0, 0));
    let refused = updates.open(slots, 0, one, slots);
    let labels = model.open_frame(0, slots, 0, one, slots);
    assert_eq!(labels.first(), Label::OverLive);
    agree("pending write", refused, labels).unwrap();

    assert!(updates.terminal_written(0));
    assert!(model.terminal_written(0, 0));
    assert!(updates.open(slots + 1, 0, one, slots + 1).is_ok());
    assert!(model
        .open_frame(0, slots + 1, 0, one, slots + 1)
        .is_conformant());
}

/// One step of the node subscribing to a peer.
#[derive(Clone, Debug)]
enum Subscribing {
    Subscribe { key: u64, credit: Credit },
    Grant { key: u64, credit: Credit },
    Close { key: u64 },
    Accept { key: u64, cursor: usize },
    Page { key: u64, objects: u32, len: usize },
    End { key: u64 },
}

fn subscribing() -> impl Strategy<Value = Subscribing> {
    let key = 0..5u64;
    prop_oneof![
        2 => (key.clone(), credit(5, 80)).prop_map(|(key, credit)| Subscribing::Subscribe { key, credit }),
        2 => (key.clone(), credit(3, 48)).prop_map(|(key, credit)| Subscribing::Grant { key, credit }),
        1 => key.clone().prop_map(|key| Subscribing::Close { key }),
        2 => (key.clone(), 0..6usize).prop_map(|(key, cursor)| Subscribing::Accept { key, cursor }),
        5 => (key.clone(), 0..3u32, 1..40usize)
            .prop_map(|(key, objects, len)| Subscribing::Page { key, objects, len }),
        1 => key.prop_map(|key| Subscribing::End { key }),
    ]
}

/// An update's wire fields: sequence, acknowledged cursor, added credit.
type Wire = (u32, u64, Credit);

/// The wire fields of a model update.
fn wire(update: Update) -> Wire {
    (update.sequence, update.acknowledged, update.added)
}

/// The wire fields of the update production recorded, if it recorded one.
fn written(production: Result<regulation::Update, &str>) -> Option<Wire> {
    production
        .ok()
        .map(|update| (update.sequence, update.acknowledged, update.added))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// `Subscriptions` and `SubscriptionModel` agree on each update the
    /// node may send, field by field, and on each page and terminal outcome it receives.
    #[test]
    fn pages_agree(steps in prop::collection::vec(subscribing(), 1..80)) {
        let mut pages = Pages::new(FEED, feed::WATCH);
        let mut model = SubscriptionModel::new(FEED, feed::WATCH).expect("watch is a subscription");
        let mut guesses = Guesses::default();
        for (index, step) in steps.into_iter().enumerate() {
            let at = format!("step {index} {step:?}");
            match step {
                Subscribing::Subscribe { key, credit } => {
                    let start = guesses.fresh();
                    let production = pages.open(key, credit, start);
                    let model = model.subscribe(0, key, credit, start);
                    prop_assert_eq!(written(production), model.map(wire), "{}", at);
                }
                Subscribing::Grant { key, credit } => {
                    let production = pages.grant(key, credit);
                    let model = model.grant(0, key, credit);
                    prop_assert_eq!(written(production), model.map(wire), "{}", at);
                }
                Subscribing::Close { key } => {
                    let production = pages.close(key);
                    let model = model.close(0, key);
                    prop_assert_eq!(written(production), model.map(wire), "{}", at);
                }
                Subscribing::Accept { key, cursor } => {
                    let cursor = guesses.cursor(cursor);
                    let production = pages.accept(key, cursor);
                    prop_assert_eq!(production.is_ok(), model.accept(0, key, cursor), "{}: {:?}", at, production);
                }
                Subscribing::Page { key, objects, len } => {
                    let cursor = guesses.fresh();
                    let production = pages.page(key, objects, len, cursor).map_err(page_refusal);
                    agree(&at, production, model.page_frame(0, key, objects, len, cursor))?;
                }
                Subscribing::End { key } => {
                    let production = pages.end(key).map_err(page_refusal);
                    agree(&at, production, model.ending_frame(0, key))?;
                }
            }
        }
    }
}

/// One step of a connection's cadence buckets.
#[derive(Clone, Debug)]
enum Pacing {
    Advance(Duration),
    /// Charges `times` frames of one row back to back.
    Charge {
        table: usize,
        row: prop::sample::Index,
        times: u32,
    },
    Pause {
        table: usize,
        by: Duration,
    },
}

fn pacing() -> impl Strategy<Value = Pacing> {
    let millis = |max: u64| (0..max).prop_map(Duration::from_millis);
    prop_oneof![
        3 => millis(20_000).prop_map(Pacing::Advance),
        6 => (0..4usize, any::<prop::sample::Index>(), 1..40u32)
            .prop_map(|(table, row, times)| Pacing::Charge { table, row, times }),
        1 => (0..4usize, millis(40_000)).prop_map(|(table, by)| Pacing::Pause { table, by }),
    ]
}

proptest! {
    /// `CadenceBuckets` and `CadenceModel` agree on every charge, across
    /// refills and credited pauses. Each table is its own layout.
    #[test]
    fn cadence_agrees(steps in prop::collection::vec(pacing(), 1..200)) {
        let tables = tables();
        let mut rates = Rates::new();
        let mut model = CadenceModel::new();
        let mut now = Duration::ZERO;
        for (index, step) in steps.into_iter().enumerate() {
            let at = format!("step {index} {step:?}");
            match step {
                Pacing::Advance(by) => {
                    rates.advance(by);
                    now += by;
                }
                Pacing::Charge { table, row, times } => {
                    let layout = u16::try_from(table).expect("fits");
                    let rule = row.get(tables[table].0);
                    for frame in 0..times {
                        let production = rates.charge(layout, rule);
                        let model = match model.charge(0, layout, rule, now) {
                            Charge::Exempt => "exempt",
                            Charge::Admit => "admit",
                            Charge::Exhausted => "exhausted",
                        };
                        prop_assert_eq!(production, model, "{}: {} frame {}", at, rule.name, frame);
                    }
                }
                Pacing::Pause { table, by } => {
                    let layout = u16::try_from(table).expect("fits");
                    rates.credit_pause(layout, tables[table].0, by);
                    model.credit_pause(0, layout, tables[table].0, by, now);
                }
            }
        }
    }
}
