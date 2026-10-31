//! `Adversary`: a scripted peer that breaks header sync's rules on purpose
//! (M5-E1), one `Plan`-picked [`Act`] at a time.
//!
//! An adversary holds a fixture chain and otherwise runs the honest script,
//! so it reaches the state each act needs the way an honest peer would: its
//! setup history. [`AdversarySpec`] can keep it from announcing, so the node
//! never subscribes, or from serving, so the node's subscription stays live
//! with its credit unspent. An act is offered only once its setup history
//! is done ([`Adversary::wake`]). When it runs, it asserts its precondition
//! from the models again, computes its boundary from the models' state at
//! that moment, and checks that the models give the frame exactly the label
//! set the act is for before sending it. A frame with a fault is labelled
//! on a copy of the models, so it changes no model state.

use std::{collections::BTreeSet, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_network::zakura::{
    v9::{
        Cursor, HeadersOutcome, HeadersOutcomeCode, Message, Operation, RegtestMessage, Status,
        SubscribeHeaders,
    },
    AuxSchema, Credit,
};
use zakura_rt::time::Instant;

use super::{
    honest::{Honest, SETTLE},
    Crafted, Outgoing, SESSION,
};
use crate::model::{Label, Labels};

/// How an adversary behaves between its acts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdversarySpec {
    /// Whether it sends `Status` on its header streams, so the node may
    /// subscribe to it.
    pub announce: bool,
    /// Whether it serves the node's subscriptions with pages. Without, each
    /// stays live with its credit unspent until an act uses it.
    pub serve: bool,
    /// Whether a frame whose header alone the node can refuse is written
    /// without its payload.
    #[serde(default)]
    pub withhold: bool,
}

/// A row of header sync's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Row {
    /// `status`.
    Status,
    /// `subscribe-headers`.
    SubscribeHeaders,
    /// `headers`.
    Headers,
    /// `headers-outcome`.
    HeadersOutcome,
}

impl Row {
    /// Every row, in table order.
    pub const ALL: [Row; 4] = [
        Row::Status,
        Row::SubscribeHeaders,
        Row::Headers,
        Row::HeadersOutcome,
    ];

    /// The row's name in the table.
    pub fn name(self) -> &'static str {
        match self {
            Row::Status => "status",
            Row::SubscribeHeaders => "subscribe-headers",
            Row::Headers => "headers",
            Row::HeadersOutcome => "headers-outcome",
        }
    }

    /// The row's rule.
    fn rule(self) -> &'static zakura_network::zakura::MessageRule {
        RegtestMessage::ROWS
            .iter()
            .find(|rule| rule.name == self.name())
            .expect("header sync has every row")
    }

    /// Whether the row answers a subscription the node opened.
    fn answers(self) -> bool {
        matches!(self, Row::Headers | Row::HeadersOutcome)
    }
}

/// A field of a subscription cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CursorField {
    /// The last header's height.
    Height,
    /// The last header's hash.
    Hash,
    /// The headers consumed.
    Headers,
    /// The bytes consumed.
    Bytes,
}

impl CursorField {
    /// Every field.
    pub const ALL: [CursorField; 4] = [
        CursorField::Height,
        CursorField::Hash,
        CursorField::Headers,
        CursorField::Bytes,
    ];
}

/// A content rule the content labeller checks ([`super::content`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContentRule {
    /// A `Status` whose work anchor is above its selected tip.
    StatusAnchor,
    /// A `Status` whose retention floor is above its selected tip.
    StatusRetention,
    /// A `Status` whose header cap is 0.
    StatusHeaderCap,
    /// A `Status` whose `max_subscriptions` isn't 1.
    StatusSubscriptions,
    /// A `Status` with an unknown schema bit.
    StatusSchema,
    /// A `Status` whose message cap is below one page.
    StatusCap,
    /// An `Open` that repeats a locator.
    OpenLocators,
    /// An `Open` whose start cursor isn't its first locator.
    OpenCursor,
    /// A page whose first header doesn't link to its parent: a fixture
    /// header with a byte of its parent hash flipped.
    Linkage,
    /// `SubscriptionClosed` before the node sent `Close`.
    OutcomeWindow,
}

impl ContentRule {
    /// Every rule, by row.
    pub const ALL: [ContentRule; 10] = [
        ContentRule::StatusAnchor,
        ContentRule::StatusRetention,
        ContentRule::StatusHeaderCap,
        ContentRule::StatusSubscriptions,
        ContentRule::StatusSchema,
        ContentRule::StatusCap,
        ContentRule::OpenLocators,
        ContentRule::OpenCursor,
        ContentRule::Linkage,
        ContentRule::OutcomeWindow,
    ];

    /// The row the rule's frame is on.
    pub fn row(self) -> Row {
        match self {
            ContentRule::StatusAnchor
            | ContentRule::StatusRetention
            | ContentRule::StatusHeaderCap
            | ContentRule::StatusSubscriptions
            | ContentRule::StatusSchema
            | ContentRule::StatusCap => Row::Status,
            ContentRule::OpenLocators | ContentRule::OpenCursor => Row::SubscribeHeaders,
            ContentRule::Linkage => Row::Headers,
            ContentRule::OutcomeWindow => Row::HeadersOutcome,
        }
    }
}

/// One adversarial action, the `peer:<name>:<act>` a `Plan` picks. The
/// receiver-direction table of M5-E1, with its setup history in
/// [`Adversary::wake`] and its boundary in [`Adversary::act`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "act", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Act {
    /// A frame of `row` one byte under its minimum.
    ShortPayload {
        /// The row.
        row: Row,
    },
    /// A frame of `row` one byte over its maximum. For `headers`, that's
    /// over the credit too.
    LongPayload {
        /// The row.
        row: Row,
    },
    /// A `Status` sent as a type the table doesn't declare.
    UndeclaredType {
        /// The type: 0 or 5.
        message_type: u16,
    },
    /// A frame of `row` with flag bit `bit` set.
    ReservedFlags {
        /// The row.
        row: Row,
        /// The flag bit, 0 to 15.
        bit: u8,
    },
    /// The smallest frame of `row` the state allows: conformant.
    MinPayload {
        /// The row.
        row: Row,
    },
    /// The largest frame of `row` the state allows: conformant.
    MaxPayload {
        /// The row.
        row: Row,
    },
    /// A frame of `row`, a page or an ending, with no live subscription.
    Unsolicited {
        /// The row.
        row: Row,
    },
    /// A page whose declared length is one byte past the byte credit.
    OverCreditHeader,
    /// A page of one header past the object credit, within byte credit.
    OverCreditObjects,
    /// A second `Open` beside one live subscription: conformant, within the
    /// receiver's margin.
    SecondOpen,
    /// A third `Open` beside two held subscriptions.
    OverLive,
    /// A second ending for a subscription this peer ended, once the node
    /// has claimed the first.
    ExtraEnding,
    /// An ending and its duplicate written back to back, so the node's
    /// reader may read the duplicate's header before the first is claimed.
    ExtraEndingAhead,
    /// A `Grant` after this peer's `Close`.
    AfterClose,
    /// An `Open` toward a target no node retains, which the node ends at
    /// once: conformant, the setup of [`Act::OutcomeCrossing`].
    UnretainedOpen,
    /// A `Grant` for the [`Act::UnretainedOpen`], sent [`CROSSING_DELAY`]
    /// after it, once the node has ended the subscription and before the
    /// ending arrives: conformant.
    OutcomeCrossing,
    /// A `Grant` acknowledging the last page's cursor with one `field`
    /// changed, so it names no cursor of the subscription's history.
    BadCursor {
        /// The field changed.
        field: CursorField,
    },
    /// Up to [`BURST`] `Status` frames, written at once, stopping after the
    /// first that finds the cadence bucket empty: conformant, that one
    /// `exhausted`. A script of enough of them empties the bucket.
    StatusBurst,
    /// A frame that breaks `rule`.
    BadContent {
        /// The rule.
        rule: ContentRule,
    },
}

impl Act {
    /// The act's name in its identity.
    pub fn name(self) -> &'static str {
        match self {
            Act::ShortPayload { .. } => "short-payload",
            Act::LongPayload { .. } => "long-payload",
            Act::UndeclaredType { .. } => "undeclared-type",
            Act::ReservedFlags { .. } => "reserved-flags",
            Act::MinPayload { .. } => "min-payload",
            Act::MaxPayload { .. } => "max-payload",
            Act::Unsolicited { .. } => "unsolicited",
            Act::OverCreditHeader => "over-credit-header",
            Act::OverCreditObjects => "over-credit-objects",
            Act::SecondOpen => "second-open",
            Act::OverLive => "over-live",
            Act::ExtraEnding => "extra-ending",
            Act::ExtraEndingAhead => "extra-ending-ahead",
            Act::AfterClose => "after-close",
            Act::UnretainedOpen => "unretained-open",
            Act::OutcomeCrossing => "outcome-crossing",
            Act::BadCursor { .. } => "bad-cursor",
            Act::StatusBurst => "status-burst",
            Act::BadContent { .. } => "bad-content",
        }
    }

    /// The label of the act's frame, the first of its set in table order.
    pub fn label(self) -> Label {
        self.labels().first()
    }

    /// Every label the act's frame gets.
    pub fn labels(self) -> Labels {
        let of = Labels::of;
        match self {
            Act::ShortPayload { .. } => of(Label::ShortPayload),
            Act::LongPayload { row: Row::Headers } => {
                of(Label::LongPayload).with(Label::OverCredit)
            }
            Act::LongPayload { .. } => of(Label::LongPayload),
            Act::UndeclaredType { .. } => of(Label::UndeclaredType),
            Act::ReservedFlags { .. } => of(Label::ReservedFlags),
            Act::MinPayload { .. }
            | Act::MaxPayload { .. }
            | Act::SecondOpen
            | Act::UnretainedOpen
            | Act::OutcomeCrossing
            | Act::StatusBurst => Labels::NONE,
            Act::Unsolicited { .. } | Act::ExtraEnding | Act::ExtraEndingAhead => {
                of(Label::Unsolicited)
            }
            Act::OverCreditHeader | Act::OverCreditObjects => of(Label::OverCredit),
            Act::OverLive => of(Label::OverLive),
            Act::AfterClose => of(Label::AfterClose),
            Act::BadCursor { .. } => of(Label::BadCursor),
            Act::BadContent { .. } => of(Label::BadContent),
        }
    }

    /// The row of the act's frame.
    pub fn row(self) -> Row {
        match self {
            Act::ShortPayload { row }
            | Act::LongPayload { row }
            | Act::ReservedFlags { row, .. }
            | Act::MinPayload { row }
            | Act::MaxPayload { row }
            | Act::Unsolicited { row } => row,
            Act::UndeclaredType { .. } | Act::StatusBurst => Row::Status,
            Act::OverCreditHeader | Act::OverCreditObjects => Row::Headers,
            Act::SecondOpen
            | Act::OverLive
            | Act::AfterClose
            | Act::UnretainedOpen
            | Act::OutcomeCrossing
            | Act::BadCursor { .. } => Row::SubscribeHeaders,
            Act::ExtraEnding | Act::ExtraEndingAhead => Row::HeadersOutcome,
            Act::BadContent { rule } => rule.row(),
        }
    }

    /// Whether the act needs a subscription the node opened, held live with
    /// credit.
    fn needs_node_subscription(self) -> bool {
        match self {
            Act::ShortPayload { row }
            | Act::LongPayload { row }
            | Act::ReservedFlags { row, .. }
            | Act::MinPayload { row }
            | Act::MaxPayload { row } => row.answers(),
            Act::OverCreditHeader | Act::OverCreditObjects | Act::ExtraEndingAhead => true,
            Act::BadContent { rule } => rule.row().answers(),
            _ => false,
        }
    }
}

/// A credit of one plain one-header page: one object and 263 bytes.
const ONE_PAGE: Credit = Credit {
    objects: 1,
    // 263 fits u32.
    bytes: (super::PAGE_FIXED_BYTES + super::ENTRY_BYTES) as u32,
};

/// The most `Status` frames one [`Act::StatusBurst`] writes. The node
/// takes about 40 steps to read each, so a burst stays well inside the
/// livelock limit at one instant.
pub const BURST: usize = 40;

/// The most locators an `Open` carries.
const MAX_LOCATORS: u32 = 13;

/// The target an `Open` names that no node retains.
const UNRETAINED: block::Hash = block::Hash([0xa5; 32]);

/// Byte offsets in a `Status` payload: the work anchor's height, the
/// retention floor, the header cap, `max_subscriptions`, the message cap
/// and the schema mask.
const STATUS_ANCHOR: usize = 0;
const STATUS_RETENTION: usize = 104;
const STATUS_HEADER_CAP: usize = 108;
const STATUS_SUBSCRIPTIONS: usize = 112;
const STATUS_CAP: usize = 114;
const STATUS_SCHEMA: usize = 118;

/// Byte offsets in a `SubscribeHeaders` payload: the cursor's hash, and the
/// first locator after the count byte.
const UPDATE_CURSOR_HASH: usize = 49;
const UPDATE_LOCATORS: usize = 98;

/// An adversary's state beyond the honest script's.
#[derive(Debug)]
pub(crate) struct Adversary {
    spec: AdversarySpec,
    /// The next key for an `Open` beyond the honest script's.
    next_key: u64,
    /// Keys of the `Open`s it sent beyond the honest script's.
    opened: Vec<u64>,
    /// Those the node has sent a page for, so it applied them.
    paged: BTreeSet<u64>,
    /// The unretained `Open` whose ending a `Grant` will cross.
    crossing: Option<Crossing>,
}

/// An [`Act::UnretainedOpen`] that an [`Act::OutcomeCrossing`] follows.
#[derive(Clone, Copy, Debug)]
struct Crossing {
    key: u64,
    /// Its start cursor, which the `Grant` acknowledges.
    start: Cursor,
    /// When it was sent.
    sent: Instant,
    /// Whether its ending has reached this peer, which is too late for a
    /// crossing.
    ended: bool,
}

/// How long after its unretained `Open` an adversary sends the crossing
/// `Grant`: after the node ends the subscription, which it does on reading
/// the `Open`, within a link's 100 ms, and before the ending can arrive on
/// a link whose delays start at [`CROSSING_LINK_MS`].
pub const CROSSING_DELAY: Duration = Duration::from_millis(200);

/// The minimum delay from the node to an adversary that crosses an ending.
pub const CROSSING_LINK_MS: u64 = 500;

impl Adversary {
    /// An adversary as `spec` says.
    pub(crate) fn new(spec: AdversarySpec) -> Self {
        Adversary {
            spec,
            next_key: 2,
            opened: Vec::new(),
            paged: BTreeSet::new(),
            crossing: None,
        }
    }

    /// Whether it sends `Status`.
    pub(crate) fn announces(&self) -> bool {
        self.spec.announce
    }

    /// Whether it serves the node's subscriptions with pages.
    pub(crate) fn serves(&self) -> bool {
        self.spec.serve
    }

    /// When `act`'s setup history is done, so the `Plan` may pick it:
    /// `None` until the models are in the state it needs.
    pub(crate) fn wake(&self, act: Act, honest: &Honest) -> Option<Instant> {
        let past = honest.active;
        let models = &honest.models.subscriptions;
        let node_subscription = node_subscription(honest);
        let own = honest
            .subscribed
            .as_ref()
            .filter(|sub| !sub.ended && !sub.close_sent);
        let ready = match act {
            _ if act.needs_node_subscription() => node_subscription
                .and_then(|id| models.unspent(SESSION, id))
                .is_some_and(|unspent| unspent.objects >= 1),
            Act::Unsolicited { .. } => node_subscription.is_none() && honest.serving.is_empty(),
            // Each `Open` waits until the node paged the ones before it, so
            // the node applied them.
            Act::SecondOpen => {
                own.is_some_and(|sub| sub.paged_at.is_some()) && models.held(SESSION) == 1
            }
            Act::OverLive => {
                let applied = self.opened.iter().all(|key| self.paged.contains(key));
                own.is_some() && applied && models.held(SESSION) == 2
            }
            Act::ExtraEnding => {
                let ended = honest.last_ending.is_some()
                    && honest.serving.is_empty()
                    && node_subscription.is_none();
                return ended.then_some(past + SETTLE);
            }
            Act::AfterClose => honest.subscribed.as_ref().is_some_and(|sub| sub.close_sent),
            Act::BadCursor { .. }
            | Act::MinPayload {
                row: Row::SubscribeHeaders,
            } => own.is_some_and(|sub| sub.paged_at.is_some()),
            Act::OutcomeCrossing => {
                return self
                    .crossing
                    .filter(|crossing| !crossing.ended)
                    .map(|crossing| crossing.sent + CROSSING_DELAY);
            }
            Act::UnretainedOpen
            | Act::MaxPayload {
                row: Row::SubscribeHeaders,
            } => honest.node_status.is_some() && honest.subscribed.is_none(),
            _ => true,
        };
        ready.then_some(past)
    }

    /// Carries out `act`: asserts its precondition, builds its frames from
    /// the models' state now, and labels each, failing unless the models
    /// give the act's frame exactly its label set.
    pub(crate) fn act(&mut self, act: Act, honest: &mut Honest) -> Result<Vec<Outgoing>, String> {
        if self.wake(act, honest).is_none() {
            return Err(format!("{}'s precondition doesn't hold", act.name()));
        }
        let mut out = Vec::new();
        let frame = match act {
            Act::ShortPayload { row } => {
                let mut frame = self.row_frame(row, honest)?;
                let min = row.rule().payload.min();
                frame.payload.truncate(min - 1);
                frame.header.payload_len = min - 1;
                frame
            }
            Act::LongPayload { row } => {
                let mut frame = self.row_frame(row, honest)?;
                let max = row.rule().payload.max();
                frame.payload.resize(max + 1, 0);
                frame.header.payload_len = max + 1;
                frame
            }
            Act::UndeclaredType { message_type } => {
                let mut frame = self.row_frame(Row::Status, honest)?;
                frame.header.message_type = message_type;
                frame
            }
            Act::ReservedFlags { row, bit } => {
                let mut frame = self.row_frame(row, honest)?;
                frame.header.flags = 1 << bit;
                frame
            }
            Act::MinPayload { row } | Act::MaxPayload { row } => {
                let largest = matches!(act, Act::MaxPayload { .. });
                self.sized(row, largest, honest)?
            }
            Act::Unsolicited { row } => self.unsolicited(row, honest)?,
            Act::OverCreditHeader => {
                let (id, unspent) = live(honest)?;
                let len = usize::try_from(unspent.bytes + 1).map_err(|_| "credit fits usize")?;
                let rule = Row::Headers.rule();
                if !(rule.payload.min()..=rule.payload.max()).contains(&len) {
                    return Err(format!("over-credit-header: {len} bytes leave the row"));
                }
                let mut frame = Crafted::of(honest.page_at(id, 1).ok_or(SHORT_CHAIN)?);
                frame.payload.resize(len, 0);
                frame.header.payload_len = len;
                frame
            }
            Act::OverCreditObjects => self.over_objects(honest)?,
            Act::SecondOpen | Act::OverLive => {
                let start = honest
                    .spec
                    .subscribe
                    .as_ref()
                    .map_or(0, |subscribe| subscribe.start);
                let key = self.key();
                self.opened.push(key);
                Crafted::of(open(honest, key, &[start], ONE_PAGE)?)
            }
            Act::ExtraEnding => {
                let ending = honest.last_ending.ok_or("no ending to repeat")?;
                Crafted::of(RegtestMessage::HeadersOutcome(ending))
            }
            Act::ExtraEndingAhead => {
                let (id, _) = live(honest)?;
                let ending = ending(honest, id, HeadersOutcomeCode::NoLocatorIntersection);
                let first = Crafted::of(ending.clone());
                check(act, &honest.models.preview(&first), Labels::NONE)?;
                out.push(honest.send(ending.clone()));
                honest.serving.remove(&id);
                Crafted::of(ending)
            }
            Act::AfterClose => Crafted::of(honest.update(Some(ONE_PAGE))),
            Act::UnretainedOpen => {
                let key = self.key();
                let tip = honest.chain.status.selected_tip_height.0;
                let mut message = open(honest, key, &[tip], ONE_PAGE)?;
                if let Message::SubscribeHeaders(update) = &mut message {
                    update.initial_target_tip_hash = UNRETAINED;
                    self.crossing = Some(Crossing {
                        key,
                        start: update.acknowledged,
                        sent: Instant::now(),
                        ended: false,
                    });
                }
                Crafted::of(message)
            }
            Act::OutcomeCrossing => {
                let crossing = self.crossing.ok_or("no Open to cross")?;
                Crafted::of(RegtestMessage::SubscribeHeaders(SubscribeHeaders {
                    operation: Operation::Grant,
                    subscription_id: crossing.key,
                    update_sequence: 1,
                    initial_target_tip_hash: UNRETAINED,
                    acknowledged: crossing.start,
                    locator_hashes: Vec::new(),
                    added_credit: ONE_PAGE,
                    tree_aux_schema: AuxSchema::None,
                }))
            }
            Act::BadCursor { field } => {
                let mut message = honest.update(Some(ONE_PAGE));
                if let Message::SubscribeHeaders(update) = &mut message {
                    let cursor = &mut update.acknowledged;
                    match field {
                        CursorField::Height => cursor.height.0 += 1,
                        CursorField::Hash => cursor.hash.0[0] ^= 1,
                        CursorField::Headers => cursor.headers += 1,
                        CursorField::Bytes => cursor.bytes += 1,
                    }
                }
                Crafted::of(message)
            }
            Act::StatusBurst => {
                let status = RegtestMessage::Status(honest.chain.status.clone());
                let mut burst = Vec::new();
                for _ in 0..BURST {
                    let frame = Crafted::of(status.clone());
                    check(act, &honest.models.preview(&frame), Labels::NONE)?;
                    let sent = honest.send_crafted(frame);
                    let exhausted = sent.exhausted();
                    burst.push(sent);
                    if exhausted {
                        break;
                    }
                }
                return Ok(vec![Outgoing::burst(burst)]);
            }
            Act::BadContent { rule } => self.bad_content(rule, honest)?,
        };
        check(act, &honest.models.preview(&frame), act.labels())?;
        let withhold = self.spec.withhold
            && act.labels().iter().next().is_some()
            && (frame_faults(&frame) || header_decided(act));
        let mut sent = honest.send_crafted(frame);
        if withhold {
            sent.withhold();
        }
        out.push(sent);
        Ok(out)
    }

    /// Follows a node frame before the honest script does: a page for one
    /// of its extra `Open`s shows the node applied it, and an ending for
    /// the crossing's `Open` shows the crossing came too late.
    pub(crate) fn before_receive(&mut self, message: &RegtestMessage) {
        match message {
            Message::Headers(page) if self.opened.contains(&page.subscription_id) => {
                self.paged.insert(page.subscription_id);
            }
            Message::HeadersOutcome(outcome) => {
                if let Some(crossing) = self
                    .crossing
                    .as_mut()
                    .filter(|crossing| crossing.key == outcome.subscription_id)
                {
                    crossing.ended = true;
                }
            }
            _ => {}
        }
    }

    /// A fresh key for an `Open`.
    fn key(&mut self) -> u64 {
        let key = self.next_key;
        self.next_key += 1;
        key
    }

    /// A conformant frame of `row` in the act's state, before the act
    /// changes its header: the stream's `Status`, a fresh `Open`, or a
    /// one-header page or a legal ending for the node's subscription.
    fn row_frame(&mut self, row: Row, honest: &mut Honest) -> Result<Crafted, String> {
        Ok(Crafted::of(match row {
            Row::Status => RegtestMessage::Status(honest.chain.status.clone()),
            Row::SubscribeHeaders => {
                let key = self.key();
                let tip = honest.chain.status.selected_tip_height.0;
                open(honest, key, &[tip], ONE_PAGE)?
            }
            Row::Headers => {
                let (id, _) = live(honest)?;
                honest.page_at(id, 1).ok_or(SHORT_CHAIN)?
            }
            Row::HeadersOutcome => {
                let (id, _) = live(honest)?;
                ending(honest, id, HeadersOutcomeCode::NoLocatorIntersection)
            }
        }))
    }

    /// The smallest or largest conformant frame of `row` the state allows.
    fn sized(&mut self, row: Row, largest: bool, honest: &mut Honest) -> Result<Crafted, String> {
        Ok(match row {
            Row::Status => Crafted::of(RegtestMessage::Status(honest.chain.status.clone())),
            Row::SubscribeHeaders if largest => {
                let tip = honest.chain.status.selected_tip_height.0;
                if tip < MAX_LOCATORS {
                    return Err(format!(
                        "max-payload: a chain of {tip} has too few locators"
                    ));
                }
                let heights: Vec<u32> = (0..MAX_LOCATORS).map(|back| tip - back).collect();
                let key = self.key();
                Crafted::of(open(honest, key, &heights, ONE_PAGE)?)
            }
            Row::SubscribeHeaders => Crafted::of(honest.update(Some(ONE_PAGE))),
            Row::Headers => {
                let (id, _) = live(honest)?;
                let count = if largest {
                    honest.page_room(id).unwrap_or(0)
                } else {
                    1
                };
                if count == 0 {
                    return Err("the state allows no page".to_owned());
                }
                Crafted::of(honest.page_at(id, count).ok_or(SHORT_CHAIN)?)
            }
            Row::HeadersOutcome => {
                let (id, _) = live(honest)?;
                let ending = ending(honest, id, HeadersOutcomeCode::NoLocatorIntersection);
                honest.serving.remove(&id);
                Crafted::of(ending)
            }
        })
    }

    /// A page or an ending for subscription 1, which the node never opened.
    fn unsolicited(&mut self, row: Row, honest: &mut Honest) -> Result<Crafted, String> {
        let tip = honest.chain.status.selected_tip_height.0;
        let message = match row {
            Row::Headers => {
                let parent = honest.chain.at(tip.saturating_sub(1));
                RegtestMessage::Headers(zakura_network::zakura::v9::Headers {
                    subscription_id: 1,
                    initial_target_tip_hash: honest.chain.status.selected_tip_hash,
                    common_ancestor_height: parent.height,
                    common_ancestor_hash: parent.hash,
                    reaches_initial_target: true,
                    tree_aux_schema: AuxSchema::None,
                    entries: vec![zakura_network::zakura::HeaderEntry {
                        header: honest
                            .chain
                            .headers
                            .last()
                            .ok_or("the peer holds no header")?
                            .clone(),
                        body_size: 0,
                        tree_aux: None,
                    }],
                })
            }
            Row::HeadersOutcome => RegtestMessage::HeadersOutcome(HeadersOutcome {
                subscription_id: 1,
                initial_target_tip_hash: honest.chain.status.selected_tip_hash,
                outcome: HeadersOutcomeCode::NoLocatorIntersection,
            }),
            Row::Status | Row::SubscribeHeaders => {
                return Err(format!("{} isn't a response row", row.name()))
            }
        };
        Ok(Crafted::of(message))
    }

    /// A page of one header past the object credit, with the target
    /// distance, caps, byte credit and cursor all leaving room for it.
    fn over_objects(&mut self, honest: &mut Honest) -> Result<Crafted, String> {
        let (id, unspent) = live(honest)?;
        let count = u32::try_from(unspent.objects + 1).map_err(|_| "credit fits u32")?;
        let caps = &honest.chain.status;
        let left = honest.left(id).unwrap_or(0);
        // A page of at most 4,001 headers is a few hundred kilobytes.
        let bytes = (super::PAGE_FIXED_BYTES + super::ENTRY_BYTES * count as usize) as u64;
        if count > caps.max_headers_per_response
            || bytes > u64::from(caps.max_message_bytes)
            || bytes > unspent.bytes
            || count >= left
        {
            return Err(format!(
                "over-credit-objects: {count} headers ({bytes} bytes) don't fit the caps, the \
                 {} unspent bytes or the {left} headers left",
                unspent.bytes
            ));
        }
        Ok(Crafted::of(honest.page_at(id, count).ok_or(SHORT_CHAIN)?))
    }

    /// A frame that breaks `rule`. Encoder-refused fields are written by
    /// encoding a valid message and patching its bytes, and the frame
    /// carries the broken message for its labels.
    fn bad_content(&mut self, rule: ContentRule, honest: &mut Honest) -> Result<Crafted, String> {
        let valid = honest.chain.status.clone();
        let status = |broken: Status, at: usize, was: &[u8], now: &[u8]| {
            patched(
                RegtestMessage::Status(valid.clone()),
                RegtestMessage::Status(broken),
                at,
                was,
                now,
            )
        };
        let above = valid.selected_tip_height.0 + 1;
        match rule {
            ContentRule::StatusAnchor => status(
                Status {
                    work_anchor_height: block::Height(above),
                    ..valid.clone()
                },
                STATUS_ANCHOR,
                &valid.work_anchor_height.0.to_le_bytes(),
                &above.to_le_bytes(),
            ),
            ContentRule::StatusRetention => status(
                Status {
                    oldest_retained_height: block::Height(above),
                    ..valid.clone()
                },
                STATUS_RETENTION,
                &valid.oldest_retained_height.0.to_le_bytes(),
                &above.to_le_bytes(),
            ),
            ContentRule::StatusHeaderCap => status(
                Status {
                    max_headers_per_response: 0,
                    ..valid.clone()
                },
                STATUS_HEADER_CAP,
                &valid.max_headers_per_response.to_le_bytes(),
                &0u32.to_le_bytes(),
            ),
            ContentRule::StatusSubscriptions => status(
                Status {
                    max_subscriptions: 2,
                    ..valid.clone()
                },
                STATUS_SUBSCRIPTIONS,
                &valid.max_subscriptions.to_le_bytes(),
                &2u16.to_le_bytes(),
            ),
            ContentRule::StatusSchema => status(
                Status {
                    tree_aux_schema_mask: 2,
                    ..valid.clone()
                },
                STATUS_SCHEMA,
                &valid.tree_aux_schema_mask.to_le_bytes(),
                &2u32.to_le_bytes(),
            ),
            ContentRule::StatusCap => {
                let cap = u32::try_from(Row::Headers.rule().payload.min() - 1)
                    .map_err(|_| "a page's size fits u32")?;
                status(
                    Status {
                        max_message_bytes: cap,
                        ..valid.clone()
                    },
                    STATUS_CAP,
                    &valid.max_message_bytes.to_le_bytes(),
                    &cap.to_le_bytes(),
                )
            }
            ContentRule::OpenLocators | ContentRule::OpenCursor => {
                let tip = valid.selected_tip_height.0;
                let key = self.key();
                let open = open(honest, key, &[tip, tip - 1], ONE_PAGE)?;
                let Message::SubscribeHeaders(update) = &open else {
                    unreachable!("open builds an update");
                };
                let mut broken = update.clone();
                let (at, was, now) = if rule == ContentRule::OpenLocators {
                    broken.locator_hashes[1] = broken.locator_hashes[0];
                    (
                        UPDATE_LOCATORS + 32,
                        update.locator_hashes[1],
                        update.locator_hashes[0],
                    )
                } else {
                    broken.acknowledged.hash = broken.locator_hashes[1];
                    (
                        UPDATE_CURSOR_HASH,
                        update.acknowledged.hash,
                        update.locator_hashes[1],
                    )
                };
                let broken = RegtestMessage::SubscribeHeaders(broken);
                patched(open.clone(), broken, at, &was.0, &now.0)
            }
            ContentRule::Linkage => {
                let (id, _) = live(honest)?;
                let mut message = honest.page_at(id, 2).ok_or(SHORT_CHAIN)?;
                if let Message::Headers(page) = &mut message {
                    let mut header = *page.entries[0].header;
                    header.previous_block_hash.0[0] ^= 1;
                    page.entries[0].header = header.into();
                }
                Ok(Crafted::of(message))
            }
            ContentRule::OutcomeWindow => {
                let (id, _) = live(honest)?;
                let closed = ending(honest, id, HeadersOutcomeCode::SubscriptionClosed);
                Ok(Crafted::of(closed))
            }
        }
    }
}

/// Why a page can't be built: the peer's chain ends first.
const SHORT_CHAIN: &str = "the peer's chain is too short for the page";

/// `valid` encoded, with the bytes at `at` changed from `was` to `now`, and
/// labelled as `broken`. Fails if the bytes there aren't `was`, so a moved
/// field can't patch the wrong one.
fn patched(
    valid: RegtestMessage,
    broken: RegtestMessage,
    at: usize,
    was: &[u8],
    now: &[u8],
) -> Result<Crafted, String> {
    let mut frame = Crafted::of(valid);
    let field = frame
        .payload
        .get_mut(at..at + was.len())
        .filter(|field| *field == was)
        .ok_or_else(|| format!("the payload's field at {at} moved"))?;
    field.copy_from_slice(now);
    frame.message = broken;
    Ok(frame)
}

/// The node's live subscription this peer serves, with its unspent credit.
fn live(honest: &Honest) -> Result<(u64, crate::model::Totals), String> {
    node_subscription(honest)
        .and_then(|id| Some((id, honest.models.subscriptions.unspent(SESSION, id)?)))
        .ok_or_else(|| "the node has no live subscription on this peer".to_owned())
}

/// The live subscription the node opened that this peer serves, if any.
fn node_subscription(honest: &Honest) -> Option<u64> {
    honest
        .serving
        .keys()
        .copied()
        .find(|id| honest.models.subscriptions.unspent(SESSION, *id).is_some())
}

/// An `Open` of `key` toward the node's selected tip, with locators at
/// `heights` on the peer's chain, highest first, and `credit`.
fn open(
    honest: &Honest,
    key: u64,
    heights: &[u32],
    credit: Credit,
) -> Result<RegtestMessage, String> {
    let target = honest
        .node_status
        .as_ref()
        .map_or(block::Hash([0; 32]), |status| status.selected_tip_hash);
    let locators: Vec<_> = heights
        .iter()
        .map(|height| honest.chain.at(*height))
        .collect();
    let unique: BTreeSet<_> = locators.iter().map(|locator| locator.hash.0).collect();
    if unique.len() != locators.len() || locators.is_empty() {
        return Err("an Open needs distinct locators".to_owned());
    }
    Ok(RegtestMessage::SubscribeHeaders(SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: key,
        update_sequence: 0,
        initial_target_tip_hash: target,
        acknowledged: Cursor {
            height: locators[0].height,
            hash: locators[0].hash,
            headers: 0,
            bytes: 0,
        },
        locator_hashes: locators.iter().map(|locator| locator.hash).collect(),
        added_credit: credit,
        tree_aux_schema: AuxSchema::None,
    }))
}

/// An ending of the node's subscription `id` with `outcome`.
fn ending(honest: &Honest, id: u64, outcome: HeadersOutcomeCode) -> RegtestMessage {
    let target = honest
        .serving
        .get(&id)
        .map_or(block::Hash([0; 32]), |serving| serving.target);
    RegtestMessage::HeadersOutcome(HeadersOutcome {
        subscription_id: id,
        initial_target_tip_hash: target,
        outcome,
    })
}

/// Fails unless `labels` are `expected`.
fn check(act: Act, labels: &Labels, expected: Labels) -> Result<(), String> {
    if *labels == expected {
        Ok(())
    } else {
        Err(format!(
            "{}: the models label its frame {labels:?}, not {expected:?}",
            act.name()
        ))
    }
}

/// Whether `frame`'s header alone has a fault.
fn frame_faults(frame: &Crafted) -> bool {
    let model =
        crate::model::FrameModel::new(&RegtestMessage::ROWS, crate::model::ReaderEnd::Persistent);
    model.labels(&frame.header) != Labels::NONE
}

/// Whether the reader decides `act`'s fault from its header and its own
/// state, before the payload.
fn header_decided(act: Act) -> bool {
    matches!(
        act,
        Act::Unsolicited { .. } | Act::OverCreditHeader | Act::ExtraEnding
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerAction;

    /// A script of acts keeps its shape through a trace's scenario JSON, so
    /// a recorded run reruns.
    #[test]
    fn acts_round_trip_through_json() {
        let script = vec![
            PeerAction::Act {
                act: Act::ReservedFlags {
                    row: Row::HeadersOutcome,
                    bit: 3,
                },
            },
            PeerAction::Act {
                act: Act::BadContent {
                    rule: ContentRule::OpenCursor,
                },
            },
            PeerAction::Act { act: Act::OverLive },
            PeerAction::Grant,
        ];
        let json = serde_json::to_string(&script).expect("a script serializes");
        assert_eq!(
            json,
            r#"[{"action":"act","act":{"act":"reserved-flags","row":"headers-outcome","bit":3}},{"action":"act","act":{"act":"bad-content","rule":"open-cursor"}},{"action":"act","act":{"act":"over-live"}},{"action":"grant"}]"#
        );
        let back: Vec<PeerAction> = serde_json::from_str(&json).expect("it reads back");
        assert_eq!(back, script);
    }

    /// Each act's label is the first of its set, and only conformant acts
    /// have none.
    #[test]
    fn an_act_is_labelled_by_the_first_of_its_set() {
        let long_page = Act::LongPayload { row: Row::Headers };
        assert_eq!(long_page.label(), Label::LongPayload);
        assert!(long_page.labels().contains(Label::OverCredit));
        assert_eq!(Act::SecondOpen.label(), Label::Conformant);
        assert_eq!(Act::ExtraEnding.label(), Label::Unsolicited);
    }
}
