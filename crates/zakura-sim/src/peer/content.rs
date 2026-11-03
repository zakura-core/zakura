//! The header-sync content labeller (M5-E1, M5-Q14): `BadContent` for a
//! well-formed frame whose fields break a header-sync rule.
//!
//! It follows the header sync specification, never the production
//! validator. It reads the frame, the table's own rows, the peer's fixture
//! chain for the heights of its headers, and its own history of the
//! stream's subscriptions, and labels each rule below:
//!
//! - `Status`: a work anchor or retention floor above the selected tip, a
//!   header cap outside 1 to 4,000, a `max_subscriptions` other than the
//!   subscription row's `max_live`, an unknown schema bit, or a message cap
//!   outside one smallest page under the advertised schemas to 2 MiB.
//! - `SubscribeHeaders`: subscription ID 0. An `Open` with no locator, a
//!   repeated one or more than 13, a start cursor that isn't its first
//!   locator with nothing consumed, credit without a header
//!   or a smallest page of bytes, or a schema the node didn't advertise. A
//!   `Grant` or `Close` with locators, a `Grant` that adds neither a header
//!   nor a smallest page of bytes, a `Close` that adds credit, or an update
//!   that changes its own `Open`'s target or schema before the node ends
//!   that subscription. Rules only the sender can check, such as a start
//!   height or a target chosen from a received `Status`, aren't labelled:
//!   the receiver owes no consequence for them.
//! - `Headers`: subscription ID 0, a body-size hint above the largest
//!   block, or auxiliary records that don't match its schema. For a
//!   subscription the node opened, also a page naming another
//!   target or schema, or above the caps of the last `Status` the peer
//!   sent. Its parent, by height and hash, must be the last header sent,
//!   or for the first page a locator of the `Open`, the target if a
//!   locator is it. Its headers must link from the parent. Before the
//!   target is reached, no header may extend the target or a sent locator,
//!   a page that doesn't end at the target must end below its height, and
//!   one that does must end at that height. The marker is set on exactly
//!   the page that reaches the target. Any page must link from its parent.
//! - `HeadersOutcome`: subscription ID 0, an outcome naming another
//!   target, or one its
//!   subscription's history doesn't allow, such as `SubscriptionClosed`
//!   before the node's `Close`. A subscription whose locators hold its
//!   target reached it at `Open`.

use std::{collections::BTreeMap, sync::Arc};

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    v9::{
        Headers, HeadersOutcomeCode, Message, Operation, RegtestMessage, Status, SubscribeHeaders,
    },
    AuxSchema, MessageRole, MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE, TREE_AUX_SCHEMA_V1_BYTES,
};

use super::honest::HonestChain;
use crate::model::{Label, Labels};

/// The schema bits the specification defines: bit 0, schema 1.
const KNOWN_SCHEMAS: u32 = 1;

/// A subscription the node opened on this peer, as its frames show it.
#[derive(Clone, Debug)]
struct Served {
    /// The `Open`'s target.
    target: block::Hash,
    /// The `Open`'s schema.
    schema: AuxSchema,
    /// The `Open`'s locators, highest first.
    locators: Vec<block::Hash>,
    /// Whether one of the locators is the target.
    target_locator: bool,
    /// Pages sent.
    pages: u64,
    /// The last header sent, if a page was.
    last: Option<Frontier>,
    /// Whether it reached its target: at `Open`, if a locator is the
    /// target, or with a page that carried the target marker.
    reached: bool,
    /// Whether the node sent `Close`.
    close_received: bool,
}

/// One header stream's content history, from both directions.
#[derive(Clone, Debug, Default)]
pub(crate) struct ContentLabeller {
    /// The peer's chain, for its headers' heights. Without one, heights
    /// aren't checked.
    chain: Option<Arc<HonestChain>>,
    /// The page caps of the last `Status` the peer sent: headers and
    /// payload bytes.
    caps: Option<(u32, u32)>,
    /// The subscriptions the node opened that haven't ended, by ID.
    served: BTreeMap<u64, Served>,
    /// The schema mask of the last `Status` the node sent.
    node_schemas: Option<u32>,
    /// The target and schema of each `Open` the peer sent, by ID, until
    /// the node's ending for it arrives. The node matches a later update by
    /// key alone.
    opened: BTreeMap<u64, (block::Hash, AuxSchema)>,
}

/// The most locators an `Open` carries.
const MAX_LOCATORS: usize = 13;

/// `schema`'s bit in a `Status` mask: none for no schema, bit `n - 1` for
/// schema `n`.
fn schema_bit(schema: AuxSchema) -> u32 {
    match schema {
        AuxSchema::None => 0,
        AuxSchema::V1 => 1,
    }
}

/// The subscription row's `max_live`, which every `Status` advertises.
fn max_live() -> u16 {
    RegtestMessage::ROWS
        .iter()
        .find_map(|row| match row.role {
            MessageRole::Subscription { max_live, .. } => u16::try_from(max_live).ok(),
            _ => None,
        })
        .expect("header sync has a subscription row")
}

/// The smallest plain page, the `headers` row's minimum.
fn smallest_plain_page() -> usize {
    RegtestMessage::ROWS
        .iter()
        .find(|row| row.name == "headers")
        .expect("header sync has a headers row")
        .payload
        .min()
}

/// True if `status` breaks a `Status` rule.
fn bad_status(status: &Status) -> bool {
    let records = status.tree_aux_schema_mask & 1 != 0;
    let smallest = smallest_plain_page() + if records { TREE_AUX_SCHEMA_V1_BYTES } else { 0 };
    let cap = usize::try_from(status.max_message_bytes).unwrap_or(usize::MAX);
    status.work_anchor_height > status.selected_tip_height
        || status.oldest_retained_height > status.selected_tip_height
        || !(1..=MAX_HS_RANGE).contains(&status.max_headers_per_response)
        || status.max_subscriptions != max_live()
        || status.tree_aux_schema_mask & !KNOWN_SCHEMAS != 0
        || !(smallest..=MAX_HS_MESSAGE_BYTES).contains(&cap)
}

/// True if `page`'s headers don't form one chain from its parent.
fn unlinked(page: &Headers) -> bool {
    let mut previous = page.common_ancestor_hash;
    for entry in &page.entries {
        if entry.header.previous_block_hash != previous {
            return true;
        }
        previous = entry.header.hash();
    }
    false
}

/// True if `outcome` is allowed for a subscription that has or hasn't sent
/// a page, reached its target, and received `Close` (the specification's
/// outcome windows).
fn allowed(outcome: HeadersOutcomeCode, served: &Served) -> bool {
    match outcome {
        HeadersOutcomeCode::TargetNotRetained
        | HeadersOutcomeCode::NoLocatorIntersection
        | HeadersOutcomeCode::TargetNotSelected => served.pages == 0,
        HeadersOutcomeCode::HistoryPruned => !served.reached,
        HeadersOutcomeCode::SubscriptionSuperseded => true,
        HeadersOutcomeCode::SubscriptionClosed => served.close_received,
    }
}

impl ContentLabeller {
    /// A labeller for a peer holding `chain`.
    pub(crate) fn new(chain: Arc<HonestChain>) -> Self {
        ContentLabeller {
            chain: Some(chain),
            ..ContentLabeller::default()
        }
    }

    /// Whether `hash` sits at `height` on the peer's chain. True without a
    /// chain, which checks hashes alone.
    fn at(&self, hash: block::Hash, height: u32) -> bool {
        self.chain
            .as_ref()
            .is_none_or(|chain| chain.height(hash) == Some(height))
    }

    /// `hash`'s height on the peer's chain, if it has a chain and the hash.
    fn height(&self, hash: block::Hash) -> Option<u32> {
        self.chain.as_ref()?.height(hash)
    }

    /// `BadContent` if the peer's `message`, `payload_len` bytes long,
    /// breaks a content rule.
    pub(crate) fn label(&self, message: &RegtestMessage, payload_len: usize) -> Labels {
        let bad = match message {
            Message::Status(status) => bad_status(status),
            Message::SubscribeHeaders(update) => self.bad_update(update),
            Message::Headers(page) => {
                let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
                let over_caps = self.caps.is_some_and(|(headers, bytes)| {
                    count > headers || payload_len > usize::try_from(bytes).unwrap_or(usize::MAX)
                });
                let records = page.tree_aux_schema == AuxSchema::V1;
                let bad_entry = page.entries.iter().any(|entry| {
                    u64::from(entry.body_size) > block::MAX_BLOCK_BYTES
                        || entry.tree_aux.is_some() != records
                });
                page.subscription_id == 0
                    || bad_entry
                    || unlinked(page)
                    || over_caps
                    || self
                        .served
                        .get(&page.subscription_id)
                        .is_some_and(|served| self.bad_page(page, served))
            }
            Message::HeadersOutcome(outcome) => {
                self.served
                    .get(&outcome.subscription_id)
                    .is_some_and(|served| {
                        outcome.initial_target_tip_hash != served.target
                            || !allowed(outcome.outcome, served)
                    })
                    || outcome.subscription_id == 0
            }
        };
        Labels::NONE.with_if(bad, Label::BadContent)
    }

    /// True if the peer's `update` breaks a content rule.
    fn bad_update(&self, update: &SubscribeHeaders) -> bool {
        let added = update.added_credit;
        let smallest = |schema| {
            let records = if schema == AuxSchema::V1 {
                TREE_AUX_SCHEMA_V1_BYTES
            } else {
                0
            };
            u64::try_from(smallest_plain_page() + records).unwrap_or(u64::MAX)
        };
        let bytes = u64::from(added.bytes);
        let bad = match update.operation {
            Operation::Open => {
                let locators = &update.locator_hashes;
                let repeated = locators
                    .iter()
                    .enumerate()
                    .any(|(index, hash)| locators[..index].contains(hash));
                let cursor = update.acknowledged;
                // The start height is the sender's to get right: the node
                // takes it as given, since it may not hold the locator.
                let start = locators.first() == Some(&cursor.hash)
                    && cursor.headers == 0
                    && cursor.bytes == 0;
                let unadvertised = self.node_schemas.is_some_and(|mask| {
                    let bit = schema_bit(update.tree_aux_schema);
                    mask & bit != bit
                });
                locators.is_empty()
                    || locators.len() > MAX_LOCATORS
                    || repeated
                    || !start
                    || added.objects == 0
                    || bytes < smallest(update.tree_aux_schema)
                    || unadvertised
            }
            Operation::Grant | Operation::Close => {
                let changed = self
                    .opened
                    .get(&update.subscription_id)
                    .is_some_and(|fixed| {
                        *fixed != (update.initial_target_tip_hash, update.tree_aux_schema)
                    });
                let credit = match update.operation {
                    Operation::Grant => added.objects == 0 && bytes < smallest(AuxSchema::None),
                    _ => added.objects != 0 || added.bytes != 0,
                };
                !update.locator_hashes.is_empty() || credit || changed
            }
        };
        bad || update.subscription_id == 0
    }

    /// True if `page` breaks a rule of its subscription `served`'s history,
    /// from its parent to its marker.
    fn bad_page(&self, page: &Headers, served: &Served) -> bool {
        let parent = Frontier::new(page.common_ancestor_height, page.common_ancestor_hash);
        let parent_ok = match served.last {
            Some(last) => parent == last,
            None if served.target_locator => {
                parent.hash == served.target && self.at(parent.hash, parent.height.0)
            }
            None => served.locators.contains(&parent.hash) && self.at(parent.hash, parent.height.0),
        };
        if page.initial_target_tip_hash != served.target
            || page.tree_aux_schema != served.schema
            || !parent_ok
        {
            return true;
        }
        let Some(last) = page.entries.last().map(|entry| entry.header.hash()) else {
            return true;
        };
        if served.reached {
            return page.reaches_initial_target;
        }
        // Each header's parent, after the first's, before the last's hash.
        let passes = page.entries.iter().skip(1).any(|entry| {
            let previous = entry.header.previous_block_hash;
            previous == served.target || served.locators.contains(&previous)
        });
        let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
        let height = parent.height.0.saturating_add(count);
        let reaches = last == served.target;
        let target_height = self.height(served.target);
        let wrong_height = match (reaches, target_height) {
            (true, Some(target)) => height != target,
            (false, Some(target)) => height >= target,
            (_, None) => false,
        };
        passes
            || (!reaches && served.locators.contains(&last))
            || wrong_height
            || page.reaches_initial_target != reaches
    }

    /// The peer sent `message`, which the node takes as conformant.
    pub(crate) fn sent(&mut self, message: &RegtestMessage) {
        match message {
            Message::SubscribeHeaders(update) if update.operation == Operation::Open => {
                self.opened.insert(
                    update.subscription_id,
                    (update.initial_target_tip_hash, update.tree_aux_schema),
                );
            }
            Message::Status(status) => {
                self.caps = Some((status.max_headers_per_response, status.max_message_bytes));
            }
            Message::Headers(page) => {
                if let Some(served) = self.served.get_mut(&page.subscription_id) {
                    let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
                    served.pages += 1;
                    served.last = page.entries.last().map(|entry| {
                        Frontier::new(
                            block::Height(page.common_ancestor_height.0.saturating_add(count)),
                            entry.header.hash(),
                        )
                    });
                    served.reached |= page.reaches_initial_target;
                }
            }
            Message::HeadersOutcome(outcome) => {
                self.served.remove(&outcome.subscription_id);
            }
            Message::SubscribeHeaders(_) => {}
        }
    }

    /// The node sent `message`: an `Open` starts a subscription's history
    /// and a `Close` marks it, and a `Status` advertises its schemas.
    pub(crate) fn received(&mut self, message: &RegtestMessage) {
        match message {
            Message::Status(status) => self.node_schemas = Some(status.tree_aux_schema_mask),
            Message::HeadersOutcome(outcome) => {
                self.opened.remove(&outcome.subscription_id);
            }
            Message::SubscribeHeaders(_) | Message::Headers(_) => {}
        }
        let Message::SubscribeHeaders(update) = message else {
            return;
        };
        match update.operation {
            Operation::Open => {
                let target_locator = update
                    .locator_hashes
                    .contains(&update.initial_target_tip_hash);
                self.served.insert(
                    update.subscription_id,
                    Served {
                        target: update.initial_target_tip_hash,
                        schema: update.tree_aux_schema,
                        locators: update.locator_hashes.clone(),
                        target_locator,
                        pages: 0,
                        last: None,
                        reached: target_locator,
                        close_received: false,
                    },
                );
            }
            Operation::Close => {
                if let Some(served) = self.served.get_mut(&update.subscription_id) {
                    served.close_received = true;
                }
            }
            Operation::Grant => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::block::Height;

    use super::*;

    /// A `Status` the labeller accepts.
    fn status() -> Status {
        Status {
            work_anchor_height: Height(0),
            work_anchor_hash: block::Hash([0; 32]),
            selected_tip_height: Height(1),
            selected_tip_hash: block::Hash([1; 32]),
            suffix_cumulative_work: Default::default(),
            oldest_retained_height: Height(0),
            max_headers_per_response: 1_000,
            max_subscriptions: 1,
            max_message_bytes: 263,
            tree_aux_schema_mask: 0,
        }
    }

    /// Each `Status` rule labels, and nothing else does.
    #[test]
    fn status_rules() {
        let labeller = ContentLabeller::default();
        let label = |status: Status| labeller.label(&Message::Status(status), 122).first();
        assert_eq!(label(status()), Label::Conformant);
        for bad in [
            Status {
                max_subscriptions: 2,
                ..status()
            },
            Status {
                tree_aux_schema_mask: 2,
                ..status()
            },
            Status {
                max_message_bytes: 262,
                ..status()
            },
            Status {
                tree_aux_schema_mask: 1,
                ..status()
            },
            Status {
                max_headers_per_response: 0,
                ..status()
            },
            Status {
                max_headers_per_response: 4_001,
                ..status()
            },
            Status {
                work_anchor_height: Height(2),
                ..status()
            },
            Status {
                oldest_retained_height: Height(2),
                ..status()
            },
        ] {
            assert_eq!(label(bad.clone()), Label::BadContent, "{bad:?}");
        }
    }

    /// A subscription whose locators hold its target reached it at `Open`,
    /// so `HistoryPruned` is outside its window before any page. Without
    /// the target among them, it's allowed.
    #[test]
    fn a_target_locator_reaches_the_target_at_open() {
        use zakura_network::zakura::{
            v9::{Cursor, HeadersOutcome, SubscribeHeaders},
            AuxSchema, Credit, HeaderEntry,
        };

        let (target, other) = (block::Hash([1; 32]), block::Hash([2; 32]));
        let open = |locators: Vec<block::Hash>| {
            Message::SubscribeHeaders(SubscribeHeaders {
                operation: Operation::Open,
                subscription_id: 1,
                update_sequence: 0,
                initial_target_tip_hash: target,
                acknowledged: Cursor {
                    height: Height(1),
                    hash: locators[0],
                    headers: 0,
                    bytes: 0,
                },
                locator_hashes: locators,
                added_credit: Credit {
                    objects: 1,
                    bytes: 263,
                },
                tree_aux_schema: AuxSchema::None,
            })
        };
        let pruned = Message::HeadersOutcome(HeadersOutcome {
            subscription_id: 1,
            initial_target_tip_hash: target,
            outcome: HeadersOutcomeCode::HistoryPruned,
        });
        for (locator, expected) in [(target, Label::BadContent), (other, Label::Conformant)] {
            let mut labeller = ContentLabeller::default();
            labeller.received(&open(vec![locator]));
            assert_eq!(labeller.label(&pruned, 41).first(), expected, "{locator:?}");
        }
        // With the target among the locators, the first page extends it,
        // not an older locator.
        let mut labeller = ContentLabeller::default();
        labeller.received(&open(vec![target, other]));
        let header = |parent| {
            let mut header = *zakura_storage::header::fixture::HeaderFixture::regtest().extend(
                zakura_storage::header::fixture::HeaderFixture::regtest().anchor(),
                1,
                0,
            )[0];
            header.previous_block_hash = parent;
            header
        };
        let page = |parent| {
            Message::Headers(Headers {
                subscription_id: 1,
                initial_target_tip_hash: target,
                common_ancestor_height: Height(1),
                common_ancestor_hash: parent,
                reaches_initial_target: false,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: header(parent).into(),
                    body_size: 0,
                    tree_aux: None,
                }],
            })
        };
        assert_eq!(
            labeller.label(&page(target), 263).first(),
            Label::Conformant
        );
        assert_eq!(labeller.label(&page(other), 263).first(), Label::BadContent);
    }

    /// A later page's parent is the last header sent by height and hash,
    /// and a page carries the target marker exactly when it ends at the
    /// target. The node opens from the fixture's anchor toward its second
    /// header, so the first page of one header has no marker and the
    /// second ends at the target with it.
    #[test]
    fn pages_follow_their_history_by_height_and_hash() {
        use zakura_network::zakura::{
            v9::{Cursor, SubscribeHeaders},
            Credit, HeaderEntry,
        };
        use zakura_storage::header::fixture::HeaderFixture;

        let fixture = HeaderFixture::regtest();
        let anchor = fixture.anchor();
        let headers = fixture.extend(anchor, 2, 0);
        let target = headers[1].hash();
        let mut labeller = ContentLabeller::default();
        labeller.received(&Message::SubscribeHeaders(SubscribeHeaders {
            operation: Operation::Open,
            subscription_id: 1,
            update_sequence: 0,
            initial_target_tip_hash: target,
            acknowledged: Cursor {
                height: anchor.height,
                hash: anchor.hash,
                headers: 0,
                bytes: 0,
            },
            locator_hashes: vec![anchor.hash],
            added_credit: Credit {
                objects: 2,
                bytes: 526,
            },
            tree_aux_schema: AuxSchema::None,
        }));
        let page = |height: u32, parent: block::Hash, index: usize, marker: bool| {
            Message::Headers(Headers {
                subscription_id: 1,
                initial_target_tip_hash: target,
                common_ancestor_height: Height(height),
                common_ancestor_hash: parent,
                reaches_initial_target: marker,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: headers[index].clone(),
                    body_size: 0,
                    tree_aux: None,
                }],
            })
        };
        let label = |labeller: &ContentLabeller, message| labeller.label(&message, 263).first();
        let first = page(0, anchor.hash, 0, false);
        assert_eq!(label(&labeller, first.clone()), Label::Conformant);
        assert_eq!(
            label(&labeller, page(0, anchor.hash, 0, true)),
            Label::BadContent,
            "a marker before the target"
        );
        labeller.sent(&first);
        let parent = headers[0].hash();
        assert_eq!(
            label(&labeller, page(1, parent, 1, true)),
            Label::Conformant
        );
        assert_eq!(
            label(&labeller, page(5, parent, 1, true)),
            Label::BadContent,
            "the last header's hash at another height"
        );
        assert_eq!(
            label(&labeller, page(1, parent, 1, false)),
            Label::BadContent,
            "the target without its marker"
        );
    }

    /// Each update rule labels, and the update it breaks is otherwise
    /// conformant.
    #[test]
    fn update_rules() {
        use zakura_network::zakura::{v9::Cursor, Credit};

        let hash = block::Hash([3; 32]);
        let open = SubscribeHeaders {
            operation: Operation::Open,
            subscription_id: 1,
            update_sequence: 0,
            initial_target_tip_hash: block::Hash([1; 32]),
            acknowledged: Cursor {
                height: Height(5),
                hash,
                headers: 0,
                bytes: 0,
            },
            locator_hashes: vec![hash],
            added_credit: Credit {
                objects: 1,
                bytes: 263,
            },
            tree_aux_schema: AuxSchema::None,
        };
        let grant = SubscribeHeaders {
            operation: Operation::Grant,
            update_sequence: 1,
            locator_hashes: Vec::new(),
            ..open.clone()
        };
        let mut labeller = ContentLabeller::default();
        labeller.received(&Message::Status(status()));
        labeller.sent(&Message::SubscribeHeaders(open.clone()));
        let label = |update: SubscribeHeaders| {
            labeller
                .label(&Message::SubscribeHeaders(update), 139)
                .first()
        };
        assert_eq!(label(open.clone()), Label::Conformant);
        assert_eq!(label(grant.clone()), Label::Conformant);
        for bad in [
            SubscribeHeaders {
                subscription_id: 0,
                ..open.clone()
            },
            SubscribeHeaders {
                locator_hashes: (0..14).map(|n| block::Hash([n; 32])).collect(),
                ..open.clone()
            },
            SubscribeHeaders {
                added_credit: Credit {
                    objects: 1,
                    bytes: 262,
                },
                ..open.clone()
            },
            SubscribeHeaders {
                tree_aux_schema: AuxSchema::V1,
                added_credit: Credit {
                    objects: 1,
                    bytes: 1_000,
                },
                ..open.clone()
            },
            SubscribeHeaders {
                locator_hashes: vec![hash],
                ..grant.clone()
            },
            SubscribeHeaders {
                added_credit: Credit {
                    objects: 0,
                    bytes: 262,
                },
                ..grant.clone()
            },
            SubscribeHeaders {
                operation: Operation::Close,
                ..grant.clone()
            },
            SubscribeHeaders {
                initial_target_tip_hash: block::Hash([2; 32]),
                ..grant.clone()
            },
        ] {
            assert_eq!(label(bad.clone()), Label::BadContent, "{bad:?}");
        }
    }
}
