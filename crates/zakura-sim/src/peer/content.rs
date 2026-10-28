//! The header-sync content labeller (M5-E1, M5-Q14): `BadContent` for a
//! well-formed frame whose fields break a header-sync rule.
//!
//! It follows the header sync specification, never the production
//! validator. It reads the frame, the table's own rows, and its own history
//! of the stream's subscriptions, and labels each rule below:
//!
//! - `Status`: a work anchor or retention floor above the selected tip, a
//!   header cap outside 1 to 4,000, a `max_subscriptions` other than the
//!   subscription row's `max_live`, an unknown schema bit, or a message cap
//!   outside one smallest page under the advertised schemas to 2 MiB.
//! - `SubscribeHeaders`: an `Open` with no locator or a repeated one, or
//!   whose start cursor isn't its first locator with nothing consumed.
//! - `Headers`: a page whose headers don't link from its parent, or whose
//!   parent isn't a locator of the `Open` (the first page, which extends
//!   the target if a locator is it) or the last header sent (each later
//!   one).
//! - `HeadersOutcome`: an outcome its subscription's history doesn't
//!   allow, such as `SubscriptionClosed` before the node's `Close`. A
//!   subscription whose locators hold its target reached it at `Open`.

use std::collections::BTreeMap;

use zakura_chain::block;
use zakura_network::zakura::{
    v9::{Headers, HeadersOutcomeCode, Message, Operation, RegtestMessage, Status},
    MessageRole, MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE, TREE_AUX_SCHEMA_V1_BYTES,
};

use crate::model::{Label, Labels};

/// The schema bits the specification defines: bit 0, schema 1.
const KNOWN_SCHEMAS: u32 = 1;

/// A subscription the node opened on this peer, as its frames show it.
#[derive(Clone, Debug)]
struct Served {
    /// The `Open`'s locators, highest first.
    locators: Vec<block::Hash>,
    /// The `Open`'s target, if one of the locators is it.
    target_locator: Option<block::Hash>,
    /// Pages sent.
    pages: u64,
    /// The last header sent, if a page was.
    last: Option<block::Hash>,
    /// Whether it reached its target: at `Open`, if a locator is the
    /// target, or with a page that carried the target marker.
    reached: bool,
    /// Whether the node sent `Close`.
    close_received: bool,
}

/// One header stream's content history, from both directions.
#[derive(Clone, Debug, Default)]
pub(crate) struct ContentLabeller {
    /// The subscriptions the node opened that haven't ended, by ID.
    served: BTreeMap<u64, Served>,
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
    /// `BadContent` if the peer's `message` breaks a content rule.
    pub(crate) fn label(&self, message: &RegtestMessage) -> Labels {
        let bad = match message {
            Message::Status(status) => bad_status(status),
            Message::SubscribeHeaders(update) => {
                update.operation == Operation::Open && {
                    let locators = &update.locator_hashes;
                    let repeated = locators
                        .iter()
                        .enumerate()
                        .any(|(index, hash)| locators[..index].contains(hash));
                    let cursor = update.acknowledged;
                    let start = locators.first() == Some(&cursor.hash)
                        && cursor.headers == 0
                        && cursor.bytes == 0;
                    locators.is_empty() || repeated || !start
                }
            }
            Message::Headers(page) => {
                unlinked(page)
                    || self
                        .served
                        .get(&page.subscription_id)
                        .is_some_and(|served| {
                            let parent = page.common_ancestor_hash;
                            match (served.last, served.target_locator) {
                                (Some(last), _) => parent != last,
                                (None, Some(target)) => parent != target,
                                (None, None) => !served.locators.contains(&parent),
                            }
                        })
            }
            Message::HeadersOutcome(outcome) => self
                .served
                .get(&outcome.subscription_id)
                .is_some_and(|served| !allowed(outcome.outcome, served)),
        };
        Labels::NONE.with_if(bad, Label::BadContent)
    }

    /// The peer sent `message`, which the node takes as conformant.
    pub(crate) fn sent(&mut self, message: &RegtestMessage) {
        match message {
            Message::Headers(page) => {
                if let Some(served) = self.served.get_mut(&page.subscription_id) {
                    served.pages += 1;
                    served.last = page.entries.last().map(|entry| entry.header.hash());
                    served.reached |= page.reaches_initial_target;
                }
            }
            Message::HeadersOutcome(outcome) => {
                self.served.remove(&outcome.subscription_id);
            }
            Message::Status(_) | Message::SubscribeHeaders(_) => {}
        }
    }

    /// The node sent `message`: an `Open` starts a subscription's history
    /// and a `Close` marks it.
    pub(crate) fn received(&mut self, message: &RegtestMessage) {
        let Message::SubscribeHeaders(update) = message else {
            return;
        };
        match update.operation {
            Operation::Open => {
                self.served.insert(
                    update.subscription_id,
                    Served {
                        locators: update.locator_hashes.clone(),
                        pages: 0,
                        last: None,
                        target_locator: update
                            .locator_hashes
                            .contains(&update.initial_target_tip_hash)
                            .then_some(update.initial_target_tip_hash),
                        reached: update
                            .locator_hashes
                            .contains(&update.initial_target_tip_hash),
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
        let label = |status: Status| labeller.label(&Message::Status(status)).first();
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
            assert_eq!(labeller.label(&pruned).first(), expected, "{locator:?}");
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
        assert_eq!(labeller.label(&page(target)).first(), Label::Conformant);
        assert_eq!(labeller.label(&page(other)).first(), Label::BadContent);
    }
}
