//! The header sync version 9 subscriber: one session's side of the
//! subscription this node opens on a peer.
//!
//! [`Subscriptions`] holds the subscription rules: credit, update sequences,
//! accepted cursors, and the keys a publisher may still tombstone. This module
//! adds what only header sync knows: the target and locators an `Open` sends,
//! the parent each page must extend, the page that carries the target marker,
//! each outcome's window, and how much credit to grant.
//!
//! # Updates
//!
//! [`Subscriber::open`], the grants, and [`Subscriber::close`] record each
//! update in the subscription tools before returning it, so the caller writes
//! only updates the tools already counted. The session reader checks each
//! frame header against the same tools through [`Subscriber::precheck`].
//!
//! # Pages and acknowledgements
//!
//! [`Subscriber::receive_page`] checks a decoded page against its
//! subscription and spends the page's exact credit. It does not validate
//! headers. The reactor runs context-free and contextual validation, then
//! calls [`Subscriber::accept_through`] with a page's cursor once every header
//! through that page passed. An update only ever acknowledges an accepted
//! cursor, so a cursor that failed validation is never acknowledged.
//! [`Subscriber::validation_failed`] also stops the subscription's grants
//! until the reactor disconnects the peer.
//!
//! A page's linkage is checked without hashing its headers, except the last
//! one, whose hash decides the target marker. Before the target, a header's
//! parent hash equal to the target or to a sent locator shows that the page
//! passed the target or that the path skipped the highest sent locator on the
//! target's chain. A path from the highest such locator never contains another
//! sent locator: a higher one on that path would also be the target or one of
//! its ancestors. Whether the headers link to each other is contextual
//! validation's chain-connection check.
//!
//! # Credit
//!
//! Every header a subscription authorizes lands in the reactor's header
//! staging budget, [`HEADER_CHUNK_BUDGET_CAPACITY_V1`] headers across all
//! targets, until validation releases it. So the header window, granted minus
//! accepted, never exceeds the `staging` headers the caller reserved for this
//! subscription. The byte window is what those headers occupy at one smallest
//! page each, so bytes never bind before headers. Every grant meets the
//! `Grant` minimum of one header or `P` bytes, and before the target it leaves
//! credit for one page.
//!
//! Grants refill the window in prefixes of at most half of it. Take a window
//! of `W` headers, a link that carries `r` headers per second, and a round
//! trip `t` plus validation time `v` between a prefix's last header leaving
//! the publisher and its `Grant` arriving. Granting only after accepting the
//! whole window idles the publisher for `t + v` every window, so throughput is
//! `W / (W / r + t + v)`, always below `r`. Granting each prefix of `W / 2`
//! lets the publisher spend the other half while the grant travels, so the
//! link stays full whenever `W / 2 >= r (t + v)`. On Mainnet a full window is
//! 4,000 headers, about 6 MB of pages, so header credit binds before byte
//! credit, and at a 200 ms round trip the link stays full up to about 10,000
//! headers per second before validation time. A `Grant` costs 107 bytes per
//! prefix of up to 2,000 headers, so smaller prefixes would add updates
//! without adding throughput. The reactor should validate in prefixes of at
//! most [`Subscriber::prefix`] headers, so acceptance keeps the same pace.
//!
//! # No deadline and no revocation
//!
//! Nothing here uses a timer. Slow progress is not a violation: the reactor
//! may fetch the target from another peer, or change its target, while this
//! subscription stays live. Neither revokes credit, because no call here takes
//! credit back. Only the publisher's pages spend credit, only `Close` stops new
//! pages, and only the publisher's terminal outcome ends the subscription.

use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::Frontier;

use super::{
    publisher::is_legal, smallest_page, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode,
    Message, Operation, Status, SubscribeHeaders, MAX_HS_PUSH_CREDIT_BYTES,
    MAX_HS_PUSH_CREDIT_HEADERS, SUBSCRIBE_HEADERS,
};
use crate::zakura::{
    header_sync::{scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1, wire::AuxSchema},
    regulation::{
        SharedSubscriptions, SubscribeRefused, SubscriptionFault, SubscriptionLimits,
        Subscriptions, Update,
    },
    wire_codec::WireMessage,
    Credit,
};

#[cfg(test)]
mod tests;

/// Most locator hashes an `Open` carries.
const MAX_LOCATORS: usize = 13;

/// Why this node did not send an update or accept a cursor. The peer is not
/// at fault.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum SubscribeError {
    /// A rule the subscription tools enforce.
    #[error(transparent)]
    Tools(#[from] SubscribeRefused),
    /// An `Open` needs 1 to 13 unique locators.
    #[error("an Open needs 1 to 13 unique locators")]
    Locators,
    /// The peer's `Status` does not advertise the auxiliary schema.
    #[error("the peer does not advertise the auxiliary schema")]
    Schema,
    /// No staging budget is reserved for the subscription.
    #[error("no staging budget is reserved")]
    NoStaging,
    /// The subscription has a page that failed validation.
    #[error("the subscription failed validation")]
    Failed,
}

/// A page or outcome that breaks a subscriber rule. Each case is a protocol
/// violation: `Disconnect`.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum SubscriberFault {
    /// A rule the subscription tools enforce: an unsolicited message, or a
    /// page above its credit.
    #[error(transparent)]
    Subscription(#[from] SubscriptionFault),
    /// The message names another initial target.
    #[error("the message names another initial target")]
    Target,
    /// The page's auxiliary schema differs from its subscription's.
    #[error("the page's auxiliary schema differs from its subscription's")]
    Schema,
    /// The first page extends no sent locator, or not the target that a sent
    /// locator matched.
    #[error("the first page extends no sent locator, or not the target")]
    FirstParent,
    /// A later page does not extend the receive cursor.
    #[error("the page does not extend the receive cursor")]
    Parent,
    /// The path to the target passes a higher sent locator than the one the
    /// first page extended.
    #[error("the path passes a higher sent locator")]
    LowerLocator,
    /// A page continues past the target before the subscription reached it.
    #[error("a page continues past the initial target")]
    PastTarget,
    /// The target marker disagrees with whether the page ends at the target.
    #[error("the target marker disagrees with the page's last header")]
    Marker,
    /// A target marker after the subscription reached its target.
    #[error("a target marker after the target was reached")]
    SecondMarker,
    /// An outcome outside its window.
    #[error("{0:?} arrived outside its window")]
    OutcomeWindow(HeadersOutcomeCode),
}

impl SubscriberFault {
    /// Stable metric and trace label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Subscription(fault) => fault.label(),
            Self::Target => "target",
            Self::Schema => "schema",
            Self::FirstParent => "first_parent",
            Self::Parent => "parent",
            Self::LowerLocator => "lower_locator",
            Self::PastTarget => "past_target",
            Self::Marker => "marker",
            Self::SecondMarker => "second_marker",
            Self::OutcomeWindow(_) => "outcome_window",
        }
    }

    /// Count this fault and return it.
    fn counted(self) -> Self {
        metrics::counter!("sync.header.v9.subscriber.fault", "reason" => self.label()).increment(1);
        self
    }
}

/// An admitted page, for the reactor to validate.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Admitted {
    /// The page's subscription.
    pub(crate) id: u64,
    /// The header the page's first header extends.
    pub(crate) parent: Frontier,
    /// The page's cursor. Pass it to [`Subscriber::accept_through`] once every
    /// header through this page passes validation.
    pub(crate) cursor: Cursor,
    /// Whether the page ends at the initial target.
    pub(crate) reaches_target: bool,
    /// Whether this node sent `Close` before the page arrived.
    pub(crate) after_close: bool,
}

/// An ended subscription, for the reactor to reschedule.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Ended {
    /// The subscription.
    pub(crate) id: u64,
    /// Why it ended.
    pub(crate) outcome: HeadersOutcomeCode,
    /// Pages received.
    pub(crate) pages: u64,
    /// Whether it reached its initial target.
    pub(crate) reached: bool,
    /// The last cursor the reactor accepted, or the start.
    pub(crate) accepted: Cursor,
}

/// The live subscription's header sync state.
#[derive(Clone, Debug)]
struct Live {
    id: u64,
    target: block::Hash,
    /// Sent locators, highest first.
    locators: Vec<Frontier>,
    /// The sent locator that equals the target, if any. The subscription
    /// reached its target at `Open`.
    target_locator: Option<Frontier>,
    schema: AuxSchema,
    /// Whether the subscription reached its target, at `Open` or by a marked
    /// page.
    reached: bool,
    pages: u64,
    /// The last cursor the reactor accepted, or the start.
    accepted: Cursor,
    close_sent: bool,
    failed: bool,
}

/// One session's subscriber: the subscription this node holds on one peer.
///
/// `HEADER_BYTES` is the network's encoded header size, which fixes the
/// smallest page.
#[derive(Debug)]
pub(crate) struct Subscriber<const HEADER_BYTES: usize> {
    tools: SharedSubscriptions<u64, Cursor>,
    /// At most one: `SUBSCRIBE_HEADERS` allows one live or closing
    /// subscription, and the tools refuse a second.
    live: Option<Live>,
    next_id: u64,
}

impl<const HEADER_BYTES: usize> Subscriber<HEADER_BYTES> {
    /// `P`: the smallest legal page, and the least byte credit a `Grant` adds
    /// when it adds no header.
    const SMALLEST_PAGE: usize = smallest_page(HEADER_BYTES, AuxSchema::None);

    /// A subscriber with no subscription.
    pub(crate) fn new() -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            tools: SharedSubscriptions::new(Subscriptions::new(
                limits,
                Message::<HEADER_BYTES>::RULES,
            )),
            live: None,
            next_id: 1,
        }
    }

    /// The table the session reader checks frame headers against.
    pub(crate) fn precheck(&self) -> SharedSubscriptions<u64, Cursor> {
        self.tools.clone()
    }

    /// The credit window for `staging` reserved headers under `schema`: at
    /// most the row's window and the staging budget in headers, and one
    /// smallest page per header in bytes.
    fn window(staging: u32, schema: AuxSchema) -> Credit {
        // The budget capacity is 4,000 headers, so it fits u32.
        let headers = staging
            .min(MAX_HS_PUSH_CREDIT_HEADERS)
            .min(HEADER_CHUNK_BUDGET_CAPACITY_V1 as u32);
        // Widening usize to u64 is lossless on supported targets.
        let bytes = (u64::from(headers) * smallest_page(HEADER_BYTES, schema) as u64)
            .min(u64::from(MAX_HS_PUSH_CREDIT_BYTES));
        Credit {
            objects: headers,
            bytes: u32::try_from(bytes).expect("the byte window is at most 8 MiB"),
        }
    }

    /// The most headers the reactor validates before it accepts a cursor:
    /// half the window for `staging` reserved headers, and at least one. The
    /// module docs explain why grants overlap delivery at this size.
    pub(crate) fn prefix(staging: u32) -> u32 {
        (Self::window(staging, AuxSchema::None).objects / 2).max(1)
    }

    /// Record an `Open` to the selected tip of `status`, and return it to
    /// write.
    ///
    /// `locators` are this node's locators, highest first; the first is the
    /// start cursor. `staging` is the header staging budget reserved for the
    /// subscription. The `Open` grants the whole window, which fits one page
    /// under `schema`. A locator equal to the target reaches it at `Open`.
    pub(crate) fn open(
        &mut self,
        status: &Status,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> Result<SubscribeHeaders, SubscribeError> {
        let Some(&start) = locators.first() else {
            return Err(SubscribeError::Locators);
        };
        // At most 13 locators, so the pairwise check stays small.
        if locators.len() > MAX_LOCATORS
            || locators.iter().enumerate().any(|(index, locator)| {
                locators[..index]
                    .iter()
                    .any(|earlier| earlier.hash == locator.hash)
            })
        {
            return Err(SubscribeError::Locators);
        }
        if status.tree_aux_schema_mask & schema.mask_bit() != schema.mask_bit() {
            return Err(SubscribeError::Schema);
        }
        let window = Self::window(staging, schema);
        if window.objects == 0 {
            return Err(SubscribeError::NoStaging);
        }
        let id = self.next_id;
        let start = Cursor {
            height: start.height,
            hash: start.hash,
            headers: 0,
            bytes: 0,
        };
        let update = self.tools.lock().open(id, window, start)?;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let target = status.selected_tip_hash;
        let target_locator = locators
            .iter()
            .find(|locator| locator.hash == target)
            .copied();
        let live = Live {
            id,
            target,
            locators: locators.to_vec(),
            target_locator,
            schema,
            reached: target_locator.is_some(),
            pages: 0,
            accepted: start,
            close_sent: false,
            failed: false,
        };
        let message = live.message(Operation::Open, update);
        self.live = Some(live);
        Ok(message)
    }

    /// Remove an `Open` that was never written, when the send failed locally
    /// before the first byte.
    pub(crate) fn retract(&mut self, id: u64) -> bool {
        if self.live.as_ref().is_none_or(|live| live.id != id) {
            return false;
        }
        self.live = None;
        self.tools.lock().retract(&id)
    }

    /// Check a decoded page against its subscription, spend its exact credit,
    /// and advance the receive cursor.
    ///
    /// The first page extends the highest sent locator on the target's chain,
    /// which is the target when a sent locator equals it. Each later page
    /// extends the receive cursor. The marker is on exactly the page that
    /// ends at the target. A page after `Close` within existing credit is
    /// admitted.
    pub(crate) fn receive_page(
        &mut self,
        page: &Headers,
        payload_len: usize,
    ) -> Result<Admitted, SubscriberFault> {
        self.check_page(page, payload_len)
            .map_err(SubscriberFault::counted)
    }

    /// [`Self::receive_page`], without counting the fault.
    fn check_page(
        &mut self,
        page: &Headers,
        payload_len: usize,
    ) -> Result<Admitted, SubscriberFault> {
        let live = self
            .live
            .as_mut()
            .filter(|live| live.id == page.subscription_id)
            .ok_or(SubscriptionFault::Unknown)?;
        if page.initial_target_tip_hash != live.target {
            return Err(SubscriberFault::Target);
        }
        if page.tree_aux_schema != live.schema {
            return Err(SubscriberFault::Schema);
        }
        let parent = Frontier::new(page.common_ancestor_height, page.common_ancestor_hash);
        let mut tools = self.tools.lock();
        if live.pages == 0 {
            let extends = match live.target_locator {
                Some(target) => parent == target,
                None => live.locators.contains(&parent),
            };
            if !extends {
                return Err(SubscriberFault::FirstParent);
            }
        } else {
            let received = tools
                .received(&live.id)
                .expect("the live subscription is in the tools");
            if parent != Frontier::new(received.height, received.hash) {
                return Err(SubscriberFault::Parent);
            }
        }
        let Some(last) = page.entries.last() else {
            return Err(SubscriptionFault::OverCredit.into());
        };
        let last_hash = last.header.hash();
        if live.reached {
            if page.reaches_initial_target {
                return Err(SubscriberFault::SecondMarker);
            }
        } else {
            // A header's parent is the previous header, once chain connection
            // passes. The first header's parent is the page's parent.
            for entry in &page.entries[1..] {
                let previous = entry.header.previous_block_hash;
                if previous == live.target {
                    return Err(SubscriberFault::PastTarget);
                }
                if live.locators.iter().any(|locator| locator.hash == previous) {
                    return Err(SubscriberFault::LowerLocator);
                }
            }
            let reaches = last_hash == live.target;
            if !reaches
                && live
                    .locators
                    .iter()
                    .any(|locator| locator.hash == last_hash)
            {
                return Err(SubscriberFault::LowerLocator);
            }
            if page.reaches_initial_target != reaches {
                return Err(SubscriberFault::Marker);
            }
        }
        // A page beyond u32::MAX headers exceeds any credit.
        let count = u32::try_from(page.entries.len()).map_err(|_| SubscriptionFault::OverCredit)?;
        let height = parent
            .height
            .0
            .checked_add(count)
            .expect("the decoder rejects a page whose last height overflows");
        let consumed = tools
            .credit(&live.id)
            .expect("the live subscription is in the tools")
            .consumed();
        // Widening usize to u64 is lossless on supported targets.
        let cursor = Cursor {
            height: block::Height(height),
            hash: last_hash,
            headers: consumed.objects.saturating_add(u64::from(count)),
            bytes: consumed.bytes.saturating_add(payload_len as u64),
        };
        tools.claim_page(&live.id, count, payload_len, cursor)?;
        live.pages += 1;
        live.reached |= page.reaches_initial_target;
        Ok(Admitted {
            id: live.id,
            parent,
            cursor,
            reaches_target: page.reaches_initial_target,
            after_close: live.close_sent,
        })
    }

    /// Check a terminal outcome against its subscription's window, and end the
    /// subscription.
    pub(crate) fn receive_outcome(
        &mut self,
        outcome: &HeadersOutcome,
    ) -> Result<Ended, SubscriberFault> {
        self.check_outcome(outcome)
            .map_err(SubscriberFault::counted)
    }

    /// [`Self::receive_outcome`], without counting the fault.
    fn check_outcome(&mut self, outcome: &HeadersOutcome) -> Result<Ended, SubscriberFault> {
        let live = self
            .live
            .as_ref()
            .filter(|live| live.id == outcome.subscription_id)
            .ok_or(SubscriptionFault::Unknown)?;
        if outcome.initial_target_tip_hash != live.target {
            return Err(SubscriberFault::Target);
        }
        if !is_legal(
            outcome.outcome,
            live.pages > 0,
            live.reached,
            live.close_sent,
        ) {
            return Err(SubscriberFault::OutcomeWindow(outcome.outcome));
        }
        self.tools.lock().claim_end(&live.id)?;
        let live = self.live.take().expect("the live subscription matched");
        Ok(Ended {
            id: live.id,
            outcome: outcome.outcome,
            pages: live.pages,
            reached: live.reached,
            accepted: live.accepted,
        })
    }

    /// Record that every header through the page with `cursor` passed
    /// contextual validation, and return a `Grant` to write if one is due.
    ///
    /// Call it only after validation passes. The next update acknowledges
    /// `cursor`. `staging` is the header staging budget now reserved for the
    /// subscription.
    pub(crate) fn accept_through(
        &mut self,
        id: u64,
        cursor: &Cursor,
        staging: u32,
    ) -> Result<Option<SubscribeHeaders>, SubscribeError> {
        let live = self
            .live
            .as_mut()
            .filter(|live| live.id == id)
            .ok_or(SubscribeRefused::Unknown)?;
        if live.failed {
            return Err(SubscribeError::Failed);
        }
        self.tools.lock().accept(&id, cursor)?;
        live.accepted = *cursor;
        Ok(self.grant_due(staging))
    }

    /// Record that a page of `id` failed contextual validation. The
    /// subscription accepts no more cursors and sends no more grants. The
    /// reactor disconnects the peer.
    pub(crate) fn validation_failed(&mut self, id: u64) -> bool {
        let Some(live) = self.live.as_mut().filter(|live| live.id == id) else {
            return false;
        };
        live.failed = true;
        true
    }

    /// Record a `Grant` that refills the window for `staging` reserved
    /// headers, and return it to write, if one is due.
    ///
    /// A grant is due once the room in the window reaches [`Self::prefix`]
    /// headers, or as soon as there is room while the unspent credit cannot
    /// fit a page. Before the target, a grant also leaves credit for one page.
    /// A smaller `staging` than before revokes nothing: grants wait until
    /// acceptance brings the window under it.
    pub(crate) fn grant_due(&mut self, staging: u32) -> Option<SubscribeHeaders> {
        let live = self.live.as_ref()?;
        if live.close_sent || live.failed {
            return None;
        }
        let window = Self::window(staging, live.schema);
        let mut tools = self.tools.lock();
        let credit = tools.credit(&live.id)?;
        let (granted, unspent) = (credit.granted(), credit.unspent());
        let room = |limit: u32, granted: u64, accepted: u64| {
            u64::from(limit).saturating_sub(granted.saturating_sub(accepted))
        };
        let headers = room(window.objects, granted.objects, live.accepted.headers);
        let bytes = room(window.bytes, granted.bytes, live.accepted.bytes);
        // Widening usize to u64 is lossless on supported targets.
        let page = smallest_page(HEADER_BYTES, live.schema) as u64;
        if headers == 0 && bytes < Self::SMALLEST_PAGE as u64 {
            return None;
        }
        let starved = unspent.objects == 0 || unspent.bytes < page;
        if !starved && headers < u64::from(Self::prefix(staging)) {
            return None;
        }
        if !live.reached && (unspent.objects + headers == 0 || unspent.bytes + bytes < page) {
            return None;
        }
        let added = Credit {
            objects: u32::try_from(headers).expect("the room is within the header window"),
            bytes: u32::try_from(bytes).expect("the room is within the byte window"),
        };
        let update = tools.grant(&live.id, added).ok()?;
        drop(tools);
        Some(live.message(Operation::Grant, update))
    }

    /// Record a `Close` and return it to write. Existing credit stays, and
    /// pages within it are still admitted until the terminal outcome.
    pub(crate) fn close(&mut self) -> Result<SubscribeHeaders, SubscribeError> {
        let live = self.live.as_mut().ok_or(SubscribeRefused::Unknown)?;
        let update = self.tools.lock().close(&live.id)?;
        live.close_sent = true;
        Ok(live.message(Operation::Close, update))
    }

    /// The live subscription's ID, if there is one.
    pub(crate) fn live(&self) -> Option<u64> {
        self.live.as_ref().map(|live| live.id)
    }
}

impl Live {
    /// The wire message for `update`. Only an `Open` carries the locators.
    fn message(&self, operation: Operation, update: Update<Cursor>) -> SubscribeHeaders {
        SubscribeHeaders {
            operation,
            subscription_id: self.id,
            update_sequence: update.sequence,
            initial_target_tip_hash: self.target,
            acknowledged: update.acknowledged,
            locator_hashes: match operation {
                Operation::Open => self.locators.iter().map(|locator| locator.hash).collect(),
                Operation::Grant | Operation::Close => Vec::new(),
            },
            added_credit: update.added,
            tree_aux_schema: self.schema,
        }
    }
}
