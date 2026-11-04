//! The subscriber's header sync policy: the live subscription's identity,
//! page position, target and locators, advertised caps, terminal window, and
//! grant sizing. It holds no lock or send handle.
//!
//! [`Subscriber`](super::Subscriber) holds a [`SubscriberState`] and passes
//! each transition the subscription tools it locked, so admission stays
//! atomic with the transport precheck that shares those tools. Recording an
//! `Open`, its send rejection, page receipt, outcome receipt, acceptance,
//! and each grant are separate transitions.

use zakura_chain::block;
use zakura_header_chain::Frontier;

use super::{
    super::{
        publisher::is_legal, smallest_page, Cursor, Headers, HeadersOutcome, Operation, Status,
        SubscribeHeaders, MAX_HS_PUSH_CREDIT_BYTES, MAX_HS_PUSH_CREDIT_HEADERS, MAX_LOCATOR_HASHES,
    },
    Admitted, Ended, Seal, SubscribeError, SubscriberFault,
};
use crate::zakura::{
    header_sync::{scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1, wire::AuxSchema},
    regulation::{SubscribeRefused, SubscriptionFault, Subscriptions, Update},
    Credit,
};

/// The page caps one `Status` advertises.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct PageCaps {
    headers: u32,
    bytes: u32,
}

impl PageCaps {
    /// The caps `status` advertises.
    fn of(status: &Status) -> Self {
        Self {
            headers: status.max_headers_per_response,
            bytes: status.max_message_bytes,
        }
    }

    /// Whether these caps allow a page of `headers` and `bytes`.
    fn allow(self, headers: u32, bytes: usize) -> bool {
        headers <= self.headers && bytes <= usize::try_from(self.bytes).unwrap_or(usize::MAX)
    }
}

/// The live subscription's header sync state.
#[derive(Clone, Debug)]
pub(super) struct Live {
    id: u64,
    target: block::Hash,
    /// The target's height in the `Status` that named it.
    target_height: block::Height,
    /// The page caps of the latest `Status` from the publisher. A page must
    /// fit the caps of the latest `Status` before it on the ordered stream.
    caps: PageCaps,
    /// Sent locators, highest first.
    locators: Vec<Frontier>,
    /// The sent locator that equals the target, if any. The subscription
    /// reached its target at `Open`.
    target_locator: Option<Frontier>,
    pub(super) schema: AuxSchema,
    /// Whether the subscription reached its target, at `Open` or by a marked
    /// page.
    pub(super) reached: bool,
    pages: u64,
    /// The last cursor the reactor accepted, or the start.
    accepted: Cursor,
    close_sent: bool,
    failed: bool,
}

/// An `Open` that passed every header sync check, for the tools to record.
#[derive(Debug)]
pub(super) struct OpenPlan {
    pub(super) id: u64,
    /// The credit window the `Open` grants.
    pub(super) window: Credit,
    pub(super) start: Cursor,
    live: Live,
}

/// The subscription this node holds on one peer.
#[derive(Debug)]
pub(super) struct SubscriberState {
    /// At most one: `SUBSCRIBE_HEADERS` allows one live or closing
    /// subscription, and the tools refuse a second.
    pub(super) live: Option<Live>,
    next_id: u64,
    /// The network's encoded header size, which fixes the smallest page.
    header_bytes: usize,
}

impl SubscriberState {
    /// A subscriber with no subscription, on a network whose headers take
    /// `header_bytes`.
    pub(super) fn new(header_bytes: usize) -> Self {
        Self {
            live: None,
            next_id: 1,
            header_bytes,
        }
    }

    /// The encoded header size of the subscriber's network.
    pub(super) fn header_bytes(&self) -> usize {
        self.header_bytes
    }

    /// Hold a page or outcome frame of `payload_len` bytes to the
    /// publisher's advertised byte cap. Only a page can exceed a cap that
    /// the `Status` decoder accepted.
    pub(super) fn check_caps(&self, payload_len: usize) -> Result<(), SubscriberFault> {
        if self
            .live
            .as_ref()
            .is_some_and(|live| !live.caps.allow(0, payload_len))
        {
            return Err(SubscriberFault::AboveAdvertised);
        }
        Ok(())
    }

    /// Record a later `Status` from the peer. Pages after it must fit its
    /// caps, whether it raised or lowered them.
    pub(super) fn observe(&mut self, status: &Status) {
        if let Some(live) = self.live.as_mut() {
            live.caps = PageCaps::of(status);
        }
    }

    /// The headers of the credit window for `staging` reserved headers: at
    /// most the row's window and the staging budget.
    fn window_headers(staging: u32) -> u32 {
        // The budget capacity is 4,000 headers, so it fits u32.
        staging
            .min(MAX_HS_PUSH_CREDIT_HEADERS)
            .min(HEADER_CHUNK_BUDGET_CAPACITY_V1 as u32)
    }

    /// The credit window for `staging` reserved headers under `schema`:
    /// [`Self::window_headers`] in headers, and one smallest page per header
    /// in bytes.
    pub(super) fn window(&self, staging: u32, schema: AuxSchema) -> Credit {
        let headers = Self::window_headers(staging);
        // Widening usize to u64 is lossless on supported targets.
        let bytes = (u64::from(headers) * smallest_page(self.header_bytes, schema) as u64)
            .min(u64::from(MAX_HS_PUSH_CREDIT_BYTES));
        Credit {
            objects: headers,
            bytes: u32::try_from(bytes).expect("the byte window is at most 8 MiB"),
        }
    }

    /// The most headers the reactor validates before it accepts a cursor:
    /// half the window for `staging` reserved headers, and at least one. A
    /// larger page is accepted whole. The module docs of
    /// [`Subscriber`](super::Subscriber) explain why grants overlap delivery
    /// at this size, and when pages are too large for it.
    pub(super) fn prefix(staging: u32) -> u32 {
        (Self::window_headers(staging) / 2).max(1)
    }

    /// Check an `Open` to the selected tip of `status`, and return it for the
    /// tools to record. Nothing changes until [`Self::opened`].
    pub(super) fn plan_open(
        &self,
        status: &Status,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> Result<OpenPlan, SubscribeError> {
        let Some(&start) = locators.first() else {
            return Err(SubscribeError::Locators);
        };
        // At most 13 locators, so the pairwise check stays small.
        if locators.len() > MAX_LOCATOR_HASHES
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
        let window = self.window(staging, schema);
        if window.objects == 0 {
            return Err(SubscribeError::NoStaging);
        }
        let target = status.selected_tip_hash;
        let target_locator = locators
            .iter()
            .find(|locator| locator.hash == target)
            .copied();
        if target_locator.is_some_and(|locator| locator.height != status.selected_tip_height) {
            return Err(SubscribeError::TargetHeight);
        }
        let start = Cursor {
            height: start.height,
            hash: start.hash,
            headers: 0,
            bytes: 0,
        };
        Ok(OpenPlan {
            id: self.next_id,
            window,
            start,
            live: Live {
                id: self.next_id,
                target,
                target_height: status.selected_tip_height,
                caps: PageCaps::of(status),
                locators: locators.to_vec(),
                target_locator,
                schema,
                reached: target_locator.is_some(),
                pages: 0,
                accepted: start,
                close_sent: false,
                failed: false,
            },
        })
    }

    /// Make `plan` the live subscription once the tools recorded its
    /// `update`, and return the `Open` to write.
    pub(super) fn opened(&mut self, plan: OpenPlan, update: Update<Cursor>) -> SubscribeHeaders {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let message = plan.live.message(Operation::Open, update);
        self.live = Some(plan.live);
        message
    }

    /// Remove subscription `id`, whose `Open` failed to send, from this state
    /// and `tools`.
    pub(super) fn retract(&mut self, tools: &mut Subscriptions<u64, Cursor>, id: u64) -> bool {
        if self.live.as_ref().is_none_or(|live| live.id != id) {
            return false;
        }
        self.live = None;
        tools.retract(&id)
    }

    /// Check a decoded page against its subscription, spend its exact credit
    /// in `tools`, and advance the receive cursor. `transport_admits` says
    /// whether a transport reader already charged the page.
    pub(super) fn receive_page(
        &mut self,
        tools: &mut Subscriptions<u64, Cursor>,
        transport_admits: bool,
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
        if page.entries.is_empty() {
            return Err(SubscriptionFault::OverCredit.into());
        }
        // A page beyond u32::MAX headers exceeds any credit.
        let count = u32::try_from(page.entries.len()).map_err(|_| SubscriptionFault::OverCredit)?;
        let height = parent
            .height
            .0
            .checked_add(count)
            .expect("the decoder rejects a page whose last height overflows");
        if !live.caps.allow(count, payload_len) {
            return Err(SubscriberFault::AboveAdvertised);
        }
        // Hash each header once. An unlinked page must fault here: preparation
        // verifies Equihash before the planner notices the break, and that
        // refusal is local, so the peer would otherwise stay connected.
        let mut previous_hash = parent.hash;
        for (index, entry) in page.entries.iter().enumerate() {
            if entry.header.previous_block_hash != previous_hash {
                return Err(SubscriberFault::Linkage);
            }
            if !live.reached && index > 0 {
                if previous_hash == live.target {
                    return Err(SubscriberFault::PastTarget);
                }
                if live
                    .locators
                    .iter()
                    .any(|locator| locator.hash == previous_hash)
                {
                    return Err(SubscriberFault::LowerLocator);
                }
            }
            previous_hash = entry.header.hash();
        }
        let last_hash = previous_hash;
        if live.reached {
            if page.reaches_initial_target {
                return Err(SubscriberFault::SecondMarker);
            }
        } else {
            let reaches = last_hash == live.target;
            if !reaches
                && live
                    .locators
                    .iter()
                    .any(|locator| locator.hash == last_hash)
            {
                return Err(SubscriberFault::LowerLocator);
            }
            // Only the target is at its height on the path to it, so a page
            // that reaches that height elsewhere never reaches the target.
            if !reaches && height >= live.target_height.0 {
                return Err(SubscriberFault::PastTarget);
            }
            if reaches && height != live.target_height.0 {
                return Err(SubscriberFault::TargetHeight);
            }
            if page.reaches_initial_target != reaches {
                return Err(SubscriberFault::Marker);
            }
        }
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
        if transport_admits {
            tools.claim_admitted_page(&live.id, count, payload_len, cursor)?;
        } else {
            tools.claim_page(&live.id, count, payload_len, cursor)?;
        }
        live.pages += 1;
        live.reached |= page.reaches_initial_target;
        Ok(Admitted {
            id: live.id,
            parent,
            cursor,
            reaches_target: page.reaches_initial_target,
            after_close: live.close_sent,
            seal: Seal,
        })
    }

    /// Check a terminal outcome against its subscription's window, and end
    /// the subscription in this state and `tools`.
    pub(super) fn receive_outcome(
        &mut self,
        tools: &mut Subscriptions<u64, Cursor>,
        outcome: &HeadersOutcome,
    ) -> Result<Ended, SubscriberFault> {
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
        tools.claim_end(&live.id)?;
        let live = self.live.take().expect("the live subscription matched");
        Ok(Ended {
            id: live.id,
            outcome: outcome.outcome,
            pages: live.pages,
            reached: live.reached,
            accepted: live.accepted,
            seal: Seal,
        })
    }

    /// Record that every header through the page with `cursor` passed
    /// contextual validation, and return a `Grant` to write if one is due.
    pub(super) fn accept_through(
        &mut self,
        tools: &mut Subscriptions<u64, Cursor>,
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
        tools.accept(&id, cursor)?;
        live.accepted = *cursor;
        Ok(self.grant_due(tools, staging))
    }

    /// Record that a page of `id` failed contextual validation. The
    /// subscription accepts no more cursors and sends no more grants.
    pub(super) fn validation_failed(&mut self, id: u64) -> bool {
        let Some(live) = self.live.as_mut().filter(|live| live.id == id) else {
            return false;
        };
        live.failed = true;
        true
    }

    /// Record a `Grant` in `tools` that refills the window for `staging`
    /// reserved headers, and return it to write, if one is due.
    ///
    /// A grant is due once the room in the window reaches [`Self::prefix`]
    /// headers, or as soon as there is room while the unspent credit cannot
    /// fit a page. Before the target, a grant also leaves credit for one page.
    /// A smaller `staging` than before revokes nothing: grants wait until
    /// acceptance brings the window under it.
    pub(super) fn grant_due(
        &mut self,
        tools: &mut Subscriptions<u64, Cursor>,
        staging: u32,
    ) -> Option<SubscribeHeaders> {
        let live = self.live.as_ref()?;
        if live.close_sent || live.failed {
            return None;
        }
        let window = self.window(staging, live.schema);
        let credit = tools.credit(&live.id)?;
        let (granted, unspent) = (credit.granted(), credit.unspent());
        let room = |limit: u32, granted: u64, accepted: u64| {
            u64::from(limit).saturating_sub(granted.saturating_sub(accepted))
        };
        let headers = room(window.objects, granted.objects, live.accepted.headers);
        let bytes = room(window.bytes, granted.bytes, live.accepted.bytes);
        // Widening usize to u64 is lossless on supported targets.
        let page = smallest_page(self.header_bytes, live.schema) as u64;
        if headers == 0 && bytes < smallest_page(self.header_bytes, AuxSchema::None) as u64 {
            return None;
        }
        let starved = unspent.objects == 0 || unspent.bytes < page;
        if !starved && headers < u64::from(Self::prefix(staging)) {
            return None;
        }
        // Pages spend at most one smallest page of bytes per header, and
        // grants refill both windows together, so unspent bytes always cover
        // unspent headers at one smallest page each, and only a grant that
        // adds a header gets here. The credit therefore fits a page, as the
        // specification requires before the target.
        debug_assert!(
            headers >= 1 && unspent.bytes + bytes >= (unspent.objects + headers) * page,
            "a grant leaves credit for a page"
        );
        let added = Credit {
            objects: u32::try_from(headers).expect("the room is within the header window"),
            bytes: u32::try_from(bytes).expect("the room is within the byte window"),
        };
        let update = tools.grant(&live.id, added).ok()?;
        Some(live.message(Operation::Grant, update))
    }

    /// Record a `Close` in `tools` and return it to write.
    pub(super) fn close(
        &mut self,
        tools: &mut Subscriptions<u64, Cursor>,
    ) -> Result<SubscribeHeaders, SubscribeError> {
        let live = self.live.as_mut().ok_or(SubscribeRefused::Unknown)?;
        let update = tools.close(&live.id)?;
        live.close_sent = true;
        Ok(live.message(Operation::Close, update))
    }

    /// The live subscription's ID, if there is one.
    pub(super) fn live(&self) -> Option<u64> {
        self.live.as_ref().map(|live| live.id)
    }

    /// The live subscription, if a `Grant` can continue it toward its
    /// target: it is not closing or failed, it has not reached its target, it
    /// has no unspent header credit in `tools`, and every page it received
    /// was accepted. Returns its ID, target, accepted cursor, and schema.
    pub(super) fn resumable(
        &self,
        tools: &Subscriptions<u64, Cursor>,
    ) -> Option<(u64, block::Hash, Cursor, AuxSchema)> {
        let live = self
            .live
            .as_ref()
            .filter(|live| !live.close_sent && !live.failed && !live.reached)?;
        let idle = tools.credit(&live.id)?.unspent().objects == 0;
        (idle && tools.received(&live.id) == Some(&live.accepted)).then_some((
            live.id,
            live.target,
            live.accepted,
            live.schema,
        ))
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
