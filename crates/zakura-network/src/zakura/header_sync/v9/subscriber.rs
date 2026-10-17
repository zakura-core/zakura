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
//! only updates the tools already counted. The `Open` goes out with
//! `FramedSend::send_fenced` and the writer that `open` returns. Each later
//! `Grant` and `Close` goes out with `FramedSend::send_following` and the same
//! writer, so an update queued behind an `Open` that the session's retirement
//! skipped is skipped too. Frame headers are checked against the same tools,
//! by the transport reader that [`Subscriber::new`] attaches to, or by
//! [`Subscriber::admit`].
//!
//! # Page linkage
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
//! targets, until validation releases it, so the header window stays within
//! the `staging` headers the caller reserved.
//!
//! Grants refill the window in prefixes of at most half of it, so the
//! publisher spends one half while the grant for the other travels. Take a
//! window of `W` headers, a link that carries `r` headers per second, and a
//! round trip `t` plus validation time `v` between a prefix's last header
//! leaving the publisher and its `Grant` arriving. The link then stays full
//! whenever `W / 2 >= r (t + v)`. On Mainnet a full window is 4,000 headers,
//! about 6 MB of pages, so header credit binds before byte credit, and at a
//! 200 ms round trip the link stays full up to about 10,000 headers per second
//! before validation time. A `Grant` costs 107 bytes per prefix of up to 2,000
//! headers, so smaller prefixes would add updates without adding throughput.
//!
//! The overlap needs pages of at most `W / 2` headers. A cursor names one
//! page, so acceptance moves a whole page at a time, and a larger page spends
//! the window before its own acceptance. The publisher caps each page at its
//! advertised `max_headers_per_response`, 1,000 by default, so a full window
//! keeps the overlap. A window of at most twice that cap, such as the
//! 250-header fair share of the staging budget, moves about one window per
//! round trip plus validation. Capping unspent credit at half the window would
//! split pages, but only half the window would then be in flight, which is no
//! faster unless validation takes longer than the round trip.
//!
//! Nothing here uses a timer or takes credit back, as the specification's
//! "Header sync — stream 5, version 9" section requires.

use std::sync::Arc;

use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::Frontier;

use super::{
    publisher::is_legal, rows, smallest_page, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode,
    Operation, Status, SubscribeHeaders, MAX_HS_PUSH_CREDIT_BYTES, MAX_HS_PUSH_CREDIT_HEADERS,
    MAX_LOCATOR_HASHES, SUBSCRIBE_HEADERS,
};
use crate::zakura::{
    header_sync::{scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1, wire::AuxSchema},
    regulation::{
        ExchangeWriter, SharedSubscriptions, SubscribeRefused, SubscriptionFault,
        SubscriptionLimits, Subscriptions, Update, WriterFence,
    },
    Credit, Frame, FramedRecv,
};

#[cfg(test)]
mod tests;

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
    /// The session's fence retired, or its connection closed.
    #[error("the session's writers are fenced")]
    Fenced,
    /// A sent locator is the `Status` target at another height than the
    /// `Status` advertised.
    #[error("a locator is the target at another height than advertised")]
    TargetHeight,
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
    /// A page continues past the target, or reaches its height without it,
    /// before the subscription reached it.
    #[error("a page continues past the initial target")]
    PastTarget,
    /// The page ends at the target at another height than its `Status`
    /// advertised.
    #[error("the page reaches the target at another height than advertised")]
    TargetHeight,
    /// The target marker disagrees with whether the page ends at the target.
    #[error("the target marker disagrees with the page's last header")]
    Marker,
    /// A target marker after the subscription reached its target.
    #[error("a target marker after the target was reached")]
    SecondMarker,
    /// An outcome outside its window.
    #[error("{0:?} arrived outside its window")]
    OutcomeWindow(HeadersOutcomeCode),
    /// A page above the header count or byte cap the publisher advertised.
    #[error("the page exceeds the publisher's advertised caps")]
    AboveAdvertised,
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
            Self::TargetHeight => "target_height",
            Self::Marker => "marker",
            Self::SecondMarker => "second_marker",
            Self::OutcomeWindow(_) => "outcome_window",
            Self::AboveAdvertised => "above_advertised",
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
pub struct Admitted {
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
pub struct Ended {
    /// The subscription.
    pub(crate) id: u64,
    /// Why it ended.
    pub(crate) outcome: HeadersOutcomeCode,
    /// Pages received.
    pub(crate) pages: u64,
    /// Whether it reached its initial target.
    pub(crate) reached: bool,
    /// The last cursor the reactor accepted, or the start. Pages admitted
    /// after it and not yet accepted are not included.
    pub(crate) accepted: Cursor,
}

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
struct Live {
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
#[derive(Debug)]
pub(crate) struct Subscriber {
    tools: SharedSubscriptions<u64, Cursor>,
    /// Whether a transport reader checks frame headers against `tools`.
    transport_admits: bool,
    /// The session's fence. Each `Open` holds an exchange of it until the
    /// terminal outcome.
    fence: WriterFence,
    /// At most one: `SUBSCRIBE_HEADERS` allows one live or closing
    /// subscription, and the tools refuse a second.
    live: Option<Live>,
    next_id: u64,
    /// The network's encoded header size, which fixes the smallest page.
    header_bytes: usize,
}

impl Subscriber {
    /// A subscriber with no subscription, on a network whose headers take
    /// `header_bytes`, for the session that `fence` guards and `recv` reads.
    /// It attaches its frame header check to `recv`'s transport reader, if
    /// `recv` has one. That check charges each page the reader queues
    /// against the unspent credit until [`Self::receive_page`] spends it, so
    /// queued pages never exceed what this node authorized.
    pub(crate) fn new(header_bytes: usize, fence: WriterFence, recv: &FramedRecv) -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        let tools = SharedSubscriptions::new(Subscriptions::new(limits, rows(header_bytes)));
        let transport_admits = recv.attach_precheck(Arc::new(tools.admitting()));
        Self {
            tools,
            transport_admits,
            fence,
            live: None,
            next_id: 1,
            header_bytes,
        }
    }

    /// The encoded header size of the subscriber's network.
    pub(crate) fn header_bytes(&self) -> usize {
        self.header_bytes
    }

    /// Check a page or outcome frame header before its payload is decoded.
    ///
    /// A transport reader already ran this check and charged the page, and
    /// [`Self::receive_page`] turns that charge into spent credit. In-process
    /// channels have no reader, so this runs the same check for them.
    ///
    /// It also holds a page to the publisher's advertised byte cap. Only a
    /// page can exceed a cap that the `Status` decoder accepted.
    pub(crate) fn admit(&self, frame: &Frame) -> Result<(), SubscriberFault> {
        let len = frame.payload.len();
        if !self.transport_admits {
            self.tools
                .lock()
                .precheck(frame.message_type, len)
                .map_err(|fault| SubscriberFault::from(fault).counted())?;
        }
        if self
            .live
            .as_ref()
            .is_some_and(|live| !live.caps.allow(0, len))
        {
            return Err(SubscriberFault::AboveAdvertised.counted());
        }
        Ok(())
    }

    /// Record a later `Status` from the peer. Pages after it must fit its
    /// caps, whether it raised or lowered them.
    pub(crate) fn observe(&mut self, status: &Status) {
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
    fn window(&self, staging: u32, schema: AuxSchema) -> Credit {
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
    /// larger page is accepted whole. The module docs explain why grants
    /// overlap delivery at this size, and when pages are too large for it.
    pub(crate) fn prefix(staging: u32) -> u32 {
        (Self::window_headers(staging) / 2).max(1)
    }

    /// Record an `Open` to the selected tip of `status`, and return it with
    /// the writer to send it through `FramedSend::send_fenced`.
    ///
    /// `locators` are this node's locators, highest first; the first is the
    /// start cursor. `staging` is the header staging budget reserved for the
    /// subscription. The `Open` grants the whole window, which fits one page
    /// under `schema`. A locator equal to the target reaches it at `Open`.
    ///
    /// The subscription holds the `Open`'s exchange until its terminal
    /// outcome, so a session that ends after the `Open` was written and
    /// before the outcome closes the connection.
    pub(crate) fn open(
        &mut self,
        status: &Status,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> Result<(SubscribeHeaders, ExchangeWriter), SubscribeError> {
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
        let id = self.next_id;
        let start = Cursor {
            height: start.height,
            hash: start.hash,
            headers: 0,
            bytes: 0,
        };
        let exchange = self.fence.open().ok_or(SubscribeError::Fenced)?;
        let writer = exchange.writer();
        let update = self.tools.lock().open_fenced(id, window, start, exchange)?;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let live = Live {
            id,
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
        };
        let message = live.message(Operation::Open, update);
        self.live = Some(live);
        Ok((message, writer))
    }

    /// Remove a subscription whose `Open` failed to send. If no byte was
    /// written, nothing else happens. If its first byte was written, the peer
    /// may answer, so dropping the exchange closes the connection.
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
        if self.transport_admits {
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
    ///
    /// After the terminal outcome, the subscription is gone, so a page it
    /// admitted that the reactor had not yet accepted returns `Unknown`. That
    /// is expected: no update can acknowledge it any more.
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
    #[cfg(test)]
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
        let window = self.window(staging, live.schema);
        let mut tools = self.tools.lock();
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

    /// The live subscription, if a `Grant` can continue it toward its
    /// target: it is not closing or failed, it has not reached its target, it
    /// has no unspent header credit, and every page it received was accepted.
    /// Returns its ID, target, accepted cursor, and schema.
    pub(crate) fn resumable(&self) -> Option<(u64, block::Hash, Cursor, AuxSchema)> {
        let live = self
            .live
            .as_ref()
            .filter(|live| !live.close_sent && !live.failed && !live.reached)?;
        let tools = self.tools.lock();
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
