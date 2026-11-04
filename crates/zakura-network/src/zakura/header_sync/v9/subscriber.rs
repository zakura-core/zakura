//! The header sync version 9 subscriber: one session's side of the
//! subscription this node opens on a peer.
//!
//! [`Subscriptions`] holds the subscription rules: credit, update sequences,
//! accepted cursors, and the keys a publisher may still tombstone. This module
//! adds what only header sync knows: the target and locators an `Open` sends,
//! the parent each page must extend, the page that carries the target marker,
//! each outcome's window, and how much credit to grant. [`SubscriberState`]
//! holds that policy without the lock the transport precheck shares or the
//! session's writer fence, which stay with [`Subscriber`].
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
//! The headers must form one chain from the page parent: the first header's
//! previous hash is that parent, and each later header's previous hash is the
//! header before it. The last header's hash decides the target marker. Before
//! the target, a header's parent hash equal to the target or to a sent locator
//! shows that the page passed the target or that the path skipped the highest
//! sent locator on the target's chain. A path from the highest sent locator
//! never contains another sent locator: a higher one on that path would also
//! be the target or one of its ancestors.
//!
//! # Credit
//!
//! Every header a subscription authorizes lands in the reactor's header
//! staging budget, [`HEADER_CHUNK_BUDGET_CAPACITY_V1`] headers across all
//! targets, until validation releases it, so the header window stays within
//! the `staging` headers the caller reserved.
//!
//! [`HEADER_CHUNK_BUDGET_CAPACITY_V1`]: crate::zakura::header_sync::scheduler::peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1
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
    rows, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Status, SubscribeHeaders,
    SUBSCRIBE_HEADERS,
};
use crate::zakura::{
    header_sync::wire::AuxSchema,
    regulation::{
        ExchangeWriter, SharedSubscriptions, SubscribeRefused, SubscriptionFault,
        SubscriptionLimits, Subscriptions, WriterFence,
    },
    transport::sim_chain,
    Frame, FramedRecv,
};

mod state;

use state::SubscriberState;

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
    /// The headers do not form one chain from the page parent.
    #[error("the headers do not link from the page parent")]
    Linkage,
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
            Self::Linkage => "linkage",
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

/// Proof that the subscriber built the value that holds it. Only this module
/// and its children can name it, so only the admission owner constructs an
/// [`Admitted`] page or an [`Ended`] subscription.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Seal;

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
    seal: Seal,
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
    seal: Seal,
}

/// One session's subscriber: the subscription this node holds on one peer.
///
/// It holds the policy in [`SubscriberState`] and the resources around it:
/// the subscription tools that the transport precheck shares, and the
/// session's writer fence.
#[derive(Debug)]
pub(crate) struct Subscriber {
    tools: SharedSubscriptions<u64, Cursor>,
    /// Whether a transport reader checks frame headers against `tools`.
    transport_admits: bool,
    /// The session's fence. Each `Open` holds an exchange of it until the
    /// terminal outcome.
    fence: WriterFence,
    state: SubscriberState,
}

/// The reader's tap for this session's subscription tables, if it has one.
/// The reader waits for the precheck the caller attaches next, so tagging
/// starts before it reads the first frame.
#[cfg(any(test, zakura_sim))]
pub(crate) fn observed_tools(recv: &FramedRecv) -> Option<crate::zakura::RegulationTap<u64>> {
    let tap = recv.stream_tap()?;
    tap.tag_frames();
    Some(crate::zakura::RegulationTap::new(tap.clone(), |id| *id))
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
        let tools = SharedSubscriptions::new(sim_chain!(
            Subscriptions::new(limits, rows(header_bytes))
                => observed_by(observed_tools(recv))
        ));
        let transport_admits = recv.attach_precheck(Arc::new(tools.admitting()));
        Self {
            tools,
            transport_admits,
            fence,
            state: SubscriberState::new(header_bytes),
        }
    }

    /// The encoded header size of the subscriber's network.
    pub(crate) fn header_bytes(&self) -> usize {
        self.state.header_bytes()
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
        self.state.check_caps(len).map_err(SubscriberFault::counted)
    }

    /// Record a later `Status` from the peer. Pages after it must fit its
    /// caps, whether it raised or lowered them.
    pub(crate) fn observe(&mut self, status: &Status) {
        self.state.observe(status);
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
        let plan = self.state.plan_open(status, locators, schema, staging)?;
        let exchange = self.fence.open().ok_or(SubscribeError::Fenced)?;
        let writer = exchange.writer();
        let update = self
            .tools
            .lock()
            .open_fenced(plan.id, plan.window, plan.start, exchange)?;
        Ok((self.state.opened(plan, update), writer))
    }

    /// Remove a subscription whose `Open` failed to send. If no byte was
    /// written, nothing else happens. If its first byte was written, the peer
    /// may answer, so dropping the exchange closes the connection.
    pub(crate) fn retract(&mut self, id: u64) -> bool {
        self.state.retract(&mut self.tools.lock(), id)
    }

    /// Check a decoded page against its subscription, spend its exact credit,
    /// and advance the receive cursor.
    ///
    /// The first page extends the highest sent locator on the target's chain,
    /// which is the target when a sent locator equals it. Each later page
    /// extends the receive cursor. Every header extends the one before it,
    /// starting from that parent. The marker is on exactly the page that
    /// ends at the target. A page after `Close` within existing credit is
    /// admitted.
    pub(crate) fn receive_page(
        &mut self,
        page: &Headers,
        payload_len: usize,
    ) -> Result<Admitted, SubscriberFault> {
        self.state
            .receive_page(
                &mut self.tools.lock(),
                self.transport_admits,
                page,
                payload_len,
            )
            .map_err(SubscriberFault::counted)
    }

    /// Check a terminal outcome against its subscription's window, and end the
    /// subscription.
    pub(crate) fn receive_outcome(
        &mut self,
        outcome: &HeadersOutcome,
    ) -> Result<Ended, SubscriberFault> {
        self.state
            .receive_outcome(&mut self.tools.lock(), outcome)
            .map_err(SubscriberFault::counted)
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
        self.state
            .accept_through(&mut self.tools.lock(), id, cursor, staging)
    }

    /// Record that a page of `id` failed contextual validation. The
    /// subscription accepts no more cursors and sends no more grants. The
    /// reactor disconnects the peer.
    pub(crate) fn validation_failed(&mut self, id: u64) -> bool {
        self.state.validation_failed(id)
    }

    /// Record a `Grant` that refills the window for `staging` reserved
    /// headers, and return it to write, if one is due. See
    /// [`SubscriberState::grant_due`].
    pub(crate) fn grant_due(&mut self, staging: u32) -> Option<SubscribeHeaders> {
        self.state.grant_due(&mut self.tools.lock(), staging)
    }

    /// Record a `Close` and return it to write. Existing credit stays, and
    /// pages within it are still admitted until the terminal outcome.
    pub(crate) fn close(&mut self) -> Result<SubscribeHeaders, SubscribeError> {
        self.state.close(&mut self.tools.lock())
    }

    /// The live subscription's ID, if there is one.
    pub(crate) fn live(&self) -> Option<u64> {
        self.state.live()
    }

    /// The live subscription, if a `Grant` can continue it toward its
    /// target: it is not closing or failed, it has not reached its target, it
    /// has no unspent header credit, and every page it received was accepted.
    /// Returns its ID, target, accepted cursor, and schema.
    pub(crate) fn resumable(&self) -> Option<(u64, block::Hash, Cursor, AuxSchema)> {
        self.state.resumable(&self.tools.lock())
    }
}
