//! `header-sync/commit-only-authorized`'s credit half: what the subscriber
//! builds stays within what the node reserved for it, and each update
//! carries its subscription's state faithfully.
//!
//! - **Issuance.** Each `CreditIssued` adds at most the headers its peer's
//!   reservations hold that no earlier credit backs: those opened since the
//!   peer's last credit, less any a `ReservationEnded` returned first. Its
//!   bytes are at most one smallest page per added header under the
//!   subscription's schema, capped at 8 MiB, as the subscriber derives its
//!   byte window.
//! - **Updates.** Each `Grant` and `Close` names its own subscription, as
//!   the subscriber's state reports it, and its `Open`'s target and
//!   schema, its sequence is one more than the subscription's previous
//!   update's, and it acknowledges the subscription's latest
//!   `CursorsAccepted`, or the `Open`'s start before any, in all four
//!   fields: the header, and the header and byte credit consumed through
//!   it, which the publisher compares too.
//! - **Claims.** Each `CursorClaimed` records the cursor its subscription's
//!   pages so far consumed, ending at that page's own last header: the sum
//!   of every claimed page's entries and payload bytes, counted here from
//!   each page as the fact reads it. What the subscription's primitive
//!   counts as consumed after the claim must be that sum too.
//! - **Acceptance.** Each `CursorsAccepted` names a claimed cursor whole,
//!   reaches the last header of a page staged on that subscription, never
//!   a cursor no page staged, and never goes below an earlier acceptance.
//! - **Operations.** An `Open`'s and a `Grant`'s credit comes in an update
//!   of that operation, and a built `Close` is a `Close` that adds no
//!   credit.
//!
//! Credit and acceptance in a stale call's handler are
//! `stale-completion-inert`'s effects, so they aren't judged here. They
//! still move the subscription's state, so later updates are judged
//! against what production did.
//!
//! The model restates the smallest page from the page layout, which a unit
//! test checks against an encoded page.

use std::collections::{BTreeMap, HashMap, HashSet};

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{CreditKind, ReactorEvent, SubscribeHeaders},
    v9::Operation,
    AuxSchema, ZakuraPeerId, TREE_AUX_SCHEMA_V1_BYTES,
};

use super::Subscription;
use crate::peer::{ENTRY_BYTES, PAGE_FIXED_BYTES};

/// The most bytes one update may grant, as the `SubscribeHeaders` row
/// caps its byte window.
const MAX_CREDIT_BYTES: u64 = 8 * 1024 * 1024;

/// Bytes of a one-header Regtest page under `schema`: the page's fixed
/// fields, one entry, and one auxiliary record under V1.
pub(super) fn smallest_page(schema: AuxSchema) -> u64 {
    let aux = match schema {
        AuxSchema::None => 0,
        AuxSchema::V1 => TREE_AUX_SCHEMA_V1_BYTES,
    };
    // Widening usize to u64 is lossless on supported targets.
    (PAGE_FIXED_BYTES + ENTRY_BYTES + aux) as u64
}

/// A subscription's state, as its `Open` and later facts set it.
#[derive(Debug)]
struct Live {
    target: block::Hash,
    schema: AuxSchema,
    /// The last update's sequence.
    sequence: u32,
    /// What the next update must acknowledge: the latest accepted cursor,
    /// or the `Open`'s start.
    acknowledged: Acknowledged,
    /// The last header of each page staged on the subscription.
    staged: HashSet<Frontier>,
    /// The latest accepted cursor.
    accepted: Option<Frontier>,
    /// The headers and bytes every page claimed so far consumed.
    consumed: (u64, u64),
    /// Each cursor a claim recorded.
    claimed: HashSet<Acknowledged>,
}

/// A cursor's header and the header and byte credit consumed through it.
type Acknowledged = (Frontier, u64, u64);

/// The credit half's state.
#[derive(Debug, Default)]
pub(super) struct Credit {
    /// Each peer's reservations that no credit backs yet, by number, with
    /// their headers.
    unbacked: HashMap<ZakuraPeerId, BTreeMap<u64, usize>>,
    live: HashMap<Subscription, Live>,
    breaks: Vec<String>,
}

impl Credit {
    /// Notes `event`, judging it if it's a credit fact. `on_reactor` says
    /// the reactor's own task reported it, and `stale_handler` that it came
    /// in a stale call's handler. Reservations are noted from any task,
    /// since a reservation's last handle may drop elsewhere. Returns
    /// whether it judged a credit fact.
    pub(super) fn observe(
        &mut self,
        event: &ReactorEvent,
        on_reactor: bool,
        stale_handler: bool,
    ) -> bool {
        match event {
            ReactorEvent::ReservationOpened {
                reservation,
                peer,
                headers,
                ..
            } => {
                self.unbacked
                    .entry(peer.clone())
                    .or_default()
                    .insert(*reservation, *headers);
                false
            }
            ReactorEvent::ReservationEnded { reservation, .. } => {
                for reservations in self.unbacked.values_mut() {
                    reservations.remove(reservation);
                }
                false
            }
            // The task that received the page reports its claim.
            ReactorEvent::CursorClaimed {
                peer,
                session_id,
                subscription,
                through,
                headers,
                bytes,
                page_last,
                page_headers,
                page_bytes,
                charged,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let page = (*page_last, *page_headers, *page_bytes);
                self.claim(&key, (*through, *headers, *bytes), page, *charged);
                true
            }
            _ if !on_reactor => false,
            ReactorEvent::CreditIssued {
                kind,
                peer,
                session_id,
                subscription,
                objects,
                bytes,
                update,
                ..
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                if !stale_handler {
                    self.issue(&key, *objects, *bytes, update);
                }
                let operation = match kind {
                    CreditKind::Open => Operation::Open,
                    CreditKind::Grant => Operation::Grant,
                };
                if update.operation != operation {
                    self.breaks.push(format!(
                        "CommitOnlyAuthorized: session {session_id} subscription {subscription}'s \
                         {kind:?} credit came in a {:?} update",
                        update.operation
                    ));
                }
                match kind {
                    CreditKind::Open => {
                        if update.subscription_id != *subscription {
                            self.breaks.push(format!(
                                "CommitOnlyAuthorized: session {session_id} subscription \
                                 {subscription}'s Open names subscription {}",
                                update.subscription_id
                            ));
                        }
                        self.live.insert(
                            key,
                            Live {
                                target: update.initial_target_tip_hash,
                                schema: update.tree_aux_schema,
                                sequence: update.update_sequence,
                                acknowledged: acknowledged(update),
                                staged: HashSet::new(),
                                accepted: None,
                                consumed: (0, 0),
                                claimed: HashSet::new(),
                            },
                        );
                    }
                    CreditKind::Grant if stale_handler => {
                        if let Some(live) = self.live.get_mut(&key) {
                            live.sequence = update.update_sequence;
                        }
                    }
                    CreditKind::Grant => self.update("Grant", &key, update),
                }
                !stale_handler
            }
            ReactorEvent::CloseBuilt {
                peer,
                session_id,
                subscription,
                update,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                if update.operation != Operation::Close
                    || update.added_credit.objects != 0
                    || update.added_credit.bytes != 0
                {
                    self.breaks.push(format!(
                        "CommitOnlyAuthorized: session {session_id} subscription {subscription}'s \
                         built Close is a {:?} adding {:?}",
                        update.operation, update.added_credit
                    ));
                }
                self.update("Close", &key, update);
                true
            }
            ReactorEvent::PageStaged {
                request,
                subscription,
                ancestor,
                hashes,
                ..
            } => {
                let key = (request.peer.clone(), request.session_id, *subscription);
                if let (Some(live), Some(last)) = (self.live.get_mut(&key), hashes.last()) {
                    // A page holds at most a few thousand headers.
                    let count = u32::try_from(hashes.len()).expect("a page's count fits u32");
                    live.staged.insert(Frontier::new(
                        block::Height(ancestor.height.0.saturating_add(count)),
                        *last,
                    ));
                }
                false
            }
            ReactorEvent::CursorsAccepted {
                peer,
                session_id,
                subscription,
                through,
                headers,
                bytes,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let cursor = (*through, *headers, *bytes);
                if stale_handler {
                    if let Some(live) = self.live.get_mut(&key) {
                        live.accepted = Some(*through);
                        live.acknowledged = cursor;
                    }
                    return false;
                }
                self.accept(&key, cursor);
                true
            }
            _ => false,
        }
    }

    /// The breaks found since the last call.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }

    /// Judges the credit `update` adds to `key`'s subscription against its
    /// peer's unbacked reservations and its schema's byte bound, then marks
    /// those reservations backed.
    fn issue(&mut self, key: &Subscription, objects: u32, bytes: u32, update: &SubscribeHeaders) {
        let (peer, session, subscription) = key;
        let reserved: usize = self
            .unbacked
            .remove(peer)
            .map_or(0, |reservations| reservations.values().sum());
        // Widening u32 to usize is lossless on supported targets.
        if objects as usize > reserved {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} was granted \
                 {objects} headers, but its peer's unbacked reservations hold {reserved}"
            ));
        }
        let limit =
            (u64::from(objects) * smallest_page(update.tree_aux_schema)).min(MAX_CREDIT_BYTES);
        if u64::from(bytes) > limit {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} was granted \
                 {bytes} bytes for {objects} headers under {:?}, above {limit}",
                update.tree_aux_schema
            ));
        }
    }

    /// Judges a built `Grant` or `Close`, named `operation`, against `key`'s
    /// subscription, then records its sequence.
    fn update(&mut self, operation: &str, key: &Subscription, update: &SubscribeHeaders) {
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: a {operation} names session {session} subscription \
                 {subscription}, which the subscriber never opened"
            ));
            return;
        };
        let mut found = Vec::new();
        if update.subscription_id != *subscription {
            found.push(format!("names subscription {}", update.subscription_id));
        }
        if update.initial_target_tip_hash != live.target {
            found.push(format!(
                "names target {}, not its Open's {}",
                update.initial_target_tip_hash, live.target
            ));
        }
        if update.tree_aux_schema != live.schema {
            found.push(format!(
                "names schema {:?}, not its Open's {:?}",
                update.tree_aux_schema, live.schema
            ));
        }
        let next = live.sequence.wrapping_add(1);
        if update.update_sequence != next {
            found.push(format!(
                "has sequence {}, not {next}",
                update.update_sequence
            ));
        }
        if acknowledged(update) != live.acknowledged {
            found.push(format!(
                "acknowledges {:?}, not {:?}",
                acknowledged(update),
                live.acknowledged
            ));
        }
        live.sequence = update.update_sequence;
        for detail in found {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription}'s \
                 {operation} {detail}"
            ));
        }
    }

    /// Judges a claim of `key`'s subscription that recorded `cursor` for a
    /// page ending at `page.0`, of `page.1` entries in `page.2` bytes,
    /// after which its primitive counted `charged` consumed.
    fn claim(
        &mut self,
        key: &Subscription,
        cursor: Acknowledged,
        page: Acknowledged,
        charged: (u64, u64),
    ) {
        let (page_last, page_headers, page_bytes) = page;
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} claimed a \
                 page, but the subscriber never opened it"
            ));
            return;
        };
        live.consumed = (
            live.consumed.0.saturating_add(page_headers),
            live.consumed.1.saturating_add(page_bytes),
        );
        let (through, headers, bytes) = cursor;
        if through != page_last {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} recorded \
                 cursor {through:?} for a page ending at {page_last:?}"
            ));
        }
        if charged != live.consumed {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription}'s credit \
                 counts {charged:?} consumed after a claim, but its pages consumed {:?}",
                live.consumed
            ));
        }
        if (headers, bytes) != live.consumed {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} recorded \
                 cursor {through:?} at {headers} headers and {bytes} bytes, but its pages \
                 consumed {:?}",
                live.consumed
            ));
        }
        live.claimed.insert(cursor);
    }

    /// Judges an acceptance of `key`'s subscription through `cursor`.
    fn accept(&mut self, key: &Subscription, cursor: Acknowledged) {
        let (through, ..) = cursor;
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} accepted \
                 through {through:?}, but the subscriber never opened it"
            ));
            return;
        };
        if !live.claimed.contains(&cursor) {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} accepted \
                 {cursor:?}, which no claim recorded"
            ));
        }
        if !live.staged.contains(&through) {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} accepted \
                 through {through:?}, which no staged page ends at"
            ));
        }
        if live
            .accepted
            .is_some_and(|accepted| through.height < accepted.height)
        {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} accepted \
                 through {through:?}, below its earlier acceptance {:?}",
                live.accepted
            ));
        }
        live.accepted = Some(through);
        live.acknowledged = cursor;
    }
}

/// The cursor `update` acknowledges.
fn acknowledged(update: &SubscribeHeaders) -> Acknowledged {
    let cursor = update.acknowledged;
    (
        Frontier::new(cursor.height, cursor.hash),
        cursor.headers,
        cursor.bytes,
    )
}
