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
//! - **Updates.** Each `Grant` and `Close` names its `Open`'s target and
//!   schema, its sequence is one more than the subscription's previous
//!   update's, and it acknowledges the subscription's latest
//!   `CursorsAccepted`, or the `Open`'s start before any.
//! - **Acceptance.** Each `CursorsAccepted` reaches the last header of a
//!   page staged on that subscription, never a cursor no page staged, and
//!   never one below an earlier acceptance.
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
    acknowledged: Frontier,
    /// The last header of each page staged on the subscription.
    staged: HashSet<Frontier>,
    /// The latest accepted cursor.
    accepted: Option<Frontier>,
}

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
                match kind {
                    CreditKind::Open => {
                        self.live.insert(
                            key,
                            Live {
                                target: update.initial_target_tip_hash,
                                schema: update.tree_aux_schema,
                                sequence: update.update_sequence,
                                acknowledged: acknowledged(update),
                                staged: HashSet::new(),
                                accepted: None,
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
                update,
            } => {
                let key = (peer.clone(), *session_id, update.subscription_id);
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
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                if stale_handler {
                    if let Some(live) = self.live.get_mut(&key) {
                        live.accepted = Some(*through);
                        live.acknowledged = *through;
                    }
                    return false;
                }
                self.accept(&key, *through);
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

    /// Judges an acceptance of `key`'s subscription through `through`.
    fn accept(&mut self, key: &Subscription, through: Frontier) {
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: session {session} subscription {subscription} accepted \
                 through {through:?}, but the subscriber never opened it"
            ));
            return;
        };
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
        live.acknowledged = through;
    }
}

/// The frontier `update` acknowledges.
fn acknowledged(update: &SubscribeHeaders) -> Frontier {
    Frontier::new(update.acknowledged.height, update.acknowledged.hash)
}
