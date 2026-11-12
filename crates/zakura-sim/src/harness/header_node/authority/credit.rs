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
//!   of every claimed page's entries and bytes. A page's bytes are derived
//!   here from the page layout, its entry count and the subscription's
//!   schema, and the bytes the claim reports charging must be those. The
//!   pages' sum never exceeds what the built updates granted.
//! - **Primitive totals.** Every credit fact carries what the
//!   subscription's primitive counts after it, granted and consumed. That
//!   must be every built update's credit and every claimed page's, as
//!   counted here. An `Open` starts at its first locator with nothing
//!   consumed, at the height storage gave that header: the first entry of
//!   a locator read, or a VCT repair context's locator. An `Open` whose
//!   first locator neither gave breaks too, as does one whose sequence
//!   isn't 0. The encoder already refuses such an `Open`, and the service
//!   panics on it, so only a unit test reaches that check.
//! - **Acceptance.** The primitive stores exactly the cursor the reactor
//!   asked it to accept. Each `CursorsAccepted` names a claimed cursor whole,
//!   reaches the last header of a page staged on that subscription, never
//!   a cursor no page staged, and never goes below an earlier acceptance.
//!   It comes on the reactor's task, in the handler of an apply call's
//!   completion, under
//!   `CauseSet { Completion { call } }`, for an apply its reactor
//!   dispatched for that subscription's session and request, and names the
//!   last page that subscription staged before the apply's dispatch, as
//!   production accepts only the round's last staged cursor and only once
//!   its apply succeeded. That apply's storage operation, found by its
//!   `Submitted { call }`, must have completed `Applied`. Storage completes
//!   an apply before its reply reaches the reactor, and each drain feeds
//!   storage's facts before the reactor's, so the result is known by the
//!   drain that sees the acceptance, which judges it. An acceptance whose
//!   apply reported no storage result by then breaks too, so a run that
//!   ends on that drain can't leave it unjudged.
//! - **Operations.** An `Open`'s and a `Grant`'s credit comes in an update
//!   of that operation, and a built `Close` is a `Close` that adds no
//!   credit.
//!
//! Credit and acceptance in a stale call's handler are
//! `stale-completion-inert`'s effects, so they aren't judged here. They
//! still move the subscription's state, so later updates are judged
//! against what production did.
//!
//! The model restates page sizes from the page layout, which a unit test
//! checks against encoded pages.

use std::collections::{BTreeMap, HashMap, HashSet};

use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{CreditKind, CreditTotals, PortCall, ReactorCause, ReactorEvent, SubscribeHeaders},
    v9::Operation,
    AuxSchema, ZakuraPeerId, TREE_AUX_SCHEMA_V1_BYTES,
};

use super::{
    AuthorityFinding, AuthorityRule, AuthoritySubject, CursorFact, Subscription, UpdateKind,
};
use crate::observe::{StorageFact, StorageResult};
use crate::peer::{ENTRY_BYTES, PAGE_FIXED_BYTES};

/// The most bytes one update may grant, as the `SubscribeHeaders` row
/// caps its byte window.
const MAX_CREDIT_BYTES: u64 = 8 * 1024 * 1024;

/// Bytes of a one-header Regtest page under `schema`.
pub(super) fn smallest_page(schema: AuxSchema) -> u64 {
    page_size(1, schema)
}

/// Bytes of a Regtest page of `entries` headers under `schema`: the page's
/// fixed fields, then per header its entry and, under V1, its auxiliary
/// record.
pub(super) fn page_size(entries: u64, schema: AuxSchema) -> u64 {
    let aux = match schema {
        AuxSchema::None => 0,
        AuxSchema::V1 => TREE_AUX_SCHEMA_V1_BYTES,
    };
    // Widening usize to u64 is lossless on supported targets.
    let entry = (ENTRY_BYTES + aux) as u64;
    (PAGE_FIXED_BYTES as u64).saturating_add(entries.saturating_mul(entry))
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
    /// The headers and bytes every built update granted.
    granted: (u64, u64),
    /// The last page staged on the subscription, by its last header.
    last_staged: Option<Frontier>,
}

/// An apply call the reactor dispatched: the subscription its owner names,
/// and the last page that subscription staged before the dispatch.
#[derive(Debug)]
struct Apply {
    subscription: Subscription,
    last_staged: Option<Frontier>,
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
    /// The height of each locator read's first entry, by hash.
    read_heights: HashMap<block::Hash, block::Height>,
    /// The storage call whose completion the reactor is handling, from its
    /// latest `CauseSet`.
    completion: Option<u64>,
    /// Each apply call the reactor dispatched, by call number.
    applies: HashMap<u64, Apply>,
    /// The reactor call each storage operation was submitted for, by
    /// operation ID.
    operations: HashMap<u64, u64>,
    /// Whether each reactor call's storage operation completed `Applied`,
    /// by call number.
    applied: HashMap<u64, bool>,
    /// Acceptances since the last drain, each under its apply call, for the
    /// drain to judge against the call's storage result.
    awaiting: Vec<(Subscription, u64, Frontier)>,
    breaks: Vec<AuthorityFinding>,
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
                totals,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let subject = AuthoritySubject::cursor(&key, CursorFact::Claim, *through);
                let page = (*page_last, *page_headers, *page_bytes);
                self.claim(&key, &subject, (*through, *headers, *bytes), page);
                self.totals("claim", &key, &subject, *totals);
                true
            }
            // Only the reactor's task holds an apply call's completion, so
            // an acceptance from any other task is outside one.
            ReactorEvent::CursorsAccepted {
                peer,
                session_id,
                subscription,
                through,
                ..
            } if !on_reactor => {
                let key = (peer.clone(), *session_id, *subscription);
                self.breaks.push(credit(
                    &AuthoritySubject::cursor(&key, CursorFact::Acceptance, *through),
                    format!(
                        "session {session_id} subscription {subscription} accepted through \
                         {through:?} off the reactor's task, outside an apply call's completion"
                    ),
                ));
                true
            }
            _ if !on_reactor => false,
            ReactorEvent::CauseSet { cause } => {
                self.completion = match cause {
                    ReactorCause::Completion { call } => Some(*call),
                    _ => None,
                };
                false
            }
            ReactorEvent::Dispatched {
                call,
                op: PortCall::Apply { peer, owner, .. },
            } => {
                let subscription = (peer.clone(), owner.session_id(), owner.request_id().get());
                let last_staged = self
                    .live
                    .get(&subscription)
                    .and_then(|live| live.last_staged);
                self.applies.insert(
                    *call,
                    Apply {
                        subscription,
                        last_staged,
                    },
                );
                false
            }
            ReactorEvent::CreditIssued {
                kind,
                peer,
                session_id,
                subscription,
                objects,
                bytes,
                update,
                totals,
                ..
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let subject = AuthoritySubject::update(
                    &key,
                    match kind {
                        CreditKind::Open => UpdateKind::Open,
                        CreditKind::Grant => UpdateKind::Grant,
                    },
                    update.update_sequence,
                );
                if !stale_handler {
                    self.issue(&key, &subject, *objects, *bytes, update);
                }
                let added = (
                    u64::from(update.added_credit.objects),
                    u64::from(update.added_credit.bytes),
                );
                let operation = match kind {
                    CreditKind::Open => Operation::Open,
                    CreditKind::Grant => Operation::Grant,
                };
                if update.operation != operation {
                    self.breaks.push(credit(
                        &subject,
                        format!(
                            "session {session_id} subscription {subscription}'s {kind:?} credit \
                             came in a {:?} update",
                            update.operation
                        ),
                    ));
                }
                match kind {
                    CreditKind::Open => {
                        if update.subscription_id != *subscription {
                            self.breaks.push(credit(
                                &subject,
                                format!(
                                    "session {session_id} subscription {subscription}'s Open \
                                     names subscription {}",
                                    update.subscription_id
                                ),
                            ));
                        }
                        self.start(&key, &subject, update);
                        self.live.insert(
                            key.clone(),
                            Live {
                                target: update.initial_target_tip_hash,
                                schema: update.tree_aux_schema,
                                sequence: update.update_sequence,
                                acknowledged: acknowledged(update),
                                staged: HashSet::new(),
                                accepted: None,
                                consumed: (0, 0),
                                claimed: HashSet::new(),
                                granted: (0, 0),
                                last_staged: None,
                            },
                        );
                    }
                    CreditKind::Grant if stale_handler => {
                        if let Some(live) = self.live.get_mut(&key) {
                            live.sequence = update.update_sequence;
                        }
                    }
                    CreditKind::Grant => self.update("Grant", &key, &subject, update),
                }
                if let Some(live) = self.live.get_mut(&key) {
                    live.granted = (
                        live.granted.0.saturating_add(added.0),
                        live.granted.1.saturating_add(added.1),
                    );
                }
                self.totals("credit", &key, &subject, *totals);
                !stale_handler
            }
            ReactorEvent::CloseBuilt {
                peer,
                session_id,
                subscription,
                update,
                totals,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let subject =
                    AuthoritySubject::update(&key, UpdateKind::Close, update.update_sequence);
                self.totals("Close", &key, &subject, *totals);
                if update.operation != Operation::Close
                    || update.added_credit.objects != 0
                    || update.added_credit.bytes != 0
                {
                    self.breaks.push(credit(
                        &subject,
                        format!(
                            "session {session_id} subscription {subscription}'s built Close is a \
                             {:?} adding {:?}",
                            update.operation, update.added_credit
                        ),
                    ));
                }
                self.update("Close", &key, &subject, update);
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
                    let page_last = Frontier::new(
                        block::Height(ancestor.height.0.saturating_add(count)),
                        *last,
                    );
                    live.staged.insert(page_last);
                    live.last_staged = Some(page_last);
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
                stored,
                totals,
            } => {
                let key = (peer.clone(), *session_id, *subscription);
                let subject = AuthoritySubject::cursor(&key, CursorFact::Acceptance, *through);
                let cursor = (*through, *headers, *bytes);
                if *stored != cursor {
                    self.breaks.push(credit(
                        &subject,
                        format!(
                            "session {session_id} subscription {subscription}'s primitive stored \
                             {stored:?} accepted, not the {cursor:?} asked for"
                        ),
                    ));
                }
                // The next update acknowledges what the primitive stored.
                let cursor = *stored;
                self.totals("acceptance", &key, &subject, *totals);
                if stale_handler {
                    if let Some(live) = self.live.get_mut(&key) {
                        live.accepted = Some(cursor.0);
                        live.acknowledged = cursor;
                    }
                    return false;
                }
                self.accept(&key, &subject, cursor);
                true
            }
            _ => false,
        }
    }

    /// The breaks found since the last call, judging first each acceptance
    /// since the last call against its apply's storage result.
    pub(super) fn take_breaks(&mut self) -> Vec<AuthorityFinding> {
        self.judge_awaited();
        std::mem::take(&mut self.breaks)
    }

    /// Breaks for each acceptance since the last drain whose apply call's
    /// storage operation completed other than `Applied`, or reported no
    /// result.
    fn judge_awaited(&mut self) {
        for (key, call, through) in std::mem::take(&mut self.awaiting) {
            let (_, session, subscription) = &key;
            let why = match self.applied.get(&call) {
                None => "reported no storage result",
                Some(true) => continue,
                Some(false) => "didn't succeed",
            };
            self.breaks.push(credit(
                &AuthoritySubject::cursor(&key, CursorFact::Acceptance, through),
                format!(
                    "session {session} subscription {subscription} accepted through \
                     {through:?} in apply call {call}'s completion, whose apply {why}"
                ),
            ));
        }
    }

    /// Judges the credit `update` adds to `key`'s subscription against its
    /// peer's unbacked reservations and its schema's byte bound, then marks
    /// those reservations backed.
    fn issue(
        &mut self,
        key: &Subscription,
        subject: &AuthoritySubject,
        objects: u32,
        bytes: u32,
        update: &SubscribeHeaders,
    ) {
        let (peer, session, subscription) = key;
        let reserved: usize = self
            .unbacked
            .remove(peer)
            .map_or(0, |reservations| reservations.values().sum());
        // Widening u32 to usize is lossless on supported targets.
        if objects as usize > reserved {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} was granted \
                     {objects} headers, but its peer's unbacked reservations hold {reserved}"
                ),
            ));
        }
        let limit =
            (u64::from(objects) * smallest_page(update.tree_aux_schema)).min(MAX_CREDIT_BYTES);
        if u64::from(bytes) > limit {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} was granted \
                     {bytes} bytes for {objects} headers under {:?}, above {limit}",
                    update.tree_aux_schema
                ),
            ));
        }
    }

    /// Judges a built `Grant` or `Close`, named `operation`, against `key`'s
    /// subscription, then records its sequence. Its breaks name `subject`.
    fn update(
        &mut self,
        operation: &str,
        key: &Subscription,
        subject: &AuthoritySubject,
        update: &SubscribeHeaders,
    ) {
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(credit(
                subject,
                format!(
                    "a {operation} names session {session} subscription \
                     {subscription}, which the subscriber never opened"
                ),
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
            self.breaks.push(credit(
                subject,
                format!("session {session} subscription {subscription}'s {operation} {detail}"),
            ));
        }
    }

    /// Judges a claim of `key`'s subscription, `subject`, that recorded
    /// `cursor` for a page ending at `page.0`, of `page.1` entries, charged
    /// `page.2` bytes.
    fn claim(
        &mut self,
        key: &Subscription,
        subject: &AuthoritySubject,
        cursor: Acknowledged,
        page: Acknowledged,
    ) {
        let (page_last, page_headers, charged) = page;
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} claimed a \
                     page, but the subscriber never opened it"
                ),
            ));
            return;
        };
        let page_bytes = page_size(page_headers, live.schema);
        if charged != page_bytes {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} charged \
                     {charged} bytes for a page of {page_headers} headers under {:?}, which takes \
                     {page_bytes}",
                    live.schema
                ),
            ));
        }
        live.consumed = (
            live.consumed.0.saturating_add(page_headers),
            live.consumed.1.saturating_add(page_bytes),
        );
        if live.consumed.0 > live.granted.0 || live.consumed.1 > live.granted.1 {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription}'s pages \
                     consumed {:?} headers and bytes, above the {:?} its updates granted",
                    live.consumed, live.granted
                ),
            ));
        }
        let (through, headers, bytes) = cursor;
        if through != page_last {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} recorded \
                     cursor {through:?} for a page ending at {page_last:?}"
                ),
            ));
        }
        if (headers, bytes) != live.consumed {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} recorded \
                     cursor {through:?} at {headers} headers and {bytes} bytes, but its pages \
                     consumed {:?}",
                    live.consumed
                ),
            ));
        }
        live.claimed.insert(cursor);
    }

    /// Notes storage's `fact`: a locator read gives its first entry's
    /// height, and a resolved VCT repair context its locator's.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        match fact {
            StorageFact::Submitted {
                id,
                call: Some(call),
                ..
            } => {
                self.operations.insert(*id, *call);
            }
            // Kept for every call: a drain feeds storage's facts before the
            // reactor's, so an apply's result can arrive before its dispatch.
            StorageFact::Completed { id, result, .. } => {
                if let Some(call) = self.operations.get(id) {
                    self.applied
                        .insert(*call, matches!(result, StorageResult::Applied));
                }
            }
            _ => {}
        }
        let entry = match fact {
            StorageFact::Completed {
                result: StorageResult::Locator { tip, .. },
                ..
            } => tip,
            StorageFact::Completed {
                result:
                    StorageResult::VctResolved {
                        locator: Some(locator),
                    },
                ..
            } => locator,
            _ => return,
        };
        if let Ok(bytes) = <[u8; 32]>::try_from(decode_hex(&entry.hash).as_slice()) {
            self.note_read(block::Hash(bytes), block::Height(entry.height));
        }
    }

    /// Notes that storage gave `hash` at `height` as a locator's entry.
    pub(super) fn note_read(&mut self, hash: block::Hash, height: block::Height) {
        self.read_heights.insert(hash, height);
    }

    /// Judges the primitive `totals` a `fact` of `key`'s subscription,
    /// `subject`, reported against every built update's credit and every
    /// claimed page's.
    fn totals(
        &mut self,
        fact: &str,
        key: &Subscription,
        subject: &AuthoritySubject,
        totals: CreditTotals,
    ) {
        let (_, session, subscription) = key;
        let Some(live) = self.live.get(key) else {
            return;
        };
        let reported = (
            (totals.granted_headers, totals.granted_bytes),
            (totals.consumed_headers, totals.consumed_bytes),
        );
        if reported != (live.granted, live.consumed) {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription}'s primitive \
                     counts {totals:?} after a {fact}, but its updates granted {:?} and its pages \
                     consumed {:?}",
                    live.granted, live.consumed
                ),
            ));
        }
    }

    /// Judges an `Open`'s start: sequence 0, and its first locator, at the
    /// height the node's locator read gave it, with nothing consumed. Its
    /// breaks name `subject`.
    fn start(&mut self, key: &Subscription, subject: &AuthoritySubject, update: &SubscribeHeaders) {
        let (_, session, subscription) = key;
        if update.update_sequence != 0 {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription}'s Open has \
                     sequence {}, not 0",
                    update.update_sequence
                ),
            ));
        }
        let start = update.acknowledged;
        let first = update.locator_hashes.first().copied();
        let height = first.and_then(|first| self.read_heights.get(&first).copied());
        if first != Some(start.hash)
            || start.headers != 0
            || start.bytes != 0
            || height != Some(start.height)
        {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription}'s Open \
                     starts at {start:?}, not its first locator {first:?} at {height:?} with \
                     nothing consumed"
                ),
            ));
        }
    }

    /// Judges an acceptance of `key`'s subscription through `cursor`, which
    /// `subject` names.
    fn accept(&mut self, key: &Subscription, subject: &AuthoritySubject, cursor: Acknowledged) {
        let (through, ..) = cursor;
        let (_, session, subscription) = key;
        let Some(live) = self.live.get_mut(key) else {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     through {through:?}, but the subscriber never opened it"
                ),
            ));
            return;
        };
        if !live.claimed.contains(&cursor) {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     {cursor:?}, which no claim recorded"
                ),
            ));
        }
        if !live.staged.contains(&through) {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     through {through:?}, which no staged page ends at"
                ),
            ));
        }
        if live
            .accepted
            .is_some_and(|accepted| through.height < accepted.height)
        {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     through {through:?}, below its earlier acceptance {:?}",
                    live.accepted
                ),
            ));
        }
        live.accepted = Some(through);
        live.acknowledged = cursor;
        let last_staged = live.last_staged;
        // Production accepts only in an apply's completion, through the
        // round's last staged cursor, once that apply succeeded.
        let apply = self.completion.and_then(|call| {
            self.applies
                .get(&call)
                .map(|apply| (call, &apply.subscription, apply.last_staged))
        });
        let Some((call, applied, before)) = apply else {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     through {through:?} outside an apply call's completion"
                ),
            ));
            return;
        };
        if applied != key {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted through \
                     {through:?} in apply call {call}'s completion, an apply for {applied:?}"
                ),
            ));
            return;
        }
        if before != Some(through) {
            self.breaks.push(credit(
                subject,
                format!(
                    "session {session} subscription {subscription} accepted \
                     through {through:?} in apply call {call}'s completion, not its round's last \
                     page staged before that apply, {before:?} (now {last_staged:?})"
                ),
            ));
        }
        self.awaiting.push((key.clone(), call, through));
    }
}

/// A credit-rule break by `subject`, as `detail` says.
fn credit(subject: &AuthoritySubject, detail: String) -> AuthorityFinding {
    AuthorityFinding::new(AuthorityRule::Credit, subject.clone(), detail)
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

/// The bytes of the lowercase hex string `text`, or none if it isn't one.
fn decode_hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .filter_map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
        .collect()
}
