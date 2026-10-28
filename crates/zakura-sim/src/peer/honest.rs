//! `Honest`: the scripted peer that follows header sync's rules (M5-D).
//!
//! It holds a fixture chain and serves it: it sends `Status` when a header
//! stream starts and on production's refresh cadence, answers each of the
//! node's `SubscribeHeaders` with pages within the credit its models hold,
//! and ends a subscription the node closes with `SubscriptionClosed`. It can
//! also subscribe to the node's publisher, once, from a height on its own
//! chain, granting credit back as pages arrive or only when a `Control`
//! step releases a grant. Every frame it sends is labelled by its
//! [`StreamModels`] before the write, and every frame it reads from the node
//! updates them.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    v9::{
        Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation, RegtestMessage,
        Status, SubscribeHeaders,
    },
    AuxSchema, Credit, HeaderEntry, MessageRole, ZakuraHeaderSyncConfig, DEFAULT_HS_RANGE,
    MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE,
};
use zakura_rt::time::Instant;
use zakura_storage::header::{adapter::EnginePort, fixture::HeaderFixture};

use super::{Outgoing, PeerAction, StreamModels};
use crate::harness::header_node::{preload, Chain};

/// The model session of a peer's header stream: each stream has models of
/// its own.
const SESSION: u64 = 0;

/// How long an honest peer with nothing live stays quiet before it's ready
/// for an action, so the node has handled the last frame it sent. A frame's
/// datagrams are due within the link's delay, and time moves only once no
/// task is ready.
pub(super) const SETTLE: Duration = Duration::from_secs(1);

/// What an honest peer holds and does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HonestSpec {
    /// The headers it holds on fixture branch 0 from genesis, which it
    /// advertises and serves.
    pub chain: u32,
    /// If set, it subscribes to the node's publisher on its first header
    /// stream.
    #[serde(default)]
    pub subscribe: Option<SubscribeSpec>,
}

/// An honest peer's subscription to the node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeSpec {
    /// The height of its one locator, its start cursor, on its own chain.
    pub start: u32,
    /// The credit its `Open` grants.
    pub open: CreditSpec,
    /// When it grants more.
    pub grants: GrantPolicy,
}

/// Header and byte credit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreditSpec {
    /// Headers.
    pub objects: u32,
    /// Payload bytes.
    pub bytes: u32,
}

impl From<CreditSpec> for Credit {
    fn from(credit: CreditSpec) -> Credit {
        Credit {
            objects: credit.objects,
            bytes: credit.bytes,
        }
    }
}

/// When an honest subscriber grants more credit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum GrantPolicy {
    /// After each page, it grants back what the page spent, so its window
    /// stays the `Open`'s.
    Immediate,
    /// Only when the `Plan` picks its next `grant` action, which is offered
    /// once `quiet_ms` have passed since the last page. Each grants `grant`.
    Held {
        /// The credit each released grant adds.
        grant: CreditSpec,
        /// How long after a page the peer waits before a grant is offered.
        quiet_ms: u64,
    },
}

/// An honest peer's chain: its headers by height and its `Status`, built
/// before the run.
#[derive(Debug)]
pub struct HonestChain {
    /// The anchor, genesis.
    anchor: Frontier,
    /// The header at height `h` is `headers[h - 1]`.
    pub(super) headers: Vec<Arc<block::Header>>,
    /// Each header's height by its hash's bytes, the anchor's included.
    heights: BTreeMap<[u8; 32], u32>,
    /// The `Status` it advertises.
    pub(super) status: Status,
}

impl HonestChain {
    /// `chain` headers on `fixture`'s branch 0. Its `Status` is read from an
    /// engine holding the same chain, with production's page caps for a
    /// Regtest frame and no auxiliary schema. Call it outside a run, where
    /// fixture headers validate on any clock.
    pub fn new(fixture: &HeaderFixture, chain: u32) -> Self {
        let anchor = fixture.anchor();
        let headers = fixture.extend(anchor, chain, 0);
        let mut heights = BTreeMap::from([(anchor.hash.0, 0)]);
        heights.extend(
            (1..)
                .zip(&headers)
                .map(|(height, header)| (header.hash().0, height)),
        );
        let engine = EnginePort::new(fixture);
        preload(&engine, fixture, &Chain::main(chain));
        let snapshot = engine.snapshot();
        // A Regtest frame's payload cap is under 2 MiB plus the header, so it
        // fits u32, and so does the protocol cap.
        let frame_payload = RegtestMessage::LAYOUT[0].frame_cap.saturating_sub(8);
        let max_message_bytes = frame_payload.min(MAX_HS_MESSAGE_BYTES as u32);
        let status = Status {
            work_anchor_height: snapshot.frontiers.finalized.height,
            work_anchor_hash: snapshot.frontiers.finalized.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: snapshot.header_best_score.suffix_work.as_u256(),
            oldest_retained_height: snapshot.oldest_retained_height,
            max_headers_per_response: DEFAULT_HS_RANGE,
            max_subscriptions: 1,
            max_message_bytes,
            tree_aux_schema_mask: 0,
        };
        HonestChain {
            anchor,
            headers,
            heights,
            status,
        }
    }

    /// The height of the header `hash` on this chain, the anchor's
    /// included.
    pub(super) fn height(&self, hash: block::Hash) -> Option<u32> {
        self.heights.get(&hash.0).copied()
    }

    /// The header at `height`, as a frontier.
    pub(super) fn at(&self, height: u32) -> Frontier {
        match height.checked_sub(1) {
            None => self.anchor,
            Some(index) => {
                let header = &self.headers[usize::try_from(index).expect("u32 fits usize")];
                Frontier::new(block::Height(height), header.hash())
            }
        }
    }
}

/// A subscription the node opened on this peer.
#[derive(Debug)]
pub(super) struct Serving {
    pub(super) target: block::Hash,
    target_height: u32,
    /// The height of the last header sent, or of the page parent to come.
    position: u32,
}

/// This peer's subscription to the node.
#[derive(Debug)]
pub(super) struct Subscribed {
    pub(super) id: u64,
    target: block::Hash,
    sequence: u32,
    /// The last page's cursor, or the start.
    pub(super) last: Cursor,
    /// When the last page arrived, if one has.
    pub(super) paged_at: Option<Instant>,
    /// Whether a held grant waits for release: set by each page, cleared by
    /// its release, so one page readies one grant.
    held: bool,
    /// Credit granted so far, the `Open`'s included, as (objects, bytes).
    granted: (u64, u64),
    pub(super) close_sent: bool,
    pub(super) ended: bool,
}

/// The honest script's state on one peer, across its header streams.
pub(crate) struct Honest {
    pub(super) spec: HonestSpec,
    pub(super) chain: Arc<HonestChain>,
    /// The current header stream's models, labels and counts.
    pub(super) models: StreamModels,
    pub(super) serving: BTreeMap<u64, Serving>,
    /// Subscriptions the node opened that have ended, on any stream.
    served: u64,
    pub(super) subscribed: Option<Subscribed>,
    /// When the next `Status` is due.
    status_due: Instant,
    refresh: Duration,
    /// When it last sent or read a frame.
    pub(super) active: Instant,
    /// Whether it sends `Status`, so the node may subscribe. An adversary
    /// may stay silent.
    announce: bool,
    /// Whether it serves the node's subscriptions with pages. An adversary
    /// may hold each with its credit unspent.
    serve: bool,
    /// The node's latest `Status` on the current stream.
    pub(super) node_status: Option<Status>,
    /// The last ending it sent for a subscription the node opened.
    pub(super) last_ending: Option<HeadersOutcome>,
}

impl Honest {
    /// The script for `spec` on `chain`, sending `Status` only if
    /// `announce`, and pages only if `serve`.
    pub(crate) fn with(
        spec: HonestSpec,
        chain: Arc<HonestChain>,
        announce: bool,
        serve: bool,
    ) -> Self {
        Honest {
            spec,
            models: StreamModels::new(Instant::now(), chain.clone()),
            chain,
            serving: BTreeMap::new(),
            served: 0,
            subscribed: None,
            status_due: Instant::now(),
            refresh: ZakuraHeaderSyncConfig::default().status_refresh_interval,
            active: Instant::now(),
            announce,
            serve,
            node_status: None,
            last_ending: None,
        }
    }

    /// A header stream started: fresh models, no subscriptions the node
    /// opened on it, and its first `Status` if it announces.
    pub(crate) fn start_stream(&mut self) -> Vec<Outgoing> {
        let now = Instant::now();
        self.models = StreamModels::new(now, self.chain.clone());
        self.serving.clear();
        self.node_status = None;
        self.status_due = now + self.refresh;
        if !self.announce {
            return Vec::new();
        }
        vec![self.send(RegtestMessage::Status(self.chain.status.clone()))]
    }

    /// The next time [`Honest::tick`] has work: the next `Status` refresh.
    pub(crate) fn deadline(&self) -> Instant {
        self.status_due
    }

    /// Sends the refresh `Status` once it's due.
    pub(crate) fn tick(&mut self) -> Vec<Outgoing> {
        let now = Instant::now();
        if now < self.status_due {
            return Vec::new();
        }
        self.status_due = now + self.refresh;
        if !self.announce {
            return Vec::new();
        }
        vec![self.send(RegtestMessage::Status(self.chain.status.clone()))]
    }

    /// When a held grant may next be offered, if one is waiting: never once
    /// the subscription ended or this peer sent its `Close`, when
    /// [`Honest::release_grant`] has nothing to release.
    pub(crate) fn grant_opens(&self) -> Option<Instant> {
        let GrantPolicy::Held { quiet_ms, .. } = self.spec.subscribe.as_ref()?.grants else {
            return None;
        };
        let subscribed = self
            .subscribed
            .as_ref()
            .filter(|sub| sub.held && !sub.ended && !sub.close_sent)?;
        Some(subscribed.paged_at? + Duration::from_millis(quiet_ms))
    }

    /// When the peer may next become ready for `next`, its next scripted
    /// action: for a `grant`, the end of a held grant's quiet time, and for
    /// any other, [`SETTLE`] after its last frame once every subscription in
    /// either direction has ended, its own included, after the node opened
    /// at least one. `None` while that can't happen yet.
    pub(crate) fn wake(&self, next: Option<PeerAction>) -> Option<Instant> {
        if next == Some(PeerAction::Grant) {
            return self.grant_opens();
        }
        let own_ended =
            self.spec.subscribe.is_none() || self.subscribed.as_ref().is_some_and(|sub| sub.ended);
        (own_ended && self.serving.is_empty() && self.served > 0).then(|| self.active + SETTLE)
    }

    /// Labels and encodes `message` with the stream's models.
    pub(super) fn send(&mut self, message: RegtestMessage) -> Outgoing {
        self.send_crafted(super::Crafted::of(message))
    }

    /// Labels and encodes a crafted `frame` with the stream's models. A
    /// conformant ending is the last one sent.
    pub(super) fn send_crafted(&mut self, frame: super::Crafted) -> Outgoing {
        self.active = Instant::now();
        let ending = match &frame.message {
            Message::HeadersOutcome(outcome) => Some(*outcome),
            _ => None,
        };
        let outgoing = self.models.send_crafted(frame);
        if outgoing.conformant() {
            self.last_ending = ending.or(self.last_ending);
        }
        outgoing
    }

    /// The frames that answer the node's `message`.
    pub(crate) fn receive(
        &mut self,
        message: &RegtestMessage,
        payload_len: usize,
    ) -> Vec<Outgoing> {
        self.active = Instant::now();
        self.models.receive(message, payload_len);
        match message {
            Message::Status(status) => {
                self.node_status = Some(status.clone());
                self.subscribe(status)
            }
            Message::SubscribeHeaders(update) => match update.operation {
                Operation::Open => self.open(update),
                Operation::Grant => self.pump(),
                Operation::Close => self.close(update.subscription_id),
            },
            Message::Headers(page) => self.page(page, payload_len),
            Message::HeadersOutcome(outcome) => {
                if let Some(sub) = self
                    .subscribed
                    .as_mut()
                    .filter(|sub| sub.id == outcome.subscription_id)
                {
                    sub.ended = true;
                }
                Vec::new()
            }
        }
    }

    /// Opens this peer's subscription to the node's selected tip in
    /// `status`, once.
    fn subscribe(&mut self, status: &Status) -> Vec<Outgoing> {
        let Some(spec) = self.spec.subscribe.clone() else {
            return Vec::new();
        };
        if self.subscribed.is_some() {
            return Vec::new();
        }
        let start = self.chain.at(spec.start);
        let cursor = Cursor {
            height: start.height,
            hash: start.hash,
            headers: 0,
            bytes: 0,
        };
        self.subscribed = Some(Subscribed {
            id: 1,
            target: status.selected_tip_hash,
            sequence: 0,
            last: cursor,
            paged_at: None,
            held: false,
            granted: (spec.open.objects.into(), spec.open.bytes.into()),
            close_sent: false,
            ended: false,
        });
        vec![
            self.send(RegtestMessage::SubscribeHeaders(SubscribeHeaders {
                operation: Operation::Open,
                subscription_id: 1,
                update_sequence: 0,
                initial_target_tip_hash: status.selected_tip_hash,
                acknowledged: cursor,
                locator_hashes: vec![start.hash],
                added_credit: spec.open.into(),
                tree_aux_schema: AuxSchema::None,
            })),
        ]
    }

    /// The update after a page of this peer's subscription: `Close` once it
    /// reached its target, an immediate grant of what it spent, or nothing
    /// until a held grant is released.
    fn page(&mut self, page: &Headers, payload_len: usize) -> Vec<Outgoing> {
        let Some(sub) = self
            .subscribed
            .as_mut()
            .filter(|sub| sub.id == page.subscription_id && !sub.ended)
        else {
            return Vec::new();
        };
        let count = u32::try_from(page.entries.len()).expect("a page holds at most 4,000 headers");
        let last = page.entries.last().expect("a decoded page has a header");
        sub.last = Cursor {
            height: block::Height(page.common_ancestor_height.0 + count),
            hash: last.header.hash(),
            headers: sub.last.headers + u64::from(count),
            bytes: sub.last.bytes + payload_len as u64,
        };
        sub.paged_at = Some(Instant::now());
        if sub.close_sent {
            return Vec::new();
        }
        sub.held = true;
        let added = if page.reaches_initial_target {
            sub.close_sent = true;
            None
        } else {
            match self.spec.subscribe.as_ref().map(|spec| spec.grants) {
                Some(GrantPolicy::Immediate) => Some(Credit {
                    objects: count,
                    bytes: u32::try_from(payload_len).expect("a page fits u32"),
                }),
                _ => return Vec::new(),
            }
        };
        let update = self.update(added);
        vec![self.send(update)]
    }

    /// Releases the grant its last page held, at most what keeps the
    /// subscription's window, granted less acknowledged, within the row's
    /// credit in each unit. When nothing is held, the release sends
    /// nothing: after a release with no page since, or once this peer has
    /// sent its `Close` or its subscription has ended. That's the rule, not
    /// [`Honest::grant_opens`]'s readiness, so a grant the `Plan` queued
    /// while one was held can't over-grant or fail the run when it's taken
    /// later. Only a peer with no held grant policy can't release.
    pub(crate) fn release_grant(&mut self) -> Result<Vec<Outgoing>, String> {
        let Some(GrantPolicy::Held { grant, .. }) =
            self.spec.subscribe.as_ref().map(|spec| spec.grants)
        else {
            return Err("can't grant without a held grant policy".to_owned());
        };
        let Some(sub) = self
            .subscribed
            .as_mut()
            .filter(|sub| sub.held && !sub.ended && !sub.close_sent)
        else {
            return Ok(Vec::new());
        };
        sub.held = false;
        let window = self.models.subscriptions.row();
        let MessageRole::Subscription { credit: row, .. } = window.role else {
            unreachable!("the models follow the subscription row");
        };
        let room = |limit: u32, granted: u64, acknowledged: u64| {
            u64::from(limit).saturating_sub(granted.saturating_sub(acknowledged))
        };
        let fit = |want: u32, room: u64| u32::try_from(u64::from(want).min(room)).unwrap_or(want);
        let added = Credit {
            objects: fit(
                grant.objects,
                room(row.objects, sub.granted.0, sub.last.headers),
            ),
            bytes: fit(grant.bytes, room(row.bytes, sub.granted.1, sub.last.bytes)),
        };
        // A grant adds a header, or at least a smallest page of bytes, 263,
        // which fits u32.
        let smallest = (super::PAGE_FIXED_BYTES + super::ENTRY_BYTES) as u32;
        if added.objects == 0 && added.bytes < smallest {
            return Ok(Vec::new());
        }
        let update = self.update(Some(added));
        Ok(vec![self.send(update)])
    }

    /// The next update on this peer's subscription, acknowledging its last
    /// page: a `Grant` of `added`, or `Close` for `None`.
    pub(super) fn update(&mut self, added: Option<Credit>) -> RegtestMessage {
        let sub = self
            .subscribed
            .as_mut()
            .expect("an update follows the Open");
        sub.sequence += 1;
        if let Some(added) = added {
            sub.granted.0 += u64::from(added.objects);
            sub.granted.1 += u64::from(added.bytes);
        }
        RegtestMessage::SubscribeHeaders(SubscribeHeaders {
            operation: if added.is_some() {
                Operation::Grant
            } else {
                Operation::Close
            },
            subscription_id: sub.id,
            update_sequence: sub.sequence,
            initial_target_tip_hash: sub.target,
            acknowledged: sub.last,
            locator_hashes: Vec::new(),
            added_credit: added.unwrap_or(Credit {
                objects: 0,
                bytes: 0,
            }),
            tree_aux_schema: AuxSchema::None,
        })
    }

    /// Starts serving the node's `Open`, or ends it at once with the outcome
    /// legal before any page if its target or locators aren't on this
    /// peer's chain.
    fn open(&mut self, update: &SubscribeHeaders) -> Vec<Outgoing> {
        let id = update.subscription_id;
        let ending = |outcome| {
            RegtestMessage::HeadersOutcome(HeadersOutcome {
                subscription_id: id,
                initial_target_tip_hash: update.initial_target_tip_hash,
                outcome,
            })
        };
        let Some(&target_height) = self.chain.heights.get(&update.initial_target_tip_hash.0) else {
            self.served += 1;
            return vec![self.send(ending(HeadersOutcomeCode::TargetNotRetained))];
        };
        // Locators are highest first, and every header here is on the one
        // chain, so the first known one at or below the target is the
        // intersection.
        let position = update.locator_hashes.iter().find_map(|hash| {
            self.chain
                .heights
                .get(&hash.0)
                .copied()
                .filter(|height| *height <= target_height)
        });
        let Some(position) = position else {
            self.served += 1;
            return vec![self.send(ending(HeadersOutcomeCode::NoLocatorIntersection))];
        };
        self.serving.insert(
            id,
            Serving {
                target: update.initial_target_tip_hash,
                target_height,
                position,
            },
        );
        self.pump()
    }

    /// Ends subscription `id` after the node's `Close`.
    fn close(&mut self, id: u64) -> Vec<Outgoing> {
        let Some(serving) = self.serving.remove(&id) else {
            return Vec::new();
        };
        self.served += 1;
        vec![self.send(RegtestMessage::HeadersOutcome(HeadersOutcome {
            subscription_id: id,
            initial_target_tip_hash: serving.target,
            outcome: HeadersOutcomeCode::SubscriptionClosed,
        }))]
    }

    /// Every page the live subscriptions' unspent credit allows, toward
    /// their targets, in subscription order. Nothing if it doesn't serve.
    fn pump(&mut self) -> Vec<Outgoing> {
        let mut out = Vec::new();
        if !self.serve {
            return out;
        }
        let ids: Vec<u64> = self.serving.keys().copied().collect();
        for id in ids {
            while let Some(page) = self.next_page(id) {
                out.push(self.send(page));
            }
        }
        out
    }

    /// Subscription `id`'s next page, sized as production's publisher sizes
    /// one: [`Self::page_room`]'s headers. `None` if no page fits or the
    /// target is reached.
    fn next_page(&mut self, id: u64) -> Option<RegtestMessage> {
        let count = self.page_room(id)?;
        if count == 0 {
            return None;
        }
        self.page_at(id, count)
    }

    /// The headers subscription `id`'s next page may carry: at most the
    /// advertised caps, the unspent credit the models hold in each unit,
    /// and the headers left to the target. `None` if it isn't live.
    pub(super) fn page_room(&self, id: u64) -> Option<u32> {
        let unspent = self.models.subscriptions.unspent(SESSION, id)?;
        let serving = self.serving.get(&id)?;
        let left = serving.target_height.saturating_sub(serving.position);
        let entry = super::ENTRY_BYTES as u64;
        let fit = |bytes: u64| bytes.saturating_sub(super::PAGE_FIXED_BYTES as u64) / entry;
        let status = &self.chain.status;
        let count = u64::from(status.max_headers_per_response.min(MAX_HS_RANGE))
            .min(unspent.objects)
            .min(fit(u64::from(status.max_message_bytes)))
            .min(fit(unspent.bytes))
            .min(u64::from(left));
        Some(u32::try_from(count).expect("the count is at most MAX_HS_RANGE"))
    }

    /// The headers left to subscription `id`'s target.
    pub(super) fn left(&self, id: u64) -> Option<u32> {
        let serving = self.serving.get(&id)?;
        Some(serving.target_height.saturating_sub(serving.position))
    }

    /// Subscription `id`'s next page of `count` headers from where it
    /// stands, whatever its credit, which moves its position past them.
    /// `None` if it isn't served or the chain ends first.
    pub(super) fn page_at(&mut self, id: u64, count: u32) -> Option<RegtestMessage> {
        let serving = self.serving.get_mut(&id)?;
        if serving.position + count > u32::try_from(self.chain.headers.len()).ok()? {
            return None;
        }
        let parent = self.chain.at(serving.position);
        let entries = (serving.position + 1..=serving.position + count)
            .map(|height| HeaderEntry {
                header: self.chain.headers[usize::try_from(height - 1).expect("u32 fits usize")]
                    .clone(),
                body_size: 0,
                tree_aux: None,
            })
            .collect();
        serving.position += count;
        Some(RegtestMessage::Headers(Headers {
            subscription_id: id,
            initial_target_tip_hash: serving.target,
            common_ancestor_height: parent.height,
            common_ancestor_hash: parent.hash,
            reaches_initial_target: serving.position == serving.target_height,
            tree_aux_schema: AuxSchema::None,
            entries,
        }))
    }
}

#[cfg(test)]
mod tests {
    use zakura_network::zakura::{
        v9::{Headers, Message, Operation, RegtestMessage},
        wire_codec::decode_frame,
        AuxSchema, Frame, HeaderEntry,
    };
    use zakura_storage::header::fixture::HeaderFixture;

    use super::{
        super::{ENTRY_BYTES, PAGE_FIXED_BYTES},
        *,
    };

    /// An honest peer on 50 headers, subscribed with `open` and holding
    /// `grant`, to a node on `node` headers, and the node's chain.
    fn subscribed(node: u32, open: CreditSpec, grant: CreditSpec) -> (Honest, HonestChain) {
        let fixture = HeaderFixture::regtest();
        let node = HonestChain::new(&fixture, node);
        let spec = HonestSpec {
            chain: 50,
            subscribe: Some(SubscribeSpec {
                start: 50,
                open,
                grants: GrantPolicy::Held { grant, quiet_ms: 0 },
            }),
        };
        let mut honest = Honest::with(spec, Arc::new(HonestChain::new(&fixture, 50)), true, true);
        honest.start_stream();
        let sent = honest.receive(&Message::Status(node.status.clone()), 122);
        assert_eq!(sent.len(), 1, "the peer subscribes");
        (honest, node)
    }

    /// The node's page of header 51, which reaches its target if the node
    /// holds 51.
    fn page_51(node: &HonestChain) -> RegtestMessage {
        let parent = node.at(50);
        Message::Headers(Headers {
            subscription_id: 1,
            initial_target_tip_hash: node.status.selected_tip_hash,
            common_ancestor_height: parent.height,
            common_ancestor_hash: parent.hash,
            reaches_initial_target: node.status.selected_tip_height.0 == 51,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: node.headers[50].clone(),
                body_size: 0,
                tree_aux: None,
            }],
        })
    }

    /// The credit of the one update in `sent`.
    fn granted(sent: &[Outgoing]) -> (u32, u32) {
        assert_eq!(sent.len(), 1);
        let frame = Frame {
            message_type: u16::from_le_bytes([sent[0].bytes[0], sent[0].bytes[1]]),
            flags: 0,
            payload: sent[0].bytes[8..].to_vec(),
        };
        let Message::SubscribeHeaders(update) =
            decode_frame::<RegtestMessage>(&frame).expect("the update decodes")
        else {
            panic!("an update");
        };
        assert_eq!(update.operation, Operation::Grant);
        (update.added_credit.objects, update.added_credit.bytes)
    }

    /// A grant released after the page that reaches the target, as a grant
    /// the `Plan` queued before that page arrived would be, sends nothing:
    /// the peer has sent its `Close`, so nothing is held.
    #[test]
    fn a_grant_released_after_close_sends_nothing() {
        let credit = CreditSpec {
            objects: 1,
            bytes: 263,
        };
        let (mut honest, node) = subscribed(51, credit, credit);
        let close = honest.receive(&page_51(&node), 263);
        assert_eq!(close.len(), 1, "the target page makes the peer close");
        assert_eq!(honest.grant_opens(), None, "no grant is offered");
        let released = honest.release_grant().expect("a held policy releases");
        assert!(released.is_empty(), "nothing is held after Close");
    }

    /// One page holds one grant, and its release keeps the window within
    /// the row's credit: with the full 4,000-header window open and one
    /// header spent, a two-header grant adds one. A second release before
    /// another page sends nothing, so it can't push the window past the
    /// row's 4,000.
    #[test]
    fn one_page_holds_one_grant_within_the_window() {
        let open = CreditSpec {
            objects: 4_000,
            bytes: 4_000 * 263,
        };
        let grant = CreditSpec {
            objects: 2,
            bytes: 526,
        };
        let (mut honest, node) = subscribed(60, open, grant);
        assert!(honest.receive(&page_51(&node), 263).is_empty());
        assert!(honest.grant_opens().is_some(), "the page holds a grant");
        let sent = honest.release_grant().expect("a held policy releases");
        assert_eq!(granted(&sent), (1, 526));
        assert_eq!(honest.grant_opens(), None, "the grant was taken");
        let again = honest.release_grant().expect("a held policy releases");
        assert!(again.is_empty(), "nothing is held until the next page");
    }

    /// The page sizes the honest publisher uses are the table's: its
    /// smallest `headers` page is one plain entry, 263 bytes on Regtest.
    #[test]
    fn page_sizes_match_the_headers_row() {
        let headers = RegtestMessage::ROWS
            .iter()
            .find(|row| row.name == "headers")
            .expect("header sync has a headers row");
        assert_eq!(headers.payload.min(), PAGE_FIXED_BYTES + ENTRY_BYTES);
        assert_eq!((PAGE_FIXED_BYTES, ENTRY_BYTES), (82, 181));
    }
}
