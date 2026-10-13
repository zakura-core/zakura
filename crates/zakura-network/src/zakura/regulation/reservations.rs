//! Response reservations: the only way a response is admitted.
//!
//! A requester reserves a key before it sends a request. Each response frame
//! must claim that key before its payload is decoded. The reservation bounds
//! the response with the same [`ResponseCap`] the responder's `Serve` uses, so
//! a conformant responder can never exceed it.
//!
//! A claim has two outcomes. It succeeds, and the frame goes to the handler.
//! Or it is refused, and the refusal is a protocol violation: no conformant
//! responder could cause it, because only the response's own ending or the
//! connection's end removes a reservation.
//!
//! Whether this node still wants the work is local. [`Reservations::abandon`]
//! records that the scheduler moved on; the reservation stays live, its frames
//! still reach the handler, and the handler may skip their work. They are
//! counted in `zakura.p2p.reservation.unwanted` so the scheduler can be tuned.
//! Want never reaches the peer's record.
//!
//! There is no expiry. A local timeout or cancellation leaves the reservation
//! in place until the peer ends the exchange or the session ends.
//!
//! Every map draws its entries from one node-wide [`ReservationPool`], so the
//! requester's bookkeeping has a node-wide bound. A full pool makes the node
//! wait before its next request; it never acts against a peer.
//!
//! [`Reservations::reserve_fenced`] makes a reservation own its request's
//! [`Exchange`]. The ending's claim ends the exchange. Dropping the map at the
//! session's end drops the rest, and a started one closes the connection.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | A response claims its frames, then its ending | `a_response_claims_its_frames_then_its_ending` |
//! | Each violation refuses, and a refusal changes nothing | `every_violation_refuses` |
//! | Abandoned work still delivers and never refuses | `an_abandoned_reservation_still_delivers_and_never_refuses` |
//! | No time passing removes a reservation | `no_time_passing_removes_a_reservation` |
//! | Reserving is local and bounded | `reserving_is_local_and_bounded` |
//! | The precheck needs a live reservation with room | `the_precheck_needs_a_live_reservation_with_room` |
//! | A refused precheck allocates no decoded body | `a_refused_precheck_allocates_no_decoded_body` |
//! | A full pool waits, with no lost or spurious wakeup and no allocation | `a_full_pool_waits_without_lost_or_spurious_wakeups` |
//! | Racing threads never overcommit the pool | `racing_threads_never_overcommit_the_pool` |
//! | Refusals are exactly the violations, for any sequence | `operation_sequences_refuse_exactly_the_violations` |
//! | The ending ends a fenced exchange; the map's drop closes a started one | `a_fenced_reservation_ends_its_exchange_with_its_ending` |

use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    sync::{Arc, Mutex, OnceLock, PoisonError},
};

use thiserror::Error;

use super::{
    serve::{ending_reserve, ResponseCap},
    slots::SlotBudgetCapacityError,
    Exchange, SlotBudget, SlotPermit,
};
use crate::zakura::{FrameRejection, MessageRole, MessageRule};

#[cfg(test)]
mod tests;

/// The node-wide pool of requester reservation entries.
///
/// [`sizing`](super::sizing) derives its default from the throughput target.
#[derive(Clone, Debug)]
pub(crate) struct ReservationPool {
    entries: SlotBudget,
}

impl ReservationPool {
    /// A pool of `entries` reservations.
    pub(crate) fn new(entries: usize) -> Result<Self, SlotBudgetCapacityError> {
        Ok(Self {
            entries: SlotBudget::new(entries)?,
        })
    }

    /// Wait for an entry in FIFO order. Cancelling the wait takes nothing.
    ///
    /// Hold the entry before publishing the request, then pass it to
    /// [`Reservations::reserve`].
    pub(crate) async fn entry(&self) -> PoolEntry {
        if let Some(entry) = self.try_entry() {
            return entry;
        }
        metrics::counter!("zakura.p2p.reservation.pool_waited").increment(1);
        PoolEntry(self.entries.reserve().await)
    }

    /// Take an entry without waiting.
    pub(crate) fn try_entry(&self) -> Option<PoolEntry> {
        self.entries.try_reserve().map(PoolEntry)
    }

    /// Entries in use across the node.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.entries.reserved()
    }
}

/// One entry of the node pool. It frees when its reservation ends.
#[derive(Debug)]
#[must_use = "dropping a pool entry frees it"]
pub(crate) struct PoolEntry(SlotPermit);

/// Why a local request could not reserve its response. The peer is not at
/// fault.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ReserveRefused {
    /// The session already holds its maximum of live reservations.
    #[error("the session holds its maximum of live reservations")]
    AtCapacity,
    /// The key already has a live reservation.
    #[error("the key already has a live reservation")]
    KeyLive,
    /// The message type is not a request row.
    #[error("message type {message_type} is not a request row")]
    NotARequest {
        /// The message type.
        message_type: u16,
    },
}

/// Why a response frame could not claim a reservation.
///
/// Every refusal is a protocol violation: `Disconnect`.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ClaimRefused {
    /// No live reservation has this key: the response is unsolicited, or it
    /// follows its exchange's ending.
    #[error("no live reservation for message type {message_type}")]
    Unsolicited {
        /// The frame's message type.
        message_type: u16,
    },
    /// The row answers another request than the reservation's.
    #[error("message type {message_type} answers another request")]
    WrongRequest {
        /// The frame's message type.
        message_type: u16,
    },
    /// An ending was claimed as a frame, a frame as an ending, or the row is
    /// not a response.
    #[error("message type {message_type} has the wrong role for this claim")]
    WrongRole {
        /// The frame's message type.
        message_type: u16,
    },
    /// The frame exceeds the reservation's frame budget.
    #[error("the response exceeds its {frames}-frame budget")]
    OverFrames {
        /// Frames the reservation allows before the ending.
        frames: u32,
    },
    /// The frame exceeds the reservation's byte budget.
    #[error("the response exceeds its {bytes}-byte budget")]
    OverBytes {
        /// Payload bytes the reservation allows.
        bytes: u64,
    },
}

impl ClaimRefused {
    /// Stable metric and trace label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unsolicited { .. } => "unsolicited",
            Self::WrongRequest { .. } => "wrong_request",
            Self::WrongRole { .. } => "wrong_role",
            Self::OverFrames { .. } => "over_frames",
            Self::OverBytes { .. } => "over_bytes",
        }
    }
}

/// A claimed frame. Deliver it to the handler.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Claimed {
    /// This node abandoned the work; the handler may skip it.
    pub(crate) abandoned: bool,
}

/// A consumed reservation, with the counts that ending validation needs.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Ended {
    /// Frames before the ending.
    pub(crate) frames: u32,
    /// Payload bytes, the ending included.
    pub(crate) bytes: u64,
    /// This node abandoned the work; the handler may skip it.
    pub(crate) abandoned: bool,
}

#[derive(Debug)]
struct Reservation {
    request: u16,
    cap: ResponseCap,
    frames: u32,
    bytes: u64,
    abandoned: bool,
    _entry: PoolEntry,
    /// The request's exchange, if the reactor fences its writers. Dropping a
    /// started exchange without its ending closes the connection.
    exchange: Option<Exchange>,
}

impl Reservation {
    fn remaining_bytes(&self) -> u64 {
        self.cap.bytes - self.bytes
    }
}

/// Live response reservations for one session, keyed by `K`.
#[derive(Debug)]
pub(crate) struct Reservations<K> {
    rules: &'static [MessageRule],
    cap: usize,
    live: HashMap<K, Reservation>,
    /// Remaining payload bytes of the live reservations, as a multiset per
    /// request type, so the precheck finds the largest in `O(log n)`.
    remaining: HashMap<u16, BTreeMap<u64, usize>>,
}

impl<K: Eq + Hash> Reservations<K> {
    /// An empty map over the family `rules` that holds at most `cap` live
    /// reservations.
    ///
    /// `cap` is the smallest of the request row's `max_in_flight`, the peer's
    /// advertised limit, and the requester's own window.
    pub(crate) fn new(rules: &'static [MessageRule], cap: usize) -> Self {
        Self {
            rules,
            cap,
            live: HashMap::new(),
            remaining: HashMap::new(),
        }
    }

    /// Reserve `key` for one response to a `request_type` request, bounded by
    /// `cap`. Call it before the request's first byte is written.
    pub(crate) fn reserve(
        &mut self,
        key: K,
        request_type: u16,
        cap: ResponseCap,
        entry: PoolEntry,
    ) -> Result<(), ReserveRefused> {
        self.insert(key, request_type, cap, entry, None)
    }

    /// Reserve as [`Self::reserve`] does, and keep `exchange` until the
    /// response's ending ends it.
    ///
    /// The session owns this map, so the session's end drops every exchange
    /// still live. One whose request's first byte was written then closes the
    /// connection.
    pub(crate) fn reserve_fenced(
        &mut self,
        key: K,
        request_type: u16,
        cap: ResponseCap,
        entry: PoolEntry,
        exchange: Exchange,
    ) -> Result<(), ReserveRefused> {
        self.insert(key, request_type, cap, entry, Some(exchange))
    }

    fn insert(
        &mut self,
        key: K,
        request_type: u16,
        cap: ResponseCap,
        entry: PoolEntry,
        exchange: Option<Exchange>,
    ) -> Result<(), ReserveRefused> {
        if !matches!(
            MessageRule::find(self.rules, request_type).map(|row| row.role),
            Some(MessageRole::Request { .. })
        ) {
            return Err(ReserveRefused::NotARequest {
                message_type: request_type,
            });
        }
        if self.live.contains_key(&key) {
            return Err(ReserveRefused::KeyLive);
        }
        if self.live.len() >= self.cap {
            return Err(ReserveRefused::AtCapacity);
        }
        let reservation = Reservation {
            request: request_type,
            // The responder raises the cap to hold its largest ending; so does
            // the requester, so both sides bound the same response.
            cap: ResponseCap {
                bytes: cap.bytes.max(ending_reserve(request_type, self.rules)),
                ..cap
            },
            frames: 0,
            bytes: 0,
            abandoned: false,
            _entry: entry,
            exchange,
        };
        self.track(request_type, reservation.remaining_bytes());
        self.live.insert(key, reservation);
        Ok(())
    }

    /// Remove a reservation whose request was never written.
    ///
    /// Use it only when the send failed locally before the first byte. A
    /// written request keeps its reservation until its ending.
    pub(crate) fn retract(&mut self, key: &K) -> bool {
        let Some(reservation) = self.live.remove(key) else {
            return false;
        };
        self.untrack(reservation.request, reservation.remaining_bytes());
        true
    }

    /// Record that this node no longer wants `key`'s work.
    ///
    /// The reservation stays live and its frames still reach the handler.
    pub(crate) fn abandon(&mut self, key: &K) -> bool {
        self.live
            .get_mut(key)
            .map(|reservation| reservation.abandoned = true)
            .is_some()
    }

    /// Check a response header before its payload is read.
    ///
    /// A live reservation must answer the row's request with at least
    /// `payload_len` bytes left.
    pub(crate) fn precheck(
        &self,
        message_type: u16,
        payload_len: usize,
    ) -> Result<(), ClaimRefused> {
        let Some(MessageRole::Response { request, .. }) =
            MessageRule::find(self.rules, message_type).map(|row| row.role)
        else {
            return Err(ClaimRefused::WrongRole { message_type });
        };
        let Some((&largest, _)) = self
            .remaining
            .get(&request)
            .and_then(|remaining| remaining.last_key_value())
        else {
            return Err(ClaimRefused::Unsolicited { message_type });
        };
        // Widening usize to u64 is lossless on supported targets.
        if payload_len as u64 > largest {
            return Err(ClaimRefused::OverBytes { bytes: largest });
        }
        Ok(())
    }

    /// Charge a frame that does not end the exchange, before decode.
    pub(crate) fn claim_frame(
        &mut self,
        key: &K,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Claimed, ClaimRefused> {
        let (request, before, after, abandoned) =
            self.charge(key, message_type, payload_len, false)?;
        self.untrack(request, before);
        self.track(request, after);
        Ok(Claimed { abandoned })
    }

    /// Consume the reservation with its ending, before decode.
    pub(crate) fn claim_end(
        &mut self,
        key: &K,
        message_type: u16,
        payload_len: usize,
    ) -> Result<Ended, ClaimRefused> {
        let (request, before, _, _) = self.charge(key, message_type, payload_len, true)?;
        self.untrack(request, before);
        let mut reservation = self
            .live
            .remove(key)
            .expect("charge found this reservation and nothing removed it since");
        if let Some(exchange) = &mut reservation.exchange {
            exchange.end();
        }
        Ok(Ended {
            frames: reservation.frames,
            bytes: reservation.bytes,
            abandoned: reservation.abandoned,
        })
    }

    /// Check and charge one frame. Returns the request type, the remaining
    /// bytes before and after, and whether the work was abandoned. A refused
    /// charge changes nothing.
    fn charge(
        &mut self,
        key: &K,
        message_type: u16,
        payload_len: usize,
        ends: bool,
    ) -> Result<(u16, u64, u64, bool), ClaimRefused> {
        let refused = |refused: ClaimRefused| {
            metrics::counter!("zakura.p2p.reservation.refused", "reason" => refused.label())
                .increment(1);
            refused
        };
        let Some(MessageRole::Response {
            request,
            ends_exchange,
        }) = MessageRule::find(self.rules, message_type).map(|row| row.role)
        else {
            return Err(refused(ClaimRefused::WrongRole { message_type }));
        };
        let reservation = self
            .live
            .get_mut(key)
            .ok_or(refused(ClaimRefused::Unsolicited { message_type }))?;
        if request != reservation.request {
            return Err(refused(ClaimRefused::WrongRequest { message_type }));
        }
        if ends_exchange != ends {
            return Err(refused(ClaimRefused::WrongRole { message_type }));
        }
        if !ends && reservation.frames >= reservation.cap.frames {
            return Err(refused(ClaimRefused::OverFrames {
                frames: reservation.cap.frames,
            }));
        }
        let before = reservation.remaining_bytes();
        // Widening usize to u64 is lossless on supported targets.
        let len = payload_len as u64;
        if len > before {
            return Err(refused(ClaimRefused::OverBytes {
                bytes: reservation.cap.bytes,
            }));
        }
        reservation.bytes += len;
        if !ends {
            reservation.frames += 1;
        }
        if reservation.abandoned {
            metrics::counter!(
                "zakura.p2p.reservation.unwanted",
                "message_type" => message_type.to_string(),
            )
            .increment(1);
        }
        Ok((request, before, before - len, reservation.abandoned))
    }

    fn track(&mut self, request: u16, remaining: u64) {
        *self
            .remaining
            .entry(request)
            .or_default()
            .entry(remaining)
            .or_default() += 1;
    }

    fn untrack(&mut self, request: u16, remaining: u64) {
        let by_request = self
            .remaining
            .get_mut(&request)
            .expect("every live reservation is tracked under its request type");
        let count = by_request
            .get_mut(&remaining)
            .expect("every live reservation is tracked under its remaining bytes");
        *count -= 1;
        if *count == 0 {
            by_request.remove(&remaining);
        }
        if by_request.is_empty() {
            self.remaining.remove(&request);
        }
    }

    /// Live reservations.
    pub(crate) fn len(&self) -> usize {
        self.live.len()
    }

    /// Whether no reservation is live.
    pub(crate) fn is_empty(&self) -> bool {
        self.live.is_empty()
    }
}

/// A header check that a stream's reader runs before it reads a response
/// payload.
///
/// A service attaches one to a stream with
/// [`FramedRecv::attach_precheck`](crate::zakura::FramedRecv::attach_precheck).
/// The reader calls it for every response row, after the frame filter and
/// before it allocates the payload. A refusal disconnects the peer.
pub(crate) trait ResponsePrecheck: std::fmt::Debug + Send + Sync {
    /// Check a response header.
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection>;
}

/// A session's reservations, shared between the reactor and its readers.
#[derive(Debug)]
pub(crate) struct SharedReservations<K>(Arc<Mutex<Reservations<K>>>);

impl<K> Clone for SharedReservations<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: Eq + Hash> SharedReservations<K> {
    pub(crate) fn new(reservations: Reservations<K>) -> Self {
        Self(Arc::new(Mutex::new(reservations)))
    }

    /// Lock the reservations. No holder panics, so the lock is never poisoned.
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Reservations<K>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K: Eq + Hash + std::fmt::Debug + Send + 'static> ResponsePrecheck for SharedReservations<K> {
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        self.lock()
            .precheck(message_type, payload_len)
            .map_err(|refused| match refused {
                ClaimRefused::OverBytes { bytes } => FrameRejection::AboveReservation { bytes },
                _ => FrameRejection::Unsolicited,
            })
    }
}

/// The prechecks of one stream, chosen by the row that each response answers.
///
/// A stream may carry responses to a request, which [`SharedReservations`]
/// check, and pages of a subscription, which
/// [`SharedSubscriptions`](super::SharedSubscriptions) check. A response to a
/// row with no precheck here is unsolicited.
#[derive(Debug)]
pub(crate) struct PrecheckByRequest {
    rules: &'static [MessageRule],
    prechecks: Vec<(u16, Arc<dyn ResponsePrecheck>)>,
}

impl PrecheckByRequest {
    /// Prechecks for the family `rules`, each for the request or
    /// subscription row it names.
    pub(crate) fn new(
        rules: &'static [MessageRule],
        prechecks: Vec<(u16, Arc<dyn ResponsePrecheck>)>,
    ) -> Self {
        Self { rules, prechecks }
    }
}

impl ResponsePrecheck for PrecheckByRequest {
    fn check(&self, message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        let Some(MessageRole::Response { request, .. }) =
            MessageRule::find(self.rules, message_type).map(|row| row.role)
        else {
            return Err(FrameRejection::Unsolicited);
        };
        self.prechecks
            .iter()
            .find(|(row, _)| *row == request)
            .ok_or(FrameRejection::Unsolicited)?
            .1
            .check(message_type, payload_len)
    }
}

/// The precheck a stream's reader and its receiver share.
///
/// A transport receiver pauses ingress until the service attaches a precheck
/// or starts receiving without one. The choice applies to the first header.
#[derive(Clone, Debug, Default)]
pub(crate) struct PrecheckSlot(Arc<PrecheckSetup>);

#[derive(Debug, Default)]
struct PrecheckSetup {
    choice: OnceLock<Option<Arc<dyn ResponsePrecheck>>>,
    gated: std::sync::atomic::AtomicBool,
    ready: tokio_util::sync::CancellationToken,
}

impl PrecheckSlot {
    /// Pause ingress before spawning the transport reader.
    pub(crate) fn pause(&self) {
        self.0
            .gated
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Wait for the service to choose its header check.
    pub(crate) async fn ready(&self) {
        if self.0.gated.load(std::sync::atomic::Ordering::Acquire) {
            self.0.ready.cancelled().await;
        }
    }

    /// Attach `precheck`. Returns false if the service already chose a check.
    pub(crate) fn attach(&self, precheck: Arc<dyn ResponsePrecheck>) -> bool {
        let attached = self.0.choice.set(Some(precheck)).is_ok();
        self.0.ready.cancel();
        attached
    }

    /// Start receiving with the attached check, or with row checks alone.
    pub(crate) fn start(&self) {
        self.0.choice.get_or_init(|| None);
        self.0.ready.cancel();
    }

    /// The attached precheck.
    pub(crate) fn get(&self) -> Option<&dyn ResponsePrecheck> {
        self.0.choice.get().and_then(|choice| choice.as_deref())
    }
}
