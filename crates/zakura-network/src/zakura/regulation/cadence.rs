//! Cadence: the rate a row declares, observed by the receiver and obeyed by
//! the sender.
//!
//! Both sides read the same [`Cadence`] from the row. [`CadenceSender`]
//! leaves at least `send_interval` between two messages of a row and keeps
//! at most one of them unwritten. [`CadenceBuckets`] keeps one bucket for each
//! `(layout, message_type)` of a connection.
//!
//! The layout validator sizes buckets for a connection-wide outage. A single
//! stream can stall longer while sibling streams keep the connection alive.
//! Exhaustion therefore measures a burst, not a protocol violation, and never
//! closes. Local read pauses add tokens. On exhaustion, the reader of a
//! stream whose every row is paced ([`parks_on_exhaustion`]) waits for the
//! row's next token, so QUIC flow control slows the sender. Any other reader
//! records exhaustion and forwards the frame, and its service meters the
//! row's work itself.
//!
//! A [`MessageRole::Metered`] row is charged by payload bytes instead, in a
//! [`ByteBucket`] that belongs to the connection as the cadence buckets do.
//! Its depth already covers every byte a conformant sender can have buffered,
//! so an overrun is a violation and closes the connection.
//!
//! Rows without a cadence or a byte rate charge nothing. Commitments bound
//! requests, and reservations bound responses.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | A full bucket admits its capacity, then exhausts | `a_full_bucket_admits_its_capacity_back_to_back_then_exhausts` |
//! | One token per refill interval, up to capacity | `one_token_returns_per_refill_interval_up_to_capacity` |
//! | A sender at the refill rate never drifts | `a_sender_at_exactly_the_refill_rate_never_drifts` |
//! | Rows without a cadence charge nothing | `rows_without_a_cadence_charge_nothing` |
//! | A sender faster than the refill exhausts, only after its capacity | `a_sender_faster_than_the_refill_exhausts_only_after_its_capacity` |
//! | The bursts after an outage or a local pause are admitted | `the_burst_after_the_longest_outage_is_admitted`, `the_burst_after_a_local_pause_of_any_length_is_admitted` |
//! | Bounded delays do not exhaust a conformant sender | `a_conformant_sender_is_never_exhausted` |
//! | The sender paces rows and keeps one unwritten frame per row | `the_sender_waits_for_its_interval_and_keeps_only_the_latest_value`, `a_blocked_writer_holds_at_most_one_frame_per_row` |
//! | A row reports pacing, an unwritten frame, and a full queue apart | `a_row_reports_why_it_did_not_send` |
//! | A local minimum interval only slows a row | `a_minimum_interval_only_slows_a_row` |
//! | Charges at explicit instants match an interval-stepping bucket | `the_ledger_matches_an_interval_stepping_bucket` |
//! | A byte bucket pays its depth back to back, then refills at its rate | `a_byte_bucket_pays_its_depth_then_refills_at_its_rate` |
//! | A metered row overruns only past the receiver's depth | `a_metered_row_overruns_only_past_the_receivers_depth` |
//! | A conformant sender that stalls and resumes is never cut off | `a_sender_that_stalls_and_resumes_never_overruns_the_receiver` |
//! | Reopening sessions on one connection buys no bytes | `reopening_sessions_on_one_connection_buys_no_bytes` |
//! | A charge reports a low bucket below a quarter of its capacity | `a_charge_reports_a_low_bucket_below_a_quarter_of_capacity` |
//! | A parked frame takes the token due exactly one refill interval after the last | `a_parked_frame_takes_the_next_token_at_its_exact_instant` |
//! | Only a stream whose every row is paced parks its reader | `only_streams_whose_every_row_is_paced_park` |

use std::{sync::Arc, time::Duration};

use zakura_rt::{collections::HashMap, sync::Mutex, time::Instant};

use crate::zakura::{
    wire_codec::{encode_frame, WireMessage},
    ByteRate, Cadence, Clock, FrameGuard, FramedSend, MessageRole, MessageRule, RealClock,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
#[cfg(all(test, not(zakura_sim)))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{replay_cadence_bytes, CadenceReplaySummary};

/// The result of charging one frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CadenceCharge {
    /// The row declares no cadence.
    Exempt,
    /// The bucket had a token.
    Admit,
    /// The bucket is empty; transport buffering can cause this.
    Exhausted,
    /// A metered row's frame is past the connection's byte bucket. A
    /// conformant sender never does this.
    Overrun,
}

/// Payload bytes earned at a fixed rate, up to a depth.
///
/// The sender pays one for each metered frame before handing it on, and the
/// receiver charges one for each metered frame it reads. Both start full.
#[derive(Clone, Debug)]
pub(crate) struct ByteBucket {
    depth: u64,
    bytes_per_second: u64,
    tokens: u64,
    last_refill: Instant,
}

impl ByteBucket {
    /// A sender's bucket for `rate`: the opening allowance at the row's rate.
    pub(crate) fn sender(rate: ByteRate, now: Instant) -> Self {
        // Widening u32 to u64 is lossless.
        Self::new(
            rate.opening_allowance as u64,
            rate.bytes_per_second as u64,
            now,
        )
    }

    /// A receiver's bucket for `rate` on a connection whose stream receive
    /// window is `receive_window`. See [`ByteRate::receiver_depth`].
    pub(crate) fn receiver(rate: ByteRate, receive_window: u64, now: Instant) -> Self {
        Self::new(
            rate.receiver_depth(receive_window),
            rate.receiver_bytes_per_second(),
            now,
        )
    }

    fn new(depth: u64, bytes_per_second: u64, now: Instant) -> Self {
        Self {
            depth,
            bytes_per_second,
            tokens: depth,
            last_refill: now,
        }
    }

    /// Add the bytes that elapsed time earned, up to the depth.
    ///
    /// `last_refill` advances only by the time the earned bytes took, so the
    /// remainder carries over and the rate never drifts.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill).as_nanos();
        let rate = u128::from(self.bytes_per_second);
        let earned = elapsed.saturating_mul(rate) / 1_000_000_000;
        if earned == 0 {
            return;
        }
        let room = u128::from(self.depth - self.tokens);
        if earned >= room {
            self.tokens = self.depth;
            self.last_refill = now;
            return;
        }
        // `earned` is below `room`, which fits a u64.
        self.tokens += u64::try_from(earned).expect("earned is below the room left in a u64");
        // The time `earned` bytes took is at most `elapsed`, which fits a
        // Duration.
        let took = (earned * 1_000_000_000).div_ceil(rate);
        self.last_refill += Duration::from_nanos(u64::try_from(took).unwrap_or(u64::MAX));
    }

    /// Pay `bytes` at `now` if the bucket holds them.
    pub(crate) fn try_pay(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if self.tokens < bytes {
            return false;
        }
        self.tokens -= bytes;
        true
    }

    /// When the bucket will hold `bytes`, refilled to `now`, or `None` if it
    /// never can because `bytes` is past its depth.
    pub(crate) fn ready_at(&mut self, bytes: u64, now: Instant) -> Option<Instant> {
        self.refill(now);
        if bytes > self.depth || (self.bytes_per_second == 0 && bytes > self.tokens) {
            return None;
        }
        let missing = u128::from(bytes.saturating_sub(self.tokens));
        if missing == 0 {
            return Some(now);
        }
        let wait = (missing * 1_000_000_000).div_ceil(u128::from(self.bytes_per_second));
        Some(self.last_refill + Duration::from_nanos(u64::try_from(wait).unwrap_or(u64::MAX)))
    }

    /// The bytes the bucket holds, as of its last refill.
    #[cfg(test)]
    pub(crate) fn tokens(&self) -> u64 {
        self.tokens
    }
}

/// When a [`MeteredSender`] may send its next frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum MeteredReady {
    /// Now.
    Now,
    /// Once the rate pays for the frame, at this time.
    At(Instant),
    /// Once its last frame is written, which no clock decides.
    Unwritten,
}

/// Sends a metered row's frames no faster than its byte rate allows: the
/// sender's half of [`MessageRole::Metered`].
///
/// It pays the connection's [`ByteBucket`] for each frame before handing it
/// on, so the sessions that share a connection share its allowance and a
/// reopened session buys no bytes. It keeps at most one of its frames
/// unwritten, so the bytes it paid for and the transport has not written
/// stay within the row's sender allowance.
#[derive(Debug)]
pub(crate) struct MeteredSender {
    bucket: Arc<Mutex<ByteBucket>>,
    unwritten: zakura_rt::sync::Weak<()>,
}

impl MeteredSender {
    /// A sender that pays `bucket`, its connection's.
    pub(crate) fn new(bucket: Arc<Mutex<ByteBucket>>) -> Self {
        Self {
            bucket,
            unwritten: zakura_rt::sync::Weak::new(),
        }
    }

    /// When a frame of `bytes` payload bytes may go. A frame past the
    /// bucket's depth is never paid for: the row's layout check keeps every
    /// frame within it.
    pub(crate) fn ready(&self, bytes: u64, now: Instant) -> MeteredReady {
        if self.unwritten.strong_count() > 0 {
            return MeteredReady::Unwritten;
        }
        let mut bucket = self.bucket.lock().expect("a bucket is never poisoned");
        match bucket.ready_at(bytes, now) {
            Some(at) if at > now => MeteredReady::At(at),
            _ => MeteredReady::Now,
        }
    }

    /// Pays for a frame of `bytes` payload bytes and returns the guard the
    /// frame holds until the transport writes it, or `None` if it may not go
    /// now. See [`Self::ready`].
    pub(crate) fn pay(&mut self, bytes: u64, now: Instant) -> Option<FrameGuard> {
        if self.unwritten.strong_count() > 0 {
            return None;
        }
        let mut bucket = self.bucket.lock().expect("a bucket is never poisoned");
        if !bucket.try_pay(bytes, now) {
            return None;
        }
        let written = Arc::new(());
        self.unwritten = zakura_rt::sync::downgrade(&written);
        Some(FrameGuard::new(written))
    }
}

/// A bucket key. Message types are unique only within a layout, so the key
/// includes the layout's primary stream kind.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct CadenceKey {
    layout: u16,
    message_type: u16,
}

#[derive(Clone, Debug)]
pub(crate) struct Bucket {
    cadence: Cadence,
    tokens: u32,
    last_refill: Instant,
    /// Local read-pause time not yet converted into tokens.
    paused: Duration,
}

impl Bucket {
    pub(crate) fn new(cadence: Cadence, now: Instant) -> Self {
        Self {
            cadence,
            tokens: cadence.capacity,
            last_refill: now,
            paused: Duration::ZERO,
        }
    }

    /// Add the tokens that elapsed time earned, up to `capacity`.
    ///
    /// `last_refill` advances by whole intervals, so the remainder carries
    /// over and a sender at exactly the refill rate never drifts.
    pub(crate) fn refill(&mut self, now: Instant) {
        let interval = self.cadence.refill_interval;
        let elapsed = now.saturating_duration_since(self.last_refill);
        let earned = elapsed.as_nanos() / interval.as_nanos();
        if earned == 0 {
            return;
        }
        let earned = u32::try_from(earned).unwrap_or(u32::MAX);
        self.tokens = self.tokens.max(
            self.tokens
                .saturating_add(earned)
                .min(self.cadence.capacity),
        );
        // `earned` intervals fit in `elapsed`, so the product fits a Duration.
        self.last_refill += interval.saturating_mul(earned);
    }

    /// Takes one token after the caller refills the bucket.
    pub(crate) fn take(&mut self) -> bool {
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    /// Returns one token without exceeding the declared capacity.
    pub(crate) fn refund(&mut self) {
        self.tokens = self.tokens.saturating_add(1).min(self.cadence.capacity);
    }

    /// Whether the bucket retains no debt after its latest refill.
    pub(crate) fn is_full(&self) -> bool {
        self.tokens >= self.cadence.capacity
    }

    /// When the bucket earns its next token, as of its latest refill.
    pub(crate) fn next_token_at(&self) -> Instant {
        self.last_refill + self.cadence.refill_interval
    }

    /// Check the bucket's invariants right after a refill at `now`: less than
    /// one interval of elapsed time, and less than one interval of pause
    /// credit, remains unconverted into tokens.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    fn assert_invariants(&self, now: Instant) {
        let interval = self.cadence.refill_interval;
        assert!(self.last_refill <= now, "a bucket refilled in the future");
        assert!(
            now.saturating_duration_since(self.last_refill) < interval,
            "a refill left a whole interval unconverted"
        );
        assert!(
            self.paused < interval,
            "pause credit left a whole interval unconverted"
        );
    }

    /// Credit a local read pause: tokens accrue above `capacity` for it.
    fn credit_pause(&mut self, paused: Duration) {
        self.paused = self.paused.saturating_add(paused);
        let interval = self.cadence.refill_interval;
        let earned = self.paused.as_nanos() / interval.as_nanos();
        if earned == 0 {
            return;
        }
        let earned = u32::try_from(earned).unwrap_or(u32::MAX);
        self.tokens = self.tokens.saturating_add(earned);
        self.paused = self.paused.saturating_sub(interval.saturating_mul(earned));
    }
}

/// What [`CadenceLedger::charge`] did with one frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Charged {
    /// The bucket had a token. `low` reports that fewer than a quarter of the
    /// row's capacity remain.
    Admit { low: bool },
    /// The bucket is empty.
    Exhausted,
}

/// One connection's cadence buckets, charged at explicit instants.
///
/// It reads no clock and emits no metric. [`CadenceBuckets`] supplies the
/// time and reports the facts it returns.
#[derive(Debug, Default)]
struct CadenceLedger {
    buckets: HashMap<CadenceKey, Bucket>,
}

impl CadenceLedger {
    /// Charge one frame of a row with `cadence` at `now`.
    fn charge(&mut self, key: CadenceKey, cadence: Cadence, now: Instant) -> Charged {
        let bucket = self.bucket(key, cadence, now);
        if !bucket.take() {
            return Charged::Exhausted;
        }
        Charged::Admit {
            low: bucket.tokens < cadence.capacity / 4,
        }
    }

    /// Take a token of a row with `cadence` at `now` for a frame that found
    /// the bucket empty, or return when the bucket earns its next token.
    fn take_parked(
        &mut self,
        key: CadenceKey,
        cadence: Cadence,
        now: Instant,
    ) -> Result<(), Instant> {
        let bucket = self.bucket(key, cadence, now);
        if bucket.take() {
            return Ok(());
        }
        Err(bucket.next_token_at())
    }

    /// Credit a local read pause of `paused` to a row with `cadence` at `now`.
    fn credit_pause(&mut self, key: CadenceKey, cadence: Cadence, paused: Duration, now: Instant) {
        self.bucket(key, cadence, now).credit_pause(paused);
    }

    /// The bucket of `key`, created full on first use and refilled to `now`.
    fn bucket(&mut self, key: CadenceKey, cadence: Cadence, now: Instant) -> &mut Bucket {
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| Bucket::new(cadence, now));
        bucket.refill(now);
        bucket
    }
}

/// One connection's cadence buckets.
///
/// Created with the connection. Retiring a session or reopening a stream in
/// the same connection does not refill them. A reconnect gets fresh buckets;
/// connection admission bounds reconnects.
#[derive(Debug)]
pub(crate) struct CadenceBuckets<C: Clock = RealClock> {
    ledger: CadenceLedger,
    /// The byte bucket of each metered row, made full on first use.
    bytes: HashMap<CadenceKey, ByteBucket>,
    /// The stream receive window this node granted on the connection, which
    /// deepens each byte bucket.
    receive_window: u64,
    clock: C,
}

impl Default for CadenceBuckets<RealClock> {
    fn default() -> Self {
        Self::new(RealClock)
    }
}

impl<C: Clock> CadenceBuckets<C> {
    pub(crate) fn new(clock: C) -> Self {
        Self {
            ledger: CadenceLedger::default(),
            bytes: HashMap::new(),
            receive_window: 0,
            clock,
        }
    }

    /// These buckets on a connection whose stream receive window is
    /// `receive_window`, read when the connection starts.
    pub(crate) fn with_receive_window(mut self, receive_window: u64) -> Self {
        self.receive_window = receive_window;
        self
    }

    /// Charge one frame of `rule` on `layout`, whose row the frame filter
    /// already found, as [`Self::charge_frame`] does for a payload of the
    /// row's largest size.
    pub(crate) fn charge(&mut self, layout: u16, rule: &MessageRule) -> CadenceCharge {
        self.charge_frame(layout, rule, rule.payload.max())
    }

    /// Charge one frame of `rule` on `layout`, whose row the frame filter
    /// already found, with a payload of `payload_len` bytes. Only a metered
    /// row reads the length.
    pub(crate) fn charge_frame(
        &mut self,
        layout: u16,
        rule: &MessageRule,
        payload_len: usize,
    ) -> CadenceCharge {
        if let MessageRole::Metered { rate } = rule.role {
            return self.charge_bytes(layout, rule.message_type, rate, payload_len);
        }
        let Some(cadence) = cadence(rule) else {
            return CadenceCharge::Exempt;
        };
        let key = CadenceKey {
            layout,
            message_type: rule.message_type,
        };
        match self.ledger.charge(key, cadence, self.clock.now()) {
            Charged::Exhausted => {
                metrics::counter!(
                    "zakura.p2p.cadence.exhausted",
                    "layout" => layout.to_string(),
                    "message_type" => rule.message_type.to_string(),
                )
                .increment(1);
                CadenceCharge::Exhausted
            }
            Charged::Admit { low } => {
                if low {
                    // Count low buckets as evidence for tuning.
                    metrics::counter!(
                        "zakura.p2p.cadence.low",
                        "layout" => layout.to_string(),
                        "message_type" => rule.message_type.to_string(),
                    )
                    .increment(1);
                }
                CadenceCharge::Admit
            }
        }
    }

    /// Charge `payload_len` bytes of a metered row to its connection bucket.
    fn charge_bytes(
        &mut self,
        layout: u16,
        message_type: u16,
        rate: ByteRate,
        payload_len: usize,
    ) -> CadenceCharge {
        let now = self.clock.now();
        let receive_window = self.receive_window;
        let bucket = self
            .bytes
            .entry(CadenceKey {
                layout,
                message_type,
            })
            .or_insert_with(|| ByteBucket::receiver(rate, receive_window, now));
        // Widening usize to u64 is lossless on supported targets.
        if bucket.try_pay(payload_len as u64, now) {
            return CadenceCharge::Admit;
        }
        metrics::counter!(
            "zakura.p2p.byte_rate.overrun",
            "layout" => layout.to_string(),
            "message_type" => message_type.to_string(),
        )
        .increment(1);
        CadenceCharge::Overrun
    }

    /// Take a token of `rule`'s bucket on `layout` for a parked frame: one
    /// that [`Self::charge_frame`] found its row's bucket empty for. Returns
    /// the instant the bucket earns its next token if it holds none now.
    ///
    /// A row without a cadence holds no bucket and takes nothing. The frame's
    /// first charge took no token, so a parked frame pays exactly one token,
    /// here.
    pub(crate) fn take_parked(&mut self, layout: u16, rule: &MessageRule) -> Result<(), Instant> {
        let Some(cadence) = cadence(rule) else {
            return Ok(());
        };
        let key = CadenceKey {
            layout,
            message_type: rule.message_type,
        };
        self.ledger.take_parked(key, cadence, self.clock.now())
    }

    /// Credit a local read pause on a stream of `layout` that carries `rules`.
    ///
    /// While this node's reader waits for its own handler, the sender keeps
    /// sending into transport buffers. Metered rows earn nothing: their depth
    /// already holds the stream window those buffers fill. Each of the stream's buckets gains one
    /// token per `refill_interval` paused, above `capacity`, so the burst that
    /// the pause buffered never counts against the sender.
    pub(crate) fn credit_pause(&mut self, layout: u16, rules: &[MessageRule], paused: Duration) {
        let now = self.clock.now();
        for rule in rules {
            let Some(cadence) = cadence(rule) else {
                continue;
            };
            let key = CadenceKey {
                layout,
                message_type: rule.message_type,
            };
            self.ledger.credit_pause(key, cadence, paused, now);
        }
    }

    /// Tokens left in a bucket, if it exists.
    #[cfg(test)]
    pub(crate) fn tokens(&self, layout: u16, message_type: u16) -> Option<u32> {
        self.ledger
            .buckets
            .get(&CadenceKey {
                layout,
                message_type,
            })
            .map(|bucket| bucket.tokens)
    }
}

/// Whether the reader of a stream that carries `rules` parks on an empty
/// cadence bucket, instead of recording the exhaustion and forwarding the frame.
///
/// A parked reader holds back every later frame of its stream too. So a reader
/// parks only if every row of its stream is paced: each announcement and
/// request declares a cadence, and each response answers a request of the
/// same stream that declares one. A subscription's pages and a metered row are
/// not paced by a cadence, nor is the work an uncadenced request asks for, so
/// a park would stall that traffic. On such a stream, the service meters an
/// announcement's work itself.
pub(crate) fn parks_on_exhaustion(rules: &[MessageRule]) -> bool {
    let paced_request = |message_type| {
        MessageRule::find(rules, message_type).is_some_and(|request| {
            matches!(
                request.role,
                MessageRole::Request {
                    cadence: Some(_),
                    ..
                }
            )
        })
    };
    rules.iter().all(|rule| match rule.role {
        MessageRole::Announcement { .. } => true,
        MessageRole::Request { cadence, .. } => cadence.is_some(),
        MessageRole::Response { request, .. } => paced_request(request),
        MessageRole::Subscription { .. } | MessageRole::Metered { .. } => false,
    })
}

/// The row's cadence, if it declares one.
fn cadence(rule: &MessageRule) -> Option<Cadence> {
    match rule.role {
        MessageRole::Announcement { cadence } => Some(cadence),
        MessageRole::Request { cadence, .. } | MessageRole::Subscription { cadence, .. } => cadence,
        MessageRole::Metered { .. } | MessageRole::Response { .. } => None,
    }
}

#[derive(Debug)]
struct Slot<M> {
    message_type: u16,
    send_interval: Duration,
    latest: Option<M>,
    last_sent: Option<Instant>,
    /// Alive while this row's last frame waits for its transport write.
    unwritten: zakura_rt::sync::Weak<()>,
    /// The sender's copy of the receiver's bucket for this row, charged for
    /// every frame queued, so an early send can stay within it.
    mirror: Bucket,
}

/// Why a message could not go to the [`CadenceSender`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum CadenceSendError {
    /// The message's row declares no cadence on this sender.
    #[error("message type {message_type} declares no cadence")]
    NoCadence {
        /// The message's type.
        message_type: u16,
    },
    /// The message did not encode.
    #[error("message type {message_type} did not encode")]
    Encode {
        /// The message's type.
        message_type: u16,
    },
    /// The stream closed.
    #[error("the stream closed")]
    Closed,
}

/// Sends the rows that declare a cadence, no faster than they allow.
///
/// It keeps only the latest value of each row. It writes a value once the
/// row's `send_interval` has passed since the last write and the row's
/// previous frame has finished its transport write. A stalled writer
/// therefore holds at most one frame per row, and a value that changes
/// during the stall replaces the one waiting to be sent.
#[derive(Debug)]
pub(crate) struct CadenceSender<M, C: Clock = RealClock> {
    slots: Vec<Slot<M>>,
    clock: C,
}

impl<M: WireMessage> CadenceSender<M, RealClock> {
    /// A sender for the family's cadence rows.
    pub(crate) fn new() -> Self {
        Self::with_clock(RealClock)
    }
}

impl<M: WireMessage, C: Clock> CadenceSender<M, C> {
    pub(crate) fn with_clock(clock: C) -> Self {
        let now = clock.now();
        let slots = M::RULES
            .iter()
            .filter_map(|rule| {
                cadence(rule).map(|cadence| Slot {
                    message_type: rule.message_type,
                    send_interval: cadence.send_interval,
                    latest: None,
                    last_sent: None,
                    unwritten: zakura_rt::sync::Weak::new(),
                    mirror: Bucket::new(cadence, now),
                })
            })
            .collect();
        Self { slots, clock }
    }

    /// Replace the value waiting to be sent for `message`'s row.
    pub(crate) fn update(&mut self, message: M) -> Result<(), CadenceSendError> {
        let message_type = message.message_type();
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.message_type == message_type)
            .ok_or(CadenceSendError::NoCadence { message_type })?;
        slot.latest = Some(message);
        Ok(())
    }

    /// The earliest instant a waiting value may be sent, if any is waiting.
    ///
    /// A value whose previous frame is still unwritten is due as soon as that
    /// write finishes; poll [`Self::send_due`] again after writes progress.
    #[cfg(test)]
    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.slots
            .iter()
            .filter(|slot| slot.latest.is_some())
            .map(|slot| {
                slot.last_sent
                    .map_or_else(|| self.clock.now(), |last| last + slot.send_interval)
            })
            .min()
    }

    /// Raise `message_type`'s send interval to at least `interval`.
    ///
    /// A local policy may send less often than its row allows, never more.
    pub(crate) fn with_min_interval(
        mut self,
        message_type: u16,
        interval: Duration,
    ) -> Result<Self, CadenceSendError> {
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.message_type == message_type)
            .ok_or(CadenceSendError::NoCadence { message_type })?;
        slot.send_interval = slot.send_interval.max(interval);
        Ok(self)
    }

    /// Try to queue the latest value of `message_type`'s row without waiting.
    pub(crate) fn send_row(
        &mut self,
        message_type: u16,
        send: &FramedSend,
    ) -> Result<RowSend, CadenceSendError>
    where
        M::Error: std::fmt::Debug,
    {
        let now = self.clock.now();
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.message_type == message_type)
            .ok_or(CadenceSendError::NoCadence { message_type })?;
        send_slot(slot, now, send)
    }

    /// Try to queue the latest value of `message_type`'s row before its send
    /// interval has passed.
    ///
    /// The receiver only observes a row's bucket (capacity, one token per
    /// refill interval), so a sender may go faster than `send_interval` while
    /// it keeps tokens in that bucket. This sends early only if at least
    /// `min_gap` passed since the row's last frame and the sender's copy of the
    /// receiver's bucket keeps more than `reserve` tokens, so bursts after an
    /// outage, which the bucket is sized for, still fit. Otherwise it behaves
    /// as [`Self::send_row`], except that a deferral ends as soon as an early
    /// send would be allowed: after `min_gap`, or once the bucket refills
    /// above `reserve`, if either comes before the send interval.
    pub(crate) fn send_row_early(
        &mut self,
        message_type: u16,
        send: &FramedSend,
        min_gap: Duration,
        reserve: u32,
    ) -> Result<RowSend, CadenceSendError>
    where
        M::Error: std::fmt::Debug,
    {
        let now = self.clock.now();
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.message_type == message_type)
            .ok_or(CadenceSendError::NoCadence { message_type })?;
        slot.mirror.refill(now);
        let gap_ends = slot.last_sent.map_or(now, |last| last + min_gap);
        let tokens_at = if slot.mirror.tokens > reserve {
            now
        } else {
            slot.mirror.last_refill + slot.mirror.cadence.refill_interval
        };
        let early_at = gap_ends.max(tokens_at);
        if early_at <= now {
            return send_slot_at(slot, now, send, true);
        }
        Ok(match send_slot(slot, now, send)? {
            RowSend::DeferredUntil(due) => RowSend::DeferredUntil(due.min(early_at)),
            other => other,
        })
    }

    /// Queue every due value on `send` without waiting.
    ///
    /// Returns the number of frames queued. A full queue leaves the value
    /// waiting.
    pub(crate) fn send_due(&mut self, send: &FramedSend) -> Result<usize, CadenceSendError>
    where
        M::Error: std::fmt::Debug,
    {
        let now = self.clock.now();
        let mut queued = 0;
        for slot in &mut self.slots {
            if send_slot(slot, now, send)? == RowSend::Queued {
                queued += 1;
            }
        }
        Ok(queued)
    }
}

/// What one row did on a send attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum RowSend {
    /// No value waits.
    Idle,
    /// The latest value entered the queue.
    Queued,
    /// The row's interval has not passed. The value may be sent at this instant.
    DeferredUntil(Instant),
    /// The row's previous frame has not finished its transport write.
    Unwritten,
    /// The stream's outbound queue has no free slot.
    QueueFull,
}

impl<M> Slot<M> {
    /// Check the row's invariants: a frame waits for its write only after the
    /// row sent it, and the sender's copy of the receiver's bucket never holds
    /// more than its capacity, since the sender never credits a pause to it.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn assert_invariants(&self) {
        assert!(
            self.unwritten.strong_count() == 0 || self.last_sent.is_some(),
            "a row holds an unwritten frame it never sent"
        );
        assert!(
            self.mirror.tokens <= self.mirror.cadence.capacity,
            "the sender's bucket holds more than its capacity"
        );
        assert_eq!(self.mirror.paused, Duration::ZERO, "the sender paused");
    }

    /// Why this row cannot queue its value at `now`, if it cannot.
    ///
    /// `unwritten` reports whether the row's previous frame still waits for
    /// its transport write. An `early` send ignores the send interval.
    fn hold(&self, now: Instant, unwritten: bool, early: bool) -> Option<RowSend> {
        if self.latest.is_none() {
            return Some(RowSend::Idle);
        }
        if let Some(due) = self
            .last_sent
            .map(|last| last + self.send_interval)
            .filter(|due| !early && *due > now)
        {
            return Some(RowSend::DeferredUntil(due));
        }
        unwritten.then_some(RowSend::Unwritten)
    }
}

fn send_slot<M: WireMessage>(
    slot: &mut Slot<M>,
    now: Instant,
    send: &FramedSend,
) -> Result<RowSend, CadenceSendError> {
    send_slot_at(slot, now, send, false)
}

fn send_slot_at<M: WireMessage>(
    slot: &mut Slot<M>,
    now: Instant,
    send: &FramedSend,
    early: bool,
) -> Result<RowSend, CadenceSendError> {
    if let Some(hold) = slot.hold(now, slot.unwritten.strong_count() > 0, early) {
        return Ok(hold);
    }
    let reserved = match send.try_reserve_guarded() {
        Ok(reserved) => reserved,
        Err(crate::zakura::transport::GuardedReserveError::Full) => return Ok(RowSend::QueueFull),
        Err(_) => return Err(CadenceSendError::Closed),
    };
    let message = slot
        .latest
        .take()
        .expect("the slot was checked to hold a value above");
    let frame = encode_frame(&message).map_err(|_| CadenceSendError::Encode {
        message_type: slot.message_type,
    })?;
    let written = std::sync::Arc::new(());
    slot.unwritten = zakura_rt::sync::downgrade(&written);
    reserved.send(frame, FrameGuard::new(written));
    slot.last_sent = Some(now);
    slot.mirror.refill(now);
    slot.mirror.tokens = slot.mirror.tokens.saturating_sub(1);
    Ok(RowSend::Queued)
}
