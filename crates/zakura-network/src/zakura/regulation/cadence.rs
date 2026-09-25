//! Cadence: the rate a row declares, enforced by the receiver and obeyed by
//! the sender.
//!
//! Both sides read the same [`Cadence`] from the row. [`CadenceSender`]
//! leaves at least `send_interval` between two messages of a row and keeps
//! at most one of them unwritten. [`CadenceBuckets`] keeps one bucket for each
//! `(layout, message_type)` of a connection.
//!
//! The layout validator proves that a conformant sender never empties a
//! bucket: the bucket refills faster than the sender sends, and its capacity
//! holds the burst after the longest outage a connection survives. The
//! receiver's own read pauses add tokens as they happen. So an empty bucket
//! is a protocol violation, and the reader disconnects the peer.
//!
//! Rows without a cadence charge nothing. Commitments bound requests, and
//! reservations bound responses.
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
//! | Every accepted cadence admits every conformant sender | `a_conformant_sender_is_never_exhausted` |
//! | The sender paces rows and keeps one unwritten frame per row | `the_sender_waits_for_its_interval_and_keeps_only_the_latest_value`, `a_blocked_writer_holds_at_most_one_frame_per_row` |

use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;

use crate::zakura::{
    wire_codec::{encode_frame, WireMessage},
    Cadence, Clock, FrameGuard, FramedSend, MessageRole, MessageRule, RealClock,
};

#[cfg(test)]
mod tests;

/// The result of charging one frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum CadenceCharge {
    /// The row declares no cadence.
    Exempt,
    /// The bucket had a token.
    Admit,
    /// The bucket is empty: the sender broke the row's cadence.
    Exhausted,
}

/// A bucket key. Message types are unique only within a layout, so the key
/// includes the layout's primary stream kind.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct CadenceKey {
    layout: u16,
    message_type: u16,
}

#[derive(Clone, Debug)]
struct Bucket {
    cadence: Cadence,
    tokens: u32,
    last_refill: Instant,
    /// Local read-pause time not yet converted into tokens.
    paused: Duration,
}

impl Bucket {
    fn new(cadence: Cadence, now: Instant) -> Self {
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
    fn refill(&mut self, now: Instant) {
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

/// One connection's cadence buckets.
///
/// Created with the connection. Retiring a session or reopening a stream in
/// the same connection does not refill them. A reconnect gets fresh buckets;
/// connection admission bounds reconnects.
#[derive(Debug)]
pub(crate) struct CadenceBuckets<C: Clock = RealClock> {
    buckets: HashMap<CadenceKey, Bucket>,
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
            buckets: HashMap::new(),
            clock,
        }
    }

    /// Charge one frame of `rule` on `layout`, whose row the frame filter
    /// already found.
    pub(crate) fn charge(&mut self, layout: u16, rule: &MessageRule) -> CadenceCharge {
        let Some(cadence) = cadence(rule) else {
            return CadenceCharge::Exempt;
        };
        let now = self.clock.now();
        let key = CadenceKey {
            layout,
            message_type: rule.message_type,
        };
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| Bucket::new(cadence, now));
        bucket.refill(now);
        if bucket.tokens == 0 {
            metrics::counter!(
                "zakura.p2p.cadence.violation",
                "layout" => layout.to_string(),
                "message_type" => rule.message_type.to_string(),
            )
            .increment(1);
            return CadenceCharge::Exhausted;
        }
        bucket.tokens -= 1;
        if bucket.tokens < cadence.capacity / 4 {
            // A conformant sender came close to the limit. The values stay
            // enforced; the trace is evidence for tuning them.
            metrics::counter!(
                "zakura.p2p.cadence.low",
                "layout" => layout.to_string(),
                "message_type" => rule.message_type.to_string(),
            )
            .increment(1);
        }
        CadenceCharge::Admit
    }

    /// Credit a local read pause on a stream of `layout` that carries `rules`.
    ///
    /// While this node's reader waits for its own handler, the sender keeps
    /// sending into transport buffers. Each of the stream's buckets gains one
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
            let bucket = self
                .buckets
                .entry(key)
                .or_insert_with(|| Bucket::new(cadence, now));
            bucket.refill(now);
            bucket.credit_pause(paused);
        }
    }

    /// Tokens left in a bucket, if it exists.
    #[cfg(test)]
    pub(crate) fn tokens(&self, layout: u16, message_type: u16) -> Option<u32> {
        self.buckets
            .get(&CadenceKey {
                layout,
                message_type,
            })
            .map(|bucket| bucket.tokens)
    }
}

/// The row's cadence, if it declares one.
fn cadence(rule: &MessageRule) -> Option<Cadence> {
    match rule.role {
        MessageRole::Announcement { cadence } => Some(cadence),
        MessageRole::Request { cadence, .. } => cadence,
        MessageRole::Response { .. } => None,
    }
}

#[derive(Debug)]
struct Slot<M> {
    message_type: u16,
    send_interval: Duration,
    latest: Option<M>,
    last_sent: Option<Instant>,
    /// Alive while this row's last frame waits for its transport write.
    unwritten: std::sync::Weak<()>,
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
        let slots = M::RULES
            .iter()
            .filter_map(|rule| {
                cadence(rule).map(|cadence| Slot {
                    message_type: rule.message_type,
                    send_interval: cadence.send_interval,
                    latest: None,
                    last_sent: None,
                    unwritten: std::sync::Weak::new(),
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
            let interval_passed = slot
                .last_sent
                .is_none_or(|last| now.saturating_duration_since(last) >= slot.send_interval);
            if slot.latest.is_none() || !interval_passed || slot.unwritten.strong_count() > 0 {
                continue;
            }
            let reserved = match send.try_reserve_guarded() {
                Ok(reserved) => reserved,
                Err(crate::zakura::transport::GuardedReserveError::Full) => continue,
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
            slot.unwritten = std::sync::Arc::downgrade(&written);
            reserved.send(frame, FrameGuard::new(written));
            slot.last_sent = Some(now);
            queued += 1;
        }
        Ok(queued)
    }
}
