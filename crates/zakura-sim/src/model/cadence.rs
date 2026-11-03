//! The cadence model: one bucket per connection, layout, and row.

use std::{collections::BTreeMap, time::Duration};

use zakura_network::zakura::{Cadence, MessageRole, MessageRule};

/// A bucket's answer to one frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Charge {
    /// The row declares no cadence.
    Exempt,
    /// The bucket had a token and spent it.
    Admit,
    /// The bucket was empty. The frame is still conformant.
    Exhausted,
}

/// One row's bucket on one connection.
#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: u32,
    /// When the last whole refill interval ended.
    refilled_at: Duration,
    /// Credited pause shorter than one refill interval.
    paused: Duration,
}

/// Measures sender conformance with each row's cadence. It labels nothing:
/// a frame that empties a bucket is conformant, because one stream can
/// stall while sibling traffic keeps the connection alive.
///
/// A full bucket holds the row's `capacity` and regains one token each
/// `refill_interval`, with the remainder carried to the next refill.
/// Refills never raise a bucket above `capacity`, but a local read pause
/// credits its tokens even above it, so the receiver's own pause never
/// counts against the sender.
///
/// Times are offsets from an arbitrary start.
#[derive(Clone, Debug, Default)]
pub struct CadenceModel {
    buckets: BTreeMap<(u64, u16, u16), Bucket>,
}

/// The cadence `row` declares, if any.
fn cadence(row: &MessageRule) -> Option<Cadence> {
    match row.role {
        MessageRole::Announcement { cadence } => Some(cadence),
        MessageRole::Request { cadence, .. } | MessageRole::Subscription { cadence, .. } => cadence,
        MessageRole::Response { .. } => None,
    }
}

impl CadenceModel {
    /// An empty model.
    pub fn new() -> Self {
        Self::default()
    }

    /// The bucket for `row` on `connection` and `layout`, refilled to `now`.
    fn bucket(
        &mut self,
        connection: u64,
        layout: u16,
        row: &MessageRule,
        cadence: Cadence,
        now: Duration,
    ) -> &mut Bucket {
        let bucket = self
            .buckets
            .entry((connection, layout, row.message_type))
            .or_insert(Bucket {
                tokens: cadence.capacity,
                refilled_at: now,
                paused: Duration::ZERO,
            });
        let elapsed = now.saturating_sub(bucket.refilled_at);
        let intervals = elapsed.as_nanos() / cadence.refill_interval.as_nanos();
        if intervals > 0 {
            let intervals = u32::try_from(intervals).unwrap_or(u32::MAX);
            if bucket.tokens < cadence.capacity {
                bucket.tokens = bucket
                    .tokens
                    .saturating_add(intervals)
                    .min(cadence.capacity);
            }
            bucket.refilled_at += cadence.refill_interval.saturating_mul(intervals);
        }
        bucket
    }

    /// Charges a frame of `row` on `connection` and `layout` at `now`.
    pub fn charge(
        &mut self,
        connection: u64,
        layout: u16,
        row: &MessageRule,
        now: Duration,
    ) -> Charge {
        let Some(cadence) = cadence(row) else {
            return Charge::Exempt;
        };
        let bucket = self.bucket(connection, layout, row, cadence, now);
        if bucket.tokens == 0 {
            return Charge::Exhausted;
        }
        bucket.tokens -= 1;
        Charge::Admit
    }

    /// Credits a local read pause of `paused`, ending at `now`, to every
    /// bucket of `rules` on `connection` and `layout`.
    pub fn credit_pause(
        &mut self,
        connection: u64,
        layout: u16,
        rules: &[MessageRule],
        paused: Duration,
        now: Duration,
    ) {
        for row in rules {
            let Some(cadence) = cadence(row) else {
                continue;
            };
            let bucket = self.bucket(connection, layout, row, cadence, now);
            bucket.paused = bucket.paused.saturating_add(paused);
            let intervals = bucket.paused.as_nanos() / cadence.refill_interval.as_nanos();
            let intervals = u32::try_from(intervals).unwrap_or(u32::MAX);
            bucket.tokens = bucket.tokens.saturating_add(intervals);
            bucket.paused = bucket
                .paused
                .saturating_sub(cadence.refill_interval.saturating_mul(intervals));
        }
    }

    /// Tokens left in `row`'s bucket on `connection` and `layout`, if it
    /// exists.
    pub fn tokens(&self, connection: u64, layout: u16, message_type: u16) -> Option<u32> {
        self.buckets
            .get(&(connection, layout, message_type))
            .map(|bucket| bucket.tokens)
    }
}
