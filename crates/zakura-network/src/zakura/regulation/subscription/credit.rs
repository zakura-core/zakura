//! Renewable response credit, counted in objects and bytes.
//!
//! Ported from #977's `ResponseCredit`. Its grant bounds the credit window,
//! granted minus acknowledged, instead of unspent credit: an acknowledgement
//! trails consumption, so only the window is bounded on both sides.

use thiserror::Error;

use crate::zakura::Credit;

/// Cumulative objects and payload bytes.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Totals {
    pub(crate) objects: u64,
    pub(crate) bytes: u64,
}

impl Totals {
    /// `self + credit`, or `None` on overflow.
    fn checked_add(self, credit: Credit) -> Option<Self> {
        Some(Self {
            objects: self.objects.checked_add(u64::from(credit.objects))?,
            bytes: self.bytes.checked_add(u64::from(credit.bytes))?,
        })
    }
}

/// A grant or a response that the credit cannot cover.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("the response or grant exceeds its object or byte credit")]
pub(crate) struct CreditExceeded;

/// Whether `granted`, with `acknowledged` progress, stays within `limit`.
///
/// Both sides of a subscription call this with the same values, so a grant
/// that the subscriber sends is one the publisher accepts.
pub(crate) fn within_window(granted: Totals, acknowledged: Totals, limit: Credit) -> bool {
    granted.objects.saturating_sub(acknowledged.objects) <= u64::from(limit.objects)
        && granted.bytes.saturating_sub(acknowledged.bytes) <= u64::from(limit.bytes)
}

/// The objects and bytes a peer may still send.
///
/// Only a grant adds credit. Local scheduling and handler capacity never
/// restore spent credit, and the cumulative counters survive every grant.
#[derive(Clone, Debug)]
pub(crate) struct ResponseCredit {
    granted: Totals,
    consumed: Totals,
}

impl ResponseCredit {
    /// Credit holding `initial`.
    pub(crate) fn new(initial: Credit) -> Self {
        Self {
            granted: Totals::default()
                .checked_add(initial)
                .expect("u32 credit fits the u64 counters"),
            consumed: Totals::default(),
        }
    }

    /// Credit with the given counters, for tests near the integer bounds.
    #[cfg(test)]
    pub(crate) fn with_totals(granted: Totals, consumed: Totals) -> Self {
        assert!(consumed.objects <= granted.objects && consumed.bytes <= granted.bytes);
        Self { granted, consumed }
    }

    /// Add `added`, if the window from `acknowledged` stays within `limit`.
    ///
    /// A refused grant changes nothing.
    pub(crate) fn grant(
        &mut self,
        added: Credit,
        acknowledged: Totals,
        limit: Credit,
    ) -> Result<(), CreditExceeded> {
        let granted = self.granted.checked_add(added).ok_or(CreditExceeded)?;
        if !within_window(granted, acknowledged, limit) {
            return Err(CreditExceeded);
        }
        self.granted = granted;
        Ok(())
    }

    /// Whether the unspent credit covers `objects` and `bytes`. Run it before
    /// allocating or waiting for handler capacity.
    pub(crate) fn check(&self, objects: u64, bytes: u64) -> Result<(), CreditExceeded> {
        let unspent = self.unspent();
        if objects > unspent.objects || bytes > unspent.bytes {
            return Err(CreditExceeded);
        }
        Ok(())
    }

    /// Spend `objects` and `bytes`, before the handler starts, even if it
    /// discards them. A refused spend changes nothing.
    pub(crate) fn consume(&mut self, objects: u64, bytes: u64) -> Result<(), CreditExceeded> {
        self.check(objects, bytes)?;
        // `check` bounds each amount by `granted - consumed`, so neither sum
        // can exceed `granted`.
        self.consumed.objects += objects;
        self.consumed.bytes += bytes;
        Ok(())
    }

    /// Credit granted so far, the initial credit included.
    pub(crate) fn granted(&self) -> Totals {
        self.granted
    }

    /// Credit spent so far.
    pub(crate) fn consumed(&self) -> Totals {
        self.consumed
    }

    /// Credit granted and not yet spent.
    pub(crate) fn unspent(&self) -> Totals {
        Totals {
            objects: self.granted.objects - self.consumed.objects,
            bytes: self.granted.bytes - self.consumed.bytes,
        }
    }
}
