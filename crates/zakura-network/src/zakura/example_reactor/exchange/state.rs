//! The example exchange's policy, without runtime resources.
//!
//! [`ExchangeState`] tracks this node's requested ranges and decides each
//! response's outcome: which range an item belongs to, and whether an ending's
//! count matches what arrived. The free functions decide the watch rules: a
//! pushed item follows the previous one, each end reason arrives in its window,
//! and the publisher ends a watch for the first reason that applies.
//!
//! Nothing here owns a stream, reservation, subscription, lock, or task.
//! [`ExchangeIO`](super::ExchangeIO) holds those, runs the regulation tools'
//! prechecks and claims, and applies the decisions this module returns.

use std::collections::BTreeMap;

use zakura_chain::block::Height;

use super::Violation;
use crate::zakura::{
    example_reactor::{EndReason, ItemRange, WATCH_ITEMS},
    regulation::ClaimRefused,
    Credit,
};

#[cfg(test)]
mod tests;

/// A range this node requested, and the items it received so far.
#[derive(Debug)]
struct Download {
    range: ItemRange,
    items: Vec<(Height, Vec<u8>)>,
}

/// The ranges this node requested and the watch id it assigns next.
///
/// IO records a range only after it reserved the range's response and wrote
/// the request, so every recorded range has a live reservation.
#[derive(Debug, Default)]
pub(super) struct ExchangeState {
    downloads: BTreeMap<Height, Download>,
    next_watch: u32,
}

impl ExchangeState {
    /// Whether every requested range has ended.
    pub(super) fn idle(&self) -> bool {
        self.downloads.is_empty()
    }

    /// Record a request IO wrote for `range`.
    pub(super) fn requested(&mut self, range: ItemRange) {
        self.downloads.insert(
            range.start,
            Download {
                range,
                items: Vec::new(),
            },
        );
    }

    /// The start of the requested range containing `height`. An item outside
    /// every requested range is unsolicited.
    pub(super) fn range_of(
        &self,
        height: Height,
        message_type: u16,
    ) -> Result<Height, ClaimRefused> {
        self.downloads
            .range(..=height)
            .next_back()
            .filter(|(_, download)| height.0 - download.range.start.0 < download.range.count)
            .map(|(start, _)| *start)
            .ok_or(ClaimRefused::Unsolicited { message_type })
    }

    /// Keep an item of the range at `start`, after IO claimed its frame.
    pub(super) fn item(&mut self, start: Height, height: Height, bytes: Vec<u8>) {
        self.downloads
            .get_mut(&start)
            .expect("`range_of` returned a requested range's start")
            .items
            .push((height, bytes));
    }

    /// End the range at `start` with `ItemsDone`, after IO claimed the ending.
    ///
    /// `claimed` is the item count the reservation admitted. Returns the
    /// range's items in arrival order, or a violation if the peer reports
    /// another count.
    pub(super) fn items_done(
        &mut self,
        start: Height,
        returned: u32,
        claimed: u32,
    ) -> Result<Vec<(Height, Vec<u8>)>, Violation> {
        if claimed != returned {
            return Err(Violation::Count {
                reported: returned,
                received: claimed,
            });
        }
        Ok(self.end(start).items)
    }

    /// End the range at `start` with `RangeUnavailable`, after IO claimed the
    /// ending. Returns the range, or a violation if any item already arrived.
    pub(super) fn range_unavailable(
        &mut self,
        start: Height,
        claimed: u32,
    ) -> Result<ItemRange, Violation> {
        if claimed != 0 {
            return Err(Violation::Count {
                reported: 0,
                received: claimed,
            });
        }
        Ok(self.end(start).range)
    }

    fn end(&mut self, start: Height) -> Download {
        self.downloads
            .remove(&start)
            .expect("IO claims an ending only for a live reservation, which has a download")
    }

    /// The id the next watch takes.
    pub(super) fn next_watch(&self) -> u32 {
        self.next_watch
    }

    /// Record that IO sent the open of watch [`Self::next_watch`].
    pub(super) fn watch_opened(&mut self) {
        self.next_watch += 1;
    }
}

/// Check that a pushed item at `height` follows `received`, the watch's last
/// received height.
pub(super) fn follows(received: Height, height: Height) -> Result<(), Violation> {
    if received.next().ok() == Some(height) {
        Ok(())
    } else {
        Err(Violation::Linkage {
            received,
            got: height,
        })
    }
}

/// Check that a watch ended as `reason` inside that reason's window, after
/// `pages` pushed items and with or without a sent `Close`.
pub(super) fn end_in_window(
    reason: EndReason,
    pages: u64,
    close_sent: bool,
) -> Result<(), Violation> {
    let in_window = match reason {
        EndReason::Unavailable => pages == 0,
        EndReason::Superseded => true,
        EndReason::Closed => close_sent,
    };
    if in_window {
        Ok(())
    } else {
        Err(Violation::OutcomeWindow(reason))
    }
}

/// Whether the subscriber grants `room` back: once half the window is free.
pub(super) fn renews(room: Credit) -> bool {
    room.objects >= WATCH_ITEMS / 2
}

/// Why the publisher ends a watch now, if it does.
///
/// A closing watch ends as closed, then a superseded one as superseded. A
/// watch that has pushed nothing ends as unavailable if its next height
/// `next` is below the store's lowest height `low`.
pub(super) fn end_reason(
    closing: bool,
    superseded: bool,
    first: bool,
    next: Option<Height>,
    low: Height,
) -> Option<EndReason> {
    if closing {
        Some(EndReason::Closed)
    } else if superseded {
        Some(EndReason::Superseded)
    } else if first && next.is_some_and(|next| next < low) {
        Some(EndReason::Unavailable)
    } else {
        None
    }
}
