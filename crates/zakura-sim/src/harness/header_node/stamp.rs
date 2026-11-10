//! One node's fact stamps, shared by every view-side store its tap and
//! harness keep.
//!
//! A store takes the next stamp from the node's [`Stamps`] as it keeps a
//! fact, under its own lock, so each store's facts are in stamp order and
//! stamps order facts across stores in the order they were kept. The
//! harness's view hands them to the registry
//! ([`HasView::stamps_after`](crate::property::HasView::stamps_after)),
//! which offers each to the obligations that view-side facts answer. A
//! stamp a store doesn't hold answers nothing there.
//!
//! The stamps stay view-side. The trace log keeps its own `seq` numbers,
//! so its format doesn't change.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// A node's per-run stamp counter. Cloning shares it.
#[derive(Clone, Debug, Default)]
pub struct Stamps(Arc<AtomicU64>);

impl Stamps {
    /// The next stamp, from 1. Takes no lock and never yields, so a store
    /// may take one under its own lock.
    pub(crate) fn next(&self) -> u64 {
        self.0
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("a run stamps fewer than 2^64 facts")
    }

    /// Every stamp handed out after `after`, or every one with `None`, in
    /// increasing order.
    pub fn after(&self, after: Option<u64>) -> Vec<u64> {
        let last = self.0.load(Ordering::Relaxed);
        (after.map_or(1, |after| after.saturating_add(1))..=last).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Stamps;

    /// Stamps count from 1, clones share them, and `after` lists exactly
    /// those handed out past its argument.
    #[test]
    fn stamps_count_up_across_clones() {
        let stamps = Stamps::default();
        assert_eq!(stamps.after(None), Vec::<u64>::new());
        let clone = stamps.clone();
        assert_eq!((stamps.next(), clone.next(), stamps.next()), (1, 2, 3));
        assert_eq!(stamps.after(None), [1, 2, 3]);
        assert_eq!(clone.after(Some(1)), [2, 3]);
        assert_eq!(stamps.after(Some(3)), Vec::<u64>::new());
    }
}
