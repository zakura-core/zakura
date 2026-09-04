use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
};

use zakura_chain::block;

/// Keep enough hashes to cover ordinary sync and live propagation races while
/// bounding memory if peers advertise an unbounded stream of distinct blocks.
const DEFAULT_CAPACITY: usize = 50_000;

/// The transport that delivered a complete block body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockBodySource {
    /// Zakura native block sync or the Zakura inventory adapter.
    Zakura,

    /// The legacy TCP peer set.
    Legacy,
}

impl BlockBodySource {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Zakura => "zakura",
            Self::Legacy => "legacy",
        }
    }
}

/// A cloneable, per-node cache where the first source recorded for a hash wins.
#[derive(Clone)]
pub(super) struct FirstBlockSourceTracker {
    inner: Arc<Mutex<SeenBlockSources>>,
}

impl fmt::Debug for FirstBlockSourceTracker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FirstBlockSourceTracker")
            .finish_non_exhaustive()
    }
}

impl FirstBlockSourceTracker {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            inner: Arc::new(Mutex::new(SeenBlockSources {
                capacity,
                sources: HashMap::new(),
                insertion_order: VecDeque::new(),
            })),
        }
    }

    pub(super) fn record(&self, hash: block::Hash, source: BlockBodySource) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("first block source mutex is not held across panicking code");
        if inner.sources.contains_key(&hash) {
            return false;
        }

        inner.sources.insert(hash, source);
        inner.insertion_order.push_back(hash);
        while inner.sources.len() > inner.capacity {
            let oldest = inner
                .insertion_order
                .pop_front()
                .expect("each tracked source has an insertion-order entry");
            inner.sources.remove(&oldest);
        }

        true
    }

    #[cfg(test)]
    pub(super) fn source(&self, hash: block::Hash) -> Option<BlockBodySource> {
        self.inner
            .lock()
            .expect("first block source mutex is not held across panicking code")
            .sources
            .get(&hash)
            .copied()
    }
}

impl Default for FirstBlockSourceTracker {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

#[derive(Debug)]
struct SeenBlockSources {
    capacity: usize,
    sources: HashMap<block::Hash, BlockBodySource>,
    insertion_order: VecDeque<block::Hash>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_source_wins() {
        let tracker = FirstBlockSourceTracker::with_capacity(2);
        let legacy_first = block::Hash([1; 32]);
        let zakura_first = block::Hash([2; 32]);

        assert!(tracker.record(legacy_first, BlockBodySource::Legacy));
        assert!(!tracker.record(legacy_first, BlockBodySource::Zakura));
        assert_eq!(tracker.source(legacy_first), Some(BlockBodySource::Legacy));

        assert!(tracker.record(zakura_first, BlockBodySource::Zakura));
        assert!(!tracker.record(zakura_first, BlockBodySource::Legacy));
        assert_eq!(tracker.source(zakura_first), Some(BlockBodySource::Zakura));
        assert_eq!(BlockBodySource::Zakura.as_str(), "zakura");
        assert_eq!(BlockBodySource::Legacy.as_str(), "legacy");
    }

    #[test]
    fn oldest_hash_is_evicted_at_capacity() {
        let tracker = FirstBlockSourceTracker::with_capacity(2);
        let first = block::Hash([1; 32]);
        let second = block::Hash([2; 32]);
        let third = block::Hash([3; 32]);

        assert!(tracker.record(first, BlockBodySource::Legacy));
        assert!(tracker.record(second, BlockBodySource::Zakura));
        assert!(tracker.record(third, BlockBodySource::Zakura));

        assert_eq!(tracker.source(first), None);
        assert_eq!(tracker.source(second), Some(BlockBodySource::Zakura));
        assert_eq!(tracker.source(third), Some(BlockBodySource::Zakura));
    }
}
