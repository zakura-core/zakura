//! Streams, simulator build.
//!
//! `StreamMap` is tokio-stream's, with one change: each poll starts at a
//! stream chosen by the run's `Plan` instead of tokio-stream's thread RNG.

use std::{
    borrow::Borrow,
    hash::Hash,
    panic::Location,
    pin::Pin,
    task::{Context, Poll},
};

use tokio_stream::Stream;

use crate::sim::{start_index, DrawKind};

/// tokio-stream's `StreamMap`: a map of keyed streams, polled as one stream
/// of `(key, item)`.
///
/// Inside a run, each poll's first stream is a draw from the `Plan`.
/// Outside one it's random, as in production. The draw's site is where the
/// map was made, since `poll_next` can't see its caller.
#[derive(Debug)]
pub struct StreamMap<K, V> {
    entries: Vec<(K, V)>,
    site: &'static Location<'static>,
}

impl<K, V> StreamMap<K, V> {
    /// An empty map.
    #[track_caller]
    pub fn new() -> Self {
        StreamMap::with_capacity(0)
    }

    /// An empty map with room for `capacity` streams.
    #[track_caller]
    pub fn with_capacity(capacity: usize) -> Self {
        StreamMap {
            entries: Vec::with_capacity(capacity),
            site: Location::caller(),
        }
    }

    /// The keys and streams, in the map's internal order.
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.entries.iter()
    }

    /// The keys and streams, mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut (K, V)> {
        self.entries.iter_mut()
    }

    /// The keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.iter().map(|(key, _)| key)
    }

    /// The streams.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().map(|(_, stream)| stream)
    }

    /// The streams, mutably.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().map(|(_, stream)| stream)
    }

    /// How many streams the map can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// How many streams the map holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if the map holds no streams.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Removes every stream.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Adds `stream` under `key`, returning the stream it replaces.
    pub fn insert(&mut self, key: K, stream: V) -> Option<V>
    where
        K: Hash + Eq,
    {
        let replaced = self.remove(&key);
        self.entries.push((key, stream));
        replaced
    }

    /// Removes and returns the stream under `key`.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.entries.iter().position(|(k, _)| k.borrow() == key)?;
        Some(self.entries.swap_remove(index).1)
    }

    /// True if the map has a stream under `key`.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.entries.iter().any(|(k, _)| k.borrow() == key)
    }
}

impl<K, V> StreamMap<K, V>
where
    K: Unpin,
    V: Stream + Unpin,
{
    /// Polls every stream once, starting at the drawn one, and returns the
    /// first item with its entry's index. An ended stream leaves the map.
    ///
    /// The walk matches tokio-stream 0.1.18's `poll_next_entry`, so only the
    /// start differs from production.
    fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, V::Item)>> {
        if self.entries.is_empty() {
            return Poll::Ready(None);
        }
        let len = u32::try_from(self.entries.len()).expect("a StreamMap holds under 2^32 streams");
        // The draw is below `len`, which came from a usize.
        let start = start_index(DrawKind::StreamMap, self.site, len) as usize;
        let mut index = start;
        for _ in 0..self.entries.len() {
            let (_, stream) = &mut self.entries[index];
            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some((index, item))),
                Poll::Ready(None) => {
                    self.entries.swap_remove(index);
                    if index == self.entries.len() {
                        index = 0;
                    } else if index < start && start <= self.entries.len() {
                        // The stream swapped into `index` was already polled.
                        index = (index + 1) % self.entries.len();
                    }
                }
                Poll::Pending => index = (index + 1) % self.entries.len(),
            }
        }
        if self.entries.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<K, V> Default for StreamMap<K, V> {
    #[track_caller]
    fn default() -> Self {
        StreamMap::new()
    }
}

impl<K, V> Stream for StreamMap<K, V>
where
    K: Clone + Unpin,
    V: Stream + Unpin,
{
    type Item = (K, V::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_next_entry(cx) {
            Poll::Ready(Some((index, item))) => {
                Poll::Ready(Some((self.entries[index].0.clone(), item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries
            .iter()
            .fold((0, Some(0)), |(low, high), (_, stream)| {
                let (stream_low, stream_high) = stream.size_hint();
                let high = match (high, stream_high) {
                    (Some(a), Some(b)) => a.checked_add(b),
                    _ => None,
                };
                (low.saturating_add(stream_low), high)
            })
    }
}
