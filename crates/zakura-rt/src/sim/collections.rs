//! Hashed collections, simulator build.
//!
//! [`HashMap`] and [`HashSet`] are thin newtypes over std's that use this
//! module's [`RandomState`]. Inside a run, every `RandomState` hashes with one
//! seed, drawn from the run's `Plan` the first time a collection is made. So
//! iteration order replays with the run, and the fuzzer can still vary it.
//! Outside a run, `RandomState` is std's random one. The newtypes deref to
//! std's types for their methods, so call sites don't change.

use std::{
    borrow::Borrow,
    cell::Cell,
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{BuildHasher, Hash},
    iter::FromIterator,
    ops::{Deref, DerefMut, Index},
    panic::Location,
};

use crate::sim::{executor, DrawKind, DrawSite};

thread_local! {
    /// The current run's hash seed, once drawn.
    static RUN_SEED: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Forgets the run's hash seed. Called when a run starts and ends.
pub(crate) fn reset_run_seed() {
    let _ = RUN_SEED.try_with(|seed| seed.set(None));
}

/// The hash seed for the current run, drawing it on first use.
#[track_caller]
fn run_seed() -> Option<u64> {
    let executor = executor()?;
    if let Some(seed) = RUN_SEED.with(Cell::get) {
        return Some(seed);
    }
    let seed = executor.draw(
        DrawSite {
            kind: DrawKind::HashSeed,
            site: Location::caller(),
        },
        0,
    );
    RUN_SEED.with(|cell| cell.set(Some(seed)));
    Some(seed)
}

/// Builds hashers: seeded from the run inside a run, std's random keys
/// outside one.
#[derive(Clone)]
pub struct RandomState(Inner);

#[derive(Clone)]
enum Inner {
    Std(std::collections::hash_map::RandomState),
    Seeded(u64),
}

impl RandomState {
    /// A hasher builder for the current run, or a random one outside a run.
    #[track_caller]
    pub fn new() -> Self {
        match run_seed() {
            Some(seed) => RandomState(Inner::Seeded(seed)),
            None => RandomState(Inner::Std(std::collections::hash_map::RandomState::new())),
        }
    }
}

impl Default for RandomState {
    #[track_caller]
    fn default() -> Self {
        RandomState::new()
    }
}

impl BuildHasher for RandomState {
    type Hasher = DefaultHasher;

    fn build_hasher(&self) -> DefaultHasher {
        match &self.0 {
            Inner::Std(state) => state.build_hasher(),
            Inner::Seeded(seed) => {
                // `DefaultHasher` has fixed keys, so the seed goes in first.
                let mut hasher = DefaultHasher::new();
                std::hash::Hasher::write_u64(&mut hasher, *seed);
                hasher
            }
        }
    }
}

impl fmt::Debug for RandomState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RandomState").finish_non_exhaustive()
    }
}

/// std's `HashMap` with the run-seeded [`RandomState`].
pub struct HashMap<K, V, S = RandomState>(std::collections::HashMap<K, V, S>);

impl<K, V> HashMap<K, V, RandomState> {
    /// An empty map.
    #[track_caller]
    pub fn new() -> Self {
        HashMap(std::collections::HashMap::with_hasher(RandomState::new()))
    }

    /// An empty map with room for `capacity` entries.
    #[track_caller]
    pub fn with_capacity(capacity: usize) -> Self {
        HashMap(std::collections::HashMap::with_capacity_and_hasher(
            capacity,
            RandomState::new(),
        ))
    }
}

impl<K, V, S> HashMap<K, V, S> {
    /// An empty map using `hasher`.
    pub fn with_hasher(hasher: S) -> Self {
        HashMap(std::collections::HashMap::with_hasher(hasher))
    }

    /// An empty map with room for `capacity` entries, using `hasher`.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        HashMap(std::collections::HashMap::with_capacity_and_hasher(
            capacity, hasher,
        ))
    }

    /// The keys, consuming the map.
    pub fn into_keys(self) -> std::collections::hash_map::IntoKeys<K, V> {
        self.0.into_keys()
    }

    /// The values, consuming the map.
    pub fn into_values(self) -> std::collections::hash_map::IntoValues<K, V> {
        self.0.into_values()
    }

    /// The std map inside.
    pub fn into_inner(self) -> std::collections::HashMap<K, V, S> {
        self.0
    }
}

// `&mut self` methods are forwarded rather than reached through `DerefMut`,
// so a call like `map.insert(k, map.len())` borrow-checks as it does on std's
// map. A deref'd receiver gets no two-phase borrow.
impl<K: Eq + Hash, V, S: BuildHasher> HashMap<K, V, S> {
    /// Inserts `value` at `key`, returning the old value.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.0.insert(key, value)
    }

    /// Removes `key`, returning its value.
    pub fn remove<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        self.0.remove(key)
    }

    /// Removes `key`, returning it with its value.
    pub fn remove_entry<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
    {
        self.0.remove_entry(key)
    }

    /// The entry for `key`, for in-place changes.
    pub fn entry(&mut self, key: K) -> std::collections::hash_map::Entry<'_, K, V> {
        self.0.entry(key)
    }

    /// The value at `key`, mutably.
    pub fn get_mut<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        self.0.get_mut(key)
    }

    /// Keeps only the entries `keep` accepts.
    pub fn retain<F: FnMut(&K, &mut V) -> bool>(&mut self, keep: F) {
        self.0.retain(keep);
    }

    /// Makes room for `additional` more entries.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Shrinks the capacity to at least `min_capacity`.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.0.shrink_to(min_capacity);
    }
}

impl<K, V, S> Deref for HashMap<K, V, S> {
    type Target = std::collections::HashMap<K, V, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V, S> DerefMut for HashMap<K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V, S: Default> Default for HashMap<K, V, S> {
    #[track_caller]
    fn default() -> Self {
        HashMap(std::collections::HashMap::with_hasher(S::default()))
    }
}

impl<K: Clone, V: Clone, S: Clone> Clone for HashMap<K, V, S> {
    fn clone(&self) -> Self {
        HashMap(self.0.clone())
    }
}

impl<K: fmt::Debug, V: fmt::Debug, S> fmt::Debug for HashMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<K: Eq + Hash, V: PartialEq, S: BuildHasher> PartialEq for HashMap<K, V, S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K: Eq + Hash, V: Eq, S: BuildHasher> Eq for HashMap<K, V, S> {}

impl<K, Q, V, S> Index<&Q> for HashMap<K, V, S>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        &self.0[key]
    }
}

impl<K: Eq + Hash, V, S: BuildHasher + Default> FromIterator<(K, V)> for HashMap<K, V, S> {
    #[track_caller]
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut map = HashMap::default();
        map.extend(iter);
        map
    }
}

impl<K: Eq + Hash, V, const N: usize> From<[(K, V); N]> for HashMap<K, V, RandomState> {
    #[track_caller]
    fn from(entries: [(K, V); N]) -> Self {
        let mut map = HashMap::with_capacity(N);
        map.extend(entries);
        map
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Extend<(K, V)> for HashMap<K, V, S> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<'a, K: Eq + Hash + Copy, V: Copy, S: BuildHasher> Extend<(&'a K, &'a V)> for HashMap<K, V, S> {
    fn extend<I: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<K, V, S> IntoIterator for HashMap<K, V, S> {
    type Item = (K, V);
    type IntoIter = std::collections::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K, V, S> IntoIterator for &'a HashMap<K, V, S> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::hash_map::Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, K, V, S> IntoIterator for &'a mut HashMap<K, V, S> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = std::collections::hash_map::IterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

/// std's `HashSet` with the run-seeded [`RandomState`].
pub struct HashSet<T, S = RandomState>(std::collections::HashSet<T, S>);

impl<T> HashSet<T, RandomState> {
    /// An empty set.
    #[track_caller]
    pub fn new() -> Self {
        HashSet(std::collections::HashSet::with_hasher(RandomState::new()))
    }

    /// An empty set with room for `capacity` values.
    #[track_caller]
    pub fn with_capacity(capacity: usize) -> Self {
        HashSet(std::collections::HashSet::with_capacity_and_hasher(
            capacity,
            RandomState::new(),
        ))
    }
}

impl<T, S> HashSet<T, S> {
    /// An empty set using `hasher`.
    pub fn with_hasher(hasher: S) -> Self {
        HashSet(std::collections::HashSet::with_hasher(hasher))
    }

    /// An empty set with room for `capacity` values, using `hasher`.
    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        HashSet(std::collections::HashSet::with_capacity_and_hasher(
            capacity, hasher,
        ))
    }

    /// The std set inside.
    pub fn into_inner(self) -> std::collections::HashSet<T, S> {
        self.0
    }
}

// Forwarded for the borrow-check reason given on `HashMap`.
impl<T: Eq + Hash, S: BuildHasher> HashSet<T, S> {
    /// Adds `value`. False if it was already present.
    pub fn insert(&mut self, value: T) -> bool {
        self.0.insert(value)
    }

    /// Removes `value`. False if it was absent.
    pub fn remove<Q: Hash + Eq + ?Sized>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
    {
        self.0.remove(value)
    }

    /// Removes and returns `value`.
    pub fn take<Q: Hash + Eq + ?Sized>(&mut self, value: &Q) -> Option<T>
    where
        T: Borrow<Q>,
    {
        self.0.take(value)
    }

    /// Keeps only the values `keep` accepts.
    pub fn retain<F: FnMut(&T) -> bool>(&mut self, keep: F) {
        self.0.retain(keep);
    }

    /// Makes room for `additional` more values.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Shrinks the capacity to at least `min_capacity`.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.0.shrink_to(min_capacity);
    }
}

impl<T, S> Deref for HashSet<T, S> {
    type Target = std::collections::HashSet<T, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, S> DerefMut for HashSet<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T, S: Default> Default for HashSet<T, S> {
    #[track_caller]
    fn default() -> Self {
        HashSet(std::collections::HashSet::with_hasher(S::default()))
    }
}

impl<T: Clone, S: Clone> Clone for HashSet<T, S> {
    fn clone(&self) -> Self {
        HashSet(self.0.clone())
    }
}

impl<T: fmt::Debug, S> fmt::Debug for HashSet<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: Eq + Hash, S: BuildHasher> PartialEq for HashSet<T, S> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq + Hash, S: BuildHasher> Eq for HashSet<T, S> {}

impl<T: Eq + Hash, S: BuildHasher + Default> FromIterator<T> for HashSet<T, S> {
    #[track_caller]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut set = HashSet::default();
        set.extend(iter);
        set
    }
}

impl<T: Eq + Hash, const N: usize> From<[T; N]> for HashSet<T, RandomState> {
    #[track_caller]
    fn from(values: [T; N]) -> Self {
        let mut set = HashSet::with_capacity(N);
        set.extend(values);
        set
    }
}

impl<T: Eq + Hash, S: BuildHasher> Extend<T> for HashSet<T, S> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<'a, T: Eq + Hash + Copy + 'a, S: BuildHasher> Extend<&'a T> for HashSet<T, S> {
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<T, S> IntoIterator for HashSet<T, S> {
    type Item = T;
    type IntoIter = std::collections::hash_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T, S> IntoIterator for &'a HashSet<T, S> {
    type Item = &'a T;
    type IntoIter = std::collections::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// std's `hash_map` module, with this module's map and hasher.
pub mod hash_map {
    pub use std::collections::hash_map::{
        DefaultHasher, Drain, Entry, IntoIter, IntoKeys, IntoValues, Iter, IterMut, Keys,
        OccupiedEntry, VacantEntry, Values, ValuesMut,
    };

    pub use super::{HashMap, RandomState};
}
