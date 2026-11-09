//! Builds ordered maps without compiling a sort at each call site.

use std::collections::BTreeMap;

/// Collects `pairs` into a [`BTreeMap`], keeping the last value for a repeated key.
///
/// The result equals `pairs.into_iter().collect::<BTreeMap<K, V>>()`.
/// `BTreeMap`'s `FromIterator` first sorts the pairs, with a comparator that is
/// generic over the source iterator, so each call site compiles its own copy of
/// the standard library's stable sort, several kilobytes of code. Inserting the
/// pairs one at a time instead shares a single `BTreeMap::insert` per key and
/// value type. Each insertion costs `O(log n)`, which suits the state's maps:
/// their pairs mostly come from already ordered database ranges or other maps.
pub(crate) fn collect_ordered_map<K: Ord, V>(
    pairs: impl IntoIterator<Item = (K, V)>,
) -> BTreeMap<K, V> {
    let mut map = BTreeMap::new();
    map.extend(pairs);
    map
}
