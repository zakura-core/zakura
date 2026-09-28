//! Test values that each wire item supplies to the suites.
//!
//! Primitive items list their edge values by hand. Tuples and lists derive
//! theirs from their parts, so a composed payload needs no hand-written test
//! data.

use proptest::prelude::*;
use zakura_chain::block;

use super::{HeightLe, LeU32, List, Wire, U8};

/// Valid test values for one wire item.
pub(crate) trait WireSample: Wire<Value: 'static> {
    /// Valid values that include a shortest and a longest encoding and the
    /// edges of the item's value range.
    fn boundary_values() -> Vec<Self::Value>;

    /// A strategy for valid values.
    fn arbitrary() -> BoxedStrategy<Self::Value>;
}

/// The encoding of a value that the caller knows is valid.
pub(crate) fn encoding<T: Wire>(value: &T::Value) -> Vec<u8> {
    let mut out = Vec::new();
    T::encode(value, &mut out).expect("a valid value encodes");
    out
}

/// A boundary value with the shortest encoding.
fn shortest<T: WireSample>() -> T::Value {
    T::boundary_values()
        .into_iter()
        .min_by_key(|value| encoding::<T>(value).len())
        .expect("an item lists boundary values")
}

/// A boundary value with the longest encoding.
fn longest<T: WireSample>() -> T::Value {
    T::boundary_values()
        .into_iter()
        .max_by_key(|value| encoding::<T>(value).len())
        .expect("an item lists boundary values")
}

impl WireSample for U8 {
    fn boundary_values() -> Vec<u8> {
        vec![0, u8::MAX]
    }

    fn arbitrary() -> BoxedStrategy<u8> {
        any::<u8>().boxed()
    }
}

impl WireSample for LeU32 {
    fn boundary_values() -> Vec<u32> {
        vec![0, 1, u32::MAX]
    }

    fn arbitrary() -> BoxedStrategy<u32> {
        any::<u32>().boxed()
    }
}

impl WireSample for HeightLe {
    fn boundary_values() -> Vec<block::Height> {
        vec![block::Height(0), block::Height::MAX]
    }

    fn arbitrary() -> BoxedStrategy<block::Height> {
        (0..=block::Height::MAX.0).prop_map(block::Height).boxed()
    }
}

/// Implement [`WireSample`] for a tuple of sampled items.
///
/// The boundary values put every item's boundary value in some row, then add
/// the all-shortest and all-longest rows.
macro_rules! sample_tuple {
    ($($item:ident $index:tt),+) => {
        impl<$($item: WireSample),+> WireSample for ($($item,)+) {
            fn boundary_values() -> Vec<Self::Value> {
                let items = ($($item::boundary_values(),)+);
                let rows = [$(items.$index.len()),+].into_iter().max().unwrap_or(0);
                let mut values: Vec<Self::Value> = (0..rows)
                    .map(|row| ($(items.$index[row % items.$index.len()].clone(),)+))
                    .collect();
                values.push(($(shortest::<$item>(),)+));
                values.push(($(longest::<$item>(),)+));
                values
            }

            fn arbitrary() -> BoxedStrategy<Self::Value> {
                ($($item::arbitrary(),)+).boxed()
            }
        }
    };
}

sample_tuple!(A 0, B 1);
sample_tuple!(A 0, B 1, C 2);
sample_tuple!(A 0, B 1, C 2, D 3);
sample_tuple!(A 0, B 1, C 2, D 3, E 4);
sample_tuple!(A 0, B 1, C 2, D 3, E 4, F 5);

impl<T: WireSample, const MIN: usize, const MAX: usize> WireSample for List<T, MIN, MAX> {
    /// The shortest list, the longest list, and a list of the item's
    /// boundary values, repeated or cut to fit the count bounds.
    fn boundary_values() -> Vec<Vec<T::Value>> {
        let items = T::boundary_values();
        let count = items.len().clamp(MIN, MAX);
        let mixed = items.iter().cycle().take(count).cloned().collect();
        vec![vec![shortest::<T>(); MIN], vec![longest::<T>(); MAX], mixed]
    }

    fn arbitrary() -> BoxedStrategy<Vec<T::Value>> {
        proptest::collection::vec(T::arbitrary(), MIN..=MAX).boxed()
    }
}
