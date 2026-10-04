//! Wire items: typed fields with static length and allocation bounds.

use std::fmt;

use zakura_chain::block;

use super::{BoundedReader, WireError};
use crate::zakura::PayloadLen;

/// One field of a payload, with bounds that hold for every valid encoding.
///
/// Items compose. A tuple of items is an item, and so is a [`List`] of items,
/// so a message's whole payload is one item type. Its bounds follow from its
/// parts by arithmetic, and a row takes them with [`PayloadLen::of`].
///
/// The item test suite checks each item's bounds against real encodings:
/// `MIN_LEN` and `MAX_LEN` must be reached exactly, and no decode may request
/// more heap than `max_heap_bytes`.
///
/// [`List`]: super::List
pub trait Wire {
    /// The decoded value.
    type Value: Clone + fmt::Debug + PartialEq;

    /// Smallest valid encoding in bytes.
    const MIN_LEN: usize;

    /// Largest valid encoding in bytes.
    const MAX_LEN: usize;

    /// Most heap bytes that decoding may request while reading at most
    /// `input_len` bytes, whether the decode succeeds or fails.
    ///
    /// The bound must not decrease as `input_len` grows.
    fn max_heap_bytes(input_len: usize) -> usize;

    /// Append the encoding of `value`.
    ///
    /// Fails if `value` has no valid encoding, such as a list outside its
    /// count bounds.
    fn encode(value: &Self::Value, out: &mut Vec<u8>) -> Result<(), WireError>;

    /// Decode one value, reading exactly its encoding from `reader`.
    fn decode(reader: &mut BoundedReader<'_>) -> Result<Self::Value, WireError>;
}

impl PayloadLen {
    /// The payload bounds of a message whose payload is the item `L`.
    ///
    /// ```
    /// # use zakura_network::zakura::{wire_codec::{HeightLe, LeU32}, PayloadLen};
    /// const GET_ITEMS: PayloadLen = PayloadLen::of::<(HeightLe, LeU32)>();
    /// assert_eq!(GET_ITEMS, PayloadLen::exact(8));
    /// ```
    pub const fn of<L: Wire>() -> Self {
        Self::between(L::MIN_LEN, L::MAX_LEN)
    }
}

/// One byte.
#[derive(Debug)]
pub enum U8 {}

impl Wire for U8 {
    type Value = u8;
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 1;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &u8, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.push(*value);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<u8, WireError> {
        let [value] = reader.array("u8")?;
        Ok(value)
    }
}

/// A little-endian `u32`.
#[derive(Debug)]
pub enum LeU32 {}

impl Wire for LeU32 {
    type Value = u32;
    const MIN_LEN: usize = 4;
    const MAX_LEN: usize = 4;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &u32, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(reader.array("u32")?))
    }
}

/// A block height as a little-endian `u32`, at most [`block::Height::MAX`].
#[derive(Debug)]
pub enum HeightLe {}

impl Wire for HeightLe {
    type Value = block::Height;
    const MIN_LEN: usize = 4;
    const MAX_LEN: usize = 4;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &block::Height, out: &mut Vec<u8>) -> Result<(), WireError> {
        if *value > block::Height::MAX {
            return Err(WireError::OutOfRange("block height"));
        }
        LeU32::encode(&value.0, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<block::Height, WireError> {
        let height = block::Height(reader.read::<LeU32>()?);
        if height > block::Height::MAX {
            return Err(WireError::OutOfRange("block height"));
        }
        Ok(height)
    }
}

/// Implement [`Wire`] for a tuple: its items in order, with summed bounds.
macro_rules! wire_tuple {
    ($($item:ident $index:tt),+) => {
        impl<$($item: Wire),+> Wire for ($($item,)+) {
            type Value = ($($item::Value,)+);
            const MIN_LEN: usize = 0 $(+ $item::MIN_LEN)+;
            const MAX_LEN: usize = 0 $(+ $item::MAX_LEN)+;

            fn max_heap_bytes(input_len: usize) -> usize {
                // Each item reads at most `input_len` bytes.
                0usize $(.saturating_add($item::max_heap_bytes(input_len)))+
            }

            fn encode(value: &Self::Value, out: &mut Vec<u8>) -> Result<(), WireError> {
                $($item::encode(&value.$index, out)?;)+
                Ok(())
            }

            fn decode(reader: &mut BoundedReader<'_>) -> Result<Self::Value, WireError> {
                Ok(($(reader.read::<$item>()?,)+))
            }
        }
    };
}

wire_tuple!(A 0, B 1);
wire_tuple!(A 0, B 1, C 2);
wire_tuple!(A 0, B 1, C 2, D 3);
wire_tuple!(A 0, B 1, C 2, D 3, E 4);
wire_tuple!(A 0, B 1, C 2, D 3, E 4, F 5);
