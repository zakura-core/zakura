//! Typed wire items with static length bounds.

use std::marker::PhantomData;

use zakura_chain::{
    block,
    serialization::{ZcashDeserialize, ZcashSerialize},
};

use super::{BoundedReader, WireError};

/// One field or list item with a known encoded length range.
///
/// `MIN_LEN` and `MAX_LEN` bound every valid encoding. List descriptors use
/// `MIN_LEN` to reject a count that the remaining payload cannot hold, and
/// message rules derive their payload bounds from both constants.
pub trait Wire {
    /// The decoded value.
    type Value;
    /// Smallest valid encoding in bytes.
    const MIN_LEN: usize;
    /// Largest valid encoding in bytes.
    const MAX_LEN: usize;

    /// Append the encoding of `value` to `out`.
    fn encode(value: &Self::Value, out: &mut Vec<u8>) -> Result<(), WireError>;

    /// Decode one value from `reader`.
    fn decode(reader: &mut BoundedReader<'_>) -> Result<Self::Value, WireError>;
}

/// A little-endian `u32`.
#[derive(Debug)]
pub enum LeU32 {}

impl Wire for LeU32 {
    type Value = u32;
    const MIN_LEN: usize = 4;
    const MAX_LEN: usize = 4;

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

    fn encode(value: &block::Height, out: &mut Vec<u8>) -> Result<(), WireError> {
        LeU32::encode(&value.0, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<block::Height, WireError> {
        let raw = reader.read::<LeU32>()?;
        block::Height::try_from(raw).map_err(|_| WireError::OutOfRange("block height"))
    }
}

/// A Zcash-serialized item whose valid encodings are `MIN..=MAX` bytes.
///
/// The adapter does not check the bounds on decode: the Zcash codec and the
/// payload length already bound the read. The bounds feed list and rule
/// arithmetic, and the conformance suite checks them against real encodings.
#[derive(Debug)]
pub struct Zcash<T, const MIN: usize, const MAX: usize>(PhantomData<fn() -> T>);

impl<T, const MIN: usize, const MAX: usize> Wire for Zcash<T, MIN, MAX>
where
    T: ZcashSerialize + ZcashDeserialize,
{
    type Value = T;
    const MIN_LEN: usize = MIN;
    const MAX_LEN: usize = MAX;

    fn encode(value: &T, out: &mut Vec<u8>) -> Result<(), WireError> {
        value.zcash_serialize(out)?;
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<T, WireError> {
        reader.zcash()
    }
}

/// A 32-byte block hash.
pub type HashItem = Zcash<block::Hash, 32, 32>;
