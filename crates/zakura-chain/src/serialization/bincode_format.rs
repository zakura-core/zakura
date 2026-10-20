//! The binary format that `bincode` 1.3 writes with `bincode::DefaultOptions`.
//!
//! Existing state databases and treestate artifacts store note commitment trees and history
//! tree snapshots in this format, so it must stay byte-for-byte compatible with `bincode` 1.3:
//!
//! - `u8`, `i8` and `bool` are single bytes. Booleans must be `0` or `1`.
//! - Other integers are variable-length. Values up to 250 are a single byte. Larger values are
//!   a marker byte (251, 252 or 253) followed by a little-endian `u16`, `u32` or `u64`. Signed
//!   integers are zigzag-encoded first. Decoding accepts a wider encoding than needed, and
//!   rejects values that don't fit the target type.
//! - Floats are little-endian, and `char`s are UTF-8.
//! - Strings, byte strings, sequences and maps start with their length as a `u64`.
//! - `Option`s start with a `0` or `1` byte, and enum variants with their index as a `u32`.
//! - Structs, tuples and fixed-size arrays are their fields in order, with no length.
//! - Decoding rejects trailing bytes.
//!
//! The format is not self-describing, so it can't decode `deserialize_any`, and it isn't
//! human-readable, so serde helpers that check `is_human_readable` choose their binary form.
//! 128-bit integers aren't supported.

use std::fmt;

use serde::{de, ser, Deserialize, Serialize};
use thiserror::Error;

#[cfg(test)]
mod tests;

/// The largest integer encoded as a single byte.
const SINGLE_BYTE_MAX: u8 = 250;
/// The marker for a little-endian `u16` integer.
const U16_MARKER: u8 = 251;
/// The marker for a little-endian `u32` integer.
const U32_MARKER: u8 = 252;
/// The marker for a little-endian `u64` integer.
const U64_MARKER: u8 = 253;

/// Serializes `value` in the `bincode` 1.3 `DefaultOptions` format.
pub fn serialize<T: ?Sized + Serialize>(value: &T) -> Result<Vec<u8>, BincodeError> {
    let mut serializer = Serializer { output: Vec::new() };
    value.serialize(&mut serializer)?;

    Ok(serializer.output)
}

/// Deserializes a `T` from all of `bytes`, which use the `bincode` 1.3 `DefaultOptions` format.
pub fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, BincodeError> {
    let mut deserializer = Deserializer { input: bytes };
    let value = T::deserialize(&mut deserializer)?;

    if deserializer.input.is_empty() {
        Ok(value)
    } else {
        Err(BincodeError::TrailingBytes)
    }
}

/// An error serializing or deserializing the `bincode` 1.3 format.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BincodeError {
    /// The input ended in the middle of a value.
    #[error("unexpected end of input")]
    UnexpectedEnd,

    /// The input has bytes left over after the value.
    #[error("input has bytes after the end of the value")]
    TrailingBytes,

    /// A boolean that isn't `0` or `1`.
    #[error("invalid boolean byte {0}")]
    InvalidBool(u8),

    /// An `Option` tag that isn't `0` or `1`.
    #[error("invalid option tag {0}")]
    InvalidOptionTag(u8),

    /// An integer marker byte that this format doesn't use.
    #[error("invalid integer marker byte {0}")]
    InvalidIntegerMarker(u8),

    /// An integer or length that doesn't fit its type.
    #[error("integer is out of range for its type")]
    IntegerOutOfRange,

    /// A string or `char` that isn't valid UTF-8.
    #[error("invalid UTF-8")]
    InvalidUtf8,

    /// A sequence or map whose length isn't known before serializing it.
    #[error("sequences and maps must have a known length")]
    LengthRequired,

    /// A serde feature that this format doesn't support.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    /// An error reported by a type's `Serialize` or `Deserialize` implementation.
    #[error("{0}")]
    Custom(String),
}

impl ser::Error for BincodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::Custom(message.to_string())
    }
}

impl de::Error for BincodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::Custom(message.to_string())
    }
}

/// Converts a length to the `u64` that the format stores.
fn length_to_u64(len: usize) -> Result<u64, BincodeError> {
    u64::try_from(len).map_err(|_| BincodeError::IntegerOutOfRange)
}

fn zigzag_encode(n: i64) -> u64 {
    // Moves the sign bit to the lowest bit, so small negative numbers stay small.
    // Reinterpreting the bits as unsigned is intended.
    ((n << 1) ^ (n >> 63)) as u64
}

fn zigzag_decode(n: u64) -> i64 {
    // Reinterpreting the bits as signed is intended.
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

struct Serializer {
    output: Vec<u8>,
}

impl Serializer {
    fn write_varint(&mut self, n: u64) {
        if let Ok(byte) = u8::try_from(n) {
            if byte <= SINGLE_BYTE_MAX {
                self.output.push(byte);
                return;
            }
        }

        if let Ok(n) = u16::try_from(n) {
            self.output.push(U16_MARKER);
            self.output.extend_from_slice(&n.to_le_bytes());
        } else if let Ok(n) = u32::try_from(n) {
            self.output.push(U32_MARKER);
            self.output.extend_from_slice(&n.to_le_bytes());
        } else {
            self.output.push(U64_MARKER);
            self.output.extend_from_slice(&n.to_le_bytes());
        }
    }

    fn write_length(&mut self, len: usize) -> Result<(), BincodeError> {
        self.write_varint(length_to_u64(len)?);
        Ok(())
    }
}

impl ser::Serializer for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<(), BincodeError> {
        self.output.push(u8::from(v));
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<(), BincodeError> {
        self.output.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<(), BincodeError> {
        self.serialize_i64(v.into())
    }

    fn serialize_i32(self, v: i32) -> Result<(), BincodeError> {
        self.serialize_i64(v.into())
    }

    fn serialize_i64(self, v: i64) -> Result<(), BincodeError> {
        self.write_varint(zigzag_encode(v));
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<(), BincodeError> {
        self.output.push(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<(), BincodeError> {
        self.serialize_u64(v.into())
    }

    fn serialize_u32(self, v: u32) -> Result<(), BincodeError> {
        self.serialize_u64(v.into())
    }

    fn serialize_u64(self, v: u64) -> Result<(), BincodeError> {
        self.write_varint(v);
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<(), BincodeError> {
        self.output.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<(), BincodeError> {
        self.output.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<(), BincodeError> {
        self.output
            .extend_from_slice(v.encode_utf8(&mut [0; 4]).as_bytes());
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<(), BincodeError> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<(), BincodeError> {
        self.write_length(v.len())?;
        self.output.extend_from_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), BincodeError> {
        self.output.push(0);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), BincodeError> {
        self.output.push(1);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), BincodeError> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), BincodeError> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<(), BincodeError> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), BincodeError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<(), BincodeError> {
        self.serialize_u32(variant_index)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self, BincodeError> {
        self.write_length(len.ok_or(BincodeError::LengthRequired)?)?;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self, BincodeError> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self, BincodeError> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, BincodeError> {
        self.serialize_u32(variant_index)?;
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self, BincodeError> {
        self.write_length(len.ok_or(BincodeError::LengthRequired)?)?;
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self, BincodeError> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, BincodeError> {
        self.serialize_u32(variant_index)?;
        Ok(self)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl ser::SerializeSeq for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeMap for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), BincodeError> {
        key.serialize(&mut **self)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeStruct for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Serializer {
    type Ok = ();
    type Error = BincodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), BincodeError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), BincodeError> {
        Ok(())
    }
}

struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    fn take(&mut self, len: usize) -> Result<&'de [u8], BincodeError> {
        let (taken, rest) = self
            .input
            .split_at_checked(len)
            .ok_or(BincodeError::UnexpectedEnd)?;
        self.input = rest;

        Ok(taken)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], BincodeError> {
        let (array, rest) = self
            .input
            .split_first_chunk()
            .ok_or(BincodeError::UnexpectedEnd)?;
        self.input = rest;

        Ok(*array)
    }

    fn read_byte(&mut self) -> Result<u8, BincodeError> {
        let [byte] = self.take_array()?;
        Ok(byte)
    }

    fn read_varint(&mut self) -> Result<u64, BincodeError> {
        match self.read_byte()? {
            byte @ 0..=SINGLE_BYTE_MAX => Ok(byte.into()),
            U16_MARKER => Ok(u16::from_le_bytes(self.take_array()?).into()),
            U32_MARKER => Ok(u32::from_le_bytes(self.take_array()?).into()),
            U64_MARKER => Ok(u64::from_le_bytes(self.take_array()?)),
            marker => Err(BincodeError::InvalidIntegerMarker(marker)),
        }
    }

    fn read_int<T: TryFrom<u64>>(&mut self) -> Result<T, BincodeError> {
        T::try_from(self.read_varint()?).map_err(|_| BincodeError::IntegerOutOfRange)
    }

    fn read_signed_int<T: TryFrom<i64>>(&mut self) -> Result<T, BincodeError> {
        T::try_from(zigzag_decode(self.read_varint()?)).map_err(|_| BincodeError::IntegerOutOfRange)
    }

    fn read_length(&mut self) -> Result<usize, BincodeError> {
        self.read_int()
    }

    /// Reads a length-prefixed byte string, borrowed from the input.
    fn read_bytes(&mut self) -> Result<&'de [u8], BincodeError> {
        let len = self.read_length()?;
        self.take(len)
    }

    fn read_str(&mut self) -> Result<&'de str, BincodeError> {
        std::str::from_utf8(self.read_bytes()?).map_err(|_| BincodeError::InvalidUtf8)
    }
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = BincodeError;

    fn deserialize_any<V: de::Visitor<'de>>(self, _visitor: V) -> Result<V::Value, BincodeError> {
        Err(BincodeError::Unsupported("deserialize_any"))
    }

    fn deserialize_bool<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        match self.read_byte()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            byte => Err(BincodeError::InvalidBool(byte)),
        }
    }

    fn deserialize_i8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_i8(i8::from_le_bytes(self.take_array()?))
    }

    fn deserialize_i16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_i16(self.read_signed_int()?)
    }

    fn deserialize_i32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_i32(self.read_signed_int()?)
    }

    fn deserialize_i64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_i64(self.read_signed_int()?)
    }

    fn deserialize_u8<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_u8(self.read_byte()?)
    }

    fn deserialize_u16<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_u16(self.read_int()?)
    }

    fn deserialize_u32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_u32(self.read_int()?)
    }

    fn deserialize_u64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_u64(self.read_varint()?)
    }

    fn deserialize_f32<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_f32(f32::from_le_bytes(self.take_array()?))
    }

    fn deserialize_f64<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_f64(f64::from_le_bytes(self.take_array()?))
    }

    fn deserialize_char<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        let first = *self.input.first().ok_or(BincodeError::UnexpectedEnd)?;
        let width = match first {
            0x00..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return Err(BincodeError::InvalidUtf8),
        };
        let bytes = self.take(width).map_err(|_| BincodeError::InvalidUtf8)?;
        let c = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.chars().next())
            .ok_or(BincodeError::InvalidUtf8)?;

        visitor.visit_char(c)
    }

    fn deserialize_str<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_borrowed_str(self.read_str()?)
    }

    fn deserialize_string<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_string(self.read_str()?.to_owned())
    }

    fn deserialize_bytes<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_borrowed_bytes(self.read_bytes()?)
    }

    fn deserialize_byte_buf<V: de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        visitor.visit_byte_buf(self.read_bytes()?.to_vec())
    }

    fn deserialize_option<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        match self.read_byte()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            tag => Err(BincodeError::InvalidOptionTag(tag)),
        }
    }

    fn deserialize_unit<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        let len = self.read_length()?;
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple<V: de::Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        visitor.visit_seq(Access {
            deserializer: self,
            remaining: len,
        })
    }

    fn deserialize_tuple_struct<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, BincodeError> {
        let len = self.read_length()?;
        visitor.visit_map(Access {
            deserializer: self,
            remaining: len,
        })
    }

    fn deserialize_struct<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, BincodeError> {
        Err(BincodeError::Unsupported("deserialize_identifier"))
    }

    fn deserialize_ignored_any<V: de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, BincodeError> {
        Err(BincodeError::Unsupported("deserialize_ignored_any"))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

/// Reads a known number of sequence elements or map entries.
///
/// The size hint is the declared length, which serde's collection implementations cap before
/// preallocating, so a hostile length can't force a large allocation.
struct Access<'a, 'de> {
    deserializer: &'a mut Deserializer<'de>,
    remaining: usize,
}

impl<'de> de::SeqAccess<'de> for Access<'_, 'de> {
    type Error = BincodeError;

    fn next_element_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, BincodeError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;

        seed.deserialize(&mut *self.deserializer).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl<'de> de::MapAccess<'de> for Access<'_, 'de> {
    type Error = BincodeError;

    fn next_key_seed<K: de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, BincodeError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;

        seed.deserialize(&mut *self.deserializer).map(Some)
    }

    fn next_value_seed<V: de::DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, BincodeError> {
        seed.deserialize(&mut *self.deserializer)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl<'de> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = BincodeError;
    type Variant = Self;

    fn variant_seed<V: de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self), BincodeError> {
        let index: u32 = self.read_int()?;
        let value = seed.deserialize(de::IntoDeserializer::<BincodeError>::into_deserializer(
            index,
        ))?;

        Ok((value, self))
    }
}

impl<'de> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = BincodeError;

    fn unit_variant(self) -> Result<(), BincodeError> {
        Ok(())
    }

    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, BincodeError> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: de::Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V: de::Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, BincodeError> {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}
