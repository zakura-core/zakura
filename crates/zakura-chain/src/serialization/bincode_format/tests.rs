//! Tests for the `bincode` 1.3 format.
//!
//! The expected bytes are what `bincode::DefaultOptions` 1.3.3 writes for the same values.

use std::collections::BTreeMap;

use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use super::{deserialize, serialize, BincodeError};

fn encoded<T: Serialize>(value: &T) -> Vec<u8> {
    serialize(value).expect("test values serialize")
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, proptest_derive::Arbitrary)]
enum Variant {
    Unit,
    Newtype(u32),
    Tuple(u8, i64),
    Struct { flag: bool, name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, proptest_derive::Arbitrary)]
struct Unit;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, proptest_derive::Arbitrary)]
struct Newtype(u16);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, proptest_derive::Arbitrary)]
struct Everything {
    flag: bool,
    small: (u8, i8),
    ints: (u16, u32, u64, i16, i32, i64),
    floats: (f32, f64),
    letter: char,
    text: String,
    #[serde(with = "serde_bytes_compat")]
    bytes: Vec<u8>,
    array: [u8; 3],
    unit: (),
    unit_struct: Unit,
    newtype: Newtype,
    maybe: Option<Vec<Option<u32>>>,
    map: BTreeMap<u32, String>,
    variants: Vec<Variant>,
}

/// Serializes `Vec<u8>` with `serialize_bytes`, like `serde_bytes`.
mod serde_bytes_compat {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        struct BytesVisitor;

        impl serde::de::Visitor<'_> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bytes")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
                Ok(bytes.to_vec())
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

#[test]
fn integers_use_the_shortest_variable_length_encoding() {
    assert_eq!(encoded(&0_u64), [0]);
    assert_eq!(encoded(&250_u64), [250]);
    assert_eq!(encoded(&251_u64), [251, 251, 0]);
    assert_eq!(encoded(&u64::from(u16::MAX)), [251, 0xff, 0xff]);
    assert_eq!(encoded(&65_536_u64), [252, 0, 0, 1, 0]);
    assert_eq!(encoded(&u64::from(u32::MAX)), [252, 0xff, 0xff, 0xff, 0xff]);
    assert_eq!(encoded(&(1_u64 << 32)), [253, 0, 0, 0, 0, 1, 0, 0, 0]);
    assert_eq!(
        encoded(&u64::MAX),
        [253, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    );

    // `u16` and `u32` use the same encoding, but `u8` is always one raw byte.
    assert_eq!(encoded(&300_u16), [251, 0x2c, 0x01]);
    assert_eq!(encoded(&300_u32), [251, 0x2c, 0x01]);
    assert_eq!(encoded(&255_u8), [255]);
}

#[test]
fn signed_integers_are_zigzag_encoded() {
    assert_eq!(encoded(&0_i64), [0]);
    assert_eq!(encoded(&-1_i64), [1]);
    assert_eq!(encoded(&1_i64), [2]);
    assert_eq!(encoded(&-2_i32), [3]);
    assert_eq!(encoded(&125_i16), [250]);
    assert_eq!(encoded(&-126_i16), [251, 251, 0]);
    assert_eq!(
        encoded(&i64::MIN),
        [253, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    );
    assert_eq!(encoded(&-1_i8), [0xff]);
}

#[test]
fn values_have_the_expected_layout() {
    assert_eq!(encoded(&true), [1]);
    assert_eq!(encoded(&1.5_f32), 1.5_f32.to_le_bytes());
    assert_eq!(encoded(&'é'), "é".as_bytes());
    assert_eq!(encoded(&"hi"), [2, b'h', b'i']);
    assert_eq!(encoded(&Option::<u8>::None), [0]);
    assert_eq!(encoded(&Some(7_u8)), [1, 7]);
    assert_eq!(encoded(&vec![1_u32, 300]), [2, 1, 251, 0x2c, 0x01]);
    assert_eq!(encoded(&[1_u8, 2, 3]), [1, 2, 3]);
    assert_eq!(encoded(&BTreeMap::from([(1_u32, true)])), [1, 1, 1]);
    assert_eq!(encoded(&(Unit, Newtype(5), ())), [5]);

    assert_eq!(encoded(&Variant::Unit), [0]);
    assert_eq!(encoded(&Variant::Newtype(9)), [1, 9]);
    assert_eq!(encoded(&Variant::Tuple(4, -1)), [2, 4, 1]);
    assert_eq!(
        encoded(&Variant::Struct {
            flag: false,
            name: "a".to_string()
        }),
        [3, 0, 1, b'a']
    );
}

#[test]
fn decoding_accepts_wider_integer_encodings_that_fit() {
    assert_eq!(deserialize::<u32>(&[251, 5, 0]), Ok(5));
    assert_eq!(deserialize::<u16>(&[252, 5, 0, 0, 0]), Ok(5));
    assert_eq!(deserialize::<u64>(&[253, 5, 0, 0, 0, 0, 0, 0, 0]), Ok(5));
    assert_eq!(
        deserialize::<u16>(&[252, 0, 0, 1, 0]),
        Err(BincodeError::IntegerOutOfRange)
    );
    assert_eq!(
        deserialize::<i16>(&[252, 0, 0, 1, 0]),
        Err(BincodeError::IntegerOutOfRange)
    );
}

#[test]
fn decoding_rejects_malformed_input() {
    assert_eq!(deserialize::<u8>(&[1, 2]), Err(BincodeError::TrailingBytes));
    assert_eq!(
        deserialize::<u32>(&[251, 5]),
        Err(BincodeError::UnexpectedEnd)
    );
    assert_eq!(deserialize::<u32>(&[]), Err(BincodeError::UnexpectedEnd));
    assert_eq!(
        deserialize::<u64>(&[254]),
        Err(BincodeError::InvalidIntegerMarker(254))
    );
    assert_eq!(
        deserialize::<u64>(&[255]),
        Err(BincodeError::InvalidIntegerMarker(255))
    );
    assert_eq!(deserialize::<bool>(&[2]), Err(BincodeError::InvalidBool(2)));
    assert_eq!(
        deserialize::<Option<u8>>(&[2]),
        Err(BincodeError::InvalidOptionTag(2))
    );
    assert_eq!(
        deserialize::<String>(&[1, 0xff]),
        Err(BincodeError::InvalidUtf8)
    );
    assert_eq!(deserialize::<char>(&[0xff]), Err(BincodeError::InvalidUtf8));
    assert!(matches!(
        deserialize::<Variant>(&[4]),
        Err(BincodeError::Custom(_))
    ));
}

#[test]
fn a_hostile_length_fails_without_allocating_it() {
    let huge_length = [253, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f];

    assert_eq!(
        deserialize::<Vec<u8>>(&huge_length),
        Err(BincodeError::UnexpectedEnd)
    );
    assert_eq!(
        deserialize::<Vec<u64>>(&huge_length),
        Err(BincodeError::UnexpectedEnd)
    );
    assert_eq!(
        deserialize::<String>(&huge_length),
        Err(BincodeError::UnexpectedEnd)
    );
    assert_eq!(
        deserialize::<BTreeMap<u32, u32>>(&huge_length),
        Err(BincodeError::UnexpectedEnd)
    );
}

#[test]
fn self_describing_and_unknown_length_values_are_rejected() {
    struct UnknownLength;

    impl Serialize for UnknownLength {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_seq(std::iter::from_fn(|| None::<u8>))
        }
    }

    assert_eq!(serialize(&UnknownLength), Err(BincodeError::LengthRequired));
    assert_eq!(
        deserialize::<serde::de::IgnoredAny>(&[0]),
        Err(BincodeError::Unsupported("deserialize_ignored_any"))
    );
}

proptest! {
    #[test]
    fn every_serde_shape_round_trips(value in any::<Everything>()) {
        // NaN isn't equal to itself, so compare floats by their bits.
        let bytes = encoded(&value);
        let decoded: Everything = deserialize(&bytes).expect("encoded values decode");

        prop_assert_eq!(encoded(&decoded), bytes);
        prop_assert_eq!(decoded.floats.0.to_bits(), value.floats.0.to_bits());
        prop_assert_eq!(decoded.floats.1.to_bits(), value.floats.1.to_bits());
    }

    #[test]
    fn decoding_arbitrary_bytes_does_not_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
        let _ = deserialize::<Everything>(&bytes);
        let _ = deserialize::<Vec<Variant>>(&bytes);
    }
}
