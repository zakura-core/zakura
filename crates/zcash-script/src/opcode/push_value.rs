//! Constant values represented as opcodes.

#![allow(non_camel_case_types)]

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use bounded_vec::{BoundedVec, EmptyBoundedVec};

use crate::{num, opcode, script::Asm, signature};

/// Data values that aren’t represented within their opcode byte.
///
/// TODO: These should have lower bounds that can prevent non-minimal encodings, but that requires
///       at least `const_generic_exprs`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LargeValue {
    /// NB: The lower bound here is 1 because `PushdataBytelength([;0])` has the same encoding as
    ///     [`OP_0`].
    PushdataBytelength(BoundedVec<u8, 1, 0x4b>),
    /// A value whose byte length can fit into a single byte.
    OP_PUSHDATA1(EmptyBoundedVec<u8, 0xff>),
    /// A value whose byte length can fit into two bytes.
    OP_PUSHDATA2(EmptyBoundedVec<u8, { Self::MAX_SIZE }>),
    /// NB: This constructor is only possible when [`crate::interpreter::Flags::MinimalData`] isn't set.
    OP_PUSHDATA4(EmptyBoundedVec<u8, { Self::MAX_SIZE }>),
}

use LargeValue::*;

impl LargeValue {
    const PUSHDATA1_BYTE: u8 = 0x4c;
    const PUSHDATA2_BYTE: u8 = 0x4d;
    const PUSHDATA4_BYTE: u8 = 0x4e;

    /// The maximum number of bytes able to be stored in a single [`super::PushValue`].
    pub(crate) const MAX_SIZE: usize = 520; // bytes

    /// The number of bytes this requires in a script.
    pub(crate) fn byte_len(&self) -> usize {
        1 + match self {
            PushdataBytelength(data) => data.as_slice().len(),
            OP_PUSHDATA1(data) => 1 + data.as_slice().len(),
            OP_PUSHDATA2(data) => 2 + data.as_slice().len(),
            OP_PUSHDATA4(data) => 4 + data.as_slice().len(),
        }
    }

    /// Returns a [`LargeValue`] as minimally-encoded as possible. That is, values that
    /// should be minimally-encoded as [`SmallValue`]s will be [`LargeValue`].
    pub(crate) fn from_slice(v: &[u8]) -> Option<LargeValue> {
        if let Ok(bv) = BoundedVec::try_from(v.to_vec()) {
            Some(PushdataBytelength(bv))
        } else if let Ok(bv) = BoundedVec::try_from(v.to_vec()) {
            Some(OP_PUSHDATA1(bv))
        } else if let Ok(bv) = BoundedVec::try_from(v.to_vec()) {
            Some(OP_PUSHDATA2(bv))
        } else if let Ok(bv) = BoundedVec::try_from(v.to_vec()) {
            Some(OP_PUSHDATA4(bv))
        } else {
            None
        }
    }

    fn split_value(script: &[u8], needed_bytes: usize) -> (Result<&[u8], opcode::Error>, &[u8]) {
        match script.split_at_checked(needed_bytes) {
            None => (
                Err(opcode::Error::Read {
                    expected_bytes: needed_bytes,
                    available_bytes: script.len(),
                }),
                &[],
            ),
            Some((value, remainder)) => (
                // NB: This check would ideally be done before the `split_at_checked` call, but the
                //     C++ impl reads the bytes before checking if the size is too large.
                if needed_bytes <= Self::MAX_SIZE {
                    Ok(value)
                } else {
                    Err(opcode::Error::PushSize(Some(needed_bytes)))
                },
                remainder,
            ),
        }
    }

    /// First splits `size_size` bytes to determine the size of the value to read, then splits the
    /// value.
    fn split_tagged_value(
        script: &[u8],
        size_size: usize,
    ) -> (Result<&[u8], opcode::Error>, &[u8]) {
        let (res, rem) = Self::split_value(script, size_size);
        match res {
            Err(_) => (res, rem),
            Ok(bytes) => {
                let mut size = 0;
                for byte in bytes.iter().rev() {
                    size <<= 8;
                    size |= usize::from(*byte);
                }
                Self::split_value(rem, size)
            }
        }
    }

    /// Parse a single [`LargeValue`] from a script. Returns `None` if the first byte doesn’t
    /// correspond to a [`LargeValue`].
    pub(crate) fn parse(script: &[u8]) -> Option<(Result<LargeValue, opcode::Error>, &[u8])> {
        match script.split_first() {
            None => Some((
                Err(opcode::Error::Read {
                    expected_bytes: 1,
                    available_bytes: 0,
                }),
                &[],
            )),
            Some((leading_byte, script)) => match leading_byte {
                0x01..LargeValue::PUSHDATA1_BYTE => {
                    let (res, rem) = Self::split_value(script, (*leading_byte).into());
                    Some((
                        res.map(|v| {
                            PushdataBytelength(v.to_vec().try_into().expect("fits into BoundedVec"))
                        }),
                        rem,
                    ))
                }
                &LargeValue::PUSHDATA1_BYTE => {
                    let (res, rem) = Self::split_tagged_value(script, 1);
                    Some((
                        res.map(|v| {
                            OP_PUSHDATA1(v.to_vec().try_into().expect("fits into BoundedVec"))
                        }),
                        rem,
                    ))
                }
                &LargeValue::PUSHDATA2_BYTE => {
                    let (res, rem) = Self::split_tagged_value(script, 2);
                    Some((
                        res.map(|v| {
                            OP_PUSHDATA2(v.to_vec().try_into().expect("fits into BoundedVec"))
                        }),
                        rem,
                    ))
                }
                &LargeValue::PUSHDATA4_BYTE => {
                    let (res, rem) = Self::split_tagged_value(script, 4);
                    Some((
                        res.map(|v| {
                            OP_PUSHDATA4(v.to_vec().try_into().expect("fits into BoundedVec"))
                        }),
                        rem,
                    ))
                }
                _ => None,
            },
        }
    }

    /// Get the [`crate::interpreter::Stack`] element represented by this [`LargeValue`].
    pub(crate) fn value(&self) -> &[u8] {
        match self {
            PushdataBytelength(v) => v.as_slice(),
            OP_PUSHDATA1(v) => v.as_slice(),
            OP_PUSHDATA2(v) => v.as_slice(),
            OP_PUSHDATA4(v) => v.as_slice(),
        }
    }

    /// Returns false if there is a smaller possible encoding of the provided value.
    pub(crate) fn is_minimal_push(&self) -> bool {
        match self {
            PushdataBytelength(data) => match data.as_slice() {
                [b] => *b != 0x81 && (*b < 1 || 16 < *b),
                _ => true,
            },
            OP_PUSHDATA1(data) => usize::from(Self::PUSHDATA1_BYTE) <= data.as_slice().len(),
            OP_PUSHDATA2(data) => 0x100 <= data.as_slice().len(),
            OP_PUSHDATA4(data) => 0x10000 <= data.as_slice().len(),
        }
    }

    /// Returns the numeric value represented by the opcode, if one exists.
    pub(crate) fn to_num(&self) -> Result<i64, num::Error> {
        num::parse(
            self.value(),
            false,
            // To ensure that any encoding supported by `num` is supported here.
            Some(usize::MAX),
        )
    }
}

impl From<&LargeValue> for Vec<u8> {
    fn from(value: &LargeValue) -> Self {
        let to_vec = |prefix: Option<u8>, contents: &[u8]| {
            prefix
                .into_iter()
                .chain(num::serialize(
                    contents.len().try_into().expect("upper bound fits in i64"),
                ))
                .chain(contents.iter().copied())
                .collect()
        };

        match value {
            PushdataBytelength(bv) => to_vec(None, bv.as_slice()),
            OP_PUSHDATA1(bv) => to_vec(Some(LargeValue::PUSHDATA1_BYTE), bv.as_slice()),
            OP_PUSHDATA2(bv) => to_vec(Some(LargeValue::PUSHDATA2_BYTE), bv.as_slice()),
            OP_PUSHDATA4(bv) => to_vec(Some(LargeValue::PUSHDATA4_BYTE), bv.as_slice()),
        }
    }
}

impl Asm for LargeValue {
    fn to_asm(&self, attempt_sighash_decode: bool) -> String {
        // The logic below follows the zcashd implementation in its
        // `ScriptToAsmStr()`
        // https://github.com/zcash/zcash/blob/2352fbc1ed650ac4369006bea11f7f20ee046b84/src/core_write.cpp#L73-L115
        let mut value = self.value().to_vec();
        let mut hash_type = String::new();
        #[cfg(feature = "signature-validation")]
        if attempt_sighash_decode && value.len() > 4 {
            if let signature::Validity::Valid(signature) =
                signature::Decoded::from_bytes(&value, false, true)
            {
                value = signature.sig().serialize_der().to_vec();
                hash_type = format!("[{}]", signature.sighash_type().to_asm(false));
            }
        }
        if value.len() <= 4 {
            // zcashd ultimately uses `CScriptNum()`-> `set_vch()`, which was
            // replaced with `num::parse()` in this crate
            let n = num::parse(&value, false, Some(8)).unwrap_or(0);
            return n.to_string();
        }
        // hex::encode(), but avoids the `hex` dependency
        value
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("")
            + &hash_type
    }
}

/// Data values represented entirely by their opcode byte.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(missing_docs)]
#[repr(u8)]
pub enum SmallValue {
    // push value
    OP_0 = 0x00,
    OP_1NEGATE = 0x4f,
    OP_1 = 0x51,
    OP_2 = 0x52,
    OP_3 = 0x53,
    OP_4 = 0x54,
    OP_5 = 0x55,
    OP_6 = 0x56,
    OP_7 = 0x57,
    OP_8 = 0x58,
    OP_9 = 0x59,
    OP_10 = 0x5a,
    OP_11 = 0x5b,
    OP_12 = 0x5c,
    OP_13 = 0x5d,
    OP_14 = 0x5e,
    OP_15 = 0x5f,
    OP_16 = 0x60,
}

use SmallValue::*;

impl SmallValue {
    /// Decodes this opcode from its byte encoding.
    pub(super) fn decode(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::OP_0),
            0x4f => Some(Self::OP_1NEGATE),
            0x51 => Some(Self::OP_1),
            0x52 => Some(Self::OP_2),
            0x53 => Some(Self::OP_3),
            0x54 => Some(Self::OP_4),
            0x55 => Some(Self::OP_5),
            0x56 => Some(Self::OP_6),
            0x57 => Some(Self::OP_7),
            0x58 => Some(Self::OP_8),
            0x59 => Some(Self::OP_9),
            0x5a => Some(Self::OP_10),
            0x5b => Some(Self::OP_11),
            0x5c => Some(Self::OP_12),
            0x5d => Some(Self::OP_13),
            0x5e => Some(Self::OP_14),
            0x5f => Some(Self::OP_15),
            0x60 => Some(Self::OP_16),
            _ => None,
        }
    }

    /// Returns the byte encoding of this opcode.
    pub(crate) fn encode(self) -> u8 {
        // This is how you get the discriminant, but using `as` everywhere is too much code smell
        self as u8
    }

    /// Get the [`crate::interpreter::Stack`] element represented by this [`SmallValue`].
    pub(crate) fn value(&self) -> Vec<u8> {
        match self {
            OP_0 => vec![],
            OP_1NEGATE => vec![0x81],
            _ => vec![self.encode() - (OP_1.encode() - 1)],
        }
    }

    /// Returns the numeric value of the opcode. It will always be in the range -1..=16.
    pub(crate) fn to_num(self) -> i8 {
        match self {
            OP_0 => 0,
            OP_1NEGATE => -1,
            OP_1 => 1,
            OP_2 => 2,
            OP_3 => 3,
            OP_4 => 4,
            OP_5 => 5,
            OP_6 => 6,
            OP_7 => 7,
            OP_8 => 8,
            OP_9 => 9,
            OP_10 => 10,
            OP_11 => 11,
            OP_12 => 12,
            OP_13 => 13,
            OP_14 => 14,
            OP_15 => 15,
            OP_16 => 16,
        }
    }
}

impl Asm for SmallValue {
    /// zcashd prints small values script elements as numbers (e.g. OP_1 → "1").
    /// https://github.com/zcash/zcash/blob/v6.11.0/src/script/script.cpp#L19-L40
    fn to_asm(&self, _attempt_sighash_decode: bool) -> String {
        match self {
            OP_0 => "0",
            OP_1NEGATE => "-1",
            OP_1 => "1",
            OP_2 => "2",
            OP_3 => "3",
            OP_4 => "4",
            OP_5 => "5",
            OP_6 => "6",
            OP_7 => "7",
            OP_8 => "8",
            OP_9 => "9",
            OP_10 => "10",
            OP_11 => "11",
            OP_12 => "12",
            OP_13 => "13",
            OP_14 => "14",
            OP_15 => "15",
            OP_16 => "16",
        }
        .to_string()
    }
}
