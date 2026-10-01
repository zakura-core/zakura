//! Conversions for numbers represented as byte sequences.

use alloc::vec::Vec;

use thiserror::Error;

/// Errors that can occur when parsing integers from byte sequences.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("non-minimal encoding of script number")]
    NonMinimalEncoding(Option<Vec<u8>>),

    #[error("script number overflow: max: {max_size}, actual: {actual}")]
    Overflow { max_size: usize, actual: usize },
}

const DEFAULT_MAX_SIZE: usize = 4;

/// Convert bytes to the integer they encode.
///
/// __NB__: Setting `max_size` to more than `9` has no effect, and the special encoding of
///         [`i64::MIN`] is the only allowed 9-byte value.
pub fn parse(vch: &[u8], require_minimal: bool, max_size: Option<usize>) -> Result<i64, Error> {
    match vch.last() {
        None => Ok(0),
        Some(vch_back) => {
            let max_size = max_size.unwrap_or(DEFAULT_MAX_SIZE);
            if vch.len() > max_size {
                return Err(Error::Overflow {
                    max_size,
                    actual: vch.len(),
                });
            }
            if require_minimal {
                // Check that the number is encoded with the minimum possible number of bytes.
                //
                // If the most-significant-byte - excluding the sign bit - is zero then we're not
                // minimal. Note how this test also rejects the negative-zero encoding, 0x80.
                if (vch_back & 0x7F) == 0 {
                    // One exception: if there's more than one byte and the most significant bit of
                    // the second-most-significant-byte is set then it would have conflicted with
                    // the sign bit if one fewer byte were used, and so such encodings are minimal.
                    // An example of this is +-255, which have minimal encodings [0xff, 0x00] and
                    // [0xff, 0x80] respectively.
                    if vch.len() <= 1 || (vch[vch.len() - 2] & 0x80) == 0 {
                        return Err(Error::NonMinimalEncoding(Some(vch.to_vec())));
                    }
                }
            }

            if *vch == vec![0, 0, 0, 0, 0, 0, 0, 128, 128] {
                // Match the behaviour of the C++ code, which special-cased this encoding to avoid
                // an undefined shift of a signed type by 64 bits.
                return Ok(i64::MIN);
            };

            // Ensure defined behaviour (in Rust, left shift of `i64` by 64 bits is an arithmetic
            // overflow that may panic or give an unspecified result). The above encoding of
            // `i64::MIN` is the only allowed 9-byte encoding.
            if vch.len() > 8 {
                return Err(Error::Overflow {
                    max_size: 8,
                    actual: vch.len(),
                });
            };

            let mut result: i64 = 0;
            for (i, vch_i) in vch.iter().enumerate() {
                result |= i64::from(*vch_i) << (8 * i);
            }

            // If the input vector's most significant byte is 0x80, remove it from the result's msb
            // and return a negative.
            if vch_back & 0x80 != 0 {
                return Ok(-(result & !(0x80 << (8 * (vch.len() - 1)))));
            }

            Ok(result)
        }
    }
}

/// Convert an i64 to the corresponding byte sequence.
pub fn serialize(value: i64) -> Vec<u8> {
    if value == 0 {
        return Vec::new();
    }

    if value == i64::MIN {
        // The code below was based on buggy C++ code, that produced the "wrong" result for
        // INT64_MIN. In that case we intentionally return the result that the C++ code as compiled
        // for zcashd (with `-fwrapv`) originally produced on an x86_64 system.
        return vec![0, 0, 0, 0, 0, 0, 0, 128, 128];
    }

    let mut result = Vec::new();
    let neg = value < 0;
    let mut absvalue = value.abs();

    while absvalue != 0 {
        result.push(
            (absvalue & 0xff)
                .try_into()
                .unwrap_or_else(|_| unreachable!()),
        );
        absvalue >>= 8;
    }

    // - If the most significant byte is >= 0x80 and the value is positive, push a new zero-byte to
    //   make the significant byte < 0x80 again.
    // - If the most significant byte is >= 0x80 and the value is negative, push a new 0x80 byte
    //   that will be popped off when converting to an integral.
    // - If the most significant byte is < 0x80 and the value is negative, add 0x80 to it, since it
    //   will be subtracted and interpreted as a negative when converting to an integral.

    if result.last().map_or(true, |last| last & 0x80 != 0) {
        result.push(if neg { 0x80 } else { 0 });
    } else if neg {
        if let Some(last) = result.last_mut() {
            *last |= 0x80;
        }
    }

    result
}
