//! Tests for trusted preallocation during deserialization.

use proptest::{collection::size_range, prelude::*};

use std::matches;

use crate::serialization::{
    arbitrary::max_allocation_is_big_enough,
    zcash_deserialize::{MAX_INITIAL_ALLOCATION, MAX_U8_ALLOCATION},
    CompactSizeMessage, SerializationError, TrustedPreallocate, ZcashDeserialize, ZcashSerialize,
    MAX_PROTOCOL_MESSAGE_LEN,
};

impl TrustedPreallocate for u8 {
    fn max_allocation() -> u64 {
        // MAX_PROTOCOL_MESSAGE_LEN takes up 5 bytes when encoded as a CompactSize.
        (MAX_PROTOCOL_MESSAGE_LEN - 5)
            .try_into()
            .expect("MAX_PROTOCOL_MESSAGE_LEN fits in u64")
    }
}

proptest! {
#![proptest_config(ProptestConfig::with_cases(4))]

    #[test]
    /// Confirm that deserialize yields the expected result for any vec smaller than `MAX_U8_ALLOCATION`
    fn u8_ser_deser_roundtrip(input in any_with::<Vec<u8>>(size_range(MAX_U8_ALLOCATION).lift()) ) {
        let serialized = input.zcash_serialize_to_vec().expect("Serialization to vec must succeed");
        let cursor = std::io::Cursor::new(serialized);
        let deserialized = <Vec<u8>>::zcash_deserialize(cursor).expect("deserialization from vec must succeed");
        prop_assert_eq!(deserialized, input)
    }
}

#[test]
/// Confirm that deserialize allows vectors with length up to and including `MAX_U8_ALLOCATION`
fn u8_deser_accepts_max_valid_input() {
    let serialized = vec![0u8; MAX_U8_ALLOCATION]
        .zcash_serialize_to_vec()
        .expect("Serialization to vec must succeed");
    let cursor = std::io::Cursor::new(serialized);
    let deserialized = <Vec<u8>>::zcash_deserialize(cursor);
    assert!(deserialized.is_ok())
}

#[test]
/// Confirm that rejects vectors longer than `MAX_U8_ALLOCATION`
fn u8_deser_throws_when_input_too_large() {
    let serialized = vec![0u8; MAX_U8_ALLOCATION + 1]
        .zcash_serialize_to_vec()
        .expect("Serialization to vec must succeed");
    let cursor = std::io::Cursor::new(serialized);
    let deserialized = <Vec<u8>>::zcash_deserialize(cursor);

    assert!(matches!(
        deserialized,
        Err(SerializationError::Parse(
            "Byte vector longer than MAX_U8_ALLOCATION"
        ))
    ))
}

#[test]
/// Confirm that every u8 takes exactly 1 byte when serialized.
/// This verifies that our calculated `MAX_U8_ALLOCATION` is indeed an upper bound.
fn u8_size_is_correct() {
    for byte in u8::MIN..=u8::MAX {
        let serialized = byte
            .zcash_serialize_to_vec()
            .expect("Serialization to vec must succeed");
        assert!(serialized.len() == 1)
    }
}

#[test]
/// Verify that...
/// 1. The smallest disallowed `Vec<u8>` is too big to include in a Zcash Wire Protocol message
/// 2. The largest allowed `Vec<u8>`is exactly the size of a maximal Zcash Wire Protocol message
fn u8_max_allocation_is_correct() {
    let (
        smallest_disallowed_vec_len,
        smallest_disallowed_serialized_len,
        largest_allowed_vec_len,
        largest_allowed_serialized_len,
    ) = max_allocation_is_big_enough(0u8);

    // Confirm that shortest_disallowed_vec is only one item larger than the limit
    assert_eq!((smallest_disallowed_vec_len - 1), MAX_U8_ALLOCATION);
    // Confirm that shortest_disallowed_vec is too large to be included in a valid zcash message
    assert!(smallest_disallowed_serialized_len > MAX_PROTOCOL_MESSAGE_LEN);

    // Check that our largest_allowed_vec contains the maximum number of items
    assert_eq!(largest_allowed_vec_len, MAX_U8_ALLOCATION);
    // Check that our largest_allowed_vec is the size of a maximal protocol message
    assert_eq!(largest_allowed_serialized_len, MAX_PROTOCOL_MESSAGE_LEN);
}

/// A reader that supplies `remaining` zero bytes, then reports end of file,
/// and records the largest buffer it was asked to fill.
struct TruncatedReader {
    /// The number of bytes left to supply.
    remaining: usize,

    /// The largest `read()` buffer seen so far.
    max_read_len: usize,
}

impl std::io::Read for TruncatedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.max_read_len = self.max_read_len.max(buf.len());

        let len = buf.len().min(self.remaining);
        buf[..len].fill(0);
        self.remaining -= len;

        Ok(len)
    }
}

#[test]
/// Confirm that a short message declaring a near-maximal byte vector length does not
/// make the deserializer allocate that length before the bytes arrive.
///
/// `read_exact()` is handed the tail of the output buffer that the deserializer has
/// grown so far, so the largest buffer the reader sees tracks how much the deserializer
/// allocates as it goes. A peer that declares `MAX_U8_ALLOCATION` bytes and then ends
/// the message used to force a two megabyte allocation from a few hundred bytes of
/// input, which is the byte vector case of GHSA-xr93-pcq3-pxf8.
///
/// This proxy has one blind spot: it can not see a `Vec::with_capacity(external_count)`
/// that is followed by chunked reads, because that reserves the full length while still
/// handing the reader small buffers. Safe Rust can not observe the capacity from the
/// reader side, and a counting global allocator needs `unsafe`, which this workspace
/// denies. So the deserializer carries a matching comment telling the reader never to
/// pre-reserve the declared length.
fn u8_deser_does_not_preallocate_declared_length() {
    /// The number of body bytes the peer actually sends.
    const SUPPLIED_LEN: usize = 512;

    /// The largest buffer the deserializer may hand to the reader. The chunked read grows
    /// the buffer in `MAX_INITIAL_ALLOCATION` steps, so one chunk is the exact maximum;
    /// the factor of two leaves room to retune the chunk size without editing this test.
    const MAX_ALLOWED_READ_LEN: usize = 2 * MAX_INITIAL_ALLOCATION;

    // A CompactSize length prefix for `MAX_U8_ALLOCATION`, followed by a truncated body.
    let mut serialized = Vec::new();
    CompactSizeMessage::try_from(MAX_U8_ALLOCATION)
        .expect("MAX_U8_ALLOCATION is a valid CompactSize")
        .zcash_serialize(&mut serialized)
        .expect("serialization to vec must succeed");

    let mut reader = std::io::Read::chain(
        std::io::Cursor::new(serialized),
        TruncatedReader {
            remaining: SUPPLIED_LEN,
            max_read_len: 0,
        },
    );

    let deserialized = <Vec<u8>>::zcash_deserialize(&mut reader);

    // The message ends before the declared length, so it must be rejected.
    assert!(
        matches!(&deserialized, Err(SerializationError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof),
        "truncated byte vector must be rejected with UnexpectedEof, got: {deserialized:?}"
    );

    let max_read_len = reader.into_inner().1.max_read_len;
    assert!(
        max_read_len <= MAX_ALLOWED_READ_LEN,
        "a {SUPPLIED_LEN} byte message declaring {MAX_U8_ALLOCATION} bytes allocated a \
         {max_read_len} byte buffer, which is above the {MAX_ALLOWED_READ_LEN} byte limit"
    );
}
