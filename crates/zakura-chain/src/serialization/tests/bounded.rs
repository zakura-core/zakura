//! Actual-input bounds, nested decoding, and streaming compatibility.

use std::io::{self, Read};

use crate::{
    block::{Block, Hash},
    primitives::{Groth16Proof, Halo2Proof},
    serialization::{
        zcash_deserialize_bytes_external_count, CompactSizeMessage, SerializationError,
        TrustedPreallocate, ZcashDeserialize, ZcashReader, ZcashSerialize,
    },
    transaction::Transaction,
    transparent::Script,
};

#[test]
fn nested_limits_preserve_available_bytes_and_advance_the_parent() {
    let mut bytes = &[1, 2, 3, 4][..];
    let mut reader = ZcashReader::from_slice(&mut bytes);
    {
        let mut child = reader.with_limit(2);
        assert_eq!(child.remaining_bytes(), Some(2));
        assert!(child.read_bytes(3).is_err());
        assert_eq!(child.remaining_bytes(), Some(2));
        assert_eq!(child.read_bytes(2).unwrap(), [1, 2]);
        assert_eq!(child.remaining_bytes(), Some(0));
    }
    assert_eq!(reader.remaining_bytes(), Some(2));
    assert_eq!(reader.read_bytes(2).unwrap(), [3, 4]);
    assert!(bytes.is_empty());

    let mut stream = ZcashReader::from_stream(io::Cursor::new([1, 2]));
    assert_eq!(stream.with_limit(100).remaining_bytes(), None);
}

#[test]
fn counted_collections_reject_missing_input_before_decoding_elements() {
    // These bytes would decode as valid hashes if the count were satisfiable.
    for remaining in [0, 1, 31, 32, 63] {
        let data = vec![0; remaining];
        let mut bytes = data.as_slice();
        let mut reader = ZcashReader::from_slice(&mut bytes);
        assert!(matches!(
            reader.read_external_count::<Hash>(2),
            Err(SerializationError::Parse("Vector exceeds available input"))
        ));
        assert_eq!(reader.remaining_bytes(), Some(remaining));
    }

    let mut bytes = &[0; 64][..];
    let hashes = ZcashReader::from_slice(&mut bytes)
        .read_external_count::<Hash>(2)
        .unwrap();
    assert_eq!(hashes, [Hash([0; 32]); 2]);
    assert_eq!(hashes.capacity(), 2);
    assert!(bytes.is_empty());
}

#[test]
fn nested_byte_strings_reject_unfunded_counts() {
    let mut data = CompactSizeMessage::try_from(5_000)
        .unwrap()
        .zcash_serialize_to_vec()
        .unwrap();
    data.extend([0; 4]);
    for result in [
        Script::zcash_deserialize_from_slice(&mut data.as_slice()).map(|_| ()),
        Halo2Proof::zcash_deserialize_from_slice(&mut data.as_slice()).map(|_| ()),
        String::zcash_deserialize_from_slice(&mut data.as_slice()).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(SerializationError::Parse(
                "Byte vector exceeds available input"
            ))
        ));
    }
}

#[test]
fn external_proof_arrays_use_their_own_encoded_element_size() {
    // V5 proofs are stored separately from their spend/output prefixes.
    let minimum = usize::try_from(Groth16Proof::min_serialized_size()).unwrap();
    let bytes = vec![0; minimum];
    assert!(ZcashReader::from_slice(&mut bytes.as_slice())
        .read_external_count::<Groth16Proof>(1)
        .is_ok());
    assert!(matches!(
        ZcashReader::from_slice(&mut &bytes[..minimum - 1]).read_external_count::<Groth16Proof>(1),
        Err(SerializationError::Parse("Vector exceeds available input"))
    ));
}

#[test]
fn growth_does_not_reserve_past_the_declared_count() {
    for count in [0, 1, 1_023, 1_024, 1_025, 2_047, 2_048, 2_049, 4_097] {
        let data = vec![7; count];
        let bounded = ZcashReader::from_slice(&mut data.as_slice())
            .read_bytes(count)
            .unwrap();
        let streamed = zcash_deserialize_bytes_external_count(count, data.as_slice()).unwrap();
        assert_eq!(bounded, data);
        assert_eq!(streamed, data);
        assert_eq!(bounded.capacity(), count);
        assert_eq!(streamed.capacity(), count);
    }
}

#[test]
fn bounded_transaction_minimum_is_a_codec_bound() {
    // An empty V1 transaction is structurally decodable, even though consensus
    // requires inputs/outputs. A consensus-valid minimum would reject it here.
    let mut bytes = &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0][..];
    let txs = ZcashReader::from_slice(&mut bytes)
        .read_external_count::<Transaction>(1)
        .unwrap();
    assert_eq!(txs.len(), 1);
    assert!(bytes.is_empty());
    assert_eq!(txs[0].zcash_serialize_to_vec().unwrap().len(), 10);
}

#[test]
fn bounded_blocks_match_streaming_fixtures_and_preserve_trailing_input() {
    for &encoded in zakura_test::vectors::BLOCKS.iter() {
        let expected = Block::zcash_deserialize(encoded).unwrap();
        let mut with_trailing = encoded.to_vec();
        with_trailing.extend([0x12, 0x34]);
        let mut bytes = with_trailing.as_slice();
        let actual = Block::zcash_deserialize_from_slice(&mut bytes).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(bytes, [0x12, 0x34]);
    }
}

#[test]
fn external_streaming_implementations_remain_usable_but_do_not_claim_bounds() {
    #[derive(Debug)]
    struct StreamingByte(u8);

    impl ZcashDeserialize for StreamingByte {
        fn zcash_deserialize<R: Read>(mut reader: R) -> Result<Self, SerializationError> {
            let mut byte = [0];
            reader.read_exact(&mut byte)?;
            Ok(Self(byte[0]))
        }
    }

    impl TrustedPreallocate for StreamingByte {
        fn max_allocation() -> u64 {
            10
        }
    }

    let streamed = Vec::<StreamingByte>::zcash_deserialize(&[1, 7][..]).unwrap();
    assert_eq!(streamed[0].0, 7);
    assert!(StreamingByte::zcash_deserialize_from_slice(&mut &[7][..]).is_err());
    assert!(Vec::<StreamingByte>::zcash_deserialize_from_slice(&mut &[1, 7][..]).is_err());
}
