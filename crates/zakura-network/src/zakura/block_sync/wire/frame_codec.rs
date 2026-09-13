//! Check that GetBlocks messages have one valid byte representation.
//!
//! Requests and endings use fixed fields. Blocks use the chain decoder. These
//! tests cover both, including size boundaries and conflicting message tags.
//! Allocation checks here cover rejection before body decoding. Nested allocation
//! bounds live in `bounded_decoding`.

use super::*;
use proptest::prelude::*;
use zakura_test::allocations::measure;

fn terminal(tag: u8, height: u32, count: u32) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&height.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes
}

fn check_terminal(tag: u8, height: u32, count: u32) {
    let bytes = terminal(tag, height, count);
    let expected = height <= block::Height::MAX.0 && (1..=128).contains(&count);
    let decoded = BlockSyncMessage::decode(&bytes);
    assert_eq!(decoded.is_ok(), expected, "F02: {tag}/{height}/{count}");
    let outbound = if tag == 4 {
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(height),
            returned: count,
        }
    } else {
        BlockSyncMessage::RangeUnavailable {
            start_height: block::Height(height),
            count,
        }
    };
    assert_eq!(
        outbound.encode().is_ok(),
        expected,
        "F02 outbound boundary: tag={tag}, height={height}, count={count}"
    );
    if let Ok(decoded) = decoded {
        assert_eq!(decoded.encode().unwrap(), bytes);
        assert_eq!(decoded, outbound);
    }
    for end in 0..bytes.len() {
        assert!(
            BlockSyncMessage::decode(&bytes[..end]).is_err(),
            "F02 truncation {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(BlockSyncMessage::decode(&trailing).is_err());
    for (message_type, flags) in [
        (u16::from(tag), 1),
        (u16::from(tag), u16::MAX),
        (3, 0),
        (0, 0),
        (u16::MAX, 0),
    ] {
        assert!(BlockSyncMessage::decode_frame(Frame {
            message_type,
            flags,
            payload: bytes.clone()
        })
        .is_err());
    }
}

#[test]
fn f02_terminal_boundaries_in_both_directions() {
    for tag in [4, 5] {
        for height in [
            0,
            1,
            block::Height::MAX.0,
            block::Height::MAX.0 + 1,
            u32::MAX,
        ] {
            for count in [0, 1, 128, 129, u32::MAX] {
                check_terminal(tag, height, count);
            }
        }
    }
}

#[test]
fn f02_complete_block_truncations_tags_and_noncanonical_transaction_count() {
    let body =
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let encoded = BlockSyncMessage::Block(Arc::new(body.clone()))
        .encode()
        .unwrap();
    assert_eq!(
        BlockSyncMessage::decode(&encoded)
            .unwrap()
            .encode()
            .unwrap(),
        encoded
    );
    for end in 0..encoded.len() {
        assert!(
            BlockSyncMessage::decode(&encoded[..end]).is_err(),
            "F02 Block truncation {end}"
        );
    }
    let header_len = body.header.zcash_serialized_size();
    assert_eq!(
        encoded[1 + header_len],
        1,
        "fixture has exactly one transaction"
    );
    let mut noncanonical = encoded[..=header_len].to_vec();
    noncanonical.extend_from_slice(&[253, 1, 0]);
    noncanonical.extend_from_slice(&encoded[2 + header_len..]);
    assert!(BlockSyncMessage::decode(&noncanonical).is_err());
    for flags in [1, u16::MAX] {
        assert!(BlockSyncMessage::decode_frame(Frame {
            message_type: 3,
            flags,
            payload: encoded.clone()
        })
        .is_err());
    }
    for tag in [0, 2, 4, 5, u16::MAX] {
        assert!(BlockSyncMessage::decode_frame(Frame {
            message_type: tag,
            flags: 0,
            payload: encoded.clone()
        })
        .is_err());
    }
}

#[test]
fn f02_exact_maximum_block_and_one_byte_excess_use_the_real_codec() {
    let mut body =
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let outputs = match Arc::make_mut(&mut body.transactions[0]) {
        zakura_chain::transaction::Transaction::V1 { outputs, .. } => outputs,
        _ => panic!("the height-one fixture uses a V1 transaction"),
    };
    outputs[0].lock_script = zakura_chain::transparent::Script::new(&vec![0; 2_000_000]);
    let excess = body.zcash_serialized_size() - 2_000_000;
    let outputs = match Arc::make_mut(&mut body.transactions[0]) {
        zakura_chain::transaction::Transaction::V1 { outputs, .. } => outputs,
        _ => unreachable!(),
    };
    outputs[0].lock_script = zakura_chain::transparent::Script::new(&vec![0; 2_000_000 - excess]);
    assert_eq!(body.zcash_serialized_size(), 2_000_000);
    let payload = BlockSyncMessage::Block(Arc::new(body.clone()))
        .encode()
        .unwrap();
    assert_eq!(payload.len(), 2_000_001);
    assert_eq!(
        BlockSyncMessage::decode(&payload)
            .unwrap()
            .encode()
            .unwrap(),
        payload
    );
    let outputs = match Arc::make_mut(&mut body.transactions[0]) {
        zakura_chain::transaction::Transaction::V1 { outputs, .. } => outputs,
        _ => unreachable!(),
    };
    outputs[0].lock_script = zakura_chain::transparent::Script::new(&vec![0; 2_000_001 - excess]);
    assert!(BlockSyncMessage::Block(Arc::new(body.clone()))
        .encode()
        .is_err());
    let mut oversized = vec![3];
    body.zcash_serialize(&mut oversized).unwrap();
    assert!(BlockSyncMessage::decode(&oversized).is_err());
}

#[test]
fn f04_discriminator_mismatch_is_rejected_before_block_decode_allocation() {
    let payload = BlockSyncMessage::Block(Arc::new(
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap(),
    ))
    .encode()
    .unwrap();
    let frame = Frame {
        message_type: 4,
        flags: 0,
        payload,
    };
    let (result, stats) = measure(|| BlockSyncMessage::decode_frame(frame));
    assert!(result.is_err());
    assert_eq!(
        stats.requested_bytes, 0,
        "F04: header/payload tag mismatch must fail before allocating a Block: {stats:?}"
    );
}

proptest! {
    #[test]
    fn f02_generated_terminal_fields(tag in prop_oneof![Just(4u8), Just(5u8)], height in any::<u32>(), count in prop_oneof![1u32..=MAX_BS_BLOCKS_PER_REQUEST, any::<u32>()]) {
        check_terminal(tag, height, count);
    }

    #[test]
    fn f02_generated_legal_terminals(tag in prop_oneof![Just(4u8), Just(5u8)], height in 0u32..=block::Height::MAX.0, count in 1u32..=128) {
        check_terminal(tag, height, count);
    }

    #[test]
    fn f02_bounded_response_payloads_never_panic(tag in 3u8..=5, mut bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        bytes.insert(0, tag);
        if let Ok(decoded) = BlockSyncMessage::decode(&bytes) {
            prop_assert_eq!(decoded.encode().unwrap(), bytes);
        }
    }
}
