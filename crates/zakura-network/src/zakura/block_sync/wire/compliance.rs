//! Canonical response codecs and separately measured allocation requirements.

use super::*;
use proptest::prelude::*;
use zakura_chain::{
    primitives::{Groth16Proof, Halo2Proof},
    serialization::{CompactSizeMessage, TrustedPreallocate, ZcashReader},
    transaction::Transaction,
    transparent::{Input, Output, Script},
};
use zakura_test::allocations::measure;

fn assert_unfunded_collection<T: ZcashDeserialize + TrustedPreallocate>(
    count: usize,
    minimum_wire_bytes: usize,
    missing: usize,
) {
    let data = vec![0; count * minimum_wire_bytes - missing.min(minimum_wire_bytes)];
    let (result, allocations) =
        measure(|| ZcashReader::from_slice(&mut data.as_slice()).read_external_count::<T>(count));
    assert!(result.is_err());
    assert_eq!(
        allocations.requested_bytes,
        0,
        "F03: {} cannot reserve {count} elements from {} bytes: {allocations:?}",
        std::any::type_name::<T>(),
        data.len(),
    );
}

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
fn f03_fixed_request_and_terminal_fields_do_not_allocate() {
    for tag in [2, 4, 5] {
        for count in [1, 128] {
            let bytes = terminal(tag, block::Height::MAX.0 - 127, count);
            let (result, stats) = measure(|| BlockSyncMessage::decode(&bytes));
            assert!(result.is_ok());
            assert_eq!(stats.requested_bytes, 0, "F03 fixed fields: {stats:?}");
            assert_eq!(stats.retained_bytes, 0);
        }
    }
}

#[test]
fn f03_missing_transactions_do_not_allocate_a_peer_selected_collection() {
    let block =
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let mut bytes = vec![3];
    block.header.zcash_serialize(&mut bytes).unwrap();
    // Observe the same complete header followed by an empty or a large declared
    // transaction vector, with zero transaction bytes available in both cases.
    let mut empty = bytes.clone();
    empty.push(0);
    let (_, baseline) = measure(|| BlockSyncMessage::decode(&empty));
    bytes.extend_from_slice(&[253, 0, 4]); // canonical CompactSize(1024)
    let (result, measured) = measure(|| BlockSyncMessage::decode(&bytes));
    assert!(result.is_err());
    assert!(measured.largest_request <= baseline.largest_request,
        "F03: zero remaining transactions cannot fund a collection allocation: baseline={baseline:?}, actual={measured:?}");
}

#[test]
fn f03_actual_retained_decode_allocations_are_observed_separately_from_wire_bytes() {
    let bytes = BlockSyncMessage::Block(Arc::new(
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap(),
    ))
    .encode()
    .unwrap();
    let (result, stats) = measure(|| BlockSyncMessage::decode(&bytes));
    let BlockSyncMessage::Block(body) = result.unwrap() else {
        panic!("Block fixture");
    };
    assert!(
        stats.requests > 0,
        "the allocator must actually be installed"
    );
    assert!(stats.retained_bytes > 0);
    assert!(stats.peak_live_bytes >= stats.retained_bytes);
    // The attributed-size API explicitly excludes Arc control blocks. This V1
    // fixture owns one Block, its Header, and its transaction Arc allocations.
    let arc_overhead = (2 + body.transactions.len()) * 2 * std::mem::size_of::<usize>();
    let bound = body.attributed_memory_size_bytes() + u64::try_from(arc_overhead).unwrap();
    assert!(u64::try_from(stats.retained_bytes).unwrap() <= bound,
        "F03: measured retained allocations exceed decoded objects plus Arc controls: {stats:?}, bound={bound}");
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
    fn f03_generated_complete_transactions_preserve_decoder_results(tx in any::<Transaction>()) {
        let encoded = tx.zcash_serialize_to_vec().unwrap();
        let mut streamed_bytes = encoded.as_slice();
        let mut bounded_bytes = encoded.as_slice();
        let streamed = Transaction::zcash_deserialize(&mut streamed_bytes);
        let bounded = Transaction::zcash_deserialize_from_slice(&mut bounded_bytes);
        prop_assert_eq!(bounded.is_ok(), streamed.is_ok());
        if let Ok(expected) = streamed {
            prop_assert_eq!(bounded.unwrap(), expected);
            prop_assert_eq!(bounded_bytes, streamed_bytes);
        }
    }

    #[test]
    fn f03_generated_collections_reject_missing_input_before_allocation(
        count in 1usize..=64,
        missing in 1usize..=400,
    ) {
        // Codec minima from the wire layout, independent of production's
        // TrustedPreallocate declarations. V5 split arrays use their own sizes.
        assert_unfunded_collection::<Transaction>(count, 10, missing);
        assert_unfunded_collection::<Input>(count, 41, missing);
        assert_unfunded_collection::<Output>(count, 9, missing);
        assert_unfunded_collection::<Groth16Proof>(count, 192, missing);
        assert_unfunded_collection::<zakura_chain::sapling::Spend<zakura_chain::sapling::PerSpendAnchor>>(count, 384, missing);
        assert_unfunded_collection::<zakura_chain::sapling::SpendPrefixInTransactionV5>(count, 96, missing);
        assert_unfunded_collection::<zakura_chain::sapling::OutputInTransactionV4>(count, 948, missing);
        assert_unfunded_collection::<zakura_chain::sapling::OutputPrefixInTransactionV5>(count, 756, missing);
        assert_unfunded_collection::<zakura_chain::orchard::Action>(count, 820, missing);
    }

    #[test]
    fn f03_generated_nested_byte_strings_reject_before_allocation(
        declared in 1usize..=8_193,
        supplied_fraction in 0usize..=99,
    ) {
        let supplied = declared * supplied_fraction / 100;
        let mut bytes = Vec::new();
        CompactSizeMessage::try_from(declared).unwrap().zcash_serialize(&mut bytes).unwrap();
        bytes.resize(bytes.len() + supplied, 0);
        let (script, script_alloc) = measure(|| Script::zcash_deserialize_from_slice(&mut bytes.as_slice()));
        let (proof, proof_alloc) = measure(|| Halo2Proof::zcash_deserialize_from_slice(&mut bytes.as_slice()));
        let (string, string_alloc) = measure(|| String::zcash_deserialize_from_slice(&mut bytes.as_slice()));
        prop_assert!(script.is_err() && proof.is_err() && string.is_err());
        for observed in [script_alloc, proof_alloc, string_alloc] {
            prop_assert_eq!(observed.requested_bytes, 0, "F03 nested byte string: {:?}", observed);
        }
    }

    #[test]
    fn f03_generated_complete_proof_arrays_bound_allocation_at_growth_edges(
        count in prop_oneof![0usize..=2_049, proptest::sample::select(vec![0usize, 1, 1_023, 1_024, 1_025, 2_047, 2_048, 2_049])],
    ) {
        let bytes = vec![0; count * 192];
        let (proofs, observed) = measure(|| {
            ZcashReader::from_slice(&mut bytes.as_slice()).read_external_count::<Groth16Proof>(count)
        });
        let proofs = proofs.unwrap();
        prop_assert_eq!(proofs.len(), count);
        prop_assert!(observed.largest_request <= count * std::mem::size_of::<Groth16Proof>());
        prop_assert!(observed.peak_live_bytes <= count * std::mem::size_of::<Groth16Proof>());
    }

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
