//! Checks at the native block payload boundary and buffered replay.

use super::*;
use zakura_chain::{parameters::Network, work::equihash::Solution};
use zakura_test::allocations::measure;

#[test]
fn network_bound_blocks_keep_their_rules_for_buffered_replay() {
    use super::super::reorder::BufferedBlockBody;

    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ] {
        let decoder = ZcashDecoder::for_network(&network);
        let mut block =
            block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..])
                .unwrap();
        Arc::make_mut(&mut block.header).solution = Solution::for_proposal_for_network(&network);
        let block = Arc::new(block);
        let frame = BlockSyncMessage::Block(block.clone())
            .encode_frame()
            .unwrap();
        let (message, payload) =
            BlockSyncMessage::decode_frame_with_raw_block_payload(frame, decoder).unwrap();
        assert_eq!(message, BlockSyncMessage::Block(block.clone()));
        let mut buffered =
            BufferedBlockBody::from_decoded_block(block.clone(), payload).retain_for_backlog();
        assert_eq!(buffered.decoded_block(), block);
        buffered.retain_for_backlog_in_place();
        assert_eq!(buffered.decoded_block(), block);

        let other_network = if network.is_regtest() {
            Network::Mainnet
        } else {
            Network::new_regtest(Default::default())
        };
        let frame = BlockSyncMessage::Block(block).encode_frame().unwrap();
        assert!(BlockSyncMessage::decode_frame_with_raw_block_payload(
            frame,
            ZcashDecoder::for_network(&other_network)
        )
        .is_err());
    }
}

#[test]
fn missing_transactions_are_rejected_before_collection_allocation() {
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
        "missing transactions cannot cause a collection allocation: baseline={baseline:?}, actual={measured:?}");
}
