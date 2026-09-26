//! Check network rules at the legacy message boundary, including allocation
//! checks for header counts that only fit under Regtest's smaller encoding.

use super::*;
use std::sync::Arc;
use zakura_chain::work::equihash::Solution;
use zakura_test::allocations::measure;

fn network_block(network: &Network) -> Arc<Block> {
    let mut block =
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    Arc::make_mut(&mut block.header).solution = Solution::for_proposal_for_network(network);
    Arc::new(block)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_and_headers_messages_follow_the_codec_network() {
    let _guard = zakura_test::init();
    let networks = [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ];
    for configured_network in &networks {
        for encoded_network in &networks {
            let block = network_block(encoded_network);
            for message in [
                Message::Headers(vec![block::CountedHeader {
                    header: block.header.clone(),
                }]),
                Message::Block(block.clone()),
            ] {
                let mut codec = Codec::builder()
                    .for_network(configured_network)
                    .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
                    .finish();
                let mut bytes = BytesMut::new();
                codec.encode(message.clone(), &mut bytes).unwrap();
                let result = codec.decode(&mut bytes);
                if configured_network.is_regtest() == encoded_network.is_regtest() {
                    assert_eq!(result.unwrap(), Some(message));
                } else {
                    assert!(result.is_err());
                }
            }
        }
    }
}

#[test]
fn header_counts_use_the_network_minimum_before_allocating() {
    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ] {
        let codec = Codec::builder().for_network(&network).finish();
        let header = block::CountedHeader {
            header: network_block(&network).header.clone(),
        };
        let mut payload = vec![header; 2].zcash_serialize_to_vec().unwrap();
        payload.pop();
        let mut bytes = payload.as_slice();
        let (result, allocations) = measure(|| codec.read_headers(&mut bytes));
        assert!(matches!(
            result,
            Err(Error::Parse("Vector exceeds available input"))
        ));
        assert_eq!(allocations.requested_bytes, 0);
        assert_eq!(bytes, &payload[1..], "only the count should have been read");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_messages_reject_missing_inputs_at_the_count() {
    let _guard = zakura_test::init();
    let mut codec = Codec::builder().finish();
    // Version 1, declaring 1,024 transparent inputs with no input bytes.
    let payload = [1, 0, 0, 0, 253, 0, 4];
    let mut bytes = payload.as_slice();
    assert!(matches!(
        codec.read_tx(&mut bytes),
        Err(Error::Parse("Vector exceeds available input"))
    ));
    assert!(bytes.is_empty());

    // The same codec still accepts a complete transaction after rejection.
    codec.reconfigure_full_body_len();
    let transaction = Transaction::zcash_deserialize(&zakura_test::vectors::DUMMY_TX1[..]).unwrap();
    let message = Message::Tx(Arc::new(transaction));
    let mut encoded = BytesMut::new();
    codec.encode(message.clone(), &mut encoded).unwrap();
    assert_eq!(codec.decode(&mut encoded).unwrap(), Some(message));
    assert!(encoded.is_empty());
}
