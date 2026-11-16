//! Compact block relay messages: their encodings, and the gate that drops them unless the
//! connection negotiated and asked for them.

use std::time::Duration;

use tokio::sync::watch;

use super::*;

const SENDCMPCT: &[u8; 12] = b"sendcmpct\0\0\0";
const CMPCTBLOCK: &[u8; 12] = b"cmpctblock\0\0";
const GETBLOCKTXN: &[u8; 12] = b"getblocktxn\0";
const BLOCKTXN: &[u8; 12] = b"blocktxn\0\0\0\0";

fn frame(command: &[u8; 12], body: &[u8]) -> BytesMut {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&Network::Mainnet.magic().0);
    bytes.extend_from_slice(command);
    bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&sha256d::Checksum::from(body).0);
    bytes.extend_from_slice(body);
    BytesMut::from(&bytes[..])
}

/// `frame` with a checksum that doesn't match `body`.
fn bad_checksum_frame(command: &[u8; 12], body: &[u8]) -> BytesMut {
    let mut bytes = frame(command, body);
    bytes[20] ^= 1;
    bytes
}

fn ping() -> BytesMut {
    frame(b"ping\0\0\0\0\0\0\0\0", &7u64.to_le_bytes())
}

fn full_codec() -> Codec {
    let mut codec = Codec::builder().for_network(&Network::Mainnet).finish();
    codec.reconfigure_full_body_len();
    codec
}

fn test_block() -> Block {
    Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1687108_BYTES[..]).unwrap()
}

fn compact_block(block: &Block) -> Message {
    Message::CompactBlock(Arc::new(CompactBlock::from_block(block, 23).unwrap()))
}

fn send_compact(announce: bool, version: u64) -> Message {
    Message::SendCompact { announce, version }
}

fn sendcmpct_body(announce: u8, version: u64) -> Vec<u8> {
    let mut body = vec![announce];
    body.extend_from_slice(&version.to_le_bytes());
    body
}

fn encode(codec: &mut Codec, message: Message) {
    codec.encode(message, &mut BytesMut::new()).unwrap();
}

/// A codec that negotiated compact blocks and asked its peer for version 1 announcements.
fn requesting_codec() -> Codec {
    let mut codec = full_codec();
    codec.reconfigure_compact_blocks(true);
    encode(&mut codec, send_compact(true, 1));
    codec
}

/// Tracks an outstanding `getblocktxn` request for `hash` with a 30 second deadline.
fn track_recovery(
    codec: &mut Codec,
    hash: block::Hash,
) -> watch::Sender<Option<(block::Hash, Instant)>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let (sender, receiver) = watch::channel(Some((hash, deadline)));
    codec.reconfigure_compact_recovery(receiver);
    sender
}

/// Every compact block relay message round trips when the connection accepts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_messages_round_trip() {
    let _init_guard = zakura_test::init();
    let block = test_block();
    let mut codec = requesting_codec();
    let _recovery = track_recovery(&mut codec, block.hash());

    let messages = [
        send_compact(true, 1),
        send_compact(false, u64::MAX),
        send_compact(true, 1),
        compact_block(&block),
        Message::GetBlockTransactions(BlockTransactionsRequest {
            block_hash: block.hash(),
            indexes: vec![0, 1, 255],
        }),
        Message::BlockTransactions(BlockTransactions {
            block_hash: block.hash(),
            transactions: block.transactions.clone(),
        }),
    ];
    for message in messages {
        let mut bytes = BytesMut::new();
        codec.encode(message.clone(), &mut bytes).unwrap();
        assert_eq!(
            &bytes[4..4 + message.command().len()],
            message.command().as_bytes()
        );
        assert_eq!(codec.decode(&mut bytes).unwrap(), Some(message));
        assert!(bytes.is_empty());
    }
}

/// Malformed bodies of accepted compact block relay messages are parse errors, and invalid
/// messages fail to encode instead of panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_compact_messages_are_errors() {
    let _init_guard = zakura_test::init();
    let block = test_block();
    let mut codec = requesting_codec();
    encode(&mut codec, compact_block(&block));
    let _recovery = track_recovery(&mut codec, block::Hash([0; 32]));

    assert!(codec
        .decode(&mut frame(SENDCMPCT, &sendcmpct_body(2, 1)))
        .is_err());
    assert!(codec.decode(&mut frame(SENDCMPCT, &[1])).is_err());
    assert!(codec.decode(&mut frame(CMPCTBLOCK, &[])).is_err());
    let mut indexes = vec![0; 32];
    indexes.extend_from_slice(&[1, 0xfd, 0, 0]);
    assert!(codec.decode(&mut frame(GETBLOCKTXN, &indexes)).is_err());
    let mut count = vec![0; 32];
    count.extend_from_slice(&[0xfe, 255, 255, 255, 255]);
    assert!(codec.decode(&mut frame(BLOCKTXN, &count)).is_err());

    let mut invalid = CompactBlock::from_block(&block, 23).unwrap();
    invalid.prefilled.clear();
    assert!(codec
        .encode(
            Message::CompactBlock(Arc::new(invalid)),
            &mut BytesMut::new()
        )
        .is_err());
    assert!(codec
        .encode(
            Message::GetBlockTransactions(BlockTransactionsRequest {
                block_hash: block.hash(),
                indexes: vec![2, 1],
            }),
            &mut BytesMut::new()
        )
        .is_err());
}

/// Each compact block relay message is accepted only when its own condition holds. A
/// dropped message is skipped like an unknown command, even with a bad checksum, and the
/// next message still decodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_message_needs_its_own_condition() {
    let _init_guard = zakura_test::init();
    let block = test_block();
    for negotiated in [false, true] {
        for (announce, version) in [(false, 1), (true, 1), (true, 2)] {
            for sent_compact_block in [false, true] {
                let mut codec = full_codec();
                codec.reconfigure_compact_blocks(negotiated);
                encode(&mut codec, send_compact(announce, version));
                if sent_compact_block {
                    encode(&mut codec, compact_block(&block));
                }

                // The peer's own `sendcmpct` changes nothing this codec accepts.
                for (peer_announce, peer_version) in [(0, 1), (1, 1), (1, 2)] {
                    let peer_sendcmpct = sendcmpct_body(peer_announce, peer_version);
                    let decoded = codec
                        .decode(&mut frame(SENDCMPCT, &peer_sendcmpct))
                        .unwrap();
                    assert_eq!(decoded.is_some(), negotiated);

                    for (command, accepted) in [
                        (CMPCTBLOCK, negotiated && announce && version == 1),
                        (GETBLOCKTXN, negotiated && sent_compact_block),
                        (BLOCKTXN, false),
                    ] {
                        let case = format!(
                            "{} negotiated={negotiated} announce={announce} version={version} \
                             sent={sent_compact_block}",
                            escape_command(command),
                        );
                        // An empty body is malformed, so an accepted message is an error.
                        let decoded = codec.decode(&mut frame(command, &[]));
                        assert_eq!(decoded.is_err(), accepted, "{case}");

                        if !accepted {
                            let mut bytes = bad_checksum_frame(command, &[0; 64]);
                            bytes.extend_from_slice(&ping());
                            assert!(
                                matches!(codec.decode(&mut bytes), Ok(Some(Message::Ping(_)))),
                                "{case}"
                            );
                            assert!(bytes.is_empty(), "{case}");
                        }
                    }
                }
            }
        }
    }

    // An unnegotiated `sendcmpct` is dropped before its checksum is checked.
    let mut bytes = bad_checksum_frame(SENDCMPCT, &sendcmpct_body(1, 1));
    assert_eq!(full_codec().decode(&mut bytes).unwrap(), None);
    assert!(bytes.is_empty());
}

/// A `blocktxn` is accepted while it answers the outstanding request, even after this node
/// stops asking for announcements, and is dropped once the request ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocktxn_answering_the_request_is_accepted() {
    let _init_guard = zakura_test::init();
    let block = test_block();
    let mut codec = requesting_codec();
    let recovery = track_recovery(&mut codec, block.hash());
    encode(&mut codec, send_compact(false, 1));
    assert_eq!(codec.decode(&mut frame(CMPCTBLOCK, &[])).unwrap(), None);

    let response = Message::BlockTransactions(BlockTransactions {
        block_hash: block.hash(),
        transactions: vec![block.transactions[0].clone()],
    });
    let mut bytes = BytesMut::new();
    codec.encode(response.clone(), &mut bytes).unwrap();
    let mut late = bytes.clone();
    assert_eq!(codec.decode(&mut bytes).unwrap(), Some(response));

    recovery.send_replace(None);
    assert_eq!(codec.decode(&mut late).unwrap(), None);
    assert!(late.is_empty());
}

/// A `blocktxn` is dropped before its checksum or body is read when no request is
/// outstanding, the request is for another block, its deadline passed, it was cancelled, its
/// sender is gone, or compact blocks were not negotiated.
#[tokio::test]
async fn unrequested_blocktxn_is_dropped_before_hashing() {
    let hash = block::Hash([1; 32]);
    for case in [
        "no request",
        "another block",
        "expired",
        "cancelled",
        "sender dropped",
        "not negotiated",
    ] {
        let mut codec = requesting_codec();
        let recovery = track_recovery(&mut codec, hash);
        let mut body = hash.0.to_vec();
        match case {
            "no request" => {
                codec = requesting_codec();
            }
            "another block" => body[0] ^= 1,
            "expired" => {
                recovery.send_replace(Some((hash, Instant::now())));
            }
            "cancelled" => {
                recovery.send_replace(None);
            }
            "sender dropped" => drop(recovery),
            "not negotiated" => codec.reconfigure_compact_blocks(false),
            _ => unreachable!("every case is listed"),
        }
        // A malformed transaction count and a bad checksum must not be reached.
        body.push(0xff);
        let mut bytes = bad_checksum_frame(BLOCKTXN, &body);
        assert_eq!(codec.decode(&mut bytes).unwrap(), None, "{case}");
        assert!(bytes.is_empty(), "{case}");
    }
}
