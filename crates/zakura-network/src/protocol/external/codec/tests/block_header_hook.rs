//! A received `block` message reports its block hash once its block header has arrived.

use std::sync::Mutex;

use zakura_chain::work::equihash::Solution;

use super::*;
use crate::protocol::external::InventoryHash;

/// The number of bytes of a `block` message after which a Mainnet block header is complete.
const BLOCK_HEADER_END: usize = HEADER_LEN + block::MAX_SERIALIZED_HEADER_BYTES;

/// A codec after its handshake, and the block hashes it reported to its block header hook.
fn reporting_codec() -> (Codec, Arc<Mutex<Vec<block::Hash>>>) {
    let reported = Arc::new(Mutex::new(Vec::new()));
    let recorded = reported.clone();
    let hook: BlockHeaderHook = Arc::new(move |hash| recorded.lock().unwrap().push(hash));
    let mut codec = Codec::builder().finish();
    codec.reconfigure_full_body_len();
    codec.report_block_headers_to(Some(hook));
    (codec, reported)
}

fn block() -> Arc<Block> {
    Arc::new(
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..])
            .expect("block test vector should deserialize"),
    )
}

/// A block smaller than a Mainnet block header, because it has a Regtest solution.
fn short_block() -> Arc<Block> {
    let mut block = (*block()).clone();
    Arc::make_mut(&mut block.header).solution = Solution::Regtest([0; 36]);
    Arc::new(block)
}

/// `message` as it is sent on the wire.
fn frame(message: Message) -> BytesMut {
    let mut frame = BytesMut::new();
    let mut codec = Codec::builder().finish();
    codec.reconfigure_full_body_len();
    codec
        .encode(message, &mut frame)
        .expect("test message should encode");
    frame
}

/// The message header alone reports nothing, because it does not name a block. The hash is
/// reported when the last byte of the block header arrives, before the transactions do.
#[test]
fn a_block_message_reports_its_hash_once_its_block_header_has_arrived() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let block = block();
    let frame = frame(Message::Block(block.clone().into()));
    assert!(
        frame.len() > BLOCK_HEADER_END,
        "the block has transactions after its header"
    );

    let mut src = BytesMut::from(&frame[..BLOCK_HEADER_END - 1]);
    assert_eq!(
        codec
            .decode(&mut src)
            .expect("an incomplete message is not an error"),
        None
    );
    assert!(
        reported.lock().unwrap().is_empty(),
        "nothing is reported until the whole block header is here",
    );

    src.extend_from_slice(&frame[BLOCK_HEADER_END - 1..BLOCK_HEADER_END]);
    assert_eq!(
        codec
            .decode(&mut src)
            .expect("an incomplete message is not an error"),
        None
    );
    assert_eq!(
        *reported.lock().unwrap(),
        [block.hash()],
        "the hash is reported before the block's transactions arrive",
    );

    src.extend_from_slice(&frame[BLOCK_HEADER_END..]);
    let decoded = codec.decode(&mut src).expect("block decodes");
    assert_eq!(decoded, Some(Message::Block(block.clone().into())));
    assert_eq!(reported.lock().unwrap().len(), 1, "one report per message");

    // The next block message is reported too, whether or not it carries the same block.
    let mut src = frame.clone();
    codec.decode(&mut src).expect("block decodes");
    assert_eq!(*reported.lock().unwrap(), [block.hash(); 2]);
}

#[test]
fn a_block_message_arriving_a_byte_at_a_time_reports_once() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let block = block();
    let frame = frame(Message::Block(block.clone().into()));

    let mut src = BytesMut::new();
    let mut decoded = None;
    for byte in &frame[..] {
        src.extend_from_slice(&[*byte]);
        decoded = codec.decode(&mut src).expect("block decodes");
    }
    assert_eq!(decoded, Some(Message::Block(block.clone().into())));
    assert_eq!(*reported.lock().unwrap(), [block.hash()]);
}

/// A Regtest block can be smaller than a Mainnet block header, so its header is only known to
/// be complete when the whole message is.
#[test]
fn a_block_smaller_than_a_full_size_header_reports_when_it_is_complete() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let block = short_block();
    let frame = frame(Message::Block(block.clone().into()));
    assert!(frame.len() < BLOCK_HEADER_END);

    let mut src = BytesMut::from(&frame[..frame.len() - 1]);
    assert_eq!(
        codec
            .decode(&mut src)
            .expect("an incomplete message is not an error"),
        None
    );
    assert!(reported.lock().unwrap().is_empty());

    src.extend_from_slice(&frame[frame.len() - 1..]);
    let decoded = codec.decode(&mut src).expect("block decodes");
    assert_eq!(decoded, Some(Message::Block(block.clone().into())));
    assert_eq!(*reported.lock().unwrap(), [block.hash()]);
}

/// A short `block` body is parsed on its own, even when the bytes of the next message are
/// already buffered behind it. Read together, a truncated header and the bytes after it could
/// parse as a block header that no message carried.
#[test]
fn a_short_block_body_is_not_read_past_its_end_into_the_next_message() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();

    // A body cut off part of the way through the block header, with a matching checksum.
    let whole = frame(Message::Block(block().into()));
    let body = &whole[HEADER_LEN..HEADER_LEN + 200];
    let mut src = BytesMut::from(&whole[..HEADER_LEN]);
    src[16..20].copy_from_slice(&200u32.to_le_bytes());
    src[20..24].copy_from_slice(&message_checksum(body).0);
    src.extend_from_slice(body);
    // The rest of the header and block follow as if they were the next message.
    src.extend_from_slice(&whole[HEADER_LEN + 200..]);

    assert!(
        codec.decode(&mut src).is_err(),
        "a truncated block does not decode",
    );
    assert!(
        reported.lock().unwrap().is_empty(),
        "the bytes after the body are not part of its block header",
    );
}

#[test]
fn a_body_that_does_not_start_with_a_block_header_reports_nothing() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let mut src = frame(Message::Block(block().into()));
    // No block header has version zero.
    src[HEADER_LEN..HEADER_LEN + 4].fill(0);

    assert!(codec.decode(&mut src).is_err());
    assert!(reported.lock().unwrap().is_empty());
}

#[test]
fn only_block_messages_report_a_block_header() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let block = block();
    for message in [
        Message::Ping(Nonce(1)),
        Message::Inv(vec![InventoryHash::Block(block.hash())]),
        Message::GetData(vec![InventoryHash::Block(block.hash())]),
        Message::Tx(block.transactions[0].clone().into()),
    ] {
        let mut src = frame(message.clone());
        let decoded = codec.decode(&mut src).expect("message decodes");
        assert_eq!(decoded, Some(message));
    }
    assert!(reported.lock().unwrap().is_empty());
}

/// The report is made before the message is complete, so it cannot wait for the checksum.
#[test]
fn a_block_failing_its_checksum_has_already_reported_its_header() {
    let _init_guard = zakura_test::init();
    let (mut codec, reported) = reporting_codec();
    let block = block();
    let mut src = frame(Message::Block(block.clone().into()));
    let last = src.len() - 1;
    src[last] ^= 0xff;

    assert!(codec.decode(&mut src).is_err());
    assert_eq!(*reported.lock().unwrap(), [block.hash()]);
}
