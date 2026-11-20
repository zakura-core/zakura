//! A received `block` message reserves a receipt order once its block header has arrived.

use std::sync::Mutex;

use tokio::time::Instant;
use zakura_chain::work::equihash::Solution;

use super::*;
use crate::protocol::external::InventoryHash;

/// The first order [`reserving_codec`] hands out.
const FIRST_ORDER: u64 = 100;

/// The verified tip [`reserving_codec`] reports.
const TIP: block::Height = block::Height(7);

/// The number of bytes of a `block` message after which a Mainnet block header is complete.
const BLOCK_HEADER_END: usize = HEADER_LEN + block::MAX_SERIALIZED_HEADER_BYTES;

/// A codec after its handshake that reserves increasing orders, and the block hashes it was
/// asked to reserve for.
fn reserving_codec() -> (Codec, Arc<Mutex<Vec<block::Hash>>>) {
    let reserved = Arc::new(Mutex::new(Vec::new()));
    let recorded = reserved.clone();
    let reserve: ReserveBlockOrder = Arc::new(move |hash| {
        let mut recorded = recorded.lock().unwrap();
        let earlier = u64::try_from(recorded.len()).expect("a test reserves a few orders");
        recorded.push(hash);
        Some(BlockReservation {
            hash,
            order: FIRST_ORDER + earlier,
            verified_tip: TIP,
            floor: TIP,
            started_at: Instant::now(),
        })
    });
    let mut codec = Codec::builder()
        .with_block_order_reservation(Some(reserve))
        .finish();
    codec.reconfigure_full_body_len();
    (codec, reserved)
}

fn block() -> Arc<Block> {
    Arc::new(
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..])
            .expect("block test vector should deserialize"),
    )
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

/// The message header alone reserves nothing, so a peer cannot hold a place open for a block
/// it has not mined yet. The order is reserved when the last byte of the block header arrives,
/// for that header's hash, and before the transactions arrive.
#[test]
fn a_block_message_reserves_its_order_once_its_block_header_has_arrived() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let block = block();
    let frame = frame(Message::Block(block.clone().into()));
    assert!(
        frame.len() > BLOCK_HEADER_END,
        "the block has transactions after its header"
    );

    let mut src = BytesMut::from(&frame[..BLOCK_HEADER_END - 1]);
    assert_eq!(codec.decode(&mut src).expect("header is valid"), None);
    assert!(
        reserved.lock().unwrap().is_empty(),
        "nothing is reserved until the whole block header is here",
    );

    src.extend_from_slice(&frame[BLOCK_HEADER_END - 1..BLOCK_HEADER_END]);
    let before_reserving = Instant::now();
    assert_eq!(codec.decode(&mut src).expect("header is valid"), None);
    let after_reserving = Instant::now();
    assert_eq!(
        *reserved.lock().unwrap(),
        [block.hash()],
        "the order is reserved for the block's hash before its transactions arrive",
    );

    src.extend_from_slice(&frame[BLOCK_HEADER_END..]);
    let Some(Message::Block(received)) = codec.decode(&mut src).expect("block decodes") else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(
        reserved.lock().unwrap().len(),
        1,
        "one reservation per message"
    );
    assert_eq!(received.hash(), block.hash());
    let reservation = received
        .reservation()
        .expect("the decoded block carries its reservation");
    assert_eq!(reservation.hash, block.hash());
    assert_eq!(reservation.order, FIRST_ORDER);
    assert_eq!(reservation.verified_tip, TIP);
    assert!(
        (before_reserving..=after_reserving).contains(&reservation.started_at),
        "the start is when the block header arrived",
    );

    // The next block message reserves the next order.
    let mut src = frame.clone();
    let Some(Message::Block(again)) = codec.decode(&mut src).expect("block decodes") else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(again.reservation().map(|r| r.order), Some(FIRST_ORDER + 1));
}

#[test]
fn a_block_message_arriving_a_byte_at_a_time_reserves_once() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let block = block();
    let frame = frame(Message::Block(block.clone().into()));

    let mut src = BytesMut::new();
    let mut decoded = None;
    for byte in &frame[..] {
        src.extend_from_slice(&[*byte]);
        decoded = codec.decode(&mut src).expect("block decodes");
    }
    let Some(Message::Block(received)) = decoded else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(*reserved.lock().unwrap(), [block.hash()]);
    assert_eq!(received.reservation().map(|r| r.order), Some(FIRST_ORDER));
}

/// A Regtest block can be smaller than a Mainnet block header, so its header is only known to
/// be complete when the whole message is.
#[test]
fn a_block_smaller_than_a_full_size_header_reserves_when_it_is_complete() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let mut block = (*block()).clone();
    Arc::make_mut(&mut block.header).solution = Solution::Regtest([0; 36]);
    let block = Arc::new(block);
    let frame = frame(Message::Block(block.clone().into()));
    assert!(frame.len() < BLOCK_HEADER_END);

    let mut src = BytesMut::from(&frame[..frame.len() - 1]);
    assert_eq!(codec.decode(&mut src).expect("header is valid"), None);
    assert!(reserved.lock().unwrap().is_empty());

    src.extend_from_slice(&frame[frame.len() - 1..]);
    let Some(Message::Block(received)) = codec.decode(&mut src).expect("block decodes") else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(*reserved.lock().unwrap(), [block.hash()]);
    assert_eq!(received.reservation().map(|r| r.hash), Some(block.hash()));
}

#[test]
fn a_body_that_does_not_start_with_a_block_header_reserves_nothing() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let mut src = frame(Message::Block(block().into()));
    // No block header has version zero.
    src[HEADER_LEN..HEADER_LEN + 4].fill(0);

    assert!(codec.decode(&mut src).is_err());
    assert!(reserved.lock().unwrap().is_empty());
}

#[test]
fn only_block_messages_reserve_an_order() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
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
    assert!(reserved.lock().unwrap().is_empty());

    // A codec without a reservation function, or whose node cannot rank blocks yet, decodes
    // blocks without one.
    let no_tip: ReserveBlockOrder = Arc::new(|_| None);
    for reserve in [None, Some(no_tip)] {
        let mut codec = Codec::builder()
            .with_block_order_reservation(reserve)
            .finish();
        codec.reconfigure_full_body_len();
        let mut src = frame(Message::Block(block.clone().into()));
        let Some(Message::Block(received)) = codec.decode(&mut src).expect("block decodes") else {
            panic!("a block frame decodes to a block message");
        };
        assert_eq!(received.reservation(), None);
    }
}

#[test]
fn a_block_failing_its_checksum_yields_no_reservation() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let block = block();
    let mut src = frame(Message::Block(block.clone().into()));
    let last = src.len() - 1;
    src[last] ^= 0xff;

    assert!(codec.decode(&mut src).is_err());
    assert_eq!(
        *reserved.lock().unwrap(),
        [block.hash()],
        "the reservation is taken at the block header and left unused",
    );
}
