//! A received `block` message reserves a receipt order when its header is read.

use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::protocol::external::InventoryHash;

/// The first order [`reserving_codec`] hands out.
const FIRST_ORDER: u64 = 100;

/// The verified tip [`reserving_codec`] reports.
const TIP: block::Height = block::Height(7);

/// A codec after its handshake that reserves increasing orders, and a count of its
/// reservations.
fn reserving_codec() -> (Codec, Arc<AtomicU64>) {
    let reserved = Arc::new(AtomicU64::new(0));
    let counter = reserved.clone();
    let reserve: ReserveBlockOrder =
        Arc::new(move || Some((FIRST_ORDER + counter.fetch_add(1, Ordering::Relaxed), TIP)));
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

#[test]
fn a_block_message_reserves_its_order_when_its_header_is_read() {
    let _init_guard = zakura_test::init();
    let (mut codec, reserved) = reserving_codec();
    let block = block();
    let frame = frame(Message::Block(block.clone().into()));

    let mut src = BytesMut::from(&frame[..HEADER_LEN + 10]);
    let before_header = Instant::now();
    assert_eq!(codec.decode(&mut src).expect("header is valid"), None);
    let after_header = Instant::now();
    assert_eq!(
        reserved.load(Ordering::Relaxed),
        1,
        "the order is reserved before the body arrives",
    );

    src.extend_from_slice(&frame[HEADER_LEN + 10..]);
    let Some(Message::Block(received)) = codec.decode(&mut src).expect("block decodes") else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(
        reserved.load(Ordering::Relaxed),
        1,
        "one reservation per message"
    );
    assert_eq!(received.hash(), block.hash());
    let reservation = received
        .reservation()
        .expect("the decoded block carries its reservation");
    assert_eq!(reservation.order, FIRST_ORDER);
    assert_eq!(reservation.verified_tip, TIP);
    assert!(
        (before_header..=after_header).contains(&reservation.started_at),
        "the start is when the header was read",
    );

    // The next block message reserves the next order.
    let mut src = frame.clone();
    let Some(Message::Block(again)) = codec.decode(&mut src).expect("block decodes") else {
        panic!("a block frame decodes to a block message");
    };
    assert_eq!(again.reservation().map(|r| r.order), Some(FIRST_ORDER + 1));
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
    assert_eq!(reserved.load(Ordering::Relaxed), 0);

    // A codec without a reservation function, or whose node cannot rank blocks yet, decodes
    // blocks without one.
    let no_tip: ReserveBlockOrder = Arc::new(|| None);
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
    let mut src = frame(Message::Block(block().into()));
    let last = src.len() - 1;
    src[last] ^= 0xff;

    assert!(codec.decode(&mut src).is_err());
    assert_eq!(
        reserved.load(Ordering::Relaxed),
        1,
        "the reservation is taken at the header and left unused",
    );
}
