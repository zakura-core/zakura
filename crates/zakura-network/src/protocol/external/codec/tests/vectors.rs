//! Fixed test vectors for codec.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::LazyLock;

use crate::meta_addr::MetaAddr;
use chrono::DateTime;
use futures::prelude::*;

use super::*;

static VERSION_TEST_VECTOR: LazyLock<Message> = LazyLock::new(|| {
    let services = PeerServices::NODE_NETWORK;
    let timestamp = Utc
        .timestamp_opt(1_568_000_000, 0)
        .single()
        .expect("in-range number of seconds and valid nanosecond");

    VersionMessage {
        version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        services,
        timestamp,
        address_recv: AddrInVersion::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 6)), 8233),
            services,
        ),
        address_from: AddrInVersion::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 6)), 8233),
            services,
        ),
        nonce: Nonce(0x9082_4908_8927_9238),
        user_agent: "Zebra".to_owned(),
        start_height: block::Height(540_000),
        relay: true,
    }
    .into()
});

/// Check that the version test vector serializes and deserializes correctly
#[test]
fn version_message_round_trip() {
    let (rt, _init_guard) = zakura_test::init_async();

    let v = &*VERSION_TEST_VECTOR;

    use tokio_util::codec::{FramedRead, FramedWrite};
    let v_bytes = rt.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(&mut bytes, Codec::builder().finish());
            fw.send(v.clone())
                .await
                .expect("message should be serialized");
        }
        bytes
    });

    let v_parsed = rt.block_on(async {
        let mut fr = FramedRead::new(Cursor::new(&v_bytes), Codec::builder().finish());
        fr.next()
            .await
            .expect("a next message should be available")
            .expect("that message should deserialize")
    });

    assert_eq!(*v, v_parsed);
}

/// Pruned and v2 hints survive both version and address-gossip wire encodings.
#[tokio::test]
async fn pruned_services_round_trip_in_version_and_address_messages() {
    use tokio_util::codec::{FramedRead, FramedWrite};
    let services =
        PeerServices::NODE_NETWORK | PeerServices::NODE_PRUNED | PeerServices::NODE_P2P_V2;
    let Message::Version(mut version) = VERSION_TEST_VECTOR.clone() else {
        unreachable!()
    };
    version.services = services;
    version.address_from = AddrInVersion::new(version.address_from.addr(), services);
    version.address_recv = AddrInVersion::new(version.address_recv.addr(), services);
    let addr = MetaAddr::new_gossiped_meta_addr(
        "192.0.2.1:8233".parse().unwrap(),
        services,
        version.timestamp.try_into().unwrap(),
    );
    for message in [Message::Version(version), Message::Addr(vec![addr])] {
        let mut bytes = Vec::new();
        let mut writer = FramedWrite::new(&mut bytes, Codec::builder().finish());
        writer.send(message.clone()).await.unwrap();
        drop(writer);
        let decoded = FramedRead::new(Cursor::new(bytes), Codec::builder().finish())
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded, message);
    }
}

/// Check that version deserialization rejects out-of-range timestamps with
/// an error.
#[test]
fn version_timestamp_out_of_range() {
    let v_err = deserialize_version_with_time(i64::MAX);
    assert!(
        matches!(v_err, Err(Error::Parse(_))),
        "expected error with version timestamp: {}",
        i64::MAX
    );

    let v_err = deserialize_version_with_time(i64::MIN);
    assert!(
        matches!(v_err, Err(Error::Parse(_))),
        "expected error with version timestamp: {}",
        i64::MIN
    );

    deserialize_version_with_time(1620777600).expect("recent time is valid");
    deserialize_version_with_time(0).expect("zero time is valid");
    deserialize_version_with_time(DateTime::<Utc>::MIN_UTC.timestamp()).expect("min time is valid");
    deserialize_version_with_time(DateTime::<Utc>::MAX_UTC.timestamp()).expect("max time is valid");
}

/// Deserialize a `Version` message containing `time`, and return the result.
fn deserialize_version_with_time(time: i64) -> Result<Message, Error> {
    let (rt, _init_guard) = zakura_test::init_async();

    let v = &*VERSION_TEST_VECTOR;

    use tokio_util::codec::{FramedRead, FramedWrite};
    let v_bytes = rt.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(&mut bytes, Codec::builder().finish());
            fw.send(v.clone())
                .await
                .expect("message should be serialized");
        }

        let old_bytes = bytes.clone();

        // tweak the version bytes so the timestamp is set to `time`
        // Version serialization is specified at:
        // https://developer.bitcoin.org/reference/p2p_networking.html#version
        bytes[36..44].copy_from_slice(&time.to_le_bytes());

        // Checksum is specified at:
        // https://developer.bitcoin.org/reference/p2p_networking.html#message-headers
        let checksum = sha256d::Checksum::from(&bytes[HEADER_LEN..]);
        bytes[20..24].copy_from_slice(&checksum.0);

        debug!(?time,
               old_len = ?old_bytes.len(), new_len = ?bytes.len(),
               old_bytes = ?&old_bytes[36..44], new_bytes = ?&bytes[36..44]);

        bytes
    });

    rt.block_on(async {
        let mut fr = FramedRead::new(Cursor::new(&v_bytes), Codec::builder().finish());
        fr.next().await.expect("a next message should be available")
    })
}

/// L.ignored: ignored Bloom commands pass their checksum and leave nothing behind. The
/// decoder continues to the next message in the same turn, as the protocol rule requires.
#[test]
fn bloom_filter_messages_are_discarded_without_dropping_following_messages() {
    let mut codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();

    for (command, body) in [
        (BLOOM_FILTER_COMMANDS[0], &[1, 2, 3][..]),
        (BLOOM_FILTER_COMMANDS[1], &[4, 5][..]),
        (BLOOM_FILTER_COMMANDS[2], &[][..]),
    ] {
        bytes.extend_from_slice(&wire_message(command, body));
    }
    bytes.extend_from_slice(&wire_message(*b"verack\0\0\0\0\0\0", &[]));

    assert_eq!(
        codec.decode(&mut bytes).expect("message should be decoded"),
        Some(Message::Verack),
        "message following discarded bloom filter messages should be decoded",
    );
    assert!(bytes.is_empty(), "all complete messages should be consumed");
}

fn wire_message(command: [u8; 12], body: &[u8]) -> Vec<u8> {
    let body_len = u32::try_from(body.len()).expect("test message body fits in a u32");
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&Network::Mainnet.magic().0);
    bytes.extend_from_slice(&command);
    bytes.extend_from_slice(&body_len.to_le_bytes());
    bytes.extend_from_slice(&sha256d::Checksum::from(body).0);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn reject_message_no_extra_data_round_trip() {
    let (rt, _init_guard) = zakura_test::init_async();

    let v = Message::Reject {
        message: "experimental".to_string(),
        ccode: RejectReason::Malformed,
        reason: "message could not be decoded".to_string(),
        data: None,
    };

    use tokio_util::codec::{FramedRead, FramedWrite};
    let v_bytes = rt.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(&mut bytes, Codec::builder().finish());
            fw.send(v.clone())
                .await
                .expect("message should be serialized");
        }
        bytes
    });

    let v_parsed = rt.block_on(async {
        let mut fr = FramedRead::new(Cursor::new(&v_bytes), Codec::builder().finish());
        fr.next()
            .await
            .expect("a next message should be available")
            .expect("that message should deserialize")
    });

    assert_eq!(v, v_parsed);
}

#[test]
fn reject_message_extra_data_round_trip() {
    let (rt, _init_guard) = zakura_test::init_async();

    let v = Message::Reject {
        message: "block".to_string(),
        ccode: RejectReason::Invalid,
        reason: "invalid block difficulty".to_string(),
        data: Some([0xff; 32]),
    };

    use tokio_util::codec::{FramedRead, FramedWrite};
    let v_bytes = rt.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(&mut bytes, Codec::builder().finish());
            fw.send(v.clone())
                .await
                .expect("message should be serialized");
        }
        bytes
    });

    let v_parsed = rt.block_on(async {
        let mut fr = FramedRead::new(Cursor::new(&v_bytes), Codec::builder().finish());
        fr.next()
            .await
            .expect("a next message should be available")
            .expect("that message should deserialize")
    });

    assert_eq!(v, v_parsed);
}

/// Block and transaction messages over `MAX_BLOCK_BYTES` fit in a protocol message,
/// but are rejected before their bodies are parsed.
#[test]
fn oversized_block_and_tx_messages_are_rejected() {
    use tokio_util::codec::{FramedRead, FramedWrite};

    let _init_guard = zakura_test::init();

    let max = usize::try_from(zakura_chain::block::MAX_BLOCK_BYTES).unwrap();
    let body = vec![0; max + 1];
    assert!(body.len() <= MAX_PROTOCOL_MESSAGE_LEN);

    let codec = Codec::builder()
        .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
        .finish();
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        assert!(matches!(
            codec.read_block(&mut body.as_slice()),
            Err(Error::Parse("block is larger than MAX_BLOCK_BYTES"))
        ));
        assert!(matches!(
            codec.read_tx(&mut body.as_slice()),
            Err(Error::Parse("transaction is larger than MAX_BLOCK_BYTES"))
        ));
    });

    // A real block one transaction over the limit is rejected end to end.
    let block = zakura_chain::block::tests::generate::oversized_multi_transaction_block();
    let msg_bytes = zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut bytes = Vec::new();
        FramedWrite::new(
            &mut bytes,
            Codec::builder()
                .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
                .finish(),
        )
        .send(Message::Block(block.into()))
        .await
        .expect("an oversized block still fits in a protocol message");
        bytes
    });
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut fr = FramedRead::new(
            Cursor::new(&msg_bytes),
            Codec::builder()
                .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
                .finish(),
        );
        assert!(matches!(
            fr.next().await.expect("a message is available"),
            Err(Error::Parse("block is larger than MAX_BLOCK_BYTES"))
        ));
    });
}

#[test]
fn max_msg_size_round_trip() {
    use zakura_chain::serialization::ZcashDeserializeInto;

    //let (rt, _init_guard) = zakura_test::init_async();
    let _init_guard = zakura_test::init();

    // make tests with a Tx message
    let tx: Transaction = zakura_test::vectors::DUMMY_TX1
        .zcash_deserialize_into()
        .unwrap();
    let msg = Message::Tx(tx.into());

    use tokio_util::codec::{FramedRead, FramedWrite};

    // i know the above msg has a body of 85 bytes
    let size = 85;

    // reducing the max size to body size - 1
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(
                &mut bytes,
                Codec::builder().with_max_body_len(size - 1).finish(),
            );
            fw.send(msg.clone())
                .await
                .expect_err("message should not encode as it is bigger than the max allowed value");
        }
    });

    // send again with the msg body size as max size
    let msg_bytes = zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut bytes = Vec::new();
        {
            let mut fw = FramedWrite::new(
                &mut bytes,
                Codec::builder().with_max_body_len(size).finish(),
            );
            fw.send(msg.clone())
                .await
                .expect("message should encode with the msg body size as max allowed value");
        }
        bytes
    });

    // receive with a reduced max size
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut fr = FramedRead::new(
            Cursor::new(&msg_bytes),
            Codec::builder().with_max_body_len(size - 1).finish(),
        );
        fr.next()
            .await
            .expect("a next message should be available")
            .expect_err("message should not decode as it is bigger than the max allowed value")
    });

    // receive again with the tx size as max size
    zakura_test::MULTI_THREADED_RUNTIME.block_on(async {
        let mut fr = FramedRead::new(
            Cursor::new(&msg_bytes),
            Codec::builder().with_max_body_len(size).finish(),
        );
        fr.next()
            .await
            .expect("a next message should be available")
            .expect("message should decode with the msg body size as max allowed value")
    });
}

/// Check that the version test vector deserializes correctly without the relay byte
#[test]
fn version_message_omitted_relay() {
    let _init_guard = zakura_test::init();

    let version = match VERSION_TEST_VECTOR.clone() {
        Message::Version(mut version) => {
            version.relay = false;
            version.into()
        }
        _ => unreachable!("const is the Message::Version variant"),
    };

    let codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();

    codec
        .write_body(&version, &mut (&mut bytes).writer())
        .expect("encoding should succeed");
    bytes.truncate(bytes.len() - 1);

    let relay = match codec.read_version(Cursor::new(&bytes)) {
        Ok(Message::Version(VersionMessage { relay, .. })) => relay,
        err => panic!("bytes should successfully decode to version message, got: {err:?}"),
    };

    assert!(relay, "relay should be true when omitted from message");
}

/// Check that the version test vector deserializes `relay` correctly with the relay byte
#[test]
fn version_message_with_relay() {
    let _init_guard = zakura_test::init();
    let codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();

    codec
        .write_body(&VERSION_TEST_VECTOR, &mut (&mut bytes).writer())
        .expect("encoding should succeed");

    let relay = match codec.read_version(Cursor::new(&bytes)) {
        Ok(Message::Version(VersionMessage { relay, .. })) => relay,
        err => panic!("bytes should successfully decode to version message, got: {err:?}"),
    };

    assert!(relay, "relay should be true");

    bytes.clear();

    let version = match VERSION_TEST_VECTOR.clone() {
        Message::Version(mut version) => {
            version.relay = false;
            version.into()
        }
        _ => unreachable!("const is the Message::Version variant"),
    };

    codec
        .write_body(&version, &mut (&mut bytes).writer())
        .expect("encoding should succeed");

    let relay = match codec.read_version(Cursor::new(&bytes)) {
        Ok(Message::Version(VersionMessage { relay, .. })) => relay,
        err => panic!("bytes should successfully decode to version message, got: {err:?}"),
    };

    assert!(!relay, "relay should be false");
}

/// Check that the codec enforces size limits on `user_agent` field of version messages.
#[test]
fn version_user_agent_size_limits() {
    let _init_guard = zakura_test::init();
    let codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();
    let [valid_version_message, invalid_version_message]: [Message; 2] = {
        let services = PeerServices::NODE_NETWORK;
        let timestamp = Utc
            .timestamp_opt(1_568_000_000, 0)
            .single()
            .expect("in-range number of seconds and valid nanosecond");

        [
            "X".repeat(MAX_USER_AGENT_LENGTH),
            "X".repeat(MAX_USER_AGENT_LENGTH + 1),
        ]
        .map(|user_agent| {
            VersionMessage {
                version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
                services,
                timestamp,
                address_recv: AddrInVersion::new(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 6)), 8233),
                    services,
                ),
                address_from: AddrInVersion::new(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 6)), 8233),
                    services,
                ),
                nonce: Nonce(0x9082_4908_8927_9238),
                user_agent,
                start_height: block::Height(540_000),
                relay: true,
            }
            .into()
        })
    };

    // Check that encoding and decoding will succeed when the user_agent is not longer than MAX_USER_AGENT_LENGTH
    codec
        .write_body(&valid_version_message, &mut (&mut bytes).writer())
        .expect("encoding valid version msg should succeed");
    codec
        .read_version(Cursor::new(&bytes))
        .expect("decoding valid version msg should succeed");

    bytes.clear();

    let mut writer = (&mut bytes).writer();

    // Check that encoding will return an error when the user_agent is longer than MAX_USER_AGENT_LENGTH
    match codec.write_body(&invalid_version_message, &mut writer) {
        Err(Error::Parse(error_msg)) if error_msg.contains("user agent too long") => {}
        result => panic!("expected write error: user agent too long, got: {result:?}"),
    };

    // Encode the rest of the message onto `bytes` (relay should be optional)
    {
        let Message::Version(VersionMessage {
            user_agent,
            start_height,
            ..
        }) = invalid_version_message
        else {
            unreachable!("version_message is a version");
        };

        user_agent
            .zcash_serialize(&mut writer)
            .expect("writing user_agent should succeed");
        writer
            .write_u32::<LittleEndian>(start_height.0)
            .expect("writing start_height should succeed");
    }

    // Check that decoding will return an error when the user_agent is longer than MAX_USER_AGENT_LENGTH
    match codec.read_version(Cursor::new(&bytes)) {
        Err(Error::Parse(error_msg)) if error_msg.contains("user agent too long") => {}
        result => panic!("expected read error: user agent too long, got: {result:?}"),
    };
}

/// Check that the codec enforces size limits on `message` and `reason` fields of reject messages.
#[test]
fn reject_command_and_reason_size_limits() {
    let _init_guard = zakura_test::init();
    let codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();

    let valid_message = "X".repeat(MAX_REJECT_MESSAGE_LENGTH);
    let invalid_message = "X".repeat(MAX_REJECT_MESSAGE_LENGTH + 1);
    let valid_reason = "X".repeat(MAX_REJECT_REASON_LENGTH);
    let invalid_reason = "X".repeat(MAX_REJECT_REASON_LENGTH + 1);

    let valid_reject_message = Message::Reject {
        message: valid_message.clone(),
        ccode: RejectReason::Invalid,
        reason: valid_reason.clone(),
        data: None,
    };

    // Check that encoding and decoding will succeed when `message` and `reason` fields are within size limits.
    codec
        .write_body(&valid_reject_message, &mut (&mut bytes).writer())
        .expect("encoding valid reject msg should succeed");
    codec
        .read_reject(Cursor::new(&bytes))
        .expect("decoding valid reject msg should succeed");

    let invalid_reject_messages = [
        (
            "reject message too long",
            Message::Reject {
                message: invalid_message,
                ccode: RejectReason::Invalid,
                reason: valid_reason,
                data: None,
            },
        ),
        (
            "reject reason too long",
            Message::Reject {
                message: valid_message,
                ccode: RejectReason::Invalid,
                reason: invalid_reason,
                data: None,
            },
        ),
    ];

    for (expected_err_msg, invalid_reject_message) in invalid_reject_messages {
        // Check that encoding will return an error when the reason or message are too long.
        match codec.write_body(&invalid_reject_message, &mut (&mut bytes).writer()) {
            Err(Error::Parse(error_msg)) if error_msg.contains(expected_err_msg) => {}
            result => panic!("expected write error: {expected_err_msg}, got: {result:?}"),
        };

        bytes.clear();

        // Encode the message onto `bytes` without size checks
        {
            let Message::Reject {
                message,
                ccode,
                reason,
                data,
            } = invalid_reject_message
            else {
                unreachable!("invalid_reject_message is a reject");
            };

            let mut writer = (&mut bytes).writer();

            message
                .zcash_serialize(&mut writer)
                .expect("writing message should succeed");

            writer
                .write_u8(ccode as u8)
                .expect("writing ccode should succeed");

            reason
                .zcash_serialize(&mut writer)
                .expect("writing reason should succeed");

            if let Some(data) = data {
                writer
                    .write_all(&data)
                    .expect("writing data should succeed");
            }
        }

        // Check that decoding will return an error when the reason or message are too long.
        match codec.read_reject(Cursor::new(&bytes)) {
            Err(Error::Parse(error_msg)) if error_msg.contains(expected_err_msg) => {}
            result => panic!("expected read error: {expected_err_msg}, got: {result:?}"),
        };
    }
}

/// Regression test for GHSA-438q-jx8f-cccv: read_headers() must reject
/// inbound `headers` messages with more than 160 entries.
#[test]
fn headers_message_exceeding_protocol_cap_is_rejected() {
    use zakura_chain::serialization::ZcashDeserializeInto;

    let _init_guard = zakura_test::init();

    let header: block::Header = zakura_test::vectors::DUMMY_HEADER
        .zcash_deserialize_into()
        .expect("dummy header should deserialize");
    let counted = block::CountedHeader {
        header: header.into(),
    };

    // 161 headers — one more than the protocol limit of 160.
    let msg = Message::Headers(vec![counted.clone(); 161]);

    let mut codec = Codec::builder()
        .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
        .finish();
    let mut bytes = BytesMut::new();
    codec
        .encode(msg, &mut bytes)
        .expect("encoding should succeed");

    codec
        .decode(&mut bytes)
        .expect_err("decoding 161 headers should be rejected");
}

/// Verify that a headers message at exactly the protocol cap (160) is accepted.
#[test]
fn headers_message_at_protocol_cap_is_accepted() {
    use zakura_chain::serialization::ZcashDeserializeInto;

    let _init_guard = zakura_test::init();

    let header: block::Header = zakura_test::vectors::DUMMY_HEADER
        .zcash_deserialize_into()
        .expect("dummy header should deserialize");
    let counted = block::CountedHeader {
        header: header.into(),
    };

    let msg = Message::Headers(vec![counted; 160]);

    let mut codec = Codec::builder()
        .with_max_body_len(MAX_PROTOCOL_MESSAGE_LEN)
        .finish();
    let mut bytes = BytesMut::new();
    codec
        .encode(msg, &mut bytes)
        .expect("encoding should succeed");

    let decoded = codec
        .decode(&mut bytes)
        .expect("decoding should not error")
        .expect("a message should be present");

    match decoded {
        Message::Headers(headers) => assert_eq!(headers.len(), 160),
        other => panic!("expected Headers, got {other:?}"),
    }
}

/// A panic in synchronous message-body dispatch becomes a terminal parse error
/// without unwinding the runtime task.
#[test]
fn body_dispatch_panic_returns_parse_error_and_task_survives() {
    let _init_guard = zakura_test::init();

    let mut panicking_frame = BytesMut::new();
    Codec::builder()
        .finish()
        .encode(Message::Ping(Nonce(1)), &mut panicking_frame)
        .expect("test ping should encode");

    zakura_test::MULTI_THREADED_RUNTIME.block_on(async move {
        let task = tokio::spawn(async move {
            let mut codec = Codec::builder().finish();
            codec.inject_body_decode_panic(BodyDecodePanic::CommandDispatch);

            let error = codec
                .decode(&mut panicking_frame)
                .expect_err("injected body parser panic should become an error");
            match error {
                Error::Parse(message) => {
                    assert_eq!(message, PANICKED_MESSAGE_BODY_PARSE_ERROR);
                }
                other => panic!("body parser panic should be a parse error: {other:?}"),
            }
        });

        task.await
            .expect("body parser panic should not terminate the runtime task");
    });
}

/// The containment boundary includes block conversion after parallel
/// deserialization.
#[test]
fn block_conversion_panic_returns_parse_error() {
    let _init_guard = zakura_test::init();
    let block = Block::zcash_deserialize(zakura_test::vectors::BLOCKS[0])
        .expect("block test vector should deserialize");

    let mut block_frame = BytesMut::new();
    let mut encoding_codec = Codec::builder().finish();
    encoding_codec.reconfigure_full_body_len();
    encoding_codec
        .encode(
            Message::Block(std::sync::Arc::new(block).into()),
            &mut block_frame,
        )
        .expect("test block should encode");

    zakura_test::MULTI_THREADED_RUNTIME.block_on(async move {
        let task = tokio::spawn(async move {
            let mut codec = Codec::builder().finish();
            codec.reconfigure_full_body_len();
            codec.inject_body_decode_panic(BodyDecodePanic::AfterBlockConversion);

            codec
                .decode(&mut block_frame)
                .expect_err("post-conversion panic should become an error")
        });

        let error = task
            .await
            .expect("post-conversion panic should not terminate the runtime task");
        match error {
            Error::Parse(message) => {
                assert_eq!(message, PANICKED_MESSAGE_BODY_PARSE_ERROR);
            }
            other => panic!("block conversion panic should be a parse error: {other:?}"),
        }
    });
}

/// The containment boundary includes transaction conversion after parallel
/// deserialization.
#[test]
fn transaction_conversion_panic_returns_parse_error() {
    use zakura_chain::serialization::ZcashDeserializeInto;

    let _init_guard = zakura_test::init();
    let tx: Transaction = zakura_test::vectors::DUMMY_TX1
        .zcash_deserialize_into()
        .expect("dummy transaction should deserialize");

    let mut tx_frame = BytesMut::new();
    Codec::builder()
        .finish()
        .encode(Message::Tx(tx.into()), &mut tx_frame)
        .expect("test transaction should encode");

    zakura_test::MULTI_THREADED_RUNTIME.block_on(async move {
        let task = tokio::spawn(async move {
            let mut codec = Codec::builder().finish();
            codec.inject_body_decode_panic(BodyDecodePanic::AfterTransactionConversion);

            codec
                .decode(&mut tx_frame)
                .expect_err("post-conversion panic should become an error")
        });

        let error = task
            .await
            .expect("post-conversion panic should not terminate the runtime task");
        match error {
            Error::Parse(message) => {
                assert_eq!(message, PANICKED_MESSAGE_BODY_PARSE_ERROR);
            }
            other => panic!("transaction conversion panic should be a parse error: {other:?}"),
        }
    });
}

/// Check that the version test vector deserialization fails when there's a network magic mismatch.
#[test]
fn message_with_wrong_network_magic_returns_error() {
    let _init_guard = zakura_test::init();
    let mut codec = Codec::builder().finish();
    let mut bytes = BytesMut::new();

    codec
        .encode(VERSION_TEST_VECTOR.clone(), &mut bytes)
        .expect("encoding should succeed");

    let mut codec = Codec::builder()
        .for_network(&Network::new_default_testnet())
        .finish();

    codec
        .decode(&mut bytes)
        .expect_err("decoding message with mismatching network magic should return an error");
}

/// Check that `escape_command` escapes control characters and NUL padding.
#[test]
fn escape_command_escapes_control_characters() {
    // An ANSI clear-screen escape sequence and a newline, followed by
    // printable ASCII.
    assert_eq!(escape_command(b"\x1b[2J\nFORGED!"), r"\x1b[2J\nFORGED!");

    // NUL padding in a well-formed command is escaped, not dropped.
    assert_eq!(
        escape_command(b"version\0\0\0\0\0"),
        r"version\x00\x00\x00\x00\x00",
    );
}

/// Check that an unknown command containing control characters is escaped in
/// the "unknown message command" debug event, so a peer can't forge log lines
/// or inject terminal escape sequences into log output.
#[test]
fn unknown_command_is_escaped_in_debug_log() {
    use std::sync::{Arc, Mutex};

    let _init_guard = zakura_test::init();

    /// A log writer that appends formatted log bytes to a shared buffer.
    #[derive(Clone)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("no code panics while holding the log buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // A mainnet message with an empty body, and an unknown command containing
    // an ANSI clear-screen escape sequence and a newline.
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(&Network::Mainnet.magic().0);
    bytes.extend_from_slice(b"\x1b[2J\nFORGED!");
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&sha256d::Checksum::from(&[][..]).0);

    let log_buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer({
            let log_buffer = log_buffer.clone();
            move || log_buffer.clone()
        })
        .finish();

    let decode_result = {
        // A test-scoped subscriber is safe here because `decode` is
        // synchronous: its events can't be routed through another thread's
        // subscriber, and no spans are recorded.
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        Codec::builder().finish().decode(&mut bytes)
    };

    assert!(
        decode_result
            .expect("unknown commands are ignored, not errors")
            .is_none(),
        "unknown commands should not decode to a message",
    );

    // When `debug!` events are statically disabled (for example, by zakurad's
    // `release_max_level_info` feature in the same build), there is no log
    // output to check.
    if tracing::level_filters::STATIC_MAX_LEVEL < tracing::Level::DEBUG {
        return;
    }

    let log_output = String::from_utf8(
        log_buffer
            .0
            .lock()
            .expect("no code panics while holding the log buffer lock")
            .clone(),
    )
    .expect("the fmt subscriber writes valid UTF-8");

    assert!(
        log_output.contains("unknown message command from peer"),
        "expected an unknown command debug event, got: {log_output:?}",
    );
    assert!(
        log_output.contains(r"\x1b[2J\nFORGED!"),
        "expected the escaped command string, got: {log_output:?}",
    );
    assert!(
        !log_output.contains('\x1b'),
        "raw ANSI escape bytes must not reach the log output: {log_output:?}",
    );
    assert!(
        !log_output.contains("\nFORGED"),
        "raw newlines from the command must not reach the log output: {log_output:?}",
    );
}

/// A warm-up ping encodes as `ping` with its padding after the nonce, decodes as a plain
/// `Ping` because receivers ignore the trailing bytes, and fits the protocol message limit
/// at its largest padding.
#[test]
fn warmup_ping_encodes_as_padded_ping() {
    let _init_guard = zakura_test::init();
    let mut codec = Codec::builder().finish();
    codec.reconfigure_full_body_len();
    let nonce = Nonce(0x0123_4567_89ab_cdef);

    for padding in [0, 1, 16 * 1024, crate::constants::MAX_PING_PADDING] {
        let mut bytes = BytesMut::new();
        codec
            .encode(Message::PaddedPing { nonce, padding }, &mut bytes)
            .expect("a warm-up ping within the message limit encodes");

        assert_eq!(&bytes[4..16], b"ping\0\0\0\0\0\0\0\0");
        let body_len = 8 + usize::try_from(padding).expect("padding fits in usize");
        assert_eq!(bytes.len(), HEADER_LEN + body_len);
        assert!(bytes[HEADER_LEN + 8..].iter().all(|byte| *byte == 0));

        assert_eq!(
            codec.decode(&mut bytes).expect("a padded ping decodes"),
            Some(Message::Ping(nonce)),
        );
        assert!(bytes.is_empty(), "the decoder consumes the padding");
    }

    let mut bytes = BytesMut::new();
    codec
        .encode(
            Message::PaddedPing {
                nonce,
                padding: crate::constants::MAX_PING_PADDING + 1,
            },
            &mut bytes,
        )
        .expect_err("a warm-up ping over the message limit does not encode");
}

/// The message checksum matches the shared double SHA-256 checksum for empty, short and
/// block-sized bodies, including lengths around SHA-256's 64-byte block boundary.
#[test]
fn message_checksum_matches_sha256d_checksum() {
    let _init_guard = zakura_test::init();

    let body: Vec<u8> = (0..2_100_000u32)
        .map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes()[3])
        .collect();

    // Fixed lengths around the 64-byte block boundary, then lengths spread over the whole range
    // up to a full block from a fixed pseudo-random sequence.
    // The shift leaves 21 bits, so each length is below 2^21 and fits in `usize` and `body`.
    let spread = (1..=64u64).map(|i| (i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 43) as usize);
    let fixed = [0, 1, 55, 56, 63, 64, 65, 119, 128, 1_000, 65_537, 2_100_000];

    for len in fixed.into_iter().chain(spread) {
        assert_eq!(
            message_checksum(&body[..len]),
            sha256d::Checksum::from(&body[..len]),
            "checksums differ for a {len} byte body",
        );
    }

    // The test vector from zakura-chain's sha256d checksum tests.
    assert_eq!(
        message_checksum(b"hello"),
        sha256d::Checksum([0x95, 0x95, 0xc9, 0xdf]),
    );
}

/// Encodes `block` as a `block` message, as a connection does after its handshake.
fn encode_block(codec: &mut Codec, block: &std::sync::Arc<Block>) -> BytesMut {
    let mut frame = BytesMut::new();
    codec.reconfigure_full_body_len();
    codec
        .encode(Message::Block(block.clone().into()), &mut frame)
        .expect("test block should encode");
    frame
}

/// A block close to the maximum size, which is large enough to share its checksum.
fn large_block() -> std::sync::Arc<Block> {
    let block = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_TESTNET_141042_BYTES[..])
        .expect("block test vector should deserialize");
    std::sync::Arc::new(block)
}

/// A block sent through connections that share checksums is byte-for-byte the block a
/// connection on its own sends, on the first send and on the ones that reuse its checksum.
#[test]
fn block_sent_with_shared_checksums_is_unchanged() {
    let _init_guard = zakura_test::init();
    let block = large_block();

    let alone = encode_block(&mut Codec::builder().finish(), &block);

    let shared = BlockChecksums::default();
    let mut first = Codec::builder()
        .with_block_checksums(shared.clone())
        .finish();
    let mut second = Codec::builder().with_block_checksums(shared).finish();

    assert_eq!(encode_block(&mut first, &block), alone);
    assert_eq!(encode_block(&mut second, &block), alone);
    assert_eq!(encode_block(&mut first, &block), alone);
}

/// A large block whose checksum is already known is sent with that checksum, not hashed again.
#[test]
fn large_block_is_sent_with_its_remembered_checksum() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let body_len = encode_block(&mut Codec::builder().finish(), &block).len() - HEADER_LEN;

    // A value the body does not hash to, so it can only come from the shared checksums.
    let remembered = sha256d::Checksum([0xAB; 4]);
    let shared = BlockChecksums::default();
    shared.insert(block.hash(), body_len, remembered);

    let mut codec = Codec::builder().with_block_checksums(shared).finish();
    let frame = encode_block(&mut codec, &block);

    assert_eq!(frame[20..HEADER_LEN], remembered.0);
}

/// A checksum remembered for a body of another length is not used. The body is hashed, and
/// later sends reuse that checksum.
#[test]
fn checksum_remembered_for_another_body_length_is_replaced() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let alone = encode_block(&mut Codec::builder().finish(), &block);
    let body_len = alone.len() - HEADER_LEN;

    let shared = BlockChecksums::default();
    shared.insert(block.hash(), body_len + 1, sha256d::Checksum([0xAB; 4]));

    let mut codec = Codec::builder()
        .with_block_checksums(shared.clone())
        .finish();

    assert_eq!(encode_block(&mut codec, &block), alone);
    assert_eq!(
        shared.get(block.hash(), body_len),
        Some(sha256d::Checksum::from(&alone[HEADER_LEN..])),
    );
}

/// A small block is always hashed, even when a checksum is remembered for it.
#[test]
fn small_block_checksum_is_always_hashed() {
    let _init_guard = zakura_test::init();
    let block = std::sync::Arc::new(
        Block::zcash_deserialize(zakura_test::vectors::BLOCKS[0])
            .expect("block test vector should deserialize"),
    );
    let alone = encode_block(&mut Codec::builder().finish(), &block);
    let body_len = alone.len() - HEADER_LEN;
    assert!(body_len < MIN_SHARED_CHECKSUM_BODY_LEN);

    let shared = BlockChecksums::default();
    shared.insert(block.hash(), body_len, sha256d::Checksum([0xAB; 4]));

    let mut codec = Codec::builder().with_block_checksums(shared).finish();

    assert_eq!(encode_block(&mut codec, &block), alone);
}

/// When there is no room left, the block forgotten is the one sent least recently. A block
/// that was sent again in the meantime is kept.
#[test]
fn block_checksums_forget_the_block_sent_least_recently() {
    let _init_guard = zakura_test::init();
    let hash = |index: usize| {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        block::Hash(bytes)
    };
    let checksum = sha256d::Checksum([1; 4]);

    let checksums = BlockChecksums::default();
    for index in 0..SHARED_CHECKSUM_BLOCKS {
        checksums.insert(hash(index), 100_000, checksum);
    }

    // Sending the oldest block again makes the second-oldest the next to be forgotten.
    assert_eq!(checksums.get(hash(0), 100_000), Some(checksum));
    checksums.insert(hash(SHARED_CHECKSUM_BLOCKS), 100_000, checksum);

    assert_eq!(checksums.get(hash(0), 100_000), Some(checksum));
    assert_eq!(checksums.get(hash(1), 100_000), None);
    assert_eq!(
        checksums.get(hash(SHARED_CHECKSUM_BLOCKS), 100_000),
        Some(checksum)
    );
}
