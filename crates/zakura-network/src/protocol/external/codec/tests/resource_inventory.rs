//! Every TCP command's admission before its body exists, and its discard path.

use super::*;

/// The 18 dispatched commands, by their on-wire names.
const DISPATCHED_COMMANDS: [&[u8; 12]; 18] = [
    b"version\0\0\0\0\0",
    b"verack\0\0\0\0\0\0",
    b"ping\0\0\0\0\0\0\0\0",
    b"pong\0\0\0\0\0\0\0\0",
    b"reject\0\0\0\0\0\0",
    crate::zakura::P2P_V2_UPGRADE_COMMAND_BYTES,
    b"addr\0\0\0\0\0\0\0\0",
    b"addrv2\0\0\0\0\0\0",
    b"getaddr\0\0\0\0\0",
    b"block\0\0\0\0\0\0\0",
    b"getblocks\0\0\0",
    b"headers\0\0\0\0\0",
    b"getheaders\0\0",
    b"inv\0\0\0\0\0\0\0\0\0",
    b"getdata\0\0\0\0\0",
    b"notfound\0\0\0\0",
    b"tx\0\0\0\0\0\0\0\0\0\0",
    b"mempool\0\0\0\0\0",
];

const UNKNOWN_COMMAND: &[u8; 12] = b"sendcmpct\0\0\0";

fn header(command: &[u8; 12], body: &[u8], claimed_len: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_LEN);
    bytes.extend_from_slice(&Network::Mainnet.magic().0);
    bytes.extend_from_slice(command);
    bytes.extend_from_slice(&claimed_len.to_le_bytes());
    bytes.extend_from_slice(&sha256d::Checksum::from(body).0);
    bytes
}

fn message(command: &[u8; 12], body: &[u8]) -> Vec<u8> {
    let mut bytes = header(command, body, u32::try_from(body.len()).unwrap());
    bytes.extend_from_slice(body);
    bytes
}

fn full_codec() -> Codec {
    let mut codec = Codec::builder().for_network(&Network::Mainnet).finish();
    codec.reconfigure_full_body_len();
    codec
}

/// The dispatch table in this module's source has the inventory's commands and no
/// others. A new command fails here until it joins the inventory.
#[test]
fn the_dispatch_table_matches_the_inventory() {
    let source = include_str!("../../codec.rs");
    let start = source.find("let msg = match &command {").unwrap();
    let end = start + source[start..].find("_ => {").unwrap();
    let arms = source[start..end]
        .lines()
        .filter(|line| line.contains("=>"))
        .count();
    assert_eq!(arms, DISPATCHED_COMMANDS.len());
    for command in DISPATCHED_COMMANDS {
        let unique = DISPATCHED_COMMANDS
            .iter()
            .filter(|c| **c == command)
            .count();
        assert_eq!(unique, 1);
        assert!(!BLOOM_FILTER_COMMANDS.contains(command));
    }
}

/// A header that claims the largest legal body reserves a fixed amount for every
/// command, and the decoder waits for the body without parsing it.
#[test]
fn a_claimed_body_length_does_not_allocate_the_body() {
    let commands = DISPATCHED_COMMANDS
        .iter()
        .copied()
        .chain(BLOOM_FILTER_COMMANDS.iter())
        .chain([UNKNOWN_COMMAND]);
    for command in commands {
        // The upgrade command has its own smaller hard cap.
        let claimed = if command == crate::zakura::P2P_V2_UPGRADE_COMMAND_BYTES {
            crate::zakura::MAX_PRELUDE_PAYLOAD_BYTES
        } else {
            MAX_PROTOCOL_MESSAGE_LEN
        };
        let mut codec = full_codec();
        let mut src = BytesMut::from(&header(command, &[], u32::try_from(claimed).unwrap())[..]);
        assert!(matches!(codec.decode(&mut src), Ok(None)));
        assert!(
            src.capacity() <= 2 * (MAX_UNRECEIVED_BODY_RESERVATION + HEADER_LEN),
            "{} reserved {} bytes",
            escape_command(command),
            src.capacity(),
        );
    }
}

/// One byte over the protocol's limit stops at the header, before any allocation.
#[test]
fn an_oversized_claim_stops_at_the_header() {
    let over = u32::try_from(MAX_PROTOCOL_MESSAGE_LEN + 1).unwrap();
    let mut codec = full_codec();
    let mut src = BytesMut::from(&header(UNKNOWN_COMMAND, &[], over)[..]);
    assert!(codec.decode(&mut src).is_err());
}

/// L.handshake: the handshake phase applies its smaller limit to every command, and
/// the refused body has no reservation.
#[test]
fn the_handshake_limit_covers_every_command() {
    let over = u32::try_from(MAX_HANDSHAKE_BODY_LEN + 1).unwrap();
    for command in DISPATCHED_COMMANDS
        .iter()
        .copied()
        .chain(BLOOM_FILTER_COMMANDS.iter())
        .chain([UNKNOWN_COMMAND])
    {
        let mut codec = Codec::builder().for_network(&Network::Mainnet).finish();
        let mut src = BytesMut::from(&header(command, &[], over)[..]);
        assert!(codec.decode(&mut src).is_err());
        assert!(src.capacity() < MAX_HANDSHAKE_BODY_LEN + HEADER_LEN + 1024);
    }
}

/// A partial body keeps the decoder waiting, and it completes when the rest arrives.
#[test]
fn a_partial_body_grows_only_with_its_bytes() {
    let body = 7u64.to_le_bytes();
    let bytes = message(b"ping\0\0\0\0\0\0\0\0", &body);
    let mut codec = full_codec();
    let mut src = BytesMut::from(&bytes[..bytes.len() - 1]);
    assert!(matches!(codec.decode(&mut src), Ok(None)));
    src.extend_from_slice(&bytes[bytes.len() - 1..]);
    assert!(matches!(codec.decode(&mut src), Ok(Some(Message::Ping(_)))));
    assert!(src.is_empty());
}

/// L.ignored: a bad checksum on an ignored Bloom command stays fatal, with no decoded
/// message.
#[test]
fn a_bad_bloom_checksum_stays_fatal() {
    for command in BLOOM_FILTER_COMMANDS {
        let mut bad = header(&command, &[0], 1);
        bad.push(1);
        let mut src = BytesMut::from(&bad[..]);
        assert!(full_codec().decode(&mut src).is_err());
    }
}

/// An unknown command is checked and ignored without closing the connection, and it
/// leaves the next message intact.
#[test]
fn unknown_commands_are_ignored_after_their_checksum() {
    let ping = message(b"ping\0\0\0\0\0\0\0\0", &7u64.to_le_bytes());
    let mut bytes = message(UNKNOWN_COMMAND, &[1; 1024]);
    bytes.extend_from_slice(&ping);
    let mut codec = full_codec();
    let mut src = BytesMut::from(&bytes[..]);
    assert!(matches!(codec.decode(&mut src), Ok(Some(Message::Ping(_)))));
    assert!(src.is_empty(), "the discarded body left the buffer");

    let mut bad = header(UNKNOWN_COMMAND, &[0], 1);
    bad.push(1);
    let mut src = BytesMut::from(&bad[..]);
    assert!(full_codec().decode(&mut src).is_err());
}

/// `commands` Bloom commands with `body_len`-byte bodies, then a ping.
fn bloom_burst(commands: usize, body_len: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for _ in 0..commands {
        bytes.extend_from_slice(&message(&BLOOM_FILTER_COMMANDS[1], &vec![1; body_len]));
    }
    bytes.extend_from_slice(&message(b"ping\0\0\0\0\0\0\0\0", &7u64.to_le_bytes()));
    bytes
}

/// L.ignored and L.unknown: one decode call discards a run of Bloom and unknown
/// commands and returns the next message. The work is bounded by the buffered bytes,
/// and no command in the run faults the peer.
#[test]
fn a_discard_run_ends_at_the_next_message() {
    let mut bytes = message(UNKNOWN_COMMAND, &[1; 8]);
    bytes.extend_from_slice(&message(&BLOOM_FILTER_COMMANDS[0], &[1; 8]));
    bytes.extend_from_slice(&message(UNKNOWN_COMMAND, &[1; 8]));
    bytes.extend_from_slice(&bloom_burst(1000, 8));
    let mut src = BytesMut::from(&bytes[..]);
    assert!(matches!(
        full_codec().decode(&mut src),
        Ok(Some(Message::Ping(_)))
    ));
    assert!(src.is_empty());
}

/// L.ignored and L.unknown: discarded commands never leave a complete message waiting
/// for another socket read. Each peer sends discarded commands and a ping, then goes
/// quiet, and the reader still yields the ping.
#[tokio::test(start_paused = true)]
async fn a_quiet_peer_gets_its_message_after_discarded_commands() {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::FramedRead;

    let ping = message(b"ping\0\0\0\0\0\0\0\0", &7u64.to_le_bytes());
    let mut unknown_then_ping = message(UNKNOWN_COMMAND, &[1; 8]);
    unknown_then_ping.extend_from_slice(&ping);
    let cases = [
        ("64 Bloom commands", bloom_burst(64, 8)),
        ("1000 Bloom commands", bloom_burst(1000, 8)),
        ("2 MiB of Bloom bodies", bloom_burst(64, 32 * 1024)),
        ("an unknown command", unknown_then_ping),
    ];

    let mut stalled = Vec::new();
    for (case, bytes) in cases {
        let (mut peer, local) = tokio::io::duplex(bytes.len());
        peer.write_all(&bytes).await.unwrap();
        let mut reader = FramedRead::new(local, full_codec());
        // The clock is paused, so the timeout fires as soon as the reader waits.
        let next = tokio::time::timeout(std::time::Duration::from_secs(60), reader.next()).await;
        if !matches!(next, Ok(Some(Ok(Message::Ping(_))))) {
            stalled.push((case, format!("{next:?}")));
        }
        drop(peer);
    }
    assert!(
        stalled.is_empty(),
        "the ping stalled behind discarded commands: {stalled:?}"
    );
}

/// L.handshake: a `version` user agent above its limit stops at its length.
#[test]
fn a_long_user_agent_stops_at_its_length() {
    // Every fixed field of a version message, then a user agent one byte too long.
    let mut body = vec![0; 4 + 8 + 8 + 26 + 26 + 8];
    let too_long = MAX_USER_AGENT_LENGTH + 1;
    CompactSizeMessage::try_from(too_long)
        .unwrap()
        .zcash_serialize(&mut body)
        .unwrap();
    body.extend(std::iter::repeat_n(b'x', too_long));
    let mut src = BytesMut::from(&message(b"version\0\0\0\0\0", &body)[..]);
    assert!(Codec::builder()
        .for_network(&Network::Mainnet)
        .finish()
        .decode(&mut src)
        .is_err());
}
