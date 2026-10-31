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

/// The handshake phase applies its smaller limit to every command.
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

/// Ignored Bloom commands pass their checksum and leave nothing behind. The decoder
/// continues to the next message in the same turn, as the protocol rule requires.
#[test]
fn ignored_commands_are_discarded_after_their_checksum() {
    let ping = message(b"ping\0\0\0\0\0\0\0\0", &7u64.to_le_bytes());
    for command in BLOOM_FILTER_COMMANDS {
        let body = vec![1; 1024];
        let mut bytes = message(&command, &body);
        bytes.extend_from_slice(&ping);
        let mut codec = full_codec();
        let mut src = BytesMut::from(&bytes[..]);
        assert!(matches!(codec.decode(&mut src), Ok(Some(Message::Ping(_)))));
        assert!(src.is_empty());

        // A bad checksum stays fatal, with no decoded message.
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
    assert!(matches!(codec.decode(&mut src), Ok(None)));
    assert_eq!(src.len(), ping.len(), "the discarded body left the buffer");
    assert!(matches!(codec.decode(&mut src), Ok(Some(Message::Ping(_)))));

    let mut bad = header(UNKNOWN_COMMAND, &[0], 1);
    bad.push(1);
    let mut src = BytesMut::from(&bad[..]);
    assert!(full_codec().decode(&mut src).is_err());
}
