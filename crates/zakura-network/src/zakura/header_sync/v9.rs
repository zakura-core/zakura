#![allow(dead_code, unused_imports)] // activation removes this: the reactor still speaks version 8

//! Header sync version 9: the subscription protocol on stream 5.
//!
//! Version 9 replaces `GetHeaders` with `SubscribeHeaders`, whose `Open`,
//! `Grant`, and `Close` updates control a credit window of pushed `Headers`
//! pages. The rules are in the peer message regulation specification, section
//! "Header sync — stream 5, version 9".
//!
//! This module holds the wire codec and the message table. Nothing negotiates
//! or uses them yet: the reactor, its stream declaration, and the capability
//! bits stay on version 8 until activation switches them all at once.

mod wire;

#[cfg(any(test, feature = "header-fuzz"))]
use super::config::{COMMON_HEADER_BYTES, REGTEST_HEADER_BYTES};

pub(crate) use wire::*;

#[cfg(test)]
mod wire_tests;

/// Decode fuzz bytes as one version 9 frame, and check that a decoded message
/// re-encodes to the same payload.
///
/// The first byte selects the frame: bit 7 picks Regtest headers over Mainnet
/// and Testnet headers, bits 3 through 6 are the frame flags, and bits 0
/// through 2 are the message type. The remaining bytes are the payload.
/// Returns whether the frame decoded.
///
/// # Panics
///
/// If a frame with flags or an undeclared message type decodes, or a decoded
/// message does not re-encode to its payload and decode back to itself.
#[cfg(any(test, feature = "header-fuzz"))]
pub fn replay_header_sync_v9_codec_bytes(bytes: &[u8]) -> bool {
    let Some((&selector, payload)) = bytes.split_first() else {
        return false;
    };
    let frame = crate::zakura::Frame {
        message_type: u16::from(selector & 0x07),
        flags: u16::from((selector >> 3) & 0x0f),
        payload: payload.to_vec(),
    };
    if selector & 0x80 == 0 {
        replay_frame::<COMMON_HEADER_BYTES>(&frame)
    } else {
        replay_frame::<REGTEST_HEADER_BYTES>(&frame)
    }
}

/// Decode `frame` as a version 9 message for `HEADER_BYTES`-byte headers, and
/// check its canonical re-encoding. Returns whether it decoded.
#[cfg(any(test, feature = "header-fuzz"))]
fn replay_frame<const HEADER_BYTES: usize>(frame: &crate::zakura::Frame) -> bool {
    use crate::zakura::wire_codec::{decode_frame, encode_frame, WireMessage};

    let Ok(message) = decode_frame::<Message<HEADER_BYTES>>(frame) else {
        return false;
    };
    assert_eq!(frame.flags, 0, "a frame with flags decoded");
    assert!(
        crate::zakura::MessageRule::find(Message::<HEADER_BYTES>::RULES, frame.message_type)
            .is_some(),
        "an undeclared message type decoded"
    );
    let encoded = encode_frame(&message).expect("a decoded message encodes");
    assert_eq!(
        encoded.payload, frame.payload,
        "a decoded message re-encodes to its payload"
    );
    assert_eq!(
        decode_frame::<Message<HEADER_BYTES>>(&encoded).expect("a canonical encoding decodes"),
        message
    );
    true
}
