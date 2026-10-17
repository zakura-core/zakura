#![allow(dead_code, unused_imports)] // activation removes this: the reactor still speaks version 8

//! Header sync version 9, which the peer message regulation specification
//! defines in "Header sync — stream 5, version 9".
//!
//! This module holds the wire codec, the message table, the publisher, and the
//! subscriber. Nothing negotiates or uses them yet: the reactor, its stream
//! declaration, and the capability bits stay on version 8 until activation
//! switches them all at once.

mod publisher;
mod subscriber;
mod wire;

#[cfg(any(test, feature = "header-fuzz"))]
use crate::zakura::{
    wire_codec::{decode_frame, encode_frame, WireError, WireMessage},
    Frame,
};
pub(crate) use wire::*;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod wire_tests;

/// Decode fuzz bytes as one version 9 frame, and check that a decoded message
/// re-encodes to the same payload.
///
/// Bit 7 of the first byte picks Regtest headers over Mainnet and Testnet
/// headers, and its low two bits pick the message type. The remaining bytes
/// are the payload. Returns whether the frame decoded.
///
/// # Panics
///
/// If a decoded message does not re-encode to its payload.
#[cfg(any(test, feature = "header-fuzz"))]
pub fn replay_header_sync_v9_codec_bytes(bytes: &[u8]) -> bool {
    let Some((&selector, payload)) = bytes.split_first() else {
        return false;
    };
    let frame = Frame {
        message_type: u16::from(selector & 3) + 1,
        flags: 0,
        payload: payload.to_vec(),
    };
    if selector & 0x80 == 0 {
        replay::<CommonMessage>(&frame)
    } else {
        replay::<RegtestMessage>(&frame)
    }
}

/// Decode `frame` as an `M`, and check that it re-encodes to the same payload.
/// Returns whether it decoded.
#[cfg(any(test, feature = "header-fuzz"))]
fn replay<M: WireMessage<Error = WireError>>(frame: &Frame) -> bool {
    let Ok(message) = decode_frame::<M>(frame) else {
        return false;
    };
    let encoded = encode_frame(&message).expect("a decoded message encodes");
    assert_eq!(
        encoded.payload, frame.payload,
        "a decoded message re-encodes to its payload"
    );
    true
}
