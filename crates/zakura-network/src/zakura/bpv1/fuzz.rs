//! BPv1 fuzz drivers and what they share (`tool-2`, `p2p-fuzz`).
//!
//! The machine replays live beside their machines (`peer/fuzz.rs`,
//! `blocks/fuzz.rs`), where they reach private state for the drain checks.
//! This module holds the targets that span more than one machine or no
//! machine at all:
//!
//! - [`replay_bpv1_mesh_bytes`]: four nodes of production `PeerState`,
//!   `BlockTable`, `ProposerState` and `PeerPaths` machines, routed through
//!   the production router and the production frame codec;
//! - [`replay_bpv1_wire_bytes`]: every BPv1 frame decoder on arbitrary bytes;
//! - [`replay_bpv1_codec_bytes`]: encoding, any-`k` decoding, the re-encode
//!   check, part proofs and canonical assembly.
//!
//! [`quality`] names what each target can reach and measures how much of it
//! a run of inputs reached. [`seed_inputs`] lists the in-code seeds.

use zakura_quic::NodeId;

use super::{
    codec::{encode_body, EncodedBody},
    profile::Profile,
};

pub(crate) mod chain;
mod codec;
mod mesh;
pub mod quality;
mod seeds;
#[cfg(test)]
mod tests;
mod wire;

pub use codec::{replay_bpv1_codec_bytes, CodecReplaySummary};
pub use mesh::{replay_bpv1_mesh_bytes, MeshReplaySummary};
pub use quality::{measure_bpv1_targets, Reached, TargetQuality};
pub use seeds::{seed_inputs, Bpv1Seed};
pub use wire::{replay_bpv1_wire_bytes, WireReplaySummary};

/// Reads fuzz bytes front to back. Past the end, every read returns zero, so
/// a short input still decodes to a complete history.
#[derive(Clone, Debug)]
pub(crate) struct FuzzBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> FuzzBytes<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Whether every byte has been read.
    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The next byte, or zero past the end.
    pub(crate) fn byte(&mut self) -> u8 {
        match self.bytes.split_first() {
            Some((&first, rest)) => {
                self.bytes = rest;
                first
            }
            None => 0,
        }
    }

    /// A little-endian `u16`.
    pub(crate) fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.byte(), self.byte()])
    }

    /// Up to `len` of the remaining bytes.
    pub(crate) fn take(&mut self, len: usize) -> &'a [u8] {
        let (taken, rest) = self.bytes.split_at(len.min(self.bytes.len()));
        self.bytes = rest;
        taken
    }
}

/// A node id of 32 `b` bytes. It skips key derivation, which costs
/// milliseconds per input. BPv1 machines only compare and hash node ids.
pub(crate) fn fuzz_node(b: u8) -> NodeId {
    NodeId::from_bytes(&[b; 32])
}

/// One block body the drivers encode, named for the layout it reaches.
#[derive(Clone, Debug)]
pub(crate) struct Geometry {
    /// The layout this body reaches.
    pub(crate) name: &'static str,
    /// The encoded body.
    pub(crate) encoded: EncodedBody,
}

/// The `TINY` bodies: `(name, body bytes, tx_count)`.
///
/// - `empty`: no parts; the block assembles from `HeaderMeta` alone.
/// - `trimmed`: one stripe of `k = 3` whose last source part crosses the
///   wire shorter than `S` (decision-18).
/// - `exact`: one full stripe of `k = 4`, no trimming.
/// - `multi-stripe`: three stripes of `k = 4` over ten source parts, so two
///   source slots are padding (decision-18).
const TINY_BODIES: [(&str, usize, u32); 4] = [
    ("layout:empty", 0, 1),
    ("layout:trimmed", 21, 2),
    ("layout:exact", 32, 2),
    ("layout:multi-stripe", 300, 3),
];

/// The `W2` bodies. A second W2 stripe needs a 256 KiB body, which costs
/// milliseconds to encode per input, so multi-stripe layouts run under
/// `TINY` only.
const W2_BODIES: [(&str, usize, u32); 3] = [
    ("layout:empty", 0, 1),
    ("layout:trimmed", 10_000, 2),
    ("layout:exact", 16_384, 2),
];

/// The bodies for `profile`: the `W2` list for the production profile, the
/// `TINY` list otherwise. Each body's bytes are a pattern salted by `salt`,
/// so two blocks of one layout have distinct parts.
pub(crate) fn geometries(profile: &Profile, salt: u8) -> Vec<Geometry> {
    let bodies: &[(&str, usize, u32)] = if *profile == Profile::W2 {
        &W2_BODIES
    } else {
        &TINY_BODIES
    };
    bodies
        .iter()
        .map(|&(name, len, tx_count)| {
            // `i % 251` is below 256, so the cast keeps every value.
            let body: Vec<u8> = (0..len)
                .map(|i| ((i % 251) as u8).wrapping_mul(31).wrapping_add(salt))
                .collect();
            Geometry {
                name,
                encoded: encode_body(profile, &body, tx_count)
                    .expect("every listed body fits its profile"),
            }
        })
        .collect()
}
