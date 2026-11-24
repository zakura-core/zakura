//! Decoder for the `bpv1_peer` fuzz target (`tool-2`).

use std::time::Duration;

use zakura_chain::block;
use zakura_rt::time::Instant;

use super::{
    BlockAdmitted, PeerConfig, PeerInput, PeerState, PullGranted, RecvHave, RecvPartPrefix,
    RecvWant, RecvWantEnd, Retire,
};
use crate::zakura::bpv1::{
    commitment::CoinbaseBinding,
    paths::Arrival,
    profile::Profile,
    wire::{part_payload_len, Bpv1Message, HaveFlags, WantBitmap, HAVE_FLUSH_INTERVAL},
};

/// Summary returned by [`replay_bpv1_peer_bytes`] (`tool-2`, `p2p-fuzz`).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerReplaySummary {
    /// Total inputs replayed.
    pub steps: usize,
    /// Protocol close row triggered, if any.
    pub closed_row: Option<u8>,
}

/// Replays arbitrary fuzz bytes as a sequence of [`PeerInput`]s against a fresh
/// [`PeerState`] (`Profile::TINY`), asserting its invariants after every step
/// and a full drain on `SessionEnd` (`tool-2`).
pub fn replay_bpv1_peer_bytes(bytes: &[u8]) -> PeerReplaySummary {
    use zakura_quic::NodeSecretKey;

    let local = NodeSecretKey::from_bytes(&[1u8; 32]).public();
    let remote = NodeSecretKey::from_bytes(&[2u8; 32]).public();
    let third = NodeSecretKey::from_bytes(&[3u8; 32]).public();
    let config = PeerConfig::for_profile(Profile::TINY);
    let mut peer = PeerState::new(config, local, remote, 1);
    let mut now = Instant::now();
    let coding = Profile::TINY.derive_coding(256, 2).expect("TINY coding");
    // An empty body has no parts, so every index and stripe is out of its range.
    let empty = Profile::TINY
        .derive_coding(0, 1)
        .expect("TINY empty coding");
    let block_a = block::Hash([0x11; 32]);
    let block_b = block::Hash([0x22; 32]);
    let block_empty = block::Hash([0x33; 32]);
    let mut cursor = 0usize;
    let mut steps = 0usize;

    while cursor < bytes.len() {
        let op = bytes[cursor];
        let arg = bytes.get(cursor + 1).copied().unwrap_or(0);
        cursor += 2;
        // Op bit 0x80 picks block B over A; with it, arg bit 0x80 picks the
        // empty block instead. Indices and stripes keep A's geometry.
        let (block_id, block_coding) = match (op & 0x80 != 0, arg & 0x80 != 0) {
            (false, _) => (block_a, coding),
            (true, false) => (block_b, coding),
            (true, true) => (block_empty, empty),
        };
        let input = match op & 0x0F {
            // Op bit 0x40 names a third node as the proposer, so pushes
            // reach the remote (decision-24). Op bit 0x20 puts the remote
            // outside the `Have` initial group (decision-26).
            0 => PeerInput::BlockAdmitted(BlockAdmitted {
                block_id,
                coding: block_coding,
                proposer_node: if op & 0x40 != 0 { third } else { remote },
                header_meta: Some(Box::new(Bpv1Message::BlockDone { block_id })),
                as_proposer: false,
                have_held_until: (op & 0x20 != 0).then(|| now + Duration::from_millis(500)),
            }),
            // The remote proposes (decision-19).
            1 => PeerInput::RecvHeaderMeta {
                block_id,
                message: Box::new(Bpv1Message::HeaderMeta {
                    header: block_id.0.to_vec(),
                    binding: CoinbaseBinding {
                        coinbase: vec![0x01],
                        txid_siblings: Vec::new(),
                        auth_siblings: Vec::new(),
                        light_client_root: [0; 32],
                    },
                    coding: block_coding,
                    part_root: block_id.0,
                    proposer_node: remote,
                }),
            },
            2 => {
                let index = u32::from(arg) % 24;
                PeerInput::RecvPartPrefix(RecvPartPrefix {
                    block_id,
                    index,
                    payload: vec![0; part_payload_len(&coding, index).unwrap_or(36)],
                })
            }
            // Op bit 0x10 sets the push flag, and bit 0x20 the relay flag
            // with it (decision-24).
            3 => PeerInput::RecvHave(RecvHave {
                block_id,
                flags: match op & 0x30 {
                    0 => HaveFlags::Plain,
                    0x10 => HaveFlags::Push,
                    _ => HaveFlags::Relay,
                },
                indices: vec![u32::from(arg) % 24],
                frame_bytes: 38,
            }),
            4 => {
                let mut bm = WantBitmap::EMPTY;
                bm.set(u32::from(arg) % coding.n);
                PeerInput::PullGranted(PullGranted {
                    block_id,
                    stripe: u32::from(arg >> 4) % coding.stripes,
                    requested: bm,
                    arrival: Arrival {
                        at: now + Duration::from_millis(50),
                        rtt: Duration::from_millis(20),
                        rate_bps: 1_000_000,
                    },
                })
            }
            5 => {
                let mut bm = WantBitmap::EMPTY;
                bm.set(u32::from(arg) % coding.n);
                PeerInput::RecvWant(RecvWant {
                    want_id: u32::from(arg).max(1),
                    block_id,
                    stripe: 0,
                    requested: bm,
                })
            }
            6 => PeerInput::RecvWantEnd(RecvWantEnd {
                want_id: u32::from(arg).max(1),
                sent: u16::from(arg & 3),
            }),
            7 => PeerInput::RecvStripeDone {
                block_id,
                stripe: u32::from(arg) % coding.stripes,
            },
            8 => PeerInput::RecvBlockDone { block_id },
            9 => PeerInput::LocalStripeDone {
                block_id,
                stripe: u32::from(arg) % coding.stripes,
            },
            10 => PeerInput::LocalBlockDone { block_id },
            // Op bit 0x10 marks the index as this node's pushed seed (decision-24).
            11 => PeerInput::Ready {
                block_id,
                indices: vec![u32::from(arg) % coding.total_parts().unwrap_or(1)],
                push: op & 0x10 != 0,
            },
            12 => PeerInput::Retire(Retire {
                block_id,
                stripes: block_coding.stripes,
                n: block_coding.n,
            }),
            13 => {
                now += Duration::from_secs(u64::from(arg % 15));
                PeerInput::Tick
            }
            14 if arg == 0xFF => PeerInput::ControlStalled,
            _ => {
                now += HAVE_FLUSH_INTERVAL;
                PeerInput::Tick
            }
        };
        let _ = peer.step(now, input);
        peer.assert_invariants();
        steps += 1;
    }

    let closed_row = peer.close_cause().map(|c| {
        // A fieldless `repr(u8)` enum's discriminant is its declared row number.
        c.row as u8
    });
    let _ = peer.step(now, PeerInput::SessionEnd);
    peer.assert_drained();

    PeerReplaySummary { steps, closed_row }
}
