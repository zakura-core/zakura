//! Decoder for the `bpv1_block` fuzz target (`tool-2`).

use std::time::Duration;

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    BlockConfig, BlockInput, BlockOutput, BlockTable, HeaderAdmissionResult, MetaEnvelope,
    PullPeerState, ReleaseReason, WorkId,
};
use crate::zakura::bpv1::peer::PartAuth;
use crate::zakura::bpv1::{
    close::Bpv1CloseRow, codec::encode_body, commitment::CoinbaseBinding, paths::PathEstimate,
    profile::Profile, unknown_parent::UnknownParentVerdict,
};

/// The verified tip hash and every admitted header's parent, so a block one
/// above the verified tip extends it.
const FUZZ_TIP_HASH: block::Hash = block::Hash([0; 32]);

/// Summary returned by [`replay_bpv1_block_bytes`] (`tool-2`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockReplaySummary {
    /// Number of decoded inputs applied.
    pub steps: usize,
    /// Final pending metadata count.
    pub pending_meta: usize,
    /// Final retained part count across active blocks.
    pub retained_parts: usize,
}

/// Deterministically replays arbitrary fuzz bytes through a fresh [`BlockTable`]
/// using [`Profile::TINY`], asserting its invariants after every step (`tool-2`).
pub fn replay_bpv1_block_bytes(bytes: &[u8]) -> BlockReplaySummary {
    use zakura_quic::NodeSecretKey;

    let config = BlockConfig::for_profile(Profile::TINY);
    let mut table = BlockTable::new(config);
    let mut now = Instant::now();
    let peers: [NodeId; 3] = [
        NodeSecretKey::from_bytes(&[1u8; 32]).public(),
        NodeSecretKey::from_bytes(&[2u8; 32]).public(),
        NodeSecretKey::from_bytes(&[3u8; 32]).public(),
    ];
    let encoded = encode_body(&Profile::TINY, &[0x33; 128], 2).expect("TINY 128-byte body encodes");
    let coding = encoded.coding;
    // The third block has an empty body: no parts, so it assembles at admission.
    let empty = encode_body(&Profile::TINY, &[], 1).expect("TINY empty body encodes");
    let blocks = [
        (block::Hash([0x10; 32]), &encoded),
        (block::Hash([0x20; 32]), &encoded),
        (block::Hash([0x30; 32]), &empty),
    ];
    let mut pending_works: Vec<WorkId> = Vec::new();
    let mut steps = 0usize;

    for chunk in bytes.chunks(3) {
        let op = chunk[0] % 13;
        let a = *chunk.get(1).unwrap_or(&0);
        let b = *chunk.get(2).unwrap_or(&0);
        now += Duration::from_millis(u64::from(b % 8) * 500);
        let peer = peers[usize::from(a % 3)];
        let (block_id, body) = blocks[usize::from(a >> 2) % blocks.len()];
        let peer_states: Vec<PullPeerState> = peers
            .iter()
            .map(|&p| PullPeerState {
                peer: p,
                ahead_bytes: 0,
                can_pull: true,
                window_remaining: 8,
            })
            .collect();

        let input = match op {
            0 => BlockInput::TipHeight {
                height: u32::from(b),
                hash: FUZZ_TIP_HASH,
            },
            1 => {
                let env = MetaEnvelope {
                    block_id,
                    header_bytes: vec![0xAA; 40],
                    binding: CoinbaseBinding {
                        coinbase: vec![0x01],
                        txid_siblings: Vec::new(),
                        auth_siblings: Vec::new(),
                        light_client_root: [0u8; 32],
                    },
                    coding: body.coding,
                    part_root: body.part_root,
                    proposer_node: peers[0],
                };
                BlockInput::MetaReceived {
                    envelope: Box::new(env),
                    from_peer: peer,
                    session_gen: 1,
                }
            }
            2 => {
                let wid = pending_works.pop().unwrap_or(WorkId(u64::from(b)));
                let result = match a % 4 {
                    0 => HeaderAdmissionResult::Admitted {
                        height: table.top().saturating_add(1),
                        parent_hash: FUZZ_TIP_HASH,
                    },
                    1 => HeaderAdmissionResult::MissingParent {
                        parent_hash: block::Hash([0x01; 32]),
                    },
                    2 => HeaderAdmissionResult::PeerInvalid,
                    _ => HeaderAdmissionResult::LocalError,
                };
                BlockInput::HeaderAdmitted {
                    work_id: wid,
                    result,
                }
            }
            3 => {
                let wid = pending_works.pop().unwrap_or(WorkId(u64::from(b)));
                BlockInput::CommitmentChecked {
                    work_id: wid,
                    ok: (a & 1) == 0,
                    peers: peers.to_vec(),
                    rng_seed: u64::from(b),
                }
            }
            4 => BlockInput::ParentAdmitted {
                hash: block::Hash([0x01; 32]),
                height: table.top(),
            },
            5 => BlockInput::PathObserved {
                peer,
                estimate: PathEstimate {
                    min_rtt: Some(Duration::from_millis(u64::from(b.max(1)))),
                    rate_in: Some(u64::from(a.max(1)) * 50_000),
                    rate_out: Some(u64::from(a.max(1)) * 50_000),
                },
            },
            6 => BlockInput::PeerAnnounced {
                peer,
                block_id,
                indices: vec![u32::from(b % (coding.n as u8))],
                peer_states,
            },
            7 => BlockInput::ReleaseClaims {
                peer,
                block_id,
                stripe: 0,
                slots: vec![u32::from(b % (coding.n as u8))],
                reason: [
                    ReleaseReason::Late,
                    ReleaseReason::Failed,
                    ReleaseReason::Refused,
                ][usize::from(a % 3)],
                peer_states,
            },
            8 => BlockInput::SessionEnded { peer, peer_states },
            9 => {
                // The IO shell charges a part before proof; a refused
                // charge leaves the uncharged insert a no-op.
                let part = encoded.parts[usize::from(b) % encoded.parts.len()].clone();
                // usize -> u64 widens on every supported target.
                let bytes = part.payload().len() as u64;
                let _ = table.charge_part(now, &block_id, part.index(), peer, bytes);
                BlockInput::InsertPart {
                    block_id,
                    part,
                    from_peer: peer,
                    auth: [PartAuth::Seed, PartAuth::Push, PartAuth::Want(1)][usize::from(a % 3)],
                    peer_states,
                }
            }
            // The proposer flags `b`'s bits among indices 0..8 for relay: this
            // node's pushed seeds (decision-19, decision-24).
            10 => BlockInput::PushFlagged {
                peer: peers[0],
                block_id,
                indices: (0..8u32).filter(|i| b & (1 << i) != 0).collect(),
                relay: true,
            },
            // A holder flags `b`'s bits among indices 0..8 for push (decision-24).
            11 => BlockInput::PushFlagged {
                peer,
                block_id,
                indices: (0..8u32).filter(|i| b & (1 << i) != 0).collect(),
                relay: false,
            },
            // Odd `a` answers the latest unknown-parent check.
            _ if a & 1 == 1 => BlockInput::UnknownParentChecked {
                work_id: pending_works.pop().unwrap_or(WorkId(u64::from(b))),
                verdict: [
                    UnknownParentVerdict::Park,
                    UnknownParentVerdict::Drop,
                    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row3InvalidHeader),
                    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
                ][usize::from(b % 4)],
            },
            _ => BlockInput::Tick { peer_states },
        };

        let out = table.step(now, input);
        table.assert_invariants();
        for o in out.outputs {
            match o {
                BlockOutput::AdmitHeader { work_id, .. }
                | BlockOutput::CheckUnknownParent { work_id, .. }
                | BlockOutput::VerifyCommitment { work_id, .. } => {
                    pending_works.push(work_id);
                }
                _ => {}
            }
        }
        steps += 1;
    }

    BlockReplaySummary {
        steps,
        pending_meta: table.pending_meta_len(),
        retained_parts: table.retained_part_count(),
    }
}
